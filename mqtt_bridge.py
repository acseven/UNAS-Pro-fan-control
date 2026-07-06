#!/usr/bin/python3
"""MQTT bridge for UNAS-Pro-fan-control.

Publishes the sensor snapshot written by fan_control.sh (via
fan_control_state.sh) to an MQTT broker with Home Assistant device-based
discovery, and lets Home Assistant tune the fan-curve target temperatures by
writing /root/fan_control.conf (sourced by fan_control.sh every loop).

Design constraints (see README):
- stdlib only: UniFi OS firmware updates wipe apt packages, but /root and
  python3 survive. So this file hand-rolls a minimal MQTT 3.1.1 client
  (CONNECT with auth + LWT, PUBLISH QoS 0, SUBSCRIBE, PINGREQ) instead of
  depending on paho-mqtt or mosquitto-clients.
- Fan safety is never in this process: fan_control.sh runs its own loop and
  only reads the curve parameters from a conf file. If this bridge dies, fans
  keep working.

Usage:
  mqtt_bridge.py             run the bridge (needs /root/mqtt_bridge.conf)
  mqtt_bridge.py --clear     remove the device from Home Assistant (publishes
                             an empty retained discovery config) and exit
  mqtt_bridge.py --selftest  run offline packet/logic self-checks and exit

Repo: https://github.com/hoxxep/UNAS-Pro-fan-control
License: MIT
"""

import json
import os
import re
import select
import signal
import socket
import ssl
import struct
import sys
import time

VERSION = "1.0.0"

BRIDGE_CONF = os.environ.get("MQTT_BRIDGE_CONF", "/root/mqtt_bridge.conf")
FAN_CONF = os.environ.get("FAN_CONF", "/root/fan_control.conf")
STATE_FILE = os.environ.get("FAN_STATE_FILE", "/run/fan_control/state.json")
KEEPALIVE = 60
STATE_EXPIRE_AFTER = 180  # seconds; ~3x the 60s fan_control loop

# Curve parameters exposed as Home Assistant number entities. Ranges are hard
# caps chosen to sit below the fixed *_MAX ceilings in fan_control.sh, so a
# slider can never invert the curve (TGT >= MAX). MIN_FAN is exposed as a
# percentage but stored raw (0-255) in the conf, matching fan_control.sh.
PARAMS = {
    "sys_tgt": {"conf_key": "SYS_TGT", "name": "System target temp", "min": 35, "max": 65, "unit": "°C"},
    "hdd_tgt": {"conf_key": "HDD_TGT", "name": "HDD target temp", "min": 25, "max": 45, "unit": "°C"},
    "ssd_tgt": {"conf_key": "SSD_TGT", "name": "SSD target temp", "min": 35, "max": 62, "unit": "°C"},
    "min_fan_pct": {"conf_key": "MIN_FAN", "name": "Minimum fan speed", "min": 10, "max": 100, "unit": "%"},
}


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Minimal MQTT 3.1.1 encoding/decoding
# ---------------------------------------------------------------------------

def encode_varint(n):
    out = bytearray()
    while True:
        byte = n % 128
        n //= 128
        out.append(byte | 0x80 if n else byte)
        if not n:
            return bytes(out)


def encode_str(s):
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b


def connect_packet(client_id, username, password, will_topic, will_payload):
    flags = 0x02  # clean session
    payload = encode_str(client_id)
    if will_topic:
        flags |= 0x04 | 0x20  # will flag + will retain, QoS 0
        payload += encode_str(will_topic) + encode_str(will_payload)
    if username:
        flags |= 0x80
        payload += encode_str(username)
        if password is not None:
            flags |= 0x40
            payload += encode_str(password)
    var = encode_str("MQTT") + bytes([4, flags]) + struct.pack(">H", KEEPALIVE)
    body = var + payload
    return bytes([0x10]) + encode_varint(len(body)) + body


def publish_packet(topic, payload, retain=False):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    body = encode_str(topic) + payload
    return bytes([0x30 | (0x01 if retain else 0x00)]) + encode_varint(len(body)) + body


def subscribe_packet(packet_id, topics):
    body = struct.pack(">H", packet_id)
    for t in topics:
        body += encode_str(t) + b"\x00"  # QoS 0
    return bytes([0x82]) + encode_varint(len(body)) + body


PINGREQ = bytes([0xC0, 0x00])
DISCONNECT = bytes([0xE0, 0x00])


def parse_publish(flags, body):
    """Parse an incoming PUBLISH body -> (topic, payload)."""
    tlen = struct.unpack(">H", body[:2])[0]
    topic = body[2:2 + tlen].decode("utf-8", "replace")
    pos = 2 + tlen
    if (flags >> 1) & 0x03:  # QoS > 0 carries a packet id (we subscribe QoS 0)
        pos += 2
    return topic, body[pos:]


class MqttClient:
    """Blocking-socket MQTT 3.1.1 client, QoS 0 only."""

    def __init__(self, conf):
        self.conf = conf
        self.sock = None
        self.last_send = 0.0
        self.on_message = None

    def connect(self, will_topic, will_payload):
        raw = socket.create_connection((self.conf["host"], self.conf["port"]), timeout=15)
        if self.conf["tls"]:
            # Certificate verification is always on. For a self-signed broker
            # cert, point MQTT_TLS_CA at the CA/cert file instead.
            ctx = ssl.create_default_context(cafile=self.conf["tls_ca"] or None)
            raw = ctx.wrap_socket(raw, server_hostname=self.conf["host"])
        self.sock = raw
        self.sock.settimeout(15)
        self._send(connect_packet(self.conf["client_id"], self.conf["user"],
                                  self.conf["password"], will_topic, will_payload))
        ptype, _, body = self._read_packet()
        if ptype != 0x20 or len(body) < 2 or body[1] != 0:
            rc = body[1] if len(body) >= 2 else -1
            raise ConnectionError("CONNACK refused (return code %d; 4/5 = bad credentials)" % rc)

    def subscribe(self, topics):
        self._send(subscribe_packet(1, topics))
        # SUBACK may arrive after queued retained PUBLISHes on some brokers;
        # accept both until the SUBACK shows up.
        deadline = time.time() + 15
        while time.time() < deadline:
            ptype, flags, body = self._read_packet()
            if ptype == 0x90:
                if any(rc == 0x80 for rc in body[2:]):
                    raise ConnectionError("SUBACK reported failure")
                return
            self._dispatch(ptype, flags, body)
        raise ConnectionError("no SUBACK")

    def publish(self, topic, payload, retain=False):
        self._send(publish_packet(topic, payload, retain))

    def loop(self, timeout):
        """Process incoming packets for up to `timeout` seconds; keepalive."""
        readable, _, _ = select.select([self.sock], [], [], timeout)
        if readable:
            ptype, flags, body = self._read_packet()
            self._dispatch(ptype, flags, body)
        if time.time() - self.last_send > KEEPALIVE / 2:
            self._send(PINGREQ)

    def disconnect(self):
        try:
            self._send(DISCONNECT)
            self.sock.close()
        except OSError:
            pass

    def _dispatch(self, ptype, flags, body):
        if ptype == 0x30 and self.on_message:
            topic, payload = parse_publish(flags, body)
            self.on_message(topic, payload)
        # PINGRESP (0xD0) and anything else: nothing to do at QoS 0.

    def _send(self, data):
        self.sock.sendall(data)
        self.last_send = time.time()

    def _recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("socket closed")
            buf += chunk
        return buf

    def _read_packet(self):
        first = self._recv_exact(1)[0]
        remaining, mult = 0, 1
        for _ in range(4):
            byte = self._recv_exact(1)[0]
            remaining += (byte & 0x7F) * mult
            if not byte & 0x80:
                break
            mult *= 128
        else:
            raise ConnectionError("bad varint")
        body = self._recv_exact(remaining) if remaining else b""
        return first & 0xF0, first & 0x0F, body


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def parse_conf_file(path):
    """Parse a bash-style KEY=value file (comments, optional quotes)."""
    conf = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            conf[key.strip()] = value
    return conf


def sanitize_id(s):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", s)


def load_bridge_conf(path):
    raw = parse_conf_file(path)
    if "MQTT_HOST" not in raw:
        sys.exit("MQTT_HOST missing from %s" % path)
    device_id = sanitize_id(raw.get("MQTT_DEVICE_ID") or socket.gethostname().split(".")[0].lower())
    return {
        "host": raw["MQTT_HOST"],
        "port": int(raw.get("MQTT_PORT", "1883")),
        "user": raw.get("MQTT_USER") or None,
        "password": raw.get("MQTT_PASS"),
        "tls": raw.get("MQTT_TLS", "").lower() in ("1", "true", "yes"),
        "tls_ca": raw.get("MQTT_TLS_CA", ""),
        "device_id": device_id,
        "client_id": ("unasfc-" + device_id)[:23],
        "discovery_prefix": raw.get("MQTT_DISCOVERY_PREFIX", "homeassistant"),
    }


def write_fan_conf(params_raw):
    """Atomically (re)write the fan_control.sh override conf. Owns the file."""
    lines = [
        "# Written by mqtt_bridge.py -- fan-curve overrides tuned from Home Assistant.",
        "# Sourced by fan_control.sh on every loop iteration. Deleting this file",
        "# returns fan_control.sh to the defaults hardcoded at the top of the script.",
    ] + ["%s=%d" % (k, v) for k, v in sorted(params_raw.items())]
    tmp = FAN_CONF + ".tmp"
    with open(tmp, "w") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp, FAN_CONF)


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def pct_to_raw(pct):
    return round(pct * 255 / 100)


def raw_to_pct(raw):
    return round(raw * 100 / 255)


# ---------------------------------------------------------------------------
# Home Assistant discovery
# ---------------------------------------------------------------------------

def topics(conf):
    base = "unas_fan_control/" + conf["device_id"]
    return {
        "state": base + "/state",
        "availability": base + "/availability",
        "command": base + "/set/",  # + param key
        "discovery": "%s/device/%s/config" % (conf["discovery_prefix"], conf["device_id"]),
    }


def discovery_payload(conf, state):
    t = topics(conf)
    uid = conf["device_id"]

    def sensor(key, name, template, unit, device_class=None):
        c = {
            "p": "sensor",
            "name": name,
            "unique_id": "%s_%s" % (uid, key),
            "value_template": template,
            "unit_of_measurement": unit,
            "state_class": "measurement",
            "expire_after": STATE_EXPIRE_AFTER,
        }
        if device_class:
            c["device_class"] = device_class
        return c

    cmps = {
        "sys_temp": sensor("sys_temp", "System temperature",
                           "{{ value_json.sys_temp }}", "°C", "temperature"),
        "hdd_temp": sensor("hdd_temp", "HDD temperature (max)",
                           "{{ value_json.hdd_temp }}", "°C", "temperature"),
        "ssd_temp": sensor("ssd_temp", "SSD temperature (max)",
                           "{{ value_json.ssd_temp }}", "°C", "temperature"),
        "fan_duty": sensor("fan_duty", "Fan duty",
                           "{{ value_json.fan_duty_pct }}", "%"),
    }
    for serial in sorted(state.get("drives", {})):
        d = state["drives"][serial]
        key = "drive_" + sanitize_id(serial)
        name = "%s %s temperature" % (d.get("class", "Drive"), os.path.basename(d.get("dev", serial)))
        cmps[key] = sensor(key, name,
                           "{{ value_json.drives['%s'].temp }}" % serial, "°C", "temperature")
    for tach in sorted(state.get("tachs", {})):
        key = "tach_" + sanitize_id(tach)
        cmps[key] = sensor(key, "Fan %s" % tach,
                           "{{ value_json.tachs['%s'] }}" % tach, "rpm")

    for pkey, spec in PARAMS.items():
        template = ("{{ (value_json.params.min_fan * 100 / 255) | round(0) }}"
                    if pkey == "min_fan_pct"
                    else "{{ value_json.params.%s }}" % pkey)
        cmps[pkey] = {
            "p": "number",
            "name": spec["name"],
            "unique_id": "%s_%s" % (uid, pkey),
            "command_topic": t["command"] + pkey,
            "value_template": template,
            "min": spec["min"],
            "max": spec["max"],
            "step": 1,
            "mode": "slider",
            "unit_of_measurement": spec["unit"],
            "entity_category": "config",
        }

    return {
        "dev": {
            "ids": [uid],
            "name": "UNAS Fan Control",
            "mf": "Ubiquiti",
            "sw": VERSION,
        },
        "o": {
            "name": "unas-fan-control",
            "sw": VERSION,
            "url": "https://github.com/hoxxep/UNAS-Pro-fan-control",
        },
        "cmps": cmps,
        "state_topic": t["state"],
        "availability_topic": t["availability"],
    }


def discovery_signature(state):
    """Set of dynamic components; discovery is republished when it changes."""
    return (tuple(sorted(state.get("drives", {}))), tuple(sorted(state.get("tachs", {}))))


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------

class Bridge:
    def __init__(self, conf):
        self.conf = conf
        self.topics = topics(conf)
        self.state = None
        self.state_mtime = 0.0
        self.signature = None
        self.client = None

    def run(self):
        backoff = 5
        while True:
            try:
                self.connect_and_loop()
                backoff = 5
            except (OSError, ConnectionError) as e:
                log("MQTT connection lost: %s -- reconnecting in %ds" % (e, backoff))
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def connect_and_loop(self):
        self.client = MqttClient(self.conf)
        self.client.on_message = self.handle_message
        self.client.connect(self.topics["availability"], "offline")
        log("Connected to %s:%d as %s" % (self.conf["host"], self.conf["port"], self.conf["client_id"]))
        self.client.subscribe(["homeassistant/status", self.topics["command"] + "+"])
        self.client.publish(self.topics["availability"], "online", retain=True)
        self.signature = None  # force discovery republish on (re)connect
        while True:
            self.check_state_file()
            self.client.loop(2.0)

    def check_state_file(self):
        try:
            mtime = os.stat(STATE_FILE).st_mtime
        except OSError:
            return  # fan_control.sh hasn't written a snapshot yet
        if mtime == self.state_mtime:
            return
        try:
            with open(STATE_FILE) as f:
                state = json.load(f)
        except (OSError, ValueError) as e:
            log("Bad state file: %s" % e)
            return
        self.state_mtime = mtime
        self.state = state
        self.publish_discovery_if_changed()
        self.publish_state()

    def publish_discovery_if_changed(self):
        sig = discovery_signature(self.state)
        if sig == self.signature:
            return
        payload = json.dumps(discovery_payload(self.conf, self.state))
        self.client.publish(self.topics["discovery"], payload, retain=True)
        self.signature = sig
        log("Published discovery (%d drives, %d tachs)"
            % (len(self.state.get("drives", {})), len(self.state.get("tachs", {}))))

    def publish_state(self):
        if self.state is not None:
            self.client.publish(self.topics["state"], json.dumps(self.state))

    def handle_message(self, topic, payload):
        payload = payload.decode("utf-8", "replace").strip()
        if topic == "homeassistant/status":
            if payload == "online" and self.state is not None:
                log("Home Assistant birth message -- republishing")
                self.signature = None
                self.publish_discovery_if_changed()
                self.client.publish(self.topics["availability"], "online", retain=True)
                self.publish_state()
        elif topic.startswith(self.topics["command"]):
            self.handle_command(topic[len(self.topics["command"]):], payload)

    def handle_command(self, pkey, payload):
        spec = PARAMS.get(pkey)
        if not spec:
            log("Unknown parameter command: %s" % pkey)
            return
        try:
            value = clamp(round(float(payload)), spec["min"], spec["max"])
        except ValueError:
            log("Ignoring non-numeric %s command: %r" % (pkey, payload))
            return
        params = dict((self.state or {}).get("params", {}))
        params[pkey if pkey != "min_fan_pct" else "min_fan"] = \
            value if pkey != "min_fan_pct" else pct_to_raw(value)
        conf_raw = {}
        for key, sp in PARAMS.items():
            skey = "min_fan" if key == "min_fan_pct" else key
            if skey in params:
                conf_raw[sp["conf_key"]] = params[skey]
        write_fan_conf(conf_raw)
        log("Set %s=%s (conf updated; fan_control picks it up within 60s)" % (pkey, value))
        # Optimistically patch the cached state so the HA slider doesn't snap
        # back while waiting for the next fan_control loop iteration.
        if self.state is not None:
            self.state.setdefault("params", {}).update(params)
            self.publish_state()


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def clear(conf):
    """Remove the device from Home Assistant and exit."""
    client = MqttClient(conf)
    client.connect(None, None)
    t = topics(conf)
    client.publish(t["discovery"], b"", retain=True)  # empty retained = remove
    client.publish(t["availability"], b"", retain=True)
    client.disconnect()
    log("Cleared retained discovery config for device '%s'" % conf["device_id"])


def selftest():
    # varint
    assert encode_varint(0) == b"\x00"
    assert encode_varint(127) == b"\x7f"
    assert encode_varint(128) == b"\x80\x01"
    assert encode_varint(16383) == b"\xff\x7f"
    assert encode_varint(2097152) == b"\x80\x80\x80\x01"
    # CONNECT golden packet (no auth, no will)
    pkt = connect_packet("cid", None, None, None, None)
    assert pkt == bytes([0x10, 15]) + b"\x00\x04MQTT\x04\x02\x00" + bytes([KEEPALIVE]) + b"\x00\x03cid"
    # CONNECT with auth + will sets the right flags
    pkt = connect_packet("c", "u", "p", "t", "offline")
    assert pkt[9] == 0x02 | 0x04 | 0x20 | 0x80 | 0x40
    # PUBLISH roundtrip
    pkt = publish_packet("a/b", b"hi", retain=True)
    assert pkt[0] == 0x31
    topic, payload = parse_publish(0x01, pkt[2:])
    assert (topic, payload) == ("a/b", b"hi")
    # SUBSCRIBE
    pkt = subscribe_packet(1, ["x/+"])
    assert pkt[0] == 0x82 and pkt.endswith(b"\x00\x03x/+\x00")
    # conf parsing
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as f:
        f.write('# comment\nMQTT_HOST="broker.local"\nMQTT_PORT=1884\nMQTT_USER=u\n')
        path = f.name
    os.environ["MQTT_BRIDGE_CONF"] = path
    raw = parse_conf_file(path)
    assert raw == {"MQTT_HOST": "broker.local", "MQTT_PORT": "1884", "MQTT_USER": "u"}
    os.unlink(path)
    # clamping and MIN_FAN conversion
    assert clamp(200, 25, 45) == 45 and clamp(-5, 25, 45) == 25
    assert pct_to_raw(15) == 38 and raw_to_pct(39) == 15
    assert sanitize_id("WD-WX/1 2") == "WD-WX_1_2"
    # discovery payload sanity
    conf = {"device_id": "unas", "discovery_prefix": "homeassistant"}
    state = {"sys_temp": 50, "hdd_temp": 34, "ssd_temp": 52, "fan_duty_pct": 25,
             "drives": {"WD1": {"class": "HDD", "dev": "/dev/sda", "temp": 34}},
             "tachs": {"adt7475_fan1": 3170},
             "params": {"sys_tgt": 50, "hdd_tgt": 32, "ssd_tgt": 50, "min_fan": 39}}
    d = discovery_payload(conf, state)
    assert d["dev"]["ids"] == ["unas"] and "o" in d
    assert d["cmps"]["drive_WD1"]["value_template"] == "{{ value_json.drives['WD1'].temp }}"
    assert d["cmps"]["hdd_tgt"]["p"] == "number" and d["cmps"]["hdd_tgt"]["max"] == 45
    assert d["cmps"]["tach_adt7475_fan1"]["unit_of_measurement"] == "rpm"
    assert all("unique_id" in c for c in d["cmps"].values())
    json.dumps(d)  # must serialize
    assert discovery_signature(state) == (("WD1",), ("adt7475_fan1",))
    print("selftest OK")


def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    if not os.path.exists(BRIDGE_CONF):
        sys.exit("Missing %s -- copy mqtt_bridge.conf.example and edit it." % BRIDGE_CONF)
    conf = load_bridge_conf(BRIDGE_CONF)
    if "--clear" in sys.argv:
        clear(conf)
        return

    bridge = Bridge(conf)

    def on_term(signum, frame):
        try:
            bridge.client.publish(bridge.topics["availability"], "offline", retain=True)
            bridge.client.disconnect()
        except (OSError, ConnectionError, AttributeError):
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    bridge.run()


if __name__ == "__main__":
    main()
