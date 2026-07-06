#!/bin/bash

# Optional state-file helper for fan_control.sh.
#
# When installed at /root/fan_control_state.sh, fan_control.sh sources it and
# calls state_begin / state_add_drive / state_end on every loop iteration. The
# result is an atomic JSON snapshot of temperatures, fan speeds, tachometers,
# and the active curve parameters at /run/fan_control/state.json (tmpfs).
# mqtt_bridge.py reads that snapshot and publishes it to MQTT/Home Assistant.
#
# Failures here must never break fan control: state_end swallows all errors.
# Without this file installed, fan_control.sh falls back to no-op stubs.
#
# Repo: https://github.com/hoxxep/UNAS-Pro-fan-control
# License: MIT

STATE_DIR=/run/fan_control
STATE_FILE="$STATE_DIR/state.json"

state_begin() {
    STATE_DRIVES=()
}

# state_add_drive CLASS DEV TEMP SERIAL
state_add_drive() {
    STATE_DRIVES+=("$1"$'\t'"$2"$'\t'"$3"$'\t'"${4:-}")
}

# Write the snapshot. Reads fan_control.sh globals: SYS_TEMP HDD_TEMP SSD_TEMP
# FAN_SPEED SYS_TGT SYS_MAX HDD_TGT HDD_MAX SSD_TGT SSD_MAX MIN_FAN.
state_end() {
    (
        set +e
        mkdir -p "$STATE_DIR" 2>/dev/null || exit 0

        # Collect tachometers and PWM readings from the same fan-controller
        # chips fan_control.sh drives (skip drive and PSU/PMBus chips).
        local rows=() hw name f p bn v
        ((${#STATE_DRIVES[@]})) && rows+=("${STATE_DRIVES[@]/#/D$'\t'}")
        for hw in /sys/class/hwmon/hwmon*; do
            [[ -e "$hw" ]] || continue
            name="$(cat "$hw/name" 2>/dev/null || echo hwmon)"
            case "$name" in
                nvme|drivetemp) continue ;;
                *pmbus*|*dps[0-9]*|*psu*) continue ;;
            esac
            for f in "$hw"/fan*_input; do
                [[ -e "$f" ]] || continue
                v="$(cat "$f" 2>/dev/null)"
                [[ "$v" =~ ^[0-9]+$ ]] || continue
                rows+=("T"$'\t'"${name}_$(basename "${f%_input}")"$'\t'"$v")
            done
            for p in "$hw"/pwm*; do
                [[ -e "$p" ]] || continue
                bn="$(basename "$p")"
                [[ "$bn" =~ ^pwm[0-9]+$ ]] || continue
                v="$(cat "$p" 2>/dev/null)"
                [[ "$v" =~ ^[0-9]+$ ]] || continue
                rows+=("P"$'\t'"${name}_${bn}"$'\t'"$v")
            done
        done

        printf '%s\n' "${rows[@]}" | jq -R -s \
            --argjson sys_temp "${SYS_TEMP:-0}" \
            --argjson hdd_temp "${HDD_TEMP:-0}" \
            --argjson ssd_temp "${SSD_TEMP:-0}" \
            --argjson fan_speed "${FAN_SPEED:-0}" \
            --argjson sys_tgt "${SYS_TGT:-0}" --argjson sys_max "${SYS_MAX:-0}" \
            --argjson hdd_tgt "${HDD_TGT:-0}" --argjson hdd_max "${HDD_MAX:-0}" \
            --argjson ssd_tgt "${SSD_TGT:-0}" --argjson ssd_max "${SSD_MAX:-0}" \
            --argjson min_fan "${MIN_FAN:-0}" '
            (split("\n") | map(select(length > 0) | split("\t"))) as $rows
            | {
                ts: (now | floor),
                sys_temp: $sys_temp,
                hdd_temp: $hdd_temp,
                ssd_temp: $ssd_temp,
                fan_speed_raw: $fan_speed,
                fan_duty_pct: (($fan_speed / 255 * 100) | round),
                # Drives keyed by serial number (stable across /dev renames);
                # falls back to the device basename when SMART has no serial.
                drives: ($rows | map(select(.[0] == "D") | {
                    key: (if (.[4] // "") != "" then .[4] else (.[2] | sub(".*/"; "")) end),
                    value: {class: .[1], dev: .[2], temp: (.[3] | tonumber)}
                }) | from_entries),
                tachs: ($rows | map(select(.[0] == "T")
                    | {key: .[1], value: (.[2] | tonumber)}) | from_entries),
                pwms: ($rows | map(select(.[0] == "P")
                    | {key: .[1], value: (.[2] | tonumber)}) | from_entries),
                params: {
                    sys_tgt: $sys_tgt, sys_max: $sys_max,
                    hdd_tgt: $hdd_tgt, hdd_max: $hdd_max,
                    ssd_tgt: $ssd_tgt, ssd_max: $ssd_max,
                    min_fan: $min_fan
                }
            }' > "$STATE_FILE.tmp" 2>/dev/null \
            && mv "$STATE_FILE.tmp" "$STATE_FILE"
        exit 0
    ) || true
}
