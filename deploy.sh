#!/bin/bash

# Run remotely to deploy the fan control service onto a UNAS Pro.
#
# Usage: ./deploy.sh HOSTNAME
#
# Repo: https://github.com/hoxxep/UNAS-Pro-fan-control
# Author: Liam Gray
# License: MIT

set -euo pipefail

HOST="$1"

scp fan_control.sh "${HOST}:/root/fan_control.sh"
scp fan_control.service "${HOST}:/etc/systemd/system/fan_control.service"

# Optional MQTT/Home Assistant bridge. Inert until /root/mqtt_bridge.conf is
# created on the device (see mqtt_bridge.conf.example and README.md).
scp fan_control_state.sh "${HOST}:/root/fan_control_state.sh"
scp mqtt_bridge.py "${HOST}:/root/mqtt_bridge.py"
scp mqtt_bridge.conf.example "${HOST}:/root/mqtt_bridge.conf.example"
scp mqtt_bridge.service "${HOST}:/etc/systemd/system/mqtt_bridge.service"

ssh "$HOST" -t '\
    chmod +x /root/fan_control.sh /root/mqtt_bridge.py && \
    systemctl daemon-reload && \
    systemctl enable fan_control.service && \
    systemctl restart fan_control.service && \
    systemctl enable mqtt_bridge.service && \
    systemctl restart mqtt_bridge.service; \
    systemctl status fan_control.service mqtt_bridge.service'
