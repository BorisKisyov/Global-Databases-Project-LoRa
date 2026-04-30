#!/bin/sh
set -eu

if [ -z "${CHIRPSTACK_API_KEY:-}" ] || [ -z "${CHIRPSTACK_TENANT_ID:-}" ]; then
  echo "CHIRPSTACK_API_KEY and CHIRPSTACK_TENANT_ID must be set before starting chirpstack-simulator."
  echo "Run tools/bootstrap_chirpstack.py first, then start the simulator profile."
  exit 1
fi

cat /etc/chirpstack-simulator.toml.template \
  | sed "s#__API_KEY__#${CHIRPSTACK_API_KEY}#g" \
  | sed "s#__TENANT_ID__#${CHIRPSTACK_TENANT_ID}#g" \
  | sed "s#__DEVICE_COUNT__#${SIM_DEVICE_COUNT:-5}#g" \
  | sed "s#__DURATION__#${SIM_DURATION:-0s}#g" \
  | sed "s#__UPLINK_INTERVAL__#${SIM_UPLINK_INTERVAL:-30s}#g" \
  | sed "s#__PAYLOAD_HEX__#${SIM_PAYLOAD_HEX:-03E807D2320CE4}#g" \
  > /tmp/chirpstack-simulator.toml

exec chirpstack-simulator -c /tmp/chirpstack-simulator.toml
