#!/bin/bash

if [ -z "$NETUID" ]; then echo "NETUID is not set" && exit 1; fi
if [ -z "$WALLET_NAME" ]; then echo "WALLET_NAME is not set" && exit 1; fi
if [ -z "$HOTKEY_NAME" ]; then echo "HOTKEY_NAME is not set" && exit 1; fi
if [ -z "$SUBTENSOR_NETWORK" ]; then echo "SUBTENSOR_NETWORK is not set" && exit 1; fi

# PORT is no longer used (no axon is served); it is still accepted so an existing .env keeps working.
exec python neurons/validator.py \
  --netuid ${NETUID} \
  --wallet.name ${WALLET_NAME} \
  --wallet.hotkey ${HOTKEY_NAME} \
  --subtensor.network ${SUBTENSOR_NETWORK} \
  ${PORT:+--axon.port ${PORT}} \
  --logging.${LOG_LEVEL:-info} \
  --logging.record_log --logging.logging_dir /root/.allways \
  "$@"
