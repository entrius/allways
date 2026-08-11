"""Construct the bond relay from a validator neuron — the one place that knows both sides.

A vault address is the on/off switch. Without one there is no relayer at all, the swap loop's
relay hooks are inert, and a SOL-only deployment pays nothing for split collateral existing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import bittensor as bt

from allways.validator.relay.engine import BondRelay, RelayConfig
from allways.vault import BondVaultClient, VaultConfigError, codec
from allways.vault.client import resolve_signer, resolve_vault_address

_CONFIG_FILE = Path.home() / '.allways' / 'config.json'


def _file_config() -> dict:
    """The `alw config` file, so an operator's `alw config set vault-address` is honoured by the
    neuron too. Env still wins (resolve_vault_address checks it first)."""
    try:
        return json.loads(_CONFIG_FILE.read_text())
    except Exception:
        return {}


def build_bond_relay(validator: Any, read_only: bool = False) -> Optional[BondRelay]:
    """The relayer, or None when no vault is configured. Never raises: a misconfigured vault must
    degrade to "no TAO relaying" (the fuse then holds TAO entry closed), not to a dead validator."""
    config = _file_config()
    subtensor = None
    try:
        # Address first: an unconfigured vault must not cost a SOL-only validator a websocket.
        resolve_vault_address(config)
        subtensor = bt.Subtensor(config=validator.config)
        vault = BondVaultClient.from_config(subtensor, config, keypair=resolve_signer(validator.wallet))
    except VaultConfigError as e:
        bt.logging.info(f'bond relay off — {e}')
        return None
    except codec.VaultCodecError as e:
        bt.logging.warning(f'bond relay off — {e}')
        _close(subtensor)
        return None
    except Exception as e:
        bt.logging.warning(f'bond relay off — could not build the vault client: {e}')
        _close(subtensor)
        return None

    validator.vault_subtensor = subtensor
    relay = BondRelay(
        validator.solana_client,
        vault,
        validator.state_store,
        read_only=read_only,
        config=RelayConfig.from_env(),
        config_fn=validator.solana_config_cache.config,
    )
    bt.logging.info(
        f'bond relay on — vault {vault.address}, signer {vault.keypair.ss58_address}, '
        f'heartbeat every {relay.cfg.heartbeat_interval_secs}s, fee cadence {relay.cfg.fee_cadence_secs}s'
    )
    return relay


def axon_vault_client(validator: Any) -> Optional[BondVaultClient]:
    """A read-only vault client on the AXON's subtensor, or None when this validator has no relay.
    Handler threads must never touch the relay's own websocket (mid-extrinsic on the forward loop),
    so this rebinds the resolved address + metadata per call — and so follows a socket reconnect."""
    relay = getattr(validator, 'bond_relay', None)
    axon_subtensor = getattr(validator, 'axon_subtensor', None)
    if relay is None or axon_subtensor is None:
        return None
    return BondVaultClient(axon_subtensor, relay.vault.address, metadata=relay.vault.metadata)


def _close(subtensor) -> None:
    if subtensor is not None:
        try:
            subtensor.close()
        except Exception:
            pass
