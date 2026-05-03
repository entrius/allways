"""Dendrite-lite: ephemeral keypair + validator discovery for TAO-less users.

Users don't have TAO wallets. This module provides:
- Ephemeral sr25519 keypair generation/storage for transport-layer auth
- Validator discovery from metagraph
- Dendrite broadcast helper
"""

from pathlib import Path
from typing import List, Optional

import bittensor as bt

from allways.contract_client import AllwaysContractClient

EPHEMERAL_WALLET_DIR = Path.home() / '.allways' / 'ephemeral_wallet'
EPHEMERAL_WALLET_NAME = 'allways_ephemeral'
EPHEMERAL_HOTKEY_NAME = 'default'


def get_ephemeral_wallet() -> bt.Wallet:
    """Get or create an ephemeral wallet for dendrite-lite transport auth.

    The ephemeral keypair is stored in ~/.allways/ephemeral_wallet/.
    It's NOT used for authentication — the real auth is the source chain
    address proof inside the synapse payload.
    """
    wallet_path = str(EPHEMERAL_WALLET_DIR.parent)
    wallet = bt.Wallet(name=EPHEMERAL_WALLET_NAME, hotkey=EPHEMERAL_HOTKEY_NAME, path=wallet_path)

    hotkey_file = Path(wallet_path) / EPHEMERAL_WALLET_NAME / 'hotkeys' / EPHEMERAL_HOTKEY_NAME
    if not hotkey_file.exists():
        hotkey_file.parent.mkdir(parents=True, exist_ok=True)
        wallet.create_if_non_existent(coldkey_use_password=False, hotkey_use_password=False)
        bt.logging.info('Created ephemeral wallet for dendrite-lite')

    return wallet


def discover_validators(
    subtensor: bt.Subtensor,
    netuid: int,
    contract_client: Optional[AllwaysContractClient] = None,
) -> List[bt.AxonInfo]:
    """Discover validator axon endpoints from metagraph.

    Filters for UIDs with validator_permit=True and is_serving=True.
    When contract_client is provided, also filters to only whitelisted validators.
    Returns list of axon endpoints.
    """
    metagraph = subtensor.metagraph(netuid=netuid)
    axons = []

    for uid in range(metagraph.n):
        if not metagraph.validator_permit[uid]:
            continue
        axon = metagraph.axons[uid]
        if not axon.is_serving:
            continue
        if contract_client:
            try:
                if not contract_client.is_validator(metagraph.hotkeys[uid]):
                    continue
            except Exception:
                pass
        axons.append(axon)

    return axons


def broadcast_synapse(
    wallet: bt.Wallet,
    axons: List[bt.AxonInfo],
    synapse: bt.Synapse,
    timeout: float = 30.0,
) -> list:
    """Broadcast a synapse to all validator axons via dendrite.

    Returns list of response synapses.
    """
    import asyncio

    dendrite = bt.Dendrite(wallet=wallet)
    timeout = resolve_dendrite_timeout(timeout)

    loop = asyncio.new_event_loop()
    try:
        responses = loop.run_until_complete(dendrite(axons=axons, synapse=synapse, deserialize=False, timeout=timeout))
    finally:
        loop.close()

    return responses


def resolve_dendrite_timeout(default: float) -> float:
    """Honor ALW_DENDRITE_TIMEOUT as an override for slow chains (e.g. testnet).

    Rejects ``nan``, ``inf``, and non-positive values so a typo in the env
    var doesn't silently break dendrite calls (issue #240). ``float()`` alone
    accepts all three, and the dendrite call would either hang on ``nan``,
    bypass the override on ``inf``, or fail every send instantly on ``0``/
    negative — none of which are useful, so we fall back to ``default``.
    """
    import math
    import os

    override = os.environ.get('ALW_DENDRITE_TIMEOUT')
    if not override:
        return default
    try:
        value = float(override)
    except ValueError:
        bt.logging.warning(f'ALW_DENDRITE_TIMEOUT={override!r} is not a number, using default {default}')
        return default
    if not math.isfinite(value) or value <= 0:
        bt.logging.warning(
            f'ALW_DENDRITE_TIMEOUT={override!r} must be a finite positive float, using default {default}'
        )
        return default
    return value
