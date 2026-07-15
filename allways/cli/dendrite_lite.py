"""Dendrite-lite: ephemeral keypair + validator discovery for TAO-less users.

Users don't have TAO wallets. This module provides:
- Ephemeral sr25519 keypair generation/storage for transport-layer auth
- Validator discovery from metagraph (all, or one by hotkey with a disk cache)
- Dendrite broadcast helper
"""

import json
import time
from pathlib import Path
from typing import Callable, List, Optional

import bittensor as bt

EPHEMERAL_WALLET_DIR = Path.home() / '.allways' / 'ephemeral_wallet'
EPHEMERAL_WALLET_NAME = 'allways_ephemeral'
EPHEMERAL_HOTKEY_NAME = 'default'

AXON_CACHE_FILE = Path.home() / '.allways' / 'axon_cache.json'
AXON_CACHE_TTL_SECS = 3600


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
        wallet.create_if_non_existent(coldkey_use_password=False, hotkey_use_password=False, suppress=True)
        bt.logging.info('Created ephemeral wallet for dendrite-lite')

    return wallet


def discover_validators(
    subtensor: bt.Subtensor,
    netuid: int,
) -> List[bt.AxonInfo]:
    """Discover validator axon endpoints from metagraph.

    Filters for UIDs with validator_permit=True and is_serving=True. The
    contract-side whitelist is enforced on-chain at vote time (Config.validators),
    so the announce hop just reaches every serving validator."""
    metagraph = subtensor.metagraph(netuid=netuid)
    axons = []

    for uid in range(metagraph.n):
        if not metagraph.validator_permit[uid]:
            continue
        axon = metagraph.axons[uid]
        if not axon.is_serving:
            continue
        axons.append(axon)

    return axons


def find_validator_axon(
    subtensor_factory: Callable[[], bt.Subtensor],
    netuid: int,
    hotkey: str,
    fresh: bool = False,
) -> Optional[bt.AxonInfo]:
    """The axon of one specific validator hotkey, with a disk cache so the routed
    happy path skips the multi-second metagraph sync entirely.

    ``subtensor_factory`` defers the chain connection to the cache-miss path.
    ``fresh=True`` bypasses the cache (used after a dendrite failure to a cached
    axon — validators move IPs). Returns None if the hotkey isn't a serving
    validator on the subnet."""
    if not fresh:
        cached = _read_axon_cache(netuid, hotkey)
        if cached is not None:
            return cached
    metagraph = subtensor_factory().metagraph(netuid=netuid)
    for uid in range(metagraph.n):
        if metagraph.hotkeys[uid] != hotkey:
            continue
        axon = metagraph.axons[uid]
        if metagraph.validator_permit[uid] and axon.is_serving:
            _write_axon_cache(netuid, hotkey, axon)
            return axon
        return None
    return None


def invalidate_axon_cache() -> None:
    AXON_CACHE_FILE.unlink(missing_ok=True)


def _read_axon_cache(netuid: int, hotkey: str) -> Optional[bt.AxonInfo]:
    try:
        entry = json.loads(AXON_CACHE_FILE.read_text())
        fresh_enough = time.time() - float(entry['cached_at']) < AXON_CACHE_TTL_SECS
        if entry['netuid'] == netuid and entry['hotkey'] == hotkey and fresh_enough:
            return bt.AxonInfo(
                version=entry.get('version', 0),
                ip=entry['ip'],
                port=entry['port'],
                ip_type=entry.get('ip_type', 4),
                hotkey=hotkey,
                coldkey=entry.get('coldkey', ''),
            )
    except (OSError, ValueError, KeyError, TypeError):
        pass  # unreadable/stale cache is a miss, never an error
    return None


def _write_axon_cache(netuid: int, hotkey: str, axon: bt.AxonInfo) -> None:
    try:
        AXON_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        AXON_CACHE_FILE.write_text(
            json.dumps(
                {
                    'netuid': netuid,
                    'hotkey': hotkey,
                    'ip': axon.ip,
                    'port': axon.port,
                    'ip_type': getattr(axon, 'ip_type', 4),
                    'version': getattr(axon, 'version', 0),
                    'coldkey': getattr(axon, 'coldkey', ''),
                    'cached_at': time.time(),
                }
            )
        )
    except OSError:
        pass  # best-effort cache


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
    """Honor ALW_DENDRITE_TIMEOUT as an override for slow chains (e.g. testnet)."""
    import os

    override = os.environ.get('ALW_DENDRITE_TIMEOUT')
    if not override:
        return default
    try:
        return float(override)
    except ValueError:
        return default
