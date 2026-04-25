"""Shared commitment parsing logic — used by validator, miner, and CLI."""

import math
from typing import List, Optional

import bittensor as bt
from substrateinterface.utils.ss58 import ss58_encode

from allways.chains import SUPPORTED_CHAINS, canonical_pair
from allways.classes import MinerPair
from allways.constants import COMMITMENT_VERSION

SS58_PREFIX = 42


def parse_commitment_data(raw: str, uid: int = 0, hotkey: str = '') -> Optional[MinerPair]:
    """Parse a commitment string into a MinerPair.

    Format: v{VERSION}:{src_chain}:{src_addr}:{dst_chain}:{dst_addr}:{rate}:{counter_rate}
    Both rates are 'canonical_dest per 1 canonical_source'. rate is for source→dest, counter_rate for dest→source.
    Example: v1:btc:bc1q...:tao:5C...:340:350
    """
    try:
        parts = raw.split(':')
        if len(parts) != 7:
            return None

        version_str = parts[0]
        if not version_str.startswith('v'):
            return None

        version = int(version_str[1:])
        if version != COMMITMENT_VERSION:
            return None

        src_chain = parts[1]
        src_addr = parts[2]
        dst_chain = parts[3]
        dst_addr = parts[4]
        rate_str = parts[5]
        rate = float(rate_str)
        counter_rate_str = parts[6]
        counter_rate = float(counter_rate_str)

        if not (math.isfinite(rate) and rate > 0):
            return None
        if not (math.isfinite(counter_rate) and counter_rate > 0):
            return None

        if src_chain not in SUPPORTED_CHAINS or dst_chain not in SUPPORTED_CHAINS:
            return None

        if src_chain == dst_chain:
            return None

        # Normalize to canonical direction (alphabetical ordering).
        # When swapping direction, swap rates too: the posted "forward" rate becomes "reverse".
        canon_from, _ = canonical_pair(src_chain, dst_chain)
        if src_chain != canon_from:
            src_chain, dst_chain = dst_chain, src_chain
            src_addr, dst_addr = dst_addr, src_addr
            rate, counter_rate = counter_rate, rate
            rate_str, counter_rate_str = counter_rate_str, rate_str

        return MinerPair(
            uid=uid,
            hotkey=hotkey,
            from_chain=src_chain,
            from_address=src_addr,
            to_chain=dst_chain,
            to_address=dst_addr,
            rate=rate,
            rate_str=rate_str,
            counter_rate=counter_rate,
            counter_rate_str=counter_rate_str,
        )
    except (ValueError, IndexError):
        return None


def decode_commitment_field(metadata) -> Optional[str]:
    """Decode the raw commitment bytes from a CommitmentOf query result.

    Handles multiple SCALE response formats:
    - Bittensor SDK (subtensor.substrate): fields[0] is a tuple wrapping a dict of int tuples
    - Plain SubstrateInterface: fields[0] is a SCALE object whose .value is a dict with hex string
    """
    try:
        val = metadata.value if hasattr(metadata, 'value') else metadata
        if not val:
            return None
        field = val['info']['fields'][0]
        if hasattr(field, 'value'):
            field = field.value
        elif isinstance(field, tuple):
            field = field[0]
        raw_value = next(iter(field.values()))
        if isinstance(raw_value, str) and raw_value.startswith('0x'):
            return bytes.fromhex(raw_value[2:]).decode('utf-8', errors='ignore')
        byte_tuple = raw_value[0] if raw_value else raw_value
        return bytes(byte_tuple).decode('utf-8', errors='ignore')
    except (TypeError, KeyError, IndexError, StopIteration):
        return None


def get_commitment(subtensor: bt.Subtensor, netuid: int, hotkey: str, block: Optional[int] = None) -> Optional[str]:
    """Read a commitment from chain, bypassing SDK's get_commitment which logs ERROR on empty UIDs."""
    metadata = subtensor.substrate.query(
        module='Commitments',
        storage_function='CommitmentOf',
        params=[netuid, hotkey],
        block_hash=subtensor.determine_block_hash(block),
    )
    if metadata is None:
        return None
    return decode_commitment_field(metadata)


def read_miner_commitment(
    subtensor: bt.Subtensor,
    netuid: int,
    hotkey: str,
    block: Optional[int] = None,
    metagraph: Optional['bt.Metagraph'] = None,
) -> Optional[MinerPair]:
    """Read a single miner's commitment, optionally at a specific block.

    When ``metagraph`` is None the uid lookup is skipped (uid defaults to 0).
    Callers that need the uid — or want to avoid returning commitments for
    unregistered hotkeys — must pass their cached metagraph. Downloading a
    fresh metagraph here on every call was a 30s+ RPC on testnet finney.
    """
    uid = 0
    if metagraph is not None:
        hotkey_to_uid = {metagraph.hotkeys[u]: u for u in range(metagraph.n.item())}
        resolved = hotkey_to_uid.get(hotkey)
        if resolved is None:
            return None
        uid = resolved
    commitment = get_commitment(subtensor, netuid, hotkey, block=block)
    if commitment:
        return parse_commitment_data(commitment, uid=uid, hotkey=hotkey)
    return None


def read_miner_commitments(subtensor: bt.Subtensor, netuid: int) -> List[MinerPair]:
    """Read all miner commitments for the netuid in a single RPC call.

    Uses substrate-interface's ``query_map`` over the ``CommitmentOf`` double map
    keyed by ``(netuid, hotkey)``. One RPC round-trip returns every committed
    hotkey on the subnet — cheaper than the old N-RPC for-loop, matters most
    on full validator polling cadence.
    """
    pairs: List[MinerPair] = []
    try:
        metagraph = subtensor.metagraph(netuid)
        hotkey_to_uid = {metagraph.hotkeys[uid]: uid for uid in range(metagraph.n.item())}
        result = subtensor.substrate.query_map(
            module='Commitments',
            storage_function='CommitmentOf',
            params=[netuid],
        )
        for key, metadata in result:
            # query_map returns the second-map key (hotkey AccountId) as raw
            # bytes inside a single-element tuple, not an SS58 string. Encode
            # it so we can look the miner up in the metagraph's hotkey index.
            raw = key.value if hasattr(key, 'value') else key
            if isinstance(raw, tuple) and len(raw) == 1:
                raw = raw[0]
            if isinstance(raw, (tuple, list)):
                raw = bytes(raw)
            if isinstance(raw, (bytes, bytearray)) and len(raw) == 32:
                hotkey = ss58_encode(bytes(raw), SS58_PREFIX)
            else:
                hotkey = str(raw)
            uid = hotkey_to_uid.get(hotkey)
            if uid is None:
                continue  # miner dereg'd but commitment still in storage
            commitment = decode_commitment_field(metadata)
            if not commitment:
                continue
            pair = parse_commitment_data(commitment, uid=uid, hotkey=hotkey)
            if pair:
                pairs.append(pair)
    except (ConnectionError, TimeoutError) as e:
        bt.logging.warning(f'Transient error reading commitments: {e}')
    except Exception as e:
        bt.logging.error(f'Error reading commitments: {e}')
    return pairs
