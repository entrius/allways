"""Shared commitment parsing logic — used by validator, miner, and CLI."""

from typing import List, Optional

import bittensor as bt

from allways.chains import SUPPORTED_CHAINS, canonical_pair
from allways.classes import MinerPair
from allways.constants import COMMITMENT_VERSION


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
    """Read a single miner's commitment, optionally at a specific block."""
    if metagraph is None:
        metagraph = subtensor.metagraph(netuid)
    hotkey_to_uid = {metagraph.hotkeys[uid]: uid for uid in range(metagraph.n.item())}
    uid = hotkey_to_uid.get(hotkey)
    if uid is None:
        return None
    commitment = get_commitment(subtensor, netuid, hotkey, block=block)
    if commitment:
        return parse_commitment_data(commitment, uid=uid, hotkey=hotkey)
    return None


def read_miner_commitments(subtensor: bt.Subtensor, netuid: int) -> List[MinerPair]:
    """Read all miner commitments from chain, parse into MinerPair list."""
    pairs = []
    try:
        metagraph = subtensor.metagraph(netuid)
        for uid in range(metagraph.n.item()):
            hotkey = metagraph.hotkeys[uid]
            commitment = get_commitment(subtensor, netuid, hotkey)
            if commitment:
                pair = parse_commitment_data(commitment, uid=uid, hotkey=hotkey)
                if pair:
                    pairs.append(pair)
    except (ConnectionError, TimeoutError) as e:
        bt.logging.warning(f'Transient error reading commitments: {e}')
    except Exception as e:
        bt.logging.error(f'Error reading commitments: {e}')
    return pairs
