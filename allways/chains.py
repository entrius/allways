import math
from dataclasses import dataclass

from allways.constants import (
    EXTENSION_BUCKET_BLOCKS,
    EXTENSION_PADDING_SECONDS,
    EXTENSION_TIER1_PADDING_SECONDS,
    MAX_EXTENSION_BLOCKS,
)

SUBTENSOR_BLOCK_SECONDS = 12


@dataclass(frozen=True)
class ChainDefinition:
    """Definition of a supported chain."""

    id: str  # Short identifier (e.g. 'btc')
    name: str  # Display name (e.g. 'Bitcoin')
    native_unit: str  # Smallest unit name (e.g. 'satoshi')
    decimals: int  # Precision (e.g. 8 for BTC, 9 for TAO)
    env_prefix: str  # .env variable prefix (e.g. 'BTC' -> BTC_RPC_URL)
    seconds_per_block: int = 12  # Average block time on this chain
    min_confirmations: int = 1  # Minimum confirmations before accepting a transaction


# ─── Supported Chains ────────────────────────────────────
CHAIN_BTC = ChainDefinition(
    id='btc',
    name='Bitcoin',
    native_unit='satoshi',
    decimals=8,
    env_prefix='BTC',
    seconds_per_block=600,
    min_confirmations=3,
)
CHAIN_TAO = ChainDefinition(
    id='tao',
    name='Bittensor',
    native_unit='rao',
    decimals=9,
    env_prefix='TAO',
    seconds_per_block=12,
    min_confirmations=6,
)

SUPPORTED_CHAINS = {
    'btc': CHAIN_BTC,
    'tao': CHAIN_TAO,
}


def get_chain(chain_id: str) -> ChainDefinition:
    """Lookup chain by ID. Raises KeyError if unsupported."""
    return SUPPORTED_CHAINS[chain_id]


def canonical_pair(chain_a: str, chain_b: str) -> tuple:
    """Return (source, dest) in canonical order for consistent commitment storage.

    Determines the rate unit: rate is always 'dest per 1 source' in this ordering.

    Ordering rules:
    1. If TAO is in the pair, TAO is always dest — rates are denominated in TAO.
    2. Otherwise, alphabetical — deterministic fallback for non-TAO pairs (e.g. BTC-ETH).
    """
    if chain_b == 'tao':
        return (chain_a, chain_b)
    if chain_a == 'tao':
        return (chain_b, chain_a)
    return (chain_a, chain_b) if chain_a < chain_b else (chain_b, chain_a)


def confirmations_to_subtensor_blocks(chain_id: str) -> int:
    """How many subtensor blocks a chain's min_confirmations take."""
    chain = get_chain(chain_id)
    return math.ceil(chain.min_confirmations * chain.seconds_per_block / SUBTENSOR_BLOCK_SECONDS)


def compute_extension_target(
    from_chain_id: str,
    current_confirmations: int,
    current_subnet_block: int,
) -> int:
    """Subtensor block to extend a reservation/timeout to.

    Covers the remaining source-chain confirmations plus a padding buffer,
    rounded up to EXTENSION_BUCKET_BLOCKS so validators with slightly different
    confirmation counts converge on the same target. Capped at
    MAX_EXTENSION_BLOCKS — the contract enforces the same cap, this is just a
    pre-check to avoid wasting a tx on a doomed proposal.
    """
    chain = get_chain(from_chain_id)
    remaining = max(0, chain.min_confirmations - current_confirmations)
    seconds_needed = remaining * chain.seconds_per_block + EXTENSION_PADDING_SECONDS
    blocks_needed = math.ceil(seconds_needed / SUBTENSOR_BLOCK_SECONDS)
    blocks_needed = math.ceil(blocks_needed / EXTENSION_BUCKET_BLOCKS) * EXTENSION_BUCKET_BLOCKS
    blocks_needed = min(blocks_needed, MAX_EXTENSION_BLOCKS)
    return current_subnet_block + blocks_needed


def compute_extension_target_tier1(
    from_chain_id: str,
    current_subnet_block: int,
) -> int:
    """Tier-1 (first extension) target: enough wall-clock for one chain block
    to land plus padding. Triggered on tx visibility alone — we haven't yet
    seen any confirmation, so we just budget for the *next* block. Tier-2
    handles the remaining confirmations once we have hard evidence.

    Same bucketing/cap pattern as the main helper so two validators on the
    same chain converge to the same target.
    """
    chain = get_chain(from_chain_id)
    seconds_needed = chain.seconds_per_block + EXTENSION_TIER1_PADDING_SECONDS
    blocks_needed = math.ceil(seconds_needed / SUBTENSOR_BLOCK_SECONDS)
    blocks_needed = math.ceil(blocks_needed / EXTENSION_BUCKET_BLOCKS) * EXTENSION_BUCKET_BLOCKS
    blocks_needed = min(blocks_needed, MAX_EXTENSION_BLOCKS)
    return current_subnet_block + blocks_needed
