import math
from dataclasses import dataclass

from allways.constants import (
    EXTENSION_BUCKET_BLOCKS,
    EXTENSION_PADDING_SECONDS,
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
    min_confirmations=2,
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
    remaining_blocks: int,
    current_subnet_block: int,
) -> int:
    """Subtensor block to extend a reservation/timeout to.

    Covers ``remaining_blocks`` source-chain blocks plus a padding buffer,
    bucket-rounded so validators converge, capped at MAX_EXTENSION_BLOCKS.
    """
    chain = get_chain(from_chain_id)
    seconds_needed = remaining_blocks * chain.seconds_per_block + EXTENSION_PADDING_SECONDS
    blocks_needed = math.ceil(seconds_needed / SUBTENSOR_BLOCK_SECONDS)
    blocks_needed = math.ceil(blocks_needed / EXTENSION_BUCKET_BLOCKS) * EXTENSION_BUCKET_BLOCKS
    blocks_needed = min(blocks_needed, MAX_EXTENSION_BLOCKS)
    # Anchor on current, not deadline: contract caps ``target - current_at_exec``
    # at MAX_EXTENSION_BLOCKS (lib.rs:670, :1090), so a deadline anchor blows
    # the cap whenever propose fires before the deadline.
    return current_subnet_block + blocks_needed


# Status returned by ``classify_send_runway`` — see that function for semantics.
RUNWAY_OK = 'ok'
RUNWAY_EXTENSION_REQUIRED = 'extension_required'
RUNWAY_TOO_SHORT = 'too_short'

# Subtensor blocks set aside for the user's source tx to propagate to
# validators' RPC view before the extension propose flow can pick it up.
# ~1 minute covers typical Blockstream/Esplora indexing lag.
SEND_PROPAGATION_BUFFER_BLOCKS = 5


def classify_send_runway(
    from_chain_id: str,
    current_subnet_block: int,
    reserved_until_block: int,
    extend_threshold_blocks: int,
) -> tuple[str, int]:
    """Classify whether broadcasting the source tx now is safe.

    The validator extension flow (optimistic_extensions.py) needs at least
    ``extend_threshold_blocks`` of runway before the deadline to land a
    propose tx and let its challenge window elapse — without that, the
    propose is mechanically doomed and the reservation will expire. The
    user's tx also needs a ~1 min propagation buffer so the validator can
    actually see it before proposing.

    Returns ``(status, remaining_blocks)`` where status is one of:
      * ``RUNWAY_OK`` — full confirmation window fits in remaining TTL.
        No validator extension needed; broadcast freely.
      * ``RUNWAY_EXTENSION_REQUIRED`` — confirmations won't fit in TTL,
        but there is enough runway for validators to auto-extend once
        the tx is visible. Caller should warn but may proceed.
      * ``RUNWAY_TOO_SHORT`` — TTL is below the extension floor; even
        the auto-extension cannot rescue this broadcast. Caller should
        refuse to send.
    """
    remaining = reserved_until_block - current_subnet_block
    confirmation_subnet_blocks = confirmations_to_subtensor_blocks(from_chain_id)

    if remaining < extend_threshold_blocks + SEND_PROPAGATION_BUFFER_BLOCKS:
        return (RUNWAY_TOO_SHORT, remaining)
    if remaining < confirmation_subnet_blocks + SEND_PROPAGATION_BUFFER_BLOCKS:
        return (RUNWAY_EXTENSION_REQUIRED, remaining)
    return (RUNWAY_OK, remaining)
