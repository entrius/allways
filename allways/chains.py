import math
from dataclasses import dataclass

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
