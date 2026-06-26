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
    # Smallest amount that can actually exist/move on-chain, in native units
    # (BTC dust floor, TAO existential deposit). 1 = no floor.
    min_onchain_amount: int = 1
    # Replay-freshness grace (seconds): a tx is fresh iff block_time > floor - grace.
    # Default 0 (strict >). Absorbs honest miner clock skew; MUST stay well under
    # reservation_ttl_secs — the replay window is exactly this wide (B2).
    replay_grace_secs: int = 0


# ─── Supported Chains ────────────────────────────────────
CHAIN_BTC = ChainDefinition(
    id='btc',
    name='Bitcoin',
    native_unit='satoshi',
    decimals=8,
    env_prefix='BTC',
    seconds_per_block=600,
    min_confirmations=2,
    # 1000 sat, not the bare 546 P2PKH dust line: margin vs higher dustrelayfee / wallet quirks, and a tighter executable-rate ceiling.
    min_onchain_amount=1000,
)
CHAIN_TAO = ChainDefinition(
    id='tao',
    name='Bittensor',
    native_unit='rao',
    decimals=9,
    env_prefix='TAO',
    seconds_per_block=12,
    min_confirmations=6,
    # Existential deposit: accounts below this are reaped.
    min_onchain_amount=500,
)
CHAIN_SOL = ChainDefinition(
    id='sol',
    name='Solana',
    native_unit='lamport',
    decimals=9,
    env_prefix='SOL',
    # ~400ms slots; int-rounded up to 1 (the only consumers are the substrate-era
    # extension helpers, which the Solana validator no longer drives).
    seconds_per_block=1,
    # Confirmations are slots here; ~32 slots ≈ finalization (~13s), the swap-leg finality floor.
    min_confirmations=32,
    # Rent-exempt minimum for a 0-data System account — the SOL analog of TAO's
    # existential deposit (a credit below this can't keep a fresh account alive).
    min_onchain_amount=890880,
)

SUPPORTED_CHAINS = {
    'btc': CHAIN_BTC,
    'tao': CHAIN_TAO,
    'sol': CHAIN_SOL,
}


def get_chain(chain_id: str) -> ChainDefinition:
    """Lookup chain by ID. Raises KeyError if unsupported."""
    return SUPPORTED_CHAINS[chain_id]


def canonical_pair(chain_a: str, chain_b: str) -> tuple:
    """Return (source, dest) in canonical order for consistent commitment storage.

    Determines the rate unit: rate is always 'dest per 1 source' in this ordering.

    Ordering rules (priority):
    1. SOL is the hub asset (collateral, fee, and `sol_amount` notional are all SOL): it is always the
       canonical SOURCE, so every launch-pair rate reads uniformly as 'dest per 1 SOL' (e.g. TAO per SOL,
       BTC per SOL).
    2. Else if TAO is in the pair, TAO is dest — legacy denomination for non-SOL pairs.
    3. Else alphabetical — deterministic fallback (e.g. BTC-ETH).
    """
    if chain_a == 'sol':
        return (chain_a, chain_b)
    if chain_b == 'sol':
        return (chain_b, chain_a)
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
