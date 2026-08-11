import math
from dataclasses import dataclass

from allways.constants import (
    EXTENSION_BUCKET_SECONDS,
    EXTENSION_PADDING_SECONDS,
    NUMERAIRE_CHAIN,
)


@dataclass(frozen=True)
class ChainDefinition:
    """Definition of a supported chain."""

    id: str  # Short identifier (e.g. 'btc')
    name: str  # Display name (e.g. 'Bitcoin')
    native_unit: str  # Smallest unit name (e.g. 'satoshi')
    decimals: int  # Precision (e.g. 8 for BTC, 9 for TAO)
    # .env variable prefix — the NETWORK's, not the asset's ('ARB' -> ARB_NETWORK), so assets
    # sharing a network share its config. Only {id}_TOKEN_CONTRACT keys off the asset.
    env_prefix: str
    # Network names the provider accepts in {env_prefix}_NETWORK; [0] is the default (mainnet).
    # Empty = the network isn't picked by name here — either it resolves another way (sol/tao,
    # by RPC URL / bittensor network) or another asset on the same network owns the setting.
    networks: tuple[str, ...] = ()
    # Which of ``networks`` the `alw config set env testnet` bundle selects. Named, not
    # positional: a chain can offer several testnets (BTC has three) and only one is the
    # supported one, so the choice must survive reordering the list.
    testnet_network: str = ''
    seconds_per_block: int = 12  # Average block time on this chain
    min_confirmations: int = 1  # Minimum confirmations before accepting a transaction
    # Smallest amount that can actually exist/move on-chain, in native units
    # (BTC dust floor, TAO existential deposit). 1 = no floor.
    min_onchain_amount: int = 1
    # Replay-freshness grace (seconds): a tx is fresh iff block_time >= floor - grace.
    # Default 0 (at-or-after the floor; only a tx that predates it is a replay). Absorbs honest miner
    # clock skew; MUST stay well under reservation_ttl_secs — the replay window is exactly this wide (B2).
    replay_grace_secs: int = 0
    # The network whose txs carry this asset (assets/evm.py EVM_NETWORKS key). Every EVM row
    # sets it — native coin and token alike — so both resolve their chain the same way.
    # None only for assets that ARE their own network (btc/tao/sol).
    host_chain: str | None = None
    # Canonical MAINNET contract of a hosted asset. Testnet deployments + env overrides
    # resolve in the provider (assets/erc20.py) — each address lives exactly once.
    asset_locator: str | None = None


# ─── Supported Chains ────────────────────────────────────
CHAIN_BTC = ChainDefinition(
    id='btc',
    name='Bitcoin',
    native_unit='satoshi',
    decimals=8,
    env_prefix='BTC',
    networks=('mainnet', 'testnet', 'testnet4', 'signet'),
    testnet_network='testnet4',
    seconds_per_block=600,
    min_confirmations=2,
    # 1000 sat, not the bare 546 P2PKH dust line: margin vs higher dustrelayfee / wallet quirks, and a tighter executable-rate ceiling.
    min_onchain_amount=1000,
    # Only BTC needs a grace: a block's timestamp need only beat the median of the last 11, so it can lag
    # wall clock (or go backwards) and stamp an honest deposit before reservation.created_at → false replay.
    replay_grace_secs=300,
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

CHAIN_ETH = ChainDefinition(
    id='eth',
    name='Ethereum',
    native_unit='wei',
    decimals=18,
    env_prefix='ETH',
    host_chain='ethereum',
    networks=('mainnet', 'sepolia'),
    testnet_network='sepolia',
    seconds_per_block=12,
    # ~1 beacon epoch. True finality is 2 epochs (~64 blocks); post-merge reorgs deeper than
    # 2 blocks are vanishingly rare, so 32 buys near-finality without doubling the leg time.
    min_confirmations=32,
    # Rate sanity floor, not an economic guarantee: 0.00005 ETH covers a 21k-gas transfer
    # fee only below ~2.4 gwei — miners price real gas into their quotes.
    min_onchain_amount=50_000_000_000_000,
    # Timestamps must strictly exceed the parent's, so a mined deposit can never stamp
    # earlier than a reservation created before it — no BTC-style median-time lag.
    replay_grace_secs=0,
)

CHAIN_ARBUSDC = ChainDefinition(
    id='arbusdc',
    name='USDC (Arbitrum)',
    native_unit='µUSDC',
    decimals=6,
    env_prefix='ARB',
    networks=('mainnet', 'sepolia'),  # sepolia = Arbitrum Sepolia
    testnet_network='sepolia',
    # ~4 blocks/s real; 1s is the integer floor. 90 confs ≈ 25s real (~90s in extension
    # math) — both far inside the 600s default program grace, so no grace-table arm.
    seconds_per_block=1,
    min_confirmations=90,
    # ERC-20 transfers have no protocol minimum, so 1 is the honest floor. (The old
    # F1 orientation defect that made this value load-bearing is fixed in utils/rate.py;
    # raising it to an economic floor is now safe if ever wanted.)
    min_onchain_amount=1,
    # Sequencer-stamped timestamps vs the hub clock — modest skew allowance (Arbitrum
    # timestamps are non-decreasing, but monotonicity says nothing about hub-spoke skew).
    replay_grace_secs=60,
    host_chain='arbitrum',
    # Circle-verified native USDC on Arbitrum One (developers.circle.com, 2026-08-07).
    asset_locator='0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
)

CHAIN_HYPE = ChainDefinition(
    id='hype',
    name='Hyperliquid',
    native_unit='wei',
    decimals=18,
    env_prefix='HYPE',
    host_chain='hyperliquid',
    networks=('mainnet', 'testnet'),
    testnet_network='testnet',
    # ~1s small blocks (transfers never need the 60s big blocks — those carry >2M gas).
    seconds_per_block=1,
    # HyperBFT finalizes the block sequence on inclusion — no reorgs to outrun. 2 is a
    # one-block margin against an endpoint serving a head its consensus hasn't sealed yet.
    min_confirmations=2,
    # Rate sanity floor, not an economic guarantee: 0.0001 HYPE covers a 21k-gas transfer
    # below ~4.7 gwei — miners price real gas into their quotes.
    min_onchain_amount=100_000_000_000_000,
    # Consensus-stamped timestamps vs the hub clock — modest skew allowance, as on Arbitrum.
    replay_grace_secs=60,
)

SUPPORTED_CHAINS = {
    'btc': CHAIN_BTC,
    'tao': CHAIN_TAO,
    'sol': CHAIN_SOL,
    'eth': CHAIN_ETH,
    'arbusdc': CHAIN_ARBUSDC,
    'hype': CHAIN_HYPE,
}


def get_chain_def(chain_id: str) -> ChainDefinition:
    """Registry lookup: wire id → its ChainDefinition. Raises KeyError if unsupported.

    Same facts as ``Asset.chain_def``, reached from an id instead of an asset."""
    return SUPPORTED_CHAINS[chain_id]


def canonical_pair(chain_a: str, chain_b: str) -> tuple:
    """Return (source, dest) in canonical order for consistent commitment storage.

    Determines the rate unit: rate is always 'dest per 1 source' in this ordering.

    Ordering rules (priority):
    1. The hub (`NUMERAIRE_CHAIN`) is always the canonical SOURCE, so every launch pair reads uniformly as
       'dest per 1 hub' (e.g. TAO per SOL, BTC per SOL).
    2. Else if TAO is in the pair, TAO is dest — legacy denomination for non-hub pairs.
    3. Else alphabetical — deterministic fallback (e.g. BTC-ETH).
    """
    if chain_a == NUMERAIRE_CHAIN:
        return (chain_a, chain_b)
    if chain_b == NUMERAIRE_CHAIN:
        return (chain_b, chain_a)
    if chain_b == 'tao':
        return (chain_a, chain_b)
    if chain_a == 'tao':
        return (chain_b, chain_a)
    return (chain_a, chain_b) if chain_a < chain_b else (chain_b, chain_a)


def compute_extension_target_secs(chain_id: str, confirmations: int, now_unix: int, ceiling_unix: int) -> int:
    """Unix-seconds deadline to extend a valid-but-unconfirmed leg to.

    Covers the leg's remaining confirmations plus a padding buffer, bucket-rounded (in seconds) so
    validators converge, then clamped to the contract ceiling (``max_extend_at``).
    """
    chain = get_chain_def(chain_id)
    remaining = max(0, chain.min_confirmations - confirmations)
    target = now_unix + remaining * chain.seconds_per_block + EXTENSION_PADDING_SECONDS
    target = math.ceil(target / EXTENSION_BUCKET_SECONDS) * EXTENSION_BUCKET_SECONDS
    return min(target, ceiling_unix)
