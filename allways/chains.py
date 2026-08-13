import math
from dataclasses import dataclass

from allways.constants import (
    EXTENSION_BUCKET_SECONDS,
    EXTENSION_PADDING_SECONDS,
    HUB_CHAINS,
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
    # ABI signatures the issuer refuses delivery with: 'f()' stops the whole token, 'f(address)'
    # freezes one destination. Required on every token row; () claims no freeze surface at all.
    refusal_checks: tuple[str, ...] | None = None


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
    # Monotonic slot timestamps don't help here: the freshness floor is stamped by the hub
    # clock, so hub-vs-spoke skew needs the same modest allowance as the other EVM chains.
    replay_grace_secs=60,
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
    # 5 USDC. min_onchain_amount floors the routable source amount inside is_executable_rate, so a
    # floor of 1 (0.000001 USDC) leaves the crown-eligibility gate open to absurd rates routable only
    # for dust — distorting emissions and the network's displayed rates. 5 USDC tightens that band.
    min_onchain_amount=5_000_000,
    # Sequencer-stamped timestamps vs the hub clock — modest skew allowance (Arbitrum
    # timestamps are non-decreasing, but monotonicity says nothing about hub-spoke skew).
    replay_grace_secs=60,
    host_chain='arbitrum',
    # Circle-verified native USDC on Arbitrum One (developers.circle.com, 2026-08-07).
    asset_locator='0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
    refusal_checks=('isBlacklisted(address)', 'paused()'),
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

CHAIN_BNB = ChainDefinition(
    id='bnb',
    name='BNB Smart Chain',
    native_unit='wei',
    decimals=18,
    env_prefix='BNB',
    host_chain='bsc',
    networks=('mainnet', 'testnet'),  # testnet = Chapel
    testnet_network='testnet',
    # Fermi (Jan 2026) cut blocks to 0.45s — measured 0.4502s over the last 100k mainnet blocks.
    # 1 is the integer floor, so every derived bound is conservative in wall time, never short.
    seconds_per_block=1,
    # BEP-126 finalizes a block once it and its direct child carry >2/3 attestations (~2 blocks).
    # Below that quorum — e.g. across an epoch's validator-set rotation — BSC falls back to its
    # longest-chain rule, where what bounds one dishonest proposer is turn_length: the run of
    # consecutive blocks a single validator signs (BEP-341; measured 8 on mainnet, 2026-08-11).
    # 15 outlasts one full turn, so no lone validator can build the whole span. turn_length is a
    # governance parameter and has already been raised twice — if it rises again, raise this above it.
    min_confirmations=15,
    # Rate sanity floor, not an economic guarantee: 0.0002 BNB covers a 21k-gas transfer below
    # ~9.5 gwei — well above the 3-5 gwei BSC sustained through 2022-2024, and ~200x today's
    # 0.05 gwei minimum. Miners price real gas into their quotes.
    min_onchain_amount=200_000_000_000_000,
    # Consensus-stamped timestamps vs the hub clock — modest skew allowance, as on Arbitrum.
    replay_grace_secs=60,
)

CHAIN_AVAX = ChainDefinition(
    id='avax',
    name='Avalanche',
    native_unit='wei',
    decimals=18,
    env_prefix='AVAX',
    host_chain='avalanche',
    networks=('mainnet', 'fuji'),
    testnet_network='fuji',
    # ~1.1s measured on C-Chain mainnet; the integer floor is 1, as on Arbitrum.
    seconds_per_block=1,
    # Snowman accepts a block irreversibly, so finality IS inclusion — there is no reorg
    # depth to outrun. 2 is a one-block margin against an endpoint serving an unaccepted head.
    min_confirmations=2,
    # Rate sanity floor, not an economic guarantee: 0.001 AVAX covers a 21k-gas transfer below
    # ~47 gwei — a congested C-Chain level, ~1000x the quiet base fee its dynamic fee floats at.
    min_onchain_amount=1_000_000_000_000_000,
    # Consensus-stamped timestamps vs the hub clock — modest skew allowance, as on Arbitrum.
    replay_grace_secs=60,
)

CHAIN_BASEUSDC = ChainDefinition(
    id='baseusdc',
    name='USDC (Base)',
    native_unit='µUSDC',
    decimals=6,
    env_prefix='BASE',
    networks=('mainnet', 'sepolia'),  # sepolia = Base Sepolia
    testnet_network='sepolia',
    # Measured live on both networks (2026-08-11): exactly 2s between consecutive blocks.
    seconds_per_block=2,
    # Base is an OP-stack rollup: the sequencer can reorg its own unsafe head before the L1
    # batch posts, so it needs far more depth than a fast-finality chain. 120 × 2s ≈ 240s,
    # well inside the program's 600s default fulfillment grace.
    min_confirmations=120,
    # 5 USDC — matches Arbitrum/Ethereum USDC. As a rate-sanity input to is_executable_rate, a floor
    # of 1 lets absurd rates pass the crown-eligibility gate routable only for dust; 5 USDC tightens it.
    min_onchain_amount=5_000_000,
    # Sequencer-stamped timestamps vs the hub clock — modest skew allowance (monotonic
    # timestamps say nothing about how far the sequencer's clock sits from the hub's).
    replay_grace_secs=60,
    host_chain='base',
    # Circle-verified native USDC on Base (developers.circle.com, 2026-08-11) — NOT the
    # bridged USDbC at 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA.
    asset_locator='0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
    refusal_checks=('isBlacklisted(address)', 'paused()'),
)

CHAIN_ETHUSDC = ChainDefinition(
    id='ethusdc',
    name='USDC (Ethereum)',
    native_unit='µUSDC',
    decimals=6,
    # Ethereum's prefix, shared with CHAIN_ETH: one ETH_NETWORK / ETH_RPC_URLS / ETH_PRIVATE_KEY
    # serves both assets, so they can never disagree about which Ethereum they are on.
    env_prefix='ETH',
    host_chain='ethereum',
    # CHAIN_ETH already declares the ETH_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    seconds_per_block=12,
    # CHAIN_ETH's finality, deliberately identical — two assets on one chain must not disagree
    # about its reorg depth. 32 × 12s = 384s, inside the program's 600s default fulfillment grace.
    min_confirmations=32,
    # Rate sanity floor, not an economic guarantee: 5 USDC covers FiatToken's ~65k-gas transfer
    # only below ~41 gwei — miners price real gas into their quotes, and the per-swap lever is the
    # contract's min_swap_amount. Above arbusdc's unit floor because L1 gas is not L2 gas.
    min_onchain_amount=5_000_000,
    # Ethereum's clock, as CHAIN_ETH: what this absorbs is hub-vs-spoke skew, and two assets on
    # one chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # Circle-verified native USDC on Ethereum (developers.circle.com, 2026-08-11).
    asset_locator='0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
    refusal_checks=('isBlacklisted(address)', 'paused()'),
)

CHAIN_CRO = ChainDefinition(
    id='cro',
    name='Cronos',
    native_unit='wei',
    decimals=18,
    env_prefix='CRO',
    host_chain='cronos',
    networks=('mainnet', 'testnet'),
    testnet_network='testnet',
    # 0.4749s measured over the last 100k mainnet blocks; 1 is the integer floor. Flooring up is
    # safe where the value is MULTIPLIED (the extension target overestimates the wait) but not
    # where a window is DIVIDED out of it: the 300s scan lookback lands at ~142s of wall time and
    # delivery_refused's 120-block cap at ~57s. See the SCAN_LOOKBACK_SECS note in assets/evm.py.
    seconds_per_block=1,
    # Cronos is Ethermint on CometBFT: a block commits only with >2/3 pre-commits and no fork-choice
    # rule can revert it, so finality IS inclusion — there is no reorg depth to outrun, as on
    # Avalanche. 2 is a one-block margin against an endpoint serving a head consensus hasn't sealed.
    min_confirmations=2,
    # Rate sanity floor, not an economic guarantee: 0.5 CRO covers a 21k-gas transfer below ~23,800
    # gwei — ~63x the 375 gwei base fee measured on mainnet, whose gas prices sit three orders of
    # magnitude above L1's. Miners price real gas into their quotes.
    min_onchain_amount=500_000_000_000_000_000,
    # Consensus-stamped timestamps vs the hub clock — modest skew allowance, as on Arbitrum.
    replay_grace_secs=60,
)

CHAIN_ASTER = ChainDefinition(
    id='aster',
    name='Aster',
    native_unit='wei',
    decimals=18,
    # BNB Smart Chain's prefix, shared with CHAIN_BNB: one BNB_NETWORK / BNB_RPC_URLS /
    # BNB_PRIVATE_KEY serves both assets, so they can never disagree about which BSC they are on.
    env_prefix='BNB',
    host_chain='bsc',
    # CHAIN_BNB already declares the BNB_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    seconds_per_block=1,
    # CHAIN_BNB's finality, deliberately identical — two assets on one chain must not disagree
    # about its reorg depth. See CHAIN_BNB for the derivation (BEP-126 quorum, BEP-341
    # turn_length 8). 15 × 1s, inside the program's 600s default fulfillment grace.
    min_confirmations=15,
    # Rate sanity floor, not an economic guarantee. Break-even at CHAIN_BNB's 9.5 gwei reference
    # is 0.48 ASTER for a ~50k-gas BEP-20 transfer (measured 29,698 warm); rounded up to 1, which
    # prices ~19 gwei. Deliberately the strictest floor in the registry — a floor can only
    # over-restrict, never slash. Miners price real gas in; the per-swap lever is min_swap_amount.
    min_onchain_amount=1_000_000_000_000_000_000,
    # BSC's clock, as CHAIN_BNB: what this absorbs is hub-vs-spoke skew, and two assets on one
    # chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # ASTER as published by the issuer (docs.asterdex.com, 2026-08-12).
    asset_locator='0x000Ae314E2A2172a039B26378814C252734f556A',
    # Verified source is stock OpenZeppelin ERC20 + ERC20Permit with no blacklist and no pause,
    # on a contract that can never gain one: EIP-1967 slots empty, owner() reverts (2026-08-12).
    refusal_checks=(),
)

CHAIN_UNI = ChainDefinition(
    id='uni',
    name='Uniswap',
    native_unit='wei',
    decimals=18,
    # Ethereum's prefix, shared with CHAIN_ETH and CHAIN_ETHUSDC: one ETH_NETWORK / ETH_RPC_URLS /
    # ETH_PRIVATE_KEY serves every asset on it, so they can never disagree about which Ethereum.
    env_prefix='ETH',
    host_chain='ethereum',
    # CHAIN_ETH already declares the ETH_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    seconds_per_block=12,
    # CHAIN_ETH's finality, deliberately identical — two assets on one chain must not disagree
    # about its reorg depth. 32 × 12s = 384s, inside the program's 600s default fulfillment grace.
    min_confirmations=32,
    # Rate sanity floor, not an enforced minimum: 3 UNI covers a 90k-gas dest leg (the observed
    # 57k worst case plus the two delegation checkpoints a transfer can write) only below ~62
    # gwei — a realistic-high L1 level, not the 0.1 gwei of a quiet day. Worth 2.1x
    # CHAIN_ETHUSDC's floor: 1.38x of that is the bigger gas budget, 1.51x is a higher assumed
    # gwei. Assumed gas price is per-asset economics, unlike min_confirmations which is a chain fact.
    min_onchain_amount=3_000_000_000_000_000_000,
    # Ethereum's clock, as CHAIN_ETH: what this absorbs is hub-vs-spoke skew, and two assets on
    # one chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # UNI as published by Uniswap (docs.uniswap.org governance/UNI reference, 2026-08-12).
    asset_locator='0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
    # No freeze surface: the PUSH4 dispatch table resolves 27 selectors, exactly Uni.sol's ABI, so
    # there is no blacklist/pause/owner entry point to call. It cannot gain one either — EIP-1967
    # implementation/admin/beacon slots are all zero. The minter (governance timelock) only
    # inflates supply; it can never freeze an address.
    refusal_checks=(),
)

CHAIN_QNT = ChainDefinition(
    id='qnt',
    name='Quant',
    # atto-QNT. Not 'wei' — that is ETH's subunit, and this row is a token on Ethereum, not ETH.
    native_unit='aQNT',
    decimals=18,
    # Ethereum's prefix, shared with CHAIN_ETH: one ETH_NETWORK / ETH_RPC_URLS / ETH_PRIVATE_KEY
    # serves every asset on it, so they can never disagree about which Ethereum they are on.
    env_prefix='ETH',
    host_chain='ethereum',
    # CHAIN_ETH already declares the ETH_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    seconds_per_block=12,
    # CHAIN_ETH's finality, deliberately identical — two assets on one chain must not disagree
    # about its reorg depth. 32 × 12s = 384s, inside the program's 600s default fulfillment grace.
    min_confirmations=32,
    # Rate sanity floor, not an economic guarantee: 0.1 QNT covers this contract's ~65k-gas
    # transfer only below ~48 gwei — miners price real gas into their quotes, and the per-swap
    # lever is the contract's min_swap_amount. A fraction of a token, not whole units: QNT's
    # unit value is ~60x a stablecoin's, so a floor of 1 QNT would price out honest small legs.
    min_onchain_amount=100_000_000_000_000_000,
    # Ethereum's clock, as CHAIN_ETH: what this absorbs is hub-vs-spoke skew, and two assets on
    # one chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # Quant's ICO token, deployed 2018 by Quant and verified byte-for-byte on Sourcify (solc
    # 0.4.21 StandardToken, not a proxy). mint is crowdsale-gated; nothing else is privileged.
    asset_locator='0x4a220E6096B25EADb88358cb44068A3248254675',
    # No freeze surface: the verified source has no blacklist, no pause and no owner. And none can
    # be added: EIP-1967 slots empty, and the runtime holds no DELEGATECALL/SELFDESTRUCT/CREATE.
    refusal_checks=(),
)

CHAIN_POL = ChainDefinition(
    id='pol',
    name='Polygon',
    native_unit='wei',
    decimals=18,
    env_prefix='POL',
    host_chain='polygon',
    networks=('mainnet', 'amoy'),
    testnet_network='amoy',
    # Measured 1.5000s over 50k mainnet blocks (2026-08-12); Amoy runs 1.0000s. 1 is the integer
    # floor, and unlike the sub-second chains here it UNDER-states real time. Bounds that DIVIDE by
    # it (scan lookback, the delivery_refused span) cover 1.5x the wall seconds they read; the one
    # that MULTIPLIES, compute_extension_target_secs, under-counts by 50s — inside its 120s padding.
    seconds_per_block=1,
    # Heimdall v2 milestones finalize deterministically but BEHIND the tip (the finalized tag has
    # trailed by 1-5 blocks across repeated 40-sample runs, matching the docs' 2-5s), so unlike
    # Snowman/HyperBFT there is a reorg window to outrun. Its depth is set by how long a milestone
    # can stall, not by a producer turn as on BSC: under VEBloP one elected producer builds the
    # whole unfinalized span. 100 blocks = 150s real, >=20x the deepest lag seen, and inside the
    # program's 600s fulfillment grace.
    min_confirmations=100,
    # Rate sanity floor, not an economic guarantee: 0.05 POL covers a 21k-gas transfer up to
    # ~2380 gwei — 8.4x the ~282 gwei a transfer actually costs today (249 base + 30 tip), and
    # 4.5x the ~534 gwei ceiling a send authorises (2x base + tip). Miners price real gas.
    min_onchain_amount=50_000_000_000_000_000,
    # Producer-stamped timestamps vs the hub clock, as on Arbitrum — and under VEBloP a single
    # elected producer stamps a whole span, so its clock offset never averages out across proposers.
    replay_grace_secs=60,
)

CHAIN_POLUSDC = ChainDefinition(
    id='polusdc',
    name='USDC (Polygon)',
    native_unit='µUSDC',
    decimals=6,
    # Polygon's prefix, shared with CHAIN_POL: one POL_NETWORK / POL_RPC_URLS / POL_PRIVATE_KEY
    # serves both assets, so they can never disagree about which Polygon they are on.
    env_prefix='POL',
    host_chain='polygon',
    # CHAIN_POL already declares the POL_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    # CHAIN_POL's clock and finality, deliberately identical — two assets on one chain must not
    # disagree about its reorg depth. 100 blocks = 150s of real 1.5s blocks, inside the program's
    # 600s default fulfillment grace. Pinned against CHAIN_POL by test, so neither can drift.
    seconds_per_block=1,
    min_confirmations=100,
    # Rate sanity floor, not an economic guarantee: 0.01 USDC covers FiatToken's measured 79k-gas
    # transfer below ~1700 gwei — ~7x the 230-260 gwei base fee Polygon has held for weeks. Priced
    # on the COLD recipient (79k; a warm one is 61k), since a payout goes to a user's address that
    # usually holds no USDC yet. Far under ethusdc's 5 USDC because Polygon gas costs cents, far
    # over arbusdc's unit floor because it is not free either.
    min_onchain_amount=10_000,
    # CHAIN_POL's clock, for the same reason its finality is: what this absorbs is hub-vs-spoke
    # skew, and two assets on one chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # Circle-verified native USDC on Polygon PoS (developers.circle.com, 2026-08-12) — NOT the
    # bridged USDC.e at 0x2791Bca1f2de4661eD88A30C99A7a9449Aa84174, which also answers
    # symbol() == 'USDC' and the FiatToken freeze surface, so only this pin tells them apart.
    asset_locator='0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359',
    refusal_checks=('isBlacklisted(address)', 'paused()'),
)

CHAIN_PAXG = ChainDefinition(
    id='paxg',
    name='PAX Gold',
    # atto-PAXG. Not 'wei' — that is ETH's subunit, and this row is a token on Ethereum, not ETH.
    native_unit='aPAXG',
    decimals=18,
    # Ethereum's prefix, shared with CHAIN_ETH and CHAIN_ETHUSDC: one ETH_NETWORK / ETH_RPC_URLS /
    # ETH_PRIVATE_KEY serves every asset on it, so they can never disagree about which Ethereum.
    env_prefix='ETH',
    host_chain='ethereum',
    # CHAIN_ETH already declares the ETH_NETWORK names; a second declaration would render a
    # duplicate CLI row writing the same var.
    networks=(),
    seconds_per_block=12,
    # CHAIN_ETH's finality, deliberately identical — assets on one chain must not disagree about
    # its reorg depth. 32 x 12s = 384s, inside the program's 600s default fulfillment grace.
    min_confirmations=32,
    # Rate sanity floor, not an enforced minimum. One PAXG is a troy ounce of gold, so a token
    # COUNT copied from another row would be absurd here: UNI's floor of 3 would mean three ounces,
    # over $12,000, and the pair would silently never route. Derived instead from the gas the dest
    # leg burns: 66,730 measured worst case (fresh recipient, cold slot) at 62 gwei is 0.0041 ETH,
    # ~$7.74, ~0.0018 PAXG at $4,353/oz and $1,877/ETH (2026-08-13). Rounded up to 0.002.
    min_onchain_amount=2_000_000_000_000_000,
    # Ethereum's clock, as CHAIN_ETH: what this absorbs is hub-vs-spoke skew, and two assets on
    # one chain disagreeing about that chain's clock would be indefensible.
    replay_grace_secs=60,
    # PAX Gold as published by Paxos (paxos.com/paxgold, contract verified on Etherscan, 2026-08-13).
    asset_locator='0x45804880De22913dAFE09f4980848ECE6EcbAf78',
    # Paxos freezes per address with isFrozen, NOT Circle's isBlacklisted — probed live 2026-08-13:
    # isFrozen(address) and paused() both answer, isBlacklisted(address) reverts. Declaring Circle's
    # surface here would make every probe raise, which defers each slash only as far as
    # max_extend_at (~2.6h) and then times out anyway — slashing and striking an honest miner.
    refusal_checks=('isFrozen(address)', 'paused()'),
)

SUPPORTED_CHAINS = {
    'btc': CHAIN_BTC,
    'tao': CHAIN_TAO,
    'sol': CHAIN_SOL,
    'eth': CHAIN_ETH,
    'arbusdc': CHAIN_ARBUSDC,
    'hype': CHAIN_HYPE,
    'bnb': CHAIN_BNB,
    'avax': CHAIN_AVAX,
    'baseusdc': CHAIN_BASEUSDC,
    'ethusdc': CHAIN_ETHUSDC,
    'cro': CHAIN_CRO,
    'aster': CHAIN_ASTER,
    'uni': CHAIN_UNI,
    'qnt': CHAIN_QNT,
    'pol': CHAIN_POL,
    'polusdc': CHAIN_POLUSDC,
    'paxg': CHAIN_PAXG,
}


def get_chain_def(chain_id: str) -> ChainDefinition:
    """Registry lookup: wire id → its ChainDefinition. Raises KeyError if unsupported.

    Same facts as ``Asset.chain_def``, reached from an id instead of an asset."""
    return SUPPORTED_CHAINS[chain_id]


def canonical_pair(chain_a: str, chain_b: str) -> tuple:
    """Return (source, dest) in canonical order for consistent commitment storage.

    Determines the rate unit: rate is always 'dest per 1 source' in this ordering.

    Ordering rules (priority):
    1. The pair's hub leg is always the canonical SOURCE, so every launch pair reads uniformly as
       'dest per 1 hub' (e.g. TAO per SOL, ETH per TAO). ``HUB_CHAINS`` order breaks a hub↔hub
       pair: sol↔tao stays SOL-anchored (grandfathered — stored quotes keep their convention).
    2. Else alphabetical — deterministic fallback for spoke↔spoke (never a valid swap pair).
    """
    for hub in HUB_CHAINS:
        if chain_a == hub:
            return (chain_a, chain_b)
        if chain_b == hub:
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
