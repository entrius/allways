import re

from allways.classes import MinerActivity

# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7

# ─── Contract ──────────────────────────────────────────────
# allways_swap_manager program address. Committed default is the devnet deployment;
# override with ALLWAYS_PROGRAM_ID. Must match the deployed program — a mismatch derives
# different PDAs, so every account merely reads as absent instead of erroring.
PROGRAM_ID = '6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD'

# ─── Polling ──────────────────────────────────────────────
# Bittensor base-neuron heartbeat, not the scoring/forward cadence.
MINER_POLL_INTERVAL_SECONDS = 12
VALIDATOR_POLL_INTERVAL_SECONDS = 12
# Consecutive polls of zero block progress before we force a substrate reconnect.
STALE_BLOCK_POLL_THRESHOLD = 30
# Seconds without a completed forward step before the supervisor declares the
# loop dead/hung and exits non-zero for the process manager to restart.
FORWARD_STALL_THRESHOLD_SECONDS = 600

# ─── Unit Conversions ────────────────────────────────────
TAO_TO_RAO = 1_000_000_000
BTC_TO_SAT = 100_000_000

# ─── Rate Encoding ───────────────────────────────────────
# Fixed-point scale for the miner rate: stored u128 = display_rate * RATE_PRECISION.
# Single source of truth, mirrors constants.rs (1e18).
RATE_PRECISION = 10**18
# Significant figures every posted rate is floored to. Enforced on-chain in set_quote
# (quantize_rate_sig_figs); the CLI mirrors it (quantize_rate_fixed) so previews match what is
# stored, and the validator floors on ingest to close the pre-redeploy migration window. Below
# this precision an undercut is imperceptible to takers, so the crown ignores it (equal buckets tie).
RATE_SIG_FIGS = 5
# Crown rate band: quotes within this fraction of the best qualified rate share the
# crown in proportion to collateral depth instead of losing outright to a one-tick
# undercut. Sized from mainnet data: absorbs quote staleness (~0.03% median executed-rate
# drift inside an hour) and rate gaps takers demonstrably ignore (~0.5%), while staying
# far below genuine pricing tiers (~2.6% between quote clusters) so a materially better
# rate still evicts the band and takes the crown outright. 0 restores exact-tie behaviour.
CROWN_RATE_BAND = 0.005

# ─── Transaction Fees ────────────────────────────────────
# Small headroom kept aside for extrinsic fees so a deposit doesn't burn gas
# and revert. Real fees are sub-millitao; 0.02 TAO is conservative.
MIN_BALANCE_FOR_TX_RAO = 20_000_000  # 0.02 TAO buffer for extrinsic fees
# BTC fee floor (sat/vB). Catches the case where the upstream estimator
# returns nonsense low. 5 is cheap enough to barely register on mainnet
# and still clears testnet quickly, so a single floor covers both.
# No-fault cancel (unpayable destination): advisory reason discriminants passed to cancel_swap.
# Mirror smart-contracts/.../constants.rs CANCEL_REASON_*; observability only, never consensus-bound.
CANCEL_REASON_EVM_REVERT = 0
CANCEL_REASON_ERC20_BLACKLIST = 1
CANCEL_REASON_ERC20_PAUSED = 2
CANCEL_REASON_SOL_RESERVED = 3
# An issuer-enabled transfer fee shaves the delivered log below the pinned amount, so every
# honest delivery on the token would false-slash — a hub-wide, no-fault condition (V-M2/PAXG).
CANCEL_REASON_ERC20_FEE_ENABLED = 4
# The issuer froze the destination's SPL token account (USDC's mint carries a freeze authority):
# undeliverable through no fault of the miner. Python-side first; mirror into constants.rs next release.
CANCEL_REASON_SPL_FROZEN = 5
CANCEL_REASON_OTHER = 255

BTC_MIN_FEE_RATE = 5
# Estimation-DOWN fallback (sat/vB), distinct from the mempool floor above: used only when
# /fee-estimates fails both attempts. A silent drop to the 5 floor has stranded a real dest tx
# (V-L5), so a failed estimate broadcasts at a survivable rate instead of the strand-prone floor —
# without lifting the floor itself (calm-mempool sends still pay the real low rate).
BTC_FALLBACK_FEE_RATE = 15
# Modest pad on estimated fee rates (not on explicit user overrides) against
# mempool conditions drifting between estimate and broadcast. Goal is
# 'reliably confirms within ~30 min', not 'next block at any cost'.
BTC_FEE_RATE_SAFETY_MULTIPLIER = 1.25

# ─── Scoring ─────────────────────────────────────────────
SCORING_WINDOW_BLOCKS = 300  # ~1 hour at 12s/block — scoring cadence and window width
# Unix-second axis for the Solana-sourced crown (B3.4): events carry blockTime,
# not block numbers, so the crown replay window + interval crediting are in
# seconds. The scoring *cadence* (due_for_scoring) stays subtensor-block-gated.
SCORING_WINDOW_SECS = 3600  # ~1 hour — crown replay window width
MAX_SCORING_BACKFILL_SECS = 2 * SCORING_WINDOW_SECS  # ~2 hours — backfill cap after a stall
# Crown reward-state policy (D4): the only place that decides which MinerActivity
# states earn crown. "All busy forfeits" = only AVAILABLE; add MinerActivity.FULFILLING
# here to reward in-flight miners, with no other logic change.
REWARD_MINER_STATES: frozenset[MinerActivity] = frozenset({MinerActivity.AVAILABLE})
# Hub (collateral-capable) chains, PRIORITY-ORDERED: the earlier hub anchors a hub↔hub pair, so
# sol↔tao stays SOL-anchored (grandfathered — existing quotes keep their stored convention).
# A pair is valid iff one leg is a hub; that leg is its pricing + bounds anchor ('dest per 1 hub').
HUB_CHAINS = ('sol', 'tao')
# The SOL constant — the Solana ledger's own asset (reservation fee, local collateral purse,
# the `alw miner quotes` default hub). "Is this the pair's hub" reads go through hub_leg() instead.
NUMERAIRE_CHAIN = 'sol'


def family(chain: str) -> str:
    """The backing family a chain settles in (twin of ``backing.rs::family``): an sn<N> alpha settles in TAO."""
    return 'tao' if re.fullmatch(r'sn\d+', chain) else chain


def is_hub(chain: str) -> bool:
    """True iff ``chain`` can anchor a pair (and back quotes with its own collateral purse)."""
    return chain in HUB_CHAINS


def hub_leg(from_chain: str, to_chain: str) -> str | None:
    """The pair's anchor — its pricing leg and scoring family: the literal hub if one is a leg, else the
    alphabetically first family-bearing leg (an alpha is its own scoring family). None = invalid pair."""
    for hub in HUB_CHAINS:
        if hub in (from_chain, to_chain):
            return hub
    family_legs = sorted(chain for chain in (from_chain, to_chain) if family(chain) != chain)
    return family_legs[0] if family_legs else None


def declarable_backings(from_chain: str, to_chain: str) -> list[str]:
    """The pair's hub-capable legs = the backings a quote may declare = its scoring lanes (F4):
    the hubs among the legs' families — two on sol↔tao, one on a spoke or alpha pair, none if invalid."""
    return [hub for hub in HUB_CHAINS if hub in {family(from_chain), family(to_chain)}]


# Chains paired against each hub; add a chain here to launch its pairs.
LAUNCH_SPOKES = (
    'btc',
    'tao',
    'eth',
    'arbusdc',
    'hype',
    'bnb',
    'avax',
    'baseusdc',
    'ethusdc',
    'cro',
    'aster',
    'uni',
    'qnt',
    'pol',
    'polusdc',
    'paxg',
    'solusdc',
)
# Alpha tokens paired against each hub; add a subnet here to launch its pairs.
LAUNCH_ALPHAS = (
    'sn7',
    'sn74',
)
# Every launch pair in canonical order: each hub against every spoke and alpha (sol↔tao lands once,
# under SOL, because sol never appears in LAUNCH_SPOKES). Alpha↔spoke pairs are gated on the
# emissions redesign and deliberately absent.
LAUNCH_PAIRS: tuple[tuple[str, str], ...] = tuple(
    (hub, spoke) for hub in HUB_CHAINS for spoke in LAUNCH_SPOKES if spoke != hub
) + tuple((hub, alpha) for hub in HUB_CHAINS for alpha in LAUNCH_ALPHAS)
# Fixed burn: pools sum to MINER_POOL_SHARE instead of 1.0, so at least
# BURN_RATE of every round recycles to RECYCLE_UID before any shortfall.
BURN_RATE = 0.90
MINER_POOL_SHARE = 1.0 - BURN_RATE
# Direction registry and the equal-split fallback: one entry per hub↔spoke direction
# (both ways). The per-round pool values are volume-weighted at pair level
# (scoring.compute_direction_pools); these constants are what zero volume falls back to.
DIRECTION_POOLS: dict[tuple[str, str], float] = {
    pair: MINER_POOL_SHARE / (2 * len(LAUNCH_PAIRS))
    for hub, spoke in LAUNCH_PAIRS
    for pair in ((hub, spoke), (spoke, hub))
}
# Volume-weighted pools: each pair's emission share follows the SOL notional it cleared
# over the trailing window, blended with the equal split so a quiet pair never starves
# and a busy one is capped at α + (1−α)/pairs. Weighting sits at PAIR level and splits
# evenly between the two legs — one leg can't be inflated without inflating the pair.
POOL_VOLUME_WINDOW_SECS = 24 * 3600  # flat trailing window the pool volumes sum over
POOL_VOLUME_ALPHA = 0.66  # blend dial: 0 = frozen equal split, 1 = pure volume share
# clearing_rates rows must outlive the pool volume window (plus stall headroom) — the
# crown tables only need SCORING_WINDOW_SECS, but pools read a full day back.
CLEARING_RETENTION_SECS = POOL_VOLUME_WINDOW_SECS + MAX_SCORING_BACKFILL_SECS
# Capacity curve exponent (>1 = convex): capacity = min(1, (collateral / required)^k). Convex so
# thin-parked collateral is penalised harder than linear (a miner backing the best rate on a sliver
# earns a smaller slice than the ratio alone), pushing miners to deepen. Still capped at 1.0 — depth
# past required earns nothing extra, so it never becomes pay-to-win.
CAPACITY_CURVE_EXPONENT: float = 2.0
# Flat eligibility gate (B3.3): read off the on-chain MinerState counters,
# replacing the success_rate³ × credibility ramp. A miner is crown-eligible iff
# it has at least MIN_SUCCESSFUL_SWAPS successes and at most MAX_FAILED_SWAPS
# failures — a binary 0/1 multiplier, no ramp.
MIN_SUCCESSFUL_SWAPS: int = 2
MAX_FAILED_SWAPS: int = 2
# Live-state reconcile (scoring-round backstop for lost events): a miner's event-derived
# active/collateral state is only corrected against the live chain read after its event
# stream has been quiet this long, so a stale RPC read never fights an in-flight event.
RECONCILE_QUIET_SECS = 600

# ─── Validator stake weights (reservation-lottery draw) ──
# Each validator derives the same vector — floor(alpha_stake / bucket) per whitelisted
# validator, index-aligned to Config.validators — and votes it on-chain (vote_set_weights).
# Posting is block-aligned: every validator fires just after the same block boundary, so all
# read the metagraph at ~the same stake snapshot and the quorum's hash-bound vectors converge.
SECONDS_PER_BLOCK = 12
# Alpha per draw-weight unit (floor rounding). 35k (from 50k) buys finer share resolution now that
# weight also sets the stake-discounted reservation fee, at the cost of slightly more frequent
# vector-change votes.
WEIGHTS_STAKE_BUCKET_ALPHA = 35_000
WEIGHTS_VOTE_INTERVAL_BLOCKS = 3_600  # ~12h — posting boundary cadence
# Contract vote-round lifetime — mirrors constants.rs. A non-empty round older than this is
# stale: record_vote clears and reopens it, and prior voters may legally vote again.
VOTE_ROUND_TTL_SECS = 1_800
# In-epoch retry throttle: one attempt per contract vote-round lifetime (VOTE_ROUND_TTL_SECS),
# so an unlanded round has expired (and is reopenable with our snapshot) by the time we retry.
WEIGHTS_VOTE_RETRY_SECS = 1_800

# ─── W3 bond relay (Solana ledger ↔ Bittensor vault) ─────
# Cadences the relayer runs on. All overridable per-process via the matching ALLWAYS_RELAY_* env
# vars (see validator/relay/engine.RelayConfig) — a dev stack has to run these in seconds.
# The heartbeat is a LAZY global liveness bump, not a data refresh: attestation writes are
# event-driven, and the on-chain fuse wants max-age ≥ 2× this.
RELAY_HEARTBEAT_INTERVAL_SECS = 12 * 3600
# Time-aligned global fee true-up. Every validator fires at the same boundary and reads totals at
# it, so the batch vector is byte-identical; a boundary with zero delta is skipped as pure postage.
RELAY_FEE_CADENCE_SECS = 2 * 86400
# Slow continuous vault↔attestation repair loop (crash-between-paired-writes, missed refreshes).
RELAY_RECONCILE_INTERVAL_SECS = 900
# Margin past a miner's busy/settling windows before its exit sequence calls the bond quiescent.
RELAY_QUIESCENCE_GRACE_SECS = 60
# Retention for the live-swap reimbursement snapshots; rows with an unapplied slash are exempt.
RELAY_SWAP_RETENTION_SECS = 7 * 86400

# ─── TAO bond vault (ink!) — deployed contract address, of record ───
# So a deployment isn't lost. The runtime resolves the ACTIVE vault via ALLWAYS_VAULT_ADDRESS /
# `alw config set vault-address`; this map is the reference record, keyed by subtensor network,
# and the vault CLI warns when a configured address strays from it — a bond posted to a vault no
# validator watches reads back healthy but is never attested.
TAO_HUB_VAULT_ADDRESSES = {
    'test': '5Fkn2rNGvWxZ3cMNWbbT3FVrsyBjWmpE5fU4yYGDCrKAfLhs',  # SN19
    'finney': '5EejtudSLREHj8pAvNLcDg3gm1skP1RZGtVuoDQpVVUwpWJJ',  # SN7
}

# ─── Swap outcome retention ──────────────────────────────
# Terminal completed/timed_out rows (seam stage truth after the swap PDA closes). Rows are
# tiny and only queried while an offering still polls a finished swap — 7 days is generous.
SWAP_OUTCOME_RETENTION_SECS = 7 * 86400

# ─── Collateral ──────────────────────────────────────────
# Collateral a miner must post to back a swap = collateral_amount × this/10_000. Mirrors the contract's
# COLLATERAL_REQUIREMENT_BPS (constants.rs) — keep in sync. 11_000 = 1.10×.
COLLATERAL_REQUIREMENT_BPS = 11_000


def required_collateral(collateral_amount: int) -> int:
    """Lamports a miner must hold to back ``collateral_amount`` (1.10×). Mirrors the contract."""
    return collateral_amount * COLLATERAL_REQUIREMENT_BPS // 10_000


# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID

# ─── Optimistic Extensions ───────────────────────────────
# Tunables for the propose/challenge/finalize extension flow. Per-chain timing
# (block time, confirmations) lives in allways/chains.py; the contract enforces
# the extension ceiling (max_extend_at) independently.
EXTENSION_PADDING_SECONDS = 120  # safety buffer on top of confirmation time
# Validator-view convergence: extension targets snap up to this native-seconds grid so validators
# computing `now + confirmation_runway` at slightly different wall-clock moments agree on one target_at.
# Seconds, never blocks (the deadline axis is unix-seconds); >= the slowest chain's block time so a
# bucket always spans at least one source block.
EXTENSION_BUCKET_SECONDS = 600  # 10 min

# ─── Protocol Fee ──────────────────────────────────────────
# Hardcoded 1% — matches the contract's immutable FEE_DIVISOR.
FEE_DIVISOR = 100

# Base fulfillment window (seconds, ~10 min) — the sent-cache margin's base-window buffer.
DEFAULT_FULFILLMENT_TIMEOUT_SECS = 600

# ─── Unix-axis miner runways (B4 — Solana) ────────────────
# The Solana swap deadline (`Swap.timeout_at`) is unix-seconds. Cushion the miner subtracts from
# each swap's timeout before agreeing to fulfill, so it never starts a fulfill inside the span
# where validators can no longer land an extension propose + challenge before expiry. Sized to
# that runway — two validator forward steps plus the challenge window, at 12s subtensor blocks
# ((2·5 + 8) × 12) — not operator preference; edit here if extension cadence changes.
MINER_TIMEOUT_CUSHION_SECS = 216
# Retain a miner's unmarked sent entry until past the contract's max extended deadline, else it can
# discard then re-send a still-claimable swap (#461 double-send). The contract slides the deadline
# cumulatively up to the extension ceiling, so cover that full budget plus one base window. Keep
# CONTRACT_MAX_TOTAL_EXTENSION_SECS in sync with smart-contracts/solana/.../constants.rs.
CONTRACT_MAX_TOTAL_EXTENSION_SECS = 8400  # 140 min — mirror of the contract's MAX_TOTAL_EXTENSION_SECS
SENT_CACHE_DISCARD_MARGIN_SECS = CONTRACT_MAX_TOTAL_EXTENSION_SECS + DEFAULT_FULFILLMENT_TIMEOUT_SECS
