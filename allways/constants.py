import os

from allways.classes import MinerActivity

# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7
NETUID_LOCAL = 2

# ─── Contract ──────────────────────────────────────────────
# Mainnet default; override via CONTRACT_ADDRESS env var.
CONTRACT_ADDRESS = '5DjJmTpcHZvF3aZZEafKBdo3ksmdUSZ8bBBUSFhW3Ce3xf1J'

# ─── Polling ──────────────────────────────────────────────
# Bittensor base-neuron heartbeat, not the scoring/forward cadence.
MINER_POLL_INTERVAL_SECONDS = 12
VALIDATOR_POLL_INTERVAL_SECONDS = 12
# Consecutive polls of zero block progress before we force a substrate reconnect.
STALE_BLOCK_POLL_THRESHOLD = 30
# Seconds without a completed forward step before the supervisor declares the
# loop dead/hung and exits non-zero for the process manager to restart.
FORWARD_STALL_THRESHOLD_SECONDS = 600

# ─── Commitment Format ────────────────────────────────────
COMMITMENT_VERSION = 1

# ─── Unit Conversions ────────────────────────────────────
TAO_TO_RAO = 1_000_000_000
BTC_TO_SAT = 100_000_000

# ─── Rate Encoding ───────────────────────────────────────
RATE_PRECISION = 10**18
# Significant digits enforced on every committed rate. Normalized at the CLI
# (post) and again at the validator (parse) so scoring buckets, consensus
# hashes, and contract storage all agree on the same canonical string.
RATE_SIG_FIGS = 5

# ─── Transaction Fees ────────────────────────────────────
# Small headroom kept aside for extrinsic fees so a deposit doesn't burn gas
# and revert. Real fees are sub-millitao; 0.02 TAO is conservative.
MIN_BALANCE_FOR_TX_RAO = 20_000_000  # 0.02 TAO buffer for extrinsic fees
# BTC fee floor (sat/vB). Catches the case where the upstream estimator
# returns nonsense low. 5 is cheap enough to barely register on mainnet
# and still clears testnet quickly, so a single floor covers both.
BTC_MIN_FEE_RATE = 5
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
SCORING_EMA_ALPHA = 1.0  # Instantaneous — no smoothing across passes
# Crown reward-state policy (D4): the only place that decides which MinerActivity
# states earn crown. "All busy forfeits" = only AVAILABLE; add MinerActivity.FULFILLING
# here to reward in-flight miners, with no other logic change.
REWARD_MINER_STATES: frozenset[MinerActivity] = frozenset({MinerActivity.AVAILABLE})
# Numéraire / hub asset: the subnet is hub-and-spoke — every launch pair is hub↔spoke, so a rate reads
# uniformly as 'dest per 1 hub'. SOL by construction (the contract lives on Solana: collateral, fee, and the
# sol_amount notional are all SOL). Referenced wherever code needs "is this the hub", instead of a literal.
NUMERAIRE_CHAIN = 'sol'
LAUNCH_SPOKES = ('btc', 'tao')  # chains paired against the hub; add a chain here to launch its pair
# Emission pool per direction, split evenly across every hub↔spoke direction (both ways).
DIRECTION_POOLS: dict[tuple[str, str], float] = {
    pair: 1.0 / (2 * len(LAUNCH_SPOKES))
    for spoke in LAUNCH_SPOKES
    for pair in ((NUMERAIRE_CHAIN, spoke), (spoke, NUMERAIRE_CHAIN))
}
# Idle-crown penalty: 0 = none, 1 = pure volume share, 0.5 = half-credit floor.
VOLUME_WEIGHT_ALPHA: float = 0.5
# Reward-shape weights (B3.5): reward = eligible × [w_a·crown + w_b·quality_volume].
# w_a weights the crown-time component (best-rate presence × capacity), w_b the
# realized-volume share × rate-quality (executed throughput at fair rates). Phase C
# turns w_b on at a conservative 0.2 so crown stays the primary driver while real
# throughput at good rates earns weight. Kept w_a + w_b = 1 so the total distributed
# envelope is unchanged. Too-thin on-chain clearing-rate history ⇒ rate_quality neutral
# (1.0), so w_b still pays realized volume by raw share — it never zeroes everyone.
REWARD_WEIGHT_CROWN: float = 0.8
REWARD_WEIGHT_QUALITY_VOLUME: float = 0.2
# Flat eligibility gate (B3.3): read off the on-chain MinerState counters,
# replacing the success_rate³ × credibility ramp. A miner is crown-eligible iff
# it has at least MIN_SUCCESSFUL_SWAPS successes and at most MAX_FAILED_SWAPS
# failures — a binary 0/1 multiplier, no ramp.
MIN_SUCCESSFUL_SWAPS: int = 2
MAX_FAILED_SWAPS: int = 2

# ─── Rate-quality curve (Phase C) ────────────────────────
# One-sided clamp mapping realized-VWAP-vs-market advantage → a quality
# multiplier on the quality-volume reward. At/above market (within a tolerance
# deadband) scores 1.0 — the crown already rewards best-rate presence, so
# above-market is not paid again here. Worse-than-market ramps linearly to
# RATE_QUALITY_MIN at RATE_QUALITY_FLOOR_ADV. Tunable without code changes.
RATE_QUALITY_TOLERANCE_BPS = 100  # 1% deadband around the reference (spread/timing noise) → quality 1.0
# Tightened for C-rev: both the miner's number and the reference are now REALIZED
# clearing rates (they cluster tighter than posted rates), so a 5%-worse realized
# rate is already a strong signal worth zeroing the w_b slice. Tunable, flagged.
RATE_QUALITY_FLOOR_ADV = -0.05  # at 5% worse than the reference, quality bottoms out
RATE_QUALITY_MIN = 0.0  # floor multiplier once a rate is ≥ FLOOR_ADV worse than the reference

# ─── On-chain rate-quality reference (C-rev) ─────────────
# The reference the rate-quality curve compares a miner's realized rate against
# is computed on-chain-deterministically — a trimmed, volume-weighted,
# per-miner-capped average of completed-swap clearing rates per direction (read
# off the clearing_rates table). This replaces the external market feed: no
# network, no wall-clock-of-fetch, identical across validators given identical
# ingested history. The trim + per-miner cap are the wash-resistance — a wash
# farmer's outlier/self-swap rates get trimmed and capped, so they can't drag
# the reference. Below the min-swap floor the reference is undefined → quality
# degrades to neutral 1.0 (w_b then pays realized volume by raw share).
RATE_REFERENCE_WINDOW_SECS = 86400  # 24h — wide enough that sparse completed-swap history clears the floor
RATE_REFERENCE_TRIM_FRAC = 0.10  # drop the top & bottom 10% of weight before averaging
RATE_REFERENCE_MINER_CAP_FRAC = 0.25  # cap any single miner at 25% of total reference weight
RATE_REFERENCE_MIN_SWAPS = 5  # fewer positive-weight samples in-window ⇒ reference undefined ⇒ neutral

# ─── Collateral ──────────────────────────────────────────
# Collateral a miner must post to back a swap = sol_amount × this/10_000. Mirrors the contract's
# COLLATERAL_REQUIREMENT_BPS (constants.rs) — keep in sync. 11_000 = 1.10×.
COLLATERAL_REQUIREMENT_BPS = 11_000

# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID

# ─── Reservation ─────────────────────────────────────────
RESERVATION_COOLDOWN_BLOCKS = 150  # ~30 min base cooldown on failed reservation
RESERVATION_COOLDOWN_MULTIPLIER = 2  # 150 → 300 → 600 ...
MAX_RESERVATIONS_PER_ADDRESS = 1
# ─── Optimistic Extensions ───────────────────────────────
# Tunables for the propose/challenge/finalize extension flow. Per-chain timing
# (block time, confirmations) lives in allways/chains.py; the contract enforces
# its own MAX_EXTENSION_BLOCKS independently.
EXTENSION_PADDING_SECONDS = 120  # safety buffer on top of confirmation time
# Validator-view convergence: extension targets snap up to this native-seconds grid so validators
# computing `now + confirmation_runway` at slightly different wall-clock moments agree on one target_at.
# Seconds, never blocks (the deadline axis is unix-seconds); >= the slowest chain's block time so a
# bucket always spans at least one source block.
EXTENSION_BUCKET_SECONDS = 600  # 10 min
MAX_EXTENSION_BLOCKS = 250  # client-side cap, mirrors the contract's hard cap
# Mirrors the contract's CHALLENGE_WINDOW_BLOCKS — must stay in sync with
# smart-contracts/ink/lib.rs. Validators gate finalize calls on this locally
# to avoid known-doomed txs; the contract is authoritative.
CHALLENGE_WINDOW_BLOCKS = 8
# Conservative upper bound on subtensor blocks elapsed per validator forward
# step (base poll + per-step work + jitter). Used as a safety margin when
# sizing extension targets so a single delayed step doesn't strand a propose
# whose finalize window opens past the original reservation deadline.
# Default sized for mainnet; testnet validators with slower/jankier RPC can
# override via VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE env var.
DEFAULT_VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE = 5
VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE = max(
    1,
    int(
        os.environ.get(
            'VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE',
            DEFAULT_VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE,
        )
    ),
)
# Vote to extend when this many blocks remain: a forward step to land each of
# the propose and finalize txs, plus the challenge window between them.
EXTEND_THRESHOLD_BLOCKS = 2 * VALIDATOR_FORWARD_STEP_BLOCKS_ESTIMATE + CHALLENGE_WINDOW_BLOCKS

# Cushion the miner subtracts from each swap's timeout before agreeing to
# fulfill. Pinned to EXTEND_THRESHOLD_BLOCKS so the miner won't start a
# fulfill inside the window where validators can no longer land a propose +
# challenge before expiry. Not env-overridable: the right value is system-
# determined (validator extension runway), not operator preference. Edit
# this file directly if you need a different value.
MINER_TIMEOUT_CUSHION_BLOCKS = EXTEND_THRESHOLD_BLOCKS

# Tiered escalation. First extension fires on tx visibility alone (mempool
# OK) and buys time for one block; second extension requires ≥1 confirmation
# and buys the full chain-aware confirmation window. Hard cap is enforced
# contract-side via MAX_EXTENSIONS_PER_RESERVATION / _PER_SWAP — these client
# constants must mirror the contract values.
MAX_EXTENSIONS_PER_RESERVATION = 2
MAX_EXTENSIONS_PER_SWAP = 2

# ─── Protocol Fee ──────────────────────────────────────────
# Hardcoded 1% — matches the contract's immutable FEE_DIVISOR.
FEE_DIVISOR = 100

# ─── Display Only ─────────────────────────────────────────
# Fallbacks/defaults for CLI display. Live values are written by `alw admin`
# and read from the contract at runtime.
MIN_COLLATERAL_TAO = 0.1
DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS = 50  # ~10 min
DEFAULT_MIN_SWAP_AMOUNT_RAO = 100_000_000  # 0.1 TAO
DEFAULT_MAX_SWAP_AMOUNT_RAO = 500_000_000  # 0.5 TAO
RESERVATION_TTL_BLOCKS = 50  # ~10 min

# Blocks past a retained entry's last-known timeout_block before discard. Sized for the contract's worst case:
# MAX_EXTENSIONS_PER_SWAP extensions each push the deadline up to MAX_EXTENSION_BLOCKS further (not cumulative).
# A smaller margin can discard a still-active twice-extended swap and re-send on rediscovery (#461). ~550 ≈ 1.8h.
SENT_CACHE_DISCARD_MARGIN_BLOCKS = MAX_EXTENSIONS_PER_SWAP * MAX_EXTENSION_BLOCKS + DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS

# ─── Unix-axis miner runways (B4 — Solana) ────────────────
# The Solana swap deadline (`Swap.timeout_at`) is unix-seconds, not a block height. Port the two
# block-denominated miner runways to seconds via the subtensor block time so they keep the same
# wall-clock meaning. Edit these directly if validator extension cadence changes.
SECS_PER_BLOCK = 12
MINER_TIMEOUT_CUSHION_SECS = MINER_TIMEOUT_CUSHION_BLOCKS * SECS_PER_BLOCK
SENT_CACHE_DISCARD_MARGIN_SECS = SENT_CACHE_DISCARD_MARGIN_BLOCKS * SECS_PER_BLOCK
