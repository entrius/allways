import os

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
SCORING_WINDOW_BLOCKS = 600  # ~2 hours at 12s/block — scoring cadence and window width
# Cap on how far back one scoring round replays after a stall: just resume a
# couple hours back, don't try to recover a long outage (the event-watcher only
# reconstructs ~one window back anyway). >1 window so overshoot can't re-gap.
MAX_SCORING_BACKFILL_BLOCKS = 2 * SCORING_WINDOW_BLOCKS  # ~4 hours at 12s/block
SCORING_EMA_ALPHA = 1.0  # Instantaneous — no smoothing across passes
CREDIBILITY_WINDOW_BLOCKS = 216_000  # ~30 days
DIRECTION_POOLS: dict[tuple[str, str], float] = {
    ('tao', 'btc'): 0.5,
    ('btc', 'tao'): 0.5,
}
# 100% → 1.0, 90% → 0.729, 80% → 0.512, 50% → 0.125
SUCCESS_EXPONENT: int = 3
# Idle-crown penalty: 0 = none, 1 = pure volume share, 0.5 = half-credit floor.
VOLUME_WEIGHT_ALPHA: float = 0.5
# Closed swaps required for full credibility (0 → 100% linear ramp).
CREDIBILITY_RAMP_OBSERVATIONS: int = 10

# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID

# ─── Reservation ─────────────────────────────────────────
RESERVE_SLIPPAGE_DEFAULT_BPS = 200  # 2% — applied when the synapse omits slippage
RESERVE_SLIPPAGE_MAX_BPS = 2_500  # 25% — clamp ceiling; ≥10_000 turns the rate gate into a no-op
RESERVATION_COOLDOWN_BLOCKS = 150  # ~30 min base cooldown on failed reservation
RESERVATION_COOLDOWN_MULTIPLIER = 2  # 150 → 300 → 600 ...
MAX_RESERVATIONS_PER_ADDRESS = 1
# ─── Optimistic Extensions ───────────────────────────────
# Tunables for the propose/challenge/finalize extension flow. Per-chain timing
# (block time, confirmations) lives in allways/chains.py; the contract enforces
# its own MAX_EXTENSION_BLOCKS independently.
EXTENSION_PADDING_SECONDS = 300  # safety buffer on top of confirmation time
EXTENSION_BUCKET_BLOCKS = 30  # round target up so validator views converge
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
