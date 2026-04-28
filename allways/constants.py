# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7
NETUID_LOCAL = 2

# ─── Contract ──────────────────────────────────────────────
# Mainnet default; override via CONTRACT_ADDRESS env var.
CONTRACT_ADDRESS = '5FTkUEhRmLPsALn4b7bJpVFhDQqohGbc6khnmA2aiYFLMZYP'

# ─── Polling ──────────────────────────────────────────────
# Bittensor base-neuron heartbeat, not the scoring/forward cadence.
MINER_POLL_INTERVAL_SECONDS = 12
VALIDATOR_POLL_INTERVAL_SECONDS = 12

# ─── Commitment Format ────────────────────────────────────
COMMITMENT_VERSION = 1

# ─── Unit Conversions ────────────────────────────────────
TAO_TO_RAO = 1_000_000_000
BTC_TO_SAT = 100_000_000

# ─── Rate Encoding ───────────────────────────────────────
RATE_PRECISION = 10**18

# ─── Transaction Fees ────────────────────────────────────
MIN_BALANCE_FOR_TX_RAO = 250_000_000  # 0.25 TAO minimum for extrinsic fees
BTC_MIN_FEE_RATE = 2  # sat/vB — floor to avoid stuck txs

# ─── Miner ───────────────────────────────────────────────
# Cushion subtracted from each swap's timeout before the miner agrees to
# fulfill, protecting against slow dest-chain inclusion. Overridable via
# MINER_TIMEOUT_CUSHION_BLOCKS.
DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS = 5

# ─── Scoring ─────────────────────────────────────────────
SCORING_WINDOW_BLOCKS = 1200  # ~4 hours at 12s/block — also the scoring cadence
SCORING_EMA_ALPHA = 1.0  # Instantaneous — no smoothing across passes
CREDIBILITY_WINDOW_BLOCKS = 216_000  # ~30 days
DIRECTION_POOLS: dict[tuple[str, str], float] = {
    ('tao', 'btc'): 0.04,
    ('btc', 'tao'): 0.04,
}
# 100% → 1.0, 90% → 0.729, 80% → 0.512, 50% → 0.125
SUCCESS_EXPONENT: int = 3

# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID

# ─── Reservation ─────────────────────────────────────────
RESERVATION_COOLDOWN_BLOCKS = 150  # ~30 min base cooldown on failed reservation
RESERVATION_COOLDOWN_MULTIPLIER = 2  # 150 → 300 → 600 ...
MAX_RESERVATIONS_PER_ADDRESS = 1
EXTEND_THRESHOLD_BLOCKS = 20  # ~4 min — vote to extend when this many blocks remain
# A user's tx is often invisible to a validator's RPC for the first few seconds
# after submission (mempool propagation lag, regional RPC differences). Treat
# "not found" as transient until the same entry has polled null this many times.
PENDING_CONFIRM_NULL_RETRY_LIMIT = 3

# ─── Optimistic Extensions ───────────────────────────────
# Tunables for the propose/challenge/finalize extension flow. Per-chain timing
# (block time, confirmations) lives in allways/chains.py; the contract enforces
# its own MAX_EXTENSION_BLOCKS independently.
EXTENSION_PADDING_SECONDS = 300  # safety buffer on top of confirmation time
EXTENSION_BUCKET_BLOCKS = 30  # round target up so validator views converge
MAX_EXTENSION_BLOCKS = 250  # client-side cap, mirrors the contract's hard cap

# ─── Protocol Fee ──────────────────────────────────────────
# Hardcoded 1% — matches the contract's immutable FEE_DIVISOR.
FEE_DIVISOR = 100

# ─── Display Only ─────────────────────────────────────────
# Fallbacks/defaults for CLI display. Live values are written by `alw admin`
# and read from the contract at runtime.
MIN_COLLATERAL_TAO = 0.1
DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS = 30  # ~5 min
DEFAULT_MIN_SWAP_AMOUNT_RAO = 100_000_000  # 0.1 TAO
DEFAULT_MAX_SWAP_AMOUNT_RAO = 500_000_000  # 0.5 TAO
RESERVATION_TTL_BLOCKS = 30  # ~5 min
