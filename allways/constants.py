# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7
NETUID_LOCAL = 2

# ─── Contract ──────────────────────────────────────────────
# Default mainnet address; override via CONTRACT_ADDRESS env var for testnets
# or alternate deployments.
CONTRACT_ADDRESS = '5FTkUEhRmLPsALn4b7bJpVFhDQqohGbc6khnmA2aiYFLMZYP'

# ─── Polling ──────────────────────────────────────────────
# Bittensor base neuron loop heartbeat — not the scoring / forward cadence.
MINER_POLL_INTERVAL_SECONDS = 12
VALIDATOR_POLL_INTERVAL_SECONDS = 12

# ─── Commitment Format ────────────────────────────────────
COMMITMENT_VERSION = 1

# ─── Unit Conversions ────────────────────────────────────
TAO_TO_RAO = 1_000_000_000  # 1 TAO = 10^9 rao
BTC_TO_SAT = 100_000_000  # 1 BTC = 10^8 satoshi

# ─── Rate Encoding ───────────────────────────────────────
RATE_PRECISION = 10**18  # Fixed-point precision for on-chain rate storage

# ─── Transaction Fees ────────────────────────────────────
MIN_BALANCE_FOR_TX_RAO = 250_000_000  # 0.25 TAO minimum for extrinsic fees
BTC_MIN_FEE_RATE = 2  # sat/vB — minimum BTC fee rate floor to avoid stuck txs

# ─── Miner ───────────────────────────────────────────────
# Default cushion the miner applies to every swap's timeout_block before
# deciding to fulfill. Protects against slow dest-chain inclusion eating into
# the timeout window. Overridable via MINER_TIMEOUT_CUSHION_BLOCKS env var.
DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS = 5

# ─── Scoring ─────────────────────────────────────────────
SCORING_WINDOW_BLOCKS = 3600  # ~12 hours at 12s/block
SCORING_INTERVAL_STEPS = 300  # Score every 300 forward passes (~1 hour at 12s poll)
SCORING_EMA_ALPHA = 1.0  # Instantaneous — score based on current window only, no smoothing

# ─── V1 Crown-Time Scoring ───────────────────────────────
# Rate/collateral event retention. Must be >= SCORING_WINDOW_BLOCKS so the
# window-start state can always be reconstructed from history.
EVENT_RETENTION_BLOCKS = 2 * SCORING_WINDOW_BLOCKS
# Emission allocation per swap direction. Sum of values is the portion of each
# scoring pass allocated to crown-time winners; 1 - sum() recycles to RECYCLE_UID.
DIRECTION_POOLS: dict[tuple[str, str], float] = {
    ('tao', 'btc'): 0.04,
    ('btc', 'tao'): 0.04,
}
# Harsh penalty for unreliable miners: success_rate ** SUCCESS_EXPONENT.
# 100% → 1.0, 90% → 0.729, 80% → 0.512, 50% → 0.125.
SUCCESS_EXPONENT: int = 3

# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID — emissions recycled on-chain

# ─── Reservation ─────────────────────────────────────────
RESERVATION_COOLDOWN_BLOCKS = 150  # ~30 min base cooldown on failed reservation (validator-enforced)
RESERVATION_COOLDOWN_MULTIPLIER = 2  # Exponential backoff: 150 → 300 → 600 ...
MAX_RESERVATIONS_PER_ADDRESS = 1  # 1 active reservation per source address (validator-enforced)
EXTEND_THRESHOLD_BLOCKS = 20  # ~4 min — vote to extend reservation when this many blocks remain

# ─── Protocol Fee ──────────────────────────────────────────
# Hardcoded 1% protocol fee matching the smart contract's immutable
# FEE_DIVISOR constant. No longer read from chain — both sides pin to 100.
FEE_DIVISOR = 100

# ─── Display Only (real values enforced on-chain by contract) ─────
# For CLI display and fallback logic only. Actual values are managed
# via `alw admin` commands and read from the contract at runtime.
MIN_COLLATERAL_TAO = 0.1  # Fallback when the contract min_collateral read fails
DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS = 30  # ~5 min — `alw admin set-timeout`
DEFAULT_MIN_SWAP_AMOUNT_RAO = 100_000_000  # 0.1 TAO — `alw admin set-min-swap`
DEFAULT_MAX_SWAP_AMOUNT_RAO = 500_000_000  # 0.5 TAO   — `alw admin set-max-swap`
RESERVATION_TTL_BLOCKS = 30  # ~5 min — `alw admin set-reservation-ttl`
