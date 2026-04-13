# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7
NETUID_LOCAL = 2

# ─── Contract ──────────────────────────────────────────────
# testnet
# CONTRACT_ADDRESS = '5GhkGErFdX5yYfwf6HJnAAXphsPJubPxrjUMne3sjM95oi1h'

# mainnet
CONTRACT_ADDRESS = '5FTkUEhRmLPsALn4b7bJpVFhDQqohGbc6khnmA2aiYFLMZYP'

# ─── Polling ──────────────────────────────────────────────
MINER_POLL_INTERVAL_SECONDS = 12  # Match Bittensor block time for lowest latency
VALIDATOR_POLL_INTERVAL_SECONDS = 12  # Match Bittensor block time for lowest latency

# ─── Commitment Format ────────────────────────────────────
COMMITMENT_VERSION = 1
COMMITMENT_REVEAL_BLOCKS = 360  # ~72 min at 12s/block

# ─── Unit Conversions ────────────────────────────────────
TAO_TO_RAO = 1_000_000_000  # 1 TAO = 10^9 rao
BTC_TO_SAT = 100_000_000  # 1 BTC = 10^8 satoshi

# ─── Rate Encoding ───────────────────────────────────────
RATE_PRECISION = 10**18  # Fixed-point precision for on-chain rate storage

# ─── Transaction Fees ────────────────────────────────────
MIN_BALANCE_FOR_TX_RAO = 250_000_000  # 0.25 TAO minimum for extrinsic fees
BTC_MIN_FEE_RATE = 2  # sat/vB — minimum BTC fee rate floor to avoid stuck txs

# ─── Miner Status ────────────────────────────────────────
MINER_STATUS_LOG_INTERVAL_STEPS = 50  # Full status log every ~10 min at 12s poll

# ─── Scoring ─────────────────────────────────────────────
SCORING_WINDOW_BLOCKS = 3600  # ~12 hours at 12s/block
SCORING_INTERVAL_STEPS = 300  # Score every 300 forward passes (~1 hour at 12s poll)
SCORING_EMA_ALPHA = 1.0  # Instantaneous — score based on current window only, no smoothing
SCORING_SUCCESS_EXPONENT = 8  # Harsh failure penalty: 92% → 0.51x, 96% → 0.72x

# ─── Emission Recycling ────────────────────────────────────
RECYCLE_UID = 53  # Subnet owner UID — emissions recycled on-chain
DAILY_EMISSION_ALPHA = 7200 * 0.41  # 2952 alpha/day (7200 blocks/day * 0.41 miner share)

# ─── Reservation ─────────────────────────────────────────
RESERVATION_COOLDOWN_BLOCKS = 150  # ~30 min base cooldown on failed reservation (validator-enforced)
RESERVATION_COOLDOWN_MULTIPLIER = 2  # Exponential backoff: 150 → 300 → 600 ...
MAX_RESERVATIONS_PER_ADDRESS = 1  # 1 active reservation per source address (validator-enforced)
EXTEND_THRESHOLD_BLOCKS = 20  # ~4 min — vote to extend reservation when this many blocks remain

# ─── Display Only (real values enforced on-chain by contract) ─────
# For CLI display and fallback logic only. Actual values are managed
# via `alw admin` commands and read from the contract at runtime.
DEFAULT_FEE_DIVISOR = 100  # tao_amount / fee_divisor — read from contract, fallback here
SWAP_FEE_PERCENT = 0.01  # display only — derived from DEFAULT_FEE_DIVISOR
MAX_FEE_PERCENT = 0.05  # contract enforces divisor >= 20 (max 5% fee)
MIN_COLLATERAL_TAO = 0.1  # Must be > max swap amount
DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS = 30  # ~5 min — `alw admin set-timeout`
DEFAULT_MIN_SWAP_AMOUNT_RAO = 100_000_000  # 0.1 TAO — `alw admin set-min-swap`
DEFAULT_MAX_SWAP_AMOUNT_RAO = 500_000_000  # 0.5 TAO   — `alw admin set-max-swap`
RESERVATION_TTL_BLOCKS = 30  # ~5 min — `alw admin set-reservation-ttl`
