# ─── Network ───────────────────────────────────────────────
NETUID_FINNEY = 7
NETUID_LOCAL = 2

# ─── Contract ──────────────────────────────────────────────

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
