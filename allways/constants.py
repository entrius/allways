NETUID_FINNEY = 7

# The treasury miner. TREASURY_UID is set once the treasury miner is registered on SN7;
# until then (None) every validator sends its full weight to BURN_UID.
TREASURY_UID: int | None = None
# Guard only: the UID is what is pinned. A recycled slot holds another hotkey, and falls back to BURN_UID.
TREASURY_HOTKEY = '5EvkLKgQpLbsvZViBEJsjA4utMjjRcxRXWPVrwhzLfJnFJrs'

# SN7 subnet owner hotkey's UID: weight placed here is burned by the chain.
BURN_UID = 53

# Blocks between weight sets (the chain's weights_rate_limit is honoured on top of this).
EPOCH_LENGTH_BLOCKS = 150
# Seconds between checks of whether weights are due.
POLL_INTERVAL_SECONDS = 60
