NETUID_FINNEY = 7

# The treasury miner on SN7 mainnet. TREASURY_UID is set once the treasury miner is registered;
# until then (None) every validator sends its full weight to the subnet owner's UID, which burns it.
TREASURY_UID: int | None = None
# Guard only: the UID is what is pinned. A recycled slot holds another hotkey, and the weight is burned instead.
TREASURY_HOTKEY = '5EvkLKgQpLbsvZViBEJsjA4utMjjRcxRXWPVrwhzLfJnFJrs'

# Blocks between weight sets (the chain's weights_rate_limit is honoured on top of this).
EPOCH_LENGTH_BLOCKS = 150
# Seconds between checks of whether weights are due.
POLL_INTERVAL_SECONDS = 60
