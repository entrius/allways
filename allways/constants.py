NETUID_FINNEY = 7

# The treasury miner on SN7 mainnet (registered at block 9526550). Every validator sends its full weight here.
# Set to None to send the full weight to the subnet owner's UID instead, which burns it.
TREASURY_UID: int | None = 78
# Guard only: the UID is what is pinned. A recycled slot holds another hotkey, and the weight is burned instead.
TREASURY_HOTKEY = '5CPAcJSvWzeGJqeAfgYPbVBYGqN3DT6Rt7QK85US5u1aCGYW'

# Blocks between weight sets (the chain's weights_rate_limit is honoured on top of this).
EPOCH_LENGTH_BLOCKS = 150
# Seconds between checks of whether weights are due.
POLL_INTERVAL_SECONDS = 60
