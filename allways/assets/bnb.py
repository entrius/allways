from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_BNB


class Bnb(EvmCoin):
    """BNB on BNB Smart Chain — a config-row binding of the generic EvmCoin (see chains.CHAIN_BNB).

    BSC speaks plain JSON-RPC, so nothing here is chain-specific beyond the pairing."""

    def __init__(self):
        super().__init__(CHAIN_BNB)
