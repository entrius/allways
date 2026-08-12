from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_POL


class Pol(EvmCoin):
    """POL on Polygon PoS — a config-row binding of the generic EvmCoin (see chains.CHAIN_POL).

    Polygon speaks plain JSON-RPC, so nothing here is chain-specific beyond the pairing."""

    def __init__(self):
        super().__init__(CHAIN_POL)
