from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_ETH


class Ether(EvmCoin):
    """ETH on Ethereum — a config-row binding of the generic EvmCoin (see chains.CHAIN_ETH).

    Behaviour lives in `EvmCoin`; this file is only the network/registry pairing, the
    same shape every native EVM coin takes."""

    def __init__(self):
        super().__init__(CHAIN_ETH)
