from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_HYPE


class Hype(EvmCoin):
    """HYPE on HyperEVM — a config-row binding of the generic EvmCoin (see chains.CHAIN_HYPE).

    HyperEVM speaks plain JSON-RPC, so nothing here is chain-specific beyond the pairing."""

    def __init__(self):
        super().__init__(CHAIN_HYPE)
