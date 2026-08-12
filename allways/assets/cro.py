from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_CRO


class Cro(EvmCoin):
    """CRO on Cronos — a config-row binding of the generic EvmCoin (see chains.CHAIN_CRO).

    Cronos is an Ethermint EVM behind plain JSON-RPC, so nothing here is chain-specific
    beyond the pairing."""

    def __init__(self):
        super().__init__(CHAIN_CRO)
