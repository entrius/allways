from allways.assets.evm_coin import EvmCoin
from allways.chains import CHAIN_AVAX


class Avax(EvmCoin):
    """AVAX on the Avalanche C-Chain — a config-row binding of the generic EvmCoin (see chains.CHAIN_AVAX).

    The C-Chain speaks plain JSON-RPC. Its atomic P/X-chain imports credit a balance outside the EVM
    tx set entirely, so they are invisible here — a swap leg has to be an ordinary EVM transfer."""

    def __init__(self):
        super().__init__(CHAIN_AVAX)
