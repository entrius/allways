from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_QNT


class Qnt(Erc20):
    """Quant on Ethereum — a config-row binding of the generic Erc20 (see chains.CHAIN_QNT).

    Rides Ethereum's env identity alongside native ETH and ethusdc (ETH_NETWORK, ETH_RPC_URLS,
    ETH_PRIVATE_KEY), adding only its own QNT_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_QNT)
