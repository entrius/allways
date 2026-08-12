from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_ASTER


class Aster(Erc20):
    """ASTER on BNB Smart Chain — a config-row binding of the generic Erc20 (see chains.CHAIN_ASTER).

    Rides BSC's env identity alongside native BNB (BNB_NETWORK, BNB_RPC_URLS, BNB_PRIVATE_KEY),
    adding only its own ASTER_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_ASTER)
