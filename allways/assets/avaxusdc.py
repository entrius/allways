from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_AVAXUSDC


class AvaxUsdc(Erc20):
    """USDC on Avalanche C-Chain — a config-row binding of the generic Erc20 (see chains.CHAIN_AVAXUSDC).

    Rides Avalanche's env identity alongside native AVAX (AVAX_NETWORK, AVAX_RPC_URLS,
    AVAX_PRIVATE_KEY), adding only its own AVAXUSDC_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_AVAXUSDC)
