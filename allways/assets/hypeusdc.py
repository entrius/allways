from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_HYPEUSDC


class HypeUsdc(Erc20):
    """USDC on HyperEVM — a config-row binding of the generic Erc20 (see chains.CHAIN_HYPEUSDC).

    Rides HyperEVM's env identity alongside native HYPE (HYPE_NETWORK, HYPE_RPC_URLS,
    HYPE_PRIVATE_KEY), adding only its own HYPEUSDC_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_HYPEUSDC)
