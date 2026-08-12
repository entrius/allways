from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_POLUSDC


class PolUsdc(Erc20):
    """USDC on Polygon PoS — a config-row binding of the generic Erc20 (see chains.CHAIN_POLUSDC).

    Rides Polygon's env identity alongside native POL (POL_NETWORK, POL_RPC_URLS,
    POL_PRIVATE_KEY), adding only its own POLUSDC_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_POLUSDC)
