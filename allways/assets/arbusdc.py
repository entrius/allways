from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_ARBUSDC


class ArbUsdc(Erc20):
    """USDC on Arbitrum — a config-row binding of the generic Erc20 (see chains.CHAIN_ARBUSDC)."""

    def __init__(self):
        super().__init__(CHAIN_ARBUSDC)
