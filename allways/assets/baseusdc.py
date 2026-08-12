from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_BASEUSDC


class BaseUsdc(Erc20):
    """Native Circle USDC on Base — a config-row binding of the generic Erc20 (see chains.CHAIN_BASEUSDC)."""

    def __init__(self):
        super().__init__(CHAIN_BASEUSDC)
