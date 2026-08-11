from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_ETHUSDC


class EthUsdc(Erc20):
    """USDC on Ethereum — a config-row binding of the generic Erc20 (see chains.CHAIN_ETHUSDC).

    First asset to land on an already-configured network: it shares Ethereum's env identity
    with native ETH (ETH_NETWORK, ETH_RPC_URLS, ETH_PRIVATE_KEY), adding only its own
    ETHUSDC_TOKEN_CONTRACT override."""

    def __init__(self):
        super().__init__(CHAIN_ETHUSDC)
