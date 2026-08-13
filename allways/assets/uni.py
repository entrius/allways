from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_UNI


class Uni(Erc20):
    """UNI on Ethereum — a config-row binding of the generic Erc20 (see chains.CHAIN_UNI).

    Third asset on Ethereum's network identity (ETH_NETWORK, ETH_RPC_URLS, ETH_PRIVATE_KEY),
    adding only its own UNI_TOKEN_CONTRACT override. Uni.sol has no issuer freeze surface,
    which the registry row declares rather than the provider probing for it."""

    def __init__(self):
        super().__init__(CHAIN_UNI)
