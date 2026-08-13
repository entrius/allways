from allways.assets.erc20 import Erc20
from allways.chains import CHAIN_PAXG


class Paxg(Erc20):
    """PAX Gold on Ethereum — a config-row binding of the generic Erc20 (see chains.CHAIN_PAXG).

    Shares Ethereum's env identity with eth/ethusdc (ETH_NETWORK, ETH_RPC_URLS, ETH_PRIVATE_KEY),
    adding only its own PAXG_TOKEN_CONTRACT override. Paxos freezes with isFrozen, not Circle's
    isBlacklisted — the registry row declares which, so the provider never probes to find out."""

    def __init__(self):
        super().__init__(CHAIN_PAXG)
