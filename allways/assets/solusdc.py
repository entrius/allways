from allways.assets.spl_token import SplToken
from allways.chains import CHAIN_SOLUSDC


class SolUsdc(SplToken):
    """Circle USDC on Solana — a config-row binding of the generic SplToken (see chains.CHAIN_SOLUSDC).
    Rides the Solana env identity (SOLANA_RPC_URL, the Solana keypair) beside native SOL, adding only
    its own SOLUSDC_TOKEN_MINT override."""

    def __init__(self, solana_rpc_url=None, solana_keypair=None):
        super().__init__(CHAIN_SOLUSDC, solana_rpc_url=solana_rpc_url, solana_keypair=solana_keypair)
