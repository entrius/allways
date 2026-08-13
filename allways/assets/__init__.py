from typing import Dict, NamedTuple, Optional, Set, Tuple, Type

import bittensor as bt

from allways.assets.arbusdc import ArbUsdc
from allways.assets.asset import Asset, SendResult, TransactionInfo
from allways.assets.aster import Aster
from allways.assets.avax import Avax
from allways.assets.baseusdc import BaseUsdc
from allways.assets.bnb import Bnb
from allways.assets.btc import Bitcoin
from allways.assets.chain import Chain
from allways.assets.cro import Cro
from allways.assets.erc20 import Erc20
from allways.assets.eth import Ether
from allways.assets.ethusdc import EthUsdc
from allways.assets.evm_coin import EvmCoin
from allways.assets.hype import Hype
from allways.assets.pol import Pol
from allways.assets.qnt import Qnt
from allways.assets.sol import Sol
from allways.assets.tao import Tao
from allways.assets.uni import Uni

__all__ = [
    'Asset',
    'AssetSpec',
    'Chain',
    'SendResult',
    'TransactionInfo',
    'Bitcoin',
    'Tao',
    'Sol',
    'EvmCoin',
    'Ether',
    'Hype',
    'Bnb',
    'Avax',
    'Cro',
    'Pol',
    'Erc20',
    'ArbUsdc',
    'BaseUsdc',
    'EthUsdc',
    'Aster',
    'Uni',
    'Qnt',
    'create_assets',
]


class AssetSpec(NamedTuple):
    """One registry row. The wire id is a "chain" (program strings, DB columns, /chains);
    in code it resolves to an Asset built as ``cls(**forwarded create_assets kwargs)``."""

    chain_id: str
    cls: Type[Asset]
    kwarg_names: Tuple[str, ...]  # create_assets kwargs this asset's constructor takes


ASSET_REGISTRY: Tuple[AssetSpec, ...] = (
    AssetSpec('btc', Bitcoin, ()),
    AssetSpec('tao', Tao, ('subtensor', 'wallet')),
    AssetSpec('sol', Sol, ('solana_rpc_url', 'solana_keypair')),
    AssetSpec('eth', Ether, ()),
    AssetSpec('arbusdc', ArbUsdc, ()),
    AssetSpec('hype', Hype, ()),
    AssetSpec('bnb', Bnb, ()),
    AssetSpec('avax', Avax, ()),
    AssetSpec('baseusdc', BaseUsdc, ()),
    AssetSpec('ethusdc', EthUsdc, ()),
    AssetSpec('cro', Cro, ()),
    AssetSpec('aster', Aster, ()),
    AssetSpec('uni', Uni, ()),
    AssetSpec('qnt', Qnt, ()),
    AssetSpec('pol', Pol, ()),
)


def create_assets(
    check: bool = False,
    require_send: bool = True,
    required_chains: Optional[Set[str]] = None,
    **kwargs,
) -> Dict[str, Asset]:
    """Initialize all available chain providers.

    Args:
        check: If True, verify each provider can reach its backend on init.
               Raises RuntimeError on failure.
        require_send: If False, skip validation of send credentials (e.g.
                      BTC_PRIVATE_KEY) during check. Validators only need
                      read/verify access so they pass require_send=False.
        required_chains: Chains whose provider MUST pass the check — others
                         degrade to a warning and are left out (a tao<->sol
                         miner must not need BTC credentials). None = all
                         chains required (validators verify every chain).

    Keyword arguments are forwarded to providers that need them.
    e.g. create_assets(subtensor=subtensor)
    """
    providers: Dict[str, Asset] = {}

    for chain_id, cls, kwarg_names in ASSET_REGISTRY:
        required = required_chains is None or chain_id in required_chains
        try:
            provider_kwargs = {k: kwargs[k] for k in kwarg_names if k in kwargs}
            provider = cls(**provider_kwargs)
            provider.chain  # a missing Chain binding fails here at boot, not mid-pass
            if check:
                provider.check_connection(require_send=require_send)
            providers[chain_id] = provider
        except Exception as e:
            if check and required:
                raise RuntimeError(f'{cls.__name__} failed startup check: {e}') from e
            bt.logging.warning(
                f'{cls.__name__} disabled: {e}'
                + (f' — {chain_id}-pair swaps cannot be fulfilled until this is fixed' if check else '')
            )

    if check and providers:
        bt.logging.info('Chain providers ready:')
        for chain_id, provider in providers.items():
            bt.logging.info(f'  {chain_id} → {provider.describe()}')

    return providers
