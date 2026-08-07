from typing import Dict, Optional, Set, Tuple, Type

import bittensor as bt

from allways.assets.base import Asset, TransactionInfo
from allways.assets.bitcoin import Bitcoin
from allways.assets.chain import Chain
from allways.assets.ethereum import Ether
from allways.assets.solana import Sol
from allways.assets.subtensor import Tao

__all__ = [
    'Asset',
    'Chain',
    'TransactionInfo',
    'Bitcoin',
    'Tao',
    'Sol',
    'Ether',
    'create_assets',
]

# Registry: (chain_id, asset class, kwarg names to forward). The wire id is a "chain"
# (program strings, DB columns, /chains); in code it resolves to an Asset.
ASSET_REGISTRY: Tuple[Tuple[str, Type[Asset], Tuple[str, ...]], ...] = (
    ('btc', Bitcoin, ()),
    ('tao', Tao, ('subtensor', 'wallet')),
    ('sol', Sol, ('solana_rpc_url', 'solana_keypair')),
    ('eth', Ether, ()),
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
