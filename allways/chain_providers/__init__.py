from typing import Dict, Tuple, Type

import bittensor as bt

from allways.chain_providers.base import ChainProvider, TransactionInfo
from allways.chain_providers.bitcoin import BitcoinProvider
from allways.chain_providers.subtensor import SubtensorProvider

__all__ = ['ChainProvider', 'TransactionInfo', 'BitcoinProvider', 'SubtensorProvider', 'create_chain_providers']

# Registry: (chain_id, provider_class, kwarg names to forward)
PROVIDER_REGISTRY: Tuple[Tuple[str, Type[ChainProvider], Tuple[str, ...]], ...] = (
    ('btc', BitcoinProvider, ()),
    ('tao', SubtensorProvider, ('subtensor',)),
)


def create_chain_providers(check: bool = False, require_send: bool = True, **kwargs) -> Dict[str, ChainProvider]:
    """Initialize all available chain providers.

    Args:
        check: If True, verify each provider can reach its backend on init.
               Raises RuntimeError on failure.
        require_send: If False, skip validation of send credentials (e.g.
                      BTC_PRIVATE_KEY) during check. Validators only need
                      read/verify access so they pass require_send=False.

    Keyword arguments are forwarded to providers that need them.
    e.g. create_chain_providers(subtensor=subtensor)
    """
    providers: Dict[str, ChainProvider] = {}

    for chain_id, cls, kwarg_names in PROVIDER_REGISTRY:
        try:
            provider_kwargs = {k: kwargs[k] for k in kwarg_names if k in kwargs}
            provider = cls(**provider_kwargs)
            if check:
                provider.check_connection(require_send=require_send)
            providers[chain_id] = provider
        except Exception as e:
            if check:
                raise RuntimeError(f'{cls.__name__} failed startup check: {e}') from e
            bt.logging.warning(f'{cls.__name__} not available: {e}')

    return providers
