from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Callable, Dict, Hashable, List, NamedTuple, Optional, Set, Tuple, Type

import bittensor as bt

from allways.assets.alpha import Alpha
from allways.assets.asset import Asset, MissingTestnetDeployment, SendResult, TransactionInfo
from allways.assets.btc import Bitcoin
from allways.assets.chain import Chain
from allways.assets.erc20 import Erc20
from allways.assets.evm import EvmChain
from allways.assets.evm_coin import EvmCoin
from allways.assets.sol import Sol, SolanaChain
from allways.assets.spl_token import SplToken
from allways.assets.tao import Tao, TaoChain
from allways.chains import (
    ALPHA_CHAINS,
    CHAIN_ARBUSDC,
    CHAIN_ASTER,
    CHAIN_AVAX,
    CHAIN_BASEUSDC,
    CHAIN_BNB,
    CHAIN_CRO,
    CHAIN_ETH,
    CHAIN_ETHUSDC,
    CHAIN_HYPE,
    CHAIN_PAXG,
    CHAIN_POL,
    CHAIN_POLUSDC,
    CHAIN_QNT,
    CHAIN_SOLUSDC,
    CHAIN_UNI,
)

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
    'Erc20',
    'SolanaChain',
    'SplToken',
    'Alpha',
    'asset_spec',
    'create_assets',
]


class AssetSpec(NamedTuple):
    """One registry row. The wire id is a "chain" (program strings, DB columns, /chains);
    in code it resolves to an Asset built as ``cls(**forwarded create_assets kwargs)``."""

    chain_id: str
    cls: Callable[..., Asset]  # a class, or a partial binding one to its ChainDefinition
    kwarg_names: Tuple[str, ...]  # create_assets kwargs this asset's constructor takes

    @property
    def asset_cls(self) -> Type[Asset]:
        """The class behind ``cls`` — itself, or the one a partial binds. For family checks."""
        return getattr(self.cls, 'func', self.cls)


ASSET_REGISTRY: Tuple[AssetSpec, ...] = (
    AssetSpec('btc', Bitcoin, ()),
    AssetSpec('tao', Tao, ('subtensor', 'wallet')),
    AssetSpec('sol', Sol, ('solana_rpc_url', 'solana_keypair')),
    # Every other asset is a family class bound to its chains.py row: the row holds the facts, the class the
    # behaviour, so adding a token or a coin on a supported network is a row here and a row there, no file.
    AssetSpec('eth', partial(EvmCoin, CHAIN_ETH), ()),
    AssetSpec('arbusdc', partial(Erc20, CHAIN_ARBUSDC), ()),
    AssetSpec('hype', partial(EvmCoin, CHAIN_HYPE), ()),
    AssetSpec('bnb', partial(EvmCoin, CHAIN_BNB), ()),
    AssetSpec('avax', partial(EvmCoin, CHAIN_AVAX), ()),
    AssetSpec('baseusdc', partial(Erc20, CHAIN_BASEUSDC), ()),
    AssetSpec('ethusdc', partial(Erc20, CHAIN_ETHUSDC), ()),
    AssetSpec('cro', partial(EvmCoin, CHAIN_CRO), ()),
    AssetSpec('aster', partial(Erc20, CHAIN_ASTER), ()),
    AssetSpec('uni', partial(Erc20, CHAIN_UNI), ()),
    AssetSpec('qnt', partial(Erc20, CHAIN_QNT), ()),
    AssetSpec('pol', partial(EvmCoin, CHAIN_POL), ()),
    AssetSpec('polusdc', partial(Erc20, CHAIN_POLUSDC), ()),
    AssetSpec('paxg', partial(Erc20, CHAIN_PAXG), ()),
    AssetSpec('solusdc', partial(SplToken, CHAIN_SOLUSDC), ('solana_rpc_url', 'solana_keypair')),
    # One row per subnet alpha, bound the same way: same class, different netuid.
    *(
        AssetSpec(chain_id, partial(Alpha, chain_def), ('subtensor', 'wallet'))
        for chain_id, chain_def in ALPHA_CHAINS.items()
    ),
)


# Startup checks in flight at once, the calling thread included. Enough that a boot stops waiting on one backend
# after another (~15s serial on mainnet); few enough that the EVM networks one RPC vendor serves under different
# hostnames don't add up to a burst on it.
MAX_CHECK_LANES = 4


def asset_spec(chain_id: str) -> Optional[AssetSpec]:
    """The registry row for ``chain_id``, or None when no asset has that id. Build it with ``spec.cls(...)``."""
    return next((spec for spec in ASSET_REGISTRY if spec.chain_id == chain_id), None)


def check_lane(provider: Asset) -> Hashable:
    """The backend a provider's startup check talks to. Checks on one lane run one after another, in registry
    order; only different lanes overlap. One lane per chain class, split per network for EVM, so no backend is
    asked two things at once — the TAO provider and every alpha share one ``TaoChain`` and so the subtensor's lane."""
    chain = provider.chain
    if isinstance(chain, TaoChain):
        return TaoChain
    if isinstance(chain, EvmChain):
        return EvmChain, chain.env_prefix
    return type(chain)


def run_checks(pending: List[Tuple[int, Asset]], require_send: bool) -> Dict[int, Exception]:
    """Run each provider's ``check_connection``; returns the failures by registry position.

    Backends are independent, so their lanes overlap. The subtensor's lane runs on the calling thread: a keyed
    endpoint allows ~10 requests/s and a boot's burst of reads has throttled it before (see utils/subtensor.py),
    so the neuron's subtensor keeps one caller, on the thread that built it. A failure is returned, never raised,
    so which one fails the boot is the caller's choice and not whichever lane lost the race.
    """
    lanes: Dict[Hashable, List[Tuple[int, Asset]]] = {}
    for position, provider in pending:
        lanes.setdefault(check_lane(provider), []).append((position, provider))

    def run_lane(lane: List[Tuple[int, Asset]]) -> Dict[int, Exception]:
        # One probe per backend, shared down the lane: the TAO provider and every alpha (~128) ride one subtensor,
        # and several assets sit on one EVM network. Owned by the lane, so no other thread touches it.
        checked: Dict[Hashable, Optional[Exception]] = {}
        failed: Dict[int, Exception] = {}
        for position, provider in lane:
            try:
                provider.check_connection(require_send=require_send, checked=checked)
            except Exception as e:
                failed[position] = e
        return failed

    own_lane = lanes.pop(TaoChain, [])
    if not lanes:
        return run_lane(own_lane)
    workers = min(MAX_CHECK_LANES - 1, len(lanes))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='asset-check') as pool:
        futures = [pool.submit(run_lane, lane) for lane in lanes.values()]
        failed = run_lane(own_lane)
        for future in futures:
            failed.update(future.result())
    return failed


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

    def admit(chain_id: str, provider: Optional[Asset], error: Optional[Exception]) -> None:
        required = required_chains is None or chain_id in required_chains
        if error is None:
            providers[chain_id] = provider
        elif isinstance(error, MissingTestnetDeployment):
            # A spoke with no deployment on this test network can't exist here at all, so it
            # disables rather than failing the boot. A miner that quotes the pair (explicitly
            # in required_chains) still fails hard — it must not advertise what it can't serve.
            if check and required_chains is not None and chain_id in required_chains:
                raise RuntimeError(f'{chain_id} failed startup check: {error}') from error
            bt.logging.warning(f'{chain_id} disabled on this network: {error} — its pairs are unavailable here')
        else:
            if check and required:
                raise RuntimeError(f'{chain_id} failed startup check: {error}') from error
            bt.logging.warning(
                f'{chain_id} disabled: {error}'
                + (f' — {chain_id}-pair swaps cannot be fulfilled until this is fixed' if check else '')
            )

    # Constructors stay on the calling thread, in registry order; only the checks fan out.
    built: List[Tuple[str, Optional[Asset], Optional[Exception]]] = []
    shared: Dict[Hashable, Chain] = {}  # one chain object per network, bound across every asset on it
    for chain_id, cls, kwarg_names in ASSET_REGISTRY:
        provider: Optional[Asset] = None
        error: Optional[Exception] = None
        try:
            provider_kwargs = {k: kwargs[k] for k in kwarg_names if k in kwargs}
            provider = cls(**provider_kwargs)
            provider.share_chain(shared)  # a missing Chain binding fails here at boot, not mid-pass
        except Exception as e:
            provider, error = None, e
        if check:
            built.append((chain_id, provider, error))
        else:
            admit(chain_id, provider, error)

    if check:
        failed = run_checks([(i, row[1]) for i, row in enumerate(built) if row[1] is not None], require_send)
        # Admitted in registry order whatever order the lanes finished in: the providers dict keeps its order and,
        # when several chains fail, the one that fails the boot is the first in the registry — as it was when the
        # checks ran one at a time.
        for position, (chain_id, provider, error) in enumerate(built):
            admit(chain_id, provider, failed.get(position) if error is None else error)

    if check and providers:
        alphas = [c for c in providers if c.startswith('sn') and c[2:].isdigit()]
        others = [c for c in providers if c not in alphas]
        bt.logging.info(
            f'Chain providers ready: {", ".join(others)}' + (f' + {len(alphas)} subnet alphas' if alphas else '')
        )
        for chain_id, provider in providers.items():
            bt.logging.debug(f'  {chain_id} → {provider.describe()}')

    return providers
