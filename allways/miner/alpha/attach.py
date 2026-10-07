"""Attach alpha (subnet-token) delivery to a running base miner — the one call ``neurons/miner.py`` makes.

Mirrors ``allways/miner/optimizer/attach.py``: everything it needs it reads from the miner it is handed,
it runs on its own thread, and it is off unless ``ALLWAYS_ALPHA_ENABLED`` says otherwise. Removing it is
deleting ``allways/miner/alpha/`` and the call.

The miner holds no alpha. A fill whose destination is ``snN`` buys exactly what the user receives at the
moment of the fill and sends that stake on; alpha a user sends us is unstaked back to TAO right after we
deliver. So one TAO float serves every subnet, and no subnet's price is ever carried on the balance sheet.

Threading: each role gets its own subtensor socket, because one socket is not safe to read from two
threads (the same reason the quote optimizer builds its own). Every send through the coldkey — alpha,
TAO delivery, collateral, refill — serialises on the one signer lock.
"""

import threading
import time
from pathlib import Path
from typing import Callable, Optional

import bittensor as bt

from allways.chains import get_chain_def
from allways.constants import MINER_TIMEOUT_CUSHION_SECS, is_alpha
from allways.miner.alpha.config import AlphaConfig, alpha_live
from allways.miner.alpha.config import load as load_alpha_config
from allways.miner.alpha.journal import AlphaJournal
from allways.miner.alpha.ledger import Ledger
from allways.miner.alpha.market import ALPHA_MARKET_NAME, AlphaMarket, fee_reserve_rao
from allways.miner.alpha.router import AlphaRequest, AlphaRouter
from allways.miner.alpha.safe import SafeAlpha
from allways.miner.alpha.seller import AlphaSeller
from allways.miner.alpha.signer import SubtensorSigner, guard_coldkey
from allways.utils.subtensor import build_subtensor

ALPHA_JOURNAL_NAME = 'alpha-journal.json'

MARKET_TICK_SECS = 600  # one all_subnets() read per 10 minutes prices every subnet
ROUTER_TICK_SECS = 5  # a buy settles in ~12s; the state machine wants to see it promptly
SELLER_TICK_SECS = 60  # the sell happens at receipt, but nothing is urgent once delivery is done


class AlphaGear:
    """The alpha threads and the objects they share, so a caller can tick them in tests or stop them."""

    def __init__(
        self,
        market: AlphaMarket,
        router: AlphaRouter,
        seller: AlphaSeller,
        journal: AlphaJournal,
        signer: SubtensorSigner,
        config: AlphaConfig,
    ) -> None:
        self.market = market
        self.router = router
        self.seller = seller
        self.journal = journal
        self.signer = signer
        self.config = config
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start_threads(self) -> None:
        for name, call, every in (
            ('alpha-market', self.market.tick, MARKET_TICK_SECS),
            ('alpha-router', self.router.tick, ROUTER_TICK_SECS),
            ('alpha-seller', self.seller.tick, SELLER_TICK_SECS),
        ):
            thread = threading.Thread(target=self._loop, args=(name, call, every), name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def _loop(self, name: str, call: Callable[[], None], every: float) -> None:
        """One role's tick on its own cadence. A raising tick is logged and retried: the loop is the
        thing that must not die, since a dead router means a reserved fill is never delivered."""
        while not self._stop.is_set():
            try:
                call()
            except Exception as error:
                bt.logging.error(f'{name} tick failed: {error}')
            self._stop.wait(every)

    def shutdown(self) -> None:
        self._stop.set()


def attach_alpha(miner) -> Optional[AlphaGear]:
    """Wire alpha delivery onto ``miner`` when enabled, else return None.

    Enabling it mid-flight is safe: the journal is the only durable state and an absent row simply means
    nothing was promised yet. Disabling it while a row is open is not — the fill it covers would never
    be delivered — so the gear always starts if the journal holds open rows, whatever the config says.
    """
    config = load_alpha_config()
    journal = AlphaJournal(Ledger(_state_path(miner, ALPHA_JOURNAL_NAME)))
    open_rows = journal.open_rows()
    if not alpha_live(config) and not open_rows:
        bt.logging.info('Alpha delivery off (set ALLWAYS_ALPHA_ENABLED=1 to turn it on)')
        return None
    if not alpha_live(config):
        bt.logging.warning(
            f'Alpha delivery is off but {len(open_rows)} journal row(s) are open — starting anyway to '
            'finish them; the quotes stay pulled'
        )

    alpha_defs = {chain: asset.chain_def for chain, asset in miner.assets.items() if is_alpha(chain)}
    if not alpha_defs:
        bt.logging.error('Alpha delivery not started: this miner has no sn* chain providers')
        return None
    tao = miner.assets.get('tao')
    if tao is None:
        bt.logging.error('Alpha delivery not started: alpha is bought with TAO and there is no tao provider')
        return None

    coldkey = miner.wallet.coldkeypub.ss58_address
    signer = SubtensorSigner(lambda skip=None: _connect(miner, skip), miner.wallet)
    # Collateral, refill and TAO delivery all sign from this coldkey too; hold the signer's lock around
    # them so an alpha send never races another send for the same nonce.
    guard_coldkey(miner.assets, signer)

    market = AlphaMarket(
        connect=lambda skip=None: _connect(miner, skip, archive=True),
        ledger=Ledger(_state_path(miner, ALPHA_MARKET_NAME)),
        config=config,
        tao_max_swap=_tao_max_swap(miner),
        tick_secs=MARKET_TICK_SECS,
    )

    # Delivery sends through SafeAlpha (journal before submit, one HELD row per send); the router and
    # seller read positions through plain Alpha on sockets of their own.
    miner.assets.update(_own_alphas(miner, alpha_defs, SafeAlpha, miner.wallet, signer, journal))
    # Our deposit address on every alpha chain is the coldkey that holds the stake.
    miner.my_addresses.update(dict.fromkeys(alpha_defs, coldkey))

    router = AlphaRouter(
        signer=signer,
        journal=journal,
        market=market,
        assets=_own_alphas(miner, alpha_defs, _plain_alpha(miner)),
        coldkey=coldkey,
        tao_free=lambda: tao.get_balance(coldkey),
        config=config,
    )
    seller = AlphaSeller(
        signer=signer,
        journal=journal,
        market=market,
        assets=_own_alphas(miner, alpha_defs, _plain_alpha(miner)),
        coldkey=coldkey,
        tao_max_swap=_tao_max_swap(miner),
        # Our own hotkeys hold stake that is SN7 incentive, not alpha a user sent us. Never sell it.
        keep_hotkeys=frozenset({miner.wallet.hotkey.ss58_address}),
    )

    miner.swap_fulfiller.dest_ready = _dest_ready(router)

    gear = AlphaGear(market, router, seller, journal, signer, config)
    gear.start_threads()
    bt.logging.info(
        f'Alpha wired: chains={len(alpha_defs)} enabled={config.alpha_enabled} open_rows={len(open_rows)} '
        f'fee_reserve={fee_reserve_rao(config)} rao'
    )
    return gear


def _dest_ready(router: AlphaRouter) -> Callable:
    """The fulfiller's acquire-before-send gate. A non-alpha dest is always ready; an alpha dest is ready
    once one hotkey holds the whole amount the user receives."""
    failed: set[str] = set()

    def dest_ready(swap, user_receives: int) -> bool:
        if not is_alpha(swap.to_chain):
            return True
        key = swap.key_hex
        router.request(
            AlphaRequest(
                key,
                get_chain_def(swap.to_chain).netuid,
                user_receives,
                swap.user_to_addr,
                swap.timeout_at - MINER_TIMEOUT_CUSHION_SECS,
            )
        )
        outcome = router.outcome(key)
        if outcome == 'failed' and key not in failed:
            failed.add(key)
            bt.logging.error(f'Swap {key[:16]}: alpha could not be acquired — this fill will time out')
        return outcome == 'done'

    return dest_ready


def _own_alphas(miner, alpha_defs: dict, alpha_cls, *credentials) -> dict:
    """One alpha asset per sn* chain on one fresh socket, for a single thread to read."""
    socket = _connect(miner)
    return {chain: alpha_cls(chain_def, socket, *credentials) for chain, chain_def in alpha_defs.items()}


def _plain_alpha(miner):
    """The unwrapped Alpha class, taken from a provider the miner already built rather than imported, so
    one definition of the asset is in play."""
    from allways.assets.alpha import Alpha

    return Alpha


def _connect(miner, skip: Optional[str] = None, archive: bool = False) -> bt.Subtensor:
    """A fresh subtensor for one role. ``archive`` asks for a node that still holds historical state: the
    market's one-time 24h price backfill reads past blocks, and a pruning endpoint leaves the jumpiness
    jumpiness gate blind for the first day. A failed backfill is skipped, not fatal, so this is best-effort."""
    if archive:
        try:
            return build_subtensor(network='archive')
        except Exception as error:
            bt.logging.warning(f'Alpha market could not reach an archive node ({error}); using the miner network')
    return build_subtensor(config=miner.config)


def _tao_max_swap(miner) -> Callable[[], Optional[int]]:
    """The program's TAO max swap in rao, re-read on a cadence. None is 'unknown', which holds the
    quotable set empty rather than guessing a bound."""
    cached: dict = {'at': 0.0, 'value': None}

    def tao_max_swap() -> Optional[int]:
        if time.time() - cached['at'] < MARKET_TICK_SECS and cached['value'] is not None:
            return cached['value']
        try:
            config = miner.solana_client.get_config()
            cached['value'] = int(config['tao_max_swap_amount'])
            cached['at'] = time.time()
        except Exception as error:
            bt.logging.warning(f'Alpha: TAO max swap unreadable ({error})')
        return cached['value']

    return tao_max_swap


def _state_path(miner, name: str) -> Path:
    hotkey = miner.wallet.hotkey.ss58_address
    path = Path.home() / '.allways' / 'miner' / f'{hotkey[:12]}-{name}'
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
