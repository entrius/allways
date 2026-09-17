"""The quote optimizer's picture of the program, kept by websocket push and seeded from the allways API.

Subscriptions (one ``SubscriptionFeed`` connection, separate from the swap feed) — a fixed set, whatever the market does:
- ``programSubscribe`` per managed direction: every MinerQuote whose body starts with that ``from_chain`` /
  ``to_chain`` pair (memcmp at offset 40 — discriminator 8 + miner 32 — over the two borsh strings);
- ``programSubscribe`` on every MinerState, and on every BondAttestation when a managed lane is bond-backed — one
  subscription each rather than one per miner (mainnet 2026-09-15: 109 states and 15 bonds, a few KB of pushes a day);
- ``accountSubscribe`` on the Config PDA, this miner's own quote PDAs and its delivery wallets on Solana.

Closures: ``remove_quote`` closes the PDA (lamports 0, owner back to the system program). ``programSubscribe`` sends
nothing for that — it filters on the program as owner, and a closed account no longer has it — while
``accountSubscribe`` does push the zeroed account (verified on devnet, see the build report). Our own quote PDAs are
watched by account, so a removal shows at once; a competitor's is caught by ``reconcile`` against the API, which
rebuilds its miner rows from the chain, a few minutes late at worst.

Lost pushes: a connection that dies takes its last pushes with it, and a subscription never replays them. The API seed
can't be trusted to fill the gap — it lags the chain, so a change made seconds before the drop is still old there — and
``reconcile`` never overrides a quote that was pushed. ``resync_quotes`` puts a chain read (``getProgramAccounts`` per
managed direction) over competitor quotes instead: after every seed, and whenever ``disagreements`` finds the API
listing a pushed competitor at another rate.

A subscription only reports changes, so the cache starts from a seed: other miners' quotes and purse facts from
``GET /miners``, this miner's state from its own reads. Pushes win over the seed: a seed never overwrites an
account pushed since the feed last went down.
"""

import base64
import threading
import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import base58
import bittensor as bt
from borsh_construct import String
from solders.pubkey import Pubkey

from allways.constants import RATE_PRECISION
from allways.miner.optimizer.subscription_feed import Subscription, SubscriptionFeed
from allways.solana import layouts, pdas

MINER_QUOTE_DIRECTION_OFFSET = len(layouts.DISCRIMINATORS['MinerQuote']) + 32
# The feed renews this often, opening the new connection before closing the old (no catch-up read); each renewal is a
# Helius connection credit. The swap feed keeps its own, shorter schedule.
OPTIMIZER_FEED_RESUBSCRIBE_SECONDS = 900
SYSTEM_PROGRAM = '11111111111111111111111111111111'
# A competitor quote pushed this recently stays even when the API doesn't list it: the API polls the chain, so a
# quote created a moment ago may not be in its rows yet.
RECONCILE_GRACE_SECS = 180


def account_filter(name: str) -> List[dict]:
    """``programSubscribe`` filter matching one account type by its discriminator."""
    return [{'memcmp': {'offset': 0, 'bytes': base58.b58encode(layouts.DISCRIMINATORS[name]).decode()}}]


def direction_filter(from_chain: str, to_chain: str) -> List[dict]:
    """``programSubscribe`` filters matching one direction's quotes: the MinerQuote discriminator, then its borsh
    ``from_chain`` and ``to_chain`` (u32 LE length + UTF-8 each) right after the miner pubkey."""
    direction = String.build(from_chain) + String.build(to_chain)
    return account_filter('MinerQuote') + [
        {'memcmp': {'offset': MINER_QUOTE_DIRECTION_OFFSET, 'bytes': base58.b58encode(direction).decode()}},
    ]


def fixed_rate(value) -> int:
    """A das decimal rate string ("0.455430000000000000") or float as the on-chain u128 fixed point."""
    if value in (None, ''):
        return 0
    return int(Decimal(str(value)) * RATE_PRECISION)


def make_quote(miner, lane, from_addr: str, to_addr: str, rate: int, liquidity: int = 0, updated_at: int = 0):
    """A MinerQuote-shaped record for a quote known from the API or from our own transaction."""
    from_chain, to_chain, backing = lane
    return SimpleNamespace(
        miner=miner if isinstance(miner, Pubkey) else Pubkey.from_string(str(miner)),
        from_chain=from_chain,
        to_chain=to_chain,
        collateral_chain=backing,
        miner_from_addr=from_addr,
        miner_to_addr=to_addr,
        rate=int(rate),
        liquidity=int(liquidity),
        updated_at=int(updated_at),
    )


def _row_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def market_from_das(rows: Iterable[dict], lanes, now: int) -> Tuple[List[object], Dict[str, object], Dict]:
    """``GET /miners`` rows as (quotes, states, bonds) for the managed lanes.

    A row is one miner × hub pair × backing with the hub as ``sourceChain``: ``rate`` is the hub→spoke quote and
    ``counterRate`` the spoke→hub one, both canonical (checked against the chain; see the tests' fixture), and
    ``sourceAddress`` is the miner's hub address. ``collateral`` is that backing's purse (MinerState collateral
    for sol, the attested bond otherwise); ``isActive`` / ``isReserved`` / ``hasActiveSwap`` are per purse.
    Strikes and settling locks are not in the API: they read as clear, which the market tolerance covers.
    Quote ``updated_at`` is unknown here and left 0."""
    lanes = set(lanes)
    quotes: List[object] = []
    states: Dict[str, object] = {}
    bonds: Dict[Tuple[str, str], object] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get('solanaPubkey'):
            continue
        hub, spoke, backing = row.get('sourceChain'), row.get('destChain'), row.get('backing') or 'sol'
        if backing not in pdas.BACKING_BITS:
            continue
        miner = str(row['solanaPubkey'])
        hub_addr, spoke_addr = row.get('sourceAddress') or '', row.get('destAddress') or ''
        for lane, rate, from_addr, to_addr in (
            ((hub, spoke, backing), row.get('rate'), hub_addr, spoke_addr),
            ((spoke, hub, backing), row.get('counterRate'), spoke_addr, hub_addr),
        ):
            if lane in lanes and fixed_rate(rate) > 0:
                quotes.append(make_quote(miner, lane, from_addr, to_addr, fixed_rate(rate)))
        if not any(lane[2] == backing and hub in lane[:2] and spoke in lane[:2] for lane in lanes):
            continue
        state = states.get(miner)
        if state is None:
            state = states[miner] = SimpleNamespace(
                miner=Pubkey.from_string(miner),
                collateral=0,
                active_backings=0,
                failed_swaps=0,
                settling_until=[0] * layouts.MAX_BACKING_SLOTS,
                reserved_collateral=[0] * layouts.MAX_BACKING_SLOTS,
                busy_until=[0] * layouts.MAX_BACKING_SLOTS,
                active_swap_backings=0,
            )
        bit = pdas.BACKING_BITS[backing]
        slot = bit.bit_length() - 1
        if row.get('isActive'):
            state.active_backings |= bit
        if row.get('hasActiveSwap'):
            state.active_swap_backings |= bit
        if row.get('isReserved'):
            state.busy_until[slot] = max(state.busy_until[slot], _row_int(row.get('reservedUntil')) or now + 60)
        if backing == 'sol':
            state.collateral = max(int(state.collateral), _row_int(row.get('collateral')))
        else:
            balance = _row_int(row.get('collateral'))
            bonds[(miner, backing)] = SimpleNamespace(
                miner=state.miner, chain=backing, effective_balance=balance, locked=balance > 0
            )
    return quotes, states, bonds


# Program-wide subscriptions other than the per-direction quote ones, by key → the account kind they carry.
_PROGRAM_KINDS = {'states': 'state', 'bonds': 'bond'}


class MarketFeed:
    """Cache of the accounts the optimizer decides on, keyed by account pubkey and ordered by slot."""

    def __init__(
        self,
        ws_url: str,
        program_id,
        me,
        lanes,
        decode: Callable[[str, bytes], object],
        feed: Optional[SubscriptionFeed] = None,
    ):
        self.program_id = program_id if isinstance(program_id, Pubkey) else Pubkey.from_string(str(program_id))
        self.me = str(me)
        self.lanes = [tuple(lane) for lane in lanes]
        self.decode = decode
        self.feed = feed or SubscriptionFeed(
            ws_url, self.on_push, max_session_secs=OPTIMIZER_FEED_RESUBSCRIBE_SECONDS, name='optimizer feed'
        )
        self.lock = threading.Lock()
        self.config = None
        self.quotes: Dict[str, object] = {}
        self.states: Dict[str, object] = {}
        self.bonds: Dict[Tuple[str, str], object] = {}
        self.lamports: Dict[str, int] = {}
        self.kinds: Dict[str, str] = {}
        self.slots: Dict[str, int] = {}
        self.pushed_at: Dict[str, float] = {}
        for from_chain, to_chain in self.directions:
            self.subscribe_program(f'quotes:{from_chain}:{to_chain}', direction_filter(from_chain, to_chain))
        self.subscribe_program('states', account_filter('MinerState'))
        if any(lane[2] != 'sol' for lane in self.lanes):
            self.subscribe_program('bonds', account_filter('BondAttestation'))
        self.watch(pdas.config_pda(self.program_id), 'config')
        for lane in self.lanes:
            self.watch(self.quote_pda(self.me, lane), 'quote')

    @property
    def directions(self) -> List[Tuple[str, str]]:
        """The managed (from_chain, to_chain) pairs, each one quote subscription and one chain read."""
        return sorted({lane[:2] for lane in self.lanes})

    # ── feed passthrough ──

    @property
    def live(self) -> bool:
        return self.feed.live

    @property
    def generation(self) -> int:
        return self.feed.generation

    @property
    def down_since(self) -> float:
        return self.feed.down_since

    @property
    def bytes_received(self) -> int:
        return self.feed.bytes_received

    @property
    def connections_opened(self) -> int:
        return self.feed.connections_opened

    def start(self) -> None:
        self.feed.start()

    def stop(self) -> None:
        self.feed.stop()

    # ── subscriptions ──

    def subscribe_program(self, key: str, filters: List[dict]) -> None:
        params = {'encoding': 'base64', 'commitment': 'confirmed', 'filters': filters}
        self.feed.add(Subscription(key, 'programSubscribe', [str(self.program_id), params]))

    def watch(self, pubkey, kind: str) -> bool:
        pubkey = str(pubkey)
        with self.lock:
            self.kinds.setdefault(pubkey, kind)
        return self.feed.add(
            Subscription(
                f'account:{pubkey}', 'accountSubscribe', [pubkey, {'encoding': 'base64', 'commitment': 'confirmed'}]
            )
        )

    def watch_wallet(self, address: str) -> bool:
        """Watch a delivery wallet's lamports. True when newly watched — its balance is unknown until seeded."""
        return self.watch(address, 'wallet')

    def quote_pda(self, miner, lane) -> str:
        key = miner if isinstance(miner, Pubkey) else Pubkey.from_string(str(miner))
        return str(pdas.quote_pda(key, lane[0], lane[1], lane[2], self.program_id))

    # ── pushes ──

    def on_push(self, key: str, result: dict) -> None:
        slot = int(((result or {}).get('context') or {}).get('slot') or 0)
        value = (result or {}).get('value') or {}
        if key.startswith('quotes:') or key in _PROGRAM_KINDS:
            pubkey, account = str(value.get('pubkey') or ''), value.get('account') or {}
            kind = 'quote' if key.startswith('quotes:') else _PROGRAM_KINDS[key]
        else:
            pubkey = key.split(':', 1)[1]
            account, kind = value, self.kinds.get(pubkey)
        if pubkey and kind:
            self.apply(kind, pubkey, account, slot)

    def apply(self, kind: str, pubkey: str, account: dict, slot: int = 0) -> None:
        with self.lock:
            if slot and slot < self.slots.get(pubkey, 0):
                return  # an older copy, e.g. from the session a resubscribe is replacing
            self.slots[pubkey] = max(slot, self.slots.get(pubkey, 0))
            self.pushed_at[pubkey] = time.time()
        lamports = int(account.get('lamports') or 0)
        if kind == 'wallet':
            with self.lock:
                self.lamports[pubkey] = lamports
            return
        closed = lamports == 0 or str(account.get('owner') or '') != str(self.program_id)
        decoded = None
        if not closed:
            try:
                decoded = self.decode(_ACCOUNT_NAMES[kind], base64.b64decode((account.get('data') or [''])[0]))
            except Exception as e:
                bt.logging.debug(f'optimizer feed: undecodable {kind} {pubkey}: {e}')
                return
        with self.lock:
            self._store(kind, pubkey, decoded)

    def _store(self, kind: str, pubkey: str, decoded) -> None:
        if kind == 'config':
            self.config = decoded if decoded is not None else self.config
        elif kind == 'quote':
            if decoded is None:
                self.quotes.pop(pubkey, None)
            else:
                self.quotes[pubkey] = decoded
        elif kind == 'state':
            miner = self._owner(pubkey, decoded, self.states)
            if miner is not None:
                if decoded is None:
                    self.states.pop(miner, None)
                else:
                    self.states[miner] = decoded
        elif kind == 'bond':
            key = self._owner(pubkey, decoded, self.bonds)
            if key is not None:
                if decoded is None:
                    self.bonds.pop(key, None)
                else:
                    self.bonds[key] = decoded

    def _owner(self, pubkey: str, decoded, table: dict):
        """The cache key a MinerState / bond PDA stands for — from the account itself, or for a closure, by
        re-deriving the PDA of each cached entry."""
        if decoded is not None:
            if hasattr(decoded, 'chain'):
                return (str(decoded.miner), str(decoded.chain).lower())
            return str(decoded.miner)
        for key in list(table):
            if isinstance(key, tuple):
                pda = pdas.bond_attestation_pda(Pubkey.from_string(key[0]), key[1], self.program_id)
            else:
                pda = pdas.miner_state_pda(Pubkey.from_string(key), self.program_id)
            if str(pda) == pubkey:
                return key
        return None

    # ── seeding, reconciling and local writes ──

    def seed(
        self,
        quotes: Iterable[object],
        states: Dict[str, object],
        bonds: Dict,
        config=None,
        lamports: Optional[Dict[str, int]] = None,
    ) -> None:
        """Replace the picture with a seed, except accounts pushed since the feed last went down — those are
        at least as fresh as anything the API can say."""
        since = self.feed.down_since
        by_pda = {self.quote_pda(q.miner, (q.from_chain, q.to_chain, q.collateral_chain)): q for q in quotes}
        with self.lock:
            fresh = {pubkey for pubkey, at in self.pushed_at.items() if at >= since}
            self.quotes = {pda: q for pda, q in self.quotes.items() if pda in fresh} | {
                pda: q for pda, q in by_pda.items() if pda not in fresh
            }
            state_pdas = {
                miner: str(pdas.miner_state_pda(Pubkey.from_string(miner), self.program_id)) for miner in states
            }
            self.states = {m: s for m, s in self.states.items() if self._pda_of_state(m) in fresh} | {
                m: s for m, s in states.items() if state_pdas[m] not in fresh
            }
            self.bonds = {k: b for k, b in self.bonds.items() if self._pda_of_bond(k) in fresh} | {
                k: b for k, b in bonds.items() if self._pda_of_bond(k) not in fresh
            }
            if config is not None and str(pdas.config_pda(self.program_id)) not in fresh:
                self.config = config
            for address, balance in (lamports or {}).items():
                if address not in fresh and balance is not None:
                    self.lamports[address] = int(balance)

    def reconcile(self, quotes: Iterable[object]) -> int:
        """Line competitor quotes on the managed lanes up with the API's current list (``quotes``, from
        ``market_from_das``). One the API no longer lists is dropped — its closure pushed nothing — unless it was
        pushed in the last ``RECONCILE_GRACE_SECS``; one never pushed is taken from the API, which covers a change
        between the seed's snapshot and the subscription. Our own quotes are left to their account subscriptions.
        Returns how many quotes were dropped."""
        listed = {
            self.quote_pda(q.miner, (q.from_chain, q.to_chain, q.collateral_chain)): q
            for q in quotes
            if str(q.miner) != self.me
        }
        now = time.time()
        with self.lock:
            gone = [
                pda
                for pda, q in self.quotes.items()
                if str(q.miner) != self.me
                and (q.from_chain, q.to_chain, q.collateral_chain) in self.lanes
                and pda not in listed
                and now - self.pushed_at.get(pda, 0) > RECONCILE_GRACE_SECS
            ]
            for pda in gone:
                del self.quotes[pda]
            for pda, quote in listed.items():
                if pda not in self.pushed_at:
                    self.quotes[pda] = quote
        return len(gone)

    def disagreements(self, quotes: Iterable[object]) -> int:
        """Competitor quotes the API (``quotes``, from ``market_from_das``) lists at another rate than the one pushed
        to us, the push older than ``RECONCILE_GRACE_SECS`` so the API has had time to index it. A push lost with a
        dying connection looks exactly like this. The API isn't taken as the fix (it lags the chain); the caller
        reads the chain."""
        listed = [
            (self.quote_pda(q.miner, (q.from_chain, q.to_chain, q.collateral_chain)), q)
            for q in quotes
            if str(q.miner) != self.me
        ]
        now = time.time()
        with self.lock:
            return sum(
                1
                for pda, q in listed
                if pda in self.quotes
                and pda in self.pushed_at
                and now - self.pushed_at[pda] > RECONCILE_GRACE_SECS
                and int(self.quotes[pda].rate) != int(q.rate)
            )

    def resync_quotes(self, accounts: Iterable[Tuple[str, bytes]], slot: int, read_at: float) -> Tuple[int, int]:
        """Put a chain read over the competitor quotes on the managed directions: ``accounts`` from
        ``getProgramAccounts`` per direction, ``slot`` read just before it and ``read_at`` its wall-clock start. A
        quote pushed at a later slot stands; a cached one the read doesn't return, and that wasn't pushed since, is
        closed. Our own quotes are left to their account subscriptions. Returns (quotes that appeared or changed rate,
        quotes dropped)."""
        directions = set(self.directions)
        returned, changed = set(), 0
        for pubkey, raw in accounts:
            returned.add(pubkey)
            try:
                decoded = self.decode('MinerQuote', raw)
            except Exception as e:
                bt.logging.debug(f'optimizer feed: undecodable quote {pubkey} in a chain read: {e}')
                continue
            if str(decoded.miner) == self.me:
                continue
            with self.lock:
                if slot < self.slots.get(pubkey, 0):
                    continue
                cached = self.quotes.get(pubkey)
                if cached is None or int(cached.rate) != int(decoded.rate):
                    changed += 1
                self.quotes[pubkey] = decoded
                self.slots[pubkey] = slot
                self.pushed_at[pubkey] = max(read_at, self.pushed_at.get(pubkey, 0))
        with self.lock:
            gone = [
                pda
                for pda, q in self.quotes.items()
                if str(q.miner) != self.me
                and (q.from_chain, q.to_chain) in directions
                and pda not in returned
                and self.pushed_at.get(pda, 0) < read_at
            ]
            for pda in gone:
                del self.quotes[pda]
        return changed, len(gone)

    def _pda_of_state(self, miner: str) -> str:
        return str(pdas.miner_state_pda(Pubkey.from_string(miner), self.program_id))

    def _pda_of_bond(self, key) -> str:
        return str(pdas.bond_attestation_pda(Pubkey.from_string(key[0]), key[1], self.program_id))

    def set_lamports(self, address: str, balance: int) -> None:
        with self.lock:
            if address not in self.pushed_at:
                self.lamports[address] = int(balance)

    def note_quote(self, quote) -> str:
        """Record a quote our own transaction just wrote; the push that follows replaces it with chain truth."""
        pda = self.quote_pda(quote.miner, (quote.from_chain, quote.to_chain, quote.collateral_chain))
        with self.lock:
            self.quotes[pda] = quote
        return pda

    def drop_quote(self, miner, lane) -> None:
        pda = self.quote_pda(miner, lane)
        with self.lock:
            self.quotes.pop(pda, None)

    def snapshot(self) -> Tuple[object, List[object], Dict[str, object], Dict, Dict[str, int]]:
        with self.lock:
            return self.config, list(self.quotes.values()), dict(self.states), dict(self.bonds), dict(self.lamports)


_ACCOUNT_NAMES = {'config': 'Config', 'quote': 'MinerQuote', 'state': 'MinerState', 'bond': 'BondAttestation'}
