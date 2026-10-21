"""Program events, pushed live and gap-filled on reconnect within a bounded horizon.

Live events come off ``logsSubscribe``, whose payload already carries the log lines, so a quote re-post
costs no ``getTransaction``. After a disconnect, a restart, or a fresh database, ``Backfill`` reads what
was missed, but never further back than ``HISTORY_HORIZON_SECS`` and never more than one chunk per tick.
A socket that stays up can still drop one notification, so every ``SWEEP_SECS`` the feed also lists the
recent signatures and decodes only those it never saw.

Records are NOT globally ordered (a backfilled event can arrive after newer live ones) and can repeat
across a restart. Every consumer must be order-tolerant and idempotent by (signature, event index)."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

import bittensor as bt

from allways.solana.events import TX_LOGS_TRUNCATED, EventRecord, logs_truncated
from allways.solana.program_feed import ProgramEventFeed, events_from_logs
from allways.validator.runtime.bus import Latest, Topic
from allways.validator.runtime.component import Component

# Two fee cadences plus margin. The longest swap life (10 min reservation TTL + 140 min reservation
# extensions + 10 min base timeout + 140 min timeout extensions ≈ 5h) needs less; the fee ledger
# needs every fee since the vault last settled, and with a round waiting on this validator's vote
# that is up to two boundaries back. A fresh boot reads this window once: ~19,500 transactions on
# mainnet (2026-10-19), ~40 minutes at Helius latency, during which only fee votes wait.
HISTORY_HORIZON_SECS = 13 * 3600
BACKFILL_PAGES_PER_TICK = 5
BACKFILL_DECODES_PER_TICK = 300
SEEN_SIGNATURES = 50_000
# Solana slots are ~0.4s; between two block-time anchors the feed interpolates.
SLOT_ANCHOR_SECS = 30.0
SLOT_SECS = 0.4
# No notification for this long and the socket is presumed dead: resubscribe, and the catch-up that
# every resubscribe triggers fills whatever was missed. Costs ~one signature listing on a quiet socket.
SILENT_SOCKET_SECS = 90.0
# A listed signature whose getTransaction keeps coming back empty is retried this many ticks, then given
# up as a history gap (consumers rescan) rather than skipped silently.
EMPTY_TX_RETRIES = 5
# A live socket can drop a notification without going quiet. This often the feed lists the signatures since
# the sweep before last and decodes the unseen ones: about one listing a minute on mainnet.
SWEEP_SECS = 60.0
SWEEP_PAGES = 5


class SeenSignatures:
    """Bounded memory of signatures already published, so backfill never re-decodes a live tx."""

    def __init__(self, size: int = SEEN_SIGNATURES) -> None:
        self._sigs: 'OrderedDict[str, None]' = OrderedDict()
        self._size = size
        self._lock = threading.Lock()

    def add(self, signature: str) -> bool:
        """True if newly added, False if already seen."""
        with self._lock:
            if signature in self._sigs:
                return False
            self._sigs[signature] = None
            if len(self._sigs) > self._size:
                self._sigs.popitem(last=False)
            return True

    def __contains__(self, signature: str) -> bool:
        with self._lock:
            return signature in self._sigs


class SlotClock:
    """Turns a slot into chain time without a read per event. Anchors are ``getBlockTime(slot)`` for the
    newest slot seen, at most once per ``SLOT_ANCHOR_SECS``; a slot between two anchors is interpolated,
    one past the newest is extrapolated at the measured slot rate. Error is a few seconds at worst."""

    def __init__(self, block_time: Any, min_interval: float = SLOT_ANCHOR_SECS) -> None:
        self._block_time = block_time  # callable: slot -> unix seconds or None
        self._min_interval = min_interval
        self._anchors: List[Tuple[int, int]] = []  # (slot, unix), ascending
        self._fetched_at = 0.0

    def anchor(self, slot: int, now: float) -> None:
        if self._anchors and (now - self._fetched_at < self._min_interval or slot <= self._anchors[-1][0]):
            return
        t = self._block_time(slot)
        self._fetched_at = now
        if t is not None:
            self._anchors.append((slot, int(t)))
            del self._anchors[:-64]

    def _rate(self) -> float:
        if len(self._anchors) < 2:
            return SLOT_SECS
        (s0, t0), (s1, t1) = self._anchors[-2], self._anchors[-1]
        return (t1 - t0) / (s1 - s0) if s1 > s0 and t1 >= t0 else SLOT_SECS

    def time_of(self, slot: int) -> Optional[int]:
        """Unix time for ``slot``, or None before the first anchor."""
        if not self._anchors:
            return None
        prev: Optional[Tuple[int, int]] = None
        for a_slot, a_time in self._anchors:
            if a_slot == slot:
                return a_time
            if a_slot > slot:
                if prev is None:
                    return int(round(a_time - (a_slot - slot) * self._rate()))
                p_slot, p_time = prev
                return int(round(p_time + (slot - p_slot) * (a_time - p_time) / (a_slot - p_slot)))
            prev = (a_slot, a_time)
        last_slot, last_time = self._anchors[-1]
        return int(round(last_time + (slot - last_slot) * self._rate()))


@dataclass(frozen=True)
class HistoryGap:
    """History older than ``complete_since`` (unix) is incomplete: a fresh start or an outage longer
    than the horizon. A consumer whose window reaches past it must treat that part as unknown."""

    complete_since: float


@dataclass(frozen=True)
class FeedStatus:
    catching_up: bool  # a catch-up is in progress: events past ``cursor`` may not all be published yet
    session: int  # acknowledged subscriptions so far
    last_event_at: float  # wall time of the newest published live event (0 = none yet)
    # (slot, signature) up to which every event has been published. A consumer that has drained its
    # subscription after reading this status may record it as its own durable position.
    cursor: Optional[Tuple[int, str]] = None


# Truncated txs whose accounts the feed names per hour; past this the hourly rescan heals them instead.
TRUNCATED_REREADS_PER_HOUR = 120


def tx_account_keys(tx: Optional[dict]) -> List[str]:
    """Every account a getTransaction result names, signer first (static keys, then lookup-table loads)."""
    if not tx:
        return []
    keys = ((tx.get('transaction') or {}).get('message') or {}).get('accountKeys') or []
    loaded = (tx.get('meta') or {}).get('loadedAddresses') or {}
    out = [k if isinstance(k, str) else k.get('pubkey') for k in keys]
    return [k for k in out + list(loaded.get('writable') or []) + list(loaded.get('readonly') or []) if k]


@dataclass
class _Pending:
    slot: int
    signature: str
    events: List[Tuple[str, Any]]
    truncated: bool = False  # the runtime cut this tx's logs: events after the cut never arrived


class LiveFeed(ProgramEventFeed):
    """ProgramEventFeed that keeps the signature and slot of each notification, and resubscribes when
    the socket goes quiet: a connection that still answers pings can have stopped delivering."""

    def __init__(self, ws_url: str, program_id, silent_secs: float = SILENT_SOCKET_SECS) -> None:
        super().__init__(ws_url, program_id)
        self.silent_secs = silent_secs
        self._lock = threading.Lock()
        self._inbox: List[_Pending] = []

    async def _session(self) -> None:
        import websockets

        sub = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'logsSubscribe',
            'params': [{'mentions': [self.mentions]}, {'commitment': 'confirmed'}],
        }
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
            await ws.send(json.dumps(sub))
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), self.silent_secs)
                except asyncio.TimeoutError:
                    return  # silent too long: the base loop resubscribes at once
                try:
                    self.handle_frame(json.loads(raw))
                except (ValueError, TypeError):
                    continue

    def handle_notification(self, msg: dict) -> int:
        result = ((msg or {}).get('params') or {}).get('result') or {}
        value = result.get('value') or {}
        if value.get('err') is not None:
            return 0
        events = events_from_logs(value.get('logs'), self.program_id)
        slot = int((result.get('context') or {}).get('slot') or 0)
        truncated = logs_truncated(value.get('logs'))
        with self._lock:
            self._inbox.append(_Pending(slot, str(value.get('signature')), events, truncated))
        return len(events)

    def take(self) -> List[_Pending]:
        with self._lock:
            out, self._inbox = self._inbox, []
        return out


class Backfill:
    """Reads the signatures missed since ``until_sig``, newest-first, stopping at the horizon, then hands
    them out oldest-first in bounded chunks. Returns True from ``truncated`` when the horizon cut history
    short, so consumers can mark the gap instead of trusting a partial window."""

    def __init__(self, rpc, program_id, until_sig: Optional[str], now: float) -> None:
        self.rpc = rpc
        self.program_id = program_id
        self.until_sig = until_sig
        self.floor = now - HISTORY_HORIZON_SECS
        self.truncated = until_sig is None
        self._before: Optional[str] = None
        self._paged: List[dict] = []
        self._queue: List[dict] = []
        self.paging_done = False

    def page(self, max_pages: int = BACKFILL_PAGES_PER_TICK) -> None:
        for _ in range(max_pages):
            batch = self.rpc.get_signatures_for_address(
                self.program_id, before=self._before, until=self.until_sig, limit=1000
            )
            for entry in batch:
                bt_ = entry.get('blockTime')
                if bt_ is not None and bt_ < self.floor:
                    self.truncated = True
                    self._finish()
                    return
                self._paged.append(entry)
            if len(batch) < 1000:
                self._finish()
                return
            self._before = batch[-1]['signature']

    def _finish(self) -> None:
        self._queue = list(reversed(self._paged))
        self._paged = []
        self.paging_done = True

    def peek(self, limit: Optional[int] = BACKFILL_DECODES_PER_TICK) -> List[dict]:
        """The next entries, oldest first. Each stays queued until ``pop``: a failure resumes at it."""
        return self._queue[:limit]

    def pop(self) -> None:
        self._queue.pop(0)

    @property
    def done(self) -> bool:
        return self.paging_done and not self._queue


class EventFeed(Component):
    """Publishes every program event once (best effort) to ``out``. Not critical: the actuator decides off
    the World, and the event-fed ledgers are a second line behind its own observation of live swaps."""

    name = 'feed'
    interval_secs = 2.0
    stall_secs = 120.0

    def __init__(
        self,
        rpc_factory,
        live: LiveFeed,
        resume_from: Callable[[], Optional[str]],
        out: Topic,
        gaps: Topic,
        program_id,
        status: Optional[Latest] = None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self._rpc_factory = rpc_factory
        self.live = live
        self.resume_from = resume_from  # the oldest position any consumer durably applied (None = horizon)
        self.out = out
        self.gaps = gaps  # publishes a HistoryGap when the horizon cut history short
        self.status: Latest = status if status is not None else Latest()
        self.program_id = program_id
        self.seen = SeenSignatures()
        self._rpc: Any = None
        self._clock: Optional[SlotClock] = None
        self._held: List[_Pending] = []
        self._backfill: Optional[Backfill] = None
        self._session = 0
        self._rerun = False
        self._newest_live: Optional[Tuple[int, str]] = None
        self._cursor: Optional[Tuple[int, str]] = None
        self._last_event_at = 0.0
        self._empty_tries: dict = {}
        self._recovered = 0  # events the current backfill published
        # The sweep lists back to ``_sweep_until`` (the newest signature the sweep before last listed): one
        # sweep of overlap, so a signature that became visible late is still inside the next listing.
        self._sweep_until: Optional[str] = None
        self._sweep_next: Optional[str] = None
        self._swept_at = 0.0
        self._truncated_window, self._truncated_count = 0, 0

    def setup(self) -> None:
        self._rpc = self._rpc_factory()
        self._clock = SlotClock(lambda slot: self._rpc._call('getBlockTime', [slot]))
        self.live.start()
        self._backfill = Backfill(self._rpc, self.program_id, self.resume_from(), self.clock())

    def tick(self, now: float) -> None:
        self._stamp_live(now)
        if self.live.session > max(self._session, 1):  # a re-subscribe: events since the cursor may be missing
            self._rerun = True
        self._session = self.live.session
        if self._backfill is None and self._rerun:
            self._rerun = False
            self._backfill = Backfill(self._rpc, self.program_id, self._cursor and self._cursor[1], now)
        if self._backfill is not None:
            self._step_backfill(now)
        elif self._sweep_until is None and self._cursor is not None:
            self._sweep_until = self._sweep_next = self._cursor[1]  # sweeps start from where history is whole
            self._swept_at = now
        elif self._sweep_until is not None and now - self._swept_at >= SWEEP_SECS:
            self._sweep(now)
        if self._backfill is None and self._newest_live is not None:
            self._cursor = max(self._cursor or self._newest_live, self._newest_live)
        # Set after publishing: whoever reads this status and then drains has everything up to cursor.
        self.status.set(FeedStatus(self._backfill is not None, self.live.session, self._last_event_at, self._cursor))

    def _truncated(self, sig: str, slot, block_time, tx: Optional[dict], now: float) -> List[EventRecord]:
        """A tx whose logs the runtime cut short may have changed accounts with no event saying so. Name
        every account it touched so the World re-reads them; past the hourly cap the rescan heals instead."""
        window = int(now // 3600)
        if window != self._truncated_window:
            self._truncated_window, self._truncated_count = window, 0
        self._truncated_count += 1
        if self._truncated_count > TRUNCATED_REREADS_PER_HOUR:
            bt.logging.warning(f'feed: {sig} logs truncated, over {TRUNCATED_REREADS_PER_HOUR}/h: left to the rescan')
            return []
        try:
            tx = tx if tx is not None else self._rpc.get_transaction(sig)
        except Exception as e:
            tx = None
            bt.logging.warning(f'feed: {sig} logs truncated and its transaction is unreadable ({e})')
        accounts = tx_account_keys(tx)
        if not accounts:
            self.gaps.publish(HistoryGap(block_time or now))  # can't name its accounts: rescan them all
            return []
        bt.logging.warning(
            f'feed: {sig} logs truncated (signer {accounts[0]}): re-reading the {len(accounts)} accounts it touched'
        )
        return [EventRecord(TX_LOGS_TRUNCATED, {'accounts': accounts}, slot, block_time, sig)]

    def _stamp_live(self, now: float) -> None:
        self._held.extend(self.live.take())
        if not self._held:
            return
        self._clock.anchor(max(p.slot for p in self._held), now)
        if self._clock.time_of(self._held[0].slot) is None:
            return  # no anchor yet (the block-time read failed): hold, and retry next tick
        for p in self._held:
            if self.seen.add(p.signature):
                t = self._clock.time_of(p.slot)
                records = [EventRecord(n, f, p.slot, t, p.signature) for n, f in p.events]
                if p.truncated:
                    records += self._truncated(p.signature, p.slot, t, None, now)
                self.out.publish_many(records)
                self._last_event_at = now
            self._newest_live = max(self._newest_live or (p.slot, p.signature), (p.slot, p.signature))
        self._held = []

    def _step_backfill(self, now: float) -> None:
        bf = self._backfill
        if not bf.paging_done:
            bf.page()
            if not bf.paging_done:
                return
            if bf.truncated:
                self.gaps.publish(HistoryGap(bf.floor))
        for entry in bf.peek():
            sig, slot = entry['signature'], int(entry.get('slot') or 0)
            if entry.get('err') is None and sig not in self.seen:
                published = self._publish_listed(entry, now)  # raises after its retries: retried next tick
                if published is None:
                    return  # not served yet: hold here, so nothing after it is published first
                self._recovered += published
            self._cursor = (slot, sig)
            bf.pop()
        if bf.done:
            # A resubscribe on a quiet socket backfills nothing; only a recovery is news.
            log = bt.logging.info if self._recovered else bt.logging.debug
            log(f'feed: backfill complete, {self._recovered} missed event(s) recovered')
            self._backfill, self._recovered = None, 0

    def _publish_listed(self, entry: dict, now: float) -> Optional[int]:
        """Decode and publish one listed signature's events; how many were published, or None when its
        transaction isn't served yet (retried; after ``EMPTY_TX_RETRIES`` it is published as a gap)."""
        sig, slot = entry['signature'], int(entry.get('slot') or 0)
        tx = self._rpc.get_transaction(sig)
        if tx is None:
            tries = self._empty_tries[sig] = self._empty_tries.get(sig, 0) + 1
            if tries < EMPTY_TX_RETRIES:
                return None
            bt.logging.warning(f'feed: {sig} has no transaction after {tries} tries, marking a gap')
            self.gaps.publish(HistoryGap(entry.get('blockTime') or now))
            tx = {}
        self._empty_tries.pop(sig, None)
        self._clock.anchor(slot, now)
        block_time = tx.get('blockTime') or entry.get('blockTime') or self._clock.time_of(slot)
        logs = (tx.get('meta') or {}).get('logMessages') or []
        if not self.seen.add(sig):
            return 0  # arrived live meanwhile
        records = [
            EventRecord(n, f, entry.get('slot'), block_time, sig) for n, f in events_from_logs(logs, self.program_id)
        ]
        if logs_truncated(logs):
            records += self._truncated(sig, entry.get('slot'), block_time, tx, now)
        self.out.publish_many(records)
        return len(records)

    def _sweep(self, now: float) -> None:
        """List the signatures since the sweep before last and publish any the live socket never delivered.
        Unlike a backfill it leaves the cursor and the status alone: nothing waits on it."""
        listing = Backfill(self._rpc, self.program_id, self._sweep_until, now)
        listing.page(SWEEP_PAGES)
        if not listing.paging_done:
            # More traffic in two minutes than the sweep lists: name the gap, and restart the overlap from here.
            bt.logging.warning(f'feed: sweep found over {SWEEP_PAGES} pages of new signatures, marking a gap')
            self.gaps.publish(HistoryGap(now - 2 * SWEEP_SECS))
            listing._finish()
        entries = listing.peek(limit=None)
        recovered, pending = 0, False
        for entry in entries:
            if entry.get('err') is not None or entry['signature'] in self.seen:
                continue
            published = self._publish_listed(entry, now)
            if published is None:
                pending = True  # not served yet: it stays inside the next sweep's listing
            else:
                recovered += published
        if recovered:
            bt.logging.warning(f'feed: sweep recovered {recovered} event(s) the live socket never delivered')
        if not pending and entries:
            # The next sweep lists back to this one's start, so each signature is listed by two sweeps.
            self._sweep_until, self._sweep_next = self._sweep_next, entries[-1]['signature']
        self._swept_at = now
