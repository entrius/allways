"""Program events, pushed live and gap-filled on reconnect within a bounded horizon.

Live events come off ``logsSubscribe``, whose payload already carries the log lines, so a quote re-post
costs no ``getTransaction``. After a disconnect, a restart, or a fresh database, ``Backfill`` reads what
was missed, but never further back than ``HISTORY_HORIZON_SECS`` and never more than one chunk per tick.

Records are NOT globally ordered (a backfilled event can arrive after newer live ones) and can repeat
across a restart. Every consumer must be order-tolerant and idempotent by (signature, event index)."""

from __future__ import annotations

import json
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, List, Optional, Protocol, Tuple

import bittensor as bt

from allways.solana.events import EventRecord
from allways.solana.program_feed import ProgramEventFeed, events_from_logs
from allways.validator_v2.runtime.bus import Topic
from allways.validator_v2.runtime.component import Component

# Longest swap life (10 min reservation TTL + 140 min reservation extensions + 10 min base timeout +
# 140 min timeout extensions ≈ 5h), plus margin. Nothing the validator decides needs older events.
HISTORY_HORIZON_SECS = 6 * 3600
BACKFILL_PAGES_PER_TICK = 5
BACKFILL_DECODES_PER_TICK = 300
SEEN_SIGNATURES = 50_000
# Solana slots are ~0.4s; between two block-time anchors the feed interpolates.
SLOT_ANCHOR_SECS = 30.0


class CursorStore(Protocol):
    def get(self) -> Optional[str]: ...

    def set(self, signature: str) -> None: ...


class FileCursorStore:
    """The newest signature whose events are all published. Written atomically (rename)."""

    def __init__(self, path: str) -> None:
        self.path = path

    def get(self) -> Optional[str]:
        try:
            with open(self.path) as f:
                return json.load(f).get('signature')
        except (OSError, ValueError):
            return None

    def set(self, signature: str) -> None:
        tmp = f'{self.path}.tmp'
        with open(tmp, 'w') as f:
            json.dump({'signature': signature}, f)
        os.replace(tmp, self.path)


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
    """Turns a slot into chain time without a per-event read. Anchors are ``getBlockTime(slot)`` for the
    newest slot seen, at most once per ``SLOT_ANCHOR_SECS``; times between anchors are interpolated, so
    an event is held until an anchor at or past its slot exists."""

    def __init__(self, block_time: Any, min_interval: float = SLOT_ANCHOR_SECS) -> None:
        self._block_time = block_time  # callable: slot -> unix seconds or None
        self._min_interval = min_interval
        self._anchors: List[Tuple[int, int]] = []  # (slot, unix), ascending
        self._fetched_at = 0.0

    def anchor(self, slot: int, now: float, force: bool = False) -> None:
        if not force and now - self._fetched_at < self._min_interval:
            return
        if self._anchors and slot <= self._anchors[-1][0]:
            return
        t = self._block_time(slot)
        self._fetched_at = now
        if t is not None:
            self._anchors.append((slot, int(t)))
            del self._anchors[:-64]

    def time_of(self, slot: int) -> Optional[int]:
        """Interpolated unix time for ``slot``, or None while no anchor reaches it yet."""
        prev: Optional[Tuple[int, int]] = None
        for a_slot, a_time in self._anchors:
            if a_slot == slot:
                return a_time
            if a_slot > slot:
                if prev is None:
                    return a_time  # older than the first anchor: clamp
                p_slot, p_time = prev
                return int(round(p_time + (slot - p_slot) * (a_time - p_time) / (a_slot - p_slot)))
            prev = (a_slot, a_time)
        return None


@dataclass(frozen=True)
class HistoryGap:
    """History older than ``complete_since`` (unix) is incomplete: a fresh start or an outage longer
    than the horizon. A consumer whose window reaches past it must treat that part as unknown."""

    complete_since: float


@dataclass
class _Pending:
    slot: int
    signature: str
    events: List[Tuple[str, Any]]


class LiveFeed(ProgramEventFeed):
    """ProgramEventFeed that keeps the signature and slot of each notification, for stamping."""

    def __init__(self, ws_url: str, program_id) -> None:
        super().__init__(ws_url, program_id)
        self._lock = threading.Lock()
        self._inbox: List[_Pending] = []

    def handle_notification(self, msg: dict) -> int:
        result = ((msg or {}).get('params') or {}).get('result') or {}
        value = result.get('value') or {}
        if value.get('err') is not None:
            return 0
        events = events_from_logs(value.get('logs'))
        slot = int((result.get('context') or {}).get('slot') or 0)
        with self._lock:
            self._inbox.append(_Pending(slot, str(value.get('signature')), events))
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

    def next_chunk(self, limit: int = BACKFILL_DECODES_PER_TICK) -> List[dict]:
        chunk, self._queue = self._queue[:limit], self._queue[limit:]
        return chunk

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
        cursor: CursorStore,
        out: Topic,
        gaps: Topic,
        program_id,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self._rpc_factory = rpc_factory
        self.live = live
        self.cursor = cursor
        self.out = out
        self.gaps = gaps  # publishes a HistoryGap when the horizon cut history short
        self.program_id = program_id
        self.seen = SeenSignatures()
        self._rpc: Any = None
        self._clock: Optional[SlotClock] = None
        self._held: List[_Pending] = []
        self._backfill: Optional[Backfill] = None
        self._session = 0
        self._rerun = False
        self._newest_live: Optional[str] = None

    def setup(self) -> None:
        self._rpc = self._rpc_factory()
        self._clock = SlotClock(lambda slot: self._rpc._call('getBlockTime', [slot]))
        self.live.start()
        self._backfill = Backfill(self._rpc, self.program_id, self.cursor.get(), self.clock())

    def tick(self, now: float) -> None:
        self._stamp_live(now)
        if self.live.session > max(self._session, 1):  # a re-subscribe: events since the cursor may be missing
            self._rerun = True
        self._session = self.live.session
        if self._backfill is None and self._rerun:
            self._rerun = False
            self._backfill = Backfill(self._rpc, self.program_id, self.cursor.get(), now)
        if self._backfill is not None:
            self._step_backfill(now)
        elif self._newest_live is not None:
            self.cursor.set(self._newest_live)

    def _stamp_live(self, now: float) -> None:
        self._held.extend(self.live.take())
        if not self._held:
            return
        self._clock.anchor(max(p.slot for p in self._held), now)
        still: List[_Pending] = []
        for p in self._held:
            t = self._clock.time_of(p.slot)
            if t is None:
                still.append(p)
                continue
            if self.seen.add(p.signature):
                self.out.publish_many([EventRecord(n, f, p.slot, t, p.signature) for n, f in p.events])
            self._newest_live = p.signature
        self._held = still

    def _step_backfill(self, now: float) -> None:
        bf = self._backfill
        if not bf.paging_done:
            bf.page()
            if not bf.paging_done:
                return
            if bf.truncated:
                self.gaps.publish(HistoryGap(bf.floor))
        for entry in bf.next_chunk():
            sig = entry['signature']
            if entry.get('err') is not None or sig in self.seen:
                continue
            tx = self._rpc.get_transaction(sig) or {}
            block_time = tx.get('blockTime') or entry.get('blockTime')
            logs = (tx.get('meta') or {}).get('logMessages') or []
            if self.seen.add(sig):
                self.out.publish_many(
                    [EventRecord(n, f, entry.get('slot'), block_time, sig) for n, f in events_from_logs(logs)]
                )
            self.cursor.set(sig)
        if bf.done:
            bt.logging.info('feed: backfill complete')
            self._backfill = None
