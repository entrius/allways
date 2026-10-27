"""Work that must land before a deadline, served most-urgent first."""

from __future__ import annotations

import heapq
import itertools
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple


class Urgency(IntEnum):
    """Lower runs first. Within a class, the earlier deadline runs first."""

    EXTEND = 0  # extend_reservation / extend_timeout — a miss is unrecoverable or slashes an honest miner
    VOTE = 1  # vote_initiate / confirm_swap — must reach quorum before the reservation or timeout lapses
    CRANK = 2  # resolve_pool / finalize_reservation — bounded by finalize_by
    SOFT = 3  # timeout, cancel, stale-claim reaps, weight votes, floor sweep


@dataclass(frozen=True)
class Intent:
    """One action the actuator should take. ``key`` identifies it (e.g. ``('extend_reservation', swap_key)``):
    re-planning the same action replaces the queued copy instead of duplicating it."""

    key: Tuple[Any, ...]
    urgency: Urgency
    deadline: float  # unix seconds by which it must have landed
    payload: Any = None
    not_before: float = 0.0  # unix seconds before which it must not be sent


@dataclass(order=True)
class _Entry:
    sort: Tuple[int, float, int]
    intent: Intent = field(compare=False)
    live: bool = field(default=True, compare=False)


class DeadlineQueue:
    """Thread-safe priority queue of Intents, deduplicated by key."""

    def __init__(self) -> None:
        self._heap: List[_Entry] = []
        self._by_key: Dict[Tuple[Any, ...], _Entry] = {}
        self._seq = itertools.count()
        self._lock = threading.Lock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_key)

    def push(self, intent: Intent) -> None:
        """Queue ``intent``, replacing any queued intent with the same key."""
        with self._lock:
            old = self._by_key.get(intent.key)
            if old is not None:
                old.live = False
            entry = _Entry((int(intent.urgency), intent.deadline, next(self._seq)), intent)
            self._by_key[intent.key] = entry
            heapq.heappush(self._heap, entry)

    def requeue(self, intent: Intent) -> None:
        """Put back an intent that was taken but not sent, unless a newer one with its key arrived meanwhile."""
        with self._lock:
            if intent.key in self._by_key:
                return
        self.push(intent)

    def discard(self, key: Tuple[Any, ...]) -> None:
        with self._lock:
            entry = self._by_key.pop(key, None)
            if entry is not None:
                entry.live = False

    def pop_due(self, now: float, limit: Optional[int] = None) -> List[Intent]:
        """Remove and return up to ``limit`` intents that may be sent at ``now``, most urgent first.
        Intents whose ``not_before`` is still in the future stay queued."""
        out: List[Intent] = []
        held: List[_Entry] = []
        with self._lock:
            while self._heap and (limit is None or len(out) < limit):
                entry = heapq.heappop(self._heap)
                if not entry.live:
                    continue
                if entry.intent.not_before > now:
                    held.append(entry)
                    continue
                del self._by_key[entry.intent.key]
                out.append(entry.intent)
            for entry in held:
                heapq.heappush(self._heap, entry)
        return out

    def snapshot(self) -> List[Intent]:
        with self._lock:
            return sorted((e.intent for e in self._by_key.values()), key=lambda i: (int(i.urgency), i.deadline))
