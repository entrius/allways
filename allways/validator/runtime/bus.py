"""How components talk: a latest-value cell for snapshots and a fan-out topic for streams."""

from __future__ import annotations

import threading
from collections import deque
from typing import Deque, Dict, Generic, List, Optional, Tuple, TypeVar

T = TypeVar('T')


class Latest(Generic[T]):
    """The newest value of something one component produces and others read (e.g. the World).

    Readers never block the writer and always see a whole value. ``version`` increases on every set,
    so a reader can tell a fresh value from one it has already acted on."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._value: Optional[T] = None
        self._version = 0

    def set(self, value: T) -> int:
        with self._cond:
            self._value = value
            self._version += 1
            self._cond.notify_all()
            return self._version

    def get(self) -> Tuple[Optional[T], int]:
        with self._cond:
            return self._value, self._version

    def wait_newer(self, version: int, timeout: Optional[float] = None) -> Tuple[Optional[T], int]:
        """Block until a value newer than ``version`` is set, or ``timeout`` passes."""
        with self._cond:
            self._cond.wait_for(lambda: self._version > version, timeout)
            return self._value, self._version


class Subscription(Generic[T]):
    """One subscriber's bounded buffer. When full, the oldest item is dropped and counted, so a slow
    consumer loses data visibly instead of stalling the publisher."""

    def __init__(self, name: str, maxsize: int) -> None:
        self.name = name
        self._items: Deque[T] = deque(maxlen=maxsize)
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self.dropped = 0

    def _put(self, item: T) -> None:
        with self._lock:
            if len(self._items) == self._items.maxlen:
                self.dropped += 1
            self._items.append(item)
            self._ready.set()

    def drain(self, limit: Optional[int] = None) -> List[T]:
        with self._lock:
            n = len(self._items) if limit is None else min(limit, len(self._items))
            out = [self._items.popleft() for _ in range(n)]
            if not self._items:
                self._ready.clear()
            return out

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._ready.wait(timeout)


class Topic(Generic[T]):
    """A stream published by one component and fanned out to every subscriber's own buffer."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._subs: Dict[str, Subscription[T]] = {}
        self._lock = threading.Lock()

    def subscribe(self, name: str, maxsize: int = 100_000) -> Subscription[T]:
        with self._lock:
            if name in self._subs:
                raise ValueError(f'{self.name}: subscriber {name!r} already registered')
            sub: Subscription[T] = Subscription(name, maxsize)
            self._subs[name] = sub
            return sub

    def publish(self, item: T) -> None:
        with self._lock:
            subs = list(self._subs.values())
        for sub in subs:
            sub._put(item)

    def publish_many(self, items: List[T]) -> None:
        for item in items:
            self.publish(item)
