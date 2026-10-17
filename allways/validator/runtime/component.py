"""The unit the validator is built from: one job, one thread, one heartbeat."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

import bittensor as bt

MAX_ERROR_BACKOFF_SECS = 60.0


class Component:
    """Subclasses set ``name``/``interval_secs``/``stall_secs``/``critical`` and implement ``tick``.

    ``tick`` does one bounded unit of work and returns; the base loop calls it every ``interval_secs``
    and records a heartbeat after each one. An exception is logged and backed off, never fatal: whether a
    stalled or failing component should take the process down is the supervisor's call, not its own."""

    name: str = 'component'
    interval_secs: float = 12.0
    stall_secs: float = 120.0  # no heartbeat for this long = stalled
    critical: bool = False  # a stalled critical component restarts the process

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self.clock = clock
        self.last_heartbeat: float = time.monotonic()
        self.ticks = 0
        self.errors = 0
        self._consecutive_errors = 0
        self._thread: Optional[threading.Thread] = None

    def setup(self) -> None:
        """One-time work before the first tick, on the component's own thread."""

    def tick(self, now: float) -> None:
        raise NotImplementedError

    def heartbeat_age(self) -> float:
        return time.monotonic() - self.last_heartbeat

    def run(self, stop: threading.Event) -> None:
        self.setup()
        self.last_heartbeat = time.monotonic()
        while not stop.is_set():
            started = time.monotonic()
            try:
                self.tick(self.clock())
                self._consecutive_errors = 0
            except Exception as e:
                self.errors += 1
                self._consecutive_errors += 1
                bt.logging.warning(f'{self.name}: tick failed ({self._consecutive_errors} in a row): {e}')
            self.ticks += 1
            self.last_heartbeat = time.monotonic()
            stop.wait(max(0.0, self._next_delay() - (time.monotonic() - started)))

    def _next_delay(self) -> float:
        if not self._consecutive_errors:
            return self.interval_secs
        return min(MAX_ERROR_BACKOFF_SECS, self.interval_secs * (2 ** min(self._consecutive_errors, 6)))

    def start(self, stop: threading.Event) -> threading.Thread:
        self._thread = threading.Thread(target=self.run, args=(stop,), name=self.name, daemon=True)
        self._thread.start()
        return self._thread

    @property
    def alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
