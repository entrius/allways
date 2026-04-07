"""Per-IP sliding-window rate limiter for user-facing axon endpoints.

Protects validators from request floods on unauthenticated endpoints (reserve, confirm)
where callers use dendrite-lite and have no registered hotkey. Runs in the blacklist
phase so rejected requests never reach chain queries or contract calls.
"""

import threading
import time
from collections import deque
from typing import Deque, Dict, Tuple

import bittensor as bt

from allways.constants import (
    RATE_LIMIT_CLEANUP_SECONDS,
    RATE_LIMIT_MAX_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
)


class AxonRateLimiter:
    """Sliding-window rate limiter keyed by caller IP.

    Tracks request timestamps per IP in a deque. On each check, timestamps
    outside the window are pruned and the remaining count is compared against
    the limit. Stale IP entries are garbage-collected periodically.
    """

    def __init__(
        self,
        max_requests: int = RATE_LIMIT_MAX_REQUESTS,
        window_seconds: float = RATE_LIMIT_WINDOW_SECONDS,
        cleanup_seconds: float = RATE_LIMIT_CLEANUP_SECONDS,
    ):
        self._lock = threading.Lock()
        self._requests: Dict[str, Deque[float]] = {}
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._cleanup_seconds = cleanup_seconds
        self._last_cleanup = time.monotonic()

    def is_allowed(self, ip: str) -> Tuple[bool, str]:
        """Check if a request from this IP is within the rate limit.

        Returns (allowed, reason). Records the request timestamp atomically
        if allowed, so check + record cannot race.
        """
        if not ip:
            return True, 'No IP available'

        now = time.monotonic()

        with self._lock:
            self._maybe_cleanup(now)
            timestamps = self._requests.get(ip)

            if timestamps is None:
                timestamps = deque()
                self._requests[ip] = timestamps

            # Prune timestamps outside the current window
            cutoff = now - self._window_seconds
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()

            if len(timestamps) >= self._max_requests:
                bt.logging.debug(
                    f'Rate limited {ip}: {len(timestamps)}/{self._max_requests} in {int(self._window_seconds)}s window'
                )
                return False, f'Rate limited — max {self._max_requests} requests per {int(self._window_seconds)}s'

            timestamps.append(now)
            return True, 'Allowed'

    def _maybe_cleanup(self, now: float) -> None:
        """Remove IPs with no recent activity to prevent unbounded memory growth."""
        if now - self._last_cleanup < self._cleanup_seconds:
            return

        self._last_cleanup = now
        cutoff = now - self._window_seconds
        stale = [ip for ip, ts in self._requests.items() if not ts or ts[-1] <= cutoff]
        for ip in stale:
            del self._requests[ip]

        if stale:
            bt.logging.debug(f'Rate limiter: purged {len(stale)} stale IP entries')

    def active_count(self) -> int:
        """Number of IPs currently tracked."""
        with self._lock:
            return len(self._requests)
