"""Spoke-chain leg verification off the decision path: one worker pool per chain, results cached.

Planners never call a provider. They ask for a verdict; if it isn't ready they plan nothing for that
swap this tick and check again next tick. A slow chain only delays its own swaps."""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, Hashable, Optional, Tuple

WORKERS_PER_CHAIN = 2


class LegVerifier:
    def __init__(self, verify: Callable[[str, Hashable], Any], workers_per_chain: int = WORKERS_PER_CHAIN) -> None:
        # verify(chain, request) -> verdict; request is whatever the provider needs, and must be hashable.
        self._verify = verify
        self._workers = workers_per_chain
        self._pools: Dict[str, ThreadPoolExecutor] = {}
        self._jobs: Dict[Tuple[str, Hashable], Future] = {}
        self._lock = threading.Lock()

    def _pool(self, chain: str) -> ThreadPoolExecutor:
        if chain not in self._pools:
            self._pools[chain] = ThreadPoolExecutor(self._workers, thread_name_prefix=f'legs-{chain}')
        return self._pools[chain]

    def verdict(self, chain: str, request: Hashable) -> Optional[Any]:
        """The verdict if ready, else None (and the check is started). A failed check is retried."""
        key = (chain, request)
        with self._lock:
            job = self._jobs.get(key)
            if job is None or (job.done() and job.exception() is not None):
                self._jobs[key] = self._pool(chain).submit(self._verify, chain, request)
                return None
        return job.result() if job.done() else None

    def forget(self, chain: str, request: Hashable) -> None:
        """Drop a cached verdict, e.g. when the chain tip moved and a pending leg should be re-checked."""
        with self._lock:
            self._jobs.pop((chain, request), None)

    def shutdown(self) -> None:
        for pool in self._pools.values():
            pool.shutdown(wait=False, cancel_futures=True)
