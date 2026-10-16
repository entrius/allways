"""The one way any component reaches the Solana RPC: rate-limited to the plan, metered per component,
and safe to use from every thread (each thread gets its own HTTP session)."""

from __future__ import annotations

import threading
import time
from collections import Counter
from typing import Any, Callable, Dict, Tuple

from allways.solana.client import AllwaysSolanaClient
from allways.solana.rpc import SolanaRpc


class TokenBucket:
    """``rate`` requests/second sustained, ``burst`` at most. ``acquire`` blocks until a token is free."""

    def __init__(
        self,
        rate: float,
        burst: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0 or burst < 1:
            raise ValueError('rate must be > 0 and burst >= 1')
        self.rate = rate
        self.burst = burst
        self._clock = clock
        self._sleep = sleep
        self._tokens = burst
        self._at = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.burst, self._tokens + (now - self._at) * self.rate)
        self._at = now

    def acquire(self) -> float:
        """Take one token; returns the seconds spent waiting for it."""
        waited = 0.0
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return waited
                wait = (1 - self._tokens) / self.rate
            self._sleep(wait)
            waited += wait


class MeteredRpc(SolanaRpc):
    """A SolanaRpc whose every request takes a gateway token and is counted against its component."""

    def __init__(self, url: str, gateway: 'RpcGateway', component: str, timeout: int = 30) -> None:
        super().__init__(url, timeout=timeout)
        self._gateway = gateway
        self._component = component

    def _call(self, method: str, params: list) -> Any:
        self._gateway._charge(self._component, method)
        return super()._call(method, params)


class RpcGateway:
    """Hands each component a MeteredRpc. Requests share one TokenBucket sized to the provider plan, so a
    backfill can never starve the actuator of the plan's headroom without it showing up in ``usage()``."""

    def __init__(self, url: str, rate: float, burst: float) -> None:
        self.url = url
        self.bucket = TokenBucket(rate, burst)
        self._local = threading.local()
        self._usage: Counter = Counter()
        self._lock = threading.Lock()

    def rpc(self, component: str) -> MeteredRpc:
        """This thread's client for ``component``. requests.Session isn't thread-safe, so never share one."""
        cache: Dict[str, MeteredRpc] = getattr(self._local, 'clients', None) or {}
        self._local.clients = cache
        if component not in cache:
            cache[component] = MeteredRpc(self.url, self, component)
        return cache[component]

    def client(self, component: str, program_id=None, keypair=None) -> AllwaysSolanaClient:
        """A program client for ``component`` whose requests go through this gateway. Call it from the
        component's own thread (in ``setup``), since it binds that thread's session."""
        client = AllwaysSolanaClient(self.url, program_id=program_id, keypair=keypair)
        client.rpc = self.rpc(component)
        return client

    def _charge(self, component: str, method: str) -> None:
        self.bucket.acquire()
        with self._lock:
            self._usage[(component, method)] += 1

    def usage(self) -> Dict[Tuple[str, str], int]:
        with self._lock:
            return dict(self._usage)
