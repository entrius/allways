"""Every HTTP endpoint the validator calls, rate-limited to its plan and metered per component.

The meter is a ``requests`` transport adapter mounted on each session, so providers keep their own code
and every request still passes through it. Endpoints are recognised by host. JSON-RPC calls are counted
by method, each call inside a batch separately (that is how Helius and Dwellir bill); other requests by
their first path segment. The calling component is the thread's name, since each component runs on a
thread named after it."""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import bittensor as bt
from requests.adapters import HTTPAdapter

from allways.solana.client import AllwaysSolanaClient
from allways.validator_v2.runtime.component import Component


class TokenBucket:
    """``rate`` requests/second sustained, ``burst`` at most. ``acquire`` blocks until enough tokens are free."""

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

    def acquire(self, n: int = 1) -> float:
        """Take ``n`` tokens (a batch larger than the burst waits for a full bucket, then overdraws).
        Returns the seconds spent waiting."""
        waited = 0.0
        need = min(n, self.burst)
        while True:
            with self._lock:
                now = self._clock()
                self._tokens = min(self.burst, self._tokens + (now - self._at) * self.rate)
                self._at = now
                if self._tokens >= need:
                    self._tokens -= n
                    return waited
                wait = (need - self._tokens) / self.rate
            self._sleep(wait)
            waited += wait


@dataclass(frozen=True)
class Endpoint:
    name: str  # e.g. 'helius', 'dwellir-eth', 'blockstream'
    host: str  # matched against the request URL's host
    rate: float  # requests/second this process may use
    burst: float
    daily_budget: Optional[int] = None  # warn when the projected day exceeds it


def calls_in(body: Optional[bytes], path: str) -> List[str]:
    """The billable calls in one HTTP request: each JSON-RPC method (batches count per call), else the
    first path segment."""
    if body:
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            payload = None
        if isinstance(payload, dict) and 'method' in payload:
            return [str(payload['method'])]
        if isinstance(payload, list):
            return [str(p.get('method', '?')) for p in payload if isinstance(p, dict)] or ['batch']
    segment = path.strip('/').split('/', 1)[0]
    return [f'/{segment}' if segment else '/']


class Meter:
    """Usage per (endpoint, component, call), and one TokenBucket per configured endpoint. A host with
    no configured endpoint is counted under its own name but never throttled."""

    def __init__(self, endpoints: List[Endpoint], clock: Callable[[], float] = time.time) -> None:
        self._by_host: Dict[str, Endpoint] = {e.host: e for e in endpoints}
        self._buckets: Dict[str, TokenBucket] = {e.name: TokenBucket(e.rate, e.burst) for e in endpoints}
        self._usage: Counter = Counter()
        self._throttled: Counter = Counter()  # 429s, per endpoint
        self._lock = threading.Lock()
        self._clock = clock
        self.since = clock()

    def endpoint(self, url: str) -> Tuple[str, Optional[Endpoint]]:
        host = urlsplit(url).hostname or '?'
        ep = self._by_host.get(host)
        return (ep.name if ep else host), ep

    def charge(self, url: str, body: Optional[bytes]) -> str:
        name, ep = self.endpoint(url)
        calls = calls_in(body, urlsplit(url).path)
        if ep is not None:
            self._buckets[ep.name].acquire(len(calls))
        component = threading.current_thread().name
        with self._lock:
            for call in calls:
                self._usage[(name, component, call)] += 1
        return name

    def note_status(self, endpoint: str, status: int) -> None:
        if status == 429:
            with self._lock:
                self._throttled[endpoint] += 1

    def usage(self) -> Dict[Tuple[str, str, str], int]:
        with self._lock:
            return dict(self._usage)

    def totals(self) -> Dict[str, int]:
        out: Counter = Counter()
        for (endpoint, _, _), n in self.usage().items():
            out[endpoint] += n
        return dict(out)

    def projected_daily(self, now: Optional[float] = None) -> Dict[str, int]:
        elapsed = max(1.0, (now or self._clock()) - self.since)
        return {ep: int(n * 86400 / elapsed) for ep, n in self.totals().items()}

    def attach(self, session: Any) -> Any:
        """Meter every request ``session`` makes. Returns the session."""
        adapter = MeteredAdapter(self)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        return session

    def solana_client(self, url: str, program_id=None, keypair=None) -> AllwaysSolanaClient:
        """A program client whose requests are metered. One per component: sessions aren't thread-safe."""
        client = AllwaysSolanaClient(url, program_id=program_id, keypair=keypair)
        self.attach(client.rpc._session)
        return client


class MeteredAdapter(HTTPAdapter):
    def __init__(self, meter: Meter) -> None:
        super().__init__()
        self.meter = meter

    def send(self, request, *args, **kwargs):
        body = request.body.encode() if isinstance(request.body, str) else request.body
        endpoint = self.meter.charge(request.url, body)
        response = super().send(request, *args, **kwargs)
        self.meter.note_status(endpoint, response.status_code)
        return response


# WebSocket traffic: subtensor (Dwellir, finney fallback) and the Solana logsSubscribe feed.
UNMETERED = 'websocket traffic (subtensor, Solana log subscription)'


class UsageReport(Component):
    """Logs each endpoint's usage and projected day once an hour, warning when a budget would be exceeded."""

    name = 'usage'
    interval_secs = 3600.0
    stall_secs = 3 * 3600.0

    def __init__(self, meter: Meter, endpoints: List[Endpoint], clock=time.time) -> None:
        super().__init__(clock)
        self.meter = meter
        self.budgets = {e.name: e.daily_budget for e in endpoints if e.daily_budget}

    def tick(self, now: float) -> None:
        projected = self.meter.projected_daily(now)
        for endpoint, total in sorted(self.meter.totals().items()):
            day = projected.get(endpoint, 0)
            budget = self.budgets.get(endpoint)
            line = f'usage {endpoint}: {total} requests so far, ~{day}/day projected'
            if budget and day > budget:
                bt.logging.warning(f'{line} — over its {budget}/day budget')
            else:
                bt.logging.info(line + (f' (budget {budget}/day)' if budget else ''))
        bt.logging.info(f'usage: {UNMETERED} not metered')
