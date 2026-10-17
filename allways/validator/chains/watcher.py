"""One watcher per spoke chain: the only caller of that chain's endpoint.

It owns the chain's tip and verifies swap legs against it. Planners ask for a verdict and never wait:
they get the newest one this watcher has, and the request is queued if that is missing or stale. A leg
that has confirmed is settled for good; one still pending is re-checked when the tip moves (or after a
short recheck for mempool-visible chains), never once per tick for every swap."""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple

import bittensor as bt

from allways.assets.asset import ProviderUnreachableError
from allways.validator.runtime.bus import Latest
from allways.validator.runtime.component import Component

MEMPOOL_RECHECK_SECS = 12.0
REQUEST_IDLE_SECS = 600.0  # a leg nobody asked about for this long is forgotten
CHECKS_PER_TICK = 20


@dataclass(frozen=True)
class Probe:
    """One question for a chain's endpoint, asked on the watcher's thread. ``name`` and ``args`` are the
    cache key; ``run(asset)`` asks it; ``final(result)`` says the answer can never change (a confirmed leg,
    a price pinned to a block), so it is never asked again."""

    asset: str  # chain id of the asset to ask (e.g. 'btc', 'ethusdc')
    name: str
    args: Tuple[Any, ...]
    run: Callable[[Any], Any] = field(compare=False, repr=False)
    final: Callable[[Any], bool] = field(default=lambda result: False, compare=False, repr=False)


def verify_probe(
    asset: str, tx_hash: str, recipient: str, amount: int, block_hint: int = 0, sender: Optional[str] = None
) -> Probe:
    """``Asset.verify_transaction``: settled once the leg has confirmed."""
    return Probe(
        asset,
        'verify',
        (tx_hash, recipient, int(amount), int(block_hint), sender),
        run=lambda a: a.verify_transaction(
            tx_hash=tx_hash,
            expected_recipient=recipient,
            expected_amount=int(amount),
            block_hint=int(block_hint),
            expected_sender=sender,
        ),
        final=lambda info: info is not None and bool(getattr(info, 'confirmed', False)),
    )


@dataclass(frozen=True)
class Verdict:
    tip: Optional[int]  # the tip it was judged against
    checked_at: float
    info: Any = None  # the probe's answer (for a leg: TransactionInfo, or None = not found / did not match)
    error: Optional[str] = None  # the probe failed: unknown, not "no"
    unreachable: bool = False  # the failure was the endpoint being unreachable, not a bug or a bad answer
    settled: bool = False  # the answer can never change


class ChainWatcher(Component):
    """Not critical on its own: a dead endpoint means that chain's legs stay unknown, which planners
    treat as "not yet", and the supervisor logs it."""

    stall_secs = 300.0

    def __init__(self, chain_id: str, chain: Any, assets: Dict[str, Any], seconds_per_block: float, clock=time.time):
        super().__init__(clock)
        self.name = f'chain-{chain_id}'
        self.chain = chain  # allways.assets Chain: one tip for every asset on it
        self.assets = assets  # asset chain id → Asset on this chain
        self.interval_secs = min(30.0, max(2.0, seconds_per_block / 2))
        self.tip: Latest[int] = Latest()
        self._verdicts: Dict[Probe, Verdict] = {}
        self._wanted: 'OrderedDict[Probe, float]' = OrderedDict()  # request → last asked (wall time)
        self._lock = threading.Lock()

    # ── called from other threads ──────────────────────────────────────────

    def verdict(self, request: Probe) -> Optional[Verdict]:
        """The newest verdict for ``request`` (possibly from an older tip), or None if never asked.
        Asking keeps the request alive and queues a check when one is due."""
        with self._lock:
            self._wanted[request] = self.clock()
            self._wanted.move_to_end(request)
            return self._verdicts.get(request)

    # ── the watcher's own thread ────────────────────────────────────────────

    def tick(self, now: float) -> None:
        self.chain.clear_pass_tip()
        tip = self.chain.cached_block_height()  # the one tip read this tick; every check below shares it
        if tip is not None:
            self.tip.set(tip)
        for request in self._due(now, tip):
            self._check(request, tip, now)

    def _due(self, now: float, tip: Optional[int]) -> list:
        with self._lock:
            for request in [r for r, asked in self._wanted.items() if now - asked > REQUEST_IDLE_SECS]:
                del self._wanted[request]
                self._verdicts.pop(request, None)
            due = []
            for request in self._wanted:
                v = self._verdicts.get(request)
                if v is None or (
                    not v.settled and (v.tip != tip or v.error or now - v.checked_at >= MEMPOOL_RECHECK_SECS)
                ):
                    due.append(request)
            return due[:CHECKS_PER_TICK]

    def _check(self, request: Probe, tip: Optional[int], now: float) -> None:
        asset = self.assets.get(request.asset)
        if asset is None:
            verdict = Verdict(tip, now, error=f'no provider for {request.asset}')
        else:
            try:
                info = request.run(asset)
                verdict = Verdict(tip, now, info=info, settled=bool(request.final(info)))
            except Exception as e:
                bt.logging.debug(f'{self.name}: {request.name} {request.args[:1]} failed: {e}')
                verdict = Verdict(tip, now, error=str(e), unreachable=isinstance(e, ProviderUnreachableError))
        with self._lock:
            if request in self._wanted:
                self._verdicts[request] = verdict
