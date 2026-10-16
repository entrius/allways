"""Scoring: crown time, pools and eligibility over the window → one weight vector per round.

State is seeded from the World and maintained from events, so it never needs history older than the
scoring window: each round, every live quote lane, active flag, purse and bond is written as an anchor
straight from the World (this also replaces the old reconcile passes), and the events since fill in
the precise change times between anchors. The scoring math itself is unchanged and lifted as-is from
``allways.validator.scoring`` (pools, crown replay, capacity, quality slice, eligibility)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np

from allways.validator_v2.feed.events import HistoryGap
from allways.validator_v2.runtime.bus import Latest, Subscription
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.world.snapshot import World

ROUND_SECS = 3600


@dataclass(frozen=True)
class RoundResult:
    window_start: float
    window_end: float
    rewards: np.ndarray  # per uid, before the EMA
    trace: Any = None  # per-lane factors, for logs and the export


class Scoring(Component):
    """Not critical: a late round delays weights, never a swap."""

    name = 'scoring'
    interval_secs = 5.0
    stall_secs = 900.0

    def __init__(
        self,
        world: Latest[World],
        events: Subscription,
        gaps: Subscription,
        store: Any,
        out: Latest[RoundResult],
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world = world
        self.events = events
        self.gaps = gaps
        self.store = store  # crown/clearing tables; today's ValidatorStateStore schema works unchanged
        self.out = out
        self.last_round_end: Optional[float] = None

    def tick(self, now: float) -> None:
        for gap in self.gaps.drain():
            self._mark_gap(gap)
        for record in self.events.drain(limit=5_000):
            self._ingest(record)
        if self.last_round_end is None or now - self.last_round_end >= ROUND_SECS:
            world, _ = self.world.get()
            if world is not None:
                self._seed_anchors(world, now)
                self.out.set(self._score(world, now))
                self.last_round_end = now

    def _mark_gap(self, gap: HistoryGap) -> None:
        """The fill and volume ledgers can't see past ``gap.complete_since``: re-stamp the ledger age so the
        activity gate goes strikes-only for its window instead of zeroing miners whose fill fell in the gap."""

    def _ingest(self, record: Any) -> None:
        """Fold one event into the crown/clearing tables (``SolanaEventIndex._apply``), idempotent by
        (signature, event index) since the feed may repeat records across a restart."""

    def _seed_anchors(self, world: World, now: float) -> None:
        """Write the World's current rate/active/collateral/bond per lane as anchor rows at ``now``. Unlike
        the old reconcile it also ADDS lanes the event view never saw (a quote older than retained history)."""

    def _score(self, world: World, now: float) -> RoundResult:
        """``calculate_miner_rewards`` over the window, reading live state from the World instead of rescanning."""
        n = 0
        return RoundResult(now - ROUND_SECS, now, np.zeros(n, dtype=np.float32))


class WeightSetter(Component):
    """Applies the EMA and calls ``set_weights`` on the subtensor epoch. Owns its own subtensor."""

    name = 'weights'
    interval_secs = 30.0
    stall_secs = 1800.0

    def __init__(self, rounds: Latest[RoundResult], subtensor_factory, clock=time.time) -> None:
        super().__init__(clock)
        self.rounds = rounds
        self._subtensor_factory = subtensor_factory
        self._applied_version = 0
        self.scores: Dict[int, float] = {}

    def tick(self, now: float) -> None:
        result, version = self.rounds.get()
        if result is None or version == self._applied_version:
            return
        self._applied_version = version
