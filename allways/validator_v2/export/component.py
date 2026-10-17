"""Dashboard export (opt-in, ``STORE_DB_RESULTS``): each scoring round's result, written to Postgres for
das-allways.

It writes what the round computed and nothing more: the crown intervals, the score rows and the pools, the
crown holders as of the round's end, and those rows again as the live score tip. A round the database
refused is kept and retried, oldest first, so a slow or down database never touches scoring."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

import bittensor as bt

from allways.chains import canonical_pair
from allways.constants import DIRECTION_POOLS, declarable_backings
from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.scoring.rewards import Lane, RoundResult

MAX_HELD_ROUNDS = 24  # a day of rounds; older ones are dropped rather than held through a long outage


def crown_rows(result: RoundResult) -> Dict[Lane, List[Tuple]]:
    """``crown_holders`` rows per lane: one per holder per interval, with its depth-split credit."""
    return {
        lane: [(lo, hi, *lane, hk, share, rate) for lo, hi, shares, rate in intervals for hk, share in shares.items()]
        for lane, intervals in result.intervals.items()
    }


def score_rows(result: RoundResult) -> List[Tuple]:
    """``miner_scores`` rows, keyed by the round's end."""
    return [
        (result.window_end, r.hotkey, r.from_chain, r.to_chain, r.backing, r.eligible, r.pool)
        + (r.crown_share, r.capacity, r.reward, r.qvol_share)
        for r in result.rows
    ]


def pool_rows(result: RoundResult) -> List[Tuple]:
    """``direction_pools`` rows for every paid or live lane. ``live`` is the pair's liveness (qualified volume
    on any of its lanes), the same test the pools pay on, not ``pool > 0``."""
    pair_volume: Dict[Tuple[str, str], int] = {}
    for (f, t, _), trace in result.traces.items():
        pair = canonical_pair(f, t)
        pair_volume[pair] = pair_volume.get(pair, 0) + int(trace.qualified_volume)
    rows = []
    for (f, t, b), trace in result.traces.items():
        live = pair_volume[canonical_pair(f, t)] > 0
        if trace.pool > 0 or live:
            rows.append((result.window_end, f, t, b, trace.pool, int(trace.qualified_volume), live))
    return rows


def current_crown(result: RoundResult) -> Dict[Lane, List[Tuple]]:
    """Who holds each lane's crown at the round's end: the holders of an interval that reaches it."""
    out: Dict[Lane, List[Tuple]] = {}
    for lane, intervals in result.intervals.items():
        last = intervals[-1] if intervals else None
        held = last is not None and last[1] >= result.window_end
        out[lane] = [(*lane, hk, share, last[3], result.window_end) for hk, share in last[2].items()] if held else []
    return out


class Export(Component):
    """Not critical: a lagging export only leaves the dashboard behind."""

    name = 'export'
    interval_secs = 30.0
    stall_secs = 1800.0

    def __init__(self, rounds: Latest[RoundResult], storage: Any, clock=time.time) -> None:
        super().__init__(clock)
        self.rounds, self.storage = rounds, storage
        self._seen = 0
        self._held: List[RoundResult] = []

    def tick(self, now: float) -> None:
        result, version = self.rounds.get()
        if result is not None and version != self._seen:
            self._seen = version
            self._held = (self._held + [result])[-MAX_HELD_ROUNDS:]
        while self._held and self._write(self._held[0]):
            self._held.pop(0)

    def _write(self, result: RoundResult) -> bool:
        window = (result.window_start, result.window_end)
        if result.halted:
            lanes = [(f, t, b) for f, t in DIRECTION_POOLS for b in declarable_backings(f, t)]
            done = self.storage.flush_halt_window(lanes, *window, max_ts=result.window_end).success
            return done and self.storage.upsert_current_crown_snapshot({lane: [] for lane in lanes}).success
        flushed = self.storage.flush_scoring_window(
            crown_rows_by_direction=crown_rows(result),
            crown_window_bounds_by_direction={lane: window for lane in result.traces},
            miner_score_rows=score_rows(result),
            crown_holders_max_ts=result.window_end,
            direction_pool_rows=pool_rows(result),
        )
        if not flushed.success:
            return False
        if result is self._held[-1]:  # the tips show the newest round only
            tips = self.storage.upsert_current_crown_snapshot(current_crown(result)).success
            tips = self.storage.replace_current_miner_scores(score_rows(result)).success and tips
            if not tips:
                bt.logging.warning('export: round written, live tips not refreshed')
        return True
