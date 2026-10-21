"""The dashboard export: what a round becomes in the dashboard tables, and a database outage held, not lost."""

from types import SimpleNamespace

import numpy as np

from allways.validator.export.component import Export, crown_rows, pool_rows, score_rows
from allways.validator.runtime.bus import Latest
from allways.validator.scoring.rewards import DirectionTrace, RoundResult, ScoreRow

LANE, DEAD = ('btc', 'sol', 'sol'), ('sol', 'btc', 'sol')


def _round(end=2000, halted=False):
    traces = {LANE: DirectionTrace(pool=0.1, qualified_volume=5), DEAD: DirectionTrace(pool=0.0)}
    rows = (ScoreRow('hk_a', *LANE, True, 0.1, 1.0, 0.5, 0.04, 0.0),)
    intervals = {LANE: [(1000, 1500, {'hk_b': 1.0}, 2.0), (1500, end, {'hk_a': 0.6, 'hk_b': 0.4}, 1.0)], DEAD: []}
    return RoundResult(1000, end, ('hk_a', 'hk_b'), np.zeros(2), halted, rows, traces, intervals)


def test_a_round_becomes_its_crown_intervals_scores_and_pools():
    result = _round()
    assert crown_rows(result)[LANE][-1] == (1500, 2000, *LANE, 'hk_b', 0.4, 1.0)
    assert crown_rows(result)[DEAD] == []  # the lane's window is still rewritten: nobody held it
    assert score_rows(result) == [(2000, 'hk_a', *LANE, True, 0.1, 1.0, 0.5, 0.04, 0.0)]
    # Liveness is the pair's: the reverse direction cleared nothing but its pair is live, so it is listed.
    assert pool_rows(result) == [(2000, *LANE, 0.1, 5, True), (2000, *DEAD, 0.0, 0, True)]


class _Storage:
    def __init__(self):
        self.up, self.calls = False, []

    def _ok(self, name):
        self.calls.append(name)
        return SimpleNamespace(success=self.up)

    def flush_scoring_window(self, **kw):
        return self._ok(('flush', kw['crown_holders_max_ts']))

    def flush_halt_window(self, lanes, start, end, max_ts):
        return self._ok(('halt', max_ts))

    def upsert_current_crown_snapshot(self, rows):
        return self._ok('crown')

    def replace_current_miner_scores(self, rows):
        return self._ok('scores')


def test_rounds_the_database_refused_are_written_oldest_first_once_it_is_back():
    rounds, storage = Latest(), _Storage()
    export = Export(rounds, Latest(), storage)
    rounds.set(_round(end=2000))
    export.tick(0)
    rounds.set(_round(end=3000, halted=True))
    export.tick(1)
    storage.up, storage.calls = True, []
    export.tick(2)
    assert storage.calls == [('flush', 2000), ('halt', 3000)]  # a halted newest round has no score tip
    export.tick(3)
    assert storage.calls == [('flush', 2000), ('halt', 3000)]  # nothing held, nothing rewritten
