"""Scoring as a component: when a round runs, what it reads from the World, and the weights it feeds."""

from types import SimpleNamespace

import numpy as np

from allways.constants import RATE_PRECISION, RECYCLE_UID, SCORING_WINDOW_SECS
from allways.solana import pdas
from allways.solana.events import EventRecord
from allways.validator.chains.subtensor_view import Head
from allways.validator.chains.watcher import Verdict
from allways.validator.feed.events import FeedStatus, HistoryGap
from allways.validator.runtime.bus import Latest, Topic
from allways.validator.scoring.component import LAST_SCORED, RoundResult, Scoring, WeightSetter
from allways.validator.scoring.fee_pool import MINER_SHARE
from allways.validator.scoring.ledger import LEDGER_SINCE, RATE
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World

NOW = 1_000_000
HOTKEYS = ['hk_a', 'hk_b'] + [f'filler{i}' for i in range(RECYCLE_UID)]
RATE_FIXED = int(0.0002 * RATE_PRECISION)


def _miner(pk):
    bits = pdas.BACKING_BIT_SOL
    return SimpleNamespace(
        miner=pk, active=True, active_backings=bits, collateral=0, failed_swaps=0, successful_swaps=0, settling_until=0
    )


def _quote(pk, rate=RATE_FIXED):
    return SimpleNamespace(
        miner=pk,
        from_chain='btc',
        to_chain='sol',
        collateral_chain='sol',
        rate=rate,
        miner_from_addr='',
        miner_to_addr='',
    )


def _world(quotes=(), complete=True, halted=False):
    config = SimpleNamespace(
        halted=halted,
        reservation_ttl_secs=600,
        min_swap_amount=0,
        max_swap_amount=0,
        tao_min_swap_amount=0,
        tao_max_swap_amount=0,
    )
    return World(
        taken_at=NOW,
        slot=10,
        complete=complete,
        config=config,
        accounts={
            'MinerState': {'ms_a': _miner('pk_a'), 'ms_b': _miner('pk_b')},
            'MinerQuote': {f'q{i}': q for i, q in enumerate(quotes)},
        },
        attribution={'pk_a': 'hk_a', 'pk_b': 'hk_b'},
    )


def _head(block=100, last_update=0):
    metagraph = SimpleNamespace(
        n=SimpleNamespace(item=lambda: len(HOTKEYS)),
        hotkeys=list(HOTKEYS),
        uids=np.arange(len(HOTKEYS)),
        last_update=[last_update] * len(HOTKEYS),
    )
    return Head(block, NOW, metagraph, NOW)


class _Watcher:
    def __init__(self, answer=None):
        self.answer, self.asked = answer, []

    def verdict(self, probe):
        self.asked.append(probe.args)
        return self.answer


def _scoring(tmp_path, world, catching_up=False, watchers=None, db='v.db'):
    w, h, status, rounds = Latest(), Latest(), Latest(), Latest()
    w.set(world)
    h.set(_head())
    status.set(FeedStatus(catching_up, 1, NOW, (5, 'sig5')))
    events, gaps = Topic('events'), Topic('gaps')
    store = Store(str(tmp_path / db))
    with store.tx() as c:  # pin the fee share at 1.0: no subnet read here, so each round keeps it and pays in full
        Store.set_meta(c, MINER_SHARE, '1.0')
    scoring = Scoring(
        w, h, events.subscribe('scoring'), gaps.subscribe('scoring'), status, store, rounds, {}, watchers or {}
    )
    return scoring, events, rounds, store, w, status


def _quote_set(pk, at, slot=5, **addresses):
    fields = {
        'miner': pk,
        'from_chain': 'btc',
        'to_chain': 'sol',
        'collateral_chain': 'sol',
        'rate': RATE_FIXED,
        **addresses,
    }
    return EventRecord('QuoteSet', fields, slot, at, f'sig{at}')


def test_no_round_while_the_feed_is_catching_up(tmp_path):
    scoring, _, rounds, *_ = _scoring(tmp_path, _world(), catching_up=True)
    scoring.tick(NOW)
    assert rounds.get()[0] is None


def test_a_round_runs_once_caught_up_and_then_once_per_window(tmp_path):
    scoring, _, rounds, store, *_ = _scoring(tmp_path, _world())
    scoring.tick(NOW)
    first, version = rounds.get()
    assert first.window_end == NOW and first.hotkeys == tuple(HOTKEYS)
    scoring.tick(NOW + 60)
    assert rounds.get()[1] == version
    scoring.tick(NOW + SCORING_WINDOW_SECS)
    assert rounds.get()[0].window_start == NOW  # rounds tile
    assert int(store.get_meta(LAST_SCORED)) == NOW + SCORING_WINDOW_SECS


def test_a_restart_resumes_the_window_where_the_last_round_ended(tmp_path):
    scoring, *_ = _scoring(tmp_path, _world())
    scoring.tick(NOW)
    again, _, rounds, *_ = _scoring(tmp_path, _world())
    again.tick(NOW + 600)
    assert rounds.get()[0] is None  # the scores it already folded carry over; the window isn't up
    again.tick(NOW + SCORING_WINDOW_SECS)
    assert rounds.get()[0].window_start == NOW


def test_a_quote_older_than_history_is_anchored_from_the_world_and_earns_from_then(tmp_path):
    scoring, _, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quote('pk_a')]))
    scoring.tick(NOW)  # the first round anchors the lane at NOW: no crown time before it
    assert rounds.get()[0].rewards[0] == 0
    scoring.tick(NOW + SCORING_WINDOW_SECS)
    assert rounds.get()[0].rewards[0] > 0


def test_events_of_unbound_miners_are_skipped_and_the_feed_position_is_acked(tmp_path):
    scoring, events, _, store, *_ = _scoring(tmp_path, _world(), catching_up=True)
    events.publish_many([_quote_set('pk_a', NOW - 10), _quote_set('pk_unbound', NOW - 10)])
    scoring.tick(NOW)
    rows = store.conn().execute('SELECT hotkey FROM scoring_events WHERE kind = ?', (RATE,)).fetchall()
    assert [r['hotkey'] for r in rows] == ['hk_a']
    assert tuple(store.conn().execute("SELECT slot, signature FROM acks WHERE consumer = 'scoring'").fetchone()) == (
        5,
        'sig5',
    )


def test_a_quote_waits_for_its_address_check_and_a_refused_address_zeroes_and_bans(tmp_path):
    watcher = _Watcher()
    scoring, events, rounds, store, *_ = _scoring(tmp_path, _world(), watchers={'btc': watcher})
    events.publish_many(
        [_quote_set('pk_a', NOW - 10, miner_from_addr='bc1qrefused', miner_to_addr='', liquidity=0, updated_at=0)]
    )
    scoring.tick(NOW)
    assert watcher.asked == [('bc1qrefused',)]
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW) == {}
    assert rounds.get()[0] is not None  # the round runs: only the held quote's lane waits
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW) == {}
    watcher.answer = Verdict(1, NOW, info=True)
    scoring.tick(NOW + 5)
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW) == {'hk_a': (0.0, NOW - 10)}
    assert scoring.ledger.address_banned_hotkeys(NOW - 20, NOW) == {'hk_a'}


def test_quotes_waiting_on_their_address_checks_never_hold_back_a_round(tmp_path):
    """A fresh QuoteSet every few seconds over idle addresses keeps one quote waiting on its check at all times:
    rounds still run on schedule."""
    watcher = _Watcher()  # never answers: every quote waits its full JUDGE_WAIT_SECS
    scoring, events, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quote('pk_a')]), watchers={'btc': watcher})
    for i, t in enumerate(range(NOW, NOW + SCORING_WINDOW_SECS + 1, 15)):
        events.publish(_quote_set('pk_a', t - 1, slot=10 + i, miner_from_addr=f'bc1qidle{i}', miner_to_addr=''))
        scoring.tick(t)
        assert scoring._held  # one is always waiting
    assert rounds.get()[0].window_end == NOW + SCORING_WINDOW_SECS  # both rounds ran


def test_a_round_leaves_a_lane_whose_quote_waits_on_its_check_uncredited(tmp_path):
    """The quote book already shows the new rate; until its address is judged the reconcile must not write it."""
    watcher = _Watcher()
    scoring, events, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quote('pk_a')]), watchers={'btc': watcher})
    events.publish(_quote_set('pk_a', NOW - 1, miner_from_addr='bc1qunjudged', miner_to_addr=''))
    scoring.tick(NOW)
    assert scoring._held and rounds.get()[0] is not None
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW) == {}


def _quoted(address='bc1qminer'):
    quote = _quote('pk_a')
    quote.miner_from_addr = address
    return quote


def test_a_live_quote_whose_address_starts_refusing_loses_its_crown_and_bans_the_miner(tmp_path):
    """Judged usable when quoted, refusing later (say a delegation set afterwards): the re-check catches it."""
    watcher = _Watcher(Verdict(1, NOW, info=False, settled=True))
    scoring, _, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quoted()]), watchers={'btc': watcher})
    scoring.tick(NOW)
    scoring.tick(NOW + SCORING_WINDOW_SECS)
    assert rounds.get()[0].rewards[0] > 0
    watcher.answer = Verdict(1, NOW + SCORING_WINDOW_SECS, info=True, settled=True)  # the re-check: now refusing
    scoring.tick(NOW + 2 * SCORING_WINDOW_SECS)
    result = rounds.get()[0]
    assert result.rewards[0] == 0
    assert scoring.ledger.address_banned_hotkeys(NOW, NOW + 2 * SCORING_WINDOW_SECS) == {'hk_a'}
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW + 2 * SCORING_WINDOW_SECS) == {
        'hk_a': (0.0, NOW + 2 * SCORING_WINDOW_SECS)
    }


def test_the_quote_book_adds_a_lane_only_once_its_address_is_judged_usable(tmp_path):
    watcher = _Watcher()  # not answered yet
    scoring, _, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quoted()]), watchers={'btc': watcher})
    scoring.tick(NOW)
    assert rounds.get()[0] is not None
    assert scoring.ledger.get_latest_rates_before('btc', 'sol', NOW) == {}  # not credited unjudged
    assert ('bc1qminer',) in watcher.asked
    watcher.answer = Verdict(1, NOW, info=False, settled=True)
    scoring.tick(NOW + SCORING_WINDOW_SECS)
    assert set(scoring.ledger.get_latest_rates_before('btc', 'sol', NOW + SCORING_WINDOW_SECS)) == {'hk_a'}


def test_a_halted_program_recycles_the_whole_pool(tmp_path):
    scoring, _, rounds, *_ = _scoring(tmp_path, _world(quotes=[_quote('pk_a')], halted=True))
    scoring.tick(NOW)
    result = rounds.get()[0]
    assert result.halted and result.rewards[RECYCLE_UID] == 1.0 and result.rewards.sum() == 1.0


# ── weights ───────────────────────────────────────────────────────────────────


class _Subtensor:
    def __init__(self):
        self.sets = []

    def set_weights(self, **kwargs):
        self.sets.append(kwargs)
        return True, ''


def _setter(tmp_path, alpha=0.5, last_update=0, block=200):
    rounds, head = Latest(), Latest()
    head.set(_head(block=block, last_update=last_update))
    wallet = SimpleNamespace(hotkey=SimpleNamespace(ss58_address='hk_b'))
    subtensor = _Subtensor()
    setter = WeightSetter(rounds, head, lambda: subtensor, Store(str(tmp_path / 'w.db')), wallet, 19, alpha, 150)
    setter.setup()
    return setter, rounds, head, subtensor


def _round(hotkeys, rewards):
    return RoundResult(0, NOW, tuple(hotkeys), np.array(rewards, dtype=np.float32))


def test_the_moving_average_restarts_a_uid_whose_hotkey_changed_and_survives_a_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'allways.validator.scoring.component.process_weights_for_netuid', lambda **k: (k['uids'], k['weights'])
    )
    setter, *_ = _setter(tmp_path)
    setter.fold(_round(['a', 'b'], [1.0, 0.0]))
    setter.fold(_round(['a', 'c'], [0.0, 1.0]))
    assert setter.scores.tolist() == [0.25, 0.5]  # 'c' starts from zero, not from 'b'
    again, *_ = _setter(tmp_path)
    assert again.hotkeys == ['a', 'c'] and again.scores.tolist() == [0.25, 0.5]


def test_a_new_hotkey_on_a_uid_never_inherits_the_old_hotkeys_score(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'allways.validator.scoring.component.process_weights_for_netuid', lambda **k: (k['uids'], k['weights'])
    )
    setter, *_ = _setter(tmp_path)
    setter.fold(_round(['a', 'b'], [1.0, 1.0]))
    setter.fold(_round(['a', 'c'], [0.0, 0.0]))
    assert setter.scores.tolist() == [0.25, 0.0]


def test_the_moving_average_fits_a_shrunk_metagraph_and_survives_a_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'allways.validator.scoring.component.process_weights_for_netuid', lambda **k: (k['uids'], k['weights'])
    )
    setter, *_ = _setter(tmp_path)
    setter.fold(_round(['a', 'b', 'c'], [1.0, 0.0, 1.0]))
    setter.fold(_round(['a', 'd'], [0.0, 1.0]))
    assert setter.hotkeys == ['a', 'd'] and setter.scores.tolist() == [0.25, 0.5]  # 'c' dropped, 'd' from zero
    again, *_ = _setter(tmp_path)
    assert again.hotkeys == ['a', 'd'] and again.scores.tolist() == [0.25, 0.5]


def test_weights_are_set_once_per_epoch(tmp_path, monkeypatch):
    monkeypatch.setattr(
        'allways.validator.scoring.component.process_weights_for_netuid', lambda **k: (k['uids'], k['weights'])
    )
    setter, rounds, head, subtensor = _setter(tmp_path, block=200)
    rounds.set(_round(HOTKEYS, [0.0, 1.0] + [0.0] * RECYCLE_UID))
    setter.tick(NOW)
    setter.tick(NOW + 30)
    assert len(subtensor.sets) == 1  # the metagraph's last_update is stale, so its own last set counts
    head.set(_head(block=200 + 151))
    setter.tick(NOW + 60)
    assert len(subtensor.sets) == 2


def test_a_uid_registered_after_the_round_keeps_everyone_elses_weights(tmp_path, monkeypatch):
    seen = {}

    def process(**kwargs):
        seen['weights'] = kwargs['weights']
        return kwargs['uids'], kwargs['weights']

    monkeypatch.setattr('allways.validator.scoring.component.process_weights_for_netuid', process)
    setter, rounds, head, _ = _setter(tmp_path)
    rounds.set(_round(HOTKEYS, [1.0] + [0.0] * (len(HOTKEYS) - 1)))
    grown = _head(block=200)
    grown.metagraph.hotkeys.append('newcomer')
    grown.metagraph.uids = np.arange(len(HOTKEYS) + 1)
    grown.metagraph.last_update.append(0)
    head.set(grown)
    setter.tick(NOW)
    assert len(seen['weights']) == len(HOTKEYS) + 1
    assert seen['weights'][0] == 1.0 and seen['weights'][-1] == 0.0


def test_no_scores_route_the_whole_pool_to_the_recycle_uid(tmp_path, monkeypatch):
    seen = {}

    def process(**kwargs):
        seen['weights'] = kwargs['weights']
        return kwargs['uids'], kwargs['weights']

    monkeypatch.setattr('allways.validator.scoring.component.process_weights_for_netuid', process)
    setter, rounds, _, subtensor = _setter(tmp_path)
    rounds.set(_round(HOTKEYS, [0.0] * len(HOTKEYS)))
    setter.tick(NOW)
    assert seen['weights'][RECYCLE_UID] == 1.0 and seen['weights'].sum() == 1.0


def test_a_fill_is_judged_only_once_the_feed_has_caught_up(tmp_path):
    scoring, events, _, store, _, status = _scoring(tmp_path, _world(), catching_up=True)
    fields = {
        'miner': 'pk_a',
        'swap_key': bytes(32),
        'from_chain': 'btc',
        'to_chain': 'sol',
        'collateral_chain': 'sol',
        'from_amount': 1,
        'to_amount': 1,
        'collateral_amount': 1,
    }
    events.publish(EventRecord('SwapCompleted', fields, 6, NOW - 5, 'sig6'))
    scoring.tick(NOW)

    def fills():
        return store.conn().execute('SELECT COUNT(*) FROM scoring_fills').fetchone()[0]

    assert fills() == 0  # the quotes around its reservation may still be on their way
    assert store.conn().execute("SELECT 1 FROM acks WHERE consumer = 'scoring'").fetchone() is None
    status.set(FeedStatus(False, 1, NOW, (6, 'sig6')))
    scoring.tick(NOW + 5)
    assert fills() == 1


def test_the_ledger_start_is_stamped_by_the_tick_and_a_gap_keeps_its_earlier_start(tmp_path):
    scoring, *_ = _scoring(tmp_path, _world())
    scoring.tick(NOW)
    assert scoring.ledger.store.get_meta(LEDGER_SINCE) == str(NOW)
    scoring.tick(NOW + 60)
    assert scoring.ledger.store.get_meta(LEDGER_SINCE) == str(NOW)  # stamped once

    gapped, *_ = _scoring(tmp_path, _world(), db='gap.db')
    gapped.gaps._put(HistoryGap(NOW - 3600))  # the feed found history cut short at boot
    gapped.tick(NOW)
    assert gapped.ledger.store.get_meta(LEDGER_SINCE) == str(NOW - 3600)
