"""Fee-scaled miner pool: each round scales every lane pool by
``miner_share = min(1, FEE_POOL_MULTIPLE × fees / miner_emission)`` over the trailing pool window,
and the remainder recycles to ``RECYCLE_UID``."""

from types import SimpleNamespace

import numpy as np
import pytest

from allways.constants import (
    FEE_POOL_MULTIPLE,
    MINER_EMISSION_FRACTION,
    POOL_VOLUME_WINDOW_SECS,
    RECYCLE_UID,
    SCORING_WINDOW_SECS,
    SECONDS_PER_BLOCK,
)
from allways.solana.events import EventRecord
from allways.validator.chains.subtensor_view import Head, SubtensorView
from allways.validator.export.component import pool_rows, score_rows
from allways.validator.runtime.bus import Latest
from allways.validator.scoring import fee_pool as fee_pool_mod
from allways.validator.scoring.fee_pool import MINER_SHARE, FeePool, fee_pool_share, window_fees_tao
from allways.validator.scoring.ledger import OWNER, VERSION, Ledger
from allways.validator.sol_tao_price import RateLimited, fetch_sol_tao_rate, hour_price, rate_hour
from allways.validator.store.db import Store
from tests.validator.test_scoring import HOTKEYS, NOW, _quote, _scoring, _world

TAO = 10**9
SOL = 10**9
ALPHA_PER_BLOCK = 1.0
ALPHA_PRICE = 0.01  # TAO per alpha
EMISSION_24H = ALPHA_PER_BLOCK * MINER_EMISSION_FRACTION * (POOL_VOLUME_WINDOW_SECS / SECONDS_PER_BLOCK) * ALPHA_PRICE
HOUR = 3600
NO_RATE = lambda: None  # noqa: E731


def subnet_info(alpha_per_block: float = ALPHA_PER_BLOCK, price: float = ALPHA_PRICE) -> SimpleNamespace:
    # The spot price is set far off the moving price: the emission value must read the moving one.
    return SimpleNamespace(
        alpha_out_emission=SimpleNamespace(tao=alpha_per_block), moving_price=price, price=SimpleNamespace(tao=99.0)
    )


def fees_for_share(share: float) -> int:
    """The rao fee that yields ``share`` against ``EMISSION_24H``."""
    return round(share * EMISSION_24H / FEE_POOL_MULTIPLE * TAO)


@pytest.fixture
def store(tmp_path):
    return Store(str(tmp_path / 'v.db'))


@pytest.fixture
def ledger(store):
    return Ledger(store)


@pytest.fixture(autouse=True)
def no_price_feed(monkeypatch):
    """No test reaches CoinGecko: a round's SOL rate comes from ``no_price_feed.rates`` (default: unavailable)."""
    calls = []

    def fetch(hour):
        calls.append(hour)
        return no_price_feed.rates.get(hour)

    no_price_feed.rates, no_price_feed.calls = {}, calls
    monkeypatch.setattr(fee_pool_mod.sol_tao_price, 'fetch_sol_tao_rate', fetch)
    return no_price_feed


def add_fee(ledger: Ledger, key: str, fee: int, backing: str, at: int, legs=('btc', 'tao', 1, 1)):
    """One completed, unqualified fill carrying ``fee``: inert for pools and the β slice."""
    from_chain, to_chain, from_amount, to_amount = legs
    ledger.put_fill(key, at, 'hk_x', (from_chain, to_chain, backing), (from_amount, to_amount, 0), False, fee)


def share_of(ledger: Ledger, store: Store, subnet=None, now: int = NOW):
    fees = ledger.swap_fees(now - POOL_VOLUME_WINDOW_SECS, now)
    return FeePool(store).round(fees, subnet_info() if subnet is None else subnet, now)


class TestShareMath:
    @pytest.mark.parametrize(
        'fees, emission, expected',
        [
            (0.0, 10.0, 0.0),
            (1.0, 10.0, FEE_POOL_MULTIPLE * 0.1),
            (5.0 / FEE_POOL_MULTIPLE, 5.0, 1.0),
            (10.0, 10.0, 1.0),
            (1_000.0, 10.0, 1.0),
        ],
        ids=['zero', 'scaled', 'at-cap', 'past-cap', 'far-past-cap'],
    )
    def test_share_is_multiple_times_fees_over_emission_capped_at_one(self, fees, emission, expected):
        assert fee_pool_share(fees, emission) == pytest.approx(expected)

    def test_non_positive_emission_is_refused(self):
        with pytest.raises(ValueError):
            fee_pool_share(1.0, 0.0)


class TestFeeLedger:
    def test_fee_column_migrates_onto_an_old_ledger_and_old_rows_read_zero(self, store):
        old = (
            'CREATE TABLE IF NOT EXISTS scoring_fills (swap_key TEXT PRIMARY KEY, at INTEGER NOT NULL, '
            'hotkey TEXT NOT NULL, from_chain TEXT NOT NULL, to_chain TEXT NOT NULL, backing TEXT NOT NULL, '
            'from_amount TEXT NOT NULL, to_amount TEXT NOT NULL, collateral_amount TEXT NOT NULL, '
            'qualified INTEGER NOT NULL)'
        )
        store.declare(OWNER, VERSION, [('scoring_fills', old)])
        with store.tx() as c:
            c.execute("INSERT INTO scoring_fills VALUES ('old', 100, 'hk', 'btc', 'tao', 'tao', '1', '1', '0', 0)")

        ledger = Ledger(store)
        cols = {r['name'] for r in store.conn().execute('PRAGMA table_info(scoring_fills)')}
        assert 'fee' in cols
        assert ledger.swap_fees(0, 200) == {'tao': 0}  # the old row is kept and reads 0
        add_fee(ledger, 'new', 3 * TAO, 'tao', at=150)
        assert ledger.swap_fees(0, 200) == {'tao': 3 * TAO}

    def test_only_the_swap_completed_fee_counts(self, ledger):
        """Reservation, quote-churn and pool-entry fees ride other events and never reach the fee sum;
        the swap fee is stored as the event's absolute ``fee``, not re-derived from collateral."""

        def rec(name, slot, **fields):
            return EventRecord(name, {'miner': 'pk_a', **fields}, slot, 1_000 + slot, f's{slot}')

        for record in (
            rec('PoolOpened', 1, opener='pk_u', from_chain='btc', to_chain='tao', collateral_chain='tao', fee_paid=7),
            rec('ReservationRequested', 2, router='pk_r', requests=1, fee_paid=11, collateral_chain='tao'),
            rec('QuoteRemoved', 3, from_chain='btc', to_chain='tao', collateral_chain='tao', remove_fee=13),
            rec(
                'SwapCompleted',
                4,
                swap_key=b'\x01' * 32,
                from_chain='btc',
                to_chain='tao',
                collateral_chain='tao',
                collateral_amount=500 * TAO,
                from_amount=1_000,
                to_amount=500 * TAO,
                fee=5 * TAO,
            ),
        ):
            ledger.ingest(record, 'hk_a', reservation_ttl=600)

        assert ledger.swap_fees(0, 2_000) == {'tao': 5 * TAO}
        assert window_fees_tao(ledger.swap_fees(0, 2_000), NO_RATE) == pytest.approx(5.0)

    def test_mixed_sol_and_tao_fees_convert_at_the_given_rate(self, ledger):
        add_fee(ledger, 'f-sol', SOL // 100, 'sol', at=20, legs=('sol', 'btc', 1, 1))  # 0.01 SOL
        add_fee(ledger, 'f-tao', TAO // 200, 'tao', at=21)  # 0.005 TAO
        assert window_fees_tao(ledger.swap_fees(0, 100), lambda: 2.0) == pytest.approx(0.01 * 2.0 + 0.005)

    @pytest.mark.parametrize(
        'backing, rate', [('sol', NO_RATE), ('eth', lambda: 2.0)], ids=['sol-no-rate-yet', 'unknown-backing']
    )
    def test_a_fee_with_no_conversion_is_skipped_and_tao_fees_still_count(self, ledger, backing, rate):
        add_fee(ledger, 'f-x', 10**9, backing, at=10, legs=(backing, 'btc', 1, 1))
        add_fee(ledger, 'f-tao', TAO // 2, 'tao', at=11)
        assert window_fees_tao(ledger.swap_fees(0, 100), rate) == pytest.approx(0.5)


class TestSolTaoRate:
    def test_rate_hour_is_the_last_full_hour_at_least_ten_minutes_old(self):
        assert rate_hour(10 * HOUR + 599) == 9 * HOUR
        assert rate_hour(10 * HOUR + 600) == 10 * HOUR
        assert rate_hour(10 * HOUR + 3599) == 10 * HOUR

    H = 10 * HOUR

    @pytest.mark.parametrize(
        'points, expected',
        [
            ([(H - 300, 1.0), (H, 2.0), (H + 60, 9.0)], 2.0),
            ([(H - 900, 1.0), (H - 300, 3.0), (H + 60, 9.0)], 3.0),
            ([(H - HOUR - 1, 1.0), (H + 1, 2.0)], None),
            ([(H, 0.0)], None),
            ([(H, -1.0)], None),
            ([(H, float('nan'))], None),
            ([(H, float('inf'))], None),
        ],
        ids=['exact-point', 'latest-in-hour', 'none-in-hour', 'zero', 'negative', 'nan', 'inf'],
    )
    def test_hour_price(self, points, expected):
        if expected is None:
            with pytest.raises(ValueError):
                hour_price(points, self.H)
        else:
            assert hour_price(points, self.H) == expected

    def test_rate_is_sol_usd_over_tao_usd_for_the_same_hour(self):
        asked = []

        def fetch(coin_id, hour, timeout):
            asked.append((coin_id, hour))
            return {'solana': 150.0, 'bittensor': 300.0}[coin_id]

        assert fetch_sol_tao_rate(5 * HOUR, fetch=fetch, sleep=lambda s: None) == pytest.approx(0.5)
        assert asked == [('solana', 5 * HOUR), ('bittensor', 5 * HOUR)]

    @staticmethod
    def _clock():
        t = [0.0]
        sleeps = []

        def sleep(secs):
            sleeps.append(secs)
            t[0] += secs

        return t, sleeps, sleep, lambda: t[0]

    def test_retries_back_off_and_stop_after_three_attempts(self):
        t, sleeps, sleep, clock = self._clock()
        calls = []

        def fetch(coin_id, hour, timeout):
            calls.append(coin_id)
            raise ConnectionError('down')

        assert fetch_sol_tao_rate(HOUR, fetch=fetch, sleep=sleep, clock=clock) is None
        assert len(calls) == 3
        assert sleeps == [1.0, 2.0]

    @pytest.mark.parametrize(
        'failure, expected, expected_sleeps',
        [('slow', None, None), ('retry-after-60', None, []), ('retry-after-3', 0.5, [3.0])],
    )
    def test_retries_stay_within_the_time_budget(self, failure, expected, expected_sleeps):
        """Slow reads stop at the budget; a Retry-After is honoured only when it fits it."""
        t, sleeps, sleep, clock = self._clock()
        answers = iter([RateLimited(3.0), 150.0, 300.0])

        def fetch(coin_id, hour, timeout):
            assert timeout <= 10.0
            if failure == 'slow':
                t[0] += 4.0
                raise TimeoutError('slow')
            if failure == 'retry-after-60':
                raise RateLimited(60.0)
            answer = next(answers)
            if isinstance(answer, Exception):
                raise answer
            return answer

        assert fetch_sol_tao_rate(HOUR, fetch=fetch, sleep=sleep, clock=clock) == (
            None if expected is None else pytest.approx(expected)
        )
        assert t[0] <= 10.0
        if expected_sleeps is not None:
            assert sleeps == expected_sleeps

    def test_one_fetch_per_hour_and_every_validator_gets_the_same_rate(self, tmp_path):
        rates = {9 * HOUR: 2.0, 10 * HOUR: 3.0}
        fetched = []

        def fetch(hour):
            fetched.append(hour)
            return rates[hour]

        a, b = FeePool(Store(str(tmp_path / 'a.db')), fetch), FeePool(Store(str(tmp_path / 'b.db')), fetch)
        now = 10 * HOUR + 30  # rate hour 9h
        assert a.sol_tao_rate(now) == 2.0
        assert a.sol_tao_rate(now + 400) == 2.0  # same hour: no refetch
        assert b.sol_tao_rate(9 * HOUR + 700) == 2.0  # another validator, other minute, same hour
        assert a.sol_tao_rate(now + 600) == 3.0  # hour 10h
        assert a.sol_tao_rate(now + 900) == 3.0
        assert fetched == [9 * HOUR, 9 * HOUR, 10 * HOUR]  # once per validator per hour
        assert a.stored_rate() == (3.0, 10 * HOUR)

    def test_a_failed_fetch_keeps_the_last_rate_and_warns(self, store, monkeypatch):
        pool = FeePool(store, lambda hour: None)
        pool._put(**{fee_pool_mod.SOL_TAO_RATE: '2.0', fee_pool_mod.SOL_TAO_RATE_HOUR: str(9 * HOUR)})
        logs = {'warning': [], 'error': []}
        monkeypatch.setattr(fee_pool_mod.bt.logging, 'warning', lambda m, *a, **k: logs['warning'].append(m))
        monkeypatch.setattr(fee_pool_mod.bt.logging, 'error', lambda m, *a, **k: logs['error'].append(m))

        assert pool.sol_tao_rate(11 * HOUR) == 2.0
        assert logs['error'] == [] and str(9 * HOUR) in logs['warning'][-1]
        assert pool.stored_rate() == (2.0, 9 * HOUR)
        # Past a day old it logs at ERROR, and is still used.
        assert pool.sol_tao_rate(9 * HOUR + 25 * HOUR) == 2.0
        assert len(logs['error']) == 1


class TestRoundShare:
    def test_share_is_computed_and_persisted(self, ledger, store):
        add_fee(ledger, 'f', fees_for_share(0.4), 'tao', at=NOW - 5)
        fp = share_of(ledger, store)
        assert fp.fresh
        assert fp.fees_tao == pytest.approx(fees_for_share(0.4) / TAO)
        assert fp.emission_tao == pytest.approx(EMISSION_24H)
        assert fp.share == pytest.approx(0.4)
        assert FeePool(store).last_share() == pytest.approx(0.4)

    def test_fees_beyond_the_window_do_not_count(self, ledger, store):
        add_fee(ledger, 'f', fees_for_share(0.4), 'tao', at=NOW - POOL_VOLUME_WINDOW_SECS)
        assert share_of(ledger, store).share == 0.0

    @pytest.mark.parametrize(
        'subnet',
        [subnet_info(price=0.0), subnet_info(alpha_per_block=0.0), False],
        ids=['zero-price', 'zero-emission', 'no-read'],
    )
    def test_a_failed_emission_or_price_read_keeps_the_last_share(self, ledger, store, subnet):
        with store.tx() as c:
            Store.set_meta(c, MINER_SHARE, '0.37')
        add_fee(ledger, 'f', fees_for_share(1.0) * 10, 'tao', at=NOW - 5)  # would cap at 1.0
        fees = ledger.swap_fees(NOW - POOL_VOLUME_WINDOW_SECS, NOW)
        fp = FeePool(store).round(fees, subnet or None, NOW)
        assert not fp.fresh
        assert fp.share == pytest.approx(0.37)
        assert FeePool(store).last_share() == pytest.approx(0.37)

    def test_a_failed_subnet_read_reaches_scoring_as_no_read(self):
        """The subtensor view is the one reader: a failed ``subnet`` read publishes None, never the last answer."""
        good = subnet_info()
        answers = iter([good, RuntimeError('subtensor down')])

        def subnet(netuid):
            answer = next(answers)
            if isinstance(answer, Exception):
                raise answer
            return answer

        sub = SimpleNamespace(get_current_block=lambda: 1, metagraph=lambda n: object(), get_subnet_prices=dict)
        sub.subnet = subnet
        out = Latest()
        view = SubtensorView(lambda: sub, 7, out, clock=lambda: 0)
        view.setup()
        view.tick(0.0)
        assert out.get()[0].subnet is good
        view.tick(1_000.0)  # past the metagraph sync: read again, and it fails
        assert out.get()[0].subnet is None

    def test_cold_start_counts_tao_fees_only(self, ledger, store):
        add_fee(ledger, 'f-sol', 100 * SOL, 'sol', at=NOW - 5, legs=('sol', 'btc', 1, 1))
        add_fee(ledger, 'f-tao', fees_for_share(0.2), 'tao', at=NOW - 4)
        fp = share_of(ledger, store)
        assert fp.fresh and fp.share == pytest.approx(0.2)

    def test_off_market_self_fills_on_sol_tao_do_not_move_the_share(self, tmp_path, no_price_feed):
        """The SOL rate comes from the fixed-hour market price, never from what cleared on the subnet."""
        no_price_feed.rates[rate_hour(NOW)] = 0.5

        def share(name, self_fills):
            store = Store(str(tmp_path / f'{name}.db'))
            ledger = Ledger(store)
            add_fee(ledger, 'f-sol', SOL // 10, 'sol', at=NOW - 5, legs=('sol', 'btc', 1, 1))  # 0.05 TAO at market
            for n in range(self_fills):  # min-size sol→tao fills at 1000× the market rate
                add_fee(ledger, f'self-{n}', 1, 'sol', at=NOW - 10 - n, legs=('sol', 'tao', SOL, 500 * TAO))
            return share_of(ledger, store)

        honest, dealt = share('honest', 0), share('dealt', 20)
        assert honest.share == pytest.approx(dealt.share)
        assert honest.fees_tao == pytest.approx(0.05)
        assert dealt.fees_tao == pytest.approx(0.05 + 20 * 1e-9 * 0.5)  # their own 1-lamport fees, at market


def _two_rounds(tmp_path, share=None, subnet=None, fees=0, halted=False, name='v'):
    """A quoted btc→sol lane scored over two windows (the first anchors it). ``share`` pins the stored share;
    ``subnet`` and ``fees`` feed the round's own derivation."""
    scoring, _, rounds, store, *_ = _scoring(tmp_path, _world(quotes=[_quote('pk_a')], halted=halted), db=f'{name}.db')
    with store.tx() as c:
        if share is None:
            c.execute('DELETE FROM meta WHERE key = ?', (MINER_SHARE,))
        else:
            Store.set_meta(c, MINER_SHARE, repr(share))
    head = scoring.head.get()[0]
    scoring.head.set(Head(head.block, head.block_read_at, head.metagraph, head.metagraph_read_at, {}, subnet))
    if fees:
        add_fee(scoring.ledger, 'f', fees, 'tao', at=NOW + 5)
    scoring.tick(NOW)
    scoring.tick(NOW + SCORING_WINDOW_SECS)
    return rounds.get()[0], store


class TestScaledRewards:
    @pytest.mark.parametrize('fresh', [False, True], ids=['zero-fees', 'fresh-validator-failed-read'])
    def test_zero_fees_recycle_everything(self, tmp_path, fresh):
        # Fresh: no share stored yet and no subnet read — 0.0, never 1.0.
        result, _ = _two_rounds(tmp_path, share=None, subnet=None if fresh else subnet_info())
        assert result.fee_pool.share == 0.0
        assert result.rewards[0] == 0.0
        np.testing.assert_allclose(result.rewards[RECYCLE_UID], 1.0, atol=1e-6)

    @pytest.mark.parametrize('share, fees', [(0.5, fees_for_share(0.5)), (1.0, fees_for_share(1.0) * 50)])
    def test_miner_reward_scales_by_the_share_and_the_rest_recycles(self, tmp_path, share, fees):
        full, _ = _two_rounds(tmp_path, share=1.0, name='full')  # no subnet read: keeps the pinned 1.0
        scaled, _ = _two_rounds(tmp_path, subnet=subnet_info(), fees=fees, name='scaled')
        assert full.rewards[0] > 0 and scaled.fee_pool.share == pytest.approx(share)
        np.testing.assert_allclose(scaled.rewards[0], share * full.rewards[0], atol=1e-6)
        np.testing.assert_allclose(scaled.rewards[RECYCLE_UID], 1.0 - share * full.rewards[0], atol=1e-6)
        np.testing.assert_allclose(scaled.rewards.sum(), 1.0, atol=1e-6)

    def test_the_dashboard_rows_report_the_pool_as_paid(self, tmp_path):
        full, _ = _two_rounds(tmp_path, share=1.0, name='full')
        quarter, _ = _two_rounds(tmp_path, share=0.25, name='quarter')
        assert full.rows and len(full.rows) == len(quarter.rows)
        for f, q in zip(score_rows(full), score_rows(quarter)):
            assert q[6] == pytest.approx(0.25 * f[6])  # pool
            assert q[9] == pytest.approx(0.25 * f[9])  # reward
            assert q[7] == f[7]  # crown share unchanged
        pools = {r[1:4]: r[4] for r in pool_rows(full)}
        assert pools and all(r[4] == pytest.approx(0.25 * pools[r[1:4]]) for r in pool_rows(quarter))


class TestHalted:
    def test_halted_round_recycles_everything_and_leaves_the_share_alone(self, tmp_path, no_price_feed):
        result, store = _two_rounds(tmp_path, share=0.6, subnet=subnet_info(), fees=fees_for_share(1.0), halted=True)
        assert result.halted and result.fee_pool is None
        np.testing.assert_allclose(result.rewards[RECYCLE_UID], 1.0)
        assert result.rewards.sum() == pytest.approx(1.0)
        assert FeePool(store).last_share() == pytest.approx(0.6)
        assert no_price_feed.calls == []
        assert len(result.hotkeys) == len(HOTKEYS)
