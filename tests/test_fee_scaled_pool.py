"""Fee-scaled miner pool: each round scales every lane pool by
``miner_share = min(1, FEE_POOL_MULTIPLE × fees / miner_emission)`` over the trailing pool window,
and the remainder recycles to ``RECYCLE_UID``."""

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from allways.constants import (
    FEE_POOL_MULTIPLE,
    MINER_EMISSION_FRACTION,
    POOL_VOLUME_WINDOW_SECS,
    RECYCLE_UID,
    SECONDS_PER_BLOCK,
)
from allways.solana.events import EventRecord
from allways.validator import scoring as scoring_mod
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import (
    calculate_miner_rewards,
    fee_pool_share,
    hourly_sol_tao_rate,
    round_miner_share,
    score_and_reward_miners,
    snapshot_current_miner_scores,
    window_fees_tao,
)
from allways.validator.sol_tao_price import RateLimited, fetch_sol_tao_rate, hour_price, rate_hour
from allways.validator.state_store import ValidatorStateStore
from tests.test_scoring_v1 import (  # noqa: F401 — _identity_attribution is an autouse fixture
    CROWN_SLICE,
    POOL_BTC_SOL,
    POOL_SOL_BTC,
    _identity_attribution,
    make_validator,
    pad_hotkeys_to_cover_recycle,
)

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


@pytest.fixture(autouse=True)
def _no_price_feed(monkeypatch):
    """No test reaches CoinGecko: a round's SOL rate comes from ``hourly_rates`` (default: unavailable)."""
    calls = []

    def fetch(hour):
        calls.append(hour)
        return _no_price_feed.rates.get(hour)

    _no_price_feed.rates = {}
    _no_price_feed.calls = calls
    monkeypatch.setattr(scoring_mod, 'fetch_sol_tao_rate', fetch)
    return _no_price_feed


def with_subtensor(v: SimpleNamespace, subnet=None) -> SimpleNamespace:
    """Give a scoring stand-in a subtensor whose ``subnet`` read returns ``subnet_info()`` (or ``subnet``)."""
    reads = []

    def read(netuid):
        reads.append(netuid)
        if callable(subnet):
            return subnet()
        return subnet or subnet_info()

    v.subtensor = SimpleNamespace(subnet=read)
    v.config = SimpleNamespace(netuid=7)
    v.subnet_reads = reads
    return v


def add_fee(store: ValidatorStateStore, key: str, fee: int, backing: str, at: int, legs=('btc', 'tao', 1, 1)):
    """One completed, unqualified fill carrying ``fee`` — inert for pools and the β slice."""
    from_chain, to_chain, from_amount, to_amount = legs
    store.insert_clearing_rate(at, 'hk_x', from_chain, to_chain, from_amount, to_amount, key, backing, fee=fee)


def seed_btc_sol_crown(v: SimpleNamespace, hotkey: str = 'hk_a') -> None:
    conn = v.state_store.require_connection()
    for from_chain, to_chain in (('btc', 'sol'), ('sol', 'btc')):
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            (hotkey, from_chain, to_chain, 0.00020, 0),
        )
    conn.commit()


def fees_for_share(share: float) -> int:
    """The rao fee that yields ``share`` against ``EMISSION_24H``."""
    return round(share * EMISSION_24H / FEE_POOL_MULTIPLE * TAO)


@pytest.fixture
def store(tmp_path: Path):
    s = ValidatorStateStore(db_path=tmp_path / 'state.db')
    yield s
    s.close()


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
    def test_fee_column_migrates_onto_an_old_ledger_and_old_rows_read_zero(self, tmp_path: Path):
        db = tmp_path / 'state.db'
        conn = sqlite3.connect(db)
        conn.execute(
            """
            CREATE TABLE clearing_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT, block_num INTEGER NOT NULL, hotkey TEXT NOT NULL,
                from_chain TEXT NOT NULL, to_chain TEXT NOT NULL, from_amount TEXT NOT NULL,
                to_amount TEXT NOT NULL, swap_key TEXT, backing TEXT NOT NULL DEFAULT 'sol',
                qualified INTEGER NOT NULL DEFAULT 0, collateral_amount TEXT NOT NULL DEFAULT '0'
            )
            """
        )
        conn.execute(
            'INSERT INTO clearing_rates (block_num, hotkey, from_chain, to_chain, from_amount, to_amount, swap_key, '
            "backing) VALUES (100, 'hk', 'btc', 'tao', '1', '1', 'old', 'tao')"
        )
        conn.commit()
        conn.close()

        store = ValidatorStateStore(db_path=db)
        cols = [row[1] for row in store.require_connection().execute('PRAGMA table_info(clearing_rates)')]
        assert 'fee' in cols
        assert window_fees_tao(store, 0, 200, NO_RATE) == 0.0
        add_fee(store, 'new', 3 * TAO, 'tao', at=150)
        assert store.get_swap_fees(0, 200) == {'tao': 3 * TAO}
        store.close()

    def test_only_the_swap_completed_fee_counts(self, store: ValidatorStateStore):
        """Reservation, quote-churn and pool-entry fees ride other events and never reach the fee sum;
        the swap fee is stored as the event's absolute ``fee``, not re-derived from collateral."""
        index = SolanaEventIndex(store)
        attribution = {'pk_a': 'hk_a'}

        def rec(name, slot, **fields):
            return EventRecord(
                name=name, fields={'miner': 'pk_a', **fields}, slot=slot, block_time=1_000 + slot, signature=f's{slot}'
            )

        index.ingest(
            [
                rec(
                    'PoolOpened', 1, opener='pk_u', from_chain='btc', to_chain='tao', collateral_chain='tao', fee_paid=7
                ),
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
            ],
            attribution,
        )

        assert store.get_swap_fees(0, 2_000) == {'tao': 5 * TAO}
        assert window_fees_tao(store, 0, 2_000, NO_RATE) == pytest.approx(5.0)

    def test_mixed_sol_and_tao_fees_convert_at_the_given_rate(self, store: ValidatorStateStore):
        add_fee(store, 'f-sol', SOL // 100, 'sol', at=20, legs=('sol', 'btc', 1, 1))  # 0.01 SOL
        add_fee(store, 'f-tao', TAO // 200, 'tao', at=21)  # 0.005 TAO
        assert window_fees_tao(store, 0, 100, lambda: 2.0) == pytest.approx(0.01 * 2.0 + 0.005)

    @pytest.mark.parametrize(
        'backing, rate', [('sol', NO_RATE), ('eth', lambda: 2.0)], ids=['sol-no-rate-yet', 'unknown-backing']
    )
    def test_a_fee_with_no_conversion_is_skipped_and_tao_fees_still_count(self, store, backing, rate):
        add_fee(store, 'f-x', 10**9, backing, at=10, legs=(backing, 'btc', 1, 1))
        add_fee(store, 'f-tao', TAO // 2, 'tao', at=11)
        assert window_fees_tao(store, 0, 100, rate) == pytest.approx(0.5)


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

    @staticmethod
    def _validator(path: Path):
        return SimpleNamespace(state_store=ValidatorStateStore(db_path=path / 'rate.db'))

    def test_one_fetch_per_hour_and_every_validator_gets_the_same_rate(self, tmp_path: Path):
        rates = {9 * HOUR: 2.0, 10 * HOUR: 3.0}
        fetched = []

        def fetch(hour):
            fetched.append(hour)
            return rates[hour]

        a, b = self._validator(tmp_path / 'a'), self._validator(tmp_path / 'b')
        now = 10 * HOUR + 30  # rate hour 9h
        assert hourly_sol_tao_rate(a, now, fetch) == 2.0
        assert hourly_sol_tao_rate(a, now + 400, fetch) == 2.0  # same hour: no refetch
        assert hourly_sol_tao_rate(b, 9 * HOUR + 700, fetch) == 2.0  # another validator, other minute, same hour
        assert hourly_sol_tao_rate(a, now + 600, fetch) == 3.0  # hour 10h
        assert hourly_sol_tao_rate(a, now + 900, fetch) == 3.0
        assert fetched == [9 * HOUR, 9 * HOUR, 10 * HOUR]  # once per validator per hour
        assert a.state_store.sol_tao_rate() == (3.0, 10 * HOUR)
        a.state_store.close()
        b.state_store.close()

    def test_a_failed_fetch_keeps_the_last_rate_and_warns(self, tmp_path: Path, monkeypatch):
        v = self._validator(tmp_path)
        v.state_store.set_sol_tao_rate(2.0, 9 * HOUR)
        logs = {'warning': [], 'error': []}
        monkeypatch.setattr(scoring_mod.bt.logging, 'warning', lambda m, *a, **k: logs['warning'].append(m))
        monkeypatch.setattr(scoring_mod.bt.logging, 'error', lambda m, *a, **k: logs['error'].append(m))

        assert hourly_sol_tao_rate(v, 11 * HOUR, lambda hour: None) == 2.0
        assert logs['error'] == [] and str(9 * HOUR) in logs['warning'][-1]
        assert v.state_store.sol_tao_rate() == (2.0, 9 * HOUR)
        # Past a day old it logs at ERROR, and is still used.
        assert hourly_sol_tao_rate(v, 9 * HOUR + 25 * HOUR, lambda hour: None) == 2.0
        assert len(logs['error']) == 1
        v.state_store.close()


class TestRoundShare:
    def test_share_is_computed_and_persisted(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        add_fee(v.state_store, 'f', fees_for_share(0.4), 'tao', at=v.block - 5)
        fp = round_miner_share(v, v.block)
        assert fp.fresh
        assert fp.fees_tao == pytest.approx(fees_for_share(0.4) / TAO)
        assert fp.emission_tao == pytest.approx(EMISSION_24H)
        assert fp.share == pytest.approx(0.4)
        assert v.state_store.miner_share() == pytest.approx(0.4)
        assert v.subnet_reads == [7]
        v.state_store.close()

    def test_fees_beyond_the_window_do_not_count(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        add_fee(v.state_store, 'f', fees_for_share(0.4), 'tao', at=v.block - POOL_VOLUME_WINDOW_SECS)
        assert round_miner_share(v, v.block).share == 0.0
        v.state_store.close()

    @pytest.mark.parametrize(
        'subnet',
        [
            lambda: (_ for _ in ()).throw(RuntimeError('subtensor down')),
            lambda: subnet_info(price=0.0),
            lambda: subnet_info(alpha_per_block=0.0),
            lambda: None,
        ],
        ids=['read-raises', 'zero-price', 'zero-emission', 'no-subnet'],
    )
    def test_a_failed_emission_or_price_read_keeps_the_last_share(self, tmp_path: Path, subnet):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])), subnet=subnet)
        v.state_store.set_miner_share(0.37)
        add_fee(v.state_store, 'f', fees_for_share(1.0) * 10, 'tao', at=v.block - 5)  # would cap at 1.0
        fp = round_miner_share(v, v.block)
        assert not fp.fresh
        assert fp.share == pytest.approx(0.37)
        assert v.state_store.miner_share() == pytest.approx(0.37)
        v.state_store.close()

    def test_cold_start_counts_tao_fees_only(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        add_fee(v.state_store, 'f-sol', 100 * SOL, 'sol', at=v.block - 5, legs=('sol', 'btc', 1, 1))
        add_fee(v.state_store, 'f-tao', fees_for_share(0.2), 'tao', at=v.block - 4)
        fp = round_miner_share(v, v.block)
        assert fp.fresh and fp.share == pytest.approx(0.2)
        v.state_store.close()

    def test_off_market_self_fills_on_sol_tao_do_not_move_the_share(self, tmp_path: Path, _no_price_feed):
        """The SOL rate comes from the fixed-hour market price, never from what cleared on the subnet."""

        def share(name, self_fills):
            path = tmp_path / name
            path.mkdir()
            v = with_subtensor(make_validator(path, pad_hotkeys_to_cover_recycle(['hk_a'])))
            _no_price_feed.rates[rate_hour(v.block)] = 0.5
            sol_fee = SOL // 10  # 0.1 SOL → 0.05 TAO at the market rate
            add_fee(v.state_store, 'f-sol', sol_fee, 'sol', at=v.block - 5, legs=('sol', 'btc', 1, 1))
            for n in range(self_fills):  # min-size sol→tao fills at 1000× the market rate
                add_fee(v.state_store, f'self-{n}', 1, 'sol', at=v.block - 10 - n, legs=('sol', 'tao', SOL, 500 * TAO))
            fp = round_miner_share(v, v.block)
            v.state_store.close()
            return fp

        honest, dealt = share('honest', 0), share('dealt', 20)
        assert honest.share == pytest.approx(dealt.share)
        assert honest.fees_tao == pytest.approx(0.05)
        assert dealt.fees_tao == pytest.approx(0.05 + 20 * 1e-9 * 0.5)  # their own 1-lamport fees, at market


class TestScaledRewards:
    @staticmethod
    def _round(v: SimpleNamespace, fees: int = 0):
        seed_btc_sol_crown(v)
        if fees:
            add_fee(v.state_store, 'f', fees, 'tao', at=v.block - 5)
        rewards, _ = calculate_miner_rewards(v, v.block)
        v.state_store.close()
        return rewards

    @pytest.mark.parametrize('fresh', [False, True], ids=['zero-fees', 'fresh-validator-failed-read'])
    def test_zero_fees_recycle_everything(self, tmp_path: Path, fresh):
        v = make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a']))
        if fresh:
            # No share stored yet and no subtensor to read: 0.0, never 1.0.
            v.state_store.delete_relay_meta(ValidatorStateStore.MINER_SHARE_KEY)
            assert v.state_store.miner_share() == 0.0
        else:
            with_subtensor(v)
        rewards = self._round(v)
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)

    @pytest.mark.parametrize('share, fees', [(0.5, fees_for_share(0.5)), (1.0, fees_for_share(1.0) * 50)])
    def test_miner_reward_scales_by_the_share_and_the_rest_recycles(self, tmp_path: Path, share, fees):
        rewards = self._round(with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a']))), fees)
        full = CROWN_SLICE * (POOL_BTC_SOL + POOL_SOL_BTC)
        np.testing.assert_allclose(rewards[0], share * full, atol=1e-6)
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0 - share * full, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)

    def test_the_live_tip_pays_on_the_last_rounds_share(self, tmp_path: Path):
        v = make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a']))
        seed_btc_sol_crown(v)
        full = snapshot_current_miner_scores(v, at_time=v.block)
        v.state_store.set_miner_share(0.25)
        scaled = snapshot_current_miner_scores(v, at_time=v.block)
        assert full and len(full) == len(scaled)
        for f, s in zip(full, scaled):
            assert s[6] == pytest.approx(0.25 * f[6])  # pool
            assert s[9] == pytest.approx(0.25 * f[9])  # reward
            assert s[7] == f[7]  # crown share unchanged
        v.state_store.close()


class TestHalted:
    def test_halted_round_recycles_everything_and_leaves_the_share_alone(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        seed_btc_sol_crown(v)
        v.state_store.set_miner_share(0.6)
        v.solana_config_cache.halted.return_value = True
        paid = []
        v.update_scores = lambda rewards, uids: paid.append(rewards)

        score_and_reward_miners(v)

        assert len(paid) == 1
        np.testing.assert_allclose(paid[0][RECYCLE_UID], 1.0)
        assert paid[0].sum() == pytest.approx(1.0)
        assert v.subnet_reads == []
        assert v.state_store.miner_share() == pytest.approx(0.6)
        v.state_store.close()
