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
    MINER_POOL_SHARE,
    POOL_VOLUME_WINDOW_SECS,
    RECYCLE_UID,
    SECONDS_PER_BLOCK,
)
from allways.solana.events import EventRecord
from allways.validator import scoring_trace as trace_mod
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import (
    DirectionTrace,
    build_halted_rewards,
    calculate_miner_rewards,
    compute_direction_pools,
    fee_pool_share,
    median_sol_tao_rate,
    round_miner_share,
    score_and_reward_miners,
    snapshot_current_miner_scores,
    window_fees_tao,
)
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


def subnet_info(alpha_per_block: float = ALPHA_PER_BLOCK, price: float = ALPHA_PRICE) -> SimpleNamespace:
    return SimpleNamespace(alpha_out_emission=SimpleNamespace(tao=alpha_per_block), price=SimpleNamespace(tao=price))


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
    def test_share_is_multiple_times_fees_over_emission(self):
        assert fee_pool_share(1.0, 10.0) == pytest.approx(FEE_POOL_MULTIPLE * 0.1)
        assert fee_pool_share(0.0, 10.0) == 0.0

    def test_share_caps_at_one(self):
        assert fee_pool_share(10.0, 10.0) == 1.0
        assert fee_pool_share(1_000.0, 10.0) == 1.0
        assert fee_pool_share(5.0 / FEE_POOL_MULTIPLE, 5.0) == pytest.approx(1.0)

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
        assert window_fees_tao(store, 0, 200) == 0.0
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
        assert window_fees_tao(store, 0, 2_000) == pytest.approx(5.0)

    def test_mixed_sol_and_tao_fees_convert_at_the_median_cleared_rate(self, store: ValidatorStateStore):
        # Three sol↔tao clears at 1, 2 and 10 TAO/SOL (both directions count): median 2.
        add_fee(store, 'r1', 0, 'sol', at=10, legs=('sol', 'tao', 1 * SOL, 1 * TAO))
        add_fee(store, 'r2', 0, 'sol', at=11, legs=('tao', 'sol', 4 * TAO, 2 * SOL))
        add_fee(store, 'r3', 0, 'tao', at=12, legs=('sol', 'tao', 1 * SOL, 10 * TAO))
        assert median_sol_tao_rate(store, 0, 100) == pytest.approx(2.0)

        add_fee(store, 'f-sol', SOL // 100, 'sol', at=20, legs=('sol', 'btc', 1, 1))  # 0.01 SOL
        add_fee(store, 'f-tao', TAO // 200, 'tao', at=21)  # 0.005 TAO
        assert window_fees_tao(store, 0, 100) == pytest.approx(0.01 * 2.0 + 0.005)

    def test_lamport_fees_fall_back_to_the_last_good_rate(self, store: ValidatorStateStore):
        add_fee(store, 'r1', 0, 'sol', at=10, legs=('sol', 'tao', 1 * SOL, 3 * TAO))
        assert window_fees_tao(store, 0, 100) == 0.0  # records 3 TAO/SOL as the last good rate
        add_fee(store, 'f-sol', SOL, 'sol', at=500, legs=('sol', 'btc', 1, 1))
        # The window (200, 600] cleared no sol↔tao fill: the stored rate prices the lamports.
        assert window_fees_tao(store, 200, 600) == pytest.approx(3.0)

    def test_lamport_fees_with_no_rate_ever_are_unpriceable(self, store: ValidatorStateStore):
        add_fee(store, 'f-sol', SOL, 'sol', at=10, legs=('sol', 'btc', 1, 1))
        assert window_fees_tao(store, 0, 100) is None


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

    def test_unpriceable_lamport_fees_keep_the_last_share(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        v.state_store.set_miner_share(0.25)
        add_fee(v.state_store, 'f', 100 * SOL, 'sol', at=v.block - 5, legs=('sol', 'btc', 1, 1))
        fp = round_miner_share(v, v.block)
        assert fp.fees_tao is None
        assert fp.share == pytest.approx(0.25)
        v.state_store.close()

    def test_a_fresh_validator_starts_at_full_recycle(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'fresh.db')
        assert store.miner_share() == 0.0
        # No fee history and a failed emission read: 0.0, never 1.0.
        v = SimpleNamespace(state_store=store, subtensor=None, config=SimpleNamespace(netuid=7))
        assert round_miner_share(v, 10_000).share == 0.0
        # With a good read and no fees yet, 0 fees → 0.0.
        with_subtensor(v)
        assert round_miner_share(v, 10_000).share == 0.0
        store.close()

    def test_the_emission_read_is_cached_for_the_ttl(self, tmp_path: Path):
        v = with_subtensor(make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        round_miner_share(v, v.block)
        round_miner_share(v, v.block + 60)
        assert v.subnet_reads == [7]
        v.state_store.close()


class TestScaledRewards:
    def _round(self, tmp_path: Path, share_fees: int, name: str = 'v'):
        path = tmp_path / name
        path.mkdir()
        v = with_subtensor(make_validator(path, pad_hotkeys_to_cover_recycle(['hk_a'])))
        seed_btc_sol_crown(v)
        if share_fees:
            add_fee(v.state_store, 'f', share_fees, 'tao', at=v.block - 5)
        rewards, _ = calculate_miner_rewards(v, v.block)
        v.state_store.close()
        return rewards

    def test_zero_fees_recycle_everything(self, tmp_path: Path):
        rewards = self._round(tmp_path, 0)
        assert rewards[0] == 0.0
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0, atol=1e-6)

    def test_miner_reward_scales_by_the_share_and_the_rest_recycles(self, tmp_path: Path):
        rewards = self._round(tmp_path, fees_for_share(0.5))
        full = CROWN_SLICE * (POOL_BTC_SOL + POOL_SOL_BTC)
        np.testing.assert_allclose(rewards[0], 0.5 * full, atol=1e-6)
        np.testing.assert_allclose(rewards[RECYCLE_UID], 1.0 - 0.5 * full, atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)

    def test_fees_past_the_cap_pay_the_unscaled_pools(self, tmp_path: Path):
        rewards = self._round(tmp_path, fees_for_share(1.0) * 50)
        np.testing.assert_allclose(rewards[0], CROWN_SLICE * (POOL_BTC_SOL + POOL_SOL_BTC), atol=1e-6)
        np.testing.assert_allclose(rewards.sum(), 1.0, atol=1e-6)

    def test_paid_pools_sum_to_the_scaled_share(self):
        pools = compute_direction_pools({})  # every family paid: pools sum to MINER_POOL_SHARE
        for share in (0.0, 0.3, 1.0):
            paid = sum(DirectionTrace(pool=p, miner_share=share).paid_pool for p in pools.values())
            assert paid == pytest.approx(MINER_POOL_SHARE * share)

    def test_same_inputs_give_the_same_weights(self, tmp_path: Path):
        a = self._round(tmp_path, fees_for_share(0.3), name='a')
        b = self._round(tmp_path, fees_for_share(0.3), name='b')
        assert np.array_equal(a, b)

    def test_the_trace_logs_the_fee_pool_inputs(self, tmp_path: Path, monkeypatch):
        logged = []
        monkeypatch.setattr(trace_mod.bt.logging, 'info', lambda msg, *a, **k: logged.append(msg))
        self._round(tmp_path, fees_for_share(0.5))
        header = next(m for m in logged if m.startswith('V1 scoring')).splitlines()[0]
        for field in ('fees_24h=', 'emission_24h=', f'multiple={FEE_POOL_MULTIPLE:g}', 'miner_share=0.5', 'recycled='):
            assert field in header

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

    def test_a_zero_share_still_replays_the_crown(self, tmp_path: Path):
        """Crown time is computed as before; only what it pays scales."""
        v = make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a']))
        seed_btc_sol_crown(v)
        v.state_store.set_miner_share(0.0)
        rows = snapshot_current_miner_scores(v, at_time=v.block)
        assert rows and all(r[7] > 0 and r[9] == 0.0 for r in rows)
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

    def test_build_halted_rewards_ignores_the_share(self, tmp_path: Path):
        v = make_validator(tmp_path, pad_hotkeys_to_cover_recycle(['hk_a']))
        v.state_store.set_miner_share(0.6)
        rewards, _ = build_halted_rewards(v)
        assert rewards[RECYCLE_UID] == 1.0 and rewards.sum() == 1.0
        v.state_store.close()
