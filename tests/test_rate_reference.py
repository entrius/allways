"""C-rev — the on-chain trimmed/volume-weighted/per-miner-capped rate-quality
reference. Determinism (the whole point of dropping the external feed) +
wash-resistance (trim + per-miner cap) + the thin-history neutral fallback."""

from pathlib import Path

import pytest

from allways.validator.scoring import (
    _canonical_rate_and_weight,
    build_direction_references,
    trimmed_reference,
)
from allways.validator.state_store import ValidatorStateStore


def make_store(tmp_path: Path) -> ValidatorStateStore:
    return ValidatorStateStore(db_path=tmp_path / 'state.db')


# ─── _canonical_rate_and_weight ─────────────────────────────────────────


class TestCanonicalRateAndWeight:
    def test_btc_to_tao_rate_and_btc_weight(self):
        # 0.001 BTC → 0.5 TAO ⇒ 500 TAO/BTC; weight = canonical-source (btc) native.
        rate, weight = _canonical_rate_and_weight('btc', 'tao', 100_000, 500_000_000)
        assert rate == pytest.approx(500.0)
        assert weight == 100_000

    def test_reverse_direction_is_symmetric(self):
        # Same economic rate the other way: 0.5 TAO → 0.001 BTC ⇒ still 500 TAO/BTC,
        # and the weight is still the btc leg (here the to_amount).
        rate, weight = _canonical_rate_and_weight('tao', 'btc', 500_000_000, 100_000)
        assert rate == pytest.approx(500.0)
        assert weight == 100_000

    def test_nonpositive_legs_zeroed(self):
        assert _canonical_rate_and_weight('btc', 'tao', 0, 500) == (0.0, 0)
        assert _canonical_rate_and_weight('btc', 'tao', 100, 0) == (0.0, 0)


# ─── trimmed_reference ──────────────────────────────────────────────────


def _uniform(n, rate, weight=1.0):
    return [(f'hk{i}', rate, weight) for i in range(n)]


class TestTrimmedReference:
    def test_below_min_swaps_is_none(self):
        assert trimmed_reference(_uniform(4, 100.0)) is None

    def test_uniform_samples_average_to_the_rate(self):
        assert trimmed_reference(_uniform(10, 100.0)) == pytest.approx(100.0)

    def test_trim_drops_both_tail_outliers(self):
        # 8 honest at 100, one wild-low, one wild-high; 10% weighted trim each
        # tail removes exactly the two outlier samples (1.0 weight each).
        samples = [('low', 1.0, 1.0)] + _uniform(8, 100.0) + [('high', 10_000.0, 1.0)]
        assert trimmed_reference(samples, trim_frac=0.10, cap_frac=1.0) == pytest.approx(100.0)

    def test_per_miner_cap_blunts_a_wash_farmer(self):
        # 20 honest miners (rate 100, weight 1) + one farmer dumping weight 80 at
        # an inflated 200. Isolate the cap (trim_frac=0): capping the farmer to
        # 25% of the pool pulls the reference far back toward the honest cluster.
        samples = _uniform(20, 100.0) + [('farmer', 200.0, 80.0)]
        naive = trimmed_reference(samples, trim_frac=0.0, cap_frac=1.0)
        capped = trimmed_reference(samples, trim_frac=0.0, cap_frac=0.25)
        assert naive == pytest.approx((20 * 100 + 80 * 200) / 100)  # 180
        assert capped == pytest.approx((20 * 100 + 25 * 200) / 45)  # ~155.6
        assert capped < naive

    def test_all_samples_one_miner_cap_is_scale_invariant(self):
        # A single miner's swaps: the per-miner cap scales every weight uniformly,
        # which leaves the weighted mean unchanged (cap ≠ censorship of one miner).
        samples = [('solo', 100.0, 3.0)] * 2 + [('solo', 130.0, 4.0)] * 3
        assert trimmed_reference(samples, trim_frac=0.0, cap_frac=0.25) == pytest.approx((100.0 * 6 + 130.0 * 12) / 18)

    def test_determinism_independent_of_input_order(self):
        samples = [
            ('hk_a', 101.0, 2.0),
            ('hk_b', 99.0, 5.0),
            ('hk_c', 100.0, 1.0),
            ('hk_d', 103.0, 4.0),
            ('hk_e', 98.0, 3.0),
            ('hk_f', 100.5, 2.0),
        ]
        first = trimmed_reference(samples)
        again = trimmed_reference(list(reversed(samples)))
        assert first is not None
        assert first == again  # exact float equality, not approx — determinism is the point


# ─── build_direction_references ─────────────────────────────────────────


class TestBuildDirectionReferences:
    def _seed(self, store, direction, swaps, base_time=1000):
        f, t = direction
        for i, (hk, from_amt, to_amt) in enumerate(swaps):
            store.insert_clearing_rate(base_time + i, hk, f, t, from_amt, to_amt)

    def test_empty_history_gives_neutral_none(self, tmp_path: Path):
        store = make_store(tmp_path)
        refs = build_direction_references(store, current_time=10_000)
        for ref in refs.values():
            assert ref.reference is None
            assert ref.miner_rates == {}
        store.close()

    def test_clean_history_reference_is_the_clearing_rate(self, tmp_path: Path):
        store = make_store(tmp_path)
        # 5 swaps sol→tao; both 9-dec ⇒ rate = to/from = 5e8/1e5 = 5000 TAO/SOL ⇒ reference 5000.
        self._seed(store, ('sol', 'tao'), [(f'hk{i}', 100_000, 500_000_000) for i in range(5)])
        ref = build_direction_references(store, current_time=10_000)[('sol', 'tao')]
        assert ref.reference == pytest.approx(5000.0, rel=1e-6)
        assert all(v == pytest.approx(5000.0) for v in ref.miner_rates.values())
        store.close()

    def test_per_miner_vwap_is_volume_weighted(self, tmp_path: Path):
        store = make_store(tmp_path)
        # Enough swaps to clear the floor; hk0 trades twice at different rates.
        self._seed(store, ('sol', 'tao'), [(f'hk{i}', 100_000, 500_000_000) for i in range(5)])
        store.insert_clearing_rate(2000, 'hk0', 'sol', 'tao', 100_000, 400_000_000)
        ref = build_direction_references(store, current_time=10_000)[('sol', 'tao')]
        # hk0: (5e8 + 4e8) tao / (1e5 + 1e5) sol = 4500 TAO/SOL; others unchanged at 5000.
        assert ref.miner_rates['hk0'] == pytest.approx(4500.0)
        assert ref.miner_rates['hk1'] == pytest.approx(5000.0)
        store.close()

    def test_window_excludes_old_samples(self, tmp_path: Path):
        store = make_store(tmp_path)
        # Five fresh swaps inside the window + five ancient ones outside it.
        self._seed(store, ('sol', 'tao'), [(f'hk{i}', 100_000, 500_000_000) for i in range(5)], base_time=9000)
        self._seed(store, ('sol', 'tao'), [(f'old{i}', 100_000, 100_000_000) for i in range(5)], base_time=1)
        # current_time 9100, 24h window ⇒ start 9100-86400 < 1, so all included here;
        # use a current_time far enough out that the ancient rows fall off.
        refs = build_direction_references(store, current_time=90_000)
        ref = refs[('sol', 'tao')]
        # Only the 5 fresh swaps (rate 5000) survive the window; the ancient rows
        # at block 1..5 are before 90000-86400=3600.
        assert ref.reference == pytest.approx(5000.0, rel=1e-3)
        store.close()
