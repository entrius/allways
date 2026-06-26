"""C2 — rate-quality curve, orientation, and unit-conversion tests (pure fns)."""

import pytest

from allways.constants import (
    BTC_TO_SAT,
    RATE_QUALITY_FLOOR_ADV,
    RATE_QUALITY_MIN,
    RATE_QUALITY_TOLERANCE_BPS,
    TAO_TO_RAO,
)
from allways.validator.scoring import (
    _canonical_rate_and_weight,
    quality_curve,
    rate_advantage,
    rate_quality,
)

TOL = RATE_QUALITY_TOLERANCE_BPS / 10_000.0


def realized_canonical_rate(from_chain, to_chain, from_amount, to_amount):
    """Canonical 'dest per source' display rate (drops the volume weight)."""
    return _canonical_rate_and_weight(from_chain, to_chain, from_amount, to_amount)[0]


def btc_tao_legs(tao_per_btc, btc=1.0):
    """Native (from_sat, to_rao) for selling `btc` BTC at `tao_per_btc`."""
    return int(btc * BTC_TO_SAT), int(btc * tao_per_btc * TAO_TO_RAO)


def tao_btc_legs(tao_per_btc, btc=1.0):
    """Native (from_rao, to_sat) for buying `btc` BTC at `tao_per_btc` paid."""
    return int(btc * tao_per_btc * TAO_TO_RAO), int(btc * BTC_TO_SAT)


# ── realized_canonical_rate: native → canonical TAO/BTC, both directions ──


def test_canonical_rate_btc_to_tao():
    f, t = btc_tao_legs(250.0)
    assert realized_canonical_rate('btc', 'tao', f, t) == pytest.approx(250.0)


def test_canonical_rate_tao_to_btc_same_number():
    # Paying 250 TAO for 1 BTC reads as 250 TAO/BTC regardless of direction.
    f, t = tao_btc_legs(250.0)
    assert realized_canonical_rate('tao', 'btc', f, t) == pytest.approx(250.0)


def test_canonical_rate_scales_with_volume_not_amount():
    # 3 BTC at 250 TAO/BTC is still 250 TAO/BTC.
    f, t = btc_tao_legs(250.0, btc=3.0)
    assert realized_canonical_rate('btc', 'tao', f, t) == pytest.approx(250.0)


@pytest.mark.parametrize('f,t', [(0, 100), (100, 0), (0, 0), (-5, 100)])
def test_canonical_rate_nonpositive_legs_guarded(f, t):
    assert realized_canonical_rate('btc', 'tao', f, t) == 0.0


# ── rate_advantage: direction-aware orientation ──


def test_advantage_btc_to_tao_higher_is_better():
    # Selling BTC: more TAO per BTC than market = positive advantage.
    assert rate_advantage('btc', 'tao', 260.0, 250.0) == pytest.approx(0.04)
    assert rate_advantage('btc', 'tao', 240.0, 250.0) == pytest.approx(-0.04)


def test_advantage_tao_to_btc_lower_is_better():
    # Buying BTC: paying fewer TAO per BTC than market = positive advantage.
    assert rate_advantage('tao', 'btc', 240.0, 250.0) == pytest.approx(0.04)
    assert rate_advantage('tao', 'btc', 260.0, 250.0) == pytest.approx(-0.04)


def test_advantage_zero_market_guarded():
    assert rate_advantage('btc', 'tao', 250.0, 0.0) == 0.0


# ── quality_curve: one-sided clamp shape ──


def test_curve_at_and_above_market_is_one():
    assert quality_curve(0.0) == 1.0
    assert quality_curve(0.5) == 1.0
    assert quality_curve(-TOL) == 1.0  # edge of the deadband still full


def test_curve_within_tolerance_is_one():
    assert quality_curve(-TOL / 2) == 1.0


def test_curve_floor_and_below():
    assert quality_curve(RATE_QUALITY_FLOOR_ADV) == RATE_QUALITY_MIN
    assert quality_curve(RATE_QUALITY_FLOOR_ADV - 0.1) == RATE_QUALITY_MIN


def test_curve_ramps_linearly_through_midpoint():
    mid = (-TOL + RATE_QUALITY_FLOOR_ADV) / 2
    expected = 1.0 + 0.5 * (RATE_QUALITY_MIN - 1.0)
    assert quality_curve(mid) == pytest.approx(expected)


def test_curve_is_monotonic_nonincreasing():
    advs = [0.1, 0.0, -0.02, -0.05, -0.08, -0.10, -0.2]
    qs = [quality_curve(a) for a in advs]
    assert all(qs[i] >= qs[i + 1] for i in range(len(qs) - 1))
    assert all(RATE_QUALITY_MIN <= q <= 1.0 for q in qs)


# ── rate_quality: realized-vs-reference wrapper + neutral fail-safes ──
# Signature is (from_chain, to_chain, realized_rate, reference_rate); both
# numbers are canonical TAO/BTC realized clearing rates (C-rev). The
# native→canonical conversion is realized_canonical_rate, tested above.


def test_quality_none_reference_is_neutral():
    assert rate_quality('btc', 'tao', 200.0, reference_rate=None) == 1.0


def test_quality_zero_reference_is_neutral():
    assert rate_quality('btc', 'tao', 200.0, reference_rate=0.0) == 1.0


def test_quality_zero_realized_is_neutral():
    # No realized rate for this miner in-window ⇒ nothing to judge ⇒ neutral.
    assert rate_quality('btc', 'tao', 0.0, reference_rate=250.0) == 1.0


def test_quality_at_reference_full_both_directions():
    assert rate_quality('btc', 'tao', 250.0, reference_rate=250.0) == 1.0
    assert rate_quality('tao', 'btc', 250.0, reference_rate=250.0) == 1.0


def test_quality_above_reference_capped_at_one():
    # Selling BTC at 300 vs reference 250 — great deal, but crown already pays it.
    assert rate_quality('btc', 'tao', 300.0, reference_rate=250.0) == 1.0


def test_quality_below_reference_penalized():
    # Selling BTC at 240 vs reference 250: adv -0.04 → ramp below 1.0.
    q = rate_quality('btc', 'tao', 240.0, reference_rate=250.0)
    assert RATE_QUALITY_MIN < q < 1.0
    assert q == pytest.approx(quality_curve(-0.04))


def test_quality_far_below_reference_hits_floor():
    # 20% worse than the reference — well past the floor advantage.
    assert rate_quality('btc', 'tao', 200.0, reference_rate=250.0) == RATE_QUALITY_MIN
