"""Unit tests for the pure taker swap-intake math (swap_intake.py).

collateral_amount must always be the SOL leg; to_amount must match calculate_to_amount so the CLI agrees with
the miner + validator. Concrete rates chosen so the arithmetic is hand-checkable.
"""

import pytest

from allways.cli.swap_commands.swap_intake import (
    MinerCandidate,
    compute_intake_amounts,
    rate_display_from_fixed,
    required_collateral,
    select_best_miner,
    swap_viable,
    to_smallest_units,
)

SOL = 1_000_000_000  # 1 SOL in lamports (9 dec)


def test_rate_display_from_fixed_floors_to_sig_figs():
    """Grandfathered pre-floor quotes must display FLOORED (matching set_quote + crown ingest),
    never rounded — rounding could show a rate one tick above what scoring/reserve use."""
    clean = 21 * 10**14  # 0.0021 × RATE_PRECISION, exact
    assert rate_display_from_fixed(clean) == '0.0021'
    dirty = 34512999 * 10**13  # 345.12999: would ROUND to 345.13; must floor to 345.12
    assert rate_display_from_fixed(dirty) == '345.12'


def test_to_smallest_units():
    assert to_smallest_units(1.0, 'sol') == SOL
    assert to_smallest_units(0.5, 'btc') == 50_000_000  # 8 dec
    assert to_smallest_units(2.0, 'tao') == 2_000_000_000  # 9 dec


def test_sol_to_btc_amounts():
    # 1 SOL at 0.5 BTC/SOL → 0.5 BTC; SOL is source ⇒ collateral_amount = from_amount.
    a = compute_intake_amounts('sol', 'btc', SOL, '0.5')
    assert a.from_amount == SOL
    assert a.to_amount == 50_000_000  # 0.5 BTC
    assert a.collateral_amount == SOL


def test_btc_to_sol_amounts():
    # 0.5 BTC at 0.5 BTC/SOL → 1 SOL; SOL is dest ⇒ collateral_amount = to_amount.
    a = compute_intake_amounts('btc', 'sol', 50_000_000, '0.5')
    assert a.from_amount == 50_000_000
    assert a.to_amount == SOL
    assert a.collateral_amount == SOL


def test_sol_to_tao_amounts():
    # both 9-dec; 1 SOL at 2 TAO/SOL → 2 TAO; collateral_amount = from_amount.
    a = compute_intake_amounts('sol', 'tao', SOL, '2')
    assert a.to_amount == 2_000_000_000
    assert a.collateral_amount == SOL


def test_tao_to_sol_amounts():
    # 2 TAO at 2 TAO/SOL → 1 SOL; collateral_amount = to_amount.
    a = compute_intake_amounts('tao', 'sol', 2_000_000_000, '2')
    assert a.to_amount == SOL
    assert a.collateral_amount == SOL


def test_non_sol_pair_rejected():
    with pytest.raises(ValueError):
        compute_intake_amounts('btc', 'tao', 100, '300')


def test_required_collateral_is_110_percent():
    assert required_collateral(SOL) == 1_100_000_000


def test_swap_viable_bounds_and_collateral():
    assert swap_viable(SOL, 1_100_000_000, 100_000_000, 10_000_000_000) == (True, '')
    assert swap_viable(SOL, 1_000_000_000, 100_000_000, 10_000_000_000)[0] is False  # collateral < 1.1x
    assert swap_viable(50_000_000, 10**12, 100_000_000, 10**10)[0] is False  # below min
    assert swap_viable(20_000_000_000, 10**12, 100_000_000, 10**10)[0] is False  # above max


def test_swap_viable_unset_bounds_still_checks_collateral():
    ok, reason = swap_viable(SOL, 0, 0, 0)
    assert ok is False and 'collateral' in reason  # bounds unset, but collateral still must back the leg


MIN, MAX = 100_000_000, 10_000_000_000  # 0.1 .. 10 SOL


def test_select_best_miner_picks_most_received():
    cands = [
        MinerCandidate(miner='m_low', rate_display='0.4', collateral=2 * SOL),  # 0.4 BTC/SOL
        MinerCandidate(miner='m_high', rate_display='0.6', collateral=2 * SOL),  # best for user
    ]
    best = select_best_miner(cands, 'sol', 'btc', SOL, MIN, MAX)
    assert best is not None
    cand, amts = best
    assert cand.miner == 'm_high'
    assert amts.to_amount == 60_000_000  # 0.6 BTC


def test_select_best_miner_skips_underfunded():
    cands = [
        MinerCandidate(miner='m_best', rate_display='0.6', collateral=SOL),  # can't back 1.1 SOL
        MinerCandidate(miner='m_ok', rate_display='0.4', collateral=2 * SOL),
    ]
    best = select_best_miner(cands, 'sol', 'btc', SOL, MIN, MAX)
    assert best is not None and best[0].miner == 'm_ok'


def test_select_best_miner_none_when_all_unviable():
    cands = [MinerCandidate(miner='m', rate_display='0.6', collateral=1)]
    assert select_best_miner(cands, 'sol', 'btc', SOL, MIN, MAX) is None


# ---- candidate_miners: active gate + tracked collateral ----
class _FakeCandClient:
    """Minimal client for candidate_miners: get_all('MinerQuote') + get_miner_state."""

    def __init__(self, quotes, states):
        self._quotes = quotes  # list of SimpleNamespace(miner, from_chain, to_chain, rate)
        self._states = states  # {miner: SimpleNamespace(active, collateral)}

    def get_all(self, name):
        assert name == 'MinerQuote'
        return [(f'pda{i}', q) for i, q in enumerate(self._quotes)]

    def get_miner_state(self, miner):
        return self._states.get(miner)


def test_candidate_miners_excludes_inactive():
    from types import SimpleNamespace

    from allways.cli.swap_commands.swap_intake import candidate_miners

    q = lambda m: SimpleNamespace(miner=m, from_chain='tao', to_chain='sol', rate=15 * 10**17)
    client = _FakeCandClient(
        quotes=[q('active-m'), q('inactive-m'), q('no-state-m')],
        states={
            'active-m': SimpleNamespace(active=True, collateral=976_466_670),
            'inactive-m': SimpleNamespace(active=False, collateral=5 * SOL),
            # 'no-state-m' absent → get_miner_state returns None
        },
    )
    out = candidate_miners(client, 'tao', 'sol')
    assert [c.miner for c in out] == ['active-m']
    assert out[0].collateral == 976_466_670  # tracked field, not vault lamports
