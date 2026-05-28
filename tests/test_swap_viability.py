"""Collateral viability checks — forward vs reverse payout legs."""

from allways.constants import BTC_TO_SAT, TAO_TO_RAO
from allways.utils.rate import (
    calculate_to_amount,
    check_swap_viability,
    max_dest_from_collateral,
)

BTC_DEC = 8
TAO_DEC = 9


class TestMaxDestFromCollateral:
    def test_tao_to_btc_caps_at_collateral_rate(self):
        collateral = 5 * TAO_TO_RAO
        max_dest = max_dest_from_collateral(collateral, '345', 'tao', 'btc')
        expected = calculate_to_amount(collateral, '345', True, TAO_DEC, BTC_DEC)
        assert max_dest == expected
        assert max_dest < BTC_TO_SAT  # 5 TAO cannot back 1 BTC @ 345

    def test_btc_to_tao_is_not_reverse_payout(self):
        assert max_dest_from_collateral(5 * TAO_TO_RAO, '345', 'btc', 'tao') == 0


class TestCheckSwapViability:
    def test_forward_swap_requires_tao_within_collateral(self):
        viable, reason = check_swap_viability(
            3_450_000_000,
            5 * TAO_TO_RAO,
            0,
            0,
            from_chain='btc',
            to_chain='tao',
            to_amount=3_450_000_000,
            rate='345',
        )
        assert viable is True
        assert reason == ''

    def test_forward_swap_rejects_tao_above_collateral(self):
        viable, reason = check_swap_viability(
            10 * TAO_TO_RAO,
            5 * TAO_TO_RAO,
            0,
            0,
            from_chain='btc',
            to_chain='tao',
            to_amount=10 * TAO_TO_RAO,
            rate='345',
        )
        assert viable is False
        assert 'insufficient collateral' in reason

    def test_reverse_swap_rejects_unbounded_dest_payout(self):
        collateral = 5 * TAO_TO_RAO
        tao_send = int(0.1 * TAO_TO_RAO)
        max_dest = max_dest_from_collateral(collateral, '345', 'tao', 'btc')
        viable, reason = check_swap_viability(
            tao_send,
            collateral,
            0,
            0,
            from_chain='tao',
            to_chain='btc',
            to_amount=max_dest + 1,
            rate='345',
        )
        assert viable is False
        assert reason == 'dest payout exceeds collateral-backed limit'

    def test_reverse_swap_accepts_collateral_backed_dest(self):
        collateral = 5 * TAO_TO_RAO
        tao_send = int(0.1 * TAO_TO_RAO)
        to_amount = calculate_to_amount(tao_send, '345', True, TAO_DEC, BTC_DEC)
        max_dest = max_dest_from_collateral(collateral, '345', 'tao', 'btc')
        assert to_amount <= max_dest
        viable, reason = check_swap_viability(
            tao_send,
            collateral,
            0,
            0,
            from_chain='tao',
            to_chain='btc',
            to_amount=to_amount,
            rate='345',
        )
        assert viable is True
        assert reason == ''

    def test_legacy_call_without_direction_still_checks_tao_leg(self):
        viable, _ = check_swap_viability(10 * TAO_TO_RAO, 5 * TAO_TO_RAO, 0, 0)
        assert viable is False
