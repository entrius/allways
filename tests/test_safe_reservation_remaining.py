"""safe_reservation_remaining: user-side cushion before broadcasting confirm.

Mirrors the miner-side timeout cushion. The threshold lives in
``USER_POST_TX_CUSHION_BLOCKS`` (= ``CHALLENGE_WINDOW_BLOCKS``) and gates the
``alw swap post-tx`` / ``alw swap resume-reservation`` flows so a user can't
ship a confirm into a window where validators can no longer propose a
reservation extension.
"""

from allways.cli.swap_commands.helpers import safe_reservation_remaining
from allways.constants import USER_POST_TX_CUSHION_BLOCKS


class TestSafeReservationRemaining:
    def test_returns_remaining_when_well_above_cushion(self):
        # 30 blocks of runway, well clear of the 8-block cushion.
        assert safe_reservation_remaining(reserved_until=1_000, current_block=970) == 30

    def test_returns_none_at_zero_remaining(self):
        assert safe_reservation_remaining(reserved_until=1_000, current_block=1_000) is None

    def test_returns_none_when_already_expired(self):
        assert safe_reservation_remaining(reserved_until=1_000, current_block=1_005) is None

    def test_returns_none_at_cushion_boundary(self):
        # remaining == cushion is unsafe — validators refuse propose_extend
        # once current + CHALLENGE_WINDOW >= reserved_until.
        rem_eq_cushion = USER_POST_TX_CUSHION_BLOCKS
        assert (
            safe_reservation_remaining(
                reserved_until=1_000,
                current_block=1_000 - rem_eq_cushion,
            )
            is None
        )

    def test_returns_remaining_one_block_outside_cushion(self):
        # remaining == cushion + 1 is the first safe value.
        rem_just_safe = USER_POST_TX_CUSHION_BLOCKS + 1
        assert (
            safe_reservation_remaining(
                reserved_until=1_000,
                current_block=1_000 - rem_just_safe,
            )
            == rem_just_safe
        )
