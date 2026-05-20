"""safe_reservation_remaining: user-side cushion before broadcasting confirm.

Mirrors the miner-side timeout cushion. Threshold is USER_POST_TX_CUSHION_BLOCKS
(pinned to EXTEND_THRESHOLD_BLOCKS); inside this window the validator extension
flow cannot land a propose + challenge before reservation expiry, so the
``alw swap post-tx`` / ``alw swap resume-reservation`` flows refuse to ship
a confirm that has no rescue path.
"""

from allways.cli.swap_commands.helpers import safe_reservation_remaining
from allways.constants import USER_POST_TX_CUSHION_BLOCKS


class TestSafeReservationRemaining:
    def test_returns_remaining_when_well_above_cushion(self):
        # 30 blocks beyond the cushion — comfortably safe.
        rem = USER_POST_TX_CUSHION_BLOCKS + 30
        assert safe_reservation_remaining(reserved_until=1_000, current_block=1_000 - rem) == rem

    def test_returns_none_at_zero_remaining(self):
        assert safe_reservation_remaining(reserved_until=1_000, current_block=1_000) is None

    def test_returns_none_when_already_expired(self):
        assert safe_reservation_remaining(reserved_until=1_000, current_block=1_005) is None

    def test_returns_none_at_cushion_boundary(self):
        # remaining == cushion is unsafe: validators can't propose extend
        # once current + CHALLENGE_WINDOW >= reserved_until.
        assert (
            safe_reservation_remaining(
                reserved_until=1_000,
                current_block=1_000 - USER_POST_TX_CUSHION_BLOCKS,
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
