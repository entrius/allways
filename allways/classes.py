from enum import IntEnum
from typing import Optional


class SwapStatus(IntEnum):
    ACTIVE = 0
    FULFILLED = 1
    COMPLETED = 2
    TIMED_OUT = 3


class MinerActivity(IntEnum):
    """A miner's per-instant availability for the crown, reduced from its
    activity-transition timeline. ``REWARD_MINER_STATES`` decides which states
    earn — by default only AVAILABLE (a busy miner isn't takeable)."""

    AVAILABLE = 0
    RESERVED = 1
    FULFILLING = 2


class ActivityTransition(IntEnum):
    """Edges of the ``MinerActivity`` machine. Values double as the coincident-
    instant tiebreak: at one block a swap closes before a re-reservation opens,
    which opens before its initiation, and a reservation lapse applies last so an
    in-flight swap survives its synthetic ``RESERVE_EXPIRE``."""

    FULFILL_END = 0  # SwapCompleted / SwapTimedOut
    RESERVE_START = 1  # PoolResolved
    FULFILL_START = 2  # SwapInitiated
    RESERVE_EXPIRE = 3  # synthetic, at reserve block_time + reservation_ttl_secs


# Guarded transitions; an undefined (state, transition) pair returns None so the
# caller holds state (and can warn once). The RESERVE_EXPIRE no-ops are explicit
# (not warned): a swap consumes the reservation, so its synthetic expiry fires
# later while FULFILLING, or after the swap already closed (AVAILABLE).
_ACTIVITY_TRANSITIONS = {
    (MinerActivity.AVAILABLE, ActivityTransition.RESERVE_START): MinerActivity.RESERVED,
    (MinerActivity.AVAILABLE, ActivityTransition.RESERVE_EXPIRE): MinerActivity.AVAILABLE,
    (MinerActivity.RESERVED, ActivityTransition.FULFILL_START): MinerActivity.FULFILLING,
    (MinerActivity.RESERVED, ActivityTransition.RESERVE_EXPIRE): MinerActivity.AVAILABLE,
    (MinerActivity.FULFILLING, ActivityTransition.RESERVE_EXPIRE): MinerActivity.FULFILLING,
    (MinerActivity.FULFILLING, ActivityTransition.FULFILL_END): MinerActivity.AVAILABLE,
}


def next_activity(state: MinerActivity, transition: ActivityTransition) -> Optional[MinerActivity]:
    """Next state, or ``None`` for an unexpected (undefined) transition."""
    return _ACTIVITY_TRANSITIONS.get((state, transition))
