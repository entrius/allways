from dataclasses import dataclass
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


@dataclass
class MinerPair:
    """A miner's posted exchange pair from on-chain quotes.

    After normalization, from_chain/to_chain are in canonical order.
    rate is for source→dest swaps, counter_rate is for dest→source swaps.
    Both rates use the same unit: 'dest per 1 source' in canonical order.
    """

    uid: int
    hotkey: str
    from_chain: str
    from_address: str
    to_chain: str
    to_address: str
    rate: float  # source→dest rate — for display/sorting
    rate_str: str = ''  # Raw string — for precise to_amount calculation
    counter_rate: float = 0.0  # dest→source rate (same unit as rate)
    counter_rate_str: str = ''  # Raw string — for precise to_amount calculation

    def get_rate_for_direction(self, swap_from_chain: str) -> tuple:
        """Return (rate, rate_str) for the given swap direction."""
        if swap_from_chain == self.from_chain:
            return self.rate, self.rate_str
        return self.counter_rate, self.counter_rate_str


@dataclass
class Reservation:
    """On-chain reservation record returned by `get_reservation`.

    Mirrors smart-contracts/ink/types.rs::Reservation. `hash` is the
    contract-side request_hash (keccak of miner + from_addr + chains + amounts).
    """

    hash: str
    from_addr: str
    from_chain: str
    to_chain: str
    tao_amount: int
    from_amount: int
    to_amount: int
    reserved_until: int


@dataclass
class Swap:
    """Full swap lifecycle data from the smart contract.

    Rate and miner source address are snapshotted from the miner's commitment
    at initiation time, so verification never depends on the miner remaining registered.

    Field mapping to Rust SwapData:
        user_hotkey  -> SwapData.user (AccountId)
        miner_hotkey -> SwapData.miner (AccountId)
    """

    id: int
    user_hotkey: str
    miner_hotkey: str
    from_chain: str
    to_chain: str
    from_amount: int
    to_amount: int
    tao_amount: int
    user_from_address: str
    user_to_address: str
    miner_from_address: str = ''
    miner_to_address: str = ''
    rate: str = ''
    from_tx_hash: str = ''
    from_tx_block: int = 0
    to_tx_hash: str = ''
    to_tx_block: int = 0
    status: SwapStatus = SwapStatus.ACTIVE
    initiated_block: int = 0
    timeout_block: int = 0
    fulfilled_block: int = 0
    completed_block: int = 0


@dataclass
class PendingExtension:
    """One in-flight optimistic extension proposal.

    Same shape for both reservation extensions (keyed by miner) and timeout
    extensions (keyed by swap_id) — only the on-chain Mapping differs. Maps
    to Rust types::PendingExtension { submitter, target_block, proposed_at }.
    """

    submitter: str  # validator hotkey ss58
    target_block: int
    proposed_at: int
