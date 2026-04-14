from dataclasses import dataclass
from enum import IntEnum
from typing import Optional


class SwapStatus(IntEnum):
    ACTIVE = 0
    FULFILLED = 1
    COMPLETED = 2
    TIMED_OUT = 3


class SwapRequestStatus(IntEnum):
    QUEUED = 0
    VERIFYING = 1
    CONFIRMED = 2
    REJECTED = 3
    ON_CHAIN = 4
    RETRYING = 5


class ReservationStatus(IntEnum):
    ACTIVE = 0
    CONFIRMED = 1
    EXPIRED = 2


@dataclass
class MinerPair:
    """A miner's posted exchange pair from on-chain commitments.

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
class SwapRequest:
    """API request tracking for validator-routed swap initiation."""

    request_id: str
    status: SwapRequestStatus = SwapRequestStatus.QUEUED
    miner_hotkey: str = ''
    from_chain: str = ''
    to_chain: str = ''
    from_amount: int = 0
    tao_amount: int = 0
    user_from_address: str = ''
    user_to_address: str = ''
    from_tx_hash: str = ''
    from_tx_block: int = 0
    from_proof: str = ''
    swap_id: Optional[int] = None
    reservation_id: str = ''
    reserved_at_block: int = 0
    error: str = ''
    created_at: float = 0.0
    retry_count: int = 0


@dataclass
class Reservation:
    reservation_id: str
    miner_hotkey: str
    from_chain: str
    to_chain: str
    from_amount: int
    tao_amount: int
    user_from_address: str
    user_to_address: str
    status: ReservationStatus = ReservationStatus.ACTIVE
    reserved_at_block: int = 0
    expires_at_block: int = 0
    created_at: float = 0.0
