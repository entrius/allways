from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional


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
    """A miner's posted exchange pair from on-chain commitments."""

    uid: int
    hotkey: str
    source_chain: str
    source_address: str
    dest_chain: str
    dest_address: str
    rate: float  # TAO per 1 non-TAO asset — for display/sorting
    rate_str: str = ''  # Raw string from commitment — used for precise dest_amount calculation


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
    source_chain: str
    dest_chain: str
    source_amount: int
    dest_amount: int
    tao_amount: int
    user_source_address: str
    user_dest_address: str
    miner_source_address: str = ''
    miner_dest_address: str = ''
    rate: str = ''
    source_tx_hash: str = ''
    source_tx_block: int = 0
    dest_tx_hash: str = ''
    dest_tx_block: int = 0
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
    source_chain: str = ''
    dest_chain: str = ''
    source_amount: int = 0
    tao_amount: int = 0
    user_source_address: str = ''
    user_dest_address: str = ''
    source_tx_hash: str = ''
    source_tx_block: int = 0
    source_proof: str = ''
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
    source_chain: str
    dest_chain: str
    source_amount: int
    tao_amount: int
    user_source_address: str
    user_dest_address: str
    status: ReservationStatus = ReservationStatus.ACTIVE
    reserved_at_block: int = 0
    expires_at_block: int = 0
    created_at: float = 0.0


@dataclass
class MinerScoringStats:
    """Per-miner stats accumulated from the scoring window."""

    uid: int
    windowed_fees: int = 0
    completed: int = 0
    timeouts: int = 0
    fulfillment_times_by_chain: Dict[str, List[int]] = field(default_factory=dict)
