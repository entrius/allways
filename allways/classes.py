from dataclasses import dataclass
from enum import IntEnum


class SwapStatus(IntEnum):
    ACTIVE = 0
    FULFILLED = 1
    COMPLETED = 2
    TIMED_OUT = 3


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
class PendingExtension:
    """One in-flight optimistic extension proposal.

    Same shape for both reservation extensions (keyed by miner) and timeout
    extensions (keyed by swap_id) — only the on-chain Mapping differs. Maps
    to Rust types::PendingExtension { submitter, target_block, proposed_at }.
    """

    submitter: str  # validator hotkey ss58
    target_block: int
    proposed_at: int
