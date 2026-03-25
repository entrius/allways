"""Thread-safe queue for swap confirmations awaiting source tx confirmations.

Written by axon handler thread (handle_swap_confirm), read by forward loop thread
(_process_pending_confirms). Keyed by miner_hotkey since reservations are 1:1 per miner.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

MAX_QUEUE_SIZE = 50


@dataclass
class PendingConfirm:
    """All data needed to call vote_initiate once tx confirmations are met."""

    miner_hotkey: str
    source_tx_hash: str
    source_chain: str
    dest_chain: str
    source_address: str
    dest_address: str
    tao_amount: int
    source_amount: int
    dest_amount: int
    miner_deposit_address: str
    rate_str: str
    reserved_until: int
    queued_at: float = field(default_factory=time.time)


class PendingConfirmQueue:
    """Thread-safe queue of unconfirmed swap confirmations.

    Keyed by miner_hotkey — one reservation per miner at a time,
    so re-submissions for the same miner overwrite the previous entry.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pending: Dict[str, PendingConfirm] = {}

    def enqueue(self, item: PendingConfirm) -> bool:
        """Add or replace a pending confirm. Returns False if queue is full (and not an overwrite)."""
        with self._lock:
            if item.miner_hotkey not in self._pending and len(self._pending) >= MAX_QUEUE_SIZE:
                return False
            self._pending[item.miner_hotkey] = item
            return True

    def get_all(self) -> List[PendingConfirm]:
        """Return a snapshot of all pending items."""
        with self._lock:
            return list(self._pending.values())

    def remove(self, miner_hotkey: str) -> Optional[PendingConfirm]:
        """Remove and return a specific entry."""
        with self._lock:
            return self._pending.pop(miner_hotkey, None)

    def has(self, miner_hotkey: str) -> bool:
        with self._lock:
            return miner_hotkey in self._pending

    def size(self) -> int:
        with self._lock:
            return len(self._pending)
