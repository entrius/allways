"""Send now, confirm later: tracks sent signatures and resolves them in one batched status read."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

# getSignatureStatuses accepts at most 256 signatures per call.
STATUS_BATCH = 256


@dataclass(frozen=True)
class Sent:
    signature: str
    intent: Any  # the deadline_queue.Intent that produced it
    sent_at: float


@dataclass(frozen=True)
class Resolved:
    landed: List[Sent]
    failed: List[Tuple[Sent, Any]]  # (sent, on-chain err)
    expired: List[Sent]  # never seen within the expiry window — safe to re-plan


class ConfirmTracker:
    """The actuator registers each signature here instead of blocking on it. ``resolve`` is called once per
    actuator tick; one status call covers every pending signature."""

    def __init__(self, expiry_secs: float = 90.0) -> None:
        # Past ~90s the blockhash is dead, so an unseen tx can never land and the intent may be re-sent.
        self.expiry_secs = expiry_secs
        self._pending: Dict[str, Sent] = {}
        self._lock = threading.Lock()

    def track(self, signature: str, intent: Any, now: float) -> None:
        with self._lock:
            self._pending[signature] = Sent(signature, intent, now)

    def pending(self) -> List[Sent]:
        with self._lock:
            return list(self._pending.values())

    def resolve(self, rpc, now: float) -> Resolved:
        sents = self.pending()
        landed: List[Sent] = []
        failed: List[Tuple[Sent, Any]] = []
        expired: List[Sent] = []
        for i in range(0, len(sents), STATUS_BATCH):
            chunk = sents[i : i + STATUS_BATCH]
            statuses = rpc.get_signature_statuses([s.signature for s in chunk])
            for sent, status in zip(chunk, statuses):
                if status is None:
                    if now - sent.sent_at > self.expiry_secs:
                        expired.append(sent)
                    continue
                if status.get('err') is not None:
                    failed.append((sent, status['err']))
                elif status.get('confirmationStatus') in ('confirmed', 'finalized'):
                    landed.append(sent)
        with self._lock:
            for sent in landed + [f[0] for f in failed] + expired:
                self._pending.pop(sent.signature, None)
        return Resolved(landed, failed, expired)
