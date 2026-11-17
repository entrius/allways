"""Logging for conditions that last: say when one starts and when it clears, never every tick between."""

from __future__ import annotations

from typing import Callable, Dict, Hashable, Optional

import bittensor as bt


class Episodes:
    def __init__(self) -> None:
        self._open: Dict[Hashable, str] = {}

    def start(self, key: Hashable, message: str, log: Callable[[str], None] = bt.logging.warning) -> None:
        """Log ``message`` if ``key`` isn't already open, or if its message changed."""
        if self._open.get(key) != message:
            self._open[key] = message
            log(message, stacklevel=2)  # the line is the caller's, not this helper's

    def end(self, key: Hashable, message: str = '', log: Callable[[str], None] = bt.logging.info) -> None:
        """Close ``key``; log ``message`` only if it was open."""
        if self._open.pop(key, None) is not None and message:
            log(message, stacklevel=2)

    def is_open(self, key: Hashable) -> bool:
        return key in self._open


class LowBalance:
    """Warns when a signing wallet runs low: once on the way down, again every ``remind_secs`` while it stays
    low, and once when a top-up clears it (with headroom, so a balance hovering at the line doesn't flap).
    Reads at most every ``check_secs``; ``recheck`` asks for a read on the next call (a send that failed for
    want of funds), so a drained wallet is named at once without polling it more often."""

    def __init__(
        self,
        label: str,
        unit: str,
        scale: float,
        low: int,
        check_secs: float,
        remind_secs: float,
        clear_ratio: float = 1.2,
    ) -> None:
        self.label, self.unit, self.scale, self.low = label, unit, scale, low
        self.check_secs, self.remind_secs, self.clear_ratio = check_secs, remind_secs, clear_ratio
        self._next_read = 0.0
        self._warned_at: Optional[float] = None  # set while low

    def recheck(self) -> None:
        self._next_read = 0.0

    def maybe_check(self, now: float, read: Callable[[], Optional[int]]) -> None:
        if now < self._next_read:
            return
        self._next_read = now + self.check_secs
        try:
            balance = read()
        except Exception:
            return  # unreadable is not low; the next read decides
        if balance is None:
            return
        shown = f'{balance / self.scale:.4f} {self.unit}'
        if balance < self.low:
            if self._warned_at is None or now - self._warned_at >= self.remind_secs:
                self._warned_at = now
                bt.logging.warning(
                    f'{self.label} has {shown} — top it up (warns under {self.low / self.scale:g} {self.unit})'
                )
        elif self._warned_at is not None and balance >= self.low * self.clear_ratio:
            self._warned_at = None
            bt.logging.info(f'{self.label} topped up: {shown}')


def vote_line(contract: str, kind: str, subject: object, values: object = None) -> None:
    """One INFO line per quorum vote this validator casts, the same shape on every validator, so two
    validators' proposals for a round can be lined up (``grep 'vote '``) and diffed: a round binds the
    values, and votes that differ never reach quorum together."""
    line = f'vote {contract}:{kind} {_short(subject)}'
    if values is not None:
        line += f' values={_short(values)}'
    bt.logging.info(line)


def _short(value: object) -> str:
    """Compact and stable: keys shortened to 8 characters, sequences kept in order."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()[:16]
    if isinstance(value, (list, tuple)):
        return '[' + ','.join(_short(v) for v in value) + ']'
    text = str(value)
    return text[:8] if len(text) >= 32 and text.isalnum() else text
