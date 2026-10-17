"""Logging for conditions that last: say when one starts and when it clears, never every tick between."""

from __future__ import annotations

from typing import Callable, Dict, Hashable

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
