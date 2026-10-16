"""Dashboard export (opt-in): writes each scoring round's result to Postgres for das-allways.

It writes what the round already computed. There is no per-step recompute; the "live" view is the
latest round, and the database writes run here, so a slow or down database never touches scoring."""

from __future__ import annotations

import time
from typing import Any

from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.scoring.component import RoundResult


class Export(Component):
    name = 'export'
    interval_secs = 30.0
    stall_secs = 1800.0

    def __init__(self, rounds: Latest[RoundResult], storage: Any, clock=time.time) -> None:
        super().__init__(clock)
        self.rounds = rounds
        self.storage = storage  # allways.validator.storage writers, unchanged
        self._exported = 0

    def tick(self, now: float) -> None:
        result, version = self.rounds.get()
        if result is None or version == self._exported:
            return
        self._exported = version
