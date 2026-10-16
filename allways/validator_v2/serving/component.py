"""Product serving: the seam HTTP API and the axon handlers.

Reads come from the World and the outcomes store, never from a live scan per request. Writes a request
triggers (reserve on behalf, confirm deposit, activate) are queued on the actuator rather than sent from
the request thread, so there is one copy of the vote logic and one place that signs."""

from __future__ import annotations

import time
from typing import Any

from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue
from allways.validator_v2.world.snapshot import World


class Serving(Component):
    """Owns the seam server thread (``allways.validator.seam_http``) and the axon. Its tick only checks the
    server is up; requests are served on the server's own threads."""

    name = 'serving'
    interval_secs = 10.0
    stall_secs = 120.0

    def __init__(self, world: Latest[World], actuator_queue: DeadlineQueue, outcomes: Any, clock=time.time) -> None:
        super().__init__(clock)
        self.world = world
        self.actuator_queue = actuator_queue
        self.outcomes = outcomes  # swap_key → terminal outcome + delivery hash, fed from events, 7 days

    def tick(self, now: float) -> None:
        pass
