"""Runs the components and decides what a stall means."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Sequence

import bittensor as bt

from allways.validator_v2.runtime.component import Component


@dataclass(frozen=True)
class Health:
    name: str
    critical: bool
    alive: bool
    heartbeat_age: float
    stalled: bool
    ticks: int
    errors: int


class Supervisor:
    """Starts every component on its own thread and checks heartbeats.

    Only a critical component (the World reader and the actuator) that stalls or dies restarts the
    process; a lagging scorer or relay is logged and left running, so it can never take the deadline
    path down with it."""

    def __init__(
        self,
        components: Sequence[Component],
        on_fatal: Callable[[str], None],
        check_interval_secs: float = 10.0,
    ) -> None:
        names = [c.name for c in components]
        if len(set(names)) != len(names):
            raise ValueError(f'component names must be unique: {names}')
        self.components = list(components)
        self.on_fatal = on_fatal
        self.check_interval_secs = check_interval_secs
        self.stop = threading.Event()

    def start(self) -> None:
        for component in self.components:
            component.start(self.stop)

    def health(self) -> List[Health]:
        out = []
        for c in self.components:
            age = c.heartbeat_age()
            out.append(Health(c.name, c.critical, c.alive, age, age > c.stall_secs, c.ticks, c.errors))
        return out

    def check(self) -> List[Health]:
        """One supervision pass. Returns the unhealthy components; calls ``on_fatal`` for a critical one."""
        bad = [h for h in self.health() if h.stalled or not h.alive]
        for h in bad:
            why = 'dead' if not h.alive else f'stalled {h.heartbeat_age:.0f}s'
            if h.critical:
                self.on_fatal(f'{h.name} {why}')
            else:
                bt.logging.warning(f'supervisor: {h.name} {why} (non-critical, left running)')
        return bad

    def run_forever(self) -> None:
        self.start()
        try:
            while not self.stop.is_set():
                self.check()
                time.sleep(self.check_interval_secs)
        finally:
            self.stop.set()
