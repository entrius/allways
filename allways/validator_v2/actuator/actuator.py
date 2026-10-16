"""The actuator: the only component that sends program transactions.

Every tick it asks each planner what is due given the newest World, queues the answers by deadline,
sends the most urgent ones without waiting for them to land, and settles earlier sends in one batched
status read. Nothing else in the validator can delay it: it reads the World, never another component."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterable, Optional, Protocol, Set, Tuple

import bittensor as bt

from allways.validator_v2.chain.confirm import ConfirmTracker
from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue, Intent
from allways.validator_v2.world.snapshot import World

# A World older than this is too stale to decide deadline-bound actions on; the actuator waits instead.
MAX_WORLD_AGE_SECS = 30.0
SENDS_PER_TICK = 16


class Planner(Protocol):
    """Pure decision: from a World, the intents due now. No sends, no RPC beyond read-only verification
    results it was handed (see legs.LegVerifier)."""

    name: str

    def plan(self, world: World, now: float) -> Iterable[Intent]: ...


# kind (the first element of an Intent key) → sends the tx, returns its signature (or None if no-op).
Executor = Callable[[Any, Intent], Optional[str]]


class Actuator(Component):
    name = 'actuator'
    interval_secs = 2.0
    stall_secs = 60.0
    critical = True

    def __init__(
        self,
        world: Latest[World],
        planners: Iterable[Planner],
        executors: Dict[str, Executor],
        client_factory: Callable[[], Any],
        inbox: Optional[DeadlineQueue] = None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world = world
        self.planners = list(planners)
        self.executors = executors
        self._client_factory = client_factory
        self._client: Any = None
        self.queue = inbox or DeadlineQueue()  # serving/relay enqueue their writes here too
        self.confirms = ConfirmTracker()
        self._in_flight: Set[Tuple[Any, ...]] = set()

    def setup(self) -> None:
        self._client = self._client_factory()

    def tick(self, now: float) -> None:
        self._settle(now)
        world, _ = self.world.get()
        if world is None or now - world.taken_at > MAX_WORLD_AGE_SECS:
            bt.logging.warning('actuator: no fresh World, holding sends')
            return
        for planner in self.planners:
            try:
                for intent in planner.plan(world, now):
                    if intent.key not in self._in_flight:
                        self.queue.push(intent)
            except Exception as e:
                bt.logging.warning(f'actuator: planner {planner.name} failed: {e}')
        for intent in self.queue.pop_due(now, limit=SENDS_PER_TICK):
            if intent.deadline < now:
                bt.logging.warning(f'actuator: {intent.key} past its deadline by {now - intent.deadline:.0f}s')
            self._send(intent, now)

    def _send(self, intent: Intent, now: float) -> None:
        executor = self.executors.get(intent.key[0])
        if executor is None:
            bt.logging.error(f'actuator: no executor for {intent.key[0]!r}')
            return
        try:
            signature = executor(self._client, intent)
        except Exception as e:
            bt.logging.warning(f'actuator: {intent.key} send failed: {e}')
            return  # the planner re-plans it next tick if still due
        if signature:
            self.confirms.track(signature, intent, now)
            self._in_flight.add(intent.key)

    def _settle(self, now: float) -> None:
        if not self.confirms.pending():
            return
        resolved = self.confirms.resolve(self._client.rpc, now)
        for sent in resolved.landed:
            self._in_flight.discard(sent.intent.key)
        for sent, err in resolved.failed:
            bt.logging.warning(f'actuator: {sent.intent.key} failed on-chain: {err}')
            self._in_flight.discard(sent.intent.key)
        for sent in resolved.expired:
            self._in_flight.discard(sent.intent.key)  # never landed: the planner may send it again
