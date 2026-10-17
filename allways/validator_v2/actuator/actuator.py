"""The actuator: the only component that sends program transactions.

Every tick it asks each planner what is due given the newest World, queues the answers by deadline,
sends the most urgent ones without waiting for them to land, and settles earlier sends in one batched
status read. Nothing else in the validator can delay it: it reads the World, never another component."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterable, Optional, Protocol, Set, Tuple

import bittensor as bt

from allways.validator_v2.actuator.planners import ACTIONS
from allways.validator_v2.chain.confirm import ConfirmTracker
from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue, Intent
from allways.validator_v2.world.snapshot import World

# A World older than this is too stale to decide deadline-bound actions on; the actuator waits instead.
MAX_WORLD_AGE_SECS = 30.0
SENDS_PER_TICK = 16
SENT, NOOP, FAILED = 'sent', 'noop', 'failed'


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
        self.queue = inbox or DeadlineQueue()  # writes submitted by serving and the relays
        self.confirms = ConfirmTracker()
        self._in_flight: Set[Tuple[Any, ...]] = set()
        self._submitted: Set[Tuple[Any, ...]] = set()  # in-flight keys that came from the inbox
        self._resting: Dict[Tuple[Any, ...], float] = {}  # key → when a send that went nowhere may be retried

    def setup(self) -> None:
        self._client = self._client_factory()

    def tick(self, now: float) -> None:
        self._settle(now)
        world, _ = self.world.get()
        if world is None or now - world.taken_at > MAX_WORLD_AGE_SECS:
            bt.logging.warning('actuator: no fresh World, holding sends')
            return
        planned: Dict[Tuple[Any, ...], Intent] = {}
        for planner in self.planners:
            try:
                for intent in planner.plan(world, now):
                    planned[intent.key] = intent
            except Exception as e:
                bt.logging.warning(f'actuator: planner {planner.name} failed: {e}')
        # Planned intents are rebuilt from the World every tick, so an unsent one is simply re-planned.
        # Submitted ones (serving, relays) exist only in the inbox, so an unsent one goes back in.
        submitted = {i.key: i for i in self.queue.pop_due(now)}
        self._resting = {k: t for k, t in self._resting.items() if t > now}
        due = [
            i
            for i in {**submitted, **planned}.values()
            if i.not_before <= now and i.key not in self._in_flight and i.key not in self._resting
        ]
        for key, held in submitted.items():
            if key in self._resting:
                self.queue.push(held)  # submitted work exists only in the inbox: never drop it while it rests
        due.sort(key=lambda i: (int(i.urgency), i.deadline))
        sent = 0
        for intent in due:
            action = ACTIONS.get(intent.key[0])
            if sent >= SENDS_PER_TICK or (action is not None and action.needs_ledgers and not world.complete):
                if intent.key in submitted:
                    self.queue.push(intent)
                continue
            if intent.deadline < now:
                bt.logging.warning(f'actuator: {intent.key} past its deadline by {now - intent.deadline:.0f}s')
            outcome = self._send(intent, now)
            if outcome == SENT:
                if intent.key in submitted:
                    self._submitted.add(intent.key)
            else:
                # Went nowhere: rest it, so it isn't re-sent every tick. A submitted intent that was a
                # no-op (already voted, lost a race) is done; one that failed goes back in the inbox.
                self._resting[intent.key] = now + (action.retry_secs if action is not None else 6.0)
                if intent.key in submitted and outcome == FAILED:
                    self.queue.push(intent)
            sent += 1

    def _send(self, intent: Intent, now: float) -> str:
        """SENT when a transaction went out and is being tracked; NOOP when the executor found nothing to
        do; FAILED when the send raised."""
        executor = self.executors.get(intent.key[0])
        if executor is None:
            bt.logging.error(f'actuator: no executor for {intent.key[0]!r}')
            return FAILED
        try:
            signature = executor(self._client, intent)
        except Exception as e:
            bt.logging.warning(f'actuator: {intent.key} send failed: {e}')
            return FAILED  # the planner re-plans it next tick if still due
        if not signature:
            return NOOP
        self.confirms.track(signature, intent, now)
        self._in_flight.add(intent.key)
        return SENT

    def _settle(self, now: float) -> None:
        if not self.confirms.pending():
            return
        resolved = self.confirms.resolve(self._client.rpc, now)
        for sent in resolved.landed:
            self._done(sent.intent)
        for sent, err in resolved.failed:
            bt.logging.warning(f'actuator: {sent.intent.key} failed on-chain: {err}')
            self._done(sent.intent)
        for sent in resolved.expired:
            # Never landed. A planned intent is re-planned if still due; a submitted one goes back in.
            if sent.intent.key in self._submitted:
                self.queue.push(sent.intent)
            self._done(sent.intent)

    def _done(self, intent: Intent) -> None:
        self._in_flight.discard(intent.key)
        self._submitted.discard(intent.key)
