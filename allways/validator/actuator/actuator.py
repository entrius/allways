"""The actuator: the only component that sends program transactions.

Every tick it asks each planner what is due given the newest World, queues the answers by deadline,
sends the most urgent ones without waiting for them to land, and settles earlier sends in one batched
status read. Nothing else in the validator can delay it: it reads the World, never another component."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Iterable, Optional, Protocol, Set, Tuple

import bittensor as bt

from allways.constants import LOW_BALANCE_REMIND_SECS, VOTER_BALANCE_CHECK_SECS, VOTER_LOW_BALANCE_LAMPORTS
from allways.validator.actuator.executors import StaleWorld
from allways.validator.actuator.planners import ACTIONS
from allways.validator.chain.confirm import ConfirmTracker
from allways.validator.runtime.bus import Latest
from allways.validator.runtime.component import Component
from allways.validator.runtime.deadline_queue import DeadlineQueue, Intent
from allways.validator.runtime.logs import Episodes, LowBalance
from allways.validator.world.snapshot import World

# A World older than this is too stale to decide deadline-bound actions on; the actuator waits instead.
MAX_WORLD_AGE_SECS = 30.0
SENDS_PER_TICK = 16
SENT, NOOP, FAILED = 'sent', 'noop', 'failed'
# A landed send is not re-planned for this long: the World needs a tick or two to show what it did, and a
# vote whose quorum closed its vote account can't be told apart from one never cast.
LANDED_HOLD_SECS = 30.0


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
        refresh: Callable[[Set[Tuple[str, str]]], None] = lambda keys: None,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world = world
        self.refresh = refresh  # asks the World to re-read accounts a send found stale
        self.planners = list(planners)
        self.executors = executors
        self._client_factory = client_factory
        self._client: Any = None
        # `is None`, not `or`: an empty queue is falsy, and `or` would swap the shared inbox for a private one.
        self.queue = inbox if inbox is not None else DeadlineQueue()  # writes submitted by serving and the relays
        self.confirms = ConfirmTracker()
        self._in_flight: Set[Tuple[Any, ...]] = set()
        self._submitted: Set[Tuple[Any, ...]] = set()  # in-flight keys that came from the inbox
        self._resting: Dict[Tuple[Any, ...], float] = {}  # key → when a send that went nowhere may be retried
        self._episodes = Episodes()
        self._caught_up: Optional[bool] = None  # last logged readiness: attest/confirm wait while the ledgers catch up
        self._low_balance = LowBalance(
            'actuator: Solana voting key',
            'SOL',
            1e9,
            VOTER_LOW_BALANCE_LAMPORTS,
            VOTER_BALANCE_CHECK_SECS,
            LOW_BALANCE_REMIND_SECS,
        )

    def setup(self) -> None:
        self._client = self._client_factory()

    def tick(self, now: float) -> None:
        self._settle(now)
        world, _ = self.world.get()
        if world is None:
            self._episodes.start('world', 'actuator: waiting for the first World, holding sends', bt.logging.info)
            return
        if now - world.taken_at > MAX_WORLD_AGE_SECS:
            self._episodes.start('world', f'actuator: the World is {now - world.taken_at:.0f}s old, holding sends')
            return
        self._episodes.end('world', 'actuator: World current, sending')
        self._log_readiness(world)
        self._low_balance.maybe_check(now, self._balance)
        planned: Dict[Tuple[Any, ...], Intent] = {}
        for planner in self.planners:
            try:
                for intent in planner.plan(world, now):
                    planned[intent.key] = intent
                self._episodes.end(('planner', planner.name), f'actuator: planner {planner.name} recovered')
            except Exception as e:
                self._episodes.start(('planner', planner.name), f'actuator: planner {planner.name} failed: {e}')
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
            # Submitted work exists only in the inbox: never drop it while it rests, or while an earlier
            # write with its key is in flight (a newer payload goes out once that one resolves).
            if key in self._resting or key in self._in_flight:
                self.queue.requeue(held)
        due.sort(key=lambda i: (int(i.urgency), i.deadline))
        sent = 0
        for intent in due:
            action = ACTIONS.get(intent.key[0])
            if sent >= SENDS_PER_TICK or (action is not None and action.needs_ledgers and not world.complete):
                if intent.key in submitted:
                    self.queue.requeue(intent)
                continue
            if intent.deadline < now:
                self._episodes.start(
                    ('late', intent.key), f'actuator: {label(intent)} is past its deadline, sending anyway'
                )
            outcome = self._send(intent, now)
            if outcome == SENT:
                if intent.key in submitted:
                    self._submitted.add(intent.key)
            else:
                # Went nowhere: rest it, so it isn't re-sent every tick. A submitted intent that was a
                # no-op (already voted, lost a race) is done; one that failed goes back in the inbox.
                self._resting[intent.key] = now + (action.retry_secs if action is not None else 6.0)
                if intent.key in submitted and outcome == FAILED:
                    self.queue.requeue(intent)
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
        except StaleWorld as e:
            bt.logging.warning(f'actuator: {label(intent)} not sent, {e}')
            self.refresh(e.keys)
            return NOOP
        except Exception as e:
            bt.logging.warning(f'actuator: {label(intent)} send failed: {e}')
            if _out_of_funds(e):
                self._low_balance.recheck()
            return FAILED  # the planner re-plans it next tick if still due
        if not signature:
            return NOOP
        self.confirms.track(signature, intent, now)
        self._in_flight.add(intent.key)
        return SENT

    def _log_readiness(self, world: World) -> None:
        """One line each way: the ledger-reading votes (attest, confirm) wait while the feed catches up on
        history, so an operator can tell a validator that is still catching up from one ready to vote."""
        if world.complete == self._caught_up:
            return
        self._caught_up = world.complete
        if world.complete:
            bt.logging.info('actuator: caught up on history — attest and confirm votes enabled')
        else:
            bt.logging.info('actuator: catching up on history — attest and confirm votes held until it completes')

    def _balance(self) -> Optional[int]:
        client = self._client
        return None if client is None else int(client.rpc.get_balance(client.keypair.pubkey()))

    def _settle(self, now: float) -> None:
        if not self.confirms.pending():
            return
        resolved = self.confirms.resolve(self._client.rpc, now)
        for sent in resolved.landed:
            bt.logging.success(f'actuator: {label(sent.intent)} landed ({sent.signature[:12]}…)')
            self._resting[sent.intent.key] = now + LANDED_HOLD_SECS
            self._done(sent.intent)
        for sent, err in resolved.failed:
            bt.logging.warning(f'actuator: {label(sent.intent)} failed on-chain: {err}')
            if _out_of_funds(err):
                self._low_balance.recheck()
            # A planned intent is re-planned if still due; a submitted one rests, then goes back in.
            if sent.intent.key in self._submitted:
                action = ACTIONS.get(sent.intent.key[0])
                self._resting[sent.intent.key] = now + (action.retry_secs if action is not None else 6.0)
                self.queue.requeue(sent.intent)
            self._done(sent.intent)
        for sent in resolved.expired:
            bt.logging.warning(f'actuator: {label(sent.intent)} never landed, sending again')
            # Never landed. A planned intent is re-planned if still due; a submitted one goes back in.
            if sent.intent.key in self._submitted:
                self.queue.requeue(sent.intent)
            self._done(sent.intent)

    def _done(self, intent: Intent) -> None:
        self._episodes.end(('late', intent.key))
        self._in_flight.discard(intent.key)
        self._submitted.discard(intent.key)


def label(intent: Intent) -> str:
    """``kind subject`` with long keys cut to the 16 characters the swap log uses: ``confirm_swap 9760359fcf51f12f``."""
    kind, *rest = intent.key
    subject = rest[0] if rest else ''
    if isinstance(subject, (tuple, list)):
        subject = '/'.join(str(part)[:16] for part in subject)
    return f'{kind} {str(subject)[:16]}'.rstrip()


def _out_of_funds(err: Any) -> bool:
    """A send or landed tx refused for want of lamports (not a program error such as InsufficientCollateral)."""
    text = str(err).lower()
    return any(m in text for m in ('insufficient funds', 'insufficient lamports', 'no record of a prior credit'))
