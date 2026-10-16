"""What the actuator plans, and how urgent each kind of action is.

``ACTIONS`` is the single table of every program write the validator makes: its urgency class and which
on-chain field is its deadline. The planners below decide WHETHER an action is due; the existing
decision rules (freshness gates, attest/confirm/refuse checks) move here as pure functions of the World."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, List

from allways.validator_v2.runtime.deadline_queue import Intent, Urgency
from allways.validator_v2.world.snapshot import World

SOFT_DEADLINE_SECS = 600


@dataclass(frozen=True)
class Action:
    urgency: Urgency
    deadline: Callable[[Any, float], float]  # (the account the action is about, now) -> unix deadline
    # Reads the World's event-fed ledgers (used deposits / deliveries), so it waits while the feed catches up.
    needs_ledgers: bool = False


def _field(name: str) -> Callable[[Any, float], float]:
    return lambda account, now: float(getattr(account, name))


def _soft(account: Any, now: float) -> float:
    return now + SOFT_DEADLINE_SECS


ACTIONS = {
    # Swap lifecycle (about a Reservation or a Swap)
    'extend_reservation': Action(Urgency.EXTEND, _field('reserved_until')),
    'extend_timeout': Action(Urgency.EXTEND, _field('timeout_at')),
    'vote_initiate': Action(Urgency.VOTE, _field('reserved_until'), needs_ledgers=True),
    'confirm_swap': Action(Urgency.VOTE, _field('timeout_at'), needs_ledgers=True),
    'timeout_swap': Action(Urgency.SOFT, _soft),
    'cancel_swap': Action(Urgency.SOFT, _soft),
    'close_stale_claim': Action(Urgency.SOFT, _soft),
    # Pools (about a Pool or a Reservation)
    'resolve_pool': Action(Urgency.CRANK, _soft),
    'finalize_reservation': Action(Urgency.CRANK, _field('finalize_by')),
    # Hygiene
    'vote_deactivate': Action(Urgency.SOFT, _soft),
    'vote_set_weights': Action(Urgency.SOFT, _soft),
    # Relay writes on Solana (enqueued by a hub relay, sent by the actuator)
    'vote_set_attestation': Action(Urgency.VOTE, _soft),
    'vote_attest_heartbeat': Action(Urgency.SOFT, _soft),
}


def intent(
    kind: str, subject_key: Any, account: Any, now: float, payload: Any = None, not_before: float = 0.0
) -> Intent:
    action = ACTIONS[kind]
    return Intent((kind, subject_key), action.urgency, action.deadline(account, now), payload, not_before)


class SwapPlanner:
    """Live swaps and their reservations → lifecycle intents.

    To lift from ``allways.validator.solana_swap_loop``: the per-status decisions (``_decide_*``), with
    every chain read replaced by ``LegVerifier.verdict`` and every account read by the World."""

    name = 'swaps'

    def __init__(self, legs) -> None:
        self.legs = legs

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        out: List[Intent] = []
        return out


class PoolPlanner:
    """Closed pools and drawn reservations → ``resolve_pool`` / ``finalize_reservation``.

    To lift from ``allways.validator.reserve_engine.crank``. The WebSocket-driven scheduler becomes a
    nudge that wakes the actuator early; it no longer runs its own copy of the crank."""

    name = 'pools'

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        return []


class HygienePlanner:
    """Floor sweep (``vote_deactivate``) and the stake-weight vote, from Config + MinerState + metagraph.

    To lift from ``allways.validator.floor_sweep`` and ``allways.validator.weights_vote``."""

    name = 'hygiene'

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        return []
