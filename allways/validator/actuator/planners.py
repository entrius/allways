"""What the actuator plans, and how urgent each kind of action is.

``ACTIONS`` is the single table of every program write the validator makes: its urgency class and which
on-chain field is its deadline. The planners below decide WHETHER an action is due; the existing
decision rules (freshness gates, attest/confirm/refuse checks) move here as pure functions of the World."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import bittensor as bt
from solders.pubkey import Pubkey

from allways.chains import get_chain_def
from allways.constants import FEE_DIVISOR, SECONDS_PER_BLOCK, WEIGHTS_STAKE_BUCKET_ALPHA, WEIGHTS_VOTE_INTERVAL_BLOCKS
from allways.solana import pdas
from allways.solana.client import swap_from_solana
from allways.solana.layouts import hub_busy_until, hub_swap_on
from allways.validator.actuator.swap_context import WorldContext
from allways.validator.actuator.swaps import decide, status_of
from allways.validator.runtime.bus import Latest
from allways.validator.runtime.deadline_queue import Intent, Urgency
from allways.validator.world.snapshot import World

SOFT_DEADLINE_SECS = 600


@dataclass(frozen=True)
class Action:
    urgency: Urgency
    deadline: Callable[[Any, float], float]  # (the account the action is about, now) -> unix deadline
    # Reads the World's event-fed ledgers (used deposits / deliveries), so it waits while the feed catches up.
    needs_ledgers: bool = False
    # After a send that went nowhere (lost race, already voted, rejected), wait this long before resending.
    retry_secs: float = 6.0


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
    'resolve_pool': Action(Urgency.CRANK, _soft, retry_secs=2.0),  # the draw retries until its seed slot exists
    'finalize_reservation': Action(Urgency.CRANK, _field('finalize_by')),
    # Hygiene
    'vote_deactivate': Action(Urgency.SOFT, _soft, retry_secs=300.0),
    'vote_set_weights': Action(Urgency.SOFT, _soft, retry_secs=1800.0),
    # Relay writes on Solana (enqueued by a hub relay, sent by the actuator). A held hub refuses a lower
    # attestation until its swap ends: retried each minute, not each tick.
    'vote_set_attestation': Action(Urgency.VOTE, _soft, retry_secs=60.0),
    'vote_attest_heartbeat': Action(Urgency.SOFT, _soft, retry_secs=60.0),
}


def intent(
    kind: str, subject_key: Any, account: Any, now: float, payload: Any = None, not_before: float = 0.0
) -> Intent:
    action = ACTIONS[kind]
    return Intent((kind, subject_key), action.urgency, action.deadline(account, now), payload, not_before)


@dataclass(frozen=True)
class SwapIntent:
    swap: Any  # the flat swap view the decision was made on
    action: Any  # swaps.Action


@dataclass(frozen=True)
class SwapReport:
    """What the validator currently thinks of one live swap, for the seam's /status and the logs."""

    status: str
    decision: str
    reason: Optional[str]
    legs: Dict[str, Tuple[int, int]]  # chain → (confirmations seen, required)
    from_chain: str = ''  # which of the legs is the source
    checked_at: float = 0.0


# Decision → (intent kind, which account carries its deadline)
_SENDS = {
    'attest': ('vote_initiate', 'reservation'),
    'extend_reservation': ('extend_reservation', 'reservation'),
    'confirm': ('confirm_swap', 'swap'),
    'extend_timeout': ('extend_timeout', 'swap'),
    'timeout': ('timeout_swap', 'swap'),
    'cancel': ('close_stale_claim', 'swap'),
    'refuse': ('cancel_swap', 'swap'),
}


class SwapPlanner:
    """Every live swap in the World → its decision → an intent when there is something to send. Logs a
    swap's decision only when it changes, and publishes the whole picture as ``report``."""

    name = 'swaps'

    def __init__(
        self,
        watchers: Dict[str, Any],
        providers: Dict[str, Any],
        memory: Any,
        owes_settlement: Optional[Callable[[Any], bool]] = None,
        program_id: Any = None,
        fee_divisor: int = FEE_DIVISOR,
    ) -> None:
        self.watchers = watchers  # asset chain id → the ChainWatcher for its chain
        self.providers = providers
        self.memory = memory
        self.owes_settlement = owes_settlement
        self.program_id = program_id
        self.fee_divisor = fee_divisor
        self.report: Latest = Latest()
        self._said: Dict[str, Tuple[str, Optional[str]]] = {}

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        out: List[Intent] = []
        report: Dict[str, SwapReport] = {}
        for _, acct in world.all('Swap'):
            swap = swap_from_solana(acct)
            key = swap.swap_key.hex()
            ctx = WorldContext(
                world, self.watchers, self.providers, self.memory, int(now), self.owes_settlement, self.program_id
            )
            action = decide(swap, ctx, int(now), self.fee_divisor, complete=world.complete)
            self._log(key, swap, action)
            legs = {c: _conf_pair(c, leg.info) for c, leg in ctx.legs.items()}
            report[key] = SwapReport(
                status_of(swap), action.decision.value, action.reason, legs, str(swap.from_chain), float(now)
            )
            send = _SENDS.get(action.decision.value)
            if send is not None:
                kind, deadline_from = send
                account = ctx.reservation(swap) if deadline_from == 'reservation' else swap
                out.append(intent(kind, key, account or swap, now, payload=SwapIntent(swap, action)))
        for gone in set(self._said) - set(report):
            del self._said[gone]
            delivered = world.fulfillments.get(gone)
            outcome = (
                'completed' if delivered is not None and delivered.settled else 'closed without a confirmed delivery'
            )
            bt.logging.info(f'swap {gone[:16]}: {outcome}')
        self.memory.prune(int(now))
        self.report.set(report)
        return out

    def _log(self, key: str, swap: Any, action: Any) -> None:
        said = (action.decision.value, _COUNTDOWN.sub('_in', action.reason or ''))  # a ticking clock isn't news
        if self._said.get(key) != said:
            self._said[key] = said
            reason = f' — {action.reason}' if action.reason else ''
            # A skip is a probe still in flight (seconds): detail, not progress.
            log = bt.logging.debug if action.decision.value == 'skip' else bt.logging.info
            log(f'swap {key[:16]} [{status_of(swap)} {swap.from_chain}->{swap.to_chain}]: {said[0]}{reason}')


_COUNTDOWN = re.compile(r'_in=-?\d+s')


def _conf_pair(chain: str, info: Any) -> Tuple[int, int]:
    return int(getattr(info, 'confirmations', 0) or 0), int(get_chain_def(chain).min_confirmations)


class PoolPlanner:
    """Closed pools → ``resolve_pool``. Permissionless and first-wins, so every validator cranks every closed,
    non-empty pool; a peer that got there first makes ours a benign no-op. Finalizing a seat this validator
    won for a routed user needs the routed-request queue, which serving fills, so it lands with serving."""

    name = 'pools'

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        out = []
        for pda, pool in world.all('Pool'):
            if int(getattr(pool, 'opened_at', 0)) == 0 or now <= int(pool.closes_at) or not pool.requests:
                continue
            out.append(intent('resolve_pool', pda, pool, now, payload=pool))
        return out


def derive_weight_vector(validators: Iterable[Any], attribution: Dict[str, str], metagraph: Any) -> List[int]:
    """Draw weight per whitelisted validator, aligned to ``Config.validators``: max(1, alpha_stake // bucket);
    unbound or off the metagraph → 0. The floor of 1 keeps a bound validator ahead of native (weight-0)
    bidders: all-zero weights make resolve_pool's draw uniform and break routed win odds."""
    uid_of = {hk: uid for uid, hk in enumerate(metagraph.hotkeys)}
    weights = []
    for v in validators:
        uid = uid_of.get(attribution.get(str(Pubkey.from_bytes(bytes(v.key)))))
        alpha = float(metagraph.alpha_stake[uid]) if uid is not None else 0.0
        weights.append(max(1, int(alpha // WEIGHTS_STAKE_BUCKET_ALPHA)) if uid is not None else 0)
    return weights


class HygienePlanner:
    """Two slow duties off the World and the subtensor head.

    Floor sweep: a miner still active on a hub whose purse sits under that hub's floor (a raised floor never
    touches MinerState, and the contract's own auto-deactivation can't reach an unreservable miner) is voted
    out on that hub, once the hub is idle. Stake-weight vote: once per boundary, the lottery's draw weights
    are voted to match metagraph stake."""

    name = 'hygiene'

    def __init__(self, head: Latest, me: Any = None) -> None:
        self.head = head  # Latest[subtensor_view.Head]
        self.me = me  # this validator's Solana pubkey

    def plan(self, world: World, now: float) -> Iterable[Intent]:
        if world.config is None:
            return []
        return [*self._floor_sweep(world, now), *self._weights(world, now)]

    def _floor_sweep(self, world: World, now: float) -> List[Intent]:
        floors = {'sol': int(world.config.min_collateral), 'tao': int(world.config.tao_min_collateral)}
        out = []
        for _, ms in world.all('MinerState'):
            if not ms.active:
                continue
            miner = str(Pubkey.from_bytes(bytes(ms.miner)))
            for backing, bit in pdas.BACKING_BITS.items():
                floor = floors.get(backing, 0)
                if not floor or not int(ms.active_backings or 0) & bit:
                    continue
                purse = _purse(world, ms, miner, backing)
                if purse is None or purse >= floor:
                    continue
                if hub_swap_on(ms, bit) or now < hub_busy_until(ms, bit):
                    continue  # the contract refuses to kick a busy hub; it is re-planned once idle
                out.append(intent('vote_deactivate', (miner, backing), ms, now, payload=(miner, backing)))
                break  # one hub at a time, as the contract judges them
        return out

    def _weights(self, world: World, now: float) -> List[Intent]:
        head, _ = self.head.get()
        if head is None or self.me is None:
            return []
        config = world.config
        block = head.block_at(now, SECONDS_PER_BLOCK)
        boundary = now - (block % WEIGHTS_VOTE_INTERVAL_BLOCKS) * SECONDS_PER_BLOCK
        if int(config.last_weights_update) >= boundary:
            return []  # already landed this epoch
        keys = [bytes(v.key) for v in config.validators]
        if bytes(self.me) not in keys:
            return []
        vector = derive_weight_vector(config.validators, world.attribution, head.metagraph)
        if vector == [int(v.weight) for v in config.validators]:
            return []
        epoch = block // WEIGHTS_VOTE_INTERVAL_BLOCKS
        return [intent('vote_set_weights', epoch, config, now, payload=(vector, keys))]


def _purse(world: World, ms: Any, miner: str, backing: str) -> Optional[int]:
    """A hub's own bond: the local vault for SOL, the attested balance for a vault hub. None skips: an
    unreadable or unlocked attestation never forces a kick."""
    if backing == 'sol':
        return int(ms.collateral)
    att = world.get('BondAttestation', pdas.bond_attestation_pda(miner, backing))
    if att is None or not getattr(att, 'locked', False):
        return None
    return int(att.effective_balance)
