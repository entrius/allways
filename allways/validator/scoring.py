"""Crown-time scoring pipeline.

Reward per miner is ``pool * share * success_rate ** SUCCESS_EXPONENT``;
unclaimed pool recycles to ``RECYCLE_UID``. Entry point is
``score_and_reward_miners(validator)``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chains import canonical_pair
from allways.constants import (
    CREDIBILITY_WINDOW_BLOCKS,
    DIRECTION_POOLS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
    SUCCESS_EXPONENT,
)
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.state_store import ValidatorStateStore

if TYPE_CHECKING:
    from neurons.validator import Validator


@dataclass
class DirectionTrace:
    pool: float = 0.0
    crown_blocks: Dict[str, float] = field(default_factory=dict)
    unfilled_blocks: int = 0
    best_rate: float = 0.0


def score_and_reward_miners(self: Validator) -> None:
    try:
        if _contract_is_halted(self):
            rewards, miner_uids = build_halted_rewards(self)
        else:
            rewards, miner_uids = calculate_miner_rewards(self)
        self.update_scores(rewards, miner_uids)
        prune_rate_events(self)
        prune_swap_outcomes(self)
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


def _contract_is_halted(self: Validator) -> bool:
    """Best-effort halt check. RPC flakiness should not zero every miner's
    reward, so any exception falls through to normal scoring."""
    try:
        return bool(self.contract_client.get_halted())
    except Exception as e:
        bt.logging.warning(f'halt RPC check failed, proceeding as not-halted: {e}')
        return False


def build_halted_rewards(self: Validator) -> Tuple[np.ndarray, Set[int]]:
    """During a halt, no miner earns crown; the full pool recycles."""
    n_uids = self.metagraph.n.item()
    rewards = np.zeros(n_uids, dtype=np.float32)
    if n_uids == 0:
        return rewards, set()
    recycle_uid = RECYCLE_UID if RECYCLE_UID < n_uids else 0
    rewards[recycle_uid] = 1.0
    bt.logging.info('V1 scoring: halted, recycled full pool')
    return rewards, set(range(n_uids))


def prune_rate_events(self: Validator) -> None:
    cutoff = self.block - SCORING_WINDOW_BLOCKS
    if cutoff > 0:
        self.state_store.prune_events_older_than(cutoff)


def prune_swap_outcomes(self: Validator) -> None:
    cutoff = self.block - CREDIBILITY_WINDOW_BLOCKS
    if cutoff > 0:
        self.state_store.prune_swap_outcomes_older_than(cutoff)


def calculate_miner_rewards(self: Validator) -> Tuple[np.ndarray, Set[int]]:
    """Replay the crown-time event stream over the window, derive per-miner
    rewards, recycle any undistributed pool to ``RECYCLE_UID``."""
    n_uids = self.metagraph.n.item()
    if n_uids == 0:
        return np.array([], dtype=np.float32), set()

    window_end = self.block
    window_start = max(0, window_end - SCORING_WINDOW_BLOCKS)

    # A miner's *current* active flag is irrelevant to whether they earned
    # crown during the replay window. The only at-scoring-time check is
    # metagraph membership, because a dereg'd miner has no UID to credit.
    # Active, rate, and busy are all evaluated per-block via event replay
    # inside replay_crown_time_window. Collateral-floor invariants are
    # trusted to the contract's active flag.
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    credibility_since = max(0, self.block - CREDIBILITY_WINDOW_BLOCKS)
    success_stats = self.state_store.get_success_rates_since(credibility_since)

    direction_traces: Dict[Tuple[str, str], DirectionTrace] = {}

    for (from_chain, to_chain), pool in DIRECTION_POOLS.items():
        trace = DirectionTrace(pool=pool)
        direction_traces[(from_chain, to_chain)] = trace
        crown_blocks = replay_crown_time_window(
            store=self.state_store,
            event_watcher=self.event_watcher,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            rewardable_hotkeys=rewardable_hotkeys,
            trace=trace,
        )
        total = sum(crown_blocks.values())
        if total == 0:
            continue  # empty bucket — pool recycles via the remainder below

        for hotkey, blocks in crown_blocks.items():
            uid = hotkey_to_uid.get(hotkey)
            if uid is None:
                continue  # dereg'd mid-window; credit forfeited
            share = blocks / total
            sr = success_rate(success_stats.get(hotkey))
            rewards[uid] += pool * share * (sr**SUCCESS_EXPONENT)

    recycle_uid = RECYCLE_UID if RECYCLE_UID < n_uids else 0
    distributed = float(rewards.sum())
    recycled = max(0.0, 1.0 - distributed)
    rewards[recycle_uid] += recycled

    log_scoring_trace(
        self,
        window_start=window_start,
        window_end=window_end,
        hotkey_to_uid=hotkey_to_uid,
        rewardable_hotkeys=rewardable_hotkeys,
        direction_traces=direction_traces,
        rewards=rewards,
        success_stats=success_stats,
        distributed=distributed,
        recycled=recycled,
        recycle_uid=recycle_uid,
    )

    return rewards, set(range(n_uids))


def success_rate(stats: Optional[Tuple[int, int]]) -> float:
    """All-time success rate. Zero-outcome miners default to 1.0 (optimistic)."""
    if stats is None:
        return 1.0
    completed, timed_out = stats
    total = completed + timed_out
    if total == 0:
        return 1.0
    return completed / total


def log_scoring_trace(
    self: Validator,
    *,
    window_start: int,
    window_end: int,
    hotkey_to_uid: Dict[str, int],
    rewardable_hotkeys: Set[str],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    rewards: np.ndarray,
    success_stats: Dict[str, Tuple[int, int]],
    distributed: float,
    recycled: float,
    recycle_uid: int,
) -> None:
    lines = [
        f'V1 scoring: window=[{window_start}, {window_end}], distributed={distributed:.6f}, recycled={recycled:.6f}'
    ]

    for (from_c, to_c), trace in direction_traces.items():
        holders = ', '.join(
            f'UID{hotkey_to_uid[hk]}: {blk:.0f} blk'
            for hk, blk in sorted(trace.crown_blocks.items(), key=lambda kv: -kv[1])
            if hk in hotkey_to_uid
        )
        lines.append(
            f'  [{from_c}→{to_c}] pool={trace.pool:g} holders={{{holders}}} unfilled={trace.unfilled_blocks} blk'
        )

    for uid in sorted((u for u in range(len(rewards)) if rewards[u] > 0), key=lambda u: -float(rewards[u])):
        hk = self.metagraph.hotkeys[uid]
        crown_blk = sum(t.crown_blocks.get(hk, 0.0) for t in direction_traces.values())
        if uid == recycle_uid and crown_blk == 0:
            continue
        own_reward = float(rewards[uid]) - (recycled if uid == recycle_uid else 0.0)
        sr = success_rate(success_stats.get(hk))
        lines.append(f'  uid={uid} hotkey={hk[:8]}.. crown_blk={crown_blk:.0f} sr={sr:.3f} reward={own_reward:.3f}')

    ever_active = set(self.event_watcher.get_active_miners_at(window_start))
    for e in self.event_watcher.get_active_events_in_range(window_start, window_end):
        if e['active']:
            ever_active.add(e['hotkey'])

    rates_by_hotkey: Dict[str, Dict[Tuple[str, str], float]] = {}
    for (hk, from_c, to_c), r in (getattr(self, 'last_known_rates', {}) or {}).items():
        if r > 0:
            rates_by_hotkey.setdefault(hk, {})[(from_c, to_c)] = r

    emitted = 0
    for hk in rewardable_hotkeys:
        uid = hotkey_to_uid.get(hk)
        if uid is None or uid == recycle_uid or rewards[uid] > 0:
            continue
        latest_rates = rates_by_hotkey.get(hk, {})
        if not latest_rates and hk not in ever_active:
            continue
        sr = success_rate(success_stats.get(hk))
        reason = diagnose_non_earner(hk, latest_rates, sr, ever_active, direction_traces)
        lines.append(f'  uid={uid} hotkey={hk[:8]}.. crown_blk=0 reason="{reason}" sr={sr:.3f}')
        emitted += 1
        if emitted >= 30:
            break

    if recycled > 0:
        parts = [
            f'{t.unfilled_blocks} unfilled blk in {f}→{to}'
            for (f, to), t in direction_traces.items()
            if t.unfilled_blocks > 0
        ]
        cause = '; '.join(parts) or 'no crown winners'
        lines.append(f'  recycled={recycled:.3f} → UID{recycle_uid} (subnet owner) cause={cause}')

    bt.logging.info('\n'.join(lines))


def diagnose_non_earner(
    hotkey: str,
    latest_rates: Dict[Tuple[str, str], float],
    sr: float,
    ever_active: Set[str],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
) -> str:
    if not latest_rates:
        return 'no_rate_posted'
    if hotkey not in ever_active:
        return 'not_active_during_window'
    if sr <= 0:
        return 'slashed_credibility_zero'
    parts = [
        f'{direction[0]}→{direction[1]}: own={own:g} vs best={direction_traces[direction].best_rate:g}'
        for direction, own in latest_rates.items()
        if direction in direction_traces and direction_traces[direction].best_rate > 0
    ]
    return 'outbid (' + '; '.join(parts) + ')' if parts else 'no_competing_winner'


# ─── Crown-time replay ───────────────────────────────────────────────────


class EventKind(IntEnum):
    """Ordering of coincident-block transitions in the crown-time replay.

    ACTIVE applies first because the on-chain active flag is the per-miner
    tell-all. Then BUSY (reservation ends crown for that miner), then
    RATE. So if a user reserves a miner in the same block that miner's
    best rate was posted, the reservation ends crown credit *before* the
    rate attribution — matching the intent that a busy miner doesn't earn
    a new interval.
    """

    ACTIVE = 0
    BUSY = 1
    RATE = 2


@dataclass
class ReplayEvent:
    """One transition in the chronological replay stream. ``value`` is
    polymorphic on ``kind``: rate as float, busy delta of ±1, or active as
    0/1."""

    block: int
    hotkey: str
    kind: EventKind
    value: float

    @property
    def sort_key(self) -> Tuple[int, int]:
        return (self.block, int(self.kind))


def reconstruct_window_start_state(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    rewardable_hotkeys: Set[str],
) -> Tuple[Dict[str, float], Dict[str, int], Set[str]]:
    """Snapshot rates, busy counts, and the active set as they stood at
    window_start."""
    rates: Dict[str, float] = {}
    busy_count: Dict[str, int] = dict(event_watcher.get_busy_miners_at(window_start))
    active_set: Set[str] = set(event_watcher.get_active_miners_at(window_start))

    for hotkey in rewardable_hotkeys:
        latest_rate = store.get_latest_rate_before(hotkey, from_chain, to_chain, window_start)
        if latest_rate is not None:
            rates[hotkey] = latest_rate[0]

    return rates, busy_count, active_set


def merge_replay_events(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window active, busy, and rate transitions into one
    chronologically-sorted stream."""
    events: List[ReplayEvent] = []

    for e in event_watcher.get_active_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVE, value=1.0 if e['active'] else 0.0)
        )

    for e in event_watcher.get_busy_events_in_range(window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.BUSY, value=float(e['delta'])))

    for e in store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.RATE, value=float(e['rate'])))

    events.sort(key=lambda ev: ev.sort_key)
    return events


def replay_crown_time_window(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
    rewardable_hotkeys: Set[str],
    trace: Optional[DirectionTrace] = None,
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_blocks_float}``.
    Ties at the same rate split credit evenly. A miner qualifies for crown
    at an instant iff they are on the current metagraph, were active at
    that instant, not busy, and had a positive rate posted. Active/rate/busy
    are evaluated per-block via the replay — a miner's status at scoring
    time is irrelevant other than metagraph membership (used to credit the
    UID). Collateral-floor invariants are trusted to the contract's active
    flag; halt state is handled at ``score_and_reward_miners`` entry."""
    rates, busy_count, active_set = reconstruct_window_start_state(
        store, event_watcher, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_watcher, from_chain, to_chain, window_start, window_end)

    # Rates are stored as canonical_dest per canonical_source (TAO per BTC).
    # In the canonical direction (btc→tao) higher = better; in the reverse
    # direction (tao→btc) lower = better.
    canon_from, _ = canonical_pair(from_chain, to_chain)
    lower_rate_wins = from_chain != canon_from

    crown_blocks: Dict[str, float] = {}
    prev_block = window_start

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        busy_set = {hk for hk, c in busy_count.items() if c > 0}
        holders = crown_holders_at_instant(
            rates,
            rewardable_hotkeys,
            busy=busy_set,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_blocks += duration
            return
        winner_rate = rates.get(holders[0], 0.0)
        if trace is not None and winner_rate > 0:
            trace.best_rate = winner_rate
        split = duration / len(holders)
        for hk in holders:
            crown_blocks[hk] = crown_blocks.get(hk, 0.0) + split

    def apply_event(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.BUSY:
            new_count = busy_count.get(event.hotkey, 0) + int(event.value)
            if new_count > 0:
                busy_count[event.hotkey] = new_count
            else:
                busy_count.pop(event.hotkey, None)
        else:  # ACTIVE
            if event.value > 0:
                active_set.add(event.hotkey)
            else:
                active_set.discard(event.hotkey)

    for event in replay_events:
        credit_interval(prev_block, event.block)
        apply_event(event)
        prev_block = event.block

    credit_interval(prev_block, window_end)
    if trace is not None:
        trace.crown_blocks = dict(crown_blocks)
    return crown_blocks


def crown_holders_at_instant(
    rates: Dict[str, float],
    rewardable: Set[str],
    busy: Optional[Set[str]] = None,
    active: Optional[Set[str]] = None,
    lower_rate_wins: bool = False,
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (rewardable, active, not busy, rate > 0). If the best
    rate has no qualified miner, fall through to the next-best rate.

    ``lower_rate_wins`` flips the sort: rates are stored as canonical_dest
    per canonical_source (TAO per BTC), so higher-is-better only holds in
    the canonical direction (btc→tao). In the reverse direction (tao→btc)
    a smaller TAO/BTC quote means the miner is asking less TAO for 1 BTC —
    a better deal for the swapper, which earns them the crown.

    Collateral-floor gating is trusted to the contract's active flag —
    miners who drop below the floor get auto-deactivated on-chain (fee /
    slash paths) or kicked via ``vote_deactivate`` in the min-raise edge
    case. Halt state is handled at ``score_and_reward_miners`` entry, not
    in this helper.

    ``active`` defaults to None for tests that don't care about the
    historical active flag; replay callers pass the reconstructed set."""
    busy = busy or set()

    def qualifies(hotkey: str) -> bool:
        if active is not None and hotkey not in active:
            return False
        return hotkey in rewardable and hotkey not in busy and rates.get(hotkey, 0) > 0

    by_rate: Dict[float, List[str]] = {}
    for hotkey, rate in rates.items():
        if rate > 0:
            by_rate.setdefault(rate, []).append(hotkey)

    for rate in sorted(by_rate, reverse=not lower_rate_wins):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if winners:
            return winners

    return []
