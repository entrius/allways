"""Crown-time scoring pipeline.

Reward per miner is ``pool * share * success_rate ** SUCCESS_EXPONENT``;
unclaimed pool recycles to ``RECYCLE_UID``. Entry point is
``score_and_reward_miners(validator)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

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

    # A miner's *current* active flag / collateral is irrelevant to whether
    # they earned crown during the replay window. The only at-scoring-time
    # check is metagraph membership, because a dereg'd miner has no UID to
    # credit. Active, collateral, rate, and busy are all evaluated per-block
    # via event replay inside replay_crown_time_window.
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    credibility_since = max(0, self.block - CREDIBILITY_WINDOW_BLOCKS)
    success_stats = self.state_store.get_success_rates_since(credibility_since)

    for (from_chain, to_chain), pool in DIRECTION_POOLS.items():
        crown_blocks = replay_crown_time_window(
            store=self.state_store,
            event_watcher=self.event_watcher,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            rewardable_hotkeys=rewardable_hotkeys,
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
    rewards[recycle_uid] += max(0.0, 1.0 - distributed)

    bt.logging.info(
        f'V1 scoring: window=[{window_start}, {window_end}], '
        f'distributed={distributed:.6f}, recycled={max(0.0, 1.0 - distributed):.6f}'
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


# ─── Crown-time replay ───────────────────────────────────────────────────


class EventKind(IntEnum):
    """Ordering of coincident-block transitions in the crown-time replay.

    CONFIG applies first because a contract-wide scalar change (halt,
    min/max collateral) can't be scoped to any one miner — the block
    *after* a halt must credit nobody regardless of what else happens
    that block. ACTIVE applies next because the on-chain active flag is
    the per-miner tell-all. Then BUSY (reservation ends crown for that
    miner), then COLLATERAL, then RATE. So if a user reserves a miner
    in the same block that miner's best rate was posted, the reservation
    ends crown credit *before* the rate attribution — matching the intent
    that a busy miner doesn't earn a new interval.
    """

    CONFIG = 0
    ACTIVE = 1
    BUSY = 2
    COLLATERAL = 3
    RATE = 4


@dataclass
class ReplayEvent:
    """One transition in the chronological replay stream. ``value`` is
    polymorphic on ``kind``: rate as float, collateral as rao, busy delta
    of ±1, active as 0/1, or config scalar. For CONFIG events ``hotkey``
    carries the config key name (``min_collateral``, ``max_collateral``,
    ``halted``) — the field is reused as a union slot, not a real hotkey."""

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
) -> Tuple[Dict[str, float], Dict[str, int], Dict[str, int], Set[str], Dict[str, int]]:
    """Snapshot rates, collateral, busy counts, active set, and contract
    config (min_collateral, max_collateral, halted) as they stood at
    window_start."""
    rates: Dict[str, float] = {}
    collateral: Dict[str, int] = {}
    busy_count: Dict[str, int] = dict(event_watcher.get_busy_miners_at(window_start))
    active_set: Set[str] = set(event_watcher.get_active_miners_at(window_start))
    config: Dict[str, int] = {
        'min_collateral': event_watcher.get_config_at('min_collateral', window_start),
        'max_collateral': event_watcher.get_config_at('max_collateral', window_start),
        'halted': event_watcher.get_config_at('halted', window_start),
    }

    for hotkey in rewardable_hotkeys:
        latest_rate = store.get_latest_rate_before(hotkey, from_chain, to_chain, window_start)
        if latest_rate is not None:
            rates[hotkey] = latest_rate[0]

        latest_col = event_watcher.get_latest_collateral_before(hotkey, window_start)
        if latest_col is not None:
            collateral[hotkey] = latest_col[0]
        else:
            # No event before window_start — fall back to the watcher's current
            # snapshot so a miner whose only collateral event predates retention
            # still gets credited accurately.
            snapshot = event_watcher.collateral.get(hotkey)
            if snapshot is not None:
                collateral[hotkey] = snapshot

    return rates, collateral, busy_count, active_set, config


def merge_replay_events(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window config, active, busy, collateral, and rate transitions
    into one chronologically-sorted stream."""
    events: List[ReplayEvent] = []

    for e in event_watcher.get_config_events_in_range(window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['key'], kind=EventKind.CONFIG, value=float(e['value'])))

    for e in event_watcher.get_active_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVE, value=1.0 if e['active'] else 0.0)
        )

    for e in event_watcher.get_busy_events_in_range(window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.BUSY, value=float(e['delta'])))

    for e in event_watcher.get_collateral_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(
                block=e['block'], hotkey=e['hotkey'], kind=EventKind.COLLATERAL, value=float(e['collateral_rao'])
            )
        )

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
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_blocks_float}``.
    Ties at the same rate split credit evenly. A miner qualifies for crown
    at an instant iff the contract is not halted, they are on the current
    metagraph, were active at that instant, not busy, had ``min_collateral
    <= collateral <= max_collateral`` (``max_collateral=0`` disables the
    upper bound, matching contract semantics), and had a positive rate
    posted. Active/collateral/rate/busy/config are all evaluated per-block
    via the replay — a miner's status at scoring time is irrelevant other
    than metagraph membership (used to credit the UID)."""
    rates, collateral, busy_count, active_set, config = reconstruct_window_start_state(
        store, event_watcher, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_watcher, from_chain, to_chain, window_start, window_end)

    crown_blocks: Dict[str, float] = {}
    prev_block = window_start

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        busy_set = {hk for hk, c in busy_count.items() if c > 0}
        holders = crown_holders_at_instant(
            rates,
            collateral,
            config['min_collateral'],
            rewardable_hotkeys,
            busy=busy_set,
            active=active_set,
            max_collateral=config['max_collateral'],
            halted=bool(config['halted']),
        )
        if not holders:
            return
        split = duration / len(holders)
        for hk in holders:
            crown_blocks[hk] = crown_blocks.get(hk, 0.0) + split

    def apply_event(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.COLLATERAL:
            collateral[event.hotkey] = int(event.value)
        elif event.kind is EventKind.BUSY:
            new_count = busy_count.get(event.hotkey, 0) + int(event.value)
            if new_count > 0:
                busy_count[event.hotkey] = new_count
            else:
                busy_count.pop(event.hotkey, None)
        elif event.kind is EventKind.ACTIVE:
            if event.value > 0:
                active_set.add(event.hotkey)
            else:
                active_set.discard(event.hotkey)
        else:  # CONFIG
            # ``hotkey`` carries the config key name for CONFIG events.
            config[event.hotkey] = int(event.value)

    for event in replay_events:
        credit_interval(prev_block, event.block)
        apply_event(event)
        prev_block = event.block

    credit_interval(prev_block, window_end)
    return crown_blocks


def crown_holders_at_instant(
    rates: Dict[str, float],
    collaterals: Dict[str, int],
    min_collateral: int,
    rewardable: Set[str],
    busy: Optional[Set[str]] = None,
    active: Optional[Set[str]] = None,
    max_collateral: int = 0,
    halted: bool = False,
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (not halted, rewardable, active, not busy, ``min <=
    collateral <= max``, rate > 0). If the best rate has no qualified miner,
    fall through to the next-best rate.

    ``max_collateral=0`` disables the upper bound — matches the contract's
    ``post_collateral`` semantics where 0 means "no cap". ``halted=True``
    short-circuits to return []: during a contract halt no miner holds
    crown, the pool recycles via the caller.

    ``active`` defaults to None, which means "no active-state gating" — this
    keeps the helper usable in isolation for tests that don't care about
    the historical active flag. Callers that replay events should pass the
    reconstructed active set explicitly.
    """
    if halted:
        return []
    busy = busy or set()

    def qualifies(hotkey: str) -> bool:
        if active is not None and hotkey not in active:
            return False
        collateral = collaterals.get(hotkey, 0)
        if collateral < min_collateral:
            return False
        if max_collateral > 0 and collateral > max_collateral:
            return False
        return hotkey in rewardable and hotkey not in busy and rates.get(hotkey, 0) > 0

    by_rate: Dict[float, List[str]] = {}
    for hotkey, rate in rates.items():
        if rate > 0:
            by_rate.setdefault(rate, []).append(hotkey)

    for rate in sorted(by_rate, reverse=True):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if winners:
            return winners

    return []
