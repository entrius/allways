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
        rewards, miner_uids = calculate_miner_rewards(self)
        self.update_scores(rewards, miner_uids)
        prune_rate_events(self)
        prune_swap_outcomes(self)
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


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

    in_metagraph: Set[str] = set(self.metagraph.hotkeys)
    eligible_hotkeys: Set[str] = in_metagraph & self.event_watcher.active_miners
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    credibility_since = max(0, self.block - CREDIBILITY_WINDOW_BLOCKS)
    success_stats = self.state_store.get_success_rates_since(credibility_since)
    min_collateral = int(self.event_watcher.min_collateral or 0)

    for (from_chain, to_chain), pool in DIRECTION_POOLS.items():
        crown_blocks = replay_crown_time_window(
            store=self.state_store,
            event_watcher=self.event_watcher,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            eligible_hotkeys=eligible_hotkeys,
            min_collateral=min_collateral,
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

    At the same block number, busy transitions apply before collateral
    changes, which apply before rate changes. So if a user reserves a miner
    in the same block that miner's best rate was posted, the reservation
    ends crown credit *before* the rate attribution — matching the intent
    that a busy miner doesn't earn a new interval.
    """

    BUSY = 0
    COLLATERAL = 1
    RATE = 2


@dataclass
class ReplayEvent:
    """One transition in the chronological replay stream. ``value`` is
    polymorphic on ``kind``: rate as float, collateral as rao, or busy delta
    of ±1."""

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
    eligible_hotkeys: Set[str],
) -> Tuple[Dict[str, float], Dict[str, int], Dict[str, int]]:
    """Snapshot rates, collateral, and busy counts as they stood at window_start."""
    rates: Dict[str, float] = {}
    collateral: Dict[str, int] = {}
    busy_count: Dict[str, int] = dict(event_watcher.get_busy_miners_at(window_start))

    for hotkey in eligible_hotkeys:
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

    return rates, collateral, busy_count


def merge_replay_events(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window rate, collateral, and busy transitions into one
    chronologically-sorted stream."""
    events: List[ReplayEvent] = []

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
    eligible_hotkeys: Set[str],
    min_collateral: int,
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_blocks_float}``.
    Ties at the same rate split credit evenly; busy miners are excluded so
    credit flows to the next-best idle miner."""
    rates, collateral, busy_count = reconstruct_window_start_state(
        store, event_watcher, from_chain, to_chain, window_start, eligible_hotkeys
    )
    replay_events = merge_replay_events(store, event_watcher, from_chain, to_chain, window_start, window_end)

    crown_blocks: Dict[str, float] = {}
    prev_block = window_start

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        busy_set = {hk for hk, c in busy_count.items() if c > 0}
        holders = crown_holders_at_instant(rates, collateral, min_collateral, eligible_hotkeys, busy=busy_set)
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
        else:  # BUSY
            new_count = busy_count.get(event.hotkey, 0) + int(event.value)
            if new_count > 0:
                busy_count[event.hotkey] = new_count
            else:
                busy_count.pop(event.hotkey, None)

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
    eligible: Set[str],
    busy: Optional[Set[str]] = None,
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (eligible, not busy, collateral >= min, rate > 0). If the
    best rate has no qualified miner, fall through to the next-best rate."""
    busy = busy or set()

    def qualifies(hotkey: str) -> bool:
        return (
            hotkey in eligible
            and hotkey not in busy
            and collaterals.get(hotkey, 0) >= min_collateral
            and rates.get(hotkey, 0) > 0
        )

    by_rate: Dict[float, List[str]] = {}
    for hotkey, rate in rates.items():
        if rate > 0:
            by_rate.setdefault(rate, []).append(hotkey)

    for rate in sorted(by_rate, reverse=True):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if winners:
            return winners

    return []
