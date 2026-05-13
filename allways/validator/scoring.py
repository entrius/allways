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
    CREDIBILITY_RAMP_OBSERVATIONS,
    CREDIBILITY_WINDOW_BLOCKS,
    DIRECTION_POOLS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
    SUCCESS_EXPONENT,
    VOLUME_WEIGHT_ALPHA,
)
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.scoring_trace import log_scoring_trace
from allways.validator.state_store import ValidatorStateStore

if TYPE_CHECKING:
    from neurons.validator import Validator


@dataclass
class DirectionTrace:
    pool: float = 0.0
    crown_blocks: Dict[str, float] = field(default_factory=dict)
    unfilled_blocks: int = 0
    best_rate: float = 0.0


@dataclass
class WeightingTrace:
    """Per-hotkey capacity + volume factors surfaced into the scoring log so a
    miner can self-diagnose why their reward < headline crown share."""

    collateral: int = 0
    capacity_factor: float = 1.0
    volume_rao: int = 0
    crown_share: float = 0.0
    volume_share: float = 0.0
    participation: float = 1.0
    volume_factor: float = 1.0


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
    rewards, recycle any undistributed pool to ``RECYCLE_UID``.

    Reward equation:

        reward_i = Σ_dir (pool_d × crown_share_id × sr_i^SUCCESS_EXPONENT × capacity_i) × volume_factor_i

    Where ``capacity_i = min(1.0, collateral_i / max_swap_amount)`` and
    ``volume_factor_i = (1-α) + α * min(1.0, vol_share_i / crown_share_i)``.
    Shortfalls from sr, capacity, and volume all flow to ``RECYCLE_UID``.
    """
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
    success_rates = {hk: success_rate(success_stats.get(hk)) for hk in rewardable_hotkeys}

    direction_traces: Dict[Tuple[str, str], DirectionTrace] = {}
    weighting_traces: Dict[str, WeightingTrace] = {}
    max_swap_amount = read_max_swap_amount(self)
    collaterals: Dict[str, int] = {}

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
            if hotkey not in collaterals:
                collaterals[hotkey] = read_miner_collateral(self, hotkey)
            cap = capacity_factor(collaterals[hotkey], max_swap_amount)
            wt = weighting_traces.setdefault(hotkey, WeightingTrace())
            wt.collateral = collaterals[hotkey]
            wt.capacity_factor = cap
            share = blocks / total
            rewards[uid] += pool * share * (success_rates[hotkey] ** SUCCESS_EXPONENT) * cap

    apply_volume_weighting(
        self,
        rewards=rewards,
        hotkey_to_uid=hotkey_to_uid,
        direction_traces=direction_traces,
        weighting_traces=weighting_traces,
        window_start=window_start,
    )

    recycle_uid = RECYCLE_UID if RECYCLE_UID < n_uids else 0
    distributed = float(rewards.sum())
    recycled = max(0.0, 1.0 - distributed)
    rewards[recycle_uid] += recycled

    log_scoring_trace(
        self,
        window_start=window_start,
        window_end=window_end,
        direction_traces=direction_traces,
        rewards=rewards,
        success_rates=success_rates,
        distributed=distributed,
        recycled=recycled,
        weighting_traces=weighting_traces,
        success_stats=success_stats,
    )

    return rewards, set(range(n_uids))


def capacity_factor(collateral_rao: int, max_swap_amount_rao: int) -> float:
    """Fraction of the swap-size band this miner's collateral can serve.
    Clamped to [0, 1]. Fail-safe: returns 1.0 if max_swap_amount is unset
    (cold start / RPC failure) so the first scoring pass after restart doesn't
    zero every miner."""
    if max_swap_amount_rao <= 0:
        return 1.0
    if collateral_rao <= 0:
        return 0.0
    return min(1.0, collateral_rao / max_swap_amount_rao)


def volume_factor(volume_share: float, crown_share: float, alpha: float = VOLUME_WEIGHT_ALPHA) -> float:
    """Marginal penalty on the gap between volume served and crown held.

    ``participation = min(1.0, volume_share / crown_share)`` — the cap removes
    any reward for over-serving (no incentive to force wash swaps). A miner
    matching their crown share with real volume scores 1.0; an idle crown
    holder scores ``(1 - alpha)``.
    """
    if crown_share <= 0:
        return 1.0  # no crown reward to multiply — factor is moot
    participation = min(1.0, volume_share / crown_share)
    return (1.0 - alpha) + alpha * participation


def apply_volume_weighting(
    self: Validator,
    *,
    rewards: np.ndarray,
    hotkey_to_uid: Dict[str, int],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    weighting_traces: Dict[str, WeightingTrace],
    window_start: int,
) -> None:
    """Apply the volume_factor multiplier to every crown earner.

    Aggregates crown across directions and volume across all completed swaps
    in the window. When total volume is zero (idle network) every factor is
    1.0 — we don't punish miners for a quiet window.
    """
    volumes = self.state_store.get_volume_since(window_start)
    crown_per_hotkey: Dict[str, float] = {}
    for trace in direction_traces.values():
        for hk, blk in trace.crown_blocks.items():
            crown_per_hotkey[hk] = crown_per_hotkey.get(hk, 0.0) + blk

    total_crown = sum(crown_per_hotkey.values())
    total_volume = sum(volumes.values())
    if total_crown <= 0:
        return  # no crown earners; volume factor is moot

    for hotkey, crown in crown_per_hotkey.items():
        uid = hotkey_to_uid.get(hotkey)
        if uid is None or rewards[uid] <= 0:
            continue
        crown_share = crown / total_crown
        vol = volumes.get(hotkey, 0)
        vol_share = (vol / total_volume) if total_volume > 0 else 0.0
        factor = volume_factor(vol_share, crown_share) if total_volume > 0 else 1.0
        rewards[uid] *= factor
        wt = weighting_traces.setdefault(hotkey, WeightingTrace())
        wt.volume_rao = vol
        wt.crown_share = crown_share
        wt.volume_share = vol_share
        wt.participation = min(1.0, vol_share / crown_share) if crown_share > 0 else 1.0
        wt.volume_factor = factor


def read_max_swap_amount(self: Validator) -> int:
    """Read max_swap_amount via bounds_cache. Falls back to 0 (which capacity
    interprets as 'unset → 1.0 factor') if the cache isn't wired or fails."""
    cache = getattr(self, 'bounds_cache', None)
    if cache is None:
        return 0
    try:
        return int(cache.max_swap_amount())
    except Exception as e:
        bt.logging.warning(f'capacity weighting: max_swap_amount read failed, fail-safe to 0: {e}')
        return 0


def read_miner_collateral(self: Validator, hotkey: str) -> int:
    """Read miner collateral via contract_client. Falls back to 0 on error
    (which makes capacity_factor = 0 for that miner — strict). The miner is
    on the contract's active set per replay, so a collateral read failure is
    a real problem worth logging, not silently masking."""
    client = getattr(self, 'contract_client', None)
    if client is None:
        return 0
    try:
        return int(client.get_miner_collateral(hotkey))
    except Exception as e:
        bt.logging.warning(f'capacity weighting: collateral read failed for {hotkey[:8]}: {e}')
        return 0


def success_rate(stats: Optional[Tuple[int, int]]) -> float:
    """Credibility-adjusted success rate. Raw ``completed / total`` is scaled
    by a linear ramp toward full credibility at ``CREDIBILITY_RAMP_OBSERVATIONS``
    closed swaps. Zero-outcome miners earn no trust by default — they have to
    serve swaps to climb the ramp."""
    if not stats or stats == (0, 0):
        return 0.0
    completed, timed_out = stats
    total = completed + timed_out
    return (completed / total) * min(1.0, total / CREDIBILITY_RAMP_OBSERVATIONS)


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
