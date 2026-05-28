"""Crown-time scoring pipeline.

Reward per miner is ``pool × crown_share × sr³ × ramp × capacity ×
volume_factor``; the credibility ramp is applied linearly, not cubed. Any
shortfall recycles to ``RECYCLE_UID``. Entry point is
``score_and_reward_miners(validator)``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple

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
from allways.utils.rate import is_executable_rate
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.scoring_trace import WeightingTrace, log_scoring_trace
from allways.validator.state_store import ValidatorStateStore

if TYPE_CHECKING:
    from neurons.validator import Validator


@dataclass
class DirectionTrace:
    pool: float = 0.0
    crown_blocks: Dict[str, float] = field(default_factory=dict)
    cap_weighted_blocks: Dict[str, float] = field(default_factory=dict)
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
    """Replay the crown-time event stream, derive per-miner rewards
    (pool × crown_share × sr³ × ramp × capacity × volume_factor), recycle the rest.

    Volume weighting is *per direction*: a miner earning crown on btc→tao is
    compared only to btc→tao volume on the network, not to the total of both
    directions. Otherwise heavy tao→btc flow from other miners would dilute
    a btc→tao earner's vol_share even though they own that direction."""
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
    unweighted_rewards = np.zeros(n_uids, dtype=np.float32)
    credibility_since = max(0, self.block - CREDIBILITY_WINDOW_BLOCKS)
    success_stats = self.state_store.get_success_rates_since(credibility_since)
    success_rates = {hk: success_rate(success_stats.get(hk)) for hk in rewardable_hotkeys}
    credibility_ramps = {hk: credibility_ramp(success_stats.get(hk)) for hk in rewardable_hotkeys}

    direction_traces: Dict[Tuple[str, str], DirectionTrace] = {}
    weighting_traces: Dict[str, WeightingTrace] = {}
    try:
        max_swap_amount = int(self.bounds_cache.max_swap_amount())
    except Exception as e:
        bt.logging.warning(f'max_swap_amount read failed: {e}')
        max_swap_amount = 0
    try:
        min_swap_amount = int(self.bounds_cache.min_swap_amount())
    except Exception as e:
        bt.logging.warning(f'min_swap_amount read failed: {e}')
        min_swap_amount = 0
    miner_volume_total: Dict[str, int] = {}
    miner_crown_total: Dict[str, float] = {}
    network_volume_total: int = 0
    network_crown_total: float = 0.0

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
            min_swap_rao=min_swap_amount,
            max_swap_rao=max_swap_amount,
        )
        total_crown_dir = sum(crown_blocks.values())
        volumes_dir = self.state_store.get_volume_by_direction_since(window_start, from_chain, to_chain)
        total_volume_dir = sum(volumes_dir.values())
        for hk, v in volumes_dir.items():
            miner_volume_total[hk] = miner_volume_total.get(hk, 0) + int(v)
        network_volume_total += int(total_volume_dir)
        for hk, blk in crown_blocks.items():
            miner_crown_total[hk] = miner_crown_total.get(hk, 0.0) + blk
        network_crown_total += total_crown_dir

        bt.logging.debug(
            f'V1 scoring [{from_chain}→{to_chain}]: '
            f'total_crown={total_crown_dir:.1f} blk, total_volume_rao={total_volume_dir}'
        )

        if total_crown_dir == 0:
            continue  # empty bucket — pool recycles via the remainder below

        for hotkey, blocks in crown_blocks.items():
            uid = hotkey_to_uid.get(hotkey)
            if uid is None:
                continue  # dereg'd mid-window; credit forfeited
            # Capacity is integrated per-block during the replay, so the
            # effective multiplier is the time-weighted average over the
            # miner's crown intervals. Reading current collateral here
            # would let a post-window top-up retroactively boost credit
            # already earned (#409).
            cap_blocks = trace.cap_weighted_blocks.get(hotkey, 0.0)
            cap = (cap_blocks / blocks) if blocks > 0 else 0.0
            wt = weighting_traces.setdefault(hotkey, WeightingTrace())
            wt.record_capacity(factor=cap)
            wt.record_credibility(
                closed_swaps=sum(success_stats.get(hotkey, (0, 0))),
                ramp_target=CREDIBILITY_RAMP_OBSERVATIONS,
            )
            crown_share_dir = blocks / total_crown_dir
            vol_dir = volumes_dir.get(hotkey, 0)
            vol_share_dir = (vol_dir / total_volume_dir) if total_volume_dir > 0 else 0.0
            vol_factor = volume_factor(vol_dir, total_volume_dir, crown_share_dir)
            base = (
                pool * crown_share_dir * (success_rates[hotkey] ** SUCCESS_EXPONENT) * credibility_ramps[hotkey] * cap
            )
            unweighted_rewards[uid] += base
            rewards[uid] += base * vol_factor
            if vol_factor < 1.0:
                bt.logging.debug(
                    f'V1 scoring [{from_chain}→{to_chain}] {hotkey[:8]}: '
                    f'crown_share={crown_share_dir:.3f} vol_share={vol_share_dir:.3f} '
                    f'vol_factor={vol_factor:.3f}'
                )

    record_volume_traces(
        weighting_traces=weighting_traces,
        hotkey_to_uid=hotkey_to_uid,
        rewards=rewards,
        unweighted_rewards=unweighted_rewards,
        miner_volume_total=miner_volume_total,
        miner_crown_total=miner_crown_total,
        network_volume_total=network_volume_total,
        network_crown_total=network_crown_total,
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
    )

    return rewards, set(range(n_uids))


def capacity_factor(collateral_rao: int, max_swap_amount_rao: int) -> float:
    """min(1, collateral / max_swap). Fail-safe to 1.0 when bounds unset."""
    if max_swap_amount_rao <= 0:
        return 1.0
    if collateral_rao <= 0:
        return 0.0
    return min(1.0, collateral_rao / max_swap_amount_rao)


def volume_factor(
    vol_rao: int,
    total_volume_rao: int,
    crown_share: float,
    alpha: float = VOLUME_WEIGHT_ALPHA,
) -> float:
    """(1-α) + α·min(1, vol_share/crown_share). Idle network or no crown → 1.0
    (no penalty); cap kills any over-serve bonus."""
    if total_volume_rao <= 0 or crown_share <= 0:
        return 1.0
    participation = min(1.0, (vol_rao / total_volume_rao) / crown_share)
    return (1.0 - alpha) + alpha * participation


def record_volume_traces(
    *,
    weighting_traces: Dict[str, WeightingTrace],
    hotkey_to_uid: Dict[str, int],
    rewards: np.ndarray,
    unweighted_rewards: np.ndarray,
    miner_volume_total: Dict[str, int],
    miner_crown_total: Dict[str, float],
    network_volume_total: int,
    network_crown_total: float,
) -> None:
    """Populate the per-miner volume rows of the scoring log. Volume gating is
    already applied inline in ``calculate_miner_rewards``; this only records
    aggregate counters and the effective per-miner multiplier
    (weighted / unweighted) for human-readable diagnosis."""
    for hotkey, wt in weighting_traces.items():
        uid = hotkey_to_uid.get(hotkey)
        if uid is None:
            continue
        unweighted = float(unweighted_rewards[uid])
        weighted = float(rewards[uid])
        effective = (weighted / unweighted) if unweighted > 0 else 1.0
        crown = miner_crown_total.get(hotkey, 0.0)
        crown_share = (crown / network_crown_total) if network_crown_total > 0 else 0.0
        wt.record_volume(
            vol_rao=miner_volume_total.get(hotkey, 0),
            total_volume_rao=network_volume_total,
            crown_share=crown_share,
            factor=effective,
        )


def success_rate(stats: Optional[Tuple[int, int]]) -> float:
    """Raw completed / closed ratio. Cubed in the reward. Zero observations → 0."""
    if not stats or stats == (0, 0):
        return 0.0
    completed, timed_out = stats
    return completed / (completed + timed_out)


def credibility_ramp(stats: Optional[Tuple[int, int]]) -> float:
    """Linear ramp to full credibility at CREDIBILITY_RAMP_OBSERVATIONS closed
    swaps. Applied linearly to the reward, not cubed. Zero observations → 0."""
    if not stats or stats == (0, 0):
        return 0.0
    return min(1.0, sum(stats) / CREDIBILITY_RAMP_OBSERVATIONS)


# ─── Crown-time replay ───────────────────────────────────────────────────


class EventKind(IntEnum):
    """Ordering of coincident-block transitions in the crown-time replay.

    ACTIVE applies first because the on-chain active flag is the per-miner
    tell-all. Then BUSY (busy ends crown for that miner). RESERVED_END is
    next — once a reservation terminates, the pin overlay drops so the
    miner's live rate takes effect again. RATE comes after that so a
    same-block rate update lands cleanly on the now-unpinned series.
    RESERVED_START is last so the pin captures whatever value RATE just
    wrote: a miner who posts a rate change in the same block they get
    reserved has the post-update value pinned, matching the contract's
    block-end commitment read.

    COLLATERAL is independent of qualification — it only scales the credit
    of an interval. Ordered between RATE and RESERVED_START so that a same-
    block post lands before any reservation pin captures, matching the
    intuition that capacity is observable as soon as it's posted.
    """

    ACTIVE = 0
    BUSY = 1
    RESERVED_END = 2
    RATE = 3
    COLLATERAL = 4
    RESERVED_START = 5


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
) -> Tuple[Dict[str, float], Dict[str, int], Set[str], Dict[str, float], Dict[str, int]]:
    """Snapshot rates, busy counts, active set, reservation-pin overlay, and
    posted collateral as they stood at window_start."""
    rates: Dict[str, float] = {}
    busy_count: Dict[str, int] = dict(event_watcher.get_busy_miners_at(window_start))
    active_set: Set[str] = set(event_watcher.get_active_miners_at(window_start))

    for hotkey in rewardable_hotkeys:
        latest_rate = store.get_latest_rate_before(hotkey, from_chain, to_chain, window_start)
        if latest_rate is not None:
            rates[hotkey] = latest_rate[0]

    pinned_rates: Dict[str, float] = {
        hk: rate
        for hk, rate in event_watcher.get_reservation_pins_at(window_start, from_chain, to_chain).items()
        if hk in rewardable_hotkeys and rate > 0
    }

    collaterals: Dict[str, int] = dict(event_watcher.get_miner_collaterals_at(window_start))

    return rates, busy_count, active_set, pinned_rates, collaterals


def merge_replay_events(
    store: ValidatorStateStore,
    event_watcher: ContractEventWatcher,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window active, busy, rate, and reservation-pin transitions
    into one chronologically-sorted stream."""
    events: List[ReplayEvent] = []

    for e in event_watcher.get_active_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVE, value=1.0 if e['active'] else 0.0)
        )

    for e in event_watcher.get_busy_events_in_range(window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.BUSY, value=float(e['delta'])))

    for e in store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.RATE, value=float(e['rate'])))

    for e in event_watcher.get_reservation_pin_events_in_range(window_start, window_end, from_chain, to_chain):
        kind = EventKind.RESERVED_START if e['kind'] == 'start' else EventKind.RESERVED_END
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=kind, value=float(e['rate'])))

    for e in event_watcher.get_collateral_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(
                block=e['block'], hotkey=e['hotkey'], kind=EventKind.COLLATERAL, value=float(e['collateral_rao'])
            )
        )

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
    min_swap_rao: int = 0,
    max_swap_rao: int = 0,
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_blocks_float}``.
    Ties at the same rate split credit evenly. A miner qualifies for crown
    at an instant iff they are on the current metagraph, were active at
    that instant, not busy, had a positive rate posted, and their rate is
    executable under the current contract swap bounds. Active/rate/busy/
    collateral are evaluated per-block via the replay — a miner's status at
    scoring time is irrelevant other than metagraph membership (used to
    credit the UID). The collateral-floor activation gate is still trusted
    to the contract's active flag; halt state is handled at
    ``score_and_reward_miners`` entry.

    Bounds at 0 disable the executability filter (matches the contract's
    "unset" sentinel); the rate-positive floor still applies.

    When ``trace`` is supplied, ``trace.cap_weighted_blocks`` is populated
    alongside ``trace.crown_blocks``. The weighted series multiplies each
    interval's split by ``capacity_factor(collateral_at_block, max_swap_rao)``
    so a post-window collateral boost cannot retroactively scale credit
    already earned (closes #409)."""
    rates, busy_count, active_set, pinned_rates, collaterals = reconstruct_window_start_state(
        store, event_watcher, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_watcher, from_chain, to_chain, window_start, window_end)

    # Rates are stored as canonical_dest per canonical_source (TAO per BTC).
    # In the canonical direction (btc→tao) higher = better; in the reverse
    # direction (tao→btc) lower = better.
    canon_from, _ = canonical_pair(from_chain, to_chain)
    lower_rate_wins = from_chain != canon_from

    def executable_check(rate: float) -> bool:
        return is_executable_rate(rate, from_chain, to_chain, min_swap_rao, max_swap_rao)

    crown_blocks: Dict[str, float] = {}
    cap_weighted_blocks: Dict[str, float] = {}
    prev_block = window_start

    def effective_rates() -> Dict[str, float]:
        """Live rates with pinned-during-reservation values overlaid. A miner
        in ``pinned_rates`` earns crown at the value captured when they were
        reserved, ignoring any subsequent live-rate updates until the
        reservation terminates. Closes the bump-after-pin loophole."""
        if not pinned_rates:
            return rates
        merged = dict(rates)
        merged.update(pinned_rates)
        return merged

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        busy_set = {hk for hk, c in busy_count.items() if c > 0}
        rates_for_instant = effective_rates()
        holders = crown_holders_at_instant(
            rates_for_instant,
            rewardable_hotkeys,
            busy=busy_set,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_blocks += duration
            return
        winner_rate = rates_for_instant.get(holders[0], 0.0)
        if trace is not None and winner_rate > 0:
            trace.best_rate = winner_rate
        split = duration / len(holders)
        for hk in holders:
            crown_blocks[hk] = crown_blocks.get(hk, 0.0) + split
            cap = capacity_factor(collaterals.get(hk, 0), max_swap_rao)
            cap_weighted_blocks[hk] = cap_weighted_blocks.get(hk, 0.0) + split * cap

    def apply_event(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.BUSY:
            new_count = busy_count.get(event.hotkey, 0) + int(event.value)
            if new_count > 0:
                busy_count[event.hotkey] = new_count
            else:
                busy_count.pop(event.hotkey, None)
        elif event.kind is EventKind.RESERVED_START:
            if event.value > 0:
                pinned_rates[event.hotkey] = event.value
        elif event.kind is EventKind.RESERVED_END:
            pinned_rates.pop(event.hotkey, None)
        elif event.kind is EventKind.COLLATERAL:
            collaterals[event.hotkey] = max(0, int(event.value))
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
        trace.cap_weighted_blocks = dict(cap_weighted_blocks)
    return crown_blocks


def crown_holders_at_instant(
    rates: Dict[str, float],
    rewardable: Set[str],
    busy: Optional[Set[str]] = None,
    active: Optional[Set[str]] = None,
    lower_rate_wins: bool = False,
    executable_rate_check: Optional[Callable[[float], bool]] = None,
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (rewardable, active, not busy, rate > 0, executable).
    If the best rate has no qualified miner, fall through to the next-best
    rate.

    ``lower_rate_wins`` flips the sort: rates are stored as canonical_dest
    per canonical_source (TAO per BTC), so higher-is-better only holds in
    the canonical direction (btc→tao). In the reverse direction (tao→btc)
    a smaller TAO/BTC quote means the miner is asking less TAO for 1 BTC —
    a better deal for the swapper, which earns them the crown.

    ``executable_rate_check`` (optional) rejects rates that no user can
    route under the current contract swap bounds. Sentinels like 1e10
    TAO/BTC win the rate sort but map every positive integer satoshi to a
    TAO leg outside ``[min_swap, max_swap]``, so they should not earn
    crown. When None (tests that don't care about bounds; pre-bounds-aware
    callers), no executability filter is applied.

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
        rate = rates.get(hotkey, 0)
        if rate <= 0:
            return False
        if executable_rate_check is not None and not executable_rate_check(rate):
            return False
        return hotkey in rewardable and hotkey not in busy

    by_rate: Dict[float, List[str]] = {}
    for hotkey, rate in rates.items():
        if rate > 0:
            by_rate.setdefault(rate, []).append(hotkey)

    for rate in sorted(by_rate, reverse=not lower_rate_wins):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if winners:
            return winners

    return []
