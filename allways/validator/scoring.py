"""Crown-time scoring pipeline.

Reward per miner is ``eligible × pool × crown_share × capacity``, where
``eligible`` is a flat 0/1 gate read off the on-chain ``MinerState`` counters
(B3.3) — it replaces the old ``sr³ × credibility ramp``. Any shortfall recycles
to ``RECYCLE_UID`` — the burn is dynamic (whatever the pool did not earn) and
deliberate: it keeps a penalty absolute rather than something weight
normalization stretches back to 100%. Entry is ``score_and_reward_miners``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import IntEnum
from functools import partial
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways import dev_signal
from allways.chains import canonical_pair
from allways.classes import ActivityTransition, MinerActivity, next_activity
from allways.cli.swap_commands.swap_intake import bounds_from_config, hub_bounds
from allways.constants import (
    CAPACITY_CURVE_EXPONENT,
    CLEARING_RETENTION_SECS,
    CROWN_RATE_BAND,
    DIRECTION_POOLS,
    MAX_FAILED_SWAPS,
    MAX_SCORING_BACKFILL_SECS,
    MIN_SUCCESSFUL_SWAPS,
    MINER_POOL_SHARE,
    NUMERAIRE_CHAIN,
    POOL_VOLUME_ALPHA,
    POOL_VOLUME_WINDOW_SECS,
    RECYCLE_UID,
    REWARD_MINER_STATES,
    SCORING_WINDOW_BLOCKS,
    SCORING_WINDOW_SECS,
    SWAP_OUTCOME_RETENTION_SECS,
    hub_leg,
    required_collateral,
)
from allways.solana.layouts import lock_max
from allways.solana.pdas import BACKING_BITS
from allways.utils.rate import is_executable_rate, min_executable_hub_leg
from allways.validator.binding import build_attribution
from allways.validator.scoring_trace import WeightingTrace, log_scoring_trace
from allways.validator.state_store import ValidatorStateStore

if TYPE_CHECKING:
    from allways.validator.event_index import SolanaEventIndex
    from neurons.validator import Validator


@dataclass
class DirectionTrace:
    pool: float = 0.0
    crown_time: Dict[str, float] = field(default_factory=dict)
    cap_weighted_time: Dict[str, float] = field(default_factory=dict)
    unfilled_time: int = 0
    best_rate: float = 0.0


def due_for_scoring(current_block: int, last_scored_block: int, initial_scoring_done: bool) -> bool:
    """Block-based scoring gate: fire once on a fresh process, then every
    ``SCORING_WINDOW_BLOCKS`` *blocks* — not steps. A forward pass spans several
    blocks, so a step-count gate fires too rarely and leaves blocks unscored."""
    return not initial_scoring_done or (current_block - last_scored_block) >= SCORING_WINDOW_BLOCKS


def scoring_window_bounds(current_time: int, last_scored_time: int) -> Tuple[int, int]:
    """``(window_start, window_end)`` for a scoring round, on the unix-second
    crown axis (``blockTime``). Anchors window_start to the last-scored time so
    consecutive rounds tile gap-free, capped at ``MAX_SCORING_BACKFILL_SECS``
    for the catch-up case after a stall."""
    window_end = current_time
    window_start = max(0, window_end - MAX_SCORING_BACKFILL_SECS, last_scored_time)
    return window_start, window_end


def score_and_reward_miners(self: Validator) -> None:
    try:
        # The crown replay window is on the unix-time (blockTime) axis — the same
        # axis the Solana event tables are keyed on. Captured once so the window
        # the round scores and the cursor it advances to never drift apart.
        now = int(time.time())
        halted = contract_is_halted(self)
        if halted:
            rewards, miner_uids = build_halted_rewards(self)
            _flush_halt_window(self, now)
        else:
            rewards, miner_uids = calculate_miner_rewards(self, now)
        self.update_scores(rewards, miner_uids)
        dev_signal.emit('scoring_rewards', halted=halted, rewards={i: float(r) for i, r in enumerate(rewards) if r})
        prune_crown_events(self, now)
        # Advance both cursors only after a round completes, so a mid-round
        # failure retries the same window next forward. last_scored_block gates
        # cadence (subtensor block); last_scored_time anchors the crown window.
        self.last_scored_block = self.block
        self.last_scored_time = now
    except Exception as e:
        bt.logging.error(f'Scoring failed: {e}')


def _flush_halt_window(self: Validator, current_time: int) -> None:
    """Clear crown_holders rows in the halted window, advance the
    sync_cursor watermarks, and clear the live current_crown_holders
    table. Mirrors the daemon's halt-tick semantics so the dashboard
    doesn't keep showing pre-halt holders while the validator has
    recycled the pool. Live-table clear lives here (not in the
    per-forward snapshot) so we don't pay an RPC every step just to
    check halt — halt is rare; one clear per round is enough."""
    if not self.database_storage.is_enabled():
        return
    window_end = current_time
    window_start = max(0, window_end - SCORING_WINDOW_SECS)
    if window_end <= 0:
        return
    directions = list(DIRECTION_POOLS.keys())
    self.database_storage.flush_halt_window(
        directions=directions,
        window_start=window_start,
        window_end=window_end,
        max_ts=window_end,
    )
    # Empty rows per direction → upsert_current_crown_snapshot's
    # delete-then-insert flow clears them.
    self.database_storage.upsert_current_crown_snapshot({(f, t): [] for f, t in directions})


def contract_is_halted(self: Validator) -> bool:
    """Best-effort halt check. RPC flakiness should not zero every miner's
    reward, so any exception falls through to normal scoring.

    Delegates to solana_config_cache.halted() — short TTL (~60s) so the
    per-forward live-crown writer and the per-round scoring path share one
    cached value instead of each hitting the Solana Config RPC."""
    try:
        return self.solana_config_cache.halted()
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


def prune_crown_events(self: Validator, current_time: int) -> None:
    """Trim the crown-time event tables to one trailing window. The Solana
    event tables are keyed by unix blockTime, so the cutoff is on that axis;
    this also takes over the active/activity/collateral pruning the deleted
    substrate event_watcher used to own (each preserves a per-hotkey anchor)."""
    cutoff = current_time - SCORING_WINDOW_SECS
    if cutoff <= 0:
        return
    self.state_store.prune_events_older_than(cutoff)
    self.state_store.prune_active_events(cutoff)
    self.state_store.prune_activity_events(cutoff)
    self.state_store.prune_collateral_events(cutoff)
    # clearing_rates outlives the crown cutoff by a full pool-volume window
    # (plus stall headroom) — the per-round pool weighting reads a day back.
    self.state_store.prune_clearing_rates(current_time - CLEARING_RETENTION_SECS)
    self.state_store.prune_swap_outcomes(current_time - SWAP_OUTCOME_RETENTION_SECS)


def is_eligible(miner_state, now: Optional[int] = None) -> bool:
    """The GLOBAL strike gate off the on-chain ``MinerState`` counters (B3.3):
    eligible iff the miner has at least ``MIN_SUCCESSFUL_SWAPS`` successes and at
    most ``MAX_FAILED_SWAPS`` failures — counted across every hub (one reputation).

    The settlement exclusion moved per-hub in v3.1: see ``direction_eligible``,
    which zeroes only the settling hub's contribution instead of the whole miner."""
    del now  # strikes are time-free; kept in the signature for its many call sites
    return (
        int(miner_state.successful_swaps) >= MIN_SUCCESSFUL_SWAPS and int(miner_state.failed_swaps) <= MAX_FAILED_SWAPS
    )


def hub_free(miner_state, backing: str, now: int) -> bool:
    """Whether this hub's penalty-settlement window has passed. Mirrors the contract's per-hub
    ``check_entry_gates`` (v3.1 reversed the whole-miner freeze), tolerating the pre-v3.1 scalar
    shape so mixed-version reads stay safe."""
    bit = BACKING_BITS.get(backing)
    settling = getattr(miner_state, 'settling_until', 0)
    if bit is None or not isinstance(settling, (list, tuple)):
        return now >= lock_max(settling)
    idx = bit.bit_length() - 1
    return now >= int(settling[idx]) if idx < len(settling) else True


def direction_eligible(miner_state, from_chain: str, to_chain: str, now: int) -> bool:
    """Per-direction gate: the global strikes AND a serving hub that is not mid-settle. A spoke
    pair names exactly one hub; a hub↔hub direction keeps earning while EITHER purse is clean (its
    clean-hub quotes keep serving — the settling hub's quotes are entry-gated on-chain anyway)."""
    if not is_eligible(miner_state, now):
        return False
    hubs = [c for c in (from_chain, to_chain) if c in BACKING_BITS]
    if not hubs:
        return True
    return any(hub_free(miner_state, hub, now) for hub in hubs)


def live_miner_states(solana_client, metagraph, attribution: Optional[Dict[str, str]] = None) -> Dict[str, object]:
    """``{hotkey: MinerState}`` for bound, on-metagraph miners — one chain read shared by
    the eligibility gate and the live-state reconcile. Each pubkey-keyed ``MinerState`` is
    attributed to a Bittensor hotkey via the sr25519 binding (B3.2 ``build_attribution``);
    unbound or off-metagraph miners are dropped (they have no UID to credit).
    Pass ``attribution`` to reuse one per-round binding snapshot."""
    if attribution is None:
        attribution = build_attribution(solana_client)
    metagraph_hotkeys = set(metagraph.hotkeys)
    states: Dict[str, object] = {}
    for _pubkey, ms in solana_client.get_all('MinerState'):
        hotkey = attribution.get(str(ms.miner))
        if hotkey is None or hotkey not in metagraph_hotkeys:
            continue
        states[hotkey] = ms
    return states


def build_eligibility(
    solana_client,
    metagraph,
    attribution: Optional[Dict[str, str]] = None,
    now: Optional[int] = None,
) -> Dict[str, bool]:
    """``{hotkey: eligible_bool}`` for on-metagraph miners — ``is_eligible`` over the
    on-chain ``MinerState`` counters (see ``live_miner_states``)."""
    return {hk: is_eligible(ms, now) for hk, ms in live_miner_states(solana_client, metagraph, attribution).items()}


@dataclass
class ScoreRow:
    """One (hotkey, direction) scoring snapshot: the factors the reward math
    actually used, in the shape the ``miner_scores`` / ``current_miner_scores``
    tables persist. ``reward`` is the direction's final emission share (post
    eligibility gate); the other fields are its inputs, so the dashboard can
    write the multiplication out. ``pool`` rides along because it is
    volume-weighted per round — a reader cannot re-derive it."""

    hotkey: str
    from_chain: str
    to_chain: str
    eligible: bool
    pool: float
    crown_share: float
    capacity: float
    reward: float


def build_direction_score_rows(
    from_chain: str,
    to_chain: str,
    pool: float,
    crown_time: Dict[str, float],
    cap_weighted_time: Dict[str, float],
    eligibility: Dict[str, bool],
) -> List[ScoreRow]:
    """Per-holder factors + reward for one direction — the single place the
    reward multiplication lives, shared by the round scorer
    (``calculate_miner_rewards``) and the live tip
    (``snapshot_current_miner_scores``) so the two can never disagree.
    One row per crown holder — only crown holders earn."""
    total_crown_dir = sum(crown_time.values())
    rows: List[ScoreRow] = []
    if total_crown_dir <= 0:
        return rows
    for hotkey, secs in crown_time.items():
        # Capacity is integrated over time during the replay, so the effective
        # multiplier is the time-weighted average over the miner's crown
        # intervals. Reading current collateral here would let a post-window
        # top-up retroactively boost credit already earned (#409).
        cap_secs = cap_weighted_time.get(hotkey, 0.0)
        cap = (cap_secs / secs) if secs > 0 else 0.0
        eligible = eligibility.get(hotkey, False)
        crown_share_dir = secs / total_crown_dir
        rows.append(
            ScoreRow(
                hotkey=hotkey,
                from_chain=from_chain,
                to_chain=to_chain,
                eligible=eligible,
                pool=pool,
                crown_share=crown_share_dir,
                capacity=cap,
                reward=(1.0 if eligible else 0.0) * pool * crown_share_dir * cap,
            )
        )
    return rows


def compute_direction_pools(
    clearing_volumes: Dict[Tuple[str, str], Dict[str, Tuple[int, int]]],
) -> Dict[Tuple[str, str], float]:
    """Volume-weighted pools from a trailing window of realized swaps
    (``get_clearing_volumes`` shape). Each hub↔spoke *pair* earns
    ``(1−α)/pairs + α × its share of hub-leg notional``, split evenly between
    its two directions — weighting at pair level means one leg can't be
    inflated without inflating the whole pair. No volume anywhere → the equal
    split. Pools sum to ``MINER_POOL_SHARE``, not 1.0 — the burn lives here.

    Volumes are the pair's HUB-leg notional, so they are only comparable
    within one hub's family (lamports vs rao — converting across would smuggle
    a price oracle in). Each hub family therefore holds a FIXED share of the
    pool (proportional to its pair count) and the α-blend runs within it; a
    single-family registry reduces exactly to the old global blend."""
    pair_directions: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for from_chain, to_chain in DIRECTION_POOLS:
        pair_directions.setdefault(canonical_pair(from_chain, to_chain), []).append((from_chain, to_chain))

    pair_volumes: Dict[Tuple[str, str], int] = {}
    for pair, directions in pair_directions.items():
        volume = 0
        for from_chain, to_chain in directions:
            # Hub-and-spoke: every direction has its hub on exactly one leg, so the
            # hub side is the notional comparable across the family's pairs.
            leg = 0 if from_chain == hub_leg(from_chain, to_chain) else 1
            volume += sum(sums[leg] for sums in clearing_volumes.get((from_chain, to_chain), {}).values())
        pair_volumes[pair] = volume

    if sum(pair_volumes.values()) <= 0:
        return dict(DIRECTION_POOLS)

    families: Dict[str, List[Tuple[str, str]]] = {}
    for pair in pair_directions:
        families.setdefault(hub_leg(*pair) or pair[0], []).append(pair)

    pools: Dict[Tuple[str, str], float] = {}
    total_pairs = len(pair_directions)
    for family_pairs in families.values():
        family_share = len(family_pairs) / total_pairs
        family_volume = sum(pair_volumes[p] for p in family_pairs)
        equal_pair_share = 1.0 / len(family_pairs)
        for pair in family_pairs:
            if family_volume <= 0:
                pair_share = equal_pair_share
            else:
                volume_share = pair_volumes[pair] / family_volume
                pair_share = (1.0 - POOL_VOLUME_ALPHA) * equal_pair_share + POOL_VOLUME_ALPHA * volume_share
            directions = pair_directions[pair]
            for direction in directions:
                pools[direction] = MINER_POOL_SHARE * family_share * pair_share / len(directions)
    return pools


def calculate_miner_rewards(self: Validator, current_time: int) -> Tuple[np.ndarray, Set[int]]:
    """Replay the crown-time event stream, derive per-miner rewards
    (eligible × pool × crown_share × capacity), recycle the rest.
    ``current_time`` is the unix-seconds window_end (the crown axis)."""
    n_uids = self.metagraph.n.item()
    if n_uids == 0:
        return np.array([], dtype=np.float32), set()

    window_start, window_end = scoring_window_bounds(current_time, self.last_scored_time)

    # A miner's *current* active flag is irrelevant to whether they earned
    # crown during the replay window. The only at-scoring-time check is
    # metagraph membership, because a dereg'd miner has no UID to credit.
    # Active, rate, and activity are all evaluated per-block via event replay
    # inside replay_crown_time_window. Collateral-floor invariants are
    # trusted to the contract's active flag.
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    # One live MinerState snapshot serves two jobs: the flat eligibility gate off the
    # on-chain counters (B3.3, pubkey→hotkey via the sr25519 binding; absent hotkey → not
    # eligible), and the reconcile backstop that corrects event-derived active/collateral
    # state the ingest lost — before the replay below reads those tables.
    live_states = live_miner_states(self.solana_client, self.metagraph)
    self.event_index.reconcile_live_state(live_states, now=current_time)
    # The global (strike) view feeds the trace log; rows gate per direction so a TAO settle zeroes
    # only TAO-hub contributions while SOL rows keep earning (v3.1).
    eligibility = {hk: is_eligible(ms, current_time) for hk, ms in live_states.items()}

    direction_traces: Dict[Tuple[str, str], DirectionTrace] = {}
    weighting_traces: Dict[str, WeightingTrace] = {}
    # Per-(hotkey, direction) factor snapshots — the rows miner_scores persists.
    # Only on-metagraph holders land here (a dereg'd miner is paid nothing).
    score_rows: List[ScoreRow] = []
    # Captured only when dashboard storage is enabled — the tee inside
    # replay_crown_time_window is a no-op when intervals_out is None, so
    # disabled validators pay zero cost here.
    storage_enabled = self.database_storage.is_enabled()
    intervals_by_dir: Dict[Tuple[str, str], List[Tuple[int, int, Dict[str, float], float]]] = {}
    # Per-backing bounds off the cached Config; each direction anchors on its own hub leg
    # (SOL lamports for sol-hub directions, rao for tao-hub ones). Empty map on failure =
    # both-0 per direction = permissive, matching the old failure mode.
    try:
        swap_bounds = bounds_from_config(self.solana_config_cache.config())
    except Exception as e:
        bt.logging.warning(f'swap-bounds read failed: {e}')
        swap_bounds = {}

    # Pools follow realized demand: one volume read for the trailing day, shared
    # by every direction this round so the pools it pays sum to exactly 1.
    pools = compute_direction_pools(
        self.state_store.get_clearing_volumes(current_time - POOL_VOLUME_WINDOW_SECS, current_time)
    )

    for (from_chain, to_chain), pool in pools.items():
        trace = DirectionTrace(pool=pool)
        direction_traces[(from_chain, to_chain)] = trace
        intervals: Optional[List[Tuple[int, int, Dict[str, float], float]]] = None
        if storage_enabled:
            intervals = []
            intervals_by_dir[(from_chain, to_chain)] = intervals
        min_swap_hub, max_swap_hub = hub_bounds(swap_bounds, from_chain, to_chain)
        crown_time = replay_crown_time_window(
            store=self.state_store,
            event_index=self.event_index,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            rewardable_hotkeys=rewardable_hotkeys,
            trace=trace,
            intervals_out=intervals,
            min_swap_hub=min_swap_hub,
            max_swap_hub=max_swap_hub,
            purse_known=direction_purse_known(from_chain, to_chain),
        )
        total_crown_dir = sum(crown_time.values())

        bt.logging.debug(f'V1 scoring [{from_chain}→{to_chain}]: total_crown={total_crown_dir:.1f}s')

        if total_crown_dir == 0:
            continue  # empty bucket — pool recycles via the remainder below

        rows = build_direction_score_rows(
            from_chain,
            to_chain,
            pool,
            crown_time=crown_time,
            cap_weighted_time=trace.cap_weighted_time,
            eligibility={
                hk: direction_eligible(ms, from_chain, to_chain, current_time) for hk, ms in live_states.items()
            },
        )
        for row in rows:
            uid = hotkey_to_uid.get(row.hotkey)
            if uid is None:
                continue  # dereg'd mid-window; credit forfeited
            wt = weighting_traces.setdefault(row.hotkey, WeightingTrace())
            wt.record_capacity(factor=row.capacity)
            wt.record_eligibility(eligible=row.eligible)
            rewards[uid] += row.reward
            score_rows.append(row)

    # The shortfall is burned to RECYCLE_UID rather than left for weight
    # normalization to stretch back over the earners. Normalization preserves
    # every ratio, so an undistributed pool would cost a shallow field nothing.
    # Pools sum to MINER_POOL_SHARE, so at least BURN_RATE recycles every round.
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
        eligibility=eligibility,
        distributed=distributed,
        recycled=recycled,
        weighting_traces=weighting_traces,
        swap_bounds=swap_bounds,
    )

    if storage_enabled:
        # Persist the crown intervals directly (no per-tick expansion).
        # `flush_scoring_window` deletes [window_start, window_end) per
        # direction before upserting, so the wipe matches the data exactly.
        # rate_history is NOT written here: the indexer owns it (real-time,
        # per QuoteSet event) — the validator only persists what it paid.
        crown_rows_by_dir = {d: intervals_to_crown_rows(ivs, d[0], d[1]) for d, ivs in intervals_by_dir.items()}
        # Crown intervals tile [window_start, window_end); the last one ends at
        # window_end, so the freshness cursor is "as of window_end" (unix secs).
        # window_end also keys the round in miner_scores — one snapshot row per
        # (round, hotkey, direction), written in the same transaction as the
        # crown ledger so the two can never disagree about a round.
        cursor_ts = max(0, window_end)
        self.database_storage.flush_scoring_window(
            crown_rows_by_direction=crown_rows_by_dir,
            crown_window_bounds_by_direction={d: (window_start, window_end) for d in DIRECTION_POOLS},
            miner_score_rows=miner_score_tuples(score_rows, cursor_ts),
            crown_holders_max_ts=cursor_ts,
        )

    return rewards, set(range(n_uids))


def miner_score_tuples(score_rows: List[ScoreRow], ts: int) -> List[Tuple]:
    """Shape ``ScoreRow``s for the storage layer (which takes ready-made row
    tuples): ``ts`` leads as ``round_ts`` (round flush) or snapshot ``ts``
    (live tip) — the two tables share the rest of the shape."""
    return [
        (
            ts,
            r.hotkey,
            r.from_chain,
            r.to_chain,
            r.eligible,
            r.pool,
            r.crown_share,
            r.capacity,
            r.reward,
        )
        for r in score_rows
    ]


def snapshot_current_miner_scores(self: Validator, at_time: Optional[int] = None) -> List[Tuple]:
    """Mid-round live tip: the factors each crown holder would be paid on if
    the in-progress round [last_scored_time, now] flushed right now — the same
    replay + ``build_direction_score_rows`` math ``calculate_miner_rewards``
    runs, minus weights/traces/recycle. Returns ``current_miner_scores``-shaped
    tuples for the per-forward-step wipe+insert (``miner_scores``' live
    counterpart, same pattern as ``current_crown_holders``).

    Cost: one MinerState read for eligibility plus SQLite replays over the
    partial window — bounded by the round width. Only called when dashboard
    storage is enabled."""
    ts = int(time.time()) if at_time is None else at_time
    window_start, window_end = scoring_window_bounds(ts, self.last_scored_time)
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    live_states = live_miner_states(self.solana_client, self.metagraph)
    try:
        swap_bounds = bounds_from_config(self.solana_config_cache.config())
    except Exception as e:
        bt.logging.warning(f'swap-bounds read failed in live score snapshot: {e}')
        swap_bounds = {}
    rows: List[ScoreRow] = []
    pools = compute_direction_pools(self.state_store.get_clearing_volumes(ts - POOL_VOLUME_WINDOW_SECS, ts))
    for (from_chain, to_chain), pool in pools.items():
        trace = DirectionTrace(pool=pool)
        min_swap_hub, max_swap_hub = hub_bounds(swap_bounds, from_chain, to_chain)
        crown_time = replay_crown_time_window(
            store=self.state_store,
            event_index=self.event_index,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            rewardable_hotkeys=rewardable_hotkeys,
            trace=trace,
            min_swap_hub=min_swap_hub,
            max_swap_hub=max_swap_hub,
            purse_known=direction_purse_known(from_chain, to_chain),
        )
        if not crown_time:
            continue
        dir_rows = build_direction_score_rows(
            from_chain,
            to_chain,
            pool,
            crown_time=crown_time,
            cap_weighted_time=trace.cap_weighted_time,
            eligibility={hk: direction_eligible(ms, from_chain, to_chain, ts) for hk, ms in live_states.items()},
        )
        rows.extend(dir_rows)
    return miner_score_tuples(rows, ts)


def capacity_factor(collateral_rao: int, max_swap_amount_rao: int) -> float:
    """min(1, (collateral / required_collateral(max_swap))^k) — full credit means holding enough
    to accept a max_swap fill at the contract's 1.10× gate. The exponent k (CAPACITY_CURVE_EXPONENT,
    >1) makes the ramp convex: backing a rate on a sliver of the band earns less than the raw ratio,
    so thin-parked collateral is penalised harder and depth is worth posting. Still capped at 1.0 —
    collateral past the requirement earns nothing extra, so it never becomes pay-to-win. Fail-safe to
    1.0 when bounds unset."""
    if max_swap_amount_rao <= 0:
        return 1.0
    if collateral_rao <= 0:
        return 0.0
    ratio = collateral_rao / required_collateral(max_swap_amount_rao)
    return min(1.0, ratio**CAPACITY_CURVE_EXPONENT)


# ─── Crown-time replay ───────────────────────────────────────────────────


class EventKind(IntEnum):
    """Ordering of coincident-instant transitions in the crown-time replay.

    ACTIVE applies first because the on-chain active flag is the per-miner
    tell-all. Then ACTIVITY (the MinerActivity machine — a reserved/fulfilling
    miner forfeits crown). RATE comes after so a same-instant rate update lands
    once the qualification gates are set. COLLATERAL is last — it's independent
    of qualification, only scaling an interval's credit, so a same-instant post
    is observable as soon as it lands.
    """

    ACTIVE = 0
    ACTIVITY = 1
    RATE = 2
    COLLATERAL = 3


@dataclass
class ReplayEvent:
    """One transition in the chronological replay stream. ``value`` is
    polymorphic on ``kind``: rate as float, active as 0/1, or an
    ``ActivityTransition`` value for ACTIVITY. ``hub`` scopes an ACTIVITY
    transition to one purse's machine (v3.1 per-hub busy); None = every hub."""

    block: int
    hotkey: str
    kind: EventKind
    value: float
    hub: Optional[str] = None

    @property
    def sort_key(self) -> Tuple[int, int, int]:
        # ActivityTransition values double as the within-instant tiebreak so a
        # swap's FULFILL_* applies before its reservation's RESERVE_EXPIRE.
        sub = int(self.value) if self.kind is EventKind.ACTIVITY else 0
        return (self.block, int(self.kind), sub)


_warned_activity_transitions: Set[Tuple[MinerActivity, ActivityTransition]] = set()


def warn_unexpected_activity(state: MinerActivity, transition: ActivityTransition) -> None:
    """One-time warn per (state, transition) the machine has no edge for —
    defensive only; the state is held unchanged."""
    key = (state, transition)
    if key not in _warned_activity_transitions:
        _warned_activity_transitions.add(key)
        bt.logging.warning(f'crown replay: unexpected activity transition {transition.name} in {state.name}')


def reconstruct_window_start_state(
    store: ValidatorStateStore,
    event_index: SolanaEventIndex,
    from_chain: str,
    to_chain: str,
    window_start: int,
    rewardable_hotkeys: Set[str],
) -> Tuple[Dict[str, float], Dict[str, Dict[str, MinerActivity]], Set[str], Dict[str, int]]:
    """Snapshot rates, per-miner activity, active set, and posted collateral as
    they stood at window_start. ``activity`` is per (hotkey, hub) — v3.1 busy is
    per purse — holding only busy hubs (a reservation/swap open before the window
    shows RESERVED/FULFILLING at the edge); absences default to AVAILABLE. Rate
    read is one batched query per direction (N rewardable hotkeys would otherwise
    be N point lookups, runs every forward step from snapshot_current_crown_holders)."""
    activity: Dict[str, Dict[str, MinerActivity]] = {
        hk: dict(hubs) for hk, hubs in event_index.get_activity_state_at(window_start).items()
    }
    active_set: Set[str] = set(event_index.get_active_miners_at(window_start))

    all_latest = store.get_latest_rates_before(from_chain, to_chain, window_start)
    rates: Dict[str, float] = {hk: rate_block[0] for hk, rate_block in all_latest.items() if hk in rewardable_hotkeys}

    collaterals: Dict[str, int] = dict(event_index.get_miner_collaterals_at(window_start))

    return rates, activity, active_set, collaterals


def merge_replay_events(
    store: ValidatorStateStore,
    event_index: SolanaEventIndex,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window active, activity, rate, and collateral transitions into
    one chronologically-sorted stream (incl. the synthetic RESERVE_EXPIRE)."""
    events: List[ReplayEvent] = []

    for e in event_index.get_active_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVE, value=1.0 if e['active'] else 0.0)
        )

    for e in event_index.get_activity_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(
                block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVITY, value=float(e['kind']), hub=e.get('hub')
            )
        )

    for e in store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.RATE, value=float(e['rate'])))

    for e in event_index.get_collateral_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(
                block=e['block'], hotkey=e['hotkey'], kind=EventKind.COLLATERAL, value=float(e['collateral_rao'])
            )
        )

    events.sort(key=lambda ev: ev.sort_key)
    return events


def direction_serving_hubs(from_chain: str, to_chain: str) -> List[str]:
    """The hubs a direction can serve through — its hub legs. A spoke↔spoke pair
    (never scored) falls back to every hub, reproducing the global-busy reading."""
    hubs = [c for c in (from_chain, to_chain) if c in BACKING_BITS]
    return hubs or list(BACKING_BITS)


def rewardable_by_activity(
    activity: Dict[str, Dict[str, MinerActivity]],
    hotkeys: Set[str],
    serving_hubs: List[str],
) -> Set[str]:
    """Hotkeys whose per-hub activity still permits crown on a direction served by
    ``serving_hubs``: busy forfeits only when EVERY serving hub is mid-reservation/
    swap — the activity twin of ``direction_eligible``'s any-clean-hub rule (v3.1:
    a sol-busy miner keeps earning on its tao quotes and vice versa; a hub↔hub
    direction forfeits only when both purses are busy). Absent = AVAILABLE."""
    out: Set[str] = set()
    for hk in hotkeys:
        per_hub = activity.get(hk) or {}
        if any(per_hub.get(h, MinerActivity.AVAILABLE) in REWARD_MINER_STATES for h in serving_hubs):
            out.add(hk)
    return out


def direction_purse_known(from_chain: str, to_chain: str) -> bool:
    """Whether the crown's collateral event stream IS this direction's funding purse.

    The event index tracks the local SOL purse (CollateralPosted/Withdrawn), which funds
    sol-hub directions. A tao-hub direction is funded by the attested TAO bond, which has no
    event stream yet — its purse-axis gates (can_fund, depth split, capacity) run neutral
    rather than compare rao bounds against a lamport purse. Attestation-fed purse events are
    the deferred follow-up that flips this on for TAO."""
    return hub_leg(from_chain, to_chain) == NUMERAIRE_CHAIN


def crown_can_fund(hotkey, rate, from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals):
    """Boundary-squat gate: a miner who cannot fund their own rate's smallest hub leg
    at the contract's 1.10× reserve requirement (mirroring routing's ``swap_viable``)
    is unreservable at any size and earns no crown. Unknown collateral counts as zero
    (fail closed) — the live-state reconcile seeds a baseline for every bound active
    miner, so absent means the chain doesn't know this miner either."""
    min_leg = min_executable_hub_leg(rate, from_chain, to_chain, min_swap_hub, max_swap_hub)
    return min_leg == 0 or collaterals.get(hotkey, 0) >= required_collateral(min_leg)


def crown_depth_shares(
    holders: List[str],
    collaterals: Dict[str, int],
    max_swap_hub: int,
) -> Dict[str, float]:
    """Split one interval's crown credit across band holders in proportion to
    collateral depth, so matching the leader's rate earns share only to the
    extent it is backed — depth becomes something a rival can take from you.
    Depth is capped at ``required_collateral(max_swap)``: collateral past a
    full-capacity fill buys no extra share (same never-pay-to-win bound
    ``capacity_factor`` applies at 1.0). All-zero depth (bounds unset in tests,
    or a band of unknown collaterals) falls back to an even split."""
    cap = required_collateral(max_swap_hub) if max_swap_hub > 0 else None
    depths = {hk: min(collaterals.get(hk, 0), cap) if cap else collaterals.get(hk, 0) for hk in holders}
    total = sum(depths.values())
    if total <= 0:
        return {hk: 1.0 / len(holders) for hk in holders}
    return {hk: depth / total for hk, depth in depths.items()}


def make_crown_predicates(from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals):
    """Crown-eligibility predicates ``(executable_check, can_fund)`` shared by the
    scoring replay and the live snapshot, so the live crown view can never diverge
    from the rewarded ledger. Both are the shared rate utils with this direction's
    hub-leg bounds/collateral bound in."""
    executable_check = partial(
        is_executable_rate,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_hub=min_swap_hub,
        max_swap_hub=max_swap_hub,
    )
    can_fund = partial(
        crown_can_fund,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_hub=min_swap_hub,
        max_swap_hub=max_swap_hub,
        collaterals=collaterals,
    )
    return executable_check, can_fund


def replay_crown_time_window(
    store: ValidatorStateStore,
    event_index: SolanaEventIndex,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
    rewardable_hotkeys: Set[str],
    trace: Optional[DirectionTrace] = None,
    intervals_out: Optional[List[Tuple[int, int, Dict[str, float], float]]] = None,
    min_swap_hub: int = 0,
    max_swap_hub: int = 0,
    rate_band: float = CROWN_RATE_BAND,
    purse_known: bool = True,
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_seconds_float}``.
    Every qualified miner within ``rate_band`` of the best qualified rate holds
    the crown together, splitting each interval by collateral depth
    (``crown_depth_shares``); band 0 narrows the holder set to the exact best
    rate, where depth still apportions any tie. A miner qualifies for crown
    at an instant iff they are on the current metagraph, were active at
    that instant, in a rewardable activity state (∈ REWARD_MINER_STATES — a
    reserved/fulfilling miner forfeits), had a positive rate posted, and their
    rate is executable under the current contract swap bounds. Active/rate/
    activity/collateral are evaluated per-block via the replay — a miner's status at
    scoring time is irrelevant other than metagraph membership (used to
    credit the UID). The collateral-floor activation gate is still trusted
    to the contract's active flag; halt state is handled at
    ``score_and_reward_miners`` entry.

    ``min_swap_hub``/``max_swap_hub`` are the direction's HUB-leg bounds (hub smallest-units).
    Bounds at 0 disable the executability filter (matches the contract's
    "unset" sentinel); the rate-positive floor still applies. ``purse_known=False``
    (``direction_purse_known``: the collateral event stream is not this direction's funding
    purse — tao-hub directions until attestation-fed purse events exist) keeps the rate gates
    but runs the purse-axis ones neutral: can_fund permissive, depth = even split, capacity 1.0.

    When ``trace`` is supplied, ``trace.cap_weighted_time`` is populated
    alongside ``trace.crown_time``. The weighted series multiplies each
    interval's split by ``capacity_factor(collateral_at_block, max_swap_hub)``
    so a post-window collateral boost cannot retroactively scale credit
    already earned (closes #409)."""
    rates, activity, active_set, collaterals = reconstruct_window_start_state(
        store, event_index, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_index, from_chain, to_chain, window_start, window_end)

    # Rates are stored as canonical_dest per canonical_source (BTC per TAO).
    # In the canonical direction (tao→btc) higher = better; in the reverse
    # direction (btc→tao) lower = better.
    canon_from, _ = canonical_pair(from_chain, to_chain)
    lower_rate_wins = from_chain != canon_from

    executable_check, can_fund = make_crown_predicates(from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals)

    crown_time: Dict[str, float] = {}
    cap_weighted_time: Dict[str, float] = {}
    prev_ts = window_start

    bounds_set = min_swap_hub > 0 or max_swap_hub > 0

    serving_hubs = direction_serving_hubs(from_chain, to_chain)

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        rewardable_by_state = rewardable_by_activity(activity, rewardable_hotkeys, serving_hubs)
        rates_for_instant = rates
        holders = crown_holders_at_instant(
            rates_for_instant,
            rewardable_hotkeys,
            rewardable_by_state=rewardable_by_state,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set and purse_known else None,
            rate_band=rate_band,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_time += duration
            return
        winner_rate = rates_for_instant.get(holders[0], 0.0)
        if trace is not None and winner_rate > 0:
            trace.best_rate = winner_rate
        # Holders are every qualified miner within CROWN_RATE_BAND of the best
        # rate; the interval splits across them by collateral depth, so a
        # one-tick undercut no longer takes the crown outright and depth is the
        # axis a rival must beat you on inside the band. Unknown purse
        # (tao-hub direction) → even split, no depth axis.
        shares = crown_depth_shares(holders, collaterals if purse_known else {}, max_swap_hub)
        if intervals_out is not None:
            intervals_out.append((interval_start, interval_end, dict(shares), winner_rate))
        for hk, share in shares.items():
            split = duration * share
            crown_time[hk] = crown_time.get(hk, 0.0) + split
            # Unknown collateral counts as zero, matching can_fund's fail-closed
            # (the reconcile seeds a baseline for every bound active miner);
            # capacity_factor keeps its bounds-unset fail-safe of 1.0.
            cap = capacity_factor(collaterals.get(hk, 0), max_swap_hub) if purse_known else 1.0
            cap_weighted_time[hk] = cap_weighted_time.get(hk, 0.0) + split * cap

    def apply_event(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.ACTIVITY:
            transition = ActivityTransition(int(event.value))
            per_hub = activity.setdefault(event.hotkey, {})
            # A hub-scoped edge steps one purse's machine; a NULL-hub edge (legacy
            # rows, pre-v3.1 payloads) steps every hub — the global-busy reading.
            for h in [event.hub] if event.hub else list(BACKING_BITS):
                cur = per_hub.get(h, MinerActivity.AVAILABLE)
                nxt = next_activity(cur, transition)
                if nxt is None:
                    warn_unexpected_activity(cur, transition)
                    nxt = cur
                if nxt is MinerActivity.AVAILABLE:
                    per_hub.pop(h, None)
                else:
                    per_hub[h] = nxt
            if not per_hub:
                activity.pop(event.hotkey, None)
        elif event.kind is EventKind.COLLATERAL:
            collaterals[event.hotkey] = max(0, int(event.value))
        else:  # ACTIVE
            if event.value > 0:
                active_set.add(event.hotkey)
            else:
                active_set.discard(event.hotkey)

    for event in replay_events:
        credit_interval(prev_ts, event.block)
        apply_event(event)
        prev_ts = event.block

    credit_interval(prev_ts, window_end)
    if trace is not None:
        trace.crown_time = dict(crown_time)
        trace.cap_weighted_time = dict(cap_weighted_time)
    return crown_time


def snapshot_current_crown_holders(
    self: Validator,
    at_time: Optional[int] = None,
) -> Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]]:
    """Cheap "who holds the crown right now" per direction. Used by the
    per-forward-step live-crown writer.

    Reconstructs rates/activity/active at the current time and evaluates
    ``crown_holders_at_instant`` once per direction — no event-stream walk,
    so cost is O(rewardable_hotkeys) per direction, sub-millisecond in
    practice. Returns rows in ``DatabaseStorage.upsert_current_crown_snapshot``
    shape: ``(from_chain, to_chain, hotkey, credit, rate, ts)`` keyed
    by direction. An empty list for a direction means "no qualifying
    holder right now" — instructs the storage layer to clear that
    direction's rows.

    Halt is not checked here — that RPC is expensive and halt is rare;
    instead ``_flush_halt_window`` clears the live table at the next
    scoring round when a halt is detected. Worst case the live table
    shows the actual best-rate holder during halt for ~1h, while the
    HaltBanner + top-right indicator (both fed by /halt off
    contract_events) signal the recycle state to users."""
    # The live crown reads per-instant state at "now" on the unix-time
    # (blockTime) axis the event tables use. Tests pass an explicit instant.
    ts = int(time.time()) if at_time is None else at_time
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    # Match the scoring path's executability filter so the live table never
    # credits an out-of-bounds-rate holder the ledger drops. Bounds from the
    # TTL cache (no per-step RPC); empty map on failure = permissive, as before.
    try:
        swap_bounds = bounds_from_config(self.solana_config_cache.config())
    except Exception as e:
        bt.logging.warning(f'swap-bounds read failed in live snapshot: {e}')
        swap_bounds = {}
    rows_by_direction: Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]] = {}
    for from_chain, to_chain in DIRECTION_POOLS:
        min_swap_hub, max_swap_hub = hub_bounds(swap_bounds, from_chain, to_chain)
        bounds_set = min_swap_hub > 0 or max_swap_hub > 0
        purse_known = direction_purse_known(from_chain, to_chain)
        rates, activity, active_set, collaterals = reconstruct_window_start_state(
            self.state_store,
            self.event_index,
            from_chain,
            to_chain,
            ts,
            rewardable_hotkeys,
        )
        canon_from, _ = canonical_pair(from_chain, to_chain)
        lower_rate_wins = from_chain != canon_from
        rewardable_by_state = rewardable_by_activity(
            activity, rewardable_hotkeys, direction_serving_hubs(from_chain, to_chain)
        )

        # Same predicates the scoring replay uses, so the live table never
        # credits a holder the ledger drops. Built per direction so each
        # closure captures the right chain pair.
        executable_check, can_fund = make_crown_predicates(
            from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals
        )

        holders = crown_holders_at_instant(
            rates,
            rewardable_hotkeys,
            rewardable_by_state=rewardable_by_state,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set and purse_known else None,
            rate_band=CROWN_RATE_BAND,
        )
        if holders:
            # Same band + depth split as the scoring replay, so the live table agrees
            # with what the round scorer will credit. Every row carries the band's
            # anchor (best) rate — the direction's crown rate as a taker sees it.
            shares = crown_depth_shares(holders, collaterals if purse_known else {}, max_swap_hub)
            rate = rates.get(holders[0], 0.0)
            rows_by_direction[(from_chain, to_chain)] = [
                (from_chain, to_chain, hk, share, rate, ts) for hk, share in shares.items()
            ]
        else:
            rows_by_direction[(from_chain, to_chain)] = []
    return rows_by_direction


def intervals_to_crown_rows(
    intervals: List[Tuple[int, int, Dict[str, float], float]],
    from_chain: str,
    to_chain: str,
) -> List[Tuple[int, int, str, str, str, float, float]]:
    """Convert uniform-state crown intervals to crown_holders rows.

    For each (started_at, ended_at, shares, rate) emit one row per holder
    carrying its depth-proportional credit (``crown_depth_shares`` output, so
    per-interval per-direction credits sum to 1.0). A holder's crown *time*
    over a window is then ``SUM((ended_at - started_at) * credit)``, matching
    the duration the scoring replay already integrates (no per-tick expansion)."""
    rows: List[Tuple[int, int, str, str, str, float, float]] = []
    for lo, hi, shares, rate in intervals:
        if not shares or hi <= lo:
            continue
        for hotkey, share in shares.items():
            rows.append((lo, hi, from_chain, to_chain, hotkey, share, rate))
    return rows


def crown_holders_at_instant(
    rates: Dict[str, float],
    rewardable: Set[str],
    rewardable_by_state: Optional[Set[str]] = None,
    active: Optional[Set[str]] = None,
    lower_rate_wins: bool = False,
    executable_rate_check: Optional[Callable[[float], bool]] = None,
    can_fund_at_rate: Optional[Callable[[str, float], bool]] = None,
    rate_band: float = 0.0,
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (rewardable, active, activity ∈ REWARD_MINER_STATES, rate >
    0, executable). If the best rate has no qualified miner, fall through to the
    next-best rate.

    ``rate_band`` widens the winning set: every qualified miner whose rate is
    within that fraction of the best *qualified* rate is a holder (best first).
    The anchor is the best qualified rate, not the best posted one — an
    unqualified sentinel can't drag the band out of everyone else's reach.
    0 keeps the historical exact-best-rate behaviour.

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

    ``rewardable_by_state`` (optional) is the set whose activity ∈
    REWARD_MINER_STATES — a miner outside it (reserved/fulfilling) forfeits the
    crown. None skips the state gate (tests that don't model activity).

    ``active`` defaults to None for tests that don't care about the
    historical active flag; replay callers pass the reconstructed set."""

    def qualifies(hotkey: str) -> bool:
        if active is not None and hotkey not in active:
            return False
        if rewardable_by_state is not None and hotkey not in rewardable_by_state:
            return False
        rate = rates.get(hotkey, 0)
        if rate <= 0:
            return False
        if executable_rate_check is not None and not executable_rate_check(rate):
            return False
        if can_fund_at_rate is not None and not can_fund_at_rate(hotkey, rate):
            return False
        return hotkey in rewardable

    by_rate: Dict[float, List[str]] = {}
    for hotkey, rate in rates.items():
        if rate > 0:
            by_rate.setdefault(rate, []).append(hotkey)

    ordered = sorted(by_rate, reverse=not lower_rate_wins)
    for i, rate in enumerate(ordered):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if not winners:
            continue
        if rate_band > 0:
            # Anchor at the best qualified rate, then sweep worse levels while
            # they stay inside the band. Levels are already sorted best-first,
            # so the first out-of-band level ends the sweep.
            edge = rate * (1 + rate_band) if lower_rate_wins else rate * (1 - rate_band)
            for worse in ordered[i + 1 :]:
                if (worse > edge) if lower_rate_wins else (worse < edge):
                    break
                winners.extend(hk for hk in by_rate[worse] if qualifies(hk))
        return winners

    return []
