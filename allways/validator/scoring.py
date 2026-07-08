"""Crown-time scoring pipeline.

Reward per miner is ``eligible × [w_a·crown + w_b·quality_volume]`` (B3.5),
where ``eligible`` is a flat 0/1 gate read off the on-chain ``MinerState``
counters (B3.3) — it replaces the old ``sr³ × credibility ramp``. The crown
component is ``pool × crown_share × capacity × volume_factor``; the
quality-volume component is the pool's realized-volume share (sourced from the
on-chain ``MinerDirectionStats`` accounts) gated by the ``rate_quality`` curve.
The curve compares a miner's realized rate against an on-chain reference (C-rev):
a trimmed, volume-weighted, per-miner-capped average of completed-swap clearing
rates per direction (``build_direction_references``), computed deterministically
from ingested events — no external feed. ``w_a=0.8 / w_b=0.2``; a direction with
too-thin in-window history makes ``rate_quality`` neutral (1.0), so ``w_b`` still
pays realized volume by raw share rather than zeroing anyone. Any shortfall
recycles to ``RECYCLE_UID``. Entry is ``score_and_reward_miners``.
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
from allways.chains import canonical_pair, get_chain
from allways.classes import ActivityTransition, MinerActivity, next_activity
from allways.constants import (
    DIRECTION_POOLS,
    MAX_FAILED_SWAPS,
    MAX_SCORING_BACKFILL_SECS,
    MIN_SUCCESSFUL_SWAPS,
    RATE_QUALITY_FLOOR_ADV,
    RATE_QUALITY_MIN,
    RATE_QUALITY_TOLERANCE_BPS,
    RATE_REFERENCE_MIN_SWAPS,
    RATE_REFERENCE_MINER_CAP_FRAC,
    RATE_REFERENCE_TRIM_FRAC,
    RATE_REFERENCE_WINDOW_SECS,
    RECYCLE_UID,
    REWARD_MINER_STATES,
    REWARD_WEIGHT_CROWN,
    REWARD_WEIGHT_QUALITY_VOLUME,
    SCORING_WINDOW_BLOCKS,
    SCORING_WINDOW_SECS,
    SWAP_OUTCOME_RETENTION_SECS,
    VOLUME_WEIGHT_ALPHA,
)
from allways.utils.rate import is_executable_rate, min_executable_sol_leg
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
    # clearing_rates feeds the rate-quality reference over a wider (24h) window,
    # so it has its own, later horizon — pruning it at the 1h crown cutoff would
    # starve the reference.
    self.state_store.prune_clearing_rates(current_time - RATE_REFERENCE_WINDOW_SECS)
    self.state_store.prune_swap_outcomes(current_time - SWAP_OUTCOME_RETENTION_SECS)


def is_eligible(miner_state) -> bool:
    """Flat binary crown gate off the on-chain ``MinerState`` counters (B3.3):
    eligible iff the miner has at least ``MIN_SUCCESSFUL_SWAPS`` successes and at
    most ``MAX_FAILED_SWAPS`` failures. Replaces ``success_rate³ × credibility``."""
    return (
        int(miner_state.successful_swaps) >= MIN_SUCCESSFUL_SWAPS and int(miner_state.failed_swaps) <= MAX_FAILED_SWAPS
    )


def build_eligibility(solana_client, metagraph, attribution: Optional[Dict[str, str]] = None) -> Dict[str, bool]:
    """``{hotkey: eligible_bool}`` for on-metagraph miners, from the on-chain
    ``MinerState`` counters. Each pubkey-keyed ``MinerState`` is attributed to a
    Bittensor hotkey via the sr25519 binding (B3.2 ``build_attribution``);
    unbound or off-metagraph miners are dropped (they have no UID to credit).
    Pass ``attribution`` to reuse one per-round binding snapshot."""
    if attribution is None:
        attribution = build_attribution(solana_client)
    metagraph_hotkeys = set(metagraph.hotkeys)
    eligibility: Dict[str, bool] = {}
    for _pubkey, ms in solana_client.get_all('MinerState'):
        hotkey = attribution.get(str(ms.miner))
        if hotkey is None or hotkey not in metagraph_hotkeys:
            continue
        eligibility[hotkey] = is_eligible(ms)
    return eligibility


def realized_vwap(total_to_amount: int, total_from_amount: int) -> float:
    """Realized volume-weighted average rate for a direction:
    ``total_to_amount / total_from_amount`` over all confirmed swaps. The legs
    are accumulated as exact integers on-chain (``MinerDirectionStats``); the
    only float is this final ratio. Zero from-volume ⇒ 0.0 (no executed swaps to
    average — guards divide-by-zero). Phase-C feeds this into the
    ``rate_quality`` curve."""
    to_amt = int(total_to_amount)
    from_amt = int(total_from_amount)
    if from_amt <= 0:
        return 0.0
    return to_amt / from_amt


@dataclass
class DirectionVolume:
    """A miner's realized per-direction track record, read off the on-chain
    ``MinerDirectionStats`` ledger (asset-native units). ``from_amount`` is the
    volume the ``volume_factor`` participation weighting compares within a
    direction; ``vwap`` is the realized executed rate the Phase-C quality curve
    will consume."""

    from_amount: int = 0
    to_amount: int = 0

    @property
    def vwap(self) -> float:
        return realized_vwap(self.to_amount, self.from_amount)


def build_direction_volumes(
    solana_client, metagraph, attribution: Optional[Dict[str, str]] = None
) -> Dict[str, Dict[Tuple[str, str], DirectionVolume]]:
    """``{hotkey: {(from_chain, to_chain): DirectionVolume}}`` — realized
    per-direction volume read off the on-chain ``MinerDirectionStats`` accounts
    (B3.5), replacing the per-validator ``swap_outcomes`` ledger (the #1
    cross-validator divergence source). pubkey→hotkey via the sr25519 binding
    (B3.2 ``build_attribution``); unbound or off-metagraph miners are dropped
    (no UID to credit). The PDA is one row per (miner, from, to), so amounts
    accumulate defensively in case attribution ever folds two pubkeys onto one
    hotkey. Pass ``attribution`` to reuse one per-round binding snapshot."""
    if attribution is None:
        attribution = build_attribution(solana_client)
    metagraph_hotkeys = set(metagraph.hotkeys)
    volumes: Dict[str, Dict[Tuple[str, str], DirectionVolume]] = {}
    for _pubkey, ds in solana_client.get_all('MinerDirectionStats'):
        hotkey = attribution.get(str(ds.miner))
        if hotkey is None or hotkey not in metagraph_hotkeys:
            continue
        direction = ((ds.from_chain or '').lower(), (ds.to_chain or '').lower())
        dv = volumes.setdefault(hotkey, {}).setdefault(direction, DirectionVolume())
        dv.from_amount += int(ds.total_from_amount or 0)
        dv.to_amount += int(ds.total_to_amount or 0)
    return volumes


def _canonical_rate_and_weight(from_chain: str, to_chain: str, from_amount: int, to_amount: int) -> Tuple[float, int]:
    """Canonical 'dest per source' rate (TAO per BTC for the btc/tao pair) plus
    the swap's volume weight = the **canonical-source** native leg. Weighting a
    set of per-swap canonical rates by this leg makes the weighted mean equal the
    aggregate VWAP in *either* direction (the source leg is btc's native amount
    for both btc→tao and tao→btc). Non-positive legs ⇒ ``(0.0, 0)``."""
    canon_source, canon_dest = canonical_pair(from_chain, to_chain)
    native = {from_chain: int(from_amount), to_chain: int(to_amount)}
    src_native = native.get(canon_source, 0)
    dst_native = native.get(canon_dest, 0)
    if src_native <= 0 or dst_native <= 0:
        return 0.0, 0
    src_disp = src_native / (10 ** get_chain(canon_source).decimals)
    dst_disp = dst_native / (10 ** get_chain(canon_dest).decimals)
    return dst_disp / src_disp, src_native


def trimmed_reference(
    samples: List[Tuple[str, float, float]],
    trim_frac: float = RATE_REFERENCE_TRIM_FRAC,
    cap_frac: float = RATE_REFERENCE_MINER_CAP_FRAC,
    min_swaps: int = RATE_REFERENCE_MIN_SWAPS,
) -> Optional[float]:
    """Deterministic per-direction reference from completed-swap clearing rates
    (C-rev). ``samples`` is ``[(hotkey, rate, weight)]``.

    Pipeline: (1) fewer than ``min_swaps`` positive-weight samples ⇒ ``None``;
    (2) **per-miner cap** — scale any miner whose summed weight exceeds
    ``cap_frac × total`` down to the cap (preserves its rate spread, limits its
    pull, so a wash farmer's self-swaps can't dominate); (3) **weighted trim** —
    sort by ``(rate, hotkey)`` and drop ``trim_frac`` of the (capped) weight from
    each tail, with partial inclusion at the boundary samples; (4) weighted mean
    of the survivors. Pure float ops in a fixed sorted order ⇒ identical across
    validators given identical samples."""
    pos = [(hk, float(r), float(w)) for hk, r, w in samples if r > 0 and w > 0]
    if len(pos) < min_swaps:
        return None
    total_weight = sum(w for _, _, w in pos)
    if total_weight <= 0:
        return None

    # Per-miner cap: hold each miner's total influence to cap_frac of the pool.
    cap = cap_frac * total_weight
    miner_weight: Dict[str, float] = {}
    for hk, _, w in pos:
        miner_weight[hk] = miner_weight.get(hk, 0.0) + w
    scale = {hk: (cap / mw if mw > cap else 1.0) for hk, mw in miner_weight.items()}
    capped = sorted(((hk, r, w * scale[hk]) for hk, r, w in pos), key=lambda s: (s[1], s[0]))
    total_capped = sum(w for _, _, w in capped)
    if total_capped <= 0:
        return None

    # Weighted trim: keep the central (1 - 2·trim_frac) of the weight by rate.
    lo_cut = trim_frac * total_capped
    hi_cut = (1.0 - trim_frac) * total_capped
    cum = 0.0
    num = 0.0
    den = 0.0
    for _hk, r, w in capped:
        seg_start, seg_end = cum, cum + w
        cum = seg_end
        included = min(seg_end, hi_cut) - max(seg_start, lo_cut)
        if included <= 0:
            continue
        num += r * included
        den += included
    if den <= 0:
        return None
    return num / den


@dataclass
class DirectionReference:
    """Per-direction rate-quality reference (C-rev). ``reference`` is the trimmed
    volume-weighted clearing rate (None when in-window history is too thin);
    ``miner_rates`` is each miner's own windowed realized VWAP — the numerator the
    quality curve compares against the reference, on the same windowed basis."""

    reference: Optional[float] = None
    miner_rates: Dict[str, float] = field(default_factory=dict)


def build_direction_references(state_store, current_time: int) -> Dict[Tuple[str, str], DirectionReference]:
    """``{(from,to): DirectionReference}`` for every scored direction, computed
    purely from the on-chain ``clearing_rates`` history over
    ``[current_time − RATE_REFERENCE_WINDOW_SECS, current_time]``. One query per
    direction; the trimmed reference and each miner's windowed VWAP are both
    derived from the same rows, so numerator and reference share a basis."""
    window_start = current_time - RATE_REFERENCE_WINDOW_SECS
    references: Dict[Tuple[str, str], DirectionReference] = {}
    for from_chain, to_chain in DIRECTION_POOLS:
        rows = state_store.get_clearing_rates_in_range(from_chain, to_chain, window_start, current_time)
        samples: List[Tuple[str, float, float]] = []
        miner_num: Dict[str, float] = {}
        miner_den: Dict[str, float] = {}
        for row in rows:
            rate, weight = _canonical_rate_and_weight(from_chain, to_chain, row['from_amount'], row['to_amount'])
            if rate <= 0 or weight <= 0:
                continue
            hk = row['hotkey']
            samples.append((hk, rate, float(weight)))
            miner_num[hk] = miner_num.get(hk, 0.0) + rate * weight
            miner_den[hk] = miner_den.get(hk, 0.0) + weight
        miner_rates = {hk: miner_num[hk] / miner_den[hk] for hk in miner_num if miner_den[hk] > 0}
        references[(from_chain, to_chain)] = DirectionReference(
            reference=trimmed_reference(samples), miner_rates=miner_rates
        )
    return references


def rate_advantage(from_chain: str, to_chain: str, realized_rate: float, reference_rate: float) -> float:
    """Taker-oriented relative advantage of a realized rate vs the reference,
    signed so positive = better-than-reference for the swapper. Direction-aware:
    in the canonical direction (btc→tao) a higher TAO/BTC is better; in the
    reverse (tao→btc) a lower TAO/BTC is better — the same orientation the crown
    uses (``lower_rate_wins``)."""
    if reference_rate <= 0:
        return 0.0
    canon_source, _ = canonical_pair(from_chain, to_chain)
    higher_is_better = from_chain == canon_source
    if higher_is_better:
        return (realized_rate - reference_rate) / reference_rate
    return (reference_rate - realized_rate) / reference_rate


def quality_curve(advantage: float) -> float:
    """One-sided clamp: 1.0 at/above market (within the tolerance deadband),
    ramping linearly down to ``RATE_QUALITY_MIN`` at ``RATE_QUALITY_FLOOR_ADV``.
    Above-market is capped at 1.0 — the crown already rewards best-rate presence,
    so paying it again here would double-reward rate and invite wash-trade
    farming at fake-good quotes."""
    tol = RATE_QUALITY_TOLERANCE_BPS / 10_000.0
    if advantage >= -tol:
        return 1.0
    if advantage <= RATE_QUALITY_FLOOR_ADV:
        return RATE_QUALITY_MIN
    # Linear interpolation from 1.0 (at -tol) to RATE_QUALITY_MIN (at FLOOR_ADV).
    frac = (advantage + tol) / (RATE_QUALITY_FLOOR_ADV + tol)
    return 1.0 + frac * (RATE_QUALITY_MIN - 1.0)


def rate_quality(
    from_chain: str,
    to_chain: str,
    realized_rate: float,
    reference_rate: Optional[float] = None,
) -> float:
    """Quality multiplier for a miner's realized volume in a direction: how good
    its realized rate was versus the on-chain reference (C-rev). Both numbers are
    realized clearing rates over the same window (``build_direction_references``),
    so they share a basis. A ``reference_rate`` of None/≤0 (in-window history too
    thin) ⇒ 1.0 neutral — a missing reference never zeroes everyone or hands out
    free reward, w_b then pays realized volume by raw share. No realized rate for
    this miner in-window ⇒ 1.0 (nothing to judge)."""
    if reference_rate is None or reference_rate <= 0:
        return 1.0
    if realized_rate <= 0:
        return 1.0
    return quality_curve(rate_advantage(from_chain, to_chain, realized_rate, reference_rate))


def calculate_miner_rewards(self: Validator, current_time: int) -> Tuple[np.ndarray, Set[int]]:
    """Replay the crown-time event stream, derive per-miner rewards
    (eligible × [w_a·crown + w_b·quality_volume]), recycle the rest.
    ``current_time`` is the unix-seconds window_end (the crown axis).

    Volume weighting is *per direction*: a miner earning crown on btc→tao is
    compared only to btc→tao volume on the network, not to the total of both
    directions. Otherwise heavy tao→btc flow from other miners would dilute
    a btc→tao earner's vol_share even though they own that direction."""
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
    unweighted_rewards = np.zeros(n_uids, dtype=np.float32)
    # One per-round sr25519 binding snapshot, shared by both reads (each re-verifies every binding).
    attribution = build_attribution(self.solana_client)
    # Flat eligibility gate off the on-chain MinerState counters (B3.3),
    # attributed pubkey→hotkey via the sr25519 binding. Absent hotkey → not
    # eligible (no on-chain counters ⇒ no proven successful swaps).
    eligibility = build_eligibility(self.solana_client, self.metagraph, attribution)
    # Per-direction realized volume re-sourced from the on-chain
    # MinerDirectionStats accounts (B3.5), replacing the per-validator
    # swap_outcomes ledger. Built once, sliced per direction below.
    direction_volumes = build_direction_volumes(self.solana_client, self.metagraph, attribution)
    # On-chain rate-quality reference (C-rev): trimmed volume-weighted clearing
    # rate per direction + each miner's windowed realized VWAP, built once per
    # round from the ingested clearing_rates history. Deterministic — no feed, no
    # network. A direction with too-thin history yields reference None ⇒ neutral.
    references = build_direction_references(self.state_store, current_time)

    direction_traces: Dict[Tuple[str, str], DirectionTrace] = {}
    weighting_traces: Dict[str, WeightingTrace] = {}
    # Captured only when dashboard storage is enabled — the tee inside
    # replay_crown_time_window is a no-op when intervals_out is None, so
    # disabled validators pay zero cost here.
    storage_enabled = self.database_storage.is_enabled()
    intervals_by_dir: Dict[Tuple[str, str], List[Tuple[int, int, List[str], float]]] = {}
    try:
        max_swap_amount = int(self.solana_config_cache.max_swap_amount())
    except Exception as e:
        bt.logging.warning(f'max_swap_amount read failed: {e}')
        max_swap_amount = 0
    try:
        min_swap_amount = int(self.solana_config_cache.min_swap_amount())
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
        intervals: Optional[List[Tuple[int, int, List[str], float]]] = None
        if storage_enabled:
            intervals = []
            intervals_by_dir[(from_chain, to_chain)] = intervals
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
            min_swap_lamports=min_swap_amount,
            max_swap_lamports=max_swap_amount,
        )
        total_crown_dir = sum(crown_time.values())
        vols_dir: Dict[str, DirectionVolume] = {
            hk: dirs[(from_chain, to_chain)] for hk, dirs in direction_volumes.items() if (from_chain, to_chain) in dirs
        }
        volumes_dir = {hk: dv.from_amount for hk, dv in vols_dir.items()}
        total_volume_dir = sum(volumes_dir.values())
        for hk, v in volumes_dir.items():
            miner_volume_total[hk] = miner_volume_total.get(hk, 0) + int(v)
        network_volume_total += int(total_volume_dir)
        for hk, secs in crown_time.items():
            miner_crown_total[hk] = miner_crown_total.get(hk, 0.0) + secs
        network_crown_total += total_crown_dir

        # W_B falls back to crown for a direction with no realized volume: the
        # quality-volume slice has nothing to distribute, so handing it to crown
        # keeps a quiet direction at full pool rather than recycling 20% (Phase C).
        if total_volume_dir > 0:
            w_a, w_b = REWARD_WEIGHT_CROWN, REWARD_WEIGHT_QUALITY_VOLUME
        else:
            w_a, w_b = 1.0, 0.0

        bt.logging.debug(
            f'V1 scoring [{from_chain}→{to_chain}]: '
            f'total_crown={total_crown_dir:.1f}s, total_volume_rao={total_volume_dir}'
        )

        if total_crown_dir == 0:
            continue  # empty bucket — pool recycles via the remainder below

        for hotkey, secs in crown_time.items():
            uid = hotkey_to_uid.get(hotkey)
            if uid is None:
                continue  # dereg'd mid-window; credit forfeited
            # Capacity is integrated over time during the replay, so the
            # effective multiplier is the time-weighted average over the
            # miner's crown intervals. Reading current collateral here
            # would let a post-window top-up retroactively boost credit
            # already earned (#409).
            cap_secs = trace.cap_weighted_time.get(hotkey, 0.0)
            cap = (cap_secs / secs) if secs > 0 else 0.0
            eligible = 1.0 if eligibility.get(hotkey, False) else 0.0
            wt = weighting_traces.setdefault(hotkey, WeightingTrace())
            wt.record_capacity(factor=cap)
            wt.record_eligibility(eligible=bool(eligible))
            crown_share_dir = secs / total_crown_dir
            vol_dir = volumes_dir.get(hotkey, 0)
            vol_share_dir = (vol_dir / total_volume_dir) if total_volume_dir > 0 else 0.0
            vol_factor = volume_factor(vol_dir, total_volume_dir, crown_share_dir)
            # Reward = eligible × [w_a·crown + w_b·quality_volume] (Phase C). crown
            # is the B3.3 reward (pool·share·cap·vol_factor); quality_volume is the
            # pool's volume share × rate-quality (vs the live market). w_a/w_b are
            # the direction's effective weights (0.8/0.2, or 1.0/0.0 if idle).
            crown_component = pool * crown_share_dir * cap * vol_factor
            # Quality compares the miner's OWN windowed realized rate against the
            # direction reference — same windowed clearing-rate basis (C-rev).
            ref = references.get((from_chain, to_chain))
            reference_rate = ref.reference if ref is not None else None
            realized_rate = ref.miner_rates.get(hotkey, 0.0) if ref is not None else 0.0
            quality_volume_component = (
                pool * vol_share_dir * rate_quality(from_chain, to_chain, realized_rate, reference_rate)
            )
            base = eligible * (w_a * crown_component + w_b * quality_volume_component)
            # Trace baseline is the pre-volume crown reward (dashboard attributes
            # the volume penalty off the gap to the final weighted reward).
            unweighted_rewards[uid] += eligible * w_a * pool * crown_share_dir * cap
            rewards[uid] += base
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
        eligibility=eligibility,
        distributed=distributed,
        recycled=recycled,
        weighting_traces=weighting_traces,
        min_swap_lamports=min_swap_amount,
        max_swap_lamports=max_swap_amount,
    )

    if storage_enabled:
        # Persist the crown intervals directly (no per-tick expansion).
        # `flush_scoring_window` deletes [window_start, window_end) per
        # direction before upserting, so the wipe matches the data exactly.
        crown_rows_by_dir = {d: intervals_to_crown_rows(ivs, d[0], d[1]) for d, ivs in intervals_by_dir.items()}
        rate_rows: List[Tuple[str, str, str, float, int]] = []
        for from_chain, to_chain in DIRECTION_POOLS:
            for e in self.state_store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end):
                # e['block'] is the event's unix blockTime (see state_store).
                rate_rows.append((e['hotkey'], from_chain, to_chain, float(e['rate']), e['block']))
        # Crown intervals tile [window_start, window_end); the last one ends at
        # window_end, so the freshness cursor is "as of window_end" (unix secs).
        cursor_ts = max(0, window_end)
        self.database_storage.flush_scoring_window(
            rate_rows=rate_rows,
            crown_rows_by_direction=crown_rows_by_dir,
            crown_window_bounds_by_direction={d: (window_start, window_end) for d in DIRECTION_POOLS},
            rate_snapshot_max_ts=cursor_ts,
            crown_holders_max_ts=cursor_ts,
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
    ``ActivityTransition`` value for ACTIVITY."""

    block: int
    hotkey: str
    kind: EventKind
    value: float

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
) -> Tuple[Dict[str, float], Dict[str, MinerActivity], Set[str], Dict[str, int]]:
    """Snapshot rates, per-miner activity, active set, and posted collateral as
    they stood at window_start. ``activity`` holds only non-AVAILABLE miners (a
    reservation/swap open before the window shows RESERVED/FULFILLING at the
    edge); absent hotkeys default to AVAILABLE. Rate read is one batched query
    per direction (N rewardable hotkeys would otherwise be N point lookups, runs
    every forward step from snapshot_current_crown_holders)."""
    activity: Dict[str, MinerActivity] = dict(event_index.get_activity_state_at(window_start))
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
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVITY, value=float(e['kind']))
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


def crown_can_fund(hotkey, rate, from_chain, to_chain, min_swap_lamports, max_swap_lamports, collaterals):
    """Boundary-squat gate: a miner whose own rate forces a SOL leg larger than
    their collateral earns no crown (collateral and the bounded leg are both SOL).
    Fail open on unknown collateral (absent != zero) so a missing baseline doesn't
    silently drop them."""
    if hotkey not in collaterals:
        return True
    min_leg = min_executable_sol_leg(rate, from_chain, to_chain, min_swap_lamports, max_swap_lamports)
    return min_leg == 0 or collaterals[hotkey] >= min_leg


def make_crown_predicates(from_chain, to_chain, min_swap_lamports, max_swap_lamports, collaterals):
    """Crown-eligibility predicates ``(executable_check, can_fund)`` shared by the
    scoring replay and the live snapshot, so the live crown view can never diverge
    from the rewarded ledger. Both are the shared rate utils with this direction's
    bounds/collateral bound in."""
    executable_check = partial(
        is_executable_rate,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_lamports=min_swap_lamports,
        max_swap_lamports=max_swap_lamports,
    )
    can_fund = partial(
        crown_can_fund,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_lamports=min_swap_lamports,
        max_swap_lamports=max_swap_lamports,
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
    intervals_out: Optional[List[Tuple[int, int, List[str], float]]] = None,
    min_swap_lamports: int = 0,
    max_swap_lamports: int = 0,
) -> Dict[str, float]:
    """Walk the merged event stream, return ``{hotkey: crown_seconds_float}``.
    Ties at the same rate split credit evenly. A miner qualifies for crown
    at an instant iff they are on the current metagraph, were active at
    that instant, in a rewardable activity state (∈ REWARD_MINER_STATES — a
    reserved/fulfilling miner forfeits), had a positive rate posted, and their
    rate is executable under the current contract swap bounds. Active/rate/
    activity/collateral are evaluated per-block via the replay — a miner's status at
    scoring time is irrelevant other than metagraph membership (used to
    credit the UID). The collateral-floor activation gate is still trusted
    to the contract's active flag; halt state is handled at
    ``score_and_reward_miners`` entry.

    Bounds at 0 disable the executability filter (matches the contract's
    "unset" sentinel); the rate-positive floor still applies.

    When ``trace`` is supplied, ``trace.cap_weighted_time`` is populated
    alongside ``trace.crown_time``. The weighted series multiplies each
    interval's split by ``capacity_factor(collateral_at_block, max_swap_lamports)``
    so a post-window collateral boost cannot retroactively scale credit
    already earned (closes #409)."""
    rates, activity, active_set, collaterals = reconstruct_window_start_state(
        store, event_index, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_index, from_chain, to_chain, window_start, window_end)

    # Rates are stored as canonical_dest per canonical_source (TAO per BTC).
    # In the canonical direction (btc→tao) higher = better; in the reverse
    # direction (tao→btc) lower = better.
    canon_from, _ = canonical_pair(from_chain, to_chain)
    lower_rate_wins = from_chain != canon_from

    executable_check, can_fund = make_crown_predicates(
        from_chain, to_chain, min_swap_lamports, max_swap_lamports, collaterals
    )

    crown_time: Dict[str, float] = {}
    cap_weighted_time: Dict[str, float] = {}
    prev_ts = window_start

    bounds_set = min_swap_lamports > 0 or max_swap_lamports > 0

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        rewardable_by_state = {
            hk for hk in rewardable_hotkeys if activity.get(hk, MinerActivity.AVAILABLE) in REWARD_MINER_STATES
        }
        rates_for_instant = rates
        holders = crown_holders_at_instant(
            rates_for_instant,
            rewardable_hotkeys,
            rewardable_by_state=rewardable_by_state,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set else None,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_time += duration
            return
        winner_rate = rates_for_instant.get(holders[0], 0.0)
        if trace is not None and winner_rate > 0:
            trace.best_rate = winner_rate
        if intervals_out is not None:
            intervals_out.append((interval_start, interval_end, list(holders), winner_rate))
        split = duration / len(holders)
        for hk in holders:
            crown_time[hk] = crown_time.get(hk, 0.0) + split
            # Unknown collateral (no event recorded) → capacity 1.0, matching
            # can_fund's fail-open. Only a known value scales capacity down.
            cap = capacity_factor(collaterals[hk], max_swap_lamports) if hk in collaterals else 1.0
            cap_weighted_time[hk] = cap_weighted_time.get(hk, 0.0) + split * cap

    def apply_event(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.ACTIVITY:
            cur = activity.get(event.hotkey, MinerActivity.AVAILABLE)
            nxt = next_activity(cur, ActivityTransition(int(event.value)))
            if nxt is None:
                warn_unexpected_activity(cur, ActivityTransition(int(event.value)))
                nxt = cur
            if nxt is MinerActivity.AVAILABLE:
                activity.pop(event.hotkey, None)
            else:
                activity[event.hotkey] = nxt
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
    # TTL cache (no per-step RPC); both-0 on failure = permissive, as before.
    try:
        min_swap_amount = int(self.solana_config_cache.min_swap_amount())
        max_swap_amount = int(self.solana_config_cache.max_swap_amount())
    except Exception as e:
        bt.logging.warning(f'swap-bounds read failed in live snapshot: {e}')
        min_swap_amount = max_swap_amount = 0
    rows_by_direction: Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]] = {}
    bounds_set = min_swap_amount > 0 or max_swap_amount > 0
    for from_chain, to_chain in DIRECTION_POOLS:
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
        rewardable_by_state = {
            hk for hk in rewardable_hotkeys if activity.get(hk, MinerActivity.AVAILABLE) in REWARD_MINER_STATES
        }

        # Same predicates the scoring replay uses, so the live table never
        # credits a holder the ledger drops. Built per direction so each
        # closure captures the right chain pair.
        executable_check, can_fund = make_crown_predicates(
            from_chain, to_chain, min_swap_amount, max_swap_amount, collaterals
        )

        holders = crown_holders_at_instant(
            rates,
            rewardable_hotkeys,
            rewardable_by_state=rewardable_by_state,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set else None,
        )
        if holders:
            share = 1.0 / len(holders)
            rate = rates.get(holders[0], 0.0)
            rows_by_direction[(from_chain, to_chain)] = [(from_chain, to_chain, hk, share, rate, ts) for hk in holders]
        else:
            rows_by_direction[(from_chain, to_chain)] = []
    return rows_by_direction


def intervals_to_crown_rows(
    intervals: List[Tuple[int, int, List[str], float]],
    from_chain: str,
    to_chain: str,
) -> List[Tuple[int, int, str, str, str, float, float]]:
    """Convert uniform-state crown intervals to crown_holders rows.

    For each (started_at, ended_at, holders, rate) emit one row per holder
    with credit = 1/len(holders), so the per-interval per-direction credits
    sum to 1.0 — the validator's fair-tie semantics. A holder's crown *time*
    over a window is then ``SUM((ended_at - started_at) * credit)``, matching
    the duration the scoring replay already integrates (no per-tick expansion)."""
    rows: List[Tuple[int, int, str, str, str, float, float]] = []
    for lo, hi, holders, rate in intervals:
        if not holders or hi <= lo:
            continue
        share = 1.0 / len(holders)
        for hotkey in holders:
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
) -> List[str]:
    """Take the miners posting the best rate, but only if they satisfy every
    other condition (rewardable, active, activity ∈ REWARD_MINER_STATES, rate >
    0, executable). If the best rate has no qualified miner, fall through to the
    next-best rate.

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

    for rate in sorted(by_rate, reverse=not lower_rate_wins):
        winners = [hk for hk in by_rate[rate] if qualifies(hk)]
        if winners:
            return winners

    return []
