"""Crown-time scoring pipeline.

Reward per miner is ``eligible × [w_a·crown + w_b·quality_volume]`` (B3.5),
where ``eligible`` is a flat 0/1 gate read off the on-chain ``MinerState``
counters (B3.3) — it replaces the old ``sr³ × credibility ramp``. The crown
component is ``pool × crown_share × capacity × volume_factor``; the
quality-volume component is the pool's realized-volume share (sourced from the
on-chain ``MinerDirectionStats`` accounts) gated by the rate-vs-market
``rate_quality`` curve. Phase C set ``w_a=0.8 / w_b=0.2`` and wired the
off-chain market-rate feed; a stale feed makes ``rate_quality`` neutral (1.0),
so ``w_b`` still pays realized volume by raw share rather than zeroing anyone.
Any shortfall recycles to ``RECYCLE_UID``. Entry is ``score_and_reward_miners``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import IntEnum
from functools import partial
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chains import canonical_pair, get_chain
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
    REWARD_WEIGHT_CROWN,
    REWARD_WEIGHT_QUALITY_VOLUME,
    SCORING_WINDOW_BLOCKS,
    SCORING_WINDOW_SECS,
    VOLUME_WEIGHT_ALPHA,
)
from allways.utils.rate import is_executable_rate, min_executable_tao_leg
from allways.validator.binding import build_attribution
from allways.validator.scoring_trace import WeightingTrace, log_scoring_trace
from allways.validator.state_store import ValidatorStateStore

if TYPE_CHECKING:
    from allways.validator.event_index import SolanaEventIndex
    from neurons.validator import Validator


@dataclass
class DirectionTrace:
    pool: float = 0.0
    crown_blocks: Dict[str, float] = field(default_factory=dict)
    cap_weighted_blocks: Dict[str, float] = field(default_factory=dict)
    unfilled_blocks: int = 0
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
        max_block=window_end - 1,
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


def fetch_market_rate(self: Validator) -> Optional[float]:
    """Best-effort live TAO/BTC for the rate-quality curve (Phase C). A missing
    feed or any fetch failure returns None, which ``rate_quality`` treats as
    neutral (1.0) — a dead feed must not zero rewards (matches
    ``contract_is_halted``'s fail-open posture)."""
    feed = getattr(self, 'market_rate_feed', None)
    if feed is None:
        return None
    try:
        return feed.tao_per_btc()
    except Exception as e:
        bt.logging.warning(f'market-rate fetch failed, quality neutral: {e}')
        return None


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
    this also takes over the active/busy/collateral pruning the deleted
    substrate event_watcher used to own (each preserves a per-hotkey anchor)."""
    cutoff = current_time - SCORING_WINDOW_SECS
    if cutoff <= 0:
        return
    self.state_store.prune_events_older_than(cutoff)
    self.state_store.prune_active_events(cutoff)
    self.state_store.prune_busy_events(cutoff)
    self.state_store.prune_collateral_events(cutoff)


def is_eligible(miner_state) -> bool:
    """Flat binary crown gate off the on-chain ``MinerState`` counters (B3.3):
    eligible iff the miner has at least ``MIN_SUCCESSFUL_SWAPS`` successes and at
    most ``MAX_FAILED_SWAPS`` failures. Replaces ``success_rate³ × credibility``."""
    return (
        int(miner_state.successful_swaps) >= MIN_SUCCESSFUL_SWAPS
        and int(miner_state.failed_swaps) <= MAX_FAILED_SWAPS
    )


def build_eligibility(solana_client, metagraph) -> Dict[str, bool]:
    """``{hotkey: eligible_bool}`` for on-metagraph miners, from the on-chain
    ``MinerState`` counters. Each pubkey-keyed ``MinerState`` is attributed to a
    Bittensor hotkey via the sr25519 binding (B3.2 ``build_attribution``);
    unbound or off-metagraph miners are dropped (they have no UID to credit)."""
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


def build_direction_volumes(solana_client, metagraph) -> Dict[str, Dict[Tuple[str, str], DirectionVolume]]:
    """``{hotkey: {(from_chain, to_chain): DirectionVolume}}`` — realized
    per-direction volume read off the on-chain ``MinerDirectionStats`` accounts
    (B3.5), replacing the per-validator ``swap_outcomes`` ledger (the #1
    cross-validator divergence source). pubkey→hotkey via the sr25519 binding
    (B3.2 ``build_attribution``); unbound or off-metagraph miners are dropped
    (no UID to credit). The PDA is one row per (miner, from, to), so amounts
    accumulate defensively in case attribution ever folds two pubkeys onto one
    hotkey."""
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


def _canonical_rate_and_weight(
    from_chain: str, to_chain: str, from_amount: int, to_amount: int
) -> Tuple[float, int]:
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


def realized_canonical_rate(from_chain: str, to_chain: str, from_amount: int, to_amount: int) -> float:
    """Realized executed rate in canonical 'dest per source' display units (TAO
    per BTC for the btc/tao pair), derived from native-unit leg totals.
    Orientation matches the crown's stored rates, so it's directly comparable to
    the on-chain reference. Non-positive legs ⇒ 0.0 (no executed volume to price)."""
    rate, _weight = _canonical_rate_and_weight(from_chain, to_chain, from_amount, to_amount)
    return rate


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
            rate, weight = _canonical_rate_and_weight(
                from_chain, to_chain, row['from_amount'], row['to_amount']
            )
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


def rate_advantage(from_chain: str, to_chain: str, realized_rate: float, market_rate: float) -> float:
    """Taker-oriented relative advantage of a realized rate vs market, signed so
    positive = better-than-market for the swapper. Direction-aware: in the
    canonical direction (btc→tao) a higher TAO/BTC is better; in the reverse
    (tao→btc) a lower TAO/BTC is better — the same orientation the crown uses
    (``lower_rate_wins``)."""
    if market_rate <= 0:
        return 0.0
    canon_source, _ = canonical_pair(from_chain, to_chain)
    higher_is_better = from_chain == canon_source
    if higher_is_better:
        return (realized_rate - market_rate) / market_rate
    return (market_rate - realized_rate) / market_rate


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
    from_amount: int,
    to_amount: int,
    market_rate: Optional[float] = None,
) -> float:
    """Quality multiplier for a miner's realized volume in a direction: how good
    the executed rate was versus the live market (Phase C). ``market_rate``
    None/≤0 (feed stale or unwired) ⇒ 1.0 neutral, so a dead feed never zeroes
    everyone or hands out free reward. No executed volume to price ⇒ 1.0."""
    if market_rate is None or market_rate <= 0:
        return 1.0
    realized = realized_canonical_rate(from_chain, to_chain, from_amount, to_amount)
    if realized <= 0:
        return 1.0
    return quality_curve(rate_advantage(from_chain, to_chain, realized, market_rate))


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
    # Active, rate, and busy are all evaluated per-block via event replay
    # inside replay_crown_time_window. Collateral-floor invariants are
    # trusted to the contract's active flag.
    rewardable_hotkeys: Set[str] = set(self.metagraph.hotkeys)
    hotkey_to_uid: Dict[str, int] = {self.metagraph.hotkeys[uid]: uid for uid in range(n_uids)}

    rewards = np.zeros(n_uids, dtype=np.float32)
    unweighted_rewards = np.zeros(n_uids, dtype=np.float32)
    # Flat eligibility gate off the on-chain MinerState counters (B3.3),
    # attributed pubkey→hotkey via the sr25519 binding. Absent hotkey → not
    # eligible (no on-chain counters ⇒ no proven successful swaps).
    eligibility = build_eligibility(self.solana_client, self.metagraph)
    # Per-direction realized volume re-sourced from the on-chain
    # MinerDirectionStats accounts (B3.5), replacing the per-validator
    # swap_outcomes ledger. Built once, sliced per direction below.
    direction_volumes = build_direction_volumes(self.solana_client, self.metagraph)
    # Live market rate for the rate-quality curve (Phase C), fetched once per
    # round. None (no feed / stale / failure) keeps quality neutral (1.0).
    market_rate = fetch_market_rate(self)

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
        crown_blocks = replay_crown_time_window(
            store=self.state_store,
            event_index=self.event_index,
            from_chain=from_chain,
            to_chain=to_chain,
            window_start=window_start,
            window_end=window_end,
            rewardable_hotkeys=rewardable_hotkeys,
            trace=trace,
            intervals_out=intervals,
            min_swap_rao=min_swap_amount,
            max_swap_rao=max_swap_amount,
        )
        total_crown_dir = sum(crown_blocks.values())
        vols_dir: Dict[str, DirectionVolume] = {
            hk: dirs[(from_chain, to_chain)]
            for hk, dirs in direction_volumes.items()
            if (from_chain, to_chain) in dirs
        }
        volumes_dir = {hk: dv.from_amount for hk, dv in vols_dir.items()}
        total_volume_dir = sum(volumes_dir.values())
        for hk, v in volumes_dir.items():
            miner_volume_total[hk] = miner_volume_total.get(hk, 0) + int(v)
        network_volume_total += int(total_volume_dir)
        for hk, blk in crown_blocks.items():
            miner_crown_total[hk] = miner_crown_total.get(hk, 0.0) + blk
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
            eligible = 1.0 if eligibility.get(hotkey, False) else 0.0
            wt = weighting_traces.setdefault(hotkey, WeightingTrace())
            wt.record_capacity(factor=cap)
            wt.record_eligibility(eligible=bool(eligible))
            crown_share_dir = blocks / total_crown_dir
            vol_dir = volumes_dir.get(hotkey, 0)
            vol_share_dir = (vol_dir / total_volume_dir) if total_volume_dir > 0 else 0.0
            vol_factor = volume_factor(vol_dir, total_volume_dir, crown_share_dir)
            # Reward = eligible × [w_a·crown + w_b·quality_volume] (Phase C). crown
            # is the B3.3 reward (pool·share·cap·vol_factor); quality_volume is the
            # pool's volume share × rate-quality (vs the live market). w_a/w_b are
            # the direction's effective weights (0.8/0.2, or 1.0/0.0 if idle).
            crown_component = pool * crown_share_dir * cap * vol_factor
            qv = vols_dir.get(hotkey)
            q_from = qv.from_amount if qv is not None else 0
            q_to = qv.to_amount if qv is not None else 0
            quality_volume_component = (
                pool * vol_share_dir * rate_quality(from_chain, to_chain, q_from, q_to, market_rate)
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
        min_swap_rao=min_swap_amount,
        max_swap_rao=max_swap_amount,
    )

    if storage_enabled:
        # Expand uniform-state intervals to per-block crown_holders rows.
        # `flush_scoring_window` deletes [window_start, window_end) per
        # direction before upserting, so the wipe matches the data exactly.
        crown_rows_by_dir = {d: expand_intervals_to_crown_rows(ivs, d[0], d[1]) for d, ivs in intervals_by_dir.items()}
        rate_rows: List[Tuple[str, str, str, float, int]] = []
        for from_chain, to_chain in DIRECTION_POOLS:
            for e in self.state_store.get_rate_events_in_range(from_chain, to_chain, window_start, window_end):
                rate_rows.append((e['hotkey'], from_chain, to_chain, float(e['rate']), e['block']))
        # Range delete + insert covers [window_start, window_end) exclusive
        # of window_end, so the last block actually written is window_end - 1.
        # Cursor advances to that — claiming through window_end would lie
        # to readers about what's flushed.
        cursor_block = max(0, window_end - 1)
        self.database_storage.flush_scoring_window(
            rate_rows=rate_rows,
            crown_rows_by_direction=crown_rows_by_dir,
            crown_window_bounds_by_direction={d: (window_start, window_end) for d in DIRECTION_POOLS},
            rate_snapshot_max_block=cursor_block,
            crown_holders_max_block=cursor_block,
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
    tell-all. Then BUSY (busy ends crown for that miner). RATE comes after so
    a same-instant rate update lands once the qualification gates are set.
    COLLATERAL is last — it's independent of qualification, only scaling an
    interval's credit, so a same-instant post is observable as soon as it
    lands.

    Reservation pins are gone in the Solana model (the swap rate is pinned
    on-chain and a reserved miner is busy-gated out of the crown), so there
    are no RESERVED transitions.
    """

    ACTIVE = 0
    BUSY = 1
    RATE = 2
    COLLATERAL = 3


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
    event_index: SolanaEventIndex,
    from_chain: str,
    to_chain: str,
    window_start: int,
    rewardable_hotkeys: Set[str],
) -> Tuple[Dict[str, float], Dict[str, int], Set[str], Dict[str, int]]:
    """Snapshot rates, busy counts, active set, and posted collateral as they
    stood at window_start. Rate read is one batched query per direction (N
    rewardable hotkeys would otherwise be N point lookups, runs every forward
    step from snapshot_current_crown_holders)."""
    busy_count: Dict[str, int] = dict(event_index.get_busy_miners_at(window_start))
    active_set: Set[str] = set(event_index.get_active_miners_at(window_start))

    all_latest = store.get_latest_rates_before(from_chain, to_chain, window_start)
    rates: Dict[str, float] = {hk: rate_block[0] for hk, rate_block in all_latest.items() if hk in rewardable_hotkeys}

    collaterals: Dict[str, int] = dict(event_index.get_miner_collaterals_at(window_start))

    return rates, busy_count, active_set, collaterals


def merge_replay_events(
    store: ValidatorStateStore,
    event_index: SolanaEventIndex,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
) -> List[ReplayEvent]:
    """Merge in-window active, busy, rate, and collateral transitions into one
    chronologically-sorted stream."""
    events: List[ReplayEvent] = []

    for e in event_index.get_active_events_in_range(window_start, window_end):
        events.append(
            ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.ACTIVE, value=1.0 if e['active'] else 0.0)
        )

    for e in event_index.get_busy_events_in_range(window_start, window_end):
        events.append(ReplayEvent(block=e['block'], hotkey=e['hotkey'], kind=EventKind.BUSY, value=float(e['delta'])))

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


def crown_can_fund(hotkey, rate, from_chain, to_chain, min_swap_rao, max_swap_rao, collaterals):
    """Boundary-squat gate: a miner whose own rate forces a TAO leg larger than
    their collateral earns no crown. Fail open on unknown collateral (absent !=
    zero) so a missing baseline doesn't silently drop them."""
    if hotkey not in collaterals:
        return True
    min_leg = min_executable_tao_leg(rate, from_chain, to_chain, min_swap_rao, max_swap_rao)
    return min_leg == 0 or collaterals[hotkey] >= min_leg


def make_crown_predicates(from_chain, to_chain, min_swap_rao, max_swap_rao, collaterals):
    """Crown-eligibility predicates ``(executable_check, can_fund)`` shared by the
    scoring replay and the live snapshot, so the live crown view can never diverge
    from the rewarded ledger. Both are the shared rate utils with this direction's
    bounds/collateral bound in."""
    executable_check = partial(
        is_executable_rate,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_rao=min_swap_rao,
        max_swap_rao=max_swap_rao,
    )
    can_fund = partial(
        crown_can_fund,
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_rao=min_swap_rao,
        max_swap_rao=max_swap_rao,
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
    rates, busy_count, active_set, collaterals = reconstruct_window_start_state(
        store, event_index, from_chain, to_chain, window_start, rewardable_hotkeys
    )
    replay_events = merge_replay_events(store, event_index, from_chain, to_chain, window_start, window_end)

    # Rates are stored as canonical_dest per canonical_source (TAO per BTC).
    # In the canonical direction (btc→tao) higher = better; in the reverse
    # direction (tao→btc) lower = better.
    canon_from, _ = canonical_pair(from_chain, to_chain)
    lower_rate_wins = from_chain != canon_from

    executable_check, can_fund = make_crown_predicates(from_chain, to_chain, min_swap_rao, max_swap_rao, collaterals)

    crown_blocks: Dict[str, float] = {}
    cap_weighted_blocks: Dict[str, float] = {}
    prev_block = window_start

    bounds_set = min_swap_rao > 0 or max_swap_rao > 0

    def credit_interval(interval_start: int, interval_end: int) -> None:
        duration = interval_end - interval_start
        if duration <= 0:
            return
        busy_set = {hk for hk, c in busy_count.items() if c > 0}
        rates_for_instant = rates
        holders = crown_holders_at_instant(
            rates_for_instant,
            rewardable_hotkeys,
            busy=busy_set,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set else None,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_blocks += duration
            return
        winner_rate = rates_for_instant.get(holders[0], 0.0)
        if trace is not None and winner_rate > 0:
            trace.best_rate = winner_rate
        if intervals_out is not None:
            intervals_out.append((interval_start, interval_end, list(holders), winner_rate))
        split = duration / len(holders)
        for hk in holders:
            crown_blocks[hk] = crown_blocks.get(hk, 0.0) + split
            # Unknown collateral (no event recorded) → capacity 1.0, matching
            # can_fund's fail-open. Only a known value scales capacity down.
            cap = capacity_factor(collaterals[hk], max_swap_rao) if hk in collaterals else 1.0
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


def snapshot_current_crown_holders(
    self: Validator,
    at_time: Optional[int] = None,
) -> Dict[Tuple[str, str], List[Tuple[str, str, str, float, float, int]]]:
    """Cheap "who holds the crown right now" per direction. Used by the
    per-forward-step live-crown writer.

    Reconstructs rates/busy/active at the current block and evaluates
    ``crown_holders_at_instant`` once per direction — no event-stream walk,
    so cost is O(rewardable_hotkeys) per direction, sub-millisecond in
    practice. Returns rows in ``DatabaseStorage.upsert_current_crown_snapshot``
    shape: ``(from_chain, to_chain, hotkey, credit, rate, block)`` keyed
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
    block = int(time.time()) if at_time is None else at_time
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
        rates, busy_count, active_set, collaterals = reconstruct_window_start_state(
            self.state_store,
            self.event_index,
            from_chain,
            to_chain,
            block,
            rewardable_hotkeys,
        )
        canon_from, _ = canonical_pair(from_chain, to_chain)
        lower_rate_wins = from_chain != canon_from
        busy_set = {hk for hk, c in busy_count.items() if c > 0}

        # Same predicates the scoring replay uses, so the live table never
        # credits a holder the ledger drops. Built per direction so each
        # closure captures the right chain pair.
        executable_check, can_fund = make_crown_predicates(
            from_chain, to_chain, min_swap_amount, max_swap_amount, collaterals
        )

        holders = crown_holders_at_instant(
            rates,
            rewardable_hotkeys,
            busy=busy_set,
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if bounds_set else None,
        )
        if holders:
            share = 1.0 / len(holders)
            rate = rates.get(holders[0], 0.0)
            rows_by_direction[(from_chain, to_chain)] = [
                (from_chain, to_chain, hk, share, rate, block) for hk in holders
            ]
        else:
            rows_by_direction[(from_chain, to_chain)] = []
    return rows_by_direction


def expand_intervals_to_crown_rows(
    intervals: List[Tuple[int, int, List[str], float]],
    from_chain: str,
    to_chain: str,
) -> List[Tuple[int, str, str, str, float, float]]:
    """Expand uniform-state intervals to per-block crown_holders rows.

    For each (lo, hi, holders, rate) emit (hi - lo) * len(holders) rows,
    each with credit = 1/len(holders). The per-block per-direction credits
    therefore sum to 1.0, matching the validator's fair-tie semantics."""
    rows: List[Tuple[int, str, str, str, float, float]] = []
    for lo, hi, holders, rate in intervals:
        if not holders or hi <= lo:
            continue
        share = 1.0 / len(holders)
        for block in range(lo, hi):
            for hotkey in holders:
                rows.append((block, from_chain, to_chain, hotkey, share, rate))
    return rows


def crown_holders_at_instant(
    rates: Dict[str, float],
    rewardable: Set[str],
    busy: Optional[Set[str]] = None,
    active: Optional[Set[str]] = None,
    lower_rate_wins: bool = False,
    executable_rate_check: Optional[Callable[[float], bool]] = None,
    can_fund_at_rate: Optional[Callable[[str, float], bool]] = None,
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
        if can_fund_at_rate is not None and not can_fund_at_rate(hotkey, rate):
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
