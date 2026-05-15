"""Per-round scoring log block: how the pool was distributed, who held
crown, why each non-earner earned nothing, why pool recycled. Pure
presentation — never mutates state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.constants import CREDIBILITY_RAMP_OBSERVATIONS, RECYCLE_UID, TAO_TO_RAO

if TYPE_CHECKING:
    from allways.validator.scoring import DirectionTrace
    from neurons.validator import Validator


NON_EARNER_LINE_CAP = 30


@dataclass
class WeightingTrace:
    """Per-hotkey capacity + volume + credibility factors for the scoring log."""

    collateral: int = 0
    capacity_factor: float = 1.0
    volume_rao: int = 0
    crown_share: float = 0.0
    volume_share: float = 0.0
    participation: float = 1.0
    volume_factor: float = 1.0
    closed_swaps: int = 0
    credibility_ramp: float = 0.0

    def record_capacity(self, collateral: int, factor: float) -> None:
        self.collateral = collateral
        self.capacity_factor = factor

    def record_volume(self, vol_rao: int, total_volume_rao: int, crown_share: float, factor: float) -> None:
        self.volume_rao = vol_rao
        self.crown_share = crown_share
        self.volume_share = (vol_rao / total_volume_rao) if total_volume_rao > 0 else 0.0
        self.participation = min(1.0, self.volume_share / crown_share) if crown_share > 0 else 1.0
        self.volume_factor = factor

    def record_credibility(self, closed_swaps: int, ramp_target: int) -> None:
        self.closed_swaps = closed_swaps
        self.credibility_ramp = min(1.0, closed_swaps / ramp_target) if ramp_target > 0 else 1.0


def log_scoring_trace(
    self: Validator,
    *,
    window_start: int,
    window_end: int,
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    rewards: np.ndarray,
    success_rates: Dict[str, float],
    distributed: float,
    recycled: float,
    weighting_traces: Optional[Dict[str, 'WeightingTrace']] = None,
) -> None:
    hotkeys = self.metagraph.hotkeys
    recycle_uid = RECYCLE_UID if RECYCLE_UID < len(rewards) else 0
    hotkey_to_uid = {hk: uid for uid, hk in enumerate(hotkeys)}
    weighting_traces = weighting_traces or {}

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
        hk = hotkeys[uid]
        crown_blk = sum(t.crown_blocks.get(hk, 0.0) for t in direction_traces.values())
        if uid == recycle_uid and crown_blk == 0:
            continue
        crown_reward = float(rewards[uid]) - (recycled if uid == recycle_uid else 0.0)
        sr = success_rates.get(hk, 0.0)
        wt = weighting_traces.get(hk)
        extras = ''
        if wt is not None:
            extras = (
                f' ({wt.closed_swaps}/{CREDIBILITY_RAMP_OBSERVATIONS} closed, ramp={wt.credibility_ramp:.2f})'
                f' cap={wt.capacity_factor:.2f} (col={wt.collateral / TAO_TO_RAO:g}t)'
                f' vol={wt.volume_rao / TAO_TO_RAO:g}t vol_share={wt.volume_share:.2f}'
                f' crown_share={wt.crown_share:.2f} vol_f={wt.volume_factor:.2f}'
            )
        lines.append(
            f'  uid={uid} hotkey={hk[:8]}.. crown_blk={crown_blk:.0f} sr={sr:.3f}{extras} reward={crown_reward:.3f}'
        )

    lines.extend(
        non_earner_lines(self, window_start, window_end, rewards, success_rates, direction_traces, recycle_uid)
    )

    if recycled > 0:
        parts = [
            f'{t.unfilled_blocks} unfilled blk in {f}→{to}'
            for (f, to), t in direction_traces.items()
            if t.unfilled_blocks > 0
        ]
        cause = '; '.join(parts) or 'no crown winners'
        lines.append(f'  recycled={recycled:.3f} → UID{recycle_uid} (subnet owner) cause={cause}')

    bt.logging.info('\n'.join(lines))


def non_earner_lines(
    self: Validator,
    window_start: int,
    window_end: int,
    rewards: np.ndarray,
    success_rates: Dict[str, float],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    recycle_uid: int,
) -> List[str]:
    ever_active = set(self.event_watcher.get_active_miners_at(window_start))
    for e in self.event_watcher.get_active_events_in_range(window_start, window_end):
        if e['active']:
            ever_active.add(e['hotkey'])

    rates_by_hotkey: Dict[str, Dict[Tuple[str, str], float]] = {}
    for (hk, from_c, to_c), r in (getattr(self, 'last_known_rates', {}) or {}).items():
        if r > 0:
            rates_by_hotkey.setdefault(hk, {})[(from_c, to_c)] = r

    out: List[str] = []
    for uid, hk in enumerate(self.metagraph.hotkeys):
        if uid == recycle_uid or rewards[uid] > 0:
            continue
        latest_rates = rates_by_hotkey.get(hk, {})
        if not latest_rates and hk not in ever_active:
            continue
        sr = success_rates.get(hk, 1.0)
        reason = diagnose_non_earner(hk, latest_rates, sr, ever_active, direction_traces)
        out.append(f'  uid={uid} hotkey={hk[:8]}.. crown_blk=0 reason="{reason}" sr={sr:.3f}')
        if len(out) >= NON_EARNER_LINE_CAP:
            break
    return out


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
        return 'credibility_zero'  # zero observations OR all-timeout history
    parts = [
        f'{direction[0]}→{direction[1]}: own={own:g} vs best={direction_traces[direction].best_rate:g}'
        for direction, own in latest_rates.items()
        if direction in direction_traces and direction_traces[direction].best_rate > 0
    ]
    return 'outbid (' + '; '.join(parts) + ')' if parts else 'no_competing_winner'
