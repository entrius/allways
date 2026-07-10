"""Per-round scoring log block: how the pool was distributed, who held
crown, why each non-earner earned nothing, why pool recycled. Pure
presentation — never mutates state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chains import canonical_pair
from allways.constants import (
    RECYCLE_UID,
    TAO_TO_RAO,
)
from allways.utils.rate import min_executable_sol_leg

if TYPE_CHECKING:
    from allways.validator.scoring import DirectionTrace
    from neurons.validator import Validator


NON_EARNER_LINE_CAP = 30


@dataclass
class WeightingTrace:
    """Per-hotkey capacity + volume + eligibility factors for the scoring log.

    ``capacity_factor`` is the time-weighted average of
    ``min(1, collateral / max_swap)`` over the miner's crown intervals — the
    per-block series lives in the event watcher, so a post-window collateral
    top-up cannot retroactively scale it (#409). ``eligible`` is the flat 0/1
    crown gate read off the on-chain MinerState counters (B3.3)."""

    capacity_factor: float = 1.0
    volume_rao: int = 0
    crown_share: float = 0.0
    volume_share: float = 0.0
    participation: float = 1.0
    volume_factor: float = 1.0
    eligible: bool = False

    def record_capacity(self, factor: float) -> None:
        self.capacity_factor = factor

    def record_volume(self, vol_rao: int, total_volume_rao: int, crown_share: float, factor: float) -> None:
        self.volume_rao = vol_rao
        self.crown_share = crown_share
        self.volume_share = (vol_rao / total_volume_rao) if total_volume_rao > 0 else 0.0
        self.participation = min(1.0, self.volume_share / crown_share) if crown_share > 0 else 1.0
        self.volume_factor = factor

    def record_eligibility(self, eligible: bool) -> None:
        self.eligible = eligible


def log_scoring_trace(
    self: Validator,
    *,
    window_start: int,
    window_end: int,
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    rewards: np.ndarray,
    eligibility: Dict[str, bool],
    distributed: float,
    recycled: float,
    weighting_traces: Optional[Dict[str, 'WeightingTrace']] = None,
    min_swap_lamports: int = 0,
    max_swap_lamports: int = 0,
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
            f'UID{hotkey_to_uid[hk]}: {secs:.0f}s'
            for hk, secs in sorted(trace.crown_time.items(), key=lambda kv: -kv[1])
            if hk in hotkey_to_uid
        )
        lines.append(f'  [{from_c}→{to_c}] pool={trace.pool:g} holders={{{holders}}} unfilled={trace.unfilled_time}s')

    for uid in sorted((u for u in range(len(rewards)) if rewards[u] > 0), key=lambda u: -float(rewards[u])):
        hk = hotkeys[uid]
        crown_secs = sum(t.crown_time.get(hk, 0.0) for t in direction_traces.values())
        if uid == recycle_uid and crown_secs == 0:
            continue
        crown_reward = float(rewards[uid]) - (recycled if uid == recycle_uid else 0.0)
        eligible = eligibility.get(hk, False)
        wt = weighting_traces.get(hk)
        extras = ''
        if wt is not None:
            extras = (
                f' cap={wt.capacity_factor:.2f}'
                f' vol={wt.volume_rao / TAO_TO_RAO:g}t vol_share={wt.volume_share:.2f}'
                f' crown_share={wt.crown_share:.2f} vol_f={wt.volume_factor:.2f}'
            )
        lines.append(
            f'  uid={uid} hotkey={hk[:8]}.. crown_s={crown_secs:.0f} eligible={eligible}{extras} reward={crown_reward:.3f}'
        )

    # Collateral as-of window_start mirrors the scoring replay's starting
    # state, so the non-earner diagnosis can tell "excluded by collateral"
    # from "genuinely outbid". Absent hotkey == unknown (fail-open), per the
    # gate in scoring.py.
    collaterals = dict(self.event_index.get_miner_collaterals_at(window_start))
    lines.extend(
        non_earner_lines(
            self,
            window_start,
            window_end,
            rewards,
            eligibility,
            direction_traces,
            recycle_uid,
            collaterals,
            min_swap_lamports,
            max_swap_lamports,
        )
    )

    if recycled > 0:
        parts = [
            f'{t.unfilled_time}s unfilled in {f}→{to}' for (f, to), t in direction_traces.items() if t.unfilled_time > 0
        ]
        cause = '; '.join(parts) or 'no crown winners'
        lines.append(f'  recycled={recycled:.3f} → UID{recycle_uid} (subnet owner) cause={cause}')

    bt.logging.info('\n'.join(lines))


def non_earner_lines(
    self: Validator,
    window_start: int,
    window_end: int,
    rewards: np.ndarray,
    eligibility: Dict[str, bool],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    recycle_uid: int,
    collaterals: Optional[Dict[str, int]] = None,
    min_swap_lamports: int = 0,
    max_swap_lamports: int = 0,
) -> List[str]:
    collaterals = collaterals or {}
    ever_active = set(self.event_index.get_active_miners_at(window_start))
    for e in self.event_index.get_active_events_in_range(window_start, window_end):
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
        eligible = eligibility.get(hk, False)
        reason = diagnose_non_earner(
            hk, latest_rates, eligible, ever_active, direction_traces, collaterals, min_swap_lamports, max_swap_lamports
        )
        out.append(f'  uid={uid} hotkey={hk[:8]}.. crown_s=0 reason="{reason}" eligible={eligible}')
        if len(out) >= NON_EARNER_LINE_CAP:
            break
    return out


def diagnose_non_earner(
    hotkey: str,
    latest_rates: Dict[Tuple[str, str], float],
    eligible: bool,
    ever_active: Set[str],
    direction_traces: Dict[Tuple[str, str], DirectionTrace],
    collaterals: Optional[Dict[str, int]] = None,
    min_swap_lamports: int = 0,
    max_swap_lamports: int = 0,
) -> str:
    """Best-effort reason a miner earned no crown. Direction-aware: tao→btc is
    lower-rate-wins, btc→tao higher-wins, so "outbid" only fires when the
    miner's own rate is genuinely worse than the winner's. A rate that is at
    least as good as the winner's but still earned nothing was excluded by the
    capacity / can_fund collateral gate — report that, not "outbid"."""
    collaterals = collaterals or {}
    if not latest_rates:
        return 'no_rate_posted'
    if hotkey not in ever_active:
        return 'not_active_during_window'
    if not eligible:
        return 'ineligible'  # < MIN_SUCCESSFUL_SWAPS successes or > MAX_FAILED_SWAPS failures

    outbid_parts: List[str] = []
    for (from_c, to_c), own in latest_rates.items():
        trace = direction_traces.get((from_c, to_c))
        if trace is None or trace.best_rate <= 0:
            continue
        best = trace.best_rate
        canon_from, _ = canonical_pair(from_c, to_c)
        lower_wins = from_c != canon_from
        competitive = own <= best if lower_wins else own >= best
        if not competitive:
            outbid_parts.append(f'{from_c}→{to_c}: own={own:g} vs best={best:g}')
            continue
        # Rate is at least as good as the winner's, yet earned nothing — a
        # qualification gate dropped this miner. Collateral is the usual cause.
        if hotkey not in collaterals:
            return f'unknown_collateral ({from_c}→{to_c}: own={own:g} beats/ties best={best:g}, no baseline)'
        min_leg = min_executable_sol_leg(own, from_c, to_c, min_swap_lamports, max_swap_lamports)
        have = collaterals[hotkey]
        if min_leg > 0 and have < min_leg:
            return (
                f'insufficient_collateral ({from_c}→{to_c}: have={have / TAO_TO_RAO:g}t need={min_leg / TAO_TO_RAO:g}t)'
            )
        # Competitive and funded — lost to a tie split, busy, or active-flag timing.
        return f'competitive_but_unfilled ({from_c}→{to_c}: own={own:g} vs best={best:g})'

    return 'outbid (' + '; '.join(outbid_parts) + ')' if outbid_parts else 'no_competing_winner'
