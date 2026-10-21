"""The scoring math: one round's reward vector from the ledger and the World.

Reward per miner and lane is ``eligible × pool × [(1−β) × crown_share × capacity + β × qualified_volume_share]``.
``eligible`` is a flat 0/1 gate off the on-chain timeout counter plus a fill inside the trailing activity
window; the β slice pays fills reserved on a crown holder (``fill_held_crown``). Whatever the pools did not
pay recycles to ``RECYCLE_UID``: the burn keeps a penalty absolute instead of letting normalization stretch
the earners back to 100%."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt
import numpy as np

from allways.chains import canonical_pair, get_chain_def
from allways.classes import ActivityTransition, MinerActivity, next_activity
from allways.cli.swap_commands.swap_intake import bounds_from_config
from allways.constants import (
    ADDRESS_BAN_SECS,
    ALPHA_FAMILY,
    ALPHA_PRICE_TTL_SECS,
    CAPACITY_CURVE_EXPONENT,
    CROWN_RATE_BAND,
    DIRECTION_POOLS,
    ELIGIBILITY_FILL_WINDOW_SECS,
    HUB_CHAINS,
    MINER_POOL_SHARE,
    POOL_VOLUME_ALPHA,
    POOL_VOLUME_WINDOW_SECS,
    QUALITY_VOLUME_BETA,
    RATE_PRECISION,
    RECYCLE_UID,
    REWARD_MINER_STATES,
    collateral_leg,
    declarable_backings,
    hub_leg,
    required_collateral,
    scoring_family,
)
from allways.eligibility import RecentFills, backing_active, direction_eligible, is_eligible, quote_addresses_valid
from allways.solana.pdas import BACKING_BITS
from allways.utils.rate import is_executable_rate, min_executable_hub_leg, quantize_rate_fixed
from allways.validator.scoring.fee_pool import FeePoolShare
from allways.validator.scoring.trace import WeightingTrace, log_scoring_trace
from allways.validator.world.snapshot import World

Lane = Tuple[str, str, str]  # (from_chain, to_chain, backing)
Interval = Tuple[int, int, Dict[str, float], float]  # (start, end, {hotkey: credit}, the band's best rate)


class EventKind(IntEnum):
    """Order of coincident transitions in the replay: the active flag first, then activity (a reserved or
    fulfilling miner forfeits), then rate once the gates are set, then collateral, which only scales credit."""

    ACTIVE = 0
    ACTIVITY = 1
    RATE = 2
    COLLATERAL = 3


@dataclass
class ReplayEvent:
    """One transition in the replay. ``value``: rate, active 0/1, an ``ActivityTransition``, or collateral.
    ``hub`` scopes an activity transition to one purse; None steps every hub."""

    block: int
    hotkey: str
    kind: EventKind
    value: float
    hub: Optional[str] = None

    @property
    def sort_key(self) -> Tuple[int, int, int]:
        # A swap's FULFILL_* applies before its reservation's RESERVE_EXPIRE at the same instant.
        sub = int(self.value) if self.kind is EventKind.ACTIVITY else 0
        return (self.block, int(self.kind), sub)


@dataclass
class DirectionTrace:
    pool: float = 0.0  # the lane's volume-weighted pool, before the fee share
    miner_share: float = 1.0  # the round's fee share (fee_pool)
    crown_time: Dict[str, float] = field(default_factory=dict)
    qualified_volume: int = 0  # the lane's qualified family notional over the pool window
    cap_weighted_time: Dict[str, float] = field(default_factory=dict)
    unfilled_time: int = 0
    best_rate: float = 0.0

    @property
    def paid_pool(self) -> float:
        """What the lane pays this round: its pool scaled by the fee share."""
        return self.pool * self.miner_share


@dataclass
class ScoreRow:
    """One (hotkey, lane) of a round: the factors the reward used and the reward (post eligibility)."""

    hotkey: str
    from_chain: str
    to_chain: str
    backing: str
    eligible: bool
    pool: float
    crown_share: float
    capacity: float
    reward: float
    qvol_share: float = 0.0  # share of the lane's qualified volume the β slice paid on


@dataclass(frozen=True)
class RoundResult:
    window_start: int
    window_end: int
    hotkeys: Tuple[str, ...]  # the metagraph the rewards are indexed by
    rewards: np.ndarray  # per uid, before the EMA
    halted: bool = False
    rows: Tuple[ScoreRow, ...] = ()
    traces: Dict[Lane, DirectionTrace] = field(default_factory=dict)
    intervals: Dict[Lane, List[Interval]] = field(default_factory=dict)  # crown holders over the window
    fee_pool: Optional[FeePoolShare] = None  # the share the round's pools were scaled by (None when halted)


class AlphaPrices:
    """Backing units per smallest unit of a lane's collateral leg, each kept ``ALPHA_PRICE_TTL_SECS``. The
    subnet price table from the last metagraph sync answers first; a leg it lacks is read on its own. A
    failed read keeps the last price; None (never read) leaves executability unchecked."""

    def __init__(self, assets: Dict[str, Any], table: Callable[[], Optional[Dict[int, Any]]]) -> None:
        self.assets, self._table = assets, table
        self._prices: Dict[str, Tuple[int, Optional[float]]] = {}

    def unit_value(self, from_chain: str, to_chain: str, backing: str, now: int) -> Optional[float]:
        leg = collateral_leg(backing, from_chain, to_chain)
        if leg in (None, backing):
            return 1.0
        read_at, value = self._prices.get(leg, (None, None))
        if read_at is not None and now - read_at < ALPHA_PRICE_TTL_SECS:
            return value
        provider = self.assets.get(leg)
        price = (self._table() or {}).get(getattr(provider, 'netuid', None))
        if price is not None:
            value = int(price.rao) / 10**provider.chain_def.decimals
        else:
            whole = 10 ** get_chain_def(leg).decimals
            try:
                value = provider.value_rao(whole) / whole
            except Exception as e:
                bt.logging.warning(f'{leg} price read failed, keeping the last price ({value}): {e}')
        self._prices[leg] = (now, value)
        return value


# ── what the round reads off the World ─────────────────────────────────────


def miner_states(world: World, hotkeys: Set[str]) -> Dict[str, Any]:
    """``{hotkey: MinerState}`` of bound miners on the metagraph: the others have no uid to credit."""
    out: Dict[str, Any] = {}
    for _, ms in world.all('MinerState'):
        hotkey = world.hotkey_for(ms.miner)
        if hotkey is not None and hotkey in hotkeys:
            out[hotkey] = ms
    return out


def bond_balances(world: World, hotkeys: Set[str], chain: str = 'tao') -> Dict[str, int]:
    """``{hotkey: attested bond}`` on ``chain``: the tao purse's live side for the reconcile. Unlocked = 0."""
    out: Dict[str, int] = {}
    for _, ba in world.all('BondAttestation'):
        hotkey = world.hotkey_for(ba.miner)
        if str(ba.chain).lower() == chain and hotkey is not None and hotkey in hotkeys:
            out[hotkey] = int(ba.effective_balance) if ba.locked else 0
    return out


def quote_rates(world: World, providers: Dict[str, Any], states: Dict[str, Any]) -> Dict[Tuple[str, ...], float]:
    """``{(hotkey, from, to, backing): rate}`` of every live quote, scaled as ``QuoteSet`` ingest scales it.
    A quote naming an address its chain rejects, or on a purse its miner dropped, reads 0."""
    out: Dict[Tuple[str, ...], float] = {}
    for _, q in world.all('MinerQuote'):
        hotkey = world.hotkey_for(q.miner)
        if hotkey is None:
            continue
        lane = (hotkey, str(q.from_chain).lower(), str(q.to_chain).lower(), str(q.collateral_chain).lower())
        ms = states.get(hotkey)
        live = quote_addresses_valid(q, providers) and not (ms is not None and not backing_active(ms, lane[3]))
        out[lane] = quantize_rate_fixed(int(q.rate)) / RATE_PRECISION if live else 0.0
    return out


def recent_fill_hotkeys(ledger: Any, now: int) -> Optional[RecentFills]:
    """Per purse, the hotkeys with a fill in the trailing window; None (strikes only) while the clearing
    ledger is younger than the window."""
    if now - ledger.clearing_ledger_since(now) < ELIGIBILITY_FILL_WINDOW_SECS:
        return None
    return ledger.get_recent_fill_hotkeys(now - ELIGIBILITY_FILL_WINDOW_SECS, now)


def without_address_banned(ledger: Any, states: Dict[str, Any], now: int) -> Dict[str, Any]:
    """``states`` minus miners serving an address ban: no state means no crown and no pay on any lane."""
    banned = ledger.address_banned_hotkeys(now - ADDRESS_BAN_SECS, now)
    return {hk: ms for hk, ms in states.items() if hk not in banned}


def lane_eligible_hotkeys(
    states: Dict[str, Any],
    rewardable: Set[str],
    from_chain: str,
    to_chain: str,
    now: int,
    backing: Optional[str] = None,
    recent_fills: Optional[RecentFills] = None,
) -> Set[str]:
    """The crown candidates of one lane. An ineligible miner is removed from candidacy, not just unpaid:
    otherwise it keeps holding the crown, the next-best eligible rate earns nothing and the pool burns."""
    return {
        hk
        for hk in rewardable
        if hk in states
        and direction_eligible(
            states[hk], from_chain, to_chain, now, backing=backing, hotkey=hk, recent_fills=recent_fills
        )
    }


# ── pools ───────────────────────────────────────────────────────────────────


def lane_volumes_to_directions(
    lane_volumes: Dict[Lane, Dict[str, Tuple[int, ...]]],
) -> Dict[Tuple[str, str], Dict[str, Tuple[int, ...]]]:
    """Per-lane qualified volumes collapsed to pair directions: pool volume is never split by backing."""
    out: Dict[Tuple[str, str], Dict[str, Tuple[int, ...]]] = {}
    for (from_chain, to_chain, backing), by_hotkey in lane_volumes.items():
        if backing not in declarable_backings(from_chain, to_chain):
            continue  # a lane no quote may declare now scores nothing
        direction = out.setdefault((from_chain, to_chain), {})
        for hotkey, sums in by_hotkey.items():
            prev = direction.get(hotkey, (0,) * len(sums))
            direction[hotkey] = tuple(a + b for a, b in zip(prev, sums))
    return out


def family_notional(from_chain: str, to_chain: str, sums: Tuple[int, ...]) -> int:
    """A direction's volume in its family's one unit: the TAO collateral on an alpha pair, else the hub leg."""
    if scoring_family(from_chain, to_chain) == ALPHA_FAMILY:
        return sums[2]
    return sums[0] if from_chain == hub_leg(from_chain, to_chain) else sums[1]


def qualified_volume_shares(
    lane_volume: Dict[str, Tuple[int, ...]], from_chain: str, to_chain: str
) -> Tuple[Dict[str, float], int]:
    """``({hotkey: share}, total)`` of one lane's qualified notional: the β slice's split."""
    notional = {hk: n for hk, sums in lane_volume.items() if (n := family_notional(from_chain, to_chain, sums)) > 0}
    total = sum(notional.values())
    if total <= 0:
        return {}, 0
    return {hk: n / total for hk, n in notional.items()}, total


def compute_direction_pools(clearing_volumes: Dict[Tuple[str, str], Dict[str, Tuple[int, ...]]]) -> Dict[Lane, float]:
    """Volume-weighted pools per lane over a trailing window of qualified swaps.

    Each family (sol, tao, alpha) holds an equal share, so alpha pairs can never move the SOL or TAO pools.
    A pair is live iff it cleared qualified volume; a live pair earns ``(1−α)/live_pairs + α × its share of
    the family's notional``, split evenly between its directions (pair level, so one leg can't be inflated
    alone) and then its backing lanes. No volume anywhere falls back to ``DIRECTION_POOLS``. Notional is one
    unit per family, so no price oracle is needed. Pools sum to at most ``MINER_POOL_SHARE``."""
    pair_directions: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for from_chain, to_chain in DIRECTION_POOLS:
        pair_directions.setdefault(canonical_pair(from_chain, to_chain), []).append((from_chain, to_chain))

    pair_volumes: Dict[Tuple[str, str], int] = {}
    for pair, directions in pair_directions.items():
        pair_volumes[pair] = sum(
            family_notional(f, t, sums) for f, t in directions for sums in clearing_volumes.get((f, t), {}).values()
        )

    live_pairs = {pair for pair, volume in pair_volumes.items() if volume > 0}
    if not live_pairs:
        return {
            (f, t, backing): pool / len(declarable_backings(f, t))
            for (f, t), pool in DIRECTION_POOLS.items()
            for backing in declarable_backings(f, t)
        }

    families: Dict[str, List[Tuple[str, str]]] = {}
    for pair in pair_directions:
        families.setdefault(scoring_family(*pair), []).append(pair)
    family_share = 1.0 / len(families)

    pools: Dict[Lane, float] = {}
    for family_pairs in families.values():
        family_live = [p for p in family_pairs if p in live_pairs]
        family_volume = sum(pair_volumes[p] for p in family_live)
        equal_pair_share = 1.0 / len(family_live) if family_live else 0.0
        for pair in family_pairs:
            pair_share = 0.0
            if pair in live_pairs:
                volume_share = pair_volumes[pair] / family_volume
                pair_share = (1.0 - POOL_VOLUME_ALPHA) * equal_pair_share + POOL_VOLUME_ALPHA * volume_share
            directions = pair_directions[pair]
            for f, t in directions:
                lanes = declarable_backings(f, t)
                for backing in lanes:
                    pools[(f, t, backing)] = MINER_POOL_SHARE * family_share * pair_share / len(directions) / len(lanes)
    return pools


def build_direction_score_rows(
    from_chain: str,
    to_chain: str,
    backing: str,
    pool: float,
    crown_time: Dict[str, float],
    cap_weighted_time: Dict[str, float],
    eligibility: Dict[str, bool],
    qvol_share: Optional[Dict[str, float]] = None,
) -> List[ScoreRow]:
    """Per-miner factors and reward for one lane, one row per crown holder or qualified filler: (1−β) on
    crown time × capacity, β on qualified volume share. A lane with neither pays nobody."""
    qvol_share = qvol_share or {}
    total_crown = sum(crown_time.values())
    if total_crown <= 0 and not qvol_share:
        return []
    rows: List[ScoreRow] = []
    for hotkey in list(crown_time) + [hk for hk in qvol_share if hk not in crown_time]:
        secs = crown_time.get(hotkey, 0.0)
        # Capacity is integrated over the crown intervals, so a post-window top-up can't boost earned credit.
        cap = cap_weighted_time.get(hotkey, 0.0) / secs if secs > 0 else 0.0
        eligible = eligibility.get(hotkey, False)
        crown_share = secs / total_crown if total_crown > 0 else 0.0
        qv = qvol_share.get(hotkey, 0.0)
        # The β slice is not capacity-scaled: a completed fill was backed by construction.
        share = (1.0 - QUALITY_VOLUME_BETA) * crown_share * cap + QUALITY_VOLUME_BETA * qv
        reward = (1.0 if eligible else 0.0) * pool * share
        rows.append(ScoreRow(hotkey, from_chain, to_chain, backing, eligible, pool, crown_share, cap, reward, qv))
    return rows


# ── the crown ───────────────────────────────────────────────────────────────


def capacity_factor(collateral: int, max_swap: int) -> float:
    """``min(1, (collateral / required_collateral(max_swap))^k)``: full credit means backing a max_swap fill
    at the contract's 1.10× gate; past that, collateral earns nothing extra. 1.0 when bounds are unset."""
    if max_swap <= 0:
        return 1.0
    if collateral <= 0:
        return 0.0
    return min(1.0, (collateral / required_collateral(max_swap)) ** CAPACITY_CURVE_EXPONENT)


def crown_can_fund(
    hotkey, rate, from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals, bounded_chain=None, unit_value=1.0
):
    """A miner who can't fund its own rate's smallest hub leg at the 1.10× reserve requirement is unreservable
    and earns no crown. Unknown collateral counts as zero (fail closed)."""
    min_leg = min_executable_hub_leg(rate, from_chain, to_chain, min_swap_hub, max_swap_hub, bounded_chain, unit_value)
    return min_leg == 0 or collaterals.get(hotkey, 0) >= required_collateral(min_leg)


def crown_depth_shares(holders: List[str], collaterals: Dict[str, int], max_swap_hub: int) -> Dict[str, float]:
    """One interval's credit split across the band's holders by collateral depth, capped at a max_swap fill's
    requirement (never pay-to-win). No depth at all splits evenly."""
    cap = required_collateral(max_swap_hub) if max_swap_hub > 0 else None
    depths = {hk: min(collaterals.get(hk, 0), cap) if cap else collaterals.get(hk, 0) for hk in holders}
    total = sum(depths.values())
    if total <= 0:
        return {hk: 1.0 / len(holders) for hk in holders}
    return {hk: depth / total for hk, depth in depths.items()}


def make_crown_predicates(
    from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals, backing=None, unit_value: Optional[float] = 1.0
):
    """``(executable_check, can_fund)`` for one lane: the rate utils with the backing's bounds applied to its
    collateral leg at ``unit_value``. An unreadable value (None) drops the bounds."""
    bounded_chain = collateral_leg(backing, from_chain, to_chain) if backing else None
    if unit_value is None:
        min_swap_hub = max_swap_hub = 0
        unit_value = 1.0
    bounds = dict(
        from_chain=from_chain,
        to_chain=to_chain,
        min_swap_hub=min_swap_hub,
        max_swap_hub=max_swap_hub,
        bounded_chain=bounded_chain,
        unit_value=unit_value,
    )
    return partial(is_executable_rate, **bounds), partial(crown_can_fund, collaterals=collaterals, **bounds)


def lane_serving_hubs(backing: Optional[str]) -> List[str]:
    """The hub a lane serves through: its backing. None (spoke↔spoke, never scored) means every hub."""
    return [backing] if backing is not None else list(BACKING_BITS)


def rewardable_by_activity(
    activity: Dict[str, Dict[str, MinerActivity]], hotkeys: Set[str], serving_hubs: List[str]
) -> Set[str]:
    """Hotkeys whose activity still permits crown on a lane: busy forfeits only while every serving hub is
    mid-reservation or swap, so a sol-busy miner keeps earning on its tao lanes. Absent = AVAILABLE."""
    return {
        hk
        for hk in hotkeys
        if any((activity.get(hk) or {}).get(h, MinerActivity.AVAILABLE) in REWARD_MINER_STATES for h in serving_hubs)
    }


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
    """The miners at the best rate that qualifies (rewardable, active, in a rewarding activity state, rate > 0,
    executable, fundable), falling through to the next rate when none does. ``rate_band`` widens the set to
    every qualified rate within that fraction of the best qualified one, best first. ``lower_rate_wins`` for
    the direction opposite the canonical one. None skips a gate."""

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
        if rate_band > 0:  # anchor at the best qualified rate, sweep worse levels while inside the band
            edge = rate * (1 + rate_band) if lower_rate_wins else rate * (1 - rate_band)
            for worse in ordered[i + 1 :]:
                if (worse > edge) if lower_rate_wins else (worse < edge):
                    break
                winners.extend(hk for hk in by_rate[worse] if qualifies(hk))
        return winners
    return []


_warned_activity_transitions: Set[Tuple[MinerActivity, ActivityTransition]] = set()


def warn_unexpected_activity(state: MinerActivity, transition: ActivityTransition) -> None:
    key = (state, transition)
    if key not in _warned_activity_transitions:
        _warned_activity_transitions.add(key)
        bt.logging.warning(f'crown replay: unexpected activity transition {transition.name} in {state.name}')


def window_start_state(
    ledger: Any, from_chain: str, to_chain: str, at: int, rewardable: Set[str], backing: Optional[str]
) -> Tuple[Dict[str, float], Dict[str, Dict[str, MinerActivity]], Set[str], Dict[str, int]]:
    """Rates (this lane's backing only), per-hub activity (busy hubs only), the active set and collateral as
    they stood at ``at``."""
    activity = {hk: dict(hubs) for hk, hubs in ledger.get_activity_state_at(at).items()}
    active = set(ledger.get_active_miners_at(at))
    latest = ledger.get_latest_rates_before(from_chain, to_chain, at, collateral_chain=backing)
    rates = {hk: rate_block[0] for hk, rate_block in latest.items() if hk in rewardable}
    collaterals = dict(ledger.get_miner_collaterals_at(at, backing=backing))
    return rates, activity, active, collaterals


def merge_replay_events(
    ledger: Any, from_chain: str, to_chain: str, start: int, end: int, backing: Optional[str]
) -> List[ReplayEvent]:
    """Every in-window transition in one chronological stream; rates and collateral for this lane's backing."""
    events = [
        ReplayEvent(e['block'], e['hotkey'], EventKind.ACTIVE, 1.0 if e['active'] else 0.0)
        for e in ledger.get_active_events_in_range(start, end)
    ]
    events += [
        ReplayEvent(e['block'], e['hotkey'], EventKind.ACTIVITY, float(e['kind']), e.get('hub'))
        for e in ledger.get_activity_events_in_range(start, end)
    ]
    events += [
        ReplayEvent(e['block'], e['hotkey'], EventKind.RATE, float(e['rate']))
        for e in ledger.get_rate_events_in_range(from_chain, to_chain, start, end, collateral_chain=backing)
    ]
    events += [
        ReplayEvent(e['block'], e['hotkey'], EventKind.COLLATERAL, float(e['collateral_rao']))
        for e in ledger.get_collateral_events_in_range(start, end, backing=backing)
    ]
    events.sort(key=lambda ev: ev.sort_key)
    return events


def replay_crown_time_window(
    ledger: Any,
    from_chain: str,
    to_chain: str,
    window_start: int,
    window_end: int,
    rewardable_hotkeys: Set[str],
    trace: Optional[DirectionTrace] = None,
    intervals_out: Optional[List[Interval]] = None,
    min_swap_hub: int = 0,
    max_swap_hub: int = 0,
    rate_band: float = CROWN_RATE_BAND,
    backing: Optional[str] = None,
    unit_value: Optional[float] = 1.0,
) -> Dict[str, float]:
    """``{hotkey: crown seconds}`` over the window. Every qualified miner within ``rate_band`` of the best
    qualified rate holds the crown together, splitting each interval by collateral depth. Active, rate,
    activity and collateral are replayed per instant from the ledger; only metagraph membership is read at
    scoring time. ``backing`` names the lane (None: the pair's hub leg); bounds are that backing's, applied
    to the collateral leg at ``unit_value``, and 0 disables the executability filter. With ``trace``, also
    fills its capacity-weighted crown time, so a post-window collateral boost can't scale earned credit."""
    if backing is None:
        backing = hub_leg(from_chain, to_chain)
    rates, activity, active_set, collaterals = window_start_state(
        ledger, from_chain, to_chain, window_start, rewardable_hotkeys, backing
    )
    replay = merge_replay_events(ledger, from_chain, to_chain, window_start, window_end, backing)
    lower_rate_wins = from_chain != canonical_pair(from_chain, to_chain)[0]
    executable_check, can_fund = make_crown_predicates(
        from_chain, to_chain, min_swap_hub, max_swap_hub, collaterals, backing, unit_value
    )
    purse_known = backing in HUB_CHAINS  # no purse stream (spoke↔spoke): purse gates run neutral
    gate_funding = (min_swap_hub > 0 or max_swap_hub > 0) and purse_known
    serving_hubs = lane_serving_hubs(backing)
    crown_time: Dict[str, float] = {}
    cap_weighted: Dict[str, float] = {}

    def credit(start: int, end: int) -> None:
        duration = end - start
        if duration <= 0:
            return
        holders = crown_holders_at_instant(
            rates,
            rewardable_hotkeys,
            rewardable_by_state=rewardable_by_activity(activity, rewardable_hotkeys, serving_hubs),
            active=active_set,
            lower_rate_wins=lower_rate_wins,
            executable_rate_check=executable_check,
            can_fund_at_rate=can_fund if gate_funding else None,
            rate_band=rate_band,
        )
        if not holders:
            if trace is not None:
                trace.unfilled_time += duration
            return
        best = rates.get(holders[0], 0.0)
        if trace is not None and best > 0:
            trace.best_rate = best
        shares = crown_depth_shares(holders, collaterals if purse_known else {}, max_swap_hub)
        if intervals_out is not None:
            intervals_out.append((start, end, dict(shares), best))
        for hk, share in shares.items():
            split = duration * share
            crown_time[hk] = crown_time.get(hk, 0.0) + split
            cap = capacity_factor(collaterals.get(hk, 0), max_swap_hub) if purse_known else 1.0
            cap_weighted[hk] = cap_weighted.get(hk, 0.0) + split * cap

    def apply(event: ReplayEvent) -> None:
        if event.kind is EventKind.RATE:
            rates[event.hotkey] = event.value
        elif event.kind is EventKind.ACTIVITY:
            transition = ActivityTransition(int(event.value))
            per_hub = activity.setdefault(event.hotkey, {})
            for h in [event.hub] if event.hub else list(BACKING_BITS):  # no hub: steps every hub
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
        elif event.value > 0:
            active_set.add(event.hotkey)
        else:
            active_set.discard(event.hotkey)

    prev = window_start
    for event in replay:
        credit(prev, event.block)
        apply(event)
        prev = event.block
    credit(prev, window_end)
    if trace is not None:
        trace.crown_time = dict(crown_time)
        trace.cap_weighted_time = dict(cap_weighted)
    return crown_time


def fill_held_crown(
    world: World,
    hotkeys: Set[str],
    ledger: Any,
    prices: AlphaPrices,
    hotkey: str,
    from_chain: str,
    to_chain: str,
    backing: str,
    collateral_amount: int,
    at_time: int,
) -> bool:
    """Did ``hotkey`` hold the lane's crown at ``at_time`` (its reservation instant), judged for a fill of
    ``collateral_amount``? The reserved miner is always a candidate (it was takeable by construction), and
    can-fund is bound to this swap's size, so a fill routed past a too-thin holder still counts."""
    if hotkey not in hotkeys:
        return False
    states = without_address_banned(ledger, miner_states(world, hotkeys), at_time)
    fills = recent_fill_hotkeys(ledger, at_time)
    candidates = lane_eligible_hotkeys(states, hotkeys, from_chain, to_chain, at_time, backing, fills) | {hotkey}
    min_swap, max_swap = bounds_from_config(world.config).get(backing, (0, 0))
    rates, activity, active, collaterals = window_start_state(
        ledger, from_chain, to_chain, at_time, candidates, backing
    )
    executable_check, _ = make_crown_predicates(
        from_chain,
        to_chain,
        min_swap,
        max_swap,
        collaterals,
        backing,
        prices.unit_value(from_chain, to_chain, backing, at_time),
    )
    need = required_collateral(int(collateral_amount))

    def can_back(hk: str, _rate: float) -> bool:
        return hk == hotkey or collaterals.get(hk, 0) >= need  # the reservation proves the reserved miner's

    holders = crown_holders_at_instant(
        rates,
        candidates,
        rewardable_by_state=rewardable_by_activity(activity, candidates, lane_serving_hubs(backing)) | {hotkey},
        active=active | {hotkey},
        lower_rate_wins=from_chain != canonical_pair(from_chain, to_chain)[0],
        executable_rate_check=executable_check,
        can_fund_at_rate=can_back if (min_swap > 0 or max_swap > 0) and backing in HUB_CHAINS and need > 0 else None,
        rate_band=CROWN_RATE_BAND,
    )
    return hotkey in holders


# ── one round ───────────────────────────────────────────────────────────────


def halted_round(hotkeys: Tuple[str, ...], window_start: int, window_end: int) -> RoundResult:
    """During a halt no miner earns crown: the whole pool recycles."""
    rewards = np.zeros(len(hotkeys), dtype=np.float32)
    if len(hotkeys):
        rewards[RECYCLE_UID if RECYCLE_UID < len(hotkeys) else 0] = 1.0
    return RoundResult(window_start, window_end, hotkeys, rewards, halted=True)


def replay_lane(
    ledger, lane, states, rewardable, recent_fills, swap_bounds, prices, start, end, **out
) -> Dict[str, float]:
    """One lane's crown replay over ``[start, end]``, with the lane's candidates, bounds and unit value."""
    from_chain, to_chain, backing = lane
    min_swap, max_swap = swap_bounds.get(backing, (0, 0))
    candidates = lane_eligible_hotkeys(states, rewardable, from_chain, to_chain, end, backing, recent_fills)
    return replay_crown_time_window(
        ledger,
        from_chain,
        to_chain,
        start,
        end,
        candidates,
        min_swap_hub=min_swap,
        max_swap_hub=max_swap,
        backing=backing,
        unit_value=prices.unit_value(from_chain, to_chain, backing, end),
        **out,
    )


def crown_now(world: World, hotkeys: Tuple[str, ...], ledger: Any, prices: AlphaPrices, now: int) -> Dict[Lane, List]:
    """``current_crown_holders`` rows per quoted lane as of ``now``: the round's replay over the last second.
    Reads only; the round's reconcile is what corrects the ledger."""
    lanes = [lane for lane in ledger.quoted_lanes() if lane[:2] in DIRECTION_POOLS]
    if world.config is None or world.config.halted:
        return {lane: [] for lane in lanes}
    rewardable = set(hotkeys)
    states = without_address_banned(ledger, miner_states(world, rewardable), now)
    recent_fills, swap_bounds = recent_fill_hotkeys(ledger, now), bounds_from_config(world.config)
    out: Dict[Lane, List] = {}
    for lane in lanes:
        intervals: List[Interval] = []
        replay_lane(
            ledger, lane, states, rewardable, recent_fills, swap_bounds, prices, now - 1, now, intervals_out=intervals
        )
        last = intervals[-1] if intervals and intervals[-1][1] >= now else None
        out[lane] = [(*lane, hk, share, last[3], now) for hk, share in last[2].items()] if last else []
    return out


def score_round(
    world: World,
    hotkeys: Tuple[str, ...],
    ledger: Any,
    providers: Dict[str, Any],
    prices: AlphaPrices,
    window_start: int,
    window_end: int,
    fee_pool: FeePoolShare,
) -> RoundResult:
    """Anchor the ledger to the World, replay each lane's crown over the window, pay each pool scaled by the
    round's fee share, recycle the rest. The window is on the unix-time axis the ledger is keyed on."""
    n, now = len(hotkeys), window_end
    rewards = np.zeros(n, dtype=np.float32)
    if n == 0:
        return RoundResult(window_start, window_end, hotkeys, rewards)
    rewardable = set(hotkeys)
    uid_of = {hk: uid for uid, hk in enumerate(hotkeys)}

    # The World's live state corrects any transition the ingest lost, before the replay reads the ledger.
    states = miner_states(world, rewardable)
    made = ledger.reconcile_live_state(states, now=now, live_bonds=bond_balances(world, rewardable))
    unbanned = without_address_banned(ledger, states, now)
    if len(unbanned) < len(states):
        bt.logging.warning(
            f'address-banned miners score zero this round: {sorted(hk[:8] for hk in states.keys() - unbanned.keys())}'
        )
    # The book is read for every miner: a banned miner's quotes still have to match the chain.
    live_rates = quote_rates(world, providers, states)
    made['rate'] = ledger.reconcile_live_quotes(live_rates, now=now)
    if any(made.values()):
        # Expected on a fresh ledger; afterwards each one is an event the feed missed.
        bt.logging.info('reconcile: the chain corrected ' + ', '.join(f'{n} {kind}' for kind, n in made.items() if n))
    states = unbanned
    recent_fills = recent_fill_hotkeys(ledger, now)
    eligibility = {hk: is_eligible(ms, now, hotkey=hk, recent_fills=recent_fills) for hk, ms in states.items()}
    swap_bounds = bounds_from_config(world.config)

    # One trailing-day volume read feeds the pool weighting and each lane's β slice. Each lane pays its pool × the
    # fee share, so the paid pools sum to MINER_POOL_SHARE × share and at least the rest recycles.
    lane_volumes = ledger.get_qualified_lane_volumes(now - POOL_VOLUME_WINDOW_SECS, now)
    pools = compute_direction_pools(lane_volumes_to_directions(lane_volumes))
    quoted = ledger.quoted_lanes()
    traces: Dict[Lane, DirectionTrace] = {}
    intervals: Dict[Lane, List[Interval]] = {}
    weighting: Dict[str, WeightingTrace] = {}
    rows: List[ScoreRow] = []
    for lane, pool in pools.items():
        from_chain, to_chain, backing = lane
        trace = traces[lane] = DirectionTrace(pool=pool, miner_share=fee_pool.share)
        qvol, trace.qualified_volume = qualified_volume_shares(lane_volumes.get(lane, {}), from_chain, to_chain)
        if lane not in quoted:
            continue  # never quoted: no crown, no dashboard rows to rewrite
        lane_intervals = intervals.setdefault(lane, [])  # a quoted dead lane still has its window rewritten
        if pool <= 0:
            continue  # a dead pair this window: nothing to pay
        crown_time = replay_lane(
            ledger,
            lane,
            states,
            rewardable,
            recent_fills,
            swap_bounds,
            prices,
            window_start,
            window_end,
            trace=trace,
            intervals_out=lane_intervals,
        )
        if not sum(crown_time.values()) and not qvol:
            continue
        lane_eligibility = {
            hk: direction_eligible(ms, from_chain, to_chain, now, backing=backing, hotkey=hk, recent_fills=recent_fills)
            for hk, ms in states.items()
        }
        for row in build_direction_score_rows(
            from_chain, to_chain, backing, trace.paid_pool, crown_time, trace.cap_weighted_time, lane_eligibility, qvol
        ):
            uid = uid_of.get(row.hotkey)
            if uid is None:
                continue  # dereg'd mid-window: credit forfeited
            wt = weighting.setdefault(row.hotkey, WeightingTrace())
            if row.crown_share > 0:
                wt.record_capacity(row.capacity)
            if row.qvol_share > 0:
                wt.record_qvol(row.qvol_share)
            wt.record_eligibility(row.eligible)
            rewards[uid] += row.reward
            rows.append(row)

    distributed = float(rewards.sum())
    recycled = max(0.0, 1.0 - distributed)
    rewards[RECYCLE_UID if RECYCLE_UID < n else 0] += recycled
    log_scoring_trace(
        hotkeys,
        ledger,
        window_start=window_start,
        window_end=window_end,
        direction_traces=traces,
        rewards=rewards,
        eligibility=eligibility,
        distributed=distributed,
        recycled=recycled,
        weighting_traces=weighting,
        swap_bounds=swap_bounds,
        live_rates=live_rates,
        fee_pool=fee_pool,
    )
    return RoundResult(window_start, window_end, hotkeys, rewards, False, tuple(rows), traces, intervals, fee_pool)
