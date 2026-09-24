"""Miner eligibility — the one reading of "can this miner earn emissions", shared by scoring
(the payout gate) and swap intake (routing). Pure over the on-chain ``MinerState`` plus the
validator's optional fill ledger, so a taker, a validator router and the scorer never disagree
on who is quotable.
"""

from typing import Dict, Optional, Set

from allways.constants import MAX_FAILED_SWAPS, declarable_backings
from allways.solana.layouts import lock_max
from allways.solana.pdas import BACKING_BITS

RecentFills = Dict[str, Set[str]]  # {backing: hotkeys that delivered on that purse in the window}


def is_eligible(
    miner_state,
    now: Optional[int] = None,
    *,
    hotkey: Optional[str] = None,
    recent_fills: Optional[RecentFills] = None,
) -> bool:
    """The GLOBAL binary gate, one reputation across every hub: at most ``MAX_FAILED_SWAPS``
    lifetime timeouts (on-chain ``MinerState`` counter) AND a completed fill on ANY purse inside
    the trailing ``ELIGIBILITY_FILL_WINDOW_SECS`` (``recent_fills``, from ``recent_fill_hotkeys``).
    The per-purse reading — a lane is live only while its OWN backing delivered — is
    ``purse_active`` / ``direction_eligible``. ``recent_fills=None`` means the ledger is younger
    than the window (fresh validator DB) and the gate is strikes-only. No warm-up count.

    The settlement exclusion moved per-hub in v3.1: see ``direction_eligible``,
    which zeroes only the settling hub's contribution instead of the whole miner."""
    del now  # strikes are time-free; kept in the signature for its many call sites
    if int(miner_state.failed_swaps) > MAX_FAILED_SWAPS:
        return False
    if recent_fills is None:
        return True
    return any(hotkey in hotkeys for hotkeys in recent_fills.values())


def purse_active(hotkey: Optional[str], backing: str, recent_fills: Optional[RecentFills]) -> bool:
    """Whether ``hotkey`` delivered a fill drawing on ``backing`` inside the activity window.
    None (young ledger) reads active."""
    if recent_fills is None:
        return True
    return hotkey in recent_fills.get(backing, ())


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


def direction_eligible(
    miner_state,
    from_chain: str,
    to_chain: str,
    now: int,
    backing: Optional[str] = None,
    *,
    hotkey: Optional[str] = None,
    recent_fills: Optional[RecentFills] = None,
) -> bool:
    """Per-lane gate: the global strikes AND the lane's own hub not mid-settle AND that hub
    active (a fill drawing on it inside the activity window). ``backing`` names
    the lane (V-2 fix, shipped with the F4 dual-backing lanes): a miner mid-TAO-settle is zeroed on
    the (sol↔tao, tao) lane only — the honest SOL-backed lane keeps earning, and the exclusion
    self-clears at the deadline, matching the contract's per-hub ``check_entry_gates``.

    ``backing=None`` is the pair-level reading — eligible while ANY declarable lane's hub is clean.
    A spoke pair names exactly one hub, so the two readings agree there."""
    if not is_eligible(miner_state, now, hotkey=hotkey, recent_fills=recent_fills):
        return False
    if backing is not None:
        return (
            backing in declarable_backings(from_chain, to_chain)
            and hub_free(miner_state, backing, now)
            and purse_active(hotkey, backing, recent_fills)
        )
    hubs = [c for c in (from_chain, to_chain) if c in BACKING_BITS]
    if not hubs:
        return True
    return any(hub_free(miner_state, hub, now) and purse_active(hotkey, hub, recent_fills) for hub in hubs)
