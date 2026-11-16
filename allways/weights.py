"""Where the validator's weight goes: all of it on the treasury UID, or on the burn UID when that is not safe."""

from typing import List, Optional, Sequence, Tuple


def select_target(
    hotkeys: Sequence[str],
    treasury_uid: Optional[int],
    treasury_hotkey: str,
    burn_uid: int,
) -> Tuple[int, Optional[str]]:
    """Return ``(uid, fallback_reason)``: the treasury UID with ``None``, or the burn UID with why.

    ``hotkeys`` is the metagraph's hotkey list, indexed by UID.
    """
    # An out-of-range burn UID (a small local subnet) falls back to UID 0.
    burn = burn_uid if 0 <= burn_uid < len(hotkeys) else 0
    # bool is an int subclass: True would otherwise silently pin UID 1.
    if treasury_uid is None or isinstance(treasury_uid, bool) or not isinstance(treasury_uid, int):
        return burn, 'TREASURY_UID is unset'
    if not 0 <= treasury_uid < len(hotkeys):
        return burn, f'TREASURY_UID {treasury_uid} is out of range (subnet has {len(hotkeys)} UIDs)'
    if hotkeys[treasury_uid] != treasury_hotkey:
        return burn, (
            f'hotkey mismatch at TREASURY_UID {treasury_uid}: '
            f'chain has {hotkeys[treasury_uid]}, expected {treasury_hotkey}'
        )
    return treasury_uid, None


def weight_vector(target_uid: int) -> Tuple[List[int], List[float]]:
    """The ``(uids, weights)`` passed to set_weights: everything on one UID."""
    return [target_uid], [1.0]
