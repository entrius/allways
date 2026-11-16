"""Where the validator's weight goes: all of it on the treasury UID, or on the subnet owner's UID (burned)."""

from typing import List, Optional, Sequence, Tuple


def select_target(
    hotkeys: Sequence[str],
    netuid: int,
    treasury_netuid: int,
    treasury_uid: Optional[int],
    treasury_hotkey: str,
    owner_hotkey: Optional[str],
) -> Tuple[Optional[int], Optional[str]]:
    """Return ``(uid, reason)``: the treasury UID with ``None``, the burn UID with why the treasury was not used,
    or ``None`` with why no weights can be set at all.

    ``hotkeys`` is the metagraph's hotkey list, indexed by UID. The burn UID is wherever the subnet owner hotkey is
    registered: weight placed there is burned by the chain.
    """
    if netuid != treasury_netuid:
        reason = f'netuid {netuid} is not {treasury_netuid}, the subnet the treasury constants are pinned for'
    # bool is an int subclass: True would otherwise silently pin UID 1.
    elif treasury_uid is None or isinstance(treasury_uid, bool) or not isinstance(treasury_uid, int):
        reason = 'TREASURY_UID is unset'
    elif not 0 <= treasury_uid < len(hotkeys):
        reason = f'TREASURY_UID {treasury_uid} is out of range (subnet has {len(hotkeys)} UIDs)'
    elif hotkeys[treasury_uid] != treasury_hotkey:
        reason = (
            f'hotkey mismatch at TREASURY_UID {treasury_uid}: '
            f'chain has {hotkeys[treasury_uid]}, expected {treasury_hotkey}'
        )
    else:
        return treasury_uid, None

    if not owner_hotkey or owner_hotkey not in hotkeys:
        return None, f'{reason}, and the subnet owner hotkey {owner_hotkey} is not registered on netuid {netuid}'
    return list(hotkeys).index(owner_hotkey), reason


def weight_vector(target_uid: int) -> Tuple[List[int], List[float]]:
    """The ``(uids, weights)`` passed to set_weights: everything on one UID."""
    return [target_uid], [1.0]
