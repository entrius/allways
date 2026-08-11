"""Job 1 — the slash relay (Solana → vault), with netting at verdict.

``timeout_swap`` already emits a self-contained verdict: for an off-chain backing it moves nothing
and carries the ABSOLUTE penalty/reimbursement figures. This job carries that tuple to the vault's
second quorum verbatim — never reconstructed from state, because every argument is hash-bound into
the vault round, so a recomputed figure conflicts with its peers instead of co-counting with them.

Ordering: the netted attestation is written FIRST (``engine`` sequences the flush between
``arm_verdicts`` and ``relay_verdicts``), before the vault has applied anything. Idempotency comes
from the vault's permanent ``swap_ref`` marker — checked before voting, and re-checked after a
revert, since pallet-contracts flattens every ink! ``Err`` into one ``ContractReverted``.
"""

from __future__ import annotations

from typing import Any, Dict

import bittensor as bt

from allways import dev_signal
from allways.vault import codec


def record_verdict(relay, fields: Dict[str, Any], block_time: int) -> None:
    """Persist a ``SwapTimedOut`` verdict as an obligation. The payee comes from the snapshot the
    swap loop took while the swap was live; failing that, from the verdict itself (W3.1), which is
    what lets a validator relay a swap it never saw."""
    swap_key = bytes(fields['swap_key']).hex()
    miner = str(fields['miner'])
    snapshot = relay.store.get_relay_swap(swap_key)
    user_addr = _payable(snapshot['user_addr']) if snapshot else ''
    if not user_addr:
        # No snapshot (fresh state DB, or down for the swap's whole life): the event names the payee
        # itself, so history alone is enough to discharge this. Older verdicts predate the field and
        # still land payee-less — recorded anyway, to keep netting and to block the miner's initiates.
        payee = str(fields.get('payee') or '')
        user_addr = _payable(payee) if payee else ''
    if not user_addr:
        bt.logging.error(
            f'relay: no reimbursement address for swap {swap_key[:16]} — this validator cannot '
            'relay its slash; peers that saw the swap live must carry the quorum'
        )
    relay.store.record_relay_slash(
        swap_key,
        miner,
        relay.backing,
        int(fields['penalty']),
        int(fields['reimbursement']),
        user_addr,
        block_time,
    )
    relay.mark_dirty(miner)
    bt.logging.info(
        f'relay: verdict queued for {miner[:8]} swap {swap_key[:16]} '
        f'penalty={int(fields["penalty"])} reimbursement={int(fields["reimbursement"])}'
    )
    dev_signal.emit(
        'relay_verdict_queued',
        swap_key=swap_key,
        miner=miner,
        penalty=int(fields['penalty']),
        reimbursement=int(fields['reimbursement']),
    )


def _payable(addr: str) -> str:
    """The address only counts as a payee if the vault could actually pay it. The program never
    validated the user's backing-chain address, so a malformed one reaches us intact; rejecting it
    once here beats raising on every pass forever."""
    try:
        codec.account_bytes(addr)
    except Exception:
        bt.logging.error(f'relay: reimbursement address {addr!r} is not a payable account id')
        return ''
    return addr


def arm_verdicts(relay, now: int) -> None:
    """Mark every miner with an open verdict dirty, so the netted attestation goes out ahead of
    the vault write in the same tick. This is the ordering invariant, expressed as scheduling."""
    for row in relay.store.open_relay_slashes(relay.backing):
        relay.mark_dirty(row['miner'])


def relay_verdicts(relay, now: int) -> bool:
    """Vote every open verdict onto the vault. Returns True when nothing was left owed."""
    ok = True
    for row in relay.store.open_relay_slashes(relay.backing):
        try:
            if not _relay_one(relay, row, now):
                ok = False
        except Exception as e:
            bt.logging.warning(f'relay: slash for swap {row["swap_key"][:16]} failed: {e}')
            ok = False
    return ok


def _relay_one(relay, row: Dict[str, Any], now: int) -> bool:
    swap_key = row['swap_key']
    miner = row['miner']
    hotkey = relay.hotkey_for(miner)
    if hotkey is None:
        bt.logging.warning(f'relay: {miner[:8]} has no hotkey binding — cannot slash its bond')
        return False

    applied = relay.vault.is_slashed(swap_key)
    if applied is None:
        return False  # unreadable vault: never guess whether a seizure already happened
    if applied:
        _settle(relay, row, 'already applied on the vault')
        return True
    if not row['user_addr']:
        return False  # no payee (see record_verdict) — a peer's quorum has to carry this one
    if relay.throttled(f'slash:{swap_key}', now):
        return True
    if not relay.can_write():
        return False
    relay.note_write()
    if relay.read_only:
        bt.logging.info(f'relay: WOULD vote_slash {hotkey[:8]} swap {swap_key[:16]} (read-only)')
        relay.note_vote(f'slash:{swap_key}', now)
        return True

    result = relay.vault.vote_slash(hotkey, swap_key, int(row['penalty']), row['user_addr'], int(row['reimbursement']))
    relay.note_vote(f'slash:{swap_key}', now)
    if result.ok:
        bt.logging.info(f'relay: vote_slash cast for {hotkey[:8]} swap {swap_key[:16]}')
        dev_signal.emit('relay_slash_voted', swap_key=swap_key, miner=miner, hotkey=hotkey)
        # Our vote may have been the one that reached quorum; the marker says which.
        if relay.vault.is_slashed(swap_key):
            _settle(relay, row, 'quorum applied the seizure')
        return True
    if result.reverted and relay.vault.is_slashed(swap_key):
        # AlreadySlashed reaches us as a bare ContractReverted — treat a set marker as success.
        _settle(relay, row, 'refused as already slashed')
        return True
    bt.logging.warning(f'relay: vote_slash rejected for swap {swap_key[:16]} ({result.error or "reverted"})')
    return False


def _settle(relay, row: Dict[str, Any], why: str) -> None:
    """Close the obligation: the debit is real on the vault now, so it stops being netted off the
    attestation and stops blocking the miner's initiates."""
    relay.store.mark_relay_slash_applied(row['swap_key'])
    relay.mark_dirty(row['miner'])
    bt.logging.info(f'relay: verdict {row["swap_key"][:16]} settled — {why}')
    dev_signal.emit('relay_slash_settled', swap_key=row['swap_key'], miner=row['miner'], reason=why)
