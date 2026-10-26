"""The slash relay (Solana → vault), with netting at verdict.

``timeout_swap`` already emits a self-contained verdict: for an off-chain backing it moves nothing
and carries the ABSOLUTE penalty/reimbursement figures. This job carries that tuple to the vault's
second quorum verbatim — never reconstructed from state, because every argument is hash-bound into
the vault round, so a recomputed figure conflicts with its peers instead of co-counting with them.

Ordering: the netted attestation is queued FIRST (``engine`` sequences the flush between
``arm_verdicts`` and ``relay_verdicts``), before the vault has applied anything. Verdicts the vault has
ALREADY applied are settled before that flush (``settle_applied``), so they are never netted twice. Idempotency comes
from the vault's permanent ``swap_ref`` marker — checked before voting, and re-checked after a
revert, since pallet-contracts flattens every ink! ``Err`` into one ``ContractReverted``.
"""

from __future__ import annotations

from typing import Any, Dict

import bittensor as bt

from allways import dev_signal
from allways.validator.runtime.logs import vote_line
from allways.validator.world.binding import hotkey_ss58
from allways.vault import codec


def record_verdict(relay, fields: Dict[str, Any], block_time: int) -> None:
    """Persist a ``SwapTimedOut`` verdict as an obligation. The verdict names its payee and the hotkey the
    swap pinned at initiate, so it is relayed from the event alone (a rebind can't move the seizure)."""
    swap_key = bytes(fields['swap_key']).hex()
    miner = str(fields['miner'])
    raw = bytes(fields.get('hotkey') or b'')
    hotkey = hotkey_ss58(raw) if any(raw) else None  # all zeros: a pre-pin swap or never-bound miner
    # A verdict without a payee is still recorded: it keeps netting and blocks the miner's initiates.
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
        hotkey,
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
    # A seizure the attestation doesn't show yet is re-read every tick until it does: that is what
    # releases the miner's entry hold (``has_pending_debit``).
    for row in relay.store.unreflected_relay_slashes(relay.backing):
        relay.mark_dirty(row['miner'])


def settle_applied(relay) -> None:
    """Close every open verdict whose seizure the vault already shows, before the attestation nets it.
    The vault's gross is already net of an applied seizure, so netting it again double-counts it: a
    validator that replays history on a fresh store (a new or rebuilt one) would otherwise attest a
    bond the slash already paid twice over, down to zero. A read, not a vote, so it runs whether or not
    this validator is in the vault's set. An unreadable marker leaves the verdict open (netted)."""
    for row in relay.store.open_relay_slashes(relay.backing):
        if relay.vault.is_slashed(row['swap_key']):
            _settle(relay, row, 'already applied on the vault')


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
    # The verdict's pinned hotkey is authoritative; the live binding only covers pre-pin verdicts.
    hotkey = row.get('hotkey') or relay.hotkey_for(miner)
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
    vote_line('vault', 'slash', swap_key, (hotkey, int(row['penalty']), row['user_addr'], int(row['reimbursement'])))
    result = relay.vault.vote_slash(hotkey, swap_key, int(row['penalty']), row['user_addr'], int(row['reimbursement']))
    relay.note_vote(f'slash:{swap_key}', now)
    if result.ok:
        bt.logging.success(f'relay: vote_slash cast for {hotkey[:8]} swap {swap_key[:16]}')
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
    """Close the vault obligation: the debit is real on the vault now, so it stops being netted off the
    attestation. It keeps blocking the miner's initiates until Solana's attestation reflects it."""
    relay.store.mark_relay_slash_applied(row['swap_key'])
    relay.mark_dirty(row['miner'])
    bt.logging.info(f'relay: verdict {row["swap_key"][:16]} settled — {why}')
    dev_signal.emit('relay_slash_settled', swap_key=row['swap_key'], miner=row['miner'], reason=why)
