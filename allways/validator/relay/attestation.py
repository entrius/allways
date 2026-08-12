"""Job 2 — bond attestation maintenance (vault → Solana).

The attestation is an ASSERTION by the quorum, not a reflection of the vault:

    effective bond = vault gross − unsettled protocol fees − voted-but-unapplied slash verdicts

Both subtractions are written at VERDICT time, not application time — validators do not wait for
the vault to process what they themselves voted — so the attestation deliberately leads the vault
in the pessimistic direction and the reconciler converges them. That is what closes the
previous-slash-still-relaying hole: Solana frees the miner the instant the timeout quorum lands,
the vault seizure follows minutes later, and without netting-at-verdict a new swap could open
against phantom bond in between.

Writes are EVENT-DRIVEN ONLY. An idle miner's attestation is old-but-correct, so nothing rewrites
it; global liveness is the heartbeat's job, not this one's.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import bittensor as bt

from allways import dev_signal
from allways.solana.client import benign_marker

# A vote can race a peer's identical vote, or the round's staleness clock by a second or two.
# The program's AlreadyVoted is an authoritative no-op, not a failure (same rule as floor_sweep).
_BENIGN_VOTE_MARKERS = ('AlreadyVoted',)

# The program refuses a DOWNWARD attestation write while the miner's hub is held (F5 — the write
# would strand a live swap's reserve). That refusal is DEFERRED, not owed: the program guarantees the
# write can't matter until the hub frees, so it must not hold the startup reconcile barrier down —
# but the miner stays dirty so the write retries the moment the hub clears. See `flush`/`_write_one`.
_HELD_HUB_MARKERS = ('AttestationWouldStrandSwap',)

# Sentinel `_write_one` return: written attempt refused as deferred (see _HELD_HUB_MARKERS). Distinct
# from True (settled → drop from dirty) and False (owed/transient → keep dirty AND fail the barrier).
_DEFERRED = object()


@dataclass(frozen=True)
class Attested:
    effective_balance: int
    locked: bool
    epoch: int

    @property
    def is_empty(self) -> bool:
        """No bond on the vault at all — as opposed to a bond that nets to zero, which is a real
        assertion (it says "this miner is spoken for") and must still be written."""
        return not self.locked and self.effective_balance == 0 and self.epoch == 0

    def matches(self, account: Any) -> bool:
        return (
            account is not None
            and int(account.effective_balance) == self.effective_balance
            and bool(account.locked) == self.locked
            and int(account.epoch) == self.epoch
        )


def compute(relay, miner: str, hotkey: str) -> Optional[Attested]:
    """The effective bond to assert for this miner, or None when the vault can't be read.

    None is "unknown", never "zero": a node that can't decode a dry-run must not be allowed to
    attest a bond away."""
    gross = relay.vault.get_collateral(hotkey)
    settled = relay.vault.get_settled_total(hotkey)
    lock = relay.vault.get_lock_state(hotkey)
    if gross is None or settled is None or lock is None:
        bt.logging.warning(f'relay: vault reads unavailable for {hotkey[:8]}… — leaving its attestation alone')
        return None
    accrued = relay.store.accrued_fee_total(miner, relay.backing)
    # Fees the miner has earned the protocol but the vault has not yet booked. The vault debits at
    # settle time; Solana's guards must see the miner clipped from the moment the fee is earned.
    unsettled = max(0, accrued - settled)
    pending = sum(int(row['penalty']) for row in relay.store.open_relay_slashes(relay.backing, miner))
    locked, epoch = lock
    return Attested(max(0, gross - unsettled - pending), bool(locked), int(epoch))


def flush(relay, now: int, reconciling: bool = False) -> bool:
    """Write every dirty miner's attestation that has drifted. Returns True when nothing was left
    owed — the reconcile barrier's pass condition. A miner is dropped from the dirty set only once
    it is settled or provably unwritable, so a transient fault is retried next tick."""
    ok = True
    for miner in sorted(relay._dirty):
        hotkey = relay.hotkey_for(miner)
        if hotkey is None:
            # Unbound: there is no vault identity to read, so there is nothing to assert. The
            # binding can land later; the reconcile pass will pick it up then.
            relay._dirty.discard(miner)
            continue
        try:
            outcome = _write_one(relay, miner, hotkey, now, reconciling)
        except Exception as e:
            bt.logging.warning(f'relay: attestation for {miner[:8]} failed: {e}')
            outcome, ok = False, False
        if outcome is _DEFERRED:
            # Refused while the hub is held (F5). Not owed — the program guarantees the write can't
            # matter until the hub frees — so it does NOT fail the barrier, but the miner stays dirty
            # so the write retries once the hub clears. A fleet restart mid-swap no longer wedges here.
            continue
        if outcome:
            relay._dirty.discard(miner)
        else:
            ok = False
    return ok


def _write_one(relay, miner: str, hotkey: str, now: int, reconciling: bool) -> Any:
    desired = compute(relay, miner, hotkey)
    if desired is None:
        return False
    current = relay.solana.get_bond_attestation(miner, relay.backing)
    if desired.matches(current):
        return True
    if current is None and desired.is_empty:
        # A bound miner who never posted a bond. There is nothing to assert, and writing it would
        # open a rent-paying account per registered miner for the sake of saying "zero".
        return True
    key = f'attest:{miner}:{desired.effective_balance}:{desired.locked}:{desired.epoch}'
    if relay.throttled(key, now):
        return True  # our vote is already in a live round for exactly this payload
    if not relay.can_write():
        return False
    relay.note_write()
    if relay.read_only:
        bt.logging.info(
            f'relay: WOULD vote_set_attestation {miner[:8]} '
            f'balance={desired.effective_balance} locked={desired.locked} epoch={desired.epoch} (read-only)'
        )
        relay.note_vote(key, now)
        return True
    try:
        relay.solana.vote_set_attestation(
            miner, relay.backing, desired.effective_balance, desired.locked, desired.epoch
        )
    except Exception as e:
        if benign_marker(e, _HELD_HUB_MARKERS):
            # Downward write refused while the hub is held. Throttle so we don't retry (or log) every
            # tick, and defer — the reconcile barrier must not wedge on a write the program itself
            # guarantees is immaterial until the hub frees. The miner stays dirty for a later retry.
            relay.note_vote(key, now)
            bt.logging.debug(f'relay: attestation for {miner[:8]} deferred — hub held, retrying once it frees')
            return _DEFERRED
        if not benign_marker(e, _BENIGN_VOTE_MARKERS):
            raise
        bt.logging.debug(f'relay: attestation vote for {miner[:8]} already recorded')
    relay.note_vote(key, now)
    bt.logging.info(
        f'relay: attested {miner[:8]} {relay.backing} balance={desired.effective_balance} '
        f'locked={desired.locked} epoch={desired.epoch}{" (reconcile)" if reconciling else ""}'
    )
    dev_signal.emit(
        'relay_attestation',
        miner=miner,
        backing=relay.backing,
        effective_balance=desired.effective_balance,
        locked=desired.locked,
        epoch=desired.epoch,
    )
    return True
