"""Bond attestation maintenance (vault → Solana).

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
from allways.solana.layouts import compose_attestation_epoch, split_attestation_epoch


def _epoch_str(epoch: int) -> str:
    """Composed epochs are opaque as one integer — render both halves."""
    generation, lock_epoch = split_attestation_epoch(epoch)
    return f'{lock_epoch} (gen {generation})'


@dataclass(frozen=True)
class Attested:
    effective_balance: int
    locked: bool
    epoch: int

    @property
    def is_empty(self) -> bool:
        """No bond on the vault at all — as opposed to a bond that nets to zero, which is a real
        assertion (it says "this miner is spoken for") and must still be written. Reads the VAULT
        half of the epoch: a fresh vault starts at lock epoch 0 whatever the generation."""
        return not self.locked and self.effective_balance == 0 and split_attestation_epoch(self.epoch)[1] == 0

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
    gross, lock = relay.vault.get_bond_state(hotkey)
    # No bond nets to zero whatever the fee state, so the settled counter can't change the answer:
    # skip its read. Most bound miners never bond, and the reconcile pass walks every one of them.
    settled = relay.settled_total(hotkey) if gross else 0
    if gross is None or settled is None or lock is None:
        bt.logging.warning(f'relay: vault reads unavailable for {hotkey[:8]}… — leaving its attestation alone')
        return None
    # The on-chain counter, scoped to THIS vault: the retired one's settled counter is gone, so its
    # accruals against a fresh `settled` would re-charge fees it already collected.
    accrued = relay.accrued_fees(miner)
    if accrued is None:
        bt.logging.warning(f'relay: fee counter unreadable for {miner[:8]}… — leaving its attestation alone')
        return None
    generation = relay.vault_generation()
    if generation is None:
        return None
    # Fees the miner has earned the protocol but the vault has not yet booked. The vault debits at
    # settle time; Solana's guards must see the miner clipped from the moment the fee is earned.
    unsettled = max(0, accrued - settled)
    pending = sum(int(row['penalty']) for row in relay.store.open_relay_slashes(relay.backing, miner))
    locked, epoch = lock
    # A replacement vault restarts its lock epochs at 0, which the on-chain monotonic guard would
    # read as stale forever. Compose the generation into the high half so epochs stay globally
    # monotonic across the swap.
    return Attested(
        max(0, gross - unsettled - pending),
        bool(locked),
        compose_attestation_epoch(generation, int(epoch)),
    )


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
        relay.pace()
        try:
            outcome = _write_one(relay, miner, hotkey, now, reconciling)
        except Exception as e:
            bt.logging.warning(f'relay: attestation for {miner[:8]} failed: {e}')
            outcome, ok = False, False
        if outcome:
            relay._dirty.discard(miner)
        else:
            ok = False
    return ok


def _note_reflected(relay, miner: str, desired: Attested, current: Any) -> None:
    """Release the entry hold of vault-applied slashes once Solana no longer overstates the bond.
    ``desired`` was read off the vault after the seizure, so an attestation at or under it (or none at
    all, which refuses entry on its own) can't back a swap with collateral that was taken."""
    if not relay.store.unreflected_relay_slashes(relay.backing, miner):
        return
    if current is not None and int(current.effective_balance) > desired.effective_balance:
        return
    relay.store.mark_relay_slashes_reflected(relay.backing, miner)
    bt.logging.info(f'relay: {miner[:8]} attestation reflects its applied slashes — entry hold released')


def _write_one(relay, miner: str, hotkey: str, now: int, reconciling: bool) -> bool:
    """Queue the vote when the attestation drifted. Landing it is the actuator's job: a write the program
    refuses while the hub is held stays in its inbox until the hub frees."""
    desired = compute(relay, miner, hotkey)
    if desired is None:
        return False
    current = relay.attestation(miner)
    _note_reflected(relay, miner, desired, current)
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
    payload = (str(miner), relay.backing, desired.effective_balance, desired.locked, desired.epoch)
    relay.vote('vote_set_attestation', (str(miner), relay.backing), payload)
    relay.note_vote(key, now)
    bt.logging.info(
        f'relay: attested {miner[:8]} {relay.backing} balance={desired.effective_balance} '
        f'locked={desired.locked} epoch={_epoch_str(desired.epoch)}{" (reconcile)" if reconciling else ""}'
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
