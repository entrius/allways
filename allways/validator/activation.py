"""Which of a miner's purses this validator will vote to activate — the validator half of W2's guards.

`vote_activate` forks on the backing. "sol" is gated on the local lamport collateral the program reads
for itself; an off-chain backing is gated on its BondAttestation plus the dead-man fuse. The program
re-checks every one of those at quorum, so nothing here is the enforcer. What this adds is the one fact
Solana cannot read — the vault itself — and a refusal the miner can act on instead of a doomed vote.

The vault read is not belt-and-braces. The attestation is the quorum's assertion and legitimately lags
a lock transition, so a bond that has just been unlocked (and is therefore withdrawable) still reads as
locked on Solana for a cadence. Activating into that window would put live swaps behind a bond its
owner can walk away with; the epoch check below is what closes it.
"""

from __future__ import annotations

from dataclasses import dataclass

from allways.cli.swap_commands.swap_intake import floors_from_config
from allways.solana import pdas
from allways.validator.relay.wiring import axon_vault_client


@dataclass(frozen=True)
class Eligibility:
    """A verdict on one purse. `reason` is miner-facing — it is the rejection the synapse carries."""

    ok: bool
    reason: str = ''


_OK = Eligibility(True)


def _no(reason: str) -> Eligibility:
    return Eligibility(False, reason)


def settles_locally(backing: str) -> bool:
    """Mirror of ``backing.rs::settles_locally`` — whether this purse lives on the program's own chain."""
    return backing == pdas.BACKING_CHAIN_SOL


def active_bits(miner_state) -> int:
    """The per-purse activation mask. `active` is only its OR view, so a dual-purse miner asking for
    its second purse must be read off the bits — the OR view would call it already active."""
    return int(getattr(miner_state, 'active_backings', 0) or 0)


def check(validator, miner_hotkey: str, miner_pk, miner_state, backing: str, now: int) -> Eligibility:
    """Whether this validator will vote to activate `backing` for this miner, and why not if it won't.
    Refuses rather than queues: the facts are about now, and a miner retrying beats a validator
    holding an obligation it may never be able to discharge."""
    backing = (backing or pdas.BACKING_CHAIN_SOL).lower()
    bit = pdas.BACKING_BITS.get(backing)
    if bit is None:
        return _no(f'Unknown backing "{backing}" — this subnet backs: {", ".join(pdas.BACKING_BITS)}')
    if active_bits(miner_state) & bit:
        return _no(f'Purse already active: {backing.upper()}')

    config = validator.solana_client.get_config()
    floor = floors_from_config(config).get(backing, 0)
    if settles_locally(backing):
        # Byte-identical to the pre-W2 SOL path, message included.
        if int(miner_state.collateral) < floor:
            return _no(f'Insufficient collateral: {miner_state.collateral} < {floor}')
        return _OK
    return _attested(validator, miner_hotkey, miner_pk, backing, floor, config, now)


def _attested(validator, miner_hotkey: str, miner_pk, backing: str, floor: int, config, now: int) -> Eligibility:
    """The off-chain-backing gate, in the order `vote_activate` applies it: fuse, then attestation."""
    unit = backing.upper()
    age = now - int(getattr(config, 'last_attest_heartbeat', 0) or 0)
    max_age = int(getattr(config, 'attest_max_age_secs', 0) or 0)
    if age > max_age:
        return _no(
            f'Bond attestation is fused off — the {unit} heartbeat is {age}s old (max {max_age}s). '
            f'Retry once the relay fleet is live again.'
        )

    attestation = validator.solana_client.get_bond_attestation(miner_pk, backing)
    if attestation is None:
        return _no(
            f'Bond attestation for your {unit} purse is missing. Validators mirror the vault on their '
            f'own cadence — post and lock your bond, then retry in a minute.'
        )
    if not attestation.locked:
        return _no(f'Bond attestation says your {unit} bond is UNLOCKED, and an unlocked bond backs nothing.')
    if int(attestation.effective_balance) < floor:
        return _no(
            f'Bond below the {unit} floor: {attestation.effective_balance} < {floor} (the attested figure '
            f'is your gross bond net of accrued fees and voted slashes).'
        )
    return _vault(validator, miner_hotkey, backing, attestation, floor)


def _vault(validator, miner_hotkey: str, backing: str, attestation, floor: int) -> Eligibility:
    """Verify the attestation against the bond it asserts. A stale mirror is the dangerous direction:
    it can only ever say a withdrawn bond is still there, never the reverse."""
    unit = backing.upper()
    vault = axon_vault_client(validator)
    if vault is None:
        return _no(f'Bond relay not configured on this validator, so it cannot verify your {unit} bond.')
    with validator.axon_lock:
        lock = vault.get_lock_state(miner_hotkey)
        gross = vault.get_collateral(miner_hotkey)
    if lock is None or gross is None:
        return _no(f'Bond could not be read off the {unit} vault right now — retry shortly.')

    locked, epoch = lock
    if not locked:
        return _no(f'Bond is not locked on the {unit} vault (the attestation has yet to catch up).')
    if int(epoch) != int(attestation.epoch):
        return _no(
            f'Bond attestation is stale — it asserts lock epoch {attestation.epoch}, the {unit} vault is at '
            f'{epoch}. Retry once validators have re-mirrored your bond.'
        )
    if int(gross) < floor:
        return _no(f'Bond on the {unit} vault is {gross} < {floor}.')
    return _OK
