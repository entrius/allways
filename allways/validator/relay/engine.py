"""BondRelay — the relayer's clock, its ledger of obligations, and the restart barrier.

One instance per validator, ticked once per forward step. It holds the shared context the three
jobs run against (Solana client, vault client, state store, pubkey↔hotkey attribution) and
sequences them; the jobs themselves live in the sibling modules and take the relay as their first
argument, the way ``floor_sweep`` / ``forward`` take the validator.

**Ordering is the safety property.** Within a tick, attestation writes go out before the vault
writes they anticipate — the mirror must lead the vault pessimistically, never trail it. Across a
restart, the reconcile barrier drains every owed write before the heartbeat resumes, so the
dead-man fuse keeps TAO entry shut for exactly as long as this validator is behind. The fuse being
born closed after the v3 migration is the same mechanism, not a bug to work around.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

import bittensor as bt

from allways import dev_signal
from allways.constants import (
    RELAY_FEE_CADENCE_SECS,
    RELAY_HEARTBEAT_INTERVAL_SECS,
    RELAY_QUIESCENCE_GRACE_SECS,
    RELAY_RECONCILE_INTERVAL_SECS,
    RELAY_SWAP_RETENTION_SECS,
    VOTE_ROUND_TTL_SECS,
)
from allways.solana import pdas
from allways.validator.binding import build_attribution
from allways.validator.relay import attestation as attestation_job
from allways.validator.relay import exit_relay
from allways.validator.relay import slash as slash_job

# Vault events that move a miner's bond or its books — each one makes that miner's attestation
# stale. Polling them is a latency optimisation only: the reconcile loop re-derives the same
# facts from state, so a missed or undecodable event costs freshness, never correctness.
VAULT_DIRTYING_EVENTS = {
    'CollateralPosted': 'miner',
    'CollateralWithdrawn': 'miner',
    'BondLockChanged': 'miner',
    'FeesSettled': 'miner',
    'MinerSlashed': 'miner',
}

_ATTRIBUTION_TTL_SECS = 300
_VAULT_CURSOR_KEY = 'vault_event_block'
_EXIT_KEY_PREFIX = 'exit:'


@dataclass(frozen=True)
class RelayConfig:
    """Relayer cadences. Production defaults live in ``constants``; a dev stack overrides them
    through the env because it has to watch a whole bond lifecycle inside one test run."""

    heartbeat_interval_secs: int = RELAY_HEARTBEAT_INTERVAL_SECS
    fee_cadence_secs: int = RELAY_FEE_CADENCE_SECS
    reconcile_interval_secs: int = RELAY_RECONCILE_INTERVAL_SECS
    quiescence_grace_secs: int = RELAY_QUIESCENCE_GRACE_SECS
    swap_retention_secs: int = RELAY_SWAP_RETENTION_SECS
    vote_retry_secs: int = VOTE_ROUND_TTL_SECS
    # Vault writes per tick. Each is an extrinsic waiting on block inclusion, and the relay rides
    # the forward pass — a large backlog has to drain across ticks, not stall the forward watchdog.
    max_writes_per_tick: int = 8

    @classmethod
    def from_env(cls) -> 'RelayConfig':
        def secs(name: str, default: int) -> int:
            raw = os.environ.get(name)
            try:
                return max(1, int(raw)) if raw else default
            except ValueError:
                bt.logging.warning(f'relay: {name}={raw!r} is not an integer; using {default}')
                return default

        return cls(
            heartbeat_interval_secs=secs('ALLWAYS_RELAY_HEARTBEAT_SECS', RELAY_HEARTBEAT_INTERVAL_SECS),
            fee_cadence_secs=secs('ALLWAYS_RELAY_FEE_CADENCE_SECS', RELAY_FEE_CADENCE_SECS),
            reconcile_interval_secs=secs('ALLWAYS_RELAY_RECONCILE_SECS', RELAY_RECONCILE_INTERVAL_SECS),
            quiescence_grace_secs=secs('ALLWAYS_RELAY_QUIESCENCE_GRACE_SECS', RELAY_QUIESCENCE_GRACE_SECS),
            vote_retry_secs=secs('ALLWAYS_RELAY_VOTE_RETRY_SECS', VOTE_ROUND_TTL_SECS),
            max_writes_per_tick=secs('ALLWAYS_RELAY_MAX_WRITES', 8),
        )


class BondRelay:
    def __init__(
        self,
        solana_client: Any,
        vault: Any,
        state_store: Any,
        backing: str = pdas.BACKING_CHAIN_TAO,
        read_only: bool = False,
        clock: Optional[Callable[[], float]] = None,
        config: Optional[RelayConfig] = None,
        config_fn: Optional[Callable[[], Any]] = None,
    ):
        self.solana = solana_client
        self.vault = vault
        self.store = state_store
        self.backing = backing
        self.backing_bit = pdas.BACKING_BITS.get(backing, 0)
        self.read_only = read_only
        self.clock = clock or time.time
        self.cfg = config or RelayConfig()
        self._config_fn = config_fn or solana_client.get_config

        # Miners whose attestation needs recomputing. Event-driven ONLY: an idle miner never
        # enters this set, so it never gets a write (its old attestation is old-but-correct).
        self._dirty: Set[str] = set()
        # Miners whose purse was DEACTIVATED and whose bond is still locked. Armed by the event
        # (and persisted — see arm_exit), never inferred from the bit being down.
        self._exiting: Set[str] = set()
        self._reconciled = False
        self._next_reconcile = 0.0
        self._attribution: Dict[str, str] = {}
        self._attribution_at = 0.0
        self._last_heartbeat_attempt = 0.0
        # Per-subject re-vote throttles, so an unlanded round isn't re-submitted every 12s pass.
        self._voted_at: Dict[str, float] = {}
        self._writes = 0

    # ─── attribution (Solana pubkey ↔ Bittensor hotkey, via the A5 binding) ──

    def attribution(self, refresh: bool = False) -> Dict[str, str]:
        now = self.clock()
        if refresh or not self._attribution or now - self._attribution_at >= _ATTRIBUTION_TTL_SECS:
            try:
                self._attribution = build_attribution(self.solana)
                self._attribution_at = now
            except Exception as e:
                bt.logging.warning(f'relay: attribution refresh failed ({e}); using the previous map')
        return self._attribution

    def hotkey_for(self, miner: str) -> Optional[str]:
        """The vault keys bonds by hotkey (D3); every vault call for a miner goes through here.
        None means unbound — there is no bond to reason about, so the relay leaves it alone."""
        return self.attribution().get(str(miner))

    def miner_for(self, hotkey: str) -> Optional[str]:
        for pubkey, hk in self.attribution().items():
            if hk == hotkey:
                return pubkey
        return None

    # ─── inputs from the swap loop + the event ingest ────────────────────────

    def observe_swap(self, swap: Any) -> None:
        """Snapshot a live off-chain-backed swap's reimbursement target.

        The verdict event does not carry the user's backing-chain address and the Swap PDA closes
        the moment it lands, so this is the ONLY window in which the ``vote_slash`` payee can be
        read. Called for every live swap the loop walks, hence the first-sighting-wins insert."""
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if backing != self.backing:
            return
        addr = self.user_backing_address(swap)
        if not addr:
            return
        try:
            self.store.record_relay_swap(
                _hex(swap.swap_key), str(swap.miner), backing, addr, int(self.clock())
            )
        except Exception as e:
            bt.logging.warning(f'relay: could not snapshot swap facts: {e}')

    def user_backing_address(self, swap: Any) -> str:
        """The user's address on the collateral chain — whichever leg is denominated in it.
        Needs no new on-chain field: both legs are pinned at finalize (the D5 verification)."""
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if str(getattr(swap, 'from_chain', '')).lower() == backing:
            return str(getattr(swap, 'user_from_addr', '') or '')
        if str(getattr(swap, 'to_chain', '')).lower() == backing:
            return str(getattr(swap, 'user_to_addr', '') or '')
        return ''

    def has_pending_debit(self, miner: str) -> bool:
        """Off-chain backstop to busy-until-settled: while a miner owes the vault an unapplied
        debit, this validator refuses to vote a new off-chain-backed initiate for it. Covers the
        seconds-wide window the on-chain settlement grace can't, and any relay-slower-than-grace
        tail."""
        try:
            return bool(self.store.open_relay_slashes(self.backing, str(miner)))
        except Exception as e:
            bt.logging.warning(f'relay: pending-debit check failed for {str(miner)[:8]} ({e}); assuming owed')
            return True  # fail closed: an unreadable ledger must not open a new swap

    def ingest_events(self, records: List[Any]) -> None:
        """Fold Solana program events into the relay ledger. Runs beside (not inside) the crown
        index: this one keys by Solana pubkey and keeps events from unbound miners, because a
        binding can land after the swap did."""
        for rec in records:
            try:
                self._ingest_event(rec)
            except Exception as e:
                bt.logging.warning(f'relay: could not ingest {getattr(rec, "name", "?")}: {e}')

    def _ingest_event(self, rec: Any) -> None:
        name = rec.name
        fields = rec.fields
        block_time = rec.block_time
        if name == 'SwapCompleted':
            if str(fields['collateral_chain']).lower() != self.backing or block_time is None:
                return
            # Absolute rao figure (the confirm-events seam W2b landed): Solana moved nothing, the
            # fee is owed on the vault and nets off the effective bond until it settles there.
            miner = str(fields['miner'])
            self.store.record_relay_fee(
                _hex(fields['swap_key']), miner, self.backing, int(fields['fee']), int(block_time)
            )
            self.mark_dirty(miner)
        elif name == 'SwapTimedOut':
            if str(fields['collateral_chain']).lower() != self.backing or block_time is None:
                return
            slash_job.record_verdict(self, fields, int(block_time))
        elif name == 'MinerBackingChanged':
            if str(fields['backing']).lower() != self.backing:
                return
            miner = str(fields['miner'])
            self.mark_dirty(miner)
            if fields['enabled']:
                self.disarm_exit(miner)
            else:
                # TAO-side deactivation: one-way until re-lock, so quiescence is now a stable
                # fact rather than a snapshot. The exit sequence takes it from here.
                self.arm_exit(miner)

    def mark_dirty(self, miner: str) -> None:
        self._dirty.add(str(miner))

    # ─── the tick ────────────────────────────────────────────────────────────

    def step(self, now: Optional[int] = None) -> None:
        """One forward-step tick. Never raises: a relay hiccup must not break the swap loop."""
        now = int(now if now is not None else self.clock())
        self._writes = 0
        try:
            if not self._reconciled:
                # STARTUP BARRIER. Until every owed write is submitted, the heartbeat stays down
                # and the fuse keeps TAO entry closed. Obligations are discharged by SUBMITTING
                # our vote, not by waiting for quorum — waiting would deadlock a fleet-wide
                # restart, where every validator is behind at once.
                if not self.reconcile(now):
                    bt.logging.info('relay: startup reconcile still draining — heartbeat held down')
                    return
                self._reconciled = True
                bt.logging.success('relay: startup reconcile clean — heartbeat released')
                dev_signal.emit('relay_reconciled', backing=self.backing)

            self.poll_vault_events()
            slash_job.arm_verdicts(self, now)
            attestation_job.flush(self, now)
            slash_job.relay_verdicts(self, now)
            exit_relay.run_exits(self, now)
            exit_relay.maybe_cadence_settle(self, now)
            self.maybe_heartbeat(now)
            self.maybe_reconcile(now)
        except Exception as e:
            bt.logging.warning(f'relay: step failed: {e}')

    def maybe_reconcile(self, now: int) -> None:
        if now < self._next_reconcile:
            return
        self._next_reconcile = now + self.cfg.reconcile_interval_secs
        self.reconcile(now)

    def reconcile(self, now: int) -> bool:
        """Full vault↔attestation diff plus a drain of every pending obligation.

        Returns True when nothing was left undone — the condition the startup barrier waits on.
        Repairs the one hole the event path structurally can't cover: a crash BETWEEN paired
        writes (vault mutated, attestation refresh never sent)."""
        # Rediscover WHO exists before asking what they owe: the bound set is the iteration set
        # below, so a cached map would hide a miner that bound since the last pass for a whole TTL
        # — including one that bound and bonded in the same minute.
        self.attribution(refresh=True)
        bonded = self.bonded_miners()
        for miner in bonded:
            self.mark_dirty(miner)
        ok = attestation_job.flush(self, now, reconciling=True)
        slash_job.arm_verdicts(self, now)
        ok = slash_job.relay_verdicts(self, now) and ok
        self._refresh_exiting(bonded)
        ok = exit_relay.run_exits(self, now) and ok
        try:
            self.store.prune_relay_swaps(now - self.cfg.swap_retention_secs)
        except Exception as e:
            bt.logging.warning(f'relay: prune failed: {e}')
        return ok

    def bonded_miners(self) -> List[str]:
        """Every miner the relay could owe a write for.

        Deliberately the whole BOUND set, not just the attested one: a miner's first bond is
        posted and locked on the vault before any attestation exists, and if that pair of vault
        events lands while this validator is down, nothing else would ever discover it. Enumerating
        bindings makes the reconcile loop the backstop for entry as well as for repair. A bound
        miner with no bond costs three vault reads and produces no write (see
        ``attestation._write_one``)."""
        miners: Set[str] = set(self.attribution())
        try:
            for _pda, att in self.solana.get_all('BondAttestation'):
                if str(getattr(att, 'chain', self.backing)).lower() == self.backing:
                    miners.add(str(att.miner))
        except Exception as e:
            bt.logging.warning(f'relay: attestation scan failed ({e}); reconciling the ledger only')
        try:
            miners.update(row['miner'] for row in self.store.open_relay_slashes(self.backing))
            miners.update(self.store.accrued_fee_totals(self.backing))
        except Exception as e:
            bt.logging.warning(f'relay: ledger scan failed: {e}')
        return sorted(miners)

    def arm_exit(self, miner: str) -> None:
        """Record a DEACTIVATION. This is persisted because on-chain a miner that deactivated and
        one that never activated are the same state — bit down, bond locked — and unlocking the
        second would release a bond that was only waiting to enter service. Failing to unlock is a
        support ticket; unlocking what shouldn't be is a money-safety hole, so the ambiguity
        resolves toward doing nothing."""
        miner = str(miner)
        self._exiting.add(miner)
        self.store.set_relay_meta(f'{_EXIT_KEY_PREFIX}{miner}', '1')
        bt.logging.info(f'relay: {miner[:8]} deactivated its {self.backing} purse — exit sequence armed')

    def disarm_exit(self, miner: str) -> None:
        miner = str(miner)
        self._exiting.discard(miner)
        self.store.delete_relay_meta(f'{_EXIT_KEY_PREFIX}{miner}')

    def _refresh_exiting(self, bonded: List[str]) -> None:
        """Reload the armed exits and drop any whose purse came back. Only ever REMOVES from the
        set — see ``arm_exit`` for why the arming half can't be derived from chain."""
        try:
            self._exiting.update(self.store.relay_meta_prefix(_EXIT_KEY_PREFIX))
        except Exception as e:
            bt.logging.warning(f'relay: could not reload the exit set: {e}')
        for miner in list(self._exiting):
            try:
                ms = self.solana.get_miner_state(miner)
            except Exception:
                continue
            if ms is not None and int(getattr(ms, 'active_backings', 0)) & self.backing_bit:
                self.disarm_exit(miner)

    # ─── heartbeat ───────────────────────────────────────────────────────────

    def maybe_heartbeat(self, now: int) -> None:
        """Bump the GLOBAL liveness heartbeat on a lazy cadence. Liveness is one question about
        the whole relay, not fifty questions about individual miners — so one round, and quiet
        miners can never false-positive a staleness fuse."""
        try:
            cfg = self._config_fn()
            last = int(getattr(cfg, 'last_attest_heartbeat', 0) or 0)
        except Exception as e:
            bt.logging.warning(f'relay: heartbeat config read failed: {e}')
            return
        if now - max(last, self._last_heartbeat_attempt) < self.cfg.heartbeat_interval_secs:
            return
        if self.read_only:
            bt.logging.info('relay: WOULD vote_attest_heartbeat (read-only)')
            self._last_heartbeat_attempt = now
            return
        try:
            if self.solana.has_voted(pdas.REQ_ATTEST_HEARTBEAT, pdas.config_pda(self.solana.program_id), self.me()):
                self._last_heartbeat_attempt = now
                return
            sig = self.solana.vote_attest_heartbeat()
        except Exception as e:
            bt.logging.warning(f'relay: heartbeat vote failed: {e}')
            return
        self._last_heartbeat_attempt = now
        bt.logging.info(f'relay: heartbeat vote cast ({sig[:16]}…)')
        dev_signal.emit('relay_heartbeat', sig=sig)

    def me(self):
        return self.solana.keypair.pubkey()

    # ─── vault event polling (freshness, never truth) ────────────────────────

    def poll_vault_events(self) -> None:
        head = self._vault_head()
        if head is None:
            return
        cursor = self.store.get_relay_meta(_VAULT_CURSOR_KEY)
        if cursor is None:
            # First run: start at the head. Anything earlier is already reflected in the state
            # the reconcile pass reads, so replaying history would buy nothing.
            self.store.set_relay_meta(_VAULT_CURSOR_KEY, head)
            return
        start = int(cursor) + 1
        if start > head:
            return
        try:
            events = self.vault.poll_events(start, head)
        except Exception as e:
            bt.logging.warning(f'relay: vault event poll failed: {e}')
            return
        for ev in events:
            field = VAULT_DIRTYING_EVENTS.get(ev.name)
            if field is None:
                continue
            miner = self.miner_for(str(ev.fields.get(field)))
            if miner is not None:
                self.mark_dirty(miner)
        self.store.set_relay_meta(_VAULT_CURSOR_KEY, head)

    def _vault_head(self) -> Optional[int]:
        try:
            return self.vault.head()
        except Exception as e:
            bt.logging.debug(f'relay: vault head read failed: {e}')
            return None

    # ─── vote throttle ───────────────────────────────────────────────────────

    def throttled(self, key: str, now: float) -> bool:
        """True while a round we already voted on is still within its on-chain lifetime — the same
        rule the weights vote uses, so an unlanded round is retried once it is reopenable rather
        than re-submitted every pass."""
        last = self._voted_at.get(key)
        return last is not None and now - last < self.cfg.vote_retry_secs

    def note_vote(self, key: str, now: float) -> None:
        self._voted_at[key] = now

    def can_write(self) -> bool:
        """Per-tick write budget. Exhausting it leaves the remaining obligations owed, which keeps
        the reconcile barrier closed until the backlog actually drains."""
        return self._writes < self.cfg.max_writes_per_tick

    def note_write(self) -> None:
        self._writes += 1


def _hex(value) -> str:
    return bytes(value).hex() if isinstance(value, (bytes, bytearray, list)) else str(value)


def maybe_relay(validator) -> None:
    """One never-raises relay tick off the validator's forward pass. Absent vault config, the
    relayer isn't constructed at all and this is a no-op — a SOL-only deployment pays nothing."""
    relay = getattr(validator, 'bond_relay', None)
    if relay is None:
        return
    relay.step()
