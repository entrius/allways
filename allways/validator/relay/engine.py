"""BondRelay — one hub's relayer: its clock, its ledger of obligations, and the restart barrier.

Solana is the transaction ledger; the hub's vault is the collateral, and neither can read the other.
The relay carries verdicts between them in three jobs (sibling modules, each taking the relay):

* **slash** (Solana → vault): a timeout verdict becomes ``vote_slash``, netted into the attestation first.
* **attestation** (vault → Solana): the effective bond Solana's guards read, event-driven only.
* **exits** (Solana → vault): the quiescence-gated residual settle and unlock, and the fee true-up.

Solana is read from the World and written through ``vote`` (the actuator's inbox); the vault is
this relay's own client, read and written only from the relay's thread.

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
    RELAY_BALANCE_CHECK_SECS,
    RELAY_FEE_CADENCE_SECS,
    RELAY_HEARTBEAT_INTERVAL_SECS,
    RELAY_LOW_BALANCE_RAO,
    RELAY_MINER_READ_INTERVAL_SECS,
    RELAY_QUIESCENCE_GRACE_SECS,
    RELAY_RECONCILE_INTERVAL_SECS,
    RELAY_SWAP_RETENTION_SECS,
    VOTE_ROUND_TTL_SECS,
)
from allways.solana import pdas
from allways.validator.relay import attestation as attestation_job
from allways.validator.relay import exits, stop_point
from allways.validator.relay import slash as slash_job
from allways.validator.runtime.logs import Episodes
from allways.validator.world.snapshot import World

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

_VAULT_CURSOR_KEY = 'vault_event_block'
_VAULT_POLL_MAX_BLOCKS = 50  # two substrate reads per block; keeps one step's catch-up under the stall watchdog
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
    # Vault writes per tick. Each waits on block inclusion, so a large backlog drains across ticks.
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
        world: Callable[[], Optional[World]],
        vote: Callable[[str, Any, Any], None],
        vault: Any,
        ledger: Any,
        backing: str = pdas.BACKING_CHAIN_TAO,
        program_id: Any = None,
        clock: Optional[Callable[[], float]] = None,
        config: Optional[RelayConfig] = None,
        sleep: Optional[Callable[[float], None]] = None,
        timer: Optional[Callable[[], float]] = None,
        archive: Optional[Any] = None,
    ):
        self._world = world
        self._episodes = Episodes()
        self.vote = vote  # (kind, subject, payload): handed to the actuator, landed or retried there
        self.vault = vault
        self.store = ledger
        self.backing = backing
        self.backing_bit = pdas.BACKING_BITS.get(backing, 0)
        self.program_id = program_id
        self.clock = clock or time.time
        self.cfg = config or RelayConfig()
        self._sleep = sleep or time.sleep
        # Elapsed time for pacing — monotonic, where `clock` is wall time and may be stepped.
        self._timer = timer or time.monotonic
        self._last_miner_read = 0.0
        self._settled: Dict[str, Optional[int]] = {}  # each miner's settled counter, read once per step

        # Miners whose attestation needs recomputing. Event-driven ONLY: an idle miner never
        # enters this set, so it never gets a write (its old attestation is old-but-correct).
        self._dirty: Set[str] = set()
        # Miners whose purse was DEACTIVATED and whose bond is still locked. Armed by the event
        # (and persisted — see arm_exit), never inferred from the bit being down.
        self._exiting: Set[str] = set()
        self._reconciled = False
        # The post-boot audit ring: the bound set as of this lap's start, how far into it the walk
        # has got, and when the lap ends (see `audit_ring`).
        self._ring: List[str] = []
        self._ring_pos = 0
        self._next_reconcile = 0.0
        self._last_heartbeat_attempt = 0.0
        # Per-subject re-vote throttles, so an unlanded round isn't re-submitted every 12s pass.
        self._voted_at: Dict[str, float] = {}
        self._writes = 0
        # F3: cleared when a verdict ingest write fails, so the liveness heartbeat is held down
        # until the relay has caught up — the fuse must not report "live" over a lost verdict.
        self._ingest_healthy = True
        # Whether the event feed has published everything up to now (no catch-up running). The fee
        # true-up reads totals off the ledger the feed fills, so it waits on this.
        self.feed_caught_up = True
        # Reads the vault's past settled counters, for a fresh fee ledger's stop point (see stop_point).
        self.archive = archive
        self._fee_stop_known = False
        self._last_balance_check = 0.0

    # ─── Solana, as the World holds it ───────────────────────────────────────

    def world(self) -> World:
        world = self._world()
        if world is None:
            raise RuntimeError('no World yet')
        return world

    def attribution(self) -> Dict[str, str]:
        """Solana pubkey → hotkey, off the bindings the World verified. Empty until the World is complete:
        an audit of nobody must never come back clean."""
        world = self._world()
        return dict(world.attribution) if world is not None and world.complete else {}

    def miner_state(self, miner: str) -> Optional[Any]:
        return self.world().get('MinerState', pdas.miner_state_pda(miner, self.program_id))

    def attestation(self, miner: str) -> Optional[Any]:
        return self.world().get('BondAttestation', pdas.bond_attestation_pda(miner, self.backing, self.program_id))

    def settled_total(self, hotkey: str) -> Optional[int]:
        """The vault's settled counter for ``hotkey``, read once per step (the attestation and the fee
        ledger both ask)."""
        if hotkey not in self._settled:
            self._settled[hotkey] = self.vault.get_settled_total(hotkey)
        return self._settled[hotkey]

    def collect_fees(self, entries: Any) -> Any:
        self._settled.clear()
        return self.vault.vote_collect_fees_batch(entries)

    def hotkey_for(self, miner: str) -> Optional[str]:
        """The vault keys bonds by hotkey (D3); every vault call for a miner goes through here.
        None means unbound — there is no bond to reason about, so the relay leaves it alone."""
        return self.attribution().get(str(miner))

    def miner_for(self, hotkey: str) -> Optional[str]:
        for pubkey, hk in self.attribution().items():
            if hk == hotkey:
                return pubkey
        return None

    # ─── inputs from the event feed ──────────────────────────────────────────

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

    def ingest(self, rec: Any) -> None:
        """Fold one program event into the ledger. Keyed by Solana pubkey and kept for unbound miners too,
        because a binding can land after the swap did. Raises when the write fails: the caller holds the
        event and the heartbeat, since a lost verdict would lose the slash for good."""
        name = rec.name
        fields = rec.fields
        block_time = rec.block_time
        if name == 'SwapCompleted':
            if str(fields['collateral_chain']).lower() != self.backing or block_time is None:
                return
            # Solana moved nothing: the fee is owed on the vault and nets off the bond until it settles.
            # Never under a guessed generation: a mis-tagged accrual would charge the wrong vault forever.
            miner = str(fields['miner'])
            generation = self.vault_generation()
            if generation is None:
                raise RuntimeError('vault generation unreadable — cannot tag the fee accrual')
            self.store.record_relay_fee(
                _hex(fields['swap_key']), miner, self.backing, int(fields['fee']), int(block_time), generation
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

    def vault_generation(self) -> Optional[int]:
        """The config's attestation-epoch namespace. None when unreadable: composing against a
        guessed generation would either land in a retired namespace or be refused as stale, and
        "unknown" must never be spelled as 0 — same contract as an unreadable vault read."""
        try:
            return int(getattr(self.world().config, 'vault_generation', 0) or 0)
        except Exception as e:
            bt.logging.warning(f'relay: vault generation read failed: {e}')
            return None

    def pace(self) -> None:
        """Hold successive miners' vault reads ``RELAY_MINER_READ_INTERVAL_SECS`` apart. The audit
        walks every bound miner and the batched read made each one a single fast round trip, so
        unpaced it outruns the keyed subtensor's rate limit; a lone event-driven read never waits."""
        wait = self._last_miner_read + RELAY_MINER_READ_INTERVAL_SECS - self._timer()
        if wait > 0:
            self._sleep(wait)
        self._last_miner_read = self._timer()

    def mark_dirty(self, miner: str) -> None:
        self._dirty.add(str(miner))

    # ─── the tick ────────────────────────────────────────────────────────────

    def step(self, now: Optional[int] = None) -> None:
        """One tick. Never raises: a relay hiccup is retried next tick."""
        now = int(now if now is not None else self.clock())
        self._writes = 0
        self._settled.clear()
        try:
            if not self._reconciled:
                # STARTUP BARRIER. Until every owed write is submitted, the heartbeat stays down
                # and the fuse keeps TAO entry closed. Obligations are discharged by SUBMITTING
                # our vote, not by waiting for quorum — waiting would deadlock a fleet-wide
                # restart, where every validator is behind at once.
                if not self.reconcile(now):
                    self._episodes.start(
                        'reconcile', 'relay: startup reconcile draining, heartbeat held', bt.logging.info
                    )
                    return
                self._reconciled = True
                self.store.seed_as_of = None  # seeds after boot are new miners or this validator's own rounds
                self._episodes.end('reconcile')
                bt.logging.info('relay: startup reconcile clean, heartbeat released')
                dev_signal.emit('relay_reconciled', backing=self.backing)

            self.poll_vault_events()
            self.audit_ring(now)
            slash_job.arm_verdicts(self, now)
            attestation_job.flush(self, now)
            slash_job.relay_verdicts(self, now)
            exits.run_exits(self, now)
            exits.maybe_cadence_settle(self, now)
            self.maybe_heartbeat(now)
            self.maybe_check_balance(now)
        except Exception as e:
            bt.logging.warning(f'relay: step failed: {e}')

    def audit_ring(self, now: int) -> None:
        """The slow vault↔attestation repair after boot, a few miners per tick instead of all at once.

        The bound set is laid out as a ring and walked once per ``reconcile_interval_secs``: each
        tick marks dirty the miners the clock says are due by now, and the tick's own flush re-reads
        them. Every miner is still re-checked once an interval, but no tick carries the
        whole walk — which is ~0.5s of paced vault reads per bound miner. A lap that ticks came too
        slowly to finish is finished at its boundary, so the interval stays a guarantee."""
        interval = self.cfg.reconcile_interval_secs
        lap_over = now >= self._next_reconcile
        elapsed = interval - (self._next_reconcile - now)
        # Never behind the position: a wall clock stepped back must not turn into a negative slice
        # end, which would mark nearly the whole ring.
        due = len(self._ring) if lap_over else max(self._ring_pos, int(len(self._ring) * elapsed / interval))
        for miner in self._ring[self._ring_pos : due]:
            self.mark_dirty(miner)
        self._ring_pos = due
        if not lap_over:
            return
        # A new lap is when the ring learns who exists. A miner it has not seen before is read now
        # rather than whenever the walk reaches it, so one that bound mid-lap waits at most an
        # interval for its first audit — the same bound the full walk gave.
        known = set(self._ring)
        bonded = self.bonded_miners()
        for miner in bonded:
            if miner not in known:
                self.mark_dirty(miner)
        self._open_lap(now, bonded)
        self._refresh_exiting(bonded)
        self._prune(now)

    def _open_lap(self, now: int, bonded: List[str]) -> None:
        self._ring, self._ring_pos = bonded, 0
        self._next_reconcile = now + self.cfg.reconcile_interval_secs

    def reconcile(self, now: int) -> bool:
        """Full vault↔attestation diff plus a drain of every pending obligation — the whole bound
        set in one pass. This is the startup barrier's audit; after it, ``audit_ring`` keeps the
        same repair going a slice at a time.

        Returns True when nothing was left undone — the condition the startup barrier waits on.
        Repairs the one hole the event path structurally can't cover: a crash BETWEEN paired
        writes (vault mutated, attestation refresh never sent)."""
        # Not knowing who exists (no complete World yet) is owed, not settled.
        world = self._world()
        ok = world is not None and world.complete
        if not ok or not self.resolve_fee_stop(now):
            return False  # seeding fee bases now would fix a wrong starting point for good
        bonded = self.bonded_miners()
        for miner in bonded:
            self.mark_dirty(miner)
        # This pass read everyone, so the ring's first lap starts from it instead of rediscovering
        # the same set in the same tick.
        self._open_lap(now, bonded)
        ok = attestation_job.flush(self, now, reconciling=True) and ok
        slash_job.arm_verdicts(self, now)
        ok = slash_job.relay_verdicts(self, now) and ok
        self._refresh_exiting(bonded)
        ok = exits.run_exits(self, now) and ok
        self._prune(now)
        return ok

    def resolve_fee_stop(self, now: int) -> bool:
        """On a fresh fee ledger, learn the boundary the vault last settled before any miner is seeded.
        False while it can't be learned yet (the reconcile barrier holds, and retries next tick)."""
        if self._fee_stop_known:
            return True
        if self.store.has_fee_bases():  # this database has been counting: its bases already carry the answer
            self._fee_stop_known = True
            return True
        validators = self.vault.get_validators()
        hotkeys = sorted(
            {hk for m in self.bonded_miners() if m not in self._exiting for hk in [self.hotkey_for(m)] if hk}
        )
        cadence = self.cfg.fee_cadence_secs
        reader = self.archive = self.archive or stop_point.ArchiveReader(self.vault)
        try:
            settled = stop_point.resolve(
                now, cadence, None if validators is None else len(validators), hotkeys, reader.settled_at
            )
        except Exception as e:
            bt.logging.warning(f'relay: fee stop-point lookup failed: {e}')
            settled = None
        if settled is None:
            self._episodes.start(
                'fee-stop',
                'relay: fresh fee ledger — waiting to learn where the vault last settled fees',
                bt.logging.warning,
            )
            return False
        self._episodes.end('fee-stop')
        self.store.seed_as_of = settled
        self.store.set_relay_meta('fee_stop_point', settled)
        self._fee_stop_known = True
        bt.logging.info(
            f'relay: fresh fee ledger — the vault last settled fees at boundary {settled} '
            f'({len(validators or [])} validator(s), {len(hotkeys)} bound miner(s) checked)'
        )
        return True

    def maybe_check_balance(self, now: int) -> None:
        """Warn, hourly, when the vault signer is running out of TAO to vote with."""
        if now - self._last_balance_check < RELAY_BALANCE_CHECK_SECS:
            return
        self._last_balance_check = now
        reader = getattr(self.vault, 'signer_balance', None)
        balance = reader() if reader is not None else None
        if balance is not None and balance < RELAY_LOW_BALANCE_RAO:
            bt.logging.warning(
                f'relay: vault signer has {balance / 1e9:.4f} TAO free — top it up, every vault vote '
                f'needs ~0.075 TAO free up front'
            )

    def _prune(self, now: int) -> None:
        try:
            self.store.prune_relay_swaps(now - self.cfg.swap_retention_secs)
        except Exception as e:
            bt.logging.warning(f'relay: prune failed: {e}')

    def bonded_miners(self) -> List[str]:
        """Every miner the relay could owe a write for.

        Deliberately the whole BOUND set, not just the attested one: a miner's first bond is
        posted and locked on the vault before any attestation exists, and if that pair of vault
        events lands while this validator is down, nothing else would ever discover it. Enumerating
        bindings makes the reconcile loop the backstop for entry as well as for repair. A bound
        miner with no bond costs three vault reads and produces no write (see
        ``attestation._write_one``)."""
        miners: Set[str] = set(self.attribution())
        world = self._world()
        for _pda, att in world.all('BondAttestation') if world is not None else ():
            if str(getattr(att, 'chain', self.backing)).lower() == self.backing:
                miners.add(str(att.miner))
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
                ms = self.miner_state(miner)
            except Exception:
                continue
            if ms is not None and int(getattr(ms, 'active_backings', 0)) & self.backing_bit:
                self.disarm_exit(miner)

    # ─── heartbeat ───────────────────────────────────────────────────────────

    def maybe_heartbeat(self, now: int) -> None:
        """Bump the GLOBAL liveness heartbeat on a lazy cadence. Liveness is one question about
        the whole relay, not fifty questions about individual miners — so one round, and quiet
        miners can never false-positive a staleness fuse."""
        if not self._ingest_healthy:
            self._episodes.start('dirty', 'relay: a verdict is waiting to be written, heartbeat held')
            return
        self._episodes.end('dirty', 'relay: verdicts written, heartbeat free')
        world = self._world()
        if world is None or world.config is None:
            return
        last = int(getattr(world.config, 'last_attest_heartbeat', 0) or 0)
        if now - max(last, self._last_heartbeat_attempt) < self.cfg.heartbeat_interval_secs:
            return
        self.vote('vote_attest_heartbeat', 'config', None)  # the executor skips a round already voted
        self._last_heartbeat_attempt = now
        bt.logging.debug('relay: heartbeat vote queued')
        dev_signal.emit('relay_heartbeat')

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
        # A long outage is walked one window per step; the cursor advances each step instead of
        # only once the whole gap is read, which never happened before the watchdog fired.
        end = min(head, start + _VAULT_POLL_MAX_BLOCKS - 1)
        try:
            events = self.vault.poll_events(start, end)
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
        self.store.set_relay_meta(_VAULT_CURSOR_KEY, end)

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
