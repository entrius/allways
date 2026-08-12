"""Collateral-floor sweep: vote out miners stranded active under a raised floor.

``set_min_collateral`` only rewrites the Config — it never touches MinerState, and
the contract's auto-deactivation (``apply_penalty``) fires only on fee/slash paths
an under-floor miner can no longer reach, because ``open_or_request`` refuses to
reserve them. Left alone they stay active forever: unreservable by takers yet still
earning crown. This sweep is the missing driver for the contract's
``vote_deactivate`` quorum path.

Cost model: a floor raise is a rare admin event, so the steady-state step is a
single integer comparison against the TTL-cached Config (no RPC). The
getProgramAccounts scan runs only when the cached floor rises — or once on the
first step after boot, which covers a raise that happened while this validator was
offline. Stragglers (busy miners the contract refuses to kick, votes awaiting
quorum) are rechecked with per-miner account reads on a slow retry cadence until
the pending set drains.
"""

import time
from typing import TYPE_CHECKING, Callable, Optional, Set

import bittensor as bt
from solders.pubkey import Pubkey

from allways.solana import pdas
from allways.solana.client import benign_marker
from allways.solana.layouts import hub_busy_until, hub_swap_on

if TYPE_CHECKING:
    from neurons.validator import Validator


class CollateralFloorSweep:
    RETRY_SECS = 300

    def __init__(self, solana_client, read_only: bool = False, clock: Optional[Callable[[], float]] = None):
        self._client = solana_client
        self._read_only = read_only
        self._clock = clock or time.time
        # Per-hub floors from the last tick — a raise of EITHER arms the scan.
        self._last_floors: Optional[dict] = None
        # Scan owed: set on boot and on every raise edge, cleared only by a
        # completed scan — an RPC failure at the raise moment keeps it set, so
        # the sweep retries on the slow cadence instead of losing the event.
        self._armed = False
        # Miner pubkey strings still needing a kick (busy, or vote short of quorum).
        self._pending: Set[str] = set()
        self._next_retry: float = 0.0

    def step(self, floor: int, tao_floor: int = 0) -> None:
        """One forward-step tick. Steady state (no floor raised, nothing armed
        or pending) costs a couple of comparisons; the scan sits behind a raise
        edge or the retry cursor. Each hub carries its own floor."""
        now = self._clock()
        floors = {pdas.BACKING_CHAIN_SOL: int(floor), pdas.BACKING_CHAIN_TAO: int(tao_floor)}
        if self._last_floors is None or any(v > self._last_floors.get(k, 0) for k, v in floors.items()):
            # Arm, and reset the cursor so a genuine raise scans immediately
            # instead of waiting out a previous back-off. The edge fires once
            # per raise (floors commit below), so this can't re-fire per step.
            self._armed = True
            self._next_retry = now
        self._last_floors = floors
        if now < self._next_retry:
            return
        if self._armed:
            self._scan(floors, now)
            self._armed = False
        elif self._pending:
            self._recheck(floors, now)

    def _scan(self, floors: dict, now: float) -> None:
        """Full MinerState scan — the rare, arm-time path. The retry cursor
        advances before the RPC so a throwing scan backs off instead of
        re-firing every forward step."""
        self._next_retry = now + self.RETRY_SECS
        self._pending = set()
        for _, ms in self._client.get_all('MinerState'):
            miner = self._miner_key(ms)
            if not self._resolve(miner, ms, floors, now):
                self._pending.add(miner)
        if self._pending:
            bt.logging.info(f'floor sweep: {len(self._pending)} miner(s) under a collateral floor, pending kick')

    def _recheck(self, floors: dict, now: float) -> None:
        """Per-miner account reads for the stragglers only. Cursor-first for the
        same back-off reason as ``_scan``; one unreadable miner skips, not aborts."""
        self._next_retry = now + self.RETRY_SECS
        for miner in list(self._pending):
            try:
                ms = self._client.get_miner_state(miner)
            except Exception as e:
                bt.logging.warning(f'floor sweep: {miner}: {e}')
                continue
            if ms is None or self._resolve(miner, ms, floors, now):
                self._pending.discard(miner)

    def _resolve(self, miner: str, ms, floors: dict, now: float) -> bool:
        """True when this miner needs nothing further. Each ACTIVE hub is judged
        against its OWN floor and the deficient backing is the one voted — a
        SOL-bit-down miner must never livelock a vote on an already-cleared purse."""
        try:
            if not ms.active:
                return True
            backing = self._deficient_backing(miner, ms, floors)
            if backing is None:
                return True
            # The contract rejects a kick only while THIS hub is busy (v3.1 per-hub gate); a
            # sibling swap on another purse must not defer it. Retry once this hub idles.
            bit = pdas.BACKING_BITS[backing]
            if hub_swap_on(ms, bit) or now < hub_busy_until(ms, bit):
                return False
            if self._read_only:
                bt.logging.info(f'floor sweep: WOULD vote_deactivate {miner} on {backing} (watch mode)')
                return False
            if self._client.has_voted(pdas.REQ_DEACTIVATE, miner, self._client.keypair.pubkey()):
                return False
            try:
                sig = self._client.vote_deactivate(miner, backing=backing)
                bt.logging.info(f'floor sweep: voted to deactivate {miner} on {backing} (under floor): {sig}')
            except Exception as e:
                # The has_voted read and the send can race (our vote lands in
                # between, or the stale-round clock disagrees by seconds) — the
                # contract's AlreadyVoted is an authoritative no-op, not a failure.
                if benign_marker(e, ('AlreadyVoted',)):
                    bt.logging.debug(f'floor sweep: {miner}: vote already recorded')
                else:
                    raise
        except Exception as e:
            bt.logging.warning(f'floor sweep: {miner}: {e}')
        return False

    def _deficient_backing(self, miner: str, ms, floors: dict) -> Optional[str]:
        """The first ACTIVE backing whose own purse sits under its own floor, else
        None. Never judges a hub by a sibling's floor, and never a bit the contract
        has already cleared."""
        mask = int(getattr(ms, 'active_backings', 0) or 0)
        for backing, bit in pdas.BACKING_BITS.items():
            floor = floors.get(backing)
            if not floor or not mask & bit:
                continue
            purse = self._purse(miner, ms, backing)
            if purse is not None and purse < floor:
                return backing
        return None

    def _purse(self, miner: str, ms, backing: str) -> Optional[int]:
        """This backing's own bond in its native unit: the local vault for SOL, the
        quorum-attested effective balance otherwise. None (skip) when unreadable or
        missing/unlocked, so a flaky read never forces a wrong kick."""
        if backing == pdas.BACKING_CHAIN_SOL:
            return int(ms.collateral)
        try:
            att = self._client.get_bond_attestation(miner, backing)
        except Exception as e:
            bt.logging.warning(f'floor sweep: {miner} {backing}: {e}')
            return None
        if att is None or not getattr(att, 'locked', False):
            return None
        return int(att.effective_balance)

    @staticmethod
    def _miner_key(ms) -> str:
        return str(Pubkey.from_bytes(bytes(ms.miner)))


def maybe_sweep_floor(self: 'Validator') -> None:
    """One never-raises sweep tick off the TTL-cached Config — a sweep hiccup
    must not break the forward pass, and the read adds no RPC of its own."""
    try:
        self.floor_sweep.step(
            self.solana_config_cache.min_collateral(),
            self.solana_config_cache.tao_min_collateral(),
        )
    except Exception as e:
        bt.logging.warning(f'floor sweep: {e}')
