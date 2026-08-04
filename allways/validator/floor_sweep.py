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

if TYPE_CHECKING:
    from neurons.validator import Validator


class CollateralFloorSweep:
    RETRY_SECS = 300

    def __init__(self, solana_client, read_only: bool = False, clock: Optional[Callable[[], float]] = None):
        self._client = solana_client
        self._read_only = read_only
        self._clock = clock or time.time
        self._last_floor: Optional[int] = None
        # Scan owed: set on boot and on every raise edge, cleared only by a
        # completed scan — an RPC failure at the raise moment keeps it set, so
        # the sweep retries on the slow cadence instead of losing the event.
        self._armed = False
        # Miner pubkey strings still needing a kick (busy, or vote short of quorum).
        self._pending: Set[str] = set()
        self._next_retry: float = 0.0

    def step(self, floor: int) -> None:
        """One forward-step tick. Steady state (floor unchanged, nothing armed
        or pending) costs two comparisons; everything heavier sits behind a
        raise edge or the retry cursor."""
        now = self._clock()
        if self._last_floor is None or floor > self._last_floor:
            # Arm, and reset the cursor so a genuine raise scans immediately
            # instead of waiting out a previous back-off. The edge fires once
            # per raise (floor commits below), so this can't re-fire per step.
            self._armed = True
            self._next_retry = now
        self._last_floor = floor
        if now < self._next_retry:
            return
        if self._armed:
            self._scan(floor, now)
            self._armed = False
        elif self._pending:
            self._recheck(floor, now)

    def _scan(self, floor: int, now: float) -> None:
        """Full MinerState scan — the rare, arm-time path. The retry cursor
        advances before the RPC so a throwing scan backs off instead of
        re-firing every forward step."""
        self._next_retry = now + self.RETRY_SECS
        self._pending = set()
        for _, ms in self._client.get_all('MinerState'):
            miner = self._miner_key(ms)
            if not self._resolve(miner, ms, floor, now):
                self._pending.add(miner)
        if self._pending:
            bt.logging.info(f'floor sweep: {len(self._pending)} active miner(s) under floor {floor}, pending kick')

    def _recheck(self, floor: int, now: float) -> None:
        """Per-miner account reads for the stragglers only. Cursor-first for the
        same back-off reason as ``_scan``; one unreadable miner skips, not aborts."""
        self._next_retry = now + self.RETRY_SECS
        for miner in list(self._pending):
            try:
                ms = self._client.get_miner_state(miner)
            except Exception as e:
                bt.logging.warning(f'floor sweep: {miner}: {e}')
                continue
            if ms is None or self._resolve(miner, ms, floor, now):
                self._pending.discard(miner)

    def _resolve(self, miner: str, ms, floor: int, now: float) -> bool:
        """Returns True when this miner needs nothing further. A cast vote is not
        'resolved' — the miner stays pending until quorum flips it inactive, so a
        validator restart or a reset round can never strand a half-kicked miner."""
        try:
            if not ms.active or int(ms.collateral) >= floor:
                return True
            # The contract rejects kicks on busy miners; retry once they idle.
            if ms.has_active_swap or now < int(ms.busy_until):
                return False
            if self._read_only:
                bt.logging.info(f'floor sweep: WOULD vote_deactivate {miner} (watch mode)')
                return False
            if self._client.has_voted(pdas.REQ_DEACTIVATE, miner, self._client.keypair.pubkey()):
                return False
            try:
                sig = self._client.vote_deactivate(miner)
                bt.logging.info(f'floor sweep: voted to deactivate {miner} (collateral < {floor}): {sig}')
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

    @staticmethod
    def _miner_key(ms) -> str:
        return str(Pubkey.from_bytes(bytes(ms.miner)))


def maybe_sweep_floor(self: 'Validator') -> None:
    """One never-raises sweep tick off the TTL-cached Config — a sweep hiccup
    must not break the forward pass, and the read adds no RPC of its own."""
    try:
        self.floor_sweep.step(self.solana_config_cache.min_collateral())
    except Exception as e:
        bt.logging.warning(f'floor sweep: {e}')
