"""SolanaEventIndex — crown-time miner state sourced from Solana program events (B3.4).

The crown algorithm in ``scoring.py`` reads per-instant miner state (active set, activity state, posted
collateral, quoted rate) through a small read interface. B1/B2 fed that interface from the substrate
``ContractEventWatcher``; B3.4 swaps the *writer*: ``SolanaEventIngest`` (B3.1) decodes program events and
this index persists them into the ``state_store`` event tables, attributing each on-chain Solana pubkey to
its bound Bittensor hotkey at write time (B3.2). The crown math is unchanged — it consumes the same
``get_*_at`` / ``get_*_in_range`` shapes ``ContractEventWatcher`` exposed. ``SwapCompleted`` additionally
persists its realized legs into ``clearing_rates`` (C-rev), the per-swap history the rate-quality reference
is built from. ``SwapCompleted``/``SwapTimedOut`` also record the swap's terminal outcome into
``swap_outcomes``, the seam's post-close completed-vs-slashed truth (terminal swap PDAs are closed on-chain).

The axis is unix ``blockTime`` seconds (the ``block_num``/``block`` columns are repurposed), not substrate
blocks. Reservation + swap lifecycle drive a per-miner ``MinerActivity`` machine (D4): ``PoolResolved``
opens a RESERVE_START plus a synthetic RESERVE_EXPIRE at ``block_time + reservation_ttl_secs``, and the swap
events drive FULFILL_START/END. The crown credits a miner only while its activity ∈ ``REWARD_MINER_STATES``.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Optional, Set

import bittensor as bt

from allways import dev_signal
from allways.classes import ActivityTransition, MinerActivity
from allways.constants import RATE_PRECISION
from allways.solana.events import EventRecord
from allways.validator.state_store import ValidatorStateStore

# Swap-lifecycle events → MinerActivity edges. A reserved miner enters FULFILLING on
# SwapInitiated and returns to AVAILABLE on completion/timeout (see classes.MinerActivity).
_FULFILL_TRANSITIONS = {
    'SwapInitiated': ActivityTransition.FULFILL_START,
    'SwapCompleted': ActivityTransition.FULFILL_END,
    'SwapTimedOut': ActivityTransition.FULFILL_END,
}

# Terminal events → per-swap outcome persisted for the seam (reserve_engine._swap_stage):
# terminal swap PDAs close on-chain, so this index is what disambiguates a slash from a
# completion once the account is gone. ``expired`` is a claim reaped stale before attestation
# (close_stale_claim closes the Swap PDA) — the user never completed, so it's terminal too.
_OUTCOME_BY_EVENT = {
    'SwapCompleted': 'completed',
    'SwapTimedOut': 'timed_out',
    'StaleClaimClosed': 'expired',
}


class SolanaEventIndex:
    """Persists decoded Solana program events into the validator state store and exposes the crown's
    per-instant read interface over them. One instance per validator; backed by ``ValidatorStateStore``.
    ``reservation_ttl_fn`` (the solana config cache getter) supplies the TTL used to synthesize each
    reservation's RESERVE_EXPIRE, since ``reserved_until`` isn't carried on the ``PoolResolved`` event."""

    def __init__(self, state_store: ValidatorStateStore, reservation_ttl_fn: Optional[Callable[[], int]] = None):
        self.state_store = state_store
        self._reservation_ttl_fn = reservation_ttl_fn

    # ─── write path ─────────────────────────────────────────────────────

    def ingest(self, records: List[EventRecord], attribution: Dict[str, str]) -> int:
        """Persist ``records`` (oldest-first) into the event tables, mapping each event's miner Solana
        pubkey → bound hotkey via ``attribution`` (B3.2 ``build_attribution``). Events from an unbound
        pubkey, or carrying no ``blockTime`` yet (an unstamped tip tx), are skipped — the cursor stays
        behind them so a later pass re-ingests once they stamp. Returns the count written."""
        written = 0
        for rec in records:
            block_time = rec.block_time
            if block_time is None:
                continue
            miner_pk = self._miner_str(rec)
            if miner_pk is None:
                continue
            hotkey = attribution.get(miner_pk)
            if hotkey is None:
                # Unbound (or invalid binding) miner — no UID to credit, so its events are dropped.
                continue
            if self._apply(rec, hotkey, int(block_time)):
                written += 1
        return written

    def _apply(self, rec: EventRecord, hotkey: str, block_time: int) -> bool:
        name = rec.name
        if name in ('MinerActivated', 'MinerDeactivated'):
            self.state_store.insert_active_event(block_time, hotkey, name == 'MinerActivated')
            return True
        if name == 'PoolResolved':
            return self._apply_reservation(hotkey, block_time)
        if name in _FULFILL_TRANSITIONS:
            self.state_store.insert_activity_event(block_time, hotkey, _FULFILL_TRANSITIONS[name])
            outcome = _OUTCOME_BY_EVENT.get(name)
            if outcome is not None:
                self.state_store.record_swap_outcome(bytes(rec.fields['swap_key']).hex(), outcome, block_time)
                dev_signal.emit('swap_outcome', swap_key=bytes(rec.fields['swap_key']).hex(), outcome=outcome)
            # SwapCompleted is the only swap event carrying realized legs — persist
            # them as a clearing-rate sample for the C-rev quality reference, in
            # addition to closing the fulfillment above.
            if name == 'SwapCompleted':
                self.state_store.insert_clearing_rate(
                    block_time,
                    hotkey,
                    self._chain(rec, 'from_chain'),
                    self._chain(rec, 'to_chain'),
                    int(rec.fields['from_amount']),
                    int(rec.fields['to_amount']),
                )
            return True
        if name == 'StaleClaimClosed':
            # A PendingAttestation claim reaped stale: the Swap PDA is gone, so record the terminal
            # 'expired' outcome for the seam. No activity edge — the miner never entered FULFILLING;
            # its synthetic RESERVE_EXPIRE already returns it to AVAILABLE.
            self.state_store.record_swap_outcome(
                bytes(rec.fields['swap_key']).hex(), _OUTCOME_BY_EVENT[name], block_time
            )
            dev_signal.emit(
                'swap_outcome', swap_key=bytes(rec.fields['swap_key']).hex(), outcome=_OUTCOME_BY_EVENT[name]
            )
            return True
        if name in ('CollateralPosted', 'CollateralWithdrawn'):
            total = int(rec.fields['total'])
            self.state_store.insert_collateral_event(block_time, hotkey, total)
            return True
        if name == 'QuoteSet':
            rate = int(rec.fields['rate']) / RATE_PRECISION
            self.state_store.insert_rate_event(
                hotkey, self._chain(rec, 'from_chain'), self._chain(rec, 'to_chain'), rate, block_time
            )
            return True
        if name == 'QuoteRemoved':
            # Opt-out: a zero rate ends crown credit for this direction, same as a recorded zero quote.
            self.state_store.insert_rate_event(
                hotkey, self._chain(rec, 'from_chain'), self._chain(rec, 'to_chain'), 0.0, block_time
            )
            return True
        return False  # not a crown-relevant event

    def _apply_reservation(self, hotkey: str, block_time: int) -> bool:
        """PoolResolved → RESERVE_START now + a synthetic RESERVE_EXPIRE at
        ``block_time + reservation_ttl_secs`` (``reserved_until`` isn't on the
        event). Dropped if no TTL source is wired, so the reservation never opens
        without its matching expiry."""
        ttl = self._reservation_ttl()
        if ttl is None:
            bt.logging.warning('SolanaEventIndex: no reservation_ttl; dropping PoolResolved')
            return False
        self.state_store.insert_activity_event(block_time, hotkey, ActivityTransition.RESERVE_START)
        self.state_store.insert_activity_event(block_time + ttl, hotkey, ActivityTransition.RESERVE_EXPIRE)
        return True

    def _reservation_ttl(self) -> Optional[int]:
        if self._reservation_ttl_fn is None:
            return None
        try:
            ttl = int(self._reservation_ttl_fn())
        except Exception as e:
            bt.logging.warning(f'SolanaEventIndex: reservation_ttl read failed: {e}')
            return None
        return ttl if ttl > 0 else None

    @staticmethod
    def _miner_str(rec: EventRecord) -> Optional[str]:
        try:
            return str(rec.fields['miner'])
        except (KeyError, TypeError) as e:
            bt.logging.debug(f'SolanaEventIndex: {rec.name} missing miner field: {e}')
            return None

    @staticmethod
    def _chain(rec: EventRecord, key: str) -> str:
        return str(rec.fields[key]).lower()

    # ─── read interface (consumed by scoring's crown replay) ────────────

    def get_active_miners_at(self, at_time: int) -> Set[str]:
        return self.state_store.get_active_state_at(at_time)

    def get_activity_state_at(self, at_time: int) -> Dict[str, MinerActivity]:
        return self.state_store.get_activity_state_at(at_time)

    def get_miner_collaterals_at(self, at_time: int) -> Dict[str, int]:
        return self.state_store.get_collaterals_at(at_time)

    def get_active_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_active_events_in_range(start_time, end_time)

    def get_activity_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_activity_events_in_range(start_time, end_time)

    def get_collateral_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_collateral_events_in_range(start_time, end_time)
