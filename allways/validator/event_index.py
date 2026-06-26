"""SolanaEventIndex — crown-time miner state sourced from Solana program events (B3.4).

The crown algorithm in ``scoring.py`` reads per-instant miner state (active set, busy counts, posted
collateral, quoted rate) through a small read interface. B1/B2 fed that interface from the substrate
``ContractEventWatcher``; B3.4 swaps the *writer*: ``SolanaEventIngest`` (B3.1) decodes program events and
this index persists them into the ``state_store`` event tables, attributing each on-chain Solana pubkey to
its bound Bittensor hotkey at write time (B3.2). The crown math is unchanged — it consumes the same
``get_*_at`` / ``get_*_in_range`` shapes ``ContractEventWatcher`` exposed. ``SwapCompleted`` additionally
persists its realized legs into ``clearing_rates`` (C-rev), the per-swap history the rate-quality reference
is built from.

The axis is unix ``blockTime`` seconds (the ``block_num``/``block`` columns are repurposed), not substrate
blocks. Reservation pins are gone in the Solana model (the swap rate is pinned on-chain and a reserved miner
is busy-gated out of the crown), so no pin stream is written.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

import bittensor as bt

from allways.constants import RATE_PRECISION
from allways.solana.events import EventRecord
from allways.validator.state_store import ValidatorStateStore

# Busy delta per swap-lifecycle event: a miner is busy (excluded from crown) while the running sum is > 0.
# Reservation-busy (the pool window's busy_until) has no per-instant event and is intentionally not ingested
# here — swap-lifecycle busy only (B3.4 decision; PoolResolved→busy-start deferred).
_BUSY_DELTA = {'SwapInitiated': +1, 'SwapCompleted': -1, 'SwapTimedOut': -1}


class SolanaEventIndex:
    """Persists decoded Solana program events into the validator state store and exposes the crown's
    per-instant read interface over them. One instance per validator; backed by ``ValidatorStateStore``."""

    def __init__(self, state_store: ValidatorStateStore):
        self.state_store = state_store

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
        if name in _BUSY_DELTA:
            self.state_store.insert_busy_event(block_time, hotkey, _BUSY_DELTA[name])
            # SwapCompleted is the only busy event carrying realized legs — persist
            # them as a clearing-rate sample for the C-rev quality reference, in
            # addition to closing the busy interval above.
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

    def get_busy_miners_at(self, at_time: int) -> Dict[str, int]:
        return self.state_store.get_busy_counts_at(at_time)

    def get_miner_collaterals_at(self, at_time: int) -> Dict[str, int]:
        return self.state_store.get_collaterals_at(at_time)

    def get_active_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_active_events_in_range(start_time, end_time)

    def get_busy_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_busy_events_in_range(start_time, end_time)

    def get_collateral_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        return self.state_store.get_collateral_events_in_range(start_time, end_time)
