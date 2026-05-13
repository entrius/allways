"""Validator-side mirror of live on-chain reservations.

The smart contract keys reservations by miner only (see
``smart-contracts/ink/lib.rs::reservations``); there is no
source-address index. That left ``handle_swap_reserve`` validating
the user's source balance against the *current* request only and
ignoring any other still-live reservations the same source address
already holds, so a user with funds for one swap could lock multiple
miners simultaneously and stall real liquidity (see issue #295).

This index sits next to the ``ContractEventWatcher`` and is kept in
sync via three contract events:

* ``MinerReserved``       — fetch the full reservation and upsert.
* ``ReservationCancelled``— drop the entry.
* ``SwapInitiated``       — drop the entry (reservation became a swap).

The reserve handler queries
:meth:`ReservationIndex.committed_amount_for_address` to obtain the
total ``from_amount`` already committed by the requestor's source
address on that chain, then rejects any new reservation whose
``from_amount`` would push the cumulative commitment past the user's
visible source-chain balance.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Dict, Iterable, Optional

import bittensor as bt

from allways.classes import Reservation

if TYPE_CHECKING:
    from allways.contract_client import AllwaysContractClient


class ReservationIndex:
    """Thread-safe mirror of live ``(miner_hotkey -> Reservation)`` rows.

    All public mutators take an internal lock; reads also lock so that
    iteration is safe while the event watcher upserts on the forward
    thread and an axon handler reads on a worker thread.
    """

    def __init__(self) -> None:
        self._by_miner: Dict[str, Reservation] = {}
        self._lock = threading.Lock()

    # ─── Mutation ──────────────────────────────────────────────────────

    def upsert(self, miner_hotkey: str, reservation: Reservation) -> None:
        if not miner_hotkey or reservation is None:
            return
        with self._lock:
            self._by_miner[miner_hotkey] = reservation

    def remove(self, miner_hotkey: str) -> None:
        if not miner_hotkey:
            return
        with self._lock:
            self._by_miner.pop(miner_hotkey, None)

    def reset(self, reservations: Optional[Dict[str, Reservation]] = None) -> None:
        with self._lock:
            self._by_miner = dict(reservations or {})

    # ─── Bootstrap ─────────────────────────────────────────────────────

    def hydrate_from_contract(
        self,
        contract_client: 'AllwaysContractClient',
        miner_hotkeys: Iterable[str],
        current_block: int,
    ) -> None:
        """Seed the index from contract reads at validator startup.

        Skips entries whose ``reserved_until`` is already in the past
        — they will be cleared lazily by the contract on the next
        ``vote_reserve`` for the same miner.
        """
        loaded: Dict[str, Reservation] = {}
        for hotkey in miner_hotkeys:
            try:
                res = contract_client.get_reservation(hotkey)
            except Exception as e:
                bt.logging.debug(f'ReservationIndex bootstrap: get_reservation({hotkey[:8]}) failed: {e}')
                continue
            if res is None:
                continue
            if res.reserved_until < current_block:
                continue
            loaded[hotkey] = res
        with self._lock:
            self._by_miner = loaded
        bt.logging.info(f'ReservationIndex hydrated with {len(loaded)} live reservations')

    # ─── Queries ───────────────────────────────────────────────────────

    def committed_amount_for_address(
        self,
        from_address: str,
        from_chain: str,
        current_block: int,
        exclude_miner: Optional[str] = None,
    ) -> int:
        """Sum ``from_amount`` over live reservations matching the source.

        Filters on both ``from_addr`` and ``from_chain`` because a
        user's spendable balance is per-chain — a BTC reservation has
        no claim on the user's TAO balance and vice versa. Expired
        rows still in the index (e.g. a missed ``ReservationCancelled``
        event) are ignored via the ``reserved_until`` gate.
        """
        if not from_address or not from_chain:
            return 0
        total = 0
        with self._lock:
            for hotkey, res in self._by_miner.items():
                if exclude_miner is not None and hotkey == exclude_miner:
                    continue
                if res.reserved_until < current_block:
                    continue
                if res.from_addr != from_address or res.from_chain != from_chain:
                    continue
                total = total + res.from_amount
        return total

    def snapshot(self) -> Dict[str, Reservation]:
        """Return a shallow copy — for tests/debug only."""
        with self._lock:
            return dict(self._by_miner)

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_miner)
