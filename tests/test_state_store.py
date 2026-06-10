"""Unit tests for the ``reservation_pins`` table in ValidatorStateStore.

The pin table snapshots a miner's commitment as of the reservation block so a
swap settles against the miner's rate + addresses as they were when the user
reserved — closing the rate-swing and address-theft windows. Coverage:
round-trip, overwrite, expiry purge, TTL refresh, delete_hotkey cleanup, and a
fresh ``init_db()`` creating the table.
"""

from dataclasses import replace
from pathlib import Path

from allways.validator.state_store import PendingConfirm, ReservationPin, ValidatorStateStore

PIN_SAMPLE1 = ReservationPin(
    miner_hotkey='miner-1',
    reserve_block=900,
    from_chain='btc',
    to_chain='tao',
    rate_str='345',
    counter_rate_str='0.0029',
    miner_from_address='bc1-miner',
    miner_to_address='5miner',
    reserved_until=1000,
    created_at=1.0,
)

PIN_SAMPLE2 = ReservationPin(
    miner_hotkey='miner-2',
    reserve_block=905,
    from_chain='btc',
    to_chain='tao',
    rate_str='350',
    counter_rate_str='0.0028',
    miner_from_address='bc1-miner-2',
    miner_to_address='5miner2',
    reserved_until=1005,
    created_at=2.0,
)


class TestReservationPinRoundTrip:
    def test_upsert_and_get_round_trip(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)

        pin = store.get_reservation_pin('miner-1')
        assert pin == PIN_SAMPLE1
        store.close()

    def test_persists_across_store_instances(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        store1 = ValidatorStateStore(db_path=db_path)
        store1.upsert_reservation_pin(PIN_SAMPLE1)
        store1.close()

        store2 = ValidatorStateStore(db_path=db_path)
        assert store2.get_reservation_pin('miner-1') == PIN_SAMPLE1
        store2.close()

    def test_get_unknown_hotkey_returns_none(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        assert store.get_reservation_pin('miner-unknown') is None
        store.close()

    def test_upsert_overwrites_existing_row(self, tmp_path: Path):
        """A fresh MinerReserved for a miner replaces any stale pin —
        INSERT OR REPLACE keyed on miner_hotkey, so exactly one row remains."""
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)
        store.upsert_reservation_pin(replace(PIN_SAMPLE1, reserve_block=1500, rate_str='999', reserved_until=1600))

        pin = store.get_reservation_pin('miner-1')
        assert pin.reserve_block == 1500
        assert pin.rate_str == '999'
        assert pin.reserved_until == 1600
        store.close()

    def test_remove_returns_and_deletes(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)

        removed = store.remove_reservation_pin('miner-1')
        assert removed == PIN_SAMPLE1
        assert store.get_reservation_pin('miner-1') is None
        store.close()

    def test_remove_unknown_hotkey_is_noop(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        assert store.remove_reservation_pin('miner-unknown') is None
        store.close()


class TestReservationPinPurge:
    def test_purge_drops_only_expired_rows(self, tmp_path: Path):
        store = ValidatorStateStore(
            db_path=tmp_path / 'state.db',
            current_block_fn=lambda: 1001,
        )
        store.upsert_reservation_pin(PIN_SAMPLE1)  # reserved_until=1000 → expired at 1001
        store.upsert_reservation_pin(PIN_SAMPLE2)  # reserved_until=1005 → still live

        purged = store.purge_expired_reservation_pins()
        assert purged == 1
        assert store.get_reservation_pin('miner-1') is None
        assert store.get_reservation_pin('miner-2') == PIN_SAMPLE2
        store.close()

    def test_purge_without_current_block_fn_is_noop(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)
        assert store.purge_expired_reservation_pins() == 0
        assert store.get_reservation_pin('miner-1') == PIN_SAMPLE1
        store.close()

    def test_get_expired_returns_only_expired_rows(self, tmp_path: Path):
        store = ValidatorStateStore(
            db_path=tmp_path / 'state.db',
            current_block_fn=lambda: 1001,
        )
        store.upsert_reservation_pin(PIN_SAMPLE1)  # reserved_until=1000 → expired at 1001
        store.upsert_reservation_pin(PIN_SAMPLE2)  # reserved_until=1005 → still live

        expired = store.get_expired_reservation_pins()
        assert expired == [PIN_SAMPLE1]
        # Read-only: the row is left for the caller to emit a pin-end before purging.
        assert store.get_reservation_pin('miner-1') == PIN_SAMPLE1
        store.close()

    def test_get_expired_without_current_block_fn_is_empty(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)
        assert store.get_expired_reservation_pins() == []
        store.close()

    def test_extend_reservation_deadline_keeps_pin_a_purge_would_drop(self, tmp_path: Path):
        """Regression: after the contract extends a reservation, bumping the
        pin's reserved_until must keep it alive past its original TTL."""
        store = ValidatorStateStore(
            db_path=tmp_path / 'state.db',
            current_block_fn=lambda: 1003,
        )
        store.upsert_reservation_pin(PIN_SAMPLE1)  # reserved_until=1000, would be purged at 1003
        store.extend_reservation_deadline('miner-1', 1300)

        pin = store.get_reservation_pin('miner-1')
        assert pin.reserved_until == 1300

        assert store.purge_expired_reservation_pins() == 0
        assert store.get_reservation_pin('miner-1') is not None
        store.close()

    def test_extend_reservation_deadline_unknown_hotkey_is_noop(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.extend_reservation_deadline('miner-unknown', 9999)
        assert store.get_reservation_pin('miner-unknown') is None
        store.close()

    def test_extend_reservation_deadline_bumps_both_copies(self, tmp_path: Path):
        """The shared mutator must advance BOTH the pending_confirms row and the
        reservation pin, so neither purge sweep drops a still-live reservation
        (#441). Updating only one copy is what desynced the pin."""
        store = ValidatorStateStore(
            db_path=tmp_path / 'state.db',
            current_block_fn=lambda: 1003,
        )
        store.upsert_reservation_pin(PIN_SAMPLE1)  # reserved_until=1000
        store.enqueue(
            PendingConfirm(
                miner_hotkey='miner-1',
                from_tx_hash='tx-1',
                from_chain='btc',
                to_chain='tao',
                from_address='bc1-user',
                to_address='5user',
                tao_amount=123,
                from_amount=456,
                to_amount=789,
                miner_from_address='bc1-miner',
                miner_to_address='5miner',
                rate_str='345',
                reserved_until=1000,
                queued_at=1.0,
            )
        )

        store.extend_reservation_deadline('miner-1', 1300)

        assert store.get_reservation_pin('miner-1').reserved_until == 1300
        assert store.get_all()[0].reserved_until == 1300
        # Same-block purges leave both rows alone now that both TTLs are current.
        assert store.purge_expired_pending_confirms() == 0
        assert store.purge_expired_reservation_pins() == 0
        store.close()


class TestReservationPinCrossTable:
    def test_delete_hotkey_clears_the_pin(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_reservation_pin(PIN_SAMPLE1)

        store.delete_hotkey('miner-1')
        assert store.get_reservation_pin('miner-1') is None
        store.close()

    def test_fresh_init_db_has_reservation_pins_table(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        conn = store.require_connection()
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reservation_pins'").fetchone()
        assert row is not None
        store.close()


class TestDestTipSnapshots:
    def test_upsert_load_round_trip(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=850_000, recorded_at=500)
        store.upsert_dest_tip_snapshot(swap_id=2, dest_chain='btc', tip=850_010, recorded_at=510)

        assert store.load_dest_tip_snapshots() == {1: 850_000, 2: 850_010}
        store.close()

    def test_first_write_wins_so_late_re_observation_cannot_overwrite(self, tmp_path: Path):
        # On restart, a re-observation taken after the honest dest tx already
        # landed would record a later tip and reject the payout as a replay.
        # INSERT OR IGNORE means the original (earlier) snapshot is preserved.
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=850_000, recorded_at=500)
        store.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=850_500, recorded_at=600)

        assert store.load_dest_tip_snapshots() == {1: 850_000}
        store.close()

    def test_persists_across_store_instances(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        store1 = ValidatorStateStore(db_path=db_path)
        store1.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=850_000, recorded_at=500)
        store1.close()

        store2 = ValidatorStateStore(db_path=db_path)
        assert store2.load_dest_tip_snapshots() == {1: 850_000}
        store2.close()

    def test_prune_drops_inactive_swaps(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=100, recorded_at=1)
        store.upsert_dest_tip_snapshot(swap_id=2, dest_chain='btc', tip=200, recorded_at=2)
        store.upsert_dest_tip_snapshot(swap_id=3, dest_chain='btc', tip=300, recorded_at=3)

        store.prune_dest_tip_snapshots({2})

        assert store.load_dest_tip_snapshots() == {2: 200}
        store.close()

    def test_prune_with_empty_active_set_clears_all(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.upsert_dest_tip_snapshot(swap_id=1, dest_chain='btc', tip=100, recorded_at=1)

        store.prune_dest_tip_snapshots(set())

        assert store.load_dest_tip_snapshots() == {}
        store.close()

    def test_fresh_init_db_has_dest_tip_snapshots_table(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        conn = store.require_connection()
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dest_tip_snapshots'").fetchone()
        assert row is not None
        store.close()
