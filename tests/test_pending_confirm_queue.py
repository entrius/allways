import threading
import time
from dataclasses import replace
from pathlib import Path

from allways.validator.state_store import (
    PendingConfirm,
    ValidatorStateStore,
)

PENDING_CONFIRM_SAMPLE1 = PendingConfirm(
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
    rate_str='350',
    reserved_until=100,
    queued_at=1.0,
)


PENDING_CONFIRM_SAMPLE2 = PendingConfirm(
    miner_hotkey='miner-2',
    from_tx_hash='tx-2',
    from_chain='btc',
    to_chain='tao',
    from_address='bc1-user',
    to_address='5user',
    tao_amount=123,
    from_amount=456,
    to_amount=789,
    miner_from_address='bc1-miner',
    miner_to_address='5miner',
    rate_str='350',
    reserved_until=100,
    queued_at=2.0,
)


class TestPendingConfirmQueue:
    def test_persists_across_queue_instances(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue1 = ValidatorStateStore(db_path=db_path)
        queue1.enqueue(PENDING_CONFIRM_SAMPLE1)
        queue1.enqueue(PENDING_CONFIRM_SAMPLE2)
        queue1.close()

        queue2 = ValidatorStateStore(db_path=db_path)
        items = queue2.get_all()

        assert queue2.pending_size() == 2
        assert len(items) == 2
        assert items[0].miner_hotkey == 'miner-1'
        assert items[0].from_tx_hash == 'tx-1'
        assert items[1].miner_hotkey == 'miner-2'
        assert items[1].from_tx_hash == 'tx-2'

    def test_overwrite_keeps_single_row(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue = ValidatorStateStore(db_path=db_path)

        queue.enqueue(PENDING_CONFIRM_SAMPLE1)
        queue.enqueue(replace(PENDING_CONFIRM_SAMPLE1, from_tx_hash='tx-new'))

        items = queue.get_all()
        assert queue.pending_size() == 1
        assert len(items) == 1
        assert items[0].from_tx_hash == 'tx-new'

    def test_has_reflects_enqueue_and_remove(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue = ValidatorStateStore(db_path=db_path)

        assert not queue.has('miner-1')

        queue.enqueue(PENDING_CONFIRM_SAMPLE1)
        assert queue.has('miner-1')

        removed = queue.remove('miner-1')
        assert removed is not None
        assert removed.miner_hotkey == 'miner-1'
        assert not queue.has('miner-1')

    def test_purge_expired_pending_confirms_removes_stale_entries(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue = ValidatorStateStore(
            db_path=db_path,
            current_block_fn=lambda: 101,
        )

        queue.enqueue(PENDING_CONFIRM_SAMPLE1)  # reserved_until=100 → expired at block 101
        queue.enqueue(replace(PENDING_CONFIRM_SAMPLE2, reserved_until=105))

        removed = queue.purge_expired_pending_confirms()
        assert removed == 1

        items = queue.get_all()
        assert [item.miner_hotkey for item in items] == ['miner-2']
        assert not queue.has('miner-1')
        assert queue.has('miner-2')
        assert queue.pending_size() == 1

    def test_update_reserved_until_prevents_stale_purge(self, tmp_path: Path):
        """Regression: after the contract extends a reservation, refreshing the
        cached reserved_until must keep the row alive past its original TTL."""
        db_path = tmp_path / 'state.db'
        current_block = 105
        queue = ValidatorStateStore(db_path=db_path, current_block_fn=lambda: current_block)

        queue.enqueue(PENDING_CONFIRM_SAMPLE1)  # reserved_until=100
        queue.update_reserved_until('miner-1', 130)

        items = queue.get_all()
        assert len(items) == 1
        assert items[0].reserved_until == 130

        removed = queue.purge_expired_pending_confirms()
        assert removed == 0
        assert queue.has('miner-1')

    def test_update_reserved_until_unknown_hotkey_is_noop(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue = ValidatorStateStore(db_path=db_path)
        queue.update_reserved_until('miner-unknown', 999)
        assert queue.pending_size() == 0

    def test_enqueue_and_remove_are_safe_across_threads(self, tmp_path: Path):
        db_path = tmp_path / 'state.db'
        queue = ValidatorStateStore(db_path=db_path)
        removed_hotkeys: list[str] = []

        def writer():
            queue.enqueue(PENDING_CONFIRM_SAMPLE1)
            time.sleep(0.01)
            queue.enqueue(PENDING_CONFIRM_SAMPLE2)

        def remover():
            deadline = time.monotonic() + 2.0
            while len(removed_hotkeys) < 2 and time.monotonic() < deadline:
                for item in queue.get_all():
                    removed = queue.remove(item.miner_hotkey)
                    if removed is not None:
                        removed_hotkeys.append(removed.miner_hotkey)
                time.sleep(0.001)

        writer_thread = threading.Thread(target=writer)
        remover_thread = threading.Thread(target=remover)

        writer_thread.start()
        remover_thread.start()
        writer_thread.join()
        remover_thread.join()

        assert set(removed_hotkeys) == {'miner-1', 'miner-2'}
        assert queue.pending_size() == 0
        assert queue.get_all() == []
