"""Tests for scoring window persistence (ScoringWindowStore + SwapTracker integration)."""

import json
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.scoring_store import ScoringWindowStore, _dict_to_swap, _swap_to_dict, resolved_block
from allways.validator.swap_tracker import SwapTracker


def _make_swap(swap_id: int, status: SwapStatus, initiated: int, completed: int = 0, timeout: int = 0) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='5User',
        miner_hotkey='5Miner',
        source_chain='btc',
        dest_chain='tao',
        source_amount=100_000,
        dest_amount=1_000_000_000,
        tao_amount=1_000_000_000,
        user_source_address='bc1qtest',
        user_dest_address='5Dest',
        status=status,
        initiated_block=initiated,
        completed_block=completed,
        timeout_block=timeout,
    )


class TestScoringWindowStore:
    def test_roundtrip_save_load_preserves_all_swap_fields(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        swap = _make_swap(11, SwapStatus.COMPLETED, initiated=100, completed=170, timeout=150)
        swap.miner_source_address = 'bc1qminer'
        swap.miner_dest_address = '5MinerDest'
        swap.rate = '321.12345'
        swap.source_tx_hash = 'source-hash'
        swap.source_tx_block = 12345
        swap.dest_tx_hash = 'dest-hash'
        swap.dest_tx_block = 12399
        swap.fulfilled_block = 165

        store.save([swap], {11})

        window, voted = store.load(window_blocks=3600, current_block=200)
        assert len(window) == 1
        assert window[0] == swap
        assert voted == {11}

    def test_roundtrip(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        swap = _make_swap(1, SwapStatus.COMPLETED, initiated=100, completed=120)
        store.save([swap], {1, 2, 3})

        window, voted = store.load(window_blocks=3600, current_block=200)
        assert len(window) == 1
        assert window[0].id == 1
        assert window[0].status == SwapStatus.COMPLETED
        assert window[0].completed_block == 120
        assert voted == {1, 2, 3}

    def test_prune_on_load(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        old_swap = _make_swap(1, SwapStatus.COMPLETED, initiated=10, completed=20)
        recent_swap = _make_swap(2, SwapStatus.COMPLETED, initiated=3500, completed=3550)
        store.save([old_swap, recent_swap], {1, 2})

        window, voted = store.load(window_blocks=3600, current_block=4000)
        assert len(window) == 1
        assert window[0].id == 2

    def test_empty_file(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)
        window, voted = store.load(window_blocks=3600, current_block=100)
        assert window == []
        assert voted == set()

    def test_corrupt_file(self, tmp_path):
        path = tmp_path / 'window.json'
        path.write_text('not json')
        store = ScoringWindowStore(path)
        window, voted = store.load(window_blocks=3600, current_block=100)
        assert window == []
        assert voted == set()

    def test_partial_swap_data(self, tmp_path):
        path = tmp_path / 'window.json'
        path.write_text(json.dumps({'window': [{'id': 99}], 'voted_ids': []}))
        store = ScoringWindowStore(path)
        window, voted = store.load(window_blocks=3600, current_block=100)
        assert len(window) == 0

    def test_remove(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)
        store.save([], set())
        assert path.exists()
        store.remove()
        assert not path.exists()
        store.remove()  # no error on double remove


class TestSwapToDict:
    def test_roundtrip_all_fields(self):
        swap = _make_swap(42, SwapStatus.TIMED_OUT, initiated=100, timeout=130)
        swap.rate = '345.5'
        swap.source_tx_hash = 'abc123'
        d = _swap_to_dict(swap)
        restored = _dict_to_swap(d)
        assert restored is not None
        assert restored.id == 42
        assert restored.status == SwapStatus.TIMED_OUT
        assert restored.rate == '345.5'
        assert restored.source_tx_hash == 'abc123'
        assert restored.timeout_block == 130

    def test_resolved_block_completed(self):
        swap = _make_swap(8, SwapStatus.COMPLETED, initiated=100, completed=140)
        assert resolved_block(swap) == 140

    def test_resolved_block_timeout(self):
        swap = _make_swap(9, SwapStatus.TIMED_OUT, initiated=100, timeout=150)
        assert resolved_block(swap) == 150

    def test_resolved_block_fallback(self):
        swap = _make_swap(10, SwapStatus.ACTIVE, initiated=111)
        assert resolved_block(swap) == 111


class TestSwapTrackerIntegration:
    def test_window_restored_on_initialize(self, tmp_path):
        """Regression: without persistence, window starts empty after restart,
        and the first scoring cycle with alpha=1.0 wipes all miner scores."""
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        completed = _make_swap(5, SwapStatus.COMPLETED, initiated=100, completed=120)
        store.save([completed], {5})

        mock_client = MagicMock()
        mock_client.get_next_swap_id.return_value = 1

        tracker = SwapTracker(
            client=mock_client,
            fulfillment_timeout_blocks=30,
            window_blocks=3600,
            store=store,
        )
        tracker.initialize(current_block=200)

        assert len(tracker.window) == 1
        assert tracker.window[0].id == 5
        assert tracker.voted_ids == set()

    def test_window_empty_without_store(self):
        """Without store, window is empty on cold start (the original bug)."""
        mock_client = MagicMock()
        mock_client.get_next_swap_id.return_value = 1

        tracker = SwapTracker(
            client=mock_client,
            fulfillment_timeout_blocks=30,
            window_blocks=3600,
        )
        tracker.initialize(current_block=200)

        assert len(tracker.window) == 0
        assert len(tracker.voted_ids) == 0

    def test_mark_voted_persisted_immediately(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        active = _make_swap(1, SwapStatus.FULFILLED, initiated=190, timeout=230)
        mock_client = MagicMock()
        mock_client.get_next_swap_id.return_value = 2
        mock_client.get_swap.return_value = active

        tracker = SwapTracker(
            client=mock_client,
            fulfillment_timeout_blocks=30,
            window_blocks=3600,
            store=store,
        )
        tracker.initialize(current_block=200)
        tracker.mark_voted(1)

        restarted = SwapTracker(
            client=mock_client,
            fulfillment_timeout_blocks=30,
            window_blocks=3600,
            store=store,
        )
        restarted.initialize(current_block=201)
        assert 1 in restarted.voted_ids

    def test_initialize_intersects_voted_ids_with_active_swaps(self, tmp_path):
        path = tmp_path / 'window.json'
        store = ScoringWindowStore(path)

        completed = _make_swap(5, SwapStatus.COMPLETED, initiated=100, completed=120)
        store.save([completed], {1, 2, 999})

        active_1 = _make_swap(1, SwapStatus.ACTIVE, initiated=195, timeout=240)
        active_2 = _make_swap(2, SwapStatus.FULFILLED, initiated=196, timeout=241)

        mock_client = MagicMock()
        mock_client.get_next_swap_id.return_value = 3

        def _get_swap(swap_id):
            if swap_id == 1:
                return active_1
            if swap_id == 2:
                return active_2
            return None

        mock_client.get_swap.side_effect = _get_swap

        tracker = SwapTracker(
            client=mock_client,
            fulfillment_timeout_blocks=30,
            window_blocks=3600,
            store=store,
        )
        tracker.initialize(current_block=200)

        assert tracker.voted_ids == {1, 2}

        persisted = json.loads(path.read_text())
        assert persisted['voted_ids'] == [1, 2]


class TestStoreCompaction:
    def test_load_compacts_invalid_and_stale_entries(self, tmp_path):
        path = tmp_path / 'window.json'
        old_swap = _make_swap(1, SwapStatus.COMPLETED, initiated=10, completed=20)
        fresh_swap = _make_swap(2, SwapStatus.COMPLETED, initiated=200, completed=220)

        path.write_text(
            json.dumps(
                {
                    'window': [_swap_to_dict(old_swap), {'id': 'bad'}, _swap_to_dict(fresh_swap)],
                    'voted_ids': [1, 'oops', 2],
                }
            )
        )

        store = ScoringWindowStore(path)
        window, voted = store.load(window_blocks=100, current_block=250)
        assert [s.id for s in window] == [2]
        assert voted == {1, 2}

        compacted = json.loads(path.read_text())
        assert len(compacted['window']) == 1
        assert compacted['window'][0]['id'] == 2
        assert compacted['voted_ids'] == [1, 2]

    def test_load_compacts_invalid_voted_ids_even_when_window_unchanged(self, tmp_path):
        path = tmp_path / 'window.json'
        fresh_swap = _make_swap(7, SwapStatus.COMPLETED, initiated=210, completed=230)

        path.write_text(
            json.dumps(
                {
                    'window': [_swap_to_dict(fresh_swap)],
                    'voted_ids': [7, 'oops'],
                }
            )
        )

        store = ScoringWindowStore(path)
        window, voted = store.load(window_blocks=100, current_block=250)

        assert [s.id for s in window] == [7]
        assert voted == {7}

        compacted = json.loads(path.read_text())
        assert compacted['window'][0]['id'] == 7
        assert compacted['voted_ids'] == [7]
