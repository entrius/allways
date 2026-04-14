"""SwapTracker active-set management (outcome persistence is the event watcher's job)."""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.swap_tracker import NULL_SWAP_RETRY_LIMIT, SwapTracker


def _make_swap(swap_id: int, miner_hotkey: str = 'hk_a', timeout_block: int = 500) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey=miner_hotkey,
        source_chain='btc',
        dest_chain='tao',
        source_amount=100_000,
        dest_amount=500_000_000,
        tao_amount=500_000_000,
        user_source_address='bc1q-user',
        user_dest_address='5user',
        status=SwapStatus.ACTIVE,
        initiated_block=100,
        timeout_block=timeout_block,
    )


def _make_tracker() -> SwapTracker:
    client = MagicMock()
    return SwapTracker(client=client, fulfillment_timeout_blocks=30)


class TestResolve:
    def test_resolve_drops_swap_from_active(self, tmp_path: Path):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=45, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=45, status=SwapStatus.COMPLETED, block=290)

        assert 45 not in tracker.active

    def test_resolve_clears_voted_and_retry_state(self, tmp_path: Path):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=46)
        tracker.active[swap.id] = swap
        tracker.mark_voted(46)
        tracker._null_retry_count[46] = 2

        tracker.resolve(swap_id=46, status=SwapStatus.COMPLETED, block=300)

        assert not tracker.is_voted(46)
        assert 46 not in tracker._null_retry_count

    def test_resolve_unknown_swap_is_noop(self, tmp_path: Path):
        tracker = _make_tracker()
        tracker.resolve(swap_id=999, status=SwapStatus.COMPLETED, block=300)
        # No exception, nothing changes
        assert len(tracker.active) == 0


class TestNullSwapRetry:
    def test_transient_null_does_not_drop_immediately(self):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=50)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 50

        tracker.client.get_next_swap_id.return_value = 51
        tracker.client.get_swap.return_value = None

        asyncio.run(tracker._poll_inner())

        # First None: swap stays, retry count incremented
        assert 50 in tracker.active
        assert tracker._null_retry_count.get(50) == 1

    def test_null_drops_after_retry_limit(self):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=51)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 51

        tracker.client.get_next_swap_id.return_value = 52
        tracker.client.get_swap.return_value = None

        for _ in range(NULL_SWAP_RETRY_LIMIT):
            asyncio.run(tracker._poll_inner())

        assert 51 not in tracker.active
        assert 51 not in tracker._null_retry_count

    def test_successful_refetch_resets_retry_count(self):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=52)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 52
        tracker._null_retry_count[52] = 1

        refreshed = _make_swap(swap_id=52)
        refreshed.status = SwapStatus.FULFILLED
        tracker.client.get_next_swap_id.return_value = 53
        tracker.client.get_swap.return_value = refreshed

        asyncio.run(tracker._poll_inner())

        assert 52 in tracker.active
        assert tracker.active[52].status == SwapStatus.FULFILLED
        assert 52 not in tracker._null_retry_count

    def test_terminal_status_drops_without_retry(self):
        tracker = _make_tracker()
        swap = _make_swap(swap_id=53)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 53

        terminal = _make_swap(swap_id=53)
        terminal.status = SwapStatus.COMPLETED
        tracker.client.get_next_swap_id.return_value = 54
        tracker.client.get_swap.return_value = terminal

        asyncio.run(tracker._poll_inner())

        assert 53 not in tracker.active
