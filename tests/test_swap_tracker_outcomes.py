"""C4 — verify SwapTracker writes swap_outcomes on terminal-state transitions."""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.rate_state import RateStateStore
from allways.validator.swap_tracker import SwapTracker


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


def _make_tracker(tmp_path: Path) -> SwapTracker:
    store = RateStateStore(db_path=tmp_path / 'rate_state.db')
    client = MagicMock()
    tracker = SwapTracker(
        client=client,
        fulfillment_timeout_blocks=30,
        rate_state_store=store,
    )
    return tracker


class TestResolveOutcome:
    def test_resolve_completed_writes_row_with_completed_one(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=42, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=42, status=SwapStatus.COMPLETED, block=250)

        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_miner': (1, 0)}
        tracker.rate_state_store.close()

    def test_resolve_timed_out_writes_row_with_completed_zero(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=43, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=43, status=SwapStatus.TIMED_OUT, block=260)

        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_miner': (0, 1)}
        tracker.rate_state_store.close()

    def test_resolve_idempotent_second_call_noop(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=44, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=44, status=SwapStatus.COMPLETED, block=270)
        # Swap is gone from active — second call is a no-op
        tracker.resolve(swap_id=44, status=SwapStatus.TIMED_OUT, block=280)

        # First outcome wins; second call never wrote anything
        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_miner': (1, 0)}
        tracker.rate_state_store.close()

    def test_resolve_drops_swap_from_active(self, tmp_path: Path):
        """After resolve, the swap is gone from active tracking."""
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=45, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=45, status=SwapStatus.COMPLETED, block=290)

        assert 45 not in tracker.active
        tracker.rate_state_store.close()


class TestPollInnerRecordsOutcome:
    """Covers the two paths inside _poll_inner that transition swaps to terminal state."""

    def test_contract_removed_infers_completed_writes_outcome(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=50, miner_hotkey='hk_pollinner', timeout_block=1000)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 50
        tracker._current_block = 500  # not past timeout → COMPLETED

        # client.get_next_swap_id → no new swaps
        tracker.client.get_next_swap_id.return_value = 51
        # client.get_swap(50) → None (contract removed it)
        tracker.client.get_swap.return_value = None

        asyncio.run(tracker._poll_inner())

        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_pollinner': (1, 0)}
        tracker.rate_state_store.close()

    def test_contract_removed_past_timeout_infers_timed_out(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        swap = _make_swap(swap_id=51, miner_hotkey='hk_late', timeout_block=400)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 51
        tracker._current_block = 500  # past timeout_block=400

        tracker.client.get_next_swap_id.return_value = 52
        tracker.client.get_swap.return_value = None

        asyncio.run(tracker._poll_inner())

        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_late': (0, 1)}
        tracker.rate_state_store.close()

    def test_contract_returns_terminal_state_writes_outcome(self, tmp_path: Path):
        tracker = _make_tracker(tmp_path)
        stale_swap = _make_swap(swap_id=52, miner_hotkey='hk_terminal')
        tracker.active[stale_swap.id] = stale_swap
        tracker.last_scanned_id = 52

        # Contract returns a terminal-state swap (race: resolved but still readable)
        resolved_swap = _make_swap(swap_id=52, miner_hotkey='hk_terminal')
        resolved_swap.status = SwapStatus.COMPLETED
        tracker.client.get_next_swap_id.return_value = 53
        tracker.client.get_swap.return_value = resolved_swap

        asyncio.run(tracker._poll_inner())

        rates = tracker.rate_state_store.get_all_time_success_rates()
        assert rates == {'hk_terminal': (1, 0)}
        tracker.rate_state_store.close()
