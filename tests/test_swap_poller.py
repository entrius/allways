"""Tests for allways.miner.swap_poller.SwapPoller."""

from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.miner.swap_poller import SwapPoller

MINER_HK = 'miner-hk'
OTHER_HK = 'other-hk'


def make_swap(
    swap_id: int,
    miner_hotkey: str = MINER_HK,
    status: SwapStatus = SwapStatus.ACTIVE,
) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey=miner_hotkey,
        from_chain='btc',
        to_chain='tao',
        from_amount=1_000_000,
        to_amount=345_000_000,
        tao_amount=345_000_000,
        user_from_address='bc1q-user',
        user_to_address='5user',
        miner_from_address='bc1q-miner',
        rate='345',
        status=status,
        initiated_block=100,
        timeout_block=500,
    )


def make_poller(next_id: int = 1, swaps_by_id=None):
    client = MagicMock()
    client.get_next_swap_id.return_value = next_id
    swaps_by_id = swaps_by_id or {}
    client.get_swap.side_effect = lambda sid: swaps_by_id.get(sid)
    return SwapPoller(client, MINER_HK)


class TestPollInitialState:
    def test_empty_contract(self):
        poller = make_poller(next_id=1)
        active, fulfilled = poller.poll()
        assert active == []
        assert fulfilled == []
        assert poller.last_poll_ok is True

    def test_no_swaps_for_this_miner(self):
        swaps = {1: make_swap(1, miner_hotkey=OTHER_HK)}
        poller = make_poller(next_id=2, swaps_by_id=swaps)
        active, fulfilled = poller.poll()
        assert active == []
        assert fulfilled == []


class TestPollDiscovery:
    def test_finds_active_swap_assigned_to_miner(self):
        swaps = {1: make_swap(1, status=SwapStatus.ACTIVE)}
        poller = make_poller(next_id=2, swaps_by_id=swaps)
        active, fulfilled = poller.poll()
        assert len(active) == 1
        assert active[0].id == 1
        assert fulfilled == []

    def test_finds_fulfilled_swap(self):
        swaps = {2: make_swap(2, status=SwapStatus.FULFILLED)}
        poller = make_poller(next_id=3, swaps_by_id=swaps)
        active, fulfilled = poller.poll()
        assert active == []
        assert len(fulfilled) == 1
        assert fulfilled[0].id == 2

    def test_skips_completed_swap(self):
        swaps = {1: make_swap(1, status=SwapStatus.COMPLETED)}
        poller = make_poller(next_id=2, swaps_by_id=swaps)
        active, fulfilled = poller.poll()
        assert active == []
        assert fulfilled == []

    def test_skips_timed_out_swap(self):
        swaps = {1: make_swap(1, status=SwapStatus.TIMED_OUT)}
        poller = make_poller(next_id=2, swaps_by_id=swaps)
        active, fulfilled = poller.poll()
        assert active == []

    def test_skips_none_result(self):
        poller = make_poller(next_id=2, swaps_by_id={})
        active, fulfilled = poller.poll()
        assert active == []
        assert fulfilled == []


class TestCursor:
    def test_cursor_advances_after_scan(self):
        swaps = {1: make_swap(1), 2: make_swap(2)}
        poller = make_poller(next_id=3, swaps_by_id=swaps)
        poller.poll()
        assert poller.last_scanned_id == 2

    def test_cursor_not_advanced_when_contract_empty(self):
        poller = make_poller(next_id=1)
        poller.poll()
        assert poller.last_scanned_id == 0

    def test_cursor_skips_already_scanned_ids(self):
        swaps = {1: make_swap(1)}
        poller = make_poller(next_id=2, swaps_by_id=swaps)
        poller.last_scanned_id = 5
        poller.poll()
        assert poller.client.get_swap.call_count == 0


class TestRefreshActive:
    def test_removes_resolved_swap(self):
        active_swap = make_swap(1, status=SwapStatus.ACTIVE)
        poller = make_poller(next_id=2, swaps_by_id={1: active_swap})
        poller.poll()
        assert 1 in poller.active

        completed = make_swap(1, status=SwapStatus.COMPLETED)
        poller.client.get_swap.side_effect = lambda sid: {1: completed}.get(sid)
        poller.client.get_next_swap_id.return_value = 2
        active, fulfilled = poller.poll()
        assert active == []
        assert 1 not in poller.active

    def test_removes_missing_swap(self):
        active_swap = make_swap(1, status=SwapStatus.ACTIVE)
        poller = make_poller(next_id=2, swaps_by_id={1: active_swap})
        poller.poll()

        poller.client.get_swap.side_effect = lambda sid: None
        poller.client.get_next_swap_id.return_value = 2
        poller.poll()
        assert 1 not in poller.active

    def test_updates_active_swap_state(self):
        active_swap = make_swap(1, status=SwapStatus.ACTIVE)
        poller = make_poller(next_id=2, swaps_by_id={1: active_swap})
        poller.poll()

        updated = make_swap(1, status=SwapStatus.FULFILLED)
        poller.client.get_swap.side_effect = lambda sid: {1: updated}.get(sid)
        poller.client.get_next_swap_id.return_value = 2
        active, fulfilled = poller.poll()
        assert active == []
        assert len(fulfilled) == 1


class TestErrorHandling:
    def test_exception_in_inner_sets_flag(self):
        poller = make_poller(next_id=2)
        poller.client.get_next_swap_id.side_effect = RuntimeError('rpc down')
        active, fulfilled = poller.poll()
        assert active == []
        assert fulfilled == []
        assert poller.last_poll_ok is False

    def test_successful_poll_sets_flag_true(self):
        poller = make_poller(next_id=1)
        poller.last_poll_ok = False
        poller.poll()
        assert poller.last_poll_ok is True
