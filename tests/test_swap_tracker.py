"""SwapTracker: active-set management, null-swap retries, RPC resilience.

Outcome persistence moved to ``ContractEventWatcher`` in C14 — SwapTracker
no longer writes to ``swap_outcomes``. These tests cover what the tracker
actually owns: maintaining the active set as swaps move through their
lifecycle and tolerating per-swap RPC flakes without aborting the forward
step.
"""

import asyncio
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.swap_tracker import NULL_SWAP_RETRY_LIMIT, SwapTracker


def make_swap(swap_id: int, miner_hotkey: str = 'hk_a', timeout_block: int = 500) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey=miner_hotkey,
        from_chain='btc',
        to_chain='tao',
        from_amount=100_000,
        to_amount=500_000_000,
        tao_amount=500_000_000,
        user_from_address='bc1q-user',
        user_to_address='5user',
        status=SwapStatus.ACTIVE,
        initiated_block=100,
        timeout_block=timeout_block,
    )


def make_tracker() -> SwapTracker:
    client = MagicMock()
    client.get_active_swaps.return_value = []
    return SwapTracker(client=client)


class TestResolve:
    def test_resolve_drops_swap_from_active(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=45, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=45, status=SwapStatus.COMPLETED, block=290)

        assert 45 not in tracker.active

    def test_resolve_clears_voted_and_retry_state(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=46)
        tracker.active[swap.id] = swap
        tracker.mark_voted(46)
        tracker.null_retry_count[46] = 2

        tracker.resolve(swap_id=46, status=SwapStatus.COMPLETED, block=300)

        assert not tracker.is_voted(46)
        assert 46 not in tracker.null_retry_count

    def test_resolve_unknown_swap_is_noop(self):
        tracker = make_tracker()
        tracker.resolve(swap_id=999, status=SwapStatus.COMPLETED, block=300)
        assert len(tracker.active) == 0


class TestInitialize:
    def test_initialize_empty_contract_sets_cursor_to_zero(self):
        tracker = make_tracker()
        tracker.client.get_next_swap_id.return_value = 1

        tracker.initialize()

        assert tracker.last_scanned_id == 0
        assert tracker.active == {}

    def test_initialize_seeds_active_swaps_from_contract(self):
        tracker = make_tracker()
        swap_a = make_swap(swap_id=10)
        swap_b = make_swap(swap_id=11)
        swap_b.status = SwapStatus.FULFILLED

        tracker.client.get_next_swap_id.return_value = 12
        tracker.client.get_active_swaps.return_value = [swap_a, swap_b]

        tracker.initialize()

        assert set(tracker.active) == {10, 11}
        assert tracker.last_scanned_id == 11

    def test_initialize_keeps_swap_with_extended_timeout(self):
        """A swap initiated well before one fulfillment-timeout window can
        still be active after ``vote_extend_timeout`` and must survive
        restart — the earlier initiated_block cutoff dropped these."""
        tracker = make_tracker()
        extended = make_swap(swap_id=3)
        extended.status = SwapStatus.FULFILLED
        extended.initiated_block = 100   # far in the past
        extended.timeout_block = 1020    # extended into the future

        tracker.client.get_next_swap_id.return_value = 4
        tracker.client.get_active_swaps.return_value = [extended]

        tracker.initialize()

        assert 3 in tracker.active
        assert tracker.last_scanned_id == 3

    def test_initialize_advances_cursor_past_terminal_latest(self):
        """Latest swap is terminal (pruned from contract); cursor still
        advances so the next poll doesn't rediscover it."""
        tracker = make_tracker()
        still_active = make_swap(swap_id=40)
        still_active.status = SwapStatus.FULFILLED

        tracker.client.get_next_swap_id.return_value = 42  # latest id = 41, terminal
        tracker.client.get_active_swaps.return_value = [still_active]

        tracker.initialize()

        assert set(tracker.active) == {40}
        assert tracker.last_scanned_id == 41

    def test_initialize_requests_full_scan(self):
        """An extended-timeout swap can sit behind any run of pruned
        neighbors, so the scan must disable the gap heuristic entirely."""
        tracker = make_tracker()
        tracker.client.get_next_swap_id.return_value = 500

        tracker.initialize()

        (_args, kwargs) = tracker.client.get_active_swaps.call_args
        assert kwargs['max_gap'] is None


class TestNullSwapRetry:
    def test_transient_null_does_not_drop_immediately(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=50)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 50

        tracker.client.get_next_swap_id.return_value = 51
        tracker.client.get_swap.return_value = None

        asyncio.run(tracker.poll_inner())

        assert 50 in tracker.active
        assert tracker.null_retry_count.get(50) == 1

    def test_null_drops_after_retry_limit(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=51)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 51

        tracker.client.get_next_swap_id.return_value = 52
        tracker.client.get_swap.return_value = None

        for _ in range(NULL_SWAP_RETRY_LIMIT):
            asyncio.run(tracker.poll_inner())

        assert 51 not in tracker.active
        assert 51 not in tracker.null_retry_count

    def test_successful_refetch_resets_retry_count(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=52)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 52
        tracker.null_retry_count[52] = 1

        refreshed = make_swap(swap_id=52)
        refreshed.status = SwapStatus.FULFILLED
        tracker.client.get_next_swap_id.return_value = 53
        tracker.client.get_swap.return_value = refreshed

        asyncio.run(tracker.poll_inner())

        assert 52 in tracker.active
        assert tracker.active[52].status == SwapStatus.FULFILLED
        assert 52 not in tracker.null_retry_count

    def test_terminal_status_drops_without_retry(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=53)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 53

        terminal = make_swap(swap_id=53)
        terminal.status = SwapStatus.COMPLETED
        tracker.client.get_next_swap_id.return_value = 54
        tracker.client.get_swap.return_value = terminal

        asyncio.run(tracker.poll_inner())

        assert 53 not in tracker.active


class TestRPCResilience:
    """R2: a single flaky get_swap must not abort the whole poll."""

    def test_exception_on_one_swap_still_processes_others(self):
        tracker = make_tracker()
        swap_a = make_swap(swap_id=60)
        swap_b = make_swap(swap_id=61)
        tracker.active[60] = swap_a
        tracker.active[61] = swap_b
        tracker.last_scanned_id = 61

        refreshed_b = make_swap(swap_id=61)
        refreshed_b.status = SwapStatus.FULFILLED

        def get_swap_flake(sid: int):
            if sid == 60:
                raise RuntimeError('websocket flake')
            return refreshed_b

        tracker.client.get_next_swap_id.return_value = 62
        tracker.client.get_swap.side_effect = get_swap_flake

        asyncio.run(tracker.poll_inner())

        assert 60 in tracker.active
        assert tracker.null_retry_count.get(60) == 1
        assert tracker.active[61].status == SwapStatus.FULFILLED

    def test_exception_counts_toward_retry_limit(self):
        tracker = make_tracker()
        tracker.active[70] = make_swap(swap_id=70)
        tracker.last_scanned_id = 70

        tracker.client.get_next_swap_id.return_value = 71
        tracker.client.get_swap.side_effect = RuntimeError('rpc down')

        for _ in range(NULL_SWAP_RETRY_LIMIT):
            asyncio.run(tracker.poll_inner())

        assert 70 not in tracker.active

    def test_discovery_exception_does_not_break_pass(self):
        """Flaky get_swap during new-ID discovery is skipped, not fatal."""
        tracker = make_tracker()
        tracker.last_scanned_id = 0
        good_swap = make_swap(swap_id=2)

        tracker.client.get_next_swap_id.return_value = 3  # ids 1 and 2

        def get_swap_flake(sid: int):
            if sid == 1:
                raise RuntimeError('rpc flake on id 1')
            return good_swap

        tracker.client.get_swap.side_effect = get_swap_flake

        asyncio.run(tracker.poll_inner())

        assert 2 in tracker.active
        assert 1 not in tracker.active


class TestPruneStaleVotedIds:
    """R2 bonus: voted_ids should never accumulate orphans."""

    def test_orphan_vote_without_active_entry_is_pruned(self):
        tracker = make_tracker()
        tracker.active[1] = make_swap(swap_id=1)
        tracker.mark_voted(1)
        tracker.mark_voted(99)  # orphan — 99 was never in active

        tracker.prune_stale_voted_ids()

        assert tracker.is_voted(1)
        assert not tracker.is_voted(99)

    def test_prune_empty_voted_set_is_noop(self):
        tracker = make_tracker()
        tracker.prune_stale_voted_ids()
        assert tracker.voted_ids == set()

    def test_prune_runs_automatically_in_poll_noop_path(self):
        tracker = make_tracker()
        tracker.mark_voted(42)  # orphan
        tracker.last_scanned_id = 0
        tracker.client.get_next_swap_id.return_value = 1

        asyncio.run(tracker.poll_inner())

        assert not tracker.is_voted(42)


class TestGetFulfilled:
    def test_excludes_non_fulfilled(self):
        tracker = make_tracker()
        active_swap = make_swap(swap_id=1, timeout_block=500)
        fulfilled_swap = make_swap(swap_id=2, timeout_block=500)
        fulfilled_swap.status = SwapStatus.FULFILLED
        tracker.active[1] = active_swap
        tracker.active[2] = fulfilled_swap

        result = tracker.get_fulfilled(current_block=100)
        assert [s.id for s in result] == [2]

    def test_excludes_past_timeout(self):
        tracker = make_tracker()
        expired = make_swap(swap_id=3, timeout_block=100)
        expired.status = SwapStatus.FULFILLED
        tracker.active[3] = expired

        assert tracker.get_fulfilled(current_block=101) == []
        assert tracker.get_fulfilled(current_block=100) == [expired]

    def test_timeout_zero_treated_as_unbounded(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=4, timeout_block=0)
        swap.status = SwapStatus.FULFILLED
        tracker.active[4] = swap

        assert tracker.get_fulfilled(current_block=999_999) == [swap]


class TestGetNearTimeoutFulfilled:
    def test_returns_swaps_within_threshold(self):
        from allways.constants import EXTEND_THRESHOLD_BLOCKS

        tracker = make_tracker()
        near = make_swap(swap_id=1, timeout_block=100)
        near.status = SwapStatus.FULFILLED
        far = make_swap(swap_id=2, timeout_block=100 + EXTEND_THRESHOLD_BLOCKS * 10)
        far.status = SwapStatus.FULFILLED
        tracker.active[1] = near
        tracker.active[2] = far

        # current = timeout_block - threshold → near qualifies, far doesn't
        current_block = 100 - EXTEND_THRESHOLD_BLOCKS
        result = tracker.get_near_timeout_fulfilled(current_block=current_block)
        assert [s.id for s in result] == [1]

    def test_excludes_active_status(self):
        tracker = make_tracker()
        active = make_swap(swap_id=1, timeout_block=100)
        tracker.active[1] = active

        assert tracker.get_near_timeout_fulfilled(current_block=90) == []


class TestGetTimedOut:
    def test_includes_active_and_fulfilled_past_timeout(self):
        tracker = make_tracker()
        active_expired = make_swap(swap_id=1, timeout_block=100)
        fulfilled_expired = make_swap(swap_id=2, timeout_block=100)
        fulfilled_expired.status = SwapStatus.FULFILLED
        not_yet = make_swap(swap_id=3, timeout_block=500)
        tracker.active[1] = active_expired
        tracker.active[2] = fulfilled_expired
        tracker.active[3] = not_yet

        result = tracker.get_timed_out(current_block=101)
        assert sorted(s.id for s in result) == [1, 2]

    def test_excludes_at_timeout_block(self):
        """Strict `>`: exactly at timeout_block is NOT yet timed out."""
        tracker = make_tracker()
        swap = make_swap(swap_id=1, timeout_block=100)
        tracker.active[1] = swap

        assert tracker.get_timed_out(current_block=100) == []
        assert tracker.get_timed_out(current_block=101) == [swap]

    def test_timeout_zero_is_never_timed_out(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=1, timeout_block=0)
        tracker.active[1] = swap

        assert tracker.get_timed_out(current_block=999_999) == []
