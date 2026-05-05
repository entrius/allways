"""SwapTracker: active-set management and RPC resilience.

Outcome persistence is owned by ``ContractEventWatcher``, and resolution
is event-driven: SwapCompleted/SwapTimedOut → tracker.resolve(). The
tracker holds an authoritative active set; transient None / RPC errors
during refresh are treated as "no information", never as resolution.
"""

import asyncio
from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.validator.swap_tracker import MAX_INIT_GAP, SwapTracker


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
    return SwapTracker(client=client)


class TestResolve:
    def test_resolve_drops_swap_from_active(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=45, miner_hotkey='hk_miner')
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=45, status=SwapStatus.COMPLETED, block=290)

        assert 45 not in tracker.active

    def test_resolve_clears_voted_state(self):
        tracker = make_tracker()
        swap = make_swap(swap_id=46)
        tracker.active[swap.id] = swap
        tracker.mark_voted(46)

        tracker.resolve(swap_id=46, status=SwapStatus.COMPLETED, block=300)

        assert not tracker.is_voted(46)

    def test_resolve_unknown_swap_is_noop(self):
        tracker = make_tracker()
        tracker.resolve(swap_id=999, status=SwapStatus.COMPLETED, block=300)
        assert len(tracker.active) == 0


class TestInitialize:
    def test_initialize_empty_contract_sets_cursor_to_zero(self):
        tracker = make_tracker()
        tracker.client.get_next_swap_id.return_value = 1  # next_id=1 means no swaps exist

        tracker.initialize()

        assert tracker.last_scanned_id == 0
        assert tracker.active == {}

    def test_initialize_picks_up_active_swaps_from_backward_scan(self):
        tracker = make_tracker()
        swap_active = make_swap(swap_id=10)
        swap_fulfilled = make_swap(swap_id=11)
        swap_fulfilled.status = SwapStatus.FULFILLED

        tracker.client.get_next_swap_id.return_value = 12
        tracker.client.get_swap.side_effect = lambda sid: {10: swap_active, 11: swap_fulfilled}.get(sid)

        tracker.initialize()

        assert 10 in tracker.active
        assert 11 in tracker.active
        assert tracker.last_scanned_id == 11

    def test_initialize_recovers_old_orphaned_active_swap(self):
        """An ACTIVE swap older than any cold-start time window must be picked
        up — it is the exact case (validator outage long enough that no one
        voted timeout) the recovery scan exists for.
        """
        tracker = make_tracker()
        ancient = make_swap(swap_id=1)
        ancient.initiated_block = 100  # arbitrarily old vs current_block

        tracker.client.get_next_swap_id.return_value = 2
        tracker.client.get_swap.side_effect = lambda sid: {1: ancient}.get(sid)

        tracker.initialize()

        assert 1 in tracker.active
        assert tracker.last_scanned_id == 1

    def test_initialize_halts_after_consecutive_pruned_swaps(self):
        """A long run of pruned (None) swap IDs short-circuits the scan so
        cold start stays bounded on contracts with millions of resolved swaps.
        """
        tracker = make_tracker()
        recent = make_swap(swap_id=1000)
        # swaps 1..999 all return None (pruned). The active swap at 1000 is
        # found, then the scan walks 999..(1000 - MAX_INIT_GAP) Nones and stops.
        tracker.client.get_next_swap_id.return_value = 1001
        tracker.client.get_swap.side_effect = lambda sid: recent if sid == 1000 else None

        tracker.initialize()

        assert 1000 in tracker.active
        # call_count == the active swap + MAX_INIT_GAP Nones before halting
        assert tracker.client.get_swap.call_count == 1 + MAX_INIT_GAP

    def test_initialize_skips_terminal_swaps(self):
        tracker = make_tracker()
        completed = make_swap(swap_id=20)
        completed.status = SwapStatus.COMPLETED

        tracker.client.get_next_swap_id.return_value = 21
        tracker.client.get_swap.return_value = completed

        tracker.initialize()

        assert 20 not in tracker.active


class TestRefreshNullHandling:
    def test_null_leaves_swap_in_active(self):
        """Resolution is event-driven; refresh-time None is no-op."""
        tracker = make_tracker()
        swap = make_swap(swap_id=50)
        tracker.active[swap.id] = swap
        tracker.last_scanned_id = 50

        tracker.client.get_next_swap_id.return_value = 51
        tracker.client.get_swap.return_value = None

        for _ in range(5):
            asyncio.run(tracker.poll_inner())

        assert 50 in tracker.active

    def test_event_driven_resolve_drops_swap(self):
        """The watcher's SwapCompleted/SwapTimedOut path drives resolution."""
        tracker = make_tracker()
        swap = make_swap(swap_id=51)
        tracker.active[swap.id] = swap

        tracker.resolve(swap_id=51, status=SwapStatus.COMPLETED, block=600)

        assert 51 not in tracker.active

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

        # Transient RPC error must not drop the swap — that's how a flaky WS
        # used to silently strand still-active swaps.
        assert 60 in tracker.active
        assert tracker.active[61].status == SwapStatus.FULFILLED

    def test_exception_does_not_drop_swap(self):
        tracker = make_tracker()
        tracker.active[70] = make_swap(swap_id=70)
        tracker.last_scanned_id = 70

        tracker.client.get_next_swap_id.return_value = 71
        tracker.client.get_swap.side_effect = RuntimeError('rpc down')

        for _ in range(5):
            asyncio.run(tracker.poll_inner())

        assert 70 in tracker.active

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
