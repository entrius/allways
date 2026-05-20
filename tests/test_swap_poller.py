from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.miner.swap_poller import ACTIVE_STATUSES, MAX_REFRESH_MISSES, RESCAN_WINDOW, SwapPoller


def make_swap(swap_id: int = 1, status: SwapStatus = SwapStatus.ACTIVE, miner_hotkey: str = 'miner') -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey=miner_hotkey,
        from_chain='btc',
        to_chain='tao',
        from_amount=1_000,
        to_amount=2_000,
        tao_amount=2_000,
        user_from_address='bc1q-user',
        user_to_address='5user',
        miner_from_address='bc1q-miner',
        miner_to_address='5miner',
        rate='2',
        status=status,
        initiated_block=10,
        timeout_block=100,
    )


def make_poller(client: MagicMock | None = None) -> SwapPoller:
    return SwapPoller(contract_client=client or MagicMock(), miner_hotkey='miner')


def assert_poller_invariants(poller: SwapPoller):
    assert set(poller.active_miss_counts) <= set(poller.active)
    assert all(swap.status in ACTIVE_STATUSES for swap in poller.active.values())
    assert all(0 < misses < MAX_REFRESH_MISSES for misses in poller.active_miss_counts.values())


def test_active_swap_retained_when_refresh_returns_none():
    client = MagicMock()
    client.get_next_swap_id.return_value = 2
    client.get_swap.return_value = None
    poller = make_poller(client)
    active_swap = make_swap()
    poller.active[active_swap.id] = active_swap
    poller.last_scanned_id = 1

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {1: active_swap}
    assert poller.active_miss_counts == {1: 1}
    assert active == [active_swap]
    assert fulfilled == []


def test_active_swap_retained_when_refresh_raises():
    client = MagicMock()
    client.get_next_swap_id.return_value = 2
    client.get_swap.side_effect = TimeoutError('temporary substrate miss')
    poller = make_poller(client)
    active_swap = make_swap()
    poller.active[active_swap.id] = active_swap
    poller.last_scanned_id = 1

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {1: active_swap}
    assert poller.active_miss_counts == {}
    assert active == [active_swap]
    assert fulfilled == []


def test_active_swap_removed_after_repeated_refresh_misses():
    client = MagicMock()
    client.get_next_swap_id.return_value = 2
    client.get_swap.return_value = None
    poller = make_poller(client)
    active_swap = make_swap()
    poller.active[active_swap.id] = active_swap
    poller.last_scanned_id = 1

    for _ in range(2):
        active, fulfilled = poller.poll()
        assert active == [active_swap]
        assert fulfilled == []

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {}
    assert poller.active_miss_counts == {}
    assert active == []
    assert fulfilled == []


def test_poll_sequence_preserves_active_state_invariants():
    """Mixed refresh outcomes must keep poller bookkeeping internally coherent.

    This covers the invariant the miner relies on after each poll: every miss
    counter belongs to a still-active swap, terminal swaps are gone, and a valid
    refresh clears prior transient-miss state before fulfillment cleanup sees it.
    """
    client = MagicMock()
    client.get_next_swap_id.return_value = 200
    poller = make_poller(client)
    poller.last_scanned_id = 199
    poller.active = {
        100: make_swap(100),
        101: make_swap(101, status=SwapStatus.FULFILLED),
        102: make_swap(102),
    }

    client.get_swap.side_effect = [None] * RESCAN_WINDOW + [
        None,
        TimeoutError('temporary refresh miss'),
        make_swap(102, status=SwapStatus.COMPLETED),
    ]
    active, fulfilled = poller.poll()

    assert_poller_invariants(poller)
    assert set(poller.active) == {100, 101}
    assert poller.active_miss_counts == {100: 1}
    assert active == [poller.active[100]]
    assert fulfilled == [poller.active[101]]

    client.get_swap.side_effect = [None] * RESCAN_WINDOW + [
        make_swap(100),
        make_swap(101, status=SwapStatus.FULFILLED),
    ]
    active, fulfilled = poller.poll()

    assert_poller_invariants(poller)
    assert set(poller.active) == {100, 101}
    assert poller.active_miss_counts == {}
    assert active == [poller.active[100]]
    assert fulfilled == [poller.active[101]]


def test_active_swap_removed_on_terminal_status():
    terminal_swap = make_swap(status=SwapStatus.COMPLETED)
    client = MagicMock()
    client.get_next_swap_id.return_value = 2
    client.get_swap.return_value = terminal_swap
    poller = make_poller(client)
    poller.active[terminal_swap.id] = make_swap()
    poller.last_scanned_id = 1

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {}
    assert active == []
    assert fulfilled == []


def test_active_swap_retained_when_discovery_raises():
    client = MagicMock()
    active_swap = make_swap()
    client.get_next_swap_id.return_value = 3
    client.get_swap.side_effect = [
        RuntimeError('temporary discovery miss'),
        None,
        active_swap,
    ]
    poller = make_poller(client)
    poller.active[active_swap.id] = active_swap

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {1: active_swap}
    assert poller.active_miss_counts == {}
    assert active == [active_swap]
    assert fulfilled == []


def test_discovers_new_active_swap():
    active_swap = make_swap()
    client = MagicMock()
    client.get_next_swap_id.return_value = 2
    client.get_swap.return_value = active_swap
    poller = make_poller(client)

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert poller.active == {1: active_swap}
    assert active == [active_swap]
    assert fulfilled == []
