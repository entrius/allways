"""SwapPoller: the getProgramAccounts snapshot (sync) and the pushed, point-read follow mode.

A sync is an atomic view: the poller filters the program's swaps to this miner's pubkey and splits them
into (active, fulfilled). With a live program feed it follows pushed swaps by point read instead, and
falls back to syncing whenever the feed can't be trusted.
"""

import types
from unittest.mock import MagicMock

from solders.keypair import Keypair

from allways.miner import swap_poller
from allways.miner.swap_poller import ACTIVE_STATUSES, SwapPoller
from allways.solana.client import swap_key_from_tx_hash


def _acct(miner_bytes: bytes, from_tx_hash: str, status_name: str = 'Active'):
    """Stand-in for a decoded `Swap` account (attribute access, miner as raw 32 bytes)."""
    return types.SimpleNamespace(
        user=bytes(range(32)),
        miner=miner_bytes,
        from_chain='btc',
        to_chain='tao',
        user_from_addr='bc1q-user',
        user_to_addr='5user',
        miner_from_addr='bc1q-miner',
        miner_to_addr='5miner',
        rate=2,
        collateral_amount=2_000,
        from_amount=1_000,
        to_amount=2_000,
        from_tx_hash=from_tx_hash,
        from_tx_block=10,
        to_tx_hash='',
        to_tx_block=0,
        status=types.new_class(status_name)(),
        initiated_at=1000,
        timeout_at=4600,
        max_extend_at=8000,
        fulfilled_at=0,
    )


def _client(active=(), fulfilled=()):
    client = MagicMock()
    rows = [*active, *fulfilled]
    client.get_swaps.side_effect = lambda status=None: [(f'pda{i}', a) for i, a in enumerate(rows)]
    return client


def test_active_statuses_are_active_and_fulfilled():
    assert ACTIVE_STATUSES == ('Active', 'Fulfilled')


def test_filters_to_this_miner_and_splits_active_fulfilled():
    me = Keypair().pubkey()
    other = Keypair().pubkey()
    client = _client(
        active=[_acct(bytes(me), 'aa'), _acct(bytes(other), 'bb')],
        fulfilled=[_acct(bytes(me), 'cc', 'Fulfilled')],
    )
    poller = SwapPoller(client, me)

    active, fulfilled = poller.poll()

    assert poller.last_poll_ok is True
    assert [s.from_tx_hash for s in active] == ['aa']  # 'bb' belongs to another miner
    assert [s.from_tx_hash for s in fulfilled] == ['cc']
    assert active[0].status == 'Active' and fulfilled[0].status == 'Fulfilled'


def test_poll_fetches_one_snapshot():
    me = Keypair().pubkey()
    client = _client(
        active=[_acct(bytes(me), 'aa')],
        fulfilled=[_acct(bytes(me), 'cc', 'Fulfilled')],
    )
    poller = SwapPoller(client, me)
    poller.poll()
    assert client.get_swaps.call_count == 1


def test_empty_when_no_swaps_for_miner():
    me = Keypair().pubkey()
    poller = SwapPoller(_client(active=[_acct(bytes(Keypair().pubkey()), 'aa')]), me)
    active, fulfilled = poller.poll()
    assert active == [] and fulfilled == []
    assert poller.last_poll_ok is True


def test_rpc_failure_sets_last_poll_not_ok_and_returns_empty():
    me = Keypair().pubkey()
    client = MagicMock()
    client.get_swaps.side_effect = ConnectionError('rpc down')
    poller = SwapPoller(client, me)

    active, fulfilled = poller.poll()

    assert active == [] and fulfilled == []
    assert poller.last_poll_ok is False


def test_known_set_tracks_live_swaps_only():
    me = Keypair().pubkey()
    client = _client(active=[_acct(bytes(me), 'aa')])
    poller = SwapPoller(client, me)
    poller.poll()
    assert len(poller.known) == 1

    # Next poll the swap is gone from the snapshot → known shrinks to the live set.
    client.get_swaps.side_effect = lambda status=None: []
    poller.poll()
    assert poller.known == set()


def _miner_state(ok: int, failed: int, active: bool = True):
    return types.SimpleNamespace(successful_swaps=ok, failed_swaps=failed, has_active_swap=active)


def test_terminal_outcome_named_completed(caplog):
    me = Keypair().pubkey()
    client = _client(active=[_acct(bytes(me), 'aa')])
    client.get_miner_state.return_value = _miner_state(3, 1)
    poller = SwapPoller(client, me)
    poller.poll()  # discovers + seeds counter baseline

    client.get_swaps.side_effect = lambda status=None: []
    client.get_miner_state.return_value = _miner_state(4, 1)  # +1 success
    poller.poll()

    assert poller.known == set()
    assert poller._counters == (4, 1)


def test_terminal_outcome_named_slashed(caplog):
    me = Keypair().pubkey()
    client = _client(active=[_acct(bytes(me), 'aa')])
    client.get_miner_state.return_value = _miner_state(3, 1)
    poller = SwapPoller(client, me)
    poller.poll()

    client.get_swaps.side_effect = lambda status=None: []
    client.get_miner_state.return_value = _miner_state(3, 2)  # +1 failure
    poller.poll()

    assert poller._counters == (3, 2)


def test_terminal_outcome_read_failure_degrades(caplog):
    me = Keypair().pubkey()
    client = _client(active=[_acct(bytes(me), 'aa')])
    client.get_miner_state.return_value = _miner_state(0, 0)
    poller = SwapPoller(client, me)
    poller.poll()

    client.get_swaps.side_effect = lambda status=None: []
    client.get_miner_state.side_effect = RuntimeError('rpc down')
    poller.poll()  # must not raise; falls back to ambiguous log

    assert poller.known == set()


# ─── push / follow mode ──────────────────────────────────────────────────────


class FakeFeed:
    """Stands in for ProgramEventFeed: records handlers; a test flips connected/session and pushes events."""

    def __init__(self, connected=True, session=1):
        self.connected = connected
        self.session = session
        self.handlers = {}

    def on(self, name, handler):
        self.handlers[name] = handler

    def push_initiated(self, miner, from_tx_hash):
        ev = types.SimpleNamespace(swap_key=swap_key_from_tx_hash(from_tx_hash), miner=miner)
        self.handlers['SwapInitiated']('SwapInitiated', ev)


def _idle_client():
    client = MagicMock()
    client.get_miner_state.return_value = _miner_state(0, 0, active=False)
    client.get_swaps.return_value = []
    client.get_swap.return_value = None
    return client


def test_sync_skips_the_snapshot_when_nothing_is_in_flight():
    me = Keypair().pubkey()
    client = _idle_client()
    poller = SwapPoller(client, me)  # no feed: syncs every pass, like a miner whose socket is down

    assert poller.poll() == ([], [])
    assert poller.last_poll_ok is True
    assert client.get_swaps.call_count == 0  # MinerState said no swap is Active → no getProgramAccounts


def test_live_feed_idle_miner_makes_no_calls_after_catch_up():
    me = Keypair().pubkey()
    client = _idle_client()
    poller = SwapPoller(client, me, feed=FakeFeed())

    poller.poll()  # first pass: catch-up for session 1
    client.reset_mock()
    for _ in range(5):
        assert poller.poll() == ([], [])
    assert client.method_calls == []


def test_pushed_swap_wakes_the_loop_and_is_point_read_not_scanned():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    woke = []
    poller = SwapPoller(client, me, feed=feed, wake=lambda: woke.append(1))
    poller.poll()

    feed.push_initiated(me, 'aa')
    client.get_swap.side_effect = lambda key: _acct(bytes(me), 'aa')
    active, fulfilled = poller.poll()

    assert woke == [1]
    assert [s.from_tx_hash for s in active] == ['aa'] and fulfilled == []
    assert client.get_swaps.call_count == 0
    assert client.get_swap.call_args.args[0] == swap_key_from_tx_hash('aa').hex()


def test_swap_initiated_for_another_miner_is_ignored():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    woke = []
    poller = SwapPoller(client, me, feed=feed, wake=lambda: woke.append(1))
    poller.poll()

    feed.push_initiated(Keypair().pubkey(), 'aa')
    poller.poll()
    assert woke == [] and client.get_swap.call_count == 0


def test_followed_swap_is_dropped_after_repeated_misses_then_resynced():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    poller = SwapPoller(client, me, feed=feed)
    poller.poll()
    feed.push_initiated(me, 'aa')
    client.get_swap.side_effect = lambda key: _acct(bytes(me), 'aa', 'Fulfilled')
    assert [s.from_tx_hash for s in poller.poll()[1]] == ['aa']

    client.get_swap.side_effect = lambda key: None  # closed on-chain (or one lagging node)
    poller.poll()
    assert poller.known == {swap_key_from_tx_hash('aa').hex()}  # one miss: still followed
    poller.poll()
    assert poller.known == set()  # second miss: closed

    client.get_miner_state.reset_mock()
    poller.poll()  # the close is confirmed against chain state
    assert client.get_miner_state.called


def test_pushed_key_that_never_appears_forces_a_sync_after_the_grace(monkeypatch):
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    poller = SwapPoller(client, me, feed=feed)
    poller.poll()
    clock = [1000.0]
    monkeypatch.setattr(swap_poller.time, 'monotonic', lambda: clock[0])
    feed.push_initiated(me, 'aa')

    poller.poll()  # lagging node: not readable yet, keep waiting
    assert not poller._resync
    clock[0] += swap_poller.ANNOUNCE_GRACE_SECS + 1
    poller.poll()
    assert poller._resync and poller._pending() == {}


def test_feed_down_or_new_session_syncs_from_chain():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    poller = SwapPoller(client, me, feed=feed)
    poller.poll()

    client.reset_mock()
    feed.session = 2  # resubscribed: anything in the gap must be caught up
    poller.poll()
    assert client.get_miner_state.call_count == 1
    poller.poll()
    assert client.get_miner_state.call_count == 1  # caught up; back to push

    feed.connected = False
    poller.poll()
    poller.poll()
    assert client.get_miner_state.call_count == 3  # every pass while down


def test_catch_up_finds_a_swap_whose_push_was_missed():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    poller = SwapPoller(client, me, feed=feed)
    poller.poll()

    client.get_miner_state.return_value = _miner_state(0, 0, active=True)
    client.get_swaps.return_value = [('pda', _acct(bytes(me), 'aa'))]
    feed.session = 2
    active, _ = poller.poll()
    assert [s.from_tx_hash for s in active] == ['aa']
    assert client.get_swaps.call_count == 1


def test_point_read_failure_marks_poll_failed_and_resyncs():
    me = Keypair().pubkey()
    client = _idle_client()
    feed = FakeFeed()
    poller = SwapPoller(client, me, feed=feed)
    poller.poll()
    feed.push_initiated(me, 'aa')
    client.get_swap.side_effect = ConnectionError('rpc down')

    assert poller.poll() == ([], [])
    assert poller.last_poll_ok is False and poller._resync
