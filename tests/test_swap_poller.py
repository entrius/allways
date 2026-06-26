"""B4.1 — SwapPoller against the Solana getProgramAccounts snapshot model.

No cursor, no per-id transient miss: each poll is an atomic view. The poller filters the program's
swaps to this miner's pubkey and splits them into (active, fulfilled).
"""

import types
from unittest.mock import MagicMock

from solders.keypair import Keypair

from allways.miner.swap_poller import ACTIVE_STATUSES, SwapPoller


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
        sol_amount=2_000,
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

    def get_swaps(status=None):
        rows = active if status == 'Active' else fulfilled if status == 'Fulfilled' else []
        return [(f'pda{i}', a) for i, a in enumerate(rows)]

    client.get_swaps.side_effect = get_swaps
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
