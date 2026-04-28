"""probe_pending_reservation: reconcile pending_swap.json with on-chain state.

Pins the five-branch decision the helper makes so the UX bug it fixes
(stale local file shown as ACTIVE because miner was re-reserved by another
user) doesn't regress.
"""

from unittest.mock import MagicMock

from allways.classes import Swap, SwapStatus
from allways.cli.swap_commands.helpers import (
    PendingSwapState,
    ReservationStatus,
    probe_pending_reservation,
)
from allways.contract_client import ContractError


def make_state(**overrides) -> PendingSwapState:
    base = dict(
        miner_hotkey='5MinerHk',
        miner_uid=4,
        from_chain='btc',
        to_chain='tao',
        from_amount=100_000,
        to_amount=300_000_000,
        tao_amount=300_000_000,
        user_receives=297_000_000,
        rate_str='3000',
        miner_from_address='tb1qminer',
        user_from_address='tb1quser',
        receive_address='5UserHk',
        reserved_until_block=1_000_100,
        netuid=2,
        wallet_name='default',
        hotkey_name='default',
        created_at=0.0,
        from_tx_hash='',
    )
    base.update(overrides)
    return PendingSwapState(**base)


def make_swap(**overrides) -> Swap:
    base = dict(
        id=3,
        user_hotkey='5UserHk',
        miner_hotkey='5MinerHk',
        from_chain='btc',
        to_chain='tao',
        from_amount=100_000,
        to_amount=300_000_000,
        tao_amount=300_000_000,
        user_from_address='tb1quser',
        user_to_address='5UserHk',
        status=SwapStatus.FULFILLED,
    )
    base.update(overrides)
    return Swap(**base)


def make_client(*, active_swaps=(), reserved_until=0, reservation_data=None, ttl=4032):
    """Build a contract-client double whose only behavior is what the probe touches."""
    client = MagicMock()
    client.get_miner_active_swaps.return_value = list(active_swaps)
    client.get_miner_reserved_until.return_value = reserved_until
    client.get_reservation_data.return_value = reservation_data
    client.get_reservation_ttl.return_value = ttl
    return client


def test_our_swap_when_active_swaps_match_user_addresses():
    state = make_state()
    swap = make_swap(user_from_address=state.user_from_address, user_to_address=state.receive_address)
    client = make_client(active_swaps=[swap])

    result = probe_pending_reservation(client, state)

    assert result.kind == 'our_swap'
    assert result.swap is swap


def test_active_swap_for_different_user_does_not_match():
    """Same miner can carry someone else's swap — addresses must match ours."""
    state = make_state()
    other = make_swap(user_from_address='tb1qsomeoneelse', user_to_address='5OtherHk')
    client = make_client(active_swaps=[other])

    result = probe_pending_reservation(client, state)

    assert result.kind == 'expired'


def test_expired_when_no_swap_and_no_reservation():
    state = make_state()
    client = make_client()

    result = probe_pending_reservation(client, state)

    assert result.kind == 'expired'


def test_replaced_when_reservation_amounts_differ():
    state = make_state()
    client = make_client(
        reserved_until=1_000_050,
        reservation_data=(state.tao_amount + 1, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state)

    assert result.kind == 'replaced'


def test_replaced_when_reserved_until_is_more_than_ttl_past_saved():
    """The user's bug: same miner re-reserved with same params after our swap completed."""
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100 + 4033,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
        ttl=4032,
    )

    result = probe_pending_reservation(client, state)

    assert result.kind == 'replaced'


def test_ours_active_when_amounts_match_and_within_ttl():
    """Single extension: reserved_until advanced but stays within ttl of saved."""
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100 + 4031,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
        ttl=4032,
    )

    result = probe_pending_reservation(client, state)

    assert result.kind == 'ours_active'
    assert result.reserved_until == 1_000_100 + 4031


def test_ours_active_when_unchanged():
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state)

    assert result.kind == 'ours_active'


def test_rpc_error_short_circuits_when_active_swaps_call_fails():
    state = make_state()
    client = MagicMock()
    client.get_miner_active_swaps.side_effect = ContractError('rpc down')

    result = probe_pending_reservation(client, state)

    assert result == ReservationStatus(kind='rpc_error')


def test_rpc_error_when_reservation_read_fails():
    state = make_state()
    client = make_client()
    client.get_miner_reserved_until.side_effect = ContractError('rpc down')

    result = probe_pending_reservation(client, state)

    assert result.kind == 'rpc_error'
