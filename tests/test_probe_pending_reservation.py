"""probe_pending_reservation: reconcile pending_swap.json with on-chain state.

Pins the six-branch decision the helper makes so the UX bugs it fixes don't
regress: stale local file shown as ACTIVE because the miner was re-reserved
by another user, and stale local file shown as ACTIVE because the contract
leaves expired rows in the reservation map until lazy clear.
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


# Default current block sits below every ``reserved_until`` value used here,
# so the past-block check stays inert in tests that aren't exercising it.
CURRENT_BLOCK = 1_000_000


def make_client(*, active_swaps=(), reserved_until=0, reservation_data=None):
    """Build a contract-client double whose only behavior is what the probe touches."""
    client = MagicMock()
    # has_active_swap mirrors whether the test set up any active swaps so the
    # probe's cheap-bool short-circuit behaves like real chain state.
    client.get_miner_has_active_swap.return_value = bool(active_swaps)
    client.get_miner_active_swaps.return_value = list(active_swaps)
    client.get_miner_reserved_until.return_value = reserved_until
    client.get_reservation_data.return_value = reservation_data
    return client


def test_our_swap_when_active_swaps_match_user_addresses():
    state = make_state()
    swap = make_swap(user_from_address=state.user_from_address, user_to_address=state.receive_address)
    client = make_client(active_swaps=[swap])

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'our_swap'
    assert result.swap is swap


def test_active_swap_for_different_user_does_not_match():
    """Same miner can carry someone else's swap — addresses must match ours."""
    state = make_state()
    other = make_swap(user_from_address='tb1qsomeoneelse', user_to_address='5OtherHk')
    client = make_client(active_swaps=[other])

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'expired'


def test_expired_when_no_swap_and_no_reservation():
    state = make_state()
    client = make_client()

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'expired'


def test_expired_when_reserved_until_is_in_the_past():
    """Contract leaves expired rows in the map (lazy clear). A non-zero stale
    row whose reserved_until is below current_block must read as ``expired``,
    not ``ours_active`` with ~0 blocks left."""
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, current_block=1_000_101)

    assert result.kind == 'expired'


def test_replaced_when_reservation_amounts_differ():
    state = make_state()
    client = make_client(
        reserved_until=1_000_050,
        reservation_data=(state.tao_amount + 1, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'replaced'


def test_replaced_when_reserved_until_grows_beyond_extension_ceiling():
    """The user's bug: same miner re-reserved with same params after our swap completed.

    Total legitimate growth from saved value is bounded by
    ``MAX_EXTENSIONS_PER_RESERVATION * MAX_EXTENSION_BLOCKS`` (each finalize
    bumps reserved_until by at most ``MAX_EXTENSION_BLOCKS``, capped at the
    contract's per-reservation limit). Anything beyond is a replacement.
    """
    from allways.constants import MAX_EXTENSION_BLOCKS, MAX_EXTENSIONS_PER_RESERVATION

    ceiling = MAX_EXTENSIONS_PER_RESERVATION * MAX_EXTENSION_BLOCKS
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100 + ceiling + 1,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'replaced'


def test_ours_active_when_amounts_match_and_within_extension_ceiling():
    """Optimistic extensions: reserved_until advanced but stays within the
    contract's per-reservation extension ceiling. This is the case the old
    ttl-based tolerance got wrong — one tier-0 BTC extension (~180 blocks)
    on a tier-2 reservation_ttl=50 setup blew past ttl and was misreported
    as ``replaced`` even though it was the user's own reservation."""
    from allways.constants import MAX_EXTENSION_BLOCKS, MAX_EXTENSIONS_PER_RESERVATION

    ceiling = MAX_EXTENSIONS_PER_RESERVATION * MAX_EXTENSION_BLOCKS
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100 + ceiling,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'ours_active'
    assert result.reserved_until == 1_000_100 + ceiling


def test_ours_active_after_single_btc_tier0_extension_on_short_ttl():
    """Concrete regression: BTC tier-0 honest target pushes reserved_until by
    ~180 blocks. With reservation_ttl=50 and the old `> ttl` tolerance, this
    legitimate extension was misclassified as ``replaced``. The new ceiling
    accommodates it."""
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100 + 180,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'ours_active'


def test_ours_active_when_unchanged():
    state = make_state(reserved_until_block=1_000_100)
    client = make_client(
        reserved_until=1_000_100,
        reservation_data=(state.tao_amount, state.from_amount, state.to_amount),
    )

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'ours_active'


def test_skips_active_swap_scan_when_miner_has_no_active_swap():
    """The cheap-bool short-circuit avoids the swap-range scan in the common case."""
    state = make_state()
    client = make_client()
    client.get_miner_has_active_swap.return_value = False

    probe_pending_reservation(client, state, CURRENT_BLOCK)

    client.get_miner_active_swaps.assert_not_called()


def test_rpc_error_short_circuits_when_active_swaps_call_fails():
    state = make_state()
    client = MagicMock()
    client.get_miner_has_active_swap.return_value = True
    client.get_miner_active_swaps.side_effect = ContractError('rpc down')

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result == ReservationStatus(kind='rpc_error')


def test_rpc_error_when_reservation_read_fails():
    state = make_state()
    client = make_client()
    client.get_miner_reserved_until.side_effect = ContractError('rpc down')

    result = probe_pending_reservation(client, state, CURRENT_BLOCK)

    assert result.kind == 'rpc_error'
