"""Unit tests for the routed-reservation finalize sweep (`finalize_won_seats`).

Mocks the solana_client; no chain. The sweep must finalize a won drawn seat for the FIFO
queued user at the PINNED rate, drop queues on every terminal outcome (lost, filled,
lapsed, contract-rejected), and retain them across transient faults and read-only mode.
"""

from types import SimpleNamespace

from solders.keypair import Keypair as SolKeypair

from allways.constants import RATE_PRECISION
from allways.validator.reserve_engine import ROUTED_REQUEST_TTL_SECS, draw_pool_winner, finalize_won_seats
from allways.validator.state_store import ValidatorStateStore

NOW = 1_000_000
MINER = str(SolKeypair().pubkey())
USER_A = str(SolKeypair().pubkey())
USER_B = str(SolKeypair().pubkey())
RATE_FIXED = 21 * RATE_PRECISION // 10_000  # sol->btc: 0.0021 BTC per 1 SOL, exact fixed-point


class SweepClient:
    """get_reservation + finalize_reservation, with a keypair identity for the router check."""

    def __init__(self, reservation):
        self.keypair = SolKeypair()
        self._reservation = reservation
        self.finalized = []

    def get_reservation(self, miner):
        return self._reservation

    def finalize_reservation(
        self, miner, user, user_from_addr, user_to_addr, collateral_amount, from_amount, to_amount
    ):
        self.finalized.append(
            (str(miner), str(user), user_from_addr, user_to_addr, collateral_amount, from_amount, to_amount)
        )
        return 'finalizesig'


def _drawn_seat(client, *, router=None, finalize_by=NOW + 100, reserved_until=0, from_chain='sol', to_chain='btc'):
    return SimpleNamespace(
        router=router if router is not None else client.keypair.pubkey(),
        from_chain=from_chain,
        to_chain=to_chain,
        rate=RATE_FIXED,
        reserved_until=reserved_until,
        finalize_by=finalize_by,
    )


def _validator(tmp_path, client, *, read_only=False):
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return SimpleNamespace(
        state_store=store, solana_client=client, solana_swap_loop=SimpleNamespace(read_only=read_only)
    )


def _queue(store, user, *, created_at=NOW - 10, from_amount=1_000_000_000):
    store.upsert_routed_request(MINER, 'sol', 'btc', user, f'{user[:6]}src', f'{user[:6]}dst', from_amount, created_at)


def test_won_seat_finalizes_fifo_user_at_pinned_rate(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client)
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_B, created_at=NOW - 5)
    _queue(v.state_store, USER_A, created_at=NOW - 50)  # oldest → FIFO winner
    assert finalize_won_seats(v, NOW) == [MINER]
    assert len(client.finalized) == 1
    miner, user, _src, _dst, collateral_amount, from_amount, to_amount = client.finalized[0]
    assert (miner, user) == (MINER, USER_A)
    # sol source: collateral leg == from leg; dest derived from the PINNED rate (1 SOL → 0.0021 BTC).
    assert collateral_amount == from_amount == 1_000_000_000
    assert to_amount == 210_000
    # Whole queue dropped: winner served, losers re-request via their clients.
    assert v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_draw_pool_winner_is_fifo_stub():
    oldest = {'user_pubkey': USER_A, 'created_at': 1}
    assert draw_pool_winner([oldest, {'user_pubkey': USER_B, 'created_at': 2}]) is oldest


def test_seat_lost_to_other_router_drops_queue_without_tx(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client, router=SolKeypair().pubkey())
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert not client.finalized
    assert v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_lapsed_finalize_window_drops_queue(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client, finalize_by=NOW - 1)
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert not client.finalized and v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_already_filled_reservation_drops_queue(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client, reserved_until=NOW + 300)
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert not client.finalized and v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_undrawn_pool_keeps_queue_for_next_step(tmp_path):
    # No reservation yet (pool still open / crank pending) → wait, don't drop.
    client = SweepClient(None)
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert v.state_store.distinct_routed_pools() == [(MINER, 'sol', 'btc')]
    v.state_store.close()


def test_transient_finalize_fault_retains_queue(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client)

    def _raise(*_a, **_k):
        raise RuntimeError('connection refused')

    client.finalize_reservation = _raise
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert v.state_store.distinct_routed_pools() == [(MINER, 'sol', 'btc')]  # retry next step
    v.state_store.close()


def test_contract_rejection_drops_queue(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client)

    def _raise(*_a, **_k):
        raise RuntimeError(
            'AnchorError ... Error Message: Miner already has an active reservation. custom program error'
        )

    client.finalize_reservation = _raise
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_read_only_mode_finalizes_nothing_and_retains_queue(tmp_path):
    client = SweepClient(None)
    client._reservation = _drawn_seat(client)
    v = _validator(tmp_path, client, read_only=True)
    _queue(v.state_store, USER_A)
    assert finalize_won_seats(v, NOW) == []
    assert not client.finalized
    assert v.state_store.distinct_routed_pools() == [(MINER, 'sol', 'btc')]
    v.state_store.close()


def test_stale_queue_pruned_by_ttl_backstop(tmp_path):
    client = SweepClient(None)  # miner never drawn — reservation stays None forever
    v = _validator(tmp_path, client)
    _queue(v.state_store, USER_A, created_at=NOW - ROUTED_REQUEST_TTL_SECS - 1)
    assert finalize_won_seats(v, NOW) == []
    assert v.state_store.distinct_routed_pools() == []
    v.state_store.close()


def test_upsert_retry_keeps_fifo_position(tmp_path):
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    store.upsert_routed_request(MINER, 'sol', 'btc', USER_A, 'src1', 'dst1', 100, created_at=NOW - 50)
    store.upsert_routed_request(MINER, 'sol', 'btc', USER_B, 'src2', 'dst2', 200, created_at=NOW - 20)
    # A retries with fresh details — position (created_at) must survive, fields must refresh.
    store.upsert_routed_request(MINER, 'sol', 'btc', USER_A, 'src1b', 'dst1b', 150, created_at=NOW)
    queue = store.pending_routed_requests(MINER, 'sol', 'btc')
    assert [r['user_pubkey'] for r in queue] == [USER_A, USER_B]
    assert queue[0]['created_at'] == NOW - 50
    assert (queue[0]['user_from_addr'], queue[0]['from_amount']) == ('src1b', 150)
    store.close()
