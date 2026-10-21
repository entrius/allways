"""TAO legs are verified by settlement, not by the decoded call.

A signed transfer that dispatches with an error (insufficient balance, for one) is still included
in a block and still decodes to the intended dest/amount. Only a Balances.Transfer event proves the
funds moved, so that event is what the verifier reads. Backends are mocked — no network.
"""

from unittest.mock import MagicMock

import pytest

from allways.assets.asset import ProviderUnreachableError
from allways.assets.tao import Tao, TaoChain

MINER = 'minerTAO'
USER = 'userTAO'
TXID = '0xdeposit'
BLOCK = 500


def _bare_tao():
    """A Tao on a TaoChain, both built without __init__ (no real subtensor needed)."""
    p = Tao.__new__(Tao)
    p._chain = TaoChain.__new__(TaoChain)
    p.scan_cursors, p.broadcasted_txids = {}, {}
    return p


def _transfer_event(dest, amount, sender, extrinsic_idx=0):
    return {
        'extrinsic_idx': extrinsic_idx,
        'event': {
            'module_id': 'Balances',
            'event_id': 'Transfer',
            'attributes': {'from': sender, 'to': dest, 'amount': amount},
        },
    }


def _provider(*, call_amount=5_000, events, head=BLOCK + 10):
    p = _bare_tao()  # skip __init__ (no real subtensor needed)
    p.subtensor = MagicMock()
    p.subtensor.get_current_block.return_value = head
    p.chain.block_cache = {}
    p.chain.block_hash_cache = {}
    p.chain.events_cache = {}
    p.scan_cursors = {}
    block = {
        '_raw': True,
        'extrinsics': [{'extrinsic_hash': TXID, 'dest': MINER, 'amount': call_amount, 'sender': USER}],
    }
    p.chain.get_block = lambda n: block if n == BLOCK else None
    p.chain.get_block_hash = lambda n: f'0xblock{n}'
    p.chain.get_block_events = lambda h: events
    p.chain.get_block_time = lambda n: 1_700_000_000
    return p


def _failed_dispatch_events(extrinsic_idx=0):
    """What a failed transfer actually leaves behind: the fee is taken, no Transfer is emitted."""
    return [
        {
            'extrinsic_idx': extrinsic_idx,
            'event': {'module_id': 'Balances', 'event_id': 'Withdraw', 'attributes': {'who': USER, 'amount': 166_249}},
        },
        {
            'extrinsic_idx': extrinsic_idx,
            'event': {
                'module_id': 'TransactionPayment',
                'event_id': 'TransactionFeePaid',
                'attributes': {'who': USER, 'actual_fee': 166_249, 'tip': 0},
            },
        },
        {
            'extrinsic_idx': extrinsic_idx,
            'event': {
                'module_id': 'System',
                'event_id': 'ExtrinsicFailed',
                'attributes': {'dispatch_error': {'Token': 'FundsUnavailable'}},
            },
        },
    ]


def test_included_but_failed_transfer_is_not_a_deposit():
    """The extrinsic sits in the block, decodes perfectly, moved nothing."""
    p = _provider(events=_failed_dispatch_events())
    assert p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK) is None


def test_settled_transfer_is_accepted():
    p = _provider(events=[_transfer_event(MINER, 5_000, USER)])
    info = p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK)
    assert info is not None
    assert (info.sender, info.recipient, info.amount) == (USER, MINER, 5_000)
    assert info.block_number == BLOCK


def test_amount_comes_from_the_event_not_the_call():
    """A call claiming 5000 that only settled 4999 is a short deposit, not a match."""
    p = _provider(call_amount=5_000, events=[_transfer_event(MINER, 4_999, USER)])
    assert p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK) is None


def test_sender_is_taken_from_the_event():
    """Upstream pins the sender against the reservation, so it must reflect who actually paid."""
    p = _provider(events=[_transfer_event(MINER, 5_000, 'actualPayer')])
    info = p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK)
    assert info is not None and info.sender == 'actualPayer'


def test_unreadable_events_surface_as_unreachable_not_as_absent():
    """'Unknown' must not collapse into 'no transfer' — that verdict is slash-eligible upstream."""
    p = _provider(events=[])

    def boom(_):
        raise ProviderUnreachableError('events unavailable')

    p.chain.get_block_events = boom
    with pytest.raises(ProviderUnreachableError):
        p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK)


# ─── shape drift must read as unknown, never as "no funds moved" ─────────────
# A silent None here would reject real deposits, and on a dest leg that is slash-eligible.


@pytest.mark.parametrize(
    'attributes',
    [
        'unexpected-scalar',
        {'sender': 'A', 'receiver': 'B', 'value': 7},  # renamed keys
    ],
)
def test_unreadable_transfer_payload_raises_rather_than_reading_as_absent(attributes):
    record = {
        'extrinsic_idx': 0,
        'event': {'module_id': 'Balances', 'event_id': 'Transfer', 'attributes': attributes},
    }
    with pytest.raises(ProviderUnreachableError):
        TaoChain._transfer_from_event(record)


def test_unreadable_payload_on_a_non_transfer_event_is_still_just_absent():
    """The strictness applies only once the record is known to be a Balances.Transfer."""
    record = {
        'extrinsic_idx': 0,
        'event': {'module_id': 'Balances', 'event_id': 'Withdraw', 'attributes': 'unexpected-scalar'},
    }
    assert TaoChain._transfer_from_event(record) is None


def test_unrecognised_phase_shape_raises_rather_than_matching_nothing():
    """If the phase shape moves, no record matches the index filter — that is unknown, not absent."""
    p = _provider(
        events=[
            {
                'stage': {'Applied': 0},
                'event': {'module_id': 'System', 'event_id': 'ExtrinsicSuccess', 'attributes': {}},
            }
        ]
    )
    with pytest.raises(ProviderUnreachableError):
        p.chain.settled_credit(BLOCK, 0, MINER)


def test_empty_event_list_is_unknown_not_a_no_credit():
    """We only get here having found an extrinsic in this block, so it must emit ApplyExtrinsic
    records. No events means a broken response, not a block where nothing settled."""
    p = _provider(events=[])
    with pytest.raises(ProviderUnreachableError):
        p.chain.settled_credit(BLOCK, 0, MINER)


def test_a_node_behind_the_hinted_block_is_unreachable_not_absent():
    # A not-found leg can slash; a node that hasn't imported the hinted block yet knows nothing.
    p = _provider(events=[_transfer_event(MINER, 5_000, USER)], head=BLOCK - 5)
    p.chain.get_block = lambda n: None
    with pytest.raises(ProviderUnreachableError):
        p.verify_transaction(tx_hash=TXID, expected_recipient=MINER, expected_amount=5_000, block_hint=BLOCK)


def test_a_hash_absent_from_readable_hinted_blocks_is_still_absent():
    p = _provider(events=[], head=BLOCK + 10)
    p.chain.get_block = lambda n: {'_raw': True, 'extrinsics': []}
    assert p.verify_transaction(tx_hash=TXID, expected_recipient=MINER, expected_amount=5_000, block_hint=BLOCK) is None
