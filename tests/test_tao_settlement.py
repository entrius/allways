"""TAO legs are verified by settlement, not by the decoded call.

A signed transfer that dispatches with an error (insufficient balance, for one) is still included
in a block and still decodes to the intended dest/amount. Only a Balances.Transfer event proves the
funds moved, so that event is what the verifier reads. Backends are mocked — no network.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from allways.chain_providers.base import ProviderUnreachableError
from allways.chain_providers.subtensor import SubtensorProvider

MINER = 'minerTAO'
USER = 'userTAO'
TXID = '0xdeposit'
BLOCK = 500


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
    p = SubtensorProvider.__new__(SubtensorProvider)  # skip __init__ (no real subtensor needed)
    p.subtensor = MagicMock()
    p.subtensor.get_current_block.return_value = head
    p.block_cache = {}
    p.block_hash_cache = {}
    p.events_cache = {}
    p.scan_cursors = {}
    block = {
        '_raw': True,
        'extrinsics': [{'extrinsic_hash': TXID, 'dest': MINER, 'amount': call_amount, 'sender': USER}],
    }
    p.get_block = lambda n: block if n == BLOCK else None
    p.get_block_hash = lambda n: f'0xblock{n}'
    p.get_block_events = lambda h: events
    p.get_block_time = lambda n: 1_700_000_000
    return p


def test_included_but_failed_transfer_is_not_a_deposit():
    """The exploited path: extrinsic sits in the block, decodes perfectly, moved nothing."""
    p = _provider(events=[])
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


def test_settlement_to_another_recipient_does_not_count():
    p = _provider(events=[_transfer_event('someoneElse', 5_000, USER)])
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

    p.get_block_events = boom
    with pytest.raises(ProviderUnreachableError):
        p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK)


def test_overpayment_still_settles():
    p = _provider(events=[_transfer_event(MINER, 9_000, USER)])
    info = p.fetch_matching_tx(TXID, MINER, 5_000, block_hint=BLOCK)
    assert info is not None and info.amount == 9_000


def test_events_are_fetched_once_per_block():
    calls = []
    p = _provider(events=[_transfer_event(MINER, 5_000, USER)])
    p.events_cache = {}
    real_events = [_transfer_event(MINER, 5_000, USER)]
    p.subtensor.substrate = SimpleNamespace(get_events=lambda h: calls.append(h) or real_events)
    del p.get_block_events  # exercise the real caching path
    p.get_block_events = SubtensorProvider.get_block_events.__get__(p)
    assert p.settled_credit(BLOCK, 0, MINER) == (USER, 5_000)
    assert p.settled_credit(BLOCK, 0, MINER) == (USER, 5_000)
    assert len(calls) == 1
