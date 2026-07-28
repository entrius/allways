"""B2.1 — block_time (unix seconds) is surfaced onto TransactionInfo by both providers.

The replay-freshness checks (B2.2) compare a tx's mined time against on-chain floors, so verify the
providers actually populate block_time: BTC from Esplora status.block_time, TAO from the Timestamp pallet
(millis ÷ 1000). Backends are mocked — no network.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from bittensor.utils import ss58_encode

from allways.chain_providers.base import ProviderUnreachableError, TransactionInfo
from allways.chain_providers.bitcoin import BitcoinProvider
from allways.chain_providers.subtensor import SubtensorProvider

BLOCK_TIME = 1_700_000_123


class _Resp:
    def __init__(self, *, status_code=200, json_data=None, text=''):
        self.status_code = status_code
        self.ok = status_code < 400
        self._json = json_data or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError('http error')

    def json(self):
        return self._json


def test_transaction_info_defaults_block_time_none():
    ti = TransactionInfo(tx_hash='h', confirmed=True, sender='a', recipient='b', amount=1)
    assert ti.block_time is None


def test_btc_api_surfaces_block_time(monkeypatch):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    p = BitcoinProvider()

    tx_json = {
        'status': {
            'confirmed': True,
            'block_height': 800_000,
            'block_hash': 'bh',
            'block_time': BLOCK_TIME,
        },
        'vin': [{'prevout': {'scriptpubkey_address': 'sender'}}],
        'vout': [{'scriptpubkey_address': 'recipient', 'value': 5_000}],
    }

    def fake_get(path, timeout=10):
        if path.startswith('/tx/'):
            return _Resp(json_data=tx_json)
        if path == '/blocks/tip/height':
            return _Resp(text='800010')  # 11 confs
        if path.startswith('/block/'):
            return _Resp(json_data={'in_best_chain': True})
        raise AssertionError(f'unexpected path {path}')

    monkeypatch.setattr(p, 'btc_api_get', fake_get)
    ti = p.api_verify_transaction('txhash', 'recipient', 1_000)
    assert ti is not None
    assert ti.confirmed is True
    assert ti.block_time == BLOCK_TIME


def test_subtensor_get_block_time_millis_to_seconds():
    p = SubtensorProvider.__new__(SubtensorProvider)  # skip __init__ (no real subtensor needed)
    substrate = MagicMock()
    substrate.get_block_hash.return_value = '0xabc'
    substrate.query.return_value = SimpleNamespace(value=BLOCK_TIME * 1000)  # pallet returns millis
    p.subtensor = SimpleNamespace(substrate=substrate)

    assert p.get_block_time(12_345) == BLOCK_TIME
    substrate.query.assert_called_once_with('Timestamp', 'Now', block_hash='0xabc')


def test_subtensor_get_block_time_none_on_error():
    p = SubtensorProvider.__new__(SubtensorProvider)
    substrate = MagicMock()
    substrate.get_block_hash.side_effect = RuntimeError('rpc down')
    p.subtensor = SimpleNamespace(substrate=substrate)
    assert p.get_block_time(1) is None


# ─── TAO deposit scanner (find_recent_outgoing) ─────────────────────────────
# Substrate has no address index, so the scanner follows the head incrementally.


def _scan_provider(head, blocks):
    p = SubtensorProvider.__new__(SubtensorProvider)  # skip __init__ (no real subtensor needed)
    p.subtensor = MagicMock()
    p.subtensor.get_current_block.return_value = head
    p.block_cache = {}
    p.block_hash_cache = {}
    p.events_cache = {}
    p.scan_cursors = {}
    p.get_block = lambda n: blocks.get(n)
    p.get_block_hash = lambda n: f'0xblock{n}'
    p.get_block_events = lambda h: (blocks.get(int(h.removeprefix('0xblock'))) or {}).get('_events', [])
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


def _raw_transfer_block(txid, dest, amount, sender, settled=True):
    """A block holding one transfer. ``settled=False`` models a dispatch that failed: the
    extrinsic is in the block and decodes normally, but emits no Balances.Transfer event."""
    return {
        '_raw': True,
        'extrinsics': [{'extrinsic_hash': txid, 'dest': dest, 'amount': amount, 'sender': sender}],
        '_events': [_transfer_event(dest, amount, sender)] if settled else [],
    }


def test_tao_scanner_finds_matching_transfer_in_new_blocks():
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 5000, 'userTAO')}
    p = _scan_provider(head=100, blocks=blocks)
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) == '0xdep'
    # A hit clears the cursor so a fresh reservation with the same triple rescans.
    assert p.scan_cursors == {}


def test_tao_scanner_skips_wrong_sender_and_underpay_then_advances_cursor():
    blocks = {
        99: _raw_transfer_block('0xother', 'minerTAO', 5000, 'someoneElse'),
        100: _raw_transfer_block('0xsmall', 'minerTAO', 4999, 'userTAO'),
    }
    p = _scan_provider(head=100, blocks=blocks)
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) is None
    assert p.scan_cursors[('userTAO', 'minerTAO', 5000)] == 100
    # Next tick scans ONLY blocks past the cursor — the amortized-O(1) property.
    seen = []
    p.get_block = lambda n: seen.append(n) or blocks.get(n)
    p.subtensor.get_current_block.return_value = 102
    blocks[102] = _raw_transfer_block('0xdep', 'minerTAO', 6000, 'userTAO')
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) == '0xdep'
    assert seen == [101, 102]


def test_tao_scanner_bounds_first_scan_to_lookback():
    p = _scan_provider(head=1000, blocks={})
    seen = []
    p.get_block = lambda n: seen.append(n) or None
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) is None
    assert len(seen) == SubtensorProvider.SCAN_LOOKBACK_BLOCKS
    assert seen[0] == 1000 - SubtensorProvider.SCAN_LOOKBACK_BLOCKS + 1


def test_tao_scanner_none_when_head_unreachable():
    p = _scan_provider(head=100, blocks={})
    p.subtensor.get_current_block.side_effect = RuntimeError('down')
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) is None


def test_tao_scanner_ignores_transfer_that_moved_no_funds():
    """An included-but-failed transfer decodes to the right dest/amount and must still be ignored."""
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 5000, 'userTAO', settled=False)}
    p = _scan_provider(head=100, blocks=blocks)
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) is None


def test_tao_scanner_ignores_transfer_settled_below_the_asking_amount():
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 5000, 'userTAO')}
    blocks[100]['_events'] = [_transfer_event('minerTAO', 4999, 'userTAO')]
    p = _scan_provider(head=100, blocks=blocks)
    assert p.find_recent_outgoing('userTAO', 'minerTAO', 5000) is None


def test_settled_credit_reads_amount_and_sender_from_the_event():
    """The event is authoritative: a call claiming more than it moved settles at the event amount."""
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 9_999, 'userTAO')}
    blocks[100]['_events'] = [_transfer_event('minerTAO', 5_000, 'userTAO')]
    p = _scan_provider(head=100, blocks=blocks)
    assert p.settled_credit(100, 0, 'minerTAO') == ('userTAO', 5_000)


def test_settled_credit_ignores_events_from_other_extrinsics_and_recipients():
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 5000, 'userTAO')}
    blocks[100]['_events'] = [
        _transfer_event('minerTAO', 5_000, 'userTAO', extrinsic_idx=7),  # different extrinsic
        _transfer_event('someoneElse', 5_000, 'userTAO', extrinsic_idx=0),  # different recipient
    ]
    p = _scan_provider(head=100, blocks=blocks)
    assert p.settled_credit(100, 0, 'minerTAO') is None


def test_settled_credit_sums_multiple_credits_from_one_extrinsic():
    blocks = {100: _raw_transfer_block('0xdep', 'minerTAO', 5000, 'userTAO')}
    blocks[100]['_events'] = [
        _transfer_event('minerTAO', 3_000, 'userTAO'),
        _transfer_event('minerTAO', 2_000, 'userTAO'),
    ]
    p = _scan_provider(head=100, blocks=blocks)
    assert p.settled_credit(100, 0, 'minerTAO') == ('userTAO', 5_000)


@pytest.mark.parametrize(
    'record',
    [
        {
            'phase': {'ApplyExtrinsic': 0},
            'event': {
                'module_id': 'Balances',
                'event_id': 'Transfer',
                'attributes': {'from': 'A', 'to': 'B', 'amount': 7},
            },
        },
        {'extrinsic_idx': 0, 'event': {'module_id': 'Balances', 'event_id': 'Transfer', 'attributes': ['A', 'B', 7]}},
        {'extrinsic_idx': 0, 'event': {'Balances': {'Transfer': {'from': 'A', 'to': 'B', 'amount': 7}}}},
        {
            'extrinsic_idx': 0,
            'event': {
                'module_id': 'Balances',
                'event_id': 'Transfer',
                'attributes': {'from': {'Id': 'A'}, 'to': {'Id': 'B'}, 'amount': 7},
            },
        },
    ],
)
def test_transfer_event_shapes_are_all_understood(record):
    """scalecodec emits several shapes across runtime versions; all must decode identically."""
    assert SubtensorProvider._event_extrinsic_idx(record) == 0
    assert SubtensorProvider._transfer_from_event(record) == ('A', 'B', 7)


def test_non_transfer_events_are_not_read_as_transfers():
    for record in (
        {'extrinsic_idx': 0, 'event': {'module_id': 'System', 'event_id': 'ExtrinsicFailed', 'attributes': {}}},
        {
            'extrinsic_idx': 0,
            'event': {'module_id': 'Balances', 'event_id': 'Withdraw', 'attributes': {'who': 'A', 'amount': 7}},
        },
        {
            'phase': 'Finalization',
            'event': {
                'module_id': 'Balances',
                'event_id': 'Transfer',
                'attributes': {'from': 'A', 'to': 'B', 'amount': 7},
            },
        },
    ):
        assert (
            SubtensorProvider._transfer_from_event(record) is None
            or SubtensorProvider._event_extrinsic_idx(record) is None
        )


def test_events_unavailable_raises_rather_than_reading_as_absent():
    """Fail closed as 'unknown', not 'no transfer' — the latter is slash-eligible upstream."""
    p = SubtensorProvider.__new__(SubtensorProvider)
    p.events_cache = {}
    p.subtensor = SimpleNamespace(substrate=SimpleNamespace(get_events=MagicMock(side_effect=RuntimeError('rpc down'))))
    with pytest.raises(ProviderUnreachableError):
        p.get_block_events('0xabc')


def _compact(value):
    if value < 64:
        return bytes([value << 2])
    if value < 2**14:
        return ((value << 2) | 0b01).to_bytes(2, 'little')
    return ((value << 2) | 0b10).to_bytes(4, 'little')


def test_raw_extrinsic_sender_is_read_past_the_multiaddress_variant_byte():
    """body[1] is the MultiAddress variant; the AccountId starts at body[2], not body[1]."""
    sender, dest = bytes(range(32)), bytes(range(100, 132))
    body = (
        bytes([0x84, 0x00])
        + sender  # version, MultiAddress::Id + signer
        + bytes([0x01])
        + b'\x11' * 64  # sr25519 signature (filler avoids a false call match)
        + bytes([0x00, 0x00, 0x00])  # era, nonce, tip
        + bytes([0x05, 0x03, 0x00])
        + dest
        + _compact(5_000)  # Balances.transfer_keep_alive
    )
    parsed = SubtensorProvider.parse_raw_extrinsic((_compact(len(body)) + body).hex())
    assert parsed is not None
    assert parsed['sender'] == ss58_encode(sender, ss58_format=42)
    assert parsed['dest'] == ss58_encode(dest, ss58_format=42)
    assert parsed['amount'] == 5_000


def test_raw_extrinsic_rejects_non_id_multiaddress_rather_than_shifting():
    """A non-Id signer variant has no bare AccountId to read, so it must fail closed."""
    body = bytes([0x84, 0x01]) + bytes(range(32)) + bytes([0x01]) + b'\x11' * 64 + bytes([0x00] * 3)
    assert SubtensorProvider.parse_raw_extrinsic((_compact(len(body)) + body).hex()) is None


def test_match_transfer_still_matches_by_hash_through_shared_decode():
    ext = {'extrinsic_hash': '0xabc', 'dest': 'minerTAO', 'amount': 7, 'sender': 'userTAO'}
    assert SubtensorProvider.match_transfer(ext, '0xabc', True) == ('minerTAO', 7, 'userTAO')
    assert SubtensorProvider.match_transfer(ext, '0xother', True) is None
