"""B2.1 — block_time (unix seconds) is surfaced onto TransactionInfo by both providers.

The replay-freshness checks (B2.2) compare a tx's mined time against on-chain floors, so verify the
providers actually populate block_time: BTC from Esplora status.block_time, TAO from the Timestamp pallet
(millis ÷ 1000). Backends are mocked — no network.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from allways.chain_providers.base import TransactionInfo
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
