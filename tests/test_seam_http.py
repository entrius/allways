"""Unit tests for the localhost offering seam (auth gate + routing + JSON), engine ops stubbed."""

import json
import urllib.error
import urllib.request
from types import SimpleNamespace

import pytest

from allways.validator import seam_http
from allways.validator.reserve_engine import BestQuote, ConfirmResult, ReserveResult, SwapStatus

SECRET = 'test-secret'


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(seam_http, 'reserve_on_behalf', lambda *a, **k: ReserveResult(True, '', 123, 'sig'))
    monkeypatch.setattr(seam_http, 'confirm_deposit', lambda *a, **k: ConfirmResult(True, '', 'deadbeef', 'sig'))
    monkeypatch.setattr(seam_http, 'best_quote', lambda *a, **k: BestQuote('hk', 'pk', '0.0021', 1, 1, 210000))
    monkeypatch.setattr(seam_http, 'swap_status', lambda *a, **k: SwapStatus('reserved', 999, 'user', ''))
    srv = seam_http.start_seam(SimpleNamespace(), 0, SECRET)
    yield srv
    srv.shutdown()


def _req(server, method, path, body=None, secret=SECRET):
    host, port = server.server_address
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(f'http://127.0.0.1:{port}{path}', data=data, method=method)
    if secret is not None:
        r.add_header('X-Seam-Secret', secret)
    try:
        with urllib.request.urlopen(r) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_unauthorized_without_secret(server):
    code, payload = _req(server, 'GET', '/health', secret=None)
    assert code == 401 and payload['error'] == 'unauthorized'


def test_health(server):
    assert _req(server, 'GET', '/health') == (200, {'ok': True})


def test_reserve_ok(server):
    code, payload = _req(
        server,
        'POST',
        '/reserve',
        {
            'miner_hotkey': 'hk',
            'from_chain': 'sol',
            'to_chain': 'btc',
            'user_pubkey': 'u',
            'user_from_addr': 'u',
            'user_to_addr': 'b',
            'from_amount': 1_000_000_000,
        },
    )
    assert code == 200 and payload['ok'] and payload['pool_closes_at'] == 123


def test_reserve_rejection_is_422(server, monkeypatch):
    monkeypatch.setattr(seam_http, 'reserve_on_behalf', lambda *a, **k: ReserveResult(False, 'miner is not active'))
    code, payload = _req(
        server,
        'POST',
        '/reserve',
        {
            'miner_hotkey': 'hk',
            'from_chain': 'sol',
            'to_chain': 'btc',
            'user_pubkey': 'u',
            'user_from_addr': 'u',
            'user_to_addr': 'b',
            'from_amount': 1,
        },
    )
    assert code == 422 and payload['reason'] == 'miner is not active'


def test_reserve_missing_field_is_400(server):
    code, _ = _req(server, 'POST', '/reserve', {'miner_hotkey': 'hk'})
    assert code == 400


def test_confirm_ok(server):
    code, payload = _req(server, 'POST', '/confirm', {'miner_hotkey': 'hk', 'from_tx_hash': 'tx'})
    assert code == 200 and payload['ok'] and payload['swap_key'] == 'deadbeef'


def test_rate(server):
    code, payload = _req(server, 'GET', '/rate?from=sol&to=btc&amount=1000000000')
    assert code == 200 and payload['to_amount'] == 210000 and payload['miner_hotkey'] == 'hk'


def test_status(server):
    code, payload = _req(server, 'GET', '/status?miner_hotkey=hk')
    assert code == 200 and payload['stage'] == 'reserved'
