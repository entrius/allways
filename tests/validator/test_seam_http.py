"""Unit tests for the localhost offering seam (auth gate + routing + JSON + the deposit-scan memo), request
ops stubbed."""

import json
import urllib.error
import urllib.request
from types import SimpleNamespace

import pytest

from allways.validator.serving import seam as seam_http
from allways.validator.serving.requests import BestQuote, ConfirmResult, RateQuote, ReserveResult, SwapStatus

SECRET = 'test-secret'
CANDIDATES = [
    {'miner_hotkey': 'hk', 'rate_display': '0.0021', 'to_amount': 210000, 'max_from_amount': 2_000_000_000},
    {'miner_hotkey': 'hk2', 'rate_display': '0.0020', 'to_amount': 200000, 'max_from_amount': 900_000_000},
]


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(seam_http, 'reserve_on_behalf', lambda *a, **k: ReserveResult(True, '', 123, 'sig'))
    monkeypatch.setattr(seam_http, 'confirm_deposit', lambda *a, **k: ConfirmResult(True, '', 'deadbeef', 'sig'))
    monkeypatch.setattr(
        seam_http,
        'rate_quote',
        lambda *a, **k: RateQuote(
            BestQuote('hk', 'pk', '0.0021', 1, 1, 210000),
            '',
            [{'rate_display': '0.0021', 'max_from_amount': 2_000_000_000}],
            2_000_000_000,
            100_000_000,
            CANDIDATES,
        ),
    )
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
    assert payload['levels'] == [{'rate_display': '0.0021', 'max_from_amount': 2_000_000_000}]
    assert payload['max_from_amount'] == 2_000_000_000
    assert payload['min_from_amount'] == 100_000_000 and payload['candidates'] == CANDIDATES


def test_status_forwards_optional_swap_key(server, monkeypatch):
    """swap_key drives by-key resolution in the engine — the seam must pass it through
    (and default it to '' when absent, preserving the reservation-path behavior)."""
    seen = []

    def fake_status(ctx, miner_hotkey, swap_key_hex='', from_chain='', to_chain=''):
        seen.append((miner_hotkey, swap_key_hex, from_chain, to_chain))
        return SwapStatus('timed_out', 0, '', swap_key_hex)

    monkeypatch.setattr(seam_http, 'swap_status', fake_status)
    key = 'ab' * 32
    code, payload = _req(server, 'GET', f'/status?miner_hotkey=hk&swap_key={key}')
    assert code == 200 and payload['stage'] == 'timed_out' and payload['swap_key'] == key
    _req(server, 'GET', '/status?miner_hotkey=hk')
    _req(server, 'GET', '/status?miner_hotkey=hk&from_chain=tao&to_chain=btc')
    assert seen == [('hk', key, '', ''), ('hk', '', '', ''), ('hk', '', 'tao', 'btc')]


# ── the deposit-scan memo: repeat polls from many tabs share one source-chain read ──


@pytest.fixture
def scans(monkeypatch):
    """A seam whose deposit scans are counted — each fixture builds a fresh memo."""
    calls = {'scan': 0}

    def fake_scan(_ctx, _miner_hotkey):
        calls['scan'] += 1
        return f'hash-{calls["scan"]}'

    monkeypatch.setattr(seam_http, 'scan_deposit', fake_scan)
    srv = seam_http.start_seam(SimpleNamespace(), 0, SECRET)
    yield srv, calls
    srv.shutdown()


def _scan(server, hotkey='hk'):
    return _req(server, 'GET', f'/deposit-scan?miner_hotkey={hotkey}')


def test_deposit_scan_is_memoized(scans):
    server, calls = scans
    hashes = [_scan(server)[1]['tx_hash'] for _ in range(10)]
    assert calls['scan'] == 1
    assert hashes == ['hash-1'] * 10


def test_distinct_miners_do_not_share_a_scan(scans):
    server, calls = scans
    assert _scan(server, 'hkA')[1]['tx_hash'] != _scan(server, 'hkB')[1]['tx_hash']
    assert calls['scan'] == 2


def test_scan_failures_are_not_cached(scans, monkeypatch):
    """A transient RPC fault must not pin an error for the whole bucket."""
    server, calls = scans
    state = {'fail': True}

    def flaky(_ctx, _miner_hotkey):
        if state['fail']:
            raise RuntimeError('getSignaturesForAddress: HTTP 429')
        calls['scan'] += 1
        return 'recovered'

    monkeypatch.setattr(seam_http, 'scan_deposit', flaky)
    assert _scan(server)[0] == 500
    state['fail'] = False
    # Same bucket: the retry reaches the producer instead of replaying the failure.
    assert _scan(server) == (200, {'tx_hash': 'recovered'})
