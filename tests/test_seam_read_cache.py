"""The seam's read memo: repeat polls must not each cost a chain read.

The offering polls /status per active swap on a tight loop and reconciles every live row per
poll, so identical reads arrive from many tabs and many rows at once — each one uncached
`getAccountInfo` before this. Driven through the HTTP surface, the same way the offering hits it.
"""

import json
import urllib.error
import urllib.request
from types import SimpleNamespace

import pytest

from allways.validator import seam_http
from allways.validator.reserve_engine import SwapStatus

SECRET = 'test-secret'


def _get(server, path):
    host, port = server.server_address
    r = urllib.request.Request(f'http://127.0.0.1:{port}{path}', method='GET')
    r.add_header('X-Seam-Secret', SECRET)
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


@pytest.fixture
def counted(monkeypatch):
    """A seam whose chain reads are counted — each fixture builds a fresh memo."""
    calls = {'status': 0, 'scan': 0}

    def fake_status(_validator, miner_hotkey, swap_key=''):
        calls['status'] += 1
        return SwapStatus('reserved', 999, f'user-{calls["status"]}', swap_key)

    def fake_scan(_validator, _miner_hotkey):
        calls['scan'] += 1
        return f'hash-{calls["scan"]}'

    monkeypatch.setattr(seam_http, 'swap_status', fake_status)
    monkeypatch.setattr(seam_http, 'scan_deposit', fake_scan)
    srv = seam_http.start_seam(SimpleNamespace(), 0, SECRET)
    yield srv, calls
    srv.shutdown()


def test_bucket_is_stable_within_the_ttl_and_rolls_after(monkeypatch):
    ttl = seam_http.SEAM_READ_TTL_SECS
    base = ttl * 1000  # start on a bucket boundary, so the assertions hold for any TTL
    monkeypatch.setattr(seam_http.time, 'monotonic', lambda: base)
    first = seam_http._ttl_bucket()
    monkeypatch.setattr(seam_http.time, 'monotonic', lambda: base + ttl * 0.9)
    assert seam_http._ttl_bucket() == first  # same window → cache hit
    monkeypatch.setattr(seam_http.time, 'monotonic', lambda: base + ttl * 1.1)
    assert seam_http._ttl_bucket() != first  # rolled → refresh


def test_ttl_stays_under_the_transitions_the_offering_reacts_to():
    """Reservations live minutes and dest legs need tens of seconds of confirmations — a read
    this stale changes no decision. A TTL that crept up would start hiding real transitions."""
    assert 0 < seam_http.SEAM_READ_TTL_SECS <= 5


def test_repeat_polls_cost_one_chain_read(counted):
    """The whole point: N identical polls inside one bucket, one read."""
    server, calls = counted
    payloads = [_get(server, '/status?miner_hotkey=hk') for _ in range(25)]

    assert calls['status'] == 1
    assert {p['user'] for p in payloads} == {'user-1'}  # every caller saw the same answer


def test_distinct_swaps_do_not_share_an_entry(counted):
    """A per-swap key must never serve another swap's status."""
    server, calls = counted
    a = _get(server, '/status?miner_hotkey=hkA')
    b = _get(server, '/status?miner_hotkey=hkB')
    c = _get(server, f'/status?miner_hotkey=hkA&swap_key={"ab" * 32}')

    assert calls['status'] == 3
    assert len({a['user'], b['user'], c['user']}) == 3


def test_a_new_bucket_refreshes(counted, monkeypatch):
    server, calls = counted
    monkeypatch.setattr(seam_http, '_ttl_bucket', lambda: 7)
    _get(server, '/status?miner_hotkey=hk')
    _get(server, '/status?miner_hotkey=hk')
    assert calls['status'] == 1
    monkeypatch.setattr(seam_http, '_ttl_bucket', lambda: 8)
    _get(server, '/status?miner_hotkey=hk')
    assert calls['status'] == 2


def test_deposit_scan_is_memoized_too(counted):
    server, calls = counted
    hashes = [_get(server, '/deposit-scan?miner_hotkey=hk')['tx_hash'] for _ in range(10)]
    assert calls['scan'] == 1
    assert hashes == ['hash-1'] * 10


def test_failures_are_not_cached(counted, monkeypatch):
    """A transient RPC fault must not pin an error for the whole bucket."""
    server, calls = counted
    state = {'fail': True}

    def flaky(_validator, _miner_hotkey, swap_key=''):
        if state['fail']:
            raise RuntimeError('getAccountInfo: HTTP 429')
        calls['status'] += 1
        return SwapStatus('reserved', 999, 'recovered', swap_key)

    monkeypatch.setattr(seam_http, 'swap_status', flaky)
    with pytest.raises(urllib.error.HTTPError):
        _get(server, '/status?miner_hotkey=hk')
    state['fail'] = False
    # Same bucket: the retry reaches the producer instead of replaying the failure.
    assert _get(server, '/status?miner_hotkey=hk')['user'] == 'recovered'
