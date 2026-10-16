import threading

import requests
from requests.adapters import HTTPAdapter

from allways.validator_v2.chain.confirm import ConfirmTracker
from allways.validator_v2.chain.gateway import Endpoint, Meter, TokenBucket, UsageReport, calls_in


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, secs):
        self.t += secs


def test_token_bucket_bursts_then_paces():
    clock = _Clock()
    bucket = TokenBucket(rate=10, burst=3, clock=clock, sleep=clock.sleep)
    assert [bucket.acquire() for _ in range(3)] == [0.0, 0.0, 0.0]
    assert abs(bucket.acquire() - 0.1) < 1e-9


def test_a_batch_takes_one_token_per_call():
    clock = _Clock()
    bucket = TokenBucket(rate=10, burst=10, clock=clock, sleep=clock.sleep)
    bucket.acquire(10)
    assert abs(bucket.acquire(5) - 0.5) < 1e-9


def test_calls_are_counted_the_way_providers_bill():
    assert calls_in(b'{"jsonrpc":"2.0","id":1,"method":"getSlot"}', '/') == ['getSlot']
    batch = b'[{"method":"eth_getTransactionReceipt"},{"method":"eth_getBlockByNumber"}]'
    assert calls_in(batch, '/') == ['eth_getTransactionReceipt', 'eth_getBlockByNumber']
    assert calls_in(None, '/api/tx/abc/status') == ['/api']


def _response(status):
    def send(self, request, *args, **kwargs):
        r = requests.Response()
        r.status_code, r.request, r.url, r._content = status, request, request.url, b'{}'
        return r

    return send


def _metered(monkeypatch, meter, status=200):
    monkeypatch.setattr(HTTPAdapter, 'send', _response(status))
    return meter.attach(requests.Session())


def test_every_session_is_metered_by_endpoint_component_and_call(monkeypatch):
    meter = Meter([Endpoint('helius', 'mainnet.helius-rpc.com', rate=1000, burst=1000)])
    session = _metered(monkeypatch, meter)

    def world():
        session.post('https://mainnet.helius-rpc.com/?api-key=x', json={'method': 'getMultipleAccounts'})

    t = threading.Thread(target=world, name='world')
    t.start()
    t.join()
    session.get('https://blockstream.info/api/blocks/tip/height')
    usage = meter.usage()
    assert usage[('helius', 'world', 'getMultipleAccounts')] == 1
    assert usage[('blockstream.info', 'MainThread', '/api')] == 1  # unconfigured host: counted, not throttled
    assert meter.totals() == {'helius': 1, 'blockstream.info': 1}


def test_429s_are_counted_per_endpoint(monkeypatch):
    meter = Meter([Endpoint('dwellir', 'api-eth.dwellir.com', rate=1000, burst=1000)])
    session = _metered(monkeypatch, meter, status=429)
    session.post('https://api-eth.dwellir.com/k', json={'method': 'eth_blockNumber'})
    assert meter._throttled['dwellir'] == 1


def test_projection_and_budget_warning(monkeypatch, caplog):
    clock = _Clock()
    endpoints = [Endpoint('helius', 'h.example', rate=1000, burst=1000, daily_budget=100)]
    meter = Meter(endpoints, clock=clock)
    session = _metered(monkeypatch, meter)
    for _ in range(10):
        session.post('https://h.example/', json={'method': 'getSlot'})
    clock.t = 3600
    assert meter.projected_daily() == {'helius': 240}
    warned = []
    monkeypatch.setattr('bittensor.logging.warning', lambda msg: warned.append(msg))
    UsageReport(meter, endpoints).tick(clock.t)
    assert warned and 'over its 100/day budget' in warned[0]


class _StatusRpc:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = 0

    def get_signature_statuses(self, sigs):
        self.calls += 1
        return [self.statuses.get(s) for s in sigs]


def test_confirm_tracker_resolves_in_one_batched_read():
    tracker = ConfirmTracker(expiry_secs=90)
    for sig in ('ok', 'bad', 'pending', 'lost'):
        tracker.track(sig, intent=sig, now=0 if sig != 'pending' else 50)
    rpc = _StatusRpc({'ok': {'err': None, 'confirmationStatus': 'confirmed'}, 'bad': {'err': {'x': 1}}})
    resolved = tracker.resolve(rpc, now=100)
    assert rpc.calls == 1
    assert [s.signature for s in resolved.landed] == ['ok']
    assert [s.signature for s, _ in resolved.failed] == ['bad']
    assert [s.signature for s in resolved.expired] == ['lost']
    assert [s.signature for s in tracker.pending()] == ['pending']
