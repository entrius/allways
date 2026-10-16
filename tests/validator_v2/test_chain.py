from allways.solana.rpc import SolanaRpc
from allways.validator_v2.chain.confirm import ConfirmTracker
from allways.validator_v2.chain.gateway import RpcGateway, TokenBucket


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
    waited = bucket.acquire()
    assert abs(waited - 0.1) < 1e-9
    assert abs(clock.t - 0.1) < 1e-9


def test_gateway_meters_per_component_and_method(monkeypatch):
    monkeypatch.setattr(SolanaRpc, '_call', lambda self, method, params: 42)
    gw = RpcGateway('http://unused', rate=1000, burst=1000)
    assert gw.rpc('world').get_slot() == 42
    gw.rpc('world').get_slot()
    gw.rpc('feed')._call('getBlockTime', [1])
    assert gw.usage() == {('world', 'getSlot'): 2, ('feed', 'getBlockTime'): 1}
    assert gw.rpc('world') is gw.rpc('world')  # one client per component per thread


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
