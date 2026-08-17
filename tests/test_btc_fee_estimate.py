"""BTC fee-rate estimation fallback (V-L5).

A silent drop to the 5 sat/vB mempool floor when /fee-estimates is down stranded a real dest tx.
The floor and the estimation-DOWN fallback are now distinct: a failed estimate broadcasts at the
higher `BTC_FALLBACK_FEE_RATE`, while a successful estimate still floors at `BTC_MIN_FEE_RATE`
(calm-mempool sends keep paying the real low rate). Backend mocked — no network, no real sleep.
"""

import pytest

from allways.constants import BTC_FALLBACK_FEE_RATE, BTC_MIN_FEE_RATE


class _Resp:
    def __init__(self, *, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(str(self.status_code))

    def json(self):
        return self._body


def _provider(monkeypatch):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    import allways.assets.btc as btc_mod
    from allways.assets.btc import Bitcoin

    monkeypatch.setattr(btc_mod.time, 'sleep', lambda _s: None)  # no real 3s retry wait
    return Bitcoin()


def test_estimate_success_pads_and_floors(monkeypatch):
    p = _provider(monkeypatch)
    # 20 sat/vB * 1.25 pad = 25, above the floor → returned as-is.
    monkeypatch.setattr(p, 'btc_api_get', lambda path, timeout=10: _Resp(body={'2': 20.0}))
    assert p.estimate_fee_rate() == 25


def test_calm_mempool_still_floors_low_not_fallback(monkeypatch):
    p = _provider(monkeypatch)
    # 1 sat/vB * 1.25 = 1 < floor → floor binds. The fallback must NOT leak into the success path.
    monkeypatch.setattr(p, 'btc_api_get', lambda path, timeout=10: _Resp(body={'2': 1.0}))
    assert p.estimate_fee_rate() == BTC_MIN_FEE_RATE


def test_estimation_down_uses_fallback_not_floor(monkeypatch):
    p = _provider(monkeypatch)

    def down(path, timeout=10):
        raise ConnectionError('fee-estimates unavailable')

    monkeypatch.setattr(p, 'btc_api_get', down)
    rate = p.estimate_fee_rate()
    assert rate == BTC_FALLBACK_FEE_RATE
    assert rate > BTC_MIN_FEE_RATE  # the whole point: not the strand-prone floor


def test_override_bypasses_estimation(monkeypatch):
    p = _provider(monkeypatch)

    def forbidden(*_a, **_kw):
        raise AssertionError('an explicit override must not hit the estimator')

    monkeypatch.setattr(p, 'btc_api_get', forbidden)
    assert p.estimate_fee_rate(override=7) == 7
