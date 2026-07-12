"""BTC tip hoist — ``api_calc_confirmations`` shares the pass-cached chain tip so N legs in one
forward pass issue a single ``/blocks/tip/height`` instead of one each (mirrors the shipped SOL hoist,
#542/#543). The 15s TTL that stops non-clearing callers freezing lives in the base; here we cover the
BTC-specific wiring: confirmations math, one-fetch-per-pass sharing, and ``clear_pass_tip`` re-fetch.
Backend is mocked — no network.
"""


class _Resp:
    def __init__(self, *, status_code=200, text=''):
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = text


def _provider(monkeypatch):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    from allways.chain_providers.bitcoin import BitcoinProvider

    return BitcoinProvider()


def test_confirmations_from_cached_tip(monkeypatch):
    p = _provider(monkeypatch)
    monkeypatch.setattr(p, 'btc_api_get', lambda path, timeout=10: _Resp(text='800010'))
    # tip 800010, block 800000 → 800010 - 800000 + 1 = 11 confirmations
    assert p.api_calc_confirmations(800_000) == 11


def test_tip_fetched_once_per_pass(monkeypatch):
    p = _provider(monkeypatch)
    calls = {'n': 0}

    def fake_get(path, timeout=10):
        assert path == '/blocks/tip/height'  # only the tip is fetched here
        calls['n'] += 1
        return _Resp(text='800010')

    monkeypatch.setattr(p, 'btc_api_get', fake_get)
    # Three legs in the same forward pass share ONE tip fetch — the whole point of the hoist.
    p.api_calc_confirmations(800_000)
    p.api_calc_confirmations(800_005)
    p.api_calc_confirmations(799_999)
    assert calls['n'] == 1


def test_clear_pass_tip_forces_refetch(monkeypatch):
    p = _provider(monkeypatch)
    calls = {'n': 0}

    def fake_get(path, timeout=10):
        calls['n'] += 1
        return _Resp(text='800010')

    monkeypatch.setattr(p, 'btc_api_get', fake_get)
    p.api_calc_confirmations(800_000)
    p.clear_pass_tip()  # next forward pass → fresh start-of-pass tip
    p.api_calc_confirmations(800_000)
    assert calls['n'] == 2


def test_tip_fetch_failure_yields_zero_and_is_not_cached(monkeypatch):
    p = _provider(monkeypatch)
    seq = [_Resp(status_code=503), _Resp(text='800010')]

    def fake_get(path, timeout=10):
        return seq.pop(0)

    monkeypatch.setattr(p, 'btc_api_get', fake_get)
    # Failed tip fetch → None tip → 0 confs, and None is not cached, so the next call retries and works.
    assert p.api_calc_confirmations(800_000) == 0
    assert p.api_calc_confirmations(800_000) == 11
