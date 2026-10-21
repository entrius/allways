"""Tip-gated BTC verify cache: a pending leg is refetched only when the chain tip moves.

Safety rule pinned here: only ``pending`` results are ever served from cache. Absent (None) and confirmed
results always hit the API, so no slash or confirm decision can be made on a stale view. Backend mocked.
"""

import json

from allways.assets.asset import ProviderUnreachableError

TX = 'ab' * 32
RECIP = 'bc1qrecipient'


class _Resp:
    def __init__(self, status_code=200, body=None, text=''):
        self.status_code = status_code
        self.ok = status_code < 400
        self._body = body
        self.text = text if text else json.dumps(body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(str(self.status_code))

    def json(self):
        return self._body


def _tx(confirmed=False, height=None):
    status = {'confirmed': confirmed}
    if confirmed:
        status.update(block_height=height, block_hash='h' * 64, block_time=1_700_000_000)
    return {
        'status': status,
        'vin': [{'prevout': {'scriptpubkey_address': 'bc1qsender'}}],
        'vout': [{'scriptpubkey_address': RECIP, 'value': 5000}],
    }


class _Session:
    """Scripted Esplora: tip from ``tip`` (mutable), ``tx`` body from ``tx`` (mutable); counts /tx calls."""

    def __init__(self, tip=100, tx='default'):
        self.tip = tip
        self.tx = _tx() if tx == 'default' else tx
        self.tx_calls = 0

    def request(self, method, url, timeout=None, headers=None, **kw):
        if url.endswith('/blocks/tip/height'):
            return _Resp(text=str(self.tip)) if self.tip is not None else _Resp(status_code=503, text='down')
        if url.endswith(f'/tx/{TX}'):
            self.tx_calls += 1
            if self.tx is None:
                return _Resp(status_code=404, body={}, text='not found')
            return _Resp(body=self.tx)
        if '/block/' in url and url.endswith('/status'):
            return _Resp(body={'in_best_chain': True})
        raise AssertionError(url)


def _provider(monkeypatch, session):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    monkeypatch.setenv('BTC_NETWORK', 'mainnet')
    monkeypatch.setenv('BTC_ESPLORA_URLS', 'https://bs/api')
    from allways.assets.btc import Bitcoin

    p = Bitcoin()
    p.http = session
    return p


def _verify(p):
    p.chain.clear_pass_tip()  # the validator clears the pass tip every forward step
    return p.api_verify_transaction(TX, RECIP, 5000)


def test_tip_move_refetches_and_sees_confirmation(monkeypatch):
    s = _Session(tip=100, tx=_tx(confirmed=True, height=100))  # 1 conf: pending (min_confirmations=2)
    p = _provider(monkeypatch, s)
    assert _verify(p).confirmed is False
    assert _verify(p).confirmed is False and s.tx_calls == 1
    s.tip = 101  # new block → refetch → 2 confs → confirmed
    assert _verify(p).confirmed is True
    assert s.tx_calls == 2


def test_absent_is_never_cached(monkeypatch):
    s = _Session(tx=None)
    p = _provider(monkeypatch, s)
    assert _verify(p) is None
    s.tx = _tx()  # payout enters the mempool with no tip change
    assert _verify(p) is not None
    assert s.tx_calls == 2


def test_confirmed_is_never_cached(monkeypatch):
    s = _Session(tip=105, tx=_tx(confirmed=True, height=100))
    p = _provider(monkeypatch, s)
    assert _verify(p).confirmed is True
    assert _verify(p).confirmed is True
    assert s.tx_calls == 2  # live fetch (+ reorg check) on every confirm


def test_cache_key_includes_recipient_and_amount(monkeypatch):
    s = _Session()
    p = _provider(monkeypatch, s)
    _verify(p)
    p.chain.clear_pass_tip()
    assert p.api_verify_transaction(TX, RECIP, 9000) is None  # vout pays 5000 < 9000 → must hit the API
    assert s.tx_calls == 2


def test_unreachable_is_not_cached(monkeypatch):
    import requests

    s = _Session()
    p = _provider(monkeypatch, s)

    def boom(method, url, **kw):
        if url.endswith(f'/tx/{TX}'):
            raise requests.ConnectionError('down')
        return _Session.request(s, method, url, **kw)

    s.request = boom
    try:
        _verify(p)
        raise AssertionError('expected ProviderUnreachableError')
    except ProviderUnreachableError:
        pass
    assert p._verify_cache == {}


class _LaggingTip(_Session):
    """Answers tip reads from a script: the first is the new block, later ones a backend one behind."""

    def __init__(self, tips, **kw):
        super().__init__(**kw)
        self.tips = iter(tips)

    def request(self, method, url, **kw):
        if url.endswith('/blocks/tip/height'):
            return _Resp(text=str(next(self.tips)))
        return super().request(method, url, **kw)


def test_confirmations_use_the_tip_that_keys_the_cache(monkeypatch):
    # The tip TTL lapses mid-verify and a lagging backend answers one block behind: counting from that
    # second read parked "1/2 pending" under the newer tip until the next block.
    s = _LaggingTip([101, 100, 100], tx=_tx(confirmed=True, height=100))
    p = _provider(monkeypatch, s)
    monkeypatch.setattr(type(p.chain), '_TIP_TTL_SECONDS', 0.0)
    info = _verify(p)
    assert info.confirmed is True and info.confirmations == 2
