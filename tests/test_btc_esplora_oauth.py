"""Blockstream Explorer API auth on the Esplora ladder.

Blockstream issues OAuth2 client_credentials tokens that expire after 300 s with no refresh token, so a
``url|oauth:ID:SECRET`` entry mints a bearer lazily, caches it, refreshes inside the margin, and retries the
rung once on 401 before failing over. Static ``url|key`` entries are unchanged. Backend mocked — no network.
"""

from allways.assets.btc import OAuthClientCredentials, parse_esplora_urls


class _Resp:
    def __init__(self, *, status_code=200, body=None, text=''):
        self.status_code = status_code
        self.ok = status_code < 400
        self._body = body
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(str(self.status_code))

    def json(self):
        return self._body


class _Session:
    """Records token POSTs and API requests; scripted per-test via ``api_responses``."""

    def __init__(self, token_responses, api_responses):
        self.token_responses = list(token_responses)
        self.api_responses = list(api_responses)
        self.token_posts = 0
        self.requests = []  # (url, headers)

    def post(self, url, data=None, timeout=None):
        self.token_posts += 1
        assert data['grant_type'] == 'client_credentials'
        return self.token_responses.pop(0)

    def request(self, method, url, timeout=None, headers=None, **kwargs):
        self.requests.append((url, headers))
        return self.api_responses.pop(0)


def _token(tok='tok', expires_in=300):
    return _Resp(body={'access_token': tok, 'expires_in': expires_in})


def _provider(monkeypatch, urls, session):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    monkeypatch.setenv('BTC_ESPLORA_URLS', urls)
    from allways.assets.btc import Bitcoin

    p = Bitcoin()
    p.http = session
    return p


# --- parsing ---


def test_parse_oauth_entry_builds_credentials():
    [(url, auth)] = parse_esplora_urls('https://enterprise.blockstream.info/api|oauth:cid:sec:ret')
    assert url == 'https://enterprise.blockstream.info/api'
    assert isinstance(auth, OAuthClientCredentials)
    assert (auth.client_id, auth.client_secret) == ('cid', 'sec:ret')  # secret may contain ':'


# --- token lifecycle ---


def test_stale_token_401_refreshes_and_retries_same_rung(monkeypatch):
    s = _Session([_token('old'), _token('new')], [_Resp(status_code=401), _Resp(text='ok')])
    p = _provider(monkeypatch, 'https://bs/api|oauth:id:sec,https://mempool.space/api', s)
    resp = p.btc_api_get('/blocks/tip/height')
    assert resp.text == 'ok'
    assert s.token_posts == 2
    assert [u for u, _ in s.requests] == ['https://bs/api/blocks/tip/height'] * 2  # no failover needed


def test_token_endpoint_down_fails_over_to_next_rung(monkeypatch):
    s = _Session([_Resp(status_code=503)], [_Resp(text='pub')])
    p = _provider(monkeypatch, 'https://bs/api|oauth:id:sec,https://mempool.space/api', s)
    resp = p.btc_api_get('/blocks/tip/height')
    assert resp.text == 'pub'
    assert [u for u, _ in s.requests] == ['https://mempool.space/api/blocks/tip/height']
