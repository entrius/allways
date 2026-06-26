"""C1 — market-rate feed tests (network-free; sources/clock injected)."""

import requests

from allways.constants import (
    MARKET_RATE_BTC_USD_COINBASE_URL,
    MARKET_RATE_BTC_USD_KRAKEN_URL,
    MARKET_RATE_MAX_STALE_SECS,
    MARKET_RATE_TAO_USD_COINGECKO_URL,
    MARKET_RATE_TAO_USD_MEXC_URL,
    MARKET_RATE_TTL_SECS,
)
from allways.validator.market_rate import (
    BTC_USD_SOURCES,
    TAO_USD_SOURCES,
    MarketRateFeed,
)


class FakeResponse:
    def __init__(self, payload, ok=True):
        self._payload = payload
        self._ok = ok

    def raise_for_status(self):
        if not self._ok:
            raise requests.HTTPError('bad status')

    def json(self):
        return self._payload


class FakeSession:
    """Maps url → payload dict | Exception | (payload, ok=False). Records calls."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        r = self.responses.get(url)
        if isinstance(r, Exception):
            raise r
        if isinstance(r, tuple):
            return FakeResponse(r[0], ok=r[1])
        return FakeResponse(r)


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


# Real-shape payloads for the real source URLs, so the actual parsers are exercised.
def real_payloads(btc=100_000.0, tao=400.0):
    return {
        MARKET_RATE_BTC_USD_COINBASE_URL: {'data': {'amount': str(btc)}},
        MARKET_RATE_BTC_USD_KRAKEN_URL: {'result': {'XXBTZUSD': {'c': [str(btc), '1.0']}}},
        MARKET_RATE_TAO_USD_COINGECKO_URL: {'bittensor': {'usd': str(tao)}},
        MARKET_RATE_TAO_USD_MEXC_URL: {'price': str(tao)},
    }


def make_feed(responses, clock):
    return MarketRateFeed(
        btc_usd_sources=BTC_USD_SOURCES,
        tao_usd_sources=TAO_USD_SOURCES,
        clock=clock,
        session=FakeSession(responses),
    )


def test_all_sources_respond_averages_and_divides():
    # All four real parsers exercised: BTC=100k, TAO=400 → 250 TAO/BTC.
    feed = make_feed(real_payloads(100_000, 400), Clock())
    assert feed.tao_per_btc() == 250.0


def test_averages_across_survivors_per_leg():
    # Coinbase 100k + Kraken 120k → 110k; CoinGecko 400 + MEXC 600 → 500 → 220.
    responses = {
        MARKET_RATE_BTC_USD_COINBASE_URL: {'data': {'amount': '100000'}},
        MARKET_RATE_BTC_USD_KRAKEN_URL: {'result': {'XXBTZUSD': {'c': ['120000', '1']}}},
        MARKET_RATE_TAO_USD_COINGECKO_URL: {'bittensor': {'usd': '400'}},
        MARKET_RATE_TAO_USD_MEXC_URL: {'price': '600'},
    }
    feed = make_feed(responses, Clock())
    assert feed.tao_per_btc() == 110_000 / 500


def test_single_source_down_per_leg_still_prices():
    # Kraken errors, MEXC 404s — each leg still has one survivor → 100k/400 = 250.
    responses = real_payloads(100_000, 400)
    responses[MARKET_RATE_BTC_USD_KRAKEN_URL] = requests.ConnectionError('boom')
    responses[MARKET_RATE_TAO_USD_MEXC_URL] = ({}, False)  # raise_for_status fails
    feed = make_feed(responses, Clock())
    assert feed.tao_per_btc() == 250.0


def test_one_leg_fully_dark_returns_none():
    # Both BTC sources down → no BTC leg → None (no last-good yet).
    responses = real_payloads()
    responses[MARKET_RATE_BTC_USD_COINBASE_URL] = requests.Timeout('t')
    responses[MARKET_RATE_BTC_USD_KRAKEN_URL] = requests.Timeout('t')
    feed = make_feed(responses, Clock())
    assert feed.tao_per_btc() is None


def test_malformed_json_tolerated():
    # Coinbase returns junk shape (KeyError in parser) but Kraken survives.
    responses = real_payloads(100_000, 400)
    responses[MARKET_RATE_BTC_USD_COINBASE_URL] = {'unexpected': 'shape'}
    feed = make_feed(responses, Clock())
    assert feed.tao_per_btc() == 250.0


def test_non_positive_price_rejected():
    # Coinbase reports 0 → rejected; Kraken (100k) carries the leg.
    responses = real_payloads(100_000, 400)
    responses[MARKET_RATE_BTC_USD_COINBASE_URL] = {'data': {'amount': '0'}}
    feed = make_feed(responses, Clock())
    assert feed.tao_per_btc() == 250.0


def test_ttl_cache_hit_skips_network():
    clock = Clock(1000.0)
    session = FakeSession(real_payloads(100_000, 400))
    feed = MarketRateFeed(clock=clock, session=session)
    assert feed.tao_per_btc() == 250.0
    n_after_first = len(session.calls)
    # Within TTL: no new fetches, same value.
    clock.t += MARKET_RATE_TTL_SECS - 1
    assert feed.tao_per_btc() == 250.0
    assert len(session.calls) == n_after_first


def test_refresh_after_ttl_expires():
    clock = Clock(1000.0)
    session = FakeSession(real_payloads(100_000, 400))
    feed = MarketRateFeed(clock=clock, session=session)
    assert feed.tao_per_btc() == 250.0
    n_after_first = len(session.calls)
    # Past TTL: refetch happens. Move the price to confirm the new value is used.
    session.responses = real_payloads(100_000, 200)  # TAO halves → rate doubles
    clock.t += MARKET_RATE_TTL_SECS + 1
    assert feed.tao_per_btc() == 500.0
    assert len(session.calls) > n_after_first


def test_serves_last_good_within_stale_window_then_none():
    clock = Clock(1000.0)
    session = FakeSession(real_payloads(100_000, 400))
    feed = MarketRateFeed(clock=clock, session=session)
    assert feed.tao_per_btc() == 250.0
    # Feed goes dark. Within stale window → last-good still served.
    session.responses = {url: requests.Timeout('down') for url in session.responses}
    clock.t += MARKET_RATE_TTL_SECS + 1
    assert feed.tao_per_btc() == 250.0
    # Past the stale window since last success → None.
    clock.t += MARKET_RATE_MAX_STALE_SECS
    assert feed.tao_per_btc() is None


def test_refresh_attempts_throttled_while_failing():
    # All sources error from the start; repeated calls within one TTL must not
    # re-hit the network on every call.
    clock = Clock(1000.0)
    responses = {url: requests.ConnectionError('x') for url in real_payloads()}
    session = FakeSession(responses)
    feed = MarketRateFeed(clock=clock, session=session)
    assert feed.tao_per_btc() is None
    n = len(session.calls)
    # Second call within TTL: throttled, no extra network calls.
    clock.t += 1
    assert feed.tao_per_btc() is None
    assert len(session.calls) == n


def test_never_raises_on_total_failure():
    feed = make_feed({}, Clock())  # every url missing → FakeSession returns None payload
    # A None payload makes .json() return None → parser raises → swallowed.
    assert feed.tao_per_btc() is None
