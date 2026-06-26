"""Off-chain market-rate feed for the rate-quality scoring component (Phase C).

Phase C's quality-volume reward weights a miner's realized VWAP against the live
market rate. The benchmark is off-chain — the contract can't see prices — so the
scoring validator runs this small best-effort feed. It is intentionally
standalone: it knows nothing about scoring internals; scoring asks it for one
number (canonical TAO per BTC) per round.

Recipe mirrors the ``allways-rates`` tool: ``TAO/BTC = BTC_usd / TAO_usd``, each
leg averaged over whatever public sources respond (Coinbase + Kraken for BTC,
CoinGecko + MEXC for TAO). Fail-safe: if a refresh can't price either leg, the
last good value is served until ``MARKET_RATE_MAX_STALE_SECS``, after which
``tao_per_btc()`` returns ``None`` — the caller (``rate_quality``) treats ``None``
as neutral (1.0) so a dead feed never zeroes everyone or hands out free reward.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, List, NamedTuple, Optional, Sequence

import bittensor as bt
import requests

from allways.constants import (
    MARKET_RATE_BTC_USD_COINBASE_URL,
    MARKET_RATE_BTC_USD_KRAKEN_URL,
    MARKET_RATE_HTTP_TIMEOUT_SECS,
    MARKET_RATE_MAX_STALE_SECS,
    MARKET_RATE_TAO_USD_COINGECKO_URL,
    MARKET_RATE_TAO_USD_MEXC_URL,
    MARKET_RATE_TTL_SECS,
)


class PriceSource(NamedTuple):
    """One USD price endpoint and the parser for its response shape."""

    name: str
    url: str
    parse: Callable[[dict], float]


def _coinbase_btc_usd(data: dict) -> float:
    return float(data['data']['amount'])


def _kraken_btc_usd(data: dict) -> float:
    # Kraken keys the result by its own pair name (e.g. XXBTZUSD); take the first.
    return float(next(iter(data['result'].values()))['c'][0])


def _coingecko_tao_usd(data: dict) -> float:
    return float(data['bittensor']['usd'])


def _mexc_tao_usd(data: dict) -> float:
    return float(data['price'])


BTC_USD_SOURCES = (
    PriceSource('coinbase', MARKET_RATE_BTC_USD_COINBASE_URL, _coinbase_btc_usd),
    PriceSource('kraken', MARKET_RATE_BTC_USD_KRAKEN_URL, _kraken_btc_usd),
)
TAO_USD_SOURCES = (
    PriceSource('coingecko', MARKET_RATE_TAO_USD_COINGECKO_URL, _coingecko_tao_usd),
    PriceSource('mexc', MARKET_RATE_TAO_USD_MEXC_URL, _mexc_tao_usd),
)


class MarketRateFeed:
    """Best-effort, TTL-cached canonical TAO/BTC feed.

    One instance per validator. ``tao_per_btc()`` is the whole interface; it
    refreshes at most once per ``MARKET_RATE_TTL_SECS`` and never raises. Sources
    and ``clock`` are injectable so tests stay network-free (mirrors
    ``SolanaConfigCache``)."""

    def __init__(
        self,
        btc_usd_sources: Sequence[PriceSource] = BTC_USD_SOURCES,
        tao_usd_sources: Sequence[PriceSource] = TAO_USD_SOURCES,
        clock: Optional[Callable[[], float]] = None,
        session: Optional[requests.Session] = None,
    ):
        self._btc_sources: List[PriceSource] = list(btc_usd_sources)
        self._tao_sources: List[PriceSource] = list(tao_usd_sources)
        self._clock = clock or time.time
        self._http = session or requests.Session()
        self._lock = threading.Lock()
        self._rate: Optional[float] = None
        self._rate_at: float = 0.0  # unix-seconds of the last *successful* refresh
        self._attempt_at: Optional[float] = None  # throttles refresh attempts while failing

    def _fetch_one(self, source: PriceSource) -> Optional[float]:
        """One source's USD price, or None on any failure (network, status,
        parse, non-positive). Never raises."""
        try:
            resp = self._http.get(source.url, timeout=MARKET_RATE_HTTP_TIMEOUT_SECS)
            resp.raise_for_status()
            value = source.parse(resp.json())
            if value <= 0:
                raise ValueError(f'non-positive price {value}')
            return value
        except Exception as e:
            bt.logging.debug(f'market-rate source {source.name} failed: {e}')
            return None

    def _average(self, sources: Sequence[PriceSource]) -> Optional[float]:
        """Mean of the sources that responded with a positive price, or None if
        none did."""
        values = [v for v in (self._fetch_one(s) for s in sources) if v is not None]
        if not values:
            return None
        return sum(values) / len(values)

    def _refresh(self) -> Optional[float]:
        """Fetch both legs and derive TAO/BTC, or None if either leg is dark."""
        btc_usd = self._average(self._btc_sources)
        tao_usd = self._average(self._tao_sources)
        if btc_usd is None or tao_usd is None or tao_usd <= 0:
            return None
        return btc_usd / tao_usd

    def tao_per_btc(self) -> Optional[float]:
        """Canonical TAO-per-BTC, or None when the feed is stale/unreachable.

        TTL-cached: a value fresher than ``MARKET_RATE_TTL_SECS`` is returned as
        is. Otherwise a refresh is attempted (throttled to once per TTL even
        while failing, so a dead feed can't turn a frequent caller into a request
        storm). On a failed refresh the last good value is served until
        ``MARKET_RATE_MAX_STALE_SECS`` since the last success, then None. Never
        raises — a dead feed degrades to neutral, it does not crash scoring."""
        with self._lock:
            now = self._clock()
            age = now - self._rate_at
            if self._rate is not None and age < MARKET_RATE_TTL_SECS:
                return self._rate
            if self._attempt_at is None or (now - self._attempt_at) >= MARKET_RATE_TTL_SECS:
                self._attempt_at = now
                fresh = self._refresh()
                if fresh is not None:
                    self._rate = fresh
                    self._rate_at = now
                    return fresh
            if self._rate is not None and age < MARKET_RATE_MAX_STALE_SECS:
                bt.logging.warning('market-rate refresh failed; serving last-good TAO/BTC')
                return self._rate
            bt.logging.warning('market-rate feed unavailable/stale; rate_quality will be neutral')
            return None
