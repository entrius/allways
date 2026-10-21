"""The fee pool's SOL→TAO rate: CoinGecko USD prices for SOL and TAO at one fixed UTC hour.

Every validator asks for the same hour (``rate_hour``), so every validator derives the same rate. One
round tries ``SOL_TAO_FETCH_ATTEMPTS`` times inside ``SOL_TAO_FETCH_BUDGET_SECS`` and then gives up, so
a slow or rate-limited feed never stalls scoring.
"""

import math
import time
from typing import Callable, List, Optional, Tuple

import bittensor as bt
import requests

from allways.constants import (
    SOL_TAO_FETCH_ATTEMPTS,
    SOL_TAO_FETCH_BUDGET_SECS,
    SOL_TAO_RATE_LAG_SECS,
)
from allways.miner.optimizer.market_price import COINGECKO_IDS

COINGECKO_RANGE_URL = 'https://api.coingecko.com/api/v3/coins/{id}/market_chart/range'
HOUR_SECS = 3600


class RateLimited(Exception):
    """A 429, carrying its Retry-After in seconds (None when absent or unparseable)."""

    def __init__(self, retry_after: Optional[float]):
        super().__init__(f'rate limited (retry-after={retry_after})')
        self.retry_after = retry_after


def rate_hour(now: int) -> int:
    """The last full UTC hour at least ``SOL_TAO_RATE_LAG_SECS`` old, as unix seconds."""
    return (int(now) - SOL_TAO_RATE_LAG_SECS) // HOUR_SECS * HOUR_SECS


def valid_price(value: float) -> bool:
    return math.isfinite(value) and value > 0


def hour_price(points: List[Tuple[int, float]], hour: int) -> float:
    """The price at ``hour`` from ``[(unix_secs, usd)]``: the point stamped exactly ``hour``, else the
    latest point in ``[hour - 1h, hour]``. Raises if there is none or it is not a positive finite number."""
    in_hour = [(ts, price) for ts, price in points if hour - HOUR_SECS <= ts <= hour]
    exact = [price for ts, price in in_hour if ts == hour]
    if exact:
        price = exact[-1]
    elif in_hour:
        price = max(in_hour)[1]
    else:
        raise ValueError(f'no price point in the hour ending {hour}')
    if not valid_price(price):
        raise ValueError(f'invalid price {price} for hour {hour}')
    return price


def _retry_after(header: Optional[str]) -> Optional[float]:
    try:
        return float(header) if header is not None else None
    except ValueError:
        return None


def fetch_hour_usd(coin_id: str, hour: int, timeout: float) -> float:
    """One CoinGecko range read for ``coin_id`` over the hour ending at ``hour``; USD at ``hour``."""
    resp = requests.get(
        COINGECKO_RANGE_URL.format(id=coin_id),
        params={'vs_currency': 'usd', 'from': hour - HOUR_SECS, 'to': hour},
        timeout=timeout,
    )
    if resp.status_code == 429:
        raise RateLimited(_retry_after(resp.headers.get('Retry-After')))
    resp.raise_for_status()
    points = [(int(ts) // 1000, float(price)) for ts, price in resp.json()['prices']]
    return hour_price(points, hour)


def fetch_sol_tao_rate(
    hour: int,
    fetch: Callable[[str, int, float], float] = fetch_hour_usd,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Optional[float]:
    """TAO per SOL at ``hour`` (``sol_usd / tao_usd``, both read for that same hour), or None once the
    attempts or the time budget run out. Backs off 1s/2s/4s between attempts; a 429's Retry-After is
    honoured when it fits the remaining budget, else the round gives up."""
    deadline = clock() + SOL_TAO_FETCH_BUDGET_SECS
    for attempt in range(SOL_TAO_FETCH_ATTEMPTS):
        wait = float(2**attempt)
        try:
            sol_usd = fetch(COINGECKO_IDS['sol'], hour, max(0.1, deadline - clock()))
            tao_usd = fetch(COINGECKO_IDS['tao'], hour, max(0.1, deadline - clock()))
            rate = sol_usd / tao_usd
            if valid_price(rate):
                return rate
            bt.logging.debug(f'sol/tao rate {rate} for hour {hour} rejected')
        except RateLimited as e:
            if e.retry_after is not None:
                wait = e.retry_after
            bt.logging.debug(f'sol/tao price fetch for hour {hour}: {e}')
        except Exception as e:
            bt.logging.debug(f'sol/tao price fetch for hour {hour} failed (attempt {attempt + 1}): {e}')
        if attempt == SOL_TAO_FETCH_ATTEMPTS - 1 or clock() + wait > deadline:
            break
        sleep(wait)
    return None
