"""USD spot prices for the quote optimizer's market guard.

CoinGecko in one batch, then Coinbase and MEXC spot per asset; an operator pin wins over every feed.
Each price is kept with the time it was fetched and read back only while fresh, so a dead feed reads
as "no price" — never as the last number it happened to return.
"""

import time
from typing import Dict, Iterable, List, Optional, Tuple

import bittensor as bt
import requests

# CoinGecko ids per chain id — extending the optimizer to a new chain starts here.
COINGECKO_IDS = {
    'btc': 'bitcoin',
    'tao': 'bittensor',
    'sol': 'solana',
    'eth': 'ethereum',
    'arbusdc': 'usd-coin',
    'baseusdc': 'usd-coin',
    'ethusdc': 'usd-coin',
    'polusdc': 'usd-coin',
    'solusdc': 'usd-coin',
    'hype': 'hyperliquid',
    'bnb': 'binancecoin',
    'avax': 'avalanche-2',
    'cro': 'crypto-com-chain',
    'aster': 'aster-2',
    'uni': 'uniswap',
    'qnt': 'quant-network',
    'pol': 'polygon-ecosystem-token',
    'paxg': 'pax-gold',
}

PRICE_REQUEST_TIMEOUT_SECS = 10
PRICE_MAX_AGE_SECS = 300


def _fallback_symbol(chain: str) -> str:
    return 'USDC' if chain.endswith('usdc') else chain.upper()


def _fetch_coingecko_batch(ids: List[str]) -> Dict[str, float]:
    resp = requests.get(
        'https://api.coingecko.com/api/v3/simple/price',
        params={'ids': ','.join(sorted(set(ids))), 'vs_currencies': 'usd'},
        timeout=PRICE_REQUEST_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    return {cid: float(entry['usd']) for cid, entry in resp.json().items() if 'usd' in entry}


def _fetch_coinbase(symbol: str) -> float:
    resp = requests.get(f'https://api.coinbase.com/v2/prices/{symbol}-USD/spot', timeout=PRICE_REQUEST_TIMEOUT_SECS)
    resp.raise_for_status()
    return float(resp.json()['data']['amount'])


def _fetch_mexc(symbol: str) -> float:
    resp = requests.get(
        'https://api.mexc.com/api/v3/ticker/price',
        params={'symbol': f'{symbol}USDT'},
        timeout=PRICE_REQUEST_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    return float(resp.json()['price'])


class MarketPrices:
    """USD price per chain id, refreshed on demand and read back only while fresh."""

    def __init__(self, pins: Optional[Dict[str, float]] = None, max_age_secs: int = PRICE_MAX_AGE_SECS):
        self.pins = {str(k).lower(): float(v) for k, v in (pins or {}).items()}
        self.max_age_secs = max_age_secs
        self.cache: Dict[str, Tuple[float, float]] = {}

    def refresh(self, chains: Iterable[str], now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        need = [c for c in chains if c not in self.pins]
        if not need:
            return
        ids = {c: COINGECKO_IDS.get(c, '') for c in need}
        batch: Dict[str, float] = {}
        if any(ids.values()):
            try:
                batch = _fetch_coingecko_batch([cid for cid in ids.values() if cid])
            except Exception as e:
                bt.logging.debug(f'CoinGecko price batch failed ({e}); falling back per asset')
        for chain in need:
            if ids[chain] in batch:
                self.cache[chain] = (batch[ids[chain]], now)
                continue
            for fetch in (_fetch_coinbase, _fetch_mexc):
                try:
                    self.cache[chain] = (fetch(_fallback_symbol(chain)), now)
                    break
                except Exception:
                    continue

    def usd(self, chain: str, now: Optional[float] = None) -> Optional[float]:
        if chain in self.pins:
            return self.pins[chain]
        entry = self.cache.get(chain)
        if entry is None:
            return None
        price, fetched_at = entry
        now = time.time() if now is None else now
        if price <= 0 or now - fetched_at > self.max_age_secs:
            return None
        return price
