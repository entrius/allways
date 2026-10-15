"""``/rate`` off the held market: no client read while it is fresh, a live read when it is stale."""

import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from solders.keypair import Keypair as SolKeypair

from allways.constants import RATE_PRECISION
from allways.validator.market import MARKET_MAX_AGE_SECS, MarketSnapshot
from allways.validator.reserve_engine import rate_quote

SOL = 10**9
MINER = SolKeypair().pubkey()
HOTKEY = '5' + 'A' * 47


def _quote(rate: str):
    return SimpleNamespace(
        miner=MINER,
        from_chain='sol',
        to_chain='btc',
        rate=int(Decimal(rate) * RATE_PRECISION),
        collateral_chain='sol',
        miner_from_addr='from',
        miner_to_addr='to',
    )


def _accounts(rate: str) -> dict:
    state = SimpleNamespace(miner=MINER, active=True, failed_swaps=0, collateral=100 * SOL)
    return {'MinerQuote': [(MINER, _quote(rate))], 'MinerState': [(MINER, state)], 'BondAttestation': []}


class StrictClient:
    """Any read is a test failure: a fresh market must answer the quote alone."""

    def __getattr__(self, name):
        raise AssertionError(f'client.{name} read during a held-market quote')


class RecordingClient:
    def __init__(self, rate: str):
        self.accounts = {**_accounts(rate), 'Binding': []}
        self.reads: list = []

    def get_all(self, name):
        self.reads.append(name)
        return self.accounts[name]


def _validator(client, market):
    cfg = SimpleNamespace(min_swap_amount=0, max_swap_amount=0)
    return SimpleNamespace(solana_client=client, market=market, solana_config_cache=SimpleNamespace(config=lambda: cfg))


def test_a_fresh_market_quotes_without_one_client_read():
    market = MarketSnapshot(time.time(), _accounts('0.5'), {str(MINER): HOTKEY})
    rq = rate_quote(_validator(StrictClient(), market), 'sol', 'btc', SOL)
    assert rq.quote.miner_hotkey == HOTKEY and rq.quote.rate_display == '0.5'


@pytest.mark.parametrize('held', [None, 'stale'])
def test_a_missing_or_stale_market_is_read_live(held):
    market = held and MarketSnapshot(time.time() - MARKET_MAX_AGE_SECS - 1, _accounts('0.5'), {str(MINER): HOTKEY})
    client = RecordingClient('0.6')
    rq = rate_quote(_validator(client, market), 'sol', 'btc', SOL)
    assert sorted(client.reads) == ['Binding', 'BondAttestation', 'MinerQuote', 'MinerState']
    assert [level['rate_display'] for level in rq.levels] == ['0.6']  # the live quote, not the held one
