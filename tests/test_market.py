"""``/rate`` off the held market: no client read while it is fresh, a live read when it is stale, and one
alpha-price read per TTL however many threads quote at once."""

import threading
import time
from decimal import Decimal
from types import SimpleNamespace

import bittensor as bt
import pytest
from solders.keypair import Keypair as SolKeypair

from allways.assets.alpha import Alpha
from allways.chains import CHAIN_SN7
from allways.constants import RATE_PRECISION
from allways.validator import reserve_engine
from allways.validator.market import MARKET_MAX_AGE_SECS, PRICE_TTL_SECS, AlphaPriceMemo, MarketSnapshot, memo_priced
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


def test_the_forward_read_keeps_scorings_attestations_instead_of_refetching():
    client = RecordingClient('0.5')
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=7)
    market = MarketSnapshot.read(client, [(MINER, bond)])
    assert sorted(client.reads) == ['Binding', 'MinerQuote', 'MinerState']
    assert market.get_bond_attestation(MINER, 'tao') is bond


def test_the_forward_read_fetches_attestations_until_scoring_has():
    # Boot before a successful scoring round: no attestations held yet, so the step reads them live
    # rather than quoting as if no TAO-backed offer existed.
    client = RecordingClient('0.5')
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=7)
    client.accounts['BondAttestation'] = [(MINER, bond)]
    market = MarketSnapshot.read(client, None)
    assert 'BondAttestation' in client.reads and market.get_bond_attestation(MINER, 'tao') is bond


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


class CountingAlpha(Alpha):
    def __init__(self):
        super().__init__(CHAIN_SN7, SimpleNamespace())
        self.reads: list = []

    def value_rao(self, amount, block=None):
        self.reads.append(block)
        time.sleep(0.02)  # long enough for every waiting thread to pile onto the lock
        return int(amount) * 250_000_000 // 10**self.chain_def.decimals


def test_alpha_price_memo_reads_once_per_ttl_across_threads():
    clock = [1000.0]
    alpha = CountingAlpha()
    priced = memo_priced({'sn7': alpha, 'sol': 'untouched'}, AlphaPriceMemo(clock=lambda: clock[0]))
    assert priced['sol'] == 'untouched' and priced['sn7'].netuid == alpha.netuid

    results: list = []
    threads = [threading.Thread(target=lambda: results.append(priced['sn7'].value_rao(3 * 10**9))) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [750_000_000] * 8 and alpha.reads == [None]

    clock[0] += PRICE_TTL_SECS
    assert priced['sn7'].value_rao(10**9) == 250_000_000 and alpha.reads == [None, None]
    assert priced['sn7'].value_rao(10**9, block=42) == 250_000_000 and alpha.reads[-1] == 42  # dated reads stay live


def test_rate_quote_prices_alpha_legs_off_the_shared_memo(monkeypatch):
    alpha = CountingAlpha()
    monkeypatch.setattr(reserve_engine, '_alpha_prices', AlphaPriceMemo())
    # sol→sn7 is TAO-backed: the sn7 leg is declared, so sizing it prices the alpha.
    alpha_quote = SimpleNamespace(
        miner=MINER,
        from_chain='sol',
        to_chain='sn7',
        rate=2 * RATE_PRECISION,
        collateral_chain='tao',
        miner_from_addr='from',
        miner_to_addr=bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address,  # the sn7 provider judges it
    )
    state = SimpleNamespace(miner=MINER, active=True, failed_swaps=0, collateral=0, active_backings=0b11)
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=10**13)
    accounts = {
        'MinerQuote': [(MINER, alpha_quote)],
        'MinerState': [(MINER, state)],
        'BondAttestation': [(MINER, bond)],
    }
    validator = _validator(StrictClient(), MarketSnapshot(time.time(), accounts, {str(MINER): HOTKEY}))
    validator.axon_assets = {'sn7': alpha}

    for _ in range(3):
        rq = rate_quote(validator, 'sol', 'sn7', SOL)
    assert rq.quote.collateral_amount == 500_000_000 and alpha.reads == [None]  # three quotes, one price read
