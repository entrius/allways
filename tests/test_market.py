"""``/rate`` off forward's held states and the quote memo: no client read while both are fresh, a live state
read when the held ones age out, one memo read per TTL however many threads quote at once."""

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
from allways.validator import reserve_engine, scoring
from allways.validator.market import (
    MARKET_TTL_SECS,
    PRICE_TTL_SECS,
    AlphaPriceMemo,
    QuoteMemo,
    memo_priced,
)
from allways.validator.reserve_engine import rate_quote

SOL = 10**9
MINER = SolKeypair().pubkey()
HOTKEY = '5' + 'A' * 47


def _quote(rate: str, to_chain='btc', backing='sol', to_addr='to'):
    return SimpleNamespace(
        miner=MINER,
        from_chain='sol',
        to_chain=to_chain,
        rate=int(Decimal(rate) * RATE_PRECISION),
        collateral_chain=backing,
        miner_from_addr='from',
        miner_to_addr=to_addr,
    )


def _state(**fields):
    return SimpleNamespace(**{'miner': MINER, 'active': True, 'failed_swaps': 0, 'collateral': 100 * SOL, **fields})


class StrictClient:
    """Any read is a test failure: held states and a warm memo must answer the quote alone."""

    def __getattr__(self, name):
        raise AssertionError(f'client.{name} read during a held quote')


class RecordingClient:
    program_id = None

    def __init__(self, quotes, attestations=(), states=()):
        self.accounts = {
            'MinerQuote': quotes,
            'BondAttestation': list(attestations),
            'MinerState': list(states),
            'Reservation': [],
            'Pool': [],
        }
        self.reads: list = []

    def get_all(self, name):
        self.reads.append(name)
        return self.accounts[name]


def _validator(client, held_states, hotkeys=(HOTKEY,), **config):
    cfg = SimpleNamespace(min_swap_amount=0, max_swap_amount=0, **config)
    return SimpleNamespace(
        solana_client=client,
        held_states=held_states,
        metagraph=SimpleNamespace(hotkeys=list(hotkeys)),
        solana_config_cache=SimpleNamespace(config=lambda: cfg),
    )


@pytest.fixture
def quote_memo(monkeypatch):
    memo = QuoteMemo()
    monkeypatch.setattr(reserve_engine, '_quote_memo', memo)
    return memo


def test_held_states_and_a_warm_memo_quote_without_one_client_read(quote_memo):
    quote_memo.accounts(RecordingClient([(MINER, _quote('0.5'))]))  # warm
    rq = rate_quote(_validator(StrictClient(), (time.time(), {HOTKEY: _state()})), 'sol', 'btc', SOL)
    assert rq.quote.miner_hotkey == HOTKEY and rq.quote.rate_display == '0.5'


def test_the_quote_memo_reads_once_per_ttl_across_threads():
    clock = [1000.0]
    memo = QuoteMemo(clock=lambda: clock[0])
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=7)
    client = RecordingClient([(MINER, _quote('0.5'))], [(MINER, bond)])
    get_all = client.get_all
    client.get_all = lambda name: time.sleep(0.02) or get_all(name)  # long enough for every thread to pile on

    results: list = []
    threads = [threading.Thread(target=lambda: results.append(memo.accounts(client))) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert client.reads == ['MinerQuote', 'BondAttestation', 'Reservation', 'Pool']
    assert all(
        quotes == client.accounts['MinerQuote'] and atts[(str(MINER), 'tao')] is bond
        for quotes, atts, *_holds in results
    )

    clock[0] += MARKET_TTL_SECS
    memo.accounts(client)
    assert client.reads == ['MinerQuote', 'BondAttestation', 'Reservation', 'Pool'] * 2


def test_stale_held_states_are_read_live_with_forwards_own_filter(quote_memo, monkeypatch):
    # The live path is forward's: bound + on-metagraph only, so an off-metagraph miner's quote drops there too.
    monkeypatch.setattr(scoring, 'build_attribution', lambda client: {str(MINER): HOTKEY})
    stale = (time.time() - MARKET_TTL_SECS - 1, {HOTKEY: _state()})
    client = RecordingClient([(MINER, _quote('0.5'))], states=[(MINER, _state())])
    rq = rate_quote(_validator(client, stale, hotkeys=()), 'sol', 'btc', SOL)
    assert 'MinerState' in client.reads and rq.reason == 'no miner offers this direction'
    rq = rate_quote(_validator(client, stale), 'sol', 'btc', SOL)
    assert rq.quote.miner_hotkey == HOTKEY and client.reads.count('MinerQuote') == 1  # the memo served both


def test_a_miner_off_the_held_states_is_not_quoted(quote_memo):
    quote_memo.accounts(RecordingClient([(MINER, _quote('0.5'))]))
    rq = rate_quote(_validator(StrictClient(), (time.time(), {})), 'sol', 'btc', SOL)
    assert rq.quote is None and rq.reason == 'no miner offers this direction'


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


def _alpha_validator(quote_memo, monkeypatch, **config):
    """One miner quoting sol→sn7 at 2 sn7 per SOL, TAO-backed: the sn7 leg is declared, so sizing it
    prices the alpha (which judges the address)."""
    alpha = CountingAlpha()
    monkeypatch.setattr(reserve_engine, '_alpha_prices', AlphaPriceMemo())
    alpha_quote = _quote('2', 'sn7', 'tao', bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address)
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=10**13)
    quote_memo.accounts(RecordingClient([(MINER, alpha_quote)], [(MINER, bond)]))
    held = (time.time(), {HOTKEY: _state(collateral=0, active_backings=0b11)})
    validator = _validator(StrictClient(), held, **config)
    validator.axon_assets = {'sn7': alpha}
    return validator, alpha


def test_rate_quote_prices_alpha_legs_off_the_shared_memo(quote_memo, monkeypatch):
    validator, alpha = _alpha_validator(quote_memo, monkeypatch)
    for _ in range(3):
        rq = rate_quote(validator, 'sol', 'sn7', SOL)
    assert rq.quote.collateral_amount == 500_000_000 and alpha.reads == [None]  # three quotes, one price read


def test_rate_quote_floors_an_alpha_pair_in_source_units(quote_memo, monkeypatch):
    # The TAO floor binds the declared sn7 leg: 0.1 TAO at 0.25 TAO per sn7 is 0.4 sn7, 0.2 SOL at 2 sn7 per SOL.
    validator, _ = _alpha_validator(
        quote_memo, monkeypatch, tao_min_swap_amount=SOL // 10, tao_max_swap_amount=10 * SOL
    )
    floor = rate_quote(validator, 'sol', 'sn7', SOL).min_from_amount
    assert floor == 2 * SOL // 10
    assert rate_quote(validator, 'sol', 'sn7', floor).quote is not None
    assert 'below min swap' in rate_quote(validator, 'sol', 'sn7', floor - 1).reason
