"""``/rate`` alpha pricing off the shared memo: one read per TTL however many threads quote at once."""

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
from allways.validator.market import (
    PRICE_TTL_SECS,
    AlphaPriceMemo,
    HeldStates,
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
    def __init__(self, quotes, attestations=(), states=()):
        self.accounts = {'MinerQuote': quotes, 'BondAttestation': list(attestations), 'MinerState': list(states)}
        self.reads: list = []

    def get_all(self, name):
        self.reads.append(name)
        return self.accounts[name]


def _validator(client, held_states, hotkeys=(HOTKEY,)):
    cfg = SimpleNamespace(min_swap_amount=0, max_swap_amount=0)
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


def test_rate_quote_prices_alpha_legs_off_the_shared_memo(quote_memo, monkeypatch):
    alpha = CountingAlpha()
    monkeypatch.setattr(reserve_engine, '_alpha_prices', AlphaPriceMemo())
    # sol→sn7 is TAO-backed: the sn7 leg is declared, so sizing it prices the alpha (which judges the address).
    alpha_quote = _quote('2', 'sn7', 'tao', bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address)
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=10**13)
    quote_memo.accounts(RecordingClient([(MINER, alpha_quote)], [(MINER, bond)]))
    held = HeldStates(time.time(), {HOTKEY: _state(collateral=0, active_backings=0b11)})
    validator = _validator(StrictClient(), held)
    validator.axon_assets = {'sn7': alpha}

    for _ in range(3):
        rq = rate_quote(validator, 'sol', 'sn7', SOL)
    assert rq.quote.collateral_amount == 500_000_000 and alpha.reads == [None]  # three quotes, one price read
