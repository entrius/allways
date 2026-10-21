"""``/rate`` off the bound miners' states and the World's accounts: no client read, and alpha prices memoed
so one price read per TTL serves however many threads quote at once."""

import time
from decimal import Decimal
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.assets.alpha import Alpha
from allways.chains import CHAIN_SN7
from allways.constants import RATE_PRECISION
from allways.validator.serving import requests
from allways.validator.serving.market import AlphaPriceMemo
from allways.validator.serving.requests import rate_quote
from tests.validator.fakes import Ctx

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
    """Any send-side read is a test failure: the World's accounts must answer the quote alone."""

    def __getattr__(self, name):
        raise AssertionError(f'client.{name} read during a quote')


class Accounts:
    def __init__(self, quotes, attestations=(), **config):
        self.quotes, self.config = quotes, SimpleNamespace(min_swap_amount=0, max_swap_amount=0, **config)
        self.attestations = {(str(miner), att.chain): att for miner, att in attestations}

    def get_all(self, name):
        assert name == 'MinerQuote'
        return self.quotes

    def get_config(self):
        return self.config

    def get_bond_attestation(self, miner, chain='tao'):
        return self.attestations.get((str(miner), chain))


def _ctx(accounts, states, providers=None):
    ctx = Ctx(StrictClient(), states=states, providers=providers)
    ctx.accounts = lambda: accounts
    return ctx


def test_bound_states_and_the_world_quote_without_one_client_read():
    rq = rate_quote(_ctx(Accounts([(MINER, _quote('0.5'))]), {HOTKEY: _state()}), 'sol', 'btc', SOL)
    assert rq.quote.miner_hotkey == HOTKEY and rq.quote.rate_display == '0.5'


def test_a_miner_off_the_bound_states_is_not_quoted():
    rq = rate_quote(_ctx(Accounts([(MINER, _quote('0.5'))]), {}), 'sol', 'btc', SOL)
    assert rq.quote is None and rq.reason == 'no miner offers this direction'


class CountingAlpha(Alpha):
    def __init__(self):
        super().__init__(CHAIN_SN7, SimpleNamespace())
        self.reads: list = []

    def value_rao(self, amount, block=None):
        self.reads.append(block)
        time.sleep(0.02)  # long enough for every waiting thread to pile onto the lock
        return int(amount) * 250_000_000 // 10**self.chain_def.decimals


def _alpha_ctx(monkeypatch, **config):
    """One miner quoting sol→sn7 at 2 sn7 per SOL, TAO-backed: the sn7 leg is declared, so sizing it
    prices the alpha (which judges the address)."""
    alpha = CountingAlpha()
    monkeypatch.setattr(requests, '_alpha_prices', AlphaPriceMemo())
    alpha_quote = _quote('2', 'sn7', 'tao', bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address)
    bond = SimpleNamespace(miner=MINER, chain='tao', locked=True, effective_balance=10**13)
    accounts = Accounts([(MINER, alpha_quote)], [(MINER, bond)], **config)
    states = {HOTKEY: _state(collateral=0, active_backings=0b11)}
    return _ctx(accounts, states, providers={'sn7': alpha}), alpha


def test_rate_quote_floors_an_alpha_pair_in_source_units(monkeypatch):
    # The TAO floor binds the declared sn7 leg: 0.1 TAO at 0.25 TAO per sn7 is 0.4 sn7, 0.2 SOL at 2 sn7 per SOL.
    ctx, _ = _alpha_ctx(monkeypatch, tao_min_swap_amount=SOL // 10, tao_max_swap_amount=10 * SOL)
    floor = rate_quote(ctx, 'sol', 'sn7', SOL).min_from_amount
    assert floor == 2 * SOL // 10
    assert rate_quote(ctx, 'sol', 'sn7', floor).quote is not None
    assert 'below min swap' in rate_quote(ctx, 'sol', 'sn7', floor - 1).reason
