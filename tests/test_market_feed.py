"""MarketFeed: the optimizer's cache — pushes insert, update and close accounts; competitors are subscribed once;
the API seed reads quotes in the chain's orientation and never overwrites a push."""

import base64
from collections import Counter

import base58
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.constants import RATE_PRECISION
from allways.miner.market_feed import (
    MINER_QUOTE_DIRECTION_OFFSET,
    SYSTEM_PROGRAM,
    MarketFeed,
    direction_filter,
    fixed_rate,
    make_quote,
    market_from_das,
)
from allways.solana import layouts, pdas
from allways.solana.client import AllwaysSolanaClient
from allways.solana.program_feed import SubscriptionFeed

PROGRAM = Pubkey.from_bytes(bytes([7]) * 32)
ME = Keypair().pubkey()
RIVAL = Keypair().pubkey()
LANES = [('sol', 'tao', 'sol'), ('sol', 'tao', 'tao'), ('tao', 'sol', 'sol'), ('tao', 'sol', 'tao')]


class RecordingFeed(SubscriptionFeed):
    def __init__(self):
        super().__init__('ws://test', lambda key, result: None)
        self.new_keys = []

    def add(self, sub):
        added = super().add(sub)
        if added:
            self.new_keys.append(sub.key)
        return added


def market():
    return MarketFeed(
        'ws://test',
        PROGRAM,
        ME,
        LANES,
        decode=lambda n, raw: AllwaysSolanaClient._decode(None, n, raw),
        feed=RecordingFeed(),
    )


def quote_bytes(miner, lane, rate: str, updated_at=100) -> bytes:
    body = layouts.MinerQuote.build(
        {
            'miner': bytes(miner),
            'from_chain': lane[0],
            'to_chain': lane[1],
            'collateral_chain': lane[2],
            'miner_from_addr': 'from',
            'miner_to_addr': 'to',
            'rate': fixed_rate(rate),
            'liquidity': 0,
            'updated_at': updated_at,
            'bump': 255,
        }
    )
    return layouts.DISCRIMINATORS['MinerQuote'] + body


def state_bytes(miner, collateral=10**9) -> bytes:
    body = layouts.MinerState.build(
        {
            'miner': bytes(miner),
            'collateral': collateral,
            'active': True,
            'active_backings': 1,
            'has_active_swap': False,
            'active_swap_backings': 0,
            'busy_until': [0] * 8,
            'settling_until': [0] * 8,
            'reserved_collateral': [0] * 8,
            'deactivation_at': 0,
            'successful_swaps': 3,
            'failed_swaps': 1,
            'bump': 255,
        }
    )
    return layouts.DISCRIMINATORS['MinerState'] + body


def program_push(pubkey, raw: bytes, slot: int) -> dict:
    account = {'data': [base64.b64encode(raw).decode(), 'base64'], 'lamports': 2_000_000, 'owner': str(PROGRAM)}
    return {'context': {'slot': slot}, 'value': {'pubkey': str(pubkey), 'account': account}}


def account_push(raw, slot: int, lamports=2_000_000, owner=None) -> dict:
    data = base64.b64encode(raw or b'').decode()
    return {
        'context': {'slot': slot},
        'value': {'data': [data, 'base64'], 'lamports': lamports, 'owner': owner or str(PROGRAM)},
    }


def closure(slot: int) -> dict:
    return account_push(None, slot, lamports=0, owner=SYSTEM_PROGRAM)


def test_direction_filter_matches_the_quote_body_after_the_miner():
    discriminator, direction = direction_filter('sol', 'tao')
    raw = quote_bytes(RIVAL, ('sol', 'tao', 'tao'), '0.45')
    assert MINER_QUOTE_DIRECTION_OFFSET == 40 and direction['memcmp']['offset'] == 40
    wanted = base58.b58decode(direction['memcmp']['bytes'])
    assert wanted == b'\x03\x00\x00\x00sol\x03\x00\x00\x00tao'
    assert raw[40 : 40 + len(wanted)] == wanted
    assert base58.b58decode(discriminator['memcmp']['bytes']) == raw[:8]
    assert raw[40 : 40 + len(wanted)] != base58.b58decode(direction_filter('tao', 'sol')[1]['memcmp']['bytes'])


def test_base_subscriptions_cover_both_directions_config_and_our_own_accounts():
    feed = market()
    keys = set(feed.feed.keys())
    assert {'quotes:sol:tao', 'quotes:tao:sol'} <= keys
    assert f'account:{pdas.config_pda(PROGRAM)}' in keys
    assert f'account:{pdas.miner_state_pda(ME, PROGRAM)}' in keys
    assert f'account:{pdas.bond_attestation_pda(ME, "tao", PROGRAM)}' in keys


def test_a_quote_push_inserts_and_updates_and_a_closure_removes_it():
    feed = market()
    lane = ('sol', 'tao', 'sol')
    pda = pdas.quote_pda(RIVAL, *lane, PROGRAM)
    feed.on_push('quotes:sol:tao', program_push(pda, quote_bytes(RIVAL, lane, '0.45', updated_at=100), slot=5))
    assert feed.quotes[str(pda)].rate == fixed_rate('0.45')
    feed.on_push('quotes:sol:tao', program_push(pda, quote_bytes(RIVAL, lane, '0.46', updated_at=200), slot=6))
    feed.on_push('quotes:sol:tao', program_push(pda, quote_bytes(RIVAL, lane, '0.40', updated_at=50), slot=4))
    assert (feed.quotes[str(pda)].rate, feed.quotes[str(pda)].updated_at) == (fixed_rate('0.46'), 200)
    feed.on_push(f'account:{pda}', closure(slot=7))  # programSubscribe is silent on a close; the account sub isn't
    assert str(pda) not in feed.quotes


def test_competitor_accounts_are_subscribed_once_as_their_quotes_appear():
    feed = market()
    base = len(feed.feed.new_keys)
    for slot, lane in enumerate(LANES * 2, start=1):
        pda = pdas.quote_pda(RIVAL, *lane, PROGRAM)
        feed.on_push(f'quotes:{lane[0]}:{lane[1]}', program_push(pda, quote_bytes(RIVAL, lane, '0.45'), slot))
    added = Counter(feed.feed.new_keys[base:])
    assert set(added.values()) == {1}
    assert set(added) == {f'account:{pdas.quote_pda(RIVAL, *lane, PROGRAM)}' for lane in LANES} | {
        f'account:{pdas.miner_state_pda(RIVAL, PROGRAM)}',
        f'account:{pdas.bond_attestation_pda(RIVAL, "tao", PROGRAM)}',
    }


def test_a_state_push_and_its_closure():
    feed = market()
    feed.watch_miner(str(RIVAL))
    state_pda = pdas.miner_state_pda(RIVAL, PROGRAM)
    feed.on_push(f'account:{state_pda}', account_push(state_bytes(RIVAL), slot=3))
    assert int(feed.states[str(RIVAL)].failed_swaps) == 1
    feed.on_push(f'account:{state_pda}', closure(slot=4))
    assert str(RIVAL) not in feed.states


def test_wallet_pushes_carry_lamports():
    feed = market()
    feed.watch_wallet(str(ME))
    feed.on_push(f'account:{ME}', account_push(None, slot=2, lamports=1_234, owner=SYSTEM_PROGRAM))
    assert feed.snapshot()[4] == {str(ME): 1_234}


# Mainnet uid 74 at 2026-09-15 ~13:45 UTC. The chain read at the same moment (getAccountInfo on the four quote PDAs)
# gave sol:tao:sol 0.45543, sol:tao:tao 0.43883, tao:sol:sol 0.45263, tao:sol:tao 0.44368 — so `rate` is the hub->spoke
# quote and `counterRate` the spoke->hub one, both in the canonical TAO-per-SOL unit — and each sol->tao quote's
# miner_from_addr was the SOL key, its miner_to_addr the TAO address. MinerState.collateral 5567788361, bond 2166611059.
UID74 = '53EpFa3pWDp7Y8CTn96ViC3pWSspoFz7vfdVM9YDaMMZ'
UID74_TAO = '5H3SymuiNjgYC8DT2287jRuF5WTfY6bSuzDPj1k4o6bfxZbP'
UID74_ROWS = [
    {
        'hotkey': '5D4oZfiUbEiHnhMvmL39hnzaFLojhsrZqhebE1mvTC4xPEbT',
        'backing': backing,
        'uid': 74,
        'solanaPubkey': UID74,
        'sourceChain': 'sol',
        'sourceAddress': UID74,
        'destChain': 'tao',
        'destAddress': UID74_TAO,
        'rateStr': rate,
        'rate': f'{rate}0000000000000',
        'counterRateStr': counter,
        'counterRate': f'{counter}0000000000000',
        'collateral': collateral,
        'isActive': True,
        'isReserved': False,
        'reservedUntil': None,
        'hasActiveSwap': False,
        'updatedAt': '2026-09-15T13:31:32.488Z',
        'fundableUpTo': '0',
    }
    for backing, rate, counter, collateral in (
        ('sol', '0.45543', '0.45263', '5567788361'),
        ('tao', '0.43883', '0.44368', '2166611059'),
    )
] + [
    dict(
        hotkey='x',
        backing='sol',
        solanaPubkey=UID74,
        sourceChain='sol',
        destChain='arbusdc',
        rate='97.3',
        counterRate='101.3',
    )
]


def test_das_rows_read_in_the_chains_orientation():
    quotes, states, bonds = market_from_das(UID74_ROWS, LANES, now=0)
    by_lane = {(q.from_chain, q.to_chain, q.collateral_chain): q for q in quotes}
    assert {lane: by_lane[lane].rate / RATE_PRECISION for lane in by_lane} == {
        ('sol', 'tao', 'sol'): 0.45543,
        ('sol', 'tao', 'tao'): 0.43883,
        ('tao', 'sol', 'sol'): 0.45263,
        ('tao', 'sol', 'tao'): 0.44368,
    }
    forward, reverse = by_lane[('sol', 'tao', 'sol')], by_lane[('tao', 'sol', 'sol')]
    assert (forward.miner_from_addr, forward.miner_to_addr) == (UID74, UID74_TAO)
    assert (reverse.miner_from_addr, reverse.miner_to_addr) == (UID74_TAO, UID74)
    state = states[UID74]
    assert (state.collateral, state.active_backings, state.failed_swaps) == (5567788361, 0b11, 0)
    assert (bonds[(UID74, 'tao')].effective_balance, bonds[(UID74, 'tao')].locked) == (2166611059, True)


def test_a_seed_never_overwrites_a_pushed_account_but_replaces_the_rest():
    feed = market()
    lane = ('sol', 'tao', 'sol')
    pushed = pdas.quote_pda(RIVAL, *lane, PROGRAM)
    stale_miner = Keypair().pubkey()
    feed.note_quote(make_quote(stale_miner, lane, 'a', 'b', fixed_rate('0.40')))
    feed.on_push('quotes:sol:tao', program_push(pushed, quote_bytes(RIVAL, lane, '0.46'), slot=8))
    feed.seed([make_quote(RIVAL, lane, 'a', 'b', fixed_rate('0.44'))], states={}, bonds={})
    quotes = feed.snapshot()[1]
    assert [q.rate for q in quotes] == [fixed_rate('0.46')]  # the push stands; the quote gone from the API is dropped
