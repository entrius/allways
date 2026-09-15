"""QuoteOptimizer: crown-following quotes guarded by market tolerance, funding and churn.

Pins the rules the optimizer is trusted with: it quotes an inset inside the crown band around the best
OTHER qualifying quote (never itself, never an ineligible miner, still a busy one), leads at the least
generous tolerated rate when there is no crown worth following, never spends SOL on routine repricing,
pays a fee only when fresh data say it protects more than it costs, re-posts only what it pulled, and
tells the operator on the webhook. Its picture is the feed's cache — seeded from the API, then pushed —
so a tick makes no RPC read.
"""

import json
import threading
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.constants import RATE_PRECISION
from allways.miner.optimizer import attach_optimizer
from allways.miner.optimizer import quote_optimizer as qo
from allways.miner.optimizer.market_feed import SYSTEM_PROGRAM, MarketFeed
from allways.miner.optimizer.market_price import MarketPrices
from allways.miner.optimizer.quote_optimizer import (
    DEAD_MAN_SECS,
    OPTIMIZER_TICK_SECONDS,
    Lane,
    OptimizerConfig,
    ProgramView,
    QuoteOptimizer,
    WebhookNotifier,
    band_edge_fixed,
    best_other_rate,
    default_lanes,
    in_crown_band,
    lead_rate_fixed,
    payout_for_leg,
    pending_payouts,
)
from allways.miner.optimizer.subscription_feed import SubscriptionFeed
from allways.solana import pdas
from allways.solana.client import AllwaysSolanaClient
from allways.utils.rate import quantize_rate_fixed

SOL = 10**9
TAO = 10**9
NOW = 1_000_000
PROGRAM = Pubkey.from_bytes(bytes([7]) * 32)
ME = Keypair().pubkey()
OTHER = Keypair().pubkey()
FORWARD = Lane('sol', 'tao', 'sol')  # higher TAO-per-SOL wins
REVERSE = Lane('tao', 'sol', 'sol')  # lower TAO-per-SOL wins
PRICES = {'sol': 100.0, 'tao': 220.0}
MARKET = 100.0 / 220.0  # 0.454545 TAO per SOL
EDGE = '0.45219'  # leader 0.4540, 0.4% under
LEAD = '0.43863'  # market, 3.5% under


def fixed(rate: str) -> int:
    return int(Decimal(rate) * RATE_PRECISION)


def miner_state(miner, collateral=5 * SOL, active=0b11, failed=0, settling=0, reserved=0, busy=False):
    return SimpleNamespace(
        miner=miner,
        collateral=collateral,
        active_backings=active,
        failed_swaps=failed,
        settling_until=[settling] + [0] * 7,
        reserved_collateral=[reserved] + [0] * 7,
        busy_until=[0] * 8,
        active_swap_backings=0b01 if busy else 0,
    )


def make_quote(miner, lane, rate: str, updated_at=NOW - 3600, liquidity=0):
    return SimpleNamespace(
        miner=miner,
        from_chain=lane.from_chain,
        to_chain=lane.to_chain,
        collateral_chain=lane.backing,
        miner_from_addr=f'{lane.from_chain}-{str(miner)[:6]}',
        miner_to_addr=f'{lane.to_chain}-{str(miner)[:6]}',
        rate=fixed(rate),
        liquidity=liquidity,
        updated_at=updated_at,
    )


def das_row(miner, rate=None, counter=None, backing='sol', collateral=5 * SOL, active=True):
    """One ``GET /miners`` row: the hub (sol) is the source, ``rate`` sol->tao, ``counterRate`` tao->sol."""
    return {
        'hotkey': f'hk-{str(miner)[:6]}',
        'solanaPubkey': str(miner),
        'sourceChain': 'sol',
        'destChain': 'tao',
        'backing': backing,
        'sourceAddress': f'sol-{str(miner)[:6]}',
        'destAddress': f'tao-{str(miner)[:6]}',
        'rate': rate,
        'counterRate': counter,
        'collateral': str(collateral),
        'isActive': active,
        'isReserved': False,
        'reservedUntil': None,
        'hasActiveSwap': False,
    }


def program_config(min_swap=SOL // 10, max_swap=0, min_collateral=0):
    return SimpleNamespace(
        min_swap_amount=min_swap,
        max_swap_amount=max_swap,
        min_collateral=min_collateral,
        tao_min_swap_amount=0,
        tao_max_swap_amount=0,
        tao_min_collateral=0,
        halted=False,
    )


class FakeRpc:
    def __init__(self, reads, balance):
        self.reads, self.balance = reads, balance

    def get_balance(self, address):
        self.reads.append(('get_balance', address))
        return self.balance


class FakeClient:
    """Records transactions in ``calls`` and every read in ``reads`` — any ``get_*`` it doesn't define counts."""

    def __init__(self, quotes=(), states=(), config=None):
        self.keypair = SimpleNamespace(pubkey=lambda: ME)
        self.program_id = PROGRAM
        self.quotes = list(quotes)
        self.states = list(states)
        self.config = config or program_config()
        self.calls = []
        self.reads = []
        self.rpc = FakeRpc(self.reads, SOL)

    def __getattr__(self, name):
        if name.startswith('get_'):
            return lambda *args, **kwargs: self.reads.append((name, args))
        raise AttributeError(name)

    def get_config(self):
        self.reads.append(('get_config',))
        return self.config

    def get_miner_state(self, miner):
        self.reads.append(('get_miner_state', str(miner)))
        return next((s for s in self.states if str(s.miner) == str(miner)), None)

    def set_quote(self, from_chain, to_chain, from_addr, to_addr, rate, liquidity, backing='sol'):
        self.calls.append(('set', Lane(from_chain, to_chain, backing), rate, from_addr, to_addr))

    def remove_quote(self, from_chain, to_chain, backing='sol'):
        self.calls.append(('remove', Lane(from_chain, to_chain, backing)))


class RecordingNotifier(WebhookNotifier):
    def __init__(self):
        super().__init__('', 'test')
        self.sent = []
        self.kinds = []

    def send(self, message, kind='info'):
        self.sent.append(message)
        self.kinds.append(kind)


class FakeApi:
    def __init__(self, fills=None, live=None, healthy=True, rows=(), history=()):
        self.fills, self.live, self.healthy = fills, live, healthy
        self.rows, self.history = list(rows), list(history)
        self.health_calls = self.miners_calls = 0

    def health(self):
        self.health_calls += 1
        return self.healthy

    def miners(self):
        self.miners_calls += 1
        return list(self.rows) if self.healthy else None

    def rate_history(self, hotkey):
        return list(self.history) if self.healthy else None

    def last_fill_times(self, hotkey):
        return self.fills

    def live_lanes(self):
        return self.live


class StubFeed(SubscriptionFeed):
    """The subscription transport without a socket: ``live`` is whatever the test says."""

    def __init__(self):
        super().__init__('ws://test', lambda key, result: None)
        self.is_live = True
        self.started = False
        self.stops = 0

    @property
    def live(self):
        return self.is_live

    def start(self):
        self.started = True
        self.generation += 1  # a fresh connection: whatever was pushed meanwhile was missed, so it re-seeds
        return self

    def stop(self):
        self.started = False
        self.stops += 1
        self.down_since = time.time()  # like the real feed: pushes before a stop don't outrank the next seed


def decode(name, raw):
    return AllwaysSolanaClient._decode(None, name, raw)


def build(tmp_path, client, balances=None, lanes=(FORWARD,), prices=None, api=None, **cfg):
    """An optimizer already started and seeded: the feed's cache holds the client's quotes and states (the same
    objects, so a test can move a rival's rate), and SOL balances are pushed lamports."""
    balances = {'sol': 1 * SOL, 'tao': 10 * TAO} if balances is None else balances
    # LEAD and the tolerance cases are pinned at -3.5% / +1%, not the shipped defaults.
    cfg.setdefault('max_worse_than_market_pct', 3.5)
    cfg.setdefault('max_better_than_market_pct', 1.0)
    assets = {'tao': SimpleNamespace(get_balance=lambda _addr: balances['tao'])}
    feed = MarketFeed('ws://test', PROGRAM, ME, lanes, decode=decode, feed=StubFeed())
    feed.config = client.config
    for state in client.states:
        feed.states[str(state.miner)] = state
    for quote in client.quotes:
        feed.note_quote(quote)
    client.rpc.balance = balances['sol']
    for address in (str(ME), f'sol-{str(ME)[:6]}'):
        feed.watch_wallet(address)
        feed.lamports[address] = balances['sol']
    opt = QuoteOptimizer(
        OptimizerConfig(enabled=True, lanes=list(lanes), **cfg),
        client,
        assets,
        hotkey='5HotkeyForTests',
        state_path=tmp_path / 'state.json',
        pending_payouts_fn=lambda chain: 0,
        feed=feed,
        api=api or FakeApi(),
        clock=lambda: NOW,
        prices=prices or MarketPrices(pins=PRICES),
        notifier=RecordingNotifier(),
        paper_addresses={'sol': f'sol-{str(ME)[:6]}', 'tao': f'tao-{str(ME)[:6]}'},
    )
    opt.started, opt.seeded_generation, opt.reconciled_at = True, feed.generation, NOW
    return opt


def at(opt, now):
    opt.clock = lambda: now
    opt.tick()


def crowned_client(my_rate='0.4500', other_rate='0.4540', my_updated_at=NOW - 3600, others=(), **my_state):
    quotes = [make_quote(ME, FORWARD, my_rate, updated_at=my_updated_at)]
    states = [miner_state(ME, **my_state)]
    if other_rate is not None:
        quotes.append(make_quote(OTHER, FORWARD, other_rate))
        states.append(miner_state(OTHER))
    for miner, rate, state_kw in others:
        quotes.append(make_quote(miner, FORWARD, rate))
        states.append(miner_state(miner, **state_kw))
    return FakeClient(quotes=quotes, states=states)


def sent_with(opt, text):
    """Posted notices containing ``text``, each as one plain line (title and details, markdown dropped)."""
    return [qo.plain(m) for m in opt.notifier.sent if text.lower() in qo.plain(m).lower()]


def quote_pda(miner, lane):
    return str(pdas.quote_pda(miner, lane.from_chain, lane.to_chain, lane.backing, PROGRAM))


# ─── rates ───


def test_follow_edge_is_inset_inside_the_band_and_on_tick():
    anchor = fixed('0.4540')
    forward, reverse = band_edge_fixed(anchor, reverse=False), band_edge_fixed(anchor, reverse=True)
    assert (forward, reverse) == (fixed(EDGE), fixed('0.45581'))
    for edge, rev in ((forward, False), (reverse, True)):
        assert quantize_rate_fixed(edge) == edge
        assert in_crown_band(edge, anchor, reverse=rev)


def test_lead_rate_is_the_least_generous_tolerated_rate():
    assert lead_rate_fixed(MARKET, reverse=False, max_worse_pct=3.5) == fixed(LEAD)
    assert lead_rate_fixed(MARKET, reverse=True, max_worse_pct=3.5) == fixed('0.47104')


def test_leader_skips_self_and_ineligible_miners_but_keeps_a_busy_one():
    struck, dark, settling, thin, busy, idle = (Keypair().pubkey() for _ in range(6))
    quotes = [
        make_quote(ME, FORWARD, '0.4700'),
        make_quote(struck, FORWARD, '0.4690'),
        make_quote(dark, FORWARD, '0.4680'),
        make_quote(settling, FORWARD, '0.4670'),
        make_quote(thin, FORWARD, '0.4660'),
        make_quote(busy, FORWARD, '0.4550'),
        make_quote(idle, FORWARD, '0.4500'),
    ]
    states = [
        miner_state(ME),
        miner_state(struck, failed=3),
        miner_state(dark, active=0),
        miner_state(settling, settling=NOW + 60),
        miner_state(thin, collateral=SOL // 20),
        miner_state(busy, reserved=4 * SOL, busy=True),
        miner_state(idle),
    ]
    view = ProgramView(
        now=NOW, config=program_config(), quotes=quotes, states={str(s.miner): s for s in states}, bonds={}
    )
    assert best_other_rate(view, FORWARD, str(ME)) == fixed('0.4550')


# ─── routine repricing (always free) ───


def test_follows_the_leader_once_the_update_is_free(tmp_path):
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert client.calls == [('set', FORWARD, fixed(EDGE), f'sol-{str(ME)[:6]}', f'tao-{str(ME)[:6]}')]
    assert not sent_with(opt, 'requoted')  # routine requotes go to the miner log, not the webhook


def test_routine_requotes_and_mode_changes_are_logged_not_posted(tmp_path):
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client)
    with patch.object(qo.bt.logging, 'info') as info:
        opt.run_once(NOW)
        assert opt.modes[FORWARD] == 'follow'
        next(q for q in client.quotes if str(q.miner) == str(OTHER)).rate = fixed('0.4700')
        opt.run_once(NOW + 60)
    lines = [c.args[0] for c in info.call_args_list]
    assert any(f'Requoted SOL->TAO [sol]: 0.45 → {EDGE}' in line for line in lines)
    assert any('SOL->TAO [sol] is now not following' in line for line in lines)
    assert opt.modes[FORWARD] is None
    assert not sent_with(opt, 'requoted') and not sent_with(opt, 'is now')


def test_a_leading_quote_steps_back_to_the_edge(tmp_path):
    client = crowned_client(my_rate='0.4545')
    build(tmp_path, client).run_once(NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]


def test_holds_between_the_edge_and_the_leader(tmp_path):
    client = crowned_client(my_rate='0.4530')
    build(tmp_path, client).run_once(NOW)
    assert client.calls == []


def test_never_pays_to_reprice(tmp_path):
    client = crowned_client(my_rate='0.4500', my_updated_at=NOW - 100)
    build(tmp_path, client).run_once(NOW)
    assert client.calls == []


def test_our_own_requote_is_not_free_again_until_its_window_passes(tmp_path):
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    next(q for q in client.quotes if str(q.miner) == str(OTHER)).rate = fixed('0.4560')
    opt.run_once(NOW + 60)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]
    opt.run_once(NOW + 700)
    assert [c[:3] for c in client.calls][-1] == ('set', FORWARD, band_edge_fixed(fixed('0.4560'), reverse=False))


def test_a_routine_requote_waits_a_random_delay_past_the_free_window_and_wakes_for_it(tmp_path):
    client = crowned_client(my_rate='0.4500', my_updated_at=NOW - 600)
    opt = build(tmp_path, client)
    drawn = []
    opt.rng = SimpleNamespace(randint=lambda lo, hi: drawn.append((lo, hi)) or 20)
    opt.last_tick = NOW
    opt.run_once(NOW)
    assert client.calls == [] and opt.wake_at == NOW + 20
    at(opt, NOW + 12)  # neither a regular tick nor due yet
    assert client.calls == []
    at(opt, NOW + 24)  # due, well before the next regular tick
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]
    assert drawn == [(0, qo.REQUOTE_JITTER_SECS)]  # drawn once per quote, not every tick


def test_does_not_follow_a_crown_more_generous_than_tolerance(tmp_path):
    client = crowned_client(my_rate='0.4500', other_rate='0.4700')
    build(tmp_path, client).run_once(NOW)
    assert client.calls == []


@pytest.mark.parametrize('other_rate', [None, '0.4300'])
def test_leads_at_the_least_generous_rate_without_a_crown_worth_following(tmp_path, other_rate):
    client = crowned_client(my_rate='0.4500', other_rate=other_rate)
    build(tmp_path, client).run_once(NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(LEAD))]


def test_holds_every_quote_without_a_fresh_market_price(tmp_path):
    prices = MarketPrices()
    prices.refresh = lambda chains, now=None: None
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client, prices=prices)
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, 'no fresh market price')


def test_ticks_at_most_once_per_interval(tmp_path):
    opt = build(tmp_path, crowned_client())
    with patch.object(opt, 'step') as step:
        opt.tick()
        opt.tick()
        opt.last_tick = NOW - OPTIMIZER_TICK_SECONDS
        opt.tick()
    assert step.call_count == 2


def test_no_rpc_reads_per_tick_while_the_feed_is_live(tmp_path):
    client = crowned_client(my_rate='0.4500', others=[(Keypair().pubkey(), '0.4400', {})])
    opt = build(tmp_path, client, balances={'sol': 20 * SOL, 'tao': 10 * TAO}, lanes=(FORWARD, REVERSE))
    for minute in range(15):
        if minute == 12:
            next(q for q in client.quotes if str(q.miner) == str(OTHER)).rate = fixed('0.4560')
        at(opt, NOW + 60 * minute)
    assert client.reads == []
    assert client.calls and {c[0] for c in client.calls} <= {'set', 'remove'}


# ─── market drift (may pay) ───


def test_drift_pays_to_requote_when_we_are_first_in_line(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10)
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]
    assert sent_with(opt, 'Churn fee: paid 0.01 SOL')


def test_drift_waits_while_a_takeable_quote_is_better(tmp_path):
    rival = (Keypair().pubkey(), '0.4800', {})
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10, others=[rival])
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert client.calls == []
    assert not sent_with(opt, 'not acting yet')  # deferrals are logged, not posted


def test_drift_pays_to_pull_when_the_better_quote_is_busy_and_there_is_nothing_to_follow(tmp_path):
    rival = (Keypair().pubkey(), '0.4800', {'busy': True})
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10, others=[rival])
    build(tmp_path, client).run_once(NOW)
    assert client.calls == [('remove', FORWARD)]


def test_drift_waits_when_a_full_size_fill_would_lose_less_than_the_fee(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10, collateral=SOL // 20)
    client.config = program_config(min_swap=SOL // 100)
    build(tmp_path, client).run_once(NOW)
    assert client.calls == []


def test_drift_never_pays_on_a_busy_purse(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10, busy=True)
    build(tmp_path, client).run_once(NOW)
    assert client.calls == []


def test_a_drifted_quote_is_pulled_free_when_unfunded(tmp_path):
    client = crowned_client(my_rate='0.4700')
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2})
    opt.run_once(NOW)
    assert client.calls == [('remove', FORWARD)]  # P2 needs no second short reading


# ─── funding ───


def test_underfunded_lane_is_pulled_after_consecutive_short_readings(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2})
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, f'Wallet: tao-{str(ME)[:6]} (TAO) — Holds: 0.5000 TAO')
    opt.run_once(NOW + 60)
    assert client.calls == [('remove', FORWARD)]


def _thin_purse_client(**my_state):
    client = crowned_client(my_rate='0.4530', my_updated_at=NOW - 100, collateral=SOL // 20, **my_state)
    client.config = program_config(min_swap=SOL // 100)
    return client


def test_defers_a_funding_pull_whose_fee_exceeds_a_missed_fill(tmp_path):
    client = _thin_purse_client()
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': 1})
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    opt.run_once(NOW + 120)
    assert client.calls == []
    assert not sent_with(opt, 'not acting yet')  # deferrals are logged, not posted


def test_a_zero_balance_reading_never_pays_on_its_own(tmp_path):
    client = crowned_client(my_rate='0.4530', my_updated_at=NOW - 100)
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': 0})
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == []


def test_pays_the_fee_when_the_next_strike_ends_the_hotkey(tmp_path):
    client = _thin_purse_client(failed=2)
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': 0})
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == [('remove', FORWARD)]


def test_a_short_fee_payer_fails_every_lane(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, balances={'sol': SOL // 100, 'tao': 10 * TAO})
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == [('remove', FORWARD)]
    assert sent_with(opt, 'fee reserve')


def test_a_busy_purse_claims_no_second_full_size_fill_but_still_preps_its_rate(tmp_path):
    client = crowned_client(my_rate='0.4500', busy=True)
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2})
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]


# ─── re-posting ───


def _pull_for_funding(tmp_path, balances):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, balances=balances)
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == [('remove', FORWARD)]
    client.calls.clear()
    return client, opt


def test_reposts_a_pulled_lane_once_funded_with_the_buffer(tmp_path):
    balances = {'sol': SOL, 'tao': TAO // 2}
    client, opt = _pull_for_funding(tmp_path, balances)
    opt.run_once(NOW + 120)
    assert client.calls == []
    balances['tao'] = 10 * TAO
    opt.run_once(NOW + 180)
    assert client.calls == [('set', FORWARD, fixed(EDGE), f'sol-{str(ME)[:6]}', f'tao-{str(ME)[:6]}')]
    assert sent_with(opt, f'posted SOL->TAO [sol] at {EDGE}')
    assert opt.state.pulled(FORWARD) is None


def test_a_pulled_lane_waits_for_the_crown_back_inside_tolerance(tmp_path):
    balances = {'sol': SOL, 'tao': TAO // 2}
    client, opt = _pull_for_funding(tmp_path, balances)
    balances['tao'] = 10 * TAO
    other = next(q for q in client.quotes if str(q.miner) == str(OTHER))
    other.rate = fixed('0.4700')
    opt.run_once(NOW + 120)
    assert client.calls == []
    other.rate = fixed('0.4540')
    opt.run_once(NOW + 180)
    assert [c[0] for c in client.calls] == ['set']


def test_a_quote_the_operator_removed_is_not_reposted(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    opt.feed.on_push(
        f'account:{quote_pda(ME, FORWARD)}', {'context': {'slot': 9}, 'value': {'lamports': 0, 'owner': SYSTEM_PROGRAM}}
    )
    opt.run_once(NOW + 60)
    opt.run_once(NOW + 120)
    assert client.calls == []
    assert sent_with(opt, 'removed outside the optimizer')


def test_dry_run_sends_no_transactions_but_says_what_it_would_do(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10)
    opt = build(tmp_path, client, dry_run=True)
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, 'would requote SOL->TAO [sol]: 0.47')

    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client, dry_run=True)
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == []
    assert not sent_with(opt, 'would requote')  # a routine dry-run requote is logged, not posted


def test_a_dry_run_paper_trades_a_lane_from_nothing(tmp_path):
    client = FakeClient(quotes=[make_quote(OTHER, FORWARD, '0.4540')], states=[miner_state(ME), miner_state(OTHER)])
    opt = build(tmp_path, client, dry_run=True)
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, f'would post SOL->TAO [sol] at {EDGE}')
    assert opt.paper[FORWARD].rate == fixed(EDGE) and opt.state.pulled(FORWARD) is None
    next(q for q in client.quotes if str(q.miner) == str(OTHER)).rate = fixed('0.4560')
    opt.run_once(NOW + 60)
    assert opt.paper[FORWARD].rate == fixed(EDGE)  # a fresh paper quote waits for its free window, like a real one
    opt.run_once(NOW + 600 + qo.REQUOTE_JITTER_SECS)
    assert opt.paper[FORWARD].rate == band_edge_fixed(fixed('0.4560'), reverse=False)
    assert client.calls == []


def test_shutdown_pulls_managed_quotes_and_marks_them_for_restart(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    opt.shutdown('test')
    assert client.calls == [('remove', FORWARD)]
    assert opt.state.pulled(FORWARD) is not None
    assert sent_with(opt, 'Quote optimizer stopped — Reason: test — Pulled: SOL->TAO [sol]')


# ─── seeding and the feed ───


def test_api_down_at_start_keeps_the_optimizer_off_and_retries_each_tick(tmp_path):
    api = FakeApi(healthy=False, rows=[das_row(ME, rate='0.4530'), das_row(OTHER, rate='0.4540')])
    opt = build(tmp_path, crowned_client(), api=api)
    opt.started = False
    opt.start()
    at(opt, NOW + 30)
    at(opt, NOW + 90)
    assert (opt.started, opt.feed.feed.started, api.health_calls) == (False, False, 2)
    assert len(sent_with(opt, 'waiting for the allways API')) == 1
    assert not sent_with(opt, 'quote optimizer running')
    api.healthy = True
    at(opt, NOW + 150)
    assert opt.started and opt.feed.feed.started
    assert sent_with(opt, 'quote optimizer running')


def test_seed_reads_own_updated_at_from_the_rate_history(tmp_path):
    rows = [das_row(ME, rate='0.4500'), das_row(OTHER, rate='0.4540')]
    history = [{'t': NOW - 3600, 'rate': 0.45, 'fromChain': 'sol', 'toChain': 'tao', 'backing': 'sol'}]
    client = FakeClient(states=[miner_state(ME)])
    opt = build(tmp_path, client, api=FakeApi(rows=rows, history=history))
    assert opt.seed(NOW)
    [mine] = [q for q in opt.feed.snapshot()[1] if str(q.miner) == str(ME)]
    assert (mine.rate, mine.updated_at, mine.miner_from_addr) == (fixed('0.45'), NOW - 3600, f'sol-{str(ME)[:6]}')
    opt.run_once(NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]


def test_seed_reads_our_own_bond_which_the_api_drops_with_our_last_quote(tmp_path):
    client = FakeClient(states=[miner_state(ME)])
    client.get_bond_attestation = lambda miner, chain='tao': SimpleNamespace(
        miner=ME, chain=chain, effective_balance=11 * TAO // 10, locked=True
    )
    opt = build(tmp_path, client, lanes=(Lane('sol', 'tao', 'tao'),), api=FakeApi(rows=[], history=[]))
    assert opt.seed(NOW)
    assert opt.read_view(NOW).purse(str(ME), 'tao') == 11 * TAO // 10


def test_an_unknown_own_updated_at_is_assumed_now_so_it_is_not_free(tmp_path):
    rows = [das_row(ME, rate='0.4500'), das_row(OTHER, rate='0.4540')]
    client = FakeClient(states=[miner_state(ME)])
    opt = build(tmp_path, client, api=FakeApi(rows=rows, history=[]))
    assert opt.seed(NOW)
    [mine] = [q for q in opt.feed.snapshot()[1] if str(q.miner) == str(ME)]
    assert mine.updated_at == NOW
    opt.run_once(NOW + 60)
    assert client.calls == []  # wants the edge, but the update is not known to be free yet
    opt.run_once(NOW + 600 + qo.REQUOTE_JITTER_SECS)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]


def test_a_removal_in_the_rate_history_beats_a_lagging_miners_row(tmp_path):
    rows = [das_row(ME, rate='0.4500'), das_row(OTHER, rate='0.4540')]
    history = [
        {'t': NOW - 3600, 'rate': 0.45, 'fromChain': 'sol', 'toChain': 'tao', 'backing': 'sol'},
        {'t': NOW - 60, 'rate': 0, 'fromChain': 'sol', 'toChain': 'tao', 'backing': 'sol'},
    ]
    opt = build(tmp_path, FakeClient(states=[miner_state(ME)]), api=FakeApi(rows=rows, history=history))
    assert opt.seed(NOW)
    assert not [q for q in opt.feed.snapshot()[1] if str(q.miner) == str(ME)]


def test_a_reconnect_the_api_cannot_seed_holds_all_but_funding_pulls(tmp_path):
    client = crowned_client(my_rate='0.4500')
    api = FakeApi(healthy=False)
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2}, api=api)
    opt.feed.feed.generation += 1  # the feed reconnected
    at(opt, NOW)
    assert client.calls == []  # no routine requote on a picture the API couldn't refresh
    at(opt, NOW + 60)
    assert client.calls == [('remove', FORWARD)]  # the second short reading still pulls
    assert api.miners_calls == 2


def test_dead_man_pulls_every_managed_quote_once_then_reseeds_and_reposts(tmp_path):
    client = FakeClient(
        quotes=[
            make_quote(ME, FORWARD, '0.4530'),
            make_quote(ME, REVERSE, '0.4700'),
            make_quote(OTHER, FORWARD, '0.4540'),
        ],
        states=[miner_state(ME), miner_state(OTHER)],
    )
    api = FakeApi(rows=[das_row(OTHER, rate='0.4540')])
    opt = build(tmp_path, client, balances={'sol': 20 * SOL, 'tao': 10 * TAO}, lanes=(FORWARD, REVERSE), api=api)
    stub = opt.feed.feed
    stub.is_live = False
    at(opt, NOW)
    at(opt, NOW + DEAD_MAN_SECS)
    assert client.calls == []
    at(opt, NOW + DEAD_MAN_SECS + 1)
    assert client.calls == [('remove', FORWARD), ('remove', REVERSE)]
    [alert] = sent_with(opt, 'dead-man switch')
    assert 'Pulled: SOL->TAO [sol], TAO->SOL [sol]' in alert
    at(opt, NOW + 200)
    at(opt, NOW + 260)
    assert len(client.calls) == 2 and len(sent_with(opt, 'dead-man switch')) == 1

    stub.is_live, stub.generation = True, stub.generation + 1
    api.history = [
        {'t': NOW + 121, 'rate': 0, 'fromChain': lane.from_chain, 'toChain': lane.to_chain, 'backing': 'sol'}
        for lane in (FORWARD, REVERSE)
    ]
    at(opt, NOW + 330)
    assert api.miners_calls == 1
    assert [c[:3] for c in client.calls[2:]] == [('set', FORWARD, fixed(EDGE)), ('set', REVERSE, fixed('0.47104'))]
    assert sent_with(opt, 'optimizer feed recovered')


def test_goes_idle_when_nothing_is_managed_and_wakes_when_the_api_shows_our_quote(tmp_path):
    client = crowned_client(my_rate='0.4530')
    api = FakeApi(rows=[das_row(OTHER, rate='0.4540')])
    opt = build(tmp_path, client, api=api)
    stub = opt.feed.feed
    at(opt, NOW)
    assert opt.state.live(FORWARD) and not opt.idle
    opt.feed.on_push(
        f'account:{quote_pda(ME, FORWARD)}', {'context': {'slot': 9}, 'value': {'lamports': 0, 'owner': SYSTEM_PROGRAM}}
    )
    at(opt, NOW + 60)  # the operator removed it: nothing standing, nothing to re-post
    assert opt.idle and stub.stops == 1
    calls = api.miners_calls
    at(opt, NOW + 120)
    assert api.miners_calls == calls  # idle checks wait IDLE_CHECK_SECS
    at(opt, NOW + 60 + qo.IDLE_CHECK_SECS)
    assert opt.idle and api.miners_calls == calls + 1  # still nothing of ours on the API
    api.rows.append(das_row(ME, rate='0.4530'))
    at(opt, NOW + 60 + 2 * qo.IDLE_CHECK_SECS)
    assert not opt.idle and stub.started
    assert [q for q in opt.feed.snapshot()[1] if str(q.miner) == str(ME)]  # re-seeded with the new quote
    assert client.calls == []


def test_stays_awake_while_a_pulled_quote_waits_for_funding(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2})
    at(opt, NOW)
    at(opt, NOW + 60)
    assert client.calls == [('remove', FORWARD)]
    at(opt, NOW + 120)
    assert not opt.idle and opt.feed.feed.stops == 0


def test_the_dead_man_switch_sleeps_while_idle(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    opt.idle, opt.idle_checked_at = True, NOW
    opt.feed.feed.is_live = False
    for t in (NOW, NOW + DEAD_MAN_SECS + 1, NOW + 2 * DEAD_MAN_SECS):
        at(opt, t)
    assert client.calls == [] and not sent_with(opt, 'dead-man switch')


def test_a_competitor_quote_the_api_no_longer_lists_stops_being_followed(tmp_path):
    client = crowned_client(my_rate='0.4500', other_rate='0.4540')
    opt = build(tmp_path, client, api=FakeApi(rows=[]))
    opt.reconciled_at = NOW - qo.RECONCILE_SECS
    at(opt, NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(LEAD))]  # its leader closed: lead instead


def test_usage_line_is_logged_hourly_with_bytes_connections_and_rpc_calls(tmp_path):
    opt = build(tmp_path, crowned_client(my_rate='0.4530'))
    stub = opt.feed.feed
    with patch.object(qo.bt.logging, 'info') as info:
        opt.log_usage(NOW)
        stub.bytes_received, stub.connections_opened, opt.rpc_calls = 120_000, 12, 2
        opt.log_usage(NOW + 1800)
        opt.log_usage(NOW + 3600)
    [line] = [c.args[0] for c in info.call_args_list]
    assert '0.120 MB websocket received, 12 connection(s) opened, 2 RPC call(s) — about 17 Helius credits' in line


# ─── operator alerts ───


def test_says_why_a_posted_quote_is_not_earning(tmp_path):
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client, api=FakeApi(fills={}, live={tuple(FORWARD): False}))
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert not sent_with(opt, 'not earning emissions')
    opt.run_once(NOW + 180)
    [alert] = sent_with(opt, 'SOL->TAO [sol] is live but not earning emissions')
    assert 'no completed swap on the sol purse in the last 12h; validators credit crown' in alert
    assert 'pool pays 0' in alert


def test_no_eligibility_alert_for_an_eligible_quote_or_one_never_posted(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, api=FakeApi(fills={}, live={}))
    opt.run_once(NOW + 180)
    assert not sent_with(opt, 'not earning')
    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client, api=FakeApi(fills={'sol': NOW - 60}, live={tuple(FORWARD): True}))
    opt.run_once(NOW)
    opt.run_once(NOW + 180)
    assert not sent_with(opt, 'not earning')


def test_collateral_alert_only_under_the_eligibility_floor_with_the_command(tmp_path):
    client = crowned_client(my_rate='0.4530', collateral=SOL)
    client.config = program_config(max_swap=5 * SOL, min_collateral=SOL)
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert not sent_with(opt, 'purse')  # below full capacity is not worth a ping

    client = crowned_client(my_rate='0.4530', collateral=SOL // 2)
    client.config = program_config(min_collateral=SOL)
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    [alert] = sent_with(opt, 'eligibility floor')
    assert 'Needs: 1.00 SOL' in alert and 'alw collateral deposit --amount 0.50' in alert


def test_webhook_posts_a_condition_once_and_its_recovery_once():
    notifier = WebhookNotifier('https://hook.example', 'miner')
    with patch.object(qo.requests, 'post') as post:
        notifier.alert('funds', 'wallet short')
        notifier.alert('funds', 'wallet short')
        notifier.resolve('funds', 'wallet ok')
        notifier.resolve('funds', 'wallet ok')
        notifier.event('started')
    assert post.call_count == 3
    assert post.call_args_list[0].kwargs['json']['content'] == '**wallet short**\n_miner_'


def test_discord_webhooks_get_a_colored_embed():
    notifier = WebhookNotifier('https://discord.com/api/webhooks/1/abc', 'allways miner 5EvkLKgQ · dry run')
    with patch.object(qo.requests, 'post') as post:
        notifier.alert('funds', 'Wallet short for SOL->TAO [sol]\n**Holds:** 0.5000 TAO')
        notifier.event('Quote optimizer stopped\n**Reason:** test', 'stopped')
    first, second = (call.kwargs['json'] for call in post.call_args_list)
    [embed] = first['embeds']
    assert (embed['title'], embed['description']) == ('Wallet short for SOL->TAO [sol]', '**Holds:** 0.5000 TAO')
    assert embed['color'] == qo.NOTICE_COLORS['warning'] and embed['footer'] == {
        'text': 'allways miner 5EvkLKgQ · dry run'
    }
    assert first['allowed_mentions'] == {'parse': []} and 'content' not in first
    assert second['embeds'][0]['color'] == qo.NOTICE_COLORS['stopped']


def test_a_standing_alert_does_not_repost_as_the_market_moves(tmp_path):
    prices = MarketPrices(pins=dict(PRICES))
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10)
    opt = build(tmp_path, client, prices=prices, dry_run=True)
    opt.run_once(NOW)
    prices.pins['sol'] = 100.4
    opt.run_once(NOW + 60)
    assert len(sent_with(opt, 'would requote')) == 1


def test_a_funding_alert_posts_again_only_when_the_shortfall_really_changes(tmp_path):
    balances = {'sol': SOL, 'tao': TAO // 2}
    client = crowned_client(my_rate='0.4530', my_updated_at=NOW - 100)
    opt = build(tmp_path, client, balances=balances, dry_run=True)

    def funds_alerts():
        return [m for m in opt.notifier.sent if m.startswith('Wallet short for SOL->TAO [sol]')]

    opt.run_once(NOW)
    for minute, fees in enumerate((1_000_000, 2_000_000, 3_000_000), start=1):
        balances['tao'] = TAO // 2 - fees  # the wallet drifting by transaction fees
        opt.run_once(NOW + 60 * minute)
    assert len(funds_alerts()) == 1  # the standing quote's shortfall, once
    assert len(sent_with(opt, 'stays pulled: wallet short')) == 1  # then the paper-pulled lane's, once
    assert len(sent_with(opt, 'would pull SOL->TAO [sol]')) == 1  # the dry-run pull says so once, not every tick
    balances['tao'] = TAO // 10  # a real change in what is owed
    opt.run_once(NOW + 300)
    assert len(sent_with(opt, 'stays pulled: wallet short')) == 2


# ─── config and helpers ───


def test_config_defaults_to_disabled_and_validates(tmp_path):
    cfg = OptimizerConfig.load(tmp_path / 'missing.json')
    assert cfg.enabled is False
    assert (cfg.max_better_than_market_pct, cfg.max_worse_than_market_pct) == (2.0, 1.0)
    assert set(default_lanes()) == {
        Lane('sol', 'tao', 'sol'),
        Lane('sol', 'tao', 'tao'),
        Lane('tao', 'sol', 'sol'),
        Lane('tao', 'sol', 'tao'),
    }

    path = tmp_path / 'optimizer.json'
    path.write_text(json.dumps({'enabled': True, 'lanes': ['tao:sol:tao'], 'webhook_url': 'https://x'}))
    cfg = OptimizerConfig.load(path)
    assert cfg.enabled and cfg.lanes == [Lane('tao', 'sol', 'tao')]

    for bad in ({'enabled': True, 'crown_band': 0.01}, {'lanes': ['sol:btc:sol']}, {'lanes': ['sol:tao:btc']}):
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError):
            OptimizerConfig.load(path)


def test_churn_fee_tiers_mirror_the_cli_copy():
    from allways.cli.swap_commands.helpers import QUOTE_UPDATE_FEE_TIERS, quote_update_fee_lamports

    assert qo.QUOTE_UPDATE_FEE_TIERS == QUOTE_UPDATE_FEE_TIERS
    elapsed = (0, 299, 300, 599, 600, 3600)
    assert [qo.quote_update_fee_lamports(s) for s in elapsed] == [quote_update_fee_lamports(s) for s in elapsed]


def test_an_unreadable_balance_is_unknown_not_zero(tmp_path):
    opt = build(tmp_path, crowned_client())
    opt.assets['tao'] = SimpleNamespace(get_balance=lambda _addr: (_ for _ in ()).throw(RuntimeError('recv collision')))
    assert opt.balance(opt.read_view(NOW), 'tao', 'tao-wallet') is None
    opt.assets['tao'] = SimpleNamespace(get_balance=lambda _addr: None)
    assert opt.balance(opt.read_view(NOW), 'tao', 'tao-wallet') is None


# ─── attaching to the base miner ───


def test_attach_does_nothing_unless_enabled_and_every_chain_has_a_provider(tmp_path):
    assert attach_optimizer(SimpleNamespace(assets={'sol': object(), 'tao': object()}), tmp_path / 'none.json') is None
    path = tmp_path / 'optimizer.json'
    path.write_text(json.dumps({'enabled': False}))
    assert attach_optimizer(SimpleNamespace(assets={}), path) is None
    path.write_text(json.dumps({'enabled': True}))
    assert attach_optimizer(SimpleNamespace(assets={'sol': object()}), path) is None  # no tao provider: not started


def test_runs_on_its_own_thread_and_shutdown_stops_it_before_pulling(tmp_path, monkeypatch):
    monkeypatch.setattr(qo, 'OPTIMIZER_LOOP_SECS', 0.01)
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    threads = []
    opt.tick = lambda: threads.append(threading.current_thread().name)
    opt.start_thread()
    deadline = time.monotonic() + 2
    while len(threads) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    opt.shutdown('test')
    assert set(threads) == {'quote-optimizer'} and len(threads) >= 2
    assert not opt.thread.is_alive()
    assert client.calls == [('remove', FORWARD)]
    assert opt.feed.feed.stops == 1


def test_pending_payouts_skip_sent_swaps_and_other_chains():
    swaps = [
        SimpleNamespace(to_chain='tao', to_amount=1_000, key_hex='a'),
        SimpleNamespace(to_chain='tao', to_amount=2_000, key_hex='b'),
        SimpleNamespace(to_chain='sol', to_amount=5_000, key_hex='c'),
    ]
    assert pending_payouts(swaps, {'b': object()}, 'tao') == 990


def test_payout_reads_the_backing_leg():
    assert payout_for_leg(FORWARD, fixed('0.45'), SOL) == 445_500_000  # SOL leg -> TAO payout, less 1%
    assert payout_for_leg(REVERSE, fixed('0.45'), SOL) == 990_000_000  # the SOL leg IS the payout
