"""QuoteOptimizer: crown-following quotes guarded by market tolerance, funding and churn.

Pins the rules the optimizer is trusted with: it quotes an inset inside the crown band around the best
OTHER qualifying quote (never itself, never an ineligible miner, still a busy one), leads at the least
generous tolerated rate when there is no crown worth following, never spends SOL on routine repricing,
pays a fee only when fresh data say it protects more than it costs, re-posts only what it pulled, and
tells the operator on the webhook. Its picture is the feed's cache — seeded from the API, then pushed —
so a tick makes no RPC read.
"""

import json
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.constants import RATE_PRECISION
from allways.miner.optimizer import attach_optimizer
from allways.miner.optimizer import quote_optimizer as qo
from allways.miner.optimizer.market_feed import SYSTEM_PROGRAM, MarketFeed, direction_filter, market_from_das
from allways.miner.optimizer.market_price import MarketPrices
from allways.miner.optimizer.quote_optimizer import (
    DEAD_MAN_SECS,
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
from allways.solana import layouts, pdas
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
    """``chain`` is the quotes a ``getProgramAccounts`` read returns; None (the default) fails the read."""

    def __init__(self, reads, balance):
        self.reads, self.balance = reads, balance
        self.chain = None

    def get_balance(self, address):
        self.reads.append(('get_balance', address))
        return self.balance

    def get_slot(self):
        self.reads.append(('get_slot',))
        return 1_000

    def get_program_accounts(self, program_id, disc8=None, extra_filters=None):
        self.reads.append(('get_program_accounts', extra_filters))
        if self.chain is None:
            raise ConnectionError('no chain read in this test')
        return [
            (quote_pda(q.miner, Lane(q.from_chain, q.to_chain, q.collateral_chain)), quote_account(q))
            for q in self.chain
            if extra_filters == direction_filter(q.from_chain, q.to_chain)
        ]


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


def quote_account(q) -> bytes:
    fields = ('from_chain', 'to_chain', 'collateral_chain', 'miner_from_addr', 'miner_to_addr', 'rate', 'liquidity')
    body = {name: getattr(q, name) for name in fields}
    body.update(miner=bytes(q.miner), updated_at=q.updated_at, bump=255)
    return layouts.DISCRIMINATORS['MinerQuote'] + layouts.MinerQuote.build(body)


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


def test_a_push_lost_with_the_connection_is_read_from_the_chain_on_the_reseed(tmp_path):
    # The leader moved, the feed died with that push, and the API seed still has the old rate: the reseed must read
    # the chain or the quote sits outside the crown band.
    history = [{'t': NOW - 3600, 'rate': float(EDGE), 'fromChain': 'sol', 'toChain': 'tao', 'backing': 'sol'}]
    api = FakeApi(rows=[das_row(ME, rate=EDGE), das_row(OTHER, rate='0.4540')], history=history)
    client = crowned_client(my_rate=EDGE)
    opt = build(tmp_path, client, api=api)
    client.rpc.chain = [make_quote(OTHER, FORWARD, '0.4560')]
    opt.feed.feed.generation += 1  # the feed reconnected
    at(opt, NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, band_edge_fixed(fixed('0.4560'), False))]


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


def test_a_competitor_quote_the_api_no_longer_lists_stops_being_followed(tmp_path):
    client = crowned_client(my_rate='0.4500', other_rate='0.4540')
    opt = build(tmp_path, client, api=FakeApi(rows=[]))
    opt.reconciled_at = NOW - qo.RECONCILE_SECS
    at(opt, NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(LEAD))]  # its leader closed: lead instead


# ─── operator alerts ───


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
    assert cfg.chain_read_min_secs == 600
    path.write_text(
        json.dumps({'enabled': True, 'lanes': ['tao:sol:tao'], 'webhook_url': 'https://x', 'chain_read_min_secs': 1800})
    )
    cfg = OptimizerConfig.load(path)
    assert cfg.enabled and cfg.lanes == [Lane('tao', 'sol', 'tao')] and cfg.chain_read_min_secs == 1800

    for bad in (
        {'enabled': True, 'crown_band': 0.01},
        {'lanes': ['sol:eth:sol']},  # not an optimizer chain
        {'lanes': ['sol:tao:btc']},  # btc can't back a quote
        {'lanes': ['tao:btc:sol']},  # nor can a hub that isn't on the pair
        {'chain_read_min_secs': -1},
        {'btc_fee_reserve': -0.1},
        {'lanes': ['tao:btc:tao'], 'lane_tolerance': {'btc:tao:tao': {'max_worse_than_market_pct': 2}}},  # not managed
        {'lanes': ['btc:tao:tao'], 'lane_tolerance': {'btc:tao:tao': {'crown_band': 1}}},
        {'lanes': ['btc:tao:tao'], 'lane_tolerance': {'btc:tao:tao': {'max_better_than_market_pct': -1}}},
        {'lanes': ['btc:tao:tao'], 'lane_tolerance': ['btc:tao:tao']},
    ):
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
    assert payout_for_leg(TAO_BTC, fixed('0.0022'), TAO) == 217_800  # 1 TAO -> 0.0022 BTC in sats, less 1%
    assert payout_for_leg(BTC_TAO, fixed('0.0022'), TAO) == 990_000_000  # the TAO leg IS the payout


# ─── BTC lanes (a spoke, backed by the TAO purse) ───


BTC = 10**8
TAO_BTC = Lane('tao', 'btc', 'tao')  # higher BTC-per-TAO wins
BTC_TAO = Lane('btc', 'tao', 'tao')  # lower BTC-per-TAO wins
BTC_PRICES = {**PRICES, 'btc': 100_000.0}  # 0.0022 BTC per TAO
FULL_BOND = 2_200_000_000  # backs a 2 TAO fill at the 1.10× reserve


def btc_optimizer(tmp_path, lanes=(TAO_BTC,), my_rate='0.0021912', other_rate='0.0022', btc_balance=BTC, **cfg):
    """Our quote on the first lane plus a rival's, both TAO purses holding a full bond."""
    quotes = [make_quote(ME, lanes[0], my_rate)]
    if other_rate is not None:
        quotes.append(make_quote(OTHER, lanes[0], other_rate))
    client = FakeClient(quotes=quotes, states=[miner_state(ME), miner_state(OTHER)])
    balances = {'sol': 1 * SOL, 'tao': 10 * TAO}
    opt = build(tmp_path, client, balances, lanes=lanes, prices=MarketPrices(pins=BTC_PRICES), **cfg)
    opt.assets['btc'] = SimpleNamespace(get_balance=lambda _addr: btc_balance)
    for miner in (ME, OTHER):
        opt.feed.bonds[(str(miner), 'tao')] = SimpleNamespace(
            miner=miner, chain='tao', effective_balance=FULL_BOND, locked=True
        )
    return opt, client


def test_a_btc_wallet_keeps_back_the_payouts_network_fee(tmp_path):
    payout = payout_for_leg(TAO_BTC, fixed('0.0021912'), 2 * TAO)  # a full 2 TAO fill
    reserve = 20_000  # the 0.0002 BTC default
    opt, client = btc_optimizer(tmp_path, btc_balance=payout + reserve)
    for minute in range(3):
        opt.run_once(NOW + 60 * minute)
    assert client.calls == []  # covers the payout and its fee: nothing to do

    opt, client = btc_optimizer(tmp_path, btc_balance=payout + reserve - 1)
    for minute in range(qo.FUNDING_SHORT_CONFIRM_TICKS):
        opt.run_once(NOW + 60 * minute)
    assert client.calls == [('remove', TAO_BTC)]
    (alert,) = sent_with(opt, 'Wallet short for TAO->BTC [tao]')
    assert f'Needs: {qo.format_amount(payout + reserve, "btc")}' in alert


def test_a_btc_lane_shares_the_tao_wallet_with_the_sol_lanes_in_lanes_order(tmp_path):
    sol_tao = Lane('sol', 'tao', 'tao')
    opt, _client = btc_optimizer(tmp_path, lanes=(sol_tao, BTC_TAO), my_rate='0.4500', other_rate=None)
    opt.feed.note_quote(make_quote(ME, BTC_TAO, '0.0022088'))
    opt.assets['tao'] = SimpleNamespace(get_balance=lambda _addr: 3 * TAO)  # one full 1.98 TAO payout, not two
    view = opt.read_view(NOW)
    mine = {Lane(q.from_chain, q.to_chain, q.collateral_chain): q for q in view.quotes if str(q.miner) == str(ME)}
    targets = {lane: opt.target_for(view, lane, NOW) for lane in opt.cfg.lanes}
    funding = opt.plan_funding(view, mine, targets)
    assert funding[sol_tao].fits and not funding[BTC_TAO].fits  # the earlier lane keeps the wallet
    assert funding[BTC_TAO].need == 2 * payout_for_leg(BTC_TAO, fixed('0.0022088'), 2 * TAO)


def test_lane_tolerance_overrides_the_band_on_one_lane_only(tmp_path):
    opt, _client = btc_optimizer(
        tmp_path,
        lanes=(BTC_TAO, TAO_BTC),
        other_rate=None,
        lane_tolerance={'btc:tao:tao': {'max_worse_than_market_pct': 5.0}},
    )
    assert opt.cfg.tolerance(BTC_TAO) == (5.0, 1.0)  # better falls back to the global
    assert opt.cfg.tolerance(TAO_BTC) == (3.5, 1.0)
    view = opt.read_view(NOW)
    btc_tao, tao_btc = opt.target_for(view, BTC_TAO, NOW), opt.target_for(view, TAO_BTC, NOW)
    assert (btc_tao.mode, btc_tao.rate_fixed) == ('lead', lead_rate_fixed(0.0022, reverse=True, max_worse_pct=5.0))
    assert (tao_btc.mode, tao_btc.rate_fixed) == ('lead', lead_rate_fixed(0.0022, reverse=False, max_worse_pct=3.5))
    assert 'leading at -5%' in btc_tao.reason


def test_api_rows_for_a_btc_pair_read_the_tao_hub_as_the_source():
    row = {
        **das_row(OTHER, rate='0.0022', counter='0.00225', backing='tao', collateral=FULL_BOND),
        'sourceChain': 'tao',
        'destChain': 'btc',
        'sourceAddress': 'tao-addr',
        'destAddress': 'btc-addr',
    }
    quotes, states, bonds = market_from_das([row], [TAO_BTC, BTC_TAO], NOW)
    got = {Lane(q.from_chain, q.to_chain, q.collateral_chain): q for q in quotes}
    assert got[TAO_BTC].rate == fixed('0.0022') and got[BTC_TAO].rate == fixed('0.00225')
    assert (got[TAO_BTC].miner_from_addr, got[TAO_BTC].miner_to_addr) == ('tao-addr', 'btc-addr')
    assert (got[BTC_TAO].miner_from_addr, got[BTC_TAO].miner_to_addr) == ('btc-addr', 'tao-addr')
    assert bonds[(str(OTHER), 'tao')].effective_balance == FULL_BOND
    assert states[str(OTHER)].active_backings & pdas.BACKING_BITS['tao']
