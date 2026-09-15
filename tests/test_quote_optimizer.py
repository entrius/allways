"""QuoteOptimizer: crown-following quotes guarded by market tolerance, funding and churn.

Pins the rules the optimizer is trusted with: it quotes an inset inside the crown band around the best
OTHER qualifying quote (never itself, never an ineligible miner, still a busy one), leads at the least
generous tolerated rate when there is no crown worth following, never spends SOL on routine repricing,
pays a fee only when fresh data say it protects more than it costs, re-posts only what it pulled, and
tells the operator on the webhook.
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from solders.keypair import Keypair

from allways.constants import RATE_PRECISION
from allways.miner import quote_optimizer as qo
from allways.miner.market_price import MarketPrices
from allways.miner.quote_optimizer import (
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
from allways.utils.rate import quantize_rate_fixed

SOL = 10**9
TAO = 10**9
NOW = 1_000_000
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


class FakeClient:
    def __init__(self, quotes=(), states=(), config=None):
        self.keypair = SimpleNamespace(pubkey=lambda: ME)
        self.quotes = list(quotes)
        self.states = list(states)
        self.config = config or program_config()
        self.calls = []

    def get_all(self, name):
        rows = {'MinerQuote': self.quotes, 'MinerState': self.states, 'BondAttestation': []}[name]
        return [('pda', row) for row in rows]

    def get_config(self):
        return self.config

    def get_quote(self, miner, from_chain, to_chain, backing='sol'):
        lane = Lane(from_chain, to_chain, backing)
        return next((q for q in self.quotes if str(q.miner) == str(miner) and self._lane(q) == lane), None)

    def set_quote(self, from_chain, to_chain, from_addr, to_addr, rate, liquidity, backing='sol'):
        lane = Lane(from_chain, to_chain, backing)
        self.calls.append(('set', lane, rate, from_addr, to_addr))
        existing = self.get_quote(ME, from_chain, to_chain, backing)
        if existing is None:
            existing = make_quote(ME, lane, '1')
            self.quotes.append(existing)
        existing.rate, existing.updated_at = rate, NOW

    def remove_quote(self, from_chain, to_chain, backing='sol'):
        lane = Lane(from_chain, to_chain, backing)
        self.calls.append(('remove', lane))
        self.quotes = [q for q in self.quotes if not (str(q.miner) == str(ME) and self._lane(q) == lane)]

    @staticmethod
    def _lane(q):
        return Lane(q.from_chain, q.to_chain, q.collateral_chain)


class RecordingNotifier(WebhookNotifier):
    def __init__(self):
        super().__init__('', 'test')
        self.sent = []

    def send(self, message):
        self.sent.append(message)


class FakeApi:
    def __init__(self, fills=None, live=None):
        self.fills, self.live = fills, live

    def last_fill_times(self, hotkey):
        return self.fills

    def live_lanes(self):
        return self.live


def build(tmp_path, client, balances=None, lanes=(FORWARD,), prices=None, api=None, **cfg):
    balances = {'sol': 1 * SOL, 'tao': 10 * TAO} if balances is None else balances
    assets = {
        chain: SimpleNamespace(get_balance=lambda _addr, chain=chain: balances[chain]) for chain in ('sol', 'tao')
    }
    return QuoteOptimizer(
        OptimizerConfig(enabled=True, lanes=list(lanes), **cfg),
        client,
        assets,
        hotkey='5HotkeyForTests',
        state_path=tmp_path / 'state.json',
        pending_payouts_fn=lambda chain: 0,
        api=api,
        clock=lambda: NOW,
        prices=prices or MarketPrices(pins=PRICES),
        notifier=RecordingNotifier(),
    )


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
    return [m for m in opt.notifier.sent if text in m]


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


def test_mode_changes_are_tracked_but_stay_off_the_webhook(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert opt.modes[FORWARD] == 'follow'
    next(q for q in client.quotes if str(q.miner) == str(OTHER)).rate = fixed('0.4700')
    opt.run_once(NOW + 60)
    assert opt.modes[FORWARD] is None
    assert not sent_with(opt, 'is now')


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
    with patch.object(opt, 'run_once') as run_once:
        opt.tick()
        opt.tick()
        opt.last_tick = NOW - OPTIMIZER_TICK_SECONDS
        opt.tick()
    assert run_once.call_count == 2


# ─── market drift (may pay) ───


def test_drift_pays_to_requote_when_we_are_first_in_line(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10)
    opt = build(tmp_path, client)
    opt.run_once(NOW)
    assert [c[:3] for c in client.calls] == [('set', FORWARD, fixed(EDGE))]
    assert sent_with(opt, 'paid 0.01 SOL churn fee')


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


# ─── funding ───


def test_underfunded_lane_is_pulled_after_consecutive_short_readings(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client, balances={'sol': SOL, 'tao': TAO // 2})
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, f'TAO wallet tao-{str(ME)[:6]} holds 0.5000 TAO')
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
    client.quotes = [q for q in client.quotes if str(q.miner) != str(ME)]
    opt.run_once(NOW + 60)
    opt.run_once(NOW + 120)
    assert client.calls == []
    assert sent_with(opt, 'removed outside the optimizer')


def test_dry_run_sends_no_transactions_but_says_what_it_would_do(tmp_path):
    client = crowned_client(my_rate='0.4700', my_updated_at=NOW - 10)
    opt = build(tmp_path, client, dry_run=True)
    opt.run_once(NOW)
    assert client.calls == []
    assert sent_with(opt, '[dry run] would have requoted SOL->TAO [sol]: 0.47')

    client = crowned_client(my_rate='0.4500')
    opt = build(tmp_path, client, dry_run=True)
    opt.run_once(NOW)
    opt.run_once(NOW + 60)
    assert client.calls == []
    assert not sent_with(opt, 'would have')  # a routine dry-run requote is logged, not posted


def test_shutdown_pulls_managed_quotes_and_marks_them_for_restart(tmp_path):
    client = crowned_client(my_rate='0.4530')
    opt = build(tmp_path, client)
    opt.shutdown('test')
    assert client.calls == [('remove', FORWARD)]
    assert opt.state.pulled(FORWARD) is not None
    assert sent_with(opt, 'quote optimizer stopped (test); pulled SOL->TAO [sol]')


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
    [alert] = sent_with(opt, 'needed to stay eligible')
    assert 'under the 1.00 SOL' in alert and '`alw collateral deposit --amount 0.50`' in alert


def test_webhook_posts_a_condition_once_and_its_recovery_once():
    notifier = WebhookNotifier('https://hook.example', 'miner')
    with patch.object(qo.requests, 'post') as post:
        notifier.alert('funds', 'wallet short')
        notifier.alert('funds', 'wallet short')
        notifier.resolve('funds', 'wallet ok')
        notifier.resolve('funds', 'wallet ok')
        notifier.event('started')
    assert post.call_count == 3
    assert post.call_args_list[0].kwargs['json']['content'] == '[miner] wallet short'


# ─── config and helpers ───


def test_config_defaults_to_disabled_and_validates(tmp_path):
    cfg = OptimizerConfig.load(tmp_path / 'missing.json')
    assert cfg.enabled is False
    assert (cfg.max_better_than_market_pct, cfg.max_worse_than_market_pct) == (1.0, 3.5)
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
