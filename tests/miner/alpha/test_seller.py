import logging
from types import SimpleNamespace

import pytest
from conftest import DiscordDown

from allways.miner.alpha.journal import XFER_SENT, AlphaJournal
from allways.miner.alpha.ledger import Ledger
from allways.miner.alpha.market import MIN_STAKE_RAO
from allways.miner.alpha.seller import (
    HOTKEYS_CAP,
    RUNG_WAIT_SECS,
    TOLERANCE_LADDER,
    AlphaSeller,
)
from allways.miner.alpha.signer import DEAD, INCLUDED, WAIT, Included, Signed
from allways.miner.alpha.telemetry import telemetry

RAO = 10**9
SPOT = 50_000_000  # 0.05 τ per alpha
MAX_SWAP = 2 * RAO
COLDKEY = '5Cold'
KEY = 'ab' * 32


class FakeChain:
    """Positions by (netuid, hotkey); availability per netuid defaults to the whole position."""

    def __init__(self):
        self.positions: dict[tuple[int, str], int] = {}
        self.available: dict[int, int] = {}
        self.hotkeys: list[str] = []
        self.fail_stakes = self.fail_available = False

    def get_stake_info_for_coldkey(self, coldkey):
        if self.fail_stakes:
            raise ConnectionError('stake read down')
        return [
            SimpleNamespace(netuid=n, hotkey_ss58=hk, stake=SimpleNamespace(rao=alpha))
            for (n, hk), alpha in self.positions.items()
        ]

    def free_to_send(self, netuid):
        if self.fail_available:
            raise ConnectionError('availability down')
        return self.available.get(netuid, sum(a for (n, _), a in self.positions.items() if n == netuid))


class FakeAlpha:
    def __init__(self, chain, netuid):
        self.chain, self.netuid, self.subtensor = chain, netuid, chain

    def free_to_send(self, coldkey):
        return self.chain.free_to_send(self.netuid)

    def staking_hotkeys(self, coldkey):
        return self.chain.hotkeys


class FakeSigner:
    """Outcome per send: 'success', 'fail', 'ambiguous' or 'unsent'; resolve answers ``verdict``."""

    def __init__(self):
        self.outcome, self.verdict, self.nonce = 'success', (WAIT, None), 0
        self.sends: list[dict] = []
        self.resolved: list[Signed] = []

    def compose(self, function, params):
        return (function, params)

    def send(self, call, record):
        if self.outcome == 'unsent':
            return None
        self.nonce += 1
        signed = Signed(self.nonce, f'0x{self.nonce:02x}', 100)
        record(signed)
        self.sends.append(call[1])
        return signed, None if self.outcome == 'ambiguous' else included(signed.tx_hash, self.outcome == 'success')

    def resolve(self, signed):
        self.resolved.append(signed)
        return self.verdict


def included(tx_hash, success):
    return Included(tx_hash, 101, 'block#101', 3, success, '' if success else 'SlippageTooHigh', 1_000)


class FakeMarket:
    def __init__(self):
        self.spot = {}
        self.spot_reads = []

    def spot_rao(self, netuid):
        self.spot_reads.append(netuid)
        return self.spot.get(netuid, SPOT)

    def price_rao(self, netuid):
        return self.spot.get(netuid, SPOT)

    def sim_sell(self, netuid, alpha_in):
        return alpha_in * SPOT // RAO


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def world(tmp_path, monkeypatch):
    return make_world(tmp_path, monkeypatch)


def make_world(tmp_path, monkeypatch, alerts=None):
    chain, signer, market, clock = FakeChain(), FakeSigner(), FakeMarket(), Clock()
    journal = AlphaJournal(Ledger(tmp_path / 'alpha.json'))
    alerts, events = [] if alerts is None else alerts, []
    monkeypatch.setattr(telemetry, 'event', lambda kind, **fields: events.append((kind, fields)))
    assets = {'sn64': FakeAlpha(chain, 64), 'sn7': FakeAlpha(chain, 7), 'tao': object()}
    max_swap = {'value': MAX_SWAP}
    seller = AlphaSeller(
        signer,
        journal,
        market,
        assets,
        COLDKEY,
        lambda: max_swap['value'],
        alert=lambda key, title, body: alerts.append(key),
        keep_hotkeys=frozenset({'hk-miner'}),
        clock=clock,
    )
    return SimpleNamespace(
        chain=chain,
        signer=signer,
        market=market,
        clock=clock,
        journal=journal,
        seller=seller,
        alerts=alerts,
        events=events,
        max_swap=max_swap,
    )


def sells(world) -> list[tuple[int, str, int]]:
    return [(s['netuid'], s['hotkey'], s['amount_unstaked']) for s in world.signer.sends]


def sold(hotkey, alpha_in, tx, netuid=64, tolerance=0.01):
    fields = dict(netuid=netuid, hotkey=hotkey, alpha_in=alpha_in, tao_out=alpha_in * SPOT // RAO, spot_rao=SPOT)
    return 'alpha_sold', {**fields, 'tolerance': tolerance, 'fee_rao': 1_000, 'tx': tx}


def test_sells_only_what_no_open_row_needs_and_never_from_the_needed_hotkeys_need(world, caplog):
    world.chain.positions = {(64, 'hk-a'): 100 * RAO, (64, 'hk-b'): 50 * RAO}
    world.journal.open(KEY, 64, 80 * RAO, 'hk-a', 0)  # SIZED: hk-a keeps its whole 80 even before the buy lands

    with caplog.at_level(logging.INFO):
        world.seller.tick()

    assert sells(world) == [(64, 'hk-a', 20 * RAO), (64, 'hk-b', 50 * RAO)]
    call = world.signer.sends[0]
    assert (call['limit_price'], call['allow_partial']) == (int(SPOT * 0.99), False)
    assert 'Selling alpha netuid=64 hotkey=hk-a alpha=20000000000 tolerance=1% nonce=1 tx_id=0x01' in caplog.text
    assert world.events == [sold('hk-a', 20 * RAO, '0x01'), sold('hk-b', 50 * RAO, '0x02')]


def test_a_netuid_never_sells_past_free_to_send_less_its_open_need(world):
    world.chain.positions = {(64, 'hk-a'): 100 * RAO, (64, 'hk-b'): 50 * RAO}
    world.chain.available[64] = 120 * RAO  # 30 α locked somewhere on the netuid
    world.journal.open(KEY, 64, 80 * RAO, 'hk-a', 0)

    world.seller.tick()

    assert sells(world) == [(64, 'hk-a', 20 * RAO), (64, 'hk-b', 20 * RAO)]


def test_a_position_an_xfer_sent_row_needs_is_never_sold(world):
    world.chain.positions = {(64, 'hk-a'): 100 * RAO, (64, 'hk-b'): 50 * RAO, (7, 'hk-a'): 10 * RAO}
    world.journal.open(KEY, 64, 80 * RAO, 'hk-a', 0)
    world.journal.update(KEY, state=XFER_SENT)

    world.seller.tick()

    assert sells(world) == [(7, 'hk-a', 10 * RAO)]


def test_dust_and_a_position_wholly_needed_are_left_alone(world):
    world.chain.positions = {(64, 'hk-a'): 80 * RAO, (7, 'hk-a'): MIN_STAKE_RAO * RAO // SPOT - 1}
    world.journal.open(KEY, 64, 80 * RAO, 'hk-a', 0)

    world.seller.tick()

    assert sells(world) == [] and world.market.spot_reads == [7]


def test_stake_on_a_kept_hotkey_is_never_sold_nor_counted_as_unsold(world):
    world.chain.positions = {(7, 'hk-miner'): 100 * RAO, (7, 'hk-a'): 10 * RAO}  # 5 τ of incentive, 0.5 τ bought

    world.seller.tick()

    assert sells(world) == [(7, 'hk-a', 10 * RAO)] and world.seller.alpha_in_blocked() == frozenset()


def test_ladder_climbs_one_rung_per_failure_ten_minutes_apart_then_holds_at_the_last_and_alerts(world):
    world.chain.positions = {(64, 'hk-a'): 10 * RAO}
    world.signer.outcome = 'fail'
    for tolerance in TOLERANCE_LADDER:
        world.seller.tick()
        world.seller.tick()  # the same minute: no second attempt
        assert world.signer.sends[-1]['limit_price'] == int(SPOT * (1 - tolerance))
        assert len(world.signer.sends) == TOLERANCE_LADDER.index(tolerance) + 1
        world.clock.now += RUNG_WAIT_SECS - 1
        world.seller.tick()
        assert len(world.signer.sends) == TOLERANCE_LADDER.index(tolerance) + 1
        world.clock.now += 1
    assert world.alerts == ['alpha-held:64']
    world.seller.tick()
    assert world.signer.sends[-1]['limit_price'] == int(SPOT * (1 - TOLERANCE_LADDER[-1]))

    world.signer.outcome = 'success'
    world.clock.now += RUNG_WAIT_SECS
    world.seller.tick()
    world.seller.tick()
    assert world.signer.sends[-1]['limit_price'] == int(SPOT * (1 - TOLERANCE_LADDER[0]))  # a success resets


def test_an_ambiguous_sell_is_not_sold_again_until_resolved_then_the_fresh_read_decides(world):
    world.chain.positions = {(64, 'hk-a'): 10 * RAO}
    world.signer.outcome = 'ambiguous'
    world.seller.tick()
    world.seller.tick()
    assert len(world.signer.sends) == 1 and world.signer.resolved == [Signed(1, '0x01', 100)]

    world.signer.outcome = 'success'
    world.signer.verdict = (DEAD, None)
    world.seller.tick()  # dead: the fresh read still holds 10 α, so it sells again
    assert len(world.signer.sends) == 2 and world.events == [sold('hk-a', 10 * RAO, '0x02')]

    world.signer.outcome = 'ambiguous'
    world.seller.tick()
    world.chain.positions = {}
    world.signer.verdict = (INCLUDED, included('0x03', True))
    world.seller.tick()  # landed after all: booked from the in-memory sale, nothing left to sell
    assert len(world.signer.sends) == 3 and world.events[-1] == sold('hk-a', 10 * RAO, '0x03')


@pytest.mark.parametrize('failure', ['fail_stakes', 'fail_available', 'spot'])
def test_an_unknown_read_sells_nothing(world, failure):
    world.chain.positions = {(64, 'hk-a'): 10 * RAO}
    if failure == 'spot':
        world.market.spot[64] = None
    else:
        setattr(world.chain, failure, True)

    world.seller.tick()

    assert sells(world) == []


def test_alpha_in_blocked_is_the_netuids_whose_unneeded_alpha_reaches_a_max_swap(world):
    world.chain.positions = {(64, 'hk-a'): 100 * RAO, (7, 'hk-a'): 41 * RAO, (7, 'hk-b'): 39 * RAO}
    world.chain.available = {64: 0, 7: 0}  # nothing sells, the value stays held
    world.journal.open(KEY, 64, 80 * RAO, 'hk-a', 0)  # 20 α × 0.05 τ = 1 τ unneeded on 64; 4 τ on 7
    world.seller.tick()
    assert world.seller.alpha_in_blocked() == frozenset({7})

    world.max_swap['value'] = None
    assert world.seller.alpha_in_blocked() == frozenset({7, 64})

    world.max_swap['value'] = MAX_SWAP
    world.market.spot[7] = None
    world.seller.tick()
    assert world.seller.alpha_in_blocked() == frozenset({7})

    world.chain.positions = {}
    world.seller.tick()
    assert world.seller.alpha_in_blocked() == frozenset()


def test_hotkeys_cap_alerts_past_sixty_four_entries(world):
    world.chain.hotkeys = [f'hk-{i}' for i in range(HOTKEYS_CAP + 1)]
    world.seller.tick()
    assert world.alerts == ['hotkeys-cap']


def test_a_raising_alert_never_aborts_the_tick(tmp_path, monkeypatch):
    world = make_world(tmp_path, monkeypatch, alerts=DiscordDown())
    world.chain.hotkeys = [f'hk-{i}' for i in range(HOTKEYS_CAP + 1)]
    world.chain.positions = {(64, 'hk-a'): 80 * RAO}
    world.seller.tick()
    assert world.alerts == ['hotkeys-cap'] and sells(world) == [(64, 'hk-a', 80 * RAO)]


def test_hotkeys_cap_ignores_dust_an_outsider_parked_on_us(world):
    dust = MIN_STAKE_RAO * RAO // SPOT - 1
    world.chain.hotkeys = [f'hk-{i}' for i in range(HOTKEYS_CAP + 2)]  # 64 unknown entries count, 2 dust do not
    world.chain.positions = {(64, 'hk-0'): dust, (7, 'hk-0'): dust, (64, 'hk-1'): dust}
    world.seller.tick()
    assert world.alerts == []

    world.chain.positions[(7, 'hk-1')] = dust + 1  # one real position on any netuid makes the hotkey count
    world.seller.tick()
    assert world.alerts == ['hotkeys-cap']
