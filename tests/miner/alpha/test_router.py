import json
import math
import threading
import time
from types import SimpleNamespace

import pytest
from conftest import lock_free, make_config

from allways.constants import CONTRACT_MAX_TOTAL_EXTENSION_SECS
from allways.miner.alpha.config import ALPHA_PAUSED_FLAG
from allways.miner.alpha.journal import BUY_SENT, FAILED, HELD, SIZED, XFER_SENT, AlphaJournal
from allways.miner.alpha.ledger import Ledger
from allways.miner.alpha.market import MIN_STAKE_RAO, RAO
from allways.miner.alpha.router import (
    LIMIT_LADDER,
    MAX_TOPS,
    NO_LIMIT,
    STRANDED_SECS,
    AlphaRequest,
    AlphaRouter,
)
from allways.miner.alpha.signer import DEAD, INCLUDED, POOL_REJECTED, WAIT, Included, Signed
from allways.miner.alpha.telemetry import telemetry

KEY, OTHER_KEY = 'ab' * 32, 'cd' * 32
NETUID, HOTKEY, COLDKEY = 64, '5Ftop', '5Cold'
NEED = 20 * RAO
SPOT = 50_000_000  # rao per alpha
NOW = 1_790_000_000
DEADLINE = NOW + 3600
FEE = 1000
LANDED = 101  # the block every landed buy is credited at


def request(key=KEY, deadline=DEADLINE, need=NEED) -> AlphaRequest:
    return AlphaRequest(key, NETUID, need, '5Fuser', deadline)


class FakeChain:
    """The router's read node: positions by block, a head that may lag a landed buy."""

    def __init__(self):
        self.stakes: dict[str, int] = {}  # hotkey → alpha on NETUID before any buy
        self.credits: list[tuple[int, str, int]] = []  # (block, hotkey, alpha) per landed buy
        self.head = LANDED
        self.locked = 0
        self.unreadable = False
        self.on_read = None  # what another thread does while the router reads the position
        self.fills: list[int] = []  # alpha credited per landed buy; empty = the exact sim
        self.tao_free = 2 * RAO
        self.transfers = True  # None = the toggle read fails

    def positions(self, block: int) -> dict[str, int]:
        held = dict(self.stakes)
        for at, hotkey, alpha in self.credits:
            if at <= block:
                held[hotkey] = held.get(hotkey, 0) + alpha
        return held

    def get_current_block(self):
        return self.head

    def get_stake_info_for_coldkey(self, coldkey, block=None):
        assert coldkey == COLDKEY and block <= self.head
        if self.unreadable:
            raise ConnectionError('stake read failed')
        if self.on_read:
            self.on_read()
        held = self.positions(block).items()
        return [
            SimpleNamespace(hotkey_ss58=hotkey, netuid=NETUID, stake=SimpleNamespace(rao=alpha))
            for hotkey, alpha in held
        ]

    def query_runtime_api(self, runtime_api, method, params, block=None):
        assert (runtime_api, method) == ('StakeInfoRuntimeApi', 'get_stake_availability_for_coldkeys')
        assert params == [[COLDKEY], [NETUID]] and block <= self.head
        return {COLDKEY: {NETUID: {'available': sum(self.positions(block).values()) - self.locked}}}


class FakeAlpha:
    def __init__(self, chain):
        self.chain, self.subtensor = chain, chain

    def transfers_enabled(self):
        if self.chain.transfers is None:
            raise ConnectionError('toggle read failed')
        return self.chain.transfers


class FakeMarket:
    def __init__(self):
        self.validators = {NETUID: HOTKEY}
        self.spot = SPOT
        self.sim_targets = []
        self.sim_fails = False

    def top_validator(self, netuid):
        return self.validators.get(netuid)

    def spot_rao(self, netuid):
        return self.spot

    def sim_buy(self, netuid, alpha_out):
        self.sim_targets.append(alpha_out)
        return None if self.sim_fails else -(-alpha_out * self.spot // RAO)


class FakeSigner:
    """Lands a buy at once ('included'), loses the answer ('lost') or sends nothing ('none')."""

    def __init__(self, chain, market):
        self.chain, self.market = chain, market
        self.lock = threading.RLock()
        self.mode = 'included'
        self.outcomes: list[tuple[bool, str]] = []  # per landing: (success, error); empty = success
        self.verdicts: dict[str, tuple[str, Included | None]] = {}
        self.sent = []
        self.nonce = 5

    def compose(self, function, params):
        return (function, params)

    def send(self, call, record):
        assert not lock_free(self.lock)  # every send runs under the coldkey lock
        if self.mode == 'none':
            return None
        signed = Signed(self.nonce, f'0x{self.nonce:02x}', 100)
        self.nonce += 1
        record(signed)
        self.sent.append(call)
        return (signed, None) if self.mode == 'lost' else (signed, self.land(call, signed))

    def land(self, call, signed) -> Included:
        success, error = self.outcomes.pop(0) if self.outcomes else (True, '')
        if success:
            params = call[1]
            alpha = self.chain.fills.pop(0) if self.chain.fills else params['amount_staked'] * RAO // self.market.spot
            self.chain.credits.append((LANDED, params['hotkey'], alpha))
        return Included(signed.tx_hash, LANDED, f'block#{LANDED}', 2, success, error, FEE)

    def resolve(self, signed):
        return self.verdicts.get(signed.tx_hash, (WAIT, None))


class World:
    def __init__(self, tmp_path):
        self.path = tmp_path
        self.chain, self.market = FakeChain(), FakeMarket()
        self.signer = FakeSigner(self.chain, self.market)
        self.alerts = []
        self.config = make_config(alpha_enabled=True, log_dir=tmp_path)

    def journal(self) -> AlphaJournal:
        return AlphaJournal(Ledger(self.path / 'alpha.json'))

    def router(self) -> AlphaRouter:
        """A fresh router on the same journal file: what a restart gives."""
        return AlphaRouter(
            self.signer,
            self.journal(),
            self.market,
            {f'sn{NETUID}': FakeAlpha(self.chain)},
            COLDKEY,
            lambda: self.chain.tao_free,
            self.config,
            alert=lambda key, title, body: self.alerts.append(key),
        )

    def state(self, key=KEY) -> str:
        return self.journal().row(key)['state']

    def held(self, hotkey=HOTKEY) -> int:
        return self.chain.positions(self.chain.head).get(hotkey, 0)

    def amounts(self) -> list[int]:
        return [call[1]['amount_staked'] for call in self.signer.sent]

    def events(self) -> list[dict]:
        return [json.loads(line) for line in (self.path / 'events.jsonl').read_text().splitlines()]


@pytest.fixture
def clock(monkeypatch):
    now = [NOW]
    monkeypatch.setattr(time, 'time', lambda: now[0])
    return now


@pytest.fixture
def world(tmp_path, clock):
    telemetry.configure(tmp_path)
    return World(tmp_path)


def buy_size(world, need=NEED) -> int:
    """The rao the exact sim asks for the buffered need."""
    return -(-math.ceil(need * (1 + world.config.alpha_buy_buffer)) * SPOT // RAO)


def test_a_request_sizes_buys_once_into_the_top_validator_and_holds(world):
    router = world.router()
    router.request(request())
    router.request(request())  # idempotent
    assert router.outcome(KEY) is None and world.state() == SIZED

    router.tick()

    target = math.ceil(NEED * (1 + world.config.alpha_buy_buffer))
    tao = buy_size(world)
    limit = math.ceil(SPOT * LIMIT_LADDER[0])
    assert world.signer.sent == [
        (
            'add_stake_limit',
            {'hotkey': HOTKEY, 'netuid': NETUID, 'amount_staked': tao, 'limit_price': limit, 'allow_partial': False},
        )
    ]
    assert world.market.sim_targets == [target]
    assert router.outcome(KEY) == 'done' and world.state() == HELD
    row = world.journal().row(KEY)
    assert row['reserved_rao'] == 0 and row['tops'] == 0 and world.journal().reserved_rao() == 0
    assert row['buys'] == [
        {
            'nonce': 5,
            'tx_hash': '0x05',
            'head': 100,
            'tao_in': tao,
            'limit_price': limit,
            'spot_rao': SPOT,
            'position_rao': 0,
            'block': 101,
            'success': True,
            'error': '',
            'fee_rao': FEE,
        }
    ]
    event = world.events()[-1]
    assert event['kind'] == 'alpha_bought' and event['swap'] == KEY and event['tx'] == '0x05'
    assert (event['tao_in'], event['alpha_out'], event['spot_rao'], event['limit_rao'], event['fee_rao']) == (
        tao,
        world.held(),
        SPOT,
        limit,
        FEE,
    )
    router.tick()
    assert len(world.signer.sent) == 1


def seed_sized(world) -> int:
    """A row at SIZED after its reservation write; returns the buy size the router will reserve."""
    world.router().request(request())
    tao = buy_size(world)
    world.journal().update(KEY, reserved_rao=tao)
    world.chain.tao_free = tao + int(world.config.alpha_fee_reserve_tao * RAO)  # its own reservation must not block it
    return tao


def seed_buy_sent(world, landed: bool) -> None:
    """A buy whose submit answer was lost; the chain either carried it or let its era expire."""
    world.signer.mode = 'lost'
    router = world.router()
    router.request(request())
    router.tick()
    signed = Signed(5, '0x05', 100)
    verdict = (INCLUDED, world.signer.land(world.signer.sent[0], signed)) if landed else (DEAD, None)
    world.signer.verdicts['0x05'] = verdict
    world.signer.mode = 'included'


def seed_held(world, state: str) -> None:
    world.router().request(request())
    world.chain.stakes[HOTKEY] = NEED
    world.journal().update(KEY, state=state, xfer={'nonce': 6, 'tx_hash': '0x06', 'head': 102})


@pytest.mark.parametrize(
    'seed, sends_before, sends_after, end_state',
    [
        (seed_sized, 0, 1, HELD),
        (lambda world: seed_buy_sent(world, landed=True), 1, 1, HELD),
        (lambda world: seed_buy_sent(world, landed=False), 1, 2, HELD),
        (lambda world: seed_held(world, HELD), 0, 0, HELD),
        (lambda world: seed_held(world, XFER_SENT), 0, 0, XFER_SENT),
    ],
)
def test_a_fresh_router_at_any_state_lands_one_buy_and_never_transfers(
    world, seed, sends_before, sends_after, end_state
):
    seed(world)
    assert len(world.signer.sent) == sends_before

    fresh = world.router()
    fresh.tick()
    fresh.tick()  # a dead buy re-sizes on one tick and buys on the next

    assert len(world.signer.sent) == sends_after
    assert all(call[0] == 'add_stake_limit' for call in world.signer.sent)
    assert fresh.outcome(KEY) == 'done' and world.state() == end_state
    assert world.held() >= NEED
    assert sum(buy['success'] for buy in world.journal().row(KEY)['buys']) == min(sends_after, 1)


def test_a_buy_that_may_still_land_is_never_resent_and_a_stranded_hold_alerts(world, clock):
    seed_buy_sent(world, landed=True)
    world.signer.verdicts['0x05'] = (WAIT, None)
    fresh = world.router()

    fresh.tick()
    assert len(world.signer.sent) == 1 and world.state() == BUY_SENT and fresh.outcome(KEY) is None

    world.signer.verdicts['0x05'] = (INCLUDED, world.signer.land(world.signer.sent[0], Signed(5, '0x05', 100)))
    fresh.tick()
    assert world.state() == HELD and world.alerts == []
    clock[0] += STRANDED_SECS + 1
    fresh.tick()
    assert world.alerts == [f'alpha-stranded:{KEY}'] and len(world.signer.sent) == 1


def test_a_landed_buy_is_read_at_or_after_its_block_never_on_a_lagging_node(world):
    world.chain.head = LANDED - 1
    router = world.router()
    router.request(request())

    router.tick()
    router.tick()
    assert len(world.signer.sent) == 1 and world.state() == BUY_SENT and world.journal().row(KEY)['tops'] == 0
    assert world.journal().row(KEY)['buys'][0]['success'] is True  # the landing was on disk while the read waited
    world.chain.head = LANDED
    router.tick()

    assert len(world.signer.sent) == 1 and world.state() == HELD


def test_a_short_landing_tops_up_once_at_the_stake_minimum(world):
    world.chain.fills = [NEED - 1]
    router = world.router()
    router.request(request())

    router.tick()
    assert world.state() == SIZED and world.journal().row(KEY)['tops'] == 1
    router.tick()

    assert world.amounts()[1] == MIN_STAKE_RAO
    assert world.market.sim_targets[1] == math.ceil(1 * (1 + world.config.alpha_buy_buffer))
    assert world.state() == HELD and world.held() >= NEED
    assert [event['tao_in'] for event in world.events()] == world.amounts()


def test_still_short_after_the_top_up_limit_fails_with_an_alert(world):
    world.chain.fills = [NEED - 1] + [0] * MAX_TOPS  # a landed buy that credits nothing: StakeAdded over-reports
    router = world.router()
    router.request(request())

    for _ in range(MAX_TOPS + 1):
        router.tick()

    assert len(world.signer.sent) == MAX_TOPS + 1 and world.state() == FAILED
    assert router.outcome(KEY) == 'failed' and world.alerts == [f'alpha-short:{KEY}']
    router.tick()
    assert len(world.signer.sent) == MAX_TOPS + 1


def test_the_limit_climbs_one_rung_per_refusal_on_a_fresh_spot_and_ends_unlimited(world):
    world.signer.outcomes = [(False, 'SlippageTooHigh'), (False, 'PriceLimitExceeded'), (False, 'SlippageTooHigh')]
    router = world.router()
    router.request(request())
    spots = [SPOT, SPOT + 1, SPOT + 2, SPOT + 3]

    for spot in spots:
        world.market.spot = spot
        router.tick()

    limits = [call[1]['limit_price'] for call in world.signer.sent]
    assert limits == [math.ceil(s * f) for s, f in zip(spots, LIMIT_LADDER)] + [NO_LIMIT]
    assert world.state() == HELD and world.journal().row(KEY)['tops'] == 0
    assert len(world.events()) == 1


@pytest.mark.parametrize(
    'error, alerts',
    [(POOL_REJECTED, []), ('NotEnoughBalanceToStake', ['alpha-float']), ('BalanceWithdrawalError', ['alpha-float'])],
)
def test_a_pool_rejection_or_short_tao_resends_the_buy_at_the_same_rung(world, error, alerts):
    world.signer.outcomes = [(False, error)]
    router = world.router()
    router.request(request())

    router.tick()
    assert world.state() == SIZED and world.alerts == alerts
    router.tick()

    assert [call[1]['limit_price'] for call in world.signer.sent] == [math.ceil(SPOT * LIMIT_LADDER[0])] * 2
    assert world.state() == HELD and world.journal().row(KEY)['tops'] == 0


def test_the_free_tao_check_and_the_buy_run_under_one_coldkey_lock(world):
    reads = []
    router = world.router()
    router.tao_free = lambda: reads.append(lock_free(world.signer.lock)) or world.chain.tao_free
    router.request(request())

    router.tick()

    assert reads == [False] and world.state() == HELD and lock_free(world.signer.lock)


def test_any_other_chain_failure_fails_the_swap_with_an_alert(world):
    world.signer.outcomes = [(False, 'HotKeyAccountNotExists')]
    router = world.router()
    router.request(request())

    router.tick()
    router.tick()

    assert len(world.signer.sent) == 1 and world.state() == FAILED
    assert router.outcome(KEY) == 'failed' and world.alerts == [f'alpha-buy:{KEY}']


def test_a_late_request_is_refused_and_a_late_sized_row_fails(world, clock):
    router = world.router()
    router.request(request(deadline=NOW + 60))
    assert router.outcome(KEY) == 'failed' and world.journal().row(KEY) is None

    router.request(request(OTHER_KEY, deadline=NOW + 120))
    clock[0] += 61
    router.tick()

    assert world.signer.sent == [] and world.state(OTHER_KEY) == FAILED
    assert router.outcome(OTHER_KEY) == 'failed' and world.alerts == [f'alpha-late:{OTHER_KEY}']


@pytest.mark.parametrize('state', [SIZED, HELD])
def test_a_row_past_the_deadline_and_every_extension_fails_so_the_seller_may_sell(world, clock, state):
    seed_held(world, state)
    world.chain.unreadable = True  # a sized row whose position never reads would otherwise sit forever
    router = world.router()
    clock[0] = DEADLINE + CONTRACT_MAX_TOTAL_EXTENSION_SECS
    router.tick()
    assert world.state() == state and world.alerts == []

    clock[0] += 1
    router.tick()

    assert world.state() == FAILED and world.alerts == [f'alpha-expired:{KEY}']
    assert not world.journal().open_rows() and world.signer.sent == []


def test_a_sized_row_already_holding_its_need_holds_even_past_the_late_line(world, clock):
    world.chain.stakes[HOTKEY] = NEED
    router = world.router()
    router.request(request(deadline=NOW + 120))
    clock[0] += 61

    router.tick()

    assert world.signer.sent == [] and world.state() == HELD and world.alerts == []


def test_a_fill_on_a_standing_quote_is_routed_under_pause_and_when_alpha_is_disabled(world):
    (world.path / ALPHA_PAUSED_FLAG).touch()
    world.config = make_config(alpha_enabled=False, log_dir=world.path)
    router = world.router()

    router.request(request())
    router.tick()

    assert router.outcome(KEY) == 'done' and len(world.signer.sent) == 1


def test_a_request_waits_for_a_top_validator(world):
    world.market.validators.clear()
    router = world.router()

    router.request(request())
    assert router.outcome(KEY) is None and world.journal().row(KEY) is None

    world.market.validators[NETUID] = HOTKEY
    router.request(request())
    assert world.journal().row(KEY)['hotkey'] == HOTKEY


@pytest.mark.parametrize(
    'stakes, locked, buys',
    [
        ({HOTKEY: NEED // 2}, 0, [NEED - NEED // 2]),  # on the row's hotkey: lowers the buy
        ({HOTKEY: NEED}, NEED // 2, [NEED // 2]),  # held but locked: only the free part counts
        ({HOTKEY: NEED}, 0, []),  # already held in full: no buy at all
    ],
)
def test_alpha_counts_only_when_free(world, stakes, locked, buys):
    world.chain.stakes, world.chain.locked = stakes, locked
    router = world.router()
    router.request(request())

    router.tick()

    assert world.market.sim_targets == [math.ceil(gap * (1 + world.config.alpha_buy_buffer)) for gap in buys]
    assert world.state() == HELD


def test_transfers_off_or_unreadable_alerts_each_tick_and_never_buys(world):
    router = world.router()
    router.request(request())
    world.chain.transfers = False
    router.tick()
    router.tick()
    world.chain.transfers = None
    router.tick()

    assert world.signer.sent == [] and world.state() == SIZED and world.alerts == [f'alpha-transfers:{KEY}'] * 2
    world.chain.transfers = True
    router.tick()
    assert len(world.signer.sent) == 1 and world.state() == HELD


@pytest.mark.parametrize(
    'stakes, other_held_on, hotkey, buys',
    [
        ({'5Fnew': NEED}, None, '5Fnew', []),  # the stake followed a hotkey swap: the row follows it
        ({'5Fnew': NEED, HOTKEY: NEED}, None, HOTKEY, []),  # a tie keeps the row's hotkey
        ({'5Fnew': NEED}, '5Fnew', HOTKEY, [NEED]),  # another held fill owns it: not ours, buy into ours
    ],
)
def test_the_row_follows_the_single_hotkey_holding_our_alpha(world, stakes, other_held_on, hotkey, buys):
    world.chain.stakes = stakes
    router = world.router()
    if other_held_on:
        router.request(request(OTHER_KEY))
        router.journal.update(OTHER_KEY, state=HELD, hotkey=other_held_on)
    router.request(request())

    router.tick()

    assert world.journal().row(KEY)['hotkey'] == hotkey and world.state() == HELD
    assert [call[1]['hotkey'] for call in world.signer.sent] == [hotkey] * len(buys)
    assert world.market.sim_targets == [math.ceil(gap * (1 + world.config.alpha_buy_buffer)) for gap in buys]


def test_a_transfer_recorded_during_the_read_keeps_its_hotkey(world):
    seed_held(world, HELD)
    world.chain.stakes = {'5Fnew': NEED}
    router = world.router()
    world.chain.on_read = lambda: router.journal.update(KEY, state=XFER_SENT)

    router.tick()

    row = world.journal().row(KEY)
    assert (row['state'], row['hotkey']) == (XFER_SENT, HOTKEY) and world.signer.sent == []


def test_an_unreadable_position_or_size_never_buys(world):
    router = world.router()
    router.request(request())
    world.chain.unreadable = True
    router.tick()
    world.chain.unreadable, world.market.sim_fails = False, True
    router.tick()
    world.market.sim_fails, world.signer.mode = False, 'none'
    router.tick()

    assert world.signer.sent == [] and world.state() == SIZED
    world.signer.mode = 'included'
    router.tick()
    assert len(world.signer.sent) == 1 and world.state() == HELD


def test_a_reservation_blocks_a_second_buy_that_would_breach_the_fee_reserve(world):
    world.signer.mode = 'lost'
    router = world.router()
    router.request(request())
    router.tick()
    tao = world.amounts()[0]
    world.chain.tao_free = 2 * tao + int(world.config.alpha_fee_reserve_tao * RAO) - 1
    world.signer.mode = 'included'
    router.request(request(OTHER_KEY))

    router.tick()
    assert len(world.signer.sent) == 1 and world.alerts == ['alpha-float'] and world.state(OTHER_KEY) == SIZED

    world.signer.verdicts['0x05'] = (INCLUDED, world.signer.land(world.signer.sent[0], Signed(5, '0x05', 100)))
    router.tick()  # the first buy lands: its reservation is spent, the second may go
    router.tick()
    assert len(world.signer.sent) == 2 and world.state() == HELD and world.state(OTHER_KEY) == HELD
    assert world.held() >= 2 * NEED  # the first fill's alpha never counts for the second


def test_a_row_still_buying_claims_nothing_so_a_later_row_holds_without_a_top_up(world):
    router = world.router()
    router.request(request())
    router.request(request(OTHER_KEY, need=NEED // 2))
    world.chain.tao_free = buy_size(world, NEED // 2) + int(world.config.alpha_fee_reserve_tao * RAO)

    router.tick()

    assert world.amounts() == [buy_size(world, NEED // 2)] and world.alerts == ['alpha-float']
    assert world.state() == SIZED and world.state(OTHER_KEY) == HELD
    assert world.journal().row(OTHER_KEY)['tops'] == 0


@pytest.mark.parametrize('transfer_beats_the_read', [False, True])
def test_a_held_row_short_by_a_fee_tops_up_unless_a_transfer_records_first(world, transfer_beats_the_read):
    seed_held(world, HELD)
    world.chain.stakes[HOTKEY] = NEED - 1
    router = world.router()
    if transfer_beats_the_read:
        world.chain.on_read = lambda: router.journal.update(KEY, state=XFER_SENT)

    router.tick()
    router.tick()

    if transfer_beats_the_read:
        assert world.signer.sent == [] and world.state() == XFER_SENT
    else:
        assert world.amounts() == [MIN_STAKE_RAO] and world.state() == HELD
        assert world.held() >= NEED and world.journal().row(KEY)['tops'] == 0
