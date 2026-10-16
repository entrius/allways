import base64

from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.events import EventRecord
from allways.validator_v2.feed.events import FeedStatus, HistoryGap
from allways.validator_v2.runtime.bus import Latest, Topic
from allways.validator_v2.world.snapshot import WorldReader
from allways.validator_v2.world.touches import touches

MINER = Keypair().pubkey()
NOW = 2_000_000_000


def _quote_fields(rate, updated_at=NOW, addrs=True):
    f = {
        'miner': MINER,
        'from_chain': 'btc',
        'to_chain': 'sol',
        'collateral_chain': 'sol',
        'rate': rate,
        'liquidity': 1,
        'updated_at': updated_at,
        'update_fee': 0,
    }
    if addrs:
        f.update(miner_from_addr='bc1qsrc', miner_to_addr='So1dst')
    return f


class _Rpc:
    def __init__(self, client):
        self.client = client
        self.calls = []

    def get_slot(self):
        return self.client.slot

    def _call(self, method, params):
        self.calls.append((method, params))
        keys = params[0]
        return {'context': {'slot': self.client.slot}, 'value': [self.client.live.get(k) for k in keys]}


class _Client:
    """Program state as a dict pda → raw account bytes; get_all scans it, getMultipleAccounts reads it."""

    def __init__(self):
        self.program_id = None
        self.slot = 100
        self.live = {}  # pda → rpc account value
        self.scans = 0
        self.rpc = _Rpc(self)

    def put(self, name, pda, value):
        raw = layouts.DISCRIMINATORS[name] + layouts.ACCOUNT_LAYOUTS[name].build(value)
        self.live[str(pda)] = {'data': [base64.b64encode(raw).decode(), 'base64']}

    def _decode(self, name, raw):
        return layouts.ACCOUNT_LAYOUTS[name].parse(raw[8:])

    def get_all(self, name):
        self.scans += 1
        disc = layouts.DISCRIMINATORS[name]
        out = []
        for pda, value in self.live.items():
            raw = base64.b64decode(value['data'][0])
            if raw[:8] == disc:
                out.append((pda, self._decode(name, raw)))
        return out

    def get_config(self):
        return {'halted': False}


def _reader(client):
    events, gaps, status, out = Topic('e'), Topic('g'), Latest(), Latest()
    status.set(FeedStatus(catching_up=False, session=1, last_event_at=0))
    reader = WorldReader(lambda: client, events.subscribe('w'), gaps.subscribe('w'), status, out, clock=lambda: NOW)
    reader.setup()
    return reader, events, gaps, status, out


def _ev(name, fields, slot, sig='s'):
    return EventRecord(name, fields, slot, NOW, f'{sig}-{slot}')


def _quote_pda():
    return str(pdas.quote_pda(MINER, 'btc', 'sol', 'sol'))


def test_a_quote_update_needs_no_account_read():
    client = _Client()
    reader, events, _, _, out = _reader(client)
    reader.tick(NOW)  # boot scan
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=200))
    reader.tick(NOW)
    world, _ = out.get()
    assert world.get('MinerQuote', _quote_pda()).rate == 5
    assert client.rpc.calls == []  # applied straight from the event


def test_an_older_event_never_overwrites_newer_state():
    client = _Client()
    reader, events, _, _, out = _reader(client)
    reader.tick(NOW)
    events.publish(_ev('QuoteSet', _quote_fields(rate=7), slot=300))
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=200))  # a backfilled older one, arriving late
    events.publish(
        _ev(
            'QuoteRemoved',
            {k: _quote_fields(0)[k] for k in ('miner', 'from_chain', 'to_chain', 'collateral_chain')},
            slot=250,
        )
    )
    reader.tick(NOW)
    assert out.get()[0].get('MinerQuote', _quote_pda()).rate == 7


def test_events_older_than_the_boot_scan_leave_scanned_state_alone():
    client = _Client()
    client.slot = 500
    reader, events, _, _, out = _reader(client)
    reader.tick(NOW)
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=400))
    reader.tick(NOW)
    assert out.get()[0].get('MinerQuote', _quote_pda()) is None


def test_touched_accounts_are_read_in_one_batch_and_closed_ones_leave():
    client = _Client()
    reader, events, _, _, out = _reader(client)
    swap_key = bytes(range(32))
    swap_pda = str(pdas.swap_pda(swap_key))
    reader._accounts['Swap'][swap_pda] = 'stale'
    events.publish(_ev('SwapCompleted', {'swap_key': swap_key, 'miner': MINER, 'collateral_chain': 'sol'}, slot=200))
    events.publish(_ev('CollateralPosted', {'miner': MINER, 'amount': 1, 'total': 1}, slot=201))
    reader._need_scan = False
    reader.tick(NOW)
    assert len(client.rpc.calls) == 1  # one getMultipleAccounts for every touched account
    method, (keys, opts) = client.rpc.calls[0]
    assert method == 'getMultipleAccounts' and opts['minContextSlot'] == 201
    assert swap_pda in keys and str(pdas.miner_state_pda(MINER)) in keys
    assert out.get()[0].get('Swap', swap_pda) is None  # read back null: closed


def test_the_earliest_claim_on_a_deposit_wins_whatever_order_it_arrives_in():
    client = _Client()
    reader, events, _, _, out = _reader(client)
    reader.tick(NOW)
    first, second = bytes([1] * 32), bytes([2] * 32)
    claim = {'miner': MINER, 'user': MINER, 'from_tx_hash': 'ABCDEF' * 10 + 'ab12', 'from_tx_block': 1}
    events.publish(_ev('SwapClaimed', {**claim, 'swap_key': second}, slot=300, sig='b'))
    events.publish(_ev('SwapClaimed', {**claim, 'swap_key': first}, slot=200, sig='a'))
    events.publish(_ev('SwapInitiated', {'swap_key': first, 'miner': MINER}, slot=310))
    reader.tick(NOW)
    (held,) = out.get()[0].deposits.values()
    assert held.swap_key == first.hex() and held.initiated


def test_a_history_gap_triggers_one_rescan():
    client = _Client()
    reader, _, gaps, _, _ = _reader(client)
    reader.tick(NOW)
    scans = client.scans
    reader.tick(NOW)
    assert client.scans == scans  # steady state never scans
    gaps.publish(HistoryGap(NOW - 3600))
    reader.tick(NOW)
    assert client.scans > scans


def test_the_world_is_incomplete_while_the_feed_catches_up():
    client = _Client()
    reader, _, _, status, out = _reader(client)
    status.set(FeedStatus(catching_up=True, session=1, last_event_at=0))
    reader.tick(NOW)
    assert out.get()[0].complete is False


def test_every_miner_event_rereads_its_state_reservation_and_pool():
    t = touches('ReservationExtended', {'miner': MINER, 'collateral_chain': 'tao'})
    assert {k for k, _ in t.reads} == {'MinerState', 'Reservation', 'Pool'}
    assert str(pdas.reservation_pda(MINER, 'tao')) in {p for _, p in t.reads}
    # No hub named: every hub's reservation and pool.
    t = touches('MinerActivated', {'miner': MINER, 'at': 0})
    assert sum(1 for k, _ in t.reads if k == 'Reservation') == 2
    assert touches('ConfigChanged', {'setting': 'reservation_ttl'}).reads == {('Config', str(pdas.config_pda()))}
    assert touches('QuoteSet', _quote_fields(1, addrs=False)).reads == {('MinerQuote', _quote_pda())}
