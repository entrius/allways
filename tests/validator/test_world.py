import base64
from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana import layouts, pdas
from allways.solana.events import EventRecord
from allways.validator.feed.events import HISTORY_HORIZON_SECS, FeedStatus, HistoryGap
from allways.validator.runtime.bus import Latest, Topic
from allways.validator.store.db import Store
from allways.validator.world.snapshot import RESCAN_SECS, WorldReader
from allways.validator.world.touches import touches

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

    def get_program_accounts(self, program_id, disc8=None):
        self.client.scans += 1
        out = []
        for pda, value in self.client.live.items():
            raw = base64.b64decode(value['data'][0])
            if raw[:8] == disc8:
                out.append((pda, raw))
        return out

    def get_account_info(self, pda):
        value = self.client.live.get(str(pda))
        return None if value is None else base64.b64decode(value['data'][0])

    def _call(self, method, params):
        self.calls.append((method, params))
        keys = params[0]
        return {'context': {'slot': self.client.slot}, 'value': [self.client.live.get(k) for k in keys]}


class _Client:
    """Program state as a dict pda → rpc account value; scans and getMultipleAccounts both read it."""

    def __init__(self):
        self.program_id = None
        self.slot = 100
        self.live = {}
        self.scans = 0
        self.rpc = _Rpc(self)
        self._store(pdas.config_pda(), layouts.DISCRIMINATORS['Config'] + b'opaque')

    def put(self, name, pda, value):
        self._store(pda, layouts.DISCRIMINATORS[name] + layouts.ACCOUNT_LAYOUTS[name].build(value))

    def _store(self, pda, raw):
        self.live[str(pda)] = {'data': [base64.b64encode(raw).decode(), 'base64'], 'owner': str(self.program_id)}

    def _decode(self, name, raw):
        if name == 'Config':
            return {'raw': raw[8:]}  # opaque here: the World only hands Config through
        return layouts.ACCOUNT_LAYOUTS[name].parse(raw[8:])


def _reader(client, tmp_path, status=None):
    events, gaps, feed_status, out = Topic('e'), Topic('g'), Latest(), Latest()
    feed_status.set(status or FeedStatus(catching_up=False, session=1, last_event_at=0, cursor=(150, 'sig-150')))
    store = Store(str(tmp_path / 'v.db'))
    reader = WorldReader(
        lambda: client, events.subscribe('w'), gaps.subscribe('w'), feed_status, out, store, clock=lambda: NOW
    )
    reader.setup()
    return reader, events, gaps, feed_status, out


def _ev(name, fields, slot, sig='s'):
    return EventRecord(name, fields, slot, NOW, f'{sig}-{slot}')


def _quote_pda():
    return str(pdas.quote_pda(MINER, 'btc', 'sol', 'sol'))


def test_a_quote_update_needs_no_account_read(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)  # boot scan
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=200))
    reader.tick(NOW)
    world, _ = out.get()
    assert world.get('MinerQuote', _quote_pda()).rate == 5
    assert client.rpc.calls == []  # applied straight from the event


def test_an_older_event_never_overwrites_newer_state(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
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


def test_events_older_than_the_boot_scan_leave_scanned_state_alone(tmp_path):
    client = _Client()
    client.slot = 500
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=400))
    reader.tick(NOW)
    assert out.get()[0].get('MinerQuote', _quote_pda()) is None


def test_touched_accounts_are_read_in_one_batch_and_closed_ones_leave(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
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


def test_the_earliest_claim_on_a_deposit_wins_whatever_order_it_arrives_in(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    first, second = bytes([1] * 32), bytes([2] * 32)
    claim = {'miner': MINER, 'user': MINER, 'from_tx_hash': 'ABCDEF' * 10 + 'ab12', 'from_tx_block': 1}
    events.publish(_ev('SwapClaimed', {**claim, 'swap_key': second}, slot=300, sig='b'))
    events.publish(_ev('SwapClaimed', {**claim, 'swap_key': first}, slot=200, sig='a'))
    events.publish(_ev('SwapInitiated', {'swap_key': first, 'miner': MINER}, slot=310))
    reader.tick(NOW)
    (held,) = out.get()[0].deposits.values()
    assert held.swap_key == first.hex() and held.initiated


def test_a_history_gap_triggers_one_rescan(tmp_path):
    client = _Client()
    reader, _, gaps, _, _ = _reader(client, tmp_path)
    reader.tick(NOW)
    scans = client.scans
    reader.tick(NOW)
    assert client.scans == scans  # steady state never scans
    gaps.publish(HistoryGap(NOW - 3600))
    reader.tick(NOW)
    assert client.scans > scans


def test_the_world_is_incomplete_while_the_feed_catches_up(tmp_path):
    client = _Client()
    reader, _, _, status, out = _reader(client, tmp_path)
    status.set(FeedStatus(catching_up=True, session=1, last_event_at=0, cursor=None))
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


def test_a_restart_restores_the_world_instead_of_scanning(tmp_path):
    client = _Client()
    reader, events, _, _, _ = _reader(client, tmp_path)
    reader.tick(NOW)
    events.publish(_ev('QuoteSet', _quote_fields(rate=9), slot=200))
    claim = {'swap_key': bytes([3] * 32), 'miner': MINER, 'user': MINER, 'from_tx_hash': 'f' * 64, 'from_tx_block': 1}
    events.publish(_ev('SwapClaimed', claim, slot=201))
    reader.tick(NOW)
    scans = client.scans

    again, _, _, _, out = _reader(client, tmp_path)  # same database, new process
    again.tick(NOW)
    world = out.get()[0]
    assert client.scans == scans  # restored: no scan
    assert world.get('MinerQuote', _quote_pda()).rate == 9
    assert list(world.deposits) == ['f' * 64]
    assert world.config == {'raw': b'opaque'}


def test_the_world_records_its_feed_position_with_its_state(tmp_path):
    client = _Client()
    reader, _, _, _, _ = _reader(client, tmp_path)
    reader.tick(NOW)
    assert reader.store.resume_point(['world']) == 'sig-150'


def test_a_snapshot_older_than_the_horizon_is_rescanned(tmp_path):
    client = _Client()
    reader, _, _, _, _ = _reader(client, tmp_path)
    reader.tick(NOW)
    with reader.store.tx() as c:
        Store.set_meta(c, 'world.saved_at', str(NOW - HISTORY_HORIZON_SECS - 3600))
    scans = client.scans
    again, _, _, _, _ = _reader(client, tmp_path)
    again.tick(NOW)
    assert client.scans > scans


# ── lost events: the World recovers instead of trusting the stream ──────────


def test_a_failed_account_read_is_retried_until_it_lands(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    swap_pda = str(pdas.swap_pda(bytes(range(32))))
    reader._accounts['Swap'][swap_pda] = 'stale'
    real, calls = client.rpc._call, []

    def flaky(method, params):
        calls.append(method)
        if len(calls) == 1:
            raise RuntimeError('HTTP 429')
        return real(method, params)

    client.rpc._call = flaky
    events.publish(_ev('SwapCompleted', {'swap_key': bytes(range(32)), 'miner': MINER, 'collateral_chain': 'sol'}, 200))
    try:
        reader.tick(NOW)
    except RuntimeError:
        pass  # the component backs off and ticks again
    reader.tick(NOW)
    assert len(calls) == 2 and out.get()[0].get('Swap', swap_pda) is None  # the retry read it back closed


def _live_quote(client, rate):
    fields = {k: v for k, v in _quote_fields(rate).items() if k != 'update_fee'}
    client.put('MinerQuote', _quote_pda(), {**fields, 'miner': bytes(MINER), 'bump': 255})


def test_an_account_the_program_does_not_own_reads_as_absent_and_the_world_keeps_publishing(tmp_path):
    # Anyone can send lamports to a miner's reservation address before it exists: a system-owned, empty account.
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    reservation = str(pdas.reservation_pda(MINER, 'sol'))
    client.live[reservation] = {'data': ['', 'base64'], 'owner': '11111111111111111111111111111111'}
    _live_quote(client, rate=7)
    events.publish(_ev('CollateralPosted', {'miner': MINER, 'amount': 1, 'total': 1}, slot=200))  # reads every hub's
    reader.request_reads({('MinerQuote', _quote_pda())})
    reader.tick(NOW + 1)
    world = out.get()[0]
    assert world.taken_at == NOW + 1  # still publishing
    assert world.get('Reservation', reservation) is None
    assert world.get('MinerQuote', _quote_pda()).rate == 7  # the rest of the read landed
    assert not reader._pending_reads


def test_an_account_that_will_not_decode_is_retried_alone(tmp_path):
    client = _Client()
    reader, _, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    swap_pda = str(pdas.swap_pda(bytes(range(32))))
    client._store(swap_pda, layouts.DISCRIMINATORS['Swap'] + b'truncated')
    _live_quote(client, rate=7)
    reader.request_reads({('Swap', swap_pda), ('MinerQuote', _quote_pda())})
    reader.tick(NOW + 1)
    assert out.get()[0].taken_at == NOW + 1 and out.get()[0].get('MinerQuote', _quote_pda()).rate == 7
    assert reader._pending_reads == {('Swap', swap_pda)}
    client.rpc.calls.clear()
    reader.tick(NOW + 2)
    assert [keys for _, (keys, _) in client.rpc.calls] == [[swap_pda]]  # retried on its own


def test_an_account_the_actuator_found_stale_is_read_on_the_next_tick(tmp_path):
    client = _Client()
    reader, _, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    swap_pda = str(pdas.swap_pda(bytes(range(32))))
    reader._accounts['Swap'][swap_pda] = 'stale'
    reader.request_reads({('Swap', swap_pda)})
    reader.tick(NOW)
    assert out.get()[0].get('Swap', swap_pda) is None


def test_the_world_rescans_every_account_once_an_hour(tmp_path):
    client = _Client()
    reader, *_ = _reader(client, tmp_path)
    reader.tick(NOW)
    scans = client.scans
    reader.tick(NOW + RESCAN_SECS - 1)
    assert client.scans == scans
    reader.tick(NOW + RESCAN_SECS)
    assert client.scans > scans


def test_a_bad_event_does_not_lose_the_rest_of_the_drain(tmp_path):
    client = _Client()
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    events.publish(_ev('QuoteSet', {'miner': MINER}, slot=200))  # malformed: no lane
    events.publish(_ev('QuoteSet', _quote_fields(rate=5), slot=201))
    reader.tick(NOW)
    assert out.get()[0].get('MinerQuote', _quote_pda()).rate == 5


def test_a_live_swap_spends_its_deposit_even_without_its_events(tmp_path):
    client = _Client()
    reader, _, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    tx_hash = 'ABCDEF' * 10 + 'ab12'
    reader._note_initiated(SimpleNamespace(status='Active', from_tx_hash=tx_hash), slot=300)
    reader.tick(NOW)
    (held,) = out.get()[0].deposits.values()
    assert held.initiated
    reader._note_initiated(SimpleNamespace(status='PendingAttestation', from_tx_hash='ab' * 32), slot=301)
    assert len(reader._deposits) == 1  # a claim not yet attested spends nothing


def test_a_restored_world_marks_every_live_swaps_deposit_spent(tmp_path):
    """The deposit ledger alone decides "already claimed", so a restart must not bring back a live swap's
    deposit as unspent, whatever the saved ledger row says."""
    client = _Client()
    live = SimpleNamespace(status='Active', from_tx_hash='ABCDEF' * 10 + 'ab12')
    client._store('SwapPda1111', layouts.DISCRIMINATORS['Swap'] + b'live')
    decode = client._decode
    client._decode = lambda name, raw: live if name == 'Swap' else decode(name, raw)
    reader, _, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)
    (held,) = out.get()[0].deposits.values()
    assert held.initiated
    with reader.store.tx() as c:
        c.execute('UPDATE world_deposits SET initiated = 0')  # a save whose row predates the swap going live

    again, _, _, _, out = _reader(client, tmp_path)
    again.tick(NOW)
    (held,) = out.get()[0].deposits.values()
    assert held.initiated
