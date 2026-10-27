"""The hub relay as a component: its recent fee rows, and what it hands the actuator."""

from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana import pdas
from allways.solana.events import EventRecord
from allways.validator.actuator.actuator import Actuator
from allways.validator.actuator.executors import vote_set_attestation
from allways.validator.actuator.planners import intent
from allways.validator.feed.events import FeedStatus
from allways.validator.hubs import hub
from allways.validator.relay.component import RELAY_EVENTS, HubRelay
from allways.validator.relay.engine import RelayConfig
from allways.validator.relay.ledger import RelayLedger
from allways.validator.runtime.bus import Latest, Topic
from allways.validator.runtime.deadline_queue import DeadlineQueue
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World

NOW = 10_500  # the last fee boundary is 10_000
MINER, HOTKEY = str(Keypair().pubkey()), 'hk'


def _ledger(tmp_path):
    return RelayLedger(Store(str(tmp_path / 'r.db')), 'tao')


def test_only_the_fees_after_a_time_and_in_this_generation_count_beyond_the_counter(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_relay_fee('a', MINER, 'tao', 5, 9_900, vault_generation=1)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    ledger.record_relay_fee('c', MINER, 'tao', 4, 10_300, vault_generation=0)  # a retired vault's fee
    ledger.record_relay_fee('d', 'other', 'tao', 9, 10_300, vault_generation=1)
    assert ledger.fees_after(MINER, 1, 10_000) == 3
    assert ledger.fees_after(MINER, 1, 9_000) == 8
    assert ledger.fees_after(MINER, 1, 10_200) == 0, 'a fee AT the boundary is in the total as of it'


def test_pruning_drops_rows_past_the_horizon(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_relay_fee('a', MINER, 'tao', 5, 9_900, vault_generation=1)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    ledger.prune_relay_fees(10_000)
    rows = ledger.store.conn().execute('SELECT swap_key FROM relay_tao_fees').fetchall()
    assert [r['swap_key'] for r in rows] == ['b']


def test_the_pre_v17_fee_base_table_is_dropped(tmp_path):
    store = Store(str(tmp_path / 'r.db'))
    store.conn().execute(
        'CREATE TABLE relay_tao_fee_base (miner TEXT, generation INTEGER, base INTEGER, as_of INTEGER)'
    )
    RelayLedger(store, 'tao')
    names = {r['name'] for r in store.conn().execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert 'relay_tao_fee_base' not in names and 'relay_tao_fees' in names


class _Vault:
    address = 'vault'

    def __init__(self):
        self.settled = {}

    def head(self):
        return None

    def get_bond_state(self, hotkey):
        return 100, (True, 3)

    def get_settled_total(self, hotkey):
        return self.settled.get(hotkey, 0)

    def get_validators(self):
        return ['us']

    def get_lock_state(self, hotkey):
        return (True, 3)

    def is_slashed(self, swap_ref):
        return False


def _world():
    miner_state = SimpleNamespace(
        miner=MINER,
        active_backings=pdas.BACKING_BIT_TAO,
        has_active_swap=False,
        busy_until=0,
        settling_until=0,
        fee_generation=1,
        accrued_fees=[0] * 8,
    )
    return World(
        taken_at=NOW,
        slot=10,
        complete=True,
        config=SimpleNamespace(last_attest_heartbeat=NOW, vault_generation=1),
        accounts={'MinerState': {str(pdas.miner_state_pda(MINER)): miner_state}, 'BondAttestation': {}},
        attribution={MINER: HOTKEY},
    )


def _relay(tmp_path, maxsize=100_000):
    world, status = Latest(), Latest()
    world.set(_world())
    status.set(FeedStatus(False, 1, NOW, (9, 'sig9')))
    events, inbox = Topic('events'), DeadlineQueue()
    store = Store(str(tmp_path / 'v.db'))
    relay = HubRelay(
        hub('tao'),
        world,
        events.subscribe('relay-tao', maxsize, accept=lambda r: r.name in RELAY_EVENTS),
        status,
        _Vault,
        inbox,
        store,
        config=RelayConfig(max_writes_per_tick=8),
        clock=lambda: NOW,
    )
    return relay, events, inbox, store


def _timed_out(swap_key=b'\x01' * 32, payee='5' + 'A' * 47):
    fields = {
        'swap_key': swap_key,
        'miner': MINER,
        'collateral_chain': 'tao',
        'penalty': 22,
        'reimbursement': 22,
        'payee': payee,
        'hotkey': b'\x00' * 32,
    }
    return EventRecord('SwapTimedOut', fields, 9, NOW - 5, 'sig9')


def test_the_relay_attests_through_the_actuator_inbox_and_acks_the_feed(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator.relay.engine.RELAY_MINER_READ_INTERVAL_SECS', 0)
    relay, events, inbox, store = _relay(tmp_path)
    relay.tick(NOW)
    (queued,) = inbox.snapshot()
    assert queued.key == ('vote_set_attestation', (MINER, 'tao')) and queued.payload[:2] == (MINER, 'tao')
    assert store.conn().execute("SELECT slot FROM acks WHERE consumer = 'relay-tao'").fetchone()['slot'] == 9


class _Executors:
    def __init__(self, result):
        self.result, self.sent = result, []

    def __call__(self, client, i):
        self.sent.append(i.key)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _actuator(executor):
    world = Latest()
    world.set(_world())
    act = Actuator(world, [], {'vote_set_attestation': executor}, lambda: SimpleNamespace(rpc=None))
    act.setup()
    return act


def test_a_relay_vote_that_was_already_cast_is_dropped_but_a_failed_one_is_retried():
    noop = _Executors(None)
    act = _actuator(noop)
    act.queue.push(intent('vote_set_attestation', (MINER, 'tao'), None, NOW, (MINER, 'tao', 1, True, 3)))
    act.tick(NOW)
    assert len(act.queue) == 0
    failing = _Executors(RuntimeError('AttestationWouldStrandSwap'))
    act = _actuator(failing)
    act.queue.push(intent('vote_set_attestation', (MINER, 'tao'), None, NOW, (MINER, 'tao', 1, True, 3)))
    act.tick(NOW)
    assert len(act.queue) == 1  # back in the inbox, resting until the hub frees


class _Client:
    program_id = None
    keypair = SimpleNamespace(pubkey=lambda: 'me')

    def __init__(self, voted=False, error=None):
        self.voted, self.error, self.calls = voted, error, []

    def has_voted(self, *args):
        return self.voted

    def vote_set_attestation(self, *args):
        self.calls.append(args)
        if self.error:
            raise RuntimeError(self.error)
        return 'sig'

    def vote_attest_heartbeat(self):
        self.calls.append(())
        return 'sig'


def test_the_attestation_executor_treats_already_voted_as_done_and_a_held_hub_as_a_failure():
    i = intent('vote_set_attestation', (MINER, 'tao'), None, NOW, (MINER, 'tao', 1, True, 3))
    assert vote_set_attestation(_Client(), i) == 'sig'
    assert vote_set_attestation(_Client(error='custom program error: AlreadyVoted'), i) is None
    # A late vote for the value peers just wrote is refused on-chain, and is done, not a failure.
    assert vote_set_attestation(_Client(error='Error Code: AttestationUnchanged'), i) is None
    try:
        vote_set_attestation(_Client(error='AttestationWouldStrandSwap'), i)
    except RuntimeError:
        pass
    else:
        raise AssertionError('a held hub must be retried, not dropped')


def test_a_verdict_the_ledger_refused_holds_the_ack_until_it_lands(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator.relay.engine.RELAY_MINER_READ_INTERVAL_SECS', 0)
    relay, events, _, store = _relay(tmp_path)
    relay.tick(NOW)
    write = relay.ledger.record_relay_slash
    relay.ledger.record_relay_slash = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('disk'))
    events.publish(EventRecord('SwapTimedOut', _timed_out().fields, 20, NOW - 5, 'sig20'))
    relay.feed_status.set(FeedStatus(False, 1, NOW, (20, 'sig20')))
    relay.tick(NOW)
    assert relay._held and store.resume_point(['relay-tao']) != 'sig20'  # a restart replays the verdict
    relay.ledger.record_relay_slash = write
    relay.tick(NOW)
    assert not relay._held and store.resume_point(['relay-tao']) == 'sig20'
    assert relay.has_pending_debit(MINER)


def test_a_swap_fee_recorded_twice_counts_once(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)  # the same event replayed
    assert ledger.fees_after(MINER, 1, 10_000) == 3


def test_an_unreadable_debit_ledger_holds_new_swaps(tmp_path):
    relay, *_ = _relay(tmp_path)

    def boom(*_):
        raise RuntimeError('ledger unreadable')

    relay.ledger.owes_relay_debit = boom
    assert relay.has_pending_debit(MINER) is True


def test_events_the_relay_never_reads_cannot_crowd_out_a_verdict(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator.relay.engine.RELAY_MINER_READ_INTERVAL_SECS', 0)
    relay, events, _, store = _relay(tmp_path, maxsize=4)
    relay.tick(NOW)
    flood = [
        EventRecord('CollateralPosted', {'miner': MINER, 'amount': 1, 'total': 1}, 15, NOW, f'f{i}') for i in range(50)
    ]
    events.publish_many(flood + [EventRecord('SwapTimedOut', _timed_out().fields, 20, NOW - 5, 'sig20')])
    relay.feed_status.set(FeedStatus(False, 1, NOW, (20, 'sig20')))
    relay.tick(NOW)
    assert relay.events.dropped == 0 and relay.has_pending_debit(MINER)  # the verdict got through
    assert store.resume_point(['relay-tao']) == 'sig20'


def test_a_relay_that_lost_events_stops_acking_and_holds_its_heartbeat(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator.relay.engine.RELAY_MINER_READ_INTERVAL_SECS', 0)
    relay, events, _, store = _relay(tmp_path, maxsize=2)
    relay.tick(NOW)
    before = store.resume_point(['relay-tao'])
    lost = [
        EventRecord('SwapTimedOut', _timed_out(bytes([i]) * 32).fields, 20 + i, NOW - 5, f'sig{20 + i}')
        for i in range(5)
    ]
    events.publish_many(lost)
    relay.feed_status.set(FeedStatus(False, 1, NOW, (24, 'sig24')))
    relay.tick(NOW)
    relay.tick(NOW + 60)
    assert relay.events.dropped == 3
    assert store.resume_point(['relay-tao']) == before  # a restart replays what this process never saw
    assert relay.relay._ingest_healthy is False  # the heartbeat stays down, so the hub's entry fuse shuts
