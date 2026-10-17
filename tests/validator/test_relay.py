"""The hub relay as a component: its fee ledger without full history, and what it hands the actuator."""

from types import SimpleNamespace

from solders.keypair import Keypair

from allways.solana import pdas
from allways.solana.events import EventRecord
from allways.validator.actuator.actuator import Actuator
from allways.validator.actuator.executors import vote_attest_heartbeat, vote_set_attestation
from allways.validator.actuator.planners import intent
from allways.validator.feed.events import FeedStatus
from allways.validator.hubs import hub
from allways.validator.relay.component import HubRelay
from allways.validator.relay.engine import RelayConfig
from allways.validator.relay.ledger import RelayLedger
from allways.validator.runtime.bus import Latest, Topic
from allways.validator.runtime.deadline_queue import DeadlineQueue
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World

CADENCE = 1000
NOW = 10_500  # the last fee boundary is 10_000
MINER, HOTKEY = str(Keypair().pubkey()), 'hk'


def _ledger(tmp_path, settled=0, now=NOW):
    clock = {'now': now}
    vault = {'settled': settled}
    ledger = RelayLedger(
        Store(str(tmp_path / 'r.db')),
        'tao',
        settled=lambda miner: vault['settled'],
        boundary=lambda t: (t // CADENCE) * CADENCE,
        clock=lambda: clock['now'],
    )
    return ledger, vault, clock


def test_a_fresh_ledger_seeds_from_the_vault_and_counts_only_fees_after_the_boundary(tmp_path):
    ledger, *_ = _ledger(tmp_path, settled=7)
    ledger.record_relay_fee('a', MINER, 'tao', 5, 9_900, vault_generation=1)  # before the boundary: in settled
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 7 + 3
    assert ledger.accrued_fee_total(MINER, 'tao', at_time=10_100, vault_generation=1) == 7


def test_a_short_count_heals_once_the_vault_settles_the_boundary(tmp_path):
    ledger, vault, clock = _ledger(tmp_path, settled=4)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 7
    clock['now'], vault['settled'] = 11_050, 12  # the 11_000 round settled 5 fees this ledger never saw
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 12
    vault['settled'] = 12
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 12  # healed once, never again


def test_an_exit_residual_settled_past_the_boundary_is_not_counted_twice(tmp_path):
    ledger, vault, _ = _ledger(tmp_path, settled=0)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 3
    ledger.set_relay_meta(f'exit:{MINER}', '1')
    vault['settled'] = 3  # the residual settle covers the fee after the boundary
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 3


def test_folding_keeps_the_totals_and_drops_the_rows(tmp_path):
    ledger, *_ = _ledger(tmp_path, settled=7)
    ledger.record_relay_fee('b', MINER, 'tao', 3, 10_200, vault_generation=1)
    ledger.record_relay_fee('c', MINER, 'tao', 2, 10_400, vault_generation=1)
    ledger.record_relay_fee('d', 'unseeded', 'tao', 9, 10_300, vault_generation=1)
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 12
    ledger.fold(10_300)
    assert ledger.accrued_fee_total(MINER, 'tao', vault_generation=1) == 12
    rows = ledger.store.conn().execute('SELECT swap_key FROM relay_tao_fees ORDER BY swap_key').fetchall()
    assert [r['swap_key'] for r in rows] == ['c', 'd']  # an unseeded miner's rows wait for its seed


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

    def get_lock_state(self, hotkey):
        return (True, 3)

    def is_slashed(self, swap_ref):
        return False


def _world():
    miner_state = SimpleNamespace(
        miner=MINER, active_backings=pdas.BACKING_BIT_TAO, has_active_swap=False, busy_until=0, settling_until=0
    )
    return World(
        taken_at=NOW,
        slot=10,
        complete=True,
        config=SimpleNamespace(last_attest_heartbeat=NOW, vault_generation=1),
        accounts={'MinerState': {str(pdas.miner_state_pda(MINER)): miner_state}, 'BondAttestation': {}},
        attribution={MINER: HOTKEY},
    )


def _relay(tmp_path):
    world, status = Latest(), Latest()
    world.set(_world())
    status.set(FeedStatus(False, 1, NOW, (9, 'sig9')))
    events, inbox = Topic('events'), DeadlineQueue()
    store = Store(str(tmp_path / 'v.db'))
    relay = HubRelay(
        hub('tao'),
        world,
        events.subscribe('relay-tao'),
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


def test_a_verdict_it_cannot_write_is_held_with_the_heartbeat(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator.relay.engine.RELAY_MINER_READ_INTERVAL_SECS', 0)
    relay, events, _, _ = _relay(tmp_path)
    relay.tick(NOW)
    broken = relay.ledger.record_relay_slash
    relay.ledger.record_relay_slash = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('disk'))
    events.publish(_timed_out())
    relay.tick(NOW)
    assert relay._held and relay.relay._ingest_healthy is False
    relay.ledger.record_relay_slash = broken
    relay.tick(NOW)
    assert not relay._held and relay.has_pending_debit(MINER)


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
    try:
        vote_set_attestation(_Client(error='AttestationWouldStrandSwap'), i)
    except RuntimeError:
        pass
    else:
        raise AssertionError('a held hub must be retried, not dropped')


def test_the_heartbeat_executor_skips_a_round_it_already_voted_in():
    i = intent('vote_attest_heartbeat', 'config', None, NOW)
    voted = _Client(voted=True)
    assert vote_attest_heartbeat(voted, i) is None and voted.calls == []
    assert vote_attest_heartbeat(_Client(), i) == 'sig'
