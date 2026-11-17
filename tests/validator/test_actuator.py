from types import SimpleNamespace

from allways.solana import pdas
from allways.validator import hubs
from allways.validator.actuator.actuator import LANDED_HOLD_SECS, Actuator
from allways.validator.actuator.executors import StaleWorld
from allways.validator.actuator.planners import ACTIONS, intent
from allways.validator.runtime.bus import Latest
from allways.validator.runtime.deadline_queue import DeadlineQueue
from allways.validator.world.snapshot import World

NOW = 1_000_000.0


def _world(taken_at=NOW, complete=True):
    return World(taken_at=taken_at, slot=0, complete=complete, config=None, accounts={})


class _Planner:
    name = 'test'

    def __init__(self, intents):
        self.intents = intents

    def plan(self, world, now):
        return list(self.intents)


class _Rpc:
    def __init__(self):
        self.statuses = {}

    def get_signature_statuses(self, sigs):
        return [self.statuses.get(s) for s in sigs]


def _actuator(intents, sent):
    world = Latest()
    world.set(_world())
    rpc = _Rpc()

    def execute(client, it):
        sent.append(it.key)
        return f'sig-{it.key[1]}'

    act = Actuator(world, [_Planner(intents)], {k: execute for k in ACTIONS}, lambda: SimpleNamespace(rpc=rpc))
    act.setup()
    return act, rpc, world


def test_extends_are_sent_before_votes_and_soft_work():
    swap = SimpleNamespace(timeout_at=NOW + 500)
    resv = SimpleNamespace(reserved_until=NOW + 400)
    plans = [
        intent('timeout_swap', 'a', swap, NOW),
        intent('confirm_swap', 'b', swap, NOW),
        intent('extend_reservation', 'c', resv, NOW),
        intent('extend_timeout', 'd', swap, NOW),
    ]
    sent = []
    act, _, _ = _actuator(plans, sent)
    act.tick(NOW)
    assert [k[0] for k in sent] == ['extend_reservation', 'extend_timeout', 'confirm_swap', 'timeout_swap']


def test_an_in_flight_action_is_not_resent_until_it_resolves():
    resv = SimpleNamespace(reserved_until=NOW + 400)
    sent = []
    act, rpc, world = _actuator([intent('extend_reservation', 'c', resv, NOW)], sent)
    act.tick(NOW)
    world.set(_world(NOW + 2))
    act.tick(NOW + 2)
    assert len(sent) == 1  # still pending, not resent
    world.set(_world(NOW + 200))
    act.tick(NOW + 200)  # never seen past expiry → may be sent again
    assert len(sent) == 2


def test_a_stale_world_holds_all_sends():
    sent = []
    act, _, world = _actuator([intent('timeout_swap', 'a', SimpleNamespace(), NOW)], sent)
    world.set(_world(taken_at=NOW - 120))
    act.tick(NOW)
    assert sent == []


def test_ledger_votes_wait_while_the_world_is_incomplete_but_extends_do_not():
    swap = SimpleNamespace(timeout_at=NOW + 500)
    resv = SimpleNamespace(reserved_until=NOW + 400)
    sent = []
    act, _, world = _actuator(
        [intent('confirm_swap', 'b', swap, NOW), intent('extend_reservation', 'c', resv, NOW)], sent
    )
    world.set(_world(complete=False))
    act.tick(NOW)
    assert [k[0] for k in sent] == ['extend_reservation']
    world.set(_world(NOW + 2, complete=True))
    act.tick(NOW + 2)
    assert [k[0] for k in sent] == ['extend_reservation', 'confirm_swap']


def test_a_submitted_intent_whose_send_expired_is_sent_again():
    sent = []
    act, _, world = _actuator([], sent)
    act.queue.push(intent('vote_set_attestation', 'm', SimpleNamespace(), NOW))
    act.tick(NOW)
    world.set(_world(NOW + 2))
    act.tick(NOW + 2)
    assert len(sent) == 1 and len(act.queue) == 0
    world.set(_world(NOW + 200))
    act.tick(NOW + 200)  # never landed: back in the inbox, and out again
    assert len(sent) == 2


def test_hub_registry_matches_the_program_backings():
    assert {h.chain for h in hubs.HUBS} == set(pdas.BACKING_BITS)
    assert [h.chain for h in hubs.relayed_hubs()] == ['tao']
    assert hubs.hub('TAO').backing_bit == pdas.BACKING_BIT_TAO


def test_a_send_the_chain_contradicts_is_dropped_and_its_accounts_re_read():
    world, refreshed, sent = Latest(), [], []
    world.set(_world())

    def stale(client, it):
        sent.append(it.key)
        raise StaleWorld({('Swap', 'pda')})

    act = Actuator(
        world,
        [_Planner([intent('timeout_swap', 'a', SimpleNamespace(), NOW)])],
        {k: stale for k in ACTIONS},
        lambda: SimpleNamespace(rpc=_Rpc()),
        refresh=refreshed.append,
    )
    act.setup()
    act.tick(NOW)
    assert sent == [('timeout_swap', 'a')] and refreshed == [{('Swap', 'pda')}]
    act.tick(NOW + 2)
    assert len(sent) == 1  # rests like any send that went nowhere, until the World is current again


def test_a_landed_vote_is_not_re_sent_while_the_world_catches_up():
    sent = []
    act, rpc, world = _actuator([intent('vote_initiate', 'a', SimpleNamespace(reserved_until=NOW + 500), NOW)], sent)
    act.tick(NOW)
    rpc.statuses['sig-a'] = {'err': None, 'confirmationStatus': 'confirmed'}
    act.tick(NOW + 2)  # landed; the World still shows the swap pending, so the planner asks again
    act.tick(NOW + 4)
    assert sent == [('vote_initiate', 'a')]
    world.set(_world(taken_at=NOW + 2 + LANDED_HOLD_SECS))
    act.tick(NOW + 2 + LANDED_HOLD_SECS)  # still planned after the hold: the World says it didn't take
    assert len(sent) == 2


def test_writes_pushed_into_the_shared_inbox_after_boot_are_sent():
    # The inbox is empty when the actuator is built (it always is at boot), and the relays push later.
    world, inbox, sent = Latest(), DeadlineQueue(), []
    world.set(_world())

    def execute(client, it):
        sent.append(it.key)
        return f'sig-{it.key[1]}'

    act = Actuator(world, [], {k: execute for k in ACTIONS}, lambda: SimpleNamespace(rpc=_Rpc()), inbox)
    act.setup()
    inbox.push(intent('vote_set_attestation', ('miner', 'tao'), None, NOW, payload=('miner', 'tao', 1, True, 1)))
    act.tick(NOW)
    assert sent == [('vote_set_attestation', ('miner', 'tao'))]


def test_a_submitted_write_queued_behind_an_in_flight_one_is_sent_once_it_lands():
    sent = []
    act, rpc, world = _actuator([], sent)
    act.queue.push(intent('vote_set_attestation', 'm', None, NOW, payload=('m', 'tao', 100, True, 7)))
    act.tick(NOW)
    # The relay nets a verdict: a new payload under the same key while the first is still in flight.
    act.queue.push(intent('vote_set_attestation', 'm', None, NOW + 2, payload=('m', 'tao', 80, True, 7)))
    world.set(_world(NOW + 2))
    act.tick(NOW + 2)
    assert len(sent) == 1 and len(act.queue) == 1  # held in the inbox, not dropped
    rpc.statuses['sig-m'] = {'err': None, 'confirmationStatus': 'confirmed'}
    world.set(_world(NOW + 4 + LANDED_HOLD_SECS))
    act.tick(NOW + 4)  # the first lands
    act.tick(NOW + 4 + LANDED_HOLD_SECS)
    assert len(sent) == 2 and len(act.queue) == 0


def test_a_submitted_write_that_failed_on_chain_is_retried_after_its_rest():
    sent = []
    act, rpc, world = _actuator([], sent)
    act.queue.push(intent('vote_attest_heartbeat', 'config', None, NOW))
    act.tick(NOW)
    rpc.statuses['sig-config'] = {'err': {'InstructionError': [0, 'Custom']}}
    act.tick(NOW + 2)
    assert len(sent) == 1 and len(act.queue) == 1  # back in the inbox, resting
    retry = ACTIONS['vote_attest_heartbeat'].retry_secs
    world.set(_world(NOW + 2 + retry))
    act.tick(NOW + 2 + retry - 1)
    assert len(sent) == 1
    act.tick(NOW + 2 + retry)
    assert len(sent) == 2
