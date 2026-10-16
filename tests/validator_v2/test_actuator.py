from types import SimpleNamespace

from allways.solana import pdas
from allways.validator_v2 import hubs
from allways.validator_v2.actuator.actuator import Actuator
from allways.validator_v2.actuator.planners import ACTIONS, intent
from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.deadline_queue import Urgency
from allways.validator_v2.world.snapshot import World

NOW = 1_000_000.0


def _world(taken_at=NOW):
    return World(taken_at=taken_at, config=None, accounts={}, read_at={})


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


def test_action_table_urgencies():
    assert {k for k, a in ACTIONS.items() if a.urgency is Urgency.EXTEND} == {'extend_reservation', 'extend_timeout'}
    assert intent('finalize_reservation', 'x', SimpleNamespace(finalize_by=123), NOW).deadline == 123


def test_hub_registry_matches_the_program_backings():
    assert {h.chain for h in hubs.HUBS} == set(pdas.BACKING_BITS)
    assert [h.chain for h in hubs.relayed_hubs()] == ['tao']
    assert hubs.hub('TAO').backing_bit == pdas.BACKING_BIT_TAO
