"""A vote or crank that loses the race to its peers is routine with more voters than quorum needs, so it logs at
debug; anything else a send hits (VoteHashMismatch above all) stays a warning."""

from types import SimpleNamespace

import bittensor as bt
import pytest

from allways.assets import asset
from allways.solana.client import benign_marker
from allways.solana.rpc import SolanaRpc
from allways.validator.actuator.actuator import Actuator
from allways.validator.actuator.planners import ACTIONS, intent
from allways.validator.runtime.bus import Latest
from allways.validator.world.snapshot import World

NOW = 1_000_000.0
# The shapes a refusal reaches the actuator in: a pre-flight simulation reject, and a landed failed tx.
NOT_PENDING_PREFLIGHT = 'Transaction simulation failed: Error processing Instruction 0: custom program error: 0x178f'
CLOSED_LANDED = {'InstructionError': [0, {'Custom': 3012}]}
HASH_MISMATCH_PREFLIGHT = 'Transaction simulation failed: Error processing Instruction 0: custom program error: 0x177d'
# What the intent is about: the deadlines the planner reads off it.
ACCT = SimpleNamespace(reserved_until=NOW + 60, timeout_at=NOW + 60)


@pytest.fixture
def logged(monkeypatch):
    lines = {'warning': [], 'debug': []}
    for level in lines:
        monkeypatch.setattr(bt.logging, level, lambda msg, *a, _l=level, **kw: lines[_l].append(msg))
    return lines


def test_benign_marker_reads_every_form_of_the_code():
    assert benign_marker(Exception(NOT_PENDING_PREFLIGHT), ('NotPending',)) == 'NotPending'
    assert benign_marker(Exception(str(CLOSED_LANDED)), ('AccountNotInitialized',)) == 'AccountNotInitialized'
    assert benign_marker(Exception(HASH_MISMATCH_PREFLIGHT), ('NotPending', 'AccountNotInitialized')) is None


class _Rpc:
    def __init__(self):
        self.statuses = {}

    def get_signature_statuses(self, sigs):
        return [self.statuses.get(s) for s in sigs]


def _actuator(intents, execute):
    world = Latest()
    world.set(World(taken_at=NOW, slot=0, complete=True, config=None, accounts={}))
    rpc = _Rpc()

    class _Planner:
        name = 'test'

        def plan(self, w, now):
            return list(intents)

    act = Actuator(world, [_Planner()], {k: execute for k in ACTIONS}, lambda: SimpleNamespace(rpc=rpc))
    act.setup()
    return act, rpc


def _raise(text):
    def execute(client, it):
        raise RuntimeError(text)

    return execute


def test_a_late_initiate_is_quiet(logged):
    act, _ = _actuator([intent('vote_initiate', 'aa' * 16, ACCT, NOW)], _raise(NOT_PENDING_PREFLIGHT))
    act.tick(NOW)
    assert not logged['warning'] and any('a peer got there first (NotPending)' in m for m in logged['debug'])


def test_a_hash_mismatch_still_warns(logged):
    act, _ = _actuator([intent('vote_set_attestation', 'miner', ACCT, NOW)], _raise(HASH_MISMATCH_PREFLIGHT))
    act.tick(NOW)
    assert any('send failed' in m for m in logged['warning'])


def test_the_same_refusal_on_another_action_still_warns(logged):
    # NotPending is a lost race for an initiate, not for an attestation.
    act, _ = _actuator([intent('vote_set_attestation', 'miner', ACCT, NOW)], _raise(NOT_PENDING_PREFLIGHT))
    act.tick(NOW)
    assert any('send failed' in m for m in logged['warning'])


def test_a_confirm_that_lands_after_the_swap_closed_is_quiet(logged):
    key = 'bb' * 16
    act, rpc = _actuator([intent('confirm_swap', key, ACCT, NOW)], lambda client, it: f'sig-{it.key[1]}')
    act.tick(NOW)
    rpc.statuses[f'sig-{key}'] = {'err': CLOSED_LANDED}
    act.tick(NOW + 2)
    assert not logged['warning'] and any('landed after a peer got there first' in m for m in logged['debug'])


def test_a_verdict_on_a_tx_warns_once(logged):
    message = 'verify_transaction: sender mismatch on tx 61WChZfMmdZ7vdz5... (test)'
    for _ in range(3):
        asset.warn_once(message)
    assert logged['warning'] == [message] and logged['debug'] == [message, message]


def test_an_unreachable_rpc_never_logs_its_key():
    rpc = SolanaRpc('http://127.0.0.1:9/?api-key=be1aa4f7-secret', timeout=1)
    with pytest.raises(Exception) as e:
        rpc._call('getSlot', [])
    assert 'secret' not in str(e.value) and 'api-key=***' in str(e.value)
