"""The lines an operator watches for: a signing wallet running low, and whether the validator has caught up on
history (attest and confirm wait until it has)."""

from types import SimpleNamespace

from allways.validator.actuator import actuator as actuator_mod
from allways.validator.actuator.actuator import Actuator, _out_of_funds
from allways.validator.actuator.planners import ACTIONS
from allways.validator.actuator.swap_context import SwapMemory
from allways.validator.feed.events import HISTORY_HORIZON_SECS
from allways.validator.runtime import logs as logs_mod
from allways.validator.runtime.bus import Latest
from allways.validator.runtime.logs import LowBalance
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World

NOW = 1_000_000.0
LOW = 100


def _capture(monkeypatch, module):
    out = []
    monkeypatch.setattr(module.bt.logging, 'warning', lambda m, *a, **k: out.append(('warning', m)))
    monkeypatch.setattr(module.bt.logging, 'info', lambda m, *a, **k: out.append(('info', m)))
    return out


def _watch():
    return LowBalance('test wallet', 'SOL', 1, LOW, check_secs=60, remind_secs=3600)


def test_a_low_wallet_warns_once_then_reminds_and_says_when_topped_up(monkeypatch):
    out = _capture(monkeypatch, logs_mod)
    watch, balance = _watch(), [50]
    read = lambda: balance[0]  # noqa: E731
    watch.maybe_check(NOW, read)
    assert [k for k, _ in out] == ['warning']
    for t in range(1, 59):  # still low, read every minute: no repeat inside the reminder interval
        watch.maybe_check(NOW + 60 * t, read)
    assert len(out) == 1
    watch.maybe_check(NOW + 3600, read)
    assert [k for k, _ in out] == ['warning', 'warning']
    balance[0] = 110  # above the line but inside the headroom: no flap
    watch.maybe_check(NOW + 3660, read)
    assert len(out) == 2
    balance[0] = 130
    watch.maybe_check(NOW + 3720, read)
    assert out[-1][0] == 'info' and 'topped up' in out[-1][1]


def test_a_healthy_wallet_is_silent_and_read_at_most_once_per_interval(monkeypatch):
    out = _capture(monkeypatch, logs_mod)
    watch, reads = _watch(), []
    for t in range(0, 120, 2):
        watch.maybe_check(NOW + t, lambda: reads.append(1) or 500)
    assert out == [] and len(reads) == 2


def test_recheck_reads_at_once_and_an_unreadable_balance_is_not_low(monkeypatch):
    out = _capture(monkeypatch, logs_mod)
    watch = _watch()
    watch.maybe_check(NOW, lambda: None)
    watch.maybe_check(NOW + 1, lambda: 1 / 0)
    assert out == []
    watch.maybe_check(NOW + 2, lambda: 5)
    assert out == []  # inside the read interval
    watch.recheck()
    watch.maybe_check(NOW + 3, lambda: 5)
    assert [k for k, _ in out] == ['warning']


def test_only_a_shortage_of_lamports_counts_as_out_of_funds():
    assert _out_of_funds(
        'Transaction simulation failed: Attempt to debit an account but found no record of a prior credit.'
    )
    assert _out_of_funds(RuntimeError('insufficient funds for fee'))
    assert _out_of_funds('custom program error: insufficient lamports 10, need 20')
    assert not _out_of_funds('custom program error: 0x1773 InsufficientCollateral')


def _world(complete):
    return World(taken_at=NOW, slot=0, complete=complete, config=None, accounts={})


def test_the_actuator_says_when_it_is_catching_up_and_when_it_is_caught_up(monkeypatch):
    out = _capture(monkeypatch, actuator_mod)
    world = Latest()
    rpc = SimpleNamespace(get_signature_statuses=lambda sigs: [None] * len(sigs), get_balance=lambda pk: 10**9)
    client = SimpleNamespace(rpc=rpc, keypair=SimpleNamespace(pubkey=lambda: 'me'))
    act = Actuator(world, [], {k: (lambda c, i: None) for k in ACTIONS}, lambda: client)
    act.setup()
    for complete in (False, False, True, True, False, True):
        world.set(_world(complete))
        act.tick(NOW)
    lines = [m for k, m in out if 'history' in m]
    assert len(lines) == 4
    assert 'held' in lines[0] and 'enabled' in lines[1] and 'held' in lines[2] and 'enabled' in lines[3]


def test_the_actuator_warns_when_its_solana_key_runs_low(monkeypatch):
    out = _capture(monkeypatch, logs_mod)
    world = Latest()
    world.set(_world(True))
    rpc = SimpleNamespace(get_signature_statuses=lambda sigs: [None] * len(sigs), get_balance=lambda pk: 1_000)
    client = SimpleNamespace(rpc=rpc, keypair=SimpleNamespace(pubkey=lambda: 'me'))
    act = Actuator(world, [], {k: (lambda c, i: None) for k in ACTIONS}, lambda: client)
    act.setup()
    act.tick(NOW)
    act.tick(NOW + 2)
    warnings = [m for k, m in out if k == 'warning']
    assert len(warnings) == 1 and 'Solana voting key' in warnings[0]


def test_collateral_verdicts_older_than_the_horizon_are_pruned(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    memory = SwapMemory(store)
    now = int(NOW)
    memory.record_collateral(('m', 'tao', now - HISTORY_HORIZON_SECS - 1, 5), True)
    memory.record_collateral(('m', 'tao', now - 60, 5), False)
    memory.prune(now)
    assert list(memory.collateral) == [('m', 'tao', now - 60, 5)]
    assert list(SwapMemory(store).collateral) == [('m', 'tao', now - 60, 5)]  # gone from disk too
