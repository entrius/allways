"""Serving as a component: request reads answered from the World, outcomes kept from events, and the seam
answering a live request off both."""

import json
import socket
import urllib.request
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair

from allways.solana import pdas
from allways.solana.events import EventRecord
from allways.validator_v2.actuator.planners import SwapReport
from allways.validator_v2.actuator.swap_context import SwapMemory
from allways.validator_v2.app import Config, build
from allways.validator_v2.chains.subtensor_view import Head
from allways.validator_v2.feed.events import FeedStatus
from allways.validator_v2.runtime.bus import Latest, Topic
from allways.validator_v2.serving import requests
from allways.validator_v2.serving.component import Serving
from allways.validator_v2.serving.store import ServingStore
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import Accounts, DepositClaim, PayoutRoute, World, tx_key

NOW = 1_000_000
MINER = Keypair().pubkey()
HOTKEY = bt.Keypair.create_from_seed('0x' + '11' * 32)


def _reservation(**over):
    fields = dict(
        reserved_until=NOW + 600,
        finalize_by=0,
        claimed_swap_key=b'\x00' * 32,
        from_chain='btc',
        to_chain='sol',
        from_amount=100_000,
        to_amount=5_000,
        miner_from_addr='minerBTC',
        from_addr='userBTC',
        user='User1111',
        collateral_chain='sol',
        collateral_amount=5_000,
        created_at=NOW - 10,
    )
    fields.update(over)
    return SimpleNamespace(**fields)


def _world(reservation=None, **ledgers):
    binding = SimpleNamespace(miner=MINER, hotkey=HOTKEY.public_key, hotkey_sig=HOTKEY.sign(bytes(MINER)), bound_at=1)
    accounts = {'Binding': {str(pdas.binding_pda(MINER)): binding}, 'Reservation': {}, 'Swap': {}}
    if reservation is not None:
        accounts['Reservation'][str(pdas.reservation_pda(MINER, 'sol'))] = reservation
    return World(
        taken_at=NOW,
        slot=5,
        complete=True,
        config=SimpleNamespace(halted=False),
        accounts=accounts,
        attribution={str(MINER): HOTKEY.ss58_address},
        **ledgers,
    )


def _latest(value):
    latest = Latest()
    latest.set(value)
    return latest


def test_requests_read_accounts_from_the_world_and_resolve_hotkeys_by_its_verified_bindings():
    accounts = Accounts(_world(_reservation()))
    assert accounts.get_reservation(MINER, 'sol').from_amount == 100_000
    assert accounts.get_reservation(MINER, 'tao') is None
    assert accounts.get_config().halted is False
    assert accounts.miner_for(HOTKEY.ss58_address) == MINER
    assert accounts.miner_for(bt.Keypair.create_from_seed('0x' + '22' * 32).ss58_address) is None


def _store(tmp_path, world):
    store = Store(str(tmp_path / 'v.db'))
    return ServingStore(store, lambda: world, SwapMemory(store), {}), store


def test_outcomes_and_deliveries_are_kept_from_events_after_the_swap_closes(tmp_path):
    serving, _ = _store(tmp_path, _world())
    key = b'\x07' * 32
    serving.ingest(EventRecord('SwapFulfilled', {'swap_key': key, 'to_tx_hash': 'dest-tx'}, 1, NOW - 60, 's1'))
    timed_out = {'swap_key': key, 'collateral_chain': 'sol', 'reimbursement': 42}
    serving.ingest(EventRecord('SwapTimedOut', timed_out, 2, NOW, 'sig-refund'))
    assert serving.get_swap_outcome(key.hex()) == 'timed_out'
    assert serving.get_swap_fulfillment(key.hex()) == 'dest-tx'
    assert serving.get_swap_refund(key.hex()) == {
        'refund_amount': 42,
        'refund_chain': 'sol',
        'refund_tx_hash': 'sig-refund',
    }
    serving.prune(NOW + 8 * 86400)
    assert serving.get_swap_outcome(key.hex()) is None


def test_deposit_and_payout_facts_come_from_the_world(tmp_path):
    world = _world(
        deposits={tx_key('AB' * 32): DepositClaim('ff' * 32, 1, NOW, initiated=True)},
        routes={'k': PayoutRoute('btc', 'Sender', 'Wallet', NOW)},
    )
    serving, _ = _store(tmp_path, world)
    assert serving.deposit_claim('ab' * 32) == ('ff' * 32, True)
    serving.record_deposit_claim('def', 'ee' * 32, NOW)  # this validator's own claim, until the World has it
    assert serving.deposit_claim('def') == ('ee' * 32, False)
    assert serving.is_payout_route('btc', 'Sender', 'Wallet')


def _free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _serving(tmp_path, world, report=None):
    store = Store(str(tmp_path / 's.db'))
    world_latest = _latest(world)
    status = _latest(FeedStatus(False, 1, NOW, (9, 'sig9')))
    metagraph = SimpleNamespace(hotkeys=[HOTKEY.ss58_address], S=[1.0], n=SimpleNamespace(item=lambda: 1))
    serving = Serving(
        world_latest,
        _latest(Head(1, NOW, metagraph, NOW)),
        Topic('events').subscribe('serving'),
        status,
        store,
        ServingStore(store, lambda: world, SwapMemory(store), {}),
        lambda: SimpleNamespace(keypair=Keypair()),
        {},
        _latest(report or {}),
        {},
        lambda: None,
        seam=(_free_port(), 'secret'),
    )
    serving.setup()
    return serving


def _get(serving, path):
    request = urllib.request.Request(f'http://127.0.0.1:{serving.seam[0]}{path}', headers={'X-Seam-Secret': 'secret'})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def test_the_seam_answers_status_off_the_world(tmp_path, monkeypatch):
    monkeypatch.setattr('allways.validator_v2.serving.requests.time.time', lambda: NOW)
    serving = _serving(tmp_path, _world(_reservation()))
    try:
        status = _get(serving, f'/status?miner_hotkey={HOTKEY.ss58_address}')
        assert status['stage'] == 'reserved' and status['detail']['from_amount'] == 100_000
    finally:
        serving._server.shutdown()


def test_status_reports_leg_confirmations_and_a_refusal_from_the_swap_planner(tmp_path, monkeypatch):
    monkeypatch.setattr(requests, 'attest_reject_reason', lambda *a: None)  # the offline gates pass
    report = {
        'aa' * 32: SwapReport(
            'PendingAttestation', 'reject', 'stale deposit', {'btc': (2, 3), 'sol': (0, 32)}, 'btc', NOW
        ),
    }
    serving = _serving(tmp_path, _world(), report)
    try:
        detail = {}
        requests._attach_leg_confs(serving, bytes.fromhex('aa' * 32), detail)
        assert detail['leg_confs'] == {
            'source': {'have': 2, 'need': 3, 'at': NOW},
            'dest': {'have': 0, 'need': 32, 'at': NOW},
        }
        requests._add_reject_reason(serving, SimpleNamespace(), bytes.fromhex('aa' * 32), 'claimed', detail)
        assert detail['reject_reason'] == 'stale deposit'
    finally:
        serving._server.shutdown()


def test_serving_runs_only_with_the_axon_or_the_seam(tmp_path):
    base = dict(solana_rpc_url='http://unused', program_id=None, keypair=Keypair(), netuid=19)
    (tmp_path / 'a').mkdir()
    (tmp_path / 'b').mkdir()
    app = build(Config(state_dir=str(tmp_path / 'a'), **base), assets={}, subtensor_factory=lambda: None)
    assert 'serving' not in {c.name for c in app.components}
    app = build(Config(state_dir=str(tmp_path / 'b'), seam=(0, 's'), **base), assets={}, subtensor_factory=lambda: None)
    assert 'serving' in {c.name for c in app.components}
