"""The production swap path: decisions over the World and the chain watchers, and the planner's intents."""

from types import SimpleNamespace

import pytest
from solders.keypair import Keypair

from allways.chains import get_chain_def
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.actuator.planners import SwapPlanner
from allways.validator.actuator.swap_context import SwapMemory
from allways.validator.chains.watcher import Verdict
from allways.validator.runtime.deadline_queue import Urgency
from allways.validator.store.db import Store
from allways.validator.world.snapshot import DepositClaim, Fulfillment, World, tx_key

MINER, USER = Keypair().pubkey(), Keypair().pubkey()
NOW = 10_000
SRC_TX, DST_TX = 'aa' * 32, 'bb' * 32


class FakeProvider:
    """Offline chain rules only; every on-chain question goes through the (fake) watcher."""

    chain_def = SimpleNamespace(replay_grace_secs=0)

    @property
    def chain(self):
        return self

    def is_valid_address(self, address):
        return True

    def normalize_address(self, address):
        return address.lower()

    def canonical_tx_hash(self, tx_hash):
        return tx_hash


def _status(name):
    return type(name, (), {})()


def _swap(status='Fulfilled', timeout_at=NOW + 600, max_extend_at=NOW + 9000, src=SRC_TX, collateral_chain='sol'):
    return SimpleNamespace(
        miner=MINER,
        user=USER,
        from_chain='btc',
        to_chain='sol',
        user_from_addr='userBTC',
        user_to_addr='userSOL',
        miner_from_addr='minerBTC',
        miner_to_addr='minerSOL',
        rate='5',
        collateral_amount=1000,
        from_amount=500,
        to_amount=1000,
        from_tx_hash=src,
        from_tx_block=0,
        to_tx_hash=DST_TX,
        to_tx_block=0,
        status=_status(status),
        initiated_at=NOW - 100,
        fulfilled_at=0,
        timeout_at=timeout_at,
        max_extend_at=max_extend_at,
        collateral_chain=collateral_chain,
    )


def _reservation(created_at=NOW - 200, reserved_until=NOW + 600, max_extend_at=NOW + 9000, src=SRC_TX):
    return SimpleNamespace(
        created_at=created_at,
        reserved_until=reserved_until,
        max_extend_at=max_extend_at,
        claimed_swap_key=swap_key_from_tx_hash(src),
    )


class _Watcher:
    """Answers each probe by name: a Verdict, or None for 'asked but not answered yet'."""

    def __init__(self, **answers):
        self.answers = answers
        self.asked = []

    def verdict(self, probe):
        self.asked.append(probe.name)
        return self.answers.get(probe.name)


def _ok(confirmed=True, block_time=NOW - 50, checked_at=NOW):
    info = SimpleNamespace(confirmed=confirmed, block_time=block_time, confirmations=6 if confirmed else 0)
    return Verdict(1, checked_at, info=info, settled=confirmed)


def _world(*swaps, reservation=None, deposits=None, fulfillments=None, complete=True):
    accounts = {'Swap': {f'swap{i}': s for i, s in enumerate(swaps)}, 'Reservation': {}}
    if reservation is not None:
        accounts['Reservation'][str(pdas.reservation_pda(MINER, 'sol'))] = reservation
    return World(
        taken_at=NOW,
        slot=1,
        complete=complete,
        config=None,
        accounts=accounts,
        deposits=deposits or {},
        fulfillments=fulfillments or {},
    )


def _planner(tmp_path, btc=None, sol=None, db='v.db'):
    providers = {'btc': FakeProvider(), 'sol': FakeProvider()}
    watchers = {'btc': btc or _Watcher(), 'sol': sol or _Watcher()}
    return SwapPlanner(watchers, providers, SwapMemory(Store(str(tmp_path / db))))


def _decision(planner, world):
    intents = planner.plan(world, NOW)
    (report,) = planner.report.get()[0].values()
    return report, intents


def test_a_verified_delivery_confirms(tmp_path):
    planner = _planner(tmp_path, sol=_Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW)))
    report, (intent,) = _decision(planner, _world(_swap()))
    assert report.decision == 'confirm'
    assert intent.key[0] == 'confirm_swap' and intent.urgency is Urgency.VOTE and intent.deadline == NOW + 600
    assert report.legs == {'sol': (6, get_chain_def('sol').min_confirmations)}  # the dest leg, for the seam


def test_an_unanswered_leg_never_becomes_a_timeout_even_past_the_ceiling(tmp_path):
    planner = _planner(tmp_path)  # the watcher has not answered anything yet
    swap = _swap(timeout_at=NOW - 10_000, max_extend_at=NOW - 1)
    report, intents = _decision(planner, _world(swap))
    assert report.decision == 'skip' and intents == []


def test_an_unreachable_dest_past_the_ceiling_times_out_but_a_bug_does_not(tmp_path):
    swap = _swap(timeout_at=NOW - 10_000, max_extend_at=NOW - 1)
    down = Verdict(1, NOW, error='timed out', unreachable=True)
    report, _ = _decision(_planner(tmp_path, sol=_Watcher(verify=down)), _world(swap))
    assert report.decision == 'timeout'
    bug = Verdict(1, NOW, error="KeyError: 'meta'")
    report, _ = _decision(_planner(tmp_path, sol=_Watcher(verify=bug), db='b.db'), _world(swap))
    assert report.decision == 'skip'


def test_a_not_found_checked_before_the_deadline_does_not_slash(tmp_path):
    swap = _swap(timeout_at=NOW - 5)
    early = Verdict(1, NOW - 30, info=None)  # read 25s before the deadline passed
    planner = _planner(
        tmp_path,
        sol=_Watcher(verify=early, cancel_evidence=Verdict(1, NOW), delivery_refused=Verdict(1, NOW, info=False)),
    )
    report, intents = _decision(planner, _world(swap))
    assert report.decision == 'wait' and 'after the deadline' in report.reason and intents == []
    planner.watchers['sol'].answers['verify'] = Verdict(1, NOW, info=None)  # re-read after the deadline
    report, (intent,) = _decision(planner, _world(swap))
    assert report.decision == 'timeout' and intent.key[0] == 'timeout_swap'


def test_a_stale_confirmed_delivery_times_out_instead_of_waiting_forever(tmp_path):
    """A confirmed read is final and never re-checked, so it must not wait on a re-read after the deadline."""
    swap = _swap(timeout_at=NOW - 5)
    stale = _ok(block_time=NOW - 500, checked_at=NOW - 30)  # mined before initiated_at, read before the deadline
    planner = _planner(
        tmp_path,
        sol=_Watcher(verify=stale, cancel_evidence=Verdict(1, NOW), delivery_refused=Verdict(1, NOW, info=False)),
    )
    report, (intent,) = _decision(planner, _world(swap))
    assert report.decision == 'timeout' and intent.key[0] == 'timeout_swap'


def test_a_deposit_another_swap_initiated_on_is_rejected(tmp_path):
    other = DepositClaim(swap_key='ff' * 32, slot=1, seen_at=NOW, initiated=True)
    planner = _planner(tmp_path, btc=_Watcher(verify=_ok()))
    world = _world(_swap('PendingAttestation'), reservation=_reservation(), deposits={tx_key(SRC_TX): other})
    report, intents = _decision(planner, world)
    assert report.decision == 'reject' and 'already claimed' in report.reason and intents == []


def test_an_earlier_claim_that_never_initiated_does_not_hold_a_canonical_deposit(tmp_path):
    # Another key on the same tx_key is another spelling of it, which never attests (corpus C02).
    other = DepositClaim(swap_key='ff' * 32, slot=1, seen_at=NOW)
    planner = _planner(tmp_path, btc=_Watcher(verify=_ok()))
    world = _world(_swap('PendingAttestation'), reservation=_reservation(), deposits={tx_key(SRC_TX): other})
    report, _ = _decision(planner, world)
    assert report.decision == 'attest'


@pytest.mark.parametrize('initiated', [False, True])
def test_a_delivery_some_swap_claimed_as_its_deposit_never_confirms(tmp_path, initiated):
    # The claim's row alone decides it, initiated or not: that tx pays the claiming swap, not this one.
    deposits = {tx_key(DST_TX): DepositClaim('ee' * 32, 1, NOW, initiated=initiated)}
    planner = _planner(tmp_path, sol=_Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW)))
    report, intents = _decision(planner, _world(_swap(), deposits=deposits))
    assert report.decision == 'wait' and 'source deposit' in report.reason and intents == []
    planner = _planner(tmp_path, sol=_Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW)), db='v2.db')
    report, _ = _decision(planner, _world(_swap(timeout_at=NOW - 1), deposits=deposits))
    assert report.decision == 'timeout'


def test_a_fresh_verified_deposit_attests_against_the_reservation_deadline(tmp_path):
    planner = _planner(tmp_path, btc=_Watcher(verify=_ok()))
    report, (intent,) = _decision(planner, _world(_swap('PendingAttestation'), reservation=_reservation()))
    assert report.decision == 'attest'
    assert intent.key[0] == 'vote_initiate' and intent.deadline == NOW + 600


def test_a_delivery_another_swap_completed_on_cannot_confirm(tmp_path):
    spent = Fulfillment(DST_TX.upper(), NOW, str(MINER), settled=True)
    planner = _planner(tmp_path, sol=_Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW)))
    report, intents = _decision(planner, _world(_swap(), fulfillments={'ee' * 32: spent}))
    assert report.decision == 'wait' and 'another swap' in report.reason and intents == []


def test_the_first_verified_swap_keeps_a_shared_delivery_across_a_restart(tmp_path):
    sol = _Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW))
    first, second = _swap(src='01' * 32), _swap(src='02' * 32)
    planner = _planner(tmp_path, sol=sol)
    assert _decision(planner, _world(first))[0].decision == 'confirm'
    again = _planner(tmp_path, sol=sol)  # same database, new process
    report, _ = _decision(again, _world(second))
    assert report.decision == 'wait' and 'another swap' in report.reason


# ── while the World catches up ───────────────────────────────────────────────


def test_a_verified_claim_is_extended_not_attested_while_the_world_catches_up(tmp_path):
    planner = _planner(tmp_path, btc=_Watcher(verify=_ok()))
    claim = _swap('PendingAttestation')
    near = _world(claim, reservation=_reservation(reserved_until=NOW + 60), complete=False)
    report, (intent,) = _decision(planner, near)
    assert report.decision == 'extend_reservation' and 'catching up' in report.reason
    assert intent.key[0] == 'extend_reservation' and intent.payload.action.target_at > NOW + 60
    far = _world(claim, reservation=_reservation(reserved_until=NOW + 600), complete=False)
    report, intents = _decision(planner, far)
    assert report.decision == 'wait' and intents == []
    report, (intent,) = _decision(planner, _world(claim, reservation=_reservation(reserved_until=NOW + 60)))
    assert intent.key[0] == 'vote_initiate'  # caught up: it votes


def test_a_verified_delivery_is_extended_not_confirmed_or_claimed_while_the_world_catches_up(tmp_path):
    planner = _planner(tmp_path, sol=_Watcher(verify=_ok(), cancel_evidence=Verdict(1, NOW)))
    report, (intent,) = _decision(planner, _world(_swap(timeout_at=NOW + 60), complete=False))
    assert report.decision == 'extend_timeout' and intent.key[0] == 'extend_timeout'
    assert planner.memory.deliveries == {}  # an incomplete ledger never decides which swap a tx pays
    report, (intent,) = _decision(planner, _world(_swap(timeout_at=NOW + 60)))
    assert intent.key[0] == 'confirm_swap'


def test_a_verified_claim_survives_a_long_catch_up_and_attests_after_it(tmp_path):
    """A fresh database catches up for a long time; a claim the user already paid into must still be live at the
    end, and attest then."""
    planner = _planner(tmp_path, btc=_Watcher(verify=_ok()))
    claim, reservation = _swap('PendingAttestation'), _reservation(reserved_until=NOW + 300)
    for now in range(NOW, NOW + 40 * 60, 12):
        world = _world(claim, reservation=reservation, complete=False)
        for intent in planner.plan(world, now):
            if intent.key[0] == 'extend_reservation':  # the extension lands
                reservation.reserved_until = intent.payload.action.target_at
        assert reservation.reserved_until > now, f'lapsed at +{now - NOW}s'
    (intent,) = planner.plan(_world(claim, reservation=reservation), now)
    assert intent.key[0] == 'vote_initiate'
