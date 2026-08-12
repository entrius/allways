"""W3 — the cross-chain bond relayer.

Everything here is about ORDER and REFUSAL, because that is where the money safety lives: the
attestation must lead the vault pessimistically, an obligation must survive a restart, and a
validator that cannot read the vault must decline rather than guess.
"""

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from allways.solana import pdas
from allways.validator import forward as forward_mod
from allways.validator.relay import attestation as attestation_job
from allways.validator.relay import exit_relay, wiring
from allways.validator.relay.engine import BondRelay, RelayConfig
from allways.validator.state_store import ValidatorStateStore
from allways.vault import codec
from allways.vault.client import VaultCallResult

MINER = 'MinerPubkey1111111111111111111111111111111'
MINER2 = 'MinerPubkey2222222222222222222222222222222'
HOTKEY = codec.account_ss58(bytes([1] * 32))
HOTKEY2 = codec.account_ss58(bytes([2] * 32))
USER_TAO = codec.account_ss58(bytes([9] * 32))
SWAP = 'aa' * 32
SWAP2 = 'bb' * 32
NOW = 1_800_000_000
RAO = 10**9


# --- fakes --------------------------------------------------------------------------------------


class FakeVault:
    """An in-memory bond vault. Reads answer None when `readable` is off — the "this node can't
    decode a dry-run" case every caller has to survive."""

    def __init__(self, collateral=None, settled=None, lock=None, readable=True, timeline=None):
        self.timeline = timeline if timeline is not None else []
        self.collateral = {HOTKEY: 100 * RAO} if collateral is None else dict(collateral)
        self.settled = dict(settled or {})
        self.lock = {HOTKEY: (True, 3)} if lock is None else dict(lock)
        self.slashed = set()
        self.readable = readable
        self.calls = []
        self.reject = set()
        self.events = []
        self.head = lambda: 100

    def get_collateral(self, hotkey):
        return self.collateral.get(hotkey, 0) if self.readable else None

    def get_settled_total(self, hotkey):
        return self.settled.get(hotkey, 0) if self.readable else None

    def get_lock_state(self, hotkey):
        return self.lock.get(hotkey, (False, 0)) if self.readable else None

    def is_slashed(self, swap_ref):
        return (swap_ref in self.slashed) if self.readable else None

    def vote_slash(self, hotkey, swap_ref, penalty, user, reimbursement):
        self.calls.append(('vote_slash', hotkey, swap_ref, penalty, user, reimbursement))
        self.timeline.append('vault:vote_slash')
        if 'vote_slash' in self.reject:
            return VaultCallResult(ok=False, error='ContractReverted')
        # Single-validator quorum: the marker goes down and the seizure lands, exactly as the
        # contract does it — so the attestation the relay converges to afterwards is the real one.
        self.slashed.add(swap_ref)
        current = self.collateral.get(hotkey, 0)
        self.collateral[hotkey] = current - min(penalty, current)
        return VaultCallResult(ok=True)

    def vote_unlock(self, hotkey, epoch):
        self.calls.append(('vote_unlock', hotkey, epoch))
        if 'vote_unlock' in self.reject:
            return VaultCallResult(ok=False, error='ContractReverted')
        self.lock[hotkey] = (False, epoch + 1)
        return VaultCallResult(ok=True)

    def vote_collect_fees_batch(self, entries):
        self.calls.append(('vote_collect_fees_batch', list(entries)))
        if 'vote_collect_fees_batch' in self.reject:
            return VaultCallResult(ok=False, error='ContractReverted')
        for hotkey, total in entries:
            if total > self.settled.get(hotkey, 0):
                self.settled[hotkey] = total
        return VaultCallResult(ok=True)

    def poll_events(self, start, end):
        return self.events


class FakeSolana:
    def __init__(self, miner_states=None, attestations=None, config=None, timeline=None):
        self.timeline = timeline if timeline is not None else []
        # The pubkey->hotkey bindings `build_attribution` would derive off the Binding PDAs.
        self.attributions: dict = {}
        self.keypair = SimpleNamespace(pubkey=lambda: 'validator-pubkey')
        self.program_id = None
        self.miner_states = miner_states or {}
        self.attestations = attestations or {}
        self.config = config or SimpleNamespace(last_attest_heartbeat=0)
        self.calls = []
        self.voted = set()

    def get_all(self, name):
        if name != 'BondAttestation':
            return []
        return [(f'pda-{m}', a) for m, a in self.attestations.items()]

    def get_bond_attestation(self, miner, chain='tao'):
        return self.attestations.get(str(miner))

    def get_miner_state(self, miner):
        return self.miner_states.get(str(miner))

    def get_config(self):
        return self.config

    def has_voted(self, req_type, target, voter):
        return (req_type, str(target)) in self.voted

    def vote_set_attestation(self, miner, chain, balance, locked, epoch):
        self.calls.append(('vote_set_attestation', str(miner), chain, balance, locked, epoch))
        self.timeline.append('solana:vote_set_attestation')
        self.attestations[str(miner)] = SimpleNamespace(
            miner=miner, chain=chain, effective_balance=balance, locked=locked, epoch=epoch
        )
        return 'sig-attest'

    def vote_attest_heartbeat(self):
        self.calls.append(('vote_attest_heartbeat',))
        return 'sig-heartbeat'


@pytest.fixture(autouse=True)
def _attribution_from_chain():
    """`build_attribution` re-scans the Binding PDAs; the fakes answer it from `attributions`."""
    with patch('allways.validator.relay.engine.build_attribution', lambda client: dict(client.attributions)):
        yield


def _store():
    return ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db')


def _miner_state(**kw):
    base = dict(
        miner=MINER,
        active_backings=pdas.BACKING_BIT_TAO,
        has_active_swap=False,
        busy_until=0,
        settling_until=0,
        collateral=0,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _relay(vault=None, solana=None, store=None, attribution=None, **cfg):
    vault = vault or FakeVault()
    solana = solana or FakeSolana(miner_states={MINER: _miner_state()})
    relay = BondRelay(
        solana,
        vault,
        store or _store(),
        read_only=False,
        clock=lambda: NOW,
        config=RelayConfig(**cfg),
    )
    bindings = attribution if attribution is not None else {MINER: HOTKEY, MINER2: HOTKEY2}
    solana.attributions = dict(bindings)
    relay._attribution = dict(bindings)
    relay._attribution_at = NOW
    return relay


def _timeout_event(swap_key=SWAP, miner=MINER, penalty=22 * RAO, reimbursement=22 * RAO, chain='tao', payee=''):
    return SimpleNamespace(
        name='SwapTimedOut',
        block_time=NOW,
        fields={
            'swap_key': bytes.fromhex(swap_key),
            'miner': miner,
            'collateral_amount': 20 * RAO,
            'slash': 0,
            'collateral_chain': chain,
            'penalty': penalty,
            'reimbursement': reimbursement,
            'payee': payee,
        },
    )


def _completed_event(swap_key=SWAP2, miner=MINER, fee=2 * RAO, chain='tao'):
    return SimpleNamespace(
        name='SwapCompleted',
        block_time=NOW,
        fields={'swap_key': bytes.fromhex(swap_key), 'miner': miner, 'fee': fee, 'collateral_chain': chain},
    )


def _live_swap(backing='tao', from_chain='sol', to_chain='tao'):
    return SimpleNamespace(
        swap_key=bytes.fromhex(SWAP),
        miner=MINER,
        collateral_chain=backing,
        from_chain=from_chain,
        to_chain=to_chain,
        user_from_addr='user-sol-addr',
        user_to_addr=USER_TAO,
    )


# --- the effective bond (job 2's arithmetic) ----------------------------------------------------


def test_the_effective_bond_is_gross_minus_unsettled_fees_minus_unapplied_verdicts():
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 100 * RAO}, settled={HOTKEY: 1 * RAO}))
    relay.store.record_relay_fee(SWAP2, MINER, 'tao', 3 * RAO, NOW)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW)

    got = attestation_job.compute(relay, MINER, HOTKEY)
    # 100 gross − (3 accrued − 1 already settled) − 22 voted-but-unapplied = 76
    assert got.effective_balance == 76 * RAO
    assert (got.locked, got.epoch) == (True, 3)


def test_an_applied_verdict_stops_being_subtracted():
    relay = _relay()
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW)
    relay.store.mark_relay_slash_applied(SWAP)
    assert attestation_job.compute(relay, MINER, HOTKEY).effective_balance == 100 * RAO


def test_the_effective_bond_floors_at_zero_rather_than_going_negative():
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 5 * RAO}))
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 50 * RAO, 50 * RAO, USER_TAO, NOW)
    assert attestation_job.compute(relay, MINER, HOTKEY).effective_balance == 0


def test_an_unreadable_vault_yields_no_attestation_at_all():
    # The dangerous failure would be attesting 0 and stranding a healthy miner, or attesting the
    # gross figure and letting a swap open against bond that is already spoken for.
    relay = _relay(vault=FakeVault(readable=False))
    assert attestation_job.compute(relay, MINER, HOTKEY) is None


def test_an_idle_miner_is_never_rewritten():
    # Writes are event-driven: once the attestation says the truth, a miner nothing happened to
    # gets no further write, however many passes go by.
    relay = _relay()
    relay.step(NOW)
    before = len([c for c in relay.solana.calls if c[0] == 'vote_set_attestation'])
    assert before == 1, 'the first pass asserts the bond it just discovered'
    relay._next_reconcile = 0  # even forcing the slow repair loop
    relay.step(NOW + 1)
    relay.step(NOW + 2)
    assert len([c for c in relay.solana.calls if c[0] == 'vote_set_attestation']) == before


def test_an_unchanged_attestation_is_not_rewritten():
    solana = FakeSolana(
        miner_states={MINER: _miner_state()},
        attestations={MINER: SimpleNamespace(effective_balance=100 * RAO, locked=True, epoch=3, chain='tao')},
    )
    relay = _relay(solana=solana)
    relay.mark_dirty(MINER)
    assert attestation_job.flush(relay, NOW)
    assert not [c for c in solana.calls if c[0] == 'vote_set_attestation']


def test_a_fee_confirm_nets_the_bond_down_without_any_vault_write():
    relay = _relay()
    relay._reconciled = True
    relay.ingest_events([_completed_event(fee=2 * RAO)])
    relay.step(NOW)
    write = next(c for c in relay.solana.calls if c[0] == 'vote_set_attestation')
    assert write[3] == 98 * RAO, 'the fee is clipped at earn time, not at settle time'
    assert not relay.vault.calls, 'Solana confirms move nothing on the vault'


def test_a_sol_backed_confirm_is_not_the_relays_business():
    relay = _relay()
    relay._reconciled = True
    relay.ingest_events([_completed_event(chain='sol')])
    assert relay.store.accrued_fee_totals('tao') == {}


# --- job 1: the slash relay ---------------------------------------------------------------------


def test_the_netted_attestation_is_written_before_the_vault_is_told_anything():
    # The pinned ordering invariant: Solana frees the miner the instant the timeout quorum lands,
    # and the vault seizure follows minutes later. Without netting FIRST, a new swap could open
    # against bond this verdict has already spent.
    timeline = []
    relay = _relay(
        vault=FakeVault(timeline=timeline),
        solana=FakeSolana(miner_states={MINER: _miner_state()}, timeline=timeline),
    )
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay._reconciled = True
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)

    # And nothing after: once the seizure is real on the vault, gross-minus-nothing is the same
    # 78 the mirror already asserted, so the reconcile pass has nothing to converge.
    assert timeline == ['solana:vote_set_attestation', 'vault:vote_slash']
    attest = next(c for c in relay.solana.calls if c[0] == 'vote_set_attestation')
    assert attest[3] == 78 * RAO, '100 gross - 22 penalty, at verdict time'
    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[2:] == (SWAP, 22 * RAO, USER_TAO, 22 * RAO), 'the event tuple, verbatim'


def test_the_verdict_figures_are_relayed_verbatim_not_recomputed():
    # Every argument is hash-bound into the vault round, so a validator that recomputed the penalty
    # from state would conflict with its peers instead of co-counting.
    relay = _relay()
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event(penalty=7 * RAO, reimbursement=5 * RAO)])
    row = relay.store.open_relay_slashes('tao')[0]
    assert (row['penalty'], row['reimbursement']) == (7 * RAO, 5 * RAO)


def test_a_verdict_already_applied_on_the_vault_is_settled_without_a_second_vote():
    relay = _relay()
    relay.vault.slashed.add(SWAP)
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert relay.store.open_relay_slashes('tao') == []


def test_a_revert_whose_marker_is_set_counts_as_success():
    # AlreadySlashed reaches us as a bare ContractReverted, so the marker is what we believe.
    vault = FakeVault()
    vault.reject.add('vote_slash')
    relay = _relay(vault=vault)
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event()])
    vault.slashed.add(SWAP)
    relay.step(NOW)
    assert relay.store.open_relay_slashes('tao') == []


def test_a_revert_with_no_marker_leaves_the_obligation_open():
    vault = FakeVault()
    vault.reject.add('vote_slash')
    relay = _relay(vault=vault)
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_an_unreadable_marker_is_never_treated_as_permission_to_slash_again():
    relay = _relay(vault=FakeVault(readable=False))
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']


def test_a_verdict_with_neither_snapshot_nor_payee_is_recorded_but_not_relayed():
    # No snapshot and a payee-less verdict (a swap that timed out before W3.1) leaves nobody to pay.
    # The row still exists: it keeps netting and keeps blocking initiates until a peer's quorum lands.
    relay = _relay()
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_a_verdict_for_an_unseen_swap_is_relayed_from_the_payee_in_the_event():
    # W3.1, the blind-spot closure: this validator has no snapshot — fresh state DB, or down for the
    # swap's whole life — and relays it anyway, because the verdict names who is owed.
    relay = _relay()
    assert relay.store.get_relay_swap(SWAP) is None
    relay.ingest_events([_timeout_event(payee=USER_TAO)])
    relay.step(NOW)

    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[2:] == (SWAP, 22 * RAO, USER_TAO, 22 * RAO), 'the event tuple, verbatim'
    assert relay.store.open_relay_slashes('tao') == []


def test_the_snapshot_stays_the_fast_path_when_both_name_a_payee():
    # The snapshot is read from the swap while it was live; the event is the fallback, not an
    # override. Same value in practice — this pins which one is authoritative if they ever differ.
    relay = _relay()
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event(payee=HOTKEY2)])
    assert relay.store.open_relay_slashes('tao')[0]['user_addr'] == USER_TAO


def test_a_malformed_payee_in_the_event_is_refused_like_any_other():
    # The program never validated the user's backing-chain address, and putting it in the event
    # changed nothing about that — an unpayable one must not reach the vault.
    relay = _relay()
    relay.ingest_events([_timeout_event(payee='not-an-ss58')])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert len(relay.store.open_relay_slashes('tao')) == 1


# --- the reimbursement snapshot -----------------------------------------------------------------


def test_the_payee_is_the_user_leg_denominated_in_the_backing():
    relay = _relay()
    relay.observe_swap(_live_swap(from_chain='sol', to_chain='tao'))
    assert relay.store.get_relay_swap(SWAP)['user_addr'] == USER_TAO

    # the reverse direction pins the SOURCE leg instead
    other = _live_swap(from_chain='tao', to_chain='sol')
    other.swap_key = bytes.fromhex(SWAP2)
    other.user_from_addr = USER_TAO
    other.user_to_addr = 'user-sol-addr'
    relay.observe_swap(other)
    assert relay.store.get_relay_swap(SWAP2)['user_addr'] == USER_TAO


def test_a_locally_backed_swap_needs_no_snapshot():
    relay = _relay()
    relay.observe_swap(_live_swap(backing='sol'))
    assert relay.store.get_relay_swap(SWAP) is None


def test_the_first_sighting_wins_so_a_later_pass_cannot_rewrite_the_payee():
    relay = _relay()
    relay.observe_swap(_live_swap())
    tampered = _live_swap()
    tampered.user_to_addr = HOTKEY2
    relay.observe_swap(tampered)
    assert relay.store.get_relay_swap(SWAP)['user_addr'] == USER_TAO


def test_a_rebind_after_acceptance_does_not_redirect_the_seizure_off_the_bonded_hotkey():
    # F1: the swap is accepted while MINER is bound to HOTKEY (H1). The miner then rebinds its
    # pubkey to a fresh, unbonded HOTKEY2 (H2) before the timeout lands. The seizure must still hit
    # H1 — the hotkey that carried the bond when the swap was live — not the empty H2.
    relay = _relay()
    relay.observe_swap(_live_swap())
    assert relay.store.get_relay_swap(SWAP)['hotkey'] == HOTKEY

    relay._attribution = {MINER: HOTKEY2}  # the mid-swap rebind
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)

    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[1] == HOTKEY, 'the seizure targets the observe-time hotkey, not the rebind'


def test_a_pre_f1_snapshot_without_a_hotkey_falls_back_to_the_live_binding():
    # Rows recorded before F1 carry a NULL hotkey; the relay must still slash via the live lookup.
    relay = _relay()
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)  # no hotkey
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[1] == HOTKEY


def test_the_hotkey_snapshot_round_trips_through_swap_and_slash():
    store = _store()
    store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW, HOTKEY)
    assert store.get_relay_swap(SWAP)['hotkey'] == HOTKEY
    store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW, HOTKEY)
    assert store.open_relay_slashes('tao')[0]['hotkey'] == HOTKEY


# --- F3: the relay's own cursor, propagated failures, and heartbeat gating -----------------------


def test_a_clean_ingest_reports_healthy():
    relay = _relay()
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW, HOTKEY)
    assert relay.ingest_events([_timeout_event()]) is True
    assert relay._ingest_healthy is True


def test_a_failed_verdict_write_is_not_swallowed_and_holds_the_heartbeat(monkeypatch):
    relay = _relay()
    monkeypatch.setattr(
        relay.store, 'record_relay_slash', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('db down'))
    )
    assert relay.ingest_events([_timeout_event()]) is False
    assert relay._ingest_healthy is False
    # The liveness heartbeat must stay down while a verdict write is still dirty.
    relay.solana.calls.clear()
    relay.maybe_heartbeat(NOW + 10**9)
    assert not [c for c in relay.solana.calls if c[0] == 'vote_attest_heartbeat']


def _fake_validator(relay, ingest):
    return SimpleNamespace(
        state_store=relay.store,
        event_ingest=ingest,
        bond_relay=relay,
        solana_client=SimpleNamespace(attributions={MINER: HOTKEY}),
        event_index=SimpleNamespace(ingest=lambda records, attribution: len(records)),
    )


def test_a_failed_verdict_write_holds_the_relay_cursor_until_it_lands(monkeypatch):
    # F3: forward advances the crown cursor regardless, but the relay's own cursor must hold on a
    # write failure so the SwapTimedOut is re-read next step (the inserts are idempotent).
    relay = _relay()
    store = relay.store
    ingest = SimpleNamespace(poll=lambda cursor: ([_timeout_event(payee=USER_TAO)], 'sig1'))
    validator = _fake_validator(relay, ingest)
    monkeypatch.setattr(forward_mod, 'build_attribution', lambda client: {})

    orig = store.record_relay_slash
    monkeypatch.setattr(store, 'record_relay_slash', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('db down')))
    forward_mod.ingest_solana_events(validator)
    assert store.get_relay_event_cursor() is None, 'the cursor holds while the write is failing'
    assert store.open_relay_slashes('tao') == []

    monkeypatch.setattr(store, 'record_relay_slash', orig)
    forward_mod.ingest_solana_events(validator)
    assert store.get_relay_event_cursor() == 'sig1', 'once it lands, the cursor advances'
    assert len(store.open_relay_slashes('tao')) == 1


def test_a_relay_that_failed_to_build_is_retried_on_a_later_tick(monkeypatch):
    clock = {'t': 1000.0}
    monkeypatch.setattr(wiring.time, 'monotonic', lambda: clock['t'])
    sentinel = object()
    outcomes = [None, sentinel]
    monkeypatch.setattr(wiring, 'build_bond_relay', lambda v, read_only=False: outcomes.pop(0))
    loop = SimpleNamespace(relay=None)
    validator = SimpleNamespace(bond_relay=None, solana_swap_loop=loop)

    assert wiring.ensure_bond_relay(validator) is None  # first tick: transient failure, still off
    assert validator.bond_relay is None
    clock['t'] += wiring._RELAY_REBUILD_INTERVAL_SECS + 1  # a later tick, past the throttle
    assert wiring.ensure_bond_relay(validator) is sentinel  # self-healed
    assert validator.bond_relay is sentinel
    assert loop.relay is sentinel  # the swap loop's observe hook is rewired


# --- the off-chain busy-until-settled backstop --------------------------------------------------


def test_a_miner_owing_an_unapplied_debit_blocks_its_own_next_initiate():
    relay = _relay()
    assert not relay.has_pending_debit(MINER)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW)
    assert relay.has_pending_debit(MINER)
    relay.store.mark_relay_slash_applied(SWAP)
    assert not relay.has_pending_debit(MINER)


def test_an_unreadable_ledger_fails_closed():
    relay = _relay()
    relay.store.close()
    assert relay.has_pending_debit(MINER), 'a ledger we cannot read must not open a new swap'


# --- job 3: the exit sequence -------------------------------------------------------------------


def _exiting_relay(vault=None, **state):
    ms = _miner_state(active_backings=0, **state)
    relay = _relay(vault=vault, solana=FakeSolana(miner_states={MINER: ms}))
    relay._exiting.add(MINER)
    return relay


def test_an_exit_waits_for_the_in_flight_swap():
    relay = _exiting_relay(has_active_swap=True)
    exit_relay.run_exits(relay, NOW)
    assert not relay.vault.calls


@pytest.mark.parametrize('field', ['busy_until', 'settling_until'])
def test_an_exit_waits_out_both_locks(field):
    relay = _exiting_relay(**{field: NOW + 5})
    exit_relay.run_exits(relay, NOW)
    assert not relay.vault.calls


def test_an_exit_waits_for_every_slash_to_land_on_the_vault():
    # The permanent swap_ref markers ARE the checklist: a miner must never unlock owing a seizure.
    relay = _exiting_relay()
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW)
    exit_relay.run_exits(relay, NOW)
    assert not relay.vault.calls


def test_a_quiescent_exit_settles_the_residual_before_it_unlocks():
    relay = _exiting_relay()
    relay.store.record_relay_fee(SWAP2, MINER, 'tao', 4 * RAO, NOW)

    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 4 * RAO)])]
    assert relay.vault.lock[HOTKEY] == (True, 3), 'still locked until the settle is on the books'

    relay._voted_at.clear()  # next pass, after the settle round applied
    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls[-1] == ('vote_unlock', HOTKEY, 3)


def test_the_exit_settle_is_a_one_entry_batch_so_the_fleet_is_never_dragged_in():
    relay = _exiting_relay()
    relay.store.record_relay_fee(SWAP2, MINER, 'tao', 4 * RAO, NOW)
    relay.store.record_relay_fee('cc' * 32, MINER2, 'tao', 9 * RAO, NOW)
    exit_relay.run_exits(relay, NOW)
    entries = relay.vault.calls[0][1]
    assert entries == [(HOTKEY, 4 * RAO)]


def test_an_exit_owing_nothing_goes_straight_to_unlock():
    relay = _exiting_relay()
    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls == [('vote_unlock', HOTKEY, 3)]


def test_the_unlock_names_the_current_epoch():
    # The epoch is hash-bound into the vault round, so a stale round can't unlock a re-locked bond.
    relay = _exiting_relay(vault=FakeVault(lock={HOTKEY: (True, 11)}))
    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls == [('vote_unlock', HOTKEY, 11)]


def test_re_activating_the_purse_cancels_the_exit():
    relay = _relay(solana=FakeSolana(miner_states={MINER: _miner_state()}))
    relay._exiting.add(MINER)
    exit_relay.run_exits(relay, NOW)
    assert not relay.vault.calls and MINER not in relay._exiting


def test_deactivating_the_tao_purse_arms_the_exit():
    relay = _relay()
    relay.ingest_events(
        [
            SimpleNamespace(
                name='MinerBackingChanged',
                block_time=NOW,
                fields={'miner': MINER, 'backing': 'tao', 'enabled': False, 'active_backings': 0},
            )
        ]
    )
    assert MINER in relay._exiting


def test_an_already_unlocked_bond_leaves_the_exit_set():
    relay = _exiting_relay(vault=FakeVault(lock={HOTKEY: (False, 4)}))
    exit_relay.run_exits(relay, NOW)
    assert not relay.vault.calls and MINER not in relay._exiting


# --- the fee true-up cadence --------------------------------------------------------------------


def test_the_batch_vector_is_ordered_by_account_id_so_every_validator_agrees():
    # The round key is the batch-contents hash: a differently-ordered vector is a different round,
    # and the quorum would never converge.
    relay = _relay(attribution={MINER: HOTKEY2, MINER2: HOTKEY})  # deliberately "wrong" insertion order
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW)
    relay.store.record_relay_fee(SWAP2, MINER2, 'tao', 7 * RAO, NOW)
    entries = exit_relay.cadence_entries(relay, NOW)
    assert [h for h, _ in entries] == [HOTKEY, HOTKEY2]
    assert codec.account_bytes(entries[0][0]) < codec.account_bytes(entries[1][0])


def test_the_vector_is_derived_from_the_event_stream_not_from_the_vault():
    # Membership must not depend on a vault read, or two validators reading at slightly different
    # moments would build different vectors. Stale entries are the vault's no-op to make.
    relay = _relay(vault=FakeVault(settled={HOTKEY: 5 * RAO}))
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY, 5 * RAO)]


def test_totals_are_read_at_the_boundary_not_at_the_moment_of_firing():
    relay = _relay()
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW - 100)
    relay.store.record_relay_fee(SWAP2, MINER, 'tao', 6 * RAO, NOW + 100)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY, 5 * RAO)]


def test_a_boundary_with_no_delta_is_skipped_as_pure_postage():
    relay = _relay(vault=FakeVault(settled={HOTKEY: 5 * RAO}), fee_cadence_secs=1000)
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW)
    assert not relay.vault.calls
    assert relay.store.get_relay_meta('fee_cadence_boundary') is not None, 'and it is not retried'


def test_a_boundary_with_a_delta_fires_once_and_only_once():
    relay = _relay(fee_cadence_secs=1000)
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW)
    exit_relay.maybe_cadence_settle(relay, NOW + 5)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 5 * RAO)])]


def test_an_unreadable_vault_defers_the_round_instead_of_deciding_blind():
    relay = _relay(vault=FakeVault(readable=False), fee_cadence_secs=1000)
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW)
    assert not relay.vault.calls
    assert relay.store.get_relay_meta('fee_cadence_boundary') is None, 'the boundary is retried'


def test_a_rejected_batch_leaves_the_boundary_open_for_the_next_tick():
    vault = FakeVault()
    vault.reject.add('vote_collect_fees_batch')
    relay = _relay(vault=vault, fee_cadence_secs=1000)
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW)
    assert relay.store.get_relay_meta('fee_cadence_boundary') is None


def test_a_miner_with_no_binding_is_excluded_rather_than_breaking_the_vector():
    relay = _relay(attribution={MINER2: HOTKEY2})
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 5 * RAO, NOW)
    relay.store.record_relay_fee(SWAP2, MINER2, 'tao', 7 * RAO, NOW)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY2, 7 * RAO)]


# --- heartbeat + the restart barrier ------------------------------------------------------------


def test_the_heartbeat_stays_down_until_the_backlog_is_drained():
    # The fuse being closed while this validator is behind is the point: TAO entry reopens only
    # once the relay has proved it is caught up.
    vault = FakeVault(readable=False)
    relay = _relay(vault=vault, heartbeat_interval_secs=1)
    relay.mark_dirty(MINER)
    relay.step(NOW)
    assert not [c for c in relay.solana.calls if c[0] == 'vote_attest_heartbeat']
    assert not relay._reconciled


def test_the_heartbeat_is_released_once_reconcile_comes_back_clean():
    relay = _relay(heartbeat_interval_secs=1)
    relay.step(NOW)
    assert relay._reconciled
    assert ('vote_attest_heartbeat',) in relay.solana.calls


def test_the_heartbeat_is_lazy_not_per_pass():
    relay = _relay(heartbeat_interval_secs=3600)
    relay.step(NOW)
    relay.step(NOW + 10)
    assert [c for c in relay.solana.calls if c[0] == 'vote_attest_heartbeat'] == [('vote_attest_heartbeat',)]


def test_a_fresh_on_chain_heartbeat_needs_no_vote_from_us():
    solana = FakeSolana(
        miner_states={MINER: _miner_state()},
        config=SimpleNamespace(last_attest_heartbeat=NOW - 5),
    )
    relay = _relay(solana=solana, heartbeat_interval_secs=3600)
    relay.step(NOW)
    assert not [c for c in solana.calls if c[0] == 'vote_attest_heartbeat']


def test_a_vote_already_in_the_live_round_is_not_cast_twice():
    solana = FakeSolana(miner_states={MINER: _miner_state()})
    solana.voted.add((pdas.REQ_ATTEST_HEARTBEAT, str(pdas.config_pda(None))))
    relay = _relay(solana=solana, heartbeat_interval_secs=1)
    relay.step(NOW)
    assert not [c for c in solana.calls if c[0] == 'vote_attest_heartbeat']


def test_a_bond_that_never_entered_service_is_never_unlocked():
    # THE trap: on chain, "deactivated" and "never activated" are the same state — purse bit down,
    # bond locked. A miner that has bonded and is waiting to activate must not have its bond
    # released out from under it.
    relay = _exiting_relay()
    relay._exiting.clear()  # nothing armed it: no deactivation ever happened
    relay.reconcile(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_unlock']
    assert MINER not in relay._exiting


def test_reconcile_resumes_an_exit_stranded_by_a_restart():
    # The arming half IS persisted, so a validator that restarts mid-exit picks the sequence back
    # up from its own ledger rather than from an ambiguous on-chain bit.
    store = _store()
    first = _relay(store=store)
    first.arm_exit(MINER)

    second = _relay(store=store, solana=FakeSolana(miner_states={MINER: _miner_state(active_backings=0)}))
    assert MINER not in second._exiting
    second.reconcile(NOW)
    assert MINER in second._exiting
    assert [c for c in second.vault.calls if c[0] == 'vote_unlock']


def test_reconcile_repairs_an_attestation_that_drifted_while_we_were_down():
    solana = FakeSolana(
        miner_states={MINER: _miner_state()},
        attestations={MINER: SimpleNamespace(chain='tao', miner=MINER, effective_balance=1, locked=True, epoch=3)},
    )
    relay = _relay(solana=solana)
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 100 * RAO, True, 3) in solana.calls


def test_the_write_budget_holds_the_barrier_shut_rather_than_stalling_the_pass():
    relay = _relay(max_writes_per_tick=1)
    for i, miner in enumerate([MINER, MINER2]):
        relay.solana.miner_states[miner] = _miner_state(miner=miner)
        relay.vault.collateral[[HOTKEY, HOTKEY2][i]] = 10 * RAO
        relay.vault.lock[[HOTKEY, HOTKEY2][i]] = (True, 1)
        relay.mark_dirty(miner)
    assert not attestation_job.flush(relay, NOW)
    assert len([c for c in relay.solana.calls if c[0] == 'vote_set_attestation']) == 1


def test_an_unbound_miner_is_left_alone_rather_than_retried_forever():
    relay = _relay(attribution={})
    relay.mark_dirty(MINER)
    assert attestation_job.flush(relay, NOW)
    assert not relay._dirty


class _StrandingSolana(FakeSolana):
    """`vote_set_attestation` is refused with the program's held-hub error (F5) — a downward write
    the program won't accept while the miner's hub is held."""

    def vote_set_attestation(self, miner, chain, balance, locked, epoch):
        raise Exception("Program error: custom(6067) 'AttestationWouldStrandSwap'")


def test_a_write_refused_while_the_hub_is_held_defers_without_wedging_the_barrier():
    # F5: the program refuses a downward attestation write while the miner's hub is held. That refusal
    # must NOT hold the startup reconcile barrier down — a fleet restart mid-swap would otherwise blank
    # TAO entry fleet-wide for the swap's whole life. It is deferred (the program guarantees it can't
    # matter until the hub frees), and the miner stays dirty so the write retries once the hub clears.
    solana = _StrandingSolana(miner_states={MINER: _miner_state()})
    relay = _relay(solana=solana)
    relay.mark_dirty(MINER)
    assert attestation_job.flush(relay, NOW)  # barrier passes despite the refusal (deferred, not owed)
    assert MINER in relay._dirty  # ...but the write is not forgotten — it stays owed for a later retry


def test_read_only_mode_writes_nothing_anywhere():
    relay = _relay(heartbeat_interval_secs=1)
    relay.read_only = True
    relay.store.record_relay_swap(SWAP, MINER, 'tao', USER_TAO, NOW)
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert not relay.vault.calls
    assert not relay.solana.calls


def test_a_step_never_raises_out_into_the_forward_pass():
    relay = _relay()
    relay.solana.get_all = lambda name: (_ for _ in ()).throw(RuntimeError('rpc down'))
    relay.store.close()
    relay.step(NOW)  # must not raise


def test_reconcile_discovers_a_first_bond_no_event_ever_told_us_about():
    # A miner posts and locks on the vault before any attestation exists. If that pair of events
    # lands while this validator is down, nothing else would discover it — so reconcile enumerates
    # the whole bound set, not just the attested one.
    solana = FakeSolana(miner_states={MINER: _miner_state()})
    relay = _relay(solana=solana)
    assert solana.get_bond_attestation(MINER) is None
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 100 * RAO, True, 3) in solana.calls


def test_a_bound_miner_with_no_bond_gets_no_account_opened_for_it():
    # Writing "zero" would open a rent-paying attestation per registered miner to say nothing.
    solana = FakeSolana(miner_states={MINER: _miner_state()})
    relay = _relay(vault=FakeVault(collateral={}, lock={}), solana=solana)
    relay.reconcile(NOW)
    assert not [c for c in solana.calls if c[0] == 'vote_set_attestation']


def test_a_bond_netted_to_zero_is_still_asserted():
    # Nets-to-zero is a real claim — "this miner's bond is entirely spoken for" — and the entry
    # guards must see it. Only "no bond at all" is silence.
    solana = FakeSolana(miner_states={MINER: _miner_state()})
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 5 * RAO}), solana=solana)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 50 * RAO, 50 * RAO, USER_TAO, NOW)
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 0, True, 3) in solana.calls


def test_a_malformed_reimbursement_address_is_rejected_once_not_every_pass():
    # The program never validated the user's backing-chain address, so a malformed one reaches the
    # relay intact. It must be refused as a payee, not raise on every tick forever.
    relay = _relay()
    relay.store.record_relay_swap(SWAP, MINER, 'tao', 'not-an-ss58', NOW)
    relay.ingest_events([_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_reconcile_refreshes_who_exists_before_asking_what_they_owe():
    # A miner that binds after startup must not stay invisible for an attribution TTL — the bound
    # set is the reconcile iteration set, so a stale map hides it completely.
    relay = _relay(attribution={MINER2: HOTKEY2})  # a stale map that predates the new binding
    relay.solana.attributions = {MINER: HOTKEY, MINER2: HOTKEY2}
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 100 * RAO, True, 3) in relay.solana.calls


# ─── vault generation: the attestation epoch namespace ───────────────────────────────────────────


def test_epoch_composition_round_trips_and_orders_across_generations():
    from allways.solana.layouts import compose_attestation_epoch as compose
    from allways.solana.layouts import split_attestation_epoch as split

    assert split(compose(0, 0)) == (0, 0)
    assert split(compose(3, 7)) == (3, 7)
    # Generation 0 must compose to the bare lock epoch, so a pre-upgrade fleet's epochs are unchanged.
    assert compose(0, 9) == 9
    # The whole point: ANY epoch in a newer generation outranks EVERY epoch the retired vault
    # could have produced, which is what keeps the on-chain monotonic guard satisfiable.
    assert compose(1, 0) > compose(0, 2**32 - 1)
    with pytest.raises(ValueError):
        compose(1, 2**32)


def test_attestation_composes_the_configured_vault_generation():
    vault = FakeVault()
    vault.lock[HOTKEY] = (True, 1)
    vault.collateral[HOTKEY] = 5 * RAO
    solana = FakeSolana(
        miner_states={MINER: _miner_state()},
        config=SimpleNamespace(last_attest_heartbeat=0, vault_generation=2),
    )
    relay = _relay(vault=vault, solana=solana)

    desired = attestation_job.compute(relay, MINER, HOTKEY)
    from allways.solana.layouts import split_attestation_epoch as split

    assert split(desired.epoch) == (2, 1), 'generation in the high half, vault lock epoch in the low'
    assert desired.locked is True


def test_attestation_is_skipped_when_the_generation_is_unreadable():
    """Unknown must never be spelled as generation 0 — that would attest into a retired namespace."""
    vault = FakeVault()
    vault.lock[HOTKEY] = (True, 1)
    vault.collateral[HOTKEY] = 5 * RAO

    class Broken(FakeSolana):
        def get_config(self):
            raise RuntimeError('rpc down')

    relay = _relay(vault=vault, solana=Broken(miner_states={MINER: _miner_state()}))
    assert attestation_job.compute(relay, MINER, HOTKEY) is None


def test_a_fresh_vault_is_still_empty_whatever_the_generation():
    """`is_empty` must read the VAULT half: a replacement vault starts at lock epoch 0 in a
    non-zero generation, and that is still "no bond posted", not a real assertion worth writing."""
    from allways.solana.layouts import compose_attestation_epoch as compose

    assert attestation_job.Attested(0, False, compose(4, 0)).is_empty
    assert not attestation_job.Attested(0, False, compose(4, 1)).is_empty


def test_fees_are_scoped_to_the_vault_generation_that_will_collect_them():
    """A replacement vault restarts its settled counter at 0. If accruals were summed across
    generations, fees the retired vault already collected would be charged a second time."""
    store = _store()
    store.record_relay_fee('aa', MINER, 'tao', 1_800_000, NOW - 100, vault_generation=0)
    store.record_relay_fee('bb', MINER, 'tao', 500_000, NOW, vault_generation=1)

    assert store.accrued_fee_total(MINER, 'tao', vault_generation=0) == 1_800_000
    assert store.accrued_fee_total(MINER, 'tao', vault_generation=1) == 500_000
    # Unscoped stays the whole history — that read is miner discovery, not accounting.
    assert store.accrued_fee_total(MINER, 'tao') == 2_300_000
    assert set(store.accrued_fee_totals('tao', vault_generation=1)) == {MINER}


def test_attestation_does_not_recharge_fees_the_retired_vault_settled():
    """The live bug: 0.8 τ posted to a fresh vault was attested as 0.7982 τ because 1.8M rao of
    already-settled fees came along from the retired vault."""
    vault = FakeVault()
    vault.lock[HOTKEY] = (True, 1)
    vault.collateral[HOTKEY] = 800_000_000
    vault.settled[HOTKEY] = 0  # a fresh vault has collected nothing yet
    store = _store()
    store.record_relay_fee('old', MINER, 'tao', 1_800_000, NOW - 500, vault_generation=0)
    solana = FakeSolana(
        miner_states={MINER: _miner_state()},
        config=SimpleNamespace(last_attest_heartbeat=0, vault_generation=1),
    )
    relay = _relay(vault=vault, solana=solana, store=store)

    desired = attestation_job.compute(relay, MINER, HOTKEY)
    assert desired.effective_balance == 800_000_000, 'the retired vault already collected that fee'

    # A fee earned under the CURRENT generation still nets off, exactly as before.
    store.record_relay_fee('new', MINER, 'tao', 2_000_000, NOW, vault_generation=1)
    assert attestation_job.compute(relay, MINER, HOTKEY).effective_balance == 798_000_000
