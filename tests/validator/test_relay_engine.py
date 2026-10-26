"""W3 — the cross-chain bond relayer.

Everything here is about ORDER and REFUSAL, because that is where the money safety lives: the
attestation must lead the vault pessimistically, an obligation must survive a restart, and a
validator that cannot read the vault must decline rather than guess.
"""

from types import SimpleNamespace

import pytest
from solders.pubkey import Pubkey

from allways.constants import RELAY_FEE_SETTLE_GRACE_SECS as GRACE
from allways.solana import pdas
from allways.validator.relay import attestation as attestation_job
from allways.validator.relay import exits as exit_relay
from allways.validator.relay import slash as slash_job
from allways.validator.relay.engine import BondRelay, RelayConfig
from allways.validator.relay.ledger import RelayLedger
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World
from allways.vault import codec
from allways.vault.client import VaultCallResult

MINER = str(Pubkey(bytes([3] * 32)))
MINER2 = str(Pubkey(bytes([4] * 32)))
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
        self.validators = ['us']

    def get_collateral(self, hotkey):
        return self.collateral.get(hotkey, 0) if self.readable else None

    def get_settled_total(self, hotkey):
        return self.settled.get(hotkey, 0) if self.readable else None

    def get_validators(self):
        return list(self.validators) if self.readable else None

    def get_lock_state(self, hotkey):
        return self.lock.get(hotkey, (False, 0)) if self.readable else None

    def get_bond_state(self, hotkey):
        return self.get_collateral(hotkey), self.get_lock_state(hotkey)

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
        self.calls.append(('poll_events', start, end))
        return self.events


class FakeChain:
    """The program as the relay sees it: the World it reads, and votes that land as soon as they're queued
    (the actuator's job, here instant). ``complete`` off is a World still loading."""

    def __init__(self, miner_states=None, attestations=None, config=None, timeline=None):
        self.timeline = timeline if timeline is not None else []
        self.attributions: dict = {}  # the pubkey→hotkey map the World verified off the Binding PDAs
        self.miner_states = miner_states or {}
        self.attestations = attestations or {}
        self.config = config or SimpleNamespace(last_attest_heartbeat=0)
        self.complete = True
        self.calls = []

    def world(self):
        accounts = {
            'MinerState': {str(pdas.miner_state_pda(m)): ms for m, ms in self.miner_states.items()},
            'BondAttestation': {str(pdas.bond_attestation_pda(m, 'tao')): a for m, a in self.attestations.items()},
        }
        return World(NOW, 1, self.complete, self.config, accounts, attribution=dict(self.attributions))

    def vote(self, kind, subject, payload):
        self.calls.append((kind, *(payload or ())))
        if kind == 'vote_set_attestation':
            miner, chain, balance, locked, epoch = payload
            self.timeline.append('solana:vote_set_attestation')
            self.attestations[miner] = SimpleNamespace(
                miner=miner, chain=chain, effective_balance=balance, locked=locked, epoch=epoch
            )


def _store():
    return RelayLedger(Store(':memory:'), 'tao')


def _relay(vault=None, solana=None, store=None, attribution=None, **cfg):
    chain = solana or FakeChain(miner_states={MINER: _miner_state()})
    relay = BondRelay(
        chain.world,
        chain.vote,
        vault or FakeVault(),
        store or _store(),
        clock=lambda: NOW,
        config=RelayConfig(**cfg),
        sleep=lambda _secs: None,
    )
    chain.attributions = dict(attribution if attribution is not None else {MINER: HOTKEY, MINER2: HOTKEY2})
    relay.chain = chain
    return relay


def _ingest(relay, records):
    """What the hub component does with a batch: a failed write holds the heartbeat."""
    ok = True
    for record in records:
        try:
            relay.ingest(record)
        except Exception:
            ok = False
    relay._ingest_healthy = ok
    return ok


def _miner_state(**kw):
    base = dict(
        miner=MINER,
        active_backings=pdas.BACKING_BIT_TAO,
        has_active_swap=False,
        busy_until=0,
        settling_until=0,
        collateral=0,
        fee_generation=0,
        accrued_fees=[0] * 8,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _fee(relay, swap_key, miner, fee, at, generation=0):
    """A TAO-backed confirm as the relay sees it: the program accrues the miner's on-chain counter, and the
    feed delivers the event's row."""
    states = relay.chain.miner_states
    ms = states.setdefault(miner, _miner_state(miner=miner))
    if ms.fee_generation != generation:
        ms.fee_generation, ms.accrued_fees = generation, [0] * 8
    ms.accrued_fees[pdas.BACKING_BIT_TAO.bit_length() - 1] += fee
    relay.store.record_relay_fee(swap_key, miner, 'tao', fee, at, vault_generation=generation)


def _timeout_event(
    swap_key=SWAP, miner=MINER, penalty=22 * RAO, reimbursement=22 * RAO, chain='tao', payee=USER_TAO, hotkey=None
):
    fields = {
        'swap_key': bytes.fromhex(swap_key),
        'miner': miner,
        'collateral_amount': 20 * RAO,
        'slash': 0,
        'collateral_chain': chain,
        'penalty': penalty,
        'reimbursement': reimbursement,
        'payee': payee,
    }
    # V-M1: the verdict carries the initiate-pinned hotkey (raw bytes). None = a pre-upgrade event
    # without the field, which the relay must keep tolerating.
    if hotkey is not None:
        fields['hotkey'] = hotkey
    return SimpleNamespace(name='SwapTimedOut', block_time=NOW, fields=fields)


def _completed_event(swap_key=SWAP2, miner=MINER, fee=2 * RAO, chain='tao'):
    return SimpleNamespace(
        name='SwapCompleted',
        block_time=NOW,
        fields={'swap_key': bytes.fromhex(swap_key), 'miner': miner, 'fee': fee, 'collateral_chain': chain},
    )


# --- the effective bond (job 2's arithmetic) ----------------------------------------------------


def test_the_effective_bond_is_gross_minus_unsettled_fees_minus_unapplied_verdicts():
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 100 * RAO}, settled={HOTKEY: 1 * RAO}))
    _fee(relay, SWAP2, MINER, 3 * RAO, NOW)
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


def test_a_fee_confirm_nets_the_bond_down_without_any_vault_write():
    relay = _relay()
    relay._reconciled = True
    relay.chain.miner_states[MINER].accrued_fees[1] += 2 * RAO  # the confirm quorum's accrual
    _ingest(relay, [_completed_event(fee=2 * RAO)])
    relay.step(NOW)
    write = next(c for c in relay.chain.calls if c[0] == 'vote_set_attestation')
    assert write[3] == 98 * RAO, 'the fee is clipped at earn time, not at settle time'
    assert not relay.vault.calls, 'Solana confirms move nothing on the vault'


def test_a_sol_backed_confirm_is_not_the_relays_business():
    relay = _relay()
    relay._reconciled = True
    _ingest(relay, [_completed_event(chain='sol')])
    assert relay.store.fees_after(MINER, 0, 0) == 0


# --- job 1: the slash relay ---------------------------------------------------------------------


def test_the_netted_attestation_is_written_before_the_vault_is_told_anything():
    # The pinned ordering invariant: Solana frees the miner the instant the timeout quorum lands,
    # and the vault seizure follows minutes later. Without netting FIRST, a new swap could open
    # against bond this verdict has already spent.
    timeline = []
    relay = _relay(
        vault=FakeVault(timeline=timeline),
        solana=FakeChain(miner_states={MINER: _miner_state()}, timeline=timeline),
    )
    relay._reconciled = True
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)

    # And nothing after: once the seizure is real on the vault, gross-minus-nothing is the same
    # 78 the mirror already asserted, so the reconcile pass has nothing to converge.
    assert timeline == ['solana:vote_set_attestation', 'vault:vote_slash']
    attest = next(c for c in relay.chain.calls if c[0] == 'vote_set_attestation')
    assert attest[3] == 78 * RAO, '100 gross - 22 penalty, at verdict time'
    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[2:] == (SWAP, 22 * RAO, USER_TAO, 22 * RAO), 'the event tuple, verbatim'


def test_the_verdict_figures_are_relayed_verbatim_not_recomputed():
    # Every argument is hash-bound into the vault round, so a validator that recomputed the penalty
    # from state would conflict with its peers instead of co-counting.
    relay = _relay()
    _ingest(relay, [_timeout_event(penalty=7 * RAO, reimbursement=5 * RAO)])
    row = relay.store.open_relay_slashes('tao')[0]
    assert (row['penalty'], row['reimbursement']) == (7 * RAO, 5 * RAO)


def test_a_verdict_already_applied_on_the_vault_is_settled_without_a_second_vote():
    relay = _relay()
    relay.vault.slashed.add(SWAP)
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert relay.store.open_relay_slashes('tao') == []


def test_a_verdict_the_vault_already_applied_is_not_netted_twice():
    # A fresh store replays a timeout whose seizure landed long ago: the vault's gross is already
    # net of it. Netting the penalty again attested 0 on testnet (vali-gamma, 2026-10-26).
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 78 * RAO}))
    relay.vault.slashed.add(SWAP)
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    attest = next(c for c in relay.chain.calls if c[0] == 'vote_set_attestation')
    assert attest[3] == 78 * RAO, 'the gross already paid the slash; 56 would be the double count'


def test_a_validator_outside_the_vault_set_still_sees_an_applied_seizure():
    # Membership gates vault VOTES, not vault reads: a validator waiting for admission must not
    # attest a phantom-low bond meanwhile, and must not vote the slash either.
    vault = FakeVault(collateral={HOTKEY: 78 * RAO})
    vault.keypair = SimpleNamespace(ss58_address='not-a-member')
    vault.slashed.add(SWAP)
    relay = _relay(vault=vault)
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    attest = next(c for c in relay.chain.calls if c[0] == 'vote_set_attestation')
    assert attest[3] == 78 * RAO
    assert relay.store.open_relay_slashes('tao') == []
    assert not [c for c in vault.calls if c[0] == 'vote_slash']


def test_an_unreadable_marker_keeps_the_verdict_netted():
    # Can't tell whether the seizure happened: stay pessimistic (netted) rather than release it.
    relay = _relay()
    _ingest(relay, [_timeout_event()])
    relay.vault.readable = False
    slash_job.settle_applied(relay)
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_a_revert_whose_marker_is_set_counts_as_success():
    # AlreadySlashed reaches us as a bare ContractReverted, so the marker is what we believe.
    vault = FakeVault()
    vault.reject.add('vote_slash')
    relay = _relay(vault=vault)
    _ingest(relay, [_timeout_event()])
    vault.slashed.add(SWAP)
    relay.step(NOW)
    assert relay.store.open_relay_slashes('tao') == []


def test_a_revert_with_no_marker_leaves_the_obligation_open():
    vault = FakeVault()
    vault.reject.add('vote_slash')
    relay = _relay(vault=vault)
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_an_unreadable_marker_is_never_treated_as_permission_to_slash_again():
    relay = _relay(vault=FakeVault(readable=False))
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']


def test_a_verdict_without_a_payee_is_recorded_but_not_relayed():
    # A payee-less verdict (a swap that timed out before the field existed) leaves nobody to pay.
    # The row still exists: it keeps netting and keeps blocking initiates until a peer's quorum lands.
    relay = _relay()
    _ingest(relay, [_timeout_event(payee='')])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert len(relay.store.open_relay_slashes('tao')) == 1


def test_a_verdict_is_relayed_from_the_payee_in_the_event():
    # A validator that never saw the swap live relays it anyway, because the verdict names who is owed.
    relay = _relay()
    _ingest(relay, [_timeout_event(payee=USER_TAO)])
    relay.step(NOW)

    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[2:] == (SWAP, 22 * RAO, USER_TAO, 22 * RAO), 'the event tuple, verbatim'
    assert relay.store.open_relay_slashes('tao') == []


def test_a_malformed_payee_in_the_event_is_refused_like_any_other():
    # The program never validated the user's backing-chain address, and putting it in the event
    # changed nothing about that — an unpayable one must not reach the vault.
    relay = _relay()
    _ingest(relay, [_timeout_event(payee='not-an-ss58')])
    relay.step(NOW)
    assert not [c for c in relay.vault.calls if c[0] == 'vote_slash']
    assert len(relay.store.open_relay_slashes('tao')) == 1


# --- the reimbursement snapshot -----------------------------------------------------------------


def test_a_snapshotless_validator_reads_the_hotkey_from_the_verdict_itself():
    # V-M1: no observe-time snapshot (down for the swap's whole life) — the verdict's pinned hotkey
    # is the seizure target, taking precedence over the live binding lookup.
    relay = _relay(attribution={MINER: HOTKEY2})  # live binding disagrees; the pin must win
    _ingest(relay, [_timeout_event(payee=USER_TAO, hotkey=bytes([1] * 32))])
    relay.step(NOW)
    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[1] == HOTKEY, 'the seizure targets the hotkey pinned on-chain at initiate'


def test_a_zeroed_verdict_hotkey_is_treated_as_unbound_not_a_target():
    # A pre-pin swap / never-bound miner emits [0]*32 — that must not become a vault AccountId; the
    # relay falls back to the live lookup exactly as before the field existed.
    relay = _relay()
    _ingest(relay, [_timeout_event(payee=USER_TAO, hotkey=bytes(32))])
    relay.step(NOW)
    slash = next(c for c in relay.vault.calls if c[0] == 'vote_slash')
    assert slash[1] == HOTKEY, 'zeros defer to the live binding'


# --- F3: the relay's own cursor, propagated failures, and heartbeat gating -----------------------


def test_a_failed_verdict_write_is_not_swallowed_and_holds_the_heartbeat(monkeypatch):
    relay = _relay()
    monkeypatch.setattr(
        relay.store, 'record_relay_slash', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('db down'))
    )
    assert _ingest(relay, [_timeout_event()]) is False
    assert relay._ingest_healthy is False
    # The liveness heartbeat must stay down while a verdict write is still dirty.
    relay.chain.calls.clear()
    relay.maybe_heartbeat(NOW + 10**9)
    assert not [c for c in relay.chain.calls if c[0] == 'vote_attest_heartbeat']


# --- the off-chain busy-until-settled backstop --------------------------------------------------


def test_a_miner_owing_an_unapplied_debit_blocks_its_own_next_initiate():
    relay = _relay()
    assert not relay.has_pending_debit(MINER)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW)
    assert relay.has_pending_debit(MINER)
    relay.store.mark_relay_slash_applied(SWAP)
    assert relay.has_pending_debit(MINER), 'applied on the vault is not yet reflected on Solana'
    relay.store.mark_relay_slashes_reflected('tao', MINER)
    assert not relay.has_pending_debit(MINER)


class _InboxChain(FakeChain):
    """Votes sit in the actuator's inbox instead of landing: a held hub, or a quorum still forming."""

    def vote(self, kind, subject, payload):
        self.calls.append((kind, *(payload or ())))
        if kind == 'vote_set_attestation':
            self.timeline.append('solana:vote_set_attestation')


def _stale_attestation(balance=100 * RAO):
    return SimpleNamespace(miner=MINER, chain='tao', effective_balance=balance, locked=True, epoch=3)


def test_the_entry_hold_outlives_the_vault_seizure_until_solana_shows_it():
    # A2: the netted attestation is only QUEUED, and the vault slash is voted in the same tick. Once
    # the vault applies it the verdict stops netting; if the hold dropped then, this validator would
    # vote a new TAO swap against the pre-slash 100 Solana still shows, backed by 78 of real bond.
    chain = _InboxChain(miner_states={MINER: _miner_state()}, attestations={MINER: _stale_attestation()})
    relay = _relay(solana=chain)
    relay._reconciled = True
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    assert relay.vault.slashed == {SWAP} and relay.store.open_relay_slashes('tao') == []
    assert chain.attestations[MINER].effective_balance == 100 * RAO
    assert relay.has_pending_debit(MINER), 'the seizure is real but Solana still advertises the old bond'

    relay.step(NOW + 1)  # still unlanded: the hold persists across ticks
    assert relay.has_pending_debit(MINER)

    chain.attestations[MINER] = _stale_attestation(78 * RAO)
    relay.step(NOW + 2)
    assert not relay.has_pending_debit(MINER)


def test_the_entry_hold_survives_a_restart_while_the_attestation_lags():
    store = _store()
    chain = _InboxChain(miner_states={MINER: _miner_state()}, attestations={MINER: _stale_attestation()})
    relay = _relay(solana=chain, store=store)
    relay._reconciled = True
    _ingest(relay, [_timeout_event()])
    relay.step(NOW)
    restarted = _relay(solana=chain, store=store, vault=relay.vault)
    assert restarted.has_pending_debit(MINER)
    restarted.step(NOW + 1)  # the boot reconcile re-reads it, and Solana still overstates the bond
    assert restarted.has_pending_debit(MINER)


# --- job 3: the exit sequence -------------------------------------------------------------------


def _exiting_relay(vault=None, **state):
    ms = _miner_state(active_backings=0, **state)
    relay = _relay(vault=vault, solana=FakeChain(miner_states={MINER: ms}))
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
    _fee(relay, SWAP2, MINER, 4 * RAO, NOW)

    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 4 * RAO)])]
    assert relay.vault.lock[HOTKEY] == (True, 3), 'still locked until the settle is on the books'

    relay._voted_at.clear()  # next pass, after the settle round applied
    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls[-1] == ('vote_unlock', HOTKEY, 3)


def test_the_unlock_names_the_current_epoch():
    # The epoch is hash-bound into the vault round, so a stale round can't unlock a re-locked bond.
    relay = _exiting_relay(vault=FakeVault(lock={HOTKEY: (True, 11)}))
    exit_relay.run_exits(relay, NOW)
    assert relay.vault.calls == [('vote_unlock', HOTKEY, 11)]


# --- the fee true-up cadence --------------------------------------------------------------------


def test_the_batch_vector_is_ordered_by_account_id_so_every_validator_agrees():
    # The round key is the batch-contents hash: a differently-ordered vector is a different round,
    # and the quorum would never converge.
    relay = _relay(attribution={MINER: HOTKEY2, MINER2: HOTKEY})  # deliberately "wrong" insertion order
    _fee(relay, SWAP, MINER, 5 * RAO, NOW)
    _fee(relay, SWAP2, MINER2, 7 * RAO, NOW)
    entries = exit_relay.cadence_entries(relay, NOW)
    assert [h for h, _ in entries] == [HOTKEY, HOTKEY2]
    assert codec.account_bytes(entries[0][0]) < codec.account_bytes(entries[1][0])


def test_a_miner_the_vault_has_fully_settled_is_left_out_so_old_and_fresh_validators_agree():
    # A validator that has run for months remembers a miner whose fees were settled long ago; a fresh
    # one never saw them. Listing by what is still owed gives both the same bytes.
    relay = _relay(vault=FakeVault(settled={HOTKEY: 5 * RAO}))
    _fee(relay, SWAP, MINER, 5 * RAO, NOW)
    _fee(relay, SWAP2, MINER2, 7 * RAO, NOW)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY2, 7 * RAO)]


def test_an_unreadable_settled_counter_yields_no_vector_rather_than_a_guessed_one():
    relay = _relay(vault=FakeVault(readable=False))
    _fee(relay, SWAP, MINER, 5 * RAO, NOW)
    assert exit_relay.cadence_entries(relay, NOW) is None


def test_the_round_waits_out_the_grace_so_a_fee_just_before_the_boundary_is_everywhere():
    relay = _relay(fee_cadence_secs=1000)
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE - 1)
    assert not relay.vault.calls
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 5 * RAO)])]


def test_the_round_waits_for_the_event_feed_to_catch_up():
    relay = _relay(fee_cadence_secs=1000)
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 10)
    relay.feed_caught_up = False
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE)
    assert not relay.vault.calls
    assert relay.store.get_relay_meta('fee_cadence_boundary') is None, 'the boundary is still owed'
    relay.feed_caught_up = True
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE + 12)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 5 * RAO)])]


def test_totals_are_read_at_the_boundary_not_at_the_moment_of_firing():
    relay = _relay()
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 100)
    _fee(relay, SWAP2, MINER, 6 * RAO, NOW + 100)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY, 5 * RAO)]


def test_an_unreadable_vault_defers_the_round_instead_of_deciding_blind():
    relay = _relay(vault=FakeVault(readable=False), fee_cadence_secs=1000)
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 10)
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE)
    assert not relay.vault.calls
    assert relay.store.get_relay_meta('fee_cadence_boundary') is None, 'the boundary is retried'


def test_a_miner_with_no_binding_is_excluded_rather_than_breaking_the_vector():
    relay = _relay(attribution={MINER2: HOTKEY2})
    _fee(relay, SWAP, MINER, 5 * RAO, NOW)
    _fee(relay, SWAP2, MINER2, 7 * RAO, NOW)
    assert exit_relay.cadence_entries(relay, NOW) == [(HOTKEY2, 7 * RAO)]


# --- heartbeat + the restart barrier ------------------------------------------------------------


def test_the_heartbeat_stays_down_until_the_backlog_is_drained():
    # The fuse being closed while this validator is behind is the point: TAO entry reopens only
    # once the relay has proved it is caught up.
    vault = FakeVault(readable=False)
    relay = _relay(vault=vault, heartbeat_interval_secs=1)
    relay.mark_dirty(MINER)
    relay.step(NOW)
    assert not [c for c in relay.chain.calls if c[0] == 'vote_attest_heartbeat']
    assert not relay._reconciled


def test_the_heartbeat_is_released_once_reconcile_comes_back_clean():
    relay = _relay(heartbeat_interval_secs=1)
    relay.step(NOW)
    assert relay._reconciled
    assert ('vote_attest_heartbeat',) in relay.chain.calls


def test_the_startup_pass_is_not_repeated_in_the_same_step():
    # The barrier's pass is a full reconcile, so the periodic one must count from it. Walking the
    # vault twice in the boot step doubles a stall the forward loop is already waiting out.
    relay = _relay()
    reads = []
    read = relay.vault.get_collateral
    relay.vault.get_collateral = lambda hotkey: reads.append(hotkey) or read(hotkey)
    relay.step(NOW)
    assert relay._reconciled
    assert sorted(reads) == sorted([HOTKEY, HOTKEY2]), 'one walk of the bound set, not two'
    assert relay._next_reconcile == NOW + relay.cfg.reconcile_interval_secs


def _ring_relay(count, **cfg):
    """A booted relay over `count` bound miners with no bond, and the list its vault reads land in."""
    miners = {str(Pubkey(bytes([i + 40] * 32))): codec.account_ss58(bytes([i + 10] * 32)) for i in range(count)}
    relay = _relay(vault=FakeVault(collateral={}, lock={}), attribution=miners, **cfg)
    relay.step(NOW)
    reads = []
    read = relay.vault.get_collateral
    relay.vault.get_collateral = lambda hotkey: reads.append(hotkey) or read(hotkey)
    return relay, miners, reads


def test_after_boot_the_audit_walks_the_bound_set_a_few_miners_per_step():
    # The full walk in one step is what stalled the forward loop every interval. After the boot
    # audit the same repair is spread over the interval: each step re-reads only the miners due.
    relay, miners, reads = _ring_relay(10, reconcile_interval_secs=100)
    per_step = []
    for elapsed in range(10, 100, 10):
        before = len(reads)
        relay.step(NOW + elapsed)
        per_step.append(len(reads) - before)
    assert per_step == [1] * 9
    relay.step(NOW + 100)
    assert sorted(reads) == sorted(miners.values()), 'every miner re-checked once inside the interval'


def test_a_lap_the_steps_were_too_slow_for_is_finished_at_its_boundary():
    relay, miners, reads = _ring_relay(10, reconcile_interval_secs=100)
    relay.step(NOW + 30)
    assert len(reads) == 3
    relay.step(NOW + 250)  # one long stall later, the interval is still a guarantee
    assert sorted(reads) == sorted(miners.values())


def test_a_miner_that_binds_mid_lap_joins_the_ring_at_the_next_lap():
    relay, miners, reads = _ring_relay(4, reconcile_interval_secs=100)
    late = codec.account_ss58(bytes([99] * 32))
    relay.chain.attributions['MinerLate'] = late
    relay.step(NOW + 50)
    assert late not in reads, 'the ring only learns who exists when a lap opens'
    relay.step(NOW + 100)  # closes the first lap and opens the next
    assert late in reads, 'a newcomer is read as its lap opens, not when the walk reaches it'


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

    second = _relay(store=store, solana=FakeChain(miner_states={MINER: _miner_state(active_backings=0)}))
    assert MINER not in second._exiting
    second.reconcile(NOW)
    assert MINER in second._exiting
    assert [c for c in second.vault.calls if c[0] == 'vote_unlock']


def test_reconcile_repairs_an_attestation_that_drifted_while_we_were_down():
    solana = FakeChain(
        miner_states={MINER: _miner_state()},
        attestations={MINER: SimpleNamespace(chain='tao', miner=MINER, effective_balance=1, locked=True, epoch=3)},
    )
    relay = _relay(solana=solana)
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 100 * RAO, True, 3) in solana.calls


def test_reconcile_discovers_a_first_bond_no_event_ever_told_us_about():
    # A miner posts and locks on the vault before any attestation exists. If that pair of events
    # lands while this validator is down, nothing else would discover it — so reconcile enumerates
    # the whole bound set, not just the attested one.
    solana = FakeChain(miner_states={MINER: _miner_state()})
    relay = _relay(solana=solana)
    assert solana.attestations.get(MINER) is None
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 100 * RAO, True, 3) in solana.calls


def test_a_bond_netted_to_zero_is_still_asserted():
    # Nets-to-zero is a real claim — "this miner's bond is entirely spoken for" — and the entry
    # guards must see it. Only "no bond at all" is silence.
    solana = FakeChain(miner_states={MINER: _miner_state()})
    relay = _relay(vault=FakeVault(collateral={HOTKEY: 5 * RAO}), solana=solana)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 50 * RAO, 50 * RAO, USER_TAO, NOW)
    relay.reconcile(NOW)
    assert ('vote_set_attestation', MINER, 'tao', 0, True, 3) in solana.calls


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
    solana = FakeChain(
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

    relay = _relay(vault=vault)
    relay._world = lambda: None  # no World to read the config from
    assert attestation_job.compute(relay, MINER, HOTKEY) is None


def test_attestation_does_not_recharge_fees_the_retired_vault_settled():
    """The live bug: 0.8 τ posted to a fresh vault was attested as 0.7982 τ because 1.8M rao of
    already-settled fees came along from the retired vault."""
    vault = FakeVault()
    vault.lock[HOTKEY] = (True, 1)
    vault.collateral[HOTKEY] = 800_000_000
    vault.settled[HOTKEY] = 0  # a fresh vault has collected nothing yet
    solana = FakeChain(
        miner_states={MINER: _miner_state()},
        config=SimpleNamespace(last_attest_heartbeat=0, vault_generation=1),
    )
    relay = _relay(vault=vault, solana=solana)
    _fee(relay, 'old', MINER, 1_800_000, NOW - 500, generation=0)

    desired = attestation_job.compute(relay, MINER, HOTKEY)
    assert desired.effective_balance == 800_000_000, 'the retired vault already collected that fee'

    # A fee earned under the CURRENT generation still nets off, exactly as before.
    _fee(relay, 'new', MINER, 2_000_000, NOW, generation=1)
    assert attestation_job.compute(relay, MINER, HOTKEY).effective_balance == 798_000_000


def test_two_validators_with_different_histories_attest_and_true_up_the_same_figures():
    """The 3.8.0 two-validator deadlock: each validator netted fees off its own event history, one had
    seen a stretch of fees the other hadn't, and neither attestation could ever reach quorum. The total
    is now the on-chain counter, so only the fees after a boundary come from local history, and the
    feed replays those to every validator."""
    vault = FakeVault(collateral={HOTKEY: 100 * RAO}, settled={HOTKEY: 4 * RAO})
    veteran = _relay(vault=vault)
    _fee(veteran, 'old1', MINER, 6 * RAO, NOW - 90_000)  # long before either's history window
    _fee(veteran, 'old2', MINER, 3 * RAO, NOW - 50_000)
    _fee(veteran, 'new', MINER, 1 * RAO, NOW + 100)  # after the boundary: inside every history
    fresh = _relay(vault=vault, solana=veteran.chain)  # same chain, a database that saw only the last fee
    fresh.store.record_relay_fee('new', MINER, 'tao', 1 * RAO, NOW + 100)

    assert attestation_job.compute(veteran, MINER, HOTKEY) == attestation_job.compute(fresh, MINER, HOTKEY)
    assert attestation_job.compute(fresh, MINER, HOTKEY).effective_balance == (100 - (10 - 4)) * RAO
    assert exit_relay.cadence_entries(veteran, NOW) == exit_relay.cadence_entries(fresh, NOW) == [(HOTKEY, 9 * RAO)]


def test_a_miner_state_without_the_fee_counter_is_unknown_not_zero():
    # A pre-v17 account: attesting as if it owed nothing would hand back bond its fees already spent.
    relay = _relay(solana=FakeChain(miner_states={MINER: SimpleNamespace(miner=MINER, active_backings=2)}))
    assert relay.accrued_fees(MINER) is None
    assert attestation_job.compute(relay, MINER, HOTKEY) is None
    assert exit_relay.cadence_entries(relay, NOW) is None, 'the true-up declines rather than leaving it out'


def test_only_miners_owing_the_current_vault_are_fee_debtors():
    solana = FakeChain(config=SimpleNamespace(last_attest_heartbeat=0, vault_generation=2))
    relay = _relay(solana=solana)
    _fee(relay, SWAP, MINER, 5 * RAO, NOW, generation=1)  # a retired vault's counter
    _fee(relay, SWAP2, MINER2, 7 * RAO, NOW, generation=2)
    assert relay.fee_debtors() == [MINER2]


def test_vault_event_poll_walks_a_long_gap_one_window_per_step():
    vault = FakeVault()
    vault.head = lambda: 1000
    relay = _relay(vault=vault)
    relay.store.set_relay_meta('vault_event_block', 0)

    relay.poll_vault_events()
    relay.poll_vault_events()

    assert [c for c in vault.calls if c[0] == 'poll_events'] == [('poll_events', 1, 50), ('poll_events', 51, 100)]
    assert relay.store.get_relay_meta('vault_event_block') == '100'


def test_an_unreadable_ledger_fails_closed():
    relay = _relay()
    relay.store.owes_relay_debit = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('disk'))
    assert relay.has_pending_debit(MINER), 'a ledger we cannot read must not open a new swap'


def test_an_audit_before_the_world_is_complete_does_not_release_the_barrier():
    relay = _relay()
    relay.chain.complete = False  # no verified bindings yet: an audit of nobody must not read clean
    relay.step(NOW)
    assert not relay._reconciled
    relay.chain.complete = True
    relay.step(NOW + 12)
    assert relay._reconciled


def test_a_step_never_raises():
    relay = _relay()
    relay.chain.world = lambda: (_ for _ in ()).throw(RuntimeError('no world'))
    relay._world = relay.chain.world
    relay.step(NOW)  # must not raise


# --- two validators: what each one sends must meet the other's in the same round -----------------------


def test_a_fee_batch_the_vault_refused_is_not_resent_every_tick_but_a_changed_batch_is():
    relay = _relay(fee_cadence_secs=1000)
    relay.vault.reject.add('vote_collect_fees_batch')  # e.g. this exact batch already voted, round still open
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 10)
    for tick in range(10):
        exit_relay.maybe_cadence_settle(relay, NOW + GRACE + 12 * tick)
    assert relay.vault.calls == [('vote_collect_fees_batch', [(HOTKEY, 5 * RAO)])]
    assert relay.store.get_relay_meta('fee_cadence_boundary') is None, 'the boundary is still owed'
    _fee(relay, SWAP2, MINER, 6 * RAO, NOW - 5)  # a new fee: a different round
    exit_relay.maybe_cadence_settle(relay, NOW + GRACE + 200)
    assert relay.vault.calls[-1] == ('vote_collect_fees_batch', [(HOTKEY, 11 * RAO)])


def test_a_validator_outside_the_vault_set_holds_its_vault_votes_until_admitted():
    vault = FakeVault()
    vault.keypair = SimpleNamespace(ss58_address='beta')  # the set is ['us']
    relay = _relay(vault=vault, fee_cadence_secs=1000)
    _fee(relay, SWAP, MINER, 5 * RAO, NOW - 10)
    relay.step(NOW + GRACE)
    assert relay._reconciled, 'a non-member owes the vault nothing, so the barrier opens'
    assert not [c for c in vault.calls if c[0].startswith('vote_')]
    vault.validators.append('beta')
    relay.step(NOW + GRACE + 600)  # membership is re-read on its interval
    assert ('vote_collect_fees_batch', [(HOTKEY, 5 * RAO)]) in vault.calls


def test_after_an_unmet_round_two_validators_out_of_phase_still_meet_in_one_round():
    # The heartbeat is overdue (the last round never reached quorum) and the two validators came back two
    # hours apart. Each must keep voting while it is overdue, so their votes share a live round (1800s).
    interval = 12 * 3600
    config = SimpleNamespace(last_attest_heartbeat=NOW - 2 * interval)
    a, b = (_relay(solana=FakeChain(config=config), heartbeat_interval_secs=interval) for _ in range(2))
    voted = {id(a): [], id(b): []}
    for t in range(0, 4 * 3600, 12):
        for relay, joined in ((a, 0), (b, 2 * 3600)):
            if t < joined:
                continue
            before = len(relay.chain.calls)
            relay.maybe_heartbeat(NOW + t)
            if len(relay.chain.calls) > before:
                voted[id(relay)].append(t)
    assert any(abs(x - y) < 1800 for x in voted[id(a)] for y in voted[id(b)])


def test_an_unmet_heartbeat_round_is_revoted_once_per_round_lifetime_until_it_lands():
    interval = 12 * 3600
    boundary = (NOW // interval + 1) * interval
    config = SimpleNamespace(last_attest_heartbeat=boundary - interval)
    relay = _relay(solana=FakeChain(config=config), heartbeat_interval_secs=interval)
    relay._reconciled = True
    for t in range(0, 3600, 12):
        relay.maybe_heartbeat(boundary + t)
    assert len(relay.chain.calls) == 2  # at the boundary and once more when the first round lapsed
    config.last_attest_heartbeat = boundary + 3600  # quorum landed
    relay.maybe_heartbeat(boundary + 7200)
    assert len(relay.chain.calls) == 2
