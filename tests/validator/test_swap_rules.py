"""The swap rules (``swaps.decide``) through the production context, with every chain question answered
on the spot by the providers below instead of a watcher thread. No chain, no votes."""

from dataclasses import replace
from types import SimpleNamespace

from allways.assets.asset import ProviderUnreachableError
from allways.constants import CANCEL_REASON_INVALID_DEST
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.actuator.swap_context import SwapMemory, WorldContext
from allways.validator.actuator.swaps import COLLATERAL_REJECT_REASON, Decision, decide
from allways.validator.chains.watcher import Verdict
from allways.validator.store.db import Store
from allways.validator.world.snapshot import Fulfillment, World, tx_key

INITIATED_AT = 1000  # dest-freshness floor
RESV_CREATED_AT = 1200  # source-freshness floor
FRESH = 5000  # block_time comfortably after both floors
DEFAULT_CLAIM_KEY = swap_key_from_tx_hash('srctx')  # keccak of make_swap's default from_tx_hash — the live claim


def make_swap(
    status='Fulfilled',
    to_amount=1000,
    from_amount=500,
    timeout_at=2000,
    max_extend_at=10_000,
    key=b'\x01' * 32,
    rate='5',
    from_chain='btc',
    to_chain='sol',
    collateral_chain='sol',
    collateral_amount=None,
):
    if collateral_amount is None:  # the program's invariant: an exact backing leg IS collateral_amount
        collateral_amount = from_amount if collateral_chain == from_chain else to_amount
    return SimpleNamespace(
        swap_key=key,
        miner='minerPK',
        status=status,
        from_chain=from_chain,
        to_chain=to_chain,
        collateral_chain=collateral_chain,
        collateral_amount=collateral_amount,
        from_tx_hash='srctx',
        to_tx_hash='dsttx',
        miner_from_addr='minerBTC',
        miner_to_addr='minerSOL',
        user_from_addr='userBTC',
        user_to_addr='userSOL',
        from_amount=from_amount,
        to_amount=to_amount,
        rate=rate,
        from_tx_block=0,
        to_tx_block=0,
        timeout_at=timeout_at,
        max_extend_at=max_extend_at,
        initiated_at=INITIATED_AT,
    )


class RecordingProvider:
    """verify_transaction → matched info / None (absent or detail mismatch) / raises (unreachable). result
    True=confirmed, False=matched-but-unconfirmed (extendable), None=absent. block_time, confirmations and a
    per-chain replay_grace_secs feed the freshness + extension-target math."""

    def __init__(self, result=True, block_time=FRESH, grace=0, confirmations=0, valid_addr=True):
        self.result = result
        self.block_time = block_time
        self.grace = grace
        self.confirmations = confirmations
        self.refuses = False
        self.valid_addr = valid_addr
        self.cancel_reason = None  # sound no-fault-cancel evidence (a CANCEL_REASON_* int), else None
        self.calls = []

    def delivery_refused(self, address, since_unix):
        if self.refuses == 'unreachable':
            raise ProviderUnreachableError('down')
        return self.refuses

    def cancel_evidence(self, address, amount, tx_hash=None, from_address=None, block_hint=0):
        self.calls.append(('cancel_evidence', block_hint))
        if self.cancel_reason == 'raise':
            raise ProviderUnreachableError('down')
        return self.cancel_reason

    @property
    def chain(self):
        return self  # fused-provider shape: the asset is its own chain

    def is_valid_address(self, address):
        return self.valid_addr

    def normalize_address(self, address):
        return address.lower() if isinstance(address, str) else address  # mirror the EVM provider

    def canonical_tx_hash(self, tx_hash):
        return tx_hash

    @property
    def chain_def(self):
        return SimpleNamespace(replay_grace_secs=self.grace)

    def verify_transaction(self, tx_hash, expected_recipient, expected_amount, block_hint=0, expected_sender=None):
        self.calls.append(SimpleNamespace(tx_hash=tx_hash, recipient=expected_recipient, amount=expected_amount))
        if self.result == 'unreachable':
            raise ProviderUnreachableError('down')
        if self.result is None:
            return None  # tx not found / details mismatch
        return SimpleNamespace(
            confirmed=bool(self.result), block_time=self.block_time, confirmations=self.confirmations
        )


def make_reservation(created_at=RESV_CREATED_AT, reserved_until=1_000_000, max_extend_at=10_000, claimed_swap_key=None):
    # Defaults model a LIVE reservation pinning make_swap's default claim (reserved_until well past any test
    # `now`, claim slot pointing at the swap) so PendingAttestation decisions aren't reaped as stale.
    return SimpleNamespace(
        created_at=created_at,
        reserved_until=reserved_until,
        max_extend_at=max_extend_at,
        claimed_swap_key=DEFAULT_CLAIM_KEY if claimed_swap_key is None else claimed_swap_key,
    )


class _Answer:
    """A watcher that answers each probe immediately against its provider, as of ``now``."""

    def __init__(self, provider, now):
        self.provider, self.now = provider, now

    def verdict(self, probe):
        try:
            return Verdict(1, self.now, info=probe.run(self.provider))
        except Exception as e:
            return Verdict(1, self.now, error=str(e), unreachable=isinstance(e, ProviderUnreachableError))


class _Watchers(dict):
    def __init__(self, providers, now):
        super().__init__()
        self.providers, self.now = providers, now

    def __contains__(self, chain):
        return chain in self.providers

    def get(self, chain, default=None):
        return _Answer(self.providers.get(chain), self.now)


class _Context(WorldContext):
    def __init__(self, rules, now):
        world = World(now, 1, True, None, {})
        super().__init__(world, _Watchers(rules.providers, now), rules.providers, rules.memory, now, rules.owes)
        self._reservation = rules.reservation

    def reservation(self, swap):
        return self._reservation


class Rules:
    def __init__(self, providers, reservation, owes=None):
        self.providers, self.reservation, self.owes = providers, reservation, owes
        self.memory = SwapMemory(Store(':memory:'))

    def decide(self, swap, now):
        return decide(swap, _Context(self, now), now, fee_divisor=100)


def rules_with(result=True, created_at=RESV_CREATED_AT, reservation=None):
    providers = {'btc': RecordingProvider(result), 'sol': RecordingProvider(result)}
    resv = reservation if reservation is not None else make_reservation(created_at=created_at)
    return Rules(providers, resv), providers


def _tao_backed(owes, backing='tao'):
    """sol→tao: the one pair both purses can legally back. ``owes``: the relay's pending-debit answer."""
    swap = make_swap(
        status='PendingAttestation',
        from_chain='sol',
        to_chain='tao',
        collateral_chain=backing,
        from_amount=1_000_000_000,
        to_amount=5_000_000_000,
    )
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    return Rules(providers, make_reservation(), None if owes is None else (lambda s: owes)), swap


def test_fulfilled_confirms_and_fetches_only_the_dest_leg():
    rules, providers = rules_with(result=True)
    swap = make_swap(status='Fulfilled', to_amount=1000)
    assert rules.decide(swap, now=1500).decision == Decision.CONFIRM
    # dest leg verified against 99% of to_amount, not the full amount
    dest_call = providers['sol'].calls[-1]
    assert dest_call.amount == 990 and dest_call.recipient == 'userSOL'
    # P0: the SOURCE leg is NOT re-fetched for a Fulfilled swap — it was already verified + frozen at
    # attestation. Fulfilled must make exactly ONE leg fetch (dest only).
    assert providers['btc'].calls == [], 'Fulfilled re-fetched the source leg — should be dest-only'


def test_fulfilled_source_provider_down_still_confirms():
    # P0 behavior note: a source-provider OUTAGE no longer blocks confirming an already-attested payout,
    # because the source isn't re-fetched. (Pre-P0 this SKIPped on the source `down` guard.)
    rules, providers = rules_with(result=True)
    providers['btc'].result = 'unreachable'  # source provider down — must be irrelevant now
    assert rules.decide(make_swap(status='Fulfilled'), now=1500).decision == Decision.CONFIRM
    assert providers['btc'].calls == []  # source never touched


def _delivery_pair(second_miner='minerPK'):
    rules, providers = rules_with()
    first, second = make_swap(key=b'\x01' * 32), make_swap(key=b'\x02' * 32)
    second.from_tx_hash = 'srctx2'  # a swap's key is its deposit's: two swaps, two deposits
    first.to_tx_hash, second.to_tx_hash = '0x' + 'ab' * 32, '0X' + 'AB' * 32
    second.miner = second_miner
    return rules, providers, first, second


def test_delivery_confirms_one_swap_per_miner():
    # One delivery tx on two of a miner's swaps, in any casing: the first verified keeps it.
    rules, _, first, second = _delivery_pair()
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM
    action = rules.decide(second, now=1500)
    assert action.decision == Decision.WAIT and 'another swap' in action.reason
    assert rules.decide(first, now=1501).decision == Decision.CONFIRM  # the second never blocks the first
    assert rules.decide(second, now=2500).decision == Decision.TIMEOUT


def test_unverified_delivery_does_not_use_the_tx():
    # A hash only counts once it verifies; a failing submission leaves it free for the real swap.
    rules, providers, first, second = _delivery_pair()
    providers['sol'].result = None
    assert rules.decide(second, now=1500).decision == Decision.WAIT
    providers['sol'].result = True
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM


def test_another_miner_naming_the_tx_cannot_block_the_swap():
    rules, _, first, second = _delivery_pair(second_miner='otherPK')
    assert rules.decide(second, now=1500).decision == Decision.CONFIRM
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM


def test_fulfilled_dest_pending_far_from_timeout_waits():
    rules, providers = rules_with(result=True)
    providers['sol'].result = False  # dest matched but unconfirmed, timeout far off
    assert rules.decide(make_swap(status='Fulfilled'), now=1500).decision == Decision.WAIT


def test_fulfilled_stale_dest_tx_rejected_as_replay():
    rules, providers = rules_with(result=True)
    providers['sol'].block_time = INITIATED_AT - 1  # payout mined before the swap was initiated
    assert rules.decide(make_swap(status='Fulfilled'), now=1500).decision == Decision.WAIT


def test_fulfilled_dest_missing_block_time_rejected():
    rules, providers = rules_with(result=True)
    providers['sol'].block_time = None  # cannot prove freshness → fail closed
    assert rules.decide(make_swap(status='Fulfilled'), now=1500).decision == Decision.WAIT


def test_fulfilled_overdue_absent_dest_times_out():
    # Absent/mismatched dest past the deadline must not escape the slash → TIMEOUT (Active|Fulfilled).
    rules, providers = rules_with(result=True)
    providers['sol'].result = None  # dest tx absent
    assert rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == Decision.TIMEOUT


def test_fulfilled_overdue_refusing_dest_waits_no_slash():
    # B-lite slash gate: positive evidence the dest can't receive (code seen in window) → never slash.
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = True
    assert rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == Decision.WAIT


def test_fulfilled_overdue_refusal_check_down_defers():
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = 'unreachable'
    assert rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == Decision.WAIT


def test_fulfilled_refusing_dest_past_ceiling_times_out():
    # C1 terminal exit: an undeliverable dest never recovers, so at/after max_extend_at the refusal guard
    # must stop deferring and TIMEOUT — else has_active_swap never clears and the hub's collateral freezes.
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = True
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1200)  # now past the ceiling
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_active_overdue_refusing_dest_waits_no_slash():
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = True
    assert rules.decide(make_swap(status='Active', timeout_at=1000), now=1500).decision == Decision.WAIT


def test_active_invalid_dest_refuses_before_ceiling():
    rules, providers = rules_with(result=None)
    providers['sol'].valid_addr = False
    action = rules.decide(make_swap(status='Active', timeout_at=1000), now=1500)
    assert action.decision == Decision.REFUSE
    assert action.reason_code == CANCEL_REASON_INVALID_DEST


def test_fulfilled_sound_cancel_evidence_refuses_no_slash():
    # Sound evidence the dest can't receive (miner's reverted tx / issuer freeze / reserved account) →
    # REFUSE (cancel_swap, no slash/fee/strike), carrying the reason code.
    rules, providers = rules_with(result=None)
    providers['sol'].cancel_reason = 3  # CANCEL_REASON_SOL_RESERVED
    action = rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500)
    assert action.decision == Decision.REFUSE
    assert action.reason_code == 3


def test_cancel_evidence_is_read_at_the_block_the_delivery_landed():
    # An alpha refusal is recorded at the delivery's block, so the probe passes the swap's to_tx_block (#798).
    rules, providers = rules_with(result=None)
    swap = make_swap(status='Fulfilled', timeout_at=1000)
    swap.to_tx_block = 8_257_100
    rules.decide(swap, now=1500)
    assert ('cancel_evidence', 8_257_100) in providers['sol'].calls


def test_cancel_evidence_fires_early_before_deadline():
    # Cancel is strictly lenient, so it fires ahead of the deadline — no waiting for the ceiling.
    rules, providers = rules_with(result=None)
    providers['sol'].cancel_reason = 3
    action = rules.decide(make_swap(status='Fulfilled', timeout_at=9999), now=1500)  # not overdue
    assert action.decision == Decision.REFUSE


def test_active_sound_cancel_evidence_refuses_without_fulfillment():
    # Solana/ERC-20 refusal is provable without a delivery tx, so an Active swap into an unpayable dest
    # cancels immediately rather than waiting for a fulfillment that can never land.
    rules, providers = rules_with(result=None)
    providers['sol'].cancel_reason = 3
    action = rules.decide(make_swap(status='Active', timeout_at=9999), now=1500)
    assert action.decision == Decision.REFUSE


def test_getcode_hint_without_sound_evidence_does_not_refuse():
    # A getCode/issuer HINT (delivery_refused) is NOT sufficient to cancel — only sound cancel_evidence is.
    # With no sound evidence and before the ceiling, the hint defers (WAIT), never REFUSE and never slash.
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = True  # hint only
    providers['sol'].cancel_reason = None  # no sound evidence
    action = rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500)
    assert action.decision == Decision.WAIT


def test_active_refusing_dest_past_ceiling_times_out():
    # C1: past max_extend_at an undeliverable dest is terminal — timing out is the only path that clears
    # the hub bit, so the refusal guard must yield to a TIMEOUT once the ceiling is reached.
    rules, providers = rules_with(result=None)
    providers['sol'].refuses = True
    swap = make_swap(status='Active', timeout_at=1000, max_extend_at=1200)  # now past the ceiling
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_active_invalid_dest_refuses_past_ceiling():
    rules, providers = rules_with(result=None)
    providers['sol'].valid_addr = False
    swap = make_swap(status='Active', timeout_at=1000, max_extend_at=1200)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REFUSE
    assert action.reason_code == CANCEL_REASON_INVALID_DEST


def test_fulfilled_invalid_dest_refuses_when_provider_is_reachable():
    rules, providers = rules_with(result=True)
    providers['sol'].valid_addr = False
    action = rules.decide(make_swap(status='Fulfilled'), now=1500)
    assert action.decision == Decision.REFUSE
    assert action.reason_code == CANCEL_REASON_INVALID_DEST


def test_fulfilled_overdue_but_verifiable_still_confirms():
    # Overdue is irrelevant when both legs verify — a good fulfillment still confirms.
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == Decision.CONFIRM


def test_fulfilled_pending_dest_at_ceiling_overdue_times_out():
    # Accountability guardrail: a payout that's broadcast but still unconfirmed, whose extension budget
    # is exhausted (timeout_at == max_extend_at, no room left to slide) and is now overdue, MUST slash.
    # The ceiling is ample runway (MAX_TOTAL_EXTENSION_SECS); failing to confirm within it means the
    # miner underfee'd/mis-sent the payout — its responsibility, so the timeout is warranted, not a
    # false slash. This tripwire guards that the exhausted-extension path still slashes.
    rules, providers = rules_with(result=True)
    providers['sol'].result = False  # dest matched but unconfirmed (pending)
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1000)  # at ceiling — no extend room
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_pending_attestation_source_ok_attests():
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.ATTEST


def test_pending_attestation_rejects_dest_equal_to_miner_delivery():
    # V-H1 (chain-aware attest gate): user_to_addr NORMALIZE-equal to the miner's delivery address → never
    # attest. Catches the EVM checksum-variant a self-represented taker uses to slip the on-chain raw
    # compare (provider normalize lowercases). Poisoned dest → the miner must never be obligated.
    rules, _ = rules_with(result=True)
    swap = make_swap(status='PendingAttestation')
    swap.user_to_addr = swap.miner_to_addr.upper()  # case-variant of the pinned miner delivery address
    assert rules.decide(swap, now=1500).decision == Decision.REJECT


def test_pending_attestation_source_missing_waits():
    rules, _ = rules_with(result=None)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_pending_attestation_stale_deposit_rejected_as_replay():
    rules, providers = rules_with(result=True)
    providers['btc'].block_time = RESV_CREATED_AT - 1  # deposit predates the reservation
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_pending_attestation_no_reservation_waits():
    rules, _ = rules_with(result=True)
    rules.reservation = None
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_pending_attestation_same_second_deposit_attests():
    # A deposit sent immediately after reserving lands in the same unix second as the floor; block_time
    # granularity is seconds, so `block_time == floor` is honest, not a replay.
    rules, providers = rules_with(result=True)
    providers['btc'].block_time = RESV_CREATED_AT
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.ATTEST


def test_pending_attestation_grace_allows_slightly_old_deposit():
    rules, providers = rules_with(result=True)
    providers['btc'].grace = 300
    providers['btc'].block_time = RESV_CREATED_AT - 100  # within grace of the floor
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.ATTEST


def test_pending_attestation_honest_sol_to_btc_attests():
    # Forward direction (is_reverse=False): rate 5 → 1 SOL (1e9 lamports) maps to 5e8 sats.
    rules, _ = rules_with(result=True)
    swap = make_swap(
        status='PendingAttestation', from_chain='sol', to_chain='btc', from_amount=1_000_000_000, to_amount=500_000_000
    )
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


def test_pending_attestation_honest_btc_to_sol_reverse_attests():
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='PendingAttestation', to_amount=1000), now=1500).decision == Decision.ATTEST


def test_pending_attestation_absurd_to_amount_rejected():
    rules, _ = rules_with(result=True)
    swap = make_swap(status='PendingAttestation', to_amount=1000 * 10_000)  # expected 1000
    assert rules.decide(swap, now=1500).decision == Decision.REJECT


def _alpha(value_rao):
    """sol→sn7, tao-backed: the sn7 leg is declared and pinned to the fill block."""
    rules, providers = rules_with(result=True)
    price_calls = []

    def price(amount, block=None):
        price_calls.append((amount, block))
        return value_rao(amount)

    providers['sn7'] = SimpleNamespace(
        value_rao=price,
        price_calls=price_calls,
        chain=SimpleNamespace(
            block_at=lambda created_at: 777,
            normalize_address=str,
            is_valid_address=lambda a: True,
        ),
    )
    swap = make_swap(
        status='PendingAttestation',
        from_chain='sol',
        to_chain='sn7',
        collateral_chain='tao',
        from_amount=1_000_000_000,
        to_amount=5_000_000_000,
        collateral_amount=10**9,
    )
    return rules, providers, swap


def test_pending_attestation_computes_and_saves_pinned_pass():
    rules, providers, swap = _alpha(lambda amount: 10**9)
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST
    assert providers['sn7'].price_calls == [(5_000_000_000, 777)]
    assert rules.memory.collateral[('minerPK', 'tao', RESV_CREATED_AT, 10**9)] is True


def test_pending_attestation_saved_pass_does_not_reprice():
    rules, providers, swap = _alpha(lambda amount: (_ for _ in ()).throw(AssertionError('repriced')))
    rules.memory.record_collateral(('minerPK', 'tao', RESV_CREATED_AT, 10**9), True)
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST
    assert providers['sn7'].price_calls == []


def test_pending_attestation_saved_fail_rejects_without_repricing():
    rules, providers, swap = _alpha(lambda amount: (_ for _ in ()).throw(AssertionError('repriced')))
    rules.memory.record_collateral(('minerPK', 'tao', RESV_CREATED_AT, 10**9), False)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT
    assert providers['sn7'].price_calls == []
    assert providers['sol'].calls == []
    assert action.reason == COLLATERAL_REJECT_REASON


def test_pending_attestation_ignores_saved_verdict_for_different_collateral():
    rules, providers, swap = _alpha(lambda amount: 2 * 10**9)
    rules.memory.record_collateral(('minerPK', 'tao', RESV_CREATED_AT, 2 * 10**9), True)

    assert rules.decide(swap, now=1500).decision == Decision.REJECT
    assert providers['sn7'].price_calls == [(5_000_000_000, 777)]


def test_pending_attestation_unreachable_pinned_price_skips():
    def down(amount):
        raise ProviderUnreachableError('price')

    rules, _, swap = _alpha(down)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.SKIP and 'price' in action.reason


def test_pending_attestation_price_move_after_fill_does_not_change_verdict():
    price = {'value': 10**9}
    rules, providers, swap = _alpha(lambda amount: price['value'])
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST
    price['value'] = 2 * 10**9
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST
    assert providers['sn7'].price_calls == [(5_000_000_000, 777)]


def test_pending_attestation_exact_leg_never_reads_a_price():
    def never(amount):
        raise AssertionError('an exact leg was priced')

    rules, providers = rules_with(result=True)
    for provider in providers.values():
        provider.value_rao = never
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.ATTEST


def test_pending_attestation_to_amount_off_by_two_rejected():
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='PendingAttestation', to_amount=1002), now=1500).decision == Decision.REJECT


def test_pending_attestation_to_amount_off_by_one_attests():
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='PendingAttestation', to_amount=1001), now=1500).decision == Decision.ATTEST


def test_pending_attestation_garbage_rate_zero_expected_rejected():
    rules, _ = rules_with(result=True)
    swap = make_swap(status='PendingAttestation', rate='0', to_amount=1000)  # expected_to == 0
    assert rules.decide(swap, now=1500).decision == Decision.REJECT


def test_pending_attestation_zero_to_amount_rejected():
    rules, _ = rules_with(result=True)
    assert rules.decide(make_swap(status='PendingAttestation', to_amount=0), now=1500).decision == Decision.REJECT


def test_active_timed_out_vs_waiting():
    rules, _ = rules_with()
    assert rules.decide(make_swap(status='Active', timeout_at=1000), now=1500).decision == Decision.TIMEOUT
    assert rules.decide(make_swap(status='Active', timeout_at=2000), now=1500).decision == Decision.WAIT


def test_active_overdue_never_extends():
    # Active has no mark_fulfilled = no broadcast evidence → overdue must slash, never extend.
    rules, providers = rules_with(result=False)  # even a pending-looking source can't save an Active
    swap = make_swap(status='Active', timeout_at=1000, max_extend_at=10_000)
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_provider_unreachable_skips():
    rules, _ = rules_with(result='unreachable')
    assert rules.decide(make_swap(status='Fulfilled'), now=1500).decision == Decision.SKIP


def test_provider_unreachable_past_ceiling_times_out():
    # An unjudgeable dest leg (RPC down, or meta with no token-balance arrays) defers only up to
    # max_extend_at — same bound as a missing provider — else the collateral never frees.
    rules, _ = rules_with(result='unreachable')
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1200)
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


# ── deadline extensions for valid-but-unconfirmed legs ──


def test_source_pending_near_expiry_extends_reservation():
    # BTC source pending at 0/2 confs: raw = now + 2*600 + 120 = 2820 → 600s bucket lands on 3000.
    resv = make_reservation(reserved_until=1600, max_extend_at=10_000)
    rules, _ = rules_with(result=False, reservation=resv)
    action = rules.decide(make_swap(status='PendingAttestation'), now=1500)
    assert action.decision == Decision.EXTEND_RESERVATION
    assert action.target_at == 3000


def test_source_pending_with_time_left_waits():
    resv = make_reservation(reserved_until=5000, max_extend_at=10_000)  # 3500s of runway > padding
    rules, _ = rules_with(result=False, reservation=resv)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_source_pending_at_ceiling_no_extend():
    resv = make_reservation(reserved_until=1600, max_extend_at=1600)  # no room below ceiling
    rules, _ = rules_with(result=False, reservation=resv)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_source_pending_no_reservation_waits():
    rules, _ = rules_with(result=False, reservation=None)
    rules.reservation = None
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.WAIT


def test_dest_pending_near_timeout_extends_timeout():
    # SOL dest pending at 0/32 confs: raw = now + 32*1 + 120 = 1652 → 600s bucket up to 1800 (<ceiling).
    rules, providers = rules_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=1600, max_extend_at=10_000)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.EXTEND_TIMEOUT
    assert action.target_at == 1800


def test_dest_pending_far_from_timeout_waits():
    rules, providers = rules_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=5000, max_extend_at=10_000)
    assert rules.decide(swap, now=1500).decision == Decision.WAIT


def test_dest_pending_at_ceiling_overdue_times_out():
    # Extensions exhausted (timeout_at == max_extend_at) and overdue → slash, don't wait forever.
    rules, providers = rules_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1000)
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_dest_absent_overdue_times_out_not_extended():
    rules, providers = rules_with(result=True)
    providers['sol'].result = None  # dest absent, not merely unconfirmed
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=10_000)
    assert rules.decide(swap, now=1500).decision == Decision.TIMEOUT


def test_extension_target_clamped_to_ceiling():
    # A tight ceiling caps target_at at max_extend_at; still strictly past the deadline.
    resv = make_reservation(reserved_until=1600, max_extend_at=1800)
    rules, _ = rules_with(result=False, reservation=resv)
    action = rules.decide(make_swap(status='PendingAttestation'), now=1500)
    assert action.decision == Decision.EXTEND_RESERVATION
    assert action.target_at == 1800  # clamped down from the bucketed 3000
    assert action.target_at > 1600


# ── Deferred-confirmation reaper: a PendingAttestation claim whose reservation can no longer carry it to
# attestation (expired past its ceiling, or its slot re-resolved) is reaped via close_stale_claim, freeing
# the miner. The stale check precedes the leg fetch, since no source status can rescue a dead reservation.
def test_pending_attestation_expired_reservation_reaps():
    resv = make_reservation(reserved_until=1000)  # ran past its ceiling (< now 1500), source never confirmed
    rules, _ = rules_with(result=None, reservation=resv)  # source absent (dropped/RBF'd)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.CANCEL


def test_pending_attestation_expired_reservation_reaps_even_if_source_pending():
    resv = make_reservation(reserved_until=1000)
    rules, _ = rules_with(result=False, reservation=resv)  # still in mempool, but the reservation is already dead
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.CANCEL


def test_pending_attestation_superseded_claim_reaps():
    resv = make_reservation(reserved_until=1_000_000, claimed_swap_key=b'\x09' * 32)  # slot points elsewhere
    rules, _ = rules_with(result=True, reservation=resv)
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.CANCEL


def test_pending_attestation_live_matching_reservation_not_reaped():
    # Regression: a live reservation whose claim slot matches must attest on a confirmed source, never reap.
    rules, _ = rules_with(result=True)  # default reservation is live + matching
    assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.ATTEST


def test_an_unapplied_vault_debit_blocks_the_miners_next_initiate():
    # The contract's settlement grace covers the common case; this covers the tail where the relay
    # is slower than the grace, and the seconds-wide window right after the verdict.
    rules, swap = _tao_backed(owes=True)
    assert rules.decide(swap, now=1500).decision == Decision.WAIT


def test_a_settled_miner_attests_normally():
    rules, swap = _tao_backed(owes=False)
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


def test_a_locally_backed_swap_never_consults_the_relay():
    # SOL settles atomically inside timeout_swap, so there is no pending debit to wait on — and a
    # SOL-only deployment must not pay for split collateral existing.
    rules, swap = _tao_backed(owes=True, backing='sol')
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


def test_with_no_relay_running_a_tao_backed_swap_attests():
    rules, swap = _tao_backed(owes=None)
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


class TestMissingProviderNeverSlashes:
    """A spoke with no provider on this validator (unsupported/disabled net, e.g. #678) is
    UNVERIFIABLE, not payment-absent. Below the extension ceiling a swap on that spoke SKIPs (never slash
    on the absence of a verifier); PAST max_extend_at it TIMEOUTs so the collateral can't freeze forever.
    A missing provider reads as a 'down' leg (like an unreachable RPC); decide() applies the ceiling."""

    def test_fulfilled_missing_dest_provider_skips_below_ceiling(self):
        # Dest tx merely absent → TIMEOUT; dest PROVIDER gone (before the ceiling) → SKIP: absence of a
        # verifier, not of a payment. max_extend_at default (10_000) is well past now.
        rules, providers = rules_with(result=None)
        del providers['sol']
        assert rules.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == Decision.SKIP

    def test_pending_attestation_missing_source_provider_skips(self):
        rules, providers = rules_with(result=True)
        del providers['btc']
        assert rules.decide(make_swap(status='PendingAttestation'), now=1500).decision == Decision.SKIP

    def test_active_missing_dest_provider_skips_below_ceiling(self):
        # The Active branch never calls _fetch_leg; below the ceiling a missing dest provider must SKIP,
        # not fall through to the overdue TIMEOUT.
        rules, providers = rules_with(result=True)
        del providers['sol']
        swap = make_swap(status='Active', timeout_at=1000, max_extend_at=10_000)  # overdue, before ceiling
        assert rules.decide(swap, now=5000).decision == Decision.SKIP

    def test_active_missing_dest_provider_times_out_past_ceiling(self):
        # A permanently-missing provider must not SKIP forever (freezing the taker deposit + miner
        # collateral). Past max_extend_at it TIMEOUTs to free the collateral.
        rules, providers = rules_with(result=True)
        del providers['sol']
        swap = make_swap(status='Active', timeout_at=1000, max_extend_at=1200)
        assert rules.decide(swap, now=5000).decision == Decision.TIMEOUT

    def test_fulfilled_missing_dest_provider_times_out_past_ceiling(self):
        rules, providers = rules_with(result=None)
        del providers['sol']
        swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1200)
        assert rules.decide(swap, now=5000).decision == Decision.TIMEOUT


# ── Published leg confirmations (seam serves these; it must never recompute them) ────────────


def test_a_delivery_another_miner_only_reported_does_not_veto_a_deposit():
    # Any miner can mark_fulfilled with a user's real deposit hash as its to_tx_hash (free text).
    rules, _ = rules_with(result=True)
    swap = make_swap(status='PendingAttestation')
    ctx = _Context(rules, 1500)
    ctx.world = replace(ctx.world, fulfillments={'other': Fulfillment(swap.from_tx_hash, 1400, 'sybil', False)})
    assert decide(swap, ctx, 1500, fee_divisor=100).decision == Decision.ATTEST


def test_a_delivery_this_validator_verified_is_still_refused_as_a_deposit():
    rules, _ = rules_with(result=True)
    swap = make_swap(status='PendingAttestation')
    rules.memory.claim_delivery('another-miner', tx_key(swap.from_tx_hash), 'ab' * 32, 1400)
    assert rules.decide(swap, now=1500).decision == Decision.REJECT
