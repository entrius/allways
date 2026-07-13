"""Unit tests for the Solana swap loop — decisions, Option-A 99% verification (B1), the B2 replay
freshness gates (source vs Reservation.created_at, dest vs Swap.initiated_at), and the D3 deadline
extensions for valid-but-unconfirmed legs.

Mocks the solana client (get_swaps / get_reservation) + chain providers; no chain, no votes.
"""

from types import SimpleNamespace

from allways.chain_providers.base import ProviderUnreachableError
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.solana_swap_loop import SolanaSwapLoop, SwapAction, SwapDecision, _is_benign_resolve

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
):
    return SimpleNamespace(
        swap_key=key,
        miner='minerPK',
        status=status,
        from_chain=from_chain,
        to_chain=to_chain,
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

    def __init__(self, result=True, block_time=FRESH, grace=0, confirmations=0):
        self.result = result
        self.block_time = block_time
        self.grace = grace
        self.confirmations = confirmations
        self.calls = []

    def get_chain(self):
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


def loop_with(result=True, created_at=RESV_CREATED_AT, reservation=None):
    providers = {'btc': RecordingProvider(result), 'sol': RecordingProvider(result)}
    resv = reservation if reservation is not None else make_reservation(created_at=created_at)
    client = SimpleNamespace(
        get_swaps=lambda: [],
        get_reservation=lambda miner: resv,
    )
    return SolanaSwapLoop(client, providers, fee_divisor=100), providers


def test_expected_user_receives_is_99_percent():
    loop, _ = loop_with()
    assert loop.expected_user_receives(make_swap(to_amount=1000)) == 990
    assert loop.expected_user_receives(make_swap(to_amount=10_000)) == 9_900


def test_fulfilled_confirms_and_fetches_only_the_dest_leg():
    loop, providers = loop_with(result=True)
    swap = make_swap(status='Fulfilled', to_amount=1000)
    assert loop.decide(swap, now=1500).decision == SwapDecision.CONFIRM
    # dest leg verified against 99% of to_amount, not the full amount
    dest_call = providers['sol'].calls[-1]
    assert dest_call.amount == 990 and dest_call.recipient == 'userSOL'
    # P0: the SOURCE leg is NOT re-fetched for a Fulfilled swap — it was already verified + frozen at
    # attestation. Fulfilled must make exactly ONE leg fetch (dest only).
    assert providers['btc'].calls == [], 'Fulfilled re-fetched the source leg — should be dest-only'


def test_fulfilled_source_provider_down_still_confirms():
    # P0 behavior note: a source-provider OUTAGE no longer blocks confirming an already-attested payout,
    # because the source isn't re-fetched. (Pre-P0 this SKIPped on the source `down` guard.)
    loop, providers = loop_with(result=True)
    providers['btc'].result = 'unreachable'  # source provider down — must be irrelevant now
    assert loop.decide(make_swap(status='Fulfilled'), now=1500).decision == SwapDecision.CONFIRM
    assert providers['btc'].calls == []  # source never touched


def test_fulfilled_dest_pending_far_from_timeout_waits():
    loop, providers = loop_with(result=True)
    providers['sol'].result = False  # dest matched but unconfirmed, timeout far off
    assert loop.decide(make_swap(status='Fulfilled'), now=1500).decision == SwapDecision.WAIT


def test_fulfilled_stale_dest_tx_rejected_as_replay():
    loop, providers = loop_with(result=True)
    providers['sol'].block_time = INITIATED_AT - 1  # payout mined before the swap was initiated
    assert loop.decide(make_swap(status='Fulfilled'), now=1500).decision == SwapDecision.WAIT


def test_fulfilled_dest_missing_block_time_rejected():
    loop, providers = loop_with(result=True)
    providers['sol'].block_time = None  # cannot prove freshness → fail closed
    assert loop.decide(make_swap(status='Fulfilled'), now=1500).decision == SwapDecision.WAIT


def test_fulfilled_overdue_absent_dest_times_out():
    # Absent/mismatched dest past the deadline must not escape the slash → TIMEOUT (Active|Fulfilled).
    loop, providers = loop_with(result=True)
    providers['sol'].result = None  # dest tx absent
    assert loop.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == SwapDecision.TIMEOUT


def test_fulfilled_overdue_but_verifiable_still_confirms():
    # Overdue is irrelevant when both legs verify — a good fulfillment still confirms.
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='Fulfilled', timeout_at=1000), now=1500).decision == SwapDecision.CONFIRM


def test_fulfilled_pending_dest_at_ceiling_overdue_times_out():
    # Accountability guardrail: a payout that's broadcast but still unconfirmed, whose extension budget
    # is exhausted (timeout_at == max_extend_at, no room left to slide) and is now overdue, MUST slash.
    # The ceiling is ample runway (MAX_TOTAL_EXTENSION_SECS); failing to confirm within it means the
    # miner underfee'd/mis-sent the payout — its responsibility, so the timeout is warranted, not a
    # false slash. This tripwire guards that the exhausted-extension path still slashes.
    loop, providers = loop_with(result=True)
    providers['sol'].result = False  # dest matched but unconfirmed (pending)
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1000)  # at ceiling — no extend room
    assert loop.decide(swap, now=1500).decision == SwapDecision.TIMEOUT


def test_pending_attestation_source_ok_attests():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_source_missing_waits():
    loop, _ = loop_with(result=None)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_pending_attestation_stale_deposit_rejected_as_replay():
    loop, providers = loop_with(result=True)
    providers['btc'].block_time = RESV_CREATED_AT - 1  # deposit predates the reservation
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_pending_attestation_no_reservation_waits():
    loop, _ = loop_with(result=True)
    loop.client.get_reservation = lambda miner: None
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_pending_attestation_same_second_deposit_attests():
    # A deposit sent immediately after reserving lands in the same unix second as the floor; block_time
    # granularity is seconds, so `block_time == floor` is honest, not a replay.
    loop, providers = loop_with(result=True)
    providers['btc'].block_time = RESV_CREATED_AT
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_grace_allows_slightly_old_deposit():
    loop, providers = loop_with(result=True)
    providers['btc'].grace = 300
    providers['btc'].block_time = RESV_CREATED_AT - 100  # within grace of the floor
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_honest_sol_to_btc_attests():
    # Forward direction (is_reverse=False): rate 5 → 1 SOL (1e9 lamports) maps to 5e8 sats.
    loop, _ = loop_with(result=True)
    swap = make_swap(
        status='PendingAttestation', from_chain='sol', to_chain='btc', from_amount=1_000_000_000, to_amount=500_000_000
    )
    assert loop.decide(swap, now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_honest_btc_to_sol_reverse_attests():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation', to_amount=1000), now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_absurd_to_amount_rejected():
    loop, _ = loop_with(result=True)
    swap = make_swap(status='PendingAttestation', to_amount=1000 * 10_000)  # expected 1000
    assert loop.decide(swap, now=1500).decision == SwapDecision.REJECT


def test_pending_attestation_to_amount_off_by_two_rejected():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation', to_amount=1002), now=1500).decision == SwapDecision.REJECT


def test_pending_attestation_to_amount_off_by_one_attests():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation', to_amount=1001), now=1500).decision == SwapDecision.ATTEST


def test_pending_attestation_garbage_rate_zero_expected_rejected():
    loop, _ = loop_with(result=True)
    swap = make_swap(status='PendingAttestation', rate='0', to_amount=1000)  # expected_to == 0
    assert loop.decide(swap, now=1500).decision == SwapDecision.REJECT


def test_pending_attestation_zero_to_amount_rejected():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation', to_amount=0), now=1500).decision == SwapDecision.REJECT


def test_reject_casts_no_vote():
    swaps = [('pk1', make_swap(status='PendingAttestation', to_amount=1000 * 10_000, key=b'\x07' * 32))]
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    client = VoteRecordingClient(swaps)
    loop = SolanaSwapLoop(client, providers, fee_divisor=100)
    loop.run_once(now=1500)
    assert client.calls == []  # rate-inconsistent claim → no on-chain vote
    assert loop._cast_vote(swaps[0][1], SwapAction(SwapDecision.REJECT)) is False
    assert client.calls == []


def test_active_timed_out_vs_waiting():
    loop, _ = loop_with()
    assert loop.decide(make_swap(status='Active', timeout_at=1000), now=1500).decision == SwapDecision.TIMEOUT
    assert loop.decide(make_swap(status='Active', timeout_at=2000), now=1500).decision == SwapDecision.WAIT


def test_active_overdue_never_extends():
    # Active has no mark_fulfilled = no broadcast evidence → overdue must slash, never extend.
    loop, providers = loop_with(result=False)  # even a pending-looking source can't save an Active
    swap = make_swap(status='Active', timeout_at=1000, max_extend_at=10_000)
    assert loop.decide(swap, now=1500).decision == SwapDecision.TIMEOUT


def test_provider_unreachable_skips():
    loop, _ = loop_with(result='unreachable')
    assert loop.decide(make_swap(status='Fulfilled'), now=1500).decision == SwapDecision.SKIP


# ─── D3: deadline extensions for valid-but-unconfirmed legs ───


def test_fetch_leg_tristate():
    # confirmed→ok, matched-unconfirmed→pending, absent/mismatch→no, unreachable→down.
    loop, _ = loop_with()
    loop.providers = {'btc': RecordingProvider(True)}
    assert loop._fetch_leg('btc', 'tx', 'r', 1)[0] == 'ok'
    loop.providers = {'btc': RecordingProvider(False)}
    status, info = loop._fetch_leg('btc', 'tx', 'r', 1)
    assert status == 'pending' and info is not None
    loop.providers = {'btc': RecordingProvider(None)}
    assert loop._fetch_leg('btc', 'tx', 'r', 1) == ('no', None)
    loop.providers = {'btc': RecordingProvider('unreachable')}
    assert loop._fetch_leg('btc', 'tx', 'r', 1) == ('down', None)


def test_source_pending_near_expiry_extends_reservation():
    # BTC source pending at 0/2 confs: raw = now + 2*600 + 120 = 2820 → 600s bucket lands on 3000.
    resv = make_reservation(reserved_until=1600, max_extend_at=10_000)
    loop, _ = loop_with(result=False, reservation=resv)
    action = loop.decide(make_swap(status='PendingAttestation'), now=1500)
    assert action.decision == SwapDecision.EXTEND_RESERVATION
    assert action.target_at == 3000


def test_source_pending_with_time_left_waits():
    resv = make_reservation(reserved_until=5000, max_extend_at=10_000)  # 3500s of runway > padding
    loop, _ = loop_with(result=False, reservation=resv)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_source_pending_at_ceiling_no_extend():
    resv = make_reservation(reserved_until=1600, max_extend_at=1600)  # no room below ceiling
    loop, _ = loop_with(result=False, reservation=resv)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_source_pending_no_reservation_waits():
    loop, _ = loop_with(result=False, reservation=None)
    loop.client.get_reservation = lambda miner: None
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.WAIT


def test_dest_pending_near_timeout_extends_timeout():
    # SOL dest pending at 0/32 confs: raw = now + 32*1 + 120 = 1652 → 600s bucket up to 1800 (<ceiling).
    loop, providers = loop_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=1600, max_extend_at=10_000)
    action = loop.decide(swap, now=1500)
    assert action.decision == SwapDecision.EXTEND_TIMEOUT
    assert action.target_at == 1800


def test_dest_pending_far_from_timeout_waits():
    loop, providers = loop_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=5000, max_extend_at=10_000)
    assert loop.decide(swap, now=1500).decision == SwapDecision.WAIT


def test_dest_pending_at_ceiling_overdue_times_out():
    # Extensions exhausted (timeout_at == max_extend_at) and overdue → slash, don't wait forever.
    loop, providers = loop_with(result=True)
    providers['sol'].result = False
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=1000)
    assert loop.decide(swap, now=1500).decision == SwapDecision.TIMEOUT


def test_dest_absent_overdue_times_out_not_extended():
    loop, providers = loop_with(result=True)
    providers['sol'].result = None  # dest absent, not merely unconfirmed
    swap = make_swap(status='Fulfilled', timeout_at=1000, max_extend_at=10_000)
    assert loop.decide(swap, now=1500).decision == SwapDecision.TIMEOUT


def test_extension_target_clamped_to_ceiling():
    # A tight ceiling caps target_at at max_extend_at; still strictly past the deadline.
    resv = make_reservation(reserved_until=1600, max_extend_at=1800)
    loop, _ = loop_with(result=False, reservation=resv)
    action = loop.decide(make_swap(status='PendingAttestation'), now=1500)
    assert action.decision == SwapDecision.EXTEND_RESERVATION
    assert action.target_at == 1800  # clamped down from the bucketed 3000
    assert action.target_at > 1600


# ── Deferred-confirmation reaper: a PendingAttestation claim whose reservation can no longer carry it to
# attestation (expired past its ceiling, or its slot re-resolved) is reaped via close_stale_claim, freeing
# the miner. The stale check precedes the leg fetch, since no source status can rescue a dead reservation.
def test_pending_attestation_expired_reservation_reaps():
    resv = make_reservation(reserved_until=1000)  # ran past its ceiling (< now 1500), source never confirmed
    loop, _ = loop_with(result=None, reservation=resv)  # source absent (dropped/RBF'd)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.CANCEL


def test_pending_attestation_expired_reservation_reaps_even_if_source_pending():
    resv = make_reservation(reserved_until=1000)
    loop, _ = loop_with(result=False, reservation=resv)  # still in mempool, but the reservation is already dead
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.CANCEL


def test_pending_attestation_superseded_claim_reaps():
    resv = make_reservation(reserved_until=1_000_000, claimed_swap_key=b'\x09' * 32)  # slot points elsewhere
    loop, _ = loop_with(result=True, reservation=resv)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.CANCEL


def test_pending_attestation_live_matching_reservation_not_reaped():
    # Regression: a live reservation whose claim slot matches must attest on a confirmed source, never reap.
    loop, _ = loop_with(result=True)  # default reservation is live + matching
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500).decision == SwapDecision.ATTEST


class ExtendRecordingClient:
    """Captures extend/close calls; raisers simulate the contract ceiling / lost-race rejections."""

    def __init__(self, reservation_exc=None, timeout_exc=None, close_exc=None):
        self.reservation_exc = reservation_exc
        self.timeout_exc = timeout_exc
        self.close_exc = close_exc
        self.calls = []
        self.keypair = SimpleNamespace(pubkey=lambda: 'VALIDATOR')

    def extend_reservation(self, miner, target_at):
        self.calls.append(('extend_reservation', miner, target_at))
        if self.reservation_exc:
            raise self.reservation_exc

    def extend_timeout(self, swap_key, miner, target_at):
        self.calls.append(('extend_timeout', swap_key, miner, target_at))
        if self.timeout_exc:
            raise self.timeout_exc

    def close_stale_claim(self, miner, swap_key):
        self.calls.append(('close_stale_claim', miner, swap_key))
        if self.close_exc:
            raise self.close_exc


def test_cast_extend_reservation_calls_client():
    client = ExtendRecordingClient()
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.EXTEND_RESERVATION, 3240)) is True
    assert client.calls == [('extend_reservation', 'minerPK', 3240)]


def test_cast_extend_tolerates_not_later_as_noop():
    client = ExtendRecordingClient(reservation_exc=RuntimeError('ExtensionNotLater: already slid'))
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.EXTEND_RESERVATION, 3240)) is False  # no raise


def test_cast_extend_tolerates_exceeds_ceiling_as_noop():
    client = ExtendRecordingClient(timeout_exc=RuntimeError('ExtensionExceedsCeiling'))
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.EXTEND_TIMEOUT, 2160)) is False  # no raise


def test_cast_cancel_calls_close_stale_claim():
    client = ExtendRecordingClient()
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.CANCEL)) is True
    assert client.calls[0][0] == 'close_stale_claim' and client.calls[0][1] == 'minerPK'


def test_cast_cancel_tolerates_claim_not_expired_as_noop():
    # Another validator's clock hasn't crossed expiry yet — contract rejects with ClaimNotExpired; benign.
    client = ExtendRecordingClient(close_exc=RuntimeError('ClaimNotExpired'))
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.CANCEL)) is False  # no raise


def test_cast_cancel_tolerates_already_reaped_as_noop():
    # A peer already reaped it — the Swap PDA is gone; benign lost race.
    client = ExtendRecordingClient(close_exc=RuntimeError('AccountNotInitialized: swap'))
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop._cast_vote(make_swap(), SwapAction(SwapDecision.CANCEL)) is False  # no raise


def test_run_once_discovers_and_decides_mix():
    swaps = [
        ('pk1', make_swap(status='Active', timeout_at=1000, key=b'\x01' * 32)),
        ('pk2', make_swap(status='Fulfilled', key=b'\x02' * 32)),
    ]
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    client = SimpleNamespace(
        get_swaps=lambda: swaps,
        get_reservation=lambda miner: make_reservation(),
    )
    loop = SolanaSwapLoop(client, providers, fee_divisor=100, read_only=True)
    out = dict(loop.run_once(now=1500))
    assert out[(b'\x01' * 32).hex()] == SwapDecision.TIMEOUT
    assert out[(b'\x02' * 32).hex()] == SwapDecision.CONFIRM


class VoteRecordingClient:
    """Fake solana client capturing vote calls; has_voted toggles per (req_type) to test the skip guard."""

    def __init__(self, swaps, already_voted=False):
        self._swaps = swaps
        self.already_voted = already_voted
        self.calls = []
        self.keypair = SimpleNamespace(pubkey=lambda: 'VALIDATOR')

    def get_swaps(self):
        return self._swaps

    def get_reservation(self, miner):
        return make_reservation()

    def has_voted(self, req_type, target, voter):
        return self.already_voted

    def vote_initiate(self, swap_key, miner):
        self.calls.append(('vote_initiate', swap_key, miner))

    def confirm_swap(self, swap_key, miner, from_chain, to_chain):
        self.calls.append(('confirm_swap', swap_key, miner, from_chain, to_chain))

    def timeout_swap(self, swap_key, miner, user):
        self.calls.append(('timeout_swap', swap_key, miner, user))


def test_run_once_casts_votes_per_decision():
    swaps = [
        ('pk1', make_swap(status='PendingAttestation', key=b'\x01' * 32)),
        ('pk2', make_swap(status='Active', timeout_at=1000, key=b'\x02' * 32)),
        ('pk3', make_swap(status='Fulfilled', key=b'\x03' * 32)),
    ]
    swaps[1][1].user = 'USERPK'  # timeout vote needs the user pubkey
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    client = VoteRecordingClient(swaps)
    loop = SolanaSwapLoop(client, providers, fee_divisor=100)
    loop.run_once(now=1500)
    kinds = [c[0] for c in client.calls]
    assert kinds == ['vote_initiate', 'timeout_swap', 'confirm_swap']


def test_run_once_skips_already_voted():
    swaps = [('pk1', make_swap(status='Fulfilled', key=b'\x05' * 32))]
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    client = VoteRecordingClient(swaps, already_voted=True)
    loop = SolanaSwapLoop(client, providers, fee_divisor=100)
    loop.run_once(now=1500)
    assert client.calls == []  # has_voted → no re-submission


def make_pool(opened_at=100, closes_at=200, requests=1, miner='minerPK'):
    return SimpleNamespace(
        miner=miner,
        opened_at=opened_at,
        closes_at=closes_at,
        requests=[SimpleNamespace(router='r')] * requests,
    )


class PoolRecordingClient:
    """get_all('Pool') → fixtures; resolve_pool captures the miners cranked."""

    def __init__(self, pools):
        self._pools = pools
        self.resolved = []

    def get_all(self, name):
        assert name == 'Pool'
        return list(enumerate(self._pools))

    def resolve_pool(self, miner):
        self.resolved.append(miner)
        return 'SIG'


def test_resolve_pools_only_closed_nonempty():
    pools = [
        make_pool(opened_at=100, closes_at=200, requests=2, miner='closed'),  # eligible
        make_pool(opened_at=0, closes_at=200, requests=2, miner='emptyslot'),  # opened_at==0 → skip
        make_pool(opened_at=100, closes_at=900, requests=2, miner='stillopen'),  # window open → skip
        make_pool(opened_at=100, closes_at=200, requests=0, miner='norequests'),  # no draw → skip
    ]
    client = PoolRecordingClient(pools)
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    resolved = loop.resolve_pools_once(now=500)
    assert client.resolved == ['closed']
    assert resolved == ['closed']


def test_resolve_pools_read_only_casts_nothing():
    client = PoolRecordingClient([make_pool(miner='closed')])
    loop = SolanaSwapLoop(client, {}, fee_divisor=100, read_only=True)
    assert loop.resolve_pools_once(now=500) == []
    assert client.resolved == []


def test_resolve_pools_one_failure_does_not_break_sweep():
    class Boom(PoolRecordingClient):
        def resolve_pool(self, miner):
            if miner == 'bad':
                raise RuntimeError('rpc down')
            return super().resolve_pool(miner)

    client = Boom([make_pool(miner='bad'), make_pool(miner='good')])
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop.resolve_pools_once(now=500) == ['good']


def test_is_benign_resolve_classifies_lost_race():
    assert _is_benign_resolve(RuntimeError('custom program error: NoRequests'))
    assert _is_benign_resolve(RuntimeError('PoolNotClosed'))
    assert not _is_benign_resolve(RuntimeError('rpc down'))


def test_resolve_pools_lost_race_is_not_counted_and_sweep_continues():
    # A peer resolved the pool between our read and our tx → benign NoRequests, swallowed quietly.
    class Raced(PoolRecordingClient):
        def resolve_pool(self, miner):
            if miner == 'raced':
                raise RuntimeError('custom program error: NoRequests')
            return super().resolve_pool(miner)

    client = Raced([make_pool(miner='raced'), make_pool(miner='mine')])
    loop = SolanaSwapLoop(client, {}, fee_divisor=100)
    assert loop.resolve_pools_once(now=500) == ['mine']  # loser not counted, sweep unbroken
