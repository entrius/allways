"""Unit tests for the Solana swap loop — decisions, Option-A 99% verification (B1), and the B2 replay
freshness gates (source vs Reservation.created_at, dest vs Swap.initiated_at).

Mocks the solana client (get_swaps / get_reservation) + chain providers; no chain, no votes.
"""

from types import SimpleNamespace

from allways.chain_providers.base import ProviderUnreachableError
from allways.validator.solana_swap_loop import SolanaSwapLoop, SwapDecision

INITIATED_AT = 1000  # dest-freshness floor
RESV_CREATED_AT = 1200  # source-freshness floor
FRESH = 5000  # block_time comfortably after both floors


def make_swap(status='Fulfilled', to_amount=1000, from_amount=500, timeout_at=2000, key=b'\x01' * 32):
    return SimpleNamespace(
        swap_key=key,
        miner='minerPK',
        status=status,
        from_chain='btc',
        to_chain='sol',
        from_tx_hash='srctx',
        to_tx_hash='dsttx',
        miner_from_addr='minerBTC',
        miner_to_addr='minerSOL',
        user_to_addr='userSOL',
        from_amount=from_amount,
        to_amount=to_amount,
        from_tx_block=0,
        to_tx_block=0,
        timeout_at=timeout_at,
        initiated_at=INITIATED_AT,
    )


class RecordingProvider:
    """verify_transaction → confirmed-match info / None (missing) / raises (unreachable). block_time and
    a per-chain replay_grace_secs feed the freshness checks."""

    def __init__(self, result=True, block_time=FRESH, grace=0):
        self.result = result
        self.block_time = block_time
        self.grace = grace
        self.calls = []

    def get_chain(self):
        return SimpleNamespace(replay_grace_secs=self.grace)

    def verify_transaction(self, tx_hash, expected_recipient, expected_amount, block_hint=0, expected_sender=None):
        self.calls.append(SimpleNamespace(tx_hash=tx_hash, recipient=expected_recipient, amount=expected_amount))
        if self.result == 'unreachable':
            raise ProviderUnreachableError('down')
        if self.result is None:
            return None  # tx not found
        return SimpleNamespace(confirmed=bool(self.result), block_time=self.block_time)


def loop_with(result=True, created_at=RESV_CREATED_AT):
    providers = {'btc': RecordingProvider(result), 'sol': RecordingProvider(result)}
    client = SimpleNamespace(
        get_swaps=lambda: [],
        get_reservation=lambda miner: SimpleNamespace(created_at=created_at),
    )
    return SolanaSwapLoop(client, providers, fee_divisor=100), providers


def test_expected_user_receives_is_99_percent():
    loop, _ = loop_with()
    assert loop.expected_user_receives(make_swap(to_amount=1000)) == 990
    assert loop.expected_user_receives(make_swap(to_amount=10_000)) == 9_900


def test_fulfilled_both_legs_ok_confirms_and_checks_99_percent_dest():
    loop, providers = loop_with(result=True)
    swap = make_swap(status='Fulfilled', to_amount=1000)
    assert loop.decide(swap, now=1500) == SwapDecision.CONFIRM
    # dest leg verified against 99% of to_amount, not the full amount
    dest_call = providers['sol'].calls[-1]
    assert dest_call.amount == 990 and dest_call.recipient == 'userSOL'
    # source leg verified against the full from_amount to the miner
    src_call = providers['btc'].calls[-1]
    assert src_call.amount == 500 and src_call.recipient == 'minerBTC'


def test_fulfilled_dest_unconfirmed_waits():
    loop, providers = loop_with(result=True)
    providers['sol'].result = False  # dest not confirmed
    assert loop.decide(make_swap(status='Fulfilled'), now=1500) == SwapDecision.WAIT


def test_fulfilled_stale_dest_tx_rejected_as_replay():
    loop, providers = loop_with(result=True)
    providers['sol'].block_time = INITIATED_AT - 1  # payout mined before the swap was initiated
    assert loop.decide(make_swap(status='Fulfilled'), now=1500) == SwapDecision.WAIT


def test_fulfilled_dest_missing_block_time_rejected():
    loop, providers = loop_with(result=True)
    providers['sol'].block_time = None  # cannot prove freshness → fail closed
    assert loop.decide(make_swap(status='Fulfilled'), now=1500) == SwapDecision.WAIT


def test_pending_attestation_source_ok_attests():
    loop, _ = loop_with(result=True)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500) == SwapDecision.ATTEST


def test_pending_attestation_source_missing_waits():
    loop, _ = loop_with(result=None)
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500) == SwapDecision.WAIT


def test_pending_attestation_stale_deposit_rejected_as_replay():
    loop, providers = loop_with(result=True)
    providers['btc'].block_time = RESV_CREATED_AT - 1  # deposit predates the reservation
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500) == SwapDecision.WAIT


def test_pending_attestation_no_reservation_waits():
    loop, _ = loop_with(result=True)
    loop.client.get_reservation = lambda miner: None
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500) == SwapDecision.WAIT


def test_pending_attestation_grace_allows_slightly_old_deposit():
    loop, providers = loop_with(result=True)
    providers['btc'].grace = 300
    providers['btc'].block_time = RESV_CREATED_AT - 100  # within grace of the floor
    assert loop.decide(make_swap(status='PendingAttestation'), now=1500) == SwapDecision.ATTEST


def test_active_timed_out_vs_waiting():
    loop, _ = loop_with()
    assert loop.decide(make_swap(status='Active', timeout_at=1000), now=1500) == SwapDecision.TIMEOUT
    assert loop.decide(make_swap(status='Active', timeout_at=2000), now=1500) == SwapDecision.WAIT


def test_provider_unreachable_skips():
    loop, _ = loop_with(result='unreachable')
    assert loop.decide(make_swap(status='Fulfilled'), now=1500) == SwapDecision.SKIP


def test_run_once_discovers_and_decides_mix():
    swaps = [
        ('pk1', make_swap(status='Active', timeout_at=1000, key=b'\x01' * 32)),
        ('pk2', make_swap(status='Fulfilled', key=b'\x02' * 32)),
    ]
    providers = {'btc': RecordingProvider(True), 'sol': RecordingProvider(True)}
    client = SimpleNamespace(
        get_swaps=lambda: swaps,
        get_reservation=lambda miner: SimpleNamespace(created_at=RESV_CREATED_AT),
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
        return SimpleNamespace(created_at=RESV_CREATED_AT)

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
