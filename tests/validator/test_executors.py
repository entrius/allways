import base64
import time
from types import SimpleNamespace

import pytest
from solders.hash import Hash
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.transaction import Transaction

from allways.constants import CANCEL_REASON_ALPHA_STAKE_REFUSED, CANCEL_REASON_OTHER
from allways.solana import pdas
from allways.solana.client import swap_from_solana, swap_key_from_tx_hash
from allways.validator.actuator.executors import EXECUTORS, SWAP_EXECUTORS, SendingClient, StaleWorld
from allways.validator.actuator.planners import SwapIntent, intent

SWAP = SimpleNamespace(
    from_tx_hash='aa' * 32,
    miner=Keypair().pubkey(),
    user=Keypair().pubkey(),
    from_chain='btc',
    to_chain='sol',
    user_from_addr='u',
    miner_from_addr='m',
    collateral_chain='sol',
    timeout_at=100,
)


def _intent(kind, target_at=None):
    return intent(
        kind,
        'k',
        SimpleNamespace(timeout_at=100, reserved_until=100),
        0,
        payload=SwapIntent(SWAP, SimpleNamespace(target_at=target_at, reason='r', reason_code=None)),
    )


class _Client:
    def __init__(self, voted=False, error=None):
        self.voted, self.error, self.calls = voted, error, []
        self.keypair = Keypair()

    def has_voted(self, *a):
        return self.voted

    def __getattr__(self, name):
        def call(*a, **k):
            self.calls.append(name)
            if self.error:
                raise RuntimeError(self.error)
            return f'sig-{name}'

        return call


def test_a_vote_already_cast_is_not_sent_again():
    client = _Client(voted=True)
    assert SWAP_EXECUTORS['confirm_swap'](client, _intent('confirm_swap')) is None
    assert client.calls == []
    assert SWAP_EXECUTORS['confirm_swap'](_Client(), _intent('confirm_swap')) == 'sig-confirm_swap'


def test_a_lost_extension_race_is_a_no_op_but_a_real_failure_raises():
    assert (
        SWAP_EXECUTORS['extend_timeout'](_Client(error='custom: ExtensionNotLater'), _intent('extend_timeout', 5))
        is None
    )
    try:
        SWAP_EXECUTORS['extend_timeout'](_Client(error='InsufficientFunds'), _intent('extend_timeout', 5))
    except RuntimeError:
        return
    raise AssertionError('a real failure was swallowed')


class _RefusedAttest(_Client):
    def __init__(self, reserved_until, max_extend_at):
        super().__init__()
        self.reservation = SimpleNamespace(reserved_until=reserved_until, max_extend_at=max_extend_at)
        self.extended = []

    def get_reservation(self, miner, backing='sol'):
        return self.reservation

    def vote_initiate(self, *a, **k):
        self.calls.append('vote_initiate')
        raise RuntimeError('Blockhash not found')

    def extend_reservation(self, miner, target_at, backing='sol', **k):
        self.extended.append(target_at)
        return 'sig-extend'


@pytest.mark.parametrize('left, extends', [(60, True), (3000, False)])
def test_a_refused_attest_extends_the_reservation_only_near_expiry(left, extends):
    # #813: a refused vote_initiate near expiry slides the reservation, so the claim is retried, not reaped.
    now = int(time.time())
    client = _RefusedAttest(reserved_until=now + left, max_extend_at=now + 7200)
    with pytest.raises(RuntimeError, match='Blockhash'):
        SWAP_EXECUTORS['vote_initiate'](client, _intent('vote_initiate'))
    assert bool(client.extended) is extends
    if extends:
        assert now + left < client.extended[0] <= now + 7200


def test_a_refused_attest_at_the_ceiling_is_not_extended():
    now = int(time.time())
    client = _RefusedAttest(reserved_until=now + 60, max_extend_at=now + 60)
    with pytest.raises(RuntimeError):
        SWAP_EXECUTORS['vote_initiate'](client, _intent('vote_initiate'))
    assert client.extended == []


class _Rpc:
    def __init__(self, failures=0):
        self.failures, self.sent, self.confirmed = failures, 0, False

    def get_latest_blockhash(self):
        return '11111111111111111111111111111111'

    def send_transaction(self, raw, skip_preflight=False):
        self.sent += 1
        if self.failures:
            self.failures -= 1
            raise RuntimeError('Blockhash not found')
        return 'sig'

    def confirm(self, sig):
        self.confirmed = True


def test_a_send_returns_without_waiting_for_confirmation_and_retries_a_stale_blockhash(monkeypatch):
    monkeypatch.setattr('allways.validator.actuator.executors.time.sleep', lambda s: None)
    client = SendingClient('http://unused', keypair=Keypair())
    client.rpc = _Rpc(failures=2)
    assert client._send([]) == 'sig'
    assert client.rpc.sent == 3 and not client.rpc.confirmed


# ── votes that cost someone are cast only on the swap as it is on-chain now ──


class Active:
    pass


class Fulfilled:
    pass


def _account(status=Active, timeout_at=100):
    fields = dict(vars(SWAP), user_to_addr='d', miner_to_addr='md', rate=1, collateral_amount=1, from_amount=1)
    fields.update(to_amount=1, from_tx_block=1, to_tx_hash='', to_tx_block=0, initiated_at=1, max_extend_at=500)
    fields.update(fulfilled_at=0, status=status(), timeout_at=timeout_at)
    return SimpleNamespace(**fields)


class _Chain(_Client):
    """A client whose get_swap / get_reservation answer from on-chain state."""

    program_id = None

    def __init__(self, swap=None, reservation=None):
        super().__init__()
        self.swap, self.reservation = swap, reservation

    def get_swap(self, key):
        return self.swap

    def get_reservation(self, miner, backing):
        return self.reservation


def _planned(kind, status=Active, timeout_at=100):
    planned = swap_from_solana(_account(status, timeout_at))
    return intent(kind, 'k', planned, 0, payload=SwapIntent(planned, SimpleNamespace(reason_code=None)))


def test_a_timeout_is_cast_only_if_the_chain_still_shows_the_swap_as_planned():
    assert SWAP_EXECUTORS['timeout_swap'](_Chain(_account()), _planned('timeout_swap')) == 'sig-timeout_swap'
    for changed in (_account(status=Fulfilled), _account(timeout_at=400), None):
        chain = _Chain(changed)
        with pytest.raises(StaleWorld) as stale:
            SWAP_EXECUTORS['timeout_swap'](chain, _planned('timeout_swap'))
        assert chain.calls == [] and {k for k, _ in stale.value.keys} == {'Swap'}


def test_a_refusal_cancel_is_rechecked_the_same_way():
    with pytest.raises(StaleWorld):
        SWAP_EXECUTORS['cancel_swap'](_Chain(_account(status=Fulfilled)), _planned('cancel_swap'))


def test_a_refusal_sends_the_planned_reason_to_the_program():
    """The reason code is what the program records for a no-fault cancel (an alpha recipient's refused stake, a
    frozen account, …); only a refusal with none falls back to OTHER."""
    sent = []

    class _Recording(_Chain):
        def cancel_swap(self, swap_key, miner, reason):
            sent.append(reason)
            return 'sig'

    planned = swap_from_solana(_account())
    for code in (CANCEL_REASON_ALPHA_STAKE_REFUSED, None):
        refusal = intent('cancel_swap', 'k', planned, 0, payload=SwapIntent(planned, SimpleNamespace(reason_code=code)))
        assert SWAP_EXECUTORS['cancel_swap'](_Recording(_account()), refusal) == 'sig'
    assert sent == [CANCEL_REASON_ALPHA_STAKE_REFUSED, CANCEL_REASON_OTHER]


def test_a_stale_claim_is_reaped_only_if_its_reservation_has_lapsed_on_chain():
    key = swap_key_from_tx_hash(SWAP.from_tx_hash)
    live = SimpleNamespace(reserved_until=time.time() + 600, claimed_swap_key=key)
    with pytest.raises(StaleWorld):
        SWAP_EXECUTORS['close_stale_claim'](_Chain(reservation=live), _planned('close_stale_claim'))
    lapsed = SimpleNamespace(reserved_until=time.time() - 1, claimed_swap_key=key)
    assert SWAP_EXECUTORS['close_stale_claim'](_Chain(reservation=lapsed), _planned('close_stale_claim'))


class _LaggingRpc:
    """Answers the first send with "blockhash not found" though that tx landed; the resend then fails
    preflight against it."""

    def __init__(self):
        self.sent, self.landed = [], set()

    def get_latest_blockhash(self):
        return str(Hash.new_unique())

    def send_transaction(self, raw, skip_preflight=False):
        sig = str(Transaction.from_bytes(base64.b64decode(raw)).signatures[0])
        self.sent.append(sig)
        if len(self.sent) == 1:
            self.landed.add(sig)
            raise RuntimeError('sendTransaction: Blockhash not found')
        raise RuntimeError('sendTransaction: simulation failed: custom program error NotPending')

    def get_signature_statuses(self, sigs):
        return [{'err': None} if s in self.landed else None for s in sigs]


def test_a_send_the_rpc_called_stale_but_that_landed_is_reported_as_sent():
    client = SendingClient.__new__(SendingClient)
    client.keypair, client.rpc = Keypair(), _LaggingRpc()
    ix = Instruction(Keypair().pubkey(), b'', [])
    assert client._send([ix]) == client.rpc.sent[0]


# ── a floor kick is cast only while the hub, read live, is still under its floor ──


class _Purses(_Client):
    """A client whose miner state, config and bond attestation answer from on-chain state."""

    program_id = None

    def __init__(self, collateral=50, attested=5, locked=True, backings=pdas.BACKING_BIT_SOL | pdas.BACKING_BIT_TAO):
        super().__init__()
        self.ms = SimpleNamespace(active=True, active_backings=backings, collateral=collateral)
        self.att = SimpleNamespace(locked=locked, effective_balance=attested)

    def get_miner_state(self, miner):
        return self.ms

    def get_config(self):
        return SimpleNamespace(min_collateral=100, tao_min_collateral=1000)

    def get_bond_attestation(self, miner, chain):
        return self.att


def _kick(backing):
    miner = str(SWAP.miner)
    return intent('vote_deactivate', (miner, backing), None, 0, payload=(miner, backing))


def test_a_floor_kick_is_sent_while_the_live_purse_is_under_its_floor():
    assert EXECUTORS['vote_deactivate'](_Purses(), _kick('sol')) == 'sig-vote_deactivate'
    assert EXECUTORS['vote_deactivate'](_Purses(), _kick('tao')) == 'sig-vote_deactivate'


def test_a_floor_kick_is_not_sent_once_the_live_purse_meets_its_floor():
    topped_up = {
        'sol': [_Purses(collateral=100), _Purses(backings=pdas.BACKING_BIT_TAO)],
        'tao': [_Purses(attested=1000), _Purses(locked=False), _Purses(backings=pdas.BACKING_BIT_SOL)],
    }
    for backing, chains in topped_up.items():
        for chain in chains:
            with pytest.raises(StaleWorld) as stale:
                EXECUTORS['vote_deactivate'](chain, _kick(backing))
            assert chain.calls == [] and 'MinerState' in {k for k, _ in stale.value.keys}
