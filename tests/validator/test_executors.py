import time
from types import SimpleNamespace

import pytest
from solders.keypair import Keypair

from allways.solana.client import swap_from_solana, swap_key_from_tx_hash
from allways.validator.actuator.executors import SWAP_EXECUTORS, SendingClient, StaleWorld
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


def test_a_stale_claim_is_reaped_only_if_its_reservation_has_lapsed_on_chain():
    key = swap_key_from_tx_hash(SWAP.from_tx_hash)
    live = SimpleNamespace(reserved_until=time.time() + 600, claimed_swap_key=key)
    with pytest.raises(StaleWorld):
        SWAP_EXECUTORS['close_stale_claim'](_Chain(reservation=live), _planned('close_stale_claim'))
    lapsed = SimpleNamespace(reserved_until=time.time() - 1, claimed_swap_key=key)
    assert SWAP_EXECUTORS['close_stale_claim'](_Chain(reservation=lapsed), _planned('close_stale_claim'))
