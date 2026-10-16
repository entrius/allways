from types import SimpleNamespace

from solders.keypair import Keypair

from allways.validator_v2.actuator.executors import SWAP_EXECUTORS, SendingClient, dry_run
from allways.validator_v2.actuator.planners import SwapIntent, intent

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


def test_shadow_mode_sends_nothing():
    client = _Client()
    for kind, execute in dry_run(SWAP_EXECUTORS).items():
        assert execute(client, _intent(kind, 5)) is None
    assert client.calls == []


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
    monkeypatch.setattr('allways.validator_v2.actuator.executors.time.sleep', lambda s: None)
    client = SendingClient('http://unused', keypair=Keypair())
    client.rpc = _Rpc(failures=2)
    assert client._send([]) == 'sig'
    assert client.rpc.sent == 3 and not client.rpc.confirmed
