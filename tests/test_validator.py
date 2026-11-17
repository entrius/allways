from types import SimpleNamespace

import pytest

from allways import __spec_version__, constants
from allways.subtensor import Connections, fallback_endpoints, parse_endpoints, redact_endpoint, redact_text
from neurons import validator

MY_HOTKEY = 'validator-hotkey'
MY_UID = 9
OWNER = 'owner-hotkey'
OWNER_UID = 53


class FakeSubtensor:
    def __init__(
        self, since=1000, rate_limit=100, registered=True, success=True, n=64, permit=True, owner=OWNER, genesis='0xaa'
    ):
        self.since, self.rate_limit, self.registered, self.success = since, rate_limit, registered, success
        self.hotkeys = [f'hotkey-{uid}' for uid in range(n)]
        self.hotkeys[OWNER_UID] = OWNER
        self.permits = [False] * n
        self.permits[MY_UID] = permit
        self.owner = owner
        self.calls, self.metagraph_syncs, self.closed = [], 0, False
        self.network, self.chain_endpoint = 'fake', 'ws://fake:9944'
        self.substrate = SimpleNamespace(get_block_hash=lambda block: genesis)

    def get_uid_for_hotkey_on_subnet(self, hotkey, netuid):
        return MY_UID if self.registered else None

    def blocks_since_last_update(self, netuid, uid):
        return self.since

    def weights_rate_limit(self, netuid):
        return self.rate_limit

    def metagraph(self, netuid):
        self.metagraph_syncs += 1
        return SimpleNamespace(hotkeys=self.hotkeys, validator_permit=self.permits)

    def get_subnet_owner_hotkey(self, netuid):
        return self.owner

    def get_subnet_hyperparameters(self, netuid):
        return SimpleNamespace(weights_version=319)

    def set_weights(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(success=self.success, message='boom')

    def close(self):
        self.closed = True


def config(epoch_length=150, netuid=7):
    return SimpleNamespace(netuid=netuid, neuron=SimpleNamespace(epoch_length=epoch_length))


WALLET = SimpleNamespace(hotkey=SimpleNamespace(ss58_address=MY_HOTKEY))


@pytest.fixture
def logs(monkeypatch):
    """Messages the validator logs, by level."""
    seen = {'info': [], 'warning': [], 'error': []}
    for level, bucket in seen.items():
        monkeypatch.setattr(validator.bt.logging, level, bucket.append)
    return seen


def parse(monkeypatch, *argv):
    monkeypatch.setattr('sys.argv', ['validator.py', *argv])
    return validator.get_config()


# ─── step ───────────────────────────────────────────────────


def test_step_burns_to_the_owner_uid_while_treasury_uid_is_unset(monkeypatch):
    monkeypatch.setattr(validator, 'TREASURY_UID', None)
    st = FakeSubtensor()
    validator.step(st, WALLET, config(), submit=True)
    (call,) = st.calls
    assert call['uids'] == [OWNER_UID]
    assert call['weights'] == [1.0]
    assert call['netuid'] == 7
    assert call['version_key'] == __spec_version__
    assert call['wallet'] is WALLET
    assert call['max_attempts'] == 1
    assert call['mev_protection'] is False
    assert call['wait_for_inclusion'] is True
    assert call['wait_for_finalization'] is False
    assert st.metagraph_syncs == 1


def test_step_sets_full_weight_on_the_treasury_uid(monkeypatch):
    monkeypatch.setattr(validator, 'TREASURY_UID', 12)
    st = FakeSubtensor()
    st.hotkeys[12] = constants.TREASURY_HOTKEY
    validator.step(st, WALLET, config(), submit=True)
    assert st.calls[0]['uids'] == [12]
    assert st.calls[0]['weights'] == [1.0]


def test_step_burns_when_the_treasury_slot_was_recycled(monkeypatch):
    monkeypatch.setattr(validator, 'TREASURY_UID', 12)
    st = FakeSubtensor()
    validator.step(st, WALLET, config(), submit=True)
    assert st.calls[0]['uids'] == [OWNER_UID]


def test_step_burns_on_another_netuid_even_with_the_treasury_in_place(monkeypatch):
    monkeypatch.setattr(validator, 'TREASURY_UID', 12)
    st = FakeSubtensor()
    st.hotkeys[12] = constants.TREASURY_HOTKEY
    validator.step(st, WALLET, config(netuid=19), submit=True)
    assert st.calls[0]['uids'] == [OWNER_UID]
    assert st.calls[0]['netuid'] == 19


@pytest.mark.parametrize('owner', ['someone-not-registered', None])
def test_step_sets_no_weights_when_the_owner_hotkey_is_missing(monkeypatch, logs, owner):
    monkeypatch.setattr(validator, 'TREASURY_UID', None)
    st = FakeSubtensor(owner=owner)
    validator.step(st, WALLET, config(), submit=True)
    assert st.calls == []
    assert any('NO WEIGHTS SET' in line for line in logs['error'])


def test_step_does_not_submit_without_a_validator_permit(logs):
    st = FakeSubtensor(permit=False)
    validator.step(st, WALLET, config(), submit=True)
    assert st.calls == []
    assert sum('no validator permit' in line for line in logs['error']) == 1


@pytest.mark.parametrize('since', [0, 100, 150])
def test_step_waits_out_the_epoch(since):
    st = FakeSubtensor(since=since)
    validator.step(st, WALLET, config(epoch_length=150), submit=True)
    assert st.calls == []
    assert st.metagraph_syncs == 0


def test_step_never_undercuts_the_chain_rate_limit():
    st = FakeSubtensor(since=200, rate_limit=300)
    validator.step(st, WALLET, config(epoch_length=150), submit=True)
    assert st.calls == []
    st.since = 301
    validator.step(st, WALLET, config(epoch_length=150), submit=True)
    assert len(st.calls) == 1


def test_step_does_not_submit_when_disabled():
    st = FakeSubtensor()
    validator.step(st, WALLET, config(), submit=False)
    assert st.calls == []


def test_step_skips_an_unregistered_hotkey():
    st = FakeSubtensor(registered=False)
    validator.step(st, WALLET, config(), submit=True)
    assert st.calls == []


def test_step_survives_a_rejected_extrinsic():
    st = FakeSubtensor(success=False)
    validator.step(st, WALLET, config(), submit=True)
    assert len(st.calls) == 1


def test_version_key_clears_the_chain_weights_version():
    # SN7 weights_version hyperparameter, read from finney at block 9522371.
    assert __spec_version__ >= 319


# ─── dry run ────────────────────────────────────────────────


def test_dry_run_reports_the_burn_target_and_submits_nothing(monkeypatch, logs):
    monkeypatch.setattr(validator, 'TREASURY_UID', None)
    st = FakeSubtensor()
    code = validator.dry_run(config(), Connections('ws://primary:9944', fallbacks=[], connector=lambda network: st))
    assert code == 0
    assert st.calls == []
    assert st.closed
    assert any('TREASURY_UID is unset' in line and f'UID {OWNER_UID}' in line for line in logs['warning'])
    assert any(f'would set weights uids=[{OWNER_UID}] weights=[1.0]' in line for line in logs['info'])


def test_dry_run_reports_the_treasury_target(monkeypatch, logs):
    monkeypatch.setattr(validator, 'TREASURY_UID', 12)
    st = FakeSubtensor()
    st.hotkeys[12] = constants.TREASURY_HOTKEY
    code = validator.dry_run(config(), Connections('ws://primary:9944', fallbacks=[], connector=lambda network: st))
    assert code == 0
    assert st.calls == []
    assert any('would set weights uids=[12] weights=[1.0]' in line for line in logs['info'])


def test_dry_run_fails_when_there_is_no_safe_target(monkeypatch, logs):
    monkeypatch.setattr(validator, 'TREASURY_UID', None)
    st = FakeSubtensor(owner='someone-not-registered')
    code = validator.dry_run(config(), Connections('ws://primary:9944', fallbacks=[], connector=lambda network: st))
    assert code == 1
    assert st.calls == []
    assert any('NO WEIGHTS SET' in line for line in logs['error'])


# ─── config and env contract ────────────────────────────────


@pytest.mark.parametrize(
    'argv, endpoint',
    [
        (['--subtensor.network', 'ws://x:9944'], 'ws://x:9944'),
        (['--subtensor.network', 'test'], 'wss://test.finney.opentensor.ai:443'),
        (['--subtensor.network', 'finney'], 'wss://entrypoint-finney.opentensor.ai:443'),
        (['--subtensor.network', 'wss://api.example.com/secret-key'], 'wss://api.example.com/secret-key'),
        (['--subtensor.chain_endpoint', 'ws://y:9944'], 'ws://y:9944'),
        (
            ['--subtensor.network', 'test', '--subtensor.chain_endpoint', 'ws://y:9944'],
            'wss://test.finney.opentensor.ai:443',
        ),
        ([], 'wss://entrypoint-finney.opentensor.ai:443'),
    ],
)
def test_primary_endpoint_follows_the_flags(monkeypatch, argv, endpoint):
    assert validator.primary_endpoint(parse(monkeypatch, *argv)) == endpoint


def test_the_old_entrypoint_invocation_still_parses(monkeypatch):
    # Exactly what origin/allways-old:scripts/vali-entrypoint.sh passes, with a typical old .env alongside.
    for key, value in {
        'NETUID': '7',
        'PORT': '8091',
        'LOG_LEVEL': 'info',
        'SOLANA_RPC_URL': 'https://rpc.example.com',
        'STORE_DB_RESULTS': 'true',
        'WANDB_API_KEY': '',
        'AXON_PUBLISH': '127.0.0.1:9000',
    }.items():
        monkeypatch.setenv(key, value)
    cfg = parse(
        monkeypatch,
        *('--netuid', '7'),
        *('--wallet.name', 'vali'),
        *('--wallet.hotkey', 'default'),
        *('--subtensor.network', 'ws://127.0.0.1:9944'),
        *('--axon.port', '8091'),
        '--logging.info',
        '--logging.record_log',
        *('--logging.logging_dir', '/root/.allways'),
        # extras an operator may have appended through "$@"
        '--neuron.wandb_off',
        *('--validator.poll_interval', '5'),
    )
    assert cfg.netuid == 7
    assert cfg.wallet.name == 'vali'
    assert cfg.wallet.hotkey == 'default'
    assert validator.primary_endpoint(cfg) == 'ws://127.0.0.1:9944'
    assert cfg.logging.record_log is True
    assert cfg.logging.logging_dir == '/root/.allways'
    assert cfg.dry_run is False
    assert cfg.neuron.disable_set_weights is False
    assert cfg.neuron.epoch_length == constants.EPOCH_LENGTH_BLOCKS


@pytest.mark.parametrize('raw, value', [(None, 10), ('', 10), ('  ', 10), ('abc', 10), ('25', 25), (' 7 ', 7)])
def test_env_int_falls_back_to_the_default(monkeypatch, raw, value):
    monkeypatch.delenv('LOG_FILE_MAX_MB', raising=False)
    if raw is not None:
        monkeypatch.setenv('LOG_FILE_MAX_MB', raw)
    assert validator.env_int('LOG_FILE_MAX_MB', 10) == value


# ─── connections ────────────────────────────────────────────


class Dialer:
    """Stands in for bt.Subtensor: each endpoint is a genesis hash, or an exception to raise on connect."""

    def __init__(self, **chains):
        self.chains = chains
        self.dialed = []

    def __call__(self, network):
        self.dialed.append(network)
        chain = self.chains[network]
        if isinstance(chain, Exception):
            raise chain
        return FakeSubtensor(genesis=chain)


def test_connections_use_the_primary_when_it_answers():
    dial = Dialer(primary='0xaa', backup='0xaa')
    conns = Connections('primary', fallbacks=['backup'], connector=dial)
    assert conns.open()[1] is True
    assert dial.dialed == ['primary']


def test_fallback_serves_one_pass_then_the_primary_is_tried_again():
    dial = Dialer(primary='0xaa', backup='0xaa')
    conns = Connections('primary', fallbacks=['backup'], connector=dial)
    conns.open()
    dial.chains['primary'] = ConnectionError('down')
    assert conns.open()[1] is False
    dial.chains['primary'] = '0xaa'
    assert conns.open()[1] is True
    assert dial.dialed == ['primary', 'primary', 'backup', 'primary']


def test_fallback_on_another_chain_is_never_used():
    dial = Dialer(primary='0xaa', mainnet='0xbb', backup='0xaa')
    conns = Connections('primary', fallbacks=['mainnet', 'backup'], connector=dial)
    conns.open()
    dial.chains['primary'] = ConnectionError('down')
    subtensor, on_primary = conns.open()
    assert on_primary is False
    assert subtensor.substrate.get_block_hash(0) == '0xaa'
    dial.chains['backup'] = ConnectionError('down')
    with pytest.raises(ConnectionError):
        conns.open()


def test_fallbacks_wait_until_the_primary_chain_is_known():
    dial = Dialer(primary=ConnectionError('down'), backup='0xaa')
    conns = Connections('primary', fallbacks=['backup'], connector=dial)
    with pytest.raises(ConnectionError):
        conns.open()
    assert dial.dialed == ['primary']


def test_a_failed_pass_on_the_primary_lets_a_fallback_serve_the_next_one():
    dial = Dialer(primary='0xaa', backup='0xaa')
    conns = Connections('primary', fallbacks=['backup'], connector=dial)
    conns.open()
    conns.primary_failed()
    assert conns.open()[1] is False
    assert conns.open()[1] is True


def test_a_failed_pass_with_no_fallback_goes_back_to_the_primary():
    dial = Dialer(primary='0xaa')
    conns = Connections('primary', fallbacks=[], connector=dial)
    conns.open()
    conns.primary_failed()
    assert conns.open()[1] is True


def test_a_primary_that_connects_but_cannot_be_read_is_closed():
    opened = []

    def dial(network):
        st = FakeSubtensor()
        st.substrate = SimpleNamespace(get_block_hash=lambda block: (_ for _ in ()).throw(ConnectionError('dropped')))
        opened.append(st)
        return st

    conns = Connections('primary', fallbacks=[], connector=dial)
    with pytest.raises(ConnectionError):
        conns.open()
    assert len(opened) == 1
    assert opened[0].closed
    assert conns.genesis is None


def test_keyed_endpoints_are_redacted():
    assert redact_endpoint('wss://api.example.com/secret-key') == 'wss://api.example.com/<redacted>'
    assert redact_endpoint('wss://entrypoint-finney.opentensor.ai:443') == 'wss://entrypoint-finney.opentensor.ai:443'
    assert redact_endpoint('finney') == 'finney'
    Connections('wss://api.example.com/secret-key', fallbacks=[], connector=None)
    assert redact_text('failed on wss://api.example.com/secret-key') == 'failed on wss://api.example.com/<redacted>'


def test_fallback_endpoints_exclude_the_primary(monkeypatch):
    monkeypatch.setenv('SUBTENSOR_FALLBACK_ENDPOINTS', ' wss://a:443 , finney,,wss://b:443 , wss://a:443')
    assert fallback_endpoints('finney') == ['wss://a:443', 'wss://b:443']
    assert parse_endpoints(None) == []
