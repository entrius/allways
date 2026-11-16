from types import SimpleNamespace

import pytest

from allways import __spec_version__, constants
from allways.subtensor import endpoint_list, parse_endpoints, redact_endpoint
from neurons import validator

MY_HOTKEY = 'validator-hotkey'


class FakeSubtensor:
    def __init__(self, since=1000, rate_limit=100, registered=True, success=True, n=64):
        self.since, self.rate_limit, self.registered, self.success = since, rate_limit, registered, success
        self.hotkeys = [f'hotkey-{uid}' for uid in range(n)]
        self.calls = []

    def get_uid_for_hotkey_on_subnet(self, hotkey, netuid):
        return 9 if self.registered else None

    def blocks_since_last_update(self, netuid, uid):
        return self.since

    def weights_rate_limit(self, netuid):
        return self.rate_limit

    def metagraph(self, netuid):
        return SimpleNamespace(hotkeys=self.hotkeys)

    def set_weights(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(success=self.success, message='boom')


def config(epoch_length=150):
    return SimpleNamespace(netuid=7, neuron=SimpleNamespace(epoch_length=epoch_length))


WALLET = SimpleNamespace(hotkey=SimpleNamespace(ss58_address=MY_HOTKEY))


def test_step_burns_everything_while_treasury_uid_is_unset(monkeypatch):
    monkeypatch.setattr(validator, 'TREASURY_UID', None)
    st = FakeSubtensor()
    validator.step(st, WALLET, config(), submit=True)
    (call,) = st.calls
    assert call['uids'] == [constants.BURN_UID]
    assert call['weights'] == [1.0]
    assert call['netuid'] == 7
    assert call['version_key'] == __spec_version__
    assert call['wallet'] is WALLET


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
    assert st.calls[0]['uids'] == [constants.BURN_UID]


@pytest.mark.parametrize('since', [0, 100, 150])
def test_step_waits_out_the_epoch(since):
    st = FakeSubtensor(since=since)
    validator.step(st, WALLET, config(epoch_length=150), submit=True)
    assert st.calls == []


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


@pytest.mark.parametrize(
    'env, mode',
    [
        ({}, 'full'),
        ({'VALIDATOR_MODE': 'full'}, 'full'),
        ({'VALIDATOR_MODE': ' Watch '}, 'watch'),
        ({'VALIDATOR_MODE': 'vote'}, 'vote'),
        ({'VALIDATOR_DEV_MODE': '1'}, 'watch'),
        ({'VALIDATOR_MODE': 'full', 'VALIDATOR_DEV_MODE': '1'}, 'full'),
    ],
)
def test_validator_mode(monkeypatch, env, mode):
    monkeypatch.delenv('VALIDATOR_MODE', raising=False)
    monkeypatch.delenv('VALIDATOR_DEV_MODE', raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert validator.validator_mode() == mode


def test_unknown_validator_mode_refuses_to_run(monkeypatch):
    monkeypatch.setenv('VALIDATOR_MODE', 'ful')
    with pytest.raises(ValueError):
        validator.validator_mode()


def test_obsolete_flags_are_ignored(monkeypatch):
    argv = [
        'validator.py',
        '--netuid',
        '7',
        '--axon.port',
        '8091',
        '--neuron.wandb_off',
        '--validator.poll_interval',
        '5',
    ]
    monkeypatch.setattr('sys.argv', argv)
    cfg = validator.get_config()
    assert cfg.netuid == 7
    assert cfg.dry_run is False
    assert cfg.neuron.epoch_length == constants.EPOCH_LENGTH_BLOCKS


def test_keyed_endpoints_are_redacted():
    assert redact_endpoint('wss://api.example.com/secret-key') == 'wss://api.example.com/<redacted>'
    assert redact_endpoint('wss://entrypoint-finney.opentensor.ai:443') == 'wss://entrypoint-finney.opentensor.ai:443'
    assert redact_endpoint('finney') == 'finney'


def test_fallback_endpoints_follow_the_primary(monkeypatch):
    monkeypatch.setenv('SUBTENSOR_FALLBACK_ENDPOINTS', ' wss://a:443 , finney,,wss://b:443 ')
    assert endpoint_list('finney') == ['finney', 'wss://a:443', 'wss://b:443']
    assert parse_endpoints(None) == []
