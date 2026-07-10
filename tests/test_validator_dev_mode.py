"""VALIDATOR_DEV_MODE — observe-only gate for staging a validator.

Dev mode makes the validator take no binding actions: the Solana swap loop logs "WOULD …"
instead of voting, and should_set_weights() returns False so no Bittensor weights are set.
Enable with VALIDATOR_DEV_MODE=1.
"""

from neurons.base.neuron import validator_dev_mode


def test_off_by_default(monkeypatch):
    monkeypatch.delenv('VALIDATOR_DEV_MODE', raising=False)
    assert validator_dev_mode() is False


def test_dev_mode_enables(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '1')
    assert validator_dev_mode() is True


def test_explicit_zero_is_off(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '0')
    assert validator_dev_mode() is False


def test_arbitrary_value_is_off(monkeypatch):
    # Only an exact "1" enables it — no accidental truthiness.
    monkeypatch.setenv('VALIDATOR_DEV_MODE', 'true')
    assert validator_dev_mode() is False
