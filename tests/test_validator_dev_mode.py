"""VALIDATOR_DEV_MODE — observe-only gate for staging a validator.

Dev mode makes the validator take no binding actions: the Solana swap loop logs "WOULD …"
instead of voting, and should_set_weights() returns False so no Bittensor weights are set.
VALIDATOR_DEV_MODE=1 is the flag; SOLANA_VALIDATOR_READONLY is the deprecated alias (still
honored, warns once) that only ever gated Solana votes.
"""

import neurons.base.neuron as neuron
from neurons.base.neuron import validator_dev_mode


def _reset():
    neuron._dev_mode_warned = False


def test_off_by_default(monkeypatch):
    monkeypatch.delenv('VALIDATOR_DEV_MODE', raising=False)
    monkeypatch.delenv('SOLANA_VALIDATOR_READONLY', raising=False)
    _reset()
    assert validator_dev_mode() is False


def test_dev_mode_enables(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '1')
    monkeypatch.delenv('SOLANA_VALIDATOR_READONLY', raising=False)
    _reset()
    assert validator_dev_mode() is True


def test_explicit_zero_is_off(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '0')
    monkeypatch.delenv('SOLANA_VALIDATOR_READONLY', raising=False)
    _reset()
    assert validator_dev_mode() is False


def test_deprecated_alias_still_honored_and_warns_once(monkeypatch):
    monkeypatch.delenv('VALIDATOR_DEV_MODE', raising=False)
    monkeypatch.setenv('SOLANA_VALIDATOR_READONLY', '1')
    _reset()

    warnings = []
    monkeypatch.setattr(neuron.bt.logging, 'warning', lambda msg, *a, **k: warnings.append(msg))

    assert validator_dev_mode() is True
    assert validator_dev_mode() is True  # still honored on repeat calls
    # ...but the deprecation warning fires exactly once (guarded by _dev_mode_warned).
    assert len(warnings) == 1
    assert 'VALIDATOR_DEV_MODE' in warnings[0]


def test_dev_mode_takes_precedence_over_alias(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '1')
    monkeypatch.setenv('SOLANA_VALIDATOR_READONLY', '1')
    _reset()
    warnings = []
    monkeypatch.setattr(neuron.bt.logging, 'warning', lambda msg, *a, **k: warnings.append(msg))
    assert validator_dev_mode() is True
    # New flag wins before the alias is consulted, so no deprecation warning.
    assert warnings == []
