"""VALIDATOR_MODE — the validator authority ladder.

watch — observe-only: the Solana swap loop logs "WOULD …" instead of voting, and
        should_set_weights() returns False. (= legacy VALIDATOR_DEV_MODE=1)
vote  — casts Solana consensus votes but sets no Bittensor weights (mainnet burn-in).
full  — votes and sets weights. Production default.

Unknown values raise: a typo must not silently promote a staging validator to 'full'.
"""

import pytest

from neurons.base.neuron import validator_mode


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv('VALIDATOR_MODE', raising=False)
    monkeypatch.delenv('VALIDATOR_DEV_MODE', raising=False)


def test_full_by_default():
    assert validator_mode() == 'full'


@pytest.mark.parametrize('mode', ['full', 'vote', 'watch'])
def test_explicit_modes(monkeypatch, mode):
    monkeypatch.setenv('VALIDATOR_MODE', mode)
    assert validator_mode() == mode


def test_mode_is_case_and_whitespace_tolerant(monkeypatch):
    monkeypatch.setenv('VALIDATOR_MODE', ' Vote ')
    assert validator_mode() == 'vote'


def test_unknown_mode_raises(monkeypatch):
    # Fail fast — never silently run 'full' off a typo.
    monkeypatch.setenv('VALIDATOR_MODE', 'observe')
    with pytest.raises(ValueError, match='VALIDATOR_MODE'):
        validator_mode()


def test_legacy_dev_mode_maps_to_watch(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '1')
    assert validator_mode() == 'watch'


def test_legacy_explicit_zero_is_full(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '0')
    assert validator_mode() == 'full'


def test_legacy_arbitrary_value_is_full(monkeypatch):
    # Only an exact "1" enables the legacy flag — no accidental truthiness.
    monkeypatch.setenv('VALIDATOR_DEV_MODE', 'true')
    assert validator_mode() == 'full'


def test_validator_mode_wins_over_legacy(monkeypatch):
    monkeypatch.setenv('VALIDATOR_DEV_MODE', '1')
    monkeypatch.setenv('VALIDATOR_MODE', 'vote')
    assert validator_mode() == 'vote'
