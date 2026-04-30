"""Tests for resolve_dendrite_timeout — env var sanitization (issue #240)."""

import os

import pytest

from allways.cli.dendrite_lite import resolve_dendrite_timeout

ENV_VAR = 'ALW_DENDRITE_TIMEOUT'


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    monkeypatch.delenv(ENV_VAR, raising=False)


class TestResolveDendriteTimeout:
    def test_unset_returns_default(self):
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_empty_returns_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_valid_float_overrides(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '60')
        assert resolve_dendrite_timeout(30.0) == 60.0

    def test_valid_fractional_float_overrides(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '12.5')
        assert resolve_dendrite_timeout(30.0) == 12.5

    def test_nan_falls_back_to_default(self, monkeypatch):
        """The reproducer from issue #240."""
        monkeypatch.setenv(ENV_VAR, 'nan')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_inf_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, 'inf')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_negative_inf_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '-inf')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_zero_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '0')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_negative_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, '-1')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_non_numeric_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, 'foo')
        assert resolve_dendrite_timeout(30.0) == 30.0

    def test_whitespace_around_valid_value(self, monkeypatch):
        """float() strips whitespace — make sure that path still works."""
        monkeypatch.setenv(ENV_VAR, '  45  ')
        assert resolve_dendrite_timeout(30.0) == 45.0

    def test_default_propagates_on_fallback(self, monkeypatch):
        """When falling back, the *caller's* default must be honored, not 30."""
        monkeypatch.setenv(ENV_VAR, 'nan')
        assert resolve_dendrite_timeout(7.5) == 7.5
