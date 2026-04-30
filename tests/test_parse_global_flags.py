"""Tests for parse_global_flags: missing-value detection (issue #238)."""

import sys

import click
import pytest

from allways.cli.swap_commands.helpers import parse_global_flags


@pytest.fixture(autouse=True)
def restore_argv():
    saved = sys.argv[:]
    yield
    sys.argv[:] = saved


class TestParseGlobalFlags:
    def test_normal_flag_value_form(self):
        sys.argv = ['alw', 'view', 'rates', '--netuid', '7']
        assert parse_global_flags() == {'netuid': '7'}
        assert sys.argv == ['alw', 'view', 'rates']

    def test_normal_equals_form(self):
        sys.argv = ['alw', 'view', 'rates', '--netuid=7']
        assert parse_global_flags() == {'netuid': '7'}
        assert sys.argv == ['alw', 'view', 'rates']

    def test_bare_flag_at_end_raises(self):
        """Reproducer from issue #238: alw view rates --netuid"""
        sys.argv = ['alw', 'view', 'rates', '--netuid']
        with pytest.raises(click.UsageError, match='--netuid requires a value'):
            parse_global_flags()

    def test_empty_equals_form_raises(self):
        sys.argv = ['alw', 'view', 'rates', '--netuid=']
        with pytest.raises(click.UsageError, match='--netuid requires a value'):
            parse_global_flags()

    def test_bare_network_at_end_raises(self):
        """Same fix applies to all global flags, not just --netuid."""
        sys.argv = ['alw', 'status', '--network']
        with pytest.raises(click.UsageError, match='--network requires a value'):
            parse_global_flags()

    def test_empty_wallet_equals_raises(self):
        sys.argv = ['alw', 'status', '--wallet=']
        with pytest.raises(click.UsageError, match='--wallet requires a value'):
            parse_global_flags()

    def test_unknown_flag_passes_through(self):
        """Non-global flags must survive for the subcommand to see them."""
        sys.argv = ['alw', 'swap', 'now', '--amount', '0.1']
        assert parse_global_flags() == {}
        assert sys.argv == ['alw', 'swap', 'now', '--amount', '0.1']

    def test_global_flag_alias_resolves(self):
        """--wallet.name is an alias for --wallet."""
        sys.argv = ['alw', 'view', 'rates', '--wallet.name', 'alice']
        assert parse_global_flags() == {'wallet': 'alice'}
        assert sys.argv == ['alw', 'view', 'rates']

    def test_multiple_globals_in_one_call(self):
        sys.argv = ['alw', 'view', 'rates', '--netuid', '7', '--network', 'finney']
        assert parse_global_flags() == {'netuid': '7', 'network': 'finney'}
        assert sys.argv == ['alw', 'view', 'rates']

    def test_no_args(self):
        sys.argv = ['alw']
        assert parse_global_flags() == {}
        assert sys.argv == ['alw']
