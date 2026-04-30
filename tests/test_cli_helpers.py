"""Tests for allways.cli.swap_commands.helpers."""

import click
import pytest

from allways.cli.swap_commands.helpers import parse_netuid


class TestParseNetuid:
    def test_int_passthrough(self):
        assert parse_netuid(7) == 7
        assert parse_netuid(0) == 0

    def test_str_digits(self):
        assert parse_netuid('7') == 7
        assert parse_netuid('0') == 0
        assert parse_netuid('  42  ') == 42

    def test_rejects_non_numeric_string(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid('notanumber')

    def test_rejects_empty_string(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid('')
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid('   ')

    def test_rejects_fractional_string(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid('1.5')

    def test_rejects_negative_string(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid('-1')

    def test_rejects_negative_int(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid(-1)

    def test_rejects_bool(self):
        # bool is an int subclass — we don't want True/False quietly becoming 1/0.
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid(True)
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid(False)

    def test_rejects_other_types(self):
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid(None)
        with pytest.raises(click.UsageError, match='non-negative integer'):
            parse_netuid(1.0)

    def test_source_appears_in_error(self):
        with pytest.raises(click.UsageError, match='--netuid'):
            parse_netuid('bad', source='--netuid')
