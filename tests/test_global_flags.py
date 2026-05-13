import sys

import click
import pytest

from allways.cli.swap_commands.helpers import parse_global_flags


def test_bare_netuid_global_flag_reports_missing_value(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['alw', 'view', 'rates', '--netuid'])

    with pytest.raises(click.UsageError, match='--netuid requires a value'):
        parse_global_flags()


def test_empty_netuid_global_flag_reports_missing_value(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['alw', 'view', 'rates', '--netuid='])

    with pytest.raises(click.UsageError, match='--netuid requires a value'):
        parse_global_flags()


def test_netuid_global_flag_with_value_is_stripped(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['alw', 'view', 'rates', '--netuid', '7'])

    overrides = parse_global_flags()

    assert overrides == {'netuid': '7'}
    assert sys.argv == ['alw', 'view', 'rates']
