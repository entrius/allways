"""`alw config` renders with no config file, and never prints the RPC API key."""

import json

import pytest
from click.testing import CliRunner

from allways.cli import main
from allways.cli.swap_commands import helpers


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    path = tmp_path / 'config.json'
    monkeypatch.setattr(main, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, 'CONFIG_FILE', path)
    monkeypatch.setattr(helpers, '_CLI_OVERRIDES', {})
    for var in ('SOLANA_RPC_URL', 'SOLANA_RPC_API_KEY', 'SOLANA_KEYPAIR_PATH', 'BTC_NETWORK', 'ALLWAYS_PROGRAM_ID'):
        monkeypatch.delenv(var, raising=False)
    return path


def _show():
    return CliRunner().invoke(main.config_group, [])


def test_show_config_without_file_renders_defaults(config_file):
    result = _show()

    assert result.exit_code == 0, result.output
    assert 'No config file yet' in result.output
    assert 'finney' in result.output  # network default
    assert '(not set)' in result.output  # wallet/hotkey/router
    assert '127.0.0.1:8899' in result.output  # solana-rpc localnet default
    assert 'alw view config' in result.output


def test_show_config_redacts_rpc_api_key(config_file, monkeypatch):
    config_file.write_text(json.dumps({'solana-rpc': 'https://x.y/rpc'}))
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'sekrit')

    result = _show()

    assert result.exit_code == 0, result.output
    assert 'sekrit' not in result.output
    assert '***' in result.output
