"""`alw config` shows every settable key's EFFECTIVE value + source (works with no config file),
and an unreachable RPC fails with the resolved URL + a first-run hint instead of a raw traceback."""

import json
from unittest.mock import MagicMock

import pytest
import requests
from click.testing import CliRunner

from allways.cli import main
from allways.cli.swap_commands import helpers
from allways.solana.rpc import SolanaRpc, SolanaRpcUnreachable


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


def test_show_config_reports_sources_and_derived_rpc(config_file):
    config_file.write_text(json.dumps(helpers.ENV_BUNDLES['testnet'] | {'wallet': 'alice'}))

    result = _show()

    assert result.exit_code == 0, result.output
    assert 'alice' in result.output
    assert 'devnet' in result.output
    assert 'api.devnet.solana' in result.output  # rpc derived from the network name
    assert 'solana-network' in result.output  # ...and labeled as such
    assert 'config' in result.output


def test_show_config_redacts_rpc_api_key(config_file, monkeypatch):
    config_file.write_text(json.dumps({'solana-rpc': 'https://x.y/rpc'}))
    monkeypatch.setenv('SOLANA_RPC_API_KEY', 'sekrit')

    result = _show()

    assert result.exit_code == 0, result.output
    assert 'sekrit' not in result.output
    assert '***' in result.output


def test_show_config_survives_invalid_json(config_file):
    config_file.write_text('{not json')

    result = _show()

    assert result.exit_code == 0, result.output
    assert 'could not parse' in result.output
    assert 'finney' in result.output  # still renders the defaults table


def test_safe_read_unreachable_names_url_and_hints_first_run(config_file, capsys):
    def boom():
        raise SolanaRpcUnreachable('getAccountInfo: could not connect', url='http://127.0.0.1:8899')

    with pytest.raises(SystemExit):
        helpers.safe_read(boom, what='read config')
    out = capsys.readouterr().out
    assert 'http://127.0.0.1:8899' in out
    assert 'alw config set env testnet' in out  # no config file → first-run hint


def test_safe_read_unreachable_skips_hint_when_configured(config_file, capsys):
    config_file.write_text('{}')

    def boom():
        raise SolanaRpcUnreachable('getAccountInfo: could not connect', url='https://x.y/rpc')

    with pytest.raises(SystemExit):
        helpers.safe_read(boom, what='read config')
    out = capsys.readouterr().out
    assert 'https://x.y/rpc' in out
    assert 'config set env' not in out


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr('allways.solana.rpc.time.sleep', lambda *_: None)


def test_rpc_connection_error_raises_unreachable_with_url():
    rpc = SolanaRpc('http://127.0.0.1:8899')
    rpc._session = MagicMock()
    rpc._session.post.side_effect = requests.ConnectionError('refused')

    with pytest.raises(SolanaRpcUnreachable) as exc:
        rpc.get_account_info('11111111111111111111111111111111')
    assert exc.value.url == 'http://127.0.0.1:8899'
