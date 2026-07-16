"""`alw status` shows the configured bittensor identity (coldkey + TAO balance) alongside the
Solana keypair, and no longer points at a browser swap flow (unsupported)."""

import types
from unittest.mock import MagicMock

from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands import status


def _client():
    client = MagicMock()
    client.rpc.url = 'https://api.devnet.solana.com'
    client.get_config.return_value = types.SimpleNamespace(halted=False)
    client.rpc.get_account_lamports.return_value = 5_000_000_000
    client.get_miner_state.return_value = None
    return client


def _patch(monkeypatch, config, tao):
    monkeypatch.setattr(status, 'get_effective_config', lambda: config)
    monkeypatch.setattr(status, 'get_solana_cli_context', lambda need_keypair=True: ({}, _client()))
    monkeypatch.setattr(status, '_load_caller', lambda _: Keypair().pubkey())
    monkeypatch.setattr(status, '_tao_identity', lambda _: tao)
    monkeypatch.setattr(status, '_saved_miner', lambda: None)


def test_status_shows_tao_identity_when_wallet_configured(monkeypatch):
    _patch(monkeypatch, {'wallet': 'ck', 'network': 'test'}, ('5FabcColdkey', 10.5))

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'ck (5FabcColdkey)' in result.output
    assert '10.5000 τ' in result.output
    assert 'browser' not in result.output.lower()


def test_status_omits_tao_lines_without_wallet(monkeypatch):
    _patch(monkeypatch, {'network': 'test'}, None)

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'TAO balance' not in result.output
    assert 'browser' not in result.output.lower()


def test_status_json_includes_tao_fields(monkeypatch):
    _patch(monkeypatch, {'wallet': 'ck', 'network': 'test'}, ('5FabcColdkey', None))

    result = CliRunner().invoke(status.status_command, ['--json'])

    assert result.exit_code == 0, result.output
    assert '"coldkey": "5FabcColdkey"' in result.output
    assert '"tao_balance": null' in result.output


def test_tao_identity_none_without_wallet():
    assert status._tao_identity({}) is None
