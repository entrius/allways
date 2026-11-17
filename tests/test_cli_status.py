"""`alw status` renders the configured identity and never prints the RPC API key."""

import types
from unittest.mock import MagicMock

from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands import status


def _client(binding=None):
    client = MagicMock()
    client.rpc.url = 'https://api.devnet.solana.com'
    client.get_config.return_value = types.SimpleNamespace(halted=False)
    client.rpc.get_account_lamports.return_value = 5_000_000_000
    client.get_miner_state.return_value = None
    client.get_binding.return_value = binding
    return client


def _patch(monkeypatch, config, tao, binding=None, local_hotkey=None):
    monkeypatch.setattr(status, 'get_effective_config', lambda: config)
    monkeypatch.setattr(status, 'get_solana_cli_context', lambda need_keypair=True: ({}, _client(binding)))
    monkeypatch.setattr(status, '_load_caller', lambda _: Keypair().pubkey())
    monkeypatch.setattr(status, '_tao_identity', lambda _: tao)
    monkeypatch.setattr(status, '_configured_hotkey_ss58', lambda _: local_hotkey)
    monkeypatch.setattr(status, '_saved_miner', lambda: None)


def test_status_shows_tao_identity_when_wallet_configured(monkeypatch):
    _patch(monkeypatch, {'wallet': 'ck', 'network': 'test'}, ('5FabcColdkey', 10.5))

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'ck (5FabcColdkey)' in result.output
    assert '10.5000 τ' in result.output
    assert 'browser' not in result.output.lower()


def test_status_redacts_rpc_api_key(monkeypatch):
    _patch(monkeypatch, {'network': 'test'}, None)
    client = _client()
    client.rpc.url = 'https://mainnet.helius-rpc.com/?api-key=supersecretvalue'
    monkeypatch.setattr(status, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'supersecretvalue' not in result.output
    assert 'api-key=***' in result.output


def test_status_json_redacts_rpc_api_key(monkeypatch):
    _patch(monkeypatch, {'network': 'test'}, None)
    client = _client()
    client.rpc.url = 'https://mainnet.helius-rpc.com/?api-key=supersecretvalue'
    monkeypatch.setattr(status, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))

    result = CliRunner().invoke(status.status_command, ['--json'])

    assert result.exit_code == 0, result.output
    assert 'supersecretvalue' not in result.output
