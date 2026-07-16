"""`alw status` shows the configured bittensor identity (coldkey + TAO balance) alongside the
Solana keypair, and no longer points at a browser swap flow (unsupported)."""

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


def _binding(hotkey: bytes):
    return types.SimpleNamespace(hotkey=hotkey, miner=None)


def test_status_shows_bound_hotkey_with_match_note(monkeypatch):
    hk = b'\x01' * 32
    ss58 = status.hotkey_bytes_to_ss58(hk)
    _patch(monkeypatch, {'network': 'test'}, None, binding=_binding(hk), local_hotkey=ss58)

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    flat = ' '.join(result.output.split())
    assert f'Bound hotkey: {ss58}' in flat
    assert 'matches your configured hotkey' in flat


def test_status_flags_binding_config_mismatch(monkeypatch):
    hk = b'\x01' * 32
    _patch(monkeypatch, {'network': 'test'}, None, binding=_binding(hk), local_hotkey='5SomethingElse')

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'configured hotkey differs' in ' '.join(result.output.split())


def test_status_unbound_points_at_bind_command(monkeypatch):
    _patch(monkeypatch, {'network': 'test'}, None)

    result = CliRunner().invoke(status.status_command, [])

    assert result.exit_code == 0, result.output
    assert 'alw bind-hotkey' in result.output


def test_status_json_includes_bound_hotkey(monkeypatch):
    hk = b'\x02' * 32
    ss58 = status.hotkey_bytes_to_ss58(hk)
    _patch(monkeypatch, {'network': 'test'}, None, binding=_binding(hk))

    result = CliRunner().invoke(status.status_command, ['--json'])

    assert result.exit_code == 0, result.output
    assert f'"bound_hotkey": "{ss58}"' in result.output
