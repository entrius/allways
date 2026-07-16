"""D6 — `alw view config` / `alw view validators` render the on-chain Config (mocked, no chain)."""

import types
from unittest.mock import MagicMock

from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands import view


def _config(**over):
    fields = dict(
        admin=Keypair().pubkey(),
        version=1,
        consensus_threshold_percent=51,
        fulfillment_timeout_secs=600,
        reservation_ttl_secs=600,
        min_collateral=2_000_000_000,
        max_collateral=0,
        min_swap_amount=0,
        max_swap_amount=0,
        halted=False,
        reservation_fee_lamports=1_000_000,
        pool_window_secs=60,
        weights_update_min_interval_secs=1200,
        max_total_extension_secs=3600,
        validators=[],
    )
    fields.update(over)
    return types.SimpleNamespace(**fields)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(view, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))


def test_view_config_renders_every_field(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    for label in ('Halted:', 'Consensus threshold:', 'Reservation fee:', 'Pool window:', 'Max total extension:'):
        assert label in result.output
    assert '51%' in result.output
    assert '0.001000 SOL' in result.output  # reservation fee in SOL
    assert 'On-chain Program Config' in result.output
    assert 'alw config' in result.output  # cross-link to the local CLI settings


def test_view_config_reports_uninitialized(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = None
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    assert 'not initialized' in result.output


def test_view_validators_lists_pubkeys_and_weights(monkeypatch):
    v = Keypair().pubkey()
    vinfo = types.SimpleNamespace(key=bytes(v), weight=3)
    client = MagicMock()
    client.get_config.return_value = _config(validators=[vinfo], consensus_threshold_percent=67)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['validators'])

    assert result.exit_code == 0, result.output
    assert str(v) in result.output
    assert 'weight=3' in result.output
    assert '67%' in result.output


def test_view_validators_handles_empty_set(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(validators=[])
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['validators'])

    assert result.exit_code == 0, result.output
    assert 'No validators registered' in result.output
