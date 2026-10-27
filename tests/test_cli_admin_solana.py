"""B4.4 — admin CLI repointed onto Solana (config setters, validator set, treasury, halt)."""

import types
from unittest.mock import MagicMock

from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands import admin


def _config(**over):
    fields = dict(
        consensus_threshold_percent=51,
        fulfillment_timeout_secs=600,
        reservation_ttl_secs=600,
        min_collateral=0,
        max_collateral=0,
        min_swap_amount=0,
        max_swap_amount=0,
        halted=False,
        reservation_fee_lamports=0,
        pool_window_secs=60,
        weights_update_min_interval_secs=1200,
        max_total_extension_secs=3600,
        validators=[],
    )
    fields.update(over)
    return types.SimpleNamespace(**fields)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(admin, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))


def test_withdraw_treasury_defaults_to_admin_and_full_balance(monkeypatch):
    client = MagicMock()
    admin_pk = Keypair().pubkey()
    client.get_config.return_value = _config(admin=admin_pk)
    client.get_treasury.return_value = types.SimpleNamespace(total=2_000_000_000)  # 2 SOL
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['withdraw-treasury', '--yes'])

    assert result.exit_code == 0, result.output
    args = client.withdraw_treasury.call_args.args
    assert args[0] == admin_pk and args[1] == 2_000_000_000


def test_withdraw_treasury_rejects_non_admin_recipient(monkeypatch):
    client = MagicMock()
    admin_pk = Keypair().pubkey()
    client.get_config.return_value = _config(admin=admin_pk)
    client.get_treasury.return_value = types.SimpleNamespace(total=2_000_000_000)
    _patch_client(monkeypatch, client)
    outsider = str(Keypair().pubkey())

    result = CliRunner().invoke(admin.admin_group, ['withdraw-treasury', outsider, '--yes'])

    assert result.exit_code != 0
    assert 'only allows treasury withdrawals to the admin' in result.output
    client.withdraw_treasury.assert_not_called()


def test_halt_submits_without_pre_reading_config(monkeypatch):
    # A stale RPC read must never block a halt: the command submits unconditionally
    # (halting an already-halted system is a harmless no-op on-chain).
    client = MagicMock()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['danger', 'halt'], input='y\n')

    assert result.exit_code == 0, result.output
    client.set_halted.assert_called_once_with(True)
    client.get_config.assert_not_called()


def test_set_reservation_fee_converts_sol_to_lamports(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(reservation_fee_lamports=0)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['set-reservation-fee', '0.001'], input='y\n')

    assert result.exit_code == 0, result.output
    client.set_reservation_fee.assert_called_once_with(1_000_000)
