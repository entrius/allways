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


def test_ink_only_commands_are_gone():
    cmds = set(admin.admin_group.commands)
    assert {'recycle-fees', 'enable-chain-ext', 'transfer-ownership'} & cmds == set()
    assert 'withdraw-treasury' in cmds


def test_set_threshold_reads_config_and_calls_solana_setter(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(consensus_threshold_percent=51)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['set-threshold', '67'], input='y\n')

    assert result.exit_code == 0, result.output
    assert 'Current: 51%' in result.output
    client.set_consensus_threshold.assert_called_once_with(67)


def test_add_vali_calls_add_validator_with_weight(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(validators=[])
    _patch_client(monkeypatch, client)
    pk = str(Keypair().pubkey())

    result = CliRunner().invoke(admin.admin_group, ['add-vali', pk, '--weight', '3'], input='y\n')

    assert result.exit_code == 0, result.output
    args = client.add_validator.call_args.args
    assert str(args[0]) == pk and args[1] == 3


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


def test_withdraw_treasury_accepts_explicit_admin_recipient(monkeypatch):
    client = MagicMock()
    admin_pk = Keypair().pubkey()
    client.get_config.return_value = _config(admin=admin_pk)
    client.get_treasury.return_value = types.SimpleNamespace(total=2_000_000_000)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['withdraw-treasury', str(admin_pk), '--yes'])

    assert result.exit_code == 0, result.output
    assert client.withdraw_treasury.call_args.args[0] == admin_pk


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


def test_set_pool_window_passes_seconds(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(pool_window_secs=60)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['set-pool-window', '120'], input='y\n')

    assert result.exit_code == 0, result.output
    client.set_pool_window.assert_called_once_with(120)


def test_set_weights_interval_passes_seconds(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(weights_update_min_interval_secs=1200)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['set-weights-interval', '900'], input='y\n')

    assert result.exit_code == 0, result.output
    client.set_weights_update_min_interval.assert_called_once_with(900)


def test_set_max_extension_passes_seconds(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(max_total_extension_secs=3600)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['set-max-extension', '7200'], input='y\n')

    assert result.exit_code == 0, result.output
    client.set_max_total_extension.assert_called_once_with(7200)
