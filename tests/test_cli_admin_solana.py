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


def test_withdraw_treasury_defaults_to_full_balance(monkeypatch):
    client = MagicMock()
    client.get_treasury.return_value = types.SimpleNamespace(total=2_000_000_000)  # 2 SOL
    _patch_client(monkeypatch, client)
    pk = str(Keypair().pubkey())

    result = CliRunner().invoke(admin.admin_group, ['withdraw-treasury', pk, '--yes'])

    assert result.exit_code == 0, result.output
    args = client.withdraw_treasury.call_args.args
    assert str(args[0]) == pk and args[1] == 2_000_000_000


def test_halt_skips_when_already_halted(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(halted=True)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(admin.admin_group, ['danger', 'halt'])

    assert result.exit_code == 0
    assert 'already halted' in result.output
    client.set_halted.assert_not_called()
