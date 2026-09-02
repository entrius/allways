"""Vault governance CLI — the guards that stop a validator voting the opposite of what they mean."""

from unittest.mock import MagicMock

from click.testing import CliRunner

from allways.cli.swap_commands import vault as vault_cli
from allways.vault.client import VaultCallResult

VALIDATORS = ['5Django', '5Eve']
HOTKEY = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def _client(monkeypatch, **over):
    client = MagicMock()
    client.keypair.ss58_address = '5Signer'
    client.get_validators.return_value = VALIDATORS
    client.get_min_collateral.return_value = 250_000_000
    client.get_max_collateral.return_value = 10_000_000_000
    client.get_consensus_threshold.return_value = 66
    client.get_vote_round_ttl.return_value = 600
    client.get_staking_hotkey.return_value = HOTKEY
    client.get_netuid.return_value = 7
    client.admin_call.return_value = VaultCallResult(ok=True, extrinsic_hash='0xabc')
    for key, value in over.items():
        getattr(client, key).return_value = value
    monkeypatch.setattr(vault_cli, '_client', lambda use_coldkey=False: client)
    return client


def _run(args, stdin=None):
    return CliRunner().invoke(vault_cli.vault_admin_group, args, input=stdin)


def test_a_sub_majority_threshold_is_refused_before_it_reaches_the_chain(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['-y', 'set-config', '--threshold', '50'])
    assert result.exit_code != 0
    assert '51' in result.output
    client.admin_call.assert_not_called()


def test_the_majority_floor_itself_is_accepted(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['-y', 'set-config', '--threshold', '51'])
    assert result.exit_code == 0
    client.admin_call.assert_called_once()


def test_max_collateral_zero_is_named_as_unlimited_and_gated(monkeypatch):
    """0 is the contract's UNLIMITED sentinel: a validator meaning "close the vault" would
    otherwise be voting the exact opposite, unanimously."""
    client = _client(monkeypatch)
    result = _run(['set-config', '--max-collateral', '0'], stdin='n\n')
    assert 'UNLIMITED' in result.output
    client.admin_call.assert_not_called()


def test_declining_the_unlimited_prompt_submits_nothing(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['set-config', '--max-collateral', '0'], stdin='n\n')
    assert 'Cancelled' in result.output
    client.admin_call.assert_not_called()


def test_a_positive_max_collateral_needs_no_unlimited_prompt(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['-y', 'set-config', '--max-collateral', '5'])
    assert 'UNLIMITED' not in result.output
    client.admin_call.assert_called_once()


def test_set_recycle_target_votes_both_fields_and_prints_the_peer_command(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['-y', 'set-recycle-target', HOTKEY, '9'])
    assert result.exit_code == 0
    label, hotkey_arg, netuid_arg = client.admin_call.call_args.args
    assert label == 'vote_set_recycle_target'
    assert len(hotkey_arg) == 32
    assert netuid_arg == (9).to_bytes(2, 'little')
    # Every other validator must run the identical command or they open their own round.
    # (Normalised: rich wraps the printed command to the terminal width.)
    printed = ' '.join(result.output.split())
    assert f'set-recycle-target {HOTKEY} 9' in printed


def test_set_recycle_target_refuses_a_malformed_address(monkeypatch):
    client = _client(monkeypatch)
    result = _run(['-y', 'set-recycle-target', 'not-an-address', '9'])
    assert result.exit_code != 0
    client.admin_call.assert_not_called()


def test_set_config_peer_command_round_trips_for_precision_losing_rao():
    """The printed TAO amount must re-parse to the IDENTICAL rao — `%g`/`float` both drop values."""
    for rao in [0, 1, 250_000_000, 1_000_000_007, 9_999_999_999, 10_000_000_007, 123_456_789_012_345]:
        printed = vault_cli._rao_to_tao_flag(rao)
        assert vault_cli._tao_flag_to_rao(printed, 'x') == rao
    # Prove the hazard the fix removes: the same string via float truncates a rao.
    assert int(float(vault_cli._rao_to_tao_flag(1_000_000_007)) * 1_000_000_000) != 1_000_000_007


def test_set_config_votes_exact_rao_and_prints_a_round_tripping_peer_command(monkeypatch):
    import re

    client = _client(monkeypatch)
    result = _run(['-y', 'set-config', '--min-collateral', '1.000000007', '--max-collateral', '9.000000023'])
    assert result.exit_code == 0
    _label, min_arg, max_arg, _thr, _ttl = client.admin_call.call_args.args
    assert int.from_bytes(min_arg, 'little') == 1_000_000_007
    assert int.from_bytes(max_arg, 'little') == 9_000_000_023
    printed = ' '.join(result.output.split())
    m = re.search(r'--min-collateral (\S+) --max-collateral (\S+)', printed)
    assert m
    assert vault_cli._tao_flag_to_rao(m.group(1), 'min') == 1_000_000_007
    assert vault_cli._tao_flag_to_rao(m.group(2), 'max') == 9_000_000_023


# ─── _report / vault-address guards (v3.1 testnet findings) ──────────────────


def test_transfer_failed_names_the_gas_precharge(capsys):
    """Posting near the signer's whole balance dies on the pallet's fee pre-charge; a bare
    "Call failed (TransferFailed)" gives the operator nothing to act on."""
    vault_cli._report(VaultCallResult(ok=False, error='TransferFailed'), 'unused')
    out = capsys.readouterr().out
    assert 'TransferFailed' in out
    assert 'pre-charges' in out
    assert 'headroom' in out


def test_off_record_vault_address_warns(capsys):
    """A stale configured vault reads back healthy but no validator attests bonds posted to it."""
    from allways.constants import TAO_HUB_VAULT_ADDRESSES

    vault_cli._warn_if_off_record('5GAE4JD8zpQUfYLKKqWifLMEpEo9YrqkkUUdxjsmHyogBEcD', 'test')
    out = capsys.readouterr().out
    assert 'of-record' in out
    assert TAO_HUB_VAULT_ADDRESSES['test'][:8] in out


def test_on_record_or_unknown_network_vault_address_stays_quiet(capsys):
    from allways.constants import TAO_HUB_VAULT_ADDRESSES

    vault_cli._warn_if_off_record(TAO_HUB_VAULT_ADDRESSES['test'], 'test')
    vault_cli._warn_if_off_record(TAO_HUB_VAULT_ADDRESSES['finney'], 'finney')
    vault_cli._warn_if_off_record('5AnyVault', 'local')  # no of-record entry
    vault_cli._warn_if_off_record('5AnyVault', None)
    assert capsys.readouterr().out == ''


def test_success_report_hides_the_event_dump(capsys):
    """Success reads as one green line + extrinsic; the raw event list is failure diagnostics."""
    result = VaultCallResult(ok=True, events=['Balances.Withdraw', 'System.ExtrinsicSuccess'], extrinsic_hash='0xabc')
    vault_cli._report(result, 'Posted 2.200000000 τ')
    out = capsys.readouterr().out
    assert 'Posted 2.200000000' in out
    assert 'events:' not in out
    assert '0xabc' in out


def test_failure_report_keeps_the_event_dump(capsys):
    result = VaultCallResult(ok=False, error='Unknown', events=['System.ExtrinsicFailed'], extrinsic_hash='0xdef')
    vault_cli._report(result, 'unused')
    out = capsys.readouterr().out
    assert 'events: System.ExtrinsicFailed' in out


def test_vault_deposit_takes_amount_flag_and_keeps_post_collateral_alias(monkeypatch):
    client = _client(monkeypatch, post_collateral=VaultCallResult(ok=True, extrinsic_hash='0xabc'))
    for cmd in ('deposit', 'post-collateral'):
        res = CliRunner().invoke(vault_cli.vault_group, [cmd, '--amount', '1.5'])
        assert res.exit_code == 0, res.output
        assert client.post_collateral.call_args.args == (1_500_000_000,)
