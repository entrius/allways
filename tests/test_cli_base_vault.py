"""alw base-vault — every local refusal fires before anything is signed, on either chain."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from eth_account import Account
from eth_account.messages import encode_defunct
from solders.pubkey import Pubkey

from allways.cli.swap_commands import base_vault as cli
from allways.solana.client import evm_bind_message
from allways.vault.base_client import BaseBond, BaseCallResult, BaseVaultError

OWNER = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'
OWNER_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
PROGRAM_ID = Pubkey.from_string('6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD')
OTHER = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
VAULT = '0x1111111111111111111111111111111111111111'
PUBKEY = Pubkey.from_bytes(bytes(range(1, 33)))
STRANGER = Pubkey.from_bytes(bytes(range(2, 34)))
MIN = 100_000_000  # 100 USDC
OK = BaseCallResult(ok=True, tx_hash='0xabc')


def _vault(monkeypatch, bond=None, min_collateral=MIN, max_collateral=0, usdc=10**12, **over):
    vault = MagicMock()
    vault.signer_address = OWNER
    vault.address = VAULT
    vault.get_bond.return_value = bond or BaseBond(0, False, 0, bytes(32))
    vault.min_collateral.return_value = min_collateral
    vault.max_collateral.return_value = max_collateral
    vault.usdc_balance.return_value = usdc
    for name in ('post_collateral_with_permit', 'lock_bond', 'withdraw_collateral'):
        getattr(vault, name).return_value = OK
    vault.approve_and_post.return_value = [OK, OK]
    for key, value in over.items():
        getattr(vault, key).side_effect = value
    monkeypatch.setattr(cli, '_client', lambda: vault)
    return vault


def _solana(monkeypatch, bound_to=None, claimed_by=None):
    client = MagicMock()
    client.keypair.pubkey.return_value = PUBKEY
    client.program_id = PROGRAM_ID
    client.get_evm_binding.return_value = SimpleNamespace(evm_address=bytes.fromhex(bound_to[2:])) if bound_to else None
    client.get_evm_address_binding.return_value = SimpleNamespace(miner=claimed_by) if claimed_by else None
    monkeypatch.setattr(cli, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))
    monkeypatch.setattr(cli, '_base_account', lambda: Account.from_key(OWNER_KEY))
    return client


def _bind_message():
    return evm_bind_message(bytes.fromhex(OWNER[2:]), PUBKEY, PROGRAM_ID)


def _signed_elsewhere(message=None, key=OWNER_KEY):
    return Account.from_key(key).sign_message(encode_defunct(text=message or _bind_message())).signature


def _run(args, stdin=None):
    return CliRunner().invoke(cli.base_vault_group, args, input=stdin)


def _text(result):
    return ' '.join(result.output.split())


# ─── bind ────────────────────────────────────────────────────────────────────


def test_bind_refuses_a_pubkey_already_bound_to_another_address(monkeypatch):
    client = _solana(monkeypatch, bound_to=OTHER)
    result = _run(['bind', '-y'])
    assert result.exit_code != 0
    assert OTHER in _text(result) and 'never changes' in _text(result)
    client.bind_evm_address.assert_not_called()


def test_bind_refuses_an_address_another_pubkey_already_claimed(monkeypatch):
    client = _solana(monkeypatch, claimed_by=STRANGER)
    result = _run(['bind', '-y'])
    assert result.exit_code != 0
    assert 'already claimed' in _text(result)
    client.bind_evm_address.assert_not_called()


def test_bind_reads_no_sol_collateral(monkeypatch):
    client = _solana(monkeypatch)
    assert _run(['bind', '-y']).exit_code == 0
    client.get_miner_state.assert_not_called()
    client.get_config.assert_not_called()


def test_bind_is_a_noop_when_already_bound_both_ways(monkeypatch):
    client = _solana(monkeypatch, bound_to=OWNER, claimed_by=PUBKEY)
    result = _run(['bind', '-y'])
    assert result.exit_code == 0
    assert 'Already bound' in _text(result)
    client.bind_evm_address.assert_not_called()


def test_bind_sends_the_base_address_signed_by_the_base_key(monkeypatch):
    client = _solana(monkeypatch)
    assert _run(['bind', '-y']).exit_code == 0
    client.bind_evm_address.assert_called_once_with(bytes.fromhex(OWNER[2:]), bytes(_signed_elsewhere()))


def test_bind_with_address_alone_prints_the_message_and_sends_nothing(monkeypatch):
    client = _solana(monkeypatch)
    monkeypatch.setattr(cli, '_base_account', lambda: pytest.fail('no Base key needed'))
    result = _run(['bind', '--address', OWNER])
    assert result.exit_code == 0
    assert _bind_message() in result.output
    client.bind_evm_address.assert_not_called()


def test_bind_accepts_a_signature_made_elsewhere(monkeypatch):
    client = _solana(monkeypatch)
    monkeypatch.setattr(cli, '_base_account', lambda: pytest.fail('no Base key needed'))
    sig = _signed_elsewhere()
    raw_v = sig[:64] + bytes([sig[64] - 27])  # a wallet returning v as 0/1 is normalized
    assert _run(['bind', '--address', OWNER, '--signature', '0x' + raw_v.hex(), '-y']).exit_code == 0
    client.bind_evm_address.assert_called_once_with(bytes.fromhex(OWNER[2:]), bytes(sig))


def test_bind_refuses_a_signature_from_another_key_or_message(monkeypatch):
    client = _solana(monkeypatch)
    for sig in (_signed_elsewhere(key='0x' + '42' * 32), _signed_elsewhere(message='something else')):
        result = _run(['bind', '--address', OWNER, '--signature', sig.hex(), '-y'])
        assert result.exit_code != 0 and 'not ' + OWNER in _text(result)
    result = _run(['bind', '--signature', _signed_elsewhere().hex(), '-y'])
    assert result.exit_code != 0 and 'needs --address' in _text(result)
    client.bind_evm_address.assert_not_called()


# ─── deposit ─────────────────────────────────────────────────────────────────


def test_deposit_uses_a_permit_by_default(monkeypatch):
    vault = _vault(monkeypatch)
    result = _run(['deposit', '--amount', '250.5'])
    assert result.exit_code == 0, result.output
    vault.post_collateral_with_permit.assert_called_once_with(250_500_000)
    vault.approve_and_post.assert_not_called()


def test_deposit_approve_flag_falls_back_to_two_txs(monkeypatch):
    vault = _vault(monkeypatch)
    assert _run(['deposit', '--amount', '250', '--approve']).exit_code == 0
    vault.approve_and_post.assert_called_once_with(250_000_000)
    vault.post_collateral_with_permit.assert_not_called()


def test_deposit_refuses_more_precision_than_usdc_has(monkeypatch):
    vault = _vault(monkeypatch)
    result = _run(['deposit', '--amount', '1.0000001'])
    assert result.exit_code != 0 and '6 decimals' in _text(result)
    vault.post_collateral_with_permit.assert_not_called()


@pytest.mark.parametrize('amount', ['0', '-5'])
def test_deposit_refuses_a_non_positive_amount(monkeypatch, amount):
    vault = _vault(monkeypatch)
    assert _run(['deposit', '--amount', amount]).exit_code != 0
    vault.post_collateral_with_permit.assert_not_called()


def test_deposit_refuses_to_cross_the_vault_max(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(400_000_000, False, 0, bytes(32)), max_collateral=500_000_000)
    result = _run(['deposit', '--amount', '200'])
    assert result.exit_code != 0
    assert 'max' in _text(result) and '100.000000 USDC' in _text(result)
    vault.post_collateral_with_permit.assert_not_called()


def test_deposit_refuses_more_usdc_than_the_wallet_holds(monkeypatch):
    vault = _vault(monkeypatch, usdc=5_000_000)
    result = _run(['deposit', '--amount', '10'])
    assert result.exit_code != 0 and 'not enough' in _text(result)
    vault.post_collateral_with_permit.assert_not_called()


def test_deposit_below_the_lock_floor_warns_but_still_deposits(monkeypatch):
    vault = _vault(monkeypatch)
    result = _run(['deposit', '--amount', '40'])
    assert result.exit_code == 0
    assert 'under the 100.000000 USDC needed to lock' in _text(result)
    vault.post_collateral_with_permit.assert_called_once()


def test_deposit_low_eth_surfaces_the_gas_refusal(monkeypatch):
    _vault(
        monkeypatch,
        post_collateral_with_permit=BaseVaultError('Not enough Base ETH for gas: 0x… holds 0.00000001 ETH'),
    )
    result = _run(['deposit', '--amount', '150'])
    assert result.exit_code != 0
    assert 'Not enough Base ETH' in _text(result) and 'Nothing was sent' in _text(result)


# ─── lock ────────────────────────────────────────────────────────────────────


def test_lock_refuses_without_a_solana_binding(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, False, 0, bytes(32)))
    _solana(monkeypatch)
    result = _run(['lock'])
    assert result.exit_code != 0 and 'base-vault bind' in _text(result)
    vault.lock_bond.assert_not_called()


def test_lock_refuses_when_the_binding_names_another_address(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, False, 0, bytes(32)))
    _solana(monkeypatch, bound_to=OTHER, claimed_by=PUBKEY)
    result = _run(['lock'])
    assert result.exit_code != 0 and 'never attest' in _text(result)
    vault.lock_bond.assert_not_called()


def test_lock_refuses_below_the_min_collateral(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN - 1, False, 0, bytes(32)))
    _solana(monkeypatch, bound_to=OWNER, claimed_by=PUBKEY)
    result = _run(['lock'])
    assert result.exit_code != 0 and 'needed to lock' in _text(result)
    vault.lock_bond.assert_not_called()


def test_lock_refuses_an_already_locked_bond(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, True, 2, bytes(PUBKEY)))
    _solana(monkeypatch, bound_to=OWNER, claimed_by=PUBKEY)
    result = _run(['lock'])
    assert result.exit_code != 0 and 'already locked' in _text(result)
    vault.lock_bond.assert_not_called()


def test_lock_names_the_bound_solana_pubkey(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, False, 0, bytes(32)))
    _solana(monkeypatch, bound_to=OWNER, claimed_by=PUBKEY)
    assert _run(['lock']).exit_code == 0
    vault.lock_bond.assert_called_once_with(bytes(PUBKEY))


# ─── withdraw ────────────────────────────────────────────────────────────────


def test_withdraw_refuses_a_locked_bond_and_explains_the_exit(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, True, 1, bytes(PUBKEY)))
    result = _run(['withdraw', '--amount', '10'])
    assert result.exit_code != 0
    assert 'quit -> settle -> unlock -> withdraw' in _text(result)
    vault.withdraw_collateral.assert_not_called()


def test_withdraw_refuses_more_than_the_bond(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, False, 1, bytes(PUBKEY)))
    result = _run(['withdraw', '--amount', '101'])
    assert result.exit_code != 0
    vault.withdraw_collateral.assert_not_called()


def test_withdraw_an_unlocked_bond(monkeypatch):
    vault = _vault(monkeypatch, bond=BaseBond(MIN, False, 1, bytes(PUBKEY)))
    assert _run(['withdraw', '--amount', '100']).exit_code == 0
    vault.withdraw_collateral.assert_called_once_with(MIN)


# ─── off-record warning ──────────────────────────────────────────────────────


def test_an_off_record_vault_warns(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'BASE_HUB_VAULT_ADDRESSES', {'sepolia': OTHER})
    cli._warn_if_off_record(VAULT, 'sepolia')
    out = ' '.join(capsys.readouterr().out.split())
    assert 'not the of-record vault' in out and OTHER in out


def test_the_of_record_vault_does_not_warn_whatever_its_casing(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'BASE_HUB_VAULT_ADDRESSES', {'sepolia': OTHER})
    cli._warn_if_off_record(OTHER.lower(), 'sepolia')
    assert capsys.readouterr().out == ''


def test_no_record_for_the_network_means_no_warning(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'BASE_HUB_VAULT_ADDRESSES', {})
    cli._warn_if_off_record(VAULT, 'sepolia')
    assert capsys.readouterr().out == ''


def test_the_client_builder_warns_from_the_configured_address(monkeypatch, capsys):
    monkeypatch.setenv('ALLWAYS_BASE_VAULT_ADDRESS', VAULT)
    monkeypatch.setenv('BASE_NETWORK', 'sepolia')
    monkeypatch.setenv('BASE_PRIVATE_KEY', '0x' + '11' * 32)
    monkeypatch.setattr(cli, 'get_effective_config', lambda: {})
    monkeypatch.setattr(cli, 'BASE_HUB_VAULT_ADDRESSES', {'sepolia': OTHER})
    vault = cli._client()
    assert vault.address == VAULT
    assert 'not the of-record vault' in ' '.join(capsys.readouterr().out.split())
