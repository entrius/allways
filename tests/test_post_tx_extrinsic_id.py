"""`post-tx` takes btcli's `<block>-<idx>` and relays the creditable hash, a MEV shield unwrapped to its inner send."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from allways.assets.alpha import Alpha
from allways.assets.asset import ProviderUnreachableError
from allways.assets.tao import Tao
from allways.chains import CHAIN_SN7
from allways.cli.swap_commands import post_tx
from allways.cli.swap_commands.post_tx import parse_extrinsic_id

USER = '5ELSfSpQzLwnauZijPRWN1Zdqh5er6g5iZ8Q7vEm4Pfcnsc5'
OTHER = '5H8ctmwaDk8GSThzEDWpQLqaTgLHdj62tHyxDtMNTtLEdr1M'
BLOCK = 8083554


def _ext(tag, module, function, args=(), signer=USER, nonce=8):
    call_args = [{'name': name, 'value': value} for name, value in args]
    value = {
        'address': signer,
        'nonce': nonce,
        'call': {'call_module': module, 'call_function': function, 'call_args': call_args},
    }
    return SimpleNamespace(extrinsic_hash=bytes([tag]) * 32, value=value)


def _transfer_stake(tag, netuid=CHAIN_SN7.netuid, **kw):
    args = (
        ('destination_coldkey', OTHER),
        ('hotkey', 'hot'),
        ('origin_netuid', netuid),
        ('destination_netuid', netuid),
        ('alpha_amount', 25_000_000_000),
    )
    return _ext(tag, 'SubtensorModule', 'transfer_stake', args, **kw)


def _shield(tag=0x0F, **kw):
    return _ext(tag, 'MevShield', 'submit_encrypted', (('ciphertext', '0x…'),), **kw)


def _hash(ext):
    return '0x' + ext.extrinsic_hash.hex()


def _alpha(blocks):
    p = Alpha(CHAIN_SN7, SimpleNamespace())
    p.chain.get_block = lambda n: blocks.get(n)
    return p


def test_plain_transfer_stake_id_resolves_to_its_own_hash():
    inner = _transfer_stake(0xB3)
    assert _alpha({BLOCK: {'extrinsics': [_shield(), inner]}}).locate_transfer(BLOCK, 1) == (_hash(inner), BLOCK)


def test_shield_id_resolves_to_the_signers_next_nonce_in_the_same_block():
    decoy = _transfer_stake(0xDD, signer=OTHER, nonce=9)
    inner = _transfer_stake(0xB3, nonce=9)
    p = _alpha({BLOCK: {'extrinsics': [_transfer_stake(0xAA, nonce=7), _shield(nonce=8), decoy, inner]}})
    assert p.locate_transfer(BLOCK, 1) == (_hash(inner), BLOCK)


def test_shield_id_follows_the_inner_into_a_later_block():
    inner = _transfer_stake(0xB3, nonce=9)
    p = _alpha(
        {BLOCK: {'extrinsics': [_shield(nonce=8)]}, BLOCK + 1: {'extrinsics': []}, BLOCK + 2: {'extrinsics': [inner]}}
    )
    assert p.locate_transfer(BLOCK, 0) == (_hash(inner), BLOCK + 2)


def test_shield_whose_inner_never_lands_is_refused():
    blocks = {n: {'extrinsics': []} for n in range(BLOCK, BLOCK + 4)}
    blocks[BLOCK] = {'extrinsics': [_shield(nonce=8)]}
    with pytest.raises(ValueError, match='revealed nothing'):
        _alpha(blocks).locate_transfer(BLOCK, 0)


def test_shield_revealing_a_non_transfer_is_refused():
    revealed = _ext(0xCC, 'SubtensorModule', 'add_stake', nonce=9)
    with pytest.raises(ValueError, match='add_stake is not a creditable transfer'):
        _alpha({BLOCK: {'extrinsics': [_shield(nonce=8), revealed]}}).locate_transfer(BLOCK, 0)


def test_index_outside_the_block_is_refused():
    with pytest.raises(ValueError, match='none at index 3'):
        _alpha({BLOCK: {'extrinsics': [_transfer_stake(0xB3)]}}).locate_transfer(BLOCK, 3)


def test_raw_fallback_block_cannot_resolve_an_id():
    with pytest.raises(ProviderUnreachableError):
        _alpha({BLOCK: {'extrinsics': [], '_raw': True}}).locate_transfer(BLOCK, 0)


def test_tao_chain_resolves_a_balances_transfer_with_its_own_decoder():
    transfer = _ext(0xB3, 'Balances', 'transfer_keep_alive', (('dest', {'Id': OTHER}), ('value', 10)))
    p = Tao(SimpleNamespace())
    p.get_block = lambda n: {'extrinsics': [transfer]}
    assert p.locate_transfer(BLOCK, 0) == (_hash(transfer), BLOCK)


def test_only_block_dash_idx_is_an_extrinsic_id():
    assert parse_extrinsic_id('8083554-7') == (8083554, 7)
    for text in ('0xab20', '54foaURhGH', '8083554', '8083554-', '8083554-7-1'):
        assert parse_extrinsic_id(text) is None


def test_tail_block_not_minted_yet_is_retryable():
    p = _alpha({BLOCK: {'extrinsics': [_shield(nonce=8)]}})
    p.chain.get_current_block_height = lambda: BLOCK
    with pytest.raises(ProviderUnreachableError, match='not minted yet, retry shortly'):
        p.locate_transfer(BLOCK, 0)


def test_post_tx_relays_the_resolved_inner_hash_and_block():
    inner = _transfer_stake(0xB3, nonce=9)
    resv = SimpleNamespace(from_chain='sn7', user='taker')
    relay = MagicMock(return_value='deadbeef')
    provider = _alpha({BLOCK: {'extrinsics': [_shield(nonce=8), inner]}})

    def run(argv, provider=provider):
        with (
            patch.object(post_tx, 'get_solana_cli_context', return_value=({}, MagicMock())),
            patch.object(post_tx, 'load_pending_swap', return_value={}),
            patch.object(post_tx, '_find_reservations', return_value=[('minerpk', 'hotkey', resv)]),
            patch.object(post_tx, 'gate_provider', return_value=provider),
            patch.object(post_tx, 'relay_deposit', relay),
        ):
            return CliRunner().invoke(post_tx.post_tx_command, argv)

    assert run([f'{BLOCK}-0']).exit_code == 0
    assert relay.call_args.args[4:6] == (_hash(inner), BLOCK)
    assert run([f'{BLOCK}-0', '--block', '5']).exit_code == 0
    assert relay.call_args.args[4:6] == (_hash(inner), 5)  # an explicit --block wins
    relay.reset_mock()
    assert run([f'{BLOCK}-0'], provider=SimpleNamespace(chain=object())).exit_code == 1 and not relay.called
