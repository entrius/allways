"""B4.3 — miner-facing CLI repointed onto Solana (collateral / deactivate / mark-fulfilled / bind / post).

CliRunner drives each command with get_solana_cli_context / get_cli_context patched to a fake client. The
localnet checkpoint is the authoritative write-path gate; these lock the wiring (right builder, right units,
a verifying binding sig, rate scaled by RATE_PRECISION).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import bittensor as bt
from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands.collateral import collateral_group
from allways.cli.swap_commands.miner_commands import miner_group
from allways.cli.swap_commands.pair import post_pair
from allways.constants import RATE_PRECISION


def _config(**over):
    base = dict(
        min_collateral=1_000_000,
        max_collateral=10_000_000_000,
        fulfillment_timeout_secs=600,
        consensus_threshold_percent=51,
        halted=False,
    )
    base.update(over)
    return SimpleNamespace(**base)


def _state(**over):
    base = dict(
        active=False,
        has_active_swap=False,
        busy_until=0,
        deactivation_at=0,
        successful_swaps=0,
        failed_swaps=0,
    )
    base.update(over)
    return SimpleNamespace(**base)


def _client(**over):
    c = MagicMock()
    c.keypair = Keypair()
    c.get_config.return_value = over.pop('config', _config())
    c.get_collateral_lamports.return_value = over.pop('collateral', 0)
    c.get_miner_state.return_value = over.pop('state', _state())
    c.rpc.get_account_lamports.return_value = over.pop('free', 100_000_000_000)
    for k, v in over.items():
        getattr(c, k).return_value = v
    return c


def test_deposit_calls_post_collateral_with_lamports():
    c = _client()
    with patch('allways.cli.swap_commands.collateral.get_solana_cli_context', return_value=({}, c)):
        res = CliRunner().invoke(collateral_group, ['deposit', '--amount', '2', '--yes'])
    assert res.exit_code == 0, res.output
    c.post_collateral.assert_called_once_with(2_000_000_000)  # 2 SOL → lamports


def test_deposit_respects_max_collateral():
    c = _client(config=_config(max_collateral=1_000_000), collateral=900_000)
    with patch('allways.cli.swap_commands.collateral.get_solana_cli_context', return_value=({}, c)):
        res = CliRunner().invoke(collateral_group, ['deposit', '--amount', '5', '--yes'])
    assert res.exit_code != 0  # a rejected deposit must exit non-zero (script-safe)
    c.post_collateral.assert_not_called()
    assert 'exceed the max collateral' in res.output


def test_withdraw_blocked_while_active():
    c = _client(state=_state(active=True))
    with patch('allways.cli.swap_commands.collateral.get_solana_cli_context', return_value=({}, c)):
        res = CliRunner().invoke(collateral_group, ['withdraw', '--amount', '1', '--yes'])
    assert res.exit_code != 0  # a blocked withdrawal must exit non-zero (script-safe)
    c.withdraw_collateral.assert_not_called()
    assert 'while miner is active' in res.output


def test_withdraw_blocked_within_cooldown():
    # Inactive but deactivated 'now' with a long timeout → still in the 2× cooldown window.
    with patch('allways.cli.swap_commands.collateral.time') as t:
        t.time.return_value = 1_000_000
        c = _client(state=_state(active=False, deactivation_at=1_000_000), config=_config(fulfillment_timeout_secs=600))
        with patch('allways.cli.swap_commands.collateral.get_solana_cli_context', return_value=({}, c)):
            res = CliRunner().invoke(collateral_group, ['withdraw', '--amount', '1', '--yes'])
    assert res.exit_code != 0  # a blocked withdrawal must exit non-zero (script-safe)
    c.withdraw_collateral.assert_not_called()
    assert 'cooldown active' in res.output


def test_deactivate_calls_self_deactivate():
    with patch('allways.cli.swap_commands.miner_commands.time') as t:
        t.time.return_value = 5_000
        c = _client(state=_state(active=True))
        c.deactivate.return_value = 'SIG' * 10
        with patch('allways.cli.swap_commands.miner_commands.get_solana_cli_context', return_value=({}, c)):
            res = CliRunner().invoke(miner_group, ['deactivate'])
    assert res.exit_code == 0, res.output
    c.deactivate.assert_called_once_with()


def test_bind_hotkey_signs_and_binds():
    wallet = SimpleNamespace(hotkey=bt.Keypair.create_from_seed('0x' + '11' * 32))
    c = _client()
    c.get_binding.return_value = None
    c.bind_hotkey.return_value = 'SIG' * 10
    with (
        patch('allways.cli.swap_commands.miner_commands.get_cli_context', return_value=({}, wallet, None, None)),
        patch('allways.cli.swap_commands.miner_commands.get_solana_cli_context', return_value=({}, c)),
    ):
        res = CliRunner().invoke(miner_group, ['bind-hotkey', '--yes'])
    assert res.exit_code == 0, res.output
    c.bind_hotkey.assert_called_once()
    hotkey_bytes, sig = c.bind_hotkey.call_args.args
    # The submitted signature must verify against the hotkey over the Solana pubkey bytes.
    assert bt.Keypair(public_key='0x' + hotkey_bytes.hex()).verify(bytes(c.keypair.pubkey()), sig)


def test_bind_hotkey_skips_when_already_bound():
    wallet = SimpleNamespace(hotkey=bt.Keypair.create_from_seed('0x' + '22' * 32))
    c = _client()
    c.get_binding.return_value = object()  # already bound
    with (
        patch('allways.cli.swap_commands.miner_commands.get_cli_context', return_value=({}, wallet, None, None)),
        patch('allways.cli.swap_commands.miner_commands.get_solana_cli_context', return_value=({}, c)),
    ):
        res = CliRunner().invoke(miner_group, ['bind-hotkey', '--yes'])
    assert res.exit_code == 0
    c.bind_hotkey.assert_not_called()
    assert 'already bound' in res.output


def test_post_pair_calls_set_quote_with_scaled_rate():
    wallet = SimpleNamespace(hotkey=SimpleNamespace(ss58_address='5Fhotkey'))
    c = _client()
    with (
        patch('allways.cli.swap_commands.pair.get_cli_context', return_value=({}, wallet, None, None)),
        patch('allways.cli.swap_commands.pair.get_solana_cli_context', return_value=({}, c)),
        patch('allways.cli.swap_commands.pair.write_rate_posted_flag'),
    ):
        # btc bc1qsrc tao 5dst 345 (same rate both directions)
        res = CliRunner().invoke(post_pair, ['btc', 'bc1qsrc', 'tao', '5dst', '345', '--yes'])
    assert res.exit_code == 0, res.output
    # Two directions posted (rate + counter both 345), rate scaled by RATE_PRECISION.
    assert c.set_quote.call_count == 2
    first = c.set_quote.call_args_list[0].args
    assert first[4] == int(345 * RATE_PRECISION)
    # forward direction keeps src addr on from-leg, dst addr on to-leg
    assert (first[0], first[1], first[2], first[3]) == ('btc', 'tao', 'bc1qsrc', '5dst')
