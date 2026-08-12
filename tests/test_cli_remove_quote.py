"""F7 — re-pricing a direction under a new backing orphans the old backing's quote.

`alw miner quotes` must WARN when a sibling quote on the OTHER purse is still live at its old price,
and `alw miner remove-quote` must give a miner a per-backing path to take one down (its sibling
stays). Without both, an abandoned quote keeps trading and a taker can reserve against it.
"""

import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands import miner_commands, numeraire


def _flat(result) -> str:
    return ' '.join(result.output.split())


def _quote(rate=500_000_000):
    return types.SimpleNamespace(rate=rate, updated_at=0, miner_from_addr='addr')


def test_quotes_warns_a_live_sibling_quote_on_the_other_backing():
    """Post SOL-backed earlier, then re-price the same direction TAO-backed: the SOL quote is still
    live and must be surfaced with the retract command."""
    client = MagicMock()
    client.keypair.pubkey.return_value = 'miner-pk'
    client.get_miner_state.return_value = MagicMock()

    def get_quote(_miner, from_chain, to_chain, backing):
        # Only the prior SOL-backed sol->tao quote exists; the TAO slots are new.
        if backing == 'sol' and (from_chain, to_chain) == ('sol', 'tao'):
            return _quote()
        return None

    client.get_quote.side_effect = get_quote
    with (
        patch.object(numeraire, 'get_cli_context', return_value=({}, MagicMock(), None, None)),
        patch.object(numeraire, 'get_solana_cli_context', return_value=({}, client)),
        patch.object(numeraire, 'resolve_quote_backing', return_value='tao'),
    ):
        result = CliRunner().invoke(
            numeraire.quotes_command,
            [
                '--sol-address',
                'SOLADDR',
                '--tao-price',
                '0.5',
                '--tao-address',
                'TAOADDR',
                '--backing',
                'tao',
                '--dry-run',
            ],
        )
    assert result.exit_code == 0, result.output
    out = _flat(result)
    assert 'sol-backed' in out and 'still' in out
    assert 'remove-quote --from sol --to tao --backing sol' in out


def _run_remove(argv, client, confirm_input=None):
    with patch.object(miner_commands, 'get_solana_cli_context', return_value=({}, client)):
        return CliRunner().invoke(miner_commands.miner_remove_quote, argv, input=confirm_input)


def test_remove_quote_retracts_only_the_named_backing():
    client = MagicMock()
    client.keypair.pubkey.return_value = 'miner-pk'
    client.get_quote.return_value = _quote()  # both backings live
    client.remove_quote.return_value = 'SIG' + '0' * 40
    result = _run_remove(['--from', 'sol', '--to', 'tao', '--backing', 'sol', '--yes'], client)
    assert result.exit_code == 0, result.output
    client.remove_quote.assert_called_once_with('sol', 'tao', backing='sol')
    assert 'removed' in _flat(result).lower()


def test_remove_quote_needs_backing_when_both_are_live():
    client = MagicMock()
    client.keypair.pubkey.return_value = 'miner-pk'
    client.get_quote.return_value = _quote()  # sol AND tao both live on sol<->tao
    result = _run_remove(['--from', 'sol', '--to', 'tao', '--yes'], client)
    assert result.exit_code != 0
    assert '--backing' in result.output
    client.remove_quote.assert_not_called()
