"""Unit tests for the SOL-numéraire quote derivation (one price per chain → all directions)."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands import numeraire
from allways.cli.swap_commands.numeraire import derive_hub_numeraire_quotes


def test_derives_both_directions_per_chain():
    specs = derive_hub_numeraire_quotes('sol', 'SOLADDR', {'btc': (0.002, 'BTCADDR'), 'tao': (0.5, 'TAOADDR')})
    pairs = {(s.from_chain, s.to_chain): s for s in specs}
    assert set(pairs) == {('sol', 'btc'), ('btc', 'sol'), ('sol', 'tao'), ('tao', 'sol')}
    # zero spread ⇒ both directions store the same canonical 'X per SOL' rate.
    assert pairs[('sol', 'btc')].rate == 0.002
    assert pairs[('btc', 'sol')].rate == 0.002


def test_addresses_oriented_per_direction():
    specs = derive_hub_numeraire_quotes('sol', 'SOLADDR', {'btc': (0.002, 'BTCADDR')})
    fwd = next(s for s in specs if s.from_chain == 'sol')
    rev = next(s for s in specs if s.to_chain == 'sol')
    assert (fwd.from_addr, fwd.to_addr) == ('SOLADDR', 'BTCADDR')
    assert (rev.from_addr, rev.to_addr) == ('BTCADDR', 'SOLADDR')


def test_spread_applies_symmetric_margin():
    # 100 bps: sol->X at price*0.99, X->sol at price*1.01.
    specs = derive_hub_numeraire_quotes('sol', 'S', {'btc': (1.0, 'B')}, spread_bps=100)
    fwd = next(s for s in specs if s.from_chain == 'sol')
    rev = next(s for s in specs if s.to_chain == 'sol')
    assert fwd.rate == 0.99
    assert rev.rate == 1.01


def test_skips_sol_and_nonpositive_prices():
    specs = derive_hub_numeraire_quotes('sol', 'S', {'sol': (1.0, 'S'), 'btc': (0.0, 'B'), 'tao': (-1.0, 'T')})
    assert specs == []


def test_alpha_price_reuses_tao_address_without_alpha_address_flag():
    client = MagicMock()
    client.keypair.pubkey.return_value = 'miner-pk'
    client.get_miner_state.return_value = MagicMock()
    client.get_quote.return_value = None
    wallet = MagicMock()
    with (
        patch.object(numeraire, 'get_cli_context', return_value=({}, wallet, None, None)),
        patch.object(numeraire, 'get_solana_cli_context', return_value=({}, client)),
        patch.object(numeraire, 'resolve_quote_backing', return_value='tao'),
        patch.object(numeraire, 'write_rate_posted_flag'),
    ):
        result = CliRunner().invoke(
            numeraire.quotes_command,
            ['--sol-address', 'SOLADDR', '--tao-address', 'TAOADDR', '--sn7-price', '2', '--yes'],
        )
    assert result.exit_code == 0, result.output
    posted_addresses = {(call.args[2], call.args[3]) for call in client.set_quote.call_args_list}
    assert posted_addresses == {('SOLADDR', 'TAOADDR'), ('TAOADDR', 'SOLADDR')}


def test_help_names_alpha_price_pattern_once_and_hides_the_flags():
    output = CliRunner().invoke(numeraire.quotes_command, ['--help']).output
    assert '--sn<N>-price' in output
    assert '--sn7-price' not in output and '--sn7-address' not in output


def test_alpha_price_requires_tao_address():
    result = CliRunner().invoke(numeraire.quotes_command, ['--sol-address', 'SOLADDR', '--sn7-price', '2', '--dry-run'])
    assert result.exit_code != 0
    assert '--tao-address required with --sn7-price' in result.output


def test_alpha_anchor_posts_the_alpha_against_every_priced_spoke():
    client = MagicMock()
    client.keypair.pubkey.return_value = 'miner-pk'
    client.get_quote.return_value = None
    with (
        patch.object(numeraire, 'get_cli_context', return_value=({}, MagicMock(), None, None)),
        patch.object(numeraire, 'get_solana_cli_context', return_value=({}, client)),
        patch.object(numeraire, 'resolve_quote_backing', return_value='tao'),
        patch.object(numeraire, 'write_rate_posted_flag'),
    ):
        result = CliRunner().invoke(
            numeraire.quotes_command,
            ['--hub', 'sn7', '--tao-address', 'TAOADDR', '--avax-price', '0.15', '--avax-address', 'AVAXADDR']
            + ['--btc-price', '1e-6', '--btc-address', 'BTCADDR', '--yes'],
        )
    assert result.exit_code == 0, result.output
    posted = {
        (c.args[0], c.args[1], c.args[2], c.args[3], c.kwargs['backing']) for c in client.set_quote.call_args_list
    }
    assert posted == {
        ('sn7', 'avax', 'TAOADDR', 'AVAXADDR', 'tao'),
        ('avax', 'sn7', 'AVAXADDR', 'TAOADDR', 'tao'),
        ('sn7', 'btc', 'TAOADDR', 'BTCADDR', 'tao'),
        ('btc', 'sn7', 'BTCADDR', 'TAOADDR', 'tao'),
    }


def test_alpha_anchor_refuses_pairs_it_does_not_anchor():
    for flags in (['--tao-price', '1'], ['--sn64-price', '1']):  # tao↔sn7 anchors on TAO; alpha↔alpha is no pair
        result = CliRunner().invoke(
            numeraire.quotes_command, ['--hub', 'sn7', '--tao-address', 'TAOADDR', *flags, '--dry-run']
        )
        assert result.exit_code != 0 and 'is not a sn7-anchored pair' in result.output


def test_unknown_anchor_is_refused():
    result = CliRunner().invoke(numeraire.quotes_command, ['--hub', 'btc', '--dry-run'])
    assert result.exit_code != 0 and '--hub must be one of' in result.output
