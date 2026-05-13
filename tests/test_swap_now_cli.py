from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import swap_now_command


def test_swap_now_accepts_uppercase_chain_options():
    client = MagicMock()
    client.get_halted.return_value = False

    with (
        patch(
            'allways.cli.swap_commands.swap.get_cli_context', return_value=({'netuid': 7}, object(), object(), client)
        ),
        patch('allways.cli.swap_commands.swap.load_pending_swap', return_value=None),
        patch('allways.cli.swap_commands.swap.create_chain_providers', return_value={}),
        patch('allways.cli.swap_commands.swap.read_miner_commitments', return_value=[]),
    ):
        result = CliRunner().invoke(
            swap_now_command,
            [
                '--from',
                'BTC',
                '--to',
                'TAO',
                '--amount',
                '0.001',
                '--receive-address',
                '5test',
                '--from-address',
                'bc1qtest',
                '--from-tx-hash',
                'abc123',
                '--auto',
                '--yes',
            ],
        )

    assert result.exit_code == 0
    assert 'Unknown source chain' not in result.output
    assert 'No miners currently post rates for BTC/TAO.' in result.output
