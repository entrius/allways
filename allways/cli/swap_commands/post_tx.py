"""alw swap post-tx - Submit source transaction hash for a pending swap reservation.

Phase-9 stub: the reserve→deposit→confirm relay moves on-chain to Solana
(submit_swap_claim against an on-chain Reservation); the taker CLI intake is not
wired yet."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import phase9_unavailable


@click.command('post-tx', cls=StyledCommand, show_disclaimer=True)
@click.argument('tx_hash', required=False, default=None, type=str)
@click.option(
    '--block',
    'tx_block',
    type=int,
    default=0,
    help=(
        'Override the source-tx block number. Usually unnecessary — the CLI '
        'looks it up automatically across the whole reservation window. Use '
        'this only when automatic lookup fails (e.g. running against a node '
        'that has pruned block bodies, or the tx landed on a different node).'
    ),
)
def post_tx_command(tx_hash: str, tx_block: int):
    """Submit your source transaction hash for a pending swap reservation.

    [dim]Reads reservation context from ~/.allways/pending_swap.json (saved by `alw swap now`).[/dim]

    [dim]Examples:
        $ alw swap post-tx abc123def...
        $ alw swap post-tx abc123def... --block 12345   (escape hatch)
        $ alw swap post-tx  (prompts for tx hash)[/dim]
    """
    phase9_unavailable('Swap post-tx')
