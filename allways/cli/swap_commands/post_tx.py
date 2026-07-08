"""alw swap post-tx - Submit source transaction hash for a pending swap reservation.

Deferred: advancing a reservation with a source-tx hash requires verifying the deposit against the source
chain (a chain-provider check the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import not_implemented


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
    not_implemented('Swap post-tx (CLI fund relay)')
