"""alw swap post-tx - Submit source transaction hash for a pending swap reservation.

Deferred: advancing a reservation with a source-tx hash requires verifying the deposit against the source
chain (a chain-provider check the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import not_implemented


@click.command('post-tx', cls=StyledCommand, show_disclaimer=True)
@click.argument('tx_hash', required=False, default=None, type=str)
def post_tx_command(tx_hash: str):
    """Submit your source transaction hash to advance a reservation (not yet available from the CLI).

    [dim]Relaying a deposit verifies your source transaction against the source chain, which the CLI
    taker path does not do yet — use the browser swap flow to complete a swap end-to-end.[/dim]
    """
    not_implemented('Swap post-tx (CLI fund relay)')
