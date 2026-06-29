"""alw status - Quick dashboard showing network, wallet, and swap state.

Stub: the pending-reservation/swap dashboard reads the on-chain
reservation pool (Solana); its Solana-backed re-port is pending (`alw swap now` origination is live)."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import taker_view_unavailable


@click.command('status', cls=StyledCommand)
def status_command():
    """Show a quick dashboard of your current state.

    [dim]Displays network info, wallet balance, active swaps,
    pending reservations, and miner status (if applicable).[/dim]

    [dim]Examples:
        $ alw status[/dim]
    """
    taker_view_unavailable('Swap status dashboard')
