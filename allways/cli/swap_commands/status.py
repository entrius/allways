"""alw status - Quick dashboard showing network, wallet, and swap state.

Phase-9 stub: the pending-reservation/swap dashboard reads the on-chain
reservation pool (Solana); the taker CLI intake is not wired yet."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import phase9_unavailable


@click.command('status', cls=StyledCommand)
def status_command():
    """Show a quick dashboard of your current state.

    [dim]Displays network info, wallet balance, active swaps,
    pending reservations, and miner status (if applicable).[/dim]

    [dim]Examples:
        $ alw status[/dim]
    """
    phase9_unavailable('Swap status dashboard')
