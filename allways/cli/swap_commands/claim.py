"""alw claim - Claim a pending slash payout for a timed-out swap.

Phase-9 stub: the slash/refund payout flow moves on-chain to Solana with the
reservation pool; the taker CLI intake is not wired yet."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import phase9_unavailable


@click.command('claim', cls=StyledCommand, show_disclaimer=True)
@click.argument('swap_id', type=int)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def claim_command(swap_id: int, yes: bool):
    """Claim a pending slash payout for a timed-out swap.

    [dim]If a miner failed to fulfill your swap before the timeout,
    you can claim a slash payout from their collateral.
    Only the original swap user can claim.[/dim]

    [dim]Examples:
        $ alw claim 42[/dim]
    """
    phase9_unavailable('Slash claim')
