"""alw claim - Claim a pending slash payout for a timed-out swap.

Stub: the slash/refund payout flow moves on-chain to Solana with the
reservation pool; its Solana-backed re-port is pending (`alw swap now` origination is live)."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import taker_view_unavailable


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
    taker_view_unavailable('Slash claim')
