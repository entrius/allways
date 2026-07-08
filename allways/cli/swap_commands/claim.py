"""alw claim - Claim a pending slash payout for a timed-out swap.

Deferred: the slash/refund payout needs source-chain verification of the failed swap (a chain-provider check
the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import not_implemented


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
    not_implemented('Slash claim (CLI fund relay)')
