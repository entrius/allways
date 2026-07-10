"""alw claim - Claim a pending slash payout for a timed-out swap.

Deferred: the slash/refund payout needs source-chain verification of the failed swap (a chain-provider check
the CLI taker path does not do yet). Exits non-zero; use the browser flow."""

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import not_implemented


@click.command('claim', cls=StyledCommand, show_disclaimer=True)
@click.argument('swap_key', type=str, required=False, default=None)
def claim_command(swap_key):
    """Claim a slash payout for a timed-out swap (not yet available from the CLI).

    [dim]When a miner fails to fulfill before the timeout, the user is owed a slash payout from the
    miner's collateral. Settling it needs source-chain verification the CLI taker path does not do
    yet — use the browser swap flow. Inspect a swap with `alw view swap <swap_key>`.[/dim]
    """
    not_implemented('Slash claim (CLI fund relay)')
