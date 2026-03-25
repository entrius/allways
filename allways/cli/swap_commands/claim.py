"""alw claim - Claim a pending slash payout for a timed-out swap."""

import rich_click as click

from allways.cli.swap_commands.helpers import console, from_rao, get_cli_context, loading
from allways.contract_client import ContractError


@click.command('claim')
@click.argument('swap_id', type=int)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def claim_command(swap_id: int, yes: bool):
    """Claim a pending slash payout for a timed-out swap.

    \b
    If a miner failed to fulfill your swap before the timeout,
    you can claim a slash payout from their collateral.

    Example:
        alw claim 42
    """
    _, wallet, _, client = get_cli_context()

    console.print(f'\n[bold]Claim Slash — Swap #{swap_id}[/bold]\n')

    try:
        pending_rao = client.get_pending_slash(swap_id)
    except ContractError as e:
        console.print(f'[red]Failed to read pending slash: {e}[/red]')
        return

    if pending_rao == 0:
        console.print('[yellow]No pending slash for this swap[/yellow]\n')
        return

    console.print(f'  Swap ID:    {swap_id}')
    console.print(f'  Amount:     [green]{from_rao(pending_rao):.4f} TAO[/green] ({pending_rao} rao)')
    console.print(f'  Claiming:   {wallet.hotkey.ss58_address}\n')

    if not yes and not click.confirm('Confirm claiming slash?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.claim_slash(wallet=wallet, swap_id=swap_id)
        console.print(f'[green]Successfully claimed {from_rao(pending_rao):.4f} TAO from swap #{swap_id}![/green]\n')
    except ContractError as e:
        console.print(f'[red]Failed to claim slash: {e}[/red]\n')
