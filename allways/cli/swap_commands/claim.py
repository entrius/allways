"""alw claim - Claim a pending slash payout for a timed-out swap."""

import os

import click

from allways.classes import SwapStatus
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import console, from_rao, get_cli_context, loading, print_contract_error
from allways.cli.swap_commands.view import DEFAULT_DASHBOARD_URL
from allways.contract_client import ContractError


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
    _, wallet, _, client = get_cli_context()

    console.print(f'\n[bold]Claim Slash — Swap #{swap_id}[/bold]\n')

    try:
        pending_rao = client.get_pending_slash(swap_id)
    except ContractError as e:
        print_contract_error('Failed to read pending slash', e)
        return

    if pending_rao == 0:
        try:
            swap = client.get_swap(swap_id)
        except ContractError:
            swap = None

        if swap is not None and swap.status in (SwapStatus.ACTIVE, SwapStatus.FULFILLED):
            console.print(
                f'[yellow]Swap #{swap_id} is still in progress (status: {swap.status.name}).[/yellow]\n'
                '[dim]A slash is only created if the swap times out unfulfilled.[/dim]\n'
            )
            return

        dashboard_url = os.environ.get('ALLWAYS_DASHBOARD_URL', DEFAULT_DASHBOARD_URL).rstrip('/')
        console.print(
            f'[yellow]Nothing to claim for swap #{swap_id}.[/yellow]\n'
            '[dim]The slash was either paid directly to the user at timeout, already claimed,\n'
            'or the swap never timed out. Only the original swap user can claim a pending slash.\n'
            f'Refund history:[/dim] {dashboard_url}/swap/{swap_id}\n'
        )
        return

    console.print(f'  Swap ID:    {swap_id}')
    console.print(f'  Amount:     [green]{from_rao(pending_rao):.4f} TAO[/green] ({pending_rao} rao)')
    console.print(f'  Claiming:   {wallet.hotkey.ss58_address}')
    console.print('[dim]  Only the original swap user can claim; others will be rejected on-chain.[/dim]\n')

    if not yes and not click.confirm('Confirm claiming slash?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.claim_slash(wallet=wallet, swap_id=swap_id)
        console.print(f'[green]Successfully claimed {from_rao(pending_rao):.4f} TAO from swap #{swap_id}![/green]\n')
    except ContractError as e:
        print_contract_error('Failed to claim slash', e)
