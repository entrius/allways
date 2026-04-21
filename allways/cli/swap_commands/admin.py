"""alw admin - Contract administration commands (owner-only)."""

import click

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    console,
    from_rao,
    get_cli_context,
    is_valid_ss58,
    loading,
    print_contract_error,
    to_rao,
)
from allways.contract_client import ContractError


@click.group('admin', cls=StyledGroup, show_disclaimer=True)
def admin_group():
    """Contract administration commands (owner-only)."""
    pass


@admin_group.command('set-timeout', show_disclaimer=True)
@click.argument('blocks', type=int)
def set_timeout(blocks: int):
    """Set the fulfillment timeout in blocks (minimum 10).

    [dim]Examples:
        $ alw admin set-timeout 300[/dim]
    """
    if blocks < 10:
        console.print('[red]Blocks must be >= 10 (contract minimum)[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_fulfillment_timeout()
    except ContractError as e:
        print_contract_error('Failed to read fulfillment timeout', e)
        return

    current_minutes = current * SECONDS_PER_BLOCK / 60
    new_minutes = blocks * SECONDS_PER_BLOCK / 60

    console.print('\n[bold]Set Fulfillment Timeout[/bold]\n')
    console.print(f'  Current: {current} blocks (~{current_minutes:.0f} min)')
    console.print(f'  New:     {blocks} blocks (~{new_minutes:.0f} min)\n')

    if not click.confirm('Confirm updating timeout?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_fulfillment_timeout(wallet=wallet, blocks=blocks)
        console.print(f'[green]Fulfillment timeout set to {blocks} blocks[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set fulfillment timeout', e)


@admin_group.command('set-reservation-ttl', show_disclaimer=True)
@click.argument('blocks', type=int)
def set_reservation_ttl(blocks: int):
    """Set the reservation TTL in blocks (how long a user has to send funds).

    [dim]Examples:
        $ alw admin set-reservation-ttl 50[/dim]
    """
    if blocks <= 0:
        console.print('[red]Blocks must be positive[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_reservation_ttl()
    except ContractError as e:
        print_contract_error('Failed to read reservation TTL', e)
        return

    current_minutes = current * SECONDS_PER_BLOCK / 60
    new_minutes = blocks * SECONDS_PER_BLOCK / 60

    console.print('\n[bold]Set Reservation TTL[/bold]\n')
    console.print(f'  Current: {current} blocks (~{current_minutes:.0f} min)')
    console.print(f'  New:     {blocks} blocks (~{new_minutes:.0f} min)\n')

    if not click.confirm('Confirm updating reservation TTL?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_reservation_ttl(wallet=wallet, blocks=blocks)
        console.print(f'[green]Reservation TTL set to {blocks} blocks[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set reservation TTL', e)


@admin_group.command('set-min-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def set_min_collateral(amount_tao: float):
    """Set the minimum collateral amount (in TAO).

    [dim]Examples:
        $ alw admin set-min-collateral 2.0[/dim]
    """
    if amount_tao <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_min_collateral()
        current_max_rao = client.get_max_collateral()
    except ContractError as e:
        print_contract_error('Failed to read collateral bounds', e)
        return

    # Max of 0 means "unlimited" — no upper bound to violate.
    if current_max_rao > 0 and amount_rao > current_max_rao:
        console.print(
            f'[red]New min collateral ({amount_tao:.4f} TAO) exceeds current max collateral '
            f'({from_rao(current_max_rao):.4f} TAO). Raise max first or pick a smaller min.[/red]'
        )
        return

    console.print('\n[bold]Set Minimum Collateral[/bold]\n')
    console.print(f'  Current: {from_rao(current_rao):.4f} TAO')
    console.print(f'  New:     {amount_tao:.4f} TAO\n')

    if not click.confirm('Confirm updating minimum collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_min_collateral_amount(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Minimum collateral set to {amount_tao:.4f} TAO[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set minimum collateral', e)


@admin_group.command('set-max-collateral', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def set_max_collateral(amount_tao: float):
    """Set the maximum collateral amount (in TAO). Use 0 to remove the cap.

    [dim]Examples:
        $ alw admin set-max-collateral 100.0
        $ alw admin set-max-collateral 0[/dim]
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_max_collateral()
        current_min_rao = client.get_min_collateral()
    except ContractError as e:
        print_contract_error('Failed to read collateral bounds', e)
        return

    # amount_rao == 0 means "unlimited" — no upper bound to check.
    if amount_rao > 0 and current_min_rao > 0 and amount_rao < current_min_rao:
        console.print(
            f'[red]New max collateral ({amount_tao:.4f} TAO) is below current min collateral '
            f'({from_rao(current_min_rao):.4f} TAO). Lower min first or pick a larger max.[/red]'
        )
        return

    console.print('\n[bold]Set Maximum Collateral[/bold]\n')
    console.print(f'  Current: {from_rao(current_rao):.4f} TAO{" (unlimited)" if current_rao == 0 else ""}')
    console.print(f'  New:     {amount_tao:.4f} TAO{" (unlimited)" if amount_rao == 0 else ""}\n')

    if not click.confirm('Confirm updating maximum collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_max_collateral_amount(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Maximum collateral set to {amount_tao:.4f} TAO[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set maximum collateral', e)


@admin_group.command('set-min-swap', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def set_min_swap(amount_tao: float):
    """Set the minimum swap amount in TAO. Use 0 to remove the minimum.

    [dim]Examples:
        $ alw admin set-min-swap 1.0
        $ alw admin set-min-swap 0[/dim]
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_min_swap_amount()
        current_max_rao = client.get_max_swap_amount()
    except ContractError as e:
        print_contract_error('Failed to read swap bounds', e)
        return

    # Max of 0 means "unlimited" — no upper bound to violate.
    if current_max_rao > 0 and amount_rao > current_max_rao:
        console.print(
            f'[red]New min swap ({amount_tao:.4f} TAO) exceeds current max swap '
            f'({from_rao(current_max_rao):.4f} TAO). Raise max first or pick a smaller min.[/red]'
        )
        return

    console.print('\n[bold]Set Minimum Swap Amount[/bold]\n')
    console.print(f'  Current: {from_rao(current_rao):.4f} TAO{" (no minimum)" if current_rao == 0 else ""}')
    console.print(f'  New:     {amount_tao:.4f} TAO{" (no minimum)" if amount_rao == 0 else ""}\n')

    if not click.confirm('Confirm updating minimum swap amount?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_min_swap_amount(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Minimum swap amount set to {amount_tao:.4f} TAO[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set minimum swap amount', e)


@admin_group.command('set-max-swap', show_disclaimer=True)
@click.argument('amount_tao', type=float)
def set_max_swap(amount_tao: float):
    """Set the maximum swap amount in TAO. Use 0 to remove the maximum.

    [dim]Examples:
        $ alw admin set-max-swap 50.0
        $ alw admin set-max-swap 0[/dim]
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_max_swap_amount()
        current_min_rao = client.get_min_swap_amount()
    except ContractError as e:
        print_contract_error('Failed to read swap bounds', e)
        return

    # amount_rao == 0 means "unlimited" — no upper bound to check.
    if amount_rao > 0 and current_min_rao > 0 and amount_rao < current_min_rao:
        console.print(
            f'[red]New max swap ({amount_tao:.4f} TAO) is below current min swap '
            f'({from_rao(current_min_rao):.4f} TAO). Lower min first or pick a larger max.[/red]'
        )
        return

    console.print('\n[bold]Set Maximum Swap Amount[/bold]\n')
    console.print(f'  Current: {from_rao(current_rao):.4f} TAO{" (no maximum)" if current_rao == 0 else ""}')
    console.print(f'  New:     {amount_tao:.4f} TAO{" (no maximum)" if amount_rao == 0 else ""}\n')

    if not click.confirm('Confirm updating maximum swap amount?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_max_swap_amount(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Maximum swap amount set to {amount_tao:.4f} TAO[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set maximum swap amount', e)


@admin_group.command('set-threshold', show_disclaimer=True)
@click.argument('percent', type=int)
def set_threshold(percent: int):
    """Set the consensus threshold percentage (1-100).

    [dim]Examples:
        $ alw admin set-threshold 67[/dim]
    """
    if percent <= 0 or percent > 100:
        console.print('[red]Threshold must be 1-100[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_consensus_threshold()
    except ContractError as e:
        print_contract_error('Failed to read threshold', e)
        return

    console.print('\n[bold]Set Consensus Threshold[/bold]\n')
    console.print(f'  Current: {current}%')
    console.print(f'  New:     {percent}%\n')

    if not click.confirm('Confirm updating consensus threshold?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_consensus_threshold(wallet=wallet, percent=percent)
        console.print(f'[green]Consensus threshold set to {percent}%[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set consensus threshold', e)


@admin_group.command('add-vali', show_disclaimer=True)
@click.argument('hotkey', type=str)
def add_vali(hotkey: str):
    """Add a validator to the contract.

    [dim]Examples:
        $ alw admin add-vali 5Cxyz...[/dim]
    """
    _, wallet, _, client = get_cli_context()

    try:
        already_registered = client.is_validator(hotkey)
    except ContractError as e:
        print_contract_error('Failed to check validator status', e)
        return

    console.print('\n[bold]Add Validator[/bold]\n')
    console.print(f'  Hotkey: {hotkey}')

    if already_registered:
        console.print('  [yellow]Warning: This hotkey is already a registered validator[/yellow]')

    console.print()

    if not click.confirm('Confirm adding validator?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.add_validator(wallet=wallet, validator=hotkey)
        console.print(f'[green]Validator {hotkey} added[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to add validator', e)


@admin_group.command('remove-vali', show_disclaimer=True)
@click.argument('hotkey', type=str)
def remove_vali(hotkey: str):
    """Remove a validator from the contract.

    [dim]Examples:
        $ alw admin remove-vali 5Cxyz...[/dim]
    """
    _, wallet, _, client = get_cli_context()

    try:
        is_registered = client.is_validator(hotkey)
    except ContractError as e:
        print_contract_error('Failed to check validator status', e)
        return

    console.print('\n[bold]Remove Validator[/bold]\n')
    console.print(f'  Hotkey: {hotkey}')

    if not is_registered:
        console.print('  [yellow]Warning: This hotkey is not a registered validator[/yellow]')

    console.print()

    if not click.confirm('Confirm removing validator?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.remove_validator(wallet=wallet, validator=hotkey)
        console.print(f'[green]Validator {hotkey} removed[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to remove validator', e)


@admin_group.command('set-recycle-address', show_disclaimer=True)
@click.argument('account_id', type=str)
def set_recycle_address(account_id: str):
    """Set the address where recycled fees are transferred.

    [dim]Examples:
        $ alw admin set-recycle-address 5Cxyz...[/dim]
    """
    if not is_valid_ss58(account_id):
        console.print('[red]Not a valid SS58 address[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_recycle_address()
    except ContractError:
        current = ''

    console.print('\n[bold]Set Recycle Address[/bold]\n')
    if current:
        console.print(f'  Current: {current}')
    console.print(f'  New:     {account_id}\n')

    if current == account_id:
        console.print('[yellow]This address is already set. Nothing to do.[/yellow]')
        return

    if not click.confirm('Confirm?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_recycle_address(wallet=wallet, address=account_id)
        console.print(f'[green]Recycle address set to {account_id}[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to set recycle address', e)


@admin_group.command('recycle-fees', show_disclaimer=True)
def recycle_fees():
    """Transfer accumulated fees to the designated recycle address.

    [dim]Examples:
        $ alw admin recycle-fees[/dim]
    """
    _, wallet, _, client = get_cli_context()

    try:
        accumulated = client.get_accumulated_fees()
        destination = client.get_recycle_address()
        total_recycled = client.get_total_recycled_fees()
    except ContractError as e:
        print_contract_error('Failed to read fee state', e)
        return

    console.print('\n[bold]Recycle Fees[/bold]\n')
    console.print(f'  Amount:       {from_rao(accumulated):.6f} TAO')
    console.print(f'  Destination:  {destination}')
    console.print(f'  Total recycled so far: {from_rao(total_recycled):.6f} TAO\n')

    if accumulated == 0:
        console.print('[yellow]No fees to recycle.[/yellow]')
        return

    if not click.confirm('Confirm recycling fees?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.recycle_fees(wallet=wallet)
        console.print(f'[green]Recycled {from_rao(accumulated):.6f} TAO to {destination}[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to recycle fees', e)


@admin_group.command('transfer-ownership', show_disclaimer=True)
@click.argument('account_id', type=str)
def transfer_ownership(account_id: str):
    """Transfer contract ownership to a new account. This is irreversible.

    [dim]Examples:
        $ alw admin transfer-ownership 5Cxyz...[/dim]
    """
    if not is_valid_ss58(account_id):
        console.print('[red]Not a valid SS58 address[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current_owner = client.get_owner()
    except ContractError as e:
        print_contract_error('Failed to read current owner', e)
        return

    if current_owner == account_id:
        console.print('[yellow]This account is already the owner. Nothing to do.[/yellow]')
        return

    console.print('\n[bold red]Transfer Ownership[/bold red]\n')
    console.print(f'  Current owner: {current_owner}')
    console.print(f'  New owner:     {account_id}')
    console.print('\n  [bold red]WARNING: This action is irreversible![/bold red]\n')

    confirmation = click.prompt('Type "TRANSFER" to confirm')
    if confirmation != 'TRANSFER':
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.transfer_ownership(wallet=wallet, new_owner=account_id)
        console.print(f'[green]Ownership transferred to {account_id}[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to transfer ownership', e)


@click.group('danger', cls=StyledGroup, show_disclaimer=True)
def danger_group():
    """Dangerous operations that affect system availability."""
    pass


@danger_group.command('halt', show_disclaimer=True)
def halt_system():
    """Halt the system — blocks all new swap reservations.

    [dim]Existing in-flight swaps will continue through their lifecycle.[/dim]

    [dim]Examples:
        $ alw admin danger halt[/dim]
    """
    _, wallet, _, client = get_cli_context()

    try:
        already_halted = client.get_halted()
    except ContractError as e:
        print_contract_error('Failed to read halt status', e)
        return

    if already_halted:
        console.print('[yellow]System is already halted[/yellow]')
        return

    try:
        active_swaps = client.get_active_swaps()
    except ContractError:
        active_swaps = []

    console.print('\n[bold red]Halt System[/bold red]\n')
    console.print('  This will block all new swap reservations.')
    console.print('  Existing swaps will continue to completion.\n')
    console.print(f'  In-flight swaps that will be unaffected: {len(active_swaps)}\n')

    if not click.confirm('Confirm halting the system?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_halted(wallet=wallet, halted=True)
        console.print('[red]System is now halted — no new reservations[/red]\n')
    except ContractError as e:
        print_contract_error('Failed to halt system', e)


@danger_group.command('resume', show_disclaimer=True)
def resume_system():
    """Resume the system — allows new swap reservations again.

    [dim]Examples:
        $ alw admin danger resume[/dim]
    """
    _, wallet, _, client = get_cli_context()

    try:
        is_halted = client.get_halted()
    except ContractError as e:
        print_contract_error('Failed to read halt status', e)
        return

    if not is_halted:
        console.print('[yellow]System is not halted[/yellow]')
        return

    console.print('\n[bold]Resume System[/bold]\n')
    console.print('  This will allow new swap reservations again.\n')

    if not click.confirm('Confirm resuming the system?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_halted(wallet=wallet, halted=False)
        console.print('[green]System resumed — new reservations are now allowed[/green]\n')
    except ContractError as e:
        print_contract_error('Failed to resume system', e)


admin_group.add_command(danger_group)
