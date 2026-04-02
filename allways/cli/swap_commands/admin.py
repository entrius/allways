"""alw admin - Contract administration commands (owner-only)."""

import rich_click as click

from allways.cli.swap_commands.helpers import SECONDS_PER_BLOCK, console, from_rao, get_cli_context, loading, to_rao
from allways.contract_client import ContractError


@click.group('admin')
def admin_group():
    """Contract administration commands (owner-only).

    \b
    Subcommands:
        set-timeout <blocks>            Set fulfillment timeout
        set-reservation-ttl <blocks>    Set reservation TTL
        set-fee-divisor <divisor>       Set fee divisor (100 = 1%, 50 = 2%)
        set-min-collateral <amount_tao> Set minimum collateral
        set-max-collateral <amount_tao> Set maximum collateral (0 = unlimited)
        set-min-swap <amount_tao>       Set minimum swap amount (0 = no minimum)
        set-max-swap <amount_tao>       Set maximum swap amount (0 = no maximum)
        set-votes <count>               Set required validator votes
        add-vali <hotkey>               Add a validator
        remove-vali <hotkey>            Remove a validator
        recycle-fees                    Stake accumulated fees on-chain via chain extension
        transfer-ownership <account_id> Transfer contract ownership
        danger halt                     Halt the system (block new reservations)
        danger resume                   Resume the system (allow new reservations)
    """
    pass


@admin_group.command('set-timeout')
@click.argument('blocks', type=int)
def set_timeout(blocks: int):
    """Set the fulfillment timeout in blocks (minimum 10).

    Example:
        alw admin set-timeout 300
    """
    if blocks < 10:
        console.print('[red]Blocks must be >= 10 (contract minimum)[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_fulfillment_timeout()
    except ContractError as e:
        console.print(f'[red]Failed to read fulfillment timeout: {e}[/red]')
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
        console.print(f'[red]Failed to set fulfillment timeout: {e}[/red]\n')


@admin_group.command('set-reservation-ttl')
@click.argument('blocks', type=int)
def set_reservation_ttl(blocks: int):
    """Set the reservation TTL in blocks (how long a user has to send funds).

    Example:
        alw admin set-reservation-ttl 50
    """
    if blocks <= 0:
        console.print('[red]Blocks must be positive[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_reservation_ttl()
    except ContractError as e:
        console.print(f'[red]Failed to read reservation TTL: {e}[/red]')
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
        console.print(f'[red]Failed to set reservation TTL: {e}[/red]\n')


@admin_group.command('set-fee-divisor')
@click.argument('divisor', type=int)
def set_fee_divisor(divisor: int):
    """Set the fee divisor (100 = 1% fee, 50 = 2% fee, 20 = 5% fee max).

    Example:
        alw admin set-fee-divisor 100
    """
    if divisor < 20:
        console.print('[red]Divisor must be at least 20 (max 5% fee)[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_fee_divisor()
    except ContractError as e:
        console.print(f'[red]Failed to read fee divisor: {e}[/red]')
        return

    current_pct = 100 / current if current > 0 else 0
    new_pct = 100 / divisor

    console.print('\n[bold]Set Fee Divisor[/bold]\n')
    console.print(f'  Current: {current} ({current_pct:g}% fee)')
    console.print(f'  New:     {divisor} ({new_pct:g}% fee)\n')

    if not click.confirm('Confirm updating fee divisor?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_fee_divisor(wallet=wallet, divisor=divisor)
        console.print(f'[green]Fee divisor set to {divisor} ({new_pct:g}% fee)[/green]\n')
    except ContractError as e:
        console.print(f'[red]Failed to set fee divisor: {e}[/red]\n')


@admin_group.command('set-min-collateral')
@click.argument('amount_tao', type=float)
def set_min_collateral(amount_tao: float):
    """Set the minimum collateral amount (in TAO).

    Example:
        alw admin set-min-collateral 2.0
    """
    if amount_tao <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_min_collateral()
    except ContractError as e:
        console.print(f'[red]Failed to read min collateral: {e}[/red]')
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
        console.print(f'[red]Failed to set minimum collateral: {e}[/red]\n')


@admin_group.command('set-max-collateral')
@click.argument('amount_tao', type=float)
def set_max_collateral(amount_tao: float):
    """Set the maximum collateral amount (in TAO). Use 0 to remove the cap.

    Example:
        alw admin set-max-collateral 100.0
        alw admin set-max-collateral 0
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_max_collateral()
    except ContractError as e:
        console.print(f'[red]Failed to read max collateral: {e}[/red]')
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
        console.print(f'[red]Failed to set maximum collateral: {e}[/red]\n')


@admin_group.command('set-min-swap')
@click.argument('amount_tao', type=float)
def set_min_swap(amount_tao: float):
    """Set the minimum swap amount in TAO. Use 0 to remove the minimum.

    Example:
        alw admin set-min-swap 1.0
        alw admin set-min-swap 0
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_min_swap_amount()
    except ContractError as e:
        console.print(f'[red]Failed to read min swap amount: {e}[/red]')
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
        console.print(f'[red]Failed to set minimum swap amount: {e}[/red]\n')


@admin_group.command('set-max-swap')
@click.argument('amount_tao', type=float)
def set_max_swap(amount_tao: float):
    """Set the maximum swap amount in TAO. Use 0 to remove the maximum.

    Example:
        alw admin set-max-swap 50.0
        alw admin set-max-swap 0
    """
    if amount_tao < 0:
        console.print('[red]Amount must be non-negative[/red]')
        return

    amount_rao = to_rao(amount_tao)

    _, wallet, _, client = get_cli_context()

    try:
        current_rao = client.get_max_swap_amount()
    except ContractError as e:
        console.print(f'[red]Failed to read max swap amount: {e}[/red]')
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
        console.print(f'[red]Failed to set maximum swap amount: {e}[/red]\n')


@admin_group.command('set-threshold')
@click.argument('percent', type=int)
def set_threshold(percent: int):
    """Set the consensus threshold percentage (1-100).

    Example:
        alw admin set-threshold 67
    """
    if percent <= 0 or percent > 100:
        console.print('[red]Threshold must be 1-100[/red]')
        return

    _, wallet, _, client = get_cli_context()

    try:
        current = client.get_consensus_threshold()
    except ContractError as e:
        console.print(f'[red]Failed to read threshold: {e}[/red]')
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
        console.print(f'[red]Failed to set consensus threshold: {e}[/red]\n')


@admin_group.command('add-vali')
@click.argument('hotkey', type=str)
def add_vali(hotkey: str):
    """Add a validator to the contract.

    Example:
        alw admin add-vali 5Cxyz...
    """
    _, wallet, _, client = get_cli_context()

    try:
        already_registered = client.is_validator(hotkey)
    except ContractError as e:
        console.print(f'[red]Failed to check validator status: {e}[/red]')
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
        console.print(f'[red]Failed to add validator: {e}[/red]\n')


@admin_group.command('remove-vali')
@click.argument('hotkey', type=str)
def remove_vali(hotkey: str):
    """Remove a validator from the contract.

    Example:
        alw admin remove-vali 5Cxyz...
    """
    _, wallet, _, client = get_cli_context()

    try:
        is_registered = client.is_validator(hotkey)
    except ContractError as e:
        console.print(f'[red]Failed to check validator status: {e}[/red]')
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
        console.print(f'[red]Failed to remove validator: {e}[/red]\n')


@admin_group.command('recycle-fees')
def recycle_fees():
    """Stake accumulated fees on-chain via chain extension.

    Example:
        alw admin recycle-fees
    """
    _, wallet, _, client = get_cli_context()

    try:
        accumulated = client.get_accumulated_fees()
    except ContractError:
        accumulated = None

    if accumulated is not None and accumulated == 0:
        console.print('\n[yellow]No accumulated fees to recycle[/yellow]\n')
        return

    fee_display = f'{from_rao(accumulated):.4f} TAO' if accumulated else 'unknown'
    console.print('\n[bold]Recycle Fees[/bold]\n')
    console.print(f'  Accumulated fees: {fee_display}')
    console.print('  Action: stake fees on-chain via chain extension\n')

    if not click.confirm('Confirm recycling fees?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.recycle_fees(wallet=wallet)
        console.print('[green]Fees recycled successfully[/green]\n')
    except ContractError as e:
        console.print(f'[red]Failed to recycle fees: {e}[/red]')
        if 'ContractReverted' in str(e):
            console.print('[dim]Hint: treasury hotkey may not be registered on the subnet[/dim]')
        console.print()


@admin_group.command('transfer-ownership')
@click.argument('account_id', type=str)
def transfer_ownership(account_id: str):
    """Transfer contract ownership to a new account. This is irreversible.

    Example:
        alw admin transfer-ownership 5Cxyz...
    """
    _, wallet, _, client = get_cli_context()

    try:
        current_owner = client.get_owner()
    except ContractError as e:
        console.print(f'[red]Failed to read current owner: {e}[/red]')
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
        console.print(f'[red]Failed to transfer ownership: {e}[/red]\n')


@click.group('danger')
def danger_group():
    """Dangerous operations that affect system availability.

    \b
    Subcommands:
        halt      Halt the system (block new reservations)
        resume    Resume the system (allow new reservations)
    """
    pass


@danger_group.command('halt')
def halt_system():
    """Halt the system — blocks all new swap reservations.

    Existing in-flight swaps will continue through their lifecycle.

    Example:
        alw admin danger halt
    """
    _, wallet, _, client = get_cli_context()

    try:
        already_halted = client.get_halted()
    except ContractError as e:
        console.print(f'[red]Failed to read halt status: {e}[/red]')
        return

    if already_halted:
        console.print('[yellow]System is already halted[/yellow]')
        return

    console.print('\n[bold red]Halt System[/bold red]\n')
    console.print('  This will block all new swap reservations.')
    console.print('  Existing swaps will continue to completion.\n')

    if not click.confirm('Confirm halting the system?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_halted(wallet=wallet, halted=True)
        console.print('[red]System is now halted — no new reservations[/red]\n')
    except ContractError as e:
        console.print(f'[red]Failed to halt system: {e}[/red]\n')


@danger_group.command('resume')
def resume_system():
    """Resume the system — allows new swap reservations again.

    Example:
        alw admin danger resume
    """
    _, wallet, _, client = get_cli_context()

    try:
        is_halted = client.get_halted()
    except ContractError as e:
        console.print(f'[red]Failed to read halt status: {e}[/red]')
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
        console.print(f'[red]Failed to resume system: {e}[/red]\n')


admin_group.add_command(danger_group)
