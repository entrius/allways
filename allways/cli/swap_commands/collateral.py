"""alw collateral - Manage miner collateral on the smart contract."""

import click
from rich.table import Table

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    blocks_to_minutes_str,
    console,
    from_rao,
    get_cli_context,
    is_valid_ss58,
    loading,
    print_contract_error,
    to_rao,
)
from allways.constants import MIN_BALANCE_FOR_TX_RAO, MIN_COLLATERAL_TAO
from allways.contract_client import ContractError, is_contract_rejection

try:
    from async_substrate_interface.errors import ExtrinsicNotFound
except ImportError:  # pragma: no cover - dependency always present in practice
    ExtrinsicNotFound = ()


@click.group('collateral', cls=StyledGroup, show_disclaimer=True)
def collateral_group():
    """Manage miner collateral."""
    pass


@collateral_group.command('deposit', show_disclaimer=True)
@click.option('--amount', default=None, type=float, help='Amount in TAO')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_deposit(amount: float | None, yes: bool):
    """Deposit collateral to the swap contract.

    [dim]Amount is in TAO. Minimum collateral to be active: see MIN_COLLATERAL_TAO.[/dim]

    [dim]Examples:
        $ alw collateral deposit --amount 5.0
        $ alw collateral deposit  (prompts interactively)[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to deposit (TAO)', type=float)

    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    amount_rao = to_rao(amount)

    _, wallet, _, client = get_cli_context()

    console.print('\n[bold]Depositing Collateral[/bold]\n')
    console.print(f'  Amount:  [green]{amount} TAO[/green] ({amount_rao} rao)')
    console.print(f'  Wallet:  {wallet.name}')
    console.print(f'  Hotkey:  {wallet.hotkey.ss58_address}')
    console.print('  [dim]Funds are debited from the hotkey balance (not the coldkey).[/dim]\n')

    try:
        max_collateral_rao = client.get_max_collateral()
        if max_collateral_rao > 0:
            current_collateral_rao = client.get_miner_collateral(wallet.hotkey.ss58_address)
            new_total_rao = current_collateral_rao + amount_rao
            if new_total_rao > max_collateral_rao:
                console.print(
                    f'[red]This would exceed the max collateral limit ({from_rao(max_collateral_rao):.4f} TAO). '
                    f'Current: {from_rao(current_collateral_rao):.4f} TAO, posting: {amount} TAO.[/red]'
                )
                return

        account_info = client.subtensor.substrate.query('System', 'Account', [wallet.hotkey.ss58_address])
        account_data = account_info.value if hasattr(account_info, 'value') else account_info
        free_balance = account_data.get('data', {}).get('free', 0)
        required = amount_rao + MIN_BALANCE_FOR_TX_RAO
        if free_balance < required:
            console.print(
                f'[red]Insufficient hotkey balance. Free: {from_rao(free_balance):.4f} TAO, '
                f'need: {from_rao(required):.4f} TAO '
                f'(amount + {from_rao(MIN_BALANCE_FOR_TX_RAO):.2f} TAO gas buffer, pre-checked so the tx does not '
                'fail on chain and waste fees).[/red]'
            )
            console.print('[dim]Collateral is posted from the hotkey, not the coldkey.[/dim]')
            console.print(
                f'[dim]Transfer TAO with: btcli wallet transfer --destination {wallet.hotkey.ss58_address} '
                '--amount <tao>[/dim]'
            )
            return
    except ContractError as e:
        # Contract rejection on a read means the contract told us this
        # deposit is invalid (e.g. ExceedsMaxCollateral) — abort. A plain
        # RPC failure is transient, so we warn and continue.
        if is_contract_rejection(e):
            print_contract_error('Pre-flight check rejected deposit', e)
            return
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')
    except Exception as e:
        console.print(f'[yellow]Warning: balance check failed ({e}), proceeding anyway[/yellow]')

    if not yes and not click.confirm('Confirm depositing collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.post_collateral(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Successfully deposited {amount} TAO collateral![/green]')
    except ContractError as e:
        print_contract_error('Failed to deposit collateral', e)


@collateral_group.command('withdraw', show_disclaimer=True)
@click.option('--amount', default=None, type=float, help='Amount in TAO')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_withdraw(amount: float | None, yes: bool):
    """Withdraw collateral from the swap contract.

    [dim]Amount is in TAO. Cannot withdraw if you have active swaps.[/dim]

    [dim]Examples:
        $ alw collateral withdraw --amount 2.0
        $ alw collateral withdraw  (prompts interactively)[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to withdraw (TAO)', type=float)

    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    amount_rao = to_rao(amount)

    _, wallet, subtensor, client = get_cli_context()

    console.print('\n[bold]Withdrawing Collateral[/bold]\n')
    console.print(f'  Amount:  [yellow]{amount} TAO[/yellow] ({amount_rao} rao)')
    console.print(f'  Wallet:  {wallet.name}')
    console.print(f'  Hotkey:  {wallet.hotkey.ss58_address}\n')

    try:
        hotkey = wallet.hotkey.ss58_address
        current_block = subtensor.get_current_block()

        # Single composite read instead of five separate RPCs.
        current_collateral_rao, is_active, has_active_swap, reserved_until, deactivation_block = (
            client.get_miner_snapshot(hotkey)
        )

        if is_active:
            console.print('[red]Cannot withdraw while miner is active. Run `alw miner deactivate` first.[/red]')
            return

        if deactivation_block > 0:
            timeout_blocks = client.get_fulfillment_timeout()
            cooldown_end = deactivation_block + (timeout_blocks * 2)
            if current_block < cooldown_end:
                remaining = cooldown_end - current_block
                console.print(
                    f'[red]Withdrawal cooldown active. ~{remaining} blocks ({blocks_to_minutes_str(remaining)}) remaining.[/red]'
                )
                return

        if reserved_until >= current_block:
            console.print('[red]Cannot withdraw while miner is reserved for a swap.[/red]')
            return

        if has_active_swap:
            console.print('[red]Cannot withdraw while miner has an active swap.[/red]')
            return

        if amount_rao > current_collateral_rao:
            console.print(
                f'[red]Insufficient collateral. Current: {from_rao(current_collateral_rao):.4f} TAO, '
                f'requested: {amount} TAO.[/red]'
            )
            return
    except ContractError as e:
        if is_contract_rejection(e):
            print_contract_error('Pre-flight check rejected withdrawal', e)
            return
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')
    except Exception as e:
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')

    if not yes and not click.confirm('Confirm withdrawing collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.withdraw_collateral(wallet=wallet, amount_rao=amount_rao)
        console.print(f'[green]Successfully withdrew {amount} TAO collateral![/green]')
    except ContractError as e:
        print_contract_error('Failed to withdraw collateral', e)


@collateral_group.command('recover-from-hotkey', show_disclaimer=True)
@click.option('--dest', default=None, help='Destination ss58 (default: your coldkey)')
@click.option('--amount', default=None, type=float, help='Amount in TAO to recover (default: sweep all free balance)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_recover_from_hotkey(dest: str | None, amount: float | None, yes: bool):
    """Move a hotkey's free TAO balance back to your coldkey.

    [dim]Collateral is deposited from — and withdrawn back to — the hotkey, so leftover
    free balance can strand there. `btcli wallet transfer` always signs with the coldkey
    and cannot move it; this signs with the hotkey, the only key that can spend it.

    This is NOT unstaking. To move STAKED tao use `btcli stake remove`.[/dim]

    [dim]Examples:
        $ alw collateral recover-from-hotkey                  (sweep all to your coldkey)
        $ alw collateral recover-from-hotkey --amount 0.5
        $ alw collateral recover-from-hotkey --dest 5C...[/dim]
    """
    _, wallet, subtensor, _ = get_cli_context(need_client=False)

    if dest is None:
        dest = wallet.coldkeypub.ss58_address
    elif not is_valid_ss58(dest):
        console.print(f'[red]Invalid destination ss58 address: {dest}[/red]')
        return

    keypair = wallet.hotkey
    src = keypair.ss58_address

    try:
        account_info = subtensor.substrate.query('System', 'Account', [src])
        account_data = account_info.value if hasattr(account_info, 'value') else account_info
        free_rao = int(account_data.get('data', {}).get('free', 0))
    except Exception as e:
        console.print(f'[red]Failed to read hotkey balance: {e}[/red]')
        return

    sweep = amount is None
    amount_rao = 0
    if sweep:
        action = 'Sweep entire free balance (minus tx fee)'
    else:
        if amount <= 0:
            console.print('[red]Amount must be positive[/red]')
            return
        amount_rao = to_rao(amount)
        required = amount_rao + MIN_BALANCE_FOR_TX_RAO
        if required > free_rao:
            console.print(
                f'[red]Insufficient hotkey balance. Free: {from_rao(free_rao):.4f} TAO, '
                f'need: {from_rao(required):.4f} TAO (amount + {from_rao(MIN_BALANCE_FOR_TX_RAO):.2f} TAO gas buffer). '
                f'Omit --amount to sweep everything.[/red]'
            )
            return
        action = f'Transfer {amount} TAO'

    console.print('\n[bold]Recovering Hotkey Balance[/bold]\n')
    console.print(f'  Source hotkey:  {src}')
    console.print(f'  Free balance:   [green]{from_rao(free_rao):.9f} TAO[/green] ({free_rao} rao)')
    console.print(f'  Destination:    {dest}')
    console.print(f'  Action:         {action}\n')

    if free_rao <= 0:
        console.print('[yellow]Nothing to recover — hotkey free balance is zero.[/yellow]')
        return

    if not yes and not click.confirm('Confirm recovering hotkey balance?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        substrate = subtensor.substrate
        if sweep:
            call = substrate.compose_call(
                call_module='Balances',
                call_function='transfer_all',
                call_params={'dest': dest, 'keep_alive': False},
            )
        else:
            call = substrate.compose_call(
                call_module='Balances',
                call_function='transfer_keep_alive',
                call_params={'dest': dest, 'value': amount_rao},
            )
        extrinsic = substrate.create_signed_extrinsic(call=call, keypair=keypair)
        with loading('Submitting transaction...'):
            receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
    except Exception as e:
        console.print(f'[red]Failed to submit transfer: {e}[/red]')
        return

    try:
        succeeded = receipt.is_success
    except ExtrinsicNotFound:
        # Included but the post-inclusion event lookup raced — the transfer
        # most likely landed. Don't claim failure; point at the balance view.
        console.print(
            '[yellow]Submitted, but could not confirm the result from chain events. '
            'Check the destination balance with `alw collateral view` or a block explorer.[/yellow]'
        )
        return

    if succeeded:
        console.print(f'[green]Recovered hotkey balance to {dest}.[/green]')
        console.print(f'[dim]Block hash: {receipt.block_hash}[/dim]')
    else:
        console.print(f'[red]Transfer failed: {receipt.error_message}[/red]')


@collateral_group.command('view')
@click.option('--hotkey', default=None, help='Hotkey to check (default: your hotkey)')
def collateral_view(hotkey: str):
    """View collateral balance.

    [dim]Examples:
        $ alw collateral view
        $ alw collateral view --hotkey 5Cxyz...[/dim]
    """
    _, wallet, _, client = get_cli_context()

    if not hotkey:
        hotkey = wallet.hotkey.ss58_address

    try:
        with loading('Reading collateral...'):
            collateral_rao = client.get_miner_collateral(hotkey)
            is_active = client.get_miner_active_flag(hotkey)
    except ContractError as e:
        print_contract_error('Failed to read collateral', e)
        return

    console.print('\n[bold]Collateral Status[/bold]\n')

    table = Table(show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    table.add_row('Hotkey', hotkey)
    table.add_row('Collateral', f'{from_rao(collateral_rao):.4f} TAO ({collateral_rao} rao)')
    table.add_row('Minimum Required', f'{MIN_COLLATERAL_TAO} TAO')

    status = '[green]Active[/green]' if is_active else '[red]Inactive[/red]'
    table.add_row('Status', status)

    console.print(table)
    console.print()
