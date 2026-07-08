"""alw collateral - Manage miner collateral on the swap program."""

import time

import click
from rich.table import Table

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    console,
    fail,
    from_lamports,
    from_rao,
    get_cli_context,
    get_solana_cli_context,
    is_valid_ss58,
    loading,
    secs_str,
    to_lamports,
    to_rao,
)
from allways.constants import MIN_BALANCE_FOR_TX_RAO
from allways.solana.client import SolanaClientError

# Lamport gas buffer kept free on the Solana keypair so a post/withdraw tx never fails on fees.
SOLANA_FEE_BUFFER_LAMPORTS = 5_000

try:
    from async_substrate_interface.errors import ExtrinsicNotFound
except ImportError:  # pragma: no cover - dependency always present in practice
    ExtrinsicNotFound = ()


@click.group('collateral', cls=StyledGroup, show_disclaimer=True)
def collateral_group():
    """Manage miner collateral."""
    pass


@collateral_group.command('deposit', show_disclaimer=True)
@click.option('--amount', default=None, type=float, help='Amount in SOL')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_deposit(amount: float | None, yes: bool):
    """Deposit SOL collateral to the swap program.

    [dim]Amount is in SOL, posted from your Solana keypair (SOLANA_KEYPAIR_PATH / ~/.solana/id.json).[/dim]

    [dim]Examples:
        $ alw collateral deposit --amount 5.0
        $ alw collateral deposit  (prompts interactively)[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to deposit (SOL)', type=float)

    if amount <= 0:
        fail('Amount must be positive')

    amount_lamports = to_lamports(amount)

    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    console.print('\n[bold]Depositing Collateral[/bold]\n')
    console.print(f'  Amount:  [green]{amount} SOL[/green] ({amount_lamports} lamports)')
    console.print(f'  Pubkey:  {pubkey}\n')

    try:
        config = client.get_config()
        if config is not None and config.max_collateral > 0:
            current = client.get_collateral_lamports(pubkey) or 0
            if current + amount_lamports > config.max_collateral:
                fail(
                    f'This would exceed the max collateral limit ({from_lamports(config.max_collateral):.4f} SOL). '
                    f'Current: {from_lamports(current):.4f} SOL, posting: {amount} SOL.'
                )

        free = client.rpc.get_account_lamports(pubkey) or 0
        required = amount_lamports + SOLANA_FEE_BUFFER_LAMPORTS
        if free < required:
            console.print(f'[dim]Fund the Solana keypair: solana transfer {pubkey} <sol>[/dim]')
            fail(
                f'Insufficient keypair balance. Free: {from_lamports(free):.4f} SOL, '
                f'need: {from_lamports(required):.4f} SOL (amount + gas buffer).'
            )
    except SolanaClientError as e:
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')
    except Exception as e:
        console.print(f'[yellow]Warning: balance check failed ({e}), proceeding anyway[/yellow]')

    if not yes and not click.confirm('Confirm depositing collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.post_collateral(amount_lamports)
        console.print(f'[green]Successfully deposited {amount} SOL collateral![/green]')
    except SolanaClientError as e:
        fail(f'Failed to deposit collateral: {e}')


@collateral_group.command('withdraw', show_disclaimer=True)
@click.option('--amount', default=None, type=float, help='Amount in SOL')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_withdraw(amount: float | None, yes: bool):
    """Withdraw SOL collateral from the swap program.

    [dim]Amount is in SOL. Cannot withdraw while active, mid-swap, busy, or in cooldown.[/dim]

    [dim]Examples:
        $ alw collateral withdraw --amount 2.0
        $ alw collateral withdraw  (prompts interactively)[/dim]
    """
    if amount is None:
        amount = click.prompt('Amount to withdraw (SOL)', type=float)

    if amount <= 0:
        fail('Amount must be positive')

    amount_lamports = to_lamports(amount)

    _, client = get_solana_cli_context()
    pubkey = client.keypair.pubkey()

    console.print('\n[bold]Withdrawing Collateral[/bold]\n')
    console.print(f'  Amount:  [yellow]{amount} SOL[/yellow] ({amount_lamports} lamports)')
    console.print(f'  Pubkey:  {pubkey}\n')

    try:
        now = int(time.time())
        ms = client.get_miner_state(pubkey)
        if ms is None:
            fail('No miner state found for this keypair (no collateral posted).')

        if ms.active:
            fail('Cannot withdraw while miner is active. Run `alw miner deactivate` first.')

        if ms.has_active_swap:
            fail('Cannot withdraw while miner has an active swap.')

        if ms.busy_until > now:
            fail('Cannot withdraw while miner is busy (open pool / held reservation).')

        if ms.deactivation_at > 0:
            config = client.get_config()
            timeout_secs = config.fulfillment_timeout_secs if config is not None else 0
            cooldown_end = ms.deactivation_at + (timeout_secs * 2)
            if now < cooldown_end:
                remaining = cooldown_end - now
                fail(f'Withdrawal cooldown active. {secs_str(remaining)} remaining.')

        current = client.get_collateral_lamports(pubkey) or 0
        if amount_lamports > current:
            fail(f'Insufficient collateral. Current: {from_lamports(current):.4f} SOL, requested: {amount} SOL.')
    except SolanaClientError as e:
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')
    except Exception as e:
        console.print(f'[yellow]Warning: pre-flight check failed ({e}), proceeding anyway[/yellow]')

    if not yes and not click.confirm('Confirm withdrawing collateral?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.withdraw_collateral(amount_lamports)
        console.print(f'[green]Successfully withdrew {amount} SOL collateral![/green]')
    except SolanaClientError as e:
        fail(f'Failed to withdraw collateral: {e}')


@collateral_group.command('recover-from-hotkey', show_disclaimer=True)
@click.option('--dest', default=None, help='Destination ss58 (default: your coldkey)')
@click.option('--amount', default=None, type=float, help='Amount in TAO to recover (default: sweep all free balance)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def collateral_recover_from_hotkey(dest: str | None, amount: float | None, yes: bool):
    """Move a hotkey's free TAO balance back to your coldkey.

    [dim]Collateral is deposited from — and withdrawn back to — the hotkey, so leftover
    free balance can strand there. `btcli wallet transfer` always signs with the coldkey
    and cannot move it; this signs with the hotkey, the only key that can spend it.

    This is NOT unstaking. To move STAKED tao use `btcli stake remove`.

    Signs with the hotkey from your `alw config` (the configured wallet/hotkey).
    Target a different one per-call with the global --wallet / --hotkey / --network flags.[/dim]

    [dim]Examples:
        $ alw collateral recover-from-hotkey                  (sweep all to your coldkey)
        $ alw collateral recover-from-hotkey --amount 0.5
        $ alw collateral recover-from-hotkey --dest 5C...[/dim]
    """
    _, wallet, subtensor, _ = get_cli_context(need_client=False)

    if dest is None:
        dest = wallet.coldkeypub.ss58_address
    elif not is_valid_ss58(dest):
        fail(f'Invalid destination ss58 address: {dest}')

    keypair = wallet.hotkey
    src = keypair.ss58_address

    try:
        account_info = subtensor.substrate.query('System', 'Account', [src])
        account_data = account_info.value if hasattr(account_info, 'value') else account_info
        free_rao = int(account_data.get('data', {}).get('free', 0))
    except Exception as e:
        fail(f'Failed to read hotkey balance: {e}')

    sweep = amount is None
    amount_rao = 0
    if sweep:
        action = 'Sweep entire free balance (minus tx fee)'
    else:
        if amount <= 0:
            fail('Amount must be positive')
        amount_rao = to_rao(amount)
        required = amount_rao + MIN_BALANCE_FOR_TX_RAO
        if required > free_rao:
            fail(
                f'Insufficient hotkey balance. Free: {from_rao(free_rao):.4f} TAO, '
                f'need: {from_rao(required):.4f} TAO (amount + {from_rao(MIN_BALANCE_FOR_TX_RAO):.2f} TAO gas buffer). '
                f'Omit --amount to sweep everything.'
            )
        action = f'Transfer {amount} TAO'

    console.print('\n[bold]Recovering Hotkey Balance[/bold]\n')
    console.print(f'  Wallet:         {wallet.name}')
    console.print(f'  Source hotkey:  {wallet.hotkey_str} ([dim]{src}[/dim])')
    console.print(f'  Free balance:   [green]{from_rao(free_rao):.9f} TAO[/green] ({free_rao} rao)')
    console.print(f'  Destination:    {dest}')
    console.print(f'  Action:         {action}')
    console.print('  [dim]Override which key is used with --wallet / --hotkey.[/dim]\n')

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
        fail(f'Failed to submit transfer: {e}')

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
        fail(f'Transfer failed: {receipt.error_message}')


@collateral_group.command('view')
@click.option('--pubkey', default=None, help='Solana pubkey to check (default: your keypair)')
def collateral_view(pubkey: str):
    """View collateral balance.

    [dim]Examples:
        $ alw collateral view
        $ alw collateral view --pubkey 7xKX...[/dim]
    """
    _, client = get_solana_cli_context()

    target = pubkey or str(client.keypair.pubkey())

    try:
        with loading('Reading collateral...'):
            collateral_lamports = client.get_collateral_lamports(target) or 0
            ms = client.get_miner_state(target)
            config = client.get_config()
    except SolanaClientError as e:
        fail(f'Failed to read collateral: {e}')

    is_active = bool(ms and ms.active)
    min_required = config.min_collateral if config is not None else 0

    console.print('\n[bold]Collateral Status[/bold]\n')

    table = Table(show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    table.add_row('Pubkey', target)
    table.add_row('Collateral', f'{from_lamports(collateral_lamports):.4f} SOL ({collateral_lamports} lamports)')
    table.add_row('Minimum Required', f'{from_lamports(min_required):.4f} SOL')

    status = '[green]Active[/green]' if is_active else '[red]Inactive[/red]'
    table.add_row('Status', status)

    console.print(table)
    console.print()
