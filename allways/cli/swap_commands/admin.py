"""alw admin - Program administration commands (admin-only, signed by the Solana keypair)."""

import os

import click
from solders.pubkey import Pubkey

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    console,
    fail,
    from_lamports,
    get_solana_cli_context,
    loading,
    secs_str,
    to_lamports,
)
from allways.solana.client import SolanaClientError


def _parse_pubkey(s: str):
    try:
        return Pubkey.from_string(s)
    except Exception:
        fail(f'Not a valid Solana pubkey: {s}')


def _confirm(prompt: str) -> bool:
    """Confirm interactively, unless a group-level `admin --yes` (or ALW_ASSUME_YES env) opts out — this
    is what makes the admin commands scriptable/headless without dropping the interactive safety prompt."""
    ctx = click.get_current_context(silent=True)
    if (ctx is not None and ctx.obj and ctx.obj.get('yes')) or os.environ.get('ALW_ASSUME_YES'):
        return True
    return click.confirm(prompt)


def _run_setter(title, getter, setter, noun, format_current, new_display, success_msg):
    _, client = get_solana_cli_context()
    try:
        current = getter(client)
    except SolanaClientError as e:
        fail(f'Failed to read {noun}: {e}')
    console.print(f'\n[bold]{title}[/bold]\n')
    console.print(f'  Current: {format_current(current)}')
    console.print(f'  New:     {new_display}\n')
    if not _confirm(f'Confirm updating {noun}?'):
        console.print('[yellow]Cancelled[/yellow]')
        return
    try:
        with loading('Submitting transaction...'):
            setter(client)
        console.print(f'[green]{success_msg}[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to set {noun}: {e}')


@click.group('admin', cls=StyledGroup, show_disclaimer=True)
@click.option('--yes', '-y', 'assume_yes', is_flag=True, help='Skip confirmation prompts (for scripting).')
@click.pass_context
def admin_group(ctx, assume_yes):
    """Program administration commands (admin-only).

    [dim]Pass --yes before the subcommand (e.g. `alw admin --yes set-max-swap 50`) or set
    ALW_ASSUME_YES=1 to run headless.[/dim]"""
    ctx.obj = {'yes': assume_yes}


@admin_group.command('set-timeout', show_disclaimer=True)
@click.argument('secs', type=int)
def set_timeout(secs: int):
    """Set the fulfillment timeout in seconds (minimum 60).

    [dim]Examples:
        $ alw admin set-timeout 600[/dim]
    """
    if secs < 60:
        fail('Seconds must be >= 60')
    _run_setter(
        title='Set Fulfillment Timeout',
        getter=lambda c: c.get_config().fulfillment_timeout_secs,
        setter=lambda c: c.set_fulfillment_timeout(secs),
        noun='fulfillment timeout',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Fulfillment timeout set to {secs_str(secs)}',
    )


@admin_group.command('set-reservation-ttl', show_disclaimer=True)
@click.argument('secs', type=int)
def set_reservation_ttl(secs: int):
    """Set the reservation TTL in seconds (how long a user has to send funds).

    [dim]Examples:
        $ alw admin set-reservation-ttl 600[/dim]
    """
    if secs <= 0:
        fail('Seconds must be positive')
    _run_setter(
        title='Set Reservation TTL',
        getter=lambda c: c.get_config().reservation_ttl_secs,
        setter=lambda c: c.set_reservation_ttl(secs),
        noun='reservation TTL',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Reservation TTL set to {secs_str(secs)}',
    )


@admin_group.command('set-reservation-fee', show_disclaimer=True)
@click.argument('amount_sol', type=FINITE_FLOAT)
def set_reservation_fee(amount_sol: float):
    """Set the flat per-request reservation fee (in SOL).

    [dim]Examples:
        $ alw admin set-reservation-fee 0.001[/dim]
    """
    if amount_sol < 0:
        fail('Amount must be non-negative')
    lamports = to_lamports(amount_sol)
    _run_setter(
        title='Set Reservation Fee',
        getter=lambda c: c.get_config().reservation_fee_lamports,
        setter=lambda c: c.set_reservation_fee(lamports),
        noun='reservation fee',
        format_current=lambda v: f'{from_lamports(v):.6f} SOL',
        new_display=f'{amount_sol:.6f} SOL',
        success_msg=f'Reservation fee set to {amount_sol:.6f} SOL',
    )


@admin_group.command('set-pool-window', show_disclaimer=True)
@click.argument('secs', type=int)
def set_pool_window(secs: int):
    """Set the reservation-pool window in seconds (how long a pool collects requests before the draw).

    [dim]Examples:
        $ alw admin set-pool-window 60[/dim]
    """
    if secs <= 0:
        fail('Seconds must be positive')
    _run_setter(
        title='Set Pool Window',
        getter=lambda c: c.get_config().pool_window_secs,
        setter=lambda c: c.set_pool_window(secs),
        noun='pool window',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Pool window set to {secs_str(secs)}',
    )


@admin_group.command('set-weights-interval', show_disclaimer=True)
@click.argument('secs', type=int)
def set_weights_interval(secs: int):
    """Set the minimum interval between validator weight updates (seconds).

    [dim]Examples:
        $ alw admin set-weights-interval 1200[/dim]
    """
    if secs <= 0:
        fail('Seconds must be positive')
    _run_setter(
        title='Set Weights Update Interval',
        getter=lambda c: c.get_config().weights_update_min_interval_secs,
        setter=lambda c: c.set_weights_update_min_interval(secs),
        noun='weights update interval',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Weights update interval set to {secs_str(secs)}',
    )


@admin_group.command('set-max-extension', show_disclaimer=True)
@click.argument('secs', type=int)
def set_max_extension(secs: int):
    """Set the max total timeout/reservation extension a single swap may accrue (seconds).

    [dim]Examples:
        $ alw admin set-max-extension 3600[/dim]
    """
    if secs < 0:
        fail('Seconds must be non-negative')
    _run_setter(
        title='Set Max Total Extension',
        getter=lambda c: c.get_config().max_total_extension_secs,
        setter=lambda c: c.set_max_total_extension(secs),
        noun='max total extension',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Max total extension set to {secs_str(secs)}',
    )


@admin_group.command('set-min-collateral', show_disclaimer=True)
@click.argument('amount_sol', type=FINITE_FLOAT)
def set_min_collateral(amount_sol: float):
    """Set the minimum collateral amount (in SOL).

    [dim]Examples:
        $ alw admin set-min-collateral 2.0[/dim]
    """
    if amount_sol <= 0:
        fail('Amount must be positive')
    lamports = to_lamports(amount_sol)
    _run_setter(
        title='Set Minimum Collateral',
        getter=lambda c: c.get_config().min_collateral,
        setter=lambda c: c.set_min_collateral(lamports),
        noun='minimum collateral',
        format_current=lambda v: f'{from_lamports(v):.4f} SOL',
        new_display=f'{amount_sol:.4f} SOL',
        success_msg=f'Minimum collateral set to {amount_sol:.4f} SOL',
    )


@admin_group.command('set-max-collateral', show_disclaimer=True)
@click.argument('amount_sol', type=FINITE_FLOAT)
def set_max_collateral(amount_sol: float):
    """Set the maximum collateral amount (in SOL). Use 0 to remove the cap.

    [dim]Examples:
        $ alw admin set-max-collateral 100.0
        $ alw admin set-max-collateral 0[/dim]
    """
    if amount_sol < 0:
        fail('Amount must be non-negative')
    lamports = to_lamports(amount_sol)
    _run_setter(
        title='Set Maximum Collateral',
        getter=lambda c: c.get_config().max_collateral,
        setter=lambda c: c.set_max_collateral(lamports),
        noun='maximum collateral',
        format_current=lambda v: f'{from_lamports(v):.4f} SOL{" (unlimited)" if v == 0 else ""}',
        new_display=f'{amount_sol:.4f} SOL{" (unlimited)" if lamports == 0 else ""}',
        success_msg=f'Maximum collateral set to {amount_sol:.4f} SOL',
    )


@admin_group.command('set-min-swap', show_disclaimer=True)
@click.argument('amount_sol', type=FINITE_FLOAT)
def set_min_swap(amount_sol: float):
    """Set the minimum swap amount in SOL (SOL-denominated swap size). Use 0 to remove.

    [dim]Examples:
        $ alw admin set-min-swap 1.0
        $ alw admin set-min-swap 0[/dim]
    """
    if amount_sol < 0:
        fail('Amount must be non-negative')
    lamports = to_lamports(amount_sol)
    _run_setter(
        title='Set Minimum Swap Amount',
        getter=lambda c: c.get_config().min_swap_amount,
        setter=lambda c: c.set_min_swap_amount(lamports),
        noun='minimum swap amount',
        format_current=lambda v: f'{from_lamports(v):.4f} SOL{" (no minimum)" if v == 0 else ""}',
        new_display=f'{amount_sol:.4f} SOL{" (no minimum)" if lamports == 0 else ""}',
        success_msg=f'Minimum swap amount set to {amount_sol:.4f} SOL',
    )


@admin_group.command('set-max-swap', show_disclaimer=True)
@click.argument('amount_sol', type=FINITE_FLOAT)
def set_max_swap(amount_sol: float):
    """Set the maximum swap amount in SOL (SOL-denominated swap size). Use 0 to remove.

    [dim]Examples:
        $ alw admin set-max-swap 50.0
        $ alw admin set-max-swap 0[/dim]
    """
    if amount_sol < 0:
        fail('Amount must be non-negative')
    lamports = to_lamports(amount_sol)
    _run_setter(
        title='Set Maximum Swap Amount',
        getter=lambda c: c.get_config().max_swap_amount,
        setter=lambda c: c.set_max_swap_amount(lamports),
        noun='maximum swap amount',
        format_current=lambda v: f'{from_lamports(v):.4f} SOL{" (no maximum)" if v == 0 else ""}',
        new_display=f'{amount_sol:.4f} SOL{" (no maximum)" if lamports == 0 else ""}',
        success_msg=f'Maximum swap amount set to {amount_sol:.4f} SOL',
    )


@admin_group.command('set-threshold', show_disclaimer=True)
@click.argument('percent', type=int)
def set_threshold(percent: int):
    """Set the consensus threshold percentage (1-100).

    [dim]Examples:
        $ alw admin set-threshold 67[/dim]
    """
    if percent <= 0 or percent > 100:
        fail('Threshold must be 1-100')
    _run_setter(
        title='Set Consensus Threshold',
        getter=lambda c: c.get_config().consensus_threshold_percent,
        setter=lambda c: c.set_consensus_threshold(percent),
        noun='consensus threshold',
        format_current=lambda v: f'{v}%',
        new_display=f'{percent}%',
        success_msg=f'Consensus threshold set to {percent}%',
    )


def _is_validator(config, pubkey: Pubkey) -> bool:
    target = bytes(pubkey)
    return any(bytes(v.key) == target for v in config.validators)


@admin_group.command('add-vali', show_disclaimer=True)
@click.argument('pubkey', type=str)
@click.option('--weight', default=1, type=int, help='Stake-draw weight for the Phase-9 lottery (default 1)')
def add_vali(pubkey: str, weight: int):
    """Add a validator to the program (by Solana pubkey).

    [dim]Examples:
        $ alw admin add-vali <pubkey> --weight 1[/dim]
    """
    pk = _parse_pubkey(pubkey)
    _, client = get_solana_cli_context()

    try:
        already = _is_validator(client.get_config(), pk)
    except SolanaClientError as e:
        fail(f'Failed to read validator set: {e}')

    console.print('\n[bold]Add Validator[/bold]\n')
    console.print(f'  Pubkey: {pk}')
    console.print(f'  Weight: {weight}')
    if already:
        console.print('  [yellow]Warning: this pubkey is already a registered validator[/yellow]')
    console.print()

    if not _confirm('Confirm adding validator?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.add_validator(pk, weight)
        console.print(f'[green]Validator {pk} added[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to add validator: {e}')


@admin_group.command('remove-vali', show_disclaimer=True)
@click.argument('pubkey', type=str)
def remove_vali(pubkey: str):
    """Remove a validator from the program (by Solana pubkey).

    [dim]Examples:
        $ alw admin remove-vali <pubkey>[/dim]
    """
    pk = _parse_pubkey(pubkey)
    _, client = get_solana_cli_context()

    try:
        registered = _is_validator(client.get_config(), pk)
    except SolanaClientError as e:
        fail(f'Failed to read validator set: {e}')

    console.print('\n[bold]Remove Validator[/bold]\n')
    console.print(f'  Pubkey: {pk}')
    if not registered:
        console.print('  [yellow]Warning: this pubkey is not a registered validator[/yellow]')
    console.print()

    if not _confirm('Confirm removing validator?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.remove_validator(pk)
        console.print(f'[green]Validator {pk} removed[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to remove validator: {e}')


@admin_group.command('withdraw-treasury', show_disclaimer=True)
@click.argument('recipient', type=str)
@click.option('--amount', default=None, type=FINITE_FLOAT, help='Amount in SOL (default: withdraw the full balance)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def withdraw_treasury(recipient: str, amount: float | None, yes: bool):
    """Withdraw accrued protocol fees from the treasury to a recipient.

    [dim]Replaces the ink! fee-recycle: on Solana fees accrue in a treasury PDA the admin draws down.

    Examples:
        $ alw admin withdraw-treasury <pubkey>
        $ alw admin withdraw-treasury <pubkey> --amount 1.5[/dim]
    """
    pk = _parse_pubkey(recipient)
    _, client = get_solana_cli_context()

    try:
        treasury = client.get_treasury()
    except SolanaClientError as e:
        fail(f'Failed to read treasury: {e}')
    total = treasury.total if treasury is not None else 0

    lamports = to_lamports(amount) if amount is not None else total
    console.print('\n[bold]Withdraw Treasury[/bold]\n')
    console.print(f'  Accrued:   {from_lamports(total):.6f} SOL')
    console.print(f'  Withdraw:  {from_lamports(lamports):.6f} SOL')
    console.print(f'  Recipient: {pk}\n')

    if lamports <= 0:
        fail('Nothing to withdraw (treasury is empty or amount is zero).')
    if lamports > total:
        fail('Requested amount exceeds the accrued treasury balance.')

    if not yes and not _confirm('Confirm withdrawing treasury fees?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.withdraw_treasury(pk, lamports)
        console.print(f'[green]Withdrew {from_lamports(lamports):.6f} SOL to {pk}[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to withdraw treasury: {e}')


@click.group('danger', cls=StyledGroup, show_disclaimer=True)
def danger_group():
    """Dangerous operations that affect system availability."""
    pass


@danger_group.command('halt', show_disclaimer=True)
def halt_system():
    """Halt the system — blocks new deposits, activations, and reservation pools.

    [dim]Existing in-flight swaps continue through their lifecycle.

    Examples:
        $ alw admin danger halt[/dim]
    """
    _, client = get_solana_cli_context()

    try:
        if client.get_config().halted:
            console.print('[yellow]System is already halted[/yellow]')
            return
    except SolanaClientError as e:
        fail(f'Failed to read halt status: {e}')

    console.print('\n[bold red]Halt System[/bold red]\n')
    console.print('  This blocks new deposits / activations / reservation pools.')
    console.print('  Existing swaps continue to completion.\n')

    if not _confirm('Confirm halting the system?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_halted(True)
        console.print('[red]System is now halted[/red]\n')
    except SolanaClientError as e:
        fail(f'Failed to halt system: {e}')


@danger_group.command('resume', show_disclaimer=True)
def resume_system():
    """Resume the system — allows new deposits, activations, and pools again.

    [dim]Examples:
        $ alw admin danger resume[/dim]
    """
    _, client = get_solana_cli_context()

    try:
        if not client.get_config().halted:
            console.print('[yellow]System is not halted[/yellow]')
            return
    except SolanaClientError as e:
        fail(f'Failed to read halt status: {e}')

    console.print('\n[bold]Resume System[/bold]\n')
    console.print('  This allows new deposits / activations / pools again.\n')

    if not _confirm('Confirm resuming the system?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            client.set_halted(False)
        console.print('[green]System resumed[/green]\n')
    except SolanaClientError as e:
        fail(f'Failed to resume system: {e}')


admin_group.add_command(danger_group)
