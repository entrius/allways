"""alw admin - Program administration commands (admin-only, signed by the Solana keypair)."""

import os

import click
from solders.pubkey import Pubkey

from allways.chains import get_chain_def
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
from allways.constants import TAO_TO_RAO
from allways.solana import pdas
from allways.solana.client import SolanaClientError
from allways.solana.layouts import hub_config


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


def _fmt_tao(rao: int) -> str:
    return f'{rao / TAO_TO_RAO:.9f} TAO'


_USDC_UNIT = 10 ** get_chain_def(pdas.BACKING_CHAIN_BASEUSDC).decimals

# Per hub: (to base units, format base units). Each backing is bounded in its own asset, never
# converted through a rate — that is what keeps a price oracle out of the guards.
_HUB_UNITS = {
    pdas.BACKING_CHAIN_SOL: (to_lamports, lambda v: f'{from_lamports(v):.4f} SOL'),
    pdas.BACKING_CHAIN_TAO: (lambda x: int(x * TAO_TO_RAO), _fmt_tao),
    pdas.BACKING_CHAIN_BASEUSDC: (lambda x: int(round(x * _USDC_UNIT)), lambda v: f'{v / _USDC_UNIT:.6f} USDC'),
}

# (Config field, client setter) per hub, for each per-hub amount lever.
_HUB_AMOUNT_LEVERS = {
    'min_swap': {
        'sol': ('min_swap_amount', 'set_min_swap_amount'),
        'tao': ('tao_min_swap_amount', 'set_tao_min_swap_amount'),
        'baseusdc': ('usdc_min_swap_amount', 'set_usdc_min_swap_amount'),
    },
    'max_swap': {
        'sol': ('max_swap_amount', 'set_max_swap_amount'),
        'tao': ('tao_max_swap_amount', 'set_tao_max_swap_amount'),
        'baseusdc': ('usdc_max_swap_amount', 'set_usdc_max_swap_amount'),
    },
    'min_collateral': {
        'sol': ('min_collateral', 'set_min_collateral'),
        'tao': ('tao_min_collateral', 'set_tao_min_collateral'),
        'baseusdc': ('usdc_min_collateral', 'set_usdc_min_collateral'),
    },
}
_LEVER_NOUNS = {
    'min_swap': ('minimum swap amount', ' (no minimum)'),
    'max_swap': ('maximum swap amount', ' (no maximum)'),
    'min_collateral': ('collateral floor', ' (unset)'),
}


def _hub_option(default: str, remote_only: bool = False):
    """`--hub`, defaulting to the hub the command always acted on so old invocations are unchanged."""
    hubs = [h for h in _HUB_UNITS if not (remote_only and h == pdas.BACKING_CHAIN_SOL)]
    return click.option(
        '--hub',
        type=click.Choice(hubs),
        default=default,
        show_default=True,
        help='Backing hub the setting applies to.',
    )


def _set_hub_amount(lever: str, hub: str, amount: float):
    field, setter_name = _HUB_AMOUNT_LEVERS[lever][hub]
    to_base, fmt = _HUB_UNITS[hub]
    noun, zero = _LEVER_NOUNS[lever]
    base = to_base(amount)
    _run_setter(
        title=f'Set {hub} {noun}',
        getter=lambda c: int(getattr(c.get_config(), field)),
        setter=lambda c: getattr(c, setter_name)(base),
        noun=f'{hub} {noun}',
        format_current=lambda v: fmt(v) + (zero if v == 0 else ''),
        new_display=fmt(base) + (zero if base == 0 else ''),
        success_msg=f'{hub} {noun} set to {fmt(base)}',
    )


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


@admin_group.command('set-finalize-window', show_disclaimer=True)
@click.argument('secs', type=int)
def set_finalize_window(secs: int):
    """Set the post-draw finalize window in seconds (how long the seat winner has to fill its reservation).

    [dim]The draw creates an UNFILLED reservation; only its router may call finalize_reservation, and only
    within this window. Too short and a slow RPC round-trip forfeits the seat (fee spent, miner held busy
    until reaped). The contract clamps this to [15, 300].[/dim]

    [dim]Examples:
        $ alw admin set-finalize-window 180[/dim]
    """
    if not 15 <= secs <= 300:
        fail('Seconds must be within [15, 300] (the contract clamps this range)')
    _run_setter(
        title='Set Finalize Window',
        getter=lambda c: c.get_config().finalize_window_secs,
        setter=lambda c: c.set_finalize_window(secs),
        noun='finalize window',
        format_current=lambda v: secs_str(v),
        new_display=secs_str(secs),
        success_msg=f'Finalize window set to {secs_str(secs)}',
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
@click.argument('amount', type=FINITE_FLOAT)
@_hub_option(default='sol')
def set_min_collateral(amount: float, hub: str):
    """Set a hub's activation floor, in that hub's asset (SOL, TAO or USDC).

    [dim]For "sol" it is the local collateral a miner must post; for "tao" and "baseusdc" it is the
    ATTESTED effective bond — net of accrued fees and voted slashes — not the vault's gross counter.[/dim]

    [dim]Examples:
        $ alw admin set-min-collateral 2.0
        $ alw admin set-min-collateral 5 --hub tao
        $ alw admin set-min-collateral 25 --hub baseusdc[/dim]
    """
    if amount <= 0:
        fail('Amount must be positive (a dust bond must not activate a purse the guards then trust)')
    _set_hub_amount('min_collateral', hub, amount)


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
@click.argument('amount', type=FINITE_FLOAT)
@_hub_option(default='sol')
def set_min_swap(amount: float, hub: str):
    """Set a hub's minimum swap size, in that hub's asset (SOL, TAO or USDC).

    [dim]Only bounds swaps whose quote declared that backing, compared in its own units and never
    converted through the rate. A remote hub (tao, baseusdc) stays closed while its minimum is 0.[/dim]

    [dim]Examples:
        $ alw admin set-min-swap 1.0
        $ alw admin set-min-swap 0.5 --hub tao
        $ alw admin set-min-swap 10 --hub baseusdc[/dim]
    """
    if amount < 0:
        fail('Amount must be non-negative')
    _set_hub_amount('min_swap', hub, amount)


@admin_group.command('set-max-swap', show_disclaimer=True)
@click.argument('amount', type=FINITE_FLOAT)
@_hub_option(default='sol')
def set_max_swap(amount: float, hub: str):
    """Set a hub's maximum swap size, in that hub's asset. Use 0 to remove the cap.

    [dim]Examples:
        $ alw admin set-max-swap 50.0
        $ alw admin set-max-swap 25 --hub tao
        $ alw admin set-max-swap 0 --hub baseusdc[/dim]
    """
    if amount < 0:
        fail('Amount must be non-negative')
    _set_hub_amount('max_swap', hub, amount)


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
@click.argument('recipient', type=str, required=False)
@click.option('--amount', default=None, type=FINITE_FLOAT, help='Amount in SOL (default: withdraw the full balance)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def withdraw_treasury(recipient: str | None, amount: float | None, yes: bool):
    """Withdraw accrued protocol fees from the treasury to the admin wallet.

    [dim]Replaces the ink! fee-recycle: on Solana fees accrue in a treasury PDA the admin draws down.
    The contract pins the recipient to the admin; onward distribution is a second, admin-signed hop.

    Examples:
        $ alw admin withdraw-treasury
        $ alw admin withdraw-treasury --amount 1.5[/dim]
    """
    _, client = get_solana_cli_context()

    try:
        config = client.get_config()
        treasury = client.get_treasury()
    except SolanaClientError as e:
        fail(f'Failed to read config/treasury: {e}')
    admin_pk = config.admin

    pk = _parse_pubkey(recipient) if recipient is not None else admin_pk
    if pk != admin_pk:
        fail(
            f'The contract only allows treasury withdrawals to the admin ({admin_pk}). Omit RECIPIENT, or move the funds onward from the admin wallet afterwards.'
        )
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

    # Do NOT gate on a pre-read of `halted` — against a load-balanced RPC a stale node can
    # misreport the state and block a legitimate halt. Just submit; the on-chain state is the
    # source of truth (setting halted=true when already halted is a harmless no-op).
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

    # Do NOT gate on a pre-read of `halted` — a stale RPC node can misreport it and refuse a
    # legitimate resume (observed on public devnet). Just submit; on-chain is the source of truth
    # (setting halted=false when already live is a harmless no-op).
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


# --- The pre-`--hub` TAO commands, kept so existing scripts keep working; each is the `--hub tao` form.


@admin_group.command('set-tao-min-swap', hidden=True)
@click.argument('amount_tao', type=FINITE_FLOAT)
def set_tao_min_swap(amount_tao: float):
    """Alias of `set-min-swap --hub tao`."""
    if amount_tao < 0:
        fail('Amount must be non-negative')
    _set_hub_amount('min_swap', 'tao', amount_tao)


@admin_group.command('set-tao-max-swap', hidden=True)
@click.argument('amount_tao', type=FINITE_FLOAT)
def set_tao_max_swap(amount_tao: float):
    """Alias of `set-max-swap --hub tao`."""
    if amount_tao < 0:
        fail('Amount must be non-negative')
    _set_hub_amount('max_swap', 'tao', amount_tao)


@admin_group.command('set-tao-min-collateral', hidden=True)
@click.argument('amount_tao', type=FINITE_FLOAT)
def set_tao_min_collateral(amount_tao: float):
    """Alias of `set-min-collateral --hub tao`."""
    if amount_tao <= 0:
        fail('Amount must be positive (a dust bond must not activate a purse the guards then trust)')
    _set_hub_amount('min_collateral', 'tao', amount_tao)


@admin_group.command('set-settlement-grace', show_disclaimer=True)
@click.argument('seconds', type=int)
def set_settlement_grace(seconds: int):
    """Set how long a miner stays busy after a non-SOL timeout while the penalty settles (60-7200s).

    [dim]Busy-until-settled: Solana reaches the timeout verdict, the bond chain seizes minutes later.
    This window blocks new reservations in between, so nothing opens against a bond that still owes
    a penalty. Unused by the SOL path, which settles atomically.[/dim]

    [dim]Examples:
        $ alw admin set-settlement-grace 900[/dim]
    """
    if not 60 <= seconds <= 7_200:
        fail('Settlement grace must be 60-7200 seconds (1 min - 2 h)')
    _run_setter(
        title='Set Settlement Grace',
        getter=lambda c: c.get_config().settlement_grace_secs,
        setter=lambda c: c.set_settlement_grace(seconds),
        noun='settlement grace',
        format_current=secs_str,
        new_display=secs_str(seconds),
        success_msg=f'Settlement grace set to {secs_str(seconds)}',
    )


@admin_group.command('set-attest-max-age', show_disclaimer=True)
@click.argument('seconds', type=int)
@_hub_option(default='tao', remote_only=True)
def set_attest_max_age(seconds: int, hub: str):
    """Set one hub's attestation dead-man fuse: max heartbeat age before its entry stops (3600-172800s).

    [dim]A circuit breaker, not a cadence — size it at >=2x the relayer heartbeat interval (12-24 h).
    While THAT hub's heartbeat is older than this, no pool, fill or activation backed by it can open;
    other hubs, exits and in-flight swaps are unaffected. A hub left at 0 stays fused shut.[/dim]

    [dim]Examples:
        $ alw admin set-attest-max-age 86400
        $ alw admin set-attest-max-age 86400 --hub baseusdc[/dim]
    """
    if not 3_600 <= seconds <= 172_800:
        fail('Attestation max age must be 3600-172800 seconds (1 h - 48 h)')
    _run_setter(
        title=f'Set Attestation Fuse Max Age ({hub})',
        getter=lambda c: hub_config(c.get_config().attest_max_age_secs, hub),
        setter=lambda c: c.set_attest_max_age(hub, seconds),
        noun=f'{hub} attestation max age',
        format_current=lambda v: secs_str(v) if v else 'unset (fused)',
        new_display=secs_str(seconds),
        success_msg=f'{hub} attestation max age set to {secs_str(seconds)}',
    )


@admin_group.command('bump-vault-generation', show_disclaimer=True)
@_hub_option(default='tao', remote_only=True)
def bump_vault_generation(hub: str):
    """Retire one hub's bond-vault attestation-epoch namespace after replacing that vault.

    [dim]A replacement vault restarts its lock epochs at 0, which the on-chain monotonic guard would
    reject as stale forever — miners could never re-attest. Bumping the generation makes the new
    vault's epochs outrank every epoch the retired one could produce. Other hubs are untouched.[/dim]

    [dim]Run this AFTER deploying the new vault and BEFORE pointing that hub's relayers at it.
    Increment-only.[/dim]
    """
    _run_setter(
        title=f'Bump Vault Generation ({hub})',
        getter=lambda c: hub_config(c.get_config().vault_generation, hub),
        setter=lambda c: c.bump_vault_generation(hub),
        noun=f'{hub} vault generation',
        format_current=str,
        new_display='next',
        success_msg=f'{hub} vault generation bumped — its relayers may now attest against the new vault',
    )


admin_group.add_command(danger_group)
