"""alw view - Inspect miners, rates, swaps, validators, config, and reservations.

`view config` and `view validators` read the on-chain Config directly. The taker aggregation
views (miners/rates/swaps/reservation) still drew from the old ink! contract surface and need a
Solana-backed re-port (MinerQuote aggregation, on-chain swaps/reservations), so they stay stubbed."""

import click
from solders.pubkey import Pubkey

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    console,
    from_lamports,
    get_solana_cli_context,
    secs_str,
    taker_view_unavailable,
)
from allways.solana.client import SolanaClientError

MINER_SORT_FIELDS = ['uid', 'rate', 'capacity', 'status']
MINER_STATUS_CHOICES = ['available', 'offline', 'in-swap', 'reserved', 'cooldown']
RATES_SORT_FIELDS = ['uid', 'rate', 'fwd', 'rev', 'capacity']


@click.group('view', cls=StyledGroup)
def view_group():
    """View swaps, miners, and rates."""
    pass


@view_group.command('miners')
@click.option('--full', is_flag=True, help='Show untruncated addresses and hotkeys')
@click.option(
    '--sort',
    'sort_by',
    type=click.Choice(MINER_SORT_FIELDS, case_sensitive=False),
    default='uid',
    show_default=True,
    help='Sort field. uid ascends; rate/capacity descend; status groups available→reserved→in-swap→cooldown→offline.',
)
@click.option(
    '--status',
    'status_filter',
    type=click.Choice(MINER_STATUS_CHOICES, case_sensitive=False),
    default=None,
    help='Only show miners in a given runtime state.',
)
@click.option(
    '--min-capacity', type=float, default=None, help='Only show miners with at least this much collateral (TAO).'
)
@click.option(
    '--search',
    default=None,
    type=str,
    help='Case-insensitive substring match against UID, addresses, and hotkey.',
)
def view_miners(full, sort_by, status_filter, min_capacity, search):
    """List active miners with their posted directions and status."""
    taker_view_unavailable('`view miners`')


@view_group.command('rates')
@click.option('--pair', default=None, type=str, help='Filter by pair (e.g. btc-tao)')
@click.option('--full', is_flag=True, help='Show untruncated addresses')
@click.option(
    '--sort',
    'sort_by',
    type=click.Choice(RATES_SORT_FIELDS, case_sensitive=False),
    default='rate',
    show_default=True,
    help='Sort field. rate = best of fwd/rev (desc); fwd/rev = that direction only (desc); uid asc; capacity desc.',
)
@click.option(
    '--min-capacity', type=float, default=None, help='Only show miners with at least this much collateral (TAO).'
)
@click.option(
    '--search',
    default=None,
    type=str,
    help='Case-insensitive substring match against UID and posted addresses.',
)
def view_rates(pair, full, sort_by, min_capacity, search):
    """Show posted miner rates per direction."""
    taker_view_unavailable('`view rates`')


@view_group.command('active-swaps')
@click.option(
    '--status',
    default=None,
    type=click.Choice(['active', 'fulfilled', 'completed', 'timed_out'], case_sensitive=False),
    help='Filter by status (active, fulfilled, completed, timed_out)',
)
def view_active_swaps(status: str):
    """List swaps currently tracked on-chain."""
    taker_view_unavailable('`view active-swaps`')


@view_group.command('swap')
@click.argument('swap_id', type=int)
@click.option('--watch', '-w', is_flag=True, help='Poll and refresh until swap completes or times out')
def view_swap(swap_id: int, watch: bool):
    """Inspect a single swap by id."""
    taker_view_unavailable('`view swap`')


def _read_config():
    """Read the on-chain program Config (read-only — no keypair needed). None if unreadable/uninitialized."""
    _, client = get_solana_cli_context(need_keypair=False)
    try:
        cfg = client.get_config()
    except SolanaClientError as e:
        console.print(f'[red]Failed to read config: {e}[/red]')
        return None
    if cfg is None:
        console.print('[yellow]Program is not initialized (no Config account).[/yellow]')
    return cfg


def _sol_or(amount: int, zero_label: str) -> str:
    return f'{from_lamports(amount):.4f} SOL' + (f' ({zero_label})' if amount == 0 else '')


@view_group.command('config')
def view_config():
    """Show the current on-chain program Config (bounds, fees, windows, threshold). Read-only."""
    cfg = _read_config()
    if cfg is None:
        return
    console.print('\n[bold]Program Config[/bold]\n')
    console.print(f'  Admin:                {cfg.admin}')
    console.print(f'  Version:              {cfg.version}')
    console.print(f'  Halted:               {"yes" if cfg.halted else "no"}')
    console.print(f'  Consensus threshold:  {cfg.consensus_threshold_percent}%')
    console.print(f'  Reservation fee:      {from_lamports(cfg.reservation_fee_lamports):.6f} SOL')
    console.print(f'  Min collateral:       {from_lamports(cfg.min_collateral):.4f} SOL')
    console.print(f'  Max collateral:       {_sol_or(cfg.max_collateral, "unlimited")}')
    console.print(f'  Min swap amount:      {_sol_or(cfg.min_swap_amount, "no minimum")}')
    console.print(f'  Max swap amount:      {_sol_or(cfg.max_swap_amount, "no maximum")}')
    console.print(f'  Fulfillment timeout:  {secs_str(cfg.fulfillment_timeout_secs)}')
    console.print(f'  Reservation TTL:      {secs_str(cfg.reservation_ttl_secs)}')
    console.print(f'  Pool window:          {secs_str(cfg.pool_window_secs)}')
    console.print(f'  Weights interval:     {secs_str(cfg.weights_update_min_interval_secs)}')
    console.print(f'  Max total extension:  {secs_str(cfg.max_total_extension_secs)}')
    console.print(f'  Validators:           {len(cfg.validators)}\n')


@view_group.command('validators')
def view_validators():
    """List the registered validators with their lottery weights and the consensus threshold."""
    cfg = _read_config()
    if cfg is None:
        return
    console.print('\n[bold]Validators[/bold]\n')
    console.print(f'  Consensus threshold: {cfg.consensus_threshold_percent}%\n')
    if not cfg.validators:
        console.print('  [yellow]No validators registered.[/yellow]\n')
        return
    for v in cfg.validators:
        console.print(f'  {Pubkey.from_bytes(bytes(v.key))}  weight={v.weight}')
    console.print()


@view_group.command('reservation')
def view_reservation():
    """Show your current pending reservation, if any."""
    taker_view_unavailable('`view reservation`')
