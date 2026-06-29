"""alw view - Inspect miners, rates, swaps, validators, and reservations.

Stub: every view read drew miner quotes/reservations/swaps from the old ink!
contract surface. Their Solana-backed taker views (MinerQuote aggregation, on-chain
swaps/Config/Reservation) are not wired yet, so each subcommand is stubbed. The group
+ subcommands are kept so `alw view --help` and command registration are unchanged."""

import click

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import taker_view_unavailable

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


@view_group.command('contract')
def view_contract():
    """Show contract config (bounds, fees, timeouts)."""
    taker_view_unavailable('`view contract`')


@view_group.command('validators')
def view_validators():
    """List the registered validators and their weights."""
    taker_view_unavailable('`view validators`')


@view_group.command('reservation')
def view_reservation():
    """Show your current pending reservation, if any."""
    taker_view_unavailable('`view reservation`')
