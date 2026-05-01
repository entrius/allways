"""alw view - View swaps, miners, and rates."""

import os
import time
from dataclasses import replace

import bittensor as bt
import click
from rich.live import Live
from rich.table import Table
from rich.text import Text

from allways.chains import SUPPORTED_CHAINS, get_chain
from allways.classes import SwapStatus
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    SWAP_STATUS_COLORS,
    clear_pending_swap,
    console,
    from_rao,
    get_cli_context,
    hydrate_pending_swap,
    load_pending_swap,
    loading,
    probe_pending_reservation,
    read_miner_commitments,
)
from allways.constants import (
    CHALLENGE_WINDOW_BLOCKS,
    FEE_DIVISOR,
    MAX_EXTENSION_BLOCKS,
    MAX_EXTENSIONS_PER_RESERVATION,
    MAX_EXTENSIONS_PER_SWAP,
)
from allways.contract_client import ContractError

DEFAULT_DASHBOARD_URL = 'https://test.all-ways.io'


@click.group('view', cls=StyledGroup)
def view_group():
    """View swaps, miners, and rates."""
    pass


MINER_SORT_FIELDS = ['uid', 'rate', 'capacity', 'status']
MINER_STATUS_CHOICES = ['available', 'offline', 'in-swap', 'reserved', 'cooldown']


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
def view_miners(
    full: bool,
    sort_by: str,
    status_filter: str | None,
    min_capacity: float | None,
    search: str | None,
):
    """View every miner on-subnet — operator view.

    [dim]Shows all miners (including offline, in-swap, or cooldown) with
    collateral, runtime status, posted addresses, and hotkey. For a
    user-shopping view of swappable miners only, use `alw view rates`.

    Status column shows live runtime state: available, reserved (with
    locked TAO amount), in-swap, offline, or cooldown (withdrawal cooldown
    blocks remaining after deactivation).

    Rate columns read as direct exchange ratios:
      BTC→TAO N  reads  1 BTC → N TAO
      TAO→BTC N  reads  N TAO → 1 BTC[/dim]

    [dim]Examples:
        $ alw view miners
        $ alw view miners --full
        $ alw view miners --sort capacity --status available
        $ alw view miners --search 5FxbWw
        $ alw view miners --min-capacity 10[/dim]
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print(f'\n[bold]Miners on SN{netuid}[/bold]\n')
    with loading('Reading commitments...'):
        pairs = read_miner_commitments(subtensor, netuid)

    if not pairs:
        console.print('[yellow]No miner commitments found[/yellow]\n')
        return

    src_up = pairs[0].from_chain.upper()
    dst_up = pairs[0].to_chain.upper()

    console.print(
        f'[dim]{src_up}→{dst_up} N  reads  1 {src_up} → N {dst_up}   |   '
        f'{dst_up}→{src_up} N  reads  N {dst_up} → 1 {src_up}[/dim]\n'
    )

    # Withdrawal cooldown = 2 * fulfillment_timeout_blocks after deactivation.
    try:
        fulfillment_timeout = client.get_fulfillment_timeout()
        current_block = subtensor.get_current_block()
    except ContractError:
        fulfillment_timeout = 0
        current_block = 0

    def _trunc(s: str) -> str:
        if full or not s:
            return s
        return s[:16] + '...' if len(s) > 16 else s

    # Build a row-data list first so we can sort/filter before printing.
    # Each row carries: original MinerPair, derived numeric fields
    # (collateral_rao, sort-status rank), and the already-formatted strings
    # used for rendering.
    status_ranks = {
        'available': 0,
        'reserved': 1,
        'in-swap': 2,
        'cooldown': 3,
        'offline': 4,
        'unknown': 5,
    }

    rows = []
    for pair in pairs:
        try:
            collateral_rao, is_active, has_swap, reserved_until, deactivation_block = client.get_miner_snapshot(
                pair.hotkey
            )
            collateral_str = f'{from_rao(collateral_rao):.4f}'
            active_str = '[green]Yes[/green]' if is_active else '[red]No[/red]'
            snapshot_ok = True
        except ContractError:
            collateral_rao = 0
            is_active = False
            has_swap = False
            reserved_until = 0
            deactivation_block = 0
            collateral_str = '[dim]—[/dim]'
            active_str = '[dim]—[/dim]'
            snapshot_ok = False

        status_parts = []
        status_tokens = set()
        if has_swap:
            status_parts.append('[blue]in-swap[/blue]')
            status_tokens.add('in-swap')
        if reserved_until and current_block and reserved_until > current_block:
            try:
                resv = client.get_reservation_data(pair.hotkey)
            except ContractError:
                resv = None
            if resv:
                tao_amount, _, _ = resv
                status_parts.append(f'[yellow]reserved {from_rao(tao_amount):.4f} TAO[/yellow]')
            else:
                status_parts.append('[yellow]reserved[/yellow]')
            status_tokens.add('reserved')
        if deactivation_block and fulfillment_timeout and current_block:
            cooldown_end = deactivation_block + 2 * fulfillment_timeout
            remaining = cooldown_end - current_block
            if remaining > 0:
                status_parts.append(f'[red]cooldown {remaining}b[/red]')
                status_tokens.add('cooldown')

        if status_parts:
            status_str = ' · '.join(status_parts)
            # Sort-key bucket: pick the most specific active state.
            if 'reserved' in status_tokens:
                status_sort_token = 'reserved'
            elif 'in-swap' in status_tokens:
                status_sort_token = 'in-swap'
            elif 'cooldown' in status_tokens:
                status_sort_token = 'cooldown'
            else:
                status_sort_token = next(iter(status_tokens))
        elif not snapshot_ok:
            status_str = '[dim]—[/dim]'
            status_sort_token = 'unknown'
        elif is_active:
            status_str = '[green]available[/green]'
            status_sort_token = 'available'
        else:
            status_str = '[dim]offline[/dim]'
            status_sort_token = 'offline'

        fwd_display = f'{pair.rate:g}' if pair.rate > 0 else '[dim]—[/dim]'
        if pair.counter_rate > 0:
            ctr_display = f'{pair.counter_rate:g}'
        elif pair.counter_rate_str:
            ctr_display = '[dim]—[/dim]'
        else:
            ctr_display = f'{pair.rate:g}'

        rows.append(
            {
                'pair': pair,
                'collateral_rao': collateral_rao,
                'status_token': status_sort_token,
                'fwd_display': fwd_display,
                'ctr_display': ctr_display,
                'collateral_str': collateral_str,
                'active_str': active_str,
                'status_str': status_str,
            }
        )

    total_before_filter = len(rows)

    # Apply filters
    if status_filter:
        target = status_filter.lower()
        rows = [r for r in rows if r['status_token'] == target]
    if min_capacity is not None:
        threshold_rao = int(min_capacity * 1_000_000_000)
        rows = [r for r in rows if r['collateral_rao'] >= threshold_rao]
    if search:
        needle = search.lower()
        rows = [
            r
            for r in rows
            if needle in str(r['pair'].uid).lower()
            or needle in r['pair'].hotkey.lower()
            or needle in (r['pair'].from_address or '').lower()
            or needle in (r['pair'].to_address or '').lower()
        ]

    # Apply sort
    sort_by = sort_by.lower()
    if sort_by == 'uid':
        rows.sort(key=lambda r: r['pair'].uid)
    elif sort_by == 'rate':
        # Sort by strongest of the two quoted rates, desc. Reverse-only miners
        # with rate=0 still get ranked by their counter_rate.
        rows.sort(key=lambda r: max(r['pair'].rate, r['pair'].counter_rate), reverse=True)
    elif sort_by == 'capacity':
        rows.sort(key=lambda r: r['collateral_rao'], reverse=True)
    elif sort_by == 'status':
        rows.sort(key=lambda r: (status_ranks.get(r['status_token'], 99), r['pair'].uid))

    if not rows:
        console.print('[yellow]No miners match the given filters.[/yellow]\n')
        return

    table = Table(show_header=True)
    table.add_column('UID', style='cyan')
    table.add_column(f'{src_up}→{dst_up}', style='green')
    table.add_column(f'{dst_up}→{src_up}', style='green')
    table.add_column('Collateral (TAO)', style='magenta')
    table.add_column('Active', style='bold')
    table.add_column('Status', style='yellow')
    table.add_column(f'{src_up} Addr', style='dim')
    table.add_column(f'{dst_up} Addr', style='dim')
    table.add_column('Hotkey', style='dim')

    for r in rows:
        pair = r['pair']
        table.add_row(
            str(pair.uid),
            r['fwd_display'],
            r['ctr_display'],
            r['collateral_str'],
            r['active_str'],
            r['status_str'],
            _trunc(pair.from_address),
            _trunc(pair.to_address),
            _trunc(pair.hotkey),
        )

    console.print(table)
    shown = len(rows)
    if shown == total_before_filter:
        console.print(f'\n[dim]Total miners: {shown}[/dim]')
    else:
        console.print(f'\n[dim]Showing {shown} of {total_before_filter} miners after filters.[/dim]')
    console.print(f'[dim]Sorted by: {sort_by}[/dim]')
    if not full:
        console.print('[dim]Use --full to show untruncated addresses and hotkeys.[/dim]')
    console.print()


RATES_SORT_FIELDS = ['uid', 'rate', 'fwd', 'rev', 'capacity']


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
def view_rates(
    pair: str,
    full: bool,
    sort_by: str,
    min_capacity: float | None,
    search: str | None,
):
    """View current exchange rates — user-shopping view.

    [dim]Only shows active miners with collateral (i.e. swappable). Each
    row is a miner's bilateral quote with posted addresses and
    TAO-denominated capacity.

    Rate columns read as direct exchange ratios:
      BTC→TAO N  reads:  1 BTC → N TAO
      TAO→BTC N  reads:  N TAO → 1 BTC

    Capacity (TAO) is the miner's posted collateral — the hard cap on the
    TAO leg of any single swap.[/dim]

    [dim]Examples:
        $ alw view rates
        $ alw view rates --pair btc-tao
        $ alw view rates --sort capacity
        $ alw view rates --min-capacity 5
        $ alw view rates --search bc1q
        $ alw view rates --full[/dim]
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print(f'\n[bold]Exchange Rates on SN{netuid}[/bold]\n')

    with loading('Reading rates...'):
        all_pairs = read_miner_commitments(subtensor, netuid)

        # Only show miners that are active and have collateral (i.e. swappable).
        # Keep the collateral value so we can display capacity without a second fetch.
        pairs_with_collateral: list[tuple] = []
        for p in all_pairs:
            try:
                is_active = client.get_miner_active_flag(p.hotkey)
                collateral = client.get_miner_collateral(p.hotkey)
                if is_active and collateral > 0:
                    pairs_with_collateral.append((p, collateral))
            except ContractError:
                continue

        try:
            min_swap_rao = client.get_min_swap_amount()
            max_swap_rao = client.get_max_swap_amount()
        except ContractError:
            min_swap_rao = 0
            max_swap_rao = 0

    if pair:
        parts = pair.lower().split('-')
        if len(parts) != 2:
            console.print('[red]Invalid pair format. Use: chain-chain (e.g. btc-tao)[/red]')
            return
        src, dst = parts
        pairs_with_collateral = [
            (p, c) for (p, c) in pairs_with_collateral if p.from_chain == src and p.to_chain == dst
        ]

    total_before_filter = len(pairs_with_collateral)

    # Apply row-level filters before grouping so the per-group stats
    # reflect only the rows that actually render.
    if min_capacity is not None:
        threshold_rao = int(min_capacity * 1_000_000_000)
        pairs_with_collateral = [(p, c) for (p, c) in pairs_with_collateral if c >= threshold_rao]
    if search:
        needle = search.lower()
        pairs_with_collateral = [
            (p, c)
            for (p, c) in pairs_with_collateral
            if needle in str(p.uid).lower()
            or needle in (p.from_address or '').lower()
            or needle in (p.to_address or '').lower()
        ]

    if not pairs_with_collateral:
        if total_before_filter > 0:
            console.print('[yellow]No rates match the given filters.[/yellow]\n')
        else:
            console.print('[yellow]No rates found[/yellow]\n')
        return

    def _trunc(s: str) -> str:
        if full or not s:
            return s
        return s[:16] + '...' if len(s) > 16 else s

    # Group by pair direction
    grouped: dict[str, list[tuple]] = {}
    for p, c in pairs_with_collateral:
        key = f'{p.from_chain}-{p.to_chain}'
        grouped.setdefault(key, []).append((p, c))

    sort_by = sort_by.lower()

    for pair_key, pair_list in grouped.items():
        src, dst = pair_key.split('-')
        src_name = SUPPORTED_CHAINS.get(src, src).name if src in SUPPORTED_CHAINS else src
        dst_name = SUPPORTED_CHAINS.get(dst, dst).name if dst in SUPPORTED_CHAINS else dst
        src_up = src.upper()
        dst_up = dst.upper()

        console.print(f'[bold]{src_name} ↔ {dst_name}[/bold]')
        console.print(
            f'[dim]{src_up}→{dst_up} N  reads  1 {src_up} → N {dst_up}   |   '
            f'{dst_up}→{src_up} N  reads  N {dst_up} → 1 {src_up}[/dim]'
        )

        table = Table(show_header=True)
        table.add_column('UID', style='cyan')
        table.add_column(f'{src_up}→{dst_up}', style='green')
        table.add_column(f'{dst_up}→{src_up}', style='green')
        table.add_column('Capacity (TAO)', style='yellow')
        table.add_column(f'{src_up} Addr', style='dim')
        table.add_column(f'{dst_up} Addr', style='dim')

        # Sort rows within this pair group. Default 'rate' = strongest quoted
        # direction (so reverse-only miners aren't buried at rate=0).
        if sort_by == 'uid':
            pair_list.sort(key=lambda x: x[0].uid)
        elif sort_by == 'fwd':
            pair_list.sort(key=lambda x: x[0].rate, reverse=True)
        elif sort_by == 'rev':
            pair_list.sort(key=lambda x: x[0].counter_rate, reverse=True)
        elif sort_by == 'capacity':
            pair_list.sort(key=lambda x: x[1], reverse=True)
        else:  # 'rate'
            pair_list.sort(key=lambda x: max(x[0].rate, x[0].counter_rate), reverse=True)
        for p, collateral in pair_list:
            fwd = f'{p.rate:g}' if p.rate > 0 else '—'
            if p.counter_rate > 0:
                rev = f'{p.counter_rate:g}'
            elif p.counter_rate_str:
                rev = '—'
            else:
                rev = fwd
            table.add_row(
                str(p.uid),
                fwd,
                rev,
                f'{from_rao(collateral):.4f}',
                _trunc(p.from_address),
                _trunc(p.to_address),
            )

        console.print(table)

        # Per-direction stats — excluding miners that don't quote that direction,
        # so a single reverse-only miner doesn't drag the forward average to zero.
        fwd_rates = [p.rate for (p, _) in pair_list if p.rate > 0]
        rev_rates = [p.counter_rate for (p, _) in pair_list if p.counter_rate > 0]
        stat_lines = []
        if len(fwd_rates) > 1:
            stat_lines.append(
                f'{src_up}→{dst_up}: best {max(fwd_rates):g} | worst {min(fwd_rates):g} | '
                f'avg {sum(fwd_rates) / len(fwd_rates):.4f}'
            )
        if len(rev_rates) > 1:
            stat_lines.append(
                f'{dst_up}→{src_up}: best {max(rev_rates):g} | worst {min(rev_rates):g} | '
                f'avg {sum(rev_rates) / len(rev_rates):.4f}'
            )
        for line in stat_lines:
            console.print(f'  [dim]{line}[/dim]')

        console.print()

    # Do-not-just-send disclaimer. The posted addresses are shown so users
    # know a miner is reachable — not as a shortcut. Direct transfers
    # bypass the reservation/validator-consensus flow and will not be
    # matched to a swap: funds can be lost.
    console.print(
        '[yellow]⚠  Do not send funds directly to these addresses.[/yellow] '
        '[dim]Use [cyan]alw swap quote[/cyan] to preview a rate, then '
        '[cyan]alw swap now[/cyan] to reserve a miner and complete the swap.[/dim]'
    )

    # Contract bounds footer — applies to the TAO leg of every swap,
    # regardless of direction. Helps users understand why tiny or huge
    # requested amounts get rejected by `alw swap quote`.
    if min_swap_rao > 0 and max_swap_rao > 0:
        console.print(
            f'[dim]Contract swap bounds (TAO leg): {from_rao(min_swap_rao):.4f}–{from_rao(max_swap_rao):.4f} TAO.[/dim]'
        )
    elif min_swap_rao > 0:
        console.print(f'[dim]Contract min swap (TAO leg): {from_rao(min_swap_rao):.4f} TAO.[/dim]')
    elif max_swap_rao > 0:
        console.print(f'[dim]Contract max swap (TAO leg): {from_rao(max_swap_rao):.4f} TAO.[/dim]')

    shown = len(pairs_with_collateral)
    if shown != total_before_filter:
        console.print(f'[dim]Showing {shown} of {total_before_filter} miners after filters.[/dim]')
    console.print(f'[dim]Sorted by: {sort_by}[/dim]')
    if not full:
        console.print('[dim]Use --full to show untruncated addresses.[/dim]')
    console.print()


@view_group.command('active-swaps')
@click.option(
    '--status',
    default=None,
    type=click.Choice(['active', 'fulfilled', 'completed', 'timed_out'], case_sensitive=False),
    help='Filter by status (active, fulfilled, completed, timed_out)',
)
def view_active_swaps(status: str):
    """View active swaps on the contract.

    [dim]Examples:
        $ alw view active-swaps
        $ alw view active-swaps --status active[/dim]
    """
    _, _, _, client = get_cli_context(need_wallet=False)

    console.print('\n[bold]Active Swaps[/bold]\n')

    try:
        with loading('Reading swaps...'):
            swaps = client.get_active_swaps()
    except ContractError as e:
        console.print(f'[red]Failed to read swaps: {e}[/red]')
        return

    if status:
        status_map = {
            'active': SwapStatus.ACTIVE,
            'fulfilled': SwapStatus.FULFILLED,
            'completed': SwapStatus.COMPLETED,
            'timed_out': SwapStatus.TIMED_OUT,
        }
        target_status = status_map.get(status.lower())
        if target_status is None:
            console.print(f'[red]Unknown status: {status}. Valid: {", ".join(status_map.keys())}[/red]')
            return
        swaps = [s for s in swaps if s.status == target_status]

    if not swaps:
        console.print('[yellow]No swaps found[/yellow]\n')
        return

    table = Table(show_header=True)
    table.add_column('ID', style='cyan')
    table.add_column('Pair', style='green')
    table.add_column('Amount', style='yellow')
    table.add_column('Status', style='bold')
    table.add_column('Miner UID', style='dim')
    table.add_column('Block', style='dim')

    for swap in swaps:
        pair_str = f'{swap.from_chain.upper()}/{swap.to_chain.upper()}'
        color = SWAP_STATUS_COLORS.get(swap.status, 'white')
        status_str = f'[{color}]{swap.status.name}[/{color}]'

        table.add_row(
            str(swap.id),
            pair_str,
            str(swap.from_amount),
            status_str,
            swap.miner_hotkey[:16] + '...',
            str(swap.initiated_block),
        )

    console.print(table)
    console.print(f'\n[dim]Total: {len(swaps)} swaps[/dim]\n')


def build_swap_text(swap, chain_info=True, current_block: int = 0, client=None):
    """Build swap display as a Rich markup string.

    ``current_block`` — when > 0 and the swap is still in-flight, a "Now"
    row is added to the timeline showing how many blocks remain until
    timeout. Gives the reader a frame of reference without having to
    cross-check against ``alw status``.

    ``client`` — when provided and the swap is in-flight, the optimistic-
    extension state (count + any pending proposal) is fetched and rendered.
    Best-effort: read failures fall back to no extension info rather than
    aborting the swap render.
    """
    color = SWAP_STATUS_COLORS.get(swap.status, 'white')
    parts = [f'\n[bold]Swap #{swap.id}[/bold] — [{color}]{swap.status.name}[/{color}]\n']

    src = swap.from_chain.upper()
    dst = swap.to_chain.upper()
    src_chain_def = get_chain(swap.from_chain)
    dst_chain_def = get_chain(swap.to_chain)
    src_human = swap.from_amount / (10**src_chain_def.decimals)
    dst_human = swap.to_amount / (10**dst_chain_def.decimals)
    parts.append(
        f'  Send [red]{src_human:g} {src}[/red] → Receive [green]{dst_human:.8f} {dst}[/green]  '
        f'[dim](rate {swap.rate})[/dim]'
    )

    timed_out = swap.status == SwapStatus.TIMED_OUT

    def step(done, label, value, failed=False):
        if failed:
            marker = '[red]✗[/red]'
            val = f'[red][strike]{label}[/strike][/red]'
            return f'    {marker} {val}'
        marker = '[green]●[/green]' if done else '[dim]○[/dim]'
        val = f'Block {value}' if value else '—'
        return f'    {marker} {label:<14s} {val}'

    parts.append('\n  [bold]Timeline:[/bold]')
    parts.append(step(True, 'Initiated', swap.initiated_block))
    fulfilled_failed = timed_out and not swap.fulfilled_block
    parts.append(step(bool(swap.fulfilled_block), 'Fulfilled', swap.fulfilled_block, failed=fulfilled_failed))
    parts.append(step(bool(swap.completed_block), 'Completed', swap.completed_block, failed=timed_out))
    if timed_out:
        parts.append(f'    [red]⏱ Timed out     Block {swap.timeout_block}[/red]')
    else:
        parts.append(f'    [dim]⏱ Timeout       Block {swap.timeout_block}[/dim]')

    in_flight = swap.status not in (SwapStatus.COMPLETED, SwapStatus.TIMED_OUT)
    if current_block > 0 and in_flight:
        blocks_left = swap.timeout_block - current_block
        if blocks_left > 0:
            minutes_left = blocks_left * SECONDS_PER_BLOCK // 60
            parts.append(
                f'    [cyan]⏲ Now            Block {current_block}[/cyan]  '
                f'[dim](~{blocks_left} blocks / ~{minutes_left} min until timeout)[/dim]'
            )
        else:
            parts.append(f'    [cyan]⏲ Now            Block {current_block}[/cyan]  [red](past timeout)[/red]')

    if client is not None and in_flight:
        try:
            ext_count = client.get_swap_extension_count(swap.id)
        except ContractError:
            ext_count = None
        try:
            pending = client.get_pending_timeout_extension(swap.id)
        except ContractError:
            pending = None
        if ext_count is not None or pending is not None:
            parts.append('')
            parts.append('  [bold]Extensions:[/bold]')
            if ext_count is not None:
                parts.append(f'    Used: {ext_count}/{MAX_EXTENSIONS_PER_SWAP}')
            if pending is not None and current_block > 0:
                finalize_at = pending.proposed_at + CHALLENGE_WINDOW_BLOCKS
                blocks_until_finalize = max(0, finalize_at - current_block)
                target_blocks = max(0, pending.target_block - current_block)
                target_min = target_blocks * SECONDS_PER_BLOCK / 60
                if blocks_until_finalize > 0:
                    finalize_hint = f'finalizable in {blocks_until_finalize} blocks'
                else:
                    finalize_hint = 'finalize window open'
                parts.append(
                    f'    Pending: target block {pending.target_block} (~{target_min:.0f} min) · '
                    f'{finalize_hint} · by {pending.submitter[:16]}...'
                )

    parts.append('')
    parts.append(f'  Source TX:  {swap.from_tx_hash or "—"}')
    parts.append(f'  Dest TX:   {swap.to_tx_hash or "—"}')

    if chain_info:
        parts.append('')
        parts.append(f'  User:      {swap.user_hotkey}')
        parts.append(f'  Miner:     {swap.miner_hotkey}')
        # user_from_address = user's own source-chain address (funds
        # originated here). user_to_address = user's dest-chain address
        # (funds land here). The old "Send to" label read like
        # user_from_address was a destination — misleading.
        parts.append(f'  From addr: {swap.user_from_address}')
        parts.append(f'  To addr:   {swap.user_to_address}')

    parts.append('')
    return '\n'.join(parts)


def display_swap(swap, chain_info=True, current_block: int = 0, client=None):
    """Render a single swap with timeline view."""
    console.print(build_swap_text(swap, chain_info=chain_info, current_block=current_block, client=client))


@view_group.command('swap')
@click.argument('swap_id', type=int)
@click.option('--watch', '-w', is_flag=True, help='Poll and refresh until swap completes or times out')
def view_swap(swap_id: int, watch: bool):
    """View details of a specific swap.

    If the swap is no longer in contract storage (completed or timed out), a
    dashboard URL is shown where resolved swap history is available.

    [dim]Examples:
        $ alw view swap 42
        $ alw view swap 42 --watch[/dim]
    """
    _, _, subtensor, client = get_cli_context(need_wallet=False)

    try:
        with loading('Reading swap...'):
            swap = client.get_swap(swap_id)
    except ContractError as e:
        console.print(f'[red]Failed to read swap: {e}[/red]')
        return

    if not swap:
        try:
            next_id = client.get_next_swap_id()
        except ContractError:
            next_id = None

        if next_id is not None and swap_id < next_id:
            dashboard_url = os.environ.get('ALLWAYS_DASHBOARD_URL', DEFAULT_DASHBOARD_URL).rstrip('/')
            console.print(
                f'[green]Swap {swap_id} has been resolved (completed or timed out).[/green]\n'
                f'[dim]Resolved swaps are removed from on-chain storage. '
                f'View history at:[/dim] {dashboard_url}/swap/{swap_id}'
            )
        elif next_id is not None:
            console.print(f'[red]Swap {swap_id} does not exist. Next swap ID: {next_id}.[/red]')
        else:
            console.print(f'[red]Swap {swap_id} not found[/red]')
        return

    if not watch:
        try:
            current_block = subtensor.get_current_block()
        except Exception:
            current_block = 0
        display_swap(swap, current_block=current_block, client=client)
        return

    watch_swap(client, swap_id, swap)


def watch_swap(client, swap_id: int, swap=None):
    """Poll and display a swap until it reaches a terminal state.

    Uses Rich Live display to update in-place without clearing the screen.
    Returns the final swap object (with inferred terminal status), or None on error/Ctrl+C.
    """
    if swap is None:
        try:
            swap = client.get_swap(swap_id)
        except ContractError:
            console.print(f'[red]Failed to read swap {swap_id}[/red]')
            return None
        if not swap:
            console.print(f'[yellow]Swap {swap_id} not found on-chain.[/yellow]')
            return None

    terminal = (SwapStatus.COMPLETED, SwapStatus.TIMED_OUT)
    if swap.status in terminal:
        display_swap(swap)
        return swap

    def current_block_or_zero():
        try:
            return client.subtensor.get_current_block()
        except Exception:
            return 0

    def render(s, chain_info=True, watching=True, current_block=0):
        markup = build_swap_text(s, chain_info=chain_info, current_block=current_block, client=client)
        if watching:
            markup += '\n[dim]Watching for updates (Ctrl+C to stop)...[/dim]\n'
        return Text.from_markup(markup)

    last_swap = swap
    try:
        with Live(render(swap, current_block=current_block_or_zero()), console=console, refresh_per_second=1) as live:
            while True:
                time.sleep(SECONDS_PER_BLOCK)
                try:
                    swap = client.get_swap(swap_id)
                except ContractError:
                    continue
                if not swap:
                    # Swap resolved — infer final status from last known state.
                    try:
                        current_block = client.subtensor.get_current_block()
                    except Exception:
                        current_block = 0
                    timed_out = last_swap.timeout_block > 0 and current_block >= last_swap.timeout_block
                    if timed_out:
                        final = replace(last_swap, status=SwapStatus.TIMED_OUT)
                    else:
                        final = replace(
                            last_swap,
                            status=SwapStatus.COMPLETED,
                            completed_block=last_swap.fulfilled_block or last_swap.initiated_block,
                        )
                    live.update(render(final, chain_info=False, watching=False))
                    return final
                last_swap = swap
                live.update(render(swap, current_block=current_block_or_zero()))
                if swap.status in terminal:
                    live.update(render(swap, watching=False, current_block=current_block_or_zero()))
                    return swap
    except KeyboardInterrupt:
        console.print(f'\n[dim]Stopped watching. Resume with: alw view swap {swap_id} --watch[/dim]\n')
        return None


@view_group.command('contract')
def view_contract():
    """View contract parameters.

    [dim]Examples:
        $ alw view contract[/dim]
    """
    config, wallet, _, client = get_cli_context(need_wallet=False)

    console.print('\n[bold]Contract Parameters[/bold]\n')

    try:
        with loading('Reading contract parameters...'):
            timeout_blocks = client.get_fulfillment_timeout()
            timeout_minutes = timeout_blocks * SECONDS_PER_BLOCK / 60
            reservation_ttl_blocks = client.get_reservation_ttl()
            reservation_ttl_minutes = reservation_ttl_blocks * SECONDS_PER_BLOCK / 60
            consensus_threshold = client.get_consensus_threshold()
            min_collateral_rao = client.get_min_collateral()
            max_collateral_rao = client.get_max_collateral()
            validator_count = client.get_validator_count()
            required_votes = max(1, (validator_count * consensus_threshold + 99) // 100) if validator_count > 0 else 1
            next_swap_id = client.get_next_swap_id()
            min_swap_rao = client.get_min_swap_amount()
            max_swap_rao = client.get_max_swap_amount()
            accumulated_fees_rao = client.get_accumulated_fees()
            total_recycled_rao = client.get_total_recycled_fees()
            owner = client.get_owner()
            recycle_address = client.get_recycle_address()
            staking_hotkey = client.get_staking_hotkey()
            recycle_netuid = client.get_netuid()
            chain_ext_enabled = client.get_chain_ext_enabled()
            halted = client.get_halted()
    except ContractError as e:
        console.print(f'[red]Failed to read contract parameters: {e}[/red]')
        return

    table = Table(show_header=True)
    table.add_column('Parameter', style='cyan')
    table.add_column('Value', style='green')

    table.add_row('Fulfillment Timeout', f'{timeout_blocks} blocks (~{timeout_minutes:.0f} min)')
    table.add_row('Reservation TTL', f'{reservation_ttl_blocks} blocks (~{reservation_ttl_minutes:.0f} min)')
    fee_pct = 100 / FEE_DIVISOR
    table.add_row('Fee', f'{fee_pct:g}% (hardcoded)')
    # Collapsed: the threshold is the knob, required_votes is what it resolves
    # to at current validator_count — showing both on separate rows read as
    # redundant (especially on small validator sets where they coincide).
    # Consensus quorum applies to vote_reserve / vote_initiate / vote_confirm /
    # vote_timeout — extensions follow the optimistic propose/challenge/finalize
    # path instead, so this row deliberately does not cover them.
    table.add_row(
        'Consensus (reserve/initiate/confirm/timeout)',
        f'{consensus_threshold}% → {required_votes} of {validator_count} validators needed',
    )
    # Contract treats 0 as "bound disabled" (see lib.rs::vote_reserve — the
    # `if self.min_swap_amount > 0 &&` guard skips the check at 0). Render
    # the sentinel explicitly instead of a bare `0.0000 TAO` that reads
    # like a real threshold.
    table.add_row(
        'Min Collateral',
        f'{from_rao(min_collateral_rao):.4f} TAO' if min_collateral_rao > 0 else 'No minimum',
    )
    table.add_row(
        'Max Collateral',
        f'{from_rao(max_collateral_rao):.4f} TAO' if max_collateral_rao > 0 else 'Unlimited',
    )
    table.add_row(
        'Min Swap Amount',
        f'{from_rao(min_swap_rao):.4f} TAO' if min_swap_rao > 0 else 'No minimum',
    )
    table.add_row(
        'Max Swap Amount',
        f'{from_rao(max_swap_rao):.4f} TAO' if max_swap_rao > 0 else 'Unlimited',
    )
    # Optimistic extension parameters. These are contract constants (no
    # on-chain getter), mirrored from allways/constants.py and held in lock-
    # step with smart-contracts/ink/lib.rs — a redeploy is the only way to
    # change them. Surfaced here so users can predict how long their swap
    # can be held alive before forced timeout.
    challenge_window_minutes = CHALLENGE_WINDOW_BLOCKS * SECONDS_PER_BLOCK / 60
    max_ext_minutes = MAX_EXTENSION_BLOCKS * SECONDS_PER_BLOCK / 60
    table.add_row(
        'Extension Challenge Window',
        f'{CHALLENGE_WINDOW_BLOCKS} blocks (~{challenge_window_minutes:.0f} min)',
    )
    table.add_row(
        'Max Extension Length',
        f'{MAX_EXTENSION_BLOCKS} blocks (~{max_ext_minutes:.0f} min) per finalize',
    )
    table.add_row('Max Extensions / Reservation', str(MAX_EXTENSIONS_PER_RESERVATION))
    table.add_row('Max Extensions / Swap', str(MAX_EXTENSIONS_PER_SWAP))
    table.add_row('Next Swap ID', str(next_swap_id))
    table.add_row('Accumulated Fees', f'{from_rao(accumulated_fees_rao):.4f} TAO')
    table.add_row('Total Recycled Fees', f'{from_rao(total_recycled_rao):.4f} TAO')
    table.add_row('Owner', owner)
    # Recycle path. Pre-latch (chain_ext_enabled = false): `recycle_fees`
    # transfers to the immutable custodial fallback. Post-latch: dispatches
    # via the subtensor `add_stake_recycle` chain extension to
    # (staking_hotkey, netuid). The latch is one-way; flipped by the owner
    # via `alw admin enable-chain-ext`.
    if chain_ext_enabled:
        table.add_row(
            'Recycle Path', f'[green]chain ext (latched)[/green] → {staking_hotkey} on netuid {recycle_netuid}'
        )
    else:
        table.add_row('Recycle Path', f'[yellow]custodial (pre-latch)[/yellow] → {recycle_address}')
        table.add_row('Latch Target', f'{staking_hotkey} on netuid {recycle_netuid}')
    table.add_row('System Status', '[red]HALTED[/red]' if halted else '[green]Running[/green]')

    console.print(table)
    console.print()


@view_group.command('validators')
def view_validators():
    """View whitelisted validators on the contract.

    [dim]Reads the validator allowlist the owner has registered via
    `alw admin add-vali` / `alw admin remove-vali`. Each listed validator
    is one of the keys that can sign vote_* messages (reserve, initiate,
    confirm, timeout, activate, etc.).

    The Identity column shows the on-chain IdentitiesV2 display name
    registered against the validator's coldkey, when one exists. Owner-
    tagged rows match the contract owner.[/dim]

    [dim]Examples:
        $ alw view validators[/dim]
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print('\n[bold]Whitelisted Validators[/bold]\n')

    try:
        with loading('Reading validator set...'):
            validators = client.get_validators()
            consensus_threshold = client.get_consensus_threshold()
            owner = client.get_owner()
    except ContractError as e:
        console.print(f'[red]Failed to read validators: {e}[/red]')
        return

    if not validators:
        console.print('[yellow]No validators whitelisted.[/yellow]\n')
        return

    # On-chain identities are keyed by coldkey in the SubtensorModule.
    # IdentitiesV2 map. Resolve each validator hotkey → coldkey through the
    # subnet metagraph; skip silently when a hotkey isn't registered on this
    # netuid (possible if the whitelist includes off-subnet validators) or
    # the RPC fails — the column just renders as a dim dash.
    hotkey_to_identity: dict[str, str] = {}
    try:
        with loading('Reading on-chain identities...'):
            metagraph = subtensor.metagraph(netuid)
            hotkey_to_coldkey = {metagraph.hotkeys[i]: metagraph.coldkeys[i] for i in range(metagraph.n.item())}
            for hk in validators:
                ck = hotkey_to_coldkey.get(hk)
                if not ck:
                    continue
                try:
                    identity = subtensor.query_identity(ck)
                except Exception:
                    identity = None
                if identity and getattr(identity, 'name', ''):
                    hotkey_to_identity[hk] = identity.name
    except Exception as e:
        bt.logging.debug(f'Identity lookup failed: {e}')

    required = max(1, (len(validators) * consensus_threshold + 99) // 100)

    table = Table(show_header=True)
    table.add_column('#', style='dim')
    table.add_column('Hotkey', style='cyan')
    table.add_column('Identity', style='green')

    for idx, hotkey in enumerate(validators, 1):
        hotkey_display = hotkey
        if hotkey == owner:
            hotkey_display = f'{hotkey} [yellow](owner)[/yellow]'
        identity_display = hotkey_to_identity.get(hotkey, '[dim]—[/dim]')
        table.add_row(str(idx), hotkey_display, identity_display)

    console.print(table)
    console.print(
        f'\n[dim]Total: {len(validators)} validators · '
        f'consensus {consensus_threshold}% → {required} votes needed for quorum on '
        f'reserve/initiate/confirm/timeout. Optimistic extensions use propose+challenge instead.[/dim]\n'
    )


@view_group.command('reservation')
def view_reservation():
    """View your active swap reservation.

    [dim]Reads local state file and validates against on-chain data.[/dim]

    [dim]Examples:
        $ alw view reservation[/dim]
    """
    _, _, subtensor, client = get_cli_context(need_wallet=False)

    state = load_pending_swap()
    if not state:
        console.print('\n[yellow]No active reservation found.[/yellow]')
        console.print('[dim]Run `alw swap now` to initiate a swap.[/dim]\n')
        return
    hydrate_pending_swap(state, client)

    current_block = subtensor.get_current_block()
    with loading('Reading reservation...'):
        status = probe_pending_reservation(client, state, current_block)

    if status.kind == 'rpc_error':
        console.print('[red]Failed to read reservation status from contract.[/red]')
        return

    console.print('\n[bold]Swap Reservation[/bold]\n')

    table = Table(show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    chain = get_chain(state.from_chain)
    human_send = state.from_amount / (10**chain.decimals)
    to_chain_def = get_chain(state.to_chain)
    human_receive = state.user_receives / (10**to_chain_def.decimals)

    src_up = state.from_chain.upper()
    dst_up = state.to_chain.upper()
    table.add_row('Direction', f'Send {src_up} → Receive {dst_up}')
    table.add_row(f'Send {src_up}', f'{human_send} {src_up}')
    table.add_row(f'  from (your {src_up})', state.user_from_address)
    table.add_row(f'  to (miner {src_up})', state.miner_from_address)
    table.add_row(f'Receive {dst_up}', f'{human_receive:.8f} {dst_up}')
    table.add_row(f'  to (your {dst_up})', state.receive_address)
    table.add_row('Miner', f'UID {state.miner_uid} ({state.miner_hotkey[:16]}...)')

    if status.kind == 'ours_active':
        table.add_row('Chain Amounts', f'[green]✓ locked {from_rao(state.tao_amount):.4f} TAO[/green]')

    sent_tx_hash = (state.from_tx_hash or '').strip()

    if status.kind == 'ours_active':
        remaining = max(0, status.reserved_until - current_block)
        remaining_min = remaining * SECONDS_PER_BLOCK / 60
        table.add_row('Status', '[green]ACTIVE[/green]')
        table.add_row('Time Remaining', f'~{remaining} blocks (~{remaining_min:.0f} min)')
        # Optimistic-extension visibility — silent on read failure: best-effort
        # signal, not core to the reservation status.
        try:
            ext_count = client.get_reservation_extension_count(state.miner_hotkey)
            table.add_row('Extensions', f'{ext_count}/{MAX_EXTENSIONS_PER_RESERVATION}')
        except ContractError:
            pass
        try:
            pending = client.get_pending_reservation_extension(state.miner_hotkey)
        except ContractError:
            pending = None
        if pending is not None:
            finalize_at = pending.proposed_at + CHALLENGE_WINDOW_BLOCKS
            blocks_until_finalize = max(0, finalize_at - current_block)
            target_blocks = max(0, pending.target_block - current_block)
            target_min = target_blocks * SECONDS_PER_BLOCK / 60
            if blocks_until_finalize > 0:
                finalize_hint = f'finalizable in {blocks_until_finalize} blocks'
            else:
                finalize_hint = 'finalize window open'
            table.add_row(
                'Pending Extension',
                f'target block {pending.target_block} (~{target_min:.0f} min) · {finalize_hint} · '
                f'by {pending.submitter[:16]}...',
            )
        if sent_tx_hash:
            tx_display = sent_tx_hash if len(sent_tx_hash) <= 24 else sent_tx_hash[:24] + '...'
            table.add_row(f'Source TX ({src_up})', tx_display)
    elif status.kind == 'our_swap':
        table.add_row('Status', f'[green]INITIATED (swap #{status.swap.id})[/green]')
        table.add_row('Swap Status', status.swap.status.name)
    else:  # 'replaced' or 'expired'
        table.add_row('Status', '[yellow]NO LONGER ACTIVE[/yellow]')
        table.add_row('Time Remaining', '—')

    console.print(table)

    if status.kind == 'ours_active':
        if sent_tx_hash:
            console.print(f'\n[green]Source tx already broadcast:[/green] [cyan]{sent_tx_hash}[/cyan]')
            console.print(
                '[dim]Validators are waiting on source-chain confirmations before initiating the swap '
                'on-chain. Nothing more to do — leave this reservation alone until it either initiates '
                'or expires.[/dim]'
            )
            console.print('\n[dim]If validators never picked up your confirm, re-broadcast it with:[/dim]')
            console.print(f'  [bold cyan]alw swap post-tx {sent_tx_hash}[/bold cyan]\n')
        else:
            console.print(
                f'\n[bold]Next step:[/bold] Send {human_send} {state.from_chain.upper()} '
                f'from [yellow]{state.user_from_address}[/yellow] to [yellow]{state.miner_from_address}[/yellow], then run:'
            )
            console.print('  [bold cyan]alw swap post-tx <your_transaction_hash>[/bold cyan]\n')
    elif status.kind == 'our_swap':
        console.print(
            f'\n[green]Reservation was consumed into swap #{status.swap.id} — it is in progress on-chain.[/green]'
        )
        console.print(f'[dim]Watch with: alw view swap {status.swap.id} --watch[/dim]')
        clear_pending_swap()
        console.print('[dim]Local reservation state cleared.[/dim]\n')
    else:  # 'replaced' or 'expired'
        console.print(
            '\n[yellow]Reservation is no longer active on-chain.[/yellow]\n'
            '[dim]Either the reservation expired before you sent funds, or your swap already '
            'initiated and has since completed. Check: alw view active-swaps[/dim]'
        )
        clear_pending_swap()
        console.print('[dim]Local reservation state cleared.[/dim]\n')
