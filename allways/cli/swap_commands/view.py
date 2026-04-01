"""alw view - View swaps, miners, and rates."""

import time
from dataclasses import replace

import rich_click as click
from rich.live import Live
from rich.table import Table
from rich.text import Text

from allways.chains import SUPPORTED_CHAINS, get_chain
from allways.classes import SwapStatus
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    SWAP_STATUS_COLORS,
    clear_pending_swap,
    console,
    from_rao,
    get_cli_context,
    load_pending_swap,
    loading,
    read_miner_commitments,
)
from allways.contract_client import ContractError


@click.group('view')
def view_group():
    """View swaps, miners, and rates.

    \b
    Subcommands:
        miners      View active miners and their pairs
        rates       View exchange rates
        swaps       View active swaps
        swap <id>   View a specific swap
    """
    pass


@view_group.command('miners')
def view_miners():
    """View active miners and their trading pairs.

    Example:
        alw view miners
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print(f'\n[bold]Miners on SN{netuid}[/bold]\n')
    with loading('Reading commitments...'):
        pairs = read_miner_commitments(subtensor, netuid)

    if not pairs:
        console.print('[yellow]No miner commitments found[/yellow]\n')
        return

    table = Table(show_header=True)
    table.add_column('UID', style='cyan')
    table.add_column('Pair', style='green')
    table.add_column('Rate (TAO/1 non-TAO)', style='yellow')
    table.add_column('Collateral (TAO)', style='magenta')
    table.add_column('Active', style='bold')
    table.add_column(f'{pairs[0].source_chain.upper()} Addr', style='dim')
    table.add_column(f'{pairs[0].dest_chain.upper()} Addr', style='dim')

    try:
        for pair in pairs:
            collateral_rao = client.get_miner_collateral(pair.hotkey)
            is_active = client.get_miner_active_flag(pair.hotkey)

            pair_str = f'{pair.source_chain.upper()} <-> {pair.dest_chain.upper()}'
            active_str = '[green]Yes[/green]' if is_active else '[red]No[/red]'

            if pair.rate_reverse_str and pair.rate != pair.rate_reverse:
                rate_display = f'{pair.rate:g} / {pair.rate_reverse:g}'
            else:
                rate_display = f'{pair.rate:g}'
            table.add_row(
                str(pair.uid),
                pair_str,
                rate_display,
                f'{from_rao(collateral_rao):.4f}',
                active_str,
                pair.source_address[:16] + '...',
                pair.dest_address[:16] + '...',
            )
    except ContractError as e:
        console.print(f'[red]Failed to read miner data: {e}[/red]')
        return

    console.print(table)
    console.print(f'\n[dim]Total miners: {len(pairs)}[/dim]\n')


@view_group.command('rates')
@click.option('--pair', default=None, type=str, help='Filter by pair (e.g. btc-tao)')
def view_rates(pair: str):
    """View current exchange rates.

    \b
    Examples:
        alw view rates
        alw view rates --pair btc-tao
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print(f'\n[bold]Exchange Rates on SN{netuid}[/bold]\n')

    with loading('Reading rates...'):
        all_pairs = read_miner_commitments(subtensor, netuid)

        # Only show miners that are active and have collateral (i.e. swappable)
        pairs = []
        skipped = 0
        for p in all_pairs:
            try:
                is_active = client.get_miner_active_flag(p.hotkey)
                collateral = client.get_miner_collateral(p.hotkey)
                if is_active and collateral > 0:
                    pairs.append(p)
            except ContractError:
                skipped += 1
                continue

    if skipped:
        console.print(f'[yellow]{skipped} miner(s) skipped (contract read error)[/yellow]')

    if pair:
        parts = pair.lower().split('-')
        if len(parts) != 2:
            console.print('[red]Invalid pair format. Use: chain-chain (e.g. btc-tao)[/red]')
            return
        src, dst = parts
        pairs = [p for p in pairs if p.source_chain == src and p.dest_chain == dst]

    if not pairs:
        console.print('[yellow]No rates found[/yellow]\n')
        return

    # Group by pair direction
    grouped = {}
    for p in pairs:
        key = f'{p.source_chain}-{p.dest_chain}'
        grouped.setdefault(key, []).append(p)

    for pair_key, pair_list in grouped.items():
        src, dst = pair_key.split('-')
        src_name = SUPPORTED_CHAINS.get(src, src).name if src in SUPPORTED_CHAINS else src
        dst_name = SUPPORTED_CHAINS.get(dst, dst).name if dst in SUPPORTED_CHAINS else dst

        console.print(f'[bold]{src_name} <-> {dst_name}[/bold]')

        table = Table(show_header=True)
        table.add_column('UID', style='cyan')
        table.add_column(f'{src.upper()}->TAO', style='green')
        table.add_column(f'TAO->{src.upper()}', style='green')
        table.add_column('Hotkey', style='dim')

        pair_list.sort(key=lambda x: x.rate, reverse=True)
        for p in pair_list:
            rev = f'{p.rate_reverse:g}' if p.rate_reverse_str else f'{p.rate:g}'
            table.add_row(str(p.uid), f'{p.rate:g}', rev, p.hotkey[:16] + '...')

        console.print(table)

        if len(pair_list) > 1:
            rates = [p.rate for p in pair_list]
            console.print(
                f'  [dim]Best: {max(rates):g} | Worst: {min(rates):g} | Avg: {sum(rates) / len(rates):.4f}[/dim]'
            )

        console.print()


@view_group.command('swaps')
@click.option('--status', default=None, type=str, help='Filter by status (active, fulfilled, completed, timed_out)')
def view_swaps(status: str):
    """View active swaps on the contract.

    Example:
        alw view swaps
        alw view swaps --status pending
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
        pair_str = f'{swap.source_chain.upper()}/{swap.dest_chain.upper()}'
        color = SWAP_STATUS_COLORS.get(swap.status, 'white')
        status_str = f'[{color}]{swap.status.name}[/{color}]'

        table.add_row(
            str(swap.id),
            pair_str,
            str(swap.source_amount),
            status_str,
            swap.miner_hotkey[:16] + '...',
            str(swap.initiated_block),
        )

    console.print(table)
    console.print(f'\n[dim]Total: {len(swaps)} swaps[/dim]\n')


def _build_swap_text(swap, chain_info=True):
    """Build swap display as a Rich markup string."""
    color = SWAP_STATUS_COLORS.get(swap.status, 'white')
    parts = [f'\n[bold]Swap #{swap.id}[/bold] — [{color}]{swap.status.name}[/{color}]\n']

    src = swap.source_chain.upper()
    dst = swap.dest_chain.upper()
    src_chain_def = get_chain(swap.source_chain)
    dst_chain_def = get_chain(swap.dest_chain)
    src_human = swap.source_amount / (10**src_chain_def.decimals)
    dst_human = swap.dest_amount / (10**dst_chain_def.decimals)
    parts.append(f'  {src} -> {dst} | {src_human:g} {src} -> {dst_human:.8f} {dst} | Rate: {swap.rate}')

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

    parts.append('')
    parts.append(f'  Source TX:  {swap.source_tx_hash or "—"}')
    parts.append(f'  Dest TX:   {swap.dest_tx_hash or "—"}')

    if chain_info:
        parts.append('')
        parts.append(f'  User:      {swap.user_hotkey}')
        parts.append(f'  Miner:     {swap.miner_hotkey}')
        parts.append(f'  Send to:   {swap.user_source_address}')
        parts.append(f'  Receive:   {swap.user_dest_address}')

    parts.append('')
    return '\n'.join(parts)


def _display_swap(swap, chain_info=True):
    """Render a single swap with timeline view."""
    console.print(_build_swap_text(swap, chain_info=chain_info))


@view_group.command('swap')
@click.argument('swap_id', type=int)
@click.option('--watch', '-w', is_flag=True, help='Poll and refresh until swap completes or times out')
def view_swap(swap_id: int, watch: bool):
    """View details of a specific swap.

    \b
    Examples:
        alw view swap 42
        alw view swap 42 --watch
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
            console.print(
                f'[green]Swap {swap_id} has been resolved (completed or timed out).[/green]\n'
                f'[dim]Resolved swaps are removed from on-chain storage.[/dim]'
            )
        elif next_id is not None:
            console.print(f'[red]Swap {swap_id} does not exist. Next swap ID: {next_id}.[/red]')
        else:
            console.print(f'[red]Swap {swap_id} not found[/red]')
        return

    if not watch:
        _display_swap(swap)
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
        _display_swap(swap)
        return swap

    def _render(s, chain_info=True, watching=True):
        markup = _build_swap_text(s, chain_info=chain_info)
        if watching:
            markup += '\n[dim]Watching for updates (Ctrl+C to stop)...[/dim]\n'
        return Text.from_markup(markup)

    last_swap = swap
    try:
        with Live(_render(swap), console=console, refresh_per_second=1) as live:
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
                    live.update(_render(final, chain_info=False, watching=False))
                    return final
                last_swap = swap
                live.update(_render(swap))
                if swap.status in terminal:
                    live.update(_render(swap, watching=False))
                    return swap
    except KeyboardInterrupt:
        console.print(f'\n[dim]Stopped watching. Resume with: alw view swap {swap_id} --watch[/dim]\n')
        return None


@view_group.command('contract')
def view_contract():
    """View contract parameters.

    Example:
        alw view contract
    """
    config, wallet, _, client = get_cli_context(need_wallet=False)

    console.print('\n[bold]Contract Parameters[/bold]\n')

    def _read(fn, default=None):
        try:
            return fn()
        except ContractError:
            if default is not None:
                return default
            raise

    try:
        with loading('Reading contract parameters...'):
            timeout_blocks = _read(client.get_fulfillment_timeout)
            timeout_minutes = timeout_blocks * SECONDS_PER_BLOCK / 60
            reservation_ttl_blocks = _read(client.get_reservation_ttl)
            reservation_ttl_minutes = reservation_ttl_blocks * SECONDS_PER_BLOCK / 60
            fee_divisor = _read(client.get_fee_divisor, default=0)
            consensus_threshold = _read(client.get_consensus_threshold)
            min_collateral_rao = _read(client.get_min_collateral)
            max_collateral_rao = _read(client.get_max_collateral)
            required_votes = _read(client.get_required_votes_count)
            validator_count = _read(client.get_validator_count)
            next_swap_id = _read(client.get_next_swap_id)
            min_swap_rao = _read(client.get_min_swap_amount)
            max_swap_rao = _read(client.get_max_swap_amount)
            accumulated_fees_rao = _read(client.get_accumulated_fees)
            total_recycled_rao = _read(client.get_total_recycled_fees)
            owner = _read(client.get_owner)
            recycle_address = _read(client.get_recycle_address, default=None)
    except ContractError as e:
        console.print(f'[red]Failed to read contract parameters: {e}[/red]')
        return

    table = Table(show_header=True)
    table.add_column('Parameter', style='cyan')
    table.add_column('Value', style='green')

    table.add_row('Fulfillment Timeout', f'{timeout_blocks} blocks (~{timeout_minutes:.0f} min)')
    table.add_row('Reservation TTL', f'{reservation_ttl_blocks} blocks (~{reservation_ttl_minutes:.0f} min)')
    if fee_divisor > 0:
        fee_pct = 100 / fee_divisor
        table.add_row('Fee', f'{fee_pct:g}% (divisor: {fee_divisor})')
    table.add_row('Consensus Threshold', f'{consensus_threshold}%')
    table.add_row('Min Collateral', f'{from_rao(min_collateral_rao):.4f} TAO')
    if max_collateral_rao > 0:
        table.add_row('Max Collateral', f'{from_rao(max_collateral_rao):.4f} TAO')
    else:
        table.add_row('Max Collateral', 'Unlimited')
    table.add_row('Min Swap Amount', f'{from_rao(min_swap_rao):.4f} TAO')
    table.add_row('Max Swap Amount', f'{from_rao(max_swap_rao):.4f} TAO')
    table.add_row('Required Validator Votes', f'{required_votes} (of {validator_count} validators)')
    table.add_row('Next Swap ID', str(next_swap_id))
    table.add_row('Accumulated Fees', f'{from_rao(accumulated_fees_rao):.4f} TAO')
    table.add_row('Total Recycled Fees', f'{from_rao(total_recycled_rao):.4f} TAO')
    table.add_row('Owner', owner)
    if recycle_address:
        table.add_row('Recycle Address', recycle_address)

    console.print(table)
    console.print()


@view_group.command('reservation')
def view_reservation():
    """View your active swap reservation.

    \b
    Reads local state file and validates against on-chain data.

    Example:
        alw view reservation
    """
    _, _, subtensor, client = get_cli_context(need_wallet=False)

    state = load_pending_swap()
    if not state:
        console.print('\n[yellow]No active reservation found.[/yellow]')
        console.print('[dim]Run `alw swap now` to initiate a swap.[/dim]\n')
        return

    # Validate against on-chain state
    try:
        with loading('Reading reservation...'):
            reserved_until = client.get_miner_reserved_until(state.miner_hotkey)
            current_block = subtensor.get_current_block()
    except ContractError as e:
        console.print(f'[red]Failed to read reservation status: {e}[/red]')
        return

    is_active = reserved_until > current_block

    console.print('\n[bold]Swap Reservation[/bold]\n')

    table = Table(show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    chain = get_chain(state.source_chain)
    human_send = state.source_amount / (10**chain.decimals)
    dest_chain_def = get_chain(state.dest_chain)
    human_receive = state.user_receives / (10**dest_chain_def.decimals)

    table.add_row('Pair', f'{state.source_chain.upper()} -> {state.dest_chain.upper()}')
    table.add_row('Send', f'{human_send} {state.source_chain.upper()}')
    table.add_row('To Address', state.miner_source_address)
    table.add_row('Receive', f'{human_receive:.8f} {state.dest_chain.upper()}')
    table.add_row('Receive Address', state.receive_address)
    table.add_row('Miner', f'UID {state.miner_uid} ({state.miner_hotkey[:16]}...)')

    if is_active:
        remaining = reserved_until - current_block
        remaining_min = remaining * SECONDS_PER_BLOCK / 60
        table.add_row('Status', '[green]ACTIVE[/green]')
        table.add_row('Time Remaining', f'~{remaining} blocks (~{remaining_min:.0f} min)')
    else:
        table.add_row('Status', '[red]EXPIRED[/red]')
        table.add_row('Time Remaining', 'Expired')

    console.print(table)

    if is_active:
        console.print(
            f'\n[bold]Next step:[/bold] Send {human_send} {state.source_chain.upper()} to the address above, then run:'
        )
        console.print('  [bold cyan]alw swap post-tx <your_transaction_hash>[/bold cyan]\n')
    else:
        console.print('\n[yellow]This reservation has expired. Run `alw swap now` to start a new one.[/yellow]')
        clear_pending_swap()
        console.print('[dim]Stale state file cleared.[/dim]\n')
