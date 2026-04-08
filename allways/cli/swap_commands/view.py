"""alw view - View swaps, miners, and rates."""

import time
from dataclasses import replace
from datetime import datetime, timezone

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
    load_pending_swap,
    loading,
    read_miner_commitments,
)
from allways.constants import FEE_DIVISOR
from allways.cli.swap_commands.history_store import get_history, get_receipt, upsert_history
from allways.contract_client import ContractError


@click.group('view', cls=StyledGroup)
def view_group():
    """View swaps, miners, and rates."""
    pass


@view_group.command('miners')
def view_miners():
    """View active miners and their trading pairs.

    [dim]Examples:
        $ alw view miners[/dim]
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

    table = Table(show_header=True)
    table.add_column('UID', style='cyan')
    table.add_column(f'{src_up}→{dst_up}', style='green')
    table.add_column(f'{dst_up}→{src_up}', style='green')
    table.add_column('Collateral (TAO)', style='magenta')
    table.add_column('Active', style='bold')
    table.add_column(f'{src_up} Addr', style='dim')
    table.add_column(f'{dst_up} Addr', style='dim')

    for pair in pairs:
        try:
            collateral_rao = client.get_miner_collateral(pair.hotkey)
            collateral_str = f'{from_rao(collateral_rao):.4f}'
        except ContractError:
            collateral_str = '[dim]—[/dim]'

        try:
            is_active = client.get_miner_active_flag(pair.hotkey)
            active_str = '[green]Yes[/green]' if is_active else '[red]No[/red]'
        except ContractError:
            active_str = '[dim]—[/dim]'

        fwd_display = f'{pair.rate:g}' if pair.rate > 0 else '[dim]—[/dim]'
        if pair.counter_rate > 0:
            ctr_display = f'{pair.counter_rate:g}'
        elif pair.counter_rate_str:
            ctr_display = '[dim]—[/dim]'
        else:
            ctr_display = f'{pair.rate:g}'

        table.add_row(
            str(pair.uid),
            fwd_display,
            ctr_display,
            collateral_str,
            active_str,
            pair.from_address[:16] + '...',
            pair.to_address[:16] + '...',
        )

    console.print(table)
    console.print(f'\n[dim]Total miners: {len(pairs)}[/dim]\n')


@view_group.command('rates')
@click.option('--pair', default=None, type=str, help='Filter by pair (e.g. btc-tao)')
def view_rates(pair: str):
    """View current exchange rates.

    [dim]Examples:
        $ alw view rates
        $ alw view rates --pair btc-tao[/dim]
    """
    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    console.print(f'\n[bold]Exchange Rates on SN{netuid}[/bold]\n')

    with loading('Reading rates...'):
        all_pairs = read_miner_commitments(subtensor, netuid)

        # Only show miners that are active and have collateral (i.e. swappable)
        pairs = []
        for p in all_pairs:
            try:
                is_active = client.get_miner_active_flag(p.hotkey)
                collateral = client.get_miner_collateral(p.hotkey)
                if is_active and collateral > 0:
                    pairs.append(p)
            except ContractError:
                continue

    if pair:
        parts = pair.lower().split('-')
        if len(parts) != 2:
            console.print('[red]Invalid pair format. Use: chain-chain (e.g. btc-tao)[/red]')
            return
        src, dst = parts
        pairs = [p for p in pairs if p.from_chain == src and p.to_chain == dst]

    if not pairs:
        console.print('[yellow]No rates found[/yellow]\n')
        return

    # Group by pair direction
    grouped = {}
    for p in pairs:
        key = f'{p.from_chain}-{p.to_chain}'
        grouped.setdefault(key, []).append(p)

    for pair_key, pair_list in grouped.items():
        src, dst = pair_key.split('-')
        src_name = SUPPORTED_CHAINS.get(src, src).name if src in SUPPORTED_CHAINS else src
        dst_name = SUPPORTED_CHAINS.get(dst, dst).name if dst in SUPPORTED_CHAINS else dst

        console.print(f'[bold]{src_name} ↔ {dst_name}[/bold]')

        table = Table(show_header=True)
        table.add_column('UID', style='cyan')
        table.add_column(f'{src.upper()}→{dst.upper()}', style='green')
        table.add_column(f'{dst.upper()}→{src.upper()}', style='green')
        table.add_column('Hotkey', style='dim')

        # Sort by the stronger of the two rates so reverse-only miners aren't
        # buried at the bottom with rate=0.
        pair_list.sort(key=lambda x: max(x.rate, x.counter_rate), reverse=True)
        for p in pair_list:
            fwd = f'{p.rate:g}' if p.rate > 0 else '—'
            if p.counter_rate > 0:
                rev = f'{p.counter_rate:g}'
            elif p.counter_rate_str:
                rev = '—'
            else:
                rev = fwd
            table.add_row(str(p.uid), fwd, rev, p.hotkey[:16] + '...')

        console.print(table)

        # Per-direction stats — excluding miners that don't quote that direction,
        # so a single reverse-only miner doesn't drag the forward average to zero.
        fwd_rates = [p.rate for p in pair_list if p.rate > 0]
        rev_rates = [p.counter_rate for p in pair_list if p.counter_rate > 0]
        stat_lines = []
        if len(fwd_rates) > 1:
            stat_lines.append(
                f'{src.upper()}→{dst.upper()}: best {max(fwd_rates):g} | worst {min(fwd_rates):g} | '
                f'avg {sum(fwd_rates) / len(fwd_rates):.4f}'
            )
        if len(rev_rates) > 1:
            stat_lines.append(
                f'{dst.upper()}→{src.upper()}: best {max(rev_rates):g} | worst {min(rev_rates):g} | '
                f'avg {sum(rev_rates) / len(rev_rates):.4f}'
            )
        for line in stat_lines:
            console.print(f'  [dim]{line}[/dim]')

        console.print()


@view_group.command('swaps')
@click.option(
    '--status',
    default=None,
    type=click.Choice(['active', 'fulfilled', 'completed', 'timed_out'], case_sensitive=False),
    help='Filter by status (active, fulfilled, completed, timed_out)',
)
def view_swaps(status: str):
    """View active swaps on the contract.

    [dim]Examples:
        $ alw view swaps
        $ alw view swaps --status active[/dim]
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


def build_swap_text(swap, chain_info=True):
    """Build swap display as a Rich markup string."""
    color = SWAP_STATUS_COLORS.get(swap.status, 'white')
    parts = [f'\n[bold]Swap #{swap.id}[/bold] — [{color}]{swap.status.name}[/{color}]\n']

    src = swap.from_chain.upper()
    dst = swap.to_chain.upper()
    src_chain_def = get_chain(swap.from_chain)
    dst_chain_def = get_chain(swap.to_chain)
    src_human = swap.from_amount / (10**src_chain_def.decimals)
    dst_human = swap.to_amount / (10**dst_chain_def.decimals)
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
    parts.append(f'  Source TX:  {swap.from_tx_hash or "—"}')
    parts.append(f'  Dest TX:   {swap.to_tx_hash or "—"}')

    if chain_info:
        parts.append('')
        parts.append(f'  User:      {swap.user_hotkey}')
        parts.append(f'  Miner:     {swap.miner_hotkey}')
        parts.append(f'  Send to:   {swap.user_from_address}')
        parts.append(f'  Receive:   {swap.user_to_address}')

    parts.append('')
    return '\n'.join(parts)


def display_swap(swap, chain_info=True):
    """Render a single swap with timeline view."""
    console.print(build_swap_text(swap, chain_info=chain_info))


def _persist_swap_state(swap) -> None:
    upsert_history(
        swap_id=swap.id,
        data={
            'status': swap.status.name,
            'source_chain': swap.from_chain,
            'dest_chain': swap.to_chain,
            'source_amount': swap.from_amount,
            'dest_amount': swap.to_amount,
            'tao_amount': swap.tao_amount,
            'user_source_address': swap.user_from_address,
            'user_dest_address': swap.user_to_address,
            'miner_hotkey': swap.miner_hotkey,
            'source_tx_hash': swap.from_tx_hash,
            'dest_tx_hash': swap.to_tx_hash,
            'initiated_block': swap.initiated_block,
            'fulfilled_block': swap.fulfilled_block,
            'completed_block': swap.completed_block,
            'timeout_block': swap.timeout_block,
        },
    )


@view_group.command('swap')
@click.argument('swap_id', type=int)
@click.option('--watch', '-w', is_flag=True, help='Poll and refresh until swap completes or times out')
def view_swap(swap_id: int, watch: bool):
    """View details of a specific swap.

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
        display_swap(swap)
        _persist_swap_state(swap)
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
        _persist_swap_state(swap)
        return swap

    def render(s, chain_info=True, watching=True):
        markup = build_swap_text(s, chain_info=chain_info)
        if watching:
            markup += '\n[dim]Watching for updates (Ctrl+C to stop)...[/dim]\n'
        return Text.from_markup(markup)

    last_swap = swap
    try:
        with Live(render(swap), console=console, refresh_per_second=1) as live:
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
                    live.update(render(final, chain_info=False, watching=False))
                    _persist_swap_state(final)
                    return final
                last_swap = swap
                live.update(render(swap))
                if swap.status in terminal:
                    live.update(render(swap, watching=False))
                    _persist_swap_state(swap)
                    return swap
    except KeyboardInterrupt:
        console.print(f'\n[dim]Stopped watching. Resume with: alw view swap {swap_id} --watch[/dim]\n')
        return None


def _fmt_ts(ts: int) -> str:
    if not ts:
        return '—'
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


@view_group.command('history')
@click.option('--limit', default=20, type=int, help='Max records to show')
@click.option(
    '--status',
    default=None,
    type=click.Choice(
        ['reserved', 'confirm_submitted', 'active', 'fulfilled', 'completed', 'timed_out'],
        case_sensitive=False,
    ),
    help='Filter by status',
)
def view_history(limit: int, status: str):
    """View locally persisted swap history.

    [dim]Examples:
        $ alw view history
        $ alw view history --status completed --limit 50[/dim]
    """
    records = get_history(limit=max(1, limit), status=status)
    if not records:
        console.print('[yellow]No local swap history found.[/yellow]')
        console.print('[dim]History is recorded as you run swap commands in this CLI.[/dim]\n')
        return

    table = Table(show_header=True)
    table.add_column('Swap ID', style='cyan')
    table.add_column('Pair', style='green')
    table.add_column('Amount', style='yellow')
    table.add_column('Status', style='bold')
    table.add_column('Updated', style='dim')

    for rec in records:
        swap_id = rec.get('swap_id')
        pair = f'{rec.get("source_chain", "?").upper()}/{rec.get("dest_chain", "?").upper()}'
        amount = rec.get('source_amount', 0)
        amount_str = str(amount) if amount else '—'
        table.add_row(
            str(swap_id) if swap_id is not None else 'pending',
            pair,
            amount_str,
            str(rec.get('status', 'UNKNOWN')).upper(),
            _fmt_ts(int(rec.get('updated_at', 0))),
        )

    console.print()
    console.print(table)
    console.print(f'\n[dim]Showing {len(records)} record(s)[/dim]\n')


@view_group.command('receipt')
@click.argument('swap_id', type=int)
def view_receipt(swap_id: int):
    """View a local receipt for a swap ID.

    [dim]Examples:
        $ alw view receipt 42[/dim]
    """
    rec = get_receipt(swap_id)
    if not rec:
        console.print(f'[yellow]No local receipt found for swap {swap_id}.[/yellow]')
        console.print('[dim]Try `alw view swap <id>` first to cache it locally.[/dim]\n')
        return

    status = str(rec.get('status', 'UNKNOWN')).upper()
    console.print(f'\n[bold]Swap Receipt #{swap_id}[/bold] — {status}\n')
    console.print(f'  Pair:       {rec.get("source_chain", "?").upper()} -> {rec.get("dest_chain", "?").upper()}')
    console.print(f'  Sent:       {rec.get("source_amount", "—")}')
    console.print(f'  Received:   {rec.get("dest_amount", "—")}')
    console.print(f'  Source TX:  {rec.get("source_tx_hash") or "—"}')
    console.print(f'  Dest TX:    {rec.get("dest_tx_hash") or "—"}')
    console.print(f'  Initiated:  Block {rec.get("initiated_block", 0) or "—"}')
    console.print(f'  Fulfilled:  Block {rec.get("fulfilled_block", 0) or "—"}')
    console.print(f'  Completed:  Block {rec.get("completed_block", 0) or "—"}')
    console.print(f'  Timeout:    Block {rec.get("timeout_block", 0) or "—"}')
    console.print(f'  Updated:    {_fmt_ts(int(rec.get("updated_at", 0)))}\n')


@view_group.command('contract')
def view_contract():
    """View contract parameters.

    [dim]Examples:
        $ alw view contract[/dim]
    """
    config, wallet, _, client = get_cli_context(need_wallet=False)

    console.print('\n[bold]Contract Parameters[/bold]\n')

    def read_safe(fn, default=None):
        try:
            return fn()
        except ContractError:
            if default is not None:
                return default
            raise

    try:
        with loading('Reading contract parameters...'):
            timeout_blocks = read_safe(client.get_fulfillment_timeout)
            timeout_minutes = timeout_blocks * SECONDS_PER_BLOCK / 60
            reservation_ttl_blocks = read_safe(client.get_reservation_ttl)
            reservation_ttl_minutes = reservation_ttl_blocks * SECONDS_PER_BLOCK / 60
            consensus_threshold = read_safe(client.get_consensus_threshold)
            min_collateral_rao = read_safe(client.get_min_collateral)
            max_collateral_rao = read_safe(client.get_max_collateral)
            validator_count = read_safe(client.get_validator_count)
            required_votes = max(1, (validator_count * consensus_threshold + 99) // 100) if validator_count > 0 else 1
            next_swap_id = read_safe(client.get_next_swap_id)
            min_swap_rao = read_safe(client.get_min_swap_amount)
            max_swap_rao = read_safe(client.get_max_swap_amount)
            accumulated_fees_rao = read_safe(client.get_accumulated_fees)
            total_recycled_rao = read_safe(client.get_total_recycled_fees)
            owner = read_safe(client.get_owner)
            recycle_address = read_safe(client.get_recycle_address, default=None)
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

    chain = get_chain(state.from_chain)
    human_send = state.from_amount / (10**chain.decimals)
    to_chain_def = get_chain(state.to_chain)
    human_receive = state.user_receives / (10**to_chain_def.decimals)

    table.add_row('Pair', f'{state.from_chain.upper()} -> {state.to_chain.upper()}')
    table.add_row('Send', f'{human_send} {state.from_chain.upper()}')
    table.add_row('To Address', state.miner_from_address)
    table.add_row('Receive', f'{human_receive:.8f} {state.to_chain.upper()}')
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
            f'\n[bold]Next step:[/bold] Send {human_send} {state.from_chain.upper()} to the address above, then run:'
        )
        console.print('  [bold cyan]alw swap post-tx <your_transaction_hash>[/bold cyan]\n')
    else:
        console.print('\n[yellow]This reservation has expired. Run `alw swap now` to start a new one.[/yellow]')
        clear_pending_swap()
        console.print('[dim]Stale state file cleared.[/dim]\n')
