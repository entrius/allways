"""alw view - View swaps, miners, and rates."""

import os
import time
from dataclasses import replace

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
from allways.contract_client import ContractError

DEFAULT_DASHBOARD_URL = 'https://test.all-ways.io'


@click.group('view', cls=StyledGroup)
def view_group():
    """View swaps, miners, and rates."""
    pass


@view_group.command('miners')
@click.option('--full', is_flag=True, help='Show untruncated addresses and hotkeys')
def view_miners(full: bool):
    """View active miners and their trading pairs.

    [dim]Status column shows live runtime state per miner: reserved (with
    locked TAO amount), has-swap, or cooldown (withdrawal cooldown blocks
    remaining after deactivation).[/dim]

    [dim]Examples:
        $ alw view miners
        $ alw view miners --full[/dim]
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

    # Withdrawal cooldown = 2 * fulfillment_timeout_blocks after deactivation.
    try:
        fulfillment_timeout = client.get_fulfillment_timeout()
        current_block = subtensor.get_current_block()
    except ContractError:
        fulfillment_timeout = 0
        current_block = 0

    table = Table(show_header=True)
    table.add_column('UID', style='cyan')
    table.add_column(f'{src_up}→{dst_up}', style='green')
    table.add_column(f'{dst_up}→{src_up}', style='green')
    table.add_column('Collateral (TAO)', style='magenta')
    table.add_column('Active', style='bold')
    table.add_column('Status', style='yellow')
    table.add_column(f'{src_up} Addr', style='dim')
    table.add_column(f'{dst_up} Addr', style='dim')

    def _trunc(s: str) -> str:
        if full or not s:
            return s
        return s[:16] + '...' if len(s) > 16 else s

    for pair in pairs:
        try:
            collateral_rao, is_active, has_swap, reserved_until, deactivation_block = client.get_miner_snapshot(
                pair.hotkey
            )
            collateral_str = f'{from_rao(collateral_rao):.4f}'
            active_str = '[green]Yes[/green]' if is_active else '[red]No[/red]'
        except ContractError:
            collateral_str = '[dim]—[/dim]'
            active_str = '[dim]—[/dim]'
            reserved_until = 0
            deactivation_block = 0
            has_swap = False

        status_parts = []
        if has_swap:
            status_parts.append('[blue]in-swap[/blue]')
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
        if deactivation_block and fulfillment_timeout and current_block:
            cooldown_end = deactivation_block + 2 * fulfillment_timeout
            remaining = cooldown_end - current_block
            if remaining > 0:
                status_parts.append(f'[red]cooldown {remaining}b[/red]')

        if status_parts:
            status_str = ' · '.join(status_parts)
        elif is_active:
            status_str = '[green]available[/green]'
        else:
            status_str = '[dim]offline[/dim]'

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
            status_str,
            _trunc(pair.from_address),
            _trunc(pair.to_address),
        )

    console.print(table)
    console.print(f'\n[dim]Total miners: {len(pairs)}[/dim]\n')
    if not full:
        console.print('[dim]Use --full to show untruncated addresses.[/dim]\n')


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
        display_swap(swap)
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
                    return final
                last_swap = swap
                live.update(render(swap))
                if swap.status in terminal:
                    live.update(render(swap, watching=False))
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
    table.add_row('System Status', '[red]HALTED[/red]' if halted else '[green]Running[/green]')

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

    # Validate against on-chain state. When the reservation struct is gone,
    # disambiguate expired-by-TTL from consumed-by-vote_initiate by probing
    # the miner for an active swap we can match back to this reservation.
    consumed_swap = None
    try:
        with loading('Reading reservation...'):
            reserved_until = client.get_miner_reserved_until(state.miner_hotkey)
            current_block = subtensor.get_current_block()
            on_chain_reservation = client.get_reservation_data(state.miner_hotkey)
            if reserved_until == 0 and on_chain_reservation is None:
                if client.get_miner_has_active_swap(state.miner_hotkey):
                    for swap in client.get_miner_active_swaps(state.miner_hotkey):
                        if (
                            swap.user_from_address == state.user_from_address
                            or swap.user_to_address == state.receive_address
                        ):
                            consumed_swap = swap
                            break
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

    if on_chain_reservation:
        chain_tao, chain_from, chain_to = on_chain_reservation
        mismatch = chain_tao != state.tao_amount or chain_from != state.from_amount or chain_to != state.to_amount
        if mismatch:
            table.add_row('Chain Amounts', '[red]mismatch with local state[/red]')
        else:
            table.add_row('Chain Amounts', f'[green]✓ locked {from_rao(chain_tao):.4f} TAO[/green]')

    if is_active:
        remaining = reserved_until - current_block
        remaining_min = remaining * SECONDS_PER_BLOCK / 60
        table.add_row('Status', '[green]ACTIVE[/green]')
        table.add_row('Time Remaining', f'~{remaining} blocks (~{remaining_min:.0f} min)')
    elif consumed_swap is not None:
        table.add_row('Status', f'[green]INITIATED (swap #{consumed_swap.id})[/green]')
        table.add_row('Swap Status', consumed_swap.status.name)
    else:
        table.add_row('Status', '[yellow]NO LONGER ACTIVE[/yellow]')
        table.add_row('Time Remaining', '—')

    console.print(table)

    if is_active:
        console.print(
            f'\n[bold]Next step:[/bold] Send {human_send} {state.from_chain.upper()} to the address above, then run:'
        )
        console.print('  [bold cyan]alw swap post-tx <your_transaction_hash>[/bold cyan]\n')
    elif consumed_swap is not None:
        console.print(
            f'\n[green]Reservation was consumed into swap #{consumed_swap.id} — it is in progress on-chain.[/green]'
        )
        console.print(f'[dim]Watch with: alw view swap {consumed_swap.id} --watch[/dim]')
        clear_pending_swap()
        console.print('[dim]Local reservation state cleared.[/dim]\n')
    else:
        console.print(
            '\n[yellow]Reservation is no longer active on-chain.[/yellow]\n'
            '[dim]Either the reservation expired before you sent funds, or your swap already '
            'initiated and has since completed. Check: alw view swaps[/dim]'
        )
        clear_pending_swap()
        console.print('[dim]Local reservation state cleared.[/dim]\n')
