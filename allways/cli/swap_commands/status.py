"""alw status - Quick dashboard showing network, wallet, and swap state."""

import click
from rich.table import Table

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    console,
    from_rao,
    get_cli_context,
    load_pending_swap,
    loading,
    read_miner_commitments,
)
from allways.constants import NETUID_FINNEY
from allways.contract_client import ContractError


@click.command('status', cls=StyledCommand)
@click.option('--netuid', default=None, type=int, help='Subnet UID')
def status_command(netuid: int):
    """Show a quick dashboard of your current state.

    [dim]Displays network info, wallet balance, active swaps,
    pending reservations, and miner status (if applicable).[/dim]

    [dim]Examples:
        $ alw status[/dim]
    """
    config, wallet, subtensor, client = get_cli_context()
    if netuid is None:
        netuid = int(config.get('netuid', NETUID_FINNEY))

    network = config.get('network', 'finney')
    hotkey = wallet.hotkey.ss58_address

    console.print('\n[bold]Allways Status[/bold]\n')

    table = Table(show_header=False, pad_edge=False, box=None)
    table.add_column(style='cyan', min_width=20)
    table.add_column(style='green')

    with loading('Loading status...'):
        # Network
        try:
            current_block = subtensor.get_current_block()
            table.add_row('Network', f'{network} (block {current_block})')
        except Exception:
            table.add_row('Network', network)

        # Wallet
        table.add_row('Wallet', f'{wallet.name} / {wallet.hotkey_str}')
        try:
            account_info = subtensor.substrate.query('System', 'Account', [wallet.coldkey.ss58_address])
            account_data = account_info.value if hasattr(account_info, 'value') else account_info
            free_balance = account_data.get('data', {}).get('free', 0)
            table.add_row('TAO Balance', f'{from_rao(free_balance):.4f} TAO')
        except Exception:
            table.add_row('TAO Balance', '[dim]unable to read[/dim]')

        # Active swaps
        try:
            active_swaps = client.get_active_swaps()
            my_swaps = [s for s in active_swaps if s.user_hotkey == wallet.coldkey.ss58_address]
            if my_swaps:
                table.add_row('Your Active Swaps', str(len(my_swaps)))
                for s in my_swaps:
                    table.add_row('', f'  #{s.id} {s.source_chain.upper()}->{s.dest_chain.upper()} [{s.status.name}]')
            else:
                table.add_row('Your Active Swaps', 'None')
        except ContractError:
            table.add_row('Your Active Swaps', '[dim]unable to read[/dim]')

        # Pending reservation
        pending = load_pending_swap()
        if pending:
            try:
                reserved_until = client.get_miner_reserved_until(pending.miner_hotkey)
                if reserved_until > subtensor.get_current_block():
                    remaining = reserved_until - subtensor.get_current_block()
                    remaining_min = remaining * SECONDS_PER_BLOCK / 60
                    table.add_row(
                        'Pending Reservation',
                        f'{pending.source_chain.upper()}->{pending.dest_chain.upper()} (~{remaining_min:.0f} min left)',
                    )
                else:
                    table.add_row('Pending Reservation', '[dim]Expired[/dim]')
            except ContractError:
                table.add_row('Pending Reservation', '[dim]unable to verify[/dim]')
        else:
            table.add_row('Pending Reservation', 'None')

        # Miner status
        try:
            collateral = client.get_miner_collateral(hotkey)
            if collateral > 0:
                is_active = client.get_miner_active_flag(hotkey)
                has_swap = client.get_miner_has_active_swap(hotkey)
                status_str = '[green]Active[/green]' if is_active else '[red]Inactive[/red]'
                if has_swap:
                    status_str += ' (has active swap)'
                table.add_row('Miner Status', status_str)
                table.add_row('Miner Collateral', f'{from_rao(collateral):.4f} TAO')

                pairs = read_miner_commitments(subtensor, netuid)
                my_pairs = [p for p in pairs if p.hotkey == hotkey]
                if my_pairs:
                    for p in my_pairs:
                        src_up, dst_up = p.source_chain.upper(), p.dest_chain.upper()
                        if p.rate > 0 and p.counter_rate > 0 and p.rate_str != p.counter_rate_str:
                            rate_display = f'{src_up}→{dst_up}: {p.rate:g} | {dst_up}→{src_up}: {p.counter_rate:g}'
                        elif p.rate > 0:
                            rate_display = f'{p.rate:g}'
                        else:
                            rate_display = f'{p.counter_rate:g}'
                        table.add_row('Miner Pair', f'{src_up} ↔ {dst_up} @ {rate_display}')
        except ContractError:
            table.add_row('Miner Status', '[dim]unable to read[/dim]')

    console.print(table)
    console.print()
