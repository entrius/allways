"""alw swap history - Show locally recorded swap history."""

import click
from rich.table import Table

from allways.chains import get_chain
from allways.cli.help import StyledCommand
from allways.classes import SwapStatus
from allways.cli.swap_commands.helpers import SWAP_HISTORY_FILE, SWAP_STATUS_COLORS, console, load_swap_history


@click.command('history', cls=StyledCommand)
@click.option('--limit', '-n', default=20, show_default=True, help='Number of entries to show')
@click.option('--clear', is_flag=True, help='Delete local swap history')
def history_command(limit: int, clear: bool):
    """Show locally recorded swap history.

    [dim]History is recorded whenever a swap completes or times out while
    being watched (alw swap now, alw view swap --watch). Resolved swaps
    are pruned from the contract, so this is the only local record.[/dim]

    [dim]Examples:
        $ alw swap history
        $ alw swap history -n 50
        $ alw swap history --clear[/dim]
    """
    if clear:
        if SWAP_HISTORY_FILE.exists():
            SWAP_HISTORY_FILE.unlink()
            console.print('Swap history cleared.')
        else:
            console.print('[dim]No history file to clear.[/dim]')
        return

    entries = load_swap_history()
    if not entries:
        console.print('\n[dim]No swap history recorded yet.[/dim]')
        console.print('[dim]History is saved when you watch a swap to completion with:[/dim]')
        console.print('[dim]  alw swap now   or   alw view swap <id> --watch[/dim]\n')
        return

    entries = list(reversed(entries))[:limit]

    table = Table(show_header=True, title=f'Swap History (last {len(entries)})')
    table.add_column('ID')
    table.add_column('Pair')
    table.add_column('Sent')
    table.add_column('Received')
    table.add_column('Status')
    table.add_column('Block', style='dim')

    for e in entries:
        from_chain = e.get('from_chain', '?')
        to_chain = e.get('to_chain', '?')
        pair_str = f'{from_chain.upper()}→{to_chain.upper()}'

        try:
            sent = f'{e["from_amount"] / 10**get_chain(from_chain).decimals:.8g} {from_chain.upper()}'
        except Exception:
            sent = str(e.get('from_amount', '?'))

        try:
            received = f'{e["to_amount"] / 10**get_chain(to_chain).decimals:.8g} {to_chain.upper()}'
        except Exception:
            received = str(e.get('to_amount', '?'))

        status = e.get('status', '?')
        try:
            color = SWAP_STATUS_COLORS.get(SwapStatus[status], 'white')
        except (KeyError, ValueError):
            color = 'white'
        status_str = f'[{color}]{status}[/{color}]'

        block = str(e.get('completed_block') or e.get('initiated_block', '?'))

        table.add_row(str(e.get('swap_id', '?')), pair_str, sent, received, status_str, block)

    console.print()
    console.print(table)
    console.print(f'\n[dim]History file: {SWAP_HISTORY_FILE}[/dim]\n')
