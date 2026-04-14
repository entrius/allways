"""alw swap quote - Preview rates and estimated receive amounts before swapping."""

from decimal import Decimal

import rich_click as click
from rich.table import Table

from allways.chains import SUPPORTED_CHAINS, canonical_pair, get_chain
from allways.cli.swap_commands.helpers import (
    console,
    find_matching_miners,
    from_rao,
    get_cli_context,
    loading,
    read_miner_commitments,
)
from allways.constants import FEE_DIVISOR
from allways.contract_client import ContractError
from allways.utils.rate import apply_fee_deduction, calculate_dest_amount


@click.command('quote')
@click.option('--from', 'source_chain', required=True, type=str, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'dest_chain', required=True, type=str, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', required=True, type=float, help='Amount to send in source chain units')
def quote_command(source_chain: str, dest_chain: str, amount: float):
    """Preview rates and estimated receive amounts for a swap.

    \b
    Shows all available miners, their rates, and what you would receive
    after fees — without committing to a swap.

    \b
    Examples:
        alw swap quote --from btc --to tao --amount 0.1
        alw swap quote --from tao --to btc --amount 50
    """
    source_chain = source_chain.lower()
    dest_chain = dest_chain.lower()

    if source_chain not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown source chain: {source_chain}[/red]')
        return
    if dest_chain not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown destination chain: {dest_chain}[/red]')
        return
    if source_chain == dest_chain:
        console.print('[red]Source and destination chains must be different[/red]')
        return
    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    # Convert to smallest units
    src_chain_def = get_chain(source_chain)
    source_amount = int(Decimal(str(amount)) * (10**src_chain_def.decimals))

    fee_divisor = FEE_DIVISOR
    fee_pct = 100 / fee_divisor

    # Find available miners
    with loading('Reading rates...'):
        all_pairs = read_miner_commitments(subtensor, netuid)
        matching = find_matching_miners(all_pairs, source_chain, dest_chain)

        available = []
        for pair in matching:
            try:
                is_active = client.get_miner_active_flag(pair.hotkey)
                has_swap = client.get_miner_has_active_swap(pair.hotkey)
                collateral = client.get_miner_collateral(pair.hotkey)
                if is_active and not has_swap and collateral > 0:
                    available.append((pair, collateral))
            except ContractError:
                continue

    if not available:
        console.print('[yellow]No active miners available for this pair[/yellow]\n')
        return

    available.sort(key=lambda x: x[0].rate, reverse=True)

    # Calculate amounts per miner
    canon_src, canon_dest = canonical_pair(source_chain, dest_chain)
    is_reverse = source_chain != canon_src
    canon_dest_decimals = get_chain(canon_dest).decimals
    canon_src_decimals = get_chain(canon_src).decimals
    dst_chain_def = get_chain(dest_chain)

    console.print(f'\n[bold]Quote: {amount} {source_chain.upper()} -> {dest_chain.upper()}[/bold]\n')

    table = Table(show_header=True)
    table.add_column('#', style='dim')
    table.add_column('UID', style='cyan')
    table.add_column(f'Rate ({dest_chain.upper()}/{source_chain.upper()})', style='green')
    table.add_column('You Receive', style='bold green')
    table.add_column('Collateral', style='yellow')

    for idx, (pair, collateral) in enumerate(available, 1):
        dest_amount = calculate_dest_amount(
            source_amount, pair.rate_str, is_reverse, canon_dest_decimals, canon_src_decimals
        )
        user_receives = apply_fee_deduction(dest_amount, fee_divisor)
        human_receives = user_receives / (10**dst_chain_def.decimals)

        table.add_row(
            str(idx),
            str(pair.uid),
            f'{pair.rate:g}',
            f'{human_receives:.8f} {dest_chain.upper()}',
            f'{from_rao(collateral):.4f} TAO',
        )

    console.print(table)
    console.print(f'  [dim](after {fee_pct:g}% fee)[/dim]\n')
