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
from allways.utils.rate import apply_fee_deduction, calculate_to_amount, check_swap_viability, derive_tao_leg


@click.command('quote')
@click.option('--from', 'from_chain', default=None, type=str, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain', default=None, type=str, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', default=None, type=float, help='Amount to send in source chain units')
def quote_command(from_chain: str, to_chain: str, amount: float):
    """Preview rates and estimated receive amounts for a swap.

    \b
    Shows all available miners, their rates, and what you would receive
    after fees — without committing to a swap. Omit any flag to be prompted.

    \b
    Examples:
        alw swap quote
        alw swap quote --from btc --to tao --amount 0.1
        alw swap quote --from tao --to btc --amount 50
    """
    supported = sorted(SUPPORTED_CHAINS.keys())
    chain_choices = click.Choice(supported, case_sensitive=False)

    if not from_chain:
        from_chain = click.prompt(f'Source chain ({"/".join(supported)})', type=chain_choices)
    from_chain = from_chain.lower()
    if from_chain not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown source chain: {from_chain}[/red]')
        return

    if not to_chain:
        remaining = [c for c in supported if c != from_chain]
        default_to = remaining[0] if remaining else None
        to_chain = click.prompt(
            f'Destination chain ({"/".join(remaining) if remaining else ""})',
            type=click.Choice(remaining, case_sensitive=False) if remaining else chain_choices,
            default=default_to,
        )
    to_chain = to_chain.lower()
    if to_chain not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown destination chain: {to_chain}[/red]')
        return
    if from_chain == to_chain:
        console.print('[red]Source and destination chains must be different[/red]')
        return

    if amount is None:
        amount = click.prompt(f'Amount to send ({from_chain.upper()})', type=float)
    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    config, _, subtensor, client = get_cli_context(need_wallet=False)
    netuid = config['netuid']

    # Convert to smallest units
    src_chain_def = get_chain(from_chain)
    from_amount = int(Decimal(str(amount)) * (10**src_chain_def.decimals))

    fee_divisor = FEE_DIVISOR
    fee_pct = 100 / fee_divisor

    # Find available miners
    with loading('Reading rates...'):
        all_pairs = read_miner_commitments(subtensor, netuid)
        matching = find_matching_miners(all_pairs, from_chain, to_chain)

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
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    is_reverse = from_chain != canon_from
    canon_to_decimals = get_chain(canon_to).decimals
    canon_from_decimals = get_chain(canon_from).decimals
    dst_chain_def = get_chain(to_chain)

    # Contract-side bounds are global — check before per-miner viability so
    # a user who requested an out-of-bounds amount gets one clear reason
    # instead of N rows each blaming collateral. Bounds are enforced against
    # the TAO leg (see vote_reserve in lib.rs).
    try:
        min_swap_rao = client.get_min_swap_amount()
        max_swap_rao = client.get_max_swap_amount()
    except ContractError:
        min_swap_rao = 0
        max_swap_rao = 0

    # Global bounds (min/max swap) are the same for every miner — check once
    # up front so a bounds-violating amount gets one clear reason instead of
    # N rows each blaming collateral. Use any row's rate to derive the TAO
    # leg; only the direction matters, not the miner.
    sample_pair = available[0][0]
    sample_to_amount = calculate_to_amount(
        from_amount, sample_pair.rate_str, is_reverse, canon_to_decimals, canon_from_decimals
    )
    request_tao_rao = derive_tao_leg(from_chain, from_amount, to_chain, sample_to_amount)

    if min_swap_rao > 0 and request_tao_rao < min_swap_rao:
        console.print(
            f'\n[red]Amount below contract minimum: {from_rao(request_tao_rao):.4f} TAO equivalent '
            f'< {from_rao(min_swap_rao):.4f} TAO min.[/red]\n'
            f'[dim]No miner can accept this — increase --amount.[/dim]\n'
        )
        return
    if max_swap_rao > 0 and request_tao_rao > max_swap_rao:
        console.print(
            f'\n[red]Amount above contract maximum: {from_rao(request_tao_rao):.4f} TAO equivalent '
            f'> {from_rao(max_swap_rao):.4f} TAO max.[/red]\n'
            f'[dim]No miner can accept this — decrease --amount.[/dim]\n'
        )
        return

    console.print(f'\n[bold]Quote: {amount} {from_chain.upper()} -> {to_chain.upper()}[/bold]\n')

    table = Table(show_header=True)
    table.add_column('#', style='dim')
    table.add_column('UID', style='cyan')
    table.add_column(f'Rate ({to_chain.upper()}/{from_chain.upper()})', style='green')
    table.add_column('You Receive', style='bold green')
    table.add_column('Collateral', style='yellow')
    table.add_column('Status', style='bold')

    viable_count = 0
    for idx, (pair, collateral) in enumerate(available, 1):
        to_amount = calculate_to_amount(from_amount, pair.rate_str, is_reverse, canon_to_decimals, canon_from_decimals)
        user_receives = apply_fee_deduction(to_amount, fee_divisor)
        human_receives = user_receives / (10**dst_chain_def.decimals)

        tao_amount_rao = derive_tao_leg(from_chain, from_amount, to_chain, to_amount)
        viable, reason = check_swap_viability(tao_amount_rao, collateral, min_swap_rao, max_swap_rao)
        if viable:
            status = '[green]available[/green]'
            viable_count += 1
        else:
            status = f'[red]{reason}[/red]'

        table.add_row(
            str(idx),
            str(pair.uid),
            f'{pair.rate:g}',
            f'{human_receives:.8f} {to_chain.upper()}',
            f'{from_rao(collateral):.4f} TAO',
            status,
        )

    console.print(table)
    console.print(f'  [dim](after {fee_pct:g}% fee)[/dim]')
    if viable_count == 0:
        console.print(
            '  [yellow]No miner can fulfill this swap at the requested amount — try a smaller amount '
            'or wait for more collateral to be posted.[/yellow]\n'
        )
    else:
        console.print()
