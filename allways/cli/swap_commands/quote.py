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
    if from_chain and to_chain:
        from_chain = from_chain.lower()
        to_chain = to_chain.lower()
        if from_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unknown source chain: {from_chain}[/red]')
            return
        if to_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unknown destination chain: {to_chain}[/red]')
            return
        if from_chain == to_chain:
            console.print('[red]Source and destination chains must be different[/red]')
            return
    else:
        chain_ids = list(SUPPORTED_CHAINS.keys())
        directions = [(s, d) for s in chain_ids for d in chain_ids if s != d]

        console.print('\n[bold]Allways Swap Quote[/bold]\n')
        console.print('[bold]What would you like to swap?[/bold]\n')
        for idx, (src, dst) in enumerate(directions, 1):
            console.print(f'  {idx}. {SUPPORTED_CHAINS[src].name} -> {SUPPORTED_CHAINS[dst].name}')

        choice = click.prompt('\nSelect', type=int, default=1)
        if choice < 1 or choice > len(directions):
            console.print('[red]Invalid selection[/red]')
            return
        from_chain, to_chain = directions[choice - 1]

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

        # Hide truly inactive miners — they've taken themselves off the subnet
        # and we can't know when they'll be back, so showing their rate would
        # pollute the quote. Miners that are active but momentarily in a swap
        # stay in the list with a status label so users can still price-shop.
        available = []
        for pair in matching:
            try:
                is_active = client.get_miner_active_flag(pair.hotkey)
                collateral = client.get_miner_collateral(pair.hotkey)
                if not is_active or collateral <= 0:
                    continue
                has_swap = client.get_miner_has_active_swap(pair.hotkey)
                available.append((pair, collateral, has_swap))
            except ContractError:
                continue

    if not available:
        console.print('[yellow]No active miners with collateral for this pair[/yellow]\n')
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

    src_up = from_chain.upper()
    dst_up = to_chain.upper()
    console.print(f'\n[bold]Quote: send {amount} {src_up} → receive {dst_up}[/bold]')
    console.print(
        '[dim]Rate shown as destination per source unit (e.g. for BTC→TAO, 345 = 1 BTC gets 345 TAO).[/dim]\n'
    )

    table = Table(show_header=True)
    table.add_column('#', style='dim')
    table.add_column('UID', style='cyan')
    table.add_column('Rate', style='green')
    table.add_column('You Receive', style='bold green')
    table.add_column('Collateral', style='yellow')
    table.add_column('Status', style='bold')

    viable_count = 0
    busy_but_fits_count = 0
    for idx, (pair, collateral, has_swap) in enumerate(available, 1):
        to_amount = calculate_to_amount(from_amount, pair.rate_str, is_reverse, canon_to_decimals, canon_from_decimals)
        user_receives = apply_fee_deduction(to_amount, fee_divisor)
        human_receives = user_receives / (10**dst_chain_def.decimals)

        tao_amount_rao = derive_tao_leg(from_chain, from_amount, to_chain, to_amount)
        viable, reason = check_swap_viability(tao_amount_rao, collateral, min_swap_rao, max_swap_rao)
        # "in swap" takes precedence over amount-viability: even if the amount
        # fits, the miner can't accept a new reservation until the current one
        # resolves. Shown yellow rather than red to signal "temporary".
        if has_swap:
            status = '[yellow]in swap[/yellow]'
            if viable:
                busy_but_fits_count += 1
        elif viable:
            status = '[green]available[/green]'
            viable_count += 1
        else:
            status = f'[red]{reason}[/red]'

        table.add_row(
            str(idx),
            str(pair.uid),
            f'{pair.rate:g}',
            f'{human_receives:.8f} {dst_up}',
            f'{from_rao(collateral):.4f} TAO',
            status,
        )

    console.print(table)
    console.print(f'  [dim](receive amount is after {fee_pct:g}% protocol fee)[/dim]')

    # Show the implied max-send amount at the best available rate so a user
    # who hit "insufficient collateral" knows the ceiling to retry under.
    # Prefer a miner that's not in-swap so the hint is actionable now; fall
    # back to the top-rate row so the user still sees a ceiling figure.
    reservable = [row for row in available if not row[2]]
    if (reservable or available) and max_swap_rao > 0:
        best_pair, best_collateral, best_has_swap = reservable[0] if reservable else available[0]
        # The TAO leg is capped by min(collateral, max_swap_rao).
        effective_tao_cap_rao = min(best_collateral, max_swap_rao)
        if from_chain == 'tao':
            max_send_human = from_rao(effective_tao_cap_rao)
        else:
            # Source is non-TAO — derive max send in source units from the
            # TAO cap divided by the miner's forward rate (dest-per-source =
            # TAO-per-source when dst=tao).
            max_send_human = _max_source_from_tao_cap(best_pair, effective_tao_cap_rao, from_chain, to_chain)
        if max_send_human is not None:
            busy_note = ' (currently in a swap — available once that clears)' if best_has_swap else ''
            console.print(
                f'  [dim]Best miner (UID {best_pair.uid}) can fulfill up to '
                f'~{max_send_human:g} {src_up} at rate {best_pair.rate:g}{busy_note}.[/dim]'
            )

    if viable_count == 0:
        if busy_but_fits_count > 0:
            console.print(
                '  [yellow]All miners that can fulfill this amount are currently in a swap — retry shortly.[/yellow]\n'
            )
        else:
            console.print(
                '  [yellow]No miner can fulfill this swap at the requested amount — try a smaller amount '
                'or wait for more collateral to be posted.[/yellow]\n'
            )
    else:
        console.print()


def _max_source_from_tao_cap(pair, tao_cap_rao: int, from_chain: str, to_chain: str) -> float | None:
    """Derive the max source-chain amount (human units) a miner can fulfill,
    given a TAO-side cap. Returns None if the direction has no TAO leg.

    Bridging through TAO: tao_cap_rao is a cap on the TAO side of the swap,
    so the max source amount is `tao_cap_rao / rate` scaled back to human
    units. Only meaningful when one side is TAO (true for every pair today).
    """
    if from_chain == 'tao' or to_chain != 'tao':
        # from_chain == 'tao' is the caller's fast path; the 'neither is TAO'
        # case currently can't occur (every chain bridges through TAO) but we
        # don't lie about it.
        return None
    if pair.rate <= 0:
        return None
    # tao_cap_rao (9-decimal) / rate → source amount in source-decimal units.
    tao_human = tao_cap_rao / 1_000_000_000
    return tao_human / pair.rate
