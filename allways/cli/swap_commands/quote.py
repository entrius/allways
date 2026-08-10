"""alw swap quote - Preview rates and estimated receive amounts before swapping.

Reads live MinerQuote/MinerState from the Solana program, runs the same miner-selection + amount math the
origination path uses (`swap_intake`), and shows every viable miner and what you would receive after the 1%
protocol fee — without committing to a swap."""

import sys

import click
from rich.table import Table
from rich.text import Text

from allways.chains import SUPPORTED_CHAINS, get_chain_def
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    backing_label,
    console,
    fail,
    from_lamports,
    get_solana_cli_context,
    load_miner_book,
    print_json,
    safe_read,
    set_json_output,
)
from allways.cli.swap_commands.swap_intake import (
    MinerCandidate,
    backing_purse,
    bounds_from_config,
    compute_intake_amounts,
    rate_display_from_fixed,
    select_best_miner,
    swap_viable,
    to_smallest_units,
)
from allways.constants import FEE_DIVISOR
from allways.utils.rate import apply_fee_deduction, directional_rate, is_executable_rate

# The failure guarantee each backing carries. It differs in TIMING, not in whether you are made
# whole — say that plainly rather than making a taker infer it from the asset name.
GUARANTEE = {
    'sol': 'instant SOL refund from the miner\'s on-chain collateral',
    'tao': 'TAO reimbursement from the miner\'s bond, shortly after the timeout',
}


def _prompt_or_fail(value, prompt_text, opt, cast=str):
    if value is not None:
        return value
    if sys.stdin.isatty():
        return click.prompt(prompt_text, type=cast)
    fail(f'{opt} is required (no TTY to prompt).')


@click.command('quote')
@click.option('--from', 'from_chain', default=None, type=str, help='Source chain (e.g. sol, btc, tao)')
@click.option('--to', 'to_chain', default=None, type=str, help='Destination chain (e.g. sol, btc, tao)')
@click.option('--amount', default=None, type=FINITE_FLOAT, help='Amount to send in source chain units')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a table.')
def quote_command(from_chain: str, to_chain: str, amount: float, as_json: bool):
    """Preview rates and estimated receive amounts for a swap.

    \b
    Shows every miner that could fund the swap, their rate, and what you would
    receive after the 1% protocol fee — without committing. The best offer is
    highlighted. Omit a flag to be prompted (interactive TTY only).

    \b
    Examples:
        alw swap quote --from sol --to btc --amount 1
        alw swap quote --from btc --to sol --amount 0.001
    """
    set_json_output(as_json)
    chains = ', '.join(SUPPORTED_CHAINS)
    from_chain = _prompt_or_fail(from_chain, f'Source chain ({chains})', '--from')
    to_chain = _prompt_or_fail(to_chain, f'Destination chain ({chains})', '--to')
    amount = _prompt_or_fail(amount, 'Amount (source units)', '--amount', cast=float)

    from_chain = from_chain.lower()
    to_chain = to_chain.lower()
    if from_chain not in SUPPORTED_CHAINS or to_chain not in SUPPORTED_CHAINS:
        fail(f'--from/--to must each be one of: {", ".join(SUPPORTED_CHAINS)}')
    if from_chain == to_chain or 'sol' not in (from_chain, to_chain):
        fail('A swap must have a SOL leg (sol<->btc or sol<->tao) and two distinct chains.')
    if amount <= 0:
        fail('--amount must be positive.')

    _, client = get_solana_cli_context(need_keypair=False)
    cfg = safe_read(lambda: client.get_config(), what='read config')
    bounds = bounds_from_config(cfg) if cfg else {}
    min_swap, max_swap = bounds.get('sol', (0, 0))

    from_amount = to_smallest_units(amount, from_chain)
    to_dec = get_chain_def(to_chain).decimals

    book = load_miner_book(client, with_reservation=False)
    candidates = []
    for e in book:
        # Inactive miners can't be reserved (the contract rejects it) — never offer one as a quote.
        if e.state is None or not e.state.active:
            continue
        for q in e.quotes:
            if q.from_chain == from_chain and q.to_chain == to_chain:
                backing = getattr(q, 'collateral_chain', None) or 'sol'
                # The purse the contract will check for THIS offer — local lamports for a
                # sol-backed quote, the attested effective bond for any other.
                purse = backing_purse(client, q.miner, e.state, backing)
                if purse is None:
                    continue
                candidates.append(
                    MinerCandidate(
                        miner=q.miner,
                        rate_display=rate_display_from_fixed(q.rate),
                        collateral=purse,
                        backing=backing,
                    )
                )

    # Build the viable set with the same guards the contract enforces, and identify the best offer.
    viable = []  # (candidate, receive_units)
    for c in candidates:
        try:
            rate = float(c.rate_display)
        except (TypeError, ValueError):
            continue
        if not is_executable_rate(rate, from_chain, to_chain, min_swap, max_swap):
            continue
        try:
            amts = compute_intake_amounts(from_chain, to_chain, from_amount, c.rate_display, c.backing)
        except ValueError:
            continue
        if amts.to_amount <= 0:
            continue
        lo, hi = bounds.get(c.backing, (min_swap, max_swap))
        ok, _reason = swap_viable(amts.collateral_amount, c.collateral, lo, hi, c.backing)
        if not ok:
            continue
        viable.append((c, apply_fee_deduction(amts.to_amount, FEE_DIVISOR)))

    best = select_best_miner(candidates, from_chain, to_chain, from_amount, min_swap, max_swap, bounds)
    best_miner = str(best[0].miner) if best else None

    if as_json:
        print_json(
            {
                'from': from_chain,
                'to': to_chain,
                'amount': amount,
                'fee_percent': 100 / FEE_DIVISOR,
                'best_miner': best_miner,
                'offers': [
                    {
                        'miner': str(c.miner),
                        'rate': directional_rate(from_chain, to_chain, c.rate_display),
                        'rate_unit': f'{to_chain.upper()} per {from_chain.upper()}',
                        # What you are owed if the miner fails: SOL refunds instantly, TAO is
                        # reimbursed from the miner's bond shortly after the timeout.
                        'backing': c.backing,
                        'guarantee': GUARANTEE[c.backing] if c.backing in GUARANTEE else 'see docs',
                        'receive': recv / 10**to_dec,
                        'collateral_sol': from_lamports(c.collateral),
                        'best': str(c.miner) == best_miner,
                    }
                    for c, recv in sorted(viable, key=lambda x: x[1], reverse=True)
                ],
            }
        )
        # Same contract as the table path: no fundable offer is a failure, format-independent.
        raise SystemExit(0 if viable else 1)

    if not viable:
        why = (
            'no miner is quoting that direction'
            if not candidates
            else 'no miner can fund that swap within bounds/collateral'
        )
        fail(f'No quote available: {why} right now.')

    viable.sort(key=lambda x: x[1], reverse=True)

    # Lead with the headline number — what you'd actually receive at the best offer — then the breakdown.
    best_recv = viable[0][1] / 10**to_dec
    console.print(
        f'\n[bold green]≈ {best_recv:.8g} {to_chain.upper()}[/bold green]'
        f'  [dim]for[/dim]  {amount:g} {from_chain.upper()}'
        f'  [dim](best of {len(viable)} offer{"s" if len(viable) != 1 else ""}, after {100 / FEE_DIVISOR:g}% fee)[/dim]'
    )

    table = Table(
        title=f'Quote: {amount:g} {from_chain.upper()} → {to_chain.upper()}  (after {100 / FEE_DIVISOR:g}% fee)',
        show_header=True,
    )
    table.add_column('Miner', style='cyan')
    table.add_column('Backing', style='cyan')
    table.add_column(f'Rate ({to_chain.upper()}/{from_chain.upper()})', style='white', justify='right')
    table.add_column(f'You receive ({to_chain.upper()})', style='green', justify='right')
    table.add_column('Collateral', justify='right')
    table.add_column('', style='bold yellow')
    for c, recv in viable:
        is_best = str(c.miner) == best_miner
        table.add_row(
            Text(str(c.miner)[:12] + '…', style='bold cyan' if is_best else 'cyan'),
            backing_label(c.backing),
            directional_rate(from_chain, to_chain, c.rate_display),
            f'{recv / 10**to_dec:.8g}',
            f'{from_lamports(c.collateral):.2f} SOL',
            '★ best' if is_best else '',
        )
    console.print(table)
    if len({c.backing for c, _ in viable}) > 1:
        console.print(
            '[dim]Backing is the miner\'s bond, and it sets what you get if they fail to deliver:\n'
            f'  sol-backed — {GUARANTEE["sol"]}\n'
            f'  tao-backed — {GUARANTEE["tao"]}[/dim]'
        )
    console.print('[dim]Preview only — run `alw swap now` to originate a reservation.[/dim]')
