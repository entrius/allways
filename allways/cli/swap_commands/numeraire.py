"""SOL-numéraire quoting — the uniform rate convention + a one-price-per-chain miner helper.

THE CONVENTION (canonical, formalized): every rate is **"X per 1 SOL"** — the price of one SOL in the other
asset. SOL is the hub (`canonical_pair` makes it the canonical source), so for every launch pair the stored
`MinerQuote.rate` reads the same way: BTC per SOL, TAO per SOL. A miner therefore needs only ONE number per
chain (its SOL price), not a rate per direction — both directions of a pair derive from it. Reverse direction
is the reciprocal, applied on-chain via `is_reverse`.

`derive_sol_numeraire_quotes` turns `{chain: (price_X_per_sol, address)}` into the per-direction quote specs.
An optional symmetric `spread_bps` gives the miner margin both ways (sol→X posted a touch low, X→sol a touch
high); `spread_bps=0` posts the zero-margin mid.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import console, get_cli_context, get_solana_cli_context, loading
from allways.cli.swap_commands.pair import write_rate_posted_flag
from allways.constants import RATE_PRECISION
from allways.solana.client import SolanaClientError


@dataclass
class QuoteSpec:
    from_chain: str
    to_chain: str
    from_addr: str  # miner's address on from_chain
    to_addr: str  # miner's address on to_chain
    rate: float  # canonical 'dest per 1 SOL' display rate


def derive_sol_numeraire_quotes(
    sol_address: str,
    chain_specs: Dict[str, Tuple[float, str]],
    spread_bps: int = 0,
) -> List[QuoteSpec]:
    """Derive both directions of every sol<->X pair from one price per chain.

    ``chain_specs``: ``{chain: (price_X_per_sol, miner_X_address)}``. ``spread_bps`` is a symmetric margin:
    sol→X is posted at ``price*(1-s)`` (miner returns slightly less X), X→sol at ``price*(1+s)`` (miner
    returns slightly less SOL); both are stored as the canonical 'X per SOL' rate. 0 = zero-margin mid.
    """
    s = spread_bps / 10_000
    specs: List[QuoteSpec] = []
    for chain, (price, addr) in chain_specs.items():
        if chain == 'sol' or price <= 0:
            continue
        specs.append(QuoteSpec('sol', chain, sol_address, addr, price * (1 - s)))  # sol -> X
        specs.append(QuoteSpec(chain, 'sol', addr, sol_address, price * (1 + s)))  # X -> sol
    return specs


@click.command('quotes', cls=StyledCommand)
@click.option('--sol-address', 'sol_address', default=None, help='Your SOL address (the hub leg).')
@click.option('--btc-price', type=float, default=None, help='BTC per 1 SOL (0/omit to skip BTC).')
@click.option('--btc-address', default=None, help='Your BTC address.')
@click.option('--tao-price', type=float, default=None, help='TAO per 1 SOL (0/omit to skip TAO).')
@click.option('--tao-address', default=None, help='Your TAO address.')
@click.option('--spread', 'spread_bps', type=int, default=0, help='Symmetric margin in bps (0 = mid).')
@click.option('--yes', 'yes', is_flag=True, help='Skip confirmation.')
def quotes_command(sol_address, btc_price, btc_address, tao_price, tao_address, spread_bps, yes):
    """Publish all SOL pairs from one price per chain (the 'X per 1 SOL' convention).

    \b
    Example:
        alw miner quotes --sol-address <sol> --btc-price 0.0021 --btc-address <btc> \\
                         --tao-price 0.5 --tao-address <tao> --spread 50
    """
    chain_specs: Dict[str, Tuple[float, str]] = {}
    if btc_price and btc_price > 0:
        if not btc_address:
            console.print('[red]--btc-address required with --btc-price[/red]')
            return
        chain_specs['btc'] = (btc_price, btc_address)
    if tao_price and tao_price > 0:
        if not tao_address:
            console.print('[red]--tao-address required with --tao-price[/red]')
            return
        chain_specs['tao'] = (tao_price, tao_address)
    if not chain_specs:
        console.print('[yellow]Nothing to post — give at least one --<chain>-price/--<chain>-address.[/yellow]')
        return
    if not sol_address:
        console.print('[red]--sol-address is required.[/red]')
        return

    specs = derive_sol_numeraire_quotes(sol_address, chain_specs, spread_bps)
    console.print('\n[bold]Publishing SOL-numéraire quotes[/bold]  [dim](X per 1 SOL)[/dim]\n')
    for sp in specs:
        console.print(f'  {sp.from_chain.upper()} → {sp.to_chain.upper()}: [green]{sp.rate:g}[/green]')
    if not yes and not click.confirm('\nConfirm publishing these quotes?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    _, wallet, _, _ = get_cli_context(need_client=False)
    _, client = get_solana_cli_context()
    posted = 0
    for sp in specs:
        try:
            with loading(f'Publishing {sp.from_chain.upper()} → {sp.to_chain.upper()}...'):
                client.set_quote(sp.from_chain, sp.to_chain, sp.from_addr, sp.to_addr, int(sp.rate * RATE_PRECISION), 0)
            posted += 1
        except SolanaClientError as e:
            console.print(f'[red]Failed {sp.from_chain.upper()} → {sp.to_chain.upper()}: {e}[/red]')
    if posted:
        console.print(f'[green]Published {posted} quote direction(s)![/green]')
        write_rate_posted_flag(wallet.hotkey.ss58_address)
