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

import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    console,
    fail,
    get_cli_context,
    get_solana_cli_context,
    loading,
    quote_update_fee_lamports,
)
from allways.cli.swap_commands.pair import write_rate_posted_flag
from allways.constants import LAUNCH_SPOKES, NUMERAIRE_CHAIN, RATE_PRECISION
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
        if chain == NUMERAIRE_CHAIN or price <= 0:
            continue
        specs.append(QuoteSpec(NUMERAIRE_CHAIN, chain, sol_address, addr, price * (1 - s)))  # hub -> X
        specs.append(QuoteSpec(chain, NUMERAIRE_CHAIN, addr, sol_address, price * (1 + s)))  # X -> hub
    return specs


def _hub_addr_kw() -> str:
    return f'{NUMERAIRE_CHAIN}_address'


def quote_options(f):
    """Attach the hub-address flag plus a ``--<spoke>-price`` / ``--<spoke>-address`` pair for every
    launch spoke. Registry-derived from ``LAUNCH_SPOKES`` — add a spoke there and its flags appear
    here automatically, with no hand-typed per-chain options. Every flag stays explicit, so posting
    quotes is fully scriptable (``--yes`` skips the confirm)."""
    hub = NUMERAIRE_CHAIN.upper()
    for spoke in reversed(LAUNCH_SPOKES):  # reversed: decorators stack bottom-up, so this restores registry order
        f = click.option(f'--{spoke}-address', default=None, help=f'Your {spoke.upper()} address.')(f)
        f = click.option(
            f'--{spoke}-price',
            type=FINITE_FLOAT,
            default=None,
            help=f'{spoke.upper()} per 1 {hub} (0/omit to skip {spoke.upper()}).',
        )(f)
    return click.option(
        f'--{NUMERAIRE_CHAIN}-address', _hub_addr_kw(), default=None, help=f'Your {hub} address (the hub leg).'
    )(f)


def _example() -> str:
    """A concrete, copy-pasteable usage line built from the current registry (not hand-typed)."""
    flags = ' '.join(f'--{s}-price <{s}-per-{NUMERAIRE_CHAIN}> --{s}-address <{s}>' for s in LAUNCH_SPOKES)
    return f'alw miner quotes --{NUMERAIRE_CHAIN}-address <{NUMERAIRE_CHAIN}> {flags} --spread 50'


@click.command('quotes', cls=StyledCommand)
@quote_options
@click.option('--spread', 'spread_bps', type=int, default=0, help='Symmetric margin in bps (0 = mid).')
@click.option('--dry-run', 'dry_run', is_flag=True, help='Preview quotes + churn fees; post nothing.')
@click.option('--yes', 'yes', is_flag=True, help='Skip confirmation.')
def quotes_command(spread_bps, dry_run, yes, **spoke_opts):
    """Publish every hub pair from one price per chain (the 'X per 1 SOL' convention).

    One --<spoke>-price + --<spoke>-address pair per launch spoke; give as many or as few as you
    like. Both directions of each pair derive from that single price.

    \b
    Example:
        {example}
    """
    sol_address = spoke_opts.get(_hub_addr_kw())
    chain_specs: Dict[str, Tuple[float, str]] = {}
    for spoke in LAUNCH_SPOKES:
        price = spoke_opts.get(f'{spoke}_price')
        addr = spoke_opts.get(f'{spoke}_address')
        if not price or price <= 0:
            continue
        if not addr:
            fail(f'--{spoke}-address required with --{spoke}-price')
        chain_specs[spoke] = (price, addr)
    if not chain_specs:
        fail('Nothing to post — give at least one --<chain>-price/--<chain>-address.')
    if not sol_address:
        fail(f'--{NUMERAIRE_CHAIN}-address is required.')

    _, wallet, _, _ = get_cli_context(need_client=False)
    _, client = get_solana_cli_context()
    miner = client.keypair.pubkey()
    now = int(time.time())

    specs = derive_sol_numeraire_quotes(sol_address, chain_specs, spread_bps)

    # Show each direction's current rate + the churn fee this update will incur (per-direction,
    # keyed on that quote's own updated_at). Creation is free; the fee decays to 0 over 10 min.
    hub = NUMERAIRE_CHAIN.upper()
    console.print(f'\n[bold]{hub}-numéraire quotes[/bold]  [dim](X per 1 {hub})[/dim]\n')
    total_fee = 0
    for sp in specs:
        cur = client.get_quote(miner, sp.from_chain, sp.to_chain)
        if cur is None:
            note = '[dim]new — free[/dim]'
        else:
            age = now - int(cur.updated_at)
            fee = quote_update_fee_lamports(age)
            total_fee += fee
            was = int(cur.rate) / RATE_PRECISION
            if fee:
                note = (
                    f'[yellow]churn fee {fee / 1e9:g} SOL[/yellow] '
                    f'[dim](was {was:g}, set {age}s ago; free to update in {max(0, 600 - age)}s)[/dim]'
                )
            else:
                note = f'[dim]free (was {was:g})[/dim]'
        console.print(f'  {sp.from_chain.upper()} → {sp.to_chain.upper()}: [green]{sp.rate:g}[/green]   {note}')

    if total_fee:
        console.print(
            f'\n[yellow]Total churn fee: {total_fee / 1e9:g} SOL[/yellow] '
            '[dim](→ treasury; each direction is free again 10 min after its last update)[/dim]'
        )

    if dry_run:
        console.print('\n[dim]--dry-run: nothing posted.[/dim]')
        return
    if not yes and not click.confirm('\nPublish these quotes?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    posted = 0
    for sp in specs:
        try:
            with loading(f'Publishing {sp.from_chain.upper()} → {sp.to_chain.upper()}...'):
                client.set_quote(sp.from_chain, sp.to_chain, sp.from_addr, sp.to_addr, int(sp.rate * RATE_PRECISION), 0)
            posted += 1
        except SolanaClientError as e:
            console.print(f'[red]Failed {sp.from_chain.upper()} → {sp.to_chain.upper()}: {e}[/red]')
    if not posted:
        fail('No quotes were published.')
    console.print(f'[green]Published {posted} quote direction(s)![/green]')
    write_rate_posted_flag(wallet.hotkey.ss58_address)


# Interpolate the registry-derived example into the help (Click doesn't format docstrings).
quotes_command.help = quotes_command.help.format(example=_example())
