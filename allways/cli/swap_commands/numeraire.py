"""Hub-numéraire quoting — the uniform rate convention + a one-price-per-chain miner helper.

THE CONVENTION (canonical, formalized): every rate is **"X per 1 hub"** — the price of one hub unit in the
other asset. The pair's hub leg is `canonical_pair`'s canonical source, so every stored `MinerQuote.rate`
reads the same way: BTC per SOL, TAO per SOL, ETH per TAO. A miner therefore needs only ONE number per
spoke (its hub price), not a rate per direction — both directions of a pair derive from it. Reverse
direction is the reciprocal, applied on-chain via `is_reverse`.

`derive_hub_numeraire_quotes` turns `{chain: (price_X_per_hub, address)}` into the per-direction quote
specs for one hub. An optional symmetric `spread_bps` gives the miner margin both ways (hub→X posted a
touch low, X→hub a touch high); `spread_bps=0` posts the zero-margin mid.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

import click

from allways.chains import uses_solana_wallet
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    FINITE_FLOAT,
    backing_label,
    console,
    declarable_backings,
    fail,
    get_cli_context,
    get_solana_cli_context,
    loading,
    quote_update_fee_lamports,
    resolve_quote_backing,
    safe_read,
)
from allways.cli.swap_commands.pair import write_rate_posted_flag
from allways.constants import HUB_CHAINS, LAUNCH_ALPHAS, LAUNCH_SPOKES, NUMERAIRE_CHAIN, RATE_PRECISION, family
from allways.solana.client import SolanaClientError
from allways.utils.rate import quantize_rate_display, quantize_rate_fixed


@dataclass
class QuoteSpec:
    from_chain: str
    to_chain: str
    from_addr: str  # miner's address on from_chain
    to_addr: str  # miner's address on to_chain
    rate: float  # canonical 'dest per 1 hub' display rate


def derive_hub_numeraire_quotes(
    hub: str,
    hub_address: str,
    chain_specs: Dict[str, Tuple[float, str]],
    spread_bps: int = 0,
) -> List[QuoteSpec]:
    """Derive both directions of every hub<->X pair from one price per chain.

    ``chain_specs``: ``{chain: (price_X_per_hub, miner_X_address)}``. ``spread_bps`` is a symmetric margin:
    hub→X is posted at ``price*(1-s)`` (miner returns slightly less X), X→hub at ``price*(1+s)`` (miner
    returns slightly less hub); both are stored as the canonical 'X per hub' rate. 0 = zero-margin mid.
    """
    s = spread_bps / 10_000
    specs: List[QuoteSpec] = []
    for chain, (price, addr) in chain_specs.items():
        if chain == hub or price <= 0:
            continue
        specs.append(QuoteSpec(hub, chain, hub_address, addr, price * (1 - s)))  # hub -> X
        specs.append(QuoteSpec(chain, hub, addr, hub_address, price * (1 + s)))  # X -> hub
    return specs


def _addr_kw(chain: str) -> str:
    return f'{chain}_address'


def quote_options(f):
    """Attach the SOL-address flag plus a ``--<chain>-price`` for every launch spoke and alpha, and a
    ``--<spoke>-address`` for every spoke (an alpha is delivered to the TAO address). Registry-derived
    from ``LAUNCH_SPOKES`` / ``LAUNCH_ALPHAS`` — add a chain there and its flags appear here
    automatically. Every flag stays explicit, so posting quotes is fully scriptable (``--yes`` skips
    the confirm). Under ``--hub tao`` the prices read 'X per 1 TAO' and ``--tao-address`` is the hub leg."""
    for chain in reversed(LAUNCH_SPOKES + LAUNCH_ALPHAS):  # reversed: decorators stack bottom-up
        if chain in LAUNCH_SPOKES:
            f = click.option(f'--{chain}-address', default=None, help=f'Your {chain.upper()} address.')(f)
        f = click.option(
            f'--{chain}-price',
            type=FINITE_FLOAT,
            default=None,
            help=f'{chain.upper()} per 1 hub unit (0/omit to skip {chain.upper()}).',
        )(f)
    return click.option(
        f'--{NUMERAIRE_CHAIN}-address',
        _addr_kw(NUMERAIRE_CHAIN),
        default=None,
        help=f'Your {NUMERAIRE_CHAIN.upper()} address (the hub leg under --hub sol).',
    )(f)


def _example() -> str:
    """A concrete, copy-pasteable usage line built from the current registry (not hand-typed)."""
    flags = ' '.join(f'--{s}-price <{s}-per-hub> --{s}-address <{s}>' for s in LAUNCH_SPOKES)
    flags += ' ' + ' '.join(f'--{a}-price <{a}-per-hub>' for a in LAUNCH_ALPHAS)
    return f'alw miner quotes --{NUMERAIRE_CHAIN}-address <{NUMERAIRE_CHAIN}> {flags} --spread 50'


@click.command('quotes', cls=StyledCommand)
@quote_options
@click.option('--spread', 'spread_bps', type=int, default=0, help='Symmetric margin in bps (0 = mid).')
@click.option(
    '--hub',
    'hub',
    type=click.Choice(HUB_CHAINS),
    default=NUMERAIRE_CHAIN,
    help='Hub leg these pairs anchor on; prices read "X per 1 <hub>". Default sol.',
)
@click.option(
    '--backing',
    default=None,
    type=str,
    help='Collateral purse backing these quotes (sol|tao). Inferred when only one of yours qualifies.',
)
@click.option('--dry-run', 'dry_run', is_flag=True, help='Preview quotes + churn fees; post nothing.')
@click.option('--yes', 'yes', is_flag=True, help='Skip confirmation.')
def quotes_command(spread_bps, hub, backing, dry_run, yes, **spoke_opts):
    """Publish every pair of one hub from one price per chain (the 'X per 1 hub' convention).

    One --<spoke>-price + --<spoke>-address pair per launch spoke; give as many or as few as you
    like. Both directions of each pair derive from that single price. --hub tao anchors the pairs
    on TAO instead of SOL (--tao-address becomes the hub leg; run once per hub you quote).

    \b
    Example:
        {example}
    """
    hub_address = spoke_opts.get(_addr_kw(hub))
    chain_specs: Dict[str, Tuple[float, str]] = {}
    for chain in LAUNCH_SPOKES + LAUNCH_ALPHAS:
        price = spoke_opts.get(f'{chain}_price')
        addr_chain = family(chain) if chain in LAUNCH_ALPHAS else chain  # an alpha lands on the TAO coldkey
        addr = spoke_opts.get(_addr_kw(addr_chain))
        if chain == hub:
            if price:
                fail(f'--{chain}-price conflicts with --hub {hub} — {chain.upper()} is the hub leg, not a chain.')
            continue
        if not price or price <= 0:
            continue
        if not addr and uses_solana_wallet(chain):
            addr = spoke_opts.get(_addr_kw(NUMERAIRE_CHAIN))  # same wallet as the SOL leg
        if not addr:
            fail(f'--{addr_chain}-address required with --{chain}-price')
        chain_specs[chain] = (price, addr)
    if not chain_specs:
        fail('Nothing to post — give at least one --<chain>-price/--<chain>-address.')
    if not hub_address:
        fail(f'--{hub}-address is required (the hub leg).')

    _, wallet, _, _ = get_cli_context(need_client=False)
    _, client = get_solana_cli_context()
    miner = client.keypair.pubkey()
    now = int(time.time())

    specs = derive_hub_numeraire_quotes(hub, hub_address, chain_specs, spread_bps)

    # Every pair here is hub<->spoke through the numeraire, so each resolves independently: sol<->tao
    # is the one that can go either way and will demand --backing from a dual-purse miner.
    miner_state = safe_read(lambda: client.get_miner_state(miner), what='read miner state')
    backings = {
        (sp.from_chain, sp.to_chain): resolve_quote_backing(miner_state, sp.from_chain, sp.to_chain, backing)
        for sp in specs
    }

    # Show each direction's current rate + the churn fee this update will incur (per-direction,
    # keyed on that quote's own updated_at). Creation is free; the fee decays to 0 over 10 min.
    hub_up = hub.upper()
    console.print(f'\n[bold]{hub_up}-numéraire quotes[/bold]  [dim](X per 1 {hub_up})[/dim]\n')
    total_fee = 0
    orphans = []  # (from, to, sibling_backing) live on the OTHER purse this run doesn't touch
    for sp in specs:
        b = backings[(sp.from_chain, sp.to_chain)]
        cur = client.get_quote(miner, sp.from_chain, sp.to_chain, b)
        for other in declarable_backings(sp.from_chain, sp.to_chain):
            if other != b and client.get_quote(miner, sp.from_chain, sp.to_chain, other) is not None:
                orphans.append((sp.from_chain, sp.to_chain, other))
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
        disp = quantize_rate_display(sp.rate)  # mirror the on-chain floor so preview == stored
        console.print(
            f'  {sp.from_chain.upper()} → {sp.to_chain.upper()} [cyan]{backing_label(b)}[/cyan]: '
            f'[green]{disp:g}[/green]   {note}'
        )

    if total_fee:
        console.print(
            f'\n[yellow]Total churn fee: {total_fee / 1e9:g} SOL[/yellow] '
            '[dim](→ treasury; each direction is free again 10 min after its last update)[/dim]'
        )

    # A quote is per (direction, backing): pricing a direction under one backing leaves a prior
    # quote on the OTHER purse live at its old price. Surface it — takers can still reserve it.
    for f, t, ob in sorted(set(orphans)):
        console.print(
            f'\n[yellow]Heads-up: your {backing_label(ob)} quote on {f.upper()} → {t.upper()} is still '
            f'live at its old price[/yellow] — this run does not touch it. Retract it with '
            f'[bold]alw miner remove-quote --from {f} --to {t} --backing {ob}[/bold].'
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
                client.set_quote(
                    sp.from_chain,
                    sp.to_chain,
                    sp.from_addr,
                    sp.to_addr,
                    quantize_rate_fixed(int(sp.rate * RATE_PRECISION)),
                    0,
                    backing=backings[(sp.from_chain, sp.to_chain)],
                )
            posted += 1
        except SolanaClientError as e:
            console.print(f'[red]Failed {sp.from_chain.upper()} → {sp.to_chain.upper()}: {e}[/red]')
    if not posted:
        fail('No quotes were published.')
    console.print(f'[green]Published {posted} quote direction(s)![/green]')
    write_rate_posted_flag(wallet.hotkey.ss58_address)


# Interpolate the registry-derived example into the help (Click doesn't format docstrings).
quotes_command.help = quotes_command.help.format(example=_example())
