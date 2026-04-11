"""alw post - Post a trading pair commitment to chain."""

import click

from allways.chains import SUPPORTED_CHAINS, canonical_pair
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import console, get_cli_context, loading
from allways.constants import COMMITMENT_VERSION


def _prompt_chain(label: str, exclude: str | None = None) -> str:
    """Prompt the user to pick a chain from SUPPORTED_CHAINS."""
    chains = [c for c in SUPPORTED_CHAINS if c != exclude]
    if len(chains) == 1:
        console.print(f'{label}: [cyan]{chains[0]}[/cyan]')
        return chains[0]
    choices = ', '.join(chains)
    while True:
        value = click.prompt(f'{label} ({choices})').strip().lower()
        if value in SUPPORTED_CHAINS and value != exclude:
            return value
        reason = 'already selected' if value == exclude else 'unsupported'
        console.print(f'[red]Invalid: {reason}. Choose from: {choices}[/red]')


def _prompt_rates(canon_src: str, canon_dest: str) -> tuple:
    """Prompt for direction-specific rates. 0 = don't offer that direction; at least one must be positive."""
    src_up, dst_up = canon_src.upper(), canon_dest.upper()
    console.print(f"\n[dim]Rates in {dst_up} per 1 {src_up} (0 = don't offer)[/dim]")
    fwd_label = f'  {src_up} to {dst_up} (user sends {src_up}, miner returns {dst_up})'
    rev_label = f'  {dst_up} to {src_up} (user sends {dst_up}, miner returns {src_up})'
    while True:
        fwd = click.prompt(fwd_label, type=float)
        if fwd < 0:
            console.print('[red]Rate cannot be negative[/red]')
        else:
            break
    if fwd > 0:
        rev = click.prompt(rev_label, type=float, default=fwd)
        if rev < 0:
            console.print('[red]Rate cannot be negative, using 0 (not offered)[/red]')
            rev = 0.0
    else:
        while True:
            rev = click.prompt(rev_label, type=float)
            if rev < 0:
                console.print('[red]Rate cannot be negative[/red]')
            elif rev == 0:
                console.print('[red]At least one direction must have a positive rate[/red]')
            else:
                break
    return fwd, rev


@click.command('pair', cls=StyledCommand)
@click.argument('src_chain', required=False, default=None, type=str)
@click.argument('src_addr', required=False, default=None, type=str)
@click.argument('dst_chain', required=False, default=None, type=str)
@click.argument('dst_addr', required=False, default=None, type=str)
@click.argument('rate', required=False, default=None, type=float)
@click.argument('counter_rate', required=False, default=None, type=float)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def post_pair(
    src_chain: str | None,
    src_addr: str | None,
    dst_chain: str | None,
    dst_addr: str | None,
    rate: float | None,
    counter_rate: float | None,
    yes: bool,
):
    """Post a trading pair to chain via commitment.

    [dim]All arguments are optional — if omitted, you'll be prompted interactively.[/dim]

    [dim]Arguments:
        SRC_CHAIN       Source chain ID (e.g. btc, tao)
        SRC_ADDR        Your receiving address on source chain
        DST_CHAIN       Destination chain ID (e.g. tao, btc)
        DST_ADDR        Your sending address on destination chain
        RATE            source→dest rate (e.g. TAO per 1 BTC for btc-tao pair)
        COUNTER_RATE    dest→source rate (optional, defaults to RATE)[/dim]

    [dim]Examples:
        $ alw miner post                                            (interactive wizard)
        $ alw miner post btc bc1q...abc tao 5Cxyz...def 340 350     (direction-specific rates)
        $ alw miner post btc bc1q...abc tao 5Cxyz...def 345         (same rate both ways)[/dim]
    """
    # --- Determine chains ---
    if src_chain is None:
        src_chain = _prompt_chain('Chain')
    else:
        src_chain = src_chain.lower()
        if src_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unsupported chain: {src_chain}[/red]')
            console.print(f'[dim]Supported: {", ".join(SUPPORTED_CHAINS.keys())}[/dim]')
            return

    if dst_chain is None:
        remaining = [c for c in SUPPORTED_CHAINS if c != src_chain]
        if len(remaining) == 1:
            dst_chain = remaining[0]
        else:
            dst_chain = _prompt_chain('Pair with', exclude=src_chain)
    else:
        dst_chain = dst_chain.lower()
        if dst_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unsupported chain: {dst_chain}[/red]')
            console.print(f'[dim]Supported: {", ".join(SUPPORTED_CHAINS.keys())}[/dim]')
            return
        if dst_chain == src_chain:
            console.print('[red]Chains must be different[/red]')
            return

    # --- Addresses ---
    if src_addr is None:
        src_addr = click.prompt(f'Your {SUPPORTED_CHAINS[src_chain].name} address')

    if dst_addr is None:
        dst_addr = click.prompt(f'Your {SUPPORTED_CHAINS[dst_chain].name} address')

    canon_src, canon_dest = canonical_pair(src_chain, dst_chain)
    rates_from_args = rate is not None

    if rate is None:
        rate, counter_rate = _prompt_rates(canon_src, canon_dest)
    elif rate < 0:
        console.print('[red]Rate cannot be negative[/red]')
        return
    else:
        if counter_rate is None:
            counter_rate = rate
        elif counter_rate < 0:
            console.print('[red]Rate cannot be negative[/red]')
            return
        if rate == 0 and counter_rate == 0:
            console.print('[red]At least one direction must have a positive rate[/red]')
            return

    # Normalize to canonical direction.
    # Positional args: RATE = user's source→dest, so swap rates to match canonical order.
    # Interactive prompts: already asked in canonical order, no rate swap needed.
    if src_chain != canon_src:
        console.print(f'[dim]Normalizing pair direction to canonical form ({canon_src} -> {canon_dest}).[/dim]')
        src_chain, dst_chain = dst_chain, src_chain
        src_addr, dst_addr = dst_addr, src_addr
        if rates_from_args:
            rate, counter_rate = counter_rate, rate

    config, wallet, subtensor, _ = get_cli_context(need_client=False)
    netuid = config['netuid']

    rate_str = f'{rate:g}'
    counter_rate_str = f'{counter_rate:g}'
    commitment_data = (
        f'v{COMMITMENT_VERSION}:{src_chain}:{src_addr}:{dst_chain}:{dst_addr}:{rate_str}:{counter_rate_str}'
    )

    data_bytes = commitment_data.encode('utf-8')
    if len(data_bytes) > 128:
        console.print(
            f'[red]Commitment too long ({len(data_bytes)} bytes, max 128). '
            f'Try a shorter address format (e.g. P2WPKH instead of P2TR).[/red]'
        )
        return

    src_up, dst_up = src_chain.upper(), dst_chain.upper()
    src_name, dst_name = SUPPORTED_CHAINS[src_chain].name, SUPPORTED_CHAINS[dst_chain].name

    console.print('\n[bold]Posting trading pair commitment[/bold]\n')
    console.print(f'  [cyan]{src_name}[/cyan]:  {src_addr}')
    console.print(f'  [cyan]{dst_name}[/cyan]:  {dst_addr}')
    if rate == counter_rate and rate > 0:
        console.print(f'  Rate:       [green]1 {src_up} = {rate:g} {dst_up} (both directions)[/green]')
    else:
        if rate > 0:
            console.print(f'  {src_up} → {dst_up}: [green]1 {src_up} = {rate:g} {dst_up}[/green]')
        else:
            console.print(f'  {src_up} → {dst_up}: [yellow]not offered[/yellow]')
        if counter_rate > 0:
            console.print(f'  {dst_up} → {src_up}: [green]1 {src_up} = {counter_rate:g} {dst_up}[/green]')
        else:
            console.print(f'  {dst_up} → {src_up}: [yellow]not offered[/yellow]')
    console.print(f'  Netuid:     {netuid}')
    console.print(f'  Data:       [dim]{commitment_data}[/dim]\n')

    if not yes and not click.confirm('Confirm posting this pair?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting commitment...'):
            call = subtensor.substrate.compose_call(
                call_module='Commitments',
                call_function='set_commitment',
                call_params={
                    'netuid': netuid,
                    'info': {
                        'fields': [[{f'Raw{len(data_bytes)}': '0x' + data_bytes.hex()}]],
                    },
                },
            )
            extrinsic = subtensor.substrate.create_signed_extrinsic(call=call, keypair=wallet.hotkey)
            receipt = subtensor.substrate.submit_extrinsic(
                extrinsic, wait_for_inclusion=True, wait_for_finalization=False
            )

        if receipt.is_success:
            console.print('[green]Pair posted successfully![/green]')
        else:
            console.print(f'[red]Failed to post pair: {receipt.error_message}[/red]')

    except Exception as e:
        console.print(f'[red]Error: {e}[/red]')
