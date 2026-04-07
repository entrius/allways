"""alw post - Post a trading pair commitment to chain."""

import rich_click as click

from allways.chains import SUPPORTED_CHAINS, canonical_pair
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
        reason = 'same as source chain' if value == exclude else 'unsupported'
        console.print(f'[red]Invalid: {reason}. Choose from: {choices}[/red]')


def _prompt_rates(canon_src: str, canon_dest: str) -> tuple:
    """Prompt for direction-specific rates. Counter rate defaults to forward; 0 = direction not supported."""
    src_up, dst_up = canon_src.upper(), canon_dest.upper()
    while True:
        fwd = click.prompt(f'Rate for {src_up} -> {dst_up} ({dst_up} per 1 {src_up})', type=float)
        if fwd > 0:
            break
        console.print('[red]Rate must be positive[/red]')
    rev = click.prompt(
        f'Rate for {dst_up} -> {src_up} ({dst_up} per 1 {src_up}, 0 = not supported)', type=float, default=fwd
    )
    if rev < 0:
        console.print('[red]Rate cannot be negative, using 0 (not supported)[/red]')
        rev = 0.0
    return fwd, rev


@click.command('pair')
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

    \b
    All arguments are optional — if omitted, you'll be prompted interactively.

    \b
    Arguments:
        SRC_CHAIN      Source chain ID (e.g. btc, tao)
        SRC_ADDR       Your receiving address on source chain
        DST_CHAIN      Destination chain ID (e.g. tao, btc)
        DST_ADDR       Your sending address on destination chain
        RATE           source→dest rate (e.g. TAO per 1 BTC for btc-tao pair)
        COUNTER_RATE   dest→source rate (optional, defaults to RATE)

    \b
    Examples:
        alw miner post                                              (interactive wizard)
        alw miner post btc bc1q...abc tao 5Cxyz...def 340 350      (direction-specific rates)
        alw miner post btc bc1q...abc tao 5Cxyz...def 345          (same rate both ways)
    """
    # --- Prompt for any missing arguments ---
    if src_chain is None:
        src_chain = _prompt_chain('Source chain (you receive on this chain)')
    else:
        src_chain = src_chain.lower()
        if src_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unsupported source chain: {src_chain}[/red]')
            console.print(f'[dim]Supported: {", ".join(SUPPORTED_CHAINS.keys())}[/dim]')
            return

    if src_addr is None:
        src_addr = click.prompt(f'Your receiving address on {SUPPORTED_CHAINS[src_chain].name}')

    if dst_chain is None:
        dst_chain = _prompt_chain('Destination chain (you send on this chain)', exclude=src_chain)
    else:
        dst_chain = dst_chain.lower()
        if dst_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unsupported destination chain: {dst_chain}[/red]')
            console.print(f'[dim]Supported: {", ".join(SUPPORTED_CHAINS.keys())}[/dim]')
            return
        if dst_chain == src_chain:
            console.print('[red]Source and destination chains must be different[/red]')
            return

    if dst_addr is None:
        dst_addr = click.prompt(f'Your sending address on {SUPPORTED_CHAINS[dst_chain].name}')

    canon_src, canon_dest = canonical_pair(src_chain, dst_chain)

    if rate is None:
        rate, counter_rate = _prompt_rates(canon_src, canon_dest)
    elif rate <= 0:
        console.print('[red]Rate must be positive[/red]')
        return
    else:
        if counter_rate is None:
            counter_rate = rate
        elif counter_rate < 0:
            console.print('[red]Rate cannot be negative[/red]')
            return

    # Normalize to canonical direction (alphabetical ordering).
    # Rates are NOT swapped — prompts and help text already define them in canonical order.
    if src_chain != canon_src:
        console.print(f'[dim]Normalizing pair direction to canonical form ({canon_src} -> {canon_dest}).[/dim]')
        src_chain, dst_chain = dst_chain, src_chain
        src_addr, dst_addr = dst_addr, src_addr

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

    console.print('\n[bold]Posting trading pair commitment[/bold]\n')
    console.print(f'  Source:      [cyan]{SUPPORTED_CHAINS[src_chain].name}[/cyan] ({src_addr})')
    console.print(f'  Destination: [cyan]{SUPPORTED_CHAINS[dst_chain].name}[/cyan] ({dst_addr})')
    if counter_rate == 0:
        console.print(f'  Rate ({src_up}->{dst_up}):  [green]1 {src_up} = {rate:g} {dst_up}[/green]')
        console.print(f'  Rate ({dst_up}->{src_up}):  [yellow]not supported[/yellow]')
    elif rate == counter_rate:
        console.print(f'  Rate:        [green]1 {src_up} = {rate:g} {dst_up} (both directions)[/green]')
    else:
        console.print(f'  Rate ({src_up}->{dst_up}):  [green]1 {src_up} = {rate:g} {dst_up}[/green]')
        console.print(f'  Rate ({dst_up}->{src_up}):  [green]1 {src_up} = {counter_rate:g} {dst_up}[/green]')
    console.print(f'  Netuid:      {netuid}')
    console.print(f'  Data:        [dim]{commitment_data}[/dim]\n')

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
