"""alw post - Post a trading pair commitment to chain."""

import rich_click as click

from allways.chains import SUPPORTED_CHAINS
from allways.cli.swap_commands.helpers import console, get_cli_context, loading
from allways.constants import COMMITMENT_VERSION


def _prompt_chain(label: str, exclude: str | None = None) -> str:
    """Prompt the user to pick a chain from SUPPORTED_CHAINS."""
    chains = [c for c in SUPPORTED_CHAINS if c != exclude]
    choices = ', '.join(chains)
    while True:
        value = click.prompt(f'{label} ({choices})').strip().lower()
        if value in SUPPORTED_CHAINS and value != exclude:
            return value
        reason = 'same as source chain' if value == exclude else 'unsupported'
        console.print(f'[red]Invalid: {reason}. Choose from: {choices}[/red]')


def _prompt_rate() -> float:
    """Prompt for a positive rate."""
    while True:
        value = click.prompt('Rate (TAO per 1 non-TAO asset)', type=float)
        if value > 0:
            return value
        console.print('[red]Rate must be positive[/red]')


@click.command('pair')
@click.argument('src_chain', required=False, default=None, type=str)
@click.argument('src_addr', required=False, default=None, type=str)
@click.argument('dst_chain', required=False, default=None, type=str)
@click.argument('dst_addr', required=False, default=None, type=str)
@click.argument('rate', required=False, default=None, type=float)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def post_pair(
    src_chain: str | None,
    src_addr: str | None,
    dst_chain: str | None,
    dst_addr: str | None,
    rate: float | None,
    yes: bool,
):
    """Post a trading pair to chain via commitment.

    \b
    All arguments are optional — if omitted, you'll be prompted interactively.

    \b
    Arguments:
        SRC_CHAIN   Source chain ID (e.g. btc, tao)
        SRC_ADDR    Your receiving address on source chain
        DST_CHAIN   Destination chain ID (e.g. tao, btc)
        DST_ADDR    Your sending address on destination chain
        RATE        TAO per 1 non-TAO asset (e.g. 345 means 1 BTC = 345 TAO)

    \b
    Examples:
        alw miner post                                        (interactive wizard)
        alw miner post btc bc1q...abc tao 5Cxyz...def 345    (all at once)
    """
    # --- Prompt for any missing arguments ---
    if src_chain is None:
        src_chain = _prompt_chain('Source chain')
    else:
        src_chain = src_chain.lower()
        if src_chain not in SUPPORTED_CHAINS:
            console.print(f'[red]Unsupported source chain: {src_chain}[/red]')
            console.print(f'[dim]Supported: {", ".join(SUPPORTED_CHAINS.keys())}[/dim]')
            return

    if src_addr is None:
        src_addr = click.prompt(f'Your receiving address on {SUPPORTED_CHAINS[src_chain].name}')

    if dst_chain is None:
        dst_chain = _prompt_chain('Destination chain', exclude=src_chain)
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

    if rate is None:
        rate = _prompt_rate()
    elif rate <= 0:
        console.print('[red]Rate must be positive[/red]')
        return

    # Normalize to canonical direction: non-TAO → TAO.
    # Rate is always "TAO per 1 non-TAO asset" regardless of direction.
    if src_chain == 'tao' and dst_chain != 'tao':
        console.print('[dim]Normalizing pair direction to canonical form (non-TAO -> TAO).[/dim]')
        src_chain, dst_chain = dst_chain, src_chain
        src_addr, dst_addr = dst_addr, src_addr

    config, wallet, subtensor, _ = get_cli_context(need_client=False)
    netuid = config['netuid']

    rate_str = f'{rate:g}'
    commitment_data = f'v{COMMITMENT_VERSION}:{src_chain}:{src_addr}:{dst_chain}:{dst_addr}:{rate_str}'

    non_tao = src_chain if src_chain != 'tao' else dst_chain
    non_tao_ticker = non_tao.upper()

    console.print('\n[bold]Posting trading pair commitment[/bold]\n')
    console.print(f'  Source:      [cyan]{SUPPORTED_CHAINS[src_chain].name}[/cyan] ({src_addr})')
    console.print(f'  Destination: [cyan]{SUPPORTED_CHAINS[dst_chain].name}[/cyan] ({dst_addr})')
    console.print(f'  Rate:        [green]1 {non_tao_ticker} = {rate:g} TAO[/green]')
    console.print(f'  Netuid:      {netuid}')
    console.print(f'  Data:        [dim]{commitment_data}[/dim]\n')

    if not yes and not click.confirm('Confirm posting this pair?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        data_bytes = commitment_data.encode('utf-8')
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
