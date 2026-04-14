"""alw swap post-tx - Submit source transaction hash for a pending swap reservation."""

import click

from allways.chain_providers import create_chain_providers
from allways.cli.dendrite_lite import discover_validators, get_ephemeral_wallet
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    clear_pending_swap,
    console,
    get_cli_context,
    load_pending_swap,
)
from allways.cli.swap_commands.swap import from_smallest_unit, poll_for_swap_creation, sign_and_broadcast_confirm
from allways.constants import NETUID_FINNEY
from allways.contract_client import ContractError


@click.command('post-tx', cls=StyledCommand, show_disclaimer=True)
@click.argument('tx_hash', required=False, default=None, type=str)
@click.option('--netuid', default=None, type=int, help='Subnet UID')
def post_tx_command(tx_hash: str, netuid: int):
    """Submit your source transaction hash for a pending swap reservation.

    [dim]Reads reservation context from ~/.allways/pending_swap.json (saved by `alw swap now`).[/dim]

    [dim]Examples:
        $ alw swap post-tx abc123def...
        $ alw swap post-tx  (prompts for tx hash)[/dim]
    """
    config, wallet, subtensor, client = get_cli_context()
    if netuid is None:
        netuid = int(config.get('netuid', NETUID_FINNEY))

    # Load pending swap state
    state = load_pending_swap()
    if not state:
        console.print('[red]No pending swap found.[/red]')
        console.print('[dim]Run `alw swap now` to initiate a swap first.[/dim]')
        return

    # Validate reservation is still active on-chain
    try:
        reserved_until = client.get_miner_reserved_until(state.miner_hotkey)
        current_block = subtensor.get_current_block()
    except ContractError as e:
        console.print(f'[red]Failed to read reservation status: {e}[/red]')
        return

    if reserved_until <= current_block:
        clear_pending_swap()
        console.print('[red]Reservation has expired.[/red]')
        console.print('[dim]Run `alw swap now` to start a new swap.[/dim]')
        return

    remaining = reserved_until - current_block
    remaining_min = remaining * SECONDS_PER_BLOCK / 60
    human_amount = from_smallest_unit(state.source_amount, state.source_chain)

    console.print('\n[bold]Pending Swap[/bold]\n')
    console.print(f'  Pair:    {state.source_chain.upper()} -> {state.dest_chain.upper()}')
    console.print(f'  Send:    {human_amount} {state.source_chain.upper()}')
    console.print(f'  To:      {state.miner_source_address}')
    console.print(f'  Miner:   UID {state.miner_uid}')
    console.print(f'  Expires: ~{remaining} blocks (~{remaining_min:.0f} min)\n')

    # Get transaction hash
    if not tx_hash:
        tx_hash = click.prompt('Enter your source transaction hash')

    if not tx_hash or not tx_hash.strip():
        console.print('[red]Transaction hash is required[/red]')
        return

    tx_hash = tx_hash.strip()

    # Set up chain provider and signing key
    chain_providers = create_chain_providers(subtensor=subtensor)
    provider = chain_providers.get(state.source_chain)
    if not provider:
        console.print(f'[red]No chain provider for {state.source_chain}[/red]')
        return

    source_key = wallet.coldkey if state.source_chain == 'tao' else None

    # Discover validators
    validator_axons = discover_validators(subtensor, netuid, contract_client=client)
    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]')
        return

    ephemeral_wallet = get_ephemeral_wallet()

    # Sign and broadcast confirm synapse
    accepted, queued = sign_and_broadcast_confirm(
        provider,
        state.user_source_address,
        source_key,
        tx_hash,
        state.miner_hotkey,
        state.receive_address,
        validator_axons,
        ephemeral_wallet,
        source_chain=state.source_chain,
        dest_chain=state.dest_chain,
    )

    if accepted == 0:
        console.print('[yellow]No validators accepted. You can retry this command.[/yellow]')
        return

    if queued > 0 and queued == accepted:
        clear_pending_swap()
        console.print('\n[green]Validators queued your transaction for auto-confirmation.[/green]')
        console.print('[dim]Swap will be initiated once confirmations are reached. Check: alw view swaps[/dim]\n')
        return

    # Poll for swap creation
    swap_id = poll_for_swap_creation(client, state.miner_hotkey)
    if swap_id is not None:
        clear_pending_swap()
        console.print(f'\n[green bold]Swap initiated! ID: {swap_id}[/green bold]')
        console.print(f'[dim]Track with: alw view swap {swap_id}[/dim]\n')
    else:
        console.print('[yellow]Votes submitted but swap not yet on-chain. Check: alw view swaps[/yellow]')
        console.print('[dim]State file kept — you can retry this command.[/dim]\n')
