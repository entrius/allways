"""alw swap post-tx - Submit source transaction hash for a pending swap reservation."""

import click

from allways.chain_providers import create_chain_providers
from allways.cli.dendrite_lite import discover_validators, get_ephemeral_wallet
from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    blocks_to_minutes_str,
    clear_pending_swap,
    console,
    get_cli_context,
    hydrate_pending_swap,
    load_pending_swap,
    loading,
    mark_pending_swap_tx_sent,
    print_contract_error,
    resolve_source_tx_block,
)
from allways.cli.swap_commands.swap import from_smallest_unit, poll_for_swap_creation, sign_and_broadcast_confirm
from allways.constants import NETUID_FINNEY
from allways.contract_client import ContractError


@click.command('post-tx', cls=StyledCommand, show_disclaimer=True)
@click.argument('tx_hash', required=False, default=None, type=str)
@click.option(
    '--block',
    'tx_block',
    type=int,
    default=0,
    help=(
        'Override the source-tx block number. Usually unnecessary — the CLI '
        'looks it up automatically across the whole reservation window. Use '
        'this only when automatic lookup fails (e.g. running against a node '
        'that has pruned block bodies, or the tx landed on a different node).'
    ),
)
def post_tx_command(tx_hash: str, tx_block: int):
    """Submit your source transaction hash for a pending swap reservation.

    [dim]Reads reservation context from ~/.allways/pending_swap.json (saved by `alw swap now`).[/dim]

    [dim]Examples:
        $ alw swap post-tx abc123def...
        $ alw swap post-tx abc123def... --block 12345   (escape hatch)
        $ alw swap post-tx  (prompts for tx hash)[/dim]
    """
    config, wallet, subtensor, client = get_cli_context()
    # --netuid handled globally in main.py; config['netuid'] already resolved.
    netuid = int(config.get('netuid', NETUID_FINNEY))

    # Load pending swap state
    state = load_pending_swap()
    if not state:
        console.print('[red]No pending swap found.[/red]')
        console.print('[dim]Run `alw swap now` to initiate a swap first.[/dim]')
        return
    # Hydrate from contract — local file is the slim user-only set; chains,
    # amounts, miner addresses are pulled live from get_reservation.
    hydrate_pending_swap(state, client)

    # Validate reservation is still active on-chain
    try:
        with loading('Reading reservation status...'):
            reserved_until = client.get_miner_reserved_until(state.miner_hotkey)
            current_block = subtensor.get_current_block()
    except ContractError as e:
        print_contract_error('Failed to read reservation status', e)
        return

    if reserved_until <= current_block:
        clear_pending_swap()
        console.print('[red]Reservation has expired.[/red]')
        console.print('[dim]Run `alw swap now` to start a new swap.[/dim]')
        return

    remaining = reserved_until - current_block
    human_amount = from_smallest_unit(state.from_amount, state.from_chain)

    console.print('\n[bold]Pending Swap[/bold]\n')
    console.print(f'  Pair:    {state.from_chain.upper()} -> {state.to_chain.upper()}')
    console.print(f'  Send:    {human_amount} {state.from_chain.upper()}')
    console.print(f'  To:      {state.miner_from_address}')
    console.print(f'  Miner:   UID {state.miner_uid}')
    console.print(f'  Expires: ~{remaining} blocks ({blocks_to_minutes_str(remaining)})\n')

    # Get transaction hash
    if not tx_hash:
        tx_hash = click.prompt('Enter your source transaction hash')

    if not tx_hash or not tx_hash.strip():
        console.print('[red]Transaction hash is required[/red]')
        return

    tx_hash = tx_hash.strip()
    # The user is asserting they've sent funds — record it so that even if
    # validators reject the confirm, `alw view reservation` reflects reality.
    mark_pending_swap_tx_sent(tx_hash)

    # Set up chain provider and signing key
    chain_providers = create_chain_providers(subtensor=subtensor)
    provider = chain_providers.get(state.from_chain)
    if not provider:
        console.print(f'[red]No chain provider for {state.from_chain}[/red]')
        return

    from_key = wallet.coldkey if state.from_chain == 'tao' else None

    # Resolve the tx's block so validators can ±3-hint rather than scan.
    # --block wins; otherwise a reservation-wide scan via the shared helper.
    from_tx_block = tx_block
    if from_tx_block > 0:
        console.print(f'[dim]Using --block {from_tx_block} (skipping lookup).[/dim]')
    else:
        from_tx_block = resolve_source_tx_block(
            provider=provider,
            tx_hash=tx_hash,
            expected_recipient=state.miner_from_address,
            expected_amount=state.from_amount,
            subtensor=subtensor,
            client=client,
            reserved_until_block=reserved_until,
        )

    # Discover validators
    with loading('Discovering validators...'):
        validator_axons = discover_validators(subtensor, netuid, contract_client=client)
    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]')
        return

    ephemeral_wallet = get_ephemeral_wallet()

    # Sign and broadcast confirm synapse
    accepted, queued, info = sign_and_broadcast_confirm(
        provider,
        state.user_from_address,
        from_key,
        tx_hash,
        state.miner_hotkey,
        state.receive_address,
        validator_axons,
        ephemeral_wallet,
        from_chain=state.from_chain,
        to_chain=state.to_chain,
        from_tx_block=from_tx_block,
        miner_uid=state.miner_uid,
    )

    if accepted == 0:
        # Translator already printed the headline. Suppress retry hint when the
        # failure is deterministic — the same tx hash will reject the same way.
        if not info.deterministic:
            console.print('[dim]You can retry this command.[/dim]')
        return

    if queued > 0 and queued == accepted:
        console.print('\n[green]Validators queued your transaction for auto-confirmation.[/green]')
        console.print(
            '[dim]Swap will be initiated once confirmations are reached. Check progress: alw view reservation[/dim]\n'
        )
        return

    # Poll for swap creation
    swap_id = poll_for_swap_creation(client, state.miner_hotkey)
    if swap_id is not None:
        clear_pending_swap()
        console.print(f'\n[green bold]Swap initiated! ID: {swap_id}[/green bold]')
        console.print(f'[dim]Watch with: alw view swap {swap_id} --watch[/dim]\n')
    else:
        console.print('[yellow]Votes submitted but swap not yet on-chain. Check: alw view reservation[/yellow]')
        console.print('[dim]State file kept — you can retry this command.[/dim]\n')
