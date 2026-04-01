"""alw miner - Miner dashboard commands."""

import asyncio
import time

import rich_click as click
from rich.table import Table

from allways.cli.dendrite_lite import discover_validators
from allways.cli.swap_commands.helpers import (
    SWAP_STATUS_COLORS,
    console,
    from_rao,
    get_cli_context,
    loading,
    read_miner_commitment,
)
from allways.contract_client import ContractError


@click.group('miner')
def miner_group():
    """Miner dashboard commands.

    \b
    Subcommands:
        post            Post a trading pair commitment
        status          View miner collateral, pair, and active swaps
        activate        Activate miner via validator API
        deactivate      Deactivate miner via validator API
        mark-fulfilled  Manually mark a swap as fulfilled
    """
    pass


@miner_group.command('status')
@click.option('--hotkey', default=None, type=str, help='Miner hotkey to check (default: your hotkey)')
def miner_status(hotkey: str):
    """View miner status: collateral, committed pair, and active swaps.

    Example:
        alw miner status
        alw miner status --hotkey 5Cxyz...
    """
    config, wallet, subtensor, client = get_cli_context()
    netuid = config['netuid']

    if not hotkey:
        hotkey = wallet.hotkey.ss58_address

    console.print(f'\n[bold]Miner Status — {hotkey[:16]}...[/bold]\n')

    # Section 1: Collateral & Status
    try:
        with loading('Reading miner status...'):
            total_rao = client.get_miner_collateral(hotkey)
            min_required_rao = client.get_min_collateral()
            is_active = client.get_miner_active_flag(hotkey)
            has_active_swap = client.get_miner_has_active_swap(hotkey)
    except ContractError as e:
        console.print(f'[red]Failed to read miner data: {e}[/red]')
        return

    table = Table(title='Collateral & Status', show_header=True)
    table.add_column('Field', style='cyan')
    table.add_column('Value', style='green')

    table.add_row('Collateral', f'{from_rao(total_rao):.4f} TAO')
    table.add_row('Min Required', f'{from_rao(min_required_rao):.4f} TAO')
    status_str = '[green]Active[/green]' if is_active else '[red]Inactive[/red]'
    table.add_row('Status', status_str)
    swap_str = '[yellow]Yes[/yellow]' if has_active_swap else '[dim]No[/dim]'
    table.add_row('Has Active Swap', swap_str)

    console.print(table)
    console.print()

    # Section 2: Committed Pair
    pair = read_miner_commitment(subtensor, netuid, hotkey)

    if pair:
        console.print('[bold]Committed Pair[/bold]\n')
        non_tao = pair.source_chain.upper()
        if pair.rate_reverse_str and pair.rate != pair.rate_reverse:
            console.print(f'  {non_tao}/TAO')
            console.print(f'    {non_tao} -> TAO: [green]{pair.rate:g}[/green]')
            console.print(f'    TAO -> {non_tao}: [green]{pair.rate_reverse:g}[/green]')
        else:
            console.print(f'  {non_tao}/TAO @ [green]{pair.rate:g}[/green]')
        console.print(f'  Source address: [dim]{pair.source_address}[/dim]')
        console.print(f'  Dest address:   [dim]{pair.dest_address}[/dim]')
    else:
        console.print('[yellow]No committed pair found[/yellow]')

    console.print()

    # Section 3: Active Swaps
    try:
        swaps = client.get_miner_active_swaps(hotkey)
    except ContractError as e:
        console.print(f'[yellow]Could not read active swaps: {e}[/yellow]')
        swaps = []

    if not swaps:
        console.print('[dim]No active swaps[/dim]\n')
        return

    swap_table = Table(title='Active Swaps', show_header=True)
    swap_table.add_column('ID', style='cyan')
    swap_table.add_column('Pair', style='green')
    swap_table.add_column('Amount', style='yellow')
    swap_table.add_column('Status', style='bold')
    swap_table.add_column('Block', style='dim')

    for swap in swaps:
        pair_str = f'{swap.source_chain.upper()}/{swap.dest_chain.upper()}'
        color = SWAP_STATUS_COLORS.get(swap.status, 'white')
        status_display = f'[{color}]{swap.status.name}[/{color}]'

        swap_table.add_row(
            str(swap.id),
            pair_str,
            str(swap.source_amount),
            status_display,
            str(swap.initiated_block),
        )

    console.print(swap_table)
    console.print(f'\n[dim]Total: {len(swaps)} active swaps[/dim]\n')


def _friendly_rejection(reason: str) -> str:
    """Convert raw validator rejection reasons into human-readable messages."""
    if not reason:
        return ''
    r = reason.lower()
    if 'already active' in r:
        return 'miner is already active'
    if 'ContractReverted' in reason or 'contractreverted' in r:
        return 'contract reverted (collateral/validator issue)'
    if 'insufficient collateral' in r:
        return reason
    if 'no commitment found' in r:
        return 'no pair commitment found — run: alw pair set'
    if 'not registered' in r:
        return 'hotkey not registered on subnet'
    return reason


@miner_group.command('activate')
def miner_activate():
    """Activate miner via dendrite broadcast to all validators.

    Broadcasts a MinerActivateSynapse to all validators. Each validator
    independently verifies commitment and collateral, then votes on contract.
    Activation requires quorum.

    Example:
        alw miner activate
    """
    import bittensor as bt

    from allways.synapses import MinerActivateSynapse

    config, wallet, subtensor, client = get_cli_context()
    netuid = config['netuid']
    hotkey = wallet.hotkey.ss58_address

    console.print(f'\n[bold]Miner Activate: {hotkey[:16]}...[/bold]\n')

    # Pre-flight: check if already active
    try:
        if client.get_miner_active_flag(hotkey):
            console.print('[yellow]Miner is already active.[/yellow]\n')
            return
    except ContractError:
        pass

    # Build synapse
    timestamp = str(int(time.time()))
    message = f'activate:{hotkey}:{timestamp}'
    signature = wallet.hotkey.sign(message.encode()).hex()

    synapse = MinerActivateSynapse(hotkey=hotkey, signature=signature, message=message)

    # Discover whitelisted validators from metagraph
    dendrite = bt.Dendrite(wallet=wallet)
    validator_axons = discover_validators(subtensor, netuid, contract_client=client)

    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]\n')
        return

    console.print(f'Broadcasting to {len(validator_axons)} validators...')

    # Broadcast
    responses = asyncio.get_event_loop().run_until_complete(
        dendrite(axons=validator_axons, synapse=synapse, deserialize=False, timeout=30.0)
    )

    # Show per-validator results
    accepted = 0
    for i, resp in enumerate(responses):
        if getattr(resp, 'accepted', None):
            console.print(f'  Validator {i + 1}: [green]accepted[/green]')
            accepted += 1
        else:
            raw_reason = getattr(resp, 'rejection_reason', '') or ''
            friendly = _friendly_rejection(raw_reason)
            console.print(f'  Validator {i + 1}: [red]rejected[/red] — {friendly}')

    console.print(f'\n{accepted}/{len(validator_axons)} validators accepted')

    if accepted == 0:
        console.print('[red]Activation failed — no validators accepted the request.[/red]')
        console.print('[dim]Check prerequisites: alw miner status[/dim]\n')
        return

    # Poll contract for activation
    with loading('Waiting for quorum...'):
        for _ in range(15):
            time.sleep(2)
            try:
                if client.get_miner_active_flag(hotkey):
                    break
            except ContractError:
                pass
        else:
            console.print(
                '[yellow]Votes submitted but quorum not yet reached. Check status with: alw miner status[/yellow]\n'
            )
            return

    console.print('[green]Miner activated successfully[/green]\n')


@miner_group.command('deactivate')
def miner_deactivate():
    """Deactivate miner directly on contract (permissionless).

    Calls deactivate() on the contract directly. No validator needed.
    After deactivation, must wait 2 * timeout_blocks before withdrawing collateral.

    Example:
        alw miner deactivate
    """
    _, wallet, _, client = get_cli_context()
    hotkey = wallet.hotkey.ss58_address

    console.print(f'\n[bold]Miner Deactivate: {hotkey[:16]}...[/bold]\n')

    try:
        with loading('Submitting transaction...'):
            tx_hash = client.deactivate_miner(wallet, hotkey)
        console.print(f'[green]Deactivated successfully[/green] (tx: {tx_hash[:16]}...)')
        timeout = client.get_fulfillment_timeout()
        console.print(
            f'[dim]Collateral withdrawal available after {timeout * 2} blocks (~{timeout * 2 * 12 // 60} min)[/dim]\n'
        )
    except ContractError as e:
        console.print(f'[red]Failed to deactivate: {e}[/red]\n')


@miner_group.command('mark-fulfilled')
@click.option('--swap-id', required=True, type=int, help='Swap ID to mark as fulfilled')
@click.option('--tx-hash', required=True, type=str, help='Destination chain transaction hash')
@click.option('--amount', required=True, type=int, help='Amount sent (in smallest unit, e.g. rao or satoshi)')
@click.option('--block', default=0, type=int, help='Destination chain block number (default: 0)')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def miner_mark_fulfilled(swap_id: int, tx_hash: str, amount: int, block: int, yes: bool):
    """Manually mark a swap as fulfilled on the contract.

    \b
    Use this when you've sent destination funds manually (e.g. via external
    wallet) and need to notify the contract.

    Examples:
        alw miner mark-fulfilled --swap-id 5 --tx-hash abc123... --amount 500000000
    """
    _, wallet, _, client = get_cli_context()
    hotkey = wallet.hotkey.ss58_address

    console.print(f'\n[bold]Mark Fulfilled — Swap #{swap_id}[/bold]\n')
    console.print(f'  Swap ID:   {swap_id}')
    console.print(f'  Tx Hash:   {tx_hash}')
    console.print(f'  Amount:    {amount}')
    console.print(f'  Block:     {block}')
    console.print(f'  Hotkey:    {hotkey}\n')

    if not yes and not click.confirm('Confirm marking swap as fulfilled?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            result = client.mark_fulfilled(
                wallet=wallet,
                swap_id=swap_id,
                dest_tx_hash=tx_hash,
                dest_amount=amount,
                dest_tx_block=block,
            )
        console.print(f'[green]Swap #{swap_id} marked as fulfilled[/green] (tx: {result[:16]}...)\n')
    except ContractError as e:
        console.print(f'[red]Failed to mark fulfilled: {e}[/red]\n')
