"""alw miner - Miner dashboard commands."""

import asyncio
import time
from typing import Optional

import click
from rich.table import Table

from allways.chains import get_chain
from allways.classes import SwapStatus
from allways.cli.dendrite_lite import discover_validators
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    SWAP_STATUS_COLORS,
    console,
    from_rao,
    get_cli_context,
    loading,
    print_contract_error,
    read_miner_commitment,
)
from allways.constants import FEE_DIVISOR
from allways.contract_client import ContractError


@click.group('miner', cls=StyledGroup)
def miner_group():
    """Miner dashboard commands."""
    pass


@miner_group.command('status')
@click.option('--hotkey', default=None, type=str, help='Miner hotkey to check (default: your hotkey)')
def miner_status(hotkey: str):
    """View miner status: collateral, committed pair, and active swaps.

    [dim]Examples:
        $ alw miner status
        $ alw miner status --hotkey 5Cxyz...[/dim]
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
        print_contract_error('Failed to read miner data', e)
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
        src_up, dst_up = pair.from_chain.upper(), pair.to_chain.upper()
        fwd_disabled = pair.rate == 0
        ctr_disabled = pair.counter_rate == 0
        if fwd_disabled or ctr_disabled or pair.rate_str != pair.counter_rate_str:
            console.print(f'  {src_up} ↔ {dst_up}')
            if fwd_disabled:
                console.print(f'    {src_up} → {dst_up}: [yellow]not supported[/yellow]')
            else:
                console.print(f'    {src_up} → {dst_up}: [green]send 1 {src_up}, get {pair.rate:g} {dst_up}[/green]')
            if ctr_disabled:
                console.print(f'    {dst_up} → {src_up}: [yellow]not supported[/yellow]')
            else:
                console.print(
                    f'    {dst_up} → {src_up}: [green]send {pair.counter_rate:g} {dst_up}, get 1 {src_up}[/green]'
                )
        else:
            console.print(f'  {src_up} ↔ {dst_up} @ [green]{pair.rate:g}[/green]')
        console.print(f'  Source address: [dim]{pair.from_address}[/dim]')
        console.print(f'  Dest address:   [dim]{pair.to_address}[/dim]')
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
        pair_str = f'{swap.from_chain.upper()}/{swap.to_chain.upper()}'
        color = SWAP_STATUS_COLORS.get(swap.status, 'white')
        status_display = f'[{color}]{swap.status.name}[/{color}]'

        swap_table.add_row(
            str(swap.id),
            pair_str,
            str(swap.from_amount),
            status_display,
            str(swap.initiated_block),
        )

    console.print(swap_table)
    console.print(f'\n[dim]Total: {len(swaps)} active swaps[/dim]\n')


def friendly_rejection(reason: str) -> str:
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

    [dim]Broadcasts a MinerActivateSynapse to all validators. Each validator
    independently verifies commitment and collateral, then votes on contract.
    Activation requires quorum.[/dim]

    [dim]Examples:
        $ alw miner activate[/dim]
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
    with loading('Discovering validators...'):
        validator_axons = discover_validators(subtensor, netuid, contract_client=client)

    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]\n')
        return

    # Broadcast
    with loading(f'Broadcasting to {len(validator_axons)} validators...'):
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
            friendly = friendly_rejection(raw_reason)
            console.print(f'  Validator {i + 1}: [red]rejected[/red] — {friendly}')

    console.print(f'\n{accepted}/{len(validator_axons)} validators accepted')

    if accepted == 0:
        console.print('[red]Activation failed — no validators accepted the request.[/red]')
        console.print('[dim]Prerequisites:[/dim]')
        console.print('[dim]  - Hotkey registered on this subnet (btcli subnets register)[/dim]')
        console.print('[dim]  - Trading pair posted (alw miner post)[/dim]')
        console.print('[dim]  - Collateral deposited >= 0.1 TAO (alw collateral deposit)[/dim]')
        console.print('[dim]Run `alw miner status` to see which are missing.[/dim]\n')
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

    [dim]Calls deactivate() on the contract directly. No validator needed.
    After deactivation, must wait 2 * timeout_blocks before withdrawing collateral.[/dim]

    [dim]Examples:
        $ alw miner deactivate[/dim]
    """
    _, wallet, subtensor, client = get_cli_context()
    hotkey = wallet.hotkey.ss58_address

    console.print(f'\n[bold]Miner Deactivate: {hotkey[:16]}...[/bold]\n')

    # Pre-flight: the contract rejects deactivate() with MinerHasActiveSwap or
    # MinerReserved (lib.rs:935-940). ink! returns those as a raw ContractReverted
    # with no variant name, so detect them here to show why instead of a module
    # error dump.
    try:
        if client.get_miner_has_active_swap(hotkey):
            console.print(
                '[red]Cannot deactivate: you have an active swap.[/red]\n'
                '[dim]Wait for it to complete or time out, then try again. '
                'Check with: alw view active-swaps[/dim]\n'
            )
            return
        reserved_until = client.get_miner_reserved_until(hotkey)
        current_block = subtensor.get_current_block()
        if reserved_until > current_block:
            remaining = reserved_until - current_block
            console.print(
                f'[red]Cannot deactivate: you have an active reservation '
                f'(~{remaining} blocks, ~{remaining * 12 // 60} min left).[/red]\n'
                '[dim]Wait for it to expire or get consumed, then try again.[/dim]\n'
            )
            return
    except ContractError as e:
        print_contract_error('Failed to read miner state', e)
        return

    try:
        with loading('Submitting transaction...'):
            tx_hash = client.deactivate_miner(wallet, hotkey)
        console.print(f'[green]Deactivated successfully[/green] (tx: {tx_hash[:16]}...)')
        timeout = client.get_fulfillment_timeout()
        console.print(
            f'[dim]Collateral withdrawal available after {timeout * 2} blocks (~{timeout * 2 * 12 // 60} min)[/dim]\n'
        )
    except ContractError as e:
        print_contract_error('Failed to deactivate', e)


@miner_group.command('mark-fulfilled')
@click.option('--swap-id', required=True, type=int, help='Swap ID to mark as fulfilled')
@click.option('--tx-hash', required=True, type=str, help='Destination chain transaction hash')
@click.option(
    '--amount',
    default=None,
    type=int,
    help=(
        "Amount sent, in the dest chain's smallest unit (rao for TAO, satoshi for BTC). "
        'Optional — if omitted, the CLI computes the expected amount from the swap struct '
        '(rate × from_amount − fee).'
    ),
)
@click.option(
    '--block',
    default=0,
    type=int,
    help='Destination chain block number (default: 0, which tells validators to scan)',
)
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
def miner_mark_fulfilled(swap_id: int, tx_hash: str, amount: Optional[int], block: int, yes: bool):
    """Manually mark a swap as fulfilled on the contract.

    [dim]Use this when you've sent destination funds manually (e.g. via external wallet)
    and need to notify the contract.[/dim]

    [dim]Examples:
        $ alw miner mark-fulfilled --swap-id 5 --tx-hash abc123...           (amount inferred)
        $ alw miner mark-fulfilled --swap-id 5 --tx-hash abc123... --amount 27500   (override)[/dim]
    """
    from allways.utils.rate import expected_swap_amounts

    _, wallet, _, client = get_cli_context()
    hotkey = wallet.hotkey.ss58_address

    # Preflight: the contract rejects mark_fulfilled with InvalidStatus when
    # status != Active, and we can't give back any of that detail once ink!
    # raises ContractReverted. Look up the swap ourselves so we can show why.
    try:
        swap = client.get_swap(swap_id)
    except ContractError as e:
        print_contract_error('Failed to read swap', e)
        return
    if swap is None:
        console.print(f'[red]Swap #{swap_id} not found on-chain.[/red]')
        return

    if swap.miner_hotkey != hotkey:
        console.print(
            f'[red]Swap #{swap_id} is assigned to a different miner ({swap.miner_hotkey[:16]}...), not you.[/red]\n'
        )
        return

    if swap.status != SwapStatus.ACTIVE:
        console.print(
            f'[yellow]Swap #{swap_id} is not Active — current status: '
            f'[bold]{swap.status.name}[/bold].[/yellow]\n'
            '[dim]mark_fulfilled is only accepted once, while the swap is Active. '
            'The contract will reject a re-call.[/dim]'
        )
        if swap.to_tx_hash:
            console.print(f'[dim]  Already recorded: tx={swap.to_tx_hash[:20]}..., to_amount={swap.to_amount}[/dim]\n')
        return

    # Infer the expected dest amount from the swap struct if caller didn't pass one.
    # Same formula the validator uses (rate × from_amount − fee), so the consensus
    # path stays aligned regardless of whether the operator guessed a raw number.
    if amount is None:
        _, inferred = expected_swap_amounts(swap, FEE_DIVISOR)
        if inferred == 0:
            console.print(
                '[red]Could not infer amount from swap struct (rate produced 0).[/red]\n'
                '[dim]Pass --amount explicitly (smallest unit of the dest chain).[/dim]'
            )
            return
        amount = inferred
        amount_source = 'inferred from swap struct'
    else:
        amount_source = 'operator-provided'

    to_chain = swap.to_chain
    to_chain_def = get_chain(to_chain)
    human_amount = amount / (10**to_chain_def.decimals)

    console.print(f'\n[bold]Mark Fulfilled — Swap #{swap_id}[/bold]\n')
    console.print(f'  Swap ID:   {swap_id}')
    console.print(f'  Tx Hash:   {tx_hash}')
    console.print(
        f'  Amount:    {amount} {to_chain_def.native_unit}  (~{human_amount:.8f} {to_chain.upper()}, {amount_source})'
    )
    console.print(f'  Block:     {block if block else "(validators will scan)"}')
    console.print(f'  Hotkey:    {hotkey}\n')

    if not yes and not click.confirm('Confirm marking swap as fulfilled?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    try:
        with loading('Submitting transaction...'):
            result = client.mark_fulfilled(
                wallet=wallet,
                swap_id=swap_id,
                to_tx_hash=tx_hash,
                to_amount=amount,
                to_tx_block=block,
            )
        console.print(f'[green]Swap #{swap_id} marked as fulfilled[/green] (tx: {result[:16]}...)\n')
    except ContractError as e:
        print_contract_error('Failed to mark fulfilled', e)
