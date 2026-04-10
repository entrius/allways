"""alw swap - Guided interactive swap with automatic fund sending."""

import os
import time
from typing import Optional

import bittensor as bt
import click
from rich.panel import Panel
from rich.table import Table

from allways.chain_providers import create_chain_providers
from allways.chains import SUPPORTED_CHAINS, canonical_pair, get_chain
from allways.classes import MinerPair, SwapStatus
from allways.cli.dendrite_lite import broadcast_synapse, discover_validators, get_ephemeral_wallet
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    PendingSwapState,
    clear_pending_swap,
    console,
    from_rao,
    get_cli_context,
    is_local_network,
    load_pending_swap,
    save_pending_swap,
)
from allways.commitments import read_miner_commitments
from allways.constants import DEFAULT_FEE_DIVISOR, NETUID_FINNEY
from allways.contract_client import ContractError
from allways.synapses import SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.rate import apply_fee_deduction, calculate_dest_amount


def _to_smallest_unit(amount: float, chain_id: str) -> int:
    """Convert a human-readable amount to the smallest unit for a chain.

    Uses Decimal to avoid IEEE 754 float artifacts (e.g. 0.1 * 10^9 = 99999999).
    """
    from decimal import Decimal

    chain = get_chain(chain_id)
    return int(Decimal(str(amount)) * (10**chain.decimals))


def _from_smallest_unit(amount: int, chain_id: str) -> float:
    """Convert from smallest unit to human-readable amount."""
    chain = get_chain(chain_id)
    return amount / (10**chain.decimals)


# =========================================================================
# Shared functions (used by swap command, post_tx command)
# =========================================================================


def sign_and_broadcast_confirm(
    provider,
    user_source_address: str,
    source_key,
    source_tx_hash: str,
    miner_hotkey: str,
    receive_address: str,
    validator_axons: list,
    ephemeral_wallet,
    source_chain: str = '',
    dest_chain: str = '',
) -> tuple:
    """Sign source tx proof and broadcast SwapConfirmSynapse to validators.

    Returns (accepted_count, queued_count). Queued means the validator accepted
    but is waiting for source tx confirmations before voting to initiate.
    """
    console.print('[dim]Submitting swap to validators...[/dim]')
    proof_message = f'allways-swap:{source_tx_hash}'
    try:
        source_proof = provider.sign_source_proof(
            user_source_address,
            proof_message,
            source_key,
        )
    except Exception as e:
        console.print(f'[red]Failed to sign source proof: {e}[/red]')
        return 0, 0

    if not source_proof:
        console.print('[red]Source proof is empty — signing failed (check chain provider RPC connection)[/red]')
        return 0, 0

    confirm_synapse = SwapConfirmSynapse(
        reservation_id=miner_hotkey,
        source_tx_hash=source_tx_hash,
        source_tx_proof=source_proof,
        source_address=user_source_address,
        dest_address=receive_address,
        source_chain=source_chain,
        dest_chain=dest_chain,
    )

    console.print(f'  Broadcasting to {len(validator_axons)} validators...')
    confirm_responses = broadcast_synapse(ephemeral_wallet, validator_axons, confirm_synapse, timeout=60.0)

    accepted = 0
    queued = 0
    for i, resp in enumerate(confirm_responses):
        reason = getattr(resp, 'rejection_reason', '') or ''
        if getattr(resp, 'accepted', None):
            accepted += 1
            if 'Queued' in reason:
                queued += 1
                console.print(f'    V{i + 1}: [yellow]queued[/yellow] {reason}')
            else:
                console.print(f'    V{i + 1}: [green]ok[/green]')
        else:
            console.print(f'    V{i + 1}: [red]no[/red] {reason}')

    return accepted, queued


def poll_for_swap_creation(client, miner_hotkey: str) -> Optional[int]:
    """Poll contract until miner has an active swap. Returns swap_id or None."""
    with console.status('[dim]Waiting for swap to appear on-chain...[/dim]'):
        errors = 0
        for i in range(60):
            time.sleep(3)
            try:
                if client.get_miner_has_active_swap(miner_hotkey):
                    next_id = client.get_next_swap_id()
                    for check_id in range(next_id - 1, max(next_id - 5, 0), -1):
                        swap = client.get_swap(check_id)
                        if swap and swap.miner_hotkey == miner_hotkey:
                            return check_id
                errors = 0
            except ContractError:
                errors += 1
                if errors >= 5:
                    console.print('[yellow]Warning: contract unreachable, still waiting...[/yellow]')
                    errors = 0
    return None


def broadcast_reserve_with_retry(
    subtensor,
    client,
    provider,
    selected_pair,
    source_chain: str,
    dest_chain: str,
    source_amount: int,
    dest_amount: int,
    tao_amount: int,
    user_source_address: str,
    source_key,
    netuid: int,
    skip_confirm: bool = False,
    max_retries: int = 2,
):
    """Reserve miner via multi-validator consensus with retry.

    Returns (reserved_until, validator_axons, ephemeral_wallet) on success, None on failure.
    """
    current_block = subtensor.get_current_block()
    reserve_proof_message = f'allways-reserve:{user_source_address}:{current_block}'
    try:
        source_address_proof = provider.sign_source_proof(
            user_source_address,
            reserve_proof_message,
            source_key,
        )
    except Exception as e:
        console.print(f'[red]Failed to sign source address proof: {e}[/red]')
        return None

    if not source_address_proof:
        console.print('[red]Source address proof is empty — signing failed (check chain provider RPC connection)[/red]')
        return None

    synapse = SwapReserveSynapse(
        miner_hotkey=selected_pair.hotkey,
        tao_amount=tao_amount,
        source_amount=source_amount,
        dest_amount=dest_amount,
        source_address=user_source_address,
        source_address_proof=source_address_proof,
        block_anchor=current_block,
        source_chain=source_chain,
        dest_chain=dest_chain,
    )

    ephemeral_wallet = get_ephemeral_wallet()
    validator_axons = discover_validators(subtensor, netuid, contract_client=client)
    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]')
        return None

    reserved = False
    reserved_until = 0
    for attempt in range(max_retries + 1):
        if attempt > 0:
            current_block = subtensor.get_current_block()
            reserve_proof_message = f'allways-reserve:{user_source_address}:{current_block}'
            try:
                source_address_proof = provider.sign_source_proof(
                    user_source_address,
                    reserve_proof_message,
                    source_key,
                )
            except Exception as e:
                console.print(f'[red]Failed to sign source address proof: {e}[/red]')
                return None
            synapse = SwapReserveSynapse(
                miner_hotkey=selected_pair.hotkey,
                tao_amount=tao_amount,
                source_amount=source_amount,
                dest_amount=dest_amount,
                source_address=user_source_address,
                source_address_proof=source_address_proof,
                block_anchor=current_block,
                source_chain=source_chain,
                dest_chain=dest_chain,
            )

        console.print(f'  Broadcasting to {len(validator_axons)} validators...')
        responses = broadcast_synapse(ephemeral_wallet, validator_axons, synapse, timeout=30.0)

        accepted = sum(1 for r in responses if getattr(r, 'accepted', None))
        for i, resp in enumerate(responses):
            status = '[green]ok[/green]' if getattr(resp, 'accepted', None) else '[red]no[/red]'
            reason = getattr(resp, 'rejection_reason', '') or ''
            console.print(f'    V{i + 1}: {status} {reason}')

        if accepted == 0:
            console.print('[red]No validators accepted the reservation.[/red]')
            if attempt < max_retries and not skip_confirm and click.confirm('Retry?'):
                console.print('[dim]Retrying reservation...[/dim]')
                continue
            return None

        with console.status(f'[dim]Waiting for quorum ({accepted} votes submitted)...[/dim]'):
            quorum_errors = 0
            for _ in range(30):
                time.sleep(2)
                try:
                    reserved_until = client.get_miner_reserved_until(selected_pair.hotkey)
                    if reserved_until > current_block:
                        reserved = True
                        break
                    quorum_errors = 0
                except ContractError:
                    quorum_errors += 1
                    if quorum_errors >= 5:
                        console.print('[yellow]Warning: contract unreachable, still waiting...[/yellow]')
                        quorum_errors = 0
        if reserved:
            ttl_remaining = reserved_until - subtensor.get_current_block()
            console.print(f'[green]Miner reserved! ~{ttl_remaining * 12 // 60} min to send funds.[/green]')

        if reserved:
            break

        if attempt < max_retries:
            console.print('[yellow]Reservation quorum not reached.[/yellow]')
            if not skip_confirm and click.confirm('Retry?'):
                console.print('[dim]Retrying reservation...[/dim]')
            else:
                return None
        else:
            console.print('[red]Reservation failed — quorum not reached after retries.[/red]')
            return None

    return (reserved_until, validator_axons, ephemeral_wallet)


# =========================================================================
# Swap-specific helpers
# =========================================================================


def _display_receipt(swap):
    """Show a rich completion receipt after a successful swap."""
    src_chain_def = get_chain(swap.source_chain)
    dst_chain_def = get_chain(swap.dest_chain)
    src_human = swap.source_amount / (10**src_chain_def.decimals)
    dst_human = swap.dest_amount / (10**dst_chain_def.decimals)
    tao_human = swap.tao_amount / (10**9)

    # Calculate fee
    fee_divisor = 100  # 1% fee
    if swap.dest_chain == 'tao':
        fee_human = tao_human / fee_divisor
        fee_unit = 'TAO'
    else:
        fee_human = dst_human / fee_divisor
        fee_unit = swap.dest_chain.upper()

    src_tx = swap.source_tx_hash[:20] + '...' if len(swap.source_tx_hash) > 20 else swap.source_tx_hash
    dst_tx = swap.dest_tx_hash[:20] + '...' if len(swap.dest_tx_hash) > 20 else swap.dest_tx_hash

    receipt = (
        f'  [green]Sent:      {src_human:g} {swap.source_chain.upper()}[/green]\n'
        f'  [green]Received:  {dst_human:.8f} {swap.dest_chain.upper()}[/green]\n'
        f'  [dim]Fee:       {fee_human:.8f} {fee_unit} (1%)[/dim]\n'
        f'\n'
        f'  Source TX: [cyan]{src_tx}[/cyan]\n'
        f'  Dest TX:   [cyan]{dst_tx}[/cyan]\n'
        f'\n'
        f'  Timeline:\n'
        f'    [green]●[/green] Initiated   Block {swap.initiated_block}\n'
        f'    [green]●[/green] Fulfilled   Block {swap.fulfilled_block}\n'
        f'    [green]●[/green] Completed   Block {swap.completed_block}'
    )
    console.print()
    console.print(Panel(receipt, title='[bold green]Swap Complete[/bold green]', expand=False))
    console.print()


def _poll_for_swap_with_progress(client, miner_hotkey: str, source_chain: str, max_polls: int = 60):
    """Poll for swap creation with a live progress display."""
    with console.status('') as status:
        errors = 0
        for i in range(max_polls):
            elapsed = i * 3
            mins = elapsed // 60
            secs = elapsed % 60
            status.update(
                f'[dim]Waiting for validators to confirm and initiate swap... {mins}:{secs:02d} elapsed[/dim]'
            )

            time.sleep(3)
            try:
                if client.get_miner_has_active_swap(miner_hotkey):
                    next_id = client.get_next_swap_id()
                    for check_id in range(next_id - 1, max(next_id - 5, 0), -1):
                        swap = client.get_swap(check_id)
                        if swap and swap.miner_hotkey == miner_hotkey:
                            return check_id
                errors = 0
            except ContractError:
                errors += 1
                if errors >= 5:
                    console.print('[yellow]Warning: contract unreachable, still waiting...[/yellow]')
                    errors = 0
    return None


def _send_btc(chain_providers, config, to_address: str, amount_sat: int, from_address: str = None):
    """Send BTC with fallback: embit lightweight -> RPC -> manual (with retry).

    Returns (tx_hash, block_number) or None (manual fallback failed/skipped).
    """
    provider = chain_providers.get('btc')
    is_local = is_local_network(config.get('network', 'finney'))
    human_amount = amount_sat / 100_000_000

    max_retries = 2
    for attempt in range(max_retries + 1):
        # 1. Try embit lightweight wallet (mainnet/testnet only — no public APIs on regtest)
        if not is_local and hasattr(provider, 'send_amount_lightweight'):
            console.print('[dim]Sending BTC via lightweight wallet...[/dim]')
            result = provider.send_amount_lightweight(to_address, amount_sat, from_address=from_address)
            if result:
                console.print(f'[green]BTC sent (tx: {result[0][:16]}...)[/green]')
                return result

        # 2. Try Bitcoin Core RPC
        console.print('[dim]Sending BTC via Bitcoin Core RPC...[/dim]')
        result = provider.send_amount(to_address, amount_sat)
        if result:
            console.print(f'[green]BTC sent via RPC (tx: {result[0][:16]}...)[/green]')
            return result

        # Retry or fall through to manual
        if attempt < max_retries:
            console.print('[yellow]BTC send failed.[/yellow]')
            if not click.confirm('Retry?', default=True):
                break
            console.print('[dim]Retrying...[/dim]')
        else:
            console.print('[yellow]BTC send failed after retries.[/yellow]')

    # 3. Manual fallback
    console.print(f'\n  Send [green]{human_amount} BTC[/green] to: [cyan]{to_address}[/cyan]\n')
    tx_hash = click.prompt('Enter transaction hash after sending (or "skip" to exit)', default='')
    if not tx_hash or tx_hash.lower() == 'skip':
        console.print('[yellow]Swap paused. Resume with: alw swap post-tx <tx_hash>[/yellow]')
        return None
    return (tx_hash.strip(), 0)


# =========================================================================
# CLI command
# =========================================================================


@click.group('swap', cls=StyledGroup, show_disclaimer=True)
def swap_group():
    """Execute and manage cross-chain swaps."""


@swap_group.command('now', show_disclaimer=True)
@click.option('--netuid', default=None, type=int, help='Subnet UID')
@click.option('--src', 'source_chain_opt', default=None, help='Source chain (e.g. btc, tao)')
@click.option('--dest', 'dest_chain_opt', default=None, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', 'amount_opt', default=None, type=float, help='Amount to send in source chain units')
@click.option('--receive-address', 'receive_address_opt', default=None, help='Receive address on destination chain')
@click.option('--source-address', 'source_address_opt', default=None, help='Source address on source chain')
@click.option('--source-tx-hash', 'source_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option('--auto', 'auto_select', is_flag=True, help='Auto-select best rate miner')
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
def swap_now_command(
    netuid: int,
    source_chain_opt: Optional[str],
    dest_chain_opt: Optional[str],
    amount_opt: Optional[float],
    receive_address_opt: Optional[str],
    source_address_opt: Optional[str],
    source_tx_hash_opt: Optional[str],
    auto_select: bool,
    skip_confirm: bool,
):
    """Guided interactive swap - step by step.

    [dim]Walks through a complete swap from start to finish:
    - Select swap direction and miner
    - Enter amount and addresses
    - Funds are sent automatically when possible
    - Transaction hash is posted to validators automatically[/dim]

    [dim]Non-interactive mode (for scripting/testing):
        $ alw swap now --src btc --dest tao --amount 0.001 \\
            --receive-address 5C... --source-address bc1q... \\
            --source-tx-hash abc123... --auto --yes[/dim]

    [dim]Interactive mode:
        $ alw swap now[/dim]
    """
    config, wallet, subtensor, client = get_cli_context()
    if netuid is None:
        netuid = int(config.get('netuid', NETUID_FINNEY))

    try:
        if client.get_halted():
            console.print('[red]System is halted — no new swaps can be initiated. Please try again later.[/red]')
            return
    except ContractError:
        pass

    # Validate provided chain options early
    if source_chain_opt and source_chain_opt not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown source chain: {source_chain_opt}[/red]')
        return
    if dest_chain_opt and dest_chain_opt not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown destination chain: {dest_chain_opt}[/red]')
        return
    if source_chain_opt and dest_chain_opt and source_chain_opt == dest_chain_opt:
        console.print('[red]Source and destination chains must be different[/red]')
        return

    console.print('\n[bold]Allways Swap[/bold]\n')

    # Check for pending reservation
    existing = load_pending_swap()
    if existing:
        try:
            reserved_until = client.get_miner_reserved_until(existing.miner_hotkey)
            current_block = subtensor.get_current_block()
            if reserved_until > current_block:
                remaining = reserved_until - current_block
                remaining_min = remaining * SECONDS_PER_BLOCK / 60
                console.print(
                    f'[yellow]You have a pending reservation (~{remaining} blocks, ~{remaining_min:.0f} min left).[/yellow]'
                )
                console.print('  Complete it with: [cyan]alw swap post-tx <tx_hash>[/cyan]\n')
                if not skip_confirm and not click.confirm('Start a new swap instead?'):
                    return
            else:
                clear_pending_swap()
        except ContractError:
            console.print('[yellow]Could not verify existing reservation (contract unreachable)[/yellow]')

    # Interactive mode: force lightweight BTC (no local Bitcoin node needed).
    # Non-interactive mode: respect environment config (local dev uses node mode for RPC signing).
    # Always respect an explicitly-set BTC_MODE.
    if 'BTC_MODE' not in os.environ and not skip_confirm:
        os.environ['BTC_MODE'] = 'lightweight'
    chain_providers = create_chain_providers(subtensor=subtensor)

    # Step 1: Select swap direction
    if source_chain_opt and dest_chain_opt:
        source_chain = source_chain_opt
        dest_chain = dest_chain_opt
    else:
        chain_ids = list(SUPPORTED_CHAINS.keys())
        directions = [(s, d) for s in chain_ids for d in chain_ids if s != d]

        console.print('[bold]What would you like to swap?[/bold]\n')
        for idx, (src, dst) in enumerate(directions, 1):
            console.print(f'  {idx}. {SUPPORTED_CHAINS[src].name} -> {SUPPORTED_CHAINS[dst].name}')

        choice = click.prompt('\nSelect', type=int, default=1)
        if choice < 1 or choice > len(directions):
            console.print('[red]Invalid selection[/red]')
            return
        source_chain, dest_chain = directions[choice - 1]

    # Show send capability for the source chain (skip in non-interactive mode)
    if not skip_confirm:
        if source_chain == 'tao':
            console.print('\n  [green]TAO will be sent automatically from your wallet.[/green]')
        else:
            is_local = is_local_network(config.get('network', 'finney'))
            has_private_key = bool(os.environ.get('BTC_PRIVATE_KEY'))

            if not is_local and has_private_key:
                console.print('\n  [green]BTC_PRIVATE_KEY set — will attempt BTC sends locally.[/green]')
            else:
                console.print('\n  [yellow]No BTC_PRIVATE_KEY found.[/yellow]')
                console.print(
                    '  [yellow]You will need to manually send BTC to the miner address'
                    ' and then run [cyan]alw swap post-tx <tx_hash>[/cyan] with the transaction hash.[/yellow]'
                )
                if not click.confirm('  Continue?', default=True):
                    console.print('[yellow]Cancelled[/yellow]')
                    return

    # Step 2: Find available miners (bilateral matching)
    console.print('\n[dim]Reading miner commitments...[/dim]')
    all_pairs = read_miner_commitments(subtensor, netuid)

    matching_pairs = []
    for p in all_pairs:
        if p.source_chain == source_chain and p.dest_chain == dest_chain:
            if p.rate > 0:
                matching_pairs.append(p)
        elif p.source_chain == dest_chain and p.dest_chain == source_chain:
            rev_rate, rev_rate_str = p.get_rate_for_direction(source_chain)
            if rev_rate > 0:
                matching_pairs.append(
                    MinerPair(
                        uid=p.uid,
                        hotkey=p.hotkey,
                        source_chain=p.dest_chain,
                        source_address=p.dest_address,
                        dest_chain=p.source_chain,
                        dest_address=p.source_address,
                        rate=rev_rate,
                        rate_str=rev_rate_str,
                        counter_rate=p.rate,
                        counter_rate_str=p.rate_str,
                    )
                )

    if not matching_pairs:
        console.print('[yellow]No miners found for this pair[/yellow]\n')
        return

    available_miners = []
    try:
        for pair in matching_pairs:
            is_active = client.get_miner_active_flag(pair.hotkey)
            has_swap = client.get_miner_has_active_swap(pair.hotkey)
            collateral = client.get_miner_collateral(pair.hotkey)
            if is_active and not has_swap and collateral > 0:
                available_miners.append((pair, collateral))
    except ContractError as e:
        console.print(f'[red]Failed to read miner data: {e}[/red]')
        return

    if not available_miners:
        console.print('[yellow]No active miners available[/yellow]\n')
        return

    available_miners.sort(key=lambda x: x[0].rate, reverse=True)

    # Show miners table
    table = Table(title='Available Miners', show_header=True)
    table.add_column('#', style='dim')
    table.add_column('UID', style='cyan')
    table.add_column('Rate (TAO)', style='green')
    table.add_column('Collateral (TAO)', style='yellow')

    for idx, (pair, collateral) in enumerate(available_miners, 1):
        table.add_row(str(idx), str(pair.uid), f'{pair.rate:g}', f'{from_rao(collateral):.4f}')

    console.print(table)

    # Step 3: Select miner (default to best rate)
    canon_src, canon_dest = canonical_pair(source_chain, dest_chain)
    best_pair = available_miners[0][0]
    console.print(
        f'\n  Best rate: send 1 {source_chain.upper()}, get {best_pair.rate:g} {dest_chain.upper()} (Miner UID {best_pair.uid})'
    )

    if auto_select or len(available_miners) == 1:
        selected_pair, selected_collateral = available_miners[0]
    else:
        choice = click.prompt('Select miner #', type=int, default=1)
        if choice < 1 or choice > len(available_miners):
            console.print('[red]Invalid selection[/red]')
            return
        selected_pair, selected_collateral = available_miners[choice - 1]

    # Step 4: Enter amount
    amount = (
        amount_opt if amount_opt is not None else click.prompt(f'\nAmount to send ({source_chain.upper()})', type=float)
    )
    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    source_amount = _to_smallest_unit(amount, source_chain)
    is_reverse = source_chain != canon_src
    dest_amount = calculate_dest_amount(
        source_amount,
        selected_pair.rate_str,
        is_reverse,
        get_chain(canon_dest).decimals,
        get_chain(canon_src).decimals,
    )

    # Show estimated receive inline
    try:
        preview_fee_divisor = client.get_fee_divisor() or DEFAULT_FEE_DIVISOR
    except ContractError:
        preview_fee_divisor = DEFAULT_FEE_DIVISOR
        console.print(
            f'[yellow]Warning: using default fee ({100 / DEFAULT_FEE_DIVISOR:g}%) — could not read from contract[/yellow]'
        )
    preview_receives = apply_fee_deduction(dest_amount, preview_fee_divisor)
    preview_fee_pct = 100 / preview_fee_divisor
    console.print(
        f'  You will receive: ~[green]{_from_smallest_unit(preview_receives, dest_chain):.8f} {dest_chain.upper()}[/green]'
        f' (after {preview_fee_pct:g}% fee)'
    )

    if source_chain == 'tao':
        tao_amount = source_amount
    elif dest_chain == 'tao':
        tao_amount = dest_amount
    else:
        tao_amount = 0

    # Validate against contract min/max swap bounds
    try:
        min_swap = client.get_min_swap_amount()
        max_swap = client.get_max_swap_amount()
        if min_swap > 0 and tao_amount < min_swap:
            console.print(
                f'[red]Amount too low. Minimum swap: {from_rao(min_swap):.4f} TAO equivalent '
                f'(you entered {from_rao(tao_amount):.4f} TAO equivalent).[/red]'
            )
            return
        if max_swap > 0 and tao_amount > max_swap:
            console.print(
                f'[red]Amount too high. Maximum swap: {from_rao(max_swap):.4f} TAO equivalent '
                f'(you entered {from_rao(tao_amount):.4f} TAO equivalent).[/red]'
            )
            return
    except ContractError:
        console.print('[yellow]Warning: could not verify swap bounds (contract unreachable)[/yellow]')

    try:
        fee_divisor = client.get_fee_divisor() or DEFAULT_FEE_DIVISOR
    except ContractError:
        fee_divisor = DEFAULT_FEE_DIVISOR
        if preview_fee_divisor != DEFAULT_FEE_DIVISOR:
            console.print(
                f'[yellow]Warning: using default fee ({100 / DEFAULT_FEE_DIVISOR:g}%)'
                f' — could not read from contract[/yellow]'
            )

    user_receives = apply_fee_deduction(dest_amount, fee_divisor)
    fee_percent = 100 / fee_divisor

    # Step 5: Enter receive address
    receive_address = receive_address_opt or click.prompt(f'Your {dest_chain.upper()} receive address')
    dest_provider = chain_providers.get(dest_chain)
    if (
        dest_provider
        and hasattr(dest_provider, 'is_valid_address')
        and not dest_provider.is_valid_address(receive_address)
    ):
        console.print(f'[yellow]Warning: address may not be valid for {dest_chain.upper()}[/yellow]')

    # Step 6: Source address (use public key — no password needed yet)
    if source_address_opt:
        user_source_address = source_address_opt
    elif source_chain == 'tao':
        user_source_address = wallet.coldkeypub.ss58_address
        console.print(f'  Source: [dim]{user_source_address}[/dim] (from wallet)')
    else:
        user_source_address = click.prompt(f'Your {source_chain.upper()} source address')

    # Step 6b: Verify sender has enough funds
    if source_chain == 'tao':
        tao_balance = subtensor.get_balance(wallet.coldkeypub.ss58_address)
        if tao_balance.rao < source_amount:
            console.print(
                f'[red]Insufficient balance: you have {tao_balance} but need {bt.Balance.from_rao(source_amount)}[/red]'
            )
            return

    # Step 7: Confirm summary
    fee_in_dest = dest_amount - user_receives

    summary = (
        f'  Send:    [red]{amount} {source_chain.upper()}[/red]\n'
        f'  Receive: [green]{_from_smallest_unit(user_receives, dest_chain):.8f} {dest_chain.upper()}[/green]\n'
        f'  Fee:     {fee_percent:g}% ({_from_smallest_unit(fee_in_dest, dest_chain):.8f} {dest_chain.upper()})\n'
        f'  Rate:    send 1 {source_chain.upper()}, get {selected_pair.rate:g} {dest_chain.upper()}\n'
        f'  Miner:   UID {selected_pair.uid}\n'
        f'  To:      {receive_address}'
    )
    console.print()
    console.print(Panel(summary, title='[bold]Swap Summary[/bold]', expand=False))
    console.print()

    if not skip_confirm and not click.confirm('Proceed?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    # Unlock coldkey once (password prompt) — all subsequent signing uses the cached key
    if source_chain == 'tao':
        source_key = wallet.coldkey
    else:
        source_key = None

    # Step 8: Reserve miner
    console.print('\n[dim]Step 1/3: Reserving miner...[/dim]')

    provider = chain_providers.get(source_chain)
    if not provider:
        console.print(f'[red]No chain provider for {source_chain}[/red]')
        return

    result = broadcast_reserve_with_retry(
        subtensor,
        client,
        provider,
        selected_pair,
        source_chain,
        dest_chain,
        source_amount,
        dest_amount,
        tao_amount,
        user_source_address,
        source_key,
        netuid,
        skip_confirm=skip_confirm,
    )
    if result is None:
        return

    reserved_until, validator_axons, ephemeral_wallet = result

    # Save pending swap state as backup
    state = PendingSwapState(
        miner_hotkey=selected_pair.hotkey,
        miner_uid=selected_pair.uid,
        source_chain=source_chain,
        dest_chain=dest_chain,
        source_amount=source_amount,
        dest_amount=dest_amount,
        tao_amount=tao_amount,
        user_receives=user_receives,
        rate_str=selected_pair.rate_str,
        miner_source_address=selected_pair.source_address,
        user_source_address=user_source_address,
        receive_address=receive_address,
        reserved_until_block=reserved_until,
        netuid=netuid,
        wallet_name=wallet.name,
        hotkey_name=wallet.hotkey_str,
        created_at=time.time(),
    )
    save_pending_swap(state)

    # Step 9: Send funds (or use pre-provided tx hash)
    if source_tx_hash_opt:
        # Funds already sent externally — use provided tx hash
        source_tx_hash = source_tx_hash_opt
        console.print(f'[dim]Using provided source tx: {source_tx_hash[:16]}...[/dim]')
    else:
        human_send = _from_smallest_unit(source_amount, source_chain)
        console.print(
            f'\n  Ready to send [bold]{human_send} {source_chain.upper()}[/bold] to miner at [cyan]{selected_pair.source_address}[/cyan]'
        )
        if not skip_confirm and not click.confirm('  Send now?', default=True):
            console.print('[yellow]Swap paused. Resume with: alw swap post-tx <tx_hash>[/yellow]')
            return

        console.print(f'\n[dim]Step 2/3: Sending {source_chain.upper()}...[/dim]')

        source_tx_hash = None
        if source_chain == 'tao':
            try:
                response = subtensor.transfer(
                    wallet=wallet,
                    destination_ss58=selected_pair.source_address,
                    amount=bt.Balance.from_rao(source_amount),
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                )
                if not response.success:
                    console.print(f'[red]TAO transfer failed: {response.message}[/red]')
                    console.print('[yellow]Resume with: alw swap post-tx <tx_hash>[/yellow]')
                    return
                try:
                    receipt = response.extrinsic_receipt
                    source_tx_hash = receipt.extrinsic_hash
                except Exception:
                    source_tx_hash = (
                        getattr(getattr(response, 'extrinsic_receipt', None), 'extrinsic_hash', '') or 'tao_transfer'
                    )
                console.print(f'[green]TAO sent (tx: {source_tx_hash[:16]}...)[/green]')
            except Exception as e:
                console.print(f'[red]Transfer error: {e}[/red]')
                console.print('[yellow]Resume with: alw swap post-tx <tx_hash>[/yellow]')
                return
        else:
            send_result = _send_btc(
                chain_providers,
                config,
                selected_pair.source_address,
                source_amount,
                from_address=user_source_address,
            )
            if send_result is None:
                return
            source_tx_hash = send_result[0]

    # Step 10: Post tx hash to validators
    console.print('\n[dim]Step 3/3: Confirming with validators...[/dim]')

    accepted, queued = sign_and_broadcast_confirm(
        provider,
        user_source_address,
        source_key,
        source_tx_hash,
        selected_pair.hotkey,
        receive_address,
        validator_axons,
        ephemeral_wallet,
        source_chain=source_chain,
        dest_chain=dest_chain,
    )

    if accepted == 0:
        console.print('[yellow]No validators accepted. Resume with: alw swap post-tx <tx_hash>[/yellow]')
        return

    all_queued = queued > 0 and queued == accepted

    if all_queued:
        # Validators queued — wait for confirmations with live progress
        chain_def = get_chain(source_chain)
        est_secs = chain_def.min_confirmations * chain_def.seconds_per_block
        est_min = est_secs / 60

        console.print(
            f'\n  Waiting for [bold]{chain_def.min_confirmations} {source_chain.upper()}[/bold]'
            f' confirmation(s) (~{est_min:.0f} min)...'
        )

        console.print(
            '\n  [dim]You can safely exit (Ctrl+C) — validators will continue processing.[/dim]'
            '\n  [dim]Resume watching later with: alw view swap <id> --watch[/dim]'
        )

    # Poll for swap creation (longer timeout when queued)
    max_polls = 600 if all_queued else 60
    try:
        swap_id = _poll_for_swap_with_progress(client, selected_pair.hotkey, source_chain, max_polls)
    except KeyboardInterrupt:
        clear_pending_swap()
        console.print('\n\n[green]Your swap is still being processed by validators.[/green]')
        console.print('[dim]Once initiated, watch with: alw view swap <id> --watch[/dim]\n')
        return

    if swap_id is None:
        clear_pending_swap()
        console.print('\n[yellow]Swap not yet initiated. Validators may still be waiting for confirmations.[/yellow]')
        console.print('[dim]Check back with: alw view swaps[/dim]\n')
        return

    clear_pending_swap()
    console.print(f'\n[green bold]Swap initiated! ID: {swap_id}[/green bold]')

    # In non-interactive mode, just print the ID and exit (let caller handle watching)
    if skip_confirm:
        return

    # Watch swap through lifecycle
    from allways.cli.swap_commands.view import watch_swap

    final_swap = watch_swap(client, swap_id)

    # Show completion receipt
    if final_swap and final_swap.status == SwapStatus.COMPLETED:
        _display_receipt(final_swap)
