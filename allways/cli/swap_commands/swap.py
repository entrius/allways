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
from allways.classes import SwapStatus
from allways.cli.dendrite_lite import broadcast_synapse, discover_validators, get_ephemeral_wallet
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    PendingSwapState,
    clear_pending_swap,
    console,
    find_matching_miners,
    from_rao,
    from_smallest_unit,
    get_cli_context,
    is_local_network,
    load_pending_swap,
    resolve_source_tx_block,
    save_pending_swap,
    to_smallest_unit,
    sign_or_prompt_external,
)
from allways.commitments import read_miner_commitments
from allways.constants import FEE_DIVISOR, NETUID_FINNEY
from allways.contract_client import ContractError
from allways.synapses import SwapConfirmSynapse, SwapReserveSynapse
from allways.utils.rate import apply_fee_deduction, calculate_to_amount, check_swap_viability, derive_tao_leg

# =========================================================================
# Shared functions (used by swap command, post_tx command)
# =========================================================================


def sign_and_broadcast_confirm(
    provider,
    user_from_address: str,
    from_key,
    from_tx_hash: str,
    miner_hotkey: str,
    receive_address: str,
    validator_axons: list,
    ephemeral_wallet,
    from_chain: str = '',
    to_chain: str = '',
    skip_confirm: bool = False,
    from_tx_block: int = 0,
) -> tuple:
    """Sign source tx proof and broadcast SwapConfirmSynapse to validators.

    Returns (accepted_count, queued_count). Queued means the validator accepted
    but is waiting for source tx confirmations before voting to initiate.
    """
    console.print('[dim]Submitting swap to validators...[/dim]')
    proof_message = f'allways-swap:{from_tx_hash}'
    from_proof = sign_or_prompt_external(
        provider,
        user_from_address,
        proof_message,
        key=from_key,
        chain=from_chain,
        skip_confirm=skip_confirm,
    )
    if not from_proof:
        console.print('[red]Could not obtain source tx proof signature — cannot confirm swap.[/red]')
        return 0, 0

    confirm_synapse = SwapConfirmSynapse(
        reservation_id=miner_hotkey,
        from_tx_hash=from_tx_hash,
        from_tx_proof=from_proof,
        from_address=user_from_address,
        from_tx_block=from_tx_block,
        to_address=receive_address,
        from_chain=from_chain,
        to_chain=to_chain,
    )

    console.print(f'  Broadcasting to {len(validator_axons)} validators...')
    confirm_responses = broadcast_synapse(ephemeral_wallet, validator_axons, confirm_synapse, timeout=60.0)

    accepted = 0
    queued = 0
    for i, resp in enumerate(confirm_responses):
        raw_reason = (getattr(resp, 'rejection_reason', '') or '').strip()
        if getattr(resp, 'accepted', None):
            accepted += 1
            if 'Queued' in raw_reason:
                queued += 1
                console.print(f'    V{i + 1}: [yellow]queued[/yellow] {raw_reason}')
            else:
                console.print(f'    V{i + 1}: [green]ok[/green]')
        else:
            # Blank = validator didn't respond; be explicit so the user can
            # distinguish "rejected with reason" from "silently unreachable".
            reason = raw_reason or '(no response — timeout or validator down)'
            console.print(f'    V{i + 1}: [red]no[/red] {reason}')

    return accepted, queued


def resolve_recent_swap_id(client, miner_hotkey: str) -> Optional[int]:
    """Return the miner's active swap id, or None if they have none.

    Delegates to ``get_miner_active_swaps``, which scans backward from
    ``next_id - 1`` stopping on consecutive pruned/resolved gaps — so
    correctness does not depend on a guessed window size. The contract
    guarantees at most one active swap per miner. Raises ``ContractError``
    on RPC failure so callers can distinguish "RPC broken" from "no swap
    yet" — the Ctrl+C-exit path suppresses; ``poll_for_swap_creation``
    counts for its retry warning.
    """
    if not client.get_miner_has_active_swap(miner_hotkey):
        return None
    active = client.get_miner_active_swaps(miner_hotkey)
    # One active swap per miner, so [0] is guaranteed to be it.
    return active[0].id if active else None


def poll_for_swap_creation(client, miner_hotkey: str) -> Optional[int]:
    """Poll contract until miner has an active swap. Returns swap_id or None."""
    with console.status('[dim]Waiting for swap to appear on-chain...[/dim]'):
        errors = 0
        for _ in range(60):
            time.sleep(3)
            try:
                swap_id = resolve_recent_swap_id(client, miner_hotkey)
                if swap_id is not None:
                    return swap_id
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
    from_chain: str,
    to_chain: str,
    from_amount: int,
    to_amount: int,
    tao_amount: int,
    user_from_address: str,
    from_key,
    netuid: int,
    skip_confirm: bool = False,
    max_retries: int = 2,
):
    """Reserve miner via multi-validator consensus with retry.

    Returns (reserved_until, validator_axons, ephemeral_wallet) on success, None on failure.
    """
    current_block = subtensor.get_current_block()
    reserve_proof_message = f'allways-reserve:{user_from_address}:{current_block}'
    from_address_proof = sign_or_prompt_external(
        provider,
        user_from_address,
        reserve_proof_message,
        key=from_key,
        chain=from_chain,
        skip_confirm=skip_confirm,
    )
    if not from_address_proof:
        console.print('[red]Could not obtain reserve signature — cannot reserve miner.[/red]')
        return None

    synapse = SwapReserveSynapse(
        miner_hotkey=selected_pair.hotkey,
        tao_amount=tao_amount,
        from_amount=from_amount,
        to_amount=to_amount,
        from_address=user_from_address,
        from_address_proof=from_address_proof,
        block_anchor=current_block,
        from_chain=from_chain,
        to_chain=to_chain,
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
            reserve_proof_message = f'allways-reserve:{user_from_address}:{current_block}'
            from_address_proof = sign_or_prompt_external(
                provider,
                user_from_address,
                reserve_proof_message,
                key=from_key,
                chain=from_chain,
                skip_confirm=skip_confirm,
            )
            if not from_address_proof:
                console.print('[red]Could not obtain reserve signature on retry.[/red]')
                return None
            synapse = SwapReserveSynapse(
                miner_hotkey=selected_pair.hotkey,
                tao_amount=tao_amount,
                from_amount=from_amount,
                to_amount=to_amount,
                from_address=user_from_address,
                from_address_proof=from_address_proof,
                block_anchor=current_block,
                from_chain=from_chain,
                to_chain=to_chain,
            )

        console.print(f'  Broadcasting to {len(validator_axons)} validators...')
        responses = broadcast_synapse(ephemeral_wallet, validator_axons, synapse, timeout=30.0)

        accepted = sum(1 for r in responses if getattr(r, 'accepted', None))
        for i, resp in enumerate(responses):
            was_accepted = bool(getattr(resp, 'accepted', None))
            raw_reason = (getattr(resp, 'rejection_reason', '') or '').strip()
            if was_accepted:
                # Accepted means the validator responded. Blank reason is
                # normal here — don't render the 'no response' fallback.
                suffix = f' {raw_reason}' if raw_reason else ''
                console.print(f'    V{i + 1}: [green]ok[/green]{suffix}')
            else:
                reason = raw_reason or '(no response — timeout or validator down)'
                console.print(f'    V{i + 1}: [red]no[/red] {reason}')

        if accepted == 0:
            console.print('[red]No validators accepted the reservation.[/red]')
            if attempt < max_retries and not skip_confirm and click.confirm('Retry?', default=True):
                console.print('[dim]Retrying reservation...[/dim]')
                continue
            return None

        # Countdown the quorum wait so users see progress, not just a spinner.
        quorum_total_s = 60
        quorum_started = time.time()
        try:
            with console.status(
                f'[dim]Waiting for on-chain quorum — 0/{accepted} broadcast votes confirmed '
                f'(~{quorum_total_s}s)...[/dim]'
            ) as status:
                quorum_errors = 0
                for _ in range(30):
                    time.sleep(2)
                    try:
                        reserved_until = client.get_miner_reserved_until(selected_pair.hotkey)
                        if reserved_until > current_block:
                            reserved = True
                            break
                        vote_count = client.get_pending_reserve_vote_count(selected_pair.hotkey)
                        elapsed = int(time.time() - quorum_started)
                        remaining = max(0, quorum_total_s - elapsed)
                        status.update(
                            f'[dim]Waiting for on-chain quorum — {vote_count}/{accepted} votes confirmed, '
                            f'{elapsed}s elapsed (~{remaining}s remaining)...[/dim]'
                        )
                        quorum_errors = 0
                    except ContractError:
                        quorum_errors += 1
                        if quorum_errors >= 5:
                            console.print('[yellow]Warning: contract unreachable, still waiting...[/yellow]')
                            quorum_errors = 0
        except KeyboardInterrupt:
            # Mirror the confirm-path behaviour: give a clean exit banner
            # instead of a raw traceback when the user bails during the wait.
            console.print('\n[yellow]Aborted — reservation may still confirm on-chain.[/yellow]')
            console.print(
                f'[dim]Check with: alw view miners (miner UID {selected_pair.uid}) or alw view reservation[/dim]'
            )
            return None
        if reserved:
            ttl_remaining = reserved_until - subtensor.get_current_block()
            console.print(f'[green]Miner reserved! ~{ttl_remaining * 12 // 60} min to send funds.[/green]')

        if reserved:
            break

        if attempt < max_retries:
            console.print(
                f"[yellow]Reservation quorum not reached ({accepted} broadcast votes didn't make it on-chain within "
                f'{quorum_total_s}s).[/yellow]'
            )
            # Default yes since we had >=1 accept — usually a transient chain
            # delay and a second attempt works without changing anything.
            if not skip_confirm and click.confirm('Retry?', default=True):
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


def display_timeout_notice(swap, dashboard_url: str):
    """Explain a TIMED_OUT result to the user after `alw swap now` watches through termination.

    The timeout path looks silent from the CLI's perspective — the contract
    refunds directly and removes the swap from storage — so surface what
    actually happened: the miner failed to fulfill, they're slashed, and the
    user is made whole in TAO from the slashed collateral.
    """
    tao_human = swap.tao_amount / (10**9) if swap.tao_amount else 0.0
    amount_line = f'[bold]{tao_human:g} TAO[/bold]' if tao_human else 'TAO equivalent to your swap'

    notice = (
        '[bold]Miner failed to fulfill within the timeout window.[/bold]\n'
        '\n'
        f"  You're made whole: {amount_line} is slashed from the miner's\n"
        '  collateral and sent directly to your TAO wallet.\n'
        '\n'
        '  If the on-chain transfer could not settle, the slash is held\n'
        f'  pending — claim it with: [cyan]alw claim {swap.id}[/cyan]\n'
        '\n'
        f'  Details: [cyan]{dashboard_url}/swap/{swap.id}[/cyan]'
    )
    console.print()
    console.print(Panel(notice, title='[bold red]Swap Timed Out[/bold red]', expand=False))
    console.print()


def display_receipt(swap):
    """Show a rich completion receipt after a successful swap."""
    src_chain_def = get_chain(swap.from_chain)
    dst_chain_def = get_chain(swap.to_chain)
    src_human = swap.from_amount / (10**src_chain_def.decimals)
    dst_human = swap.to_amount / (10**dst_chain_def.decimals)
    tao_human = swap.tao_amount / (10**9)

    # swap.to_amount is the post-fee amount the miner sent (see
    # fulfillment.py::send_dest_funds). The raw rate-quoted amount was
    # to_amount / (1 - 1/FEE_DIVISOR), and the protocol fee is 1/FEE_DIVISOR
    # of that raw amount. Shown in TAO since that's the side the fee is
    # recorded against (mirrors accumulated_fees on-chain).
    fee_pct = 100 / FEE_DIVISOR
    protocol_fee_tao = tao_human / FEE_DIVISOR

    src_tx = swap.from_tx_hash[:20] + '...' if len(swap.from_tx_hash) > 20 else swap.from_tx_hash
    dst_tx = swap.to_tx_hash[:20] + '...' if len(swap.to_tx_hash) > 20 else swap.to_tx_hash

    receipt = (
        f'  [green]Sent:      {src_human:g} {swap.from_chain.upper()}[/green]\n'
        f'  [green]Received:  {dst_human:.8f} {swap.to_chain.upper()}[/green]\n'
        f'  [dim]Protocol fee: {protocol_fee_tao:.6f} TAO ({fee_pct:g}% of swap)[/dim]\n'
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


def poll_for_swap_with_progress(client, miner_hotkey: str, from_chain: str, max_polls: int = 60):
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
                swap_id = resolve_recent_swap_id(client, miner_hotkey)
                if swap_id is not None:
                    return swap_id
                errors = 0
            except ContractError:
                errors += 1
                if errors >= 5:
                    console.print('[yellow]Warning: contract unreachable, still waiting...[/yellow]')
                    errors = 0
    return None


def send_btc(chain_providers, config, to_address: str, amount_sat: int, from_address: str = None):
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
@click.option('--from', 'from_chain_opt', default=None, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain_opt', default=None, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', 'amount_opt', default=None, type=float, help='Amount to send in source chain units')
@click.option('--receive-address', 'receive_address_opt', default=None, help='Receive address on destination chain')
@click.option('--from-address', 'from_address_opt', default=None, help='Source address on source chain')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option('--auto', 'auto_select', is_flag=True, help='Auto-select best rate miner')
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
def swap_now_command(
    from_chain_opt: Optional[str],
    to_chain_opt: Optional[str],
    amount_opt: Optional[float],
    receive_address_opt: Optional[str],
    from_address_opt: Optional[str],
    from_tx_hash_opt: Optional[str],
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
        $ alw swap now --from btc --to tao --amount 0.001 \\
            --receive-address 5C... --from-address bc1q... \\
            --from-tx-hash abc123... --auto --yes[/dim]

    [dim]Interactive mode:
        $ alw swap now[/dim]
    """
    config, wallet, subtensor, client = get_cli_context()
    # --netuid handled globally in main.py; config['netuid'] already resolved.
    netuid = int(config.get('netuid', NETUID_FINNEY))

    try:
        if client.get_halted():
            console.print('[red]System is halted — no new swaps can be initiated. Please try again later.[/red]')
            return
    except ContractError:
        pass

    # Validate provided chain options early
    if from_chain_opt and from_chain_opt not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown source chain: {from_chain_opt}[/red]')
        return
    if to_chain_opt and to_chain_opt not in SUPPORTED_CHAINS:
        console.print(f'[red]Unknown destination chain: {to_chain_opt}[/red]')
        return
    if from_chain_opt and to_chain_opt and from_chain_opt == to_chain_opt:
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
    if from_chain_opt and to_chain_opt:
        from_chain = from_chain_opt
        to_chain = to_chain_opt
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
        from_chain, to_chain = directions[choice - 1]

    # Step 2: Find available miners (bilateral matching).
    # Done BEFORE any confirm prompts — if no miner can fill this swap, bail
    # out before the user wastes time answering BTC-signing questions.
    console.print('\n[dim]Reading miner commitments...[/dim]')
    all_pairs = read_miner_commitments(subtensor, netuid)

    matching_pairs = find_matching_miners(all_pairs, from_chain, to_chain)

    if not matching_pairs:
        console.print(f'[yellow]No miners currently post rates for {from_chain.upper()}/{to_chain.upper()}.[/yellow]')
        console.print('[dim]Run [cyan]alw view rates[/cyan] to see active pairs, or try again later.[/dim]\n')
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
        console.print(
            f'[yellow]Miners post rates for {from_chain.upper()}/{to_chain.upper()}, '
            f'but none are currently eligible.[/yellow]'
        )
        console.print(
            '[dim]They may be inactive, already fulfilling another swap, or without collateral.\n'
            'Run [cyan]alw view miners[/cyan] to inspect status and try again in a few blocks.[/dim]\n'
        )
        return

    # Show send capability for the source chain (skip in non-interactive mode).
    # Only asked once we know a miner can actually fill this swap.
    if not skip_confirm:
        if from_chain == 'tao':
            console.print('\n  [green]TAO will be sent automatically from your wallet.[/green]')
        else:
            has_private_key = bool(os.environ.get('BTC_PRIVATE_KEY'))
            btc_mode = os.environ.get('BTC_MODE', 'lightweight')
            is_local = is_local_network(config.get('network', 'finney'))

            if has_private_key and not is_local:
                console.print('\n  [green]BTC_PRIVATE_KEY set — will sign and attempt BTC sends locally.[/green]')
            else:
                # External signing path — covers both the lightweight/no-key
                # case and any other environment without automatic BTC sending.
                # Taproot caveat only applies to the BYO-sig flow.
                taproot_note = (
                    ' Taproot (bc1p…) unsupported.' if btc_mode == 'lightweight' and not has_private_key else ''
                )
                console.print(
                    f'\n  [yellow]BTC signing & sending are external — you will sign at reserve/confirm'
                    f' and run [cyan]alw swap post-tx <tx_hash>[/cyan] after broadcasting.{taproot_note}[/yellow]'
                )
                if not click.confirm('  Continue?', default=True):
                    console.print('[yellow]Cancelled[/yellow]')
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
    canon_from, canon_to = canonical_pair(from_chain, to_chain)
    best_pair = available_miners[0][0]
    canon_is_reverse = from_chain != canon_from
    if canon_is_reverse:
        best_rate_line = (
            f'send {best_pair.rate:g} {from_chain.upper()} to get 1 {to_chain.upper()} (Miner UID {best_pair.uid})'
        )
    else:
        best_rate_line = (
            f'send 1 {from_chain.upper()} to get {best_pair.rate:g} {to_chain.upper()} (Miner UID {best_pair.uid})'
        )
    # Skip the "Best rate:" hint when there is only one miner — the table and
    # the upcoming summary already show the rate, so it'd be a third echo.
    if len(available_miners) > 1:
        console.print(f'\n  Best rate: {best_rate_line}')

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
        amount_opt if amount_opt is not None else click.prompt(f'\nAmount to send ({from_chain.upper()})', type=float)
    )
    if amount <= 0:
        console.print('[red]Amount must be positive[/red]')
        return

    from_amount = to_smallest_unit(amount, from_chain)
    is_reverse = from_chain != canon_from
    to_amount = calculate_to_amount(
        from_amount,
        selected_pair.rate_str,
        is_reverse,
        get_chain(canon_to).decimals,
        get_chain(canon_from).decimals,
    )

    # Show estimated receive inline — fee is a hardcoded protocol constant.
    preview_receives = apply_fee_deduction(to_amount, FEE_DIVISOR)
    preview_fee_pct = 100 / FEE_DIVISOR
    console.print(
        f'  You will receive: ~[green]{from_smallest_unit(preview_receives, to_chain):.8f} {to_chain.upper()}[/green]'
        f' (after {preview_fee_pct:g}% fee)'
    )

    tao_amount = derive_tao_leg(from_chain, from_amount, to_chain, to_amount)

    # Validate against contract min/max swap bounds + selected miner's
    # collateral. Mirrors vote_reserve (bounds) and vote_initiate
    # (collateral) so we fail loudly here instead of after the user has
    # reserved and sent funds.
    try:
        min_swap = client.get_min_swap_amount()
        max_swap = client.get_max_swap_amount()
    except ContractError:
        console.print('[yellow]Warning: could not verify swap bounds (contract unreachable)[/yellow]')
        min_swap, max_swap = 0, 0

    viable, reason = check_swap_viability(tao_amount, selected_collateral, min_swap, max_swap)
    if not viable:
        console.print(
            f'[red]Swap cannot be initiated at this amount: {reason} '
            f'(you entered {from_rao(tao_amount):.4f} TAO equivalent).[/red]'
        )
        console.print(
            '[dim]Try a different amount, pick another miner, or run `alw swap quote` to see viable rows.[/dim]'
        )
        return

    fee_divisor = FEE_DIVISOR
    user_receives = apply_fee_deduction(to_amount, fee_divisor)
    fee_percent = 100 / fee_divisor

    # Step 5: Enter receive address
    if not receive_address_opt:
        console.print(
            f'  [dim]Enter only your PUBLIC {to_chain.upper()} address. '
            f'Never paste a private key, seed phrase, or WIF here.[/dim]'
        )
    receive_address = receive_address_opt or click.prompt(f'Your {to_chain.upper()} receive address')
    to_provider = chain_providers.get(to_chain)
    if not to_provider:
        console.print(f'[red]No chain provider for {to_chain.upper()}[/red]')
        return
    if not to_provider.is_valid_address(receive_address):
        console.print(f'[red]Invalid {to_chain.upper()} address: {receive_address}[/red]')
        return

    # Step 6: Source address (use public key — no password needed yet)
    if from_address_opt:
        user_from_address = from_address_opt
    elif from_chain == 'tao':
        # Sourced from the wallet coldkey; the Summary panel renders it as
        # the "From:" row right below, so no need for a standalone line here.
        user_from_address = wallet.coldkeypub.ss58_address
    else:
        console.print(
            f'  [dim]Enter only your PUBLIC {from_chain.upper()} address. Never paste a private key or WIF here.[/dim]'
        )
        user_from_address = click.prompt(f'Your {from_chain.upper()} source address')

    # Step 6b: Verify sender has enough funds
    if from_chain == 'tao':
        tao_balance = subtensor.get_balance(wallet.coldkeypub.ss58_address)
        if tao_balance.rao < from_amount:
            console.print(
                f'[red]Insufficient balance: you have {tao_balance} but need {bt.Balance.from_rao(from_amount)}[/red]'
            )
            return

    # Step 7: Confirm summary
    fee_in_dest = to_amount - user_receives
    src_up = from_chain.upper()
    dst_up = to_chain.upper()

    # Use the forward rate for display — matches the miner's posted forward
    # quote regardless of swap direction (calculate_to_amount just toggles
    # is_reverse). For reverse swaps this reads as "send N dst to get 1 src".
    if is_reverse:
        rate_line = f'send {selected_pair.rate:g} {src_up} to get 1 {dst_up}'
    else:
        rate_line = f'send 1 {src_up} to get {selected_pair.rate:g} {dst_up}'

    # Split send/receive into two rows each — amount on one line, address
    # on the next — so there is no "amount → address" arrow that can be
    # misread as "send the funds TO that address." The From address is
    # where the user's source tx must originate; the To address is where
    # the miner will deliver the destination funds.
    receive_human = from_smallest_unit(user_receives, to_chain)
    fee_human = from_smallest_unit(fee_in_dest, to_chain)
    summary = (
        f'  You Send:     [red]{amount} {src_up}[/red]\n'
        f'    From:       [yellow]{user_from_address}[/yellow]  [dim](your {src_up} address)[/dim]\n'
        f'\n'
        f'  You Receive:  [green]{receive_human:.8f} {dst_up}[/green]\n'
        f'    To:         [yellow]{receive_address}[/yellow]  [dim](your {dst_up} address)[/dim]\n'
        f'\n'
        f'  Protocol Fee: {fee_percent:g}% ({fee_human:.8f} {dst_up})\n'
        f'  Rate:         {rate_line}\n'
        f'  Miner:        UID {selected_pair.uid}'
    )
    console.print()
    console.print(Panel(summary, title='[bold]Swap Summary[/bold]', expand=False))
    console.print(
        f'  [yellow]⚠  You must send the source funds from the "From" {src_up} address above.[/yellow]\n'
        '  [dim]Validators reject swaps where the source tx sender does not match the reserved address.[/dim]'
    )
    console.print()

    if not skip_confirm and not click.confirm('Proceed?'):
        console.print('[yellow]Cancelled[/yellow]')
        return

    # Unlock coldkey once (password prompt) — all subsequent signing uses the cached key
    if from_chain == 'tao':
        from_key = wallet.coldkey
    else:
        from_key = None

    # Step 8: Reserve miner
    console.print('\n[dim]Step 1/3: Reserving miner...[/dim]')

    provider = chain_providers.get(from_chain)
    if not provider:
        console.print(f'[red]No chain provider for {from_chain}[/red]')
        return

    result = broadcast_reserve_with_retry(
        subtensor,
        client,
        provider,
        selected_pair,
        from_chain,
        to_chain,
        from_amount,
        to_amount,
        tao_amount,
        user_from_address,
        from_key,
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
        from_chain=from_chain,
        to_chain=to_chain,
        from_amount=from_amount,
        to_amount=to_amount,
        tao_amount=tao_amount,
        user_receives=user_receives,
        rate_str=selected_pair.rate_str,
        miner_from_address=selected_pair.from_address,
        user_from_address=user_from_address,
        receive_address=receive_address,
        reserved_until_block=reserved_until,
        netuid=netuid,
        wallet_name=wallet.name,
        hotkey_name=wallet.hotkey_str,
        created_at=time.time(),
    )
    save_pending_swap(state)

    # Step 9: Send funds (or use pre-provided tx hash)
    from_tx_block = 0  # Set below so confirm synapse can give validators a ±3 hint.
    if from_tx_hash_opt:
        # Funds already sent externally — use provided tx hash
        from_tx_hash = from_tx_hash_opt
        console.print(f'[dim]Using provided source tx: {from_tx_hash[:16]}...[/dim]')
        from_tx_block = resolve_source_tx_block(
            provider=provider,
            tx_hash=from_tx_hash,
            expected_recipient=selected_pair.from_address,
            expected_amount=from_amount,
            subtensor=subtensor,
            client=client,
            reserved_until_block=reserved_until,
        )
    else:
        human_send = from_smallest_unit(from_amount, from_chain)
        console.print(
            f'\n  Ready to send [bold]{human_send} {from_chain.upper()}[/bold] to miner at [cyan]{selected_pair.from_address}[/cyan]'
        )
        if not skip_confirm and not click.confirm('  Send now?', default=True):
            console.print('[yellow]Swap paused. Resume with: alw swap post-tx <tx_hash>[/yellow]')
            return

        console.print(f'\n[dim]Step 2/3: Sending {from_chain.upper()}...[/dim]')

        from_tx_hash = None
        if from_chain == 'tao':
            try:
                response = subtensor.transfer(
                    wallet=wallet,
                    destination_ss58=selected_pair.from_address,
                    amount=bt.Balance.from_rao(from_amount),
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                )
                if not response.success:
                    console.print(f'[red]TAO transfer failed: {response.message}[/red]')
                    console.print('[yellow]Resume with: alw swap post-tx <tx_hash>[/yellow]')
                    return
                try:
                    receipt = response.extrinsic_receipt
                    from_tx_hash = receipt.extrinsic_hash
                    if getattr(receipt, 'block_hash', None):
                        from_tx_block = subtensor.substrate.get_block_number(receipt.block_hash) or 0
                except Exception:
                    from_tx_hash = (
                        getattr(getattr(response, 'extrinsic_receipt', None), 'extrinsic_hash', '') or 'tao_transfer'
                    )
                console.print(f'[green]TAO sent (tx: {from_tx_hash[:16]}...)[/green]')
            except Exception as e:
                console.print(f'[red]Transfer error ({type(e).__name__}): {e}[/red]')
                console.print('[yellow]Resume with: alw swap post-tx <tx_hash>[/yellow]')
                return
        else:
            send_result = send_btc(
                chain_providers,
                config,
                selected_pair.from_address,
                from_amount,
                from_address=user_from_address,
            )
            if send_result is None:
                return
            from_tx_hash = send_result[0]
            # send_btc returns (tx_hash, block_number). 0 means unknown
            # (e.g. lightweight broadcaster); that's fine — validator falls
            # back to the scan path.
            if len(send_result) > 1 and send_result[1]:
                from_tx_block = int(send_result[1])

    # Step 10: Post tx hash to validators
    console.print('\n[dim]Step 3/3: Confirming with validators...[/dim]')

    accepted, queued = sign_and_broadcast_confirm(
        provider,
        user_from_address,
        from_key,
        from_tx_hash,
        selected_pair.hotkey,
        receive_address,
        validator_axons,
        ephemeral_wallet,
        from_chain=from_chain,
        to_chain=to_chain,
        from_tx_block=from_tx_block,
    )

    if accepted == 0:
        console.print('[yellow]No validators accepted. Resume with: alw swap post-tx <tx_hash>[/yellow]')
        return

    all_queued = queued > 0 and queued == accepted

    if all_queued:
        # Validators queued — wait for confirmations with live progress
        chain_def = get_chain(from_chain)
        est_secs = chain_def.min_confirmations * chain_def.seconds_per_block
        est_min = est_secs / 60

        console.print(
            f'\n  Waiting for [bold]{chain_def.min_confirmations} {from_chain.upper()}[/bold]'
            f' confirmation(s) (~{est_min:.0f} min). '
            "We'll drop into live status the moment the swap is initiated on-chain."
        )
        console.print(
            '\n  [dim]If you need to step away: Ctrl+C detaches, resume anytime with `alw view reservation`.[/dim]'
        )

    # Poll for swap creation (longer timeout when queued)
    max_polls = 600 if all_queued else 60
    try:
        swap_id = poll_for_swap_with_progress(client, selected_pair.hotkey, from_chain, max_polls)
    except KeyboardInterrupt:
        # One last best-effort resolve before handing back — the swap may
        # have just been initiated while we were printing, and a concrete
        # ID in the exit banner beats telling the user to grep.
        try:
            swap_id = resolve_recent_swap_id(client, selected_pair.hotkey)
        except ContractError:
            swap_id = None
        console.print('\n\n[green]Your swap is still being processed by validators.[/green]')
        if swap_id is not None:
            clear_pending_swap()
            console.print(f'[green bold]Swap ID: {swap_id}[/green bold]')
            console.print(f'[dim]Watch with: alw view swap {swap_id} --watch[/dim]\n')
        else:
            console.print(f'[dim]Miner UID {selected_pair.uid} — check progress with: alw view reservation[/dim]\n')
        return

    if swap_id is None:
        console.print('\n[yellow]Swap not yet initiated. Validators may still be waiting for confirmations.[/yellow]')
        console.print(
            f'[dim]Miner UID {selected_pair.uid} — check: alw view reservation '
            '(pending_swap.json kept for retry with `alw swap resume-reservation`)[/dim]\n'
        )
        return

    clear_pending_swap()
    console.print(f'\n[green bold]Swap initiated! ID: {swap_id}[/green bold]')

    # In non-interactive mode, just print the ID and exit (let caller handle watching)
    if skip_confirm:
        return

    # Watch swap through lifecycle
    from allways.cli.swap_commands.view import DEFAULT_DASHBOARD_URL, watch_swap

    final_swap = watch_swap(client, swap_id)

    # Show completion receipt / timeout notice
    if final_swap and final_swap.status == SwapStatus.COMPLETED:
        display_receipt(final_swap)
    elif final_swap and final_swap.status == SwapStatus.TIMED_OUT:
        dashboard_url = os.environ.get('ALLWAYS_DASHBOARD_URL', DEFAULT_DASHBOARD_URL).rstrip('/')
        display_timeout_notice(final_swap, dashboard_url)
