"""alw swap resume - Recover an interrupted swap flow."""

import os
import time
from typing import Optional

import rich_click as click
from rich.panel import Panel

from allways.chain_providers import create_chain_providers
from allways.chains import get_chain
from allways.classes import SwapStatus
from allways.cli.dendrite_lite import discover_validators, get_ephemeral_wallet
from allways.cli.swap_commands.helpers import (
    SECONDS_PER_BLOCK,
    clear_pending_swap,
    console,
    get_cli_context,
    load_pending_swap,
    resolve_source_tx_block,
)
from allways.cli.swap_commands.swap import (
    from_smallest_unit,
    poll_for_swap_with_progress,
    resolve_recent_swap_id,
    sign_and_broadcast_confirm,
)
from allways.contract_client import ContractError


@click.command('resume')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
def resume_command(from_tx_hash_opt: Optional[str], skip_confirm: bool):
    """Resume an interrupted swap from where it left off.

    \b
    Picks up a pending swap that has an active reservation — submits the
    source transaction hash and confirms with validators. If the reservation
    has expired, guides the user to start fresh with `alw swap now`.

    \b
    Interactive mode:
        alw swap resume

    \b
    Non-interactive mode (for scripting/agents):
        alw swap resume --from-tx-hash abc123... --yes
    """
    state = load_pending_swap()
    if not state:
        console.print('[yellow]No pending swap found.[/yellow]')
        console.print('[dim]Start a new swap with: alw swap now[/dim]')
        return

    config, wallet, subtensor, client = get_cli_context()
    # --netuid handled globally in main.py. Fall back to the saved
    # reservation's netuid when neither CLI flag nor config override it,
    # so a resume stays pinned to the subnet the original reservation
    # was opened on.
    netuid = int(config.get('netuid', state.netuid))

    # Check if system is halted
    try:
        if client.get_halted():
            console.print('[red]System is halted — no swaps can be processed. Please try again later.[/red]')
            return
    except ContractError:
        pass

    # Display pending swap summary
    elapsed_min = (time.time() - state.created_at) / 60
    human_send = from_smallest_unit(state.from_amount, state.from_chain)
    human_receive = from_smallest_unit(state.user_receives, state.to_chain)
    send_label = f'{human_send} {state.from_chain.upper()}'

    summary = (
        f'  Direction:  {state.from_chain.upper()} -> {state.to_chain.upper()}\n'
        f'  Miner:      UID {state.miner_uid}\n'
        f'  Send:       {send_label}\n'
        f'  Receive:    ~{human_receive:.8f} {state.to_chain.upper()}\n'
        f'  Send to:    {state.miner_from_address}\n'
        f'  Started:    {elapsed_min:.0f} min ago'
    )
    console.print()
    console.print(Panel(summary, title='[bold]Pending Swap[/bold]', expand=False))

    # Check if swap is already on-chain (cheap bool check before expensive scan)
    try:
        if client.get_miner_has_active_swap(state.miner_hotkey):
            for swap in client.get_miner_active_swaps(state.miner_hotkey):
                is_ours = (
                    swap.user_from_address == state.user_from_address or swap.user_to_address == state.receive_address
                )
                if is_ours:
                    clear_pending_swap()
                    console.print(f'\n[green]Swap already on-chain! ID: {swap.id}[/green]')
                    if not skip_confirm:
                        from allways.cli.swap_commands.view import watch_swap

                        final = watch_swap(client, swap.id)
                        if final and final.status == SwapStatus.COMPLETED:
                            from allways.cli.swap_commands.swap import display_receipt

                            display_receipt(final)
                    return
    except ContractError:
        pass

    # Check reservation status — if expired, there's nothing to resume
    try:
        reserved_until = client.get_miner_reserved_until(state.miner_hotkey)
        current_block = subtensor.get_current_block()
        reservation_active = reserved_until > current_block
    except ContractError as e:
        console.print(f'[red]Failed to read reservation status: {e}[/red]')
        return

    if not reservation_active:
        # Reservation is cleared either on expiry or when vote_initiate succeeds.
        # A silent initiate means the swap is already in flight or has completed —
        # surface both possibilities rather than assuming expiry.
        clear_pending_swap()
        console.print('\n[yellow]Reservation is no longer active.[/yellow]')
        console.print(
            '[dim]Either the reservation expired, or your swap already initiated and may be in progress '
            'or completed. Check with: alw view active-swaps[/dim]\n'
        )
        console.print('[dim]Start a new swap with: alw swap now[/dim]')
        return

    remaining = reserved_until - current_block
    console.print(f'\n[green]Reservation still active (~{remaining * SECONDS_PER_BLOCK // 60} min left)[/green]')

    # Set up chain provider
    if 'BTC_MODE' not in os.environ:
        os.environ['BTC_MODE'] = 'lightweight'
    chain_providers = create_chain_providers(subtensor=subtensor)
    provider = chain_providers.get(state.from_chain)
    if not provider:
        console.print(f'[red]No chain provider for {state.from_chain}[/red]')
        return

    from_key = wallet.coldkey if state.from_chain == 'tao' else None

    # Discover validators before prompting for tx hash (fail early)
    validator_axons = discover_validators(subtensor, netuid, contract_client=client)
    if not validator_axons:
        console.print('[red]No validators found on metagraph[/red]')
        return

    ephemeral_wallet = get_ephemeral_wallet()

    # Prompt for source tx hash if not provided
    if not from_tx_hash_opt:
        console.print(f'\n  Send [green]{send_label}[/green] to: [cyan]{state.miner_from_address}[/cyan]\n')
        from_tx_hash_opt = click.prompt('Enter transaction hash after sending (or "skip" to exit)', default='')
        if not from_tx_hash_opt or from_tx_hash_opt.lower() == 'skip':
            console.print('[yellow]Swap paused. Resume later with: alw swap resume[/yellow]')
            return

    from_tx_hash = from_tx_hash_opt.strip()

    # Reservation-wide block lookup so a resumed tx still ±3-hints the
    # validator. 0 = miss (falls back to validator-side scan).
    from_tx_block = resolve_source_tx_block(
        provider=provider,
        tx_hash=from_tx_hash,
        expected_recipient=state.miner_from_address,
        expected_amount=state.from_amount,
        subtensor=subtensor,
        client=client,
        reserved_until_block=reserved_until,
    )

    console.print('\n[dim]Confirming with validators...[/dim]')
    accepted, queued = sign_and_broadcast_confirm(
        provider,
        state.user_from_address,
        from_key,
        from_tx_hash,
        state.miner_hotkey,
        state.receive_address,
        validator_axons,
        ephemeral_wallet,
        from_chain=state.from_chain,
        to_chain=state.to_chain,
        from_tx_block=from_tx_block,
    )

    if accepted == 0:
        console.print('[yellow]No validators accepted. You can retry: alw swap resume[/yellow]')
        return

    all_queued = queued > 0 and queued == accepted
    if all_queued:
        chain_def = get_chain(state.from_chain)
        est_min = chain_def.min_confirmations * chain_def.seconds_per_block / 60
        console.print(
            f'\n  Waiting for [bold]{chain_def.min_confirmations} {state.from_chain.upper()}[/bold]'
            f' confirmation(s) (~{est_min:.0f} min)...'
        )
        console.print('\n  [dim]You can safely exit (Ctrl+C) — validators will continue processing.[/dim]')

    max_polls = 600 if all_queued else 60
    try:
        swap_id = poll_for_swap_with_progress(client, state.miner_hotkey, state.from_chain, max_polls)
    except KeyboardInterrupt:
        try:
            swap_id = resolve_recent_swap_id(client, state.miner_hotkey)
        except ContractError:
            swap_id = None
        console.print('\n\n[green]Your swap is still being processed by validators.[/green]')
        if swap_id is not None:
            clear_pending_swap()
            console.print(f'[green bold]Swap ID: {swap_id}[/green bold]')
            console.print(f'[dim]Watch with: alw view swap {swap_id} --watch[/dim]\n')
        else:
            console.print(
                f'[dim]Miner UID {state.miner_uid} — once the swap initiates it will show in: '
                f'alw view active-swaps[/dim]\n'
            )
        return

    if swap_id is None:
        console.print('\n[yellow]Swap not yet initiated. Validators may still be waiting for confirmations.[/yellow]')
        console.print(
            f'[dim]Miner UID {state.miner_uid} — check: alw view active-swaps '
            '(pending_swap.json kept for retry with `alw swap resume`)[/dim]\n'
        )
        return

    clear_pending_swap()
    console.print(f'\n[green bold]Swap initiated! ID: {swap_id}[/green bold]')

    if skip_confirm:
        return

    from allways.cli.swap_commands.view import watch_swap

    final_swap = watch_swap(client, swap_id)

    if final_swap and final_swap.status == SwapStatus.COMPLETED:
        from allways.cli.swap_commands.swap import display_receipt

        display_receipt(final_swap)
