"""alw status - Quick dashboard: network, Solana RPC/program health, your balances, and miner/taker state.

Reads the live Solana program. If your keypair is a registered miner it summarizes miner state; otherwise it
shows the taker view (any live reservation for --miner / the saved swap). Identities shown are the Solana
keypair (always — fees + on-chain identity) plus the configured bittensor wallet's coldkey + TAO balance."""

import time

import click

from allways.cli.help import StyledCommand
from allways.cli.swap_commands.helpers import (
    ZERO_SWAP_KEY,
    console,
    fail,
    from_lamports,
    get_effective_config,
    get_solana_cli_context,
    print_json,
    resolve_solana_keypair_path,
    safe_read,
    set_json_output,
)


def _tao_identity(config):
    """(coldkey ss58, TAO balance | None) for the configured bittensor wallet, or None when no
    wallet is configured or its files are missing. Reads only the public coldkey file — no
    password; an unreachable subtensor degrades to an unknown balance, never an error."""
    name = config.get('wallet')
    if not name:
        return None
    import bittensor as bt

    try:
        ss58 = bt.Wallet(name=name).coldkeypub.ss58_address
    except Exception:
        return None
    try:
        balance = float(bt.Subtensor(network=config.get('network', 'finney')).get_balance(ss58))
    except Exception:
        balance = None
    return ss58, balance


def _load_caller(config):
    """Load the caller's Solana keypair (env/config-resolved) without creating one. Returns Pubkey or None."""
    from allways.solana import keys

    try:
        return keys.load_keypair(resolve_solana_keypair_path(config)).pubkey()
    except Exception:
        return None


@click.command('status', cls=StyledCommand)
@click.option('--miner', 'miner_pk', default=None, type=str, help='Miner pubkey to check for a taker reservation')
@click.option('--json', 'as_json', is_flag=True, help='Emit machine-readable JSON instead of a dashboard.')
def status_command(miner_pk, as_json):
    """Show a quick dashboard of network, program health, your balance, and swap state.

    [dim]Examples:
        $ alw status[/dim]
    """
    set_json_output(as_json)
    config = get_effective_config()
    network = config.get('network', 'finney')
    _, client = get_solana_cli_context(need_keypair=False)

    cfg = safe_read(lambda: client.get_config(), what='read the program config')
    program_initialized = cfg is not None
    halted = bool(cfg and cfg.halted)

    caller = _load_caller(config)
    tao = _tao_identity(config)
    balance = None
    miner_state = None
    if caller is not None:
        balance = safe_read(lambda: client.rpc.get_account_lamports(caller), what='read balance')
        miner_state = safe_read(lambda: client.get_miner_state(caller), what='read miner state')

    is_miner = miner_state is not None
    now = int(time.time())

    # Taker reservation: explicit --miner, else the miner saved by `alw swap now`.
    resv = None
    resv_miner = miner_pk or _saved_miner()
    if not is_miner and resv_miner:
        from solders.pubkey import Pubkey

        try:
            miner_key = Pubkey.from_string(resv_miner)
        except (ValueError, TypeError):
            fail(f'Invalid --miner pubkey: {resv_miner}')
        resv = safe_read(lambda: client.get_reservation(miner_key), what='read reservation')

    if as_json:
        out = {
            'network': network,
            'solana_rpc': client.rpc.url,
            'program_initialized': program_initialized,
            'halted': halted,
            'caller': str(caller) if caller else None,
            'balance_sol': from_lamports(balance) if balance is not None else None,
            'is_miner': is_miner,
        }
        if tao is not None:
            out['wallet'] = config['wallet']
            out['coldkey'] = tao[0]
            out['tao_balance'] = tao[1]
        if is_miner:
            out['miner'] = {
                'collateral_sol': from_lamports(miner_state.collateral),
                'active': miner_state.active,
                'has_active_swap': miner_state.has_active_swap,
                'successful_swaps': miner_state.successful_swaps,
                'failed_swaps': miner_state.failed_swaps,
            }
        elif resv is not None:
            out['reservation'] = {
                'miner': resv_miner,
                'from_chain': resv.from_chain,
                'to_chain': resv.to_chain,
                'reserved_until': int(resv.reserved_until),
                'deposit_claimed': bytes(resv.claimed_swap_key) != ZERO_SWAP_KEY,
            }
        print_json(out)
        return

    console.print('\n[bold]Allways Status[/bold]\n')
    console.print(f'  Network:      {network}')
    console.print(f'  Solana RPC:   {client.rpc.url}')
    console.print(
        f'  Program:      {"[green]initialized[/green]" if program_initialized else "[red]not initialized[/red]"}'
        + ('  [red](halted)[/red]' if halted else '')
    )
    if caller is not None:
        bal = f'{from_lamports(balance):.4f} SOL' if balance is not None else '[dim]unknown[/dim]'
        console.print(f'  Keypair:      {caller}')
        console.print(f'  Balance:      {bal}')
    else:
        console.print(
            '  Keypair:      [dim]none loaded (`alw config set solana-keypair <path>` or SOLANA_KEYPAIR_PATH)[/dim]'
        )
    if tao is not None:
        ss58, tao_balance = tao
        tao_s = f'{tao_balance:.4f} τ' if tao_balance is not None else '[dim]unknown[/dim]'
        console.print(f'  Wallet:       {config["wallet"]} ({ss58})')
        console.print(f'  TAO balance:  {tao_s}')

    if is_miner:
        console.print('\n[bold]Miner[/bold]\n')
        console.print(f'  Collateral:      {from_lamports(miner_state.collateral):.4f} SOL')
        console.print(f'  Active:          {"[green]yes[/green]" if miner_state.active else "[red]no[/red]"}')
        console.print(f'  Active swap:     {"[yellow]yes[/yellow]" if miner_state.has_active_swap else "no"}')
        console.print(f'  Swaps (ok/fail): {miner_state.successful_swaps} / {miner_state.failed_swaps}')
        console.print('\n[dim]Manage with `alw miner status`, `alw collateral view`.[/dim]\n')
        return

    console.print('\n[bold]Taker[/bold]\n')
    if resv is not None:
        remaining = max(0, int(resv.reserved_until) - now)
        claimed = bytes(resv.claimed_swap_key) != ZERO_SWAP_KEY
        console.print(f'  Reservation:  {resv.from_chain.upper()} → {resv.to_chain.upper()} on {resv_miner}')
        console.print(f'  Reserved:     {"expired" if remaining == 0 else f"{remaining}s remaining"}')
        console.print(f'  Deposit:      {"claimed" if claimed else "not yet sent"}')
    elif resv_miner:
        console.print(f'  [dim]No active reservation on {resv_miner}.[/dim]')
    else:
        console.print('  [dim]No pending swap. Preview with `alw swap quote`, originate with `alw swap now`.[/dim]')
    console.print()


def _saved_miner():
    from allways.cli.swap_commands.helpers import PENDING_SWAP_FILE

    if not PENDING_SWAP_FILE.exists():
        return None
    import json

    try:
        return json.loads(PENDING_SWAP_FILE.read_text()).get('miner')
    except (json.JSONDecodeError, OSError):
        return None
