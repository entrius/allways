"""alw swap - Execute and manage cross-chain swaps.

Origination is on-chain on Solana: the taker opens a per-miner reservation pool (`open_or_request`), a
permissionless stake-weighted draw (`resolve_pool`, run by the validator crank) picks the winning request,
then the taker sends source funds to the winning miner's address. `swap now` wires the flag-driven
origination slice (select miner → compute amounts → open_or_request → poll for the reservation). Auto
fund-sending + post-tx (`swap post-tx`) land next."""

import json
import time
from typing import List, Optional

import click

from allways.chains import SUPPORTED_CHAINS, get_chain
from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import PENDING_SWAP_FILE, console, fail, get_solana_cli_context
from allways.cli.swap_commands.swap_intake import (
    MinerCandidate,
    rate_display_from_fixed,
    select_best_miner,
    to_smallest_units,
)
from allways.constants import NUMERAIRE_CHAIN


@click.group('swap', cls=StyledGroup, show_disclaimer=True)
def swap_group():
    """Execute and manage cross-chain swaps."""


def _candidate_miners(client, from_chain: str, to_chain: str) -> List[MinerCandidate]:
    """All miners with a posted quote for this exact direction, with their collateral attached."""
    out: List[MinerCandidate] = []
    for _pk, q in client.get_all('MinerQuote'):
        if q.from_chain != from_chain or q.to_chain != to_chain:
            continue
        collateral = client.get_collateral_lamports(q.miner) or 0
        out.append(MinerCandidate(miner=q.miner, rate_display=rate_display_from_fixed(q.rate), collateral=collateral))
    return out


def _poll_reservation(client, miner, timeout_secs: int):
    """Poll the per-miner Reservation until the draw populates it (reserved_until != 0) or we time out."""
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        resv = client.get_reservation(miner)
        if resv is not None and int(getattr(resv, 'reserved_until', 0)) != 0:
            return resv
        time.sleep(3)
    return None


@swap_group.command('now', show_disclaimer=True)
@click.option('--from', 'from_chain_opt', default=None, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain_opt', default=None, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', 'amount_opt', default=None, type=float, help='Amount to send in source chain units')
@click.option('--receive-address', 'receive_address_opt', default=None, help='Receive address on destination chain')
@click.option('--from-address', 'from_address_opt', default=None, help='Source address on source chain')
@click.option('--from-tx-hash', 'from_tx_hash_opt', default=None, help='Source tx hash (skip fund sending)')
@click.option('--yes', 'skip_confirm', is_flag=True, help='Skip confirmation prompts')
@click.option(
    '--btc-fee-rate',
    'btc_fee_rate_opt',
    type=click.IntRange(min=1),
    default=None,
    metavar='SAT_PER_VB',
    help=(
        'Fee rate for the BTC source tx, in satoshis per virtual byte (sat/vB). '
        'Higher = faster confirmation. Typical mainnet values: 5-20. Default '
        'auto-estimates from the mempool. Lightweight wallet only.'
    ),
)
def swap_now_command(
    from_chain_opt: Optional[str],
    to_chain_opt: Optional[str],
    amount_opt: Optional[float],
    receive_address_opt: Optional[str],
    from_address_opt: Optional[str],
    from_tx_hash_opt: Optional[str],
    skip_confirm: bool,
    btc_fee_rate_opt: Optional[int],
):
    """Originate a swap: reserve a miner on-chain, then send source funds.

    [dim]Flag-driven form (interactive prompts + auto fund-sending land next):
        alw swap now --from sol --to btc --amount 1.0 --receive-address <btc-addr> --yes[/dim]
    """
    from_chain = (from_chain_opt or '').lower()
    to_chain = (to_chain_opt or '').lower()
    if from_chain not in SUPPORTED_CHAINS or to_chain not in SUPPORTED_CHAINS:
        fail(f'--from/--to must each be one of: {", ".join(SUPPORTED_CHAINS)}')
    if from_chain == to_chain or NUMERAIRE_CHAIN not in (from_chain, to_chain):
        fail(f'A launch swap must have a {NUMERAIRE_CHAIN.upper()} leg (every pair is hub<->spoke).')
    if amount_opt is None or amount_opt <= 0:
        fail('--amount (source-chain units) is required.')
    if not receive_address_opt:
        fail('--receive-address (destination chain) is required.')

    _config, client = get_solana_cli_context(need_keypair=True)
    user = client.keypair.pubkey()
    user_from_addr = str(user) if from_chain == NUMERAIRE_CHAIN else (from_address_opt or '')
    if not user_from_addr:
        fail(f'--from-address (your source-chain address) is required for a non-{NUMERAIRE_CHAIN.upper()} source.')

    cfg = client.get_config()
    min_swap = int(getattr(cfg, 'min_swap_amount', 0)) if cfg else 0
    max_swap = int(getattr(cfg, 'max_swap_amount', 0)) if cfg else 0
    pool_window = int(getattr(cfg, 'pool_window_secs', 60)) if cfg else 60

    from_amount = to_smallest_units(amount_opt, from_chain)
    candidates = _candidate_miners(client, from_chain, to_chain)
    if not candidates:
        fail(f'No miners quoting {from_chain}->{to_chain} right now.')
    best = select_best_miner(candidates, from_chain, to_chain, from_amount, min_swap, max_swap)
    if best is None:
        fail('No miner can fund an executable swap for that amount within bounds.')
    cand, amts = best
    recv = amts.to_amount / 10 ** get_chain(to_chain).decimals

    console.print(
        f'\n  Swap [cyan]{amount_opt} {from_chain.upper()}[/cyan] -> ~[cyan]{recv:.8g} {to_chain.upper()}[/cyan]'
        f'  (miner [dim]{str(cand.miner)[:8]}…[/dim], rate {cand.rate_display} per SOL)\n'
    )
    if not skip_confirm and not click.confirm('  Reserve this miner on-chain?', default=False):
        return

    sig = client.open_or_request(
        cand.miner,
        from_chain,
        to_chain,
        user,
        user_from_addr,
        receive_address_opt,
        amts.sol_amount,
        amts.from_amount,
        amts.to_amount,
    )
    console.print(f'[green]  Reservation requested[/green] (tx {sig[:16]}…). Waiting for the draw to resolve…')

    resv = _poll_reservation(client, cand.miner, timeout_secs=pool_window + 60)
    if resv is None:
        fail('  Pool not resolved yet — check `alw view reservation` shortly.')
    if str(resv.user) != str(user):
        fail("  Another taker won this miner's draw. Re-run to try again.")

    _save_pending(cand.miner, from_chain, to_chain)
    console.print(
        f'[green]  Reserved.[/green] Send [cyan]{amount_opt} {from_chain.upper()}[/cyan] to '
        f'[cyan]{resv.miner_from_addr}[/cyan], then run [bold]alw swap post-tx[/bold] with the tx hash.'
    )


def _save_pending(miner, from_chain: str, to_chain: str) -> None:
    """Persist the reserved miner so `alw view reservation` / `alw status` can find it without a flag."""
    try:
        PENDING_SWAP_FILE.parent.mkdir(parents=True, exist_ok=True)
        PENDING_SWAP_FILE.write_text(json.dumps({'miner': str(miner), 'from_chain': from_chain, 'to_chain': to_chain}))
    except OSError:
        pass  # best-effort convenience; not required for the swap to proceed
