"""alw swap - Execute and manage cross-chain swaps.

Phase-9 stub: the full taker swap flow (reserve → deposit → confirm/initiate) moves
on-chain to Solana with the reservation pool (open_or_request/resolve_pool +
submit_swap_claim). The taker CLI intake is not wired yet, so `swap now` is stubbed;
the group is kept so `post-tx`/`quote`/`resume-reservation` still register under it."""

from typing import Optional

import click

from allways.cli.help import StyledGroup
from allways.cli.swap_commands.helpers import phase9_unavailable


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
@click.option(
    '--slippage',
    'slippage',
    type=float,
    default=2.0,
    help=(
        'Maximum rate slippage as a percent (e.g. 2.0 means 2%) between your quote and '
        "the reservation. The reservation is rejected if the miner's current rate would "
        'give you more than this percentage less than your quoted amount. Default: 2.0%.'
    ),
)
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
    auto_select: bool,
    skip_confirm: bool,
    slippage: float,
    btc_fee_rate_opt: Optional[int],
):
    """Guided interactive swap - step by step.

    [dim]Walks through a complete swap from start to finish:
    - Select swap direction and miner
    - Enter amount and addresses
    - Funds are sent automatically when possible
    - Transaction hash is posted to validators automatically[/dim]

    [dim]Interactive mode:
        $ alw swap now[/dim]
    """
    phase9_unavailable('Guided swap (`swap now`)')
