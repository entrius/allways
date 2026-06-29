"""alw swap quote - Preview rates and estimated receive amounts before swapping.

Stub: the rate preview reads miner quotes from the old ink! commitment
surface; its Solana MinerQuote-backed taker view is not wired yet."""

import click

from allways.cli.swap_commands.helpers import taker_view_unavailable


@click.command('quote')
@click.option('--from', 'from_chain', default=None, type=str, help='Source chain (e.g. btc, tao)')
@click.option('--to', 'to_chain', default=None, type=str, help='Destination chain (e.g. btc, tao)')
@click.option('--amount', default=None, type=float, help='Amount to send in source chain units')
def quote_command(from_chain: str, to_chain: str, amount: float):
    """Preview rates and estimated receive amounts for a swap.

    \b
    Shows all available miners, their rates, and what you would receive
    after fees — without committing to a swap. Omit any flag to be prompted.

    \b
    Examples:
        alw swap quote
        alw swap quote --from btc --to tao --amount 0.1
        alw swap quote --from tao --to btc --amount 50
    """
    taker_view_unavailable('Swap quote preview')
