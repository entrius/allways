"""`alw swap now` must name the failing bound (F-2), not a generic "within bounds" shrug.

``unviable_reason`` existed for exactly these branches and was never called: both no-viable-miner
fail paths printed the same generic line, leaving the taker guessing WHICH bound (min/max swap,
collateral, executable rate) refused them. Same stubbed-client harness as the disclosure tests,
but the real selection/intake path runs so the reason is genuinely computed, not injected.
"""

import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import swap_now_command
from allways.cli.swap_commands.swap_intake import MinerCandidate

USER = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'


def _run(argv_extra=()):
    client = MagicMock()
    client.keypair.pubkey.return_value = USER
    # min_swap 10 SOL: the 0.00005 BTC intake's hub leg lands far below it → GATE_BELOW_MIN.
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=10**10, max_swap_amount=0, pool_window_secs=60, finalize_window_secs=150
    )
    cand = MinerCandidate(miner='miner-pk', rate_display='0.0021', collateral=10**12, backing='sol')
    argv = ['--from', 'btc', '--to', 'sol', '--amount', '0.00005', '--from-address', 'tb1qsource']
    argv += ['--receive-address', USER, *argv_extra]
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=({}, client)),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
    ):
        return CliRunner().invoke(swap_now_command, argv)


def test_auto_select_failure_names_the_bound():
    r = _run()
    assert 'below min swap' in r.output
    assert 'within bounds' not in r.output


def test_named_miner_failure_names_the_bound():
    r = _run(argv_extra=['--miner', 'miner-pk'])
    assert 'below min swap' in r.output
    assert 'within bounds' not in r.output
