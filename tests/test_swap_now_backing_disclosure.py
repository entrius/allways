"""`alw swap now` must disclose WHICH purse backs the offer (F8).

The failure guarantee — SOL refund vs TAO reimbursement — is what a taker is owed if the miner
fails, and it differs by backing. `swap now` previously showed rate + receive amount but never the
backing, so a taker funded a swap without seeing whether a SOL or TAO promise stood behind it. These
drive the command with the same stubbed-client harness as the routed tests.
"""

import time
import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import swap_now_command
from allways.cli.swap_commands.swap_intake import MinerCandidate

USER = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
EMPTY = bytes(32)


def _client(reservations):
    client = MagicMock()
    client.keypair.pubkey.return_value = USER
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60, finalize_window_secs=150
    )
    client.get_reservation.side_effect = reservations
    return client


def _flat(result) -> str:
    return ' '.join(result.output.split())


def _run(client, *, argv_extra=(), confirm_input=None):
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    # tao-backed candidate: the offer's failure guarantee is a TAO reimbursement, not a SOL refund.
    cand = MinerCandidate(miner='miner-pk', rate_display='0.0021', collateral=5 * 10**9, backing='tao')
    argv = ['--from', 'btc', '--to', 'sol', '--amount', '0.00005', '--from-address', 'tb1qsource']
    argv += ['--receive-address', USER, *argv_extra]
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=({}, client)),
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=None),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._save_pending'),
        patch('allways.cli.swap_commands.swap.time.sleep'),
    ):
        result = CliRunner().invoke(swap_now_command, argv, input=confirm_input)
    return result


def _live_resv(collateral_chain='tao'):
    return types.SimpleNamespace(
        router=USER,
        user=USER,
        reserved_until=int(time.time()) + 400,
        created_at=int(time.time()),
        finalize_by=int(time.time()) + 400,
        claimed_swap_key=EMPTY,
        collateral_chain=collateral_chain,
        miner_from_addr='miner-addr',
        from_amount=5000,
        to_amount=10**9,
    )


def test_preview_discloses_the_tao_backing_and_its_guarantee():
    client = _client([None])  # no seat to resume — abort at the bid confirm
    r = _run(client, confirm_input='n\n')  # decline before any fee is spent
    out = _flat(r)
    assert 'tao-backed' in out
    assert 'TAO reimbursement' in out  # the GUARANTEE, not a bare asset name
    assert not client.open_or_request.called


def test_seat_filled_line_echoes_the_reservations_backing():
    client = _client([_live_resv(collateral_chain='tao')])  # a live seat this taker already holds
    r = _run(client)
    out = _flat(r)
    assert 'Seat filled' in out
    assert 'tao-backed' in out
