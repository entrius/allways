"""`alw swap now --min-rate` sends only when the reservation on chain pins at least that rate.

A rival re-quoted between the preview and the bid; the reservation pinned the lower rate and the deposit went out
anyway. The floor is judged from the reservation re-read after it is live, before any funds move, and fails closed.
"""

import time
import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import _pinned_rate_refusal, swap_now_command
from allways.cli.swap_commands.swap_intake import MinerCandidate
from allways.constants import RATE_PRECISION

USER = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
EMPTY = bytes(32)


def _resv(rate_display, user=USER):
    now = int(time.time())
    return types.SimpleNamespace(
        router=user,
        user=user,
        reserved_until=now + 400,
        created_at=now,
        finalize_by=now + 400,
        claimed_swap_key=EMPTY,
        collateral_chain='sol',
        miner_from_addr='miner-addr',
        from_amount=5000,
        to_amount=10**9,
        rate=int(float(rate_display) * RATE_PRECISION),
    )


def _refusal(client, min_rate, from_chain='sol', to_chain='solusdc'):
    return _pinned_rate_refusal(client, 'miner-pk', 'sol', USER, from_chain, to_chain, min_rate)


def _client(*reservations):
    client = MagicMock()
    client.get_reservation.side_effect = list(reservations)
    return client


def test_a_pinned_rate_at_or_over_the_floor_sends():
    assert _refusal(_client(_resv('109.44')), 109.44) is None


def test_a_pinned_rate_under_the_floor_is_refused():
    assert _refusal(_client(_resv('103.4')), 108.96) == 'Reservation rate 103.4 below --min-rate 108.96; not sending'


def test_the_floor_is_directional_what_the_taker_receives_per_one_sent():
    # btc->sol stores canonical SOL per BTC inverted: 0.0021 BTC per SOL reads 476.19 SOL per BTC sent
    assert _refusal(_client(_resv('0.0021')), 470, 'btc', 'sol') is None
    assert 'below --min-rate 480' in _refusal(_client(_resv('0.0021')), 480, 'btc', 'sol')


def test_an_unreadable_reservation_fails_closed():
    failing = MagicMock()
    failing.get_reservation.side_effect = TimeoutError('rpc timed out')
    cases = [
        (failing, 'rpc timed out'),
        (_client(None), 'no live reservation of ours'),
        (_client(_resv('109.44', user='someone-else')), 'no live reservation of ours'),
        (_client(_resv('0')), 'pinned rate 0.0'),
    ]
    for client, why in cases:
        assert _refusal(client, 100.0) == f'Reservation unreadable; not sending: {why}'


def _run(client, *argv_extra):
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=10**9, to_amount=109 * 10**6)
    cand = MinerCandidate(miner='miner-pk', rate_display='109.44', collateral=5 * 10**9, backing='sol')
    argv = ['--from', 'sol', '--to', 'solusdc', '--amount', '1', '--receive-address', USER, '--yes', '--send']
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=({}, client)),
        patch('allways.cli.swap_commands.swap._gate_provider', return_value=None),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._save_pending'),
        patch('allways.cli.swap_commands.swap.time.sleep'),
        patch('allways.cli.swap_commands.swap._auto_send_wizard', return_value=True) as wizard,
    ):
        result = CliRunner().invoke(swap_now_command, [*argv, *argv_extra])
    return result, wizard


def _cli_client(pinned):
    client = MagicMock()
    client.keypair.pubkey.return_value = USER
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60, finalize_window_secs=150
    )
    client.get_reservation.side_effect = [_resv(pinned), _resv(pinned)]  # the resumable seat, then the re-read
    return client


def test_swap_now_under_the_floor_exits_non_zero_without_sending():
    result, wizard = _run(_cli_client('103.4'), '--min-rate', '108.96')
    assert result.exit_code == 1
    assert 'Reservation rate 103.4 below --min-rate 108.96; not sending' in ' '.join(result.output.split())
    wizard.assert_not_called()


def test_swap_now_at_the_floor_sends():
    result, wizard = _run(_cli_client('109.44'), '--min-rate', '109')
    assert result.exit_code == 0, result.output
    wizard.assert_called_once()


def test_swap_now_without_the_flag_never_re_reads_the_reservation():
    client = _cli_client('103.4')
    result, wizard = _run(client)
    assert result.exit_code == 0, result.output
    wizard.assert_called_once()
    assert client.get_reservation.call_count == 1
