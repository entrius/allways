"""Regression: `alw swap now` must not treat a stale/expired reservation as a resolved draw.

Guards the fund-loss bug where `_poll_reservation` returned on `reserved_until != 0` and matched a
leftover reservation from an abandoned reserve (non-zero but expired), making `swap now` instruct a
taker to send funds before the pool draw ran. Origination now uses the same `live_unclaimed`
predicate as `post-tx`, and a send is gated on the reservation outliving the *claim relay* — not the
source chain's confirmation depth, which accrues after the claim via the crank's EXTEND_RESERVATION.
"""

import time
import types
from unittest.mock import MagicMock, patch

from allways.cli.swap_commands.helpers import live_unclaimed
from allways.cli.swap_commands.swap import _SEND_MARGIN_SECS, _poll_reservation

EMPTY = bytes(32)


def _resv(reserved_until, claimed=EMPTY, user='68ToGUYj'):
    """Stand-in for a decoded Reservation (only the predicate-relevant fields)."""
    return types.SimpleNamespace(
        reserved_until=reserved_until, claimed_swap_key=claimed, user=user, miner_from_addr='miner-addr'
    )


def test_live_unclaimed_rejects_the_stale_leftover_state():
    now = int(time.time())
    # The exact state that triggered the bug: non-zero but expired, empty claim, caller's own user.
    assert live_unclaimed(_resv(now - 100)) is False
    assert live_unclaimed(_resv(0)) is False  # pre-draw: no reservation written yet
    assert live_unclaimed(None) is False
    assert live_unclaimed(_resv(now + 300)) is True  # fresh, live, unclaimed
    assert live_unclaimed(_resv(now + 300, claimed=bytes([1] + [0] * 31))) is False  # already claimed


def test_poll_skips_stale_leftover_and_waits_for_the_fresh_draw():
    now = int(time.time())
    stale = _resv(now - 5)  # residue of an abandoned reserve — non-zero but in the past
    fresh = _resv(now + 300)  # what resolve_pool writes for this request
    client = MagicMock()
    client.get_reservation.side_effect = [stale, stale, fresh]
    with patch('allways.cli.swap_commands.swap.time.sleep'):
        got = _poll_reservation(client, 'miner', timeout_secs=100)
    assert got is fresh  # never the stale leftover
    assert client.get_reservation.call_count == 3


def test_poll_times_out_rather_than_returning_a_stale_reservation():
    now = int(time.time())
    client = MagicMock()
    client.get_reservation.return_value = _resv(now - 5)  # always stale/expired
    with patch('allways.cli.swap_commands.swap.time.sleep'):
        got = _poll_reservation(client, 'miner', timeout_secs=0.05)
    assert got is None  # would rather time out than green-light a send against a stale reservation


def test_send_margin_does_not_scale_with_confirmation_depth():
    """The margin covers relaying the claim on-chain, not waiting for confirmations. It must therefore
    fit inside a freshly-won reservation (ttl 480s, minus the ~60-80s pool draw) for EVERY chain —
    including BTC, whose 2 x 600s confirmation wait alone exceeds the whole TTL."""
    from allways.chains import get_chain

    btc = get_chain('btc')
    assert btc.min_confirmations * btc.seconds_per_block > _SEND_MARGIN_SECS
    assert _SEND_MARGIN_SECS < 480 - 80  # clears a fresh reservation post-draw, on any source chain


def _run_swap_now(reserved_until, from_chain='btc'):
    """Drive `swap now` end-to-end with a stubbed chain/client, returning the CliRunner result."""
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    resv = _resv(reserved_until, user=user)
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60
    )
    client.open_or_request.return_value = 'sig' * 8
    amts = types.SimpleNamespace(sol_amount=10**9, from_amount=5000, to_amount=10**9)
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10)

    argv = ['--from', from_chain, '--to', 'sol', '--amount', '0.00005']
    argv += ['--from-address', 'tb1qsource', '--receive-address', user, '--yes']

    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap._candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._poll_reservation', return_value=resv),
        patch('allways.cli.swap_commands.swap._save_pending'),
    ):
        return CliRunner().invoke(swap_now_command, argv)


def test_btc_source_reservation_with_400s_left_is_accepted_for_send():
    """Deliverable #5: the exact state that used to refuse deterministically — a freshly won BTC-source
    reservation, ~400s of life left. BTC needs 1200s of confirmations; the claim needs only the relay."""
    result = _run_swap_now(int(time.time()) + 400)
    assert result.exit_code == 0, result.output
    assert 'Reserved.' in result.output
    assert 'miner-addr' in result.output  # the send instruction the taker was never getting
    assert 'too short' not in result.output


def test_reservation_with_seconds_left_still_refuses_the_send():
    """Deliverable #3: the guard still bites when the claim genuinely can't land, and still says DON'T send."""
    result = _run_swap_now(int(time.time()) + 5)
    assert result.exit_code != 0
    assert 'too short' in result.output
    assert 'Do NOT send funds' in result.output
