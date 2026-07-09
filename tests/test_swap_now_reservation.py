"""Regression: `alw swap now` must not treat a stale/expired reservation as a resolved draw.

Guards the fund-loss bug where `_poll_reservation` returned on `reserved_until != 0` and matched a
leftover reservation from an abandoned reserve (non-zero but expired), making `swap now` instruct a
taker to send funds before the pool draw ran. Origination now uses the same `live_unclaimed`
predicate as `post-tx`, and a send is gated on the reservation outliving the source chain's
confirmation wait.
"""

import time
import types
from unittest.mock import MagicMock, patch

from allways.cli.swap_commands.helpers import live_unclaimed
from allways.cli.swap_commands.swap import _poll_reservation, _send_margin_secs

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


def test_send_margin_scales_with_source_confirmation_wait():
    assert _send_margin_secs('btc') > _send_margin_secs('tao') > _send_margin_secs('sol')
    assert _send_margin_secs('btc') == 2 * 600 + 30  # 2 confs × 600s/block + relay slack
