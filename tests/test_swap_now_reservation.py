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
from allways.cli.swap_commands.swap import (
    _SEND_MARGIN_SECS,
    _poll_drawn,
    _poll_reservation,
    _self_crank_resolve,
)
from allways.solana.rpc import TransientRpcError

EMPTY = bytes(32)


def _resv(reserved_until, claimed=EMPTY, user='68ToGUYj'):
    """Stand-in for a decoded Reservation (only the predicate-relevant fields)."""
    return types.SimpleNamespace(
        reserved_until=reserved_until,
        claimed_swap_key=claimed,
        user=user,
        miner_from_addr='miner-addr',
        to_amount=10**9,
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


def test_self_crank_swallows_a_transient_rpc_fault():
    """A transient RPC fault while nudging the pool must not abort origination — the resolve_pool may
    already have landed. It is swallowed so the poll loop re-cranks and re-reads the real outcome. This
    is the exact fault (getSignatureStatuses -32603 mid-crank) that crashed the first mainnet BTC swap."""
    client = MagicMock()
    client.resolve_pool.side_effect = TransientRpcError('getSignatureStatuses: -32603 Internal error')
    _self_crank_resolve(client, 'miner')  # must NOT raise


def test_poll_drawn_self_heals_when_the_crank_keeps_flaking_but_the_seat_is_drawn():
    """End-to-end: every crank nudge throws a transient RPC error, yet the pool still draws our seat.
    The outcome-driven loop must return it rather than die on the nudge exception."""
    us = 'ME'
    drawn = types.SimpleNamespace(  # unfilled seat won by us: reserved_until==0, created_at==0, live
        reserved_until=0,
        created_at=0,
        finalize_by=int(time.time()) + 120,
        router=us,
    )
    client = MagicMock()
    client.resolve_pool.side_effect = TransientRpcError('boom')  # crank flakes on every pass
    client.get_reservation.side_effect = [None, drawn]  # not drawn yet, then drawn to us
    with patch('allways.cli.swap_commands.swap.time.sleep'):
        got = _poll_drawn(client, 'miner', us, timeout_secs=100)
    assert got is drawn


def _run_swap_now(reserved_until, from_chain='btc'):
    """Drive `swap now` end-to-end with a stubbed chain/client, returning the CliRunner result."""
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    resv = _resv(reserved_until, user=user)  # the FILLED reservation, post-finalize
    # The drawn (unfilled) reservation this taker gets seated with, before finalize.
    drawn = types.SimpleNamespace(
        router=user, reserved_until=0, finalize_by=int(time.time()) + 60, rate=int(0.0021 * 10**18)
    )
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60
    )
    client.get_reservation.return_value = None  # no seat held yet -> normal bid path (not a resume)
    client.open_or_request.return_value = 'sig' * 8
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10)

    argv = ['--from', from_chain, '--to', 'sol', '--amount', '0.00005']
    argv += ['--from-address', 'tb1qsource', '--receive-address', user, '--yes']

    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._poll_drawn', return_value=drawn),
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


# ── benign crank-race handling: a lost resolve_pool must not abort `swap now` ───────────────────────
# The self-crank races the validator (and peer takers) to resolve the pool. When it loses, the tx can
# fail two ways: rejected in simulation (error carries the Anchor NAME) or landed-failed (the confirm
# path stringifies status["err"] with only the numeric `{'Custom': N}` code). Both are benign — the
# draw still happened, we keep polling for our seat. Matching names alone missed the code-only form,
# which re-raised and abandoned the taker's already-paid, since-drawn seat (F2, 2026-07-12 mainnet run).


def _crank_raising(err_text):
    from allways.cli.swap_commands.swap import _self_crank_resolve

    client = MagicMock()
    client.resolve_pool.side_effect = RuntimeError(err_text)
    _self_crank_resolve(client, 'miner-pubkey')  # must NOT raise


def test_crank_swallows_benign_race_by_name():
    # pre-flight simulation rejection — carries the Anchor error name
    for name in ('PoolNotClosed', 'NoRequests', 'SeedSlotNotYetProduced', 'AlreadyFilled'):
        _crank_raising(f'Program log: AnchorError ... Error Code: {name}. Error Number: 60xx')


def test_crank_swallows_benign_race_by_numeric_code():
    # landed-then-failed tx — confirm() surfaces only the numeric code, no name (the F2 miss)
    for code in (6042, 6044, 6045, 6046):
        _crank_raising(f"tx 5abc failed: {{'InstructionError': [0, {{'Custom': {code}}}]}}")


def test_crank_reraises_a_real_error():
    import pytest

    from allways.cli.swap_commands.swap import _self_crank_resolve

    client = MagicMock()
    # a non-benign failure (e.g. MinerReserved 6022) must still propagate
    client.resolve_pool.side_effect = RuntimeError("tx 5abc failed: {'InstructionError': [0, {'Custom': 6022}]}")
    with pytest.raises(RuntimeError):
        _self_crank_resolve(client, 'miner-pubkey')


# ── pool-contention visibility (feat): surface a contested/already-open pool + the lost-draw reason ──
# `swap now` used to bid blind and, on a loss, print one ambiguous "not seated" message. These cover
# the read-only helpers that back the new pre-bid notice (odds + fee warning) and the specific
# lost-the-draw-to-<router> reason. Both are best-effort: a bad read must never block or crash a swap.

_A = bytes([1] * 32)
_B = bytes([2] * 32)
_V = bytes([7] * 32)


def _pool(opened_at, closes_at, routers):
    return types.SimpleNamespace(
        opened_at=opened_at,
        closes_at=closes_at,
        requests=[types.SimpleNamespace(router=r) for r in routers],
    )


def test_pool_contention_reports_an_open_uniform_pool():
    from allways.cli.swap_commands.swap import _pool_contention

    now = int(time.time())
    client = MagicMock()
    client.get_pool.return_value = _pool(now - 10, now + 40, [_A, _B])  # 2 takers, still in window
    c = _pool_contention(client, 'miner', types.SimpleNamespace(validators=[]))
    assert c.is_open and c.bidders == 2 and c.weighted_rivals == 0
    assert 30 <= c.closes_in <= 40


def test_pool_contention_flags_a_weighted_validator_rival():
    from allways.cli.swap_commands.swap import _pool_contention

    now = int(time.time())
    client = MagicMock()
    client.get_pool.return_value = _pool(now - 5, now + 30, [_V, _B])
    cfg = types.SimpleNamespace(validators=[types.SimpleNamespace(key=_V, weight=100)])
    c = _pool_contention(client, 'miner', cfg)
    assert c.is_open and c.bidders == 2 and c.weighted_rivals == 1  # the validator dominates the draw


def test_pool_contention_treats_a_closed_window_as_not_open():
    from allways.cli.swap_commands.swap import _pool_contention

    now = int(time.time())
    client = MagicMock()
    client.get_pool.return_value = _pool(now - 100, now - 40, [_A])  # window already passed
    assert _pool_contention(client, 'miner', types.SimpleNamespace(validators=[])).is_open is False


def test_pool_contention_never_raises_on_a_bad_read():
    from allways.cli.swap_commands.swap import _pool_contention

    client = MagicMock()
    client.get_pool.side_effect = RuntimeError('rpc down')  # must degrade to "not open", never crash a bid
    assert _pool_contention(client, 'miner', types.SimpleNamespace(validators=[])).is_open is False


def test_lost_seat_to_names_the_rival_that_won_the_draw():
    from allways.cli.swap_commands.swap import _lost_seat_to

    now = int(time.time())
    client = MagicMock()
    # a freshly-drawn, not-yet-filled seat (reserved_until==0, created_at==0, finalize_by ahead) held
    # by a DIFFERENT router than us -> we lost the draw to it.
    client.get_reservation.return_value = types.SimpleNamespace(
        reserved_until=0, created_at=0, finalize_by=now + 100, router='RIVAL'
    )
    assert _lost_seat_to(client, 'miner', 'ME') == 'RIVAL'


def test_lost_seat_to_is_none_when_no_seat_is_drawn_yet():
    from allways.cli.swap_commands.swap import _lost_seat_to

    now = int(time.time())
    client = MagicMock()
    # drawn window already lapsed -> not a live drawn seat -> "draw didn't resolve" branch, not "lost"
    client.get_reservation.return_value = types.SimpleNamespace(
        reserved_until=0, created_at=0, finalize_by=now - 100, router='RIVAL'
    )
    assert _lost_seat_to(client, 'miner', 'ME') is None


# ── resumability: recover an already-held seat instead of paying for a second bid ────────────────────
# A prior `swap now` can bid + draw (or even finalize) and then crash on a transient RPC before it
# instructs the send. Re-running must RESUME the reused per-miner reservation for this taker — not
# re-bid (double fee) — by reading `get_reservation` up front.


def _run_resume(existing, poll_resv):
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60
    )
    client.get_reservation.return_value = existing
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10)
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    argv = [
        '--from',
        'btc',
        '--to',
        'sol',
        '--amount',
        '0.00005',
        '--from-address',
        'tb1qsrc',
        '--receive-address',
        user,
        '--yes',
    ]
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch(
            'allways.cli.swap_commands.swap._poll_drawn', return_value=None
        ),  # unused on a resume; keeps the foreign-seat fall-through fast
        patch('allways.cli.swap_commands.swap._poll_reservation', return_value=poll_resv),
        patch('allways.cli.swap_commands.swap._save_pending'),
    ):
        return client, CliRunner().invoke(swap_now_command, argv)


def test_swap_now_resumes_a_drawn_seat_without_re_bidding():
    now = int(time.time())
    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    # a seat drawn to us but not finalized (reserved_until==0, created_at==0, finalize_by ahead)
    drawn = types.SimpleNamespace(
        reserved_until=0, created_at=0, finalize_by=now + 120, router=user, rate=int(0.0021 * 10**18)
    )
    live = _resv(now + 400, user=user)  # what finalize produces (returned by the patched _poll_reservation)
    client, result = _run_resume(drawn, poll_resv=live)
    assert result.exit_code == 0, result.output
    assert 'Resuming the seat you already drew' in result.output
    assert 'Reserved.' in result.output
    client.open_or_request.assert_not_called()  # NO second bid / second fee
    client.finalize_reservation.assert_called_once()  # but it did finalize the held seat


def test_swap_now_resumes_a_live_reservation_without_bidding_or_finalizing():
    now = int(time.time())
    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    live = _resv(now + 400, user=user)  # already finalized, ours, unclaimed — just needs the send
    client, result = _run_resume(live, poll_resv=None)
    assert result.exit_code == 0, result.output
    assert 'Resuming the reservation you already hold' in result.output
    assert 'Reserved.' in result.output
    client.open_or_request.assert_not_called()
    client.finalize_reservation.assert_not_called()


def test_swap_now_does_not_resume_a_foreign_seat():
    now = int(time.time())
    other = 'SOMEONE_ELSE'
    # a live reservation owned by a different taker must NOT be treated as ours -> falls through to bid
    foreign = _resv(now + 400, user=other)
    client, result = _run_resume(
        foreign, poll_resv=_resv(now + 400, user='68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF')
    )
    # with no _poll_drawn patched, the bid path runs its self-crank; we only assert it did NOT short-circuit
    # into a resume (no "Resuming" banner) — it treated the foreign seat as not-ours.
    assert 'Resuming' not in result.output
