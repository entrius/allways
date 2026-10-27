"""`alw swap now` must not treat a stale/expired reservation as a resolved draw.

`_poll_reservation` must not match a leftover reservation from an abandoned reserve (non-zero but
expired) and instruct a taker to send funds before the pool draw ran. Origination uses the same
`live_unclaimed` predicate as `post-tx`, and a send is gated on the reservation outliving the *claim relay* — not the
source chain's confirmation depth, which accrues after the claim via the crank's EXTEND_RESERVATION.
"""

import time
import types
from unittest.mock import MagicMock, patch

import pytest

from allways.cli.swap_commands.helpers import live_unclaimed
from allways.cli.swap_commands.swap import (
    _alpha_send_lines,
    _auto_send_wizard,
    _deadline_lines,
    _poll_drawn,
    _poll_reservation,
    _refuse_uncovered,
    _self_crank_resolve,
    _source_provider,
)
from allways.cli.swap_commands.swap_intake import MinerCandidate
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
    # Non-zero but expired, empty claim, caller's own user: not live.
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
        got = _poll_reservation(client, 'miner', timeout_secs=100, backing='sol')
    assert got is fresh  # never the stale leftover
    assert client.get_reservation.call_count == 3


def test_poll_times_out_rather_than_returning_a_stale_reservation():
    now = int(time.time())
    client = MagicMock()
    client.get_reservation.return_value = _resv(now - 5)  # always stale/expired
    with patch('allways.cli.swap_commands.swap.time.sleep'):
        got = _poll_reservation(client, 'miner', timeout_secs=0.05, backing='sol')
    assert got is None  # would rather time out than green-light a send against a stale reservation


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
        got = _poll_drawn(client, 'miner', us, timeout_secs=100, backing='sol')
    assert got is drawn


def _run_swap_now(reserved_until, from_chain='btc'):
    """Drive `swap now` end-to-end with a stubbed chain/client, returning the CliRunner result."""
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    resv = _resv(reserved_until, user=user)  # the FILLED reservation, post-finalize
    # The drawn (unfilled) reservation this taker gets seated with, before finalize.
    drawn = types.SimpleNamespace(
        router=user,
        reserved_until=0,
        finalize_by=int(time.time()) + 60,
        rate=int(0.0021 * 10**18),
        miner_from_addr='miner-addr',
    )
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60
    )
    client.get_reservation.return_value = None  # no seat held yet -> normal bid path (not a resume)
    client.open_or_request.return_value = 'sig' * 8
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    # The real dataclass, not a look-alike: a fake that drifts from MinerCandidate stops testing
    # the wiring it is supposed to lock.
    cand = MinerCandidate(miner='miner-pk', rate_display='0.0021', collateral=10**10)

    argv = ['--from', from_chain, '--to', 'sol', '--amount', '0.00005']
    argv += ['--from-address', 'tb1qsource', '--receive-address', user, '--yes']

    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=None),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._poll_drawn', return_value=drawn),
        patch('allways.cli.swap_commands.swap._poll_reservation', return_value=resv),
        patch('allways.cli.swap_commands.swap._save_pending'),
    ):
        return CliRunner().invoke(swap_now_command, argv)


def test_btc_source_reservation_with_400s_left_is_accepted_for_send():
    """A freshly won BTC-source
    reservation, ~400s of life left. BTC needs 1200s of confirmations; the claim needs only the relay."""
    result = _run_swap_now(int(time.time()) + 400)
    assert result.exit_code == 0, result.output
    assert 'Reserved.' in result.output
    assert 'miner-addr' in result.output  # the send instruction the taker was never getting
    assert 'too short' not in result.output


def test_reservation_with_seconds_left_still_refuses_the_send():
    """The guard still bites when the claim genuinely can't land, and still says DON'T send."""
    result = _run_swap_now(int(time.time()) + 5)
    assert result.exit_code != 0
    assert 'too short' in result.output
    assert 'Do NOT send funds' in result.output


def _declared_reservation(collateral_amount):
    return types.SimpleNamespace(
        collateral_chain='tao',
        collateral_amount=collateral_amount,
        from_amount=10**9,
        to_amount=5 * 10**9,
        created_at=1_700_000_000,
    )


def _declared_provider(value):
    return types.SimpleNamespace(
        chain=types.SimpleNamespace(block_at=lambda created_at: 123),
        value_rao=lambda amount, block=None: value,
    )


def test_refuse_uncovered_allows_a_pinned_match():
    with patch(
        'allways.cli.swap_commands.swap.declared_leg_providers', return_value={'sn7': _declared_provider(10_000)}
    ):
        assert _refuse_uncovered(object(), _declared_reservation(10_100), 'sol', 'sn7') is None


def test_refuse_uncovered_exits_on_a_pinned_mismatch(capsys):
    with (
        patch(
            'allways.cli.swap_commands.swap.declared_leg_providers',
            return_value={'sn7': _declared_provider(10_000)},
        ),
        pytest.raises(SystemExit),
    ):
        _refuse_uncovered(object(), _declared_reservation(10_101), 'sol', 'sn7')
    assert 'Do NOT send funds' in capsys.readouterr().out


# ── benign crank-race handling: a lost resolve_pool must not abort `swap now` ───────────────────────
# The self-crank races the validator (and peer takers) to resolve the pool. When it loses, the tx can
# fail two ways: rejected in simulation (error carries the Anchor NAME) or landed-failed (the confirm
# path stringifies status["err"] with only the numeric `{'Custom': N}` code). Both are benign — the
# draw still happened, we keep polling for our seat. Matching names alone missed the code-only form,
# which re-raised and abandoned the taker's already-paid, since-drawn seat.


def _crank_raising(err_text):

    client = MagicMock()
    client.resolve_pool.side_effect = RuntimeError(err_text)
    _self_crank_resolve(client, 'miner-pubkey', 'sol')  # must NOT raise


def test_crank_swallows_benign_race_by_name():
    # pre-flight simulation rejection — carries the Anchor error name
    for name in ('PoolNotClosed', 'NoRequests', 'SeedSlotNotYetProduced', 'AlreadyFilled'):
        _crank_raising(f'Program log: AnchorError ... Error Code: {name}. Error Number: 60xx')


def test_crank_swallows_benign_race_by_numeric_code():
    # landed-then-failed tx — confirm() surfaces only the numeric code, no name
    for code in (6042, 6044, 6045, 6046):
        _crank_raising(f"tx 5abc failed: {{'InstructionError': [0, {{'Custom': {code}}}]}}")


def test_crank_reraises_a_real_error():
    import pytest

    client = MagicMock()
    # a non-benign failure (e.g. MinerReserved 6022) must still propagate
    client.resolve_pool.side_effect = RuntimeError("tx 5abc failed: {'InstructionError': [0, {'Custom': 6022}]}")
    with pytest.raises(RuntimeError):
        _self_crank_resolve(client, 'miner-pubkey', 'sol')


# ── pool-contention notice: best-effort, a bad read must never block or crash a swap ──


def test_pool_contention_never_raises_on_a_bad_read():
    from allways.cli.swap_commands.swap import _pool_contention

    client = MagicMock()
    client.get_pool.side_effect = RuntimeError('rpc down')  # must degrade to "not open", never crash a bid
    assert _pool_contention(client, 'miner', types.SimpleNamespace(validators=[]), 'sol').is_open is False


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
    # The real dataclass, not a look-alike: a fake that drifts from MinerCandidate stops testing
    # the wiring it is supposed to lock.
    cand = MinerCandidate(miner='miner-pk', rate_display='0.0021', collateral=10**10)
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
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=None),
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
        reserved_until=0,
        created_at=0,
        finalize_by=now + 120,
        router=user,
        rate=int(0.0021 * 10**18),
        miner_from_addr='miner-addr',
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


def test_can_send_from_gates_on_signer(monkeypatch):
    """The auto-send safety gate: a provider must confirm it controls the pinned sender before any
    funds move (prevents the wrong-key deposit the validator would reject)."""
    from types import SimpleNamespace

    from allways.assets.sol import SolanaChain

    kp = SimpleNamespace(pubkey=lambda: 'PINNED')
    p = SolanaChain.__new__(SolanaChain)
    p.keypair = kp
    assert p.can_send_from('PINNED') is True
    assert p.can_send_from('SOMEONE_ELSE') is False
    p.keypair = None
    assert p.can_send_from('PINNED') is False


def _run_watch(client, verdict):
    """Drive _watch_swap over a live-once-then-closed swap with a stubbed closing verdict."""
    from unittest.mock import patch

    from allways.cli.swap_commands import swap as swap_mod

    out = []
    with (
        patch('allways.cli.swap_commands.swap.time.sleep'),
        patch('allways.cli.swap_commands.swap._swap_verdict', lambda *_a: verdict),
        patch.object(swap_mod.console, 'print', lambda *a, **k: out.append(' '.join(str(x) for x in a))),
    ):
        swap_mod._watch_swap(client, 'ab' * 32, 'tao')
    return ' '.join(out)


def _live_once_client():
    from unittest.mock import MagicMock

    active = types.SimpleNamespace(status=type('Active', (), {})())
    client = MagicMock()
    client.get_swap.side_effect = [active, None]  # live once, then closed
    return client


def test_watch_swap_reports_timeout_as_failure_not_delivery(monkeypatch):
    """A close whose verdict is SwapTimedOut must NOT read as 'delivered' — the real failure test
    printed '✓ COMPLETED' for a slashed miner. It names the reimbursement, its chain, and the payee."""
    ev = types.SimpleNamespace(collateral_chain='tao', reimbursement=198000000, payee='5Fpayee')
    joined = _run_watch(_live_once_client(), ('SwapTimedOut', ev))
    assert 'MINER FAILED' in joined and 'COMPLETED' not in joined
    assert '0.198 TAO' in joined and '5Fpayee' in joined
    assert 'being reimbursed' in joined  # tao pays via the vault relay — not yet moved at close


def test_watch_swap_close_with_unreadable_verdict_stays_neutral(monkeypatch):
    """RPC hiccup / reaped claim → no verdict. Say the swap closed and point at `alw view swap`;
    never claim delivery that can't be shown."""
    joined = _run_watch(_live_once_client(), None)
    assert 'closed on-chain' in joined and 'COMPLETED' not in joined and 'MINER FAILED' not in joined


def test_watch_swap_ends_on_a_close_it_never_saw_live():
    """The relay can wait out a dead validator's axon for its full timeout, and a fast swap settles
    inside that wait: the account is already closed on the first read. Its SwapCompleted event must
    end the watch at once, not after 900 s of polling a swap that finished."""
    from unittest.mock import MagicMock

    client = MagicMock()
    client.get_swap.return_value = None
    joined = _run_watch(client, ('SwapCompleted', types.SimpleNamespace()))
    assert 'COMPLETED' in joined
    assert client.get_swap.call_count == 1


def test_watch_swap_keeps_polling_a_swap_with_no_account_and_no_verdict():
    """No account and no closing event yet (not created, or the verdict is unreadable): keep
    watching rather than claim anything."""
    from unittest.mock import MagicMock, patch

    from allways.cli.swap_commands import swap as swap_mod

    client = MagicMock()
    client.get_swap.side_effect = [None, None, KeyboardInterrupt]
    out = []
    with (
        patch('allways.cli.swap_commands.swap.time.sleep'),
        patch('allways.cli.swap_commands.swap._swap_verdict', lambda *_a: None),
        patch.object(swap_mod.console, 'print', lambda *a, **k: out.append(' '.join(str(x) for x in a))),
    ):
        swap_mod._watch_swap(client, 'ab' * 32, 'tao')
    joined = ' '.join(out)
    assert client.get_swap.call_count == 3
    assert 'COMPLETED' not in joined and 'closed on-chain' not in joined
    assert 'Stopped watching' in joined


# ── manual-path deadline notice ─────────────────────────────────────────────
# _SEND_MARGIN_SECS is checked once, at reservation time. On the manual path the send and the relay
# happen outside this process, so nothing re-checks it — and a deposit landing after reserved_until
# yields no claim, no Swap, no timeout and no refund. The notice is the only thing standing between
# an out-of-band sender and a silent, permanent loss.
def test_deadline_notice_states_the_wall_clock_instant():
    now = 1_700_000_000
    lines = _deadline_lines(now + 300, want_send=False, now=now)
    body = ' '.join(lines)
    assert '300s' in body  # runway from the caller's `now`, not from import time
    assert 'reserved_until=1700000300' in body  # raw, so an agent can parse it
    assert time.strftime('%H:%M:%S UTC', time.gmtime(now + 300)) in body
    assert 'no refund' in body


# ─── Pre-reserve deliverability screens (the CLI mirror of the validator gate) ───


class _Gate:
    def __init__(self, reject=(), malformed=(), enabled=True, blocker=None):
        self.enabled = enabled
        self.blocker = blocker
        self.reject = set(reject)
        self.checked = []
        self.chain = types.SimpleNamespace(
            is_valid_address=lambda addr: addr not in set(malformed), normalize_address=lambda addr: addr
        )

    def transfers_enabled(self):
        return self.enabled

    def send_blocker(self, from_address, to_address, amount):
        return self.blocker

    def can_deliver_to(self, addr, amount, from_address=None):
        self.checked.append(addr)
        return addr not in self.reject


def _screen(gate, from_chain, to_chain, client=None):
    from allways.cli.swap_commands.swap import _screen_deliverability

    cand = types.SimpleNamespace(miner='miner-pk', rate_display='150', backing='sol')
    with patch('allways.cli.swap_commands.swap.gate_provider', return_value=gate):
        _screen_deliverability(
            client or MagicMock(), {}, cand, from_chain, to_chain, 'recvaddr', 'useraddr', 10**6, lambda: MagicMock()
        )
    return gate


def test_screen_refuses_a_pair_whose_transfers_are_switched_off():
    import pytest

    with pytest.raises(SystemExit):
        _screen(_Gate(enabled=False), 'sol', 'sn12')


def test_screen_refuses_a_source_that_cannot_go_out_as_one_transfer():
    import pytest

    client = MagicMock()
    client.get_quote.return_value = types.SimpleNamespace(miner_from_addr='mineraddr')
    with pytest.raises(SystemExit):
        _screen(_Gate(blocker='split across hotkeys'), 'sn7', 'sol', client)


def test_screen_blocks_rejecting_miner_receive_address():
    # Don't bid (and burn the entry fee) toward a miner whose receive address rejects.
    import pytest

    client = MagicMock()
    client.get_quote.return_value = types.SimpleNamespace(miner_from_addr='mineraddr')
    with pytest.raises(SystemExit):
        _screen(_Gate(reject={'mineraddr'}), 'arbusdc', 'sol', client)


def test_screen_blocks_malformed_receive_address():
    # Format is screened before any RPC-backed gate — offline and always authoritative.
    import pytest

    with pytest.raises(SystemExit):
        _screen(_Gate(malformed={'recvaddr'}), 'sol', 'arbusdc')


def test_screen_blocks_malformed_miner_receive_address():
    import pytest

    client = MagicMock()
    client.get_quote.return_value = types.SimpleNamespace(miner_from_addr='mineraddr')
    with pytest.raises(SystemExit):
        _screen(_Gate(malformed={'mineraddr'}), 'arbusdc', 'sol', client)


def test_screen_rejection_aborts_swap_now_before_any_bid():
    # Placement guard: the SOURCE-side screen (which stays — it protects takers) fires inside
    # `swap now` BEFORE the resume/bid machinery, so a miner whose receive address rejects the source
    # aborts the flow with no bid placed. (The dest-side deliverability screen was removed — a refusing
    # DEST is no longer probed here.)
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60
    )
    client.get_quote.return_value = types.SimpleNamespace(miner_from_addr='minerSOLaddr', miner_to_addr='')
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10, backing='sol')
    argv = ['--from', 'sol', '--to', 'btc', '--amount', '0.001', '--receive-address', 'userBTCaddr', '--yes']
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=_Gate(reject={'minerSOLaddr'})) as gate,
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]) as candidates,
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap._save_pending'),
    ):
        result = CliRunner().invoke(swap_now_command, argv)
    assert result.exit_code != 0
    assert 'cannot accept' in result.output
    # Candidates are drawn through both legs' gates, so a quote with a malformed address is never offered.
    assert candidates.call_args.args[3] == {'sol': gate.return_value, 'btc': gate.return_value}
    client.open_or_request.assert_not_called()
    client.get_reservation.assert_not_called()


def test_send_with_uncontrolled_source_aborts_before_any_bid():
    """Regression: `--send` with a source address the configured wallet can't sign for must abort
    BEFORE the bid. The old order spent the reservation fee, finalized, and only then (inside
    _auto_send_wizard) noticed can_send_from was false — stranding the taker on the manual
    deadline path with real money committed."""
    from click.testing import CliRunner

    from allways.cli.swap_commands.swap import swap_now_command

    user = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
    client = MagicMock()
    client.keypair.pubkey.return_value = user
    bad_provider = MagicMock()
    bad_provider.can_send_from.return_value = False

    argv = ['--from', 'tao', '--to', 'aster', '--amount', '0.15']
    argv += ['--from-address', 'tao-source-we-cannot-sign-for', '--receive-address', '0xUs']
    argv += ['--yes', '--send']

    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=None),
        patch('allways.cli.swap_commands.swap._source_provider', return_value=bad_provider),
    ):
        result = CliRunner().invoke(swap_now_command, argv)

    assert result.exit_code != 0
    assert 'cannot send TAO from' in result.output
    assert 'No bid was placed' in result.output
    client.open_or_request.assert_not_called()  # the money-touching call never happened
    client.get_config.assert_not_called()  # aborted before even reading chain config


def test_alpha_source_send_signs_with_the_bittensor_coldkey(capsys):
    """An sn<N> source settles in TAO, so `--send` hands the Alpha provider the same wallet + subtensor
    as a TAO source (the pre-bid check passes for the coldkey) and names that coldkey on the confirm line."""
    wallet = MagicMock()
    wallet.coldkeypub.ss58_address = '5Coldkey'
    with patch('allways.cli.swap_commands.swap.get_cli_context', return_value=({}, wallet, MagicMock(), None)):
        provider = _source_provider('sn19', MagicMock(), {})
    assert provider.wallet is wallet and provider.can_send_from('5Coldkey')
    with patch('allways.cli.swap_commands.swap.get_cli_context', side_effect=TimeoutError('down')):
        assert _source_provider('sn19', MagicMock(), {}) is None  # unreachable subtensor → manual flow, no traceback

    resv = types.SimpleNamespace(from_addr='5Other', from_amount=10**9, miner_from_addr='5Miner')
    with patch('allways.cli.swap_commands.swap._source_provider', return_value=provider):
        assert _auto_send_wizard(MagicMock(), {'wallet': 'w'}, resv, 'pk', 'sn19', 'sol', 1.0, True, None) is False
    assert 'Bittensor coldkey (w)' in capsys.readouterr().out


def test_alpha_source_is_told_to_send_one_plain_exact_transfer_stake():
    """Only a top-level exact transfer_stake is credited. A btcli-shielded send is too (the block author
    includes the decrypted transfer_stake itself): the user posts btcli's `<block>-<idx>` or the SDK's inner hash."""
    (line,) = _alpha_send_lines('sn7')
    assert 'plain transfer_stake for the exact amount' in line
    assert '<block>-<idx>' in line and "data['signed_extrinsic_hash']" in line
    assert _alpha_send_lines('tao') == [] and _alpha_send_lines('btc') == []
