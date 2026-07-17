"""`alw swap now` validator-routed mode: dendrite reserve → wait for the router's finalize.

Drives the command with a stubbed chain/client (same harness as test_swap_now_reservation) and a
patched dendrite layer. Covers mode selection, the routed happy path, lost/unresolved outcomes,
the confirm-or-abort native fallback, send-safety messaging, and the axon disk cache.
"""

import time
import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import swap_now_command

USER = '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF'
ROUTER = '5DtUJ9ytbeCMjovFieNwaxxqRP3DzT6iQPnZTyKmi3n6iXey'
EMPTY = bytes(32)


def _live_resv(user=USER):
    return types.SimpleNamespace(
        reserved_until=int(time.time()) + 400,
        claimed_swap_key=EMPTY,
        user=user,
        miner_from_addr='miner-addr',
        to_amount=10**9,
    )


def _client(reservations):
    client = MagicMock()
    client.keypair.pubkey.return_value = USER
    client.get_config.return_value = types.SimpleNamespace(
        min_swap_amount=1, max_swap_amount=10**18, pool_window_secs=60, finalize_window_secs=150
    )
    client.get_reservation.side_effect = reservations
    client.get_binding.return_value = types.SimpleNamespace(hotkey=b'\x11' * 32)
    return client


def _accepted(pool_closes_at=None):
    return types.SimpleNamespace(
        accepted=True, rejection_reason=None, pool_closes_at=pool_closes_at or int(time.time()) + 5
    )


def _flat(result) -> str:
    return ' '.join(result.output.split())


AXON = types.SimpleNamespace(ip='1.2.3.4', port=8091, ip_type=4, version=0, coldkey='', is_serving=True)


def _run(client, *, argv_extra=(), responses=None, axon=AXON, config=None, confirm_input=None):
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10)
    argv = ['--from', 'btc', '--to', 'sol', '--amount', '0.00005', '--from-address', 'tb1qsource']
    argv += ['--receive-address', USER, *argv_extra]
    info = types.SimpleNamespace(headline='router unreachable', accepted=0)
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(config, client)),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap.find_validator_axon', return_value=axon) as find_axon,
        patch('allways.cli.swap_commands.swap.get_ephemeral_wallet'),
        patch('allways.cli.swap_commands.swap.broadcast_synapse', return_value=responses or []) as broadcast,
        patch('allways.cli.swap_commands.swap.render_and_aggregate', return_value=info),
        patch(
            'allways.cli.swap_commands.swap.get_cli_context', return_value=({'netuid': '7'}, None, MagicMock(), None)
        ),
        patch('allways.cli.swap_commands.swap._save_pending') as save_pending,
        patch('allways.cli.swap_commands.swap.time.sleep'),
    ):
        result = CliRunner().invoke(swap_now_command, argv, input=confirm_input)
    return result, broadcast, save_pending, find_axon


def test_routed_happy_path_waits_for_router_finalize():
    client = _client([None, _live_resv()])  # no resume seat, then the router's finalize goes live
    r, broadcast, save_pending, _ = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted()])
    assert r.exit_code == 0, r.output
    synapse = broadcast.call_args.args[2]
    assert (synapse.user_pubkey, synapse.from_chain, synapse.to_chain) == (USER, 'btc', 'sol')
    assert 'Do NOT send any funds yet' in _flat(r)
    assert save_pending.called
    # never self-cranks or finalizes in routed mode
    assert not client.open_or_request.called and not client.finalize_reservation.called


def test_routed_never_prints_deposit_address_before_live():
    """Send-safety: the waiting stage must not reveal where to send."""
    client = _client([None, _live_resv()])
    r, *_ = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted()])
    out = _flat(r)
    warn_at = out.index('Do NOT send any funds yet')
    addr_at = out.index('miner-addr')
    assert warn_at < addr_at


def test_routed_lost_to_other_user_fails_with_rerun_hint():
    client = _client([None, _live_resv(user='SomeoneElse')])
    r, _, save_pending, _ = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted()])
    assert r.exit_code != 0
    out = _flat(r)
    assert 'lost this round' in out and 're-quotes fresh' in out
    assert not save_pending.called


def test_routed_unresolved_deadline_fails_safely():
    client = _client(lambda *_a: None)  # reservation never appears
    client.get_reservation.side_effect = None
    client.get_reservation.return_value = None
    # Advance a fake clock 30s per read so the ~240s poll deadline elapses in a few iterations
    # (a real clock would spin the no-op-sleep loop for minutes and bloat mock call history).
    base = time.time()
    ticks = iter(range(0, 100_000, 30))
    with patch('allways.cli.swap_commands.swap.time.time', side_effect=lambda: base + next(ticks)):
        r, _, save_pending, _ = _run(
            client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted(pool_closes_at=int(base) + 5)]
        )
    assert r.exit_code != 0
    assert 'No funds moved' in _flat(r) and not save_pending.called


def test_routed_failure_fallback_declined_aborts():
    client = _client([None])
    r, _, save_pending, _ = _run(
        client, argv_extra=['--router', ROUTER], axon=None, confirm_input='y\nn\n'
    )  # confirm the ask prompt, decline the fallback
    assert r.exit_code != 0
    assert 'Routing failed' in _flat(r) and 'no funds moved' in _flat(r)
    assert not client.open_or_request.called and not save_pending.called


def test_routed_failure_with_yes_falls_back_to_self_represented():
    drawn = types.SimpleNamespace(
        router=USER, reserved_until=0, finalize_by=int(time.time()) + 60, rate=int(0.0021 * 10**18)
    )
    client = _client([None])
    with (
        patch('allways.cli.swap_commands.swap._poll_drawn', return_value=drawn),
        patch('allways.cli.swap_commands.swap._poll_reservation', return_value=_live_resv()),
    ):
        r, _, save_pending, _ = _run(client, argv_extra=['--router', ROUTER, '--yes'], axon=None)
    assert r.exit_code == 0, r.output
    assert 'Routing failed' in _flat(r)
    assert client.open_or_request.called and client.finalize_reservation.called  # native path ran
    assert save_pending.called


def test_no_router_forces_self_represented_despite_config():
    drawn = types.SimpleNamespace(
        router=USER, reserved_until=0, finalize_by=int(time.time()) + 60, rate=int(0.0021 * 10**18)
    )
    client = _client([None])
    with (
        patch('allways.cli.swap_commands.swap._poll_drawn', return_value=drawn),
        patch('allways.cli.swap_commands.swap._poll_reservation', return_value=_live_resv()),
    ):
        r, broadcast, _, find_axon = _run(client, argv_extra=['--no-router', '--yes'], config={'router': ROUTER})
    assert r.exit_code == 0, r.output
    assert not broadcast.called and not find_axon.called
    assert client.open_or_request.called


def test_router_flag_overrides_config():
    client = _client([None, _live_resv()])
    other = '5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao'
    r, _, _, find_axon = _run(
        client, argv_extra=['--router', other, '--yes'], responses=[_accepted()], config={'router': ROUTER}
    )
    assert r.exit_code == 0, r.output
    assert find_axon.call_args.args[2] == other


def test_rejection_reason_surfaces():
    client = _client([None])
    rejected = types.SimpleNamespace(accepted=False, rejection_reason='miner is not active', pool_closes_at=0)
    r, *_ = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[rejected], confirm_input=None)
    # --yes falls back; native path unpatched -> open_or_request MagicMock succeeds then draw fails,
    # but the routed failure reason must have been shown first.
    assert 'miner is not active' in _flat(r)


def test_env_bundles_carry_routers_and_config_key_registered():
    from allways.cli.main import VALID_CONFIG_KEYS
    from allways.cli.swap_commands.helpers import ENV_BUNDLES

    assert 'router' in VALID_CONFIG_KEYS
    assert ENV_BUNDLES['mainnet']['router'] == ROUTER
    assert ENV_BUNDLES['testnet']['router'] == '5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao'


def test_pinned_pool_rate_drives_preview():
    """An open pool for the same pair previews the PINNED rate, not the drifted live quote."""
    pool = types.SimpleNamespace(
        opened_at=1,
        closes_at=int(time.time()) + 30,
        requests=[],
        from_chain='btc',
        to_chain='sol',
        rate=int(500.0 * 10**18),  # pinned; live quote says 0.0021
    )
    client = _client([None, _live_resv()])
    client.get_pool.return_value = pool
    r, *_ = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted()])
    assert 'pinned pool rate' in _flat(r)


# ─── axon disk cache ─────────────────────────────────────────────────────────


def test_axon_cache_hit_skips_metagraph(tmp_path):
    from allways.cli import dendrite_lite as dl

    with patch.object(dl, 'AXON_CACHE_FILE', tmp_path / 'axon_cache.json'):
        axon = types.SimpleNamespace(ip='1.2.3.4', port=8091, ip_type=4, version=0, coldkey='', is_serving=True)
        dl._write_axon_cache(7, ROUTER, axon)
        factory = MagicMock(side_effect=AssertionError('metagraph must not be read on a cache hit'))
        got = dl.find_validator_axon(factory, 7, ROUTER)
        assert (got.ip, got.port) == ('1.2.3.4', 8091)
        assert not factory.called


def test_axon_cache_miss_reads_metagraph_and_writes_cache(tmp_path):
    from allways.cli import dendrite_lite as dl

    with patch.object(dl, 'AXON_CACHE_FILE', tmp_path / 'axon_cache.json'):
        axon = types.SimpleNamespace(ip='5.6.7.8', port=1234, ip_type=4, version=0, coldkey='', is_serving=True)
        metagraph = types.SimpleNamespace(n=1, hotkeys=[ROUTER], axons=[axon], validator_permit=[True])
        subtensor = MagicMock()
        subtensor.metagraph.return_value = metagraph
        got = dl.find_validator_axon(lambda: subtensor, 7, ROUTER)
        assert got is axon
        assert dl._read_axon_cache(7, ROUTER) is not None  # cached for next time


def test_rerun_requotes_fresh_candidates():
    """Rate freshness: every invocation re-selects from live quotes — nothing is cached between runs."""
    for _ in range(2):
        client = _client([None, _live_resv()])
        r, _, _, find_axon = _run(client, argv_extra=['--router', ROUTER, '--yes'], responses=[_accepted()])
        assert r.exit_code == 0, r.output
        assert find_axon.called  # each run resolved candidates + axon afresh (patched per-run)


def test_stale_cached_axon_refreshes_once_then_succeeds():
    """A dead cached axon triggers ONE fresh lookup + resend before any fallback."""
    stale = types.SimpleNamespace(ip='9.9.9.9', port=1, ip_type=4, version=0, coldkey='', is_serving=True)
    fresh = AXON
    rejected = types.SimpleNamespace(accepted=False, rejection_reason=None, pool_closes_at=0)
    client = _client([None, _live_resv()])
    amts = types.SimpleNamespace(collateral_amount=10**9, from_amount=5000, to_amount=10**9)
    cand = types.SimpleNamespace(miner='miner-pk', rate_display='0.0021', collateral=10**10)
    argv = ['--from', 'btc', '--to', 'sol', '--amount', '0.00005', '--from-address', 'tb1qsource']
    argv += ['--receive-address', USER, '--router', ROUTER, '--yes']
    info = types.SimpleNamespace(headline='no response', accepted=0)
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(None, client)),
        patch('allways.cli.swap_commands.swap.candidate_miners', return_value=[cand]),
        patch('allways.cli.swap_commands.swap.select_best_miner', return_value=(cand, amts)),
        patch('allways.cli.swap_commands.swap.find_validator_axon', side_effect=[stale, fresh]) as find_axon,
        patch('allways.cli.swap_commands.swap.get_ephemeral_wallet'),
        patch('allways.cli.swap_commands.swap.broadcast_synapse', side_effect=[[rejected], [_accepted()]]) as bc,
        patch('allways.cli.swap_commands.swap.render_and_aggregate', return_value=info),
        patch(
            'allways.cli.swap_commands.swap.get_cli_context', return_value=({'netuid': '7'}, None, MagicMock(), None)
        ),
        patch('allways.cli.swap_commands.swap._save_pending') as save_pending,
        patch('allways.cli.swap_commands.swap.time.sleep'),
    ):
        r = CliRunner().invoke(swap_now_command, argv)
    assert r.exit_code == 0, r.output
    assert bc.call_count == 2  # stale send, then fresh-axon resend
    assert find_axon.call_args_list[1].kwargs.get('fresh') is True  # second lookup bypassed the cache
    assert save_pending.called


def test_axon_cache_rejects_wrong_key_and_non_validator(tmp_path):
    from allways.cli import dendrite_lite as dl

    with patch.object(dl, 'AXON_CACHE_FILE', tmp_path / 'axon_cache.json'):
        metagraph = types.SimpleNamespace(
            n=1,
            hotkeys=[ROUTER],
            axons=[types.SimpleNamespace(ip='9.9.9.9', port=1, ip_type=4, version=0, coldkey='', is_serving=True)],
            validator_permit=[False],  # not a validator
        )
        subtensor = MagicMock()
        subtensor.metagraph.return_value = metagraph
        assert dl.find_validator_axon(lambda: subtensor, 7, ROUTER) is None
        assert dl.find_validator_axon(lambda: subtensor, 7, 'unknown-hotkey') is None
        metagraph.validator_permit = [True]
        metagraph.axons[0].is_serving = False  # validator but axon down
        assert dl.find_validator_axon(lambda: subtensor, 7, ROUTER) is None


def test_native_bid_contract_rejection_fails_clean(monkeypatch):
    """A program rejection on the native bid (miner busy mid-race) must fail() with the contract's
    message, never a raw traceback (QA finding C5)."""
    from unittest.mock import MagicMock

    import pytest

    from allways.cli.swap_commands import swap as swap_mod

    client = MagicMock()
    client.open_or_request.side_effect = RuntimeError(
        "sendTransaction: {'code': -32002, 'message': 'Transaction simulation failed: custom program error: "
        "0x1774', 'data': {'logs': ['Program log: AnchorError thrown. Error Code: MinerHasActiveSwap. "
        "Error Number: 6004. Error Message: Miner has an in-flight swap; cannot proceed.']}}"
    )
    with pytest.raises(SystemExit):
        swap_mod._reserve_self_represented(client, 'miner', 'user', 'src', 'dst', 'tao', 'sol', 100, 30)
    client.open_or_request.assert_called_once()
