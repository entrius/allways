"""`alw swap now` validator-routed mode: dendrite reserve → wait for the router's finalize.

Drives the command with a stubbed chain/client (same harness as test_swap_now_reservation) and a
patched dendrite layer. Covers the routed happy path, lost/unresolved outcomes, the declined native
fallback, send-safety messaging, and the mainnet router default.
"""

import time
import types
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from allways.cli.swap_commands.swap import swap_now_command
from allways.cli.swap_commands.swap_intake import MinerCandidate

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
    # The real dataclass, not a look-alike: a fake that drifts from MinerCandidate stops testing
    # the wiring it is supposed to lock.
    cand = MinerCandidate(miner='miner-pk', rate_display='0.0021', collateral=10**10)
    argv = ['--from', 'btc', '--to', 'sol', '--amount', '0.00005', '--from-address', 'tb1qsource']
    argv += ['--receive-address', USER, *argv_extra]
    info = types.SimpleNamespace(headline='router unreachable', accepted=0)
    with (
        patch('allways.cli.swap_commands.swap.get_solana_cli_context', return_value=(config, client)),
        patch('allways.cli.swap_commands.swap.gate_provider', return_value=None),
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


def test_env_bundles_router_defaults_and_config_key_registered():
    from allways.cli.main import VALID_CONFIG_KEYS
    from allways.cli.swap_commands.helpers import ENV_BUNDLES

    assert 'router' in VALID_CONFIG_KEYS
    # Mainnet has no routing validator yet — self-represent by default (explicit '' clears any stale
    # router on re-run). Testnet routes through the Ventura Labs validator used in QA.
    assert ENV_BUNDLES['mainnet']['router'] == ''
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
