"""post-tx / resume-reservation: recognize an initiated swap before assuming expiry (#473).

After vote_initiate the contract removes the reservation row, so
``get_miner_reserved_until`` returns 0 and ``hydrate_pending_swap`` can't
repopulate the contract-derived fields. The old flows read that state as an
expired reservation: post-tx deleted pending_swap.json and told the user to
re-reserve, and resume-reservation crashed rendering the summary
(``get_chain('')``). These tests pin the probe-based reconciliation both
commands now share.
"""

from unittest.mock import MagicMock

from click.testing import CliRunner

import allways.cli.swap_commands.post_tx as post_tx_mod
import allways.cli.swap_commands.resume as resume_mod
from allways.classes import Swap, SwapStatus
from allways.cli.swap_commands.helpers import PendingSwapState
from allways.contract_client import ContractError

CURRENT_BLOCK = 1_000_000


def make_state(**overrides) -> PendingSwapState:
    """Post-initiate shape: persisted fields only — hydration fails once the
    reservation row is gone, so contract-derived fields keep their dataclass
    defaults ('' chains, 0 amounts)."""
    base = dict(
        miner_hotkey='5MinerHk',
        receive_address='5UserHk',
        netuid=2,
        wallet_name='default',
        hotkey_name='default',
        from_tx_hash='abc123',
        request_hash='req-hash',
        created_at=0.0,
    )
    base.update(overrides)
    return PendingSwapState(**base)


def make_swap(**overrides) -> Swap:
    base = dict(
        id=7,
        user_hotkey='5UserHk',
        miner_hotkey='5MinerHk',
        from_chain='btc',
        to_chain='tao',
        from_amount=100_000,
        to_amount=300_000_000,
        tao_amount=300_000_000,
        user_from_address='tb1quser',
        user_to_address='5UserHk',
        status=SwapStatus.ACTIVE,
    )
    base.update(overrides)
    return Swap(**base)


def make_initiated_client(swap) -> MagicMock:
    """Contract-client double for the post-initiate window: reservation row
    removed (get_reservation None / reserved_until 0), our swap live."""
    client = MagicMock()
    client.get_halted.return_value = False
    client.get_miner_has_active_swap.return_value = True
    client.get_miner_active_swaps.return_value = [swap]
    client.get_reservation.return_value = None
    client.get_miner_reserved_until.return_value = 0
    client.get_reservation_data.return_value = None
    return client


def make_expired_client() -> MagicMock:
    """No swap and no reservation row — a genuinely expired reservation."""
    client = MagicMock()
    client.get_halted.return_value = False
    client.get_miner_has_active_swap.return_value = False
    client.get_miner_active_swaps.return_value = []
    client.get_reservation.return_value = None
    client.get_miner_reserved_until.return_value = 0
    client.get_reservation_data.return_value = None
    return client


def wire(monkeypatch, mod, state, client) -> list:
    """Point a command module's context/state seams at test doubles.

    Returns the list that records clear_pending_swap calls. The real
    hydrate_pending_swap and probe_pending_reservation run against the
    client double so the reconciliation path under test stays unmocked.
    """
    subtensor = MagicMock()
    subtensor.get_current_block.return_value = CURRENT_BLOCK
    cleared = []
    monkeypatch.setattr(mod, 'load_pending_swap', lambda: state)
    monkeypatch.setattr(mod, 'get_cli_context', lambda *a, **k: ({}, MagicMock(), subtensor, client))
    monkeypatch.setattr(mod, 'clear_pending_swap', lambda: cleared.append(True))
    return cleared


def test_post_tx_points_at_initiated_swap_instead_of_expiring(monkeypatch):
    """The #473 repro: re-running post-tx after the swap initiated must point
    at the live swap, not claim expiry and advise a new reservation."""
    cleared = wire(monkeypatch, post_tx_mod, make_state(), make_initiated_client(make_swap()))

    result = CliRunner().invoke(post_tx_mod.post_tx_command, ['deadbeef'])

    assert result.exception is None
    assert 'already on-chain! ID: 7' in result.output
    assert 'alw view swap 7' in result.output
    assert 'no longer active' not in result.output
    assert 'alw swap now' not in result.output
    # Local record is cleared only after handing the user the swap id.
    assert cleared == [True]


def test_post_tx_expired_reservation_still_clears_and_advises_new_swap(monkeypatch):
    cleared = wire(monkeypatch, post_tx_mod, make_state(), make_expired_client())

    result = CliRunner().invoke(post_tx_mod.post_tx_command, ['deadbeef'])

    assert result.exception is None
    assert 'no longer active' in result.output
    assert 'alw swap now' in result.output
    assert cleared == [True]


def test_post_tx_rpc_error_keeps_state_file(monkeypatch):
    client = make_expired_client()
    client.get_miner_has_active_swap.side_effect = ContractError('rpc down')
    cleared = wire(monkeypatch, post_tx_mod, make_state(), client)

    result = CliRunner().invoke(post_tx_mod.post_tx_command, ['deadbeef'])

    assert result.exception is None
    assert 'Failed to read reservation status' in result.output
    assert cleared == []


def test_resume_reports_initiated_swap_instead_of_crashing(monkeypatch):
    """resume-reservation shares the root cause: it ignored the hydrate
    failure and crashed formatting the summary with an empty chain id
    (get_chain(''))."""
    cleared = wire(monkeypatch, resume_mod, make_state(), make_initiated_client(make_swap()))

    result = CliRunner().invoke(resume_mod.resume_reservation_command, ['--yes'])

    assert result.exception is None
    assert 'Swap already on-chain! ID: 7' in result.output
    assert 'alw view swap 7' in result.output
    assert cleared == [True]


def test_resume_expired_reservation_clears_and_advises_new_swap(monkeypatch):
    cleared = wire(monkeypatch, resume_mod, make_state(), make_expired_client())

    result = CliRunner().invoke(resume_mod.resume_reservation_command, ['--yes'])

    assert result.exception is None
    assert 'no longer active' in result.output
    assert 'alw swap now' in result.output
    assert cleared == [True]
