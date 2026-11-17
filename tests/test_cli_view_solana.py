"""`alw view`: config smoke, the consensus headcount math, per-hub runtime status and the net payout shown."""

import types
from unittest.mock import MagicMock

from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands import view


def _config(**over):
    fields = dict(
        admin=Keypair().pubkey(),
        version=1,
        consensus_threshold_percent=51,
        fulfillment_timeout_secs=600,
        reservation_ttl_secs=600,
        min_collateral=2_000_000_000,
        max_collateral=0,
        min_swap_amount=0,
        max_swap_amount=0,
        tao_min_swap_amount=100_000_000,
        tao_max_swap_amount=1_000_000_000,
        tao_min_collateral=250_000_000,
        settlement_grace_secs=900,
        attest_max_age_secs=86_400,
        halted=False,
        reservation_fee_lamports=1_000_000,
        pool_window_secs=60,
        weights_update_min_interval_secs=1200,
        max_total_extension_secs=3600,
        validators=[],
    )
    fields.update(over)
    return types.SimpleNamespace(**fields)


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(view, 'get_solana_cli_context', lambda need_keypair=True: ({}, client))


def test_view_config_renders_every_field(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    for label in ('Halted:', 'Consensus threshold:', 'Reservation fee:', 'Pool window:', 'Max total extension:'):
        assert label in result.output
    assert '51%' in result.output
    assert '0.001000 SOL' in result.output  # reservation fee in SOL
    assert 'On-chain Program Config' in result.output
    assert 'alw config' in result.output  # cross-link to the local CLI settings


def test_votes_needed_mirrors_contract_headcount_math():
    """consensus.rs: votes*100 >= threshold*total. Note 67% of 3 needs ALL 3 (2/3 = 66.7% < 67%)."""
    one = types.SimpleNamespace(key=b'', weight=1)
    assert view.votes_needed(_config(consensus_threshold_percent=67, validators=[one])) == 1
    assert view.votes_needed(_config(consensus_threshold_percent=67, validators=[one] * 3)) == 3
    assert view.votes_needed(_config(consensus_threshold_percent=66, validators=[one] * 3)) == 2
    assert view.votes_needed(_config(consensus_threshold_percent=51, validators=[one] * 4)) == 3


def test_miner_runtime_status_reads_per_hub_when_backing_given():
    """A SOL-only swap must not paint a dual-purse miner's free TAO purse busy."""
    from allways.cli.swap_commands.helpers import miner_runtime_status
    from allways.solana.pdas import BACKING_BIT_SOL, BACKING_BIT_TAO

    state = types.SimpleNamespace(
        active=True,
        active_backings=BACKING_BIT_SOL | BACKING_BIT_TAO,
        has_active_swap=True,
        active_swap_backings=BACKING_BIT_SOL,
        busy_until=[0, 0],
    )
    assert miner_runtime_status(state, None, 1000) == 'in-swap'  # OR view
    assert miner_runtime_status(state, None, 1000, backing='sol') == 'in-swap'
    assert miner_runtime_status(state, None, 1000, backing='tao') == 'available'


def _swap(**over):
    fields = dict(
        key_hex='cd' * 32,
        status='Fulfilled',
        from_chain='sol',
        to_chain='pol',
        miner=Keypair().pubkey(),
        user=Keypair().pubkey(),
        from_amount=100_000_000,
        to_amount=50 * 10**18,
        user_from_addr='USERSOL',
        user_to_addr='0xUSERPOL',
        miner_from_addr='MINERSOL',
        miner_to_addr='0xMINERPOL',
        from_tx_hash='srctx',
        to_tx_hash='dsttx',
        initiated_at=1,
        timeout_at=2,
        fulfilled_at=3,
    )
    fields.update(over)
    return types.SimpleNamespace(**fields)


def test_view_swap_shows_the_amount_that_actually_lands(monkeypatch):
    """`to_amount` is the gross pinned payout; the miner sends and the validator verifies it net of
    the 1% fee. Rendering the gross told the user they receive 1% more than they ever can."""
    client = MagicMock()
    client.get_swap.return_value = object()
    _patch_client(monkeypatch, client)
    monkeypatch.setattr(view, 'swap_from_solana', lambda acct, key: _swap())

    result = CliRunner().invoke(view.view_group, ['swap', 'cd' * 32])

    assert result.exit_code == 0, result.output
    assert '49.5 POL' in result.output
    assert '50 POL' not in result.output
