"""D6 — `alw view config` / `alw view validators` render the on-chain Config (mocked, no chain)."""

import json
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


def test_view_config_renders_the_tao_hub_bounds(monkeypatch):
    """Full TAO capacity needs 1.1 × tao_max_swap — an operator can't size a bond off
    bounds the config view hides, so every TAO field must render."""
    client = MagicMock()
    client.get_config.return_value = _config()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    for label in ('TAO min collateral:', 'TAO min swap:', 'TAO max swap:', 'Settlement grace:', 'Attest max age:'):
        assert label in result.output
    assert '0.2500' in result.output  # tao_min_collateral in TAO
    assert '1.0000' in result.output  # tao_max_swap in TAO


def test_view_config_json_carries_the_tao_hub_bounds(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config', '--json'])

    assert result.exit_code == 0, result.output
    import json as _json

    payload = _json.loads(result.output)
    assert payload['tao_min_collateral_tao'] == 0.25
    assert payload['tao_min_swap_amount_tao'] == 0.1
    assert payload['tao_max_swap_amount_tao'] == 1.0
    assert payload['settlement_grace_secs'] == 900
    assert payload['attest_max_age_secs'] == 86_400


def test_votes_needed_mirrors_contract_headcount_math():
    """consensus.rs: votes*100 >= threshold*total. Note 67% of 3 needs ALL 3 (2/3 = 66.7% < 67%)."""
    one = types.SimpleNamespace(key=b'', weight=1)
    assert view.votes_needed(_config(consensus_threshold_percent=67, validators=[one])) == 1
    assert view.votes_needed(_config(consensus_threshold_percent=67, validators=[one] * 3)) == 3
    assert view.votes_needed(_config(consensus_threshold_percent=66, validators=[one] * 3)) == 2
    assert view.votes_needed(_config(consensus_threshold_percent=51, validators=[one] * 4)) == 3


def test_view_config_shows_effective_votes(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(
        consensus_threshold_percent=67, validators=[types.SimpleNamespace(key=b'', weight=1)]
    )
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    assert '67% (1 of 1 validator votes)' in ' '.join(result.output.split())


def test_view_config_reports_uninitialized(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = None
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['config'])

    assert result.exit_code == 0, result.output
    assert 'not initialized' in result.output


def test_view_validators_lists_pubkeys_and_weights(monkeypatch):
    v = Keypair().pubkey()
    vinfo = types.SimpleNamespace(key=bytes(v), weight=3)
    client = MagicMock()
    client.get_config.return_value = _config(validators=[vinfo], consensus_threshold_percent=67)
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['validators'])

    assert result.exit_code == 0, result.output
    assert str(v) in result.output
    assert 'weight=3' in result.output
    assert '67%' in result.output


def test_view_validators_handles_empty_set(monkeypatch):
    client = MagicMock()
    client.get_config.return_value = _config(validators=[])
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['validators'])

    assert result.exit_code == 0, result.output
    assert 'No validators registered' in result.output


def test_view_swap_closed_is_informative_not_error(monkeypatch):
    client = MagicMock()
    client.get_swap.return_value = None
    _patch_client(monkeypatch, client)
    key = 'ab' * 32

    text = CliRunner().invoke(view.view_group, ['swap', key])
    assert text.exit_code == 0, text.output
    assert 'finished or never existed' in text.output

    js = CliRunner().invoke(view.view_group, ['swap', key, '--json'])
    assert js.exit_code == 0, js.output
    assert '"found": false' in js.output


def test_view_swap_rejects_wrong_length_key(monkeypatch):
    client = MagicMock()
    _patch_client(monkeypatch, client)

    result = CliRunner().invoke(view.view_group, ['swap', '1234'])

    assert result.exit_code == 1, result.output
    assert '32 bytes' in result.output
    client.get_swap.assert_not_called()


def test_view_reservation_scans_the_tao_hub(monkeypatch):
    """V-4: `view reservation` must scan every per-hub slot — a TAO-only seat used to render as none
    because the read defaulted to the SOL slot."""
    miner = Keypair().pubkey()
    resv = types.SimpleNamespace(
        user=Keypair().pubkey(),
        from_chain='tao',
        to_chain='btc',
        from_amount=2_000_000_000,
        to_amount=100_000,
        collateral_amount=0,
        reserved_until=9_999_999_999,
        finalize_by=0,
        claimed_swap_key=bytes(view.ZERO_SWAP_KEY),
        miner_from_addr='miner-tao-addr',
    )

    class HubClient:
        def get_reservation(self, m, backing='sol'):
            return resv if backing == 'tao' else None

    monkeypatch.setattr(view, 'get_solana_cli_context', lambda need_keypair=True: ({}, HubClient()))

    result = CliRunner().invoke(view.view_group, ['reservation', '--miner', str(miner)])

    assert result.exit_code == 0, result.output
    assert 'TAO → BTC' in result.output  # the TAO-hub seat is found, not "No active reservation"


def test_miner_runtime_status_reads_per_hub_when_backing_given():
    """V-4: a SOL-only swap must not paint a dual-purse miner's free TAO purse busy."""
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


def test_view_rates_shows_the_quotes_backing_purse(monkeypatch):
    miner = Keypair().pubkey()
    state = types.SimpleNamespace(active=True, collateral=2_700_000_000, has_active_swap=False, busy_until=0)
    quote = types.SimpleNamespace(
        miner=miner,
        from_chain='tao',
        to_chain='btc',
        rate=10**18,
        collateral_chain='tao',
        miner_from_addr='5miner',
        miner_to_addr='bc1miner',
    )
    entry = types.SimpleNamespace(
        miner=miner,
        pubkey_str=str(miner),
        collateral=state.collateral,
        state=state,
        reservation=None,
        quotes=[quote],
    )
    client = MagicMock()
    client.get_bond_attestation.return_value = types.SimpleNamespace(locked=True, effective_balance=1_000_000_000)
    _patch_client(monkeypatch, client)
    monkeypatch.setattr(view, 'load_miner_book', lambda client, with_reservation=True: [entry])

    result = CliRunner().invoke(view.view_group, ['rates'])

    assert result.exit_code == 0, result.output
    assert '1.0000 TAO' in result.output
    assert '2.7000 SOL' not in result.output

    payload = json.loads(CliRunner().invoke(view.view_group, ['rates', '--json']).output)[0]
    assert payload['collateral_tao'] == 1.0
    assert 'collateral_sol' not in payload


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


def test_view_swap_names_both_legs_end_to_end(monkeypatch):
    """The payout address (`user_to_addr`) is the one address a taker must check. It used to be
    absent, while `miner_to_addr` — a sender — was labelled "Miner to"."""
    client = MagicMock()
    client.get_swap.return_value = object()
    _patch_client(monkeypatch, client)
    monkeypatch.setattr(view, 'swap_from_solana', lambda acct, key: _swap())

    result = CliRunner().invoke(view.view_group, ['swap', 'cd' * 32])

    assert result.exit_code == 0, result.output
    assert 'USERSOL → MINERSOL' in result.output
    assert '0xMINERPOL → 0xUSERPOL' in result.output
