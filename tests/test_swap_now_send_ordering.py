"""Regression: `alw swap now --send` must never strand a deposit on a transient chain error.

Two invariants, both learned from a live BTC->SOL loss:

1. Validator resolution (the subtensor websocket connect + metagraph read) happens BEFORE the source
   send. If it fails, no funds move — the send is the last irreversible step, so a transient connect
   error aborts cleanly with the reservation still live.
2. Once funds ARE out, no exception may escape as a traceback. A relay that raises (e.g. a connect
   timeout the inner burst-retry doesn't cover) must become a recoverable "re-run post-tx" exit, so a
   supervisor can re-relay the idempotent deposit inside the reservation TTL.
"""

import types
from unittest.mock import MagicMock, patch

import pytest

from allways.cli.swap_commands import swap as swap_mod


def _resv():
    return types.SimpleNamespace(
        from_addr='btc-sender',
        miner_from_addr='miner-btc-addr',
        from_amount=145996,
        to_amount=10**9,
        reserved_until=2**31,
        user='user-pk',
        user_to_addr='sol-recv',
        from_chain='btc',
        to_chain='sol',
    )


def _provider(tx_hash='deadbeef'):
    p = MagicMock()
    p.can_send_from.return_value = True
    p.send_amount.return_value = [tx_hash]
    p.last_send_error = None
    return p


def _wizard(provider):
    """Drive _auto_send_wizard with everything but the code-under-test mocked. skip_confirm=True
    bypasses the interactive send prompt; from/to are btc->sol (the stranding case)."""
    return swap_mod._auto_send_wizard(MagicMock(), {}, _resv(), 'miner-pk', 'btc', 'sol', 0.00145996, True, None)


def test_chain_unreachable_during_validator_resolve_moves_no_funds():
    provider = _provider()
    with (
        patch.object(swap_mod, '_source_provider', return_value=provider),
        patch.object(swap_mod, 'get_cli_context', side_effect=TimeoutError('timed out')),
        patch.object(swap_mod, 'discover_validators') as disc,
    ):
        assert _wizard(provider) is False  # clean fallback, not a crash
    provider.send_amount.assert_not_called()  # THE invariant: funds untouched
    disc.assert_not_called()


def test_relay_error_after_send_is_recoverable_not_a_traceback():
    provider = _provider(tx_hash='abc123')
    with (
        patch.object(swap_mod, '_source_provider', return_value=provider),
        patch.object(swap_mod, 'get_cli_context', return_value=({'netuid': 1}, None, MagicMock(), None)),
        patch.object(swap_mod, 'discover_validators', return_value=['axon']),
        patch.object(swap_mod, '_miner_hotkey', return_value='miner-hotkey'),
        patch('allways.cli.swap_commands.post_tx.relay_deposit', side_effect=TimeoutError('timed out')),
        pytest.raises(SystemExit),
    ):
        # send_amount succeeds, then the relay raises: must convert to a `fail()` SystemExit, never
        # let TimeoutError propagate.
        _wizard(provider)
    provider.send_amount.assert_called_once()  # money did move — so the recoverable exit is required


def test_happy_path_passes_preresolved_axons_into_relay():
    provider = _provider(tx_hash='ok999')
    with (
        patch.object(swap_mod, '_source_provider', return_value=provider),
        patch.object(swap_mod, 'get_cli_context', return_value=({'netuid': 1}, None, MagicMock(), None)),
        patch.object(swap_mod, 'discover_validators', return_value=['axon-a', 'axon-b']),
        patch.object(swap_mod, '_miner_hotkey', return_value='miner-hotkey'),
        patch.object(swap_mod, '_watch_swap'),
        patch('allways.cli.swap_commands.post_tx.relay_deposit', return_value='swapkeyhex') as relay,
    ):
        assert _wizard(provider) is True
    # Validators resolved once, pre-send, and handed to relay_deposit so it does NOT reconnect after
    # the money is out.
    assert relay.call_args.kwargs['validator_axons'] == ['axon-a', 'axon-b']
