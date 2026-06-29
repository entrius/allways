"""Tests for the FCFS reserve stub.

``handle_swap_reserve`` is the user→validator entry point: a user asks a validator to enter the on-chain
reservation lottery on their behalf (high stake = better odds). It's intentionally a simple stub for now —
first-come-first-served, reject if the miner is unbound / inactive / busy, otherwise accept. The on-behalf-of
``open_or_request`` call + request-window selection land later. The live confirm/activate handlers are covered
in test_axon_solana_handlers.py.
"""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.synapses import SwapReserveSynapse
from allways.validator.axon_handlers import handle_swap_reserve

_MINER_SS58 = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'
_FAKE_PK = object()  # stand-in for the resolved Solana miner pubkey


def make_reserve_synapse(**overrides) -> SwapReserveSynapse:
    fields = dict(
        miner_hotkey=_MINER_SS58,
        tao_amount=345_000_000,
        from_amount=100_000,
        to_amount=345_000_000,
        from_address='user-addr',
        from_address_proof='proof',
        block_anchor=1000,
        from_chain='sol',
        to_chain='btc',
    )
    fields.update(overrides)
    return SwapReserveSynapse(**fields)


def make_validator(active=True, has_active_swap=False, miner_state=True) -> MagicMock:
    validator = MagicMock()
    validator.metagraph = None  # miner_label falls back to a hotkey-only label
    validator.axon_lock = threading.Lock()
    state = SimpleNamespace(active=active, has_active_swap=has_active_swap) if miner_state else None
    validator.solana_client.get_miner_state.return_value = state
    return validator


def _run(validator, synapse, miner_pk=_FAKE_PK):
    with patch('allways.validator.axon_handlers.resolve_miner_pubkey', return_value=miner_pk):
        return asyncio.run(handle_swap_reserve(validator, synapse))


class TestReserveStub:
    def test_accepts_idle_active_miner(self):
        result = _run(make_validator(), make_reserve_synapse())
        assert result.accepted is True

    def test_rejects_when_busy(self):
        result = _run(make_validator(has_active_swap=True), make_reserve_synapse())
        assert result.accepted is False
        assert 'busy' in (result.rejection_reason or '').lower()

    def test_rejects_when_inactive(self):
        result = _run(make_validator(active=False), make_reserve_synapse())
        assert result.accepted is False
        assert 'not active' in (result.rejection_reason or '').lower()

    def test_rejects_when_unbound(self):
        result = _run(make_validator(), make_reserve_synapse(), miner_pk=None)
        assert result.accepted is False
        assert 'bound' in (result.rejection_reason or '').lower()

    def test_casts_no_vote(self):
        """The FCFS stub must not reach any consensus vote / claim path."""
        validator = make_validator()
        _run(validator, make_reserve_synapse())
        validator.solana_client.vote_activate.assert_not_called()
        validator.solana_client.submit_swap_claim.assert_not_called()
        validator.solana_client.open_or_request.assert_not_called()
