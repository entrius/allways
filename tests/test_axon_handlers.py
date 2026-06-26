"""Tests for the decommissioned substrate reserve handler.

``handle_swap_reserve`` was the validator's substrate ``vote_reserve`` intake.
The reservation flow moved on-chain to Solana (the Phase-9 reservation pool), so
the handler is now a stub that rejects every request without voting. The live
Solana axon handlers (``handle_swap_confirm``, ``handle_miner_activate``) are
covered in test_axon_solana_handlers.py.
"""

import asyncio
import threading
from unittest.mock import MagicMock

from allways.synapses import SwapReserveSynapse
from allways.validator.axon_handlers import handle_swap_reserve

# A valid SS58 (Alice from the substrate dev keyring) — the synapse carries one
# even though the stub never parses it, so the shape matches a real request.
_MINER_SS58 = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_reserve_synapse(**overrides) -> SwapReserveSynapse:
    fields = dict(
        miner_hotkey=_MINER_SS58,
        tao_amount=345_000_000,
        from_amount=100_000,
        to_amount=345_000_000,
        from_address='bc1-user',
        from_address_proof='proof',
        block_anchor=1000,
        from_chain='btc',
        to_chain='tao',
    )
    fields.update(overrides)
    return SwapReserveSynapse(**fields)


def make_validator() -> MagicMock:
    validator = MagicMock()
    validator.metagraph = None  # miner_label falls back to a hotkey-only label
    validator.axon_lock = threading.Lock()
    return validator


class TestReserveStub:
    """The substrate reserve path is retired — every request is rejected."""

    def test_reserve_rejects_with_phase9_message(self):
        result = asyncio.run(handle_swap_reserve(make_validator(), make_reserve_synapse()))
        assert result.accepted is False
        assert 'Phase 9' in (result.rejection_reason or '')

    def test_reserve_casts_no_vote(self):
        """The stub must reject without reaching any contract/Solana client."""
        validator = make_validator()
        asyncio.run(handle_swap_reserve(validator, make_reserve_synapse()))
        validator.solana_client.vote_activate.assert_not_called()
        validator.solana_client.submit_swap_claim.assert_not_called()
