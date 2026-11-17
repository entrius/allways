"""Tests for the SwapReserve axon wrapper.

``handle_swap_reserve`` is now a thin transport wrapper over the shared ``reserve_on_behalf`` kernel op
(eligibility + rate logic is covered in test_reserve_engine.py). These tests assert the wrapper maps the
op's result onto the synapse and never raises. The live confirm/activate handlers are covered in
test_axon_solana_handlers.py.
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

from allways.synapses import SwapReserveSynapse
from allways.validator.serving.axon import handle_swap_reserve
from allways.validator.serving.requests import ReserveResult

_MINER_SS58 = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_reserve_synapse(**overrides) -> SwapReserveSynapse:
    fields = dict(
        miner_hotkey=_MINER_SS58,
        from_chain='sol',
        to_chain='btc',
        user_pubkey='11111111111111111111111111111111',
        user_from_addr='11111111111111111111111111111111',
        user_to_addr='userBTCaddr',
        from_amount=1_000_000_000,
    )
    fields.update(overrides)
    return SwapReserveSynapse(**fields)


def _run(synapse, result=None, raises=None):
    ctx = MagicMock()
    ctx.metagraph = None  # miner_label falls back to a hotkey-only label
    ctx.lock = threading.Lock()
    side = (lambda *a, **k: (_ for _ in ()).throw(raises)) if raises else (lambda *a, **k: result)
    with patch('allways.validator.serving.axon.reserve_on_behalf', side_effect=side):
        return asyncio.run(handle_swap_reserve(ctx, synapse))


def test_maps_ok_result():
    s = _run(make_reserve_synapse(), ReserveResult(True, '', 4242, 'sig'))
    assert s.accepted is True and s.pool_closes_at == 4242


def test_exception_is_caught():
    s = _run(make_reserve_synapse(), raises=RuntimeError('rpc down'))
    assert s.accepted is False and 'rpc down' in (s.rejection_reason or '')
