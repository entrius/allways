"""Tests for forward.initialize_pending_user_reservations — the replay path
that re-verifies queued confirms after validator restart.

Focus here is the ``block_hint`` wiring that fixes issue #108: without a hint,
the TAO provider defaults to scanning only the most recent window of blocks,
and a queued confirm whose source tx is older than that window gets dropped.
"""

from unittest.mock import MagicMock

import pytest

from allways.chain_providers.base import TransactionInfo
from allways.validator.forward import initialize_pending_user_reservations
from allways.validator.state_store import PendingConfirm


def make_pending(from_tx_block: int | None) -> PendingConfirm:
    return PendingConfirm(
        miner_hotkey='miner-1',
        from_tx_hash='tx-abc',
        from_chain='tao',
        to_chain='btc',
        from_address='5user',
        to_address='bc1-user',
        tao_amount=1,
        from_amount=2,
        to_amount=3,
        miner_from_address='5miner',
        miner_to_address='bc1-miner',
        rate_str='350',
        reserved_until=10_000,
        from_tx_block=from_tx_block,
    )


def make_validator(pending: PendingConfirm) -> MagicMock:
    validator = MagicMock()
    validator.block = 9_000
    validator.extend_reservation_voted_at = {}
    validator.pending_confirm_null_polls = {}
    validator.metagraph.hotkeys = []
    validator.state_store.get_all.return_value = [pending]
    validator.contract_client.get_miner_has_active_swap.return_value = False
    validator.contract_client.get_miner_reserved_until.return_value = pending.reserved_until

    provider = MagicMock()
    provider.verify_transaction.return_value = TransactionInfo(
        tx_hash=pending.from_tx_hash,
        confirmed=False,
        sender=pending.from_address,
        recipient=pending.miner_from_address,
        amount=pending.from_amount,
        block_number=pending.from_tx_block,
        confirmations=4,
    )
    validator.chain_providers = {pending.from_chain: provider}
    return validator


@pytest.mark.parametrize(
    'stored_block,expected_hint',
    [(500, 500), (None, 0)],
    ids=['hinted', 'legacy_null_falls_back_to_full_scan'],
)
def test_replay_passes_stored_block_as_hint(stored_block, expected_hint):
    """Stored block → passed as block_hint; None (legacy/mempool) → 0 so the
    provider uses its default scan. Without the hint, a tx older than that
    scan window is unrecoverable after restart (#108)."""
    validator = make_validator(make_pending(from_tx_block=stored_block))
    initialize_pending_user_reservations(validator)
    call = validator.chain_providers['tao'].verify_transaction.call_args
    assert call.kwargs['block_hint'] == expected_hint
