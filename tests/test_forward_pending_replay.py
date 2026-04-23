"""Replay wiring for queued confirms: stored block is passed as block_hint."""

from unittest.mock import MagicMock

import pytest

from allways.chain_providers.base import TransactionInfo
from allways.validator.forward import initialize_pending_user_reservations
from allways.validator.state_store import PendingConfirm


@pytest.mark.parametrize(
    'stored_block',
    [500, 0],
    ids=['hinted', 'unknown'],
)
def test_replay_passes_stored_block_as_hint(stored_block):
    pending = PendingConfirm(
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
        from_tx_block=stored_block,
    )
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
        tx_hash='',
        confirmed=False,
        sender='',
        recipient='',
        amount=0,
    )
    validator.chain_providers = {'tao': provider}

    initialize_pending_user_reservations(validator)

    assert provider.verify_transaction.call_args.kwargs['block_hint'] == stored_block
