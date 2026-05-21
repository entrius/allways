"""Recency-based replay defense for miner-supplied dest tx hashes.

Locks down the dest-chain tip snapshot taken at swap observation and the
single comparison used to reject a dest tx mined before its swap was
initiated. Closes the gap left by the contract enforcing ``used_from_tx``
only on the source side.
"""

from unittest.mock import MagicMock

from allways.chain_providers.base import TransactionInfo
from allways.classes import Swap, SwapStatus
from allways.validator.chain_verification import SwapVerifier


def make_swap(swap_id: int = 1, to_chain: str = 'btc', initiated_block: int = 100) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='user',
        miner_hotkey='miner',
        from_chain='tao' if to_chain == 'btc' else 'btc',
        to_chain=to_chain,
        from_amount=1,
        to_amount=1,
        tao_amount=1,
        user_from_address='from',
        user_to_address='to',
        miner_from_address='miner-from',
        miner_to_address='miner-to',
        rate='100',
        to_tx_hash='dest-hash',
        status=SwapStatus.FULFILLED,
        initiated_block=initiated_block,
    )


def tx_at(block_number) -> TransactionInfo:
    return TransactionInfo(
        tx_hash='dest-hash',
        confirmed=True,
        sender='miner-to',
        recipient='to',
        amount=1,
        block_number=block_number,
        confirmations=10,
    )


class TestObserveInitiation:
    def test_snapshots_observed_tip(self):
        btc = MagicMock()
        btc.get_current_block_height.return_value = 850_000
        v = SwapVerifier(chain_providers={'btc': btc})

        v.observe_initiation(make_swap(swap_id=1, to_chain='btc'))

        assert v.dest_tip_at_init[1] == 850_000

    def test_idempotent(self):
        btc = MagicMock()
        btc.get_current_block_height.return_value = 850_000
        v = SwapVerifier(chain_providers={'btc': btc})

        v.observe_initiation(make_swap(swap_id=1, to_chain='btc'))
        v.observe_initiation(make_swap(swap_id=1, to_chain='btc'))

        btc.get_current_block_height.assert_called_once()

    def test_tao_dest_is_noop(self):
        btc = MagicMock()
        v = SwapVerifier(chain_providers={'btc': btc})

        v.observe_initiation(make_swap(swap_id=5, to_chain='tao'))

        assert 5 not in v.dest_tip_at_init
        btc.get_current_block_height.assert_not_called()

    def test_failed_snapshot_leaves_no_entry_so_retry_is_possible(self):
        btc = MagicMock()
        btc.get_current_block_height.return_value = None
        v = SwapVerifier(chain_providers={'btc': btc})

        v.observe_initiation(make_swap(swap_id=7, to_chain='btc'))

        assert 7 not in v.dest_tip_at_init

        # Next forward step the RPC recovers — snapshot is captured.
        btc.get_current_block_height.return_value = 850_500
        v.observe_initiation(make_swap(swap_id=7, to_chain='btc'))

        assert v.dest_tip_at_init[7] == 850_500

    def test_rpc_raises_treated_as_failure(self):
        btc = MagicMock()
        btc.get_current_block_height.side_effect = RuntimeError('boom')
        v = SwapVerifier(chain_providers={'btc': btc})

        v.observe_initiation(make_swap(swap_id=9, to_chain='btc'))

        assert 9 not in v.dest_tip_at_init


class TestIsDestTxFresh:
    def test_tao_accepts_initiation_block_and_rejects_earlier(self):
        v = SwapVerifier(chain_providers={})
        swap = make_swap(to_chain='tao', initiated_block=100)
        assert v.is_dest_tx_fresh(swap, tx_at(100)) is True
        assert v.is_dest_tx_fresh(swap, tx_at(99)) is False

    def test_btc_accepts_at_snapshot_rejects_older_replay(self):
        v = SwapVerifier(chain_providers={})
        v.dest_tip_at_init[1] = 850_000
        swap = make_swap(swap_id=1, to_chain='btc')

        assert v.is_dest_tx_fresh(swap, tx_at(850_000)) is True
        assert v.is_dest_tx_fresh(swap, tx_at(849_500)) is False

    def test_failopen_when_no_snapshot(self):
        v = SwapVerifier(chain_providers={})
        swap = make_swap(swap_id=1, to_chain='btc')
        # Even an obviously old tx is accepted — defense disabled for this swap.
        assert v.is_dest_tx_fresh(swap, tx_at(1)) is True

    def test_missing_block_number_passes(self):
        v = SwapVerifier(chain_providers={})
        v.dest_tip_at_init[1] = 850_000
        swap = make_swap(swap_id=1, to_chain='btc')
        info = tx_at(850_000)
        info.block_number = None
        assert v.is_dest_tx_fresh(swap, info) is True


class TestPruneToActive:
    def test_drops_inactive_swaps(self):
        v = SwapVerifier(chain_providers={})
        v.dest_tip_at_init = {1: 100, 2: 200, 3: 300}
        v.source_verified_ids = {1, 2, 3}

        v.prune_to_active({2})

        assert v.dest_tip_at_init == {2: 200}
        assert v.source_verified_ids == {2}
