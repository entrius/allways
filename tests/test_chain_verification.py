"""Recency-based replay defense for miner-supplied dest tx hashes.

Locks down the dest-chain tip snapshot taken at swap observation and the
single comparison used to reject a dest tx mined before its swap was
initiated. Closes the gap left by the contract enforcing ``used_from_tx``
only on the source side.
"""

import asyncio
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


class TestVerifyMinerFulfillmentSourceSender:
    def _make_fulfilled_swap(self) -> Swap:
        return Swap(
            id=42,
            user_hotkey='user',
            miner_hotkey='miner',
            from_chain='btc',
            to_chain='tao',
            from_amount=1_000_000,
            to_amount=345_000_000,
            tao_amount=345_000_000,
            user_from_address='bc1q-user',
            user_to_address='5user',
            miner_from_address='bc1q-miner',
            miner_to_address='5miner',
            rate='345',
            from_tx_hash='src-hash',
            from_tx_block=50,
            to_tx_hash='dest-hash',
            to_tx_block=101,
            status=SwapStatus.FULFILLED,
            initiated_block=100,
        )

    def _run_fulfillment(self, v: SwapVerifier, swap: Swap) -> bool:
        return asyncio.run(v.verify_miner_fulfillment(swap))

    def test_source_verify_passes_user_from_as_expected_sender(self):
        swap = self._make_fulfilled_swap()
        source = MagicMock()
        source.verify_transaction.return_value = TransactionInfo(
            tx_hash='src-hash',
            confirmed=True,
            sender='bc1q-user',
            recipient='bc1q-miner',
            amount=1_000_000,
            block_number=50,
            confirmations=10,
        )
        dest = MagicMock()
        dest.verify_transaction.return_value = TransactionInfo(
            tx_hash='dest-hash',
            confirmed=True,
            sender='5miner',
            recipient='5user',
            amount=3_415_500_000,
            block_number=101,
            confirmations=10,
        )
        v = SwapVerifier(chain_providers={'btc': source, 'tao': dest})

        assert self._run_fulfillment(v, swap) is True
        source.verify_transaction.assert_called_once_with(
            tx_hash='src-hash',
            expected_recipient='bc1q-miner',
            expected_amount=1_000_000,
            block_hint=50,
            expected_sender='bc1q-user',
        )

    def test_rejects_source_tx_with_wrong_sender_when_user_bound(self):
        """Provider returns confirmed tx with empty sender — must fail when
        expected_sender is bound, matching axon/miner/forward paths."""
        swap = self._make_fulfilled_swap()

        def verify_source(**kwargs):
            assert kwargs.get('expected_sender') == 'bc1q-user'
            info = TransactionInfo(
                tx_hash='src-hash',
                confirmed=True,
                sender='',
                recipient='bc1q-miner',
                amount=1_000_000,
                block_number=50,
                confirmations=10,
            )
            if kwargs.get('expected_sender') and info.sender != kwargs['expected_sender']:
                return None
            return info

        source = MagicMock()
        source.verify_transaction.side_effect = verify_source
        dest = MagicMock()
        dest.verify_transaction.return_value = TransactionInfo(
            tx_hash='dest-hash',
            confirmed=True,
            sender='5miner',
            recipient='5user',
            amount=3_415_500_000,
            block_number=101,
            confirmations=10,
        )
        v = SwapVerifier(chain_providers={'btc': source, 'tao': dest})

        assert self._run_fulfillment(v, swap) is False
        source.verify_transaction.assert_called_once_with(
            tx_hash='src-hash',
            expected_recipient='bc1q-miner',
            expected_amount=1_000_000,
            block_hint=50,
            expected_sender='bc1q-user',
        )
