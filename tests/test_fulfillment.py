"""SwapFulfiller — timeout cushion, sender verification, send-path, send-cache.

The cushion/safety tests stay at the verify_swap_safety layer. The send-cache
tests lock in the idempotency invariant: once dest funds are sent, an unmarked
cache entry must keep blocking a duplicate send until mark_fulfilled lands or
the swap is provably past its deadline.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from allways.classes import Swap, SwapStatus
from allways.constants import (
    DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS,
    MAX_EXTENSION_BLOCKS,
    MAX_EXTENSIONS_PER_SWAP,
    MINER_TIMEOUT_CUSHION_BLOCKS,
    SENT_CACHE_DISCARD_MARGIN_BLOCKS,
)
from allways.miner.fulfillment import SentSwap, SwapFulfiller
from allways.miner.swap_poller import MAX_REFRESH_MISSES, SwapPoller


def make_fulfiller() -> SwapFulfiller:
    """Build a SwapFulfiller with mocked deps."""
    return SwapFulfiller(
        contract_client=MagicMock(),
        chain_providers={},
        wallet=MagicMock(),
        subtensor=MagicMock(),
    )


def make_swap(timeout_block: int = 500, rate: str = '345', miner_from: str = 'bc1q-miner') -> Swap:
    return Swap(
        id=1,
        user_hotkey='user',
        miner_hotkey='miner',
        from_chain='btc',
        to_chain='tao',
        from_amount=1_000_000,
        to_amount=345_000_000,
        tao_amount=345_000_000,
        user_from_address='bc1q-user',
        user_to_address='5user',
        miner_from_address=miner_from,
        rate=rate,
        status=SwapStatus.ACTIVE,
        initiated_block=100,
        timeout_block=timeout_block,
    )


class TestVerifySwapSafetyCushion:
    """The cushion is a hardcoded constant pinned to EXTEND_THRESHOLD_BLOCKS —
    miners stop fulfilling that many blocks before the timeout so the
    validator extension flow still has runway to rescue the swap."""

    def test_allows_swap_well_before_cushion_window(self):
        fulfiller = make_fulfiller()
        # Comfortably outside the cushion: deadline = 500 - cushion, current well below.
        fulfiller.subtensor.get_current_block.return_value = 500 - MINER_TIMEOUT_CUSHION_BLOCKS - 10
        result = fulfiller.verify_swap_safety(make_swap(timeout_block=500))
        assert result is not None
        assert result[1] == 'bc1q-miner'

    def test_blocks_swap_inside_cushion_window(self):
        fulfiller = make_fulfiller()
        # One block inside the cushion: current >= timeout - cushion → refused.
        fulfiller.subtensor.get_current_block.return_value = 500 - MINER_TIMEOUT_CUSHION_BLOCKS + 1
        assert fulfiller.verify_swap_safety(make_swap(timeout_block=500)) is None

    def test_blocks_swap_at_cushion_boundary(self):
        fulfiller = make_fulfiller()
        # Exact boundary: current == timeout - cushion is unsafe (>= check).
        fulfiller.subtensor.get_current_block.return_value = 500 - MINER_TIMEOUT_CUSHION_BLOCKS
        assert fulfiller.verify_swap_safety(make_swap(timeout_block=500)) is None

    def test_missing_rate_or_miner_from_address_fails_safety(self):
        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 100

        # Missing rate
        assert fulfiller.verify_swap_safety(make_swap(rate='')) is None
        # Missing miner_from_address
        assert fulfiller.verify_swap_safety(make_swap(miner_from='')) is None


class TestVerifySwapSafetyReturnsUserReceives:
    """After R5 rename, verify_swap_safety returns the POST-fee amount, and
    that's what the miner sends to the user. Lock in the math."""

    def test_return_is_post_fee_not_pre_fee(self):
        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 100
        fulfiller.fee_divisor = 100

        swap = make_swap(timeout_block=500, rate='345')
        result = fulfiller.verify_swap_safety(swap)
        assert result is not None
        user_receives_amount, _ = result
        # Pre-fee: 0.01 BTC @ 345 = 3.45 TAO = 3_450_000_000 rao
        # Post-fee: 3_450_000_000 - 34_500_000 = 3_415_500_000 rao
        assert user_receives_amount == 3_415_500_000


class TestSentCacheCleanup:
    """cleanup_stale_sends retains unmarked sends to prevent duplicate dest
    sends, but bounds retention so genuinely-resolved swaps don't leak forever."""

    def test_unmarked_stale_retained_marked_stale_removed_within_deadline(self):
        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 100  # well within all deadlines
        fulfiller.sent = {
            1: SentSwap('unmarked-stale-tx', 101, marked_fulfilled=False, timeout_block=500),
            2: SentSwap('marked-stale-tx', 102, marked_fulfilled=True, timeout_block=500),
            3: SentSwap('active-unmarked-tx', 103, marked_fulfilled=False, timeout_block=500),
        }
        fulfiller.mark_fulfilled_attempts = {1: 2, 2: 3, 3: 1}

        fulfiller.cleanup_stale_sends(active_swap_ids={3})

        # marked stale (2) removed; unmarked stale within deadline (1) retained; active (3) untouched
        assert set(fulfiller.sent) == {1, 3}
        assert fulfiller.mark_fulfilled_attempts == {1: 2, 3: 1}

    def test_retained_send_blocks_resend_after_poller_misses_and_rediscovery(self):
        swap = make_swap(timeout_block=500)
        poll_client = MagicMock()
        poll_client.get_next_swap_id.return_value = swap.id + 1
        poll_client.get_swap.return_value = None
        poller = SwapPoller(contract_client=poll_client, miner_hotkey=swap.miner_hotkey)
        poller.active[swap.id] = swap
        poller.last_scanned_id = swap.id

        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 100  # within deadline → retain
        fulfiller.sent[swap.id] = SentSwap('already-sent-dest-tx', 777, marked_fulfilled=False, timeout_block=500)

        # Transient read gap drops the swap from the poller's active set.
        for _ in range(MAX_REFRESH_MISSES):
            poller.poll()
        assert poller.active == {}

        # Cleanup must NOT drop the unmarked entry while it's only transiently gone.
        fulfiller.cleanup_stale_sends(active_swap_ids=set(poller.active))
        assert fulfiller.sent[swap.id].to_tx_hash == 'already-sent-dest-tx'

        # Swap reappears; process_swap must retry mark_fulfilled, not resend funds.
        poll_client.get_swap.return_value = swap
        poller.poll()
        assert poller.active == {swap.id: swap}

        fulfiller.verify_swap_safety = MagicMock(return_value=(3_415_500_000, swap.miner_from_address))
        fulfiller.verify_user_sent_funds = MagicMock(return_value=True)
        fulfiller.send_dest_funds = MagicMock(return_value=('second-dest-tx', 888))

        assert fulfiller.process_swap(swap) is True
        fulfiller.send_dest_funds.assert_not_called()
        fulfiller.client.mark_fulfilled.assert_called_once_with(
            wallet=fulfiller.wallet,
            swap_id=swap.id,
            to_tx_hash='already-sent-dest-tx',
            to_amount=3_415_500_000,
            to_tx_block=777,
        )
        assert fulfiller.sent[swap.id].marked_fulfilled is True

    def test_unmarked_stale_discarded_once_past_deadline_margin(self):
        fulfiller = make_fulfiller()
        fulfiller.sent = {1: SentSwap('leaked-tx', 50, marked_fulfilled=False, timeout_block=100)}
        # Provably past any possible (even fully-extended) deadline → safe to discard.
        fulfiller.subtensor.get_current_block.return_value = 100 + SENT_CACHE_DISCARD_MARGIN_BLOCKS + 1

        fulfiller.cleanup_stale_sends(active_swap_ids=set())
        assert fulfiller.sent == {}

    def test_unmarked_stale_retained_inside_deadline_margin(self):
        fulfiller = make_fulfiller()
        fulfiller.sent = {
            1: SentSwap('a', 1, marked_fulfilled=False, timeout_block=100),  # just past timeout
            2: SentSwap('b', 2, marked_fulfilled=False, timeout_block=100),  # exactly at margin boundary
        }
        # id 1: a few blocks past timeout but well inside the margin → retain.
        # id 2: exactly timeout + margin → retain (discard uses strict >).
        fulfiller.subtensor.get_current_block.return_value = 100 + SENT_CACHE_DISCARD_MARGIN_BLOCKS

        fulfiller.cleanup_stale_sends(active_swap_ids=set())
        assert set(fulfiller.sent) == {1, 2}

    def test_unmarked_stale_retained_across_two_extensions(self):
        # Regression for #461: the contract permits MAX_EXTENSIONS_PER_SWAP (2)
        # timeout extensions, each pushing timeout_block forward by up to
        # MAX_EXTENSION_BLOCKS relative to its own propose block (not cumulative),
        # so a live deadline can reach D0 + 2 * MAX_EXTENSION_BLOCKS. If the cached
        # snapshot predates both extensions (a get_swap gap drops the swap from the
        # active set, so process_swap never refreshes it), the margin must still
        # cover the fully-extended deadline. The old margin (1 * MAX_EXTENSION_BLOCKS
        # + DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS) would discard here and re-send on
        # rediscovery.
        d0 = 100
        old_single_extension_margin = MAX_EXTENSION_BLOCKS + DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS
        live_deadline = d0 + MAX_EXTENSIONS_PER_SWAP * MAX_EXTENSION_BLOCKS
        # Sanity-check this case actually exercises the gap the fix closes: the
        # current block is past the old margin but the swap is still live on-chain.
        current = d0 + old_single_extension_margin + 1
        assert current > d0 + old_single_extension_margin  # would have been discarded pre-fix
        assert current <= live_deadline  # but the swap is still active on-chain

        fulfiller = make_fulfiller()
        fulfiller.sent = {1: SentSwap('twice-extended-tx', 50, marked_fulfilled=False, timeout_block=d0)}
        fulfiller.subtensor.get_current_block.return_value = current

        fulfiller.cleanup_stale_sends(active_swap_ids=set())
        assert set(fulfiller.sent) == {1}

    def test_legacy_entry_without_deadline_never_discarded(self):
        fulfiller = make_fulfiller()
        fulfiller.sent = {1: SentSwap('legacy-tx', 5, marked_fulfilled=False)}  # timeout_block defaults to 0
        fulfiller.subtensor.get_current_block.return_value = 10**9

        fulfiller.cleanup_stale_sends(active_swap_ids=set())
        assert set(fulfiller.sent) == {1}

    def test_subtensor_failure_during_cleanup_retains_unmarked(self):
        fulfiller = make_fulfiller()
        fulfiller.sent = {1: SentSwap('tx', 5, marked_fulfilled=False, timeout_block=100)}
        fulfiller.subtensor.get_current_block.side_effect = RuntimeError('rpc down')

        # No raise, no wipe — without a block height we can't prove expiry.
        fulfiller.cleanup_stale_sends(active_swap_ids=set())
        assert set(fulfiller.sent) == {1}

    def test_cache_persistence_roundtrips_timeout_block(self, tmp_path: Path):
        cache_path = tmp_path / 'sent_cache.json'
        writer = SwapFulfiller(
            contract_client=MagicMock(),
            chain_providers={},
            wallet=MagicMock(),
            subtensor=MagicMock(),
            sent_cache_path=cache_path,
        )
        writer.sent = {7: SentSwap('tx7', 123, marked_fulfilled=False, timeout_block=456)}
        writer.save_sent_cache()

        reader = SwapFulfiller(
            contract_client=MagicMock(),
            chain_providers={},
            wallet=MagicMock(),
            subtensor=MagicMock(),
            sent_cache_path=cache_path,
        )
        assert reader.sent[7] == SentSwap('tx7', 123, marked_fulfilled=False, timeout_block=456)

    def test_legacy_three_element_cache_loads_with_zero_timeout(self, tmp_path: Path):
        cache_path = tmp_path / 'sent_cache.json'
        cache_path.write_text('{"9": ["legacy-tx", 999, false]}')  # pre-fix 3-element shape

        reader = SwapFulfiller(
            contract_client=MagicMock(),
            chain_providers={},
            wallet=MagicMock(),
            subtensor=MagicMock(),
            sent_cache_path=cache_path,
        )
        assert reader.sent[9] == SentSwap('legacy-tx', 999, marked_fulfilled=False, timeout_block=0)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
