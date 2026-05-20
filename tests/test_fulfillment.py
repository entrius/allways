"""SwapFulfiller — timeout cushion, send-cache, and send-path behavior."""

import os
from unittest.mock import MagicMock, patch

import pytest

from allways.classes import Swap, SwapStatus
from allways.constants import DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS
from allways.miner.fulfillment import SentSwap, SwapFulfiller, load_timeout_cushion_blocks
from allways.miner.swap_poller import MAX_REFRESH_MISSES, SwapPoller


def make_fulfiller(cushion_env: str | None = None) -> SwapFulfiller:
    """Build a SwapFulfiller with mocked deps. Optionally seed the env var."""
    env = {k: v for k, v in os.environ.items() if k != 'MINER_TIMEOUT_CUSHION_BLOCKS'}
    if cushion_env is not None:
        env['MINER_TIMEOUT_CUSHION_BLOCKS'] = cushion_env
    with patch.dict(os.environ, env, clear=True):
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


class TestLoadTimeoutCushionBlocks:
    def test_unset_env_returns_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert load_timeout_cushion_blocks() == DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS

    def test_empty_string_returns_default(self):
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': ''}, clear=False):
            assert load_timeout_cushion_blocks() == DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS

    def test_valid_int_is_used(self):
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': '12'}, clear=False):
            assert load_timeout_cushion_blocks() == 12

    def test_zero_is_allowed(self):
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': '0'}, clear=False):
            assert load_timeout_cushion_blocks() == 0

    def test_negative_is_clamped_to_zero(self):
        """A sign-flip typo shouldn't disable the safety margin."""
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': '-5'}, clear=False):
            assert load_timeout_cushion_blocks() == 0

    def test_invalid_string_falls_back_to_default(self):
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': 'not-a-number'}, clear=False):
            assert load_timeout_cushion_blocks() == DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS


class TestVerifySwapSafetyCushion:
    """The cushion is re-read on every verify call so operators can tune it
    without restarting the miner."""

    def test_default_cushion_allows_swap_before_deadline(self):
        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 400
        # deadline = 500 - 5 = 495, current 400 < 495 → allowed
        result = fulfiller.verify_swap_safety(make_swap(timeout_block=500))
        assert result is not None
        assert result[1] == 'bc1q-miner'

    def test_default_cushion_blocks_swap_inside_window(self):
        fulfiller = make_fulfiller()
        fulfiller.subtensor.get_current_block.return_value = 497
        # deadline = 500 - 5 = 495, current 497 >= 495 → blocked
        assert fulfiller.verify_swap_safety(make_swap(timeout_block=500)) is None

    def test_env_change_takes_effect_without_reconstruction(self):
        """Call verify_swap_safety twice; between calls change the env.
        The second call should see the new cushion value."""
        fulfiller = make_fulfiller(cushion_env='5')
        fulfiller.subtensor.get_current_block.return_value = 490
        swap = make_swap(timeout_block=500)

        # With cushion=5, effective deadline=495, current 490 → allowed
        assert fulfiller.verify_swap_safety(swap) is not None

        # Tighten the cushion at runtime — no restart
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': '15'}, clear=False):
            # effective deadline = 500 - 15 = 485, current 490 >= 485 → blocked
            assert fulfiller.verify_swap_safety(swap) is None

    def test_zero_cushion_allows_right_up_to_timeout(self):
        fulfiller = make_fulfiller(cushion_env='0')
        fulfiller.subtensor.get_current_block.return_value = 499
        # Re-patch inside the call so the hot-reload sees MINER_TIMEOUT_CUSHION_BLOCKS=0
        with patch.dict(os.environ, {'MINER_TIMEOUT_CUSHION_BLOCKS': '0'}, clear=False):
            result = fulfiller.verify_swap_safety(make_swap(timeout_block=500))
        # deadline = 500 - 0 = 500, current 499 < 500 → allowed
        assert result is not None

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
    def test_unmarked_stale_sends_are_retained(self):
        fulfiller = make_fulfiller()
        fulfiller.sent = {
            1: SentSwap('unmarked-stale-tx', 101, marked_fulfilled=False),
            2: SentSwap('marked-stale-tx', 102, marked_fulfilled=True),
            3: SentSwap('active-unmarked-tx', 103, marked_fulfilled=False),
        }
        fulfiller.mark_fulfilled_attempts = {1: 2, 2: 3, 3: 1}

        fulfiller.cleanup_stale_sends(active_swap_ids={3})

        assert fulfiller.sent == {
            1: SentSwap('unmarked-stale-tx', 101, marked_fulfilled=False),
            3: SentSwap('active-unmarked-tx', 103, marked_fulfilled=False),
        }
        assert fulfiller.mark_fulfilled_attempts == {1: 2, 3: 1}

    def test_retained_send_cache_blocks_resend_after_poller_misses_and_rediscovery(self):
        swap = make_swap()
        poll_client = MagicMock()
        poll_client.get_next_swap_id.return_value = swap.id + 1
        poll_client.get_swap.return_value = None
        poller = SwapPoller(contract_client=poll_client, miner_hotkey=swap.miner_hotkey)
        poller.active[swap.id] = swap
        poller.last_scanned_id = swap.id

        fulfiller = make_fulfiller()
        fulfiller.sent[swap.id] = SentSwap('already-sent-dest-tx', 777, marked_fulfilled=False)

        for _ in range(MAX_REFRESH_MISSES):
            poller.poll()

        assert poller.active == {}

        fulfiller.cleanup_stale_sends(active_swap_ids=set(poller.active))
        assert fulfiller.sent[swap.id] == SentSwap('already-sent-dest-tx', 777, marked_fulfilled=False)

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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
