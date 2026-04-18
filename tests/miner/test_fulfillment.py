"""SwapFulfiller — timeout cushion, sender verification, send-path behavior.

These tests stay at the verify_swap_safety layer, which is the only part
of SwapFulfiller that's exercised on every forward step and that the
refactor branch changed meaningfully (cushion env hot-reload, post-fee
user_receives_amount naming).
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from allways.constants import DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS
from allways.miner.fulfillment import SwapFulfiller, load_timeout_cushion_blocks

from tests.helpers import make_swap


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
            netuid=2,
            metagraph=MagicMock(),
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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
