"""SwapFulfiller timeout cushion, sender verification, and send-path behavior."""

import os
from unittest.mock import MagicMock, patch

import pytest

from allways.classes import Swap, SwapStatus
from allways.constants import DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS
from allways.miner.fulfillment import SwapFulfiller, load_timeout_cushion_blocks


def make_fulfiller(
    cushion_env: str | None = None,
    chain_providers: dict | None = None,
    my_addresses: dict | None = None,
) -> SwapFulfiller:
    """Build a SwapFulfiller with mocked deps. Optionally seed the env var."""
    env = {k: v for k, v in os.environ.items() if k != 'MINER_TIMEOUT_CUSHION_BLOCKS'}
    if cushion_env is not None:
        env['MINER_TIMEOUT_CUSHION_BLOCKS'] = cushion_env
    with patch.dict(os.environ, env, clear=True):
        return SwapFulfiller(
            contract_client=MagicMock(),
            chain_providers=chain_providers or {},
            wallet=MagicMock(),
            subtensor=MagicMock(),
            my_addresses=my_addresses,
        )


def make_swap(
    timeout_block: int = 500,
    rate: str = '345',
    miner_from: str = 'bc1q-miner',
    to_chain: str = 'tao',
    user_to: str = '5user',
    miner_to: str = '5miner',
) -> Swap:
    return Swap(
        id=1,
        user_hotkey='user',
        miner_hotkey='miner',
        from_chain='btc',
        to_chain=to_chain,
        from_amount=1_000_000,
        to_amount=345_000_000,
        tao_amount=345_000_000,
        user_from_address='bc1q-user',
        user_to_address=user_to,
        miner_from_address=miner_from,
        miner_to_address=miner_to,
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


class TestSendDestFundsSourceAddress:
    @pytest.mark.parametrize(
        ('to_chain', 'miner_to', 'my_addresses', 'expected_from_address', 'expect_send'),
        [
            ('btc', 'bc1q-pinned-miner', {}, 'bc1q-pinned-miner', True),
            ('btc', 'bc1q-pinned-miner', {'btc': 'bc1q-stale-cache'}, 'bc1q-pinned-miner', True),
            ('btc', '', {'btc': 'bc1q-cache-address'}, None, False),
            ('tao', '', {'tao': '5cached'}, None, True),
        ],
    )
    def test_send_dest_funds_source_address_invariants(
        self,
        to_chain,
        miner_to,
        my_addresses,
        expected_from_address,
        expect_send,
    ):
        provider = MagicMock()
        provider.send_amount.return_value = ('tx-hash', 123)
        fulfiller = make_fulfiller(chain_providers={to_chain: provider}, my_addresses=my_addresses)
        swap = make_swap(to_chain=to_chain, user_to=f'{to_chain}-user-dest', miner_to=miner_to)

        result = fulfiller.send_dest_funds(swap, 50_000)

        if not expect_send:
            assert result is None
            provider.send_amount.assert_not_called()
            return

        assert result == ('tx-hash', 123)
        provider.send_amount.assert_called_once_with(
            f'{to_chain}-user-dest',
            50_000,
            from_address=expected_from_address,
        )

    def test_btc_send_uses_swap_pinned_miner_to_address_with_empty_cache(self):
        provider = MagicMock()
        provider.send_amount.return_value = ('btc-tx', 123)
        fulfiller = make_fulfiller(chain_providers={'btc': provider}, my_addresses={})
        swap = make_swap(to_chain='btc', user_to='bc1q-user-dest', miner_to='bc1q-pinned-miner')

        result = fulfiller.send_dest_funds(swap, 50_000)

        assert result == ('btc-tx', 123)
        provider.send_amount.assert_called_once_with(
            'bc1q-user-dest',
            50_000,
            from_address='bc1q-pinned-miner',
        )

    def test_btc_send_ignores_stale_cached_address_when_swap_has_pinned_address(self):
        provider = MagicMock()
        provider.send_amount.return_value = ('btc-tx', 123)
        fulfiller = make_fulfiller(
            chain_providers={'btc': provider},
            my_addresses={'btc': 'bc1q-stale-cache'},
        )
        swap = make_swap(to_chain='btc', user_to='bc1q-user-dest', miner_to='bc1q-pinned-miner')

        result = fulfiller.send_dest_funds(swap, 50_000)

        assert result == ('btc-tx', 123)
        provider.send_amount.assert_called_once_with(
            'bc1q-user-dest',
            50_000,
            from_address='bc1q-pinned-miner',
        )

    def test_btc_send_without_swap_pinned_address_fails_before_broadcast(self):
        provider = MagicMock()
        fulfiller = make_fulfiller(
            chain_providers={'btc': provider},
            my_addresses={'btc': 'bc1q-cache-address'},
        )
        swap = make_swap(to_chain='btc', user_to='bc1q-user-dest', miner_to='')

        result = fulfiller.send_dest_funds(swap, 50_000)

        assert result is None
        provider.send_amount.assert_not_called()

    def test_tao_send_does_not_pass_source_address_hint(self):
        provider = MagicMock()
        provider.send_amount.return_value = ('tao-tx', 456)
        fulfiller = make_fulfiller(
            chain_providers={'tao': provider},
            my_addresses={'tao': '5cached'},
        )
        swap = make_swap(to_chain='tao', user_to='5user-dest', miner_to='')

        result = fulfiller.send_dest_funds(swap, 3_415_500_000)

        assert result == ('tao-tx', 456)
        provider.send_amount.assert_called_once_with(
            '5user-dest',
            3_415_500_000,
            from_address=None,
        )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
