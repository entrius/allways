"""Decision logic for OptimisticExtensionWatcher.

Mocks the contract_client + wallet — no chain, no on-chain state. Each test
asserts on whether the wrapper called the right contract write (or stayed
silent) given a specific input.
"""

from unittest.mock import MagicMock

import pytest

from allways.classes import PendingExtension
from allways.constants import EXTENSION_BUCKET_BLOCKS
from allways.contract_client import ContractError
from allways.validator.optimistic_extensions import OptimisticExtensionWatcher


OUR_HOTKEY = '5Our000000000000000000000000000000000000000000'
OTHER_HOTKEY = '5Other00000000000000000000000000000000000000000'
MINER = '5Miner00000000000000000000000000000000000000000'


def make_watcher(pending_reservation=None, pending_timeout=None, propose_raises=None):
    """Build a watcher with controllable contract responses.

    ``pending_reservation`` / ``pending_timeout``: what the get_pending_*
    readers return. ``propose_raises``: if set, every write method raises this.
    """
    cc = MagicMock()
    cc.get_pending_reservation_extension.return_value = pending_reservation
    cc.get_pending_timeout_extension.return_value = pending_timeout
    if propose_raises is not None:
        cc.propose_extend_reservation.side_effect = propose_raises
        cc.challenge_extend_reservation.side_effect = propose_raises
        cc.finalize_extend_reservation.side_effect = propose_raises
        cc.propose_extend_timeout.side_effect = propose_raises
        cc.challenge_extend_timeout.side_effect = propose_raises
        cc.finalize_extend_timeout.side_effect = propose_raises

    wallet = MagicMock()
    wallet.hotkey.ss58_address = OUR_HOTKEY
    return OptimisticExtensionWatcher(contract_client=cc, wallet=wallet)


# =============================================================================
# Reservation side
# =============================================================================


class TestMaybeProposeReservation:
    def test_proposes_when_no_pending_and_target_advances(self):
        w = make_watcher(pending_reservation=None)
        # BTC at 0/3 confs gives a 180-block bucketed extension; current=1000 →
        # target=1180. reserved_until=1100 < 1180, so propose should fire.
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER, from_chain_id='btc', from_tx_hash=bytes(32),
            current_block=1000, reserved_until=1100, observed_confirmations=0,
        )
        assert result is True
        w.contract_client.propose_extend_reservation.assert_called_once()
        call_kwargs = w.contract_client.propose_extend_reservation.call_args.kwargs
        assert call_kwargs['miner_hotkey'] == MINER
        assert call_kwargs['target_block'] == 1180

    def test_skips_when_pending_already_exists(self):
        w = make_watcher(pending_reservation=PendingExtension(OTHER_HOTKEY, 1180, 990))
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER, from_chain_id='btc', from_tx_hash=bytes(32),
            current_block=1000, reserved_until=1100, observed_confirmations=0,
        )
        assert result is False
        w.contract_client.propose_extend_reservation.assert_not_called()

    def test_skips_when_target_does_not_advance(self):
        # BTC at 3/3 confs: only padding remains (300s/12 = 25 → bucketed to 30).
        # Target = current + 30 = 1030. If reserved_until is already 1100, no need.
        w = make_watcher(pending_reservation=None)
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER, from_chain_id='btc', from_tx_hash=bytes(32),
            current_block=1000, reserved_until=1100, observed_confirmations=3,
        )
        assert result is False
        w.contract_client.propose_extend_reservation.assert_not_called()

    def test_swallows_contract_rejection(self):
        w = make_watcher(
            pending_reservation=None,
            propose_raises=ContractError('contract reverted: ProposalAlreadyPending'),
        )
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER, from_chain_id='btc', from_tx_hash=bytes(32),
            current_block=1000, reserved_until=1100, observed_confirmations=0,
        )
        assert result is False  # rejection means we didn't successfully propose


class TestMaybeChallengeReservation:
    def test_challenges_when_target_too_far(self):
        # Local expected target for BTC at 1/3 confs, current=1000 = 1150.
        # Pending target=2000 is way beyond expected + bucket(30) = 1180.
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER, from_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is True
        w.contract_client.challenge_extend_reservation.assert_called_once()

    def test_skips_when_target_within_one_bucket_tolerance(self):
        # Expected target = 1150. Bucket = 30. Pending = 1180 (= expected + bucket)
        # is the boundary — should be accepted (within tolerance).
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=995),
        )
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER, from_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is False
        w.contract_client.challenge_extend_reservation.assert_not_called()

    def test_skips_when_no_pending(self):
        w = make_watcher(pending_reservation=None)
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER, from_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is False
        w.contract_client.challenge_extend_reservation.assert_not_called()

    def test_skips_when_we_are_the_submitter(self):
        # Don't challenge our own proposal even if we'd locally compute differently.
        w = make_watcher(
            pending_reservation=PendingExtension(OUR_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER, from_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is False
        w.contract_client.challenge_extend_reservation.assert_not_called()


class TestMaybeFinalizeReservation:
    def test_finalizes_when_window_elapsed(self):
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=992),
        )
        # window=8, proposed_at=992 → finalize-eligible at block 1000.
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER, current_block=1000, challenge_window_blocks=8,
        )
        assert result is True
        w.contract_client.finalize_extend_reservation.assert_called_once()

    def test_skips_when_window_not_yet_elapsed(self):
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=997),
        )
        # 997 + 8 = 1005, current=1000 → too early.
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER, current_block=1000, challenge_window_blocks=8,
        )
        assert result is False
        w.contract_client.finalize_extend_reservation.assert_not_called()

    def test_skips_when_no_pending(self):
        w = make_watcher(pending_reservation=None)
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER, current_block=1000, challenge_window_blocks=8,
        )
        assert result is False
        w.contract_client.finalize_extend_reservation.assert_not_called()


# =============================================================================
# Timeout side — same shape, abbreviated coverage
# =============================================================================


class TestMaybeProposeTimeout:
    def test_proposes_when_no_pending_and_target_advances(self):
        w = make_watcher(pending_timeout=None)
        result = w.maybe_propose_timeout(
            swap_id=42, dest_chain_id='btc',
            current_block=1000, timeout_block=1100, observed_confirmations=0,
        )
        assert result is True
        w.contract_client.propose_extend_timeout.assert_called_once()
        assert w.contract_client.propose_extend_timeout.call_args.kwargs['swap_id'] == 42

    def test_skips_when_pending(self):
        w = make_watcher(pending_timeout=PendingExtension(OTHER_HOTKEY, 1180, 990))
        result = w.maybe_propose_timeout(
            swap_id=42, dest_chain_id='btc',
            current_block=1000, timeout_block=1100, observed_confirmations=0,
        )
        assert result is False


class TestMaybeChallengeTimeout:
    def test_challenges_when_target_too_far(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_timeout(
            swap_id=42, dest_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is True

    def test_skips_when_we_are_the_submitter(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OUR_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_timeout(
            swap_id=42, dest_chain_id='btc',
            observed_confirmations=1, current_block=1000,
        )
        assert result is False


class TestMaybeFinalizeTimeout:
    def test_finalizes_when_window_elapsed(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=992),
        )
        result = w.maybe_finalize_timeout(
            swap_id=42, current_block=1000, challenge_window_blocks=8,
        )
        assert result is True

    def test_skips_when_window_not_yet_elapsed(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=997),
        )
        result = w.maybe_finalize_timeout(
            swap_id=42, current_block=1000, challenge_window_blocks=8,
        )
        assert result is False
