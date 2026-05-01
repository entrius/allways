"""Decision logic for OptimisticExtensionWatcher.

Mocks the contract_client + wallet — no chain, no on-chain state. Each test
asserts on whether the wrapper called the right contract write (or stayed
silent) given a specific input. ``pending`` is now a caller-supplied
parameter (forward.py fetches once per row instead of three times); each
test resolves it via ``w.fetch_pending_reservation`` / ``fetch_pending_timeout``
so the existing ``make_watcher(pending_*)`` mocks still drive the value.
"""

from unittest.mock import MagicMock

from allways.classes import PendingExtension
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
    def test_tier0_proposes_on_visibility_with_short_target(self):
        # Tier 0: BTC blocks_needed=90, anchor=max(current=1000, reserved=1050)=1050,
        # target=1140.
        w = make_watcher(pending_reservation=None)
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1050,
            observed_confirmations=0,
            extension_count=0,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is True
        call_kwargs = w.contract_client.propose_extend_reservation.call_args.kwargs
        assert call_kwargs['miner_hotkey'] == MINER
        assert call_kwargs['target_block'] == 1140

    def test_tier1_proposes_with_chain_aware_target(self):
        # Tier 1: BTC remaining=2 → blocks_needed=150, anchor=max(1000,1100)=1100,
        # target=1250.
        w = make_watcher(pending_reservation=None)
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1100,
            observed_confirmations=1,
            extension_count=1,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is True
        assert w.contract_client.propose_extend_reservation.call_args.kwargs['target_block'] == 1250

    def test_tier1_skips_when_below_one_confirmation(self):
        # Tier 1 demands ≥1 confirmation — mempool-only tx is not enough.
        w = make_watcher(pending_reservation=None)
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1100,
            observed_confirmations=0,
            extension_count=1,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is False
        w.contract_client.propose_extend_reservation.assert_not_called()

    def test_skips_when_at_extension_cap(self):
        # extension_count=MAX → contract would reject; refuse locally.
        w = make_watcher(pending_reservation=None)
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1100,
            observed_confirmations=1,
            extension_count=2,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is False
        w.contract_client.propose_extend_reservation.assert_not_called()

    def test_skips_when_pending_already_exists(self):
        w = make_watcher(pending_reservation=PendingExtension(OTHER_HOTKEY, 1180, 990))
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1100,
            observed_confirmations=0,
            extension_count=0,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is False
        w.contract_client.propose_extend_reservation.assert_not_called()

    def test_swallows_contract_rejection(self):
        w = make_watcher(
            pending_reservation=None,
            propose_raises=ContractError('contract reverted: ProposalAlreadyPending'),
        )
        result = w.maybe_propose_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            from_tx_hash=bytes(32),
            current_block=1000,
            reserved_until=1050,
            observed_confirmations=0,
            extension_count=0,
            pending=w.fetch_pending_reservation(MINER),
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
            miner_hotkey=MINER,
            from_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_reservation(MINER),
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
            miner_hotkey=MINER,
            from_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is False
        w.contract_client.challenge_extend_reservation.assert_not_called()

    def test_skips_when_no_pending(self):
        w = make_watcher(pending_reservation=None)
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is False
        w.contract_client.challenge_extend_reservation.assert_not_called()

    def test_skips_when_we_are_the_submitter(self):
        # Don't challenge our own proposal even if we'd locally compute differently.
        w = make_watcher(
            pending_reservation=PendingExtension(OUR_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_reservation(
            miner_hotkey=MINER,
            from_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_reservation(MINER),
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
            miner_hotkey=MINER,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_reservation(MINER),
        )
        # Returns the applied target so the caller can refresh local caches
        # (e.g. state_store.update_reserved_until) without waiting for the
        # next event sync.
        assert result == 1180
        w.contract_client.finalize_extend_reservation.assert_called_once()

    def test_skips_when_window_not_yet_elapsed(self):
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=997),
        )
        # 997 + 8 = 1005, current=1000 → too early.
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is None
        w.contract_client.finalize_extend_reservation.assert_not_called()

    def test_skips_when_no_pending(self):
        w = make_watcher(pending_reservation=None)
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is None
        w.contract_client.finalize_extend_reservation.assert_not_called()

    def test_returns_none_when_contract_call_fails(self):
        # Contract rejection (e.g. reservation already expired) → no target.
        w = make_watcher(
            pending_reservation=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=992),
            propose_raises=ContractError('NoReservation'),
        )
        result = w.maybe_finalize_reservation(
            miner_hotkey=MINER,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_reservation(MINER),
        )
        assert result is None


class TestFetchPendingReservation:
    def test_returns_pending_when_cc_has_one(self):
        pending = PendingExtension(OTHER_HOTKEY, 1180, 990)
        w = make_watcher(pending_reservation=pending)
        assert w.fetch_pending_reservation(MINER) is pending

    def test_returns_none_when_cc_has_none(self):
        w = make_watcher(pending_reservation=None)
        assert w.fetch_pending_reservation(MINER) is None

    def test_swallows_cc_exception(self):
        # Forward loop should never see an exception bubble up — a transient
        # RPC failure means "no pending I can act on this step", same as None.
        w = make_watcher(pending_reservation=None)
        w.contract_client.get_pending_reservation_extension.side_effect = RuntimeError('rpc flake')
        assert w.fetch_pending_reservation(MINER) is None


# =============================================================================
# Timeout side — same shape, abbreviated coverage
# =============================================================================


class TestMaybeProposeTimeout:
    def test_tier0_proposes_on_visibility(self):
        # BTC tier-0: blocks_needed=90, anchor=max(current=1000, timeout=1050)=1050,
        # target=1140.
        w = make_watcher(pending_timeout=None)
        result = w.maybe_propose_timeout(
            swap_id=42,
            dest_chain_id='btc',
            current_block=1000,
            timeout_block=1050,
            observed_confirmations=0,
            extension_count=0,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is True
        kwargs = w.contract_client.propose_extend_timeout.call_args.kwargs
        assert kwargs['swap_id'] == 42
        assert kwargs['target_block'] == 1140

    def test_tier1_requires_confirmations(self):
        w = make_watcher(pending_timeout=None)
        result = w.maybe_propose_timeout(
            swap_id=42,
            dest_chain_id='btc',
            current_block=1000,
            timeout_block=1100,
            observed_confirmations=0,
            extension_count=1,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is False

    def test_skips_when_at_cap(self):
        w = make_watcher(pending_timeout=None)
        result = w.maybe_propose_timeout(
            swap_id=42,
            dest_chain_id='btc',
            current_block=1000,
            timeout_block=1100,
            observed_confirmations=1,
            extension_count=2,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is False

    def test_skips_when_pending(self):
        w = make_watcher(pending_timeout=PendingExtension(OTHER_HOTKEY, 1180, 990))
        result = w.maybe_propose_timeout(
            swap_id=42,
            dest_chain_id='btc',
            current_block=1000,
            timeout_block=1050,
            observed_confirmations=0,
            extension_count=0,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is False


class TestMaybeChallengeTimeout:
    def test_challenges_when_target_too_far(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_timeout(
            swap_id=42,
            dest_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is True

    def test_skips_when_we_are_the_submitter(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OUR_HOTKEY, target_block=2000, proposed_at=995),
        )
        result = w.maybe_challenge_timeout(
            swap_id=42,
            dest_chain_id='btc',
            observed_confirmations=1,
            current_block=1000,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is False


class TestMaybeFinalizeTimeout:
    def test_finalizes_when_window_elapsed(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=992),
        )
        result = w.maybe_finalize_timeout(
            swap_id=42,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_timeout(42),
        )
        # Returns the applied target so extend_fulfilled_near_timeout can
        # bump swap_tracker before enforce_swap_timeouts reads from it.
        assert result == 1180

    def test_skips_when_window_not_yet_elapsed(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=997),
        )
        result = w.maybe_finalize_timeout(
            swap_id=42,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is None

    def test_returns_none_when_contract_call_fails(self):
        w = make_watcher(
            pending_timeout=PendingExtension(OTHER_HOTKEY, target_block=1180, proposed_at=992),
            propose_raises=ContractError('NoSwap'),
        )
        result = w.maybe_finalize_timeout(
            swap_id=42,
            current_block=1000,
            challenge_window_blocks=8,
            pending=w.fetch_pending_timeout(42),
        )
        assert result is None


class TestFetchPendingTimeout:
    def test_returns_pending_when_cc_has_one(self):
        pending = PendingExtension(OTHER_HOTKEY, 1180, 990)
        w = make_watcher(pending_timeout=pending)
        assert w.fetch_pending_timeout(42) is pending

    def test_returns_none_when_cc_has_none(self):
        w = make_watcher(pending_timeout=None)
        assert w.fetch_pending_timeout(42) is None

    def test_swallows_cc_exception(self):
        w = make_watcher(pending_timeout=None)
        w.contract_client.get_pending_timeout_extension.side_effect = RuntimeError('rpc flake')
        assert w.fetch_pending_timeout(42) is None
