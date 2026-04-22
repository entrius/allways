"""Tests for validator axon_handlers.handle_swap_confirm.

Covers every rejection branch plus the queued-confirmation path. The
vote_initiate success path is not unit-tested here — it requires mocking
extrinsic submission and is exercised end-to-end in integration testing.
These tests focus on the validation layer, which is the security-critical
surface users and miners can reach directly via the axon.
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

from allways.chain_providers.base import TransactionInfo
from allways.classes import MinerPair
from allways.contract_client import ContractError
from allways.synapses import SwapConfirmSynapse
from allways.validator.axon_handlers import handle_swap_confirm


def make_synapse(
    reservation_id: str = 'miner-hotkey',
    from_tx_hash: str = 'abc123',
    from_tx_proof: str = 'proof',
    from_address: str = 'bc1-user',
    to_address: str = '5user',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
) -> SwapConfirmSynapse:
    return SwapConfirmSynapse(
        reservation_id=reservation_id,
        from_tx_hash=from_tx_hash,
        from_tx_proof=from_tx_proof,
        from_address=from_address,
        to_address=to_address,
        from_chain=from_chain,
        to_chain=to_chain,
    )


def make_commitment(
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    counter_rate: float = 0.0029,
    counter_rate_str: str = '0.0029',
) -> MinerPair:
    return MinerPair(
        uid=1,
        hotkey='miner-hotkey',
        from_chain=from_chain,
        from_address='bc1-miner',
        to_chain=to_chain,
        to_address='5miner',
        rate=345.0,
        rate_str='345',
        counter_rate=counter_rate,
        counter_rate_str=counter_rate_str,
    )


def make_tx_info(
    *,
    confirmed: bool = True,
    confirmations: int = 6,
    block_number: int | None = 500,
) -> TransactionInfo:
    return TransactionInfo(
        tx_hash='abc123',
        confirmed=confirmed,
        sender='bc1-user',
        recipient='bc1-miner',
        amount=100_000,
        block_number=block_number,
        confirmations=confirmations,
    )


def make_validator(
    *,
    block: int = 1000,
    reserved_until: int = 2000,
    reservation_data: tuple | None = (345_000_000, 100_000, 345_000_000),
    providers: dict | None = None,
) -> MagicMock:
    """Build a Validator mock with default-happy contract/chain state.

    Individual tests override specific attributes to simulate each branch.
    reservation_data tuple mirrors the on-chain layout used by
    handle_swap_confirm: (tao_amount, source_amount, dest_amount).
    """
    validator = MagicMock()
    validator.block = block
    validator.config.netuid = 2
    validator.axon_lock = threading.Lock()

    contract = MagicMock()
    contract.get_miner_reserved_until.return_value = reserved_until
    contract.get_reservation_data.return_value = reservation_data
    validator.axon_contract_client = contract

    if providers is None:
        btc = MagicMock()
        btc.is_valid_address.return_value = True
        btc.verify_transaction.return_value = make_tx_info()
        btc.get_chain.return_value = MagicMock(min_confirmations=6)

        tao = MagicMock()
        tao.is_valid_address.return_value = True
        tao.get_chain.return_value = MagicMock(min_confirmations=12)

        providers = {'btc': btc, 'tao': tao}
    validator.axon_chain_providers = providers

    validator.state_store = MagicMock()
    validator.wallet = MagicMock()
    return validator


_DEFAULT = object()  # distinct from None so tests can request "no commitment" explicitly


def run_handler(validator, synapse, commitment=_DEFAULT):
    """Patch read_miner_commitment and drive the async handler synchronously.

    Omitting ``commitment`` yields the happy-path default; passing ``None``
    simulates a miner with no commitment on-chain.
    """
    cmt = make_commitment() if commitment is _DEFAULT else commitment
    with patch('allways.validator.axon_handlers.read_miner_commitment', return_value=cmt):
        return asyncio.run(handle_swap_confirm(validator, synapse))


# ---------------------------------------------------------------------------
# Pre-lock input validation
# ---------------------------------------------------------------------------


class TestPreLockValidation:
    def test_rejects_missing_from_address(self):
        result = run_handler(make_validator(), make_synapse(from_address=''))
        assert result.accepted is False
        assert 'Missing source address' in result.rejection_reason

    def test_rejects_missing_from_tx_proof(self):
        result = run_handler(make_validator(), make_synapse(from_tx_proof=''))
        assert result.accepted is False
        assert 'Missing source address or proof' in result.rejection_reason

    def test_rejects_missing_to_address(self):
        """Empty to_address would otherwise be propagated into vote_initiate,
        locking a miner for a swap with no recoverable destination."""
        result = run_handler(make_validator(), make_synapse(to_address=''))
        assert result.accepted is False
        assert 'Missing destination address' in result.rejection_reason

    def test_pre_lock_rejections_do_not_touch_contract(self):
        """Input validation runs before axon_lock — no contract calls made."""
        validator = make_validator()
        run_handler(validator, make_synapse(to_address=''))
        validator.axon_contract_client.get_miner_reserved_until.assert_not_called()
        validator.axon_contract_client.get_reservation_data.assert_not_called()


# ---------------------------------------------------------------------------
# Reservation state
# ---------------------------------------------------------------------------


class TestReservationValidation:
    def test_rejects_expired_reservation(self):
        validator = make_validator(block=2000, reserved_until=500)
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'No active reservation' in result.rejection_reason

    def test_rejects_reservation_at_exact_block_boundary(self):
        """Handler uses `reserved_until < validator.block`; equal should pass
        that gate. We verify the next gate (reservation_data) is reached."""
        validator = make_validator(block=1000, reserved_until=1000, reservation_data=None)
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Reservation data not found' in result.rejection_reason

    def test_rejects_missing_reservation_data(self):
        validator = make_validator(reservation_data=None)
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Reservation data not found' in result.rejection_reason


# ---------------------------------------------------------------------------
# Commitment and swap direction
# ---------------------------------------------------------------------------


class TestCommitmentValidation:
    def test_rejects_when_no_commitment_on_chain(self):
        result = run_handler(make_validator(), make_synapse(), commitment=None)
        assert result.accepted is False
        assert 'No valid commitment' in result.rejection_reason

    def test_rejects_commitment_with_same_from_to_chain(self):
        degenerate = MinerPair(
            uid=1,
            hotkey='miner-hotkey',
            from_chain='btc',
            from_address='bc1-a',
            to_chain='btc',
            to_address='bc1-b',
            rate=1.0,
            rate_str='1',
        )
        result = run_handler(make_validator(), make_synapse(), commitment=degenerate)
        assert result.accepted is False
        assert 'No valid commitment' in result.rejection_reason

    def test_rejects_unsupported_direction_when_counter_rate_zero(self):
        """Miner opted out of the TAO→BTC leg (counter_rate=0) — a TAO-sourced
        swap request must be rejected with the direction-support message."""
        one_way = make_commitment(counter_rate=0.0, counter_rate_str='')
        synapse = make_synapse(from_chain='tao', to_chain='btc', from_address='5user', to_address='bc1-dest')
        result = run_handler(make_validator(), synapse, commitment=one_way)
        assert result.accepted is False
        assert 'does not support this swap direction' in result.rejection_reason


# ---------------------------------------------------------------------------
# Chain provider availability and address format
# ---------------------------------------------------------------------------


class TestChainProviderValidation:
    def test_rejects_unsupported_from_chain(self):
        validator = make_validator(providers={'tao': MagicMock(is_valid_address=MagicMock(return_value=True))})
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Unsupported chain: btc' in result.rejection_reason

    def test_rejects_invalid_from_tx_proof(self):
        """Without a valid signature over the tx hash from from_address, a caller
        could hijack someone else's on-chain source tx and redirect fulfillment
        to an attacker-controlled to_address."""
        validator = make_validator(reservation_data=(345_000_000, 100_000, 345_000_000))
        validator.axon_chain_providers['btc'].verify_from_proof.return_value = False
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Invalid source tx proof' in result.rejection_reason
        validator.axon_chain_providers['btc'].verify_transaction.assert_not_called()

    def test_rejects_unsupported_to_chain(self):
        """Need the source provider present (to reach the dest check), but
        strip the dest provider — validator must not continue without a way
        to validate the destination address format."""
        btc = MagicMock()
        btc.is_valid_address.return_value = True
        btc.verify_transaction.return_value = make_tx_info()
        btc.get_chain.return_value = MagicMock(min_confirmations=6)
        validator = make_validator(providers={'btc': btc})
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Unsupported destination chain: tao' in result.rejection_reason

    def test_rejects_invalid_to_address_format(self):
        """Non-empty but malformed to_address (e.g. wrong SS58 checksum)
        must be rejected before the tx is even looked up."""
        validator = make_validator()
        validator.axon_chain_providers['tao'].is_valid_address.return_value = False
        synapse = make_synapse(to_address='not-a-valid-ss58')
        result = run_handler(validator, synapse)
        assert result.accepted is False
        assert 'Invalid destination address format' in result.rejection_reason

    def test_invalid_to_address_rejected_before_source_tx_lookup(self):
        """The destination check runs before the source tx fetch — a bad
        to_address should not burn a block-scan request on the source chain."""
        validator = make_validator()
        validator.axon_chain_providers['tao'].is_valid_address.return_value = False
        run_handler(validator, make_synapse(to_address='garbage'))
        validator.axon_chain_providers['btc'].verify_transaction.assert_not_called()


# ---------------------------------------------------------------------------
# Source tx verification and queuing
# ---------------------------------------------------------------------------


class TestSourceTxVerification:
    def test_rejects_when_source_tx_not_found(self):
        validator = make_validator()
        validator.axon_chain_providers['btc'].verify_transaction.return_value = None
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Source transaction not found' in result.rejection_reason

    def test_queues_when_source_tx_unconfirmed(self):
        """Visible on-chain but below min_confirmations → accepted + queued."""
        validator = make_validator()
        validator.axon_chain_providers['btc'].verify_transaction.return_value = make_tx_info(
            confirmed=False,
            confirmations=2,
            block_number=None,
        )
        result = run_handler(validator, make_synapse())
        assert result.accepted is True
        assert 'Queued' in (result.rejection_reason or '')
        assert '2/6 confirmations' in result.rejection_reason
        validator.state_store.enqueue.assert_called_once()

    def test_queued_entry_captures_reservation_and_tx_block(self):
        """Reserved amounts (for initiate-hash match) and tx block (for replay
        block_hint) must persist on the queued entry."""
        validator = make_validator(reservation_data=(777_000_000, 55_000, 999_000_000))
        validator.axon_chain_providers['btc'].verify_transaction.return_value = make_tx_info(
            confirmed=False,
            confirmations=1,
            block_number=987_654,
        )
        run_handler(validator, make_synapse())
        queued_item = validator.state_store.enqueue.call_args[0][0]
        assert queued_item.tao_amount == 777_000_000
        assert queued_item.from_amount == 55_000
        assert queued_item.to_amount == 999_000_000
        assert queued_item.from_tx_block == 987_654


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_contract_rejection_surfaces_generic_message(self):
        """Known contract rejection → short user-facing reason, not a raw
        ContractTrapped string."""
        validator = make_validator()
        validator.axon_contract_client.get_miner_reserved_until.side_effect = ContractError('ContractTrapped: ...')
        with patch('allways.validator.axon_handlers.is_contract_rejection', return_value=True):
            result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'Contract rejected' in result.rejection_reason

    def test_non_rejection_contract_error_surfaces_raw(self):
        """RPC/connectivity errors (not contract rejections) include detail
        so the caller can distinguish transient failures from rejections."""
        validator = make_validator()
        validator.axon_contract_client.get_miner_reserved_until.side_effect = ContractError('connection reset')
        with patch('allways.validator.axon_handlers.is_contract_rejection', return_value=False):
            result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'connection reset' in result.rejection_reason

    def test_unexpected_exception_is_caught_and_reported(self):
        """Handler must never raise into the axon — any exception becomes a
        rejection so the synapse response is well-formed."""
        validator = make_validator()
        validator.axon_contract_client.get_miner_reserved_until.side_effect = RuntimeError('boom')
        result = run_handler(validator, make_synapse())
        assert result.accepted is False
        assert 'boom' in result.rejection_reason
