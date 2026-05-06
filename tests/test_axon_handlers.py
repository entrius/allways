"""Tests for allways.validator.axon_handlers.

Covers the easy-to-isolate pure helpers (hashing, SCALE encoders, direction
resolution, the synapse rejection helper, blacklist/priority coroutines) and
the validation layer of handle_swap_confirm — the security-critical surface
users and miners can reach directly via the axon. The vote_initiate success
path is exercised end-to-end in integration testing, not here.
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

from allways.chain_providers.base import TransactionInfo
from allways.classes import MinerPair
from allways.contract_client import ContractError
from allways.synapses import SwapConfirmSynapse
from allways.validator.axon_handlers import (
    blacklist_miner_activate,
    blacklist_swap_confirm,
    blacklist_swap_reserve,
    handle_swap_confirm,
    keccak256,
    priority_miner_activate,
    priority_swap_confirm,
    priority_swap_reserve,
    reject_synapse,
    resolve_swap_direction,
    scale_encode_extend_hash_input,
    scale_encode_initiate_hash_input,
    scale_encode_reserve_hash_input,
)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_pair(
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    rate: float = 350.0,
    counter_rate: float = 0.0,
) -> MinerPair:
    return MinerPair(
        uid=1,
        hotkey='5Fminer',
        from_chain=from_chain,
        from_address='bc1qminer',
        to_chain=to_chain,
        to_address='5Fminer_dest',
        rate=rate,
        rate_str=str(rate),
        counter_rate=counter_rate,
        counter_rate_str=str(counter_rate) if counter_rate else '',
    )


class TestKeccak256:
    def test_empty_input(self):
        # Known Keccak-256 of empty string (ethereum convention) — guards
        # against accidental swap to SHA3-256 (different IV/padding).
        expected = bytes.fromhex('c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470')
        assert keccak256(b'') == expected


class TestScaleEncodeReserveHashInput:
    def test_structure_lengths(self):
        miner_bytes = b'\x01' * 32
        from_addr = b'bc1qminer'
        encoded = scale_encode_reserve_hash_input(
            miner_bytes=miner_bytes,
            from_addr_bytes=from_addr,
            from_chain='btc',
            to_chain='tao',
            tao_amount=1_000,
            from_amount=2_000,
            to_amount=3_000,
        )
        # Expect: 32 (AccountId) + 1+len(from_addr) + 1+3 (btc) + 1+3 (tao) + 16+16+16 (u128s)
        expected_len = 32 + (1 + len(from_addr)) + (1 + 3) + (1 + 3) + 16 * 3
        assert len(encoded) == expected_len

    def test_miner_bytes_prefix(self):
        miner_bytes = b'\xaa' * 32
        encoded = scale_encode_reserve_hash_input(
            miner_bytes=miner_bytes,
            from_addr_bytes=b'x',
            from_chain='btc',
            to_chain='tao',
            tao_amount=0,
            from_amount=0,
            to_amount=0,
        )
        assert encoded[:32] == miner_bytes

    def test_u128_suffix_little_endian(self):
        encoded = scale_encode_reserve_hash_input(
            miner_bytes=b'\x00' * 32,
            from_addr_bytes=b'',
            from_chain='',
            to_chain='',
            tao_amount=1,
            from_amount=2,
            to_amount=3,
        )
        # Last 48 bytes = three u128s
        assert encoded[-48:-32] == (1).to_bytes(16, 'little')
        assert encoded[-32:-16] == (2).to_bytes(16, 'little')
        assert encoded[-16:] == (3).to_bytes(16, 'little')


class TestScaleEncodeExtendHashInput:
    def test_includes_miner_and_tx(self):
        miner_bytes = b'\x02' * 32
        encoded = scale_encode_extend_hash_input(miner_bytes, 'deadbeef')
        assert encoded[:32] == miner_bytes
        assert encoded[-len(b'deadbeef') :] == b'deadbeef'

    def test_empty_tx_hash(self):
        encoded = scale_encode_extend_hash_input(b'\x00' * 32, '')
        # 32 + 1 (compact zero length) = 33
        assert len(encoded) == 33


class TestScaleEncodeInitiateHashInput:
    def test_contains_all_string_fields(self):
        encoded = scale_encode_initiate_hash_input(
            miner_bytes=b'\x03' * 32,
            from_tx_hash='abcd',
            from_chain='btc',
            to_chain='tao',
            miner_from_address='bc1qminer',
            miner_to_address='5Fdest',
            rate='350',
            tao_amount=1,
            from_amount=2,
            to_amount=3,
        )
        for fragment in (b'abcd', b'btc', b'tao', b'bc1qminer', b'5Fdest', b'350'):
            assert fragment in encoded

    def test_amounts_are_final_48_bytes(self):
        encoded = scale_encode_initiate_hash_input(
            miner_bytes=b'\x00' * 32,
            from_tx_hash='',
            from_chain='',
            to_chain='',
            miner_from_address='',
            miner_to_address='',
            rate='',
            tao_amount=10,
            from_amount=20,
            to_amount=30,
        )
        assert encoded[-48:-32] == (10).to_bytes(16, 'little')
        assert encoded[-32:-16] == (20).to_bytes(16, 'little')
        assert encoded[-16:] == (30).to_bytes(16, 'little')


class TestResolveSwapDirection:
    def test_canonical_direction_returns_from_address_as_deposit(self):
        pair = _make_pair(from_chain='btc', to_chain='tao', rate=350.0)
        result = resolve_swap_direction(pair, 'btc', 'tao')
        assert result is not None
        from_chain, to_chain, deposit, fulfillment, rate, rate_str = result
        assert from_chain == 'btc'
        assert to_chain == 'tao'
        assert deposit == 'bc1qminer'
        assert fulfillment == '5Fminer_dest'
        assert rate == 350.0

    def test_reverse_direction_swaps_addresses(self):
        pair = _make_pair(from_chain='btc', to_chain='tao', rate=350.0, counter_rate=0.003)
        result = resolve_swap_direction(pair, 'tao', 'btc')
        assert result is not None
        _, _, deposit, fulfillment, rate, _ = result
        assert deposit == '5Fminer_dest'
        assert fulfillment == 'bc1qminer'
        assert rate == 0.003

    def test_zero_rate_returns_none(self):
        pair = _make_pair(rate=0.0)
        assert resolve_swap_direction(pair, 'btc', 'tao') is None

    def test_negative_rate_returns_none(self):
        pair = _make_pair(rate=-1.0)
        assert resolve_swap_direction(pair, 'btc', 'tao') is None

    def test_empty_synapse_chains_fall_back_to_commitment(self):
        pair = _make_pair(from_chain='btc', to_chain='tao', rate=350.0)
        result = resolve_swap_direction(pair, '', '')
        assert result is not None
        assert result[0] == 'btc'
        assert result[1] == 'tao'


class TestRejectSynapse:
    def test_sets_accepted_false_and_reason(self):
        synapse = MagicMock()
        reject_synapse(synapse, 'bad input')
        assert synapse.accepted is False
        assert synapse.rejection_reason == 'bad input'

    def test_no_context_no_log_error(self):
        synapse = MagicMock()
        with patch('allways.validator.axon_handlers.bt.logging.debug') as dbg:
            reject_synapse(synapse, 'why', context='')
        dbg.assert_not_called()

    def test_with_context_logs_debug(self):
        synapse = MagicMock()
        with patch('allways.validator.axon_handlers.bt.logging.debug') as dbg:
            reject_synapse(synapse, 'reason', context='SomeSynapse(x)')
        dbg.assert_called_once_with('SomeSynapse(x): reason')


class TestBlacklistMinerActivate:
    def _validator(self, hotkeys):
        v = MagicMock()
        v.metagraph.hotkeys = hotkeys
        return v

    def test_missing_dendrite_blacklisted(self):
        validator = self._validator(['5Fminer'])
        synapse = MagicMock()
        synapse.dendrite = None
        blocked, reason = _run(blacklist_miner_activate(validator, synapse))
        assert blocked is True
        assert 'dendrite' in reason.lower() or 'hotkey' in reason.lower()

    def test_missing_hotkey_blacklisted(self):
        validator = self._validator(['5Fminer'])
        synapse = MagicMock()
        synapse.dendrite = MagicMock()
        synapse.dendrite.hotkey = None
        blocked, _ = _run(blacklist_miner_activate(validator, synapse))
        assert blocked is True

    def test_unregistered_hotkey_blacklisted(self):
        validator = self._validator(['5Fminer'])
        synapse = MagicMock()
        synapse.dendrite.hotkey = '5Funknown'
        blocked, reason = _run(blacklist_miner_activate(validator, synapse))
        assert blocked is True
        assert 'unregistered' in reason.lower()

    def test_registered_hotkey_allowed(self):
        validator = self._validator(['5Fminer'])
        synapse = MagicMock()
        synapse.dendrite.hotkey = '5Fminer'
        blocked, _ = _run(blacklist_miner_activate(validator, synapse))
        assert blocked is False


class TestBlacklistSwapReserve:
    def test_pass_through_any_hotkey(self):
        # Pass-through by design — field checks happen later in handle_swap_reserve
        validator = MagicMock()
        synapse = MagicMock()
        blocked, reason = _run(blacklist_swap_reserve(validator, synapse))
        assert blocked is False
        assert reason == 'Passed'


class TestBlacklistSwapConfirm:
    def test_pass_through_any_hotkey(self):
        validator = MagicMock()
        synapse = MagicMock()
        blocked, reason = _run(blacklist_swap_confirm(validator, synapse))
        assert blocked is False
        assert reason == 'Passed'


class TestPriorityFunctions:
    def _validator(self, hotkeys, stakes):
        v = MagicMock()
        v.metagraph.hotkeys = hotkeys
        v.metagraph.S = stakes
        return v

    def test_miner_activate_returns_stake(self):
        validator = self._validator(['5Fa', '5Fb'], [100.0, 250.0])
        synapse = MagicMock()
        synapse.dendrite.hotkey = '5Fb'
        assert _run(priority_miner_activate(validator, synapse)) == 250.0

    def test_miner_activate_unknown_hotkey_returns_zero(self):
        validator = self._validator(['5Fa'], [100.0])
        synapse = MagicMock()
        synapse.dendrite.hotkey = '5Funknown'
        assert _run(priority_miner_activate(validator, synapse)) == 0.0

    def test_swap_reserve_flat_priority(self):
        # User-facing synapses use a flat priority
        assert _run(priority_swap_reserve(MagicMock(), MagicMock())) == 1.0

    def test_swap_confirm_flat_priority(self):
        assert _run(priority_swap_confirm(MagicMock(), MagicMock())) == 1.0


# ---------------------------------------------------------------------------
# handle_swap_confirm: end-to-end validation layer
# ---------------------------------------------------------------------------


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
    validator.axon_subtensor.get_current_block.return_value = block
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

    def test_queued_entry_uses_reservation_amounts(self):
        """The contract-reserved amounts are authoritative. A queued entry
        must persist those, not any user-supplied value, so the later
        auto-initiate hashes match what the miner was reserved under."""
        validator = make_validator(reservation_data=(777_000_000, 55_000, 999_000_000))
        validator.axon_chain_providers['btc'].verify_transaction.return_value = make_tx_info(
            confirmed=False,
            confirmations=1,
        )
        run_handler(validator, make_synapse())
        queued_item = validator.state_store.enqueue.call_args[0][0]
        assert queued_item.tao_amount == 777_000_000
        assert queued_item.from_amount == 55_000
        assert queued_item.to_amount == 999_000_000


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
