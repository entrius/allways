"""Tests for allways.validator.axon_handlers pure helpers and sync guards.

These cover the easy-to-isolate pieces of axon_handlers: hashing, SCALE encoders,
direction resolution, the synapse rejection helper, and blacklist/priority coroutines.
Handler bodies that drive consensus voting are not covered here.
"""

import asyncio
from unittest.mock import MagicMock

from allways.classes import MinerPair
from allways.validator.axon_handlers import (
    blacklist_miner_activate,
    blacklist_swap_confirm,
    blacklist_swap_reserve,
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
        # Known Keccak-256 of empty string (ethereum convention)
        expected = bytes.fromhex('c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470')
        assert keccak256(b'') == expected

    def test_deterministic(self):
        data = b'hello world'
        assert keccak256(data) == keccak256(data)

    def test_output_is_32_bytes(self):
        assert len(keccak256(b'anything')) == 32

    def test_different_inputs_produce_different_hashes(self):
        assert keccak256(b'a') != keccak256(b'b')


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
        assert encoded[-len(b'deadbeef'):] == b'deadbeef'

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
        # Should not raise when context empty
        synapse = MagicMock()
        reject_synapse(synapse, 'why', context='')
        assert synapse.rejection_reason == 'why'

    def test_with_context_logs_debug(self):
        synapse = MagicMock()
        reject_synapse(synapse, 'reason', context='SomeSynapse(x)')
        assert synapse.accepted is False


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
