"""Tests for allways.commitments — commitment string parsing + query_map read."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.commitments import parse_commitment_data, read_miner_commitments


class TestParseCommitmentData:
    def test_valid_two_rates(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:340:350'
        pair = parse_commitment_data(raw, uid=1, hotkey='hk1')
        assert pair is not None
        assert pair.uid == 1
        assert pair.hotkey == 'hk1'
        assert pair.from_chain == 'btc'
        assert pair.from_address == 'bc1qaddr'
        assert pair.to_chain == 'tao'
        assert pair.to_address == '5Caddr'
        assert pair.rate == 340.0
        assert pair.rate_str == '340'
        assert pair.counter_rate == 350.0
        assert pair.counter_rate_str == '350'

    def test_valid_same_rate_both_directions(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345:345'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate == 345.0
        assert pair.counter_rate == 345.0

    def test_normalization_swaps_rates(self):
        """When posted as tao->btc, normalization flips to btc->tao and swaps rates."""
        raw = 'v1:tao:5Caddr:btc:bc1qaddr:340:350'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.from_chain == 'btc'
        assert pair.to_chain == 'tao'
        assert pair.from_address == 'bc1qaddr'
        assert pair.to_address == '5Caddr'
        # Original forward rate (340) was for tao->btc, now becomes reverse
        assert pair.rate == 350.0
        assert pair.rate_str == '350'
        assert pair.counter_rate == 340.0
        assert pair.counter_rate_str == '340'

    def test_fractional_rates(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345.12:350.45'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate == 345.12
        assert pair.rate_str == '345.12'
        assert pair.counter_rate == 350.45
        assert pair.counter_rate_str == '350.45'

    def test_get_rate_for_direction(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:340:350'
        pair = parse_commitment_data(raw)
        # Forward (btc -> tao)
        rate, rate_str = pair.get_rate_for_direction('btc')
        assert rate == 340.0
        assert rate_str == '340'
        # Reverse (tao -> btc)
        rate, rate_str = pair.get_rate_for_direction('tao')
        assert rate == 350.0
        assert rate_str == '350'

    def test_get_rate_for_direction_after_normalization(self):
        """Full pipeline: tao->btc commitment normalizes, then direction lookup works."""
        raw = 'v1:tao:5Caddr:btc:bc1qaddr:340:350'
        pair = parse_commitment_data(raw)
        # After normalization: source=btc, dest=tao
        # Original 340 was tao->btc (now reverse), 350 was btc->tao (now forward)
        fwd_rate, fwd_str = pair.get_rate_for_direction('btc')
        rev_rate, rev_str = pair.get_rate_for_direction('tao')
        assert fwd_rate == 350.0
        assert fwd_str == '350'
        assert rev_rate == 340.0
        assert rev_str == '340'

    def test_wrong_part_count_too_few(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:345') is None

    def test_wrong_part_count_too_many(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:340:350:extra') is None

    def test_wrong_version(self):
        assert parse_commitment_data('v2:btc:addr:tao:addr:340:350') is None

    def test_no_version_prefix(self):
        assert parse_commitment_data('3:btc:addr:tao:addr:340:350') is None

    def test_unsupported_source_chain(self):
        assert parse_commitment_data('v1:eth:addr:tao:addr:340:350') is None

    def test_unsupported_dest_chain(self):
        assert parse_commitment_data('v1:btc:addr:eth:addr:340:350') is None

    def test_invalid_rate_not_a_number(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:abc:350') is None

    def test_invalid_reverse_rate_not_a_number(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:340:abc') is None

    def test_empty_string(self):
        assert parse_commitment_data('') is None

    def test_rate_zero(self):
        pair = parse_commitment_data('v1:btc:addr:tao:addr:0:0')
        assert pair is not None
        assert pair.rate == 0.0
        assert pair.counter_rate == 0.0

    def test_high_precision_rate_normalized_to_sig_figs(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:0.0001234567:340.987654'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate_str == '0.00012346'
        assert pair.counter_rate_str == '340.99'

    def test_normalized_rate_str_round_trips_to_float(self):
        """Scoring uses .rate (float), consensus hash uses .rate_str — they must agree."""
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:0.0001234567:340.987654'
        pair = parse_commitment_data(raw)
        assert float(pair.rate_str) == pair.rate
        assert float(pair.counter_rate_str) == pair.counter_rate

    def test_already_canonical_rate_is_unchanged(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345:0.5'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate_str == '345'
        assert pair.counter_rate_str == '0.5'

    def test_normalization_strips_trailing_zeros(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345.000000:0.500000'
        pair = parse_commitment_data(raw)
        assert pair.rate_str == '345'
        assert pair.counter_rate_str == '0.5'

    def test_negative_rate_rejected(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:-1:340') is None

    def test_negative_counter_rate_rejected(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:340:-1') is None

    def test_nan_rate_rejected(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:nan:340') is None

    def test_inf_rate_rejected(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:inf:340') is None
        assert parse_commitment_data('v1:btc:addr:tao:addr:340:-inf') is None

    def test_disabled_direction_produces_zero_dest_amount(self):
        """Full guard chain: disabled direction → rate=0 → to_amount=0 → contract rejects."""
        from allways.utils.rate import calculate_to_amount

        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345:0'
        pair = parse_commitment_data(raw)
        # Validator calls get_rate_for_direction for the disabled direction
        rate, rate_str = pair.get_rate_for_direction('tao')
        assert rate == 0.0
        assert rate <= 0  # validator guard: if selected_rate <= 0: reject
        # Even if the guard were bypassed, calculate_to_amount returns 0
        to_amount = calculate_to_amount(1_000_000_000, rate_str, is_reverse=True, to_decimals=9, from_decimals=8)
        assert to_amount == 0  # contract would reject with InvalidAmount

    def test_single_direction_forward_only(self):
        """Miner supports only BTC→TAO (counter_rate=0 means TAO→BTC not offered)."""
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:345:0'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate == 345.0
        assert pair.counter_rate == 0.0
        # Forward direction returns valid rate
        rate, rate_str = pair.get_rate_for_direction('btc')
        assert rate == 345.0
        # Counter direction returns 0
        rate, rate_str = pair.get_rate_for_direction('tao')
        assert rate == 0.0

    def test_single_direction_counter_only(self):
        """Miner posts tao→btc only. After normalization, rate=0, counter_rate has the value."""
        raw = 'v1:tao:5Caddr:btc:bc1qaddr:345:0'
        pair = parse_commitment_data(raw)
        assert pair is not None
        # Normalization flips: btc→tao becomes source→dest
        # Original rate 345 was tao→btc (now counter), original 0 was btc→tao (now forward)
        assert pair.rate == 0.0
        assert pair.counter_rate == 345.0
        # BTC→TAO returns 0 (not supported)
        rate, _ = pair.get_rate_for_direction('btc')
        assert rate == 0.0
        # TAO→BTC returns 345
        rate, _ = pair.get_rate_for_direction('tao')
        assert rate == 345.0

    def test_default_uid_and_hotkey(self):
        pair = parse_commitment_data('v1:btc:addr:tao:addr:1.0:2.0')
        assert pair.uid == 0
        assert pair.hotkey == ''

    def test_same_chain(self):
        assert parse_commitment_data('v1:btc:addr:btc:addr:340:350') is None


class TestReadMinerCommitmentsQueryMap:
    """Coverage for the query_map-batched read.

    ``read_miner_commitments`` used to do N separate ``substrate.query`` calls
    in a for-loop. It now uses ``substrate.query_map`` to pull every
    ``(hotkey, commitment)`` pair under ``Commitments.CommitmentOf(netuid)``
    in a single RPC. These tests mock the substrate interface to exercise
    the new path — the hotkey→uid filter, the decode fallthrough, and the
    "commitment exists but miner dereg'd" dropout.
    """

    def make_subtensor(self, hotkeys: list[str], rows: list[tuple[str, str]]) -> MagicMock:
        """Build a mock subtensor whose metagraph and query_map match the args.

        ``rows`` is a list of (hotkey, raw_commitment_text) pairs as they'd
        come back from Commitments.CommitmentOf. Each raw text is wrapped in
        a fake metadata object that the real ``decode_commitment_field``
        can parse.
        """
        subtensor = MagicMock()
        metagraph = SimpleNamespace(
            hotkeys=list(hotkeys),
            n=SimpleNamespace(item=lambda: len(hotkeys)),
        )
        subtensor.metagraph.return_value = metagraph

        def fake_query_map(module, storage_function, params):
            for hotkey, raw in rows:
                key = SimpleNamespace(value=hotkey)
                # Fake the ink!-shaped metadata that decode_commitment_field walks.
                metadata = SimpleNamespace(value={'info': {'fields': [{'Raw0': '0x' + raw.encode().hex()}]}})
                yield key, metadata

        subtensor.substrate.query_map.side_effect = fake_query_map
        return subtensor

    def test_returns_parsed_pairs_for_every_registered_miner(self):
        subtensor = self.make_subtensor(
            hotkeys=['hk_a', 'hk_b'],
            rows=[
                ('hk_a', 'v1:btc:bc1qaddr_a:tao:5C_a:340:350'),
                ('hk_b', 'v1:btc:bc1qaddr_b:tao:5C_b:345:355'),
            ],
        )
        pairs = read_miner_commitments(subtensor, netuid=7)
        assert len(pairs) == 2
        by_hotkey = {p.hotkey: p for p in pairs}
        assert by_hotkey['hk_a'].uid == 0
        assert by_hotkey['hk_b'].uid == 1
        assert by_hotkey['hk_a'].rate == 340.0
        assert by_hotkey['hk_b'].rate == 345.0

    def test_drops_dereg_hotkey_still_in_storage(self):
        """A miner can deregister before their commitment is cleared from
        Commitments.CommitmentOf. Those rows must be skipped."""
        subtensor = self.make_subtensor(
            hotkeys=['hk_live'],  # only hk_live is in the metagraph
            rows=[
                ('hk_live', 'v1:btc:bc1qlive:tao:5Clive:340:350'),
                ('hk_ghost', 'v1:btc:bc1qghost:tao:5Cghost:999:999'),
            ],
        )
        pairs = read_miner_commitments(subtensor, netuid=7)
        assert [p.hotkey for p in pairs] == ['hk_live']

    def test_single_query_map_call(self):
        """Regression guard: we must not fall back into an N-RPC loop."""
        subtensor = self.make_subtensor(
            hotkeys=['hk_a', 'hk_b', 'hk_c'],
            rows=[('hk_a', 'v1:btc:a:tao:a:1:1')],
        )
        read_miner_commitments(subtensor, netuid=7)
        assert subtensor.substrate.query_map.call_count == 1
        # And no per-hotkey query() calls leaked back in.
        assert subtensor.substrate.query.call_count == 0

    def test_transient_error_returns_empty_list(self):
        """ConnectionError / TimeoutError during query_map shouldn't raise."""
        subtensor = MagicMock()
        subtensor.metagraph.return_value = SimpleNamespace(hotkeys=['hk_a'], n=SimpleNamespace(item=lambda: 1))
        subtensor.substrate.query_map.side_effect = ConnectionError('websocket dead')
        with patch('allways.commitments.bt.logging.warning'):
            pairs = read_miner_commitments(subtensor, netuid=7)
        assert pairs == []
