"""Tests for allways.commitments — commitment string parsing."""

from allways.commitments import parse_commitment_data


class TestParseCommitmentData:
    def test_valid_two_rates(self):
        raw = 'v3:btc:bc1qaddr:tao:5Caddr:340:350'
        pair = parse_commitment_data(raw, uid=1, hotkey='hk1')
        assert pair is not None
        assert pair.uid == 1
        assert pair.hotkey == 'hk1'
        assert pair.source_chain == 'btc'
        assert pair.source_address == 'bc1qaddr'
        assert pair.dest_chain == 'tao'
        assert pair.dest_address == '5Caddr'
        assert pair.rate == 340.0
        assert pair.rate_str == '340'
        assert pair.counter_rate == 350.0
        assert pair.counter_rate_str == '350'

    def test_valid_same_rate_both_directions(self):
        raw = 'v3:btc:bc1qaddr:tao:5Caddr:345:345'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate == 345.0
        assert pair.counter_rate == 345.0

    def test_normalization_swaps_rates(self):
        """When posted as tao->btc, normalization flips to btc->tao and swaps rates."""
        raw = 'v3:tao:5Caddr:btc:bc1qaddr:340:350'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.source_chain == 'btc'
        assert pair.dest_chain == 'tao'
        assert pair.source_address == 'bc1qaddr'
        assert pair.dest_address == '5Caddr'
        # Original forward rate (340) was for tao->btc, now becomes reverse
        assert pair.rate == 350.0
        assert pair.rate_str == '350'
        assert pair.counter_rate == 340.0
        assert pair.counter_rate_str == '340'

    def test_fractional_rates(self):
        raw = 'v3:btc:bc1qaddr:tao:5Caddr:345.12:350.45'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.rate == 345.12
        assert pair.rate_str == '345.12'
        assert pair.counter_rate == 350.45
        assert pair.counter_rate_str == '350.45'

    def test_get_rate_for_direction(self):
        raw = 'v3:btc:bc1qaddr:tao:5Caddr:340:350'
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
        raw = 'v3:tao:5Caddr:btc:bc1qaddr:340:350'
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
        assert parse_commitment_data('v3:btc:addr:tao:addr:345') is None

    def test_wrong_part_count_too_many(self):
        assert parse_commitment_data('v3:btc:addr:tao:addr:340:350:extra') is None

    def test_wrong_version(self):
        assert parse_commitment_data('v2:btc:addr:tao:addr:340:350') is None

    def test_no_version_prefix(self):
        assert parse_commitment_data('3:btc:addr:tao:addr:340:350') is None

    def test_unsupported_source_chain(self):
        assert parse_commitment_data('v3:eth:addr:tao:addr:340:350') is None

    def test_unsupported_dest_chain(self):
        assert parse_commitment_data('v3:btc:addr:eth:addr:340:350') is None

    def test_invalid_rate_not_a_number(self):
        assert parse_commitment_data('v3:btc:addr:tao:addr:abc:350') is None

    def test_invalid_reverse_rate_not_a_number(self):
        assert parse_commitment_data('v3:btc:addr:tao:addr:340:abc') is None

    def test_empty_string(self):
        assert parse_commitment_data('') is None

    def test_rate_zero(self):
        pair = parse_commitment_data('v3:btc:addr:tao:addr:0:0')
        assert pair is not None
        assert pair.rate == 0.0
        assert pair.counter_rate == 0.0

    def test_single_direction_forward_only(self):
        """Miner supports only BTC→TAO (counter_rate=0 means TAO→BTC not offered)."""
        raw = 'v3:btc:bc1qaddr:tao:5Caddr:345:0'
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
        raw = 'v3:tao:5Caddr:btc:bc1qaddr:345:0'
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
        pair = parse_commitment_data('v3:btc:addr:tao:addr:1.0:2.0')
        assert pair.uid == 0
        assert pair.hotkey == ''

    def test_same_chain(self):
        assert parse_commitment_data('v3:btc:addr:btc:addr:340:350') is None
