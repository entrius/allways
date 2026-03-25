"""Tests for allways.commitments — commitment string parsing."""

from allways.commitments import parse_commitment_data


class TestParseCommitmentData:
    def test_valid_btc_tao(self):
        raw = 'v1:btc:bc1qaddr:tao:5Caddr:0.00015'
        pair = parse_commitment_data(raw, uid=1, hotkey='hk1')
        assert pair is not None
        assert pair.uid == 1
        assert pair.hotkey == 'hk1'
        assert pair.source_chain == 'btc'
        assert pair.source_address == 'bc1qaddr'
        assert pair.dest_chain == 'tao'
        assert pair.dest_address == '5Caddr'
        assert pair.rate == 0.00015
        assert pair.rate_str == '0.00015'

    def test_valid_tao_btc(self):
        raw = 'v1:tao:5Caddr:btc:bc1qaddr:6666.67'
        pair = parse_commitment_data(raw)
        assert pair is not None
        assert pair.source_chain == 'tao'
        assert pair.dest_chain == 'btc'

    def test_wrong_part_count_too_few(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr') is None

    def test_wrong_part_count_too_many(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:0.1:extra') is None

    def test_wrong_version(self):
        assert parse_commitment_data('v2:btc:addr:tao:addr:0.1') is None

    def test_no_version_prefix(self):
        assert parse_commitment_data('1:btc:addr:tao:addr:0.1') is None

    def test_unsupported_source_chain(self):
        assert parse_commitment_data('v1:eth:addr:tao:addr:0.1') is None

    def test_unsupported_dest_chain(self):
        assert parse_commitment_data('v1:btc:addr:eth:addr:0.1') is None

    def test_invalid_rate_not_a_number(self):
        assert parse_commitment_data('v1:btc:addr:tao:addr:abc') is None

    def test_empty_string(self):
        assert parse_commitment_data('') is None

    def test_rate_zero(self):
        pair = parse_commitment_data('v1:btc:addr:tao:addr:0')
        assert pair is not None
        assert pair.rate == 0.0

    def test_default_uid_and_hotkey(self):
        pair = parse_commitment_data('v1:btc:addr:tao:addr:1.0')
        assert pair.uid == 0
        assert pair.hotkey == ''

    def test_colon_in_address(self):
        # Extra colons split into too many parts
        assert parse_commitment_data('v1:btc:addr:with:colon:tao:addr:0.1') is None
