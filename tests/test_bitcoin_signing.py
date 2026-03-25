"""Tests for BIP-137 message signing and verification in BitcoinProvider."""

from bitcoin_message_tool.bmt import sign_message, verify_message

from allways.chain_providers.bitcoin import (
    ADDR_TYPE_P2PKH,
    ADDR_TYPE_P2SH_P2WPKH,
    ADDR_TYPE_P2TR,
    ADDR_TYPE_P2WPKH,
    detect_address_type,
)

# Known test WIF (compressed)
TEST_WIF = 'L1RrrnXkcKut5DEMwtDthjwRcTTwED36thyL1DebVrKuwvohjMNi'
TEST_MESSAGE = 'allways-reserve:bc1qtest:12345'


class TestDetectAddressType:
    def test_p2pkh(self):
        assert detect_address_type('1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa') == ADDR_TYPE_P2PKH

    def test_p2wpkh(self):
        assert detect_address_type('bc1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvwjgxxc') == ADDR_TYPE_P2WPKH

    def test_p2sh_p2wpkh(self):
        assert detect_address_type('37XAVCtKEvPbx2rpkxx7FmrUsetFXSawx5') == ADDR_TYPE_P2SH_P2WPKH

    def test_p2tr(self):
        assert detect_address_type('bc1pxyz') == ADDR_TYPE_P2TR

    def test_regtest_p2wpkh(self):
        assert detect_address_type('bcrt1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvtest') == ADDR_TYPE_P2WPKH

    def test_regtest_p2tr(self):
        assert detect_address_type('bcrt1pxyz') == ADDR_TYPE_P2TR

    def test_testnet_p2wpkh(self):
        assert detect_address_type('tb1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvtest') == ADDR_TYPE_P2WPKH

    def test_unknown(self):
        assert detect_address_type('xyz123') == 'unknown'

    def test_empty(self):
        assert detect_address_type('') == 'unknown'


class TestBIP137SignVerify:
    """Test BIP-137 sign/verify roundtrip using bitcoin-message-tool directly."""

    def test_p2pkh_roundtrip(self):
        addr, _, sig = sign_message(TEST_WIF, 'p2pkh', TEST_MESSAGE, deterministic=True)
        assert addr.startswith('1')
        valid, _, _ = verify_message(addr, TEST_MESSAGE, sig)
        assert valid

    def test_p2wpkh_roundtrip(self):
        addr, _, sig = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)
        assert addr.startswith('bc1q')
        valid, _, _ = verify_message(addr, TEST_MESSAGE, sig)
        assert valid

    def test_p2sh_p2wpkh_roundtrip(self):
        addr, _, sig = sign_message(TEST_WIF, 'p2wpkh-p2sh', TEST_MESSAGE, deterministic=True)
        assert addr.startswith('3')
        valid, _, _ = verify_message(addr, TEST_MESSAGE, sig)
        assert valid

    def test_wrong_message_fails(self):
        addr, _, sig = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)
        valid, _, _ = verify_message(addr, 'wrong-message', sig)
        assert not valid

    def test_wrong_address_fails(self):
        _, _, sig = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)
        # Use the P2PKH address from the same key — signature won't match
        p2pkh_addr, _, _ = sign_message(TEST_WIF, 'p2pkh', TEST_MESSAGE, deterministic=True)
        valid, _, _ = verify_message(p2pkh_addr, TEST_MESSAGE, sig)
        assert not valid

    def test_deterministic_signatures(self):
        _, _, sig1 = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)
        _, _, sig2 = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)
        assert sig1 == sig2

    def test_allways_proof_format(self):
        """Test with the actual message format used in swap reservation."""
        msg = 'allways-reserve:bc1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvwjgxxc:100'
        addr, _, sig = sign_message(TEST_WIF, 'p2wpkh', msg, deterministic=True)
        valid, _, _ = verify_message(addr, msg, sig)
        assert valid

    def test_allways_swap_format(self):
        """Test with the actual message format used in swap confirmation."""
        msg = 'allways-swap:abc123def456'
        addr, _, sig = sign_message(TEST_WIF, 'p2wpkh', msg, deterministic=True)
        valid, _, _ = verify_message(addr, msg, sig)
        assert valid
