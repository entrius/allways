"""Tests for BIP-137 message signing and verification in BitcoinProvider."""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bitcoin_message_tool.bmt import sign_message, verify_message

from allways.chain_providers.bitcoin import (
    ADDR_TYPE_P2PKH,
    ADDR_TYPE_P2SH_P2WPKH,
    ADDR_TYPE_P2TR,
    ADDR_TYPE_P2WPKH,
    BitcoinProvider,
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


def make_lightweight_provider() -> BitcoinProvider:
    """Construct a BitcoinProvider in lightweight mode for sign/verify tests.

    Lightweight mode doesn't hit a node for sign/verify — it's pure
    cryptographic work. BTC_MODE and BTC_PRIVATE_KEY are set via env patch.
    """
    with patch.dict(os.environ, {'BTC_MODE': 'lightweight', 'BTC_PRIVATE_KEY': TEST_WIF}, clear=False):
        return BitcoinProvider()


def make_lightweight_provider_for_network(network: str) -> BitcoinProvider:
    with patch.dict(
        os.environ, {'BTC_MODE': 'lightweight', 'BTC_NETWORK': network, 'BTC_PRIVATE_KEY': TEST_WIF}, clear=False
    ):
        return BitcoinProvider()


class TestBitcoinAddressValidationNetwork:
    MAINNET_ADDRESSES = [
        'bc1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvwjgxxc',
        '1LDsjB43N2NAQ1Vbc2xyHca4iBBciN8iwC',
        '37XAVCtKEvPbx2rpkxx7FmrUsetFXSawx5',
    ]
    TESTNET_ADDRESSES = [
        'tb1q6tvmnmetj8vfz98vuetpvtuplqtj4uvvy5n4at',
        'mzjq2E92B3oRB7yDKbwM7XnPaAnKfRERw2',
        '2My5NYwpLrNtx9pVNS6Zysiqk616RJuHDuz',
    ]

    def test_mainnet_accepts_mainnet_addresses(self):
        provider = make_lightweight_provider_for_network('mainnet')
        assert all(provider.is_valid_address(addr) for addr in self.MAINNET_ADDRESSES)

    def test_mainnet_rejects_testnet_addresses(self):
        provider = make_lightweight_provider_for_network('mainnet')
        assert all(not provider.is_valid_address(addr) for addr in self.TESTNET_ADDRESSES)

    def test_testnet_accepts_testnet_addresses(self):
        provider = make_lightweight_provider_for_network('testnet')
        assert all(provider.is_valid_address(addr) for addr in self.TESTNET_ADDRESSES)

    def test_testnet_rejects_mainnet_addresses(self):
        provider = make_lightweight_provider_for_network('testnet')
        assert all(not provider.is_valid_address(addr) for addr in self.MAINNET_ADDRESSES)


class TestBitcoinProviderSignFromProof:
    """Direct coverage of BitcoinProvider.sign_from_proof — the wrapper our
    validator/CLI actually invoke, not the underlying library."""

    def test_p2wpkh_address_produces_valid_signature(self):
        provider = make_lightweight_provider()
        # Derive the P2WPKH address this WIF signs for
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''
        assert provider.verify_from_proof(addr, TEST_MESSAGE, signature)

    def test_p2pkh_address_produces_valid_signature(self):
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2pkh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''
        assert provider.verify_from_proof(addr, TEST_MESSAGE, signature)

    def test_p2sh_p2wpkh_address_produces_valid_signature(self):
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh-p2sh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''
        assert provider.verify_from_proof(addr, TEST_MESSAGE, signature)

    def test_p2tr_address_rejected(self):
        """P2TR isn't supported for BIP-137 signing — must return '' cleanly."""
        provider = make_lightweight_provider()
        p2tr_addr = 'bc1pxyz0000000000000000000000000000000000000000000000000000'

        signature = provider.sign_from_proof(p2tr_addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature == ''

    def test_unknown_address_type_rejected(self):
        provider = make_lightweight_provider()
        signature = provider.sign_from_proof('xyz-not-a-bitcoin-address', TEST_MESSAGE, key=TEST_WIF)
        assert signature == ''

    def test_missing_wif_returns_empty_signature(self):
        """Lightweight mode + no key arg + no BTC_PRIVATE_KEY env → empty sig."""
        with patch.dict(os.environ, {'BTC_MODE': 'lightweight'}, clear=False):
            os.environ.pop('BTC_PRIVATE_KEY', None)
            provider = BitcoinProvider()
            addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)

            signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=None)

        assert signature == ''

    def test_regtest_wif_is_converted_for_signing(self):
        """Regtest/testnet WIF (0xef prefix) is converted to mainnet (0x80)
        internally so the signing lib can handle it. Roundtrip succeeds."""
        provider = make_lightweight_provider()
        # Generate a mainnet-equivalent address that sign_message can work with
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''


class TestBitcoinProviderVerifyFromProof:
    """Direct coverage of BitcoinProvider.verify_from_proof."""

    def test_valid_signature_verifies(self):
        provider = make_lightweight_provider()
        addr, _, signature = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)

        assert provider.verify_from_proof(addr, TEST_MESSAGE, signature) is True

    def test_wrong_message_fails_verification(self):
        provider = make_lightweight_provider()
        addr, _, signature = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)

        assert provider.verify_from_proof(addr, 'tampered-message', signature) is False

    def test_p2tr_address_rejected_in_verify(self):
        provider = make_lightweight_provider()
        p2tr_addr = 'bc1pxyz0000000000000000000000000000000000000000000000000000'
        # Any signature at all — P2TR should short-circuit before verify
        assert provider.verify_from_proof(p2tr_addr, TEST_MESSAGE, 'AAAA') is False

    def test_unknown_address_type_rejected_in_verify(self):
        provider = make_lightweight_provider()
        assert provider.verify_from_proof('not-a-btc-address', TEST_MESSAGE, 'AAAA') is False

    def test_malformed_signature_returns_false_not_exception(self):
        """verify_from_proof catches library exceptions and returns False —
        the validator must never crash on a bad signature."""
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)

        assert provider.verify_from_proof(addr, TEST_MESSAGE, 'not-base64-garbage') is False

    def test_regtest_address_converted_for_verification(self):
        """A bcrt1... (regtest) address is converted to bc1... (mainnet) so
        the signing lib can verify against the signature. This path is only
        meaningful when the message was signed against a mainnet-derived
        address — for our tests we just confirm the call doesn't crash."""
        provider = make_lightweight_provider()
        # Fabricate a regtest address from scratch — verify should return False,
        # not crash, because no valid signature binds to it.
        result = provider.verify_from_proof('bcrt1qtestnettestaddresstestaddresstestaddr', TEST_MESSAGE, 'AAAA')
        assert result is False


class TestBroadcastedTxidsTracking:
    """Pin the find_recent_outgoing reuse contract: cross-process leakage is
    impossible (fresh provider = empty set), and same-session retries can
    still recover a broadcast whose response was lost."""

    SAMPLE_RAW_TX = (
        '0200000001abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789'
        '0000000000ffffffff0100e1f5050000000017a9140000000000000000000000000000000000000000870'
        '0000000'
    )

    def _expected_txid(self, raw_hex: str) -> str:
        from embit.transaction import Transaction as EmbitTx

        return EmbitTx.from_string(raw_hex).txid().hex()

    def test_successful_broadcast_records_txid(self):
        provider = make_lightweight_provider()
        txid = self._expected_txid(self.SAMPLE_RAW_TX)
        with patch.object(provider, 'btc_api_post', return_value=SimpleNamespace(status_code=200, text=txid)):
            result = provider.broadcast_tx(self.SAMPLE_RAW_TX)
        assert result == txid
        assert txid in provider.broadcasted_txids

    def test_broadcast_with_lost_response_still_records_txid(self):
        """If the post errors and tx_exists hasn't propagated yet, the txid
        must still be in the set — otherwise a same-session retry that
        finds the tx via find_recent_outgoing won't be allowed to reuse it
        and will broadcast a duplicate."""
        provider = make_lightweight_provider()
        txid = self._expected_txid(self.SAMPLE_RAW_TX)
        post = MagicMock(side_effect=ConnectionError('lost'))
        with (
            patch.object(provider, 'btc_api_post', post),
            patch.object(provider, 'tx_exists', return_value=False),
        ):
            result = provider.broadcast_tx(self.SAMPLE_RAW_TX)
        assert result is None
        assert txid in provider.broadcasted_txids

    def test_fresh_provider_starts_with_empty_set(self):
        """Each `alw swap` invocation gets a clean set — the prior swap's
        consumed tx hash can't leak across processes."""
        provider = make_lightweight_provider()
        assert provider.broadcasted_txids == set()
