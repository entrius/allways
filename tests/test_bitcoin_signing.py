"""Tests for BIP-137 message signing and verification in Bitcoin."""

import os
from unittest.mock import MagicMock, patch

from bitcoin_message_tool.bmt import sign_message

from allways.assets.btc import (
    Bitcoin,
    to_mainnet_address,
)

# Known test WIF (compressed)
TEST_WIF = 'L1RrrnXkcKut5DEMwtDthjwRcTTwED36thyL1DebVrKuwvohjMNi'
TEST_MESSAGE = 'allways-reserve:bc1qtest:12345'


def make_lightweight_provider() -> Bitcoin:
    """Construct a Bitcoin in lightweight mode for sign/verify tests.

    Lightweight mode doesn't hit a node for sign/verify — it's pure
    cryptographic work. Env is pinned so a dev .env (e.g. BTC_NETWORK=regtest) cannot leak in.
    """
    with patch.dict(os.environ, {'BTC_MODE': 'lightweight', 'BTC_NETWORK': 'mainnet', 'BTC_PRIVATE_KEY': TEST_WIF}):
        return Bitcoin()


class TestBitcoinSignFromProof:
    """Direct coverage of Bitcoin.sign_from_proof — the wrapper our
    validator/CLI actually invoke, not the underlying library."""

    def test_p2wpkh_address_produces_valid_signature(self):
        provider = make_lightweight_provider()
        # Derive the P2WPKH address this WIF signs for
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr, TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''
        assert provider.verify_from_proof(addr, TEST_MESSAGE, signature)

    def test_uppercase_bech32_signs_and_verifies(self):
        """BIP-173: uppercase bech32 (QR form) is the same address — sign and verify must accept it."""
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)

        signature = provider.sign_from_proof(addr.upper(), TEST_MESSAGE, key=TEST_WIF)

        assert signature != ''
        assert provider.verify_from_proof(addr.upper(), TEST_MESSAGE, signature)


class TestBitcoinVerifyFromProof:
    """Direct coverage of Bitcoin.verify_from_proof."""

    def test_wrong_message_fails_verification(self):
        provider = make_lightweight_provider()
        addr, _, signature = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)

        assert provider.verify_from_proof(addr, 'tampered-message', signature) is False

    def test_malformed_signature_returns_false_not_exception(self):
        """verify_from_proof catches library exceptions and returns False —
        the validator must never crash on a bad signature."""
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', TEST_MESSAGE, deterministic=True)

        assert provider.verify_from_proof(addr, TEST_MESSAGE, 'not-base64-garbage') is False


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


class TestIsValidAddress:
    """BTC address format validation — must accept every standard type
    (legacy, segwit v0, and Taproot/bech32m) and reject malformed input.

    Taproot regression guard: the old bech32-only check rejected all `bc1p…`
    addresses, blocking TAO->BTC payouts to Taproot wallets (issue #448).
    """

    # mainnet P2PKH / P2SH / P2WPKH / P2WSH / P2TR
    VALID = [
        '1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2',  # mainnet P2PKH
        '3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy',  # mainnet P2SH
        'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4',  # mainnet P2WPKH
        'bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3',  # mainnet P2WSH
        'bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrcr',  # mainnet P2TR
    ]

    INVALID = [
        '',
        'notanaddress',
        '0x71C7656EC7ab88b098defB751B7401B5f6d8976F',  # ETH address
        'bc1p5cyxnuxmeuwuvkwfem96lqzszd02n6xdcjrs20cac6yqjjwudpxqkedrXXX',  # bad checksum
        'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3XX',  # corrupted v0
    ]

    def test_accepts_all_standard_types(self):
        provider = make_lightweight_provider()
        provider.network = 'mainnet'
        for addr in self.VALID:
            assert provider.is_valid_address(addr), f'should accept {addr}'

    def test_enforces_network(self):
        provider = make_lightweight_provider()
        addresses = {
            'mainnet': self.VALID[2],
            'testnet4': 'tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx',
            'regtest': 'bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080',
        }
        for network, address in addresses.items():
            provider.network = network
            assert provider.is_valid_address(address)
            assert all(not provider.is_valid_address(other) for other in addresses.values() if other != address)

    def test_bech32_case(self):
        provider = make_lightweight_provider()
        address = self.VALID[2]
        assert provider.is_valid_address(address.upper())
        assert not provider.is_valid_address('b' + address[1:].upper())

    def test_rejects_malformed(self):
        provider = make_lightweight_provider()
        for addr in self.INVALID:
            assert not provider.is_valid_address(addr), f'should reject {addr!r}'


class TestToMainnetAddress:
    """testnet/regtest -> mainnet re-encoding used by the BIP-137 verify path.
    Conversions must be byte-identical to the prior bech32/base58 behavior."""

    def test_testnet_p2wpkh(self):
        assert (
            to_mainnet_address('tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx')
            == 'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4'
        )


class TestNormalizeAddress:
    """bech32 is case-insensitive per BIP-173 (uppercase is standard in QR codes);
    base58 legacy addresses are case-sensitive and must pass through untouched."""

    def test_uppercase_bech32_matches_lowercase(self):
        btc = Bitcoin()
        upper = 'BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4'
        assert btc.normalize_address(upper) == upper.lower()

    def test_base58_passes_through_case_sensitive(self):
        btc = Bitcoin()
        legacy = '1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2'
        assert btc.normalize_address(legacy) == legacy


class TestCanSendFromCase:
    def test_uppercase_bech32_matches_derived_address(self):
        provider = make_lightweight_provider()
        addr, _, _ = sign_message(TEST_WIF, 'p2wpkh', 'x', deterministic=True)
        with patch.dict(os.environ, {'BTC_PRIVATE_KEY': TEST_WIF}, clear=False):
            assert provider.can_send_from(addr.upper())

    def test_uppercase_bech32_dest_is_sent_lowercase(self):
        btc = Bitcoin()
        upper = 'BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4'
        with patch.object(btc, 'send_amount_lightweight') as send:
            btc.send_amount(upper, 1000)
        assert send.call_args.args[0] == upper.lower()


class TestSignet:
    """signet was an accepted BTC_NETWORK with no branch here, so it fell through to mainnet
    Esplora and mainnet address encoding — a signet operator watched the real chain and derived
    real addresses. Its addresses are testnet's (embit's signet row differs only in a display
    name) and both public Esploras serve it."""

    @staticmethod
    def _p2wpkh(network: str) -> str:
        from embit.ec import PrivateKey
        from embit.networks import NETWORKS
        from embit.script import p2wpkh

        return p2wpkh(PrivateKey.from_wif(TEST_WIF).get_public_key()).address(NETWORKS[network])

    def test_derives_test_addresses_not_mainnet_ones(self, monkeypatch):
        monkeypatch.setenv('BTC_NETWORK', 'signet')
        monkeypatch.setenv('BTC_PRIVATE_KEY', TEST_WIF)
        provider = Bitcoin()
        assert provider.can_send_from(self._p2wpkh('signet'))
        assert not provider.can_send_from(self._p2wpkh('main'))

    def test_uses_signet_esplora(self, monkeypatch):
        monkeypatch.setenv('BTC_NETWORK', 'signet')
        monkeypatch.delenv('BTC_ESPLORA_URLS', raising=False)
        assert all('/signet/' in url for url, _ in Bitcoin().btc_api_bases())
