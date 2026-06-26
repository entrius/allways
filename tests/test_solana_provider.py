"""Unit tests for SolanaProvider (B7) — the native-SOL swap-leg provider.

No chain: a fake RPC returns canned getTransaction/getSlot/getBalance results. Covers fetch/verify,
block_time extraction, amount matching (>=), confirmations, failed/missing tx, unreachable backend,
address validity, the ed25519 proof sign/verify roundtrip, balance, and the SystemProgram send path.
"""

import pytest
import requests
from solders.keypair import Keypair

from allways.chain_providers.base import ProviderUnreachableError
from allways.chain_providers.solana import SolanaProvider
from allways.chains import CHAIN_SOL


def make_tx(recipient, credit, sender='SENDER', slot=100, block_time=5000, err=None, extra_keys=None):
    """Build a getTransaction (json encoding) dict crediting `credit` lamports to `recipient`."""
    extra = extra_keys or []
    keys = [sender, recipient] + extra
    pre = [10_000_000, 1_000_000] + [0] * len(extra)
    post = list(pre)
    ridx = keys.index(recipient)
    post[ridx] = pre[ridx] + credit
    return {
        'slot': slot,
        'blockTime': block_time,
        'meta': {'err': err, 'preBalances': pre, 'postBalances': post},
        'transaction': {'message': {'accountKeys': keys}},
    }


class FakeRpc:
    def __init__(self, tx=None, slot=200, balance=12345, raise_conn=False):
        self._tx = tx
        self._slot = slot
        self._balance = balance
        self._raise_conn = raise_conn
        self.url = 'fake://rpc'

    def get_transaction(self, sig, commitment='confirmed'):
        if self._raise_conn:
            raise requests.ConnectionError('down')
        return self._tx

    def get_slot(self, commitment='confirmed'):
        return self._slot

    def get_balance(self, pubkey, commitment='confirmed'):
        return self._balance


def provider_with(rpc, keypair=None):
    p = SolanaProvider(solana_rpc_url='fake://rpc', solana_keypair=keypair)
    p.rpc = rpc
    return p


class TestFetchAndVerify:
    def test_match_returns_info(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, slot=100), slot=131))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info is not None
        assert info.amount == 2_000_000  # net credit, not the requested floor
        assert info.recipient == 'RECIP'
        assert info.sender == 'SENDER'
        assert info.block_number == 100

    def test_block_time_extracted(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, block_time=1_700_000_123)))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info.block_time == 1_700_000_123  # replay-freshness floor source (B2)

    def test_exact_amount_matches(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 1_000_000)))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is not None

    def test_underpayment_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 999_999)))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

    def test_recipient_absent_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('OTHER', 5_000_000)))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

    def test_confirmations_from_slot_delta(self):
        # tip 131, tx slot 100 → 131-100+1 = 32 == min_confirmations → confirmed.
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, slot=100), slot=131))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info.confirmations == 32 and info.confirmed is True

    def test_not_yet_final_is_unconfirmed(self):
        # tip only a few slots ahead → below the 32-slot finality floor.
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, slot=100), slot=105))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info.confirmations == 6 and info.confirmed is False

    def test_failed_tx_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 5_000_000, err={'InstructionError': [0, 'Custom']})))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

    def test_missing_tx_returns_none(self):
        p = provider_with(FakeRpc(tx=None))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

    def test_empty_hash_returns_none(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 5_000_000)))
        assert p.fetch_matching_tx('', 'RECIP', 1_000_000) is None

    def test_unreachable_raises(self):
        p = provider_with(FakeRpc(raise_conn=True))
        with pytest.raises(ProviderUnreachableError):
            p.fetch_matching_tx('sig', 'RECIP', 1_000_000)

    def test_loaded_addresses_indexed(self):
        # Recipient arrives via an address-lookup-table; balances still index past static keys.
        tx = {
            'slot': 100,
            'blockTime': 5000,
            'meta': {
                'err': None,
                'preBalances': [10_000_000, 0],
                'postBalances': [10_000_000, 3_000_000],
                'loadedAddresses': {'writable': ['RECIP'], 'readonly': []},
            },
            'transaction': {'message': {'accountKeys': ['SENDER']}},
        }
        p = provider_with(FakeRpc(tx=tx, slot=200))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info is not None and info.amount == 3_000_000


class TestVerifyTransactionPostChecks:
    """The base verify_transaction layer on top of the Solana fetch."""

    def test_self_transfer_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, sender='RECIP'), slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000) is None

    def test_sender_mismatch_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, sender='ALICE'), slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='BOB') is None

    def test_sender_match_accepted(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, sender='ALICE'), slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='ALICE') is not None


class TestAddressValidity:
    def test_valid_pubkey(self):
        p = provider_with(FakeRpc())
        assert p.is_valid_address(str(Keypair().pubkey())) is True

    def test_garbage_rejected(self):
        p = provider_with(FakeRpc())
        assert p.is_valid_address('not-a-key') is False
        assert p.is_valid_address('') is False
        assert p.is_valid_address(None) is False


class TestProofRoundtrip:
    def test_sign_then_verify(self):
        kp = Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        addr = str(kp.pubkey())
        msg = 'allways-reserve:sol:42'
        sig = p.sign_from_proof(addr, msg)
        assert sig and len(sig) == 128  # 64-byte ed25519 sig, hex
        assert p.verify_from_proof(addr, msg, sig) is True

    def test_wrong_message_fails(self):
        kp = Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        addr = str(kp.pubkey())
        sig = p.sign_from_proof(addr, 'one')
        assert p.verify_from_proof(addr, 'two', sig) is False

    def test_wrong_signer_fails(self):
        kp, other = Keypair(), Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        sig = p.sign_from_proof(str(kp.pubkey()), 'msg')
        assert p.verify_from_proof(str(other.pubkey()), 'msg', sig) is False

    def test_explicit_key_argument(self):
        signer = Keypair()
        p = provider_with(FakeRpc())  # provider has no keypair
        sig = p.sign_from_proof(str(signer.pubkey()), 'msg', key=signer)
        assert p.verify_from_proof(str(signer.pubkey()), 'msg', sig) is True

    def test_0x_prefixed_signature(self):
        kp = Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        addr = str(kp.pubkey())
        sig = p.sign_from_proof(addr, 'msg')
        assert p.verify_from_proof(addr, 'msg', '0x' + sig) is True

    def test_sign_without_key_returns_empty(self):
        p = provider_with(FakeRpc())
        assert p.sign_from_proof('addr', 'msg') == ''


class TestBalanceAndHeight:
    def test_balance(self):
        p = provider_with(FakeRpc(balance=777))
        assert p.get_balance(str(Keypair().pubkey())) == 777

    def test_block_height(self):
        p = provider_with(FakeRpc(slot=4242))
        assert p.get_current_block_height() == 4242


class SendRpc(FakeRpc):
    def get_latest_blockhash(self, commitment='confirmed'):
        return str(Keypair().pubkey())  # any 32-byte base58 string parses as a Hash

    def send_transaction(self, raw_tx_b64, skip_preflight=False, preflight_commitment='confirmed'):
        self.sent = raw_tx_b64
        return 'SIG123'

    def confirm(self, sig, timeout=30.0, poll=0.4):
        return {'slot': 321, 'err': None}


class TestSend:
    def test_send_returns_sig_and_slot(self):
        p = provider_with(SendRpc(), keypair=Keypair())
        out = p.send_amount(str(Keypair().pubkey()), 2_500_000)
        assert out == ('SIG123', 321)

    def test_send_without_keypair_returns_none(self):
        p = provider_with(SendRpc())
        assert p.send_amount(str(Keypair().pubkey()), 1_000) is None

    def test_send_bad_address_returns_none(self):
        p = provider_with(SendRpc(), keypair=Keypair())
        assert p.send_amount('not-a-pubkey', 1_000) is None


def test_chain_metadata():
    p = provider_with(FakeRpc())
    chain = p.get_chain()
    assert chain is CHAIN_SOL
    assert chain.id == 'sol' and chain.native_unit == 'lamport' and chain.decimals == 9
    assert chain.min_onchain_amount == 890880  # rent-exempt floor (0-data System account)
