"""Unit tests for Sol (B7) — the native-SOL swap-leg provider.

No chain: a fake RPC returns canned getTransaction/getSlot/getBalance results. Covers fetch/verify,
block_time extraction, amount matching (>=), confirmations, failed/missing tx, unreachable backend,
address validity, the ed25519 proof sign/verify roundtrip, balance, and the SystemProgram send path.
"""

import pytest
import requests
from solders.keypair import Keypair

from allways.assets.asset import ProviderUnreachableError
from allways.assets.sol import RESERVED_ACCOUNTS, Sol
from allways.chains import CHAIN_SOL
from allways.solana.rpc import SolanaRpcError


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
    p = Sol(solana_rpc_url='fake://rpc', solana_keypair=keypair)
    p.rpc = rpc
    return p


class DedupRpc:
    """Programmable getSignatureStatuses[0] + getSlot for the own-broadcast dedup guard."""

    def __init__(self, status, slot=1000):
        self._status = status
        self._slot = slot

    def get_slot(self, commitment='confirmed'):
        return self._slot

    def get_signature_statuses(self, sigs):
        return [self._status]


class TestSendDedup:
    """H2: a landed prior broadcast must be reused, never re-sent, so a confirm() timeout can't double-pay."""

    WANT = ('RECIP', 5_000_000, 'swap1')

    def _p(self, status, slot=1000):
        p = provider_with(DedupRpc(status, slot))
        p.broadcasted_txids['SIG'] = (*self.WANT, 1000)
        return p

    def test_reuses_landed_prior(self):
        p = self._p({'err': None, 'slot': 123, 'confirmationStatus': 'confirmed'})
        assert p._prior_broadcast(self.WANT) == ('SIG', 123)

    def test_reuses_processed_prior_that_already_moved_funds(self):
        p = self._p({'err': None, 'slot': 50, 'confirmationStatus': 'processed'})
        assert p._prior_broadcast(self.WANT) == ('SIG', 50)

    def test_failed_prior_dropped_allows_fresh_send(self):
        p = self._p({'err': 'InstructionError', 'slot': 10})
        assert p._prior_broadcast(self.WANT) is None
        assert 'SIG' not in p.broadcasted_txids

    def test_pending_recent_prior_waits(self):
        p = self._p(None, slot=1010)  # head 1010, seen 1000 → within TTL, not on chain → must wait
        with pytest.raises(SolanaRpcError):
            p._prior_broadcast(self.WANT)

    def test_expired_unlanded_prior_allows_fresh_send(self):
        p = self._p(None, slot=2000)  # head-seen = 1000 > 150 TTL → blockhash expired, never landed
        assert p._prior_broadcast(self.WANT) is None
        assert 'SIG' not in p.broadcasted_txids

    def test_different_obligation_is_ignored(self):
        p = self._p({'err': None, 'slot': 5})
        assert p._prior_broadcast(('RECIP', 5_000_000, 'swapB')) is None  # different dedup scope


class TestSendGuard:
    def test_send_refuses_when_from_address_mismatches_key(self):
        # H3: never broadcast from a wallet the validator's sender-pin would reject (wasted funds).
        p = provider_with(FakeRpc(), keypair=Keypair())
        assert p.send_amount('RECIPIENT', 1000, from_address='someOtherWallet') is None


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


class TestDeliveryGates:
    """Solana's reserved account keys are read-only in every transaction, so a transfer to one
    always fails and an honest miner must never be slashed for missing it. Verified against LiteSVM
    (2026-08-10): all 31 reserved keys reject the credit; the incinerator and ordinary/PDA/program
    addresses accept it. Ownership is NOT the test — Stake/Config/AddressLookupTable are owned by
    the upgradeable loader, StakeConfig has no account, and the SPL Token program is executable
    yet fundable."""

    # One per shape the account-owner heuristic got wrong, plus the obvious burn target.
    RESERVED = [
        '11111111111111111111111111111111',  # System Program — owner NativeLoader
        'SysvarC1ock11111111111111111111111111111111',  # sysvar — owner Sysvar
        'StakeConfig11111111111111111111111111111111',  # no account exists at all
        'Stake11111111111111111111111111111111111111',  # Core BPF: owner is the upgradeable loader
        'Config1111111111111111111111111111111111111',  # ditto
        'AddressLookupTab1e1111111111111111111111111',  # ditto
        'Feature111111111111111111111111111111111111',  # no account exists
    ]
    DELIVERABLE = [
        '1nc1nerator11111111111111111111111111111111',  # deliberately not reserved — a burn must land
        'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # executable, loader-owned, receives SOL fine
    ]

    @pytest.mark.parametrize('address', RESERVED)
    def test_reserved_key_blocks_reservation_and_exempts_slash(self, address):
        p = provider_with(FakeRpc())
        assert p.can_deliver_to(address, 10**9) is False
        assert p.delivery_refused(address, 0) is True

    @pytest.mark.parametrize('address', DELIVERABLE)
    def test_fundable_special_addresses_pass(self, address):
        p = provider_with(FakeRpc())
        assert p.can_deliver_to(address, 10**9) is True
        assert p.delivery_refused(address, 0) is False

    def test_ordinary_wallet_passes(self):
        p = provider_with(FakeRpc())
        addr = str(Keypair().pubkey())
        assert p.can_deliver_to(addr, 10**9) is True
        assert p.delivery_refused(addr, 0) is False

    def test_gates_need_no_rpc(self):
        """Offline by construction — a dead RPC can neither block a reservation nor defer a slash."""
        p = provider_with(FakeRpc(raise_conn=True))
        assert p.can_deliver_to(self.RESERVED[0], 10**9) is False
        assert p.delivery_refused(self.RESERVED[0], 0) is True

    def test_reserved_set_matches_agave(self):
        """Guards against a typo silently un-blocking a key: agave v3.1.14 lists exactly 31."""
        assert len(RESERVED_ACCOUNTS) == 31
        assert all(32 <= len(a) <= 44 for a in RESERVED_ACCOUNTS)
        p = provider_with(FakeRpc())
        assert all(p.is_valid_address(a) for a in RESERVED_ACCOUNTS)


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
    chain = p.chain_def
    assert chain is CHAIN_SOL
    assert chain.id == 'sol' and chain.native_unit == 'lamport' and chain.decimals == 9
    assert chain.min_onchain_amount == 890880  # rent-exempt floor (0-data System account)


class ScanRpc(FakeRpc):
    """FakeRpc + an address-signature index for the deposit scanner."""

    def __init__(self, sigs, txs, raise_scan=False):
        super().__init__()
        self.sigs = sigs
        self.txs = txs
        self.raise_scan = raise_scan

    def get_signatures_for_address(self, address, before=None, until=None, limit=1000, commitment='confirmed'):
        if self.raise_scan:
            raise ConnectionError('rpc down')
        return self.sigs

    def get_transaction(self, sig, commitment='confirmed'):
        return self.txs.get(sig)


class TestFindRecentOutgoing:
    def test_finds_matching_deposit(self):
        tx = make_tx('MINER', 5000, sender='USER')
        p = provider_with(ScanRpc([{'signature': 'sigA', 'err': None}], {'sigA': tx}))
        assert p.find_recent_outgoing('USER', 'MINER', 5000) == 'sigA'

    def test_skips_wrong_sender_and_underpay(self):
        wrong_sender = make_tx('MINER', 5000, sender='OTHER')
        underpay = make_tx('MINER', 4999, sender='USER')
        hit = make_tx('MINER', 5000, sender='USER')
        sigs = [{'signature': s, 'err': None} for s in ('s1', 's2', 's3')]
        p = provider_with(ScanRpc(sigs, {'s1': wrong_sender, 's2': underpay, 's3': hit}))
        assert p.find_recent_outgoing('USER', 'MINER', 5000) == 's3'

    def test_skips_errored_entries_and_failed_txs(self):
        failed = make_tx('MINER', 5000, sender='USER', err={'InstructionError': []})
        sigs = [{'signature': 's1', 'err': 'x'}, {'signature': 's2', 'err': None}]
        p = provider_with(ScanRpc(sigs, {'s1': make_tx('MINER', 5000, sender='USER'), 's2': failed}))
        assert p.find_recent_outgoing('USER', 'MINER', 5000) is None

    def test_scan_failure_returns_none(self):
        p = provider_with(ScanRpc([], {}, raise_scan=True))
        assert p.find_recent_outgoing('USER', 'MINER', 5000) is None
