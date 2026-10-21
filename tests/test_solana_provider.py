"""Unit tests for Sol — the native-SOL swap-leg provider.

No chain: a fake RPC returns canned getTransaction/getSlot/getBalance results. Covers fetch/verify,
block_time extraction, amount matching (>=), confirmations, failed/missing tx, unreachable backend,
address validity, the ed25519 proof sign/verify roundtrip, balance, and the SystemProgram send path.
"""

import base58
import pytest
from solders.keypair import Keypair

from allways.assets.asset import ProviderUnreachableError
from allways.assets.sol import SYSTEM_PROGRAM, Sol
from allways.solana.rpc import SolanaRpcError, SolanaRpcUnreachable


def transfer_ix(program_idx, from_idx, to_idx, lamports, tag=2, extra_accounts=()):
    """A json-encoded System Program instruction: Transfer (tag 2) or TransferWithSeed (tag 11)."""
    data = tag.to_bytes(4, 'little') + lamports.to_bytes(8, 'little')
    accounts = [from_idx, *extra_accounts, to_idx]
    return {'programIdIndex': program_idx, 'accounts': accounts, 'data': base58.b58encode(data).decode()}


def make_tx(recipient, credit, sender='SENDER', slot=100, block_time=5000, err=None, extra_keys=None):
    """Build a getTransaction (json encoding) dict where `sender` System-transfers `credit` lamports to
    `recipient`, paying the fee itself."""
    extra = extra_keys or []
    keys = [sender, recipient] + extra + [SYSTEM_PROGRAM]
    pre = [10_000_000, 1_000_000] + [0] * len(extra) + [1]
    post = list(pre)
    ridx = keys.index(recipient)
    post[ridx] = pre[ridx] + credit
    return {
        'slot': slot,
        'blockTime': block_time,
        'meta': {'err': err, 'preBalances': pre, 'postBalances': post, 'innerInstructions': []},
        'transaction': {
            'message': {'accountKeys': keys, 'instructions': [transfer_ix(len(keys) - 1, 0, ridx, credit)]}
        },
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
            raise SolanaRpcUnreachable('down', url='http://fake')
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

    def __init__(self, status, slot=1000, slot_fails=False):
        self._status = status
        self._slot = slot
        self._slot_fails = slot_fails

    def get_slot(self, commitment='confirmed'):
        if self._slot_fails:
            raise SolanaRpcError('getSlot down')
        return self._slot

    def get_signature_statuses(self, sigs):
        return [self._status]


class TestSendDedup:
    """A landed prior broadcast must be reused, never re-sent, so a confirm() timeout can't double-pay.
    Every branch where the RPC couldn't resolve the prior send must WAIT (raise), never re-send."""

    WANT = ('RECIP', 5_000_000, 'swap1')

    def _p(self, status, slot=1000, seen=1000, slot_fails=False):
        p = provider_with(DedupRpc(status, slot, slot_fails))
        p.broadcasted_txids['SIG'] = (*self.WANT, seen)
        return p

    def test_reuses_landed_prior(self):
        p = self._p({'err': None, 'slot': 123, 'confirmationStatus': 'confirmed'})
        assert p.chain.prior_broadcast(p.broadcasted_txids, self.WANT) == ('SIG', 123)

    def test_processed_only_prior_waits(self):
        # `processed` can sit on a minority fork that never settles. Reusing it would mark the
        # leg delivered while the user is never paid → slash. Wait for confirmed/finalized.
        p = self._p({'err': None, 'slot': 50, 'confirmationStatus': 'processed'})
        with pytest.raises(SolanaRpcError):
            p.chain.prior_broadcast(p.broadcasted_txids, self.WANT)
        assert 'SIG' in p.broadcasted_txids  # kept: re-probed next pass

    def test_failed_prior_dropped_allows_fresh_send(self):
        p = self._p({'err': 'InstructionError', 'slot': 10, 'confirmationStatus': 'finalized'})
        assert p.chain.prior_broadcast(p.broadcasted_txids, self.WANT) is None
        assert 'SIG' not in p.broadcasted_txids

    def test_pending_recent_prior_waits(self):
        p = self._p(None, slot=1010)  # head 1010, seen 1000 → within TTL, not on chain → must wait
        with pytest.raises(SolanaRpcError):
            p.chain.prior_broadcast(p.broadcasted_txids, self.WANT)

    def test_expired_unlanded_prior_allows_fresh_send(self):
        p = self._p(None, slot=2000)  # head-seen = 1000 > 150 TTL → blockhash expired, never landed
        assert p.chain.prior_broadcast(p.broadcasted_txids, self.WANT) is None
        assert 'SIG' not in p.broadcasted_txids

    def test_unknown_record_slot_waits_and_backfills(self):
        # get_slot failed at record time (seen=0). head - 0 >> TTL must NOT read as "expired" —
        # the tx may still be propagating. Backfill seen with the current head and wait.
        p = self._p(None, slot=2000, seen=0)
        with pytest.raises(SolanaRpcError):
            p.chain.prior_broadcast(p.broadcasted_txids, self.WANT)
        assert p.broadcasted_txids['SIG'] == (*self.WANT, 2000)

    def test_head_unreadable_pending_prior_waits(self):
        # no status AND no head → nothing is resolvable; never evict, never re-send.
        p = self._p(None, seen=0, slot_fails=True)
        with pytest.raises(SolanaRpcError):
            p.chain.prior_broadcast(p.broadcasted_txids, self.WANT)
        assert p.broadcasted_txids['SIG'] == (*self.WANT, 0)  # no head to backfill from


class TestSendGuard:
    def test_send_refuses_when_from_address_mismatches_key(self):
        # never broadcast from a wallet the validator's sender-pin would reject (wasted funds).
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

    def test_failed_tx_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 5_000_000, err={'InstructionError': [0, 'Custom']})))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

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
            'transaction': {
                'message': {
                    'accountKeys': ['SENDER', SYSTEM_PROGRAM],
                    'instructions': [transfer_ix(1, 0, 2, 3_000_000)],
                }
            },
        }
        p = provider_with(FakeRpc(tx=tx, slot=200))
        info = p.fetch_matching_tx('sig', 'RECIP', 1_000_000)
        assert info is not None and info.amount == 3_000_000 and info.sender == 'SENDER'


PROGRAM = 'A11waysProgram1111111111111111111111111111'


def program_tx(keys, pre, post, instructions, inner=()):
    return {
        'slot': 100,
        'blockTime': 5000,
        'meta': {
            'err': None,
            'preBalances': pre,
            'postBalances': post,
            'innerInstructions': [{'index': 0, 'instructions': list(inner)}],
        },
        'transaction': {'message': {'accountKeys': keys, 'instructions': instructions}},
    }


class TestCreditComesFromSystemTransfers:
    """Only a System Program transfer out of a wallet credits a deposit, and the sender is that wallet."""

    def test_program_moved_lamports_are_not_a_deposit(self):
        # A program credits RECIP straight from an account it owns, in a tx VALIDATOR signed and paid
        # for, alongside a System transfer VALIDATOR makes elsewhere (rent for a new account).
        keys = ['VALIDATOR', 'VAULT', 'RECIP', 'NEWPDA', PROGRAM, SYSTEM_PROGRAM]
        pre = [50_000_000, 900_000_000, 1_000_000, 0, 1, 1]
        post = [48_000_000, 890_000_000, 11_000_000, 2_000_000, 1, 1]
        ix = {'programIdIndex': 4, 'accounts': [0, 1, 2, 3], 'data': ''}
        tx = program_tx(keys, pre, post, [ix], inner=[transfer_ix(5, 0, 3, 2_000_000)])
        p = provider_with(FakeRpc(tx=tx, slot=200))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None
        assert p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='VALIDATOR') is None

    def test_sender_is_the_debited_wallet_not_the_fee_payer(self):
        keys = ['RELAYER', 'ALICE', 'RECIP', SYSTEM_PROGRAM]
        tx = program_tx(keys, [9, 9, 9, 1], [9, 9, 9, 1], [transfer_ix(3, 1, 2, 4_000_000)])
        p = provider_with(FakeRpc(tx=tx, slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='RELAYER') is None
        info = p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='ALICE')
        assert info is not None and info.amount == 4_000_000

    def test_a_smart_wallet_cpi_transfer_counts(self):
        # A multisig program signs for its vault and CPIs the System Program; the member pays the fee.
        keys = ['MEMBER', 'VAULT', 'RECIP', 'MULTISIG', SYSTEM_PROGRAM]
        ix = {'programIdIndex': 3, 'accounts': [0, 1, 2, 4], 'data': ''}
        tx = program_tx(keys, [9] * 5, [9] * 5, [ix], inner=[transfer_ix(4, 1, 2, 6_000_000)])
        p = provider_with(FakeRpc(tx=tx, slot=200))
        info = p.verify_transaction('sig', 'RECIP', 5_000_000, expected_sender='VAULT')
        assert info is not None and info.amount == 6_000_000

    def test_transfers_from_one_wallet_add_up_and_with_seed_counts(self):
        keys = ['ALICE', 'RECIP', 'BASE', SYSTEM_PROGRAM]
        ixs = [transfer_ix(3, 0, 1, 1_000_000), transfer_ix(3, 0, 1, 2_000_000, tag=11, extra_accounts=(2,))]
        p = provider_with(FakeRpc(tx=program_tx(keys, [9] * 4, [9] * 4, ixs), slot=200))
        info = p.verify_transaction('sig', 'RECIP', 3_000_000, expected_sender='ALICE')
        assert info is not None and info.amount == 3_000_000

    def test_two_paying_wallets_have_no_single_sender(self):
        keys = ['ALICE', 'BOB', 'RECIP', SYSTEM_PROGRAM]
        ixs = [transfer_ix(3, 0, 2, 3_000_000), transfer_ix(3, 1, 2, 3_000_000)]
        p = provider_with(FakeRpc(tx=program_tx(keys, [9] * 4, [9] * 4, ixs), slot=200))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None

    def test_other_system_instructions_are_not_transfers(self):
        # CreateAccount (tag 0) also moves lamports, but into an account it creates, never a deposit wallet.
        keys = ['ALICE', 'RECIP', SYSTEM_PROGRAM]
        tx = program_tx(keys, [9] * 3, [9] * 3, [transfer_ix(2, 0, 1, 3_000_000, tag=0)])
        p = provider_with(FakeRpc(tx=tx, slot=200))
        assert p.fetch_matching_tx('sig', 'RECIP', 1_000_000) is None


class TestVerifyTransactionPostChecks:
    """The base verify_transaction layer on top of the Solana fetch."""

    def test_self_transfer_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, sender='RECIP'), slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000) is None

    def test_sender_mismatch_rejected(self):
        p = provider_with(FakeRpc(tx=make_tx('RECIP', 2_000_000, sender='ALICE'), slot=200))
        assert p.verify_transaction('sig', 'RECIP', 1_000_000, expected_sender='BOB') is None


class TestDeliveryGates:
    """Solana's reserved account keys are read-only in every transaction, so a transfer to one
    always fails and an honest miner must never be slashed for missing it. Verified against LiteSVM:
    all 31 reserved keys reject the credit; the incinerator and ordinary/PDA/program
    addresses accept it. Ownership is NOT the test — Stake/Config/AddressLookupTable are owned by
    the upgradeable loader, StakeConfig has no account, and the SPL Token program is executable
    yet fundable."""

    # One per shape the account-owner heuristic got wrong, plus the obvious burn target.
    RESERVED = [
        '11111111111111111111111111111111',  # System Program — owner NativeLoader
        'Stake11111111111111111111111111111111111111',  # Core BPF: owner is the upgradeable loader
    ]
    DELIVERABLE = [
        '1nc1nerator11111111111111111111111111111111',  # deliberately not reserved — a burn must land
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

    def test_gates_need_no_rpc(self):
        """Offline by construction — a dead RPC can neither block a reservation nor defer a slash."""
        p = provider_with(FakeRpc(raise_conn=True))
        assert p.can_deliver_to(self.RESERVED[0], 10**9) is False
        assert p.delivery_refused(self.RESERVED[0], 0) is True


class TestProofRoundtrip:
    def test_sign_then_verify(self):
        kp = Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        addr = str(kp.pubkey())
        msg = 'allways-reserve:sol:42'
        sig = p.chain.sign_from_proof(addr, msg)
        assert sig and len(sig) == 128  # 64-byte ed25519 sig, hex
        assert p.chain.verify_from_proof(addr, msg, sig) is True

    def test_wrong_signer_fails(self):
        kp, other = Keypair(), Keypair()
        p = provider_with(FakeRpc(), keypair=kp)
        sig = p.chain.sign_from_proof(str(kp.pubkey()), 'msg')
        assert p.chain.verify_from_proof(str(other.pubkey()), 'msg', sig) is False


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
