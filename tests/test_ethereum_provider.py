"""EthereumProvider unit tests — all offline (RPC layer mocked, signing is pure crypto).

Covers the ETH-specific hazards: EIP-55 casing at every comparison boundary, inclusion≠settlement
(reverted txs carry intact to/value fields), receipt-unavailable must read as 'unknown' not
'absent', and the endpoint failover ladder.
"""

from typing import Optional

import pytest

from allways.chain_providers.base import ProviderUnreachableError
from allways.chain_providers.ethereum import EthereumProvider

# Well-known dev key (hardhat account #0) — never funded on mainnet, deterministic address.
TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
TEST_ADDR = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'  # EIP-55 form of the key's address

RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
TX = '0x' + 'ab' * 32


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'mainnet')
    monkeypatch.delenv('ETH_RPC_URLS', raising=False)
    monkeypatch.delenv('ETH_PRIVATE_KEY', raising=False)
    return EthereumProvider()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.eth_rpc = fake_rpc
    return provider


def mined_tx_responses(
    value_hex='0xde0b6b3a7640000',  # 1 ETH
    status='0x1',
    tip=1_000_063,
    block_number='0xf4240',  # 1_000_000
    to=RECIPIENT,
    receipt: Optional[dict] = ...,
):
    block_hash = '0x' + 'cd' * 32
    if receipt is ...:
        receipt = {'status': status, 'blockNumber': block_number, 'blockHash': block_hash}
    return {
        'eth_getTransactionByHash': {
            'hash': TX,
            'from': '0x' + '11' * 20,
            'to': to,
            'value': value_hex,
            'blockNumber': block_number,
        },
        'eth_getTransactionReceipt': receipt,
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': block_hash, 'timestamp': '0x64'},
    }


class TestAddresses:
    def test_valid_lowercase(self, provider):
        assert provider.is_valid_address(TEST_ADDR.lower())

    def test_valid_checksummed(self, provider):
        assert provider.is_valid_address(TEST_ADDR)

    def test_bad_checksum_mixed_case_rejected(self, provider):
        # Flip one letter's case: mixed-case implies EIP-55, and the checksum no longer verifies.
        bad = TEST_ADDR.replace('aad', 'aaD')
        assert not provider.is_valid_address(bad)

    def test_wrong_length_and_junk(self, provider):
        assert not provider.is_valid_address('0x1234')
        assert not provider.is_valid_address('')
        assert not provider.is_valid_address(None)
        assert not provider.is_valid_address('f39Fd6e51aad88F6F4ce6aB8827279cffFb92266')  # no 0x

    def test_normalize_lowercases(self, provider):
        assert provider.normalize_address(TEST_ADDR) == TEST_ADDR.lower()


class TestProofSigning:
    def test_sign_verify_roundtrip(self, provider):
        sig = provider.sign_from_proof(TEST_ADDR, 'proof: abc', key=TEST_KEY)
        assert sig
        assert provider.verify_from_proof(TEST_ADDR, 'proof: abc', sig)

    def test_verify_is_case_insensitive_on_address(self, provider):
        sig = provider.sign_from_proof(TEST_ADDR, 'msg', key=TEST_KEY)
        assert provider.verify_from_proof(TEST_ADDR.lower(), 'msg', sig)

    def test_verify_accepts_unprefixed_signature(self, provider):
        sig = provider.sign_from_proof(TEST_ADDR, 'msg', key=TEST_KEY)
        assert provider.verify_from_proof(TEST_ADDR, 'msg', sig.removeprefix('0x'))

    def test_wrong_message_fails(self, provider):
        sig = provider.sign_from_proof(TEST_ADDR, 'msg', key=TEST_KEY)
        assert not provider.verify_from_proof(TEST_ADDR, 'other', sig)

    def test_wrong_address_fails(self, provider):
        sig = provider.sign_from_proof(TEST_ADDR, 'msg', key=TEST_KEY)
        assert not provider.verify_from_proof(RECIPIENT, 'msg', sig)

    def test_garbage_signature_fails(self, provider):
        assert not provider.verify_from_proof(TEST_ADDR, 'msg', 'not-hex')

    def test_sign_key_address_mismatch_refused(self, provider):
        # Signing for an address the key doesn't derive is a wasted (and misleading) proof.
        assert provider.sign_from_proof(RECIPIENT, 'msg', key=TEST_KEY) == ''

    def test_sign_falls_back_to_env_key(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        sig = provider.sign_from_proof(TEST_ADDR, 'msg')
        assert provider.verify_from_proof(TEST_ADDR, 'msg', sig)


class TestCanSendFrom:
    def test_matches_derived_address_any_case(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        assert provider.can_send_from(TEST_ADDR)
        assert provider.can_send_from(TEST_ADDR.lower())

    def test_no_key_or_wrong_address(self, provider, monkeypatch):
        assert not provider.can_send_from(TEST_ADDR)
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        assert not provider.can_send_from(RECIPIENT)


class TestFetchMatchingTx:
    def test_not_found(self, provider):
        rpc_stub(provider, {'eth_getTransactionByHash': None})
        assert provider.fetch_matching_tx(TX, RECIPIENT, 1) is None

    def test_mined_and_settled(self, provider):
        rpc_stub(provider, mined_tx_responses())
        info = provider.fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert info is not None
        assert info.amount == 10**18
        assert info.block_number == 1_000_000
        assert info.confirmations == 64  # tip 1_000_063 - block 1_000_000 + 1
        assert info.confirmed  # >= 32
        assert info.block_time == 100
        assert info.sender == '0x' + '11' * 20

    def test_recipient_casing_is_irrelevant(self, provider):
        # RPC returns lowercase; the reservation may hold the EIP-55 form. Must still match.
        rpc_stub(provider, mined_tx_responses(to=RECIPIENT.lower()))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is not None

    def test_underpayment_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18 + 1) is None

    def test_wrong_recipient_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.fetch_matching_tx(TX, '0x' + '22' * 20, 1) is None

    def test_contract_creation_rejected(self, provider):
        # 'to' is null for contract creation — must not crash or match.
        rpc_stub(provider, mined_tx_responses(to=None))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 1) is None

    def test_reverted_tx_rejected(self, provider):
        # Inclusion is not settlement: a reverted tx still decodes to/value intact.
        rpc_stub(provider, mined_tx_responses(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_missing_receipt_is_unknown_not_absent(self, provider):
        # A mined tx whose receipt can't be read must raise, never read as 'no deposit' —
        # returning None on a dest leg is a slash-eligible verdict.
        rpc_stub(provider, mined_tx_responses(receipt=None))
        with pytest.raises(ProviderUnreachableError):
            provider.fetch_matching_tx(TX, RECIPIENT, 10**18)

    def test_pending_tx_returns_unconfirmed(self, provider):
        responses = mined_tx_responses()
        responses['eth_getTransactionByHash'] = dict(responses['eth_getTransactionByHash'], blockNumber=None)
        rpc_stub(provider, responses)
        info = provider.fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert info is not None
        assert not info.confirmed
        assert info.confirmations == 0
        assert info.block_number is None

    def test_reorged_block_rejected_when_confirmed(self, provider):
        responses = mined_tx_responses()
        responses['eth_getBlockByNumber'] = {'hash': '0x' + 'ee' * 32, 'timestamp': '0x64'}
        rpc_stub(provider, responses)
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None

    def test_rpc_down_raises_unreachable(self, provider):
        def boom(method, params, timeout=15):
            raise RuntimeError('all ETH RPCs failed')

        provider.eth_rpc = boom
        with pytest.raises(ProviderUnreachableError):
            provider.fetch_matching_tx(TX, RECIPIENT, 1)


class TestVerifyTransactionCasing:
    def test_checksummed_expected_sender_matches_lowercase_rpc_sender(self, provider):
        responses = mined_tx_responses()
        responses['eth_getTransactionByHash'] = dict(
            responses['eth_getTransactionByHash'], **{'from': TEST_ADDR.lower()}
        )
        rpc_stub(provider, responses)
        info = provider.verify_transaction(TX, RECIPIENT, 10**18, expected_sender=TEST_ADDR)
        assert info is not None

    def test_sender_mismatch_still_rejected(self, provider):
        rpc_stub(provider, mined_tx_responses())
        assert provider.verify_transaction(TX, RECIPIENT, 10**18, expected_sender=TEST_ADDR) is None

    def test_self_transfer_rejected_across_casing(self, provider):
        responses = mined_tx_responses()
        responses['eth_getTransactionByHash'] = dict(
            responses['eth_getTransactionByHash'], **{'from': RECIPIENT.lower()}
        )
        rpc_stub(provider, responses)
        assert provider.verify_transaction(TX, RECIPIENT, 10**18) is None


class TestRpcFailover:
    class _Resp:
        def __init__(self, status_code=200, body=None):
            self.status_code = status_code
            self._body = body or {}

        def json(self):
            return self._body

    def test_second_endpoint_serves_after_first_fails(self, provider):
        calls = []

        def post(url, json=None, timeout=15):
            calls.append(url)
            if len(calls) == 1:
                raise ConnectionError('refused')
            return self._Resp(body={'result': '0x10'})

        provider.http.post = post
        provider.rpc_bases = ['https://a.example', 'https://b.example']
        assert provider.eth_rpc('eth_blockNumber', []) == '0x10'
        assert calls == ['https://a.example', 'https://b.example']

    def test_rpc_error_object_fails_over(self, provider):
        responses = iter(
            [self._Resp(body={'error': {'code': -32005, 'message': 'limit'}}), self._Resp(body={'result': '0x1'})]
        )
        provider.http.post = lambda url, json=None, timeout=15: next(responses)
        provider.rpc_bases = ['https://a.example', 'https://b.example']
        assert provider.eth_rpc('eth_chainId', []) == '0x1'

    def test_null_result_is_authoritative_no_failover(self, provider):
        calls = []

        def post(url, json=None, timeout=15):
            calls.append(url)
            return self._Resp(body={'result': None})

        provider.http.post = post
        provider.rpc_bases = ['https://a.example', 'https://b.example']
        assert provider.eth_rpc('eth_getTransactionByHash', [TX]) is None
        assert calls == ['https://a.example']

    def test_all_fail_raises(self, provider):
        provider.http.post = lambda url, json=None, timeout=15: self._Resp(status_code=500)
        provider.rpc_bases = ['https://a.example']
        with pytest.raises(Exception):
            provider.eth_rpc('eth_blockNumber', [])


class TestCheckConnection:
    def test_chain_id_mismatch_fails(self, provider):
        rpc_stub(provider, {'eth_chainId': '0xaa36a7', 'eth_blockNumber': '0x10'})  # sepolia id on mainnet cfg
        with pytest.raises(ConnectionError, match='chain id'):
            provider.check_connection(require_send=False)

    def test_missing_key_fails_when_send_required(self, provider):
        with pytest.raises(ConnectionError, match='ETH_PRIVATE_KEY'):
            provider.check_connection(require_send=True)

    def test_happy_path(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, {'eth_chainId': '0x1', 'eth_blockNumber': '0x10'})
        provider.check_connection(require_send=True)


class TestSendGuards:
    def test_key_mismatch_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        assert provider.send_amount(RECIPIENT, 10**15, from_address=RECIPIENT) is None
        assert 'key mismatch' in provider.last_send_error

    def test_no_key_refused(self, provider):
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'ETH_PRIVATE_KEY' in provider.last_send_error

    def test_insufficient_balance_refused(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(
            provider,
            {
                'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9), 'transactions': []},
                'eth_maxPriorityFeePerGas': hex(10**9),
                'eth_getTransactionCount': '0x0',
                'eth_getBalance': hex(10**12),  # far below amount + gas
                'eth_blockNumber': '0x10',
            },
        )
        assert provider.send_amount(RECIPIENT, 10**18) is None
        assert 'Insufficient ETH' in provider.last_send_error


class TestFindRecentOutgoing:
    def test_finds_settled_transfer_and_ignores_reverted(self, provider):
        matching = {
            'hash': '0x' + 'aa' * 32,
            'from': TEST_ADDR.lower(),
            'to': RECIPIENT.lower(),
            'value': hex(10**18),
        }
        reverted = dict(matching, hash='0x' + 'bb' * 32)

        def rpc(method, params, timeout=15):
            if method == 'eth_blockNumber':
                return hex(100)
            if method == 'eth_getBlockByNumber':
                return {'transactions': [reverted, matching]}
            if method == 'eth_getTransactionReceipt':
                return {'status': '0x1' if params[0] == matching['hash'] else '0x0'}
            raise AssertionError(method)

        provider.eth_rpc = rpc
        # Casing crossed on purpose: cursor keys and matching go through normalize_address.
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, 10**18) == matching['hash']

    def test_no_match_advances_cursor(self, provider):
        def rpc(method, params, timeout=15):
            if method == 'eth_blockNumber':
                return hex(100)
            if method == 'eth_getBlockByNumber':
                return {'transactions': []}
            raise AssertionError(method)

        provider.eth_rpc = rpc
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, 1) is None
        key = (TEST_ADDR.lower(), RECIPIENT.lower(), 1)
        assert provider.scan_cursors[key] == 100
