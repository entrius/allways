"""EvmCoin / EvmChain unit tests, run on Ether — all offline (RPC layer mocked, signing is pure crypto).

The representative suite for the shared native-coin provider and the EVM transport: casing at
comparison boundaries, the settled cache, the endpoint failover ladder and absence quorum, send
dedup, delivery gates and the fulfillment gas floor. The per-row invariants (network guards,
settlement depth, unknown-not-absent, the signed payload) run over every EVM row in
tests/test_evm_providers.py.
"""

from typing import Optional

import pytest
from eth_account import Account

from allways.assets.asset import ProviderUnreachableError
from allways.assets.eth import Ether
from allways.assets.evm import EvmRpcError
from allways.assets.evm_coin import DUST_PROBE_WEI, SWAP_PROBE_WEI, ZERO_ADDRESS

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
    return Ether()


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
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
            'blockHash': block_hash,
        },
        'eth_getTransactionReceipt': receipt,
        'eth_blockNumber': hex(tip),
        'eth_getBlockByNumber': {'hash': block_hash, 'timestamp': '0x64'},
    }


def raising(error: Exception):
    """An rpc_stub response that raises ``error``."""

    def rpc(params):
        raise error

    return rpc


def counting_rpc_stub(provider, responses: dict):
    """rpc_stub that also tallies calls per method into the returned dict."""
    calls: dict = {}

    def fake_rpc(method, params, timeout=15, **kw):
        calls[method] = calls.get(method, 0) + 1
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
    return calls


class TestProofSigning:
    def test_sign_verify_roundtrip_any_address_case(self, provider):
        sig = provider.chain.sign_from_proof(TEST_ADDR, 'proof: abc', key=TEST_KEY)
        assert sig
        assert provider.chain.verify_from_proof(TEST_ADDR, 'proof: abc', sig)
        assert provider.chain.verify_from_proof(TEST_ADDR.lower(), 'proof: abc', sig)

    def test_wrong_message_address_or_garbage_fails(self, provider):
        sig = provider.chain.sign_from_proof(TEST_ADDR, 'msg', key=TEST_KEY)
        assert not provider.chain.verify_from_proof(TEST_ADDR, 'other', sig)
        assert not provider.chain.verify_from_proof(RECIPIENT, 'msg', sig)
        assert not provider.chain.verify_from_proof(TEST_ADDR, 'msg', 'not-hex')

    def test_sign_key_address_mismatch_refused(self, provider):
        # Signing for an address the key doesn't derive is a wasted (and misleading) proof.
        assert provider.chain.sign_from_proof(RECIPIENT, 'msg', key=TEST_KEY) == ''


class TestCanSendFrom:
    def test_only_the_keys_own_address_any_case(self, provider, monkeypatch):
        assert not provider.can_send_from(TEST_ADDR)
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        assert provider.can_send_from(TEST_ADDR)
        assert provider.can_send_from(TEST_ADDR.lower())
        assert not provider.can_send_from(RECIPIENT)


class TestFetchMatchingTx:
    def test_contract_creation_rejected(self, provider):
        # 'to' is null for contract creation — must not crash or match.
        rpc_stub(provider, mined_tx_responses(to=None))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 1) is None

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

        provider.chain.eth_rpc = boom
        with pytest.raises(ProviderUnreachableError):
            provider.fetch_matching_tx(TX, RECIPIENT, 1)


class TestSettledCache:
    """A settled read is cached per block hash; a reorg changes the hash → cache miss → full
    refetch. Pending/reverted reads are never cached."""

    def test_reorg_invalidates_cache(self, provider):
        responses = mined_tx_responses()
        calls = counting_rpc_stub(provider, responses)
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is not None
        # Reorg: the tx now reports a different blockHash — receipt must be refetched, and with
        # the receipt/block now disagreeing with the mined-at hash, the leg is rejected until the
        # view settles (next pass refetches cleanly).
        new_hash = '0x' + 'ee' * 32
        responses['eth_getTransactionByHash'] = dict(responses['eth_getTransactionByHash'], blockHash=new_hash)
        provider.fetch_matching_tx(TX, RECIPIENT, 10**18)
        assert calls['eth_getTransactionReceipt'] == 2

    def test_pending_and_reverted_never_cached(self, provider):
        pending = mined_tx_responses()
        pending['eth_getTransactionByHash'] = dict(
            pending['eth_getTransactionByHash'], blockNumber=None, blockHash=None
        )
        rpc_stub(provider, pending)
        assert not provider.fetch_matching_tx(TX, RECIPIENT, 10**18).confirmed
        assert TX not in provider._settled_cache
        rpc_stub(provider, mined_tx_responses(status='0x0'))
        assert provider.fetch_matching_tx(TX, RECIPIENT, 10**18) is None
        assert TX not in provider._settled_cache


class TestVerifyTransactionCasing:
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

        provider.chain.http.post = post
        provider.chain.rpc_bases = ['https://a.example', 'https://b.example']
        assert provider.chain.eth_rpc('eth_blockNumber', []) == '0x10'
        assert calls == ['https://a.example', 'https://b.example']
        # A JSON-RPC error object (rate limit, pruned state) is endpoint trouble too.
        responses = iter(
            [self._Resp(body={'error': {'code': -32005, 'message': 'limit'}}), self._Resp(body={'result': '0x1'})]
        )
        provider.chain.http.post = lambda url, json=None, timeout=15: next(responses)
        assert provider.chain.eth_rpc('eth_chainId', []) == '0x1'

    def test_null_result_is_authoritative_no_failover(self, provider):
        calls = []

        def post(url, json=None, timeout=15):
            calls.append(url)
            return self._Resp(body={'result': None})

        provider.chain.http.post = post
        provider.chain.rpc_bases = ['https://a.example', 'https://b.example']
        assert provider.chain.eth_rpc('eth_getTransactionByHash', [TX]) is None
        assert calls == ['https://a.example']

    def test_all_fail_raises(self, provider):
        provider.chain.http.post = lambda url, json=None, timeout=15: self._Resp(status_code=500)
        provider.chain.rpc_bases = ['https://a.example']
        with pytest.raises(Exception):
            provider.chain.eth_rpc('eth_blockNumber', [])

    def test_revert_outranks_later_transport_failure(self, provider):
        # A deterministic revert on endpoint A must not be masked by endpoint B being down —
        # else the caller reads "transient" and broadcasts a doomed tx.
        def post(url, json=None, timeout=15):
            if 'a.example' in url:
                return self._Resp(body={'error': {'code': 3, 'message': 'execution reverted'}})
            raise ConnectionError('down')

        provider.chain.http.post = post
        provider.chain.rpc_bases = ['https://a.example', 'https://b.example']
        with pytest.raises(EvmRpcError) as exc:
            provider.chain.eth_rpc('eth_estimateGas', [{}])
        assert exc.value.is_execution_revert


class TestCheckConnection:
    def test_missing_key_fails_when_send_required(self, provider):
        with pytest.raises(ConnectionError, match='ETH_PRIVATE_KEY'):
            provider.check_connection(require_send=True)

    def test_happy_path(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)
        rpc_stub(provider, {'eth_chainId': '0x1', 'eth_blockNumber': '0x10'})
        provider.check_connection(require_send=True)


class TestSendGuards:
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

        provider.chain.eth_rpc = rpc
        # Casing crossed on purpose: cursor keys and matching go through normalize_address.
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, 10**18) == matching['hash']


class TestAbsenceQuorum:
    """'Absent' (the verdict that slashes) requires every endpoint to reachably agree."""

    class _Resp:
        status_code = 200

        def __init__(self, body):
            self._body = body

        def json(self):
            return self._body

    def _posts(self, provider, per_url: dict):
        def post(url, json=None, timeout=15):
            value = per_url[url]
            if isinstance(value, Exception):
                raise value
            return self._Resp({'result': value})

        provider.chain.http.post = post
        provider.chain.rpc_bases = list(per_url)

    def test_second_endpoint_overrules_a_stale_null(self, provider):
        self._posts(provider, {'https://a.example': None, 'https://b.example': {'hash': TX}})
        assert provider.chain.eth_rpc('eth_getTransactionByHash', [TX], null_needs_quorum=True) == {'hash': TX}

    def test_unanimous_null_is_absent(self, provider):
        self._posts(provider, {'https://a.example': None, 'https://b.example': None})
        assert provider.chain.eth_rpc('eth_getTransactionByHash', [TX], null_needs_quorum=True) is None

    def test_null_plus_unreachable_raises(self, provider):
        self._posts(provider, {'https://a.example': None, 'https://b.example': ConnectionError('down')})
        with pytest.raises(ProviderUnreachableError):
            provider.chain.eth_rpc('eth_getTransactionByHash', [TX], null_needs_quorum=True)


class TestBlockTimeUnknownNotStale:
    """An unreadable block-time must raise (SKIP), not read as 'replay' and slash."""

    def test_block_fetch_failure_raises(self, provider):
        responses = mined_tx_responses()

        def boom(params):
            raise RuntimeError('node behind')

        responses['eth_getBlockByNumber'] = boom
        rpc_stub(provider, responses)
        with pytest.raises(ProviderUnreachableError):
            provider.fetch_matching_tx(TX, RECIPIENT, 10**18)


SEND_RESPONSES = {
    'eth_blockNumber': hex(100),
    'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9)},
    'eth_maxPriorityFeePerGas': hex(10**9),
    'eth_getCode': '0x',
    'eth_getTransactionCount': '0x0',
    'eth_getBalance': hex(10**19),
}


class TestSendResilience:
    @pytest.fixture(autouse=True)
    def _key(self, provider, monkeypatch):
        # After `provider`, whose fixture delenvs the key.
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)

    def test_all_lowercase_committed_dest_still_sends(self, provider):
        # An all-lowercase dest is legal (EIP-55 is optional) and passes is_valid_address, but
        # eth-account refuses a non-checksummed `to` — the sign path checksums it, or the miner
        # could never pay this dest and would ride to a slash.
        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction='0x' + 'ee' * 32))
        assert provider.send_amount(RECIPIENT.lower(), 10**15) is not None

    def test_null_broadcast_response_still_returns_the_local_hash(self, provider):
        # The hash is computed locally from the signed tx — a quirky 'result: null' from the
        # broadcast must never become a blank hash in the miner's persisted send record.
        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction=None))
        result = provider.send_amount(RECIPIENT, 10**15)
        assert result is not None
        tx_hash, _ = result
        assert tx_hash and tx_hash.startswith('0x')
        assert tx_hash in provider.broadcasted_txids

    def test_lost_broadcast_response_recovers_via_lookup(self, provider):
        def boom(params):
            raise ConnectionError('reset mid-flight')

        rpc_stub(
            provider,
            dict(SEND_RESPONSES, eth_sendRawTransaction=boom, eth_getTransactionByHash={'hash': 'seen'}),
        )
        result = provider.send_amount(RECIPIENT, 10**15)
        assert result is not None
        assert result[0] in provider.broadcasted_txids

    def test_unprovable_broadcast_fails_closed(self, provider):
        def boom(params):
            raise ConnectionError('down')

        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction=boom, eth_getTransactionByHash=boom))
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'broadcast failed' in provider.last_send_error

    def test_prior_broadcast_in_mempool_is_reused_not_resent(self, provider):
        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, '', 95)
        # No eth_sendRawTransaction in the map: a resend attempt would fail the test.
        rpc_stub(provider, dict(SEND_RESPONSES, eth_getTransactionByHash={'hash': TX}))
        assert provider.send_amount(RECIPIENT, 10**15) == (TX, 0)

    def test_same_to_amount_concurrent_payouts_both_pay(self, provider):
        # Fund-safety #2 (v3.1): dedup keys on the SWAP, not the payout shape. A prior broadcast for
        # swap A must not be handed to concurrent swap B with the identical (to, amount) — B gets a
        # FRESH broadcast, so both users are paid.
        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, 'swap-a', 95)
        rpc_stub(provider, dict(SEND_RESPONSES, eth_getTransactionByHash={'hash': TX}))
        result = provider.send_amount(RECIPIENT, 10**15, dedup_key='swap-b')
        assert result is not None and result[0] != TX, 'swap B must broadcast its own tx'
        # Swap A retrying still reuses ITS tx.
        assert provider.send_amount(RECIPIENT, 10**15, dedup_key='swap-a') == (TX, 0)

    def test_reverted_prior_broadcast_clears_and_sends_fresh(self, provider):
        # A reverted tx moved no funds — reusing its hash would hand the validator a
        # rejected leg forever. It must clear and allow a fresh send.
        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, '', 95)
        rpc_stub(
            provider,
            dict(
                SEND_RESPONSES,
                eth_getTransactionByHash={'hash': TX, 'blockNumber': hex(96)},
                eth_getTransactionReceipt={'status': '0x0'},
                eth_sendRawTransaction='0x' + 'cc' * 32,
            ),
        )
        assert provider.send_amount(RECIPIENT, 10**15) == ('0x' + 'cc' * 32, 0)
        assert TX not in provider.broadcasted_txids

    def test_prior_broadcast_outside_window_is_pruned_not_reused(self, provider):
        # Head 100, seen at 60 → beyond SCAN_LOOKBACK_BLOCKS: a later same-amount swap must
        # never resolve to an earlier swap's consumed tx (freshness would slash the miner).
        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, '', 60)
        rpc_stub(provider, dict(SEND_RESPONSES, eth_sendRawTransaction='0x' + 'cc' * 32))
        assert provider.send_amount(RECIPIENT, 10**15) == ('0x' + 'cc' * 32, 0)
        assert TX not in provider.broadcasted_txids

    def test_mined_prior_with_unavailable_receipt_blocks_send(self, provider):
        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, '', 95)
        rpc_stub(
            provider,
            dict(
                SEND_RESPONSES,
                eth_getTransactionByHash={'hash': TX, 'blockNumber': hex(96)},
                eth_getTransactionReceipt=None,
            ),
        )
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'double send' in provider.last_send_error

    def test_unresolved_prior_broadcast_blocks_a_fresh_send(self, provider):
        def boom(params):
            raise ConnectionError('down')

        provider.broadcasted_txids[TX] = (RECIPIENT.lower(), 10**15, '', 95)
        rpc_stub(provider, dict(SEND_RESPONSES, eth_getTransactionByHash=boom))
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'double send' in provider.last_send_error

    def test_unreadable_chain_head_blocks_send(self, provider):
        def down(params):
            raise ConnectionError('down')

        rpc_stub(provider, dict(SEND_RESPONSES, eth_blockNumber=down))
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'chain head' in provider.last_send_error


class TestScannerCursorParking:
    """An unreadable block parks the cursor — leaping it would orphan a deposit forever."""

    MATCH = {
        'hash': '0x' + 'aa' * 32,
        'from': TEST_ADDR.lower(),
        'to': RECIPIENT.lower(),
        'value': hex(10**18),
    }

    def test_failed_block_parks_cursor_then_recovers(self, provider):
        state = {'broken': True}

        def rpc(method, params, timeout=15, **kw):
            if method == 'eth_blockNumber':
                return hex(100)
            if method == 'eth_getBlockByNumber':
                block_num = int(params[0], 16)
                if block_num == 80 and state['broken']:
                    raise RuntimeError('read failed')
                return {'transactions': [self.MATCH] if block_num == 80 else []}
            if method == 'eth_getTransactionReceipt':
                return {'status': '0x1'}
            raise AssertionError(method)

        provider.chain.eth_rpc = rpc
        key = (TEST_ADDR.lower(), RECIPIENT.lower(), 10**18)
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, 10**18) is None
        assert provider.scan_cursors[key] == 79
        state['broken'] = False
        assert provider.find_recent_outgoing(TEST_ADDR, RECIPIENT, 10**18) == self.MATCH['hash']


class TestDeliveryGates:
    """B-lite: simulation where helpful (reserve UX, miner gas), getCode where the decision
    must be ungameable (slash gate)."""

    def _revert(self, params):
        raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

    def test_can_deliver_to_eoa_true_without_simulation(self, provider):
        calls = counting_rpc_stub(provider, {'eth_getCode': '0x'})
        assert provider.can_deliver_to(RECIPIENT, 10**15)
        assert 'eth_estimateGas' not in calls

    def test_can_deliver_to_reverting_contract_false(self, provider):
        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': self._revert})
        assert not provider.can_deliver_to(RECIPIENT, 10**15)

    def test_can_deliver_to_fails_open_on_rpc_trouble(self, provider):
        def down(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_getCode': down})
        assert provider.can_deliver_to(RECIPIENT, 10**15)

    def test_can_deliver_to_simulates_from_committed_sender(self, provider):
        # A dest can accept from address(0) yet revert for the miner's actual sender (e.g.
        # `receive()` gated on msg.sender) — the reserve gate must run the send's simulation.
        def gas(params):
            if params[0]['from'] == TEST_ADDR:
                raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})
            return '0xb000'

        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': gas})
        assert provider.can_deliver_to(RECIPIENT, 10**15)  # sender unknown: from-zero still passes
        assert not provider.can_deliver_to(RECIPIENT, 10**15, from_address=TEST_ADDR)

    def test_can_deliver_to_applies_send_path_gas_cap(self, provider):
        # 90k estimates under MAX_TRANSFER_GAS raw, but the send path's +20% headroom puts it
        # at 108k — the miner would refuse to pay, so the reservation must never start.
        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': hex(90_000)})
        assert not provider.can_deliver_to(RECIPIENT, 10**15)

    def test_address_refused_needs_a_revert_at_the_swap_value_and_at_dust(self, provider):
        # The probe knows no swap amount or sender: it sends from address(0) at both values.
        seen = []

        def gas(params):
            seen.append(params[0])
            self._revert(params)

        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': gas})
        assert provider.address_refused(RECIPIENT)
        assert seen == [
            {'from': ZERO_ADDRESS, 'to': RECIPIENT, 'value': hex(v)} for v in (SWAP_PROBE_WEI, DUST_PROBE_WEI)
        ]

    def test_address_refused_dust_rejecting_receiver_false(self, provider):
        def gas(params):
            return '0xb000' if int(params[0]['value'], 16) == SWAP_PROBE_WEI else self._revert(params)

        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': gas})
        assert not provider.address_refused(RECIPIENT)

    @pytest.mark.parametrize(
        'estimate',
        [
            '0xb000',
            hex(90_000),  # over MAX_TRANSFER_GAS with headroom: the reserve gate's concern, not this address's fault
            raising(EvmRpcError('insufficient funds', {'code': -32000, 'message': 'insufficient funds'})),
        ],
    )
    def test_address_refused_only_on_a_structured_revert(self, provider, estimate):
        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': estimate})
        assert not provider.address_refused(RECIPIENT)

    @pytest.mark.parametrize('unreadable', ['eth_getCode', 'eth_estimateGas'])
    def test_address_refused_raises_when_the_chain_cannot_be_read(self, provider, unreadable):
        def down(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_getCode': '0x6080', 'eth_estimateGas': '0xb000', unreadable: down})
        with pytest.raises(ConnectionError):
            provider.address_refused(RECIPIENT)

    def test_delivery_refused_code_only_in_window_history(self, provider):
        # Code was attached mid-window then detached — the historical probe still sees it.
        rpc_stub(
            provider, {'eth_blockNumber': hex(1000), 'eth_getCode': lambda p: '0x' if p[1] == 'latest' else '0x6080'}
        )
        assert provider.delivery_refused(RECIPIENT, 0)

    def test_delivery_refused_clean_eoa_false(self, provider):
        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getCode': '0x'})
        assert not provider.delivery_refused(RECIPIENT, 0)

    def test_delivery_refused_raises_when_view_unavailable(self, provider):
        def down(params):
            raise ConnectionError('down')

        rpc_stub(provider, {'eth_blockNumber': hex(1000), 'eth_getCode': down})
        with pytest.raises(Exception):
            provider.delivery_refused(RECIPIENT, 0)


class TestTransferGas:
    @pytest.fixture(autouse=True)
    def _key(self, provider, monkeypatch):
        monkeypatch.setenv('ETH_PRIVATE_KEY', TEST_KEY)

    def _send(self, provider, estimate):
        responses = dict(SEND_RESPONSES, eth_sendRawTransaction='0x' + 'cc' * 32, eth_estimateGas=estimate)
        rpc_stub(provider, responses)
        return provider.send_amount(RECIPIENT, 10**15)

    def _signed_raw(self, gas):
        signed = Account.sign_transaction(
            {
                'chainId': 1,
                'nonce': 0,
                'to': RECIPIENT,
                'value': 10**15,
                'gas': gas,
                'maxFeePerGas': 3 * 10**9,
                'maxPriorityFeePerGas': 10**9,
            },
            TEST_KEY,
        )
        return getattr(signed, 'raw_transaction', None) or getattr(signed, 'rawTransaction')

    def test_contract_destination_signs_with_fulfillment_gas_floor(self, provider):
        broadcast = {}

        def capture(params):
            broadcast['raw'] = params[0]
            return TX

        responses = dict(
            SEND_RESPONSES, eth_getCode='0x6080', eth_estimateGas=hex(25_000), eth_sendRawTransaction=capture
        )
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, 10**15) == (TX, 0)
        assert broadcast['raw'].removeprefix('0x') == self._signed_raw(100_000).hex().removeprefix('0x')

    def test_contract_destination_balance_preflight_uses_fulfillment_gas_floor(self, provider):
        balance = 10**15 + 30_000 * 3 * 10**9
        responses = dict(SEND_RESPONSES, eth_getCode='0x6080', eth_estimateGas=hex(25_000), eth_getBalance=hex(balance))
        rpc_stub(provider, responses)
        assert provider.send_amount(RECIPIENT, 10**15) is None
        assert 'Insufficient' in provider.last_send_error
        assert str(100_000 * 3 * 10**9) in provider.last_send_error

    def test_reverting_destination_broadcasts_floor_gas_evidence(self, provider):
        # P0: a reverting dest no longer bails silently — the miner broadcasts a floor-gas attempt so the
        # on-chain reverted tx becomes no-fault-cancel evidence (a REFUSE, not a false timeout slash).
        def revert(params):
            raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

        assert self._send(provider, revert) is not None

    def test_absurd_estimate_broadcasts_floor_gas_evidence(self, provider):
        # An over-cap estimate likewise broadcasts at the fulfillment gas floor rather than refusing to send.
        assert self._send(provider, hex(500_000)) is not None

    def test_non_revert_estimate_error_signs_with_fulfillment_gas_floor(self, provider):
        broadcast = {}

        def capture(params):
            broadcast['raw'] = params[0]
            return TX

        def error(params):
            raise EvmRpcError('EVM error: PrecompileError', {'code': -32003, 'message': 'EVM error: PrecompileError'})

        rpc_stub(provider, dict(SEND_RESPONSES, eth_estimateGas=error, eth_sendRawTransaction=capture))
        assert provider.send_amount(RECIPIENT, 10**15) == (TX, 0)
        assert broadcast['raw'].removeprefix('0x') == self._signed_raw(100_000).hex().removeprefix('0x')
