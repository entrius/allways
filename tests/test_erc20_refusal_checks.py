"""Erc20 refusal checks — the shared slash-gate contract, offline.

`erc20.py` used to hardwire Circle's `isBlacklisted`/`paused` and probe them on every token. A
token without those functions REVERTS, `_refused_at` raised, `delivery_refused` raised, and the
caller (validator/solana_swap_loop.py `_dest_refuses`) read the exception as "defer, don't slash"
— permanently, on every swap. Miners on such a pair were unslashable.

The surface is now declared per registry row. Two directions must hold: a token with no surface
answers WITHOUT touching the RPC, and anything a declared check cannot answer raises rather than
reading as "not refused".
"""

from dataclasses import replace

import pytest

from allways.assets.erc20 import SEL_IS_BLACKLISTED, Erc20, _refusal_call
from allways.assets.evm import EvmRpcError
from allways.chains import CHAIN_ETHUSDC
from allways.constants import CANCEL_REASON_ERC20_BLACKLIST, CANCEL_REASON_ERC20_PAUSED

RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'
DECLARED = CHAIN_ETHUSDC  # ('isBlacklisted(address)', 'paused()')
NO_CHECKS = replace(CHAIN_ETHUSDC, id='noctl', name='No-checks token', refusal_checks=())


@pytest.fixture(autouse=True)
def eth_env(monkeypatch):
    monkeypatch.setenv('ETH_NETWORK', 'mainnet')
    for var in ('ETH_RPC_URLS', 'ETH_PRIVATE_KEY', 'NOCTL_TOKEN_CONTRACT', 'ETHUSDC_TOKEN_CONTRACT'):
        monkeypatch.delenv(var, raising=False)


def build(chain_def, rpc):
    provider = Erc20(chain_def)
    provider.chain.eth_rpc = rpc
    return provider


def answering(selector, value=1):
    """RPC that answers ``value`` for one selector and 0 for the rest."""

    def rpc(method, params, **kw):
        if method == 'eth_blockNumber':
            return hex(1000)
        return hex(value if params[0]['data'].startswith(selector) else 0)

    return rpc


def test_an_undeclared_row_fails_at_construction():
    # Boot is where every provider is built. A row that forgot to declare would answer
    # "never refused" for every destination and slash honest miners.
    with pytest.raises(ValueError, match='refusal_checks'):
        Erc20(replace(DECLARED, refusal_checks=None))


def test_check_connection_raises_when_token_probe_errors():
    # An ERC-20's token contract is its asset identity — a getCode probe we can't complete must fail
    # boot, not warn-and-continue (a warn boots a broken/misconfigured token as "ready").
    def rpc(method, params, **kw):
        if method == 'eth_getCode':
            raise EvmRpcError('token RPC unreachable')
        return hex(1000)

    provider = build(DECLARED, rpc)
    provider.chain.connect_network = lambda: (1, 100)  # host chain reachable; isolate the token probe
    with pytest.raises(ConnectionError):
        provider.check_connection(require_send=False)


class TestNoChecksNeverTouchTheRpc:
    """Recorded on a transcript, never by raising inside the stub — ``can_deliver_to`` swallows
    every exception into its fail-open ``True``, so a raising stub would be satisfied by the
    exact failure it exists to detect."""

    def test_delivery_refused_is_false(self):
        def forbidden(*_a, **_kw):
            raise AssertionError('a token declaring no checks must not reach the RPC')

        assert build(NO_CHECKS, forbidden).delivery_refused(RECIPIENT, since_unix=0) is False

    def test_can_deliver_to_is_true(self):
        calls = []
        provider = build(NO_CHECKS, lambda method, params, **kw: calls.append(method))
        assert provider.can_deliver_to(RECIPIENT, amount=1) is True
        assert calls == []


class TestDeclaredChecks:
    def test_each_declared_check_is_positive_evidence(self):
        for selector, _takes_address in Erc20(DECLARED)._checks:
            assert build(DECLARED, answering(selector)).delivery_refused(RECIPIENT, since_unix=0) is True

    def test_a_clean_destination_is_deliverable(self):
        assert build(DECLARED, answering('0xdead', value=0)).can_deliver_to(RECIPIENT, amount=1) is True

    @pytest.mark.parametrize(
        'answer',
        (
            pytest.param('0x', id='empty-permissive-fallback'),
            pytest.param(None, id='null'),
            pytest.param('0xnothex', id='garbage'),
        ),
    )
    def test_an_unanswerable_check_raises_rather_than_clearing(self, answer):
        # WETH9/WBNB-style fallbacks accept ANY selector and answer empty instead of reverting.
        # Reading that as 0 would clear a genuinely frozen destination and slash an honest miner.
        provider = build(DECLARED, lambda method, params, **kw: answer)
        with pytest.raises((ValueError, TypeError)):
            provider.delivery_refused(RECIPIENT, since_unix=0)

    def test_a_reverting_check_raises_rather_than_clearing(self):
        # The original bug's shape: a declared function the contract lacks must defer the slash,
        # never clear it. Deferring is safe; clearing slashes someone who did nothing wrong.
        def reverts(*_a, **_kw):
            raise EvmRpcError('execution reverted', {'code': 3, 'message': 'execution reverted'})

        with pytest.raises(EvmRpcError):
            build(DECLARED, reverts).delivery_refused(RECIPIENT, since_unix=0)

    def test_can_deliver_to_fails_open_when_the_rpc_is_down(self):
        # Reserve-time only: refusing every reservation on a flaky RPC would strand the pair, and
        # the slash gate above is what actually protects the miner.
        def down(*_a, **_kw):
            raise ConnectionError('every endpoint is down')

        assert build(DECLARED, down).can_deliver_to(RECIPIENT, amount=1) is True


class TestCancelEvidenceReadsTheDeclaredSurface:
    """#669 added the no-fault cancel; it hardcoded Circle's selectors, so a token declaring any
    other surface produced no evidence and rode to a TIMEOUT slash instead. No non-Circle token
    existed in the registry then — paxg is the first, so this is the guard for every one after."""

    PAXOS = replace(
        CHAIN_ETHUSDC, id='paxos', name='Paxos-style token', refusal_checks=('isFrozen(address)', 'paused()')
    )

    @pytest.mark.parametrize(
        'signature,expected',
        (('isFrozen(address)', CANCEL_REASON_ERC20_BLACKLIST), ('paused()', CANCEL_REASON_ERC20_PAUSED)),
    )
    def test_a_non_circle_surface_still_yields_cancel_evidence(self, signature, expected):
        answering, _ = _refusal_call(signature)

        def rpc(method, params, **kw):
            # A declared selector the contract really has answers; Circle's does not exist here
            # (exactly as on real PAXG), and must never be reached.
            assert not params[0]['data'].startswith(SEL_IS_BLACKLISTED), 'probed Circle on a Paxos-style row'
            return f'0x{int(params[0]["data"].startswith(answering)):064x}'

        provider = build(self.PAXOS, rpc)
        assert provider.cancel_evidence(RECIPIENT, amount=1) == expected

    def test_a_clean_destination_yields_no_evidence(self):
        assert build(self.PAXOS, lambda m, p, **kw: f'0x{0:064x}').cancel_evidence(RECIPIENT, amount=1) is None

    def test_rpc_trouble_yields_no_evidence_rather_than_a_false_cancel(self):
        def down(*_a, **_kw):
            raise ConnectionError('every endpoint is down')

        assert build(self.PAXOS, down).cancel_evidence(RECIPIENT, amount=1) is None
