"""Regression: transient RPC faults must not abort an in-flight swap.

The first mainnet BTC origination crashed when `getSignatureStatuses` returned a JSON-RPC -32603
Internal error mid-crank: `_call` raised it as fatal, it escaped the outcome-driven `_poll_drawn`
loop, and the CLI died — even though the bid had landed and the pool had already drawn the taker.

Now: idempotent reads retry on transient faults inside `_call`; `confirm` treats an unreadable status
as "unknown, keep polling" (never "failed"); state-changing sends are NOT auto-retried (a duplicate
submit is the caller's call).
"""

from unittest.mock import MagicMock

import pytest
import requests

from allways.solana.rpc import (
    _MAX_READ_RETRIES,
    SolanaRpc,
    SolanaRpcError,
    TransientRpcError,
)


class _Resp:
    def __init__(self, *, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._body


def _rpc(responses):
    rpc = SolanaRpc('http://rpc.test')
    rpc._session = MagicMock()
    rpc._session.post.side_effect = responses
    return rpc


def _err(code):
    return {'error': {'code': code, 'message': 'boom'}}


def _ok(result):
    return {'result': result}


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr('allways.solana.rpc.time.sleep', lambda *_: None)


def test_call_retries_transient_rpc_code_then_succeeds():
    rpc = _rpc([_Resp(body=_err(-32603)), _Resp(body=_err(-32603)), _Resp(body=_ok({'value': [None]}))])
    assert rpc._call('getSignatureStatuses', [['sig'], {}]) == {'value': [None]}
    assert rpc._session.post.call_count == 3


def test_call_retries_transient_http_status():
    rpc = _rpc([_Resp(status_code=503), _Resp(body=_ok(7))])
    assert rpc._call('getSlot', [{}]) == 7
    assert rpc._session.post.call_count == 2


def test_call_retries_transport_timeout():
    rpc = _rpc([requests.Timeout('slow'), _Resp(body=_ok(9))])
    assert rpc._call('getBalance', ['pk', {}]) == 9
    assert rpc._session.post.call_count == 2


def test_call_does_not_retry_deterministic_error():
    rpc = _rpc([_Resp(body=_err(-32602))])  # invalid params — a bug, not a hiccup
    with pytest.raises(SolanaRpcError) as ei:
        rpc._call('getAccountInfo', ['pk', {}])
    assert not isinstance(ei.value, TransientRpcError)
    assert rpc._session.post.call_count == 1


def test_call_does_not_auto_retry_send_transaction():
    # A transient on a send surfaces immediately (typed) — the caller decides whether a re-submit is safe.
    rpc = _rpc([_Resp(body=_err(-32603))])
    with pytest.raises(TransientRpcError):
        rpc._call('sendTransaction', ['rawtx', {}])
    assert rpc._session.post.call_count == 1  # exactly once — no double submit


def test_call_gives_up_after_max_retries():
    rpc = _rpc([_Resp(body=_err(-32603))] * (_MAX_READ_RETRIES + 1))
    with pytest.raises(TransientRpcError):
        rpc._call('getSlot', [{}])
    assert rpc._session.post.call_count == _MAX_READ_RETRIES + 1


def test_confirm_tolerates_transient_status_read_then_confirms():
    rpc = SolanaRpc('http://rpc.test')
    calls = {'n': 0}

    def flaky(_sigs):
        calls['n'] += 1
        if calls['n'] < 3:
            raise TransientRpcError('getSignatureStatuses: -32603')
        return [{'err': None, 'confirmationStatus': 'confirmed'}]

    rpc.get_signature_statuses = flaky
    assert rpc.confirm('sig', timeout=5, poll=0)['confirmationStatus'] == 'confirmed'
    assert calls['n'] == 3  # kept polling through the transient reads instead of raising


def test_confirm_still_raises_on_a_real_tx_error():
    rpc = SolanaRpc('http://rpc.test')
    rpc.get_signature_statuses = lambda _sigs: [{'err': {'InstructionError': [0, {'Custom': 6001}]}}]
    with pytest.raises(SolanaRpcError):
        rpc.confirm('sig', timeout=5, poll=0)
