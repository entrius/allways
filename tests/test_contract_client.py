"""AllwaysContractClient.substrate_call: reconnect-on-WS-error retry logic.

The wrapper handles transient substrate WebSocket deaths transparently:
on a connection-class exception it invokes the owner-supplied reconnect
callback (which must replace ``client.subtensor`` with a fresh handle)
and retries the call once. Anything else propagates as-is.
"""

from unittest.mock import MagicMock

import pytest
from websockets.exceptions import ConnectionClosed

from allways.contract_client import AllwaysContractClient


def make_subtensor() -> MagicMock:
    sub = MagicMock()
    sub.substrate = MagicMock()
    return sub


class TestSubstrateCall:
    def test_returns_value_when_call_succeeds(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        result = client.substrate_call(lambda s: 'ok')
        assert result == 'ok'

    def test_no_reconnect_callback_re_raises(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())

        def fn(s):
            raise ConnectionClosed(None, None)

        with pytest.raises(ConnectionClosed):
            client.substrate_call(fn)

    def test_reconnects_and_retries_on_connection_closed(self):
        first = make_subtensor()
        second = make_subtensor()

        calls = []

        def fn(s):
            calls.append(s)
            if s is first.substrate:
                raise ConnectionClosed(None, None)
            return 'recovered'

        client = AllwaysContractClient(contract_address='5xx', subtensor=first)
        client.reconnect_subtensor = lambda: setattr(client, 'subtensor', second)

        result = client.substrate_call(fn)

        assert result == 'recovered'
        assert calls == [first.substrate, second.substrate]

    def test_retries_on_timeout_and_connection_error(self):
        for exc in (TimeoutError(), ConnectionError()):
            first = make_subtensor()
            second = make_subtensor()
            attempts = [0]

            def fn(s, exc=exc):
                attempts[0] += 1
                if attempts[0] == 1:
                    raise exc
                return 'ok'

            client = AllwaysContractClient(contract_address='5xx', subtensor=first)
            client.reconnect_subtensor = lambda: setattr(client, 'subtensor', second)

            assert client.substrate_call(fn) == 'ok'
            assert attempts[0] == 2

    def test_reconnect_callback_failure_re_raises_original(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())

        original = ConnectionClosed(None, None)

        def fn(s):
            raise original

        def failing_reconnect():
            raise RuntimeError('cannot rebuild')

        client.reconnect_subtensor = failing_reconnect

        with pytest.raises(ConnectionClosed) as ei:
            client.substrate_call(fn)
        assert ei.value is original

    def test_does_not_retry_on_non_connection_error(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        reconnect = MagicMock()
        client.reconnect_subtensor = reconnect

        def fn(s):
            raise ValueError('bad payload')

        with pytest.raises(ValueError):
            client.substrate_call(fn)
        reconnect.assert_not_called()

    def test_second_attempt_failure_propagates(self):
        first = make_subtensor()
        second = make_subtensor()

        def fn(s):
            raise ConnectionClosed(None, None)

        client = AllwaysContractClient(contract_address='5xx', subtensor=first)
        client.reconnect_subtensor = lambda: setattr(client, 'subtensor', second)

        with pytest.raises(ConnectionClosed):
            client.substrate_call(fn)
