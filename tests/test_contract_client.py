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


class TestPendingExtensionDecode:
    """SCALE decode of get_pending_*_extension Option<PendingExtension> payloads."""

    @staticmethod
    def _encode_some(submitter_bytes: bytes, target: int, proposed: int) -> bytes:
        import struct
        return b'\x01' + submitter_bytes + struct.pack('<II', target, proposed)

    def test_decodes_some(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        submitter = bytes(range(32))
        payload = self._encode_some(submitter, target=12345, proposed=12000)
        result = client._decode_pending_extension(payload)
        assert result is not None
        assert result.target_block == 12345
        assert result.proposed_at == 12000

    def test_decodes_none_discriminant(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        assert client._decode_pending_extension(b'\x00') is None

    def test_empty_payload_returns_none(self):
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        assert client._decode_pending_extension(b'') is None

    def test_unexpected_discriminant_returns_none(self):
        # Anything other than 0x00/0x01 is malformed; treat as None rather than
        # raising — matches the existing get_reservation_data shape.
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        assert client._decode_pending_extension(b'\x02junk') is None


class TestExtensionSelectorWiring:
    """Selectors and arg schemas are wired up for every new method."""

    @pytest.mark.parametrize('method,expected_selector', [
        ('propose_extend_reservation', '9c9a8e8e'),
        ('challenge_extend_reservation', '40b77e21'),
        ('finalize_extend_reservation', 'baf47953'),
        ('propose_extend_timeout', '94c87a1d'),
        ('challenge_extend_timeout', '682cf8eb'),
        ('finalize_extend_timeout', 'b23b4d80'),
        ('get_pending_reservation_extension', 'd79424b8'),
        ('get_pending_timeout_extension', '6bd06828'),
    ])
    def test_selector_matches_metadata(self, method, expected_selector):
        from allways.contract_client import CONTRACT_SELECTORS
        assert CONTRACT_SELECTORS[method].hex() == expected_selector

    def test_propose_reservation_args_encode(self):
        # Spot-check that encode_args runs end-to-end for the three-field
        # propose call (the most complex new signature).
        client = AllwaysContractClient(contract_address='5xx', subtensor=make_subtensor())
        encoded = client.encode_args('propose_extend_reservation', {
            'miner': bytes(32),
            'from_tx_hash': bytes(32),
            'target_block': 100,
        })
        # 32 (miner) + 32 (hash) + 4 (u32 LE) = 68 bytes
        assert len(encoded) == 68
        assert encoded[-4:] == (100).to_bytes(4, 'little')

    def test_new_error_variants_present(self):
        from allways.contract_client import CONTRACT_ERROR_VARIANTS
        names = {CONTRACT_ERROR_VARIANTS[i][0] for i in range(27, 34)}
        assert names == {
            'ProposalAlreadyPending', 'ChallengeWindowOpen', 'ChallengeWindowClosed',
            'NoProposal', 'ExtensionTooLong', 'TargetNotForward', 'InvalidTarget',
        }
