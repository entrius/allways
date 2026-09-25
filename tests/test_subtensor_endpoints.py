"""build_subtensor: env fallback/archive lists reach bt.Subtensor, and both unset is a plain bt.Subtensor exactly as
before. redact_endpoint keeps a keyed provider's key (path, query, userinfo) out of every log line."""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from async_substrate_interface.errors import StateDiscardedError, SubstrateRequestException

from allways.utils import subtensor as st
from allways.utils.subtensor import build_subtensor, describe_subtensor, parse_endpoints, redact_endpoint

KEYED = 'wss://api-bittensor-mainnet.n.dwellir.com/0123456789abcdef'
FINNEY = 'wss://entrypoint-finney.opentensor.ai:443'
ARCHIVE = 'wss://archive.chain.opentensor.ai:443'
TESTNET = 'wss://test.finney.opentensor.ai:443'


@pytest.fixture
def fake_subtensor(monkeypatch):
    """bt.Subtensor mocked (no network); setup_config resolves ``config`` to the keyed primary."""
    import bittensor as bt

    fake = MagicMock(name='Subtensor')
    fake.setup_config.side_effect = lambda network, config: (network or KEYED, 'unknown')
    monkeypatch.setattr(bt, 'Subtensor', fake)
    monkeypatch.delenv(st.FALLBACK_ENDPOINTS_ENV, raising=False)
    monkeypatch.delenv(st.ARCHIVE_ENDPOINTS_ENV, raising=False)
    monkeypatch.setattr(st._redactor, 'secrets', {})
    return fake


# ─── build_subtensor ────────────────────────────────────────────────────────


def test_unset_env_builds_a_plain_subtensor(fake_subtensor):
    config = object()
    assert build_subtensor(config=config) is fake_subtensor.return_value
    fake_subtensor.assert_called_once_with(config=config)


def test_unset_env_cli_network_is_passed_as_is(fake_subtensor):
    build_subtensor(network='finney')
    fake_subtensor.assert_called_once_with(network='finney')


def test_empty_env_values_count_as_unset(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, ' , ,')
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, '')
    config = object()
    build_subtensor(config=config)
    fake_subtensor.assert_called_once_with(config=config)


def test_env_lists_pass_through_in_order(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, f' {FINNEY} , ws://10.0.0.2:9944 ')
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, ARCHIVE)
    config = object()
    build_subtensor(config=config)
    fake_subtensor.assert_called_once_with(
        config=config,
        fallback_endpoints=[FINNEY, 'ws://10.0.0.2:9944'],
        archive_endpoints=[ARCHIVE],
        retry_forever=False,
    )


def test_archive_alone_turns_the_retry_wrapper_on(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, ARCHIVE)
    build_subtensor(network=KEYED)
    fake_subtensor.assert_called_once_with(
        network=KEYED, fallback_endpoints=[], archive_endpoints=[ARCHIVE], retry_forever=False
    )


def test_fallback_on_the_other_chain_is_dropped(fake_subtensor, monkeypatch):
    """`alw` on testnet run from a mainnet .env must not fail over onto finney."""
    fake_subtensor.setup_config.side_effect = lambda network, config: (TESTNET, 'test')
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, f'{FINNEY},ws://10.0.0.2:9944')
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, ARCHIVE)
    build_subtensor(network='test')
    fake_subtensor.assert_called_once_with(
        network='test', fallback_endpoints=['ws://10.0.0.2:9944'], archive_endpoints=[], retry_forever=False
    )


def test_connection_error_is_raised_redacted(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, FINNEY)
    fake_subtensor.side_effect = ConnectionError(f'Unable to connect at any chains specified: {[KEYED, FINNEY]}')
    with pytest.raises(ConnectionError) as exc:
        build_subtensor(network=KEYED)
    assert '0123456789abcdef' not in str(exc.value)
    assert 'dwellir.com/<redacted>' in str(exc.value) and FINNEY in str(exc.value)


def test_sdk_log_lines_are_scrubbed(fake_subtensor, monkeypatch, caplog):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, KEYED)
    build_subtensor(network=FINNEY)
    sdk = logging.getLogger('async_substrate_interface')
    with caplog.at_level(logging.INFO, logger='async_substrate_interface'):
        sdk.error('Connection error. Trying again with %s', KEYED)
    assert caplog.messages == [
        'Connection error. Trying again with wss://api-bittensor-mainnet.n.dwellir.com/<redacted>'
    ]


def _raising(exc):
    def rpc_request(*args, **kwargs):
        raise exc

    return rpc_request


def test_current_node_discarded_state_answer_reaches_the_archive_switch(fake_subtensor, monkeypatch):
    """asi 2.2.1 only recognises the old error text; today's "UnknownBlock: …" must still raise StateDiscardedError."""
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, ARCHIVE)
    block_hash = '0x' + 'ab' * 32
    originals = {
        'rpc_request': _raising(
            SubstrateRequestException(f'Client error: UnknownBlock: State already discarded for {block_hash}')
        )
    }
    fake_subtensor.return_value.substrate = SimpleNamespace(_original_methods=originals)
    build_subtensor(network=FINNEY)
    with pytest.raises(StateDiscardedError) as exc:
        originals['rpc_request']('state_call', [])
    assert exc.value.block_hash == block_hash


def test_other_rpc_errors_pass_through_untouched(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.ARCHIVE_ENDPOINTS_ENV, ARCHIVE)
    error = SubstrateRequestException('Client error: Execution failed')
    originals = {'rpc_request': _raising(error)}
    fake_subtensor.return_value.substrate = SimpleNamespace(_original_methods=originals)
    build_subtensor(network=FINNEY)
    with pytest.raises(SubstrateRequestException) as exc:
        originals['rpc_request']('state_call', [])
    assert exc.value is error


def test_no_archive_leaves_rpc_errors_alone(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, KEYED)
    rpc_request = _raising(SubstrateRequestException('UnknownBlock: State already discarded for 0x00'))
    originals = {'rpc_request': rpc_request}
    fake_subtensor.return_value.substrate = SimpleNamespace(_original_methods=originals)
    build_subtensor(network=FINNEY)
    assert originals['rpc_request'] is rpc_request


# ─── parse_endpoints ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    'raw, expected',
    [
        (None, []),
        ('', []),
        ('  ', []),
        (FINNEY, [FINNEY]),
        (f' {FINNEY} ,, {ARCHIVE} ,', [FINNEY, ARCHIVE]),
    ],
)
def test_parse_endpoints(raw, expected):
    assert parse_endpoints(raw) == expected


# ─── redact_endpoint ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    'url, expected',
    [
        (KEYED, 'wss://api-bittensor-mainnet.n.dwellir.com/<redacted>'),
        ('wss://rpc.example.com:443/ws?apikey=secret', 'wss://rpc.example.com:443/<redacted>'),
        ('wss://user:secret@rpc.example.com', 'wss://rpc.example.com/<redacted>'),
        (FINNEY, FINNEY),
        ('ws://127.0.0.1:9944', 'ws://127.0.0.1:9944'),
        ('wss://entrypoint-finney.opentensor.ai/', 'wss://entrypoint-finney.opentensor.ai/'),
        ('finney', 'finney'),
        (None, ''),
        ('', ''),
    ],
)
def test_redact_endpoint(url, expected):
    assert redact_endpoint(url) == expected


def test_describe_subtensor_redacts_the_chain():
    sub = SimpleNamespace(network='unknown', chain_endpoint=KEYED)
    assert describe_subtensor(sub) == 'Network: unknown, Chain: wss://api-bittensor-mainnet.n.dwellir.com/<redacted>'
