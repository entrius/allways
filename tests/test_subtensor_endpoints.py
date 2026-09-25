"""build_subtensor: env fallback/archive lists reach bt.Subtensor, and both unset is a plain bt.Subtensor exactly as
before. A rate limit fails over like a dropped socket, and a connection off the primary goes back once it answers.
redact_endpoint keeps a keyed provider's key (path, query, userinfo) out of every log line."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from async_substrate_interface import sync_substrate
from async_substrate_interface.errors import MaxRetriesExceeded, StateDiscardedError, SubstrateRequestException
from async_substrate_interface.substrate_addons import RetrySyncSubstrate
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from allways.utils import subtensor as st
from allways.utils.subtensor import (
    PrimaryWatch,
    build_subtensor,
    describe_subtensor,
    is_rate_limited,
    parse_endpoints,
    redact_endpoint,
)

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


def _http(status):
    return InvalidStatus(Response(status, 'Too Many Requests' if status == 429 else 'Error', Headers()))


def test_a_429_handshake_at_construction_builds_on_the_next_endpoint(fake_subtensor, monkeypatch):
    """The SDK's constructor only moves on for ConnectionError; a throttled first handshake used to kill the boot."""
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, f'{FINNEY},ws://10.0.0.2:9944')
    built = MagicMock(name='built')
    fake_subtensor.side_effect = [_http(429), built]
    config = object()
    assert build_subtensor(config=config) is built
    assert fake_subtensor.call_args_list[1].kwargs == dict(
        config=config,
        network=FINNEY,
        fallback_endpoints=['ws://10.0.0.2:9944', KEYED],
        archive_endpoints=[],
        retry_forever=False,
    )


def test_a_rate_limited_first_request_at_construction_builds_on_the_next_endpoint(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, FINNEY)
    fake_subtensor.side_effect = [SubstrateRequestException('Too Many Requests'), MagicMock()]
    build_subtensor(network=KEYED)
    assert fake_subtensor.call_args.kwargs['network'] == FINNEY


def test_a_non_rate_limit_construction_error_is_raised_as_before(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, FINNEY)
    fake_subtensor.side_effect = [_http(403)]
    with pytest.raises(InvalidStatus):
        build_subtensor(network=KEYED)
    assert fake_subtensor.call_count == 1


def test_every_endpoint_rate_limited_at_construction_raises_the_last(fake_subtensor, monkeypatch):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, FINNEY)
    fake_subtensor.side_effect = [_http(429), _http(429)]
    with pytest.raises(InvalidStatus):
        build_subtensor(network=KEYED)
    assert fake_subtensor.call_count == 2


# ─── is_rate_limited ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    'error',
    [
        _http(429),
        st.RateLimited('x'),
        {'code': -32029, 'message': 'Request limit reached'},
        {'code': -32005, 'message': 'limit exceeded'},
        {'code': 429, 'message': 'x'},
        {'code': -32000, 'message': 'Too Many Requests'},
        {'jsonrpc': '2.0', 'id': 1, 'error': {'code': -32099, 'message': 'rate-limited, slow down'}},
        SubstrateRequestException('Rate limit exceeded'),
        SubstrateRequestException('ratelimit'),
        SubstrateRequestException("{'jsonrpc': '2.0', 'error': {'code': -32029, 'message': 'x'}}"),
        SubstrateRequestException('HTTP 429'),
        ConnectionError('server rejected WebSocket connection: HTTP 429'),
    ],
)
def test_rate_limits_are_recognised(error):
    assert is_rate_limited(error)


@pytest.mark.parametrize(
    'error',
    [
        _http(403),
        _http(503),
        {'code': -32005, 'message': 'Batched requests are not supported by this server'},
        {'code': 1010, 'message': 'Invalid Transaction'},
        {'code': -32000, 'message': 'Client error: Execution failed'},
        SubstrateRequestException('UnknownBlock: State already discarded for 0x429'),
        SubstrateRequestException('Subscription 7 invalid: TxRateLimitExceeded'),
        SubstrateRequestException('ServingRateLimitExceeded'),
        SubstrateRequestException('block 429 not found'),
        ConnectionError('Connection refused'),
    ],
)
def test_other_errors_are_not_rate_limits(error):
    assert not is_rate_limited(error)


# ─── PrimaryWatch (a real RetrySyncSubstrate over scripted sockets) ─────────

FALLBACK = 'ws://10.0.0.2:9944'
ARCHIVE_2 = 'ws://10.0.0.3:9944'


class _Socket:
    """One websocket: answers each JSON-RPC request as its endpoint currently does (a result, or an error object)."""

    def __init__(self, url, endpoints):
        self.url, self.endpoints, self.close_code, self.sent = url, endpoints, None, []

    def send(self, raw):
        self.sent.append(json.loads(raw))

    def recv(self, timeout=None, decode=None):
        request = self.sent[-1]
        answer = self.endpoints.behaviour[self.url](request['method'])
        key = 'error' if isinstance(answer, dict) and 'code' in answer else 'result'
        return json.dumps({'jsonrpc': '2.0', 'id': request['id'], key: answer}).encode()

    def close(self):
        self.close_code = 1000


class _Endpoints:
    """The network: per URL, either a handshake failure or how its sockets answer. Counts every dial."""

    def __init__(self, **behaviour):
        self.behaviour, self.dials, self.sockets = behaviour, [], []

    def set(self, url, how):
        self.behaviour[url] = how

    def dial(self, url, **kwargs):
        self.dials.append(url)
        how = self.behaviour[url]
        if isinstance(how, BaseException):
            raise how
        sock = _Socket(url, self)
        self.sockets.append(sock)
        return sock


def _ok(method):
    return f'{method}@ok'


def _limited(method):
    return {'code': -32029, 'message': 'Too Many Requests'}


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def net(monkeypatch):
    endpoints = _Endpoints()
    monkeypatch.setattr(sync_substrate, 'connect', endpoints.dial)  # the SDK's dial
    monkeypatch.setattr(st, 'ws_connect', endpoints.dial)  # the watch's probe
    return endpoints


@pytest.fixture
def logs(monkeypatch):
    lines = {'warning': [], 'success': [], 'debug': []}
    for level in lines:
        monkeypatch.setattr(st.bt.logging, level, lambda msg, *a, _l=level, **k: lines[_l].append(msg))
    return lines


def _substrate(net, primary=KEYED, fallbacks=(FALLBACK,), archives=(), clock=None):
    net.behaviour.setdefault(primary, _ok)
    substrate = RetrySyncSubstrate(
        primary, fallback_chains=list(fallbacks), archive_nodes=list(archives), _mock=True, max_retries=1
    )
    watch = PrimaryWatch.install(substrate, primary, list(fallbacks), list(archives), clock=clock or _Clock())
    assert watch is not None, 'async_substrate_interface internals moved: PrimaryWatch no longer installs'
    return substrate, watch


def test_a_rate_limit_answer_fails_over_and_the_request_is_served_by_the_fallback(net, logs):
    net.set(KEYED, _limited)
    net.set(FALLBACK, _ok)
    substrate, _ = _substrate(net)
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == FALLBACK
    assert logs['warning'] == [
        'Subtensor left the primary wss://api-bittensor-mainnet.n.dwellir.com/<redacted> for ws://10.0.0.2:9944: '
        'rate limited. Re-trying the primary every 300s'
    ]


def test_a_429_handshake_mid_run_fails_over(net, logs):
    net.set(FALLBACK, _ok)
    substrate, _ = _substrate(net)
    substrate.ws.close_code = 1006  # the primary socket dropped; its reconnect is throttled
    net.set(KEYED, _http(429))
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == FALLBACK and len(logs['warning']) == 1 and 'rate limited' in logs['warning'][0]


def test_a_non_rate_limit_rpc_error_stays_on_the_primary_as_before(net, logs):
    net.set(KEYED, lambda method: {'code': 1002, 'message': 'Verification Error: Runtime error'})
    substrate, _ = _substrate(net)
    with pytest.raises(SubstrateRequestException, match='Verification Error'):
        substrate.rpc_request('author_submitExtrinsic', ['0x00'])
    assert substrate.url == KEYED and net.dials == [KEYED] and logs['warning'] == []


def test_a_non_rate_limit_handshake_refusal_is_raised_as_before(net, logs):
    net.set(FALLBACK, _ok)
    substrate, _ = _substrate(net)
    substrate.ws.close_code = 1006
    net.set(KEYED, _http(403))
    with pytest.raises(InvalidStatus):
        substrate.rpc_request('chain_getHead', [])
    assert substrate.url == KEYED and FALLBACK not in net.dials


def test_rate_limits_are_left_alone_with_no_fallback(net, logs):
    """Archive-only: nowhere to fail over to, so a rate limit keeps surfacing as the SDK's own error."""
    net.set(KEYED, _limited)
    substrate, _ = _substrate(net, fallbacks=(), archives=(ARCHIVE,))
    with pytest.raises(SubstrateRequestException, match='Too Many Requests'):
        substrate.rpc_request('chain_getHead', [])
    assert substrate.url == KEYED


def test_the_connection_returns_to_the_primary_once_it_answers(net, logs):
    net.set(KEYED, _limited)
    net.set(FALLBACK, _ok)
    clock = _Clock()
    substrate, watch = _substrate(net, clock=clock)
    substrate.rpc_request('chain_getHead', [])
    assert substrate.url == FALLBACK

    clock.now += st.PRIMARY_RETRY_SECONDS - 1
    substrate.rpc_request('chain_getHead', [])
    assert substrate.url == FALLBACK and net.dials.count(KEYED) == 1, 'no re-try before the interval'

    net.set(KEYED, _ok)
    clock.now += 1
    fallback_socket = substrate.ws
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == substrate.chain_endpoint == KEYED and watch.retry_at is None
    assert fallback_socket.close_code is not None, 'the fallback socket is closed, not leaked'
    assert net.sockets[-1].url == KEYED and [r['method'] for r in net.sockets[-1].sent] == [
        'system_chain',
        'chain_getHead',
    ], 'the probe socket is adopted: one dial, one probe request, then the caller request'
    assert logs['success'] == ['Subtensor back on the primary wss://api-bittensor-mainnet.n.dwellir.com/<redacted>']
    assert len(logs['warning']) == 1

    clock.now += 10 * st.PRIMARY_RETRY_SECONDS
    substrate.rpc_request('chain_getHead', [])
    assert net.dials.count(KEYED) == 2, 'on the primary: no more probes'


@pytest.mark.parametrize('throttle', [_limited, _http(429)], ids=['rpc-error', 'handshake-429'])
def test_a_primary_still_throttling_is_re_tried_once_per_interval(net, logs, throttle):
    net.set(KEYED, throttle)
    net.set(FALLBACK, _ok)
    clock = _Clock()
    substrate, _ = _substrate(net, clock=clock)
    substrate.rpc_request('chain_getHead', [])
    for _ in range(3):
        clock.now += st.PRIMARY_RETRY_SECONDS
        for _ in range(5):
            substrate.rpc_request('chain_getHead', [])
    assert substrate.url == FALLBACK
    assert net.dials.count(KEYED) == 1 + 3, 'one probe per interval, however many requests'
    assert all(sock.close_code for sock in net.sockets if sock.url == KEYED), 'failed probes close their socket'
    assert len(logs['debug']) == 3
    assert len(logs['warning']) == 1 and logs['success'] == []
    assert all('<redacted>' in line and '0123456789abcdef' not in line for line in logs['debug'])


def test_returning_rewinds_the_fallbacks_so_the_next_failure_fails_over_again(net, logs):
    net.set(KEYED, _limited)
    net.set(FALLBACK, _ok)
    clock = _Clock()
    substrate, _ = _substrate(net, clock=clock)
    substrate.rpc_request('chain_getHead', [])
    net.set(KEYED, _ok)
    clock.now += st.PRIMARY_RETRY_SECONDS
    substrate.rpc_request('chain_getHead', [])
    assert substrate.url == KEYED

    net.set(KEYED, _limited)
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == FALLBACK and len(logs['warning']) == 2


def test_a_discarded_state_switch_warns_and_comes_back_with_the_archive_list_rewound(net, logs):
    net.set(ARCHIVE, _ok)
    clock = _Clock()
    substrate, _ = _substrate(net, archives=(ARCHIVE,), clock=clock)
    substrate._reinstantiate_substrate(StateDiscardedError('0xab'), use_archive=True)
    assert substrate.url == ARCHIVE
    assert logs['warning'][0].endswith(
        f'for {ARCHIVE}: state discarded, reading from an archive. Re-trying the primary every 300s'
    )
    clock.now += st.PRIMARY_RETRY_SECONDS
    substrate.rpc_request('chain_getHead', [])
    assert substrate.url == KEYED
    substrate._reinstantiate_substrate(StateDiscardedError('0xcd'), use_archive=True)
    assert substrate.url == ARCHIVE, 'a spent iter(archive_nodes) would raise StopIteration here'


def test_a_fallback_to_fallback_move_does_not_warn_again(net, logs):
    """The SDK fails over once per call (its re-run is not retried), as for a dropped socket; the next call moves on."""
    net.set(KEYED, _limited)
    net.set(FALLBACK, _limited)
    net.set(ARCHIVE_2, _ok)
    substrate, _ = _substrate(net, fallbacks=(FALLBACK, ARCHIVE_2))
    with pytest.raises(st.RateLimited):
        substrate.rpc_request('chain_getHead', [])
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == ARCHIVE_2 and len(logs['warning']) == 1


def test_every_endpoint_down_still_comes_back_to_the_primary(net, logs):
    """retry_forever=False: with the list spent the SDK raises MaxRetriesExceeded; the watch is what recovers."""
    net.set(KEYED, _limited)
    net.set(FALLBACK, _limited)
    clock = _Clock()
    substrate, _ = _substrate(net, clock=clock)
    with pytest.raises(st.RateLimited):
        substrate.rpc_request('chain_getHead', [])
    with pytest.raises(MaxRetriesExceeded):
        substrate.rpc_request('chain_getHead', [])
    net.set(KEYED, _ok)
    clock.now += st.PRIMARY_RETRY_SECONDS
    assert substrate.rpc_request('chain_getHead', [])['result'] == 'chain_getHead@ok'
    assert substrate.url == KEYED


def test_a_connection_built_off_the_primary_warns_once_and_re_tries_it(fake_subtensor, monkeypatch, net, logs):
    monkeypatch.setenv(st.FALLBACK_ENDPOINTS_ENV, FINNEY)
    net.set(FINNEY, _ok)
    on_fallback = RetrySyncSubstrate(FINNEY, _mock=True)
    fake_subtensor.side_effect = [_http(429), SimpleNamespace(substrate=on_fallback)]
    build_subtensor(network=KEYED)
    assert logs['warning'] == [
        f'Subtensor left the primary wss://api-bittensor-mainnet.n.dwellir.com/<redacted> for {FINNEY}: '
        'rate limited. Re-trying the primary every 300s'
    ]


def test_install_is_a_no_op_without_the_retry_internals():
    plain = SimpleNamespace(_original_methods=None)
    assert PrimaryWatch.install(plain, KEYED, [FINNEY], []) is None


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
