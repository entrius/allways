"""Every ``bt.Subtensor`` the neurons and ``alw`` open comes from ``build_subtensor``, so one keyed endpoint and its
fallbacks cover every TAO read and command.

The primary stays where it always was (``SUBTENSOR_NETWORK`` → ``--subtensor.network`` for the neurons, the ``alw
config`` network for the CLI) and may be a keyed provider URL. Two optional comma-separated lists ride alongside it:

  SUBTENSOR_FALLBACK_ENDPOINTS  tried in order when the current endpoint drops (a connection error, or
                                ``max_retries`` timeouts in a row) or rate-limits us (HTTP 429 at the handshake, or a
                                rate-limit JSON-RPC error), at construction and mid-run alike
  SUBTENSOR_ARCHIVE_ENDPOINTS   switched to when a read hits "State already discarded": the collateral verdict
                                prices an alpha leg at its reservation's block, which a lite node (public finney
                                included) prunes ~300 blocks later

A connection off the primary re-tries it every ``PRIMARY_RETRY_SECONDS`` and moves back once it answers, so a brief
rate limit does not leave a validator on public finney until its next restart. Both unset = a plain ``bt.Subtensor``,
constructed exactly as before.

Keyed providers carry the key in the URL, so every log or display of an endpoint goes through ``redact_endpoint``.
"""

import json
import logging
import os
import re
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlsplit

import bittensor as bt
from async_substrate_interface.errors import MaxRetriesExceeded, StateDiscardedError, SubstrateRequestException
from bittensor.utils import determine_chain_endpoint_and_network
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect as ws_connect

FALLBACK_ENDPOINTS_ENV = 'SUBTENSOR_FALLBACK_ENDPOINTS'
ARCHIVE_ENDPOINTS_ENV = 'SUBTENSOR_ARCHIVE_ENDPOINTS'
REDACTED = '<redacted>'
# Chains bittensor recognises by name or URL. A fallback on the other one would move reads, and signed extrinsics,
# across chains: `alw` on testnet run from a mainnet .env picks up that .env's finney fallback.
_CHAIN_FAMILY = {'finney': 'mainnet', 'archive': 'mainnet', 'test': 'testnet'}
# How long a connection that left the primary waits before re-trying it, and between re-tries while it stays down.
# A provider's limit is per second (Dwellir: 10 req/s, burst 20), so the throttle itself clears in seconds; what the
# wait has to outlast is its cause, a boot's burst of reads or a provider restart. Five minutes covers both, keeps a
# primary that is still throttling or down to one handshake + one request per re-try (12 an hour: no storm, no flapping
# faster than that), and returns long before a leg left unverifiable runs out its max_extend_at (hours).
PRIMARY_RETRY_SECONDS = 300
PROBE_TIMEOUT_SECONDS = 10
# A throttle, not a bad request: HTTP 429 or its wording, and -32029, a provider rate-limit code. -32005 is one too
# (Infura/Chainstack-style gateways), but a Substrate node's jsonrpsee answers it for "Batched requests are not
# supported by this server", so it only counts without that wording. "rate limit" must start a word with a lowercase
# "limit": subtensor's own errors (TxRateLimitExceeded, ServingRateLimitExceeded, …) refuse a call, they don't throttle.
_RATE_LIMIT_TEXT = re.compile(r'(?i:(?:http|status|code)\W{0,3}429\b|too many requests)|\b[Rr]ate[- _]?limit|-32029\b')
_RATE_LIMIT_CODES = {429, -32029}


def parse_endpoints(raw: Optional[str]) -> List[str]:
    """A comma-separated env list → its non-empty entries, stripped, in order."""
    return [entry.strip() for entry in (raw or '').split(',') if entry.strip()]


def redact_endpoint(url: Optional[str]) -> str:
    """``url`` safe for logs and output: scheme + host kept, any userinfo / path / query replaced, since that is where
    providers put the key. A keyless endpoint (``wss://entrypoint-finney.opentensor.ai:443``) or a network name
    (``finney``) comes back unchanged."""
    if not url:
        return ''
    parts = urlsplit(url)
    host = parts.netloc.rpartition('@')[2]
    if not parts.scheme or not host:
        return url
    if '@' in parts.netloc or parts.path.strip('/') or parts.query or parts.fragment:
        return f'{parts.scheme}://{host}/{REDACTED}'
    return url


def describe_subtensor(subtensor) -> str:
    """``str(subtensor)`` with the endpoint redacted (the SDK's own ``__str__`` prints the full URL)."""
    return f'Network: {subtensor.network}, Chain: {redact_endpoint(subtensor.chain_endpoint)}'


class _EndpointRedactor(logging.Filter):
    """Scrubs keyed endpoints out of the lines the SDK formats itself: bittensor's "Connecting to network" debug line
    and async_substrate_interface's "Connected to …" / "Trying again with …" failover lines."""

    def __init__(self):
        super().__init__()
        self.secrets: Dict[str, str] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if self.secrets:
            msg = record.getMessage()
            if any(url in msg for url in self.secrets):
                for url, safe in self.secrets.items():
                    msg = msg.replace(url, safe)
                record.msg, record.args = msg, None
        return True


_redactor = _EndpointRedactor()


def _guard_logs(endpoints: List[str]) -> None:
    for url in endpoints:
        if url and redact_endpoint(url) != url:
            _redactor.secrets[url] = redact_endpoint(url)
    if _redactor.secrets:
        for name in ('bittensor', 'async_substrate_interface'):
            logging.getLogger(name).addFilter(_redactor)  # no-op once attached


def _same_chain(network: str, endpoints: List[str]) -> List[str]:
    """``endpoints`` minus any recognisably on the other chain from ``network`` (unrecognised URLs are kept)."""
    family = _CHAIN_FAMILY.get(network)
    kept = []
    for url in endpoints:
        other = _CHAIN_FAMILY.get(determine_chain_endpoint_and_network(url)[0])
        if family and other and other != family:
            bt.logging.warning(
                f'Ignoring subtensor endpoint {redact_endpoint(url)}: it is {other}, the primary is {family}'
            )
        else:
            kept.append(url)
    return kept


def _raise_discarded_state(substrate) -> None:
    """Make a pruned-state read reach the SDK's archive switch.

    async_substrate_interface 2.2.1 raises StateDiscardedError (the error RetrySyncSubstrate moves to an archive on)
    only for the old "Api called for an unknown Block: State already discarded" text. Current nodes answer
    "UnknownBlock: State already discarded for 0x…", which surfaces as a plain SubstrateRequestException, so without
    this the archive list is never used. Wraps the raw rpc_request the retry wrapper calls, re-raising that answer as
    StateDiscardedError; a substrate without the wrapper is left alone."""
    originals = getattr(substrate, '_original_methods', None)
    if not isinstance(originals, dict) or 'rpc_request' not in originals:
        return
    rpc_request = originals['rpc_request']

    def translated(*args, **kwargs):
        try:
            return rpc_request(*args, **kwargs)
        except StateDiscardedError:
            raise
        except SubstrateRequestException as e:
            _, sep, block_hash = str(e).partition('State already discarded for ')
            if sep:
                raise StateDiscardedError(block_hash.strip()) from e
            raise

    originals['rpc_request'] = translated


class RateLimited(ConnectionError):
    """The current endpoint is throttling us. A ConnectionError, so RetrySyncSubstrate fails over on it exactly as on a
    dropped socket."""


def is_rate_limited(error: Any) -> bool:
    """True when ``error`` (an exception, or a JSON-RPC error object / response) is an endpoint throttling us."""
    if isinstance(error, RateLimited):
        return True
    if isinstance(error, InvalidStatus):
        return error.response.status_code == 429
    if isinstance(error, dict):
        error = error.get('error', error)
        if not isinstance(error, dict):
            return is_rate_limited(str(error))
        code, message = error.get('code'), str(error.get('message', ''))
        if code in _RATE_LIMIT_CODES or (code == -32005 and 'batch' not in message.lower()):
            return True
        return bool(_RATE_LIMIT_TEXT.search(message))
    text = str(error)
    return bool(_RATE_LIMIT_TEXT.search(text) or (re.search(r'-32005\b', text) and 'batch' not in text.lower()))


def _rpc_errors(results: Any):
    """Every JSON-RPC error object in a ``_make_rpc_request`` result ({id: deque([response, …])}) or a batch's
    [response, …]."""
    for responses in results.values() if isinstance(results, dict) else [results]:
        for response in responses if isinstance(responses, (list, deque)) else []:
            if isinstance(response, dict) and 'error' in response:
                yield response['error']


def _switch_reason(error: Optional[BaseException], use_archive: bool) -> str:
    if use_archive or isinstance(error, StateDiscardedError):
        return 'state discarded, reading from an archive'
    if isinstance(error, RateLimited):
        return 'rate limited'
    if isinstance(error, MaxRetriesExceeded):
        return 'no answer after max_retries timeouts'
    return f'connection error ({type(error).__name__})'


def _probe(url: str, max_size: Optional[int]):
    """An open websocket to ``url`` that has just answered one cheap request, else raises."""
    ws = ws_connect(url, open_timeout=PROBE_TIMEOUT_SECONDS, max_size=max_size)
    try:
        ws.send(json.dumps({'jsonrpc': '2.0', 'id': 'allways-probe', 'method': 'system_chain', 'params': []}))
        answer = json.loads(ws.recv(timeout=PROBE_TIMEOUT_SECONDS))
        if not isinstance(answer, dict) or 'result' not in answer:
            raise RateLimited(str(answer)) if is_rate_limited(answer) else SubstrateRequestException(str(answer))
        return ws
    except BaseException:
        ws.close()
        raise


class PrimaryWatch:
    """Keeps one RetrySyncSubstrate honest about its primary: a rate limit fails over like a dropped socket, leaving the
    primary logs one WARNING, and while off it the next request after ``PRIMARY_RETRY_SECONDS`` first re-tries the
    primary on a fresh socket, adopting that socket (and one INFO line) if it answers. Nothing runs in the background:
    the re-try rides the request that finds it due, so an idle connection costs nothing.

    Hooks four SDK internals of async_substrate_interface 2.2.1, per instance: ``_original_methods['connect']`` (the
    raw dial ``_retry`` wraps: a 429 handshake becomes RateLimited, and the due re-try happens here, where every request
    asks for its socket), ``_make_rpc_request`` / ``_make_batch_rpc_request`` (every request's raw answers, codes
    intact: a rate-limit error becomes RateLimited) and ``_reinstantiate_substrate`` (the SDK's switch, to see the
    connection leave the primary). Going back rewinds ``fallback_chains`` / ``archive_nodes`` so the next failure walks
    the lists again from the top. ``install`` is a no-op on a substrate missing any of them."""

    def __init__(
        self,
        substrate,
        primary: str,
        fallbacks: List[str],
        archives: List[str],
        clock: Callable[[], float] = time.monotonic,
    ):
        self.substrate = substrate
        self.primary = primary
        self.fallbacks = fallbacks
        self.archives = archives
        self.clock = clock
        self.retry_at: Optional[float] = None  # None = on the primary

    @classmethod
    def install(cls, substrate, primary: str, fallbacks: List[str], archives: List[str], **kwargs):
        originals = getattr(substrate, '_original_methods', None)
        hooks = ('_reinstantiate_substrate', '_make_rpc_request', '_make_batch_rpc_request')
        if (
            not isinstance(originals, dict)
            or 'connect' not in originals
            or not all(callable(getattr(substrate, name, None)) for name in hooks)
        ):
            bt.logging.debug('Subtensor substrate lacks the retry internals; rate-limit failover and return are off')
            return None
        watch = cls(substrate, primary, fallbacks, archives, **kwargs)
        originals['connect'] = watch._connect(originals['connect'])
        substrate._reinstantiate_substrate = watch._reinstantiate(substrate._reinstantiate_substrate)
        if fallbacks:  # with nowhere to go, a translated rate limit would only turn into MaxRetriesExceeded
            substrate._make_rpc_request = watch._translate(substrate._make_rpc_request)
            substrate._make_batch_rpc_request = watch._translate(substrate._make_batch_rpc_request)
        return watch

    def left_primary(self, reason: str) -> None:
        """The connection is off the primary: WARN on the move away (not on fallback → fallback), time the re-try."""
        if self.retry_at is None:
            bt.logging.warning(
                f'Subtensor left the primary {redact_endpoint(self.primary)} for '
                f'{redact_endpoint(self.substrate.url)}: {reason}. Re-trying the primary every {PRIMARY_RETRY_SECONDS}s'
            )
            self.retry_at = self.clock() + PRIMARY_RETRY_SECONDS

    def maybe_return(self) -> None:
        if self.retry_at is None or self.clock() < self.retry_at:
            return
        self.retry_at = self.clock() + PRIMARY_RETRY_SECONDS  # whatever the outcome: at most one re-try per interval
        try:
            ws = _probe(self.primary, getattr(self.substrate, 'ws_max_size', None))
        except Exception as e:
            bt.logging.debug(
                f'Subtensor primary {redact_endpoint(self.primary)} still unavailable: {redact_text(str(e))}'
            )
            return
        old, sub = self.substrate.ws, self.substrate
        sub.ws, sub.url, sub.chain_endpoint = ws, self.primary, self.primary
        sub.fallback_chains, sub.archive_nodes = iter(self.fallbacks), iter(self.archives)
        self.retry_at = None
        try:
            old.close()
        except Exception:
            pass
        bt.logging.success(f'Subtensor back on the primary {redact_endpoint(self.primary)}')

    def _connect(self, connect):
        def watched(*args, **kwargs):
            if not kwargs.get('init', args[0] if args else False):  # a request asking for its socket
                self.maybe_return()
            try:
                return connect(*args, **kwargs)
            except InvalidStatus as e:
                if self.fallbacks and is_rate_limited(e):
                    raise RateLimited(f'{redact_endpoint(self.substrate.url)} rate-limited the handshake: {e}') from e
                raise

        return watched

    def _translate(self, make_request):
        def translated(*args, **kwargs):
            try:
                results = make_request(*args, **kwargs)
            except SubstrateRequestException as e:
                if is_rate_limited(e):
                    raise RateLimited(f'{redact_endpoint(self.substrate.url)} rate-limited us: {e}') from e
                raise
            for error in _rpc_errors(results):
                if is_rate_limited(error):
                    raise RateLimited(f'{redact_endpoint(self.substrate.url)} rate-limited us: {error}')
            return results

        return translated

    def _reinstantiate(self, reinstantiate):
        def watched(e=None, use_archive=False):
            try:
                return reinstantiate(e, use_archive=use_archive)
            finally:
                # Also when the switch itself failed: the socket is then on a dead fallback, and the re-try is what
                # gets it back (retry_forever=False never cycles to the primary on its own).
                if self.substrate.url != self.primary:
                    self.left_primary(_switch_reason(e, use_archive))

        return watched


def redact_text(text: str) -> str:
    """``text`` with every keyed endpoint seen so far replaced by its redacted form (for exception messages)."""
    for url, safe in _redactor.secrets.items():
        text = text.replace(url, safe)
    return text


def build_subtensor(network: Optional[str] = None, config: Optional['bt.Config'] = None) -> bt.Subtensor:
    """A Subtensor on ``network`` (the CLI) or ``config`` (the neurons), resolved exactly as ``bt.Subtensor`` resolves
    them, plus the env fallbacks.

    ``retry_forever`` stays False. True would cycle back to the primary on its own, but the SDK re-dials inside its
    retry handler (the reconnect is itself retried), so on a total outage one call recurses through the endpoint
    cycle until RecursionError, blocking its thread for as long as every dial takes to fail. With False one call
    walks primary → fallbacks once and then raises MaxRetriesExceeded; ``PrimaryWatch`` brings the connection back to
    the primary once it answers again, and the caller's existing error path still rebuilds through here.

    The SDK's constructor only moves on from an endpoint on ConnectionError, so a primary that rate-limits the first
    handshake or request is skipped here instead: the Subtensor is built on the next endpoint (the skipped ones become
    its last fallbacks), and the watch starts off the primary, re-trying it on the usual interval.
    """
    kwargs = {k: v for k, v in (('network', network), ('config', config)) if v is not None}
    primary, primary_network = bt.Subtensor.setup_config(network, config)
    fallbacks = parse_endpoints(os.environ.get(FALLBACK_ENDPOINTS_ENV))
    archives = parse_endpoints(os.environ.get(ARCHIVE_ENDPOINTS_ENV))
    _guard_logs([primary, *fallbacks, *archives])
    fallbacks, archives = _same_chain(primary_network, fallbacks), _same_chain(primary_network, archives)
    if not fallbacks and not archives:
        return bt.Subtensor(**kwargs)
    bt.logging.info(
        f'Subtensor {redact_endpoint(primary)} with fallbacks [{", ".join(map(redact_endpoint, fallbacks))}], '
        f'archive [{", ".join(map(redact_endpoint, archives))}]'
    )
    endpoints, reason = [primary, *fallbacks], 'connection error'
    for i in range(len(endpoints)):
        try:
            subtensor = bt.Subtensor(
                **(dict(kwargs, network=endpoints[i]) if i else kwargs),
                # The skipped endpoints go last rather than away: an empty list would get a plain SubstrateInterface.
                fallback_endpoints=endpoints[i + 1 :] + endpoints[:i],
                archive_endpoints=archives,
                retry_forever=False,
            )
            break
        except ConnectionError as e:
            # RetrySyncSubstrate names every endpoint it tried in this message.
            raise ConnectionError(redact_text(str(e))) from None
        except Exception as e:
            if i + 1 == len(endpoints) or not is_rate_limited(e):
                raise
            bt.logging.debug(
                f'Subtensor {redact_endpoint(endpoints[i])} rate-limited the connect: {redact_text(str(e))}'
            )
            reason = 'rate limited'
    if archives:
        _raise_discarded_state(subtensor.substrate)
    watch = PrimaryWatch.install(subtensor.substrate, primary, fallbacks, archives)
    if watch and watch.substrate.url != primary:
        watch.left_primary(reason)
    return subtensor
