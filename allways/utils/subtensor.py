"""Every ``bt.Subtensor`` the neurons and ``alw`` open comes from ``build_subtensor``, so one keyed endpoint and its
fallbacks cover every TAO read and command.

The primary stays where it always was (``SUBTENSOR_NETWORK`` → ``--subtensor.network`` for the neurons, the ``alw
config`` network for the CLI) and may be a keyed provider URL. Two optional comma-separated lists ride alongside it:

  SUBTENSOR_FALLBACK_ENDPOINTS  tried in order when the current endpoint drops (a connection error, or
                                ``max_retries`` timeouts in a row)
  SUBTENSOR_ARCHIVE_ENDPOINTS   switched to when a read hits "State already discarded": the collateral verdict
                                prices an alpha leg at its reservation's block, which a lite node (public finney
                                included) prunes ~300 blocks later

Either switch is one-way for that connection (after an archive switch, head reads go to the archive too) until the
neuron rebuilds it, which starts again at the primary. Both unset = a plain ``bt.Subtensor``, constructed exactly as before.

Keyed providers carry the key in the URL, so every log or display of an endpoint goes through ``redact_endpoint``.
"""

import logging
import os
from typing import Dict, List, Optional
from urllib.parse import urlsplit

import bittensor as bt
from async_substrate_interface.errors import StateDiscardedError, SubstrateRequestException
from bittensor.utils import determine_chain_endpoint_and_network

FALLBACK_ENDPOINTS_ENV = 'SUBTENSOR_FALLBACK_ENDPOINTS'
ARCHIVE_ENDPOINTS_ENV = 'SUBTENSOR_ARCHIVE_ENDPOINTS'
REDACTED = '<redacted>'
# Chains bittensor recognises by name or URL. A fallback on the other one would move reads, and signed extrinsics,
# across chains: `alw` on testnet run from a mainnet .env picks up that .env's finney fallback.
_CHAIN_FAMILY = {'finney': 'mainnet', 'archive': 'mainnet', 'test': 'testnet'}


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
    walks primary → fallbacks once and then raises MaxRetriesExceeded; the caller's existing error path rebuilds
    through here, which starts again at the primary. That rebuild is how a daemon gets back to its keyed endpoint.
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
    try:
        subtensor = bt.Subtensor(
            **kwargs, fallback_endpoints=fallbacks, archive_endpoints=archives, retry_forever=False
        )
    except ConnectionError as e:
        # RetrySyncSubstrate names every endpoint it tried in this message.
        raise ConnectionError(redact_text(str(e))) from None
    if archives:
        _raise_discarded_state(subtensor.substrate)
    return subtensor
