"""Subtensor connections: the primary endpoint plus optional fallbacks, with provider keys kept out of the logs."""

import logging
import os
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import bittensor as bt

FALLBACK_ENDPOINTS_ENV = 'SUBTENSOR_FALLBACK_ENDPOINTS'
REDACTED = '<redacted>'


def parse_endpoints(raw: Optional[str]) -> List[str]:
    """A comma-separated env list as its non-empty entries, stripped, in order."""
    return [entry.strip() for entry in (raw or '').split(',') if entry.strip()]


def redact_endpoint(url: Optional[str]) -> str:
    """``url`` safe for logs: scheme and host kept; userinfo, path and query (where providers put the key) replaced."""
    if not url:
        return ''
    parts = urlsplit(url)
    host = parts.netloc.rpartition('@')[2]
    if not parts.scheme or not host:
        return url
    if '@' in parts.netloc or parts.path.strip('/') or parts.query or parts.fragment:
        return f'{parts.scheme}://{host}/{REDACTED}'
    return url


class _EndpointRedactor(logging.Filter):
    """Scrubs keyed endpoints out of the lines the SDK formats itself."""

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


def redact_text(text: str) -> str:
    """``text`` with every known keyed endpoint replaced by its redacted form."""
    for url, safe in _redactor.secrets.items():
        text = text.replace(url, safe)
    return text


def guard_logs(endpoints: List[str]) -> None:
    for url in endpoints:
        if url and redact_endpoint(url) != url:
            _redactor.secrets[url] = redact_endpoint(url)
    if _redactor.secrets:
        for name in ('bittensor', 'async_substrate_interface'):
            logging.getLogger(name).addFilter(_redactor)  # no-op once attached


def fallback_endpoints(primary: str) -> List[str]:
    """SUBTENSOR_FALLBACK_ENDPOINTS minus the primary, deduplicated, in order."""
    fallbacks: List[str] = []
    for url in parse_endpoints(os.environ.get(FALLBACK_ENDPOINTS_ENV)):
        if url != primary and url not in fallbacks:
            fallbacks.append(url)
    return fallbacks


def genesis_hash(subtensor: 'bt.Subtensor') -> str:
    return str(subtensor.substrate.get_block_hash(0))


class Connections:
    """Opens subtensor connections: the primary first, a fallback only for a pass the primary could not serve.

    A fallback is used only once the primary's genesis hash is known and the fallback's matches it, so a
    fallback on another chain (a mainnet URL in a testnet .env) is never read from or signed against.
    """

    def __init__(self, primary: str, fallbacks: Optional[List[str]] = None, connector=None):
        self.primary = primary
        self.fallbacks = fallback_endpoints(primary) if fallbacks is None else fallbacks
        self.connector = connector or bt.Subtensor
        self.genesis: Optional[str] = None
        self.skip_primary_once = False
        guard_logs([primary, *self.fallbacks])

    def primary_failed(self) -> None:
        """A pass on the primary failed after connecting: let the next pass try a fallback."""
        self.skip_primary_once = True

    def open(self) -> Tuple['bt.Subtensor', bool]:
        """Return ``(subtensor, is_primary)``, raising the primary's error when nothing usable answers."""
        skip, self.skip_primary_once = self.skip_primary_once, False
        error: Optional[Exception] = None
        if not skip:
            subtensor = None
            try:
                subtensor = self.connector(network=self.primary)
                self.genesis = genesis_hash(subtensor)
                bt.logging.info(f'Subtensor: {subtensor.network} ({redact_endpoint(subtensor.chain_endpoint)})')
                return subtensor, True
            except Exception as e:
                error = e
                close(subtensor)
                bt.logging.warning(f'Subtensor primary {redact_endpoint(self.primary)} failed: {type(e).__name__}')
        if self.genesis is None and self.fallbacks:
            bt.logging.warning('Fallback endpoints not used: the primary has not answered yet, so its chain is unknown')
        elif self.genesis is not None:
            for url in self.fallbacks:
                subtensor = None
                try:
                    subtensor = self.connector(network=url)
                    if genesis_hash(subtensor) == self.genesis:
                        bt.logging.warning(f'Subtensor: on fallback {redact_endpoint(url)} for this pass')
                        return subtensor, False
                    bt.logging.error(f'Ignoring fallback {redact_endpoint(url)}: it is a different chain')
                except Exception as e:
                    bt.logging.warning(f'Subtensor fallback {redact_endpoint(url)} failed: {type(e).__name__}')
                close(subtensor)
        if error is None:  # the primary was skipped and no fallback served: go back to it
            return self.open()
        raise error


def close(subtensor) -> None:
    if subtensor is not None:
        try:
            subtensor.close()
        except Exception:
            pass
