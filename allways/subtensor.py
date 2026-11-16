"""Subtensor connections: the primary endpoint plus optional fallbacks, with provider keys kept out of the logs."""

import logging
import os
from typing import Dict, List, Optional
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


def guard_logs(endpoints: List[str]) -> None:
    for url in endpoints:
        if url and redact_endpoint(url) != url:
            _redactor.secrets[url] = redact_endpoint(url)
    if _redactor.secrets:
        for name in ('bittensor', 'async_substrate_interface'):
            logging.getLogger(name).addFilter(_redactor)  # no-op once attached


def endpoint_list(primary: str) -> List[str]:
    """The primary followed by any SUBTENSOR_FALLBACK_ENDPOINTS, deduplicated, in order."""
    endpoints = [primary]
    for url in parse_endpoints(os.environ.get(FALLBACK_ENDPOINTS_ENV)):
        if url not in endpoints:
            endpoints.append(url)
    guard_logs(endpoints)
    return endpoints


def connect(endpoint: str) -> 'bt.Subtensor':
    subtensor = bt.Subtensor(network=endpoint)
    bt.logging.info(f'Subtensor: {subtensor.network} ({redact_endpoint(subtensor.chain_endpoint)})')
    return subtensor
