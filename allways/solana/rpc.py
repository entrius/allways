"""Thin sync JSON-RPC client over `requests` for Solana.

Only the methods the validator/miner/CLI need. Kept dependency-light (no solana-py) and sync so it drops
into the existing `asyncio.to_thread` pattern. Account data is returned base64-decoded as raw bytes.
"""

import base64
import os
import time
from typing import Any, List, Optional, Tuple

import base58
import requests


def resolve_rpc_url(explicit: Optional[str] = None) -> str:
    """The SOL RPC endpoint every consumer (neurons, provider, CLI) resolves through.

    ``explicit`` (or ``SOLANA_RPC_URL``) is the endpoint; ``SOLANA_RPC_API_KEY`` is composed on as
    the ``api-key`` query param (the keyed-provider convention, e.g. Helius), so operators set a
    plain URL and a key instead of splicing the key into the URL. A URL already carrying an
    ``api-key`` is left alone. Defaults to localnet."""
    url = explicit or os.environ.get('SOLANA_RPC_URL') or 'http://127.0.0.1:8899'
    key = os.environ.get('SOLANA_RPC_API_KEY')
    if key and 'api-key=' not in url:
        url = f'{url}{"&" if "?" in url else "?"}api-key={key}'
    return url


class SolanaRpcError(Exception):
    pass


class TransientRpcError(SolanaRpcError):
    """A transient transport/server fault — request timeout, HTTP 429/5xx, or a JSON-RPC transient code
    (-32603 internal error, -32005 node-unhealthy/behind, block-not-yet-available, …). Idempotent reads
    are retried automatically inside `_call`; a state-changing re-send is left to the caller, since the
    request may already have landed. Subclasses SolanaRpcError so existing `except SolanaRpcError`
    handlers keep working."""


# JSON-RPC error codes that mean "try again", not "your request was malformed / the tx failed".
_TRANSIENT_RPC_CODES = frozenset({-32603, -32005, -32004, -32014, -32016})
# HTTP statuses that are the provider hiccuping, not a client-side error.
_TRANSIENT_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})
# Side-effect-free methods (+ read-only simulate) — safe to auto-retry. sendTransaction and
# requestAirdrop are deliberately excluded: a duplicate submit is the caller's decision (the pool crank
# already re-sends on its next pass, and a double resolve_pool reverts benignly).
_RETRYABLE_METHODS = frozenset(
    {
        'getAccountInfo',
        'getProgramAccounts',
        'getSlot',
        'getBalance',
        'getLatestBlockhash',
        'getSignaturesForAddress',
        'getTransaction',
        'getSignatureStatuses',
        'simulateTransaction',
    }
)
_MAX_READ_RETRIES = 4
_RETRY_BACKOFF_BASE = 0.25  # seconds; doubles each attempt → 0.25, 0.5, 1.0, 2.0


class SolanaRpc:
    def __init__(self, url: str, timeout: int = 30):
        self.url = url
        self.timeout = timeout
        self._id = 0
        self._session = requests.Session()

    def _call(self, method: str, params: list) -> Any:
        self._id += 1
        payload = {'jsonrpc': '2.0', 'id': self._id, 'method': method, 'params': params}
        attempt = 0
        while True:
            try:
                resp = self._session.post(self.url, json=payload, timeout=self.timeout)
                if resp.status_code in _TRANSIENT_HTTP_STATUS:
                    raise TransientRpcError(f'{method}: HTTP {resp.status_code}')
                resp.raise_for_status()
                body = resp.json()
                if 'error' in body:
                    err = body['error']
                    code = err.get('code') if isinstance(err, dict) else None
                    if code in _TRANSIENT_RPC_CODES:
                        raise TransientRpcError(f'{method}: {err}')
                    raise SolanaRpcError(f'{method}: {err}')
                return body['result']
            except (requests.Timeout, requests.ConnectionError) as e:
                transient: TransientRpcError = TransientRpcError(f'{method}: {type(e).__name__}: {e}')
            except TransientRpcError as e:
                transient = e
            # Retry side-effect-free reads with exponential backoff; surface everything else at once.
            attempt += 1
            if method not in _RETRYABLE_METHODS or attempt > _MAX_READ_RETRIES:
                raise transient
            time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))

    # --- reads ---
    def get_account_info(self, pubkey, commitment: str = 'confirmed') -> Optional[bytes]:
        res = self._call('getAccountInfo', [str(pubkey), {'encoding': 'base64', 'commitment': commitment}])
        val = res.get('value')
        if val is None:
            return None
        return base64.b64decode(val['data'][0])

    def get_account_lamports(self, pubkey, commitment: str = 'confirmed') -> Optional[int]:
        res = self._call('getAccountInfo', [str(pubkey), {'encoding': 'base64', 'commitment': commitment}])
        val = res.get('value')
        return None if val is None else int(val['lamports'])

    def get_program_accounts(
        self,
        program_id,
        disc8: Optional[bytes] = None,
        extra_filters: Optional[list] = None,
        commitment: str = 'confirmed',
    ) -> List[Tuple[str, bytes]]:
        filters = []
        if disc8 is not None:  # memcmp on the 8-byte account discriminator at offset 0
            filters.append({'memcmp': {'offset': 0, 'bytes': base58.b58encode(disc8).decode()}})
        if extra_filters:
            filters.extend(extra_filters)
        cfg: dict = {'encoding': 'base64', 'commitment': commitment}
        if filters:
            cfg['filters'] = filters
        res = self._call('getProgramAccounts', [str(program_id), cfg])
        return [(item['pubkey'], base64.b64decode(item['account']['data'][0])) for item in res]

    def get_slot(self, commitment: str = 'confirmed') -> int:
        return int(self._call('getSlot', [{'commitment': commitment}]))

    def get_balance(self, pubkey, commitment: str = 'confirmed') -> int:
        res = self._call('getBalance', [str(pubkey), {'commitment': commitment}])
        return int(res['value'])

    def get_latest_blockhash(self, commitment: str = 'confirmed') -> str:
        res = self._call('getLatestBlockhash', [{'commitment': commitment}])
        return res['value']['blockhash']

    def get_signatures_for_address(
        self,
        address,
        before: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 1000,
        commitment: str = 'confirmed',
    ) -> List[dict]:
        opts: dict = {'limit': limit, 'commitment': commitment}
        if before:
            opts['before'] = before
        if until:
            opts['until'] = until
        return self._call('getSignaturesForAddress', [str(address), opts])

    def get_transaction(self, signature: str, commitment: str = 'confirmed') -> Optional[dict]:
        return self._call(
            'getTransaction', [signature, {'commitment': commitment, 'maxSupportedTransactionVersion': 0}]
        )

    # --- writes ---
    def simulate_transaction(self, raw_tx_b64: str) -> dict:
        return self._call('simulateTransaction', [raw_tx_b64, {'encoding': 'base64'}])

    def send_transaction(
        self, raw_tx_b64: str, skip_preflight: bool = False, preflight_commitment: str = 'confirmed'
    ) -> str:
        # preflight_commitment must match the blockhash commitment (getLatestBlockhash uses "confirmed"),
        # else a fresh validator rejects the not-yet-finalized blockhash as "Blockhash not found".
        return self._call(
            'sendTransaction',
            [
                raw_tx_b64,
                {
                    'encoding': 'base64',
                    'skipPreflight': skip_preflight,
                    'preflightCommitment': preflight_commitment,
                },
            ],
        )

    def get_signature_statuses(self, signatures: List[str]) -> list:
        res = self._call('getSignatureStatuses', [signatures, {'searchTransactionHistory': True}])
        return res['value']

    def confirm(self, signature: str, timeout: float = 30.0, poll: float = 0.4) -> dict:
        """Block until the signature reaches `confirmed`; raise on tx error or timeout. A transient RPC
        fault while polling the status is retried until the deadline: a status we cannot read is
        'unknown', not 'failed', and must never be mistaken for a failed tx (that false negative is what
        aborted an in-flight swap origination whose tx had actually landed)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                status = self.get_signature_statuses([signature])[0]
            except TransientRpcError:
                time.sleep(poll)
                continue
            if status is not None:
                if status.get('err') is not None:
                    raise SolanaRpcError(f'tx {signature} failed: {status["err"]}')
                if status.get('confirmationStatus') in ('confirmed', 'finalized'):
                    return status
            time.sleep(poll)
        raise SolanaRpcError(f'tx {signature} not confirmed within {timeout}s')

    def request_airdrop(self, pubkey, lamports: int) -> str:
        return self._call('requestAirdrop', [str(pubkey), lamports])
