"""Thin sync JSON-RPC client over `requests` for Solana.

Only the methods the validator/miner/CLI need. Kept dependency-light (no solana-py) and sync so it drops
into the existing `asyncio.to_thread` pattern. Account data is returned base64-decoded as raw bytes.
"""

import base64
import time
from typing import Any, List, Optional, Tuple

import base58
import requests


class SolanaRpcError(Exception):
    pass


class SolanaRpc:
    def __init__(self, url: str, timeout: int = 30):
        self.url = url
        self.timeout = timeout
        self._id = 0
        self._session = requests.Session()

    def _call(self, method: str, params: list) -> Any:
        self._id += 1
        payload = {'jsonrpc': '2.0', 'id': self._id, 'method': method, 'params': params}
        resp = self._session.post(self.url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        body = resp.json()
        if 'error' in body:
            raise SolanaRpcError(f'{method}: {body["error"]}')
        return body['result']

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
        """Block until the signature reaches `confirmed`; raise on error or timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.get_signature_statuses([signature])[0]
            if status is not None:
                if status.get('err') is not None:
                    raise SolanaRpcError(f'tx {signature} failed: {status["err"]}')
                if status.get('confirmationStatus') in ('confirmed', 'finalized'):
                    return status
            time.sleep(poll)
        raise SolanaRpcError(f'tx {signature} not confirmed within {timeout}s')

    def request_airdrop(self, pubkey, lamports: int) -> str:
        return self._call('requestAirdrop', [str(pubkey), lamports])
