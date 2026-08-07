import os
import re
import time
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import bittensor as bt
import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import is_checksum_address

from allways.assets.base import Asset, ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.chain import Chain
from allways.chains import CHAIN_ETH, ChainDefinition

LOG_ETH = '[EthRpc]'

# eth_chainId is checked against the configured network at startup — a wrong-network RPC
# (mainnet URL while ETH_NETWORK=sepolia) fails fast instead of verifying the wrong chain.
CHAIN_IDS = {'mainnet': 1, 'sepolia': 11_155_111}

# Public JSON-RPC defaults per network, tried in order. Override with ETH_RPC_URLS
# (keyed/paid endpoints embed their key in the URL path, so no header plumbing needed).
DEFAULT_RPC_URLS = {
    'mainnet': ('https://ethereum-rpc.publicnode.com', 'https://eth.drpc.org'),
    'sepolia': ('https://ethereum-sepolia-rpc.publicnode.com', 'https://sepolia.drpc.org'),
}

TRANSFER_GAS = 21_000
# Refuse destinations whose receive hook wants more than this — bounds the miner's gas
# spend against a hostile contract; the slash gate exempts code-bearing dests anyway.
MAX_TRANSFER_GAS = 100_000
ZERO_ADDRESS = '0x' + '00' * 20

_HEX_ADDR_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')

# Settled-tx cache bound — comfortably above any realistic concurrent-leg count.
_SETTLED_CACHE_MAX_DEFAULT = 512
# eth_maxPriorityFeePerGas fallback when an endpoint doesn't serve it (1 gwei clears
# comfortably in normal conditions without meaningfully overpaying a 21k-gas send).
FALLBACK_PRIORITY_FEE_WEI = 1_000_000_000


def rpc_tag(base: str) -> str:
    """Short, log-friendly host label for an RPC endpoint (e.g. 'publicnode', 'drpc')."""
    host = (urlparse(base).netloc or base).split(':')[0].removeprefix('www.')
    parts = host.split('.')
    return parts[-2] if len(parts) >= 2 else host


class Ether(Asset, Chain):
    """Ethereum chain provider: eth-account + public JSON-RPC (no local node required).

    Plain EOA value transfers only, by design: a swap leg is verified off the transaction's
    own from/to/value, which internal (contract-mediated) transfers don't populate — and the
    sender-pinning defense needs a provable EOA sender anyway.

    Addresses are hex and case-insensitive (EIP-55 is a display checksum), so every
    comparison goes through ``normalize_address`` (lowercase) — on-chain strings keep
    whatever casing the user committed.
    """

    def __init__(self):
        self.network = os.environ.get('ETH_NETWORK', '').lower()
        if not self.network:
            self.network = 'mainnet'
            bt.logging.warning('ETH_NETWORK unset — defaulting to mainnet; set ETH_NETWORK=sepolia for testnet.')
        if self.network not in CHAIN_IDS:
            # A typo silently becoming mainnet would pay real ETH against test swaps.
            raise ValueError(f'Unknown ETH_NETWORK {self.network!r} — expected one of {list(CHAIN_IDS)}')

        # Same rationale as the BTC provider: long-running validators + pooled idle TLS
        # sockets that public CDNs silently drop → wedged reads until timeout.
        self.http = requests.Session()
        self.http.headers['Connection'] = 'close'

        self.last_send_error: Optional[str] = None
        # Settled-tx cache, keyed tx_hash → {block_hash, block_number, block_time}. A mined tx's
        # receipt and its block's timestamp are immutable per block hash, so the validator's 12s
        # re-verify pays 1 RPC (getTransactionByHash) instead of 3 — parity with BTC's single /tx.
        # Reorg-safe: an entry is served only while the fresh tx fetch reports the SAME blockHash
        # (key-by-hash); a reorg changes it → miss → full refetch. Only fully-settled entries
        # (status 1 + timestamp) are cached — never pending/absent/reverted.
        self._settled_cache: OrderedDict[str, dict] = OrderedDict()
        # Own broadcasts, tx_hash → (to, amount, head at broadcast): send dedup scoped to this
        # process and to SCAN_LOOKBACK_BLOCKS (#461 class — a later same-amount swap must never
        # resolve to an earlier swap's consumed tx).
        self.broadcasted_txids: Dict[str, Tuple[str, int, int]] = {}
        # Deposit-scanner head cursors, keyed per (from, to, amount) — see find_recent_outgoing.
        self.scan_cursors: Dict[Tuple[str, str, int], int] = {}

        raw = os.environ.get('ETH_RPC_URLS', '')
        self.rpc_bases = [u.strip().rstrip('/') for u in raw.split(',') if u.strip()] or list(
            DEFAULT_RPC_URLS[self.network]
        )

    def _send_error(self, msg: str) -> None:
        self.last_send_error = msg
        bt.logging.error(msg)

    @property
    def chain_def(self) -> ChainDefinition:
        return CHAIN_ETH

    def describe(self) -> str:
        hosts = ', '.join(urlparse(base).netloc or base for base in self.rpc_bases)
        return f'Ethereum JSON-RPC ({self.network}): {hosts}'

    def normalize_address(self, address: str) -> str:
        return address.lower() if isinstance(address, str) else address

    # --- JSON-RPC plumbing (failover mirrors the BTC provider's Esplora narration) ---

    def eth_rpc(self, method: str, params: list, timeout: int = 15, null_needs_quorum: bool = False) -> Any:
        """Call ``method`` against each endpoint in order, narrating every failover.

        A JSON-RPC ``error`` object is treated as endpoint trouble (rate limit, pruned
        state, method gap) and falls through; ``result: null`` is authoritative data
        ("no such tx") and is returned as None. Raises the last error when all fail.

        ``null_needs_quorum``: return None only when every endpoint reachably agrees;
        null + an unreachable endpoint raises. For paths where "absent" costs money —
        one stale node in a public load balancer must not read as "never happened".
        """
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
        last_err: Optional[Exception] = None
        saw_null = False
        for i, base in enumerate(self.rpc_bases):
            pos = f'[{i + 1}/{len(self.rpc_bases)}]'
            tag = rpc_tag(base)
            nxt = rpc_tag(self.rpc_bases[i + 1]) if i + 1 < len(self.rpc_bases) else None
            tail = f'falling back to: {nxt}' if nxt else 'no providers left, giving up'
            try:
                resp = self.http.post(base, json=payload, timeout=timeout)
            except Exception as e:
                last_err = e
                bt.logging.warning(f'EthRpc {pos} {tag} {method} → request error: {e}; {tail}')
                continue

            if resp.status_code != 200:
                last_err = requests.HTTPError(f'{base} {method}: {resp.status_code}', response=resp)
                bt.logging.warning(f'EthRpc {pos} {tag} {method} → HTTP {resp.status_code}; {tail}')
                continue

            try:
                body = resp.json()
            except ValueError as e:
                last_err = e
                bt.logging.warning(f'EthRpc {pos} {tag} {method} → bad JSON: {e}; {tail}')
                continue

            if 'error' in body:
                err = body['error']
                last_err = RuntimeError(f'{base} {method}: rpc error {err}')
                bt.logging.warning(f'EthRpc {pos} {tag} {method} → rpc error {err}; {tail}')
                continue

            result = body.get('result')
            if result is None and null_needs_quorum:
                saw_null = True
                bt.logging.debug(f'EthRpc {pos} {tag} {method} → null; seeking quorum from remaining endpoint(s)')
                continue
            if i > 0:
                bt.logging.info(f'EthRpc {pos} {tag} {method} → ok (served after {i} fallback(s))')
            else:
                bt.logging.debug(f'EthRpc {pos} {tag} {method} → ok')
            return result
        if saw_null and last_err is None:
            return None
        if saw_null:
            raise ProviderUnreachableError(f'{method}: null without quorum (an endpoint was unreachable): {last_err}')
        raise last_err or RuntimeError('all ETH RPCs failed')

    def check_connection(self, require_send: bool = True) -> None:
        if require_send:
            key = os.environ.get('ETH_PRIVATE_KEY')
            if not key:
                raise ConnectionError('ETH signing requires the ETH_PRIVATE_KEY env var')
            try:
                Account.from_key(key)
            except Exception as e:
                raise ConnectionError(f'ETH_PRIVATE_KEY is not a valid 32-byte hex key: {e}') from e
        try:
            chain_id = int(self.eth_rpc('eth_chainId', [], timeout=10), 16)
            tip = int(self.eth_rpc('eth_blockNumber', [], timeout=10), 16)
        except Exception as e:
            raise ConnectionError(f'Cannot reach Ethereum RPC: {e}') from e
        expected = CHAIN_IDS[self.network]
        if chain_id != expected:
            raise ConnectionError(
                f'ETH RPC serves chain id {chain_id}, expected {expected} for {self.network} — '
                'ETH_RPC_URLS and ETH_NETWORK disagree'
            )
        bt.logging.success(f'{LOG_ETH} connected: network={self.network}, chain_id={chain_id}, tip={tip}')

    # --- Verification ---

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,  # unused — eth_getTransactionByHash is an O(1) index
    ) -> Optional[TransactionInfo]:
        """Look up an Ethereum tx by hash and match recipient + amount.

        Inclusion is not settlement (the TAO lesson): a mined tx can still have reverted, so
        a mined match requires ``eth_getTransactionReceipt`` status 1 before it counts. A
        mined tx whose receipt can't be read is 'unknown', never 'absent' — that raises
        ProviderUnreachableError rather than risking a false slash verdict.
        """
        try:
            tx = self.eth_rpc('eth_getTransactionByHash', [tx_hash], null_needs_quorum=True)
        except Exception as e:
            raise ProviderUnreachableError(f'ETH RPC unreachable: {e}') from e
        if tx is None:
            bt.logging.debug(f'{LOG_ETH} tx {tx_hash[:16]}... not found')
            return None

        to = self.chain.normalize_address(tx.get('to') or '')  # null for contract creation
        sender = self.chain.normalize_address(tx.get('from') or '')
        amount = int(tx.get('value') or '0x0', 16)
        if to != self.chain.normalize_address(expected_recipient) or amount < expected_amount:
            bt.logging.warning(
                f'{LOG_ETH} tx {tx_hash[:16]}... does not pay {expected_recipient} >= {expected_amount} wei '
                f'(to={tx.get("to")}, value={amount})'
            )
            return None

        if tx.get('blockNumber') is None:
            # In the mempool: a valid match, just not mined yet — callers queue and retry.
            return TransactionInfo(
                tx_hash=tx_hash,
                confirmed=False,
                sender=sender,
                recipient=expected_recipient,
                amount=amount,
                block_number=None,
                confirmations=0,
                block_time=None,
            )

        # Cache hit: the fresh tx fetch reports the same blockHash a fully-settled read was
        # cached under, so its receipt/status/timestamp are immutable — skip the receipt+block
        # RPCs (2 of the 3 calls on the validator's 12s re-verify path). The blockHash equality
        # IS the canonical-continuity check: a reorg changes it → miss → full refetch below.
        tx_block_hash = tx.get('blockHash') or ''
        cached = self._settled_cache.get(tx_hash)
        if cached is not None and tx_block_hash and cached['block_hash'] == tx_block_hash:
            block_number = cached['block_number']
            block_time = cached['block_time']
            tip = self.chain.cached_block_height()
            confirmations = max(0, tip - block_number + 1) if tip is not None else 0
            is_confirmed = confirmations >= self.chain_def.min_confirmations
        else:
            try:
                receipt = self.eth_rpc('eth_getTransactionReceipt', [tx_hash])
            except Exception as e:
                raise ProviderUnreachableError(f'ETH receipt fetch failed for {tx_hash[:16]}...: {e}') from e
            if receipt is None:
                raise ProviderUnreachableError(f'ETH tx {tx_hash[:16]}... is mined but its receipt is unavailable')
            if int(receipt.get('status') or '0x0', 16) != 1:
                bt.logging.warning(f'{LOG_ETH} tx {tx_hash[:16]}... reverted (status 0) — moved no funds, rejecting')
                return None

            block_number = int(receipt['blockNumber'], 16)
            tip = self.chain.cached_block_height()
            confirmations = max(0, tip - block_number + 1) if tip is not None else 0
            is_confirmed = confirmations >= self.chain_def.min_confirmations

            # The freshness gate fails closed on a missing block_time, so an unreadable
            # timestamp must be 'unknown' (raise), never a verdict — same as the receipt above.
            try:
                block = self.eth_rpc('eth_getBlockByNumber', [hex(block_number), False])
            except Exception as e:
                raise ProviderUnreachableError(f'ETH block fetch failed for {tx_hash[:16]}...: {e}') from e
            block_time = int((block or {}).get('timestamp') or '0x0', 16) or None
            if block_time is None:
                raise ProviderUnreachableError(
                    f'ETH tx {tx_hash[:16]}... is mined but block {block_number} has no readable timestamp'
                )
            if is_confirmed and block.get('hash') and block['hash'] != receipt.get('blockHash'):
                bt.logging.warning(f'{LOG_ETH} tx {tx_hash[:16]}... block was reorged out — rejecting')
                return None

            # Cache only a fully-settled, internally-consistent read (status 1, timestamped,
            # tx/receipt agree on the block hash) — pending/absent/reverted are never cached.
            receipt_block_hash = receipt.get('blockHash') or ''
            if receipt_block_hash and (not tx_block_hash or tx_block_hash == receipt_block_hash):
                self._settled_cache[tx_hash] = {
                    'block_hash': receipt_block_hash,
                    'block_number': block_number,
                    'block_time': block_time,
                }
                while len(self._settled_cache) > self._SETTLED_CACHE_MAX:
                    self._settled_cache.popitem(last=False)

        return TransactionInfo(
            tx_hash=tx_hash,
            confirmed=is_confirmed,
            sender=sender,
            recipient=expected_recipient,
            amount=amount,
            block_number=block_number,
            confirmations=confirmations,
            block_time=block_time,
        )

    def get_current_block_height(self) -> Optional[int]:
        try:
            return int(self.eth_rpc('eth_blockNumber', [], timeout=10), 16)
        except Exception as e:
            bt.logging.debug(f'ETH get_current_block_height failed: {e}')
            return None

    def get_balance(self, address: str) -> int:
        """Balance in wei at latest, 0 on failure (mirrors the other providers)."""
        try:
            return int(self.eth_rpc('eth_getBalance', [address, 'latest']), 16)
        except Exception as e:
            bt.logging.error(f'ETH get_balance failed for {address}: {e}')
            return 0

    def is_valid_address(self, address: str) -> bool:
        """0x + 40 hex, and if mixed-case the EIP-55 checksum must verify (typo protection,
        no RPC). Done by hand: eth_utils 5+ ``is_address`` accepts unprefixed hex and stopped
        validating checksums, which would wave through exactly the typos EIP-55 exists to catch."""
        if not isinstance(address, str) or not _HEX_ADDR_RE.match(address):
            return False
        body = address[2:]
        if body == body.lower() or body == body.upper():
            return True
        return is_checksum_address(address)

    def _account(self, key: Optional[Any] = None):
        raw = key if isinstance(key, str) and key else os.environ.get('ETH_PRIVATE_KEY')
        if not raw:
            return None
        try:
            return Account.from_key(raw)
        except Exception as e:
            bt.logging.error(f'ETH private key unusable: {e}')
            return None

    def can_send_from(self, address: str) -> bool:
        acct = self._account()
        return acct is not None and self.chain.normalize_address(acct.address) == self.chain.normalize_address(address)

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """EIP-191 personal_sign over ``message``. key: hex private key; None → ETH_PRIVATE_KEY."""
        acct = self._account(key)
        if acct is None:
            bt.logging.error('ETH signing requires the ETH_PRIVATE_KEY env var (or an explicit key)')
            return ''
        if self.normalize_address(acct.address) != self.normalize_address(address):
            bt.logging.error(f'ETH key derives {acct.address} but the proof address is {address} — key mismatch')
            return ''
        try:
            signed = Account.sign_message(encode_defunct(text=message), acct.key)
            return signed.signature.hex()
        except Exception as e:
            bt.logging.error(f'ETH sign_from_proof failed: {e}')
            return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Recover the EIP-191 signer and compare case-insensitively. No RPC dependency."""
        try:
            sig = signature if signature.startswith('0x') else f'0x{signature}'
            recovered = Account.recover_message(encode_defunct(text=message), signature=sig)
            return self.normalize_address(recovered) == self.normalize_address(address)
        except Exception as e:
            bt.logging.error(f'ETH verify_from_proof failed: {e}')
            return False

    # --- Sending ---

    def send_amount(self, to_address: str, amount: int, from_address: Optional[str] = None) -> SendResult:
        """Send ETH via a type-2 (EIP-1559) transfer signed with ETH_PRIVATE_KEY. Amount in wei.

        ``from_address`` (the miner's committed address) must match the key — validators
        enforce sender == committed address, so sending from any other key is a wasted tx.
        Returns (tx_hash, 0) or None.
        """
        self.last_send_error = None
        acct = self._account()
        if acct is None:
            self._send_error('ETH_PRIVATE_KEY not set or unusable')
            return None
        if from_address and self.chain.normalize_address(acct.address) != self.chain.normalize_address(from_address):
            self._send_error(f'ETH key derives {acct.address} but committed address is {from_address} — key mismatch')
            return None

        head = self.chain.get_current_block_height()
        if head is None:
            self._send_error('cannot read the chain head to scope send dedup — not sending')
            return None

        try:
            # A prior own broadcast to this dest blocks a fresh send unless provably absent
            # from every endpoint — probing by hash sees the mempool; never risk paying twice.
            want = (self.chain.normalize_address(to_address), int(amount))
            try:
                prior = self._prior_broadcast(want, head)
            except Exception as e:
                self._send_error(f'prior broadcast unresolved ({e}) — refusing a possible double send')
                return None
            if prior:
                bt.logging.info(f'Reusing prior tx {prior} from {acct.address} → {to_address} ({amount} wei)')
                return (prior, 0)

            latest = self.eth_rpc('eth_getBlockByNumber', ['latest', False])
            base_fee = int((latest or {}).get('baseFeePerGas') or '0x0', 16)
            try:
                tip_fee = int(self.eth_rpc('eth_maxPriorityFeePerGas', []), 16)
            except Exception:
                tip_fee = FALLBACK_PRIORITY_FEE_WEI
            # 2× base fee absorbs six consecutive maximally-full blocks before the cap binds.
            max_fee = 2 * base_fee + tip_fee

            gas = self._transfer_gas(acct.address, to_address, amount)
            if gas is None:
                self._send_error(f'destination {to_address} refuses ETH transfers — not sending')
                return None

            nonce = int(self.eth_rpc('eth_getTransactionCount', [acct.address, 'pending']), 16)

            balance = self.get_balance(acct.address)
            needed = amount + gas * max_fee
            if balance < needed:
                self._send_error(f'Insufficient ETH: have {balance} wei, need {amount} + {gas * max_fee} gas')
                return None

            signed = Account.sign_transaction(
                {
                    'chainId': CHAIN_IDS[self.network],
                    'nonce': nonce,
                    'to': to_address,
                    'value': amount,
                    'gas': gas,
                    'maxFeePerGas': max_fee,
                    'maxPriorityFeePerGas': tip_fee,
                },
                acct.key,
            )
            expected_txid = signed.hash.hex()
            expected_txid = expected_txid if expected_txid.startswith('0x') else f'0x{expected_txid}'
            # Record pre-broadcast so a retry can reclaim it even if the response is lost.
            self.broadcasted_txids[expected_txid] = (*want, head)

            raw = getattr(signed, 'raw_transaction', None) or getattr(signed, 'rawTransaction')
            raw_hex = raw.hex()
            raw_hex = raw_hex if raw_hex.startswith('0x') else f'0x{raw_hex}'
            try:
                tx_hash = self.eth_rpc('eth_sendRawTransaction', [raw_hex], timeout=30)
            except Exception as broadcast_err:
                # The tx may have been accepted anyway — check before declaring failure.
                try:
                    if self.eth_rpc('eth_getTransactionByHash', [expected_txid], null_needs_quorum=True) is not None:
                        return (expected_txid, 0)
                except Exception:
                    pass
                self._send_error(f'ETH broadcast failed: {broadcast_err}')
                return None

            bt.logging.info(f'Sent {amount} wei to {to_address} (tx: {tx_hash}, maxFee: {max_fee})')
            # A quirky null broadcast reply must never persist as a blank tx hash.
            return (tx_hash or expected_txid, 0)
        except Exception as e:
            self._send_error(f'ETH send failed: {type(e).__name__}: {e}')
            return None

    def _prior_broadcast(self, want: Tuple[str, int], head: int) -> Optional[str]:
        """Reusable tx hash from a prior own broadcast to (to, amount); None when a fresh send is
        provably safe; raises when an in-flight duplicate can't be ruled out. Reuse requires the
        tx to be in the mempool or settled (status 1) — a reverted tx moved no funds and clears.
        Entries age out after SCAN_LOOKBACK_BLOCKS (the old mined-scan bound), so a later
        same-amount swap can never resolve to an earlier swap's consumed tx."""
        for txid, (to_norm, amt, seen) in list(self.broadcasted_txids.items()):
            if head - seen > self.SCAN_LOOKBACK_BLOCKS:
                del self.broadcasted_txids[txid]
                continue
            if (to_norm, amt) != want:
                continue
            tx = self.eth_rpc('eth_getTransactionByHash', [txid], null_needs_quorum=True)
            if tx is None:
                continue
            if tx.get('blockNumber') is None:
                return txid
            receipt = self.eth_rpc('eth_getTransactionReceipt', [txid])
            if receipt is None:
                raise RuntimeError(f'prior tx {txid[:16]}... mined but its receipt is unavailable')
            if int(receipt.get('status') or '0x0', 16) == 1:
                return txid
            del self.broadcasted_txids[txid]
        return None

    def _transfer_gas(self, from_addr: str, to_addr: str, amount: int) -> Optional[int]:
        """Gas limit via eth_estimateGas (+20% headroom for smart-wallet receive hooks); None
        when the destination refuses or wants absurd gas. Estimator trouble falls back to the
        plain-transfer minimum rather than blocking an EOA payout."""
        try:
            est = int(self.eth_rpc('eth_estimateGas', [{'from': from_addr, 'to': to_addr, 'value': hex(amount)}]), 16)
        except Exception as e:
            return None if 'revert' in str(e).lower() else TRANSFER_GAS
        gas = est + est // 5
        return None if gas > MAX_TRANSFER_GAS else gas

    def can_deliver_to(self, address: str, amount: int) -> bool:
        """Reserve-time gate: an EOA can never refuse a transfer; a code-bearing dest must pass
        a simulated transfer. Fails open — only a positive revert blocks a reservation."""
        try:
            if (self.eth_rpc('eth_getCode', [address, 'latest']) or '0x') == '0x':
                return True
            self.eth_rpc('eth_estimateGas', [{'from': ZERO_ADDRESS, 'to': address, 'value': hex(amount)}])
            return True
        except Exception as e:
            return 'revert' not in str(e).lower()

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Slash gate: code at the destination — now or sampled across the window since
        ``since_unix`` — is positive evidence a transfer could fail; never slash over it.
        getCode is dest-only (miner can't influence it) where a simulation is gameable by
        either side. Raises when the chain view is unavailable (caller defers)."""
        tip = int(self.eth_rpc('eth_blockNumber', []), 16)
        # Probe depth clamped to what non-archive public nodes serve (~128 blocks).
        span = min(120, max(0, int(time.time()) - int(since_unix)) // self.chain_def.seconds_per_block)
        probes = ['latest'] + [hex(max(0, tip - span // d)) for d in (1, 2)]
        return any((self.eth_rpc('eth_getCode', [address, b]) or '0x') != '0x' for b in probes)

    _SETTLED_CACHE_MAX = _SETTLED_CACHE_MAX_DEFAULT

    # Plain JSON-RPC has no address→tx index (Esplora/getSignaturesForAddress have one), so the
    # deposit scanner follows the head incrementally like the TAO provider: each call scans only
    # blocks minted since the last call for this (from, to, amount) triple, bounded by
    # SCAN_LOOKBACK_BLOCKS on the first call or after a gap (≈5 min of 12s blocks).
    SCAN_LOOKBACK_BLOCKS = 25
    _MAX_SCAN_CURSORS = 64

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Tx hash of a recent settled transfer ``from_addr`` → ``to_addr`` of >= ``amount`` wei,
        else None. A hash-finder only — the seam's confirm re-verifies everything by hash, so a
        miss just means the manual rescue paths. Mined blocks only (plain JSON-RPC exposes no
        mempool filter), so it lags a broadcast by up to one block."""
        head = self.chain.get_current_block_height()
        if head is None:
            return None
        norm = self.chain.normalize_address
        want_from = norm(from_addr)
        want_to = norm(to_addr)
        key = (want_from, want_to, int(amount))
        floor = max(head - self.SCAN_LOOKBACK_BLOCKS, 0)
        last = self.scan_cursors.get(key, floor)
        for block_num in range(max(last, floor) + 1, head + 1):
            try:
                block = self.eth_rpc('eth_getBlockByNumber', [hex(block_num), True])
            except Exception:
                # Never leap an unreadable block — park just below it and retry next call.
                self._set_cursor(key, block_num - 1)
                return None
            for tx in (block or {}).get('transactions', []):
                if norm(tx.get('from') or '') != want_from:
                    continue
                if norm(tx.get('to') or '') != want_to:
                    continue
                if int(tx.get('value') or '0x0', 16) < int(amount):
                    continue
                # A mined match can still have reverted — require receipt status 1.
                try:
                    receipt = self.eth_rpc('eth_getTransactionReceipt', [tx['hash']])
                except Exception:
                    continue
                if receipt is None or int(receipt.get('status') or '0x0', 16) != 1:
                    continue
                self.scan_cursors.pop(key, None)
                return tx['hash']
        self._set_cursor(key, head)
        return None

    def _set_cursor(self, key: Tuple[str, str, int], height: int) -> None:
        self.scan_cursors[key] = height
        if len(self.scan_cursors) > self._MAX_SCAN_CURSORS:
            self.scan_cursors.pop(next(iter(self.scan_cursors)))
