import time
from typing import Optional

import bittensor as bt
from eth_account import Account
from eth_utils import to_checksum_address

from allways.assets.base import ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.evm import FALLBACK_PRIORITY_FEE_WEI, EvmAsset, EvmChain, EvmNetwork
from allways.chains import ChainDefinition

TRANSFER_GAS = 21_000
# Refuse destinations whose receive hook wants more than this — bounds the miner's gas
# spend against a hostile contract; the slash gate exempts code-bearing dests anyway.
MAX_TRANSFER_GAS = 100_000
ZERO_ADDRESS = '0x' + '00' * 20


class EvmCoin(EvmAsset, EvmChain):
    """An EVM network's native coin: eth-account + public JSON-RPC (no local node required).

    Plain EOA value transfers only, by design: a swap leg is verified off the transaction's
    own from/to/value, which internal (contract-mediated) transfers don't populate — and the
    sender-pinning defense needs a provable EOA sender anyway.

    Fused with its chain (a native coin is its network's first asset); the shared EVM
    plumbing lives in `EvmChain`/`EvmAsset` and splits out the day a second asset lands on
    the network. The `Erc20` twin for tokens; both are bound by a registry row, never
    subclassed per chain.
    """

    def __init__(self, chain_def: ChainDefinition, network_def: EvmNetwork):
        self._chain_def = chain_def
        EvmChain.__init__(self, network_def, chain_def.env_prefix)
        EvmAsset.__init__(self)

    @property
    def chain_def(self) -> ChainDefinition:
        return self._chain_def

    # --- Verification ---

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,  # unused — eth_getTransactionByHash is an O(1) index
    ) -> Optional[TransactionInfo]:
        """Look up a native-coin tx by hash and match recipient + amount.

        Inclusion is not settlement (the TAO lesson): a mined tx can still have reverted, so
        a mined match requires ``eth_getTransactionReceipt`` status 1 before it counts. A
        mined tx whose receipt can't be read is 'unknown', never 'absent' — that raises
        ProviderUnreachableError rather than risking a false slash verdict.
        """
        prefix = self.chain_def.env_prefix
        try:
            tx = self.chain.eth_rpc('eth_getTransactionByHash', [tx_hash], null_needs_quorum=True)
        except Exception as e:
            raise ProviderUnreachableError(f'{prefix} RPC unreachable: {e}') from e
        if tx is None:
            bt.logging.debug(f'{self._log} tx {tx_hash[:16]}... not found')
            return None

        to = self.chain.normalize_address(tx.get('to') or '')  # null for contract creation
        sender = self.chain.normalize_address(tx.get('from') or '')
        amount = int(tx.get('value') or '0x0', 16)
        if to != self.chain.normalize_address(expected_recipient) or amount < expected_amount:
            bt.logging.warning(
                f'{self._log} tx {tx_hash[:16]}... does not pay {expected_recipient} >= {expected_amount} '
                f'{self.chain_def.native_unit} '
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
                receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [tx_hash])
            except Exception as e:
                raise ProviderUnreachableError(f'{prefix} receipt fetch failed for {tx_hash[:16]}...: {e}') from e
            if receipt is None:
                raise ProviderUnreachableError(f'{prefix} tx {tx_hash[:16]}... is mined but its receipt is unavailable')
            if int(receipt.get('status') or '0x0', 16) != 1:
                bt.logging.warning(f'{self._log} tx {tx_hash[:16]}... reverted (status 0) — moved no funds, rejecting')
                return None

            block_number = int(receipt['blockNumber'], 16)
            tip = self.chain.cached_block_height()
            confirmations = max(0, tip - block_number + 1) if tip is not None else 0
            is_confirmed = confirmations >= self.chain_def.min_confirmations

            # The freshness gate fails closed on a missing block_time, so an unreadable
            # timestamp must be 'unknown' (raise), never a verdict — same as the receipt above.
            try:
                block = self.chain.eth_rpc('eth_getBlockByNumber', [hex(block_number), False])
            except Exception as e:
                raise ProviderUnreachableError(f'{prefix} block fetch failed for {tx_hash[:16]}...: {e}') from e
            block_time = int((block or {}).get('timestamp') or '0x0', 16) or None
            if block_time is None:
                raise ProviderUnreachableError(
                    f'{prefix} tx {tx_hash[:16]}... is mined but block {block_number} has no readable timestamp'
                )
            if is_confirmed and block.get('hash') and block['hash'] != receipt.get('blockHash'):
                bt.logging.warning(f'{self._log} tx {tx_hash[:16]}... block was reorged out — rejecting')
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

    def get_balance(self, address: str) -> int:
        """Balance in the chain's native unit at latest, 0 on failure (mirrors the other providers)."""
        try:
            return int(self.chain.eth_rpc('eth_getBalance', [address, 'latest']), 16)
        except Exception as e:
            bt.logging.error(f'{self._log} get_balance failed for {address}: {e}')
            return 0

    # --- Sending ---

    def send_amount(self, to_address: str, amount: int, from_address: Optional[str] = None) -> SendResult:
        """Send the native coin via a type-2 (EIP-1559) transfer signed with {PREFIX}_PRIVATE_KEY.

        Amount is in the chain's native unit. ``from_address`` (the miner's committed address)
        must match the key — validators enforce sender == committed address, so sending from any
        other key is a wasted tx. Returns (tx_hash, 0) or None.
        """
        self.last_send_error = None
        prefix, unit = self.chain_def.env_prefix, self.chain_def.native_unit
        norm = self.chain.normalize_address
        acct = self.chain._account()
        if acct is None:
            self._send_error(f'{self.chain._key_env} not set or unusable')
            return None
        if from_address and norm(acct.address) != norm(from_address):
            self._send_error(
                f'{prefix} key derives {acct.address} but committed address is {from_address} — key mismatch'
            )
            return None

        head = self.chain.get_current_block_height()
        if head is None:
            self._send_error('cannot read the chain head to scope send dedup — not sending')
            return None

        try:
            # A prior own broadcast to this dest blocks a fresh send unless provably absent
            # from every endpoint — probing by hash sees the mempool; never risk paying twice.
            want = (norm(to_address), int(amount))
            try:
                prior = self._prior_broadcast(want, head)
            except Exception as e:
                self._send_error(f'prior broadcast unresolved ({e}) — refusing a possible double send')
                return None
            if prior:
                bt.logging.info(f'Reusing prior tx {prior} from {acct.address} → {to_address} ({amount} {unit})')
                return (prior, 0)

            latest = self.chain.eth_rpc('eth_getBlockByNumber', ['latest', False])
            base_fee = int((latest or {}).get('baseFeePerGas') or '0x0', 16)
            try:
                tip_fee = int(self.chain.eth_rpc('eth_maxPriorityFeePerGas', []), 16)
            except Exception:
                tip_fee = FALLBACK_PRIORITY_FEE_WEI
            # 2× base fee absorbs six consecutive maximally-full blocks before the cap binds.
            max_fee = 2 * base_fee + tip_fee

            gas = self._transfer_gas(acct.address, to_address, amount)
            if gas is None:
                self._send_error(f'destination {to_address} refuses {prefix} transfers — not sending')
                return None

            nonce = int(self.chain.eth_rpc('eth_getTransactionCount', [acct.address, 'pending']), 16)

            balance = self.get_balance(acct.address)
            needed = amount + gas * max_fee
            if balance < needed:
                self._send_error(f'Insufficient {prefix}: have {balance} {unit}, need {amount} + {gas * max_fee} gas')
                return None

            signed = Account.sign_transaction(
                {
                    'chainId': self.chain.chain_id,
                    'nonce': nonce,
                    # eth-account refuses a non-checksummed `to`, but an ALL-LOWERCASE committed
                    # dest is legal (EIP-55 is optional) — checksum here or the send crashes and
                    # the miner rides to a slash on an address it could never pay.
                    'to': to_checksum_address(to_address),
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
                tx_hash = self.chain.eth_rpc('eth_sendRawTransaction', [raw_hex], timeout=30)
            except Exception as broadcast_err:
                # The tx may have been accepted anyway — check before declaring failure.
                try:
                    probe = self.chain.eth_rpc('eth_getTransactionByHash', [expected_txid], null_needs_quorum=True)
                    if probe is not None:
                        return (expected_txid, 0)
                except Exception:
                    pass
                self._send_error(f'{prefix} broadcast failed: {broadcast_err}')
                return None

            bt.logging.info(f'Sent {amount} {unit} to {to_address} (tx: {tx_hash}, maxFee: {max_fee})')
            # A quirky null broadcast reply must never persist as a blank tx hash.
            return (tx_hash or expected_txid, 0)
        except Exception as e:
            self._send_error(f'{prefix} send failed: {type(e).__name__}: {e}')
            return None

    def _transfer_gas(self, from_addr: str, to_addr: str, amount: int) -> Optional[int]:
        """Gas limit via eth_estimateGas (+20% headroom for smart-wallet receive hooks); None
        when the destination refuses or wants absurd gas. Estimator trouble falls back to the
        plain-transfer minimum rather than blocking an EOA payout."""
        params = {'from': from_addr, 'to': to_addr, 'value': hex(amount)}
        try:
            est = int(self.chain.eth_rpc('eth_estimateGas', [params]), 16)
        except Exception as e:
            return None if 'revert' in str(e).lower() else TRANSFER_GAS
        gas = est + est // 5
        return None if gas > MAX_TRANSFER_GAS else gas

    def can_deliver_to(self, address: str, amount: int) -> bool:
        """Reserve-time gate: an EOA can never refuse a transfer; a code-bearing dest must pass
        a simulated transfer. Fails open — only a positive revert blocks a reservation."""
        try:
            if (self.chain.eth_rpc('eth_getCode', [address, 'latest']) or '0x') == '0x':
                return True
            self.chain.eth_rpc('eth_estimateGas', [{'from': ZERO_ADDRESS, 'to': address, 'value': hex(amount)}])
            return True
        except Exception as e:
            return 'revert' not in str(e).lower()

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Slash gate: code at the destination — now or sampled across the window since
        ``since_unix`` — is positive evidence a transfer could fail; never slash over it.
        getCode is dest-only (miner can't influence it) where a simulation is gameable by
        either side. Raises when the chain view is unavailable (caller defers)."""
        tip = int(self.chain.eth_rpc('eth_blockNumber', []), 16)
        # Probe depth clamped to what non-archive public nodes serve (~128 blocks).
        span = min(120, max(0, int(time.time()) - int(since_unix)) // self.chain_def.seconds_per_block)
        probes = ['latest'] + [hex(max(0, tip - span // d)) for d in (1, 2)]
        return any((self.chain.eth_rpc('eth_getCode', [address, b]) or '0x') != '0x' for b in probes)

    # Plain JSON-RPC has no address→tx index (Esplora/getSignaturesForAddress have one), so the
    # deposit scanner follows the head incrementally like the TAO provider: each call scans only
    # blocks minted since the last call for this (from, to, amount) triple, bounded by
    # SCAN_LOOKBACK_BLOCKS on the first call or after a gap (≈5 min of the chain's blocks).
    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Tx hash of a recent settled transfer ``from_addr`` → ``to_addr`` of >= ``amount``, else
        None. A hash-finder only — the seam's confirm re-verifies everything by hash, so a miss
        just means the manual rescue paths. Mined blocks only (plain JSON-RPC exposes no mempool
        filter), so it lags a broadcast by up to one block."""
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
                block = self.chain.eth_rpc('eth_getBlockByNumber', [hex(block_num), True])
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
                    receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [tx['hash']])
                except Exception:
                    continue
                if receipt is None or int(receipt.get('status') or '0x0', 16) != 1:
                    continue
                self.scan_cursors.pop(key, None)
                return tx['hash']
        self._set_cursor(key, head)
        return None
