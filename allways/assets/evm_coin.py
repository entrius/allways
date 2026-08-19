import time
from typing import Optional

import bittensor as bt
from eth_account import Account
from eth_utils import to_checksum_address

from allways.assets.asset import ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.evm import EVM_NETWORKS, FALLBACK_PRIORITY_FEE_WEI, EvmAsset, EvmChain
from allways.chains import ChainDefinition
from allways.constants import CANCEL_REASON_EVM_REVERT

TRANSFER_GAS = 21_000
# Refuse destinations whose receive hook wants more than this — bounds the miner's gas
# spend against a hostile contract; the slash gate exempts code-bearing dests anyway.
MAX_TRANSFER_GAS = 100_000
# Gas limit the miner supplies on a refusal-evidence broadcast (a dest that reverts a well-gassed
# transfer). Equal to MAX_TRANSFER_GAS so a good-faith attempt provides at least the gas a cooperative
# smart-wallet delivery would consume; a resulting revert is then sound "dest refused" evidence, and a
# miner cannot under-gas to manufacture one. A revert refunds the unused gas, so real cost << the limit.
ETH_FULFILL_GAS_FLOOR = MAX_TRANSFER_GAS
ZERO_ADDRESS = '0x' + '00' * 20
# Hard ceiling on the deposit scanner's block walk, independent of block time: the walk costs one
# RPC per block, and sub-second chains would otherwise turn SCAN_LOOKBACK_BLOCKS into hundreds of
# calls per pass (HyperEVM's public RPC allows 100/min). Never binds on ~12s chains (25 blocks).
MAX_WALK_BLOCKS = 32


class EvmCoin(EvmAsset):
    """An EVM network's native coin: eth-account + public JSON-RPC (no local node required).

    Plain EOA value transfers only, by design: a swap leg is verified off the transaction's
    own from/to/value, which internal (contract-mediated) transfers don't populate — and the
    sender-pinning defense needs a provable EOA sender anyway.

    Composes its network's `EvmChain` exactly as the `Erc20` twin does — a native coin is
    an asset ON a network, not the network itself, and fusing the two stops being true the
    moment a token lands beside it. Both are bound by a registry row, never subclassed.
    """

    def __init__(self, chain_def: ChainDefinition):
        self._chain_def = chain_def
        self._chain = EvmChain(EVM_NETWORKS[chain_def.host_chain], chain_def.env_prefix)
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

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
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
            # A prior own broadcast for THIS obligation blocks a fresh send unless provably
            # absent from every endpoint — probing by hash sees the mempool; never pay twice.
            want = (norm(to_address), int(amount), dedup_key or '')
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
            # A chain that enforces a tip floor rejects anything under it outright, and the failure
            # repeats identically on every retry — clamp rather than sign an unbroadcastable tx.
            tip_fee = max(tip_fee, self.chain.network_def.min_priority_fee_wei)
            # 2× base fee absorbs six consecutive maximally-full blocks before the cap binds.
            max_fee = 2 * base_fee + tip_fee

            gas = self._transfer_gas(acct.address, to_address, amount)
            if gas is None:
                # The destination refuses a well-formed transfer (revert) or wants more gas than the cap.
                # Do NOT bail silently — broadcast a floor-gas attempt so the on-chain reverted tx becomes
                # the evidence that earns a no-fault cancel (no slash) rather than a false timeout slash.
                # The revert refunds unused gas, so the miner's real cost is intrinsic + execution, bounded
                # by ETH_FULFILL_GAS_FLOOR. The miner marks fulfilled with this hash like any delivery; the
                # validator adjudicates delivered-vs-refused from the receipt.
                gas = ETH_FULFILL_GAS_FLOOR
                bt.logging.warning(
                    f'{to_address} refuses {prefix} transfers — broadcasting a floor-gas attempt as refusal evidence'
                )

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
            # Structured on-chain verdict, not message-substring (which misses a code-3 revert whose
            # text omits the word "revert" → a doomed tx would broadcast). Mirrors the ERC-20 twin.
            return None if getattr(e, 'is_execution_revert', False) else TRANSFER_GAS
        gas = est + est // 5
        return None if gas > MAX_TRANSFER_GAS else gas

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        """Reserve-time gate: an EOA can never refuse a transfer; a code-bearing dest must pass
        the same simulation the miner's send runs — from the committed sender when known, with
        the +20% headroom and MAX_TRANSFER_GAS cap. A dest the send path would refuse (revert
        only for msg.sender != 0, or gas just under the cap from zero) must bounce here, not
        ride the miner to a timeout slash. Fails open on RPC trouble."""
        try:
            if (self.chain.eth_rpc('eth_getCode', [address, 'latest']) or '0x') == '0x':
                return True
        except Exception:
            return True
        return self._transfer_gas(from_address or ZERO_ADDRESS, address, amount) is not None

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

    def cancel_evidence(
        self, address: str, amount: int, tx_hash: Optional[str] = None, from_address: Optional[str] = None
    ) -> Optional[int]:
        """No-fault-cancel evidence for native EVM: the miner's own delivery tx reverted despite a
        correct, well-gassed attempt — sound proof the destination refused, strong enough to TERMINATE
        the swap with no slash (unlike ``delivery_refused``, a getCode deferral hint). Returns
        ``CANCEL_REASON_EVM_REVERT`` only when the recorded tx (1) reverted (receipt status 0), (2) came
        FROM the committed miner sender, (3) paid the pinned dest, (4) carried >= the required value, and
        (5) supplied a gas LIMIT >= ``ETH_FULFILL_GAS_FLOOR``. Clauses (2) and (5) are load-bearing: (5)
        stops a miner under-gassing to fake a refusal, and (2) stops a miner pointing at a reverted tx
        sent from a throwaway address (against a dest that reverts unless ``msg.sender`` is the committed
        miner) to claim refusal without ever attempting delivery from its own address. Deliberately NOT
        ``gasUsed < gas`` (a hostile contract can burn all gas to mimic OOG). ``None`` on anything short
        of that proof or on RPC trouble — the caller then keeps waiting, never slashes."""
        if not tx_hash:
            return None
        try:
            tx = self.chain.eth_rpc('eth_getTransactionByHash', [tx_hash], null_needs_quorum=True)
            receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [tx_hash]) if tx else None
        except Exception:
            return None
        if not tx or not receipt:
            return None
        norm = self.chain.normalize_address
        try:
            status = int(receipt['status'], 16)
        except (KeyError, TypeError, ValueError):
            return None
        if status != 0:
            return None  # a delivered tx is not a refusal
        if from_address and norm(tx.get('from') or '') != norm(from_address):
            return None  # not the committed miner's own attempt — could be a throwaway-sender ruse
        if norm(tx.get('to') or '') != norm(address):
            return None  # paid the wrong destination
        if int(tx.get('value') or '0x0', 16) < int(amount):
            return None  # under-paid — not a good-faith attempt
        if int(tx.get('gas') or '0x0', 16) < ETH_FULFILL_GAS_FLOOR:
            return None  # under-gassed — could be manufactured; never accept as refusal
        return CANCEL_REASON_EVM_REVERT

    # Plain JSON-RPC has no address→tx index, so the scanner walks the head incrementally: each
    # call scans blocks minted since the last for this (from, to, amount) triple. The cold-start
    # floor is min(SCAN_LOOKBACK_BLOCKS, MAX_WALK_BLOCKS) ≤ 32 blocks — under SCAN_LOOKBACK_SECS on fast chains.
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
        floor = max(head - min(self.SCAN_LOOKBACK_BLOCKS, MAX_WALK_BLOCKS), 0)
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
