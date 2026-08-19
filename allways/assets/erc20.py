import os
import time
from typing import Optional, Tuple

import bittensor as bt
from eth_account import Account
from eth_utils import keccak, to_checksum_address

from allways.assets.asset import ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.evm import EVM_NETWORKS, FALLBACK_PRIORITY_FEE_WEI, EvmAsset, EvmChain
from allways.chains import ChainDefinition
from allways.constants import (
    CANCEL_REASON_ERC20_BLACKLIST,
    CANCEL_REASON_ERC20_FEE_ENABLED,
    CANCEL_REASON_ERC20_PAUSED,
)


def _selector(signature: str) -> str:
    return '0x' + keccak(text=signature)[:4].hex()


# Computed, not transcribed — the ABI signature is the source of truth.
TRANSFER_TOPIC0 = '0x' + keccak(text='Transfer(address,address,uint256)').hex()
SEL_TRANSFER = _selector('transfer(address,uint256)')
SEL_BALANCE_OF = _selector('balanceOf(address)')
SEL_IS_BLACKLISTED = _selector('isBlacklisted(address)')
SEL_PAUSED = _selector('paused()')


def _refusal_call(signature: str) -> tuple[str, bool]:
    """A declared refusal check → (selector, takes_address)."""
    return _selector(signature), signature.endswith('(address)')


# transfer() runs contract code (~65k for Circle's FiatToken) — cap bounds the miner's
# gas spend against a pathological estimate without pinching the real cost.
MAX_TOKEN_TRANSFER_GAS = 150_000
# Estimator-down fallback: covers FiatToken's worst case while staying under the cap.
DEFAULT_TOKEN_TRANSFER_GAS = 120_000

# Widest address-pinned eth_getLogs span the free rungs actually serve (measured 2026-08-19:
# arbitrum publicnode and drpc both refuse 300). SCAN_LOOKBACK_BLOCKS derives from block TIME, so a
# 1s chain asks 12x what a 12s chain does — the scan must be bounded in blocks, independent of that.
MAX_LOG_SPAN_BLOCKS = 100

# Testnet deployments per (asset id, network). The canonical mainnet deployment is the
# registry row's asset_locator; {ASSET_ID}_TOKEN_CONTRACT overrides either (e2e fakes,
# emergency repoint) — each address lives exactly once. Keyed by asset, not by network
# prefix: one network can host several tokens, and each pins its own contract.
TESTNET_TOKEN_CONTRACTS = {
    # Circle-verified native USDC on Arbitrum Sepolia (developers.circle.com, 2026-08-07).
    'arbusdc': {'sepolia': '0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d'},
    # Circle-verified native USDC on Base Sepolia (developers.circle.com, 2026-08-11).
    'baseusdc': {'sepolia': '0x036CbD53842c5426634e7929541eC2318f3dCF7e'},
    # Circle-verified native USDC on Ethereum Sepolia (developers.circle.com, 2026-08-11).
    'ethusdc': {'sepolia': '0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238'},
    # Same address as mainnet and byte-identical runtime bytecode (keccak
    # 0xdeba17f1…0868, 12567 bytes) — verified symbol UNI, 18 decimals, 1e27 supply.
    'uni': {'sepolia': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'},
    # Quant's official test QNT, dispensed by its documented getTestQNT faucet
    # (0xCe8623CD…54825, docs.overledger.dev). Verified plain ERC20 — immutable, no
    # pause/blacklist, probes revert — so the row's declared surface holds on Sepolia.
    'qnt': {'sepolia': '0x81Dc68CB065ec6D9a4d24f6e2F442dc2A236D853'},
    # Circle-verified native USDC on Polygon Amoy (developers.circle.com, 2026-08-12).
    'polusdc': {'amoy': '0x41E94Eb019C0762f9Bfcf9Fb1E58725BfB0e7582'},
}


class MissingTestnetDeployment(ValueError):
    """The token has no pinned deployment on the configured test network (issuer-deployed
    assets like QNT/PAXG often exist on mainnet only). Distinct so create_assets can disable
    the spoke on testnet instead of failing the whole neuron's boot."""


def _token_contract(chain_def: ChainDefinition, network: str) -> str:
    var = f'{chain_def.id.upper()}_TOKEN_CONTRACT'
    override = os.environ.get(var)
    if override:
        return override
    if network != 'mainnet':
        contract = TESTNET_TOKEN_CONTRACTS.get(chain_def.id, {}).get(network)
        if not contract:
            raise MissingTestnetDeployment(f'No {chain_def.id} token contract for network {network!r} — set {var}')
        return contract
    contract = chain_def.asset_locator
    if not contract:
        raise ValueError(f'No {chain_def.id} token contract for network {network!r} — set {var}')
    return contract


def _pad_addr(address: str) -> str:
    """Address → 32-byte ABI word (lowercase hex, no 0x)."""
    return address.lower().removeprefix('0x').rjust(64, '0')


def _topic_addr(topic: str) -> str:
    """'0x' + address from a 32-byte log topic; '' when malformed (callers fail closed)."""
    t = (topic or '').lower().removeprefix('0x')
    if len(t) != 64 or t[:24] != '0' * 24 or not all(c in '0123456789abcdef' for c in t):
        return ''
    return '0x' + t[-40:]


class Erc20(EvmAsset):
    """A generic ERC-20 asset on an EVM chain, bound by its registry row.

    Composes its host network's `EvmChain` (``self._chain``), as every EVM asset does.
    Assets on one network share its env prefix, so they can never disagree about which
    network they are on; they still build an instance each (one extra tip fetch per pass).

    Settlement truth is the token contract's Transfer log, never tx.value: verification
    accepts a tx iff its status-1 receipt carries a Transfer from the PINNED contract
    paying the expected recipient, and the provable payer is the log's `from` topic.
    Delivery gates are issuer-shaped: blacklist + pause, and
    deliberately NO getCode — contract wallets receive ERC-20 fine, and a code probe
    would hand them slash immunity.
    """

    def __init__(self, chain_def: ChainDefinition):
        self._chain_def = chain_def
        # A token that forgot to declare would answer "never refused" and slash honest miners.
        if chain_def.refusal_checks is None:
            raise ValueError(f'{chain_def.id} declares no refusal_checks — name the freeze surface, or ()')
        self._checks = tuple(_refusal_call(sig) for sig in chain_def.refusal_checks)
        # A `(uint256)->uint256` fee view (PAXG's getFeeFor), None for tokens with no fee lever.
        self._fee_selector = _selector(chain_def.fee_check) if chain_def.fee_check else None
        self._chain = EvmChain(EVM_NETWORKS[chain_def.host_chain], chain_def.env_prefix)
        EvmAsset.__init__(self)
        self.token_contract = _token_contract(chain_def, self.chain.network)

    @property
    def chain_def(self) -> ChainDefinition:
        return self._chain_def

    def describe(self) -> str:
        return f'{super().describe()} — token {self.token_contract}'

    def _eth_call(self, selector: str, address: str = '', block: str = 'latest') -> int:
        """eth_call a nullary/one-address view on the token contract → uint result.

        Anything unparseable raises rather than reading as 0: an absent function and a `False`
        answer must never collapse, or a frozen destination reads as deliverable."""
        data = selector + (_pad_addr(address) if address else '')
        return int(self.chain.eth_rpc('eth_call', [{'to': self.token_contract, 'data': data}, block]), 16)

    def _fee_for(self, amount: int, block: str = 'latest') -> int:
        """The issuer's transfer fee on ``amount`` via the row's declared ``fee_check`` view.
        0 when the row declares none. Raises on RPC trouble (callers fail closed / defer)."""
        if self._fee_selector is None:
            return 0
        data = self._fee_selector + f'{int(amount):064x}'
        return int(self.chain.eth_rpc('eth_call', [{'to': self.token_contract, 'data': data}, block]), 16)

    def check_connection(self, require_send: bool = True) -> None:
        super().check_connection(require_send=require_send)
        try:
            code = self.chain.eth_rpc('eth_getCode', [self.token_contract, 'latest'])
        except Exception as e:
            # A probe we can't COMPLETE is transient (unreachable RPC / rate-limit storm) — degrade, never
            # crash the whole validator at boot over one spoke's flaky endpoint (that takes the hub down too).
            # The real config fault — a codeless/wrong contract — is a COMPLETED probe returning '0x' below.
            bt.logging.warning(
                f'{self.chain_def.id} token contract probe unreachable on {self.chain.network} ({e}) — degraded'
            )
            return
        if not code:
            # A null answer is a broken/lagging endpoint, not a codeless contract (the spec returns the
            # string '0x' for that) — same transient treatment as the unreachable case above.
            bt.logging.warning(
                f'{self.chain_def.id} token contract probe returned null on {self.chain.network} — degraded'
            )
            return
        if code == '0x':
            # A typo'd override or wrong network would otherwise surface as every send failing.
            raise ConnectionError(
                f'{self.chain_def.id} token contract {self.token_contract} has no code on {self.chain.network}'
            )
        try:
            if any(self._eth_call(sel) for sel, takes_address in self._checks if not takes_address):
                bt.logging.warning(f'{self._log} token is STOPPED — transfers fail until the issuer clears it')
        except Exception as e:
            bt.logging.warning(f'{self._log} refusal probe failed: {e}')

    # --- Verification ---

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,  # unused — eth_getTransactionByHash is an O(1) index
    ) -> Optional[TransactionInfo]:
        """Look up a token transfer by tx hash and match recipient + amount off the Transfer log.

        Mirrors the native EVM path (mempool match, settled cache, reorg check), with token
        semantics: a mined match requires a status-1 receipt whose logs contain a Transfer from
        the pinned contract; the sender is the log's `from` topic (unparseable fails closed).
        Expecteds arrive canonicalized (verify_transaction) — only on-chain values normalize here.
        A mined tx whose receipt/timestamp can't be read is 'unknown', never 'absent' — raise.
        """
        prefix = self.chain_def.env_prefix
        try:
            tx = self.chain.eth_rpc('eth_getTransactionByHash', [tx_hash], null_needs_quorum=True)
        except Exception as e:
            raise ProviderUnreachableError(f'{prefix} RPC unreachable: {e}') from e
        if tx is None:
            bt.logging.debug(f'{self._log} tx {tx_hash[:16]}... not found')
            return None

        if tx.get('blockNumber') is None:
            # In the mempool: match transfer() calldata addressed to the pinned contract —
            # a valid match, just not mined yet, so callers queue and retry.
            match = self._pending_transfer(tx, expected_recipient, expected_amount)
            if match is None:
                bt.logging.warning(
                    f'{self._log} pending tx {tx_hash[:16]}... is not a transfer paying '
                    f'{expected_recipient} >= {expected_amount}'
                )
                return None
            sender, amount = match
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

        # Cache hit: same blockHash as the fully-settled read this was cached under — the
        # receipt/log/timestamp are immutable, skip 2 of the 3 RPCs on the 12s re-verify path.
        # The blockHash equality IS the canonical-continuity check (a reorg misses → refetch).
        # The hit must also match THIS leg (recipient + amount): unlike the native path, the
        # match lives in the receipt logs the cache skips — a cached settled read must never
        # vouch for a different recipient's claim on the same tx hash. Mismatch → full refetch,
        # whose log scan gives the authoritative reject.
        tx_block_hash = tx.get('blockHash') or ''
        cached = self._settled_cache.get(tx_hash)
        if (
            cached is not None
            and tx_block_hash
            and cached['block_hash'] == tx_block_hash
            and cached['recipient'] == expected_recipient
            and cached['amount'] >= expected_amount
        ):
            sender, amount = cached['sender'], cached['amount']
            block_number = cached['block_number']
            block_time = cached['block_time']
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

            match = self._transfer_log(receipt, expected_recipient, expected_amount)
            if match is None:
                bt.logging.warning(
                    f'{self._log} tx {tx_hash[:16]}... has no Transfer log from {self.token_contract} paying '
                    f'{expected_recipient} >= {expected_amount}'
                )
                return None
            sender, amount = match
            block_number = int(receipt['blockNumber'], 16)

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
            if block.get('hash') and receipt.get('blockHash') and block['hash'] != receipt['blockHash']:
                bt.logging.warning(f'{self._log} tx {tx_hash[:16]}... block was reorged out — rejecting')
                return None

            receipt_block_hash = receipt.get('blockHash') or ''
            if receipt_block_hash and (not tx_block_hash or tx_block_hash == receipt_block_hash):
                self._settled_cache[tx_hash] = {
                    'block_hash': receipt_block_hash,
                    'block_number': block_number,
                    'block_time': block_time,
                    'sender': sender,
                    'recipient': expected_recipient,
                    'amount': amount,
                }
                while len(self._settled_cache) > self._SETTLED_CACHE_MAX:
                    self._settled_cache.popitem(last=False)

        tip = self.chain.cached_block_height()
        confirmations = max(0, tip - block_number + 1) if tip is not None else 0
        return TransactionInfo(
            tx_hash=tx_hash,
            confirmed=confirmations >= self.chain_def.min_confirmations,
            sender=sender,
            recipient=expected_recipient,
            amount=amount,
            block_number=block_number,
            confirmations=confirmations,
            block_time=block_time,
        )

    def _pending_transfer(self, tx: dict, expected_recipient: str, expected_amount: int) -> Optional[Tuple[str, int]]:
        """(sender, amount) decoded from a pending transfer() calldata match, else None."""
        norm = self.chain.normalize_address
        if norm(tx.get('to') or '') != norm(self.token_contract):
            return None
        data = (tx.get('input') or '').lower().removeprefix('0x')
        if not data.startswith(SEL_TRANSFER[2:]) or len(data) < 8 + 128:
            return None
        recipient = _topic_addr(data[8:72])
        if not recipient or recipient != expected_recipient:
            return None
        amount = int(data[72:136], 16)
        if amount < expected_amount:
            return None
        return norm(tx.get('from') or ''), amount

    def _transfer_log(self, receipt: dict, expected_recipient: str, expected_amount: int) -> Optional[Tuple[str, int]]:
        """(sender, amount) from the first Transfer log of the PINNED contract paying the
        recipient >= amount, else None. Logs from any other contract (USDC.e, fake tokens)
        never match; an unparseable `from` topic fails closed (unprovable payer — F5)."""
        contract = self.chain.normalize_address(self.token_contract)
        for log in receipt.get('logs') or []:
            if self.chain.normalize_address(log.get('address') or '') != contract:
                continue
            topics = log.get('topics') or []
            if len(topics) != 3 or (topics[0] or '').lower() != TRANSFER_TOPIC0:
                continue
            if _topic_addr(topics[2]) != expected_recipient:
                continue
            try:
                amount = int(log.get('data') or '0x0', 16)
            except (TypeError, ValueError):
                continue
            if amount < expected_amount:
                continue
            sender = _topic_addr(topics[1])
            if not sender:
                continue
            return sender, amount
        return None

    def get_balance(self, address: str) -> int:
        """Token balance (smallest units) at latest, 0 on failure (mirrors the other providers)."""
        try:
            return self._eth_call(SEL_BALANCE_OF, address)
        except Exception as e:
            bt.logging.error(f'{self._log} get_balance failed for {address}: {e}')
            return 0

    # --- Sending ---

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
        """Send tokens via transfer() signed with {PREFIX}_PRIVATE_KEY. Amount in smallest units.

        Dual-balance preflight: the token balance covers ``amount`` AND the native balance
        covers gas — a token-rich, gas-poor miner must refuse here, not burn a revert.
        Returns (tx_hash, 0) or None; dedup/rescue ladder mirrors the native EVM send.
        """
        self.last_send_error = None
        prefix = self.chain_def.env_prefix  # the NETWORK's key/RPC env
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
                bt.logging.info(f'Reusing prior tx {prior} from {acct.address} → {to_address} ({amount} units)')
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
            max_fee = 2 * base_fee + tip_fee

            # Token balance BEFORE the gas estimate: an underfunded transfer reverts in the
            # estimator too, which would misread as the destination refusing.
            token_balance = self.get_balance(acct.address)
            if token_balance < amount:
                self._send_error(f'{self._log} insufficient balance: have {token_balance} units, need {amount}')
                return None

            calldata = SEL_TRANSFER[2:] + _pad_addr(to_address) + f'{int(amount):064x}'
            gas = self._transfer_gas(acct.address, calldata)
            if gas is None:
                self._send_error(f'{self._log} destination {to_address} refuses transfers — not sending')
                return None

            gas_balance = int(self.chain.eth_rpc('eth_getBalance', [acct.address, 'latest']), 16)
            if gas_balance < gas * max_fee:
                self._send_error(f'Insufficient gas balance: have {gas_balance} wei, need {gas * max_fee}')
                return None

            nonce = int(self.chain.eth_rpc('eth_getTransactionCount', [acct.address, 'pending']), 16)
            signed = Account.sign_transaction(
                {
                    'chainId': self.chain.chain_id,
                    'nonce': nonce,
                    # eth-account refuses a non-checksummed `to` — a lowercase contract override
                    # (env repoint, e2e fake) must not brick every send.
                    'to': to_checksum_address(self.token_contract),
                    'value': 0,
                    'data': '0x' + calldata,
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

            bt.logging.info(f'{self._log} sent {amount} units to {to_address} (tx: {tx_hash}, maxFee: {max_fee})')
            # A quirky null broadcast reply must never persist as a blank tx hash.
            return (tx_hash or expected_txid, 0)
        except Exception as e:
            self._send_error(f'{self._log} send failed: {type(e).__name__}: {e}')
            return None

    def _transfer_gas(self, from_addr: str, calldata: str) -> Optional[int]:
        """Gas limit via eth_estimateGas (+20% headroom); None when the transfer would revert
        (blacklisted party / paused token — don't burn gas discovering it on-chain). Estimator
        trouble falls back to a FiatToken-sized default rather than blocking a payout on noise."""
        params = {'from': from_addr, 'to': self.token_contract, 'data': '0x' + calldata}
        try:
            est = int(self.chain.eth_rpc('eth_estimateGas', [params]), 16)
        except Exception as e:
            return None if getattr(e, 'is_execution_revert', False) else DEFAULT_TOKEN_TRANSFER_GAS
        gas = est + est // 5
        return None if gas > MAX_TOKEN_TRANSFER_GAS else gas

    # --- Delivery gates (F3: blacklist + pause, deliberately no getCode) ---

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        """Reserve-time gate: the issuer can freeze an address (blacklist) or the whole token
        (pause), per the refusal checks the token's row declares — all make delivery impossible
        and none is the miner's fault, so they must bounce the reservation. Fails open on RPC
        trouble. ``from_address`` is unused: FiatToken transfers gate on issuer state, not dest code."""
        try:
            return not self._refused_at(address)
        except Exception:
            return True

    def _refused_at(self, address: str, block: str = 'latest') -> bool:
        """Issuer refusal at ``block``, per the checks the row declares. No checks, no RPC."""
        return any(
            self._eth_call(selector, address if takes_address else '', block)
            for selector, takes_address in self._checks
        )

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Slash gate: issuer refusal (blacklisted dest, or token paused — both make delivery
        impossible through no fault of the miner) — now or sampled across the window since
        ``since_unix`` — is positive evidence; never slash over it.

        The LATEST probe is the load-bearing signal (freezes persist for months, pauses until
        the issuer acts, so a refusal that broke delivery is still visible at slash time): its
        RPC failure raises and the caller defers — a flaky RPC postpones a slash, never
        falsifies one. Historical samples are best-effort (public nodes serve historical
        eth_call only ~a minute deep at 1s blocks; verified live 2026-08-07): a failing sample
        is skipped with a warning rather than deferring every slash on the pair forever."""
        if not self._checks:
            return False
        if self._refused_at(address):
            return True
        tip = int(self.chain.eth_rpc('eth_blockNumber', []), 16)
        # Span is in blocks; seconds_per_block floors to 1 on sub-second chains, so this reaches
        # ~4× fewer wall-seconds than it reads. Fine here — the latest probe above is load-bearing.
        span = min(60, max(0, int(time.time()) - int(since_unix)) // self.chain_def.seconds_per_block)
        for probe in dict.fromkeys(hex(max(0, tip - span // d)) for d in (1, 2)):
            try:
                if self._refused_at(address, probe):
                    return True
            except Exception as e:
                bt.logging.warning(f'{self._log} historical refusal sample at {probe} failed ({e}) — skipping')
        return False

    def cancel_evidence(
        self, address: str, amount: int, tx_hash: Optional[str] = None, from_address: Optional[str] = None
    ) -> Optional[int]:
        """No-fault-cancel evidence: the issuer's own state proves the destination cannot receive —
        a frozen dest or a stopped token — attributed to the DEST, not the miner (a reverted ERC-20
        transfer alone is ambiguous: it could be miner-frozen or miner-under-balance, so we key on
        issuer state, not the tx).

        Reads the surface the ROW declares, exactly as ``_refused_at`` does. Hardcoding one issuer's
        selectors here would revert on every other token and silently yield no evidence — which
        slashes a miner for a destination its issuer froze. Returns CANCEL_REASON_ERC20_BLACKLIST
        for a per-address freeze, _PAUSED for a token-wide stop, else None. None on RPC trouble so
        the caller waits rather than slashing.

        _FEE_ENABLED covers a token whose issuer switched on a transfer fee (PAXG's admin lever):
        the fee shaves every honest delivery's log below the pinned amount, so it's a hub-wide
        no-fault condition, not a miner failure. Probed on ``amount`` (the expected delivery), so a
        fee that floors to 0 for this size never spuriously cancels."""
        try:
            for selector, takes_address in self._checks:
                if self._eth_call(selector, address if takes_address else ''):
                    return CANCEL_REASON_ERC20_BLACKLIST if takes_address else CANCEL_REASON_ERC20_PAUSED
            if self._fee_for(amount) > 0:
                return CANCEL_REASON_ERC20_FEE_ENABLED
        except Exception:
            return None
        return None

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Tx hash of a recent settled token transfer ``from_addr`` → ``to_addr`` of >= ``amount``,
        else None. eth_getLogs is address-indexed (no block walk), and a Transfer log only exists
        on a status-1 receipt, so a hit IS settled. The cursor parks on a failed range so an
        unreadable span is retried, never leapt."""
        head = self.chain.get_current_block_height()
        if head is None:
            return None
        norm = self.chain.normalize_address
        key = (norm(from_addr), norm(to_addr), int(amount))
        floor = max(head - self.SCAN_LOOKBACK_BLOCKS, 0)
        start = max(self.scan_cursors.get(key, floor), floor) + 1
        if start > head:
            return None
        # One bounded chunk per call; the cursor carries the rest into the next pass.
        end = min(head, start + MAX_LOG_SPAN_BLOCKS - 1)
        try:
            logs = self.chain.eth_rpc(
                'eth_getLogs',
                [
                    {
                        'fromBlock': hex(start),
                        'toBlock': hex(end),
                        'address': self.token_contract,
                        'topics': [TRANSFER_TOPIC0, '0x' + _pad_addr(from_addr), '0x' + _pad_addr(to_addr)],
                    }
                ],
            )
        except Exception:
            self._set_cursor(key, start - 1)
            return None
        for log in logs or []:
            try:
                value = int(log.get('data') or '0x0', 16)
            except (TypeError, ValueError):
                continue
            if value >= int(amount) and log.get('transactionHash'):
                self.scan_cursors.pop(key, None)
                return log['transactionHash']
        self._set_cursor(key, end)
        return None
