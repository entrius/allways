import os
import time
from typing import Dict, Hashable, Optional

import bittensor as bt
from eth_utils import keccak, to_checksum_address

from allways.assets.asset import MissingTestnetDeployment
from allways.assets.evm import EVM_NETWORKS, EvmAsset, EvmChain, Match
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
# gas spend against a pathological estimate without pinching the real cost. A first-time
# holder costs far more where new storage is priced up: Sepolia estimates ~154k (×1.2 ≈ 185k).
MAX_TOKEN_TRANSFER_GAS = 200_000
# Floor and estimator-down fallback: covers FiatToken's worst case while staying under the cap.
DEFAULT_TOKEN_TRANSFER_GAS = 120_000


def _token_contract(chain_def: ChainDefinition, network: str) -> str:
    """The token's contract on ``network``: {ASSET_ID}_TOKEN_CONTRACT when set (e2e fakes, emergency repoint),
    else the row's address for that network. Keyed by asset, not by network prefix: one network hosts several
    tokens, and each pins its own contract."""
    var = f'{chain_def.id.upper()}_TOKEN_CONTRACT'
    override = os.environ.get(var)
    if override:
        return override
    if network != 'mainnet':
        contract = chain_def.testnet_locator(network)
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

    def check_connection(
        self, require_send: bool = True, checked: Optional[Dict[Hashable, Optional[Exception]]] = None
    ) -> None:
        # Only the network probe is shared (see EvmAsset); the contract probes below are this token's own.
        super().check_connection(require_send=require_send, checked=checked)
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
            if self._token_stopped():
                bt.logging.warning(f'{self._log} token is STOPPED — transfers fail until the issuer clears it')
        except Exception as e:
            bt.logging.warning(f'{self._log} refusal probe failed: {e}')

    # --- Verification ---

    def _tx_match(self, tx_hash: str, tx: dict, recipient: str, amount: int) -> Optional[Match]:
        """A pending tx: match transfer() calldata addressed to the pinned contract."""
        match = self._pending_transfer(tx, recipient, amount)
        if match is None:
            bt.logging.warning(
                f'{self._log} pending tx {tx_hash[:16]}... is not a transfer paying {recipient} >= {amount}'
            )
        return match

    def _pending_transfer(self, tx: dict, expected_recipient: str, expected_amount: int) -> Optional[Match]:
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

    def _receipt_match(self, tx_hash: str, receipt: dict, recipient: str, amount: int) -> Optional[Match]:
        match = self._transfer_log(receipt, recipient, amount)
        if match is None:
            bt.logging.warning(
                f'{self._log} tx {tx_hash[:16]}... has no Transfer log from {self.token_contract} paying '
                f'{recipient} >= {amount}'
            )
        return match

    def _transfer_log(self, receipt: dict, expected_recipient: str, expected_amount: int) -> Optional[Match]:
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

    def _transfer_fields(self, from_addr: str, to_address: str, amount: int, max_fee: int) -> Optional[dict]:
        """Dual-balance preflight: the token balance covers ``amount`` AND the native balance covers gas — a
        token-rich, gas-poor miner must refuse here, not burn a revert."""
        # Token balance BEFORE the gas estimate: an underfunded transfer reverts in the
        # estimator too, which would misread as the destination refusing.
        token_balance = self.get_balance(from_addr)
        if token_balance < amount:
            self._send_error(f'{self._log} insufficient balance: have {token_balance} units, need {amount}')
            return None

        calldata = SEL_TRANSFER[2:] + _pad_addr(to_address) + f'{int(amount):064x}'
        gas = self._transfer_gas(from_addr, calldata)
        if gas is None:
            self._send_error(f'{self._log} destination {to_address} refuses transfers — not sending')
            return None

        gas_balance = int(self.chain.eth_rpc('eth_getBalance', [from_addr, 'latest']), 16)
        if gas_balance < gas * max_fee:
            self._send_error(f'Insufficient gas balance: have {gas_balance} wei, need {gas * max_fee}')
            return None
        # eth-account refuses a non-checksummed `to` — a lowercase contract override
        # (env repoint, e2e fake) must not brick every send.
        return {'to': to_checksum_address(self.token_contract), 'value': 0, 'data': '0x' + calldata, 'gas': gas}

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
        return None if gas > MAX_TOKEN_TRANSFER_GAS else max(gas, DEFAULT_TOKEN_TRANSFER_GAS)

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

    def address_refused(self, address: str, block: str = 'latest') -> bool:
        """Issuer freeze of THIS address at ``block`` — the declared checks that take an address, never
        the token-wide pause. No such checks, no RPC. Raises on RPC trouble."""
        return any(
            self._eth_call(selector, address, block) for selector, takes_address in self._checks if takes_address
        )

    def _token_stopped(self, block: str = 'latest') -> bool:
        """The whole token paused at ``block`` — the declared nullary checks. No such checks, no RPC."""
        return any(self._eth_call(selector, '', block) for selector, takes_address in self._checks if not takes_address)

    def _refused_at(self, address: str, block: str = 'latest') -> bool:
        """Issuer refusal at ``block``, per the checks the row declares: the address frozen, or the token stopped."""
        return self.address_refused(address, block) or self._token_stopped(block)

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
        self,
        address: str,
        amount: int,
        tx_hash: Optional[str] = None,
        from_address: Optional[str] = None,
        block_hint: int = 0,
    ) -> Optional[int]:
        """No-fault-cancel evidence: the issuer's own state proves the destination cannot receive —
        a frozen dest or a stopped token — attributed to the DEST, not the miner (a reverted ERC-20
        transfer alone is ambiguous: it could be miner-frozen or miner-under-balance, so we key on
        issuer state, not the tx).

        Reads the surface the ROW declares, exactly as ``_refused_at`` does. Hardcoding one issuer's
        selectors here would revert on every other token and silently yield no evidence — which
        slashes a miner for a destination its issuer froze. Returns CANCEL_REASON_ERC20_BLACKLIST
        for a per-address freeze, _PAUSED for a token-wide stop, else None. Raises on RPC trouble:
        an unread check is unknown, and the caller waits rather than slashing.

        _FEE_ENABLED covers a token whose issuer switched on a transfer fee (PAXG's admin lever):
        the fee shaves every honest delivery's log below the pinned amount, so it's a hub-wide
        no-fault condition, not a miner failure. Probed on ``amount`` (the expected delivery), so a
        fee that floors to 0 for this size never spuriously cancels."""
        for selector, takes_address in self._checks:
            if self._eth_call(selector, address if takes_address else ''):
                return CANCEL_REASON_ERC20_BLACKLIST if takes_address else CANCEL_REASON_ERC20_PAUSED
        if self._fee_for(amount) > 0:
            return CANCEL_REASON_ERC20_FEE_ENABLED
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
        try:
            logs = self.chain.eth_rpc(
                'eth_getLogs',
                [
                    {
                        'fromBlock': hex(start),
                        'toBlock': hex(head),
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
        self._set_cursor(key, head)
        return None
