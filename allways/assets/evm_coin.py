import time
from typing import Optional

import bittensor as bt
from eth_utils import to_checksum_address

from allways.assets.asset import warn_once
from allways.assets.evm import EVM_NETWORKS, EvmAsset, EvmChain, EvmRpcError, Match
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
# Values the address-only probe (address_refused) simulates receiving from address(0). A refusal must
# hold at dust AND at a swap-sized value, so a receiver that rejects dust but takes a real leg is not barred.
DUST_PROBE_WEI = 1
# 0.001 native: 20x CHAIN_ETH's rate floor, so inside any real leg, yet small enough that address(0)'s
# burned balance covers it on the chains we run (and a shortfall is a non-revert error: fails open).
SWAP_PROBE_WEI = 10**15
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

    _MATCHES_ON_TX = True

    def _tx_match(self, tx_hash: str, tx: dict, recipient: str, amount: int) -> Optional[Match]:
        to = self.chain.normalize_address(tx.get('to') or '')  # null for contract creation
        value = int(tx.get('value') or '0x0', 16)
        if to != self.chain.normalize_address(recipient) or value < amount:
            warn_once(
                f'{self._log} tx {tx_hash[:16]}... does not pay {recipient} >= {amount} '
                f'{self.chain_def.native_unit} '
                f'(to={tx.get("to")}, value={value})'
            )
            return None
        return self.chain.normalize_address(tx.get('from') or ''), value

    def get_balance(self, address: str) -> int:
        """Balance in the chain's native unit at latest, 0 on failure (mirrors the other providers)."""
        try:
            return int(self.chain.eth_rpc('eth_getBalance', [address, 'latest']), 16)
        except Exception as e:
            bt.logging.error(f'{self._log} get_balance failed for {address}: {e}')
            return 0

    # --- Sending ---

    def _transfer_fields(self, from_addr: str, to_address: str, amount: int, max_fee: int) -> Optional[dict]:
        gas = self._transfer_gas(from_addr, to_address, amount)
        if gas is None:
            # The destination refuses a well-formed transfer (revert) or wants more gas than the cap.
            # Do NOT bail silently — broadcast a floor-gas attempt so the on-chain reverted tx becomes
            # the evidence that earns a no-fault cancel (no slash) rather than a false timeout slash.
            # The revert refunds unused gas, so the miner's real cost is intrinsic + execution, bounded
            # by ETH_FULFILL_GAS_FLOOR. The miner marks fulfilled with this hash like any delivery; the
            # validator adjudicates delivered-vs-refused from the receipt.
            gas = ETH_FULFILL_GAS_FLOOR
            bt.logging.warning(
                f'{to_address} refuses {self.chain_def.env_prefix} transfers — broadcasting a floor-gas attempt as '
                'refusal evidence'
            )
        else:
            # Every send carries the floor so any failed receipt is adjudicable; unused gas is refunded.
            gas = max(gas, ETH_FULFILL_GAS_FLOOR)

        balance = self.get_balance(from_addr)
        if balance < amount + gas * max_fee:
            self._send_error(
                f'Insufficient {self.chain_def.env_prefix}: have {balance} {self.chain_def.native_unit}, '
                f'need {amount} + {gas * max_fee} gas'
            )
            return None
        # eth-account refuses a non-checksummed `to`, but an ALL-LOWERCASE committed dest is legal
        # (EIP-55 is optional) — checksum here or the send crashes and the miner rides to a slash on
        # an address it could never pay.
        return {'to': to_checksum_address(to_address), 'value': amount, 'gas': gas}

    def _transfer_gas(self, from_addr: str, to_addr: str, amount: int) -> Optional[int]:
        """Gas limit via eth_estimateGas (+20% headroom for smart-wallet receive hooks); None
        when the destination refuses or wants absurd gas. Estimator trouble falls back to the
        plain-transfer minimum rather than blocking an EOA payout."""
        try:
            est = self._estimate_transfer(from_addr, to_addr, amount)
        except Exception as e:
            # Structured on-chain verdict, not message-substring (which misses a code-3 revert whose
            # text omits the word "revert" → a doomed tx would broadcast). Mirrors the ERC-20 twin.
            return None if getattr(e, 'is_execution_revert', False) else TRANSFER_GAS
        gas = est + est // 5
        return None if gas > MAX_TRANSFER_GAS else gas

    def _estimate_transfer(self, from_addr: str, to_addr: str, amount: int) -> int:
        """The chain's gas estimate for the plain transfer; raises as the RPC does."""
        return int(
            self.chain.eth_rpc('eth_estimateGas', [{'from': from_addr, 'to': to_addr, 'value': hex(amount)}]), 16
        )

    def _has_code(self, address: str) -> bool:
        return (self.chain.eth_rpc('eth_getCode', [address, 'latest']) or '0x') != '0x'

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        """Reserve-time gate: an EOA can never refuse a transfer; a code-bearing dest must pass
        the same simulation the miner's send runs — from the committed sender when known, with
        the +20% headroom and MAX_TRANSFER_GAS cap. A dest the send path would refuse (revert
        only for msg.sender != 0, or gas just under the cap from zero) must bounce here, not
        ride the miner to a timeout slash. Fails open on RPC trouble."""
        try:
            if not self._has_code(address):
                return True
        except Exception:
            return True
        return self._transfer_gas(from_address or ZERO_ADDRESS, address, amount) is not None

    def address_refused(self, address: str) -> bool:
        """Code at ``address`` whose receive from address(0) reverts at a swap-sized value AND at dust.
        An EOA is never refused (no simulation). Only a structured revert refuses: a gas figure of any
        size or a shortfall is not this address's fault — the send path's gas cap is the reserve gate's
        concern. Raises when the chain cannot be read."""
        return self._has_code(address) and all(
            self._transfer_reverts(ZERO_ADDRESS, address, value) for value in (SWAP_PROBE_WEI, DUST_PROBE_WEI)
        )

    def _transfer_reverts(self, from_addr: str, to_addr: str, amount: int) -> bool:
        """Whether simulating the plain transfer yields a structured revert; any other endpoint answer
        is not one. A transport failure is no answer, so it raises."""
        try:
            self._estimate_transfer(from_addr, to_addr, amount)
        except EvmRpcError as e:
            return e.is_execution_revert
        return False

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
        self,
        address: str,
        amount: int,
        tx_hash: Optional[str] = None,
        from_address: Optional[str] = None,
        block_hint: int = 0,
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
        of that proof; raises on RPC trouble (unknown — the caller then keeps waiting, never slashes)."""
        if not tx_hash:
            return None
        tx = self.chain.eth_rpc('eth_getTransactionByHash', [tx_hash], null_needs_quorum=True)
        receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [tx_hash]) if tx else None
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
        if (tx.get('input') or '0x') != '0x' or tx.get('accessList'):
            return None  # calldata/access-list intrinsic gas can manufacture OOG
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
