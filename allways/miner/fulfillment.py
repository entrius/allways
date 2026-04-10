"""Swap fulfillment engine - verifies receipt and sends funds."""

import json
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import bittensor as bt

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError
from allways.classes import Swap, SwapStatus
from allways.constants import FULFILLMENT_TIMEOUT_MARGIN_BLOCKS
from allways.contract_client import AllwaysContractClient, ContractError, ContractErrorKind
from allways.utils.rate import expected_swap_amounts


class SwapFulfiller:
    """Handles the miner's side of swap fulfillment.

    1. Verify swap safety (timeout, rate, collateral)
    2. Verify user sent source funds
    3. Send destination funds to user
    4. Mark swap as fulfilled on contract (with dest_tx_hash, dest_amount)
    """

    def __init__(
        self,
        contract_client: AllwaysContractClient,
        chain_providers: Dict[str, ChainProvider],
        wallet: bt.Wallet,
        subtensor: bt.Subtensor,
        netuid: int,
        metagraph: Optional['bt.Metagraph'] = None,
        fee_divisor: int = 100,
        sent_cache_path: Optional[Path] = None,
        timeout_margin_blocks: int = FULFILLMENT_TIMEOUT_MARGIN_BLOCKS,
        recovery_log_path: Optional[Path] = None,
    ):
        self.client = contract_client
        self.providers = chain_providers
        self.wallet = wallet
        self.subtensor = subtensor
        self.netuid = netuid
        self.metagraph = metagraph
        self.fee_divisor = fee_divisor
        self._sent: Dict[int, Tuple[str, int]] = {}
        self._sent_cache_path = sent_cache_path
        self.timeout_margin_blocks = timeout_margin_blocks
        self._recovery_log_path = recovery_log_path
        self._terminal_failures: Set[int] = set()
        self._load_sent_cache()

    @staticmethod
    def _is_terminal_mark_fulfilled_error(error: ContractError) -> bool:
        if error.kind in (
            ContractErrorKind.NOT_INITIALIZED,
            ContractErrorKind.RPC_FAILURE,
            ContractErrorKind.INSUFFICIENT_BALANCE,
        ):
            return False

        msg = str(error)
        terminal_variants = ('SwapNotFound', 'InvalidStatus', 'NotAssignedMiner', 'MinerNotActive')
        return any(v in msg for v in terminal_variants)

    def _append_recovery_entry(self, entry: Dict) -> None:
        if not self._recovery_log_path:
            return

        try:
            self._recovery_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._recovery_log_path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            bt.logging.error(f"Swap {entry.get('swap_id', 'unknown')}: failed to write recovery log: {e}")

    def _append_recovery_log(
        self,
        swap: Swap,
        dest_tx_hash: str,
        dest_tx_block: int,
        dest_amount: int,
        reason: str,
        event: str,
    ) -> None:
        entry = {
            'event': event,
            'swap_id': swap.id,
            'miner_hotkey': swap.miner_hotkey,
            'source_chain': swap.source_chain,
            'dest_chain': swap.dest_chain,
            'source_tx_hash': swap.source_tx_hash,
            'dest_tx_hash': dest_tx_hash,
            'dest_tx_block': dest_tx_block,
            'dest_amount': dest_amount,
            'timeout_block': swap.timeout_block,
            'reason': reason,
        }

        bt.logging.error(
            f"Swap {swap.id}: recovery required ({event}) — sent tx={dest_tx_hash}, "
            f"block={dest_tx_block}, amount={dest_amount}, reason={reason}"
        )
        self._append_recovery_entry(entry)

    def _load_sent_cache(self):
        """Load persisted send results from disk to prevent double-sends after restart."""
        if not self._sent_cache_path or not self._sent_cache_path.exists():
            return
        try:
            data = json.loads(self._sent_cache_path.read_text())
            for swap_id_str, entry in data.items():
                self._sent[int(swap_id_str)] = (entry[0], entry[1])
            if self._sent:
                bt.logging.info(f'Restored {len(self._sent)} cached send(s) from disk')
        except Exception as e:
            bt.logging.warning(f'Failed to load sent cache: {e}')

    def _save_sent_cache(self):
        """Persist send results to disk immediately after any change."""
        if not self._sent_cache_path:
            return
        try:
            self._sent_cache_path.parent.mkdir(parents=True, exist_ok=True)
            data = {str(k): [v[0], v[1]] for k, v in self._sent.items()}
            tmp = self._sent_cache_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data))
            tmp.rename(self._sent_cache_path)
        except Exception as e:
            bt.logging.error(f'CRITICAL: Failed to persist sent cache: {e}')

    def cleanup_stale_sends(self, active_swap_ids: Set[int]):
        """Remove cached send results for swaps no longer active."""
        stale = [sid for sid in self._sent if sid not in active_swap_ids]
        for sid in stale:
            tx_hash, tx_block = self._sent[sid]
            resolved = None
            try:
                resolved = self.client.get_swap(sid)
            except Exception as e:
                bt.logging.debug(f'Swap {sid}: get_swap failed during stale send cleanup: {e}')

            if resolved is not None and resolved.status == SwapStatus.COMPLETED:
                bt.logging.info(
                    f'Swap {sid}: clearing stale send cache after completed resolution '
                    f'(tx={tx_hash}, block={tx_block})'
                )
            elif resolved is not None and resolved.status == SwapStatus.TIMED_OUT:
                bt.logging.error(
                    f'Swap {sid}: swap timed out after funds were sent '
                    f'(tx={tx_hash}, block={tx_block}) — manual recovery may be required'
                )
                self._append_recovery_entry(
                    {
                        'event': 'stale_sent_cache_cleanup',
                        'swap_id': sid,
                        'dest_tx_hash': tx_hash,
                        'dest_tx_block': tx_block,
                        'status': resolved.status.name,
                        'reason': 'swap timed out after send',
                    }
                )
            else:
                status_name = resolved.status.name if resolved else 'UNKNOWN'
                bt.logging.warning(
                    f'Swap {sid}: clearing stale send cache with status={status_name} '
                    f'(tx={tx_hash}, block={tx_block})'
                )
            self._sent.pop(sid)
            self._terminal_failures.discard(sid)
            bt.logging.debug(f'Cleaned up stale send cache for swap {sid}')
        if stale:
            self._save_sent_cache()

    def _verify_swap_safety(self, swap: Swap, enforce_timeout_margin: bool = True) -> Optional[Tuple[int, str]]:
        """Verify the swap is safe to fulfill. Returns (dest_amount, miner_source_address) or None."""
        # Timeout check
        current_block = self.subtensor.get_current_block()
        if swap.timeout_block > 0:
            blocks_left = swap.timeout_block - current_block
            if blocks_left <= 0:
                bt.logging.warning(f'Swap {swap.id}: already timed out (block {current_block} >= {swap.timeout_block})')
                return None
            if enforce_timeout_margin and blocks_left <= self.timeout_margin_blocks:
                bt.logging.warning(
                    f'Swap {swap.id}: too close to timeout to fulfill safely '
                    f'(blocks_left={blocks_left}, margin={self.timeout_margin_blocks})'
                )
                return None

        # Rate and address from swap struct (snapshotted at initiation)
        if not swap.rate or not swap.miner_source_address:
            bt.logging.error(f'Swap {swap.id}: missing rate or miner_source_address on swap struct')
            return None

        _, user_receives = expected_swap_amounts(swap, self.fee_divisor)
        if user_receives == 0:
            bt.logging.error(f'Swap {swap.id}: calculated dest_amount is 0')
            return None

        return user_receives, swap.miner_source_address

    def verify_user_sent_funds(self, swap: Swap, miner_source_address: str) -> bool:
        """Verify that the user sent funds on the source chain."""
        provider = self.providers.get(swap.source_chain)
        if not provider:
            bt.logging.error(f'No provider for chain: {swap.source_chain}')
            return False

        if not swap.source_tx_hash:
            bt.logging.warning(f'Swap {swap.id}: no source tx hash')
            return False

        try:
            tx_info = provider.verify_transaction(
                tx_hash=swap.source_tx_hash,
                expected_recipient=miner_source_address,
                expected_amount=swap.source_amount,
                block_hint=swap.source_tx_block,
            )

            if tx_info is None:
                bt.logging.debug(f'Swap {swap.id}: source tx not found or unconfirmed')
                return False

            if not tx_info.confirmed:
                bt.logging.debug(f'Swap {swap.id}: source tx not yet confirmed')
                return False

            bt.logging.info(f'Swap {swap.id}: source funds verified ({tx_info.amount} to {tx_info.recipient})')
            return True

        except ProviderUnreachableError as e:
            bt.logging.warning(f'Swap {swap.id}: provider unreachable, will retry: {e}')
            return False
        except Exception as e:
            bt.logging.error(f'Swap {swap.id}: verification error: {e}')
            return False

    def send_dest_funds(self, swap: Swap, dest_amount: int) -> Optional[Tuple[str, int]]:
        """Send destination funds to the user. Returns (tx_hash, block_number) or None."""
        provider = self.providers.get(swap.dest_chain)
        if not provider:
            bt.logging.error(f'Swap {swap.id}: no provider for dest chain: {swap.dest_chain}')
            return None

        key = self.wallet if swap.dest_chain == 'tao' else None

        # For non-TAO sends, read the miner's commitment to get the sending address.
        # Commitments are normalized to canonical order, so source_address is
        # always the canonical source chain's address.
        from_address = None
        if swap.dest_chain != 'tao':
            try:
                from allways.commitments import read_miner_commitment

                commitment = read_miner_commitment(
                    subtensor=self.subtensor,
                    netuid=self.netuid,
                    hotkey=swap.miner_hotkey,
                    metagraph=self.metagraph,
                )
                if commitment:
                    from_address = commitment.source_address
                    bt.logging.debug(
                        f'Swap {swap.id}: sending from committed {commitment.source_chain} address {from_address}'
                    )
            except Exception as e:
                bt.logging.warning(f'Swap {swap.id}: could not read commitment for from_address, will probe: {e}')

        result = provider.send_amount(swap.user_dest_address, dest_amount, key=key, from_address=from_address)
        if result:
            tx_hash, block_num = result
            bt.logging.info(
                f'Swap {swap.id}: sent {dest_amount} to {swap.user_dest_address} '
                f'on {swap.dest_chain} (tx: {tx_hash}, block: {block_num})'
            )
        else:
            bt.logging.error(
                f'Swap {swap.id}: failed to send {dest_amount} to {swap.user_dest_address} '
                f'on {swap.dest_chain} — check wallet balance and node connectivity'
            )
        return result

    def process_swap(self, swap: Swap) -> bool:
        """Full swap processing flow: verify safety -> verify funds -> send -> mark fulfilled.

        Returns True if swap was successfully fulfilled.
        """
        bt.logging.info(f'Processing swap {swap.id}: {swap.source_chain} -> {swap.dest_chain}')

        if swap.id in self._terminal_failures:
            bt.logging.warning(f'Swap {swap.id}: skipped after terminal mark_fulfilled failure (manual recovery required)')
            return True

        has_cached_send = swap.id in self._sent

        # Step 1: Verify swap safety (timeout, rate, collateral)
        safety_result = self._verify_swap_safety(swap, enforce_timeout_margin=not has_cached_send)
        if safety_result is None:
            bt.logging.warning(f'Swap {swap.id}: failed safety checks, skipping')
            return False

        dest_amount, my_source_address = safety_result

        # Step 2: Verify user sent source funds
        if not has_cached_send:
            if not self.verify_user_sent_funds(swap, my_source_address):
                bt.logging.debug(f'Swap {swap.id}: waiting for source funds confirmation')
                return False
        else:
            bt.logging.debug(f'Swap {swap.id}: skipping source verification (cached send exists)')

        # Step 3: Send destination funds (with double-send prevention)
        if has_cached_send:
            dest_tx_hash, dest_tx_block = self._sent[swap.id]
            bt.logging.info(f'Swap {swap.id}: using cached send result (tx: {dest_tx_hash}, block: {dest_tx_block})')
        else:
            send_result = self.send_dest_funds(swap, dest_amount)
            if not send_result:
                bt.logging.error(f'Swap {swap.id}: failed to send dest funds')
                return False
            dest_tx_hash, dest_tx_block = send_result
            self._sent[swap.id] = (dest_tx_hash, dest_tx_block)
            self._save_sent_cache()

        # Step 4: Mark fulfilled on contract
        try:
            self.client.mark_fulfilled(
                wallet=self.wallet,
                swap_id=swap.id,
                dest_tx_hash=dest_tx_hash,
                dest_amount=dest_amount,
                dest_tx_block=dest_tx_block,
            )
            bt.logging.success(f'Swap {swap.id}: marked as fulfilled')
            self._sent.pop(swap.id, None)
            self._terminal_failures.discard(swap.id)
            self._save_sent_cache()
            return True
        except ContractError as e:
            if self._is_terminal_mark_fulfilled_error(e):
                self._terminal_failures.add(swap.id)
                self._append_recovery_log(
                    swap=swap,
                    dest_tx_hash=dest_tx_hash,
                    dest_tx_block=dest_tx_block,
                    dest_amount=dest_amount,
                    reason=str(e),
                    event='terminal_mark_fulfilled_failure',
                )
                return True

            bt.logging.error(f'Swap {swap.id}: failed to mark fulfilled on contract: {e}')
            return False
