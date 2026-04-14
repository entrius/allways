"""Swap fulfillment engine - verifies receipt and sends funds."""

import json
import os
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import bittensor as bt

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError
from allways.classes import Swap
from allways.constants import DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS
from allways.contract_client import AllwaysContractClient, ContractError
from allways.utils.rate import expected_swap_amounts


def _load_timeout_cushion_blocks() -> int:
    """Read MINER_TIMEOUT_CUSHION_BLOCKS from env, falling back to the default.

    Values <0 are treated as 0 so operators can't accidentally disable the
    safety margin by sign-flip typos.
    """
    raw = os.environ.get('MINER_TIMEOUT_CUSHION_BLOCKS')
    if raw is None or raw == '':
        return DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS
    try:
        return max(0, int(raw))
    except ValueError:
        bt.logging.warning(
            f'Invalid MINER_TIMEOUT_CUSHION_BLOCKS={raw!r}, using default {DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS}'
        )
        return DEFAULT_MINER_TIMEOUT_CUSHION_BLOCKS


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
        my_addresses: Optional[Dict[str, str]] = None,
    ):
        self.client = contract_client
        self.providers = chain_providers
        self.wallet = wallet
        self.subtensor = subtensor
        self.netuid = netuid
        self.metagraph = metagraph
        self.fee_divisor = fee_divisor
        self.timeout_cushion_blocks = _load_timeout_cushion_blocks()
        # Chain → miner's own deposit/fulfillment address, populated at
        # startup from this miner's own commitment and refreshed by the
        # miner loop when a new rate is posted. Shared dict so the miner
        # neuron's reload mutates what we read here.
        self.my_addresses: Dict[str, str] = my_addresses if my_addresses is not None else {}
        # swap_id → (dest_tx_hash, dest_tx_block, marked_fulfilled)
        self._sent: Dict[int, Tuple[str, int, bool]] = {}
        self._sent_cache_path = sent_cache_path
        self._load_sent_cache()

    def _load_sent_cache(self):
        """Load persisted send results from disk to prevent double-sends after restart."""
        if not self._sent_cache_path or not self._sent_cache_path.exists():
            return
        try:
            data = json.loads(self._sent_cache_path.read_text())
            for swap_id_str, entry in data.items():
                # Back-compat: old cache entries were 2-tuples. Treat restored
                # entries as not-yet-marked-fulfilled so the retry path runs.
                marked = bool(entry[2]) if len(entry) >= 3 else False
                self._sent[int(swap_id_str)] = (entry[0], entry[1], marked)
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
            data = {str(k): [v[0], v[1], v[2]] for k, v in self._sent.items()}
            tmp = self._sent_cache_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data))
            tmp.rename(self._sent_cache_path)
        except Exception as e:
            bt.logging.error(f'CRITICAL: Failed to persist sent cache: {e}')

    def cleanup_stale_sends(self, active_swap_ids: Set[int]):
        """Remove cached send results for swaps no longer active."""
        stale = [sid for sid in self._sent if sid not in active_swap_ids]
        for sid in stale:
            self._sent.pop(sid)
            bt.logging.debug(f'Cleaned up stale send cache for swap {sid}')
        if stale:
            self._save_sent_cache()

    def _verify_swap_safety(self, swap: Swap) -> Optional[Tuple[int, str]]:
        """Verify the swap is safe to fulfill. Returns (dest_amount, miner_source_address) or None."""
        # Timeout check — bail out `timeout_cushion_blocks` before the hard
        # deadline so slow dest-chain inclusion can't turn a legitimate
        # fulfillment into a timeout and a slash.
        current_block = self.subtensor.get_current_block()
        effective_deadline = swap.timeout_block - self.timeout_cushion_blocks
        if current_block >= effective_deadline:
            bt.logging.warning(
                f'Swap {swap.id}: inside cushion window '
                f'(block {current_block} >= {swap.timeout_block} - {self.timeout_cushion_blocks})'
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

            # Miner self-protection: don't send dest funds unless the source tx
            # actually came from the user address tied to this swap. Validators
            # check this too at initiation, but the miner shouldn't trust that
            # alone — an exploited or buggy validator quorum shouldn't cost the
            # miner their send.
            if tx_info.sender and tx_info.sender != swap.user_source_address:
                bt.logging.warning(
                    f'Swap {swap.id}: source tx sender mismatch '
                    f'(expected {swap.user_source_address}, got {tx_info.sender}) — refusing to fulfill'
                )
                return False

            bt.logging.info(f'Swap {swap.id}: source funds verified ({tx_info.amount} from {tx_info.sender})')
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

        # Miner's own dest-chain sending address — cached from this miner's
        # committed pair at startup (refreshed when the CLI signals a new
        # post). For TAO we don't need a from_address hint because the wallet
        # keypair fully identifies the sender. For non-TAO chains we pass the
        # cached address so the provider can skip UTXO probing.
        from_address = None if swap.dest_chain == 'tao' else self.my_addresses.get(swap.dest_chain)

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

        Idempotent across forward steps. The ``_sent`` cache records both the
        dest-tx outcome and whether ``mark_fulfilled`` has already succeeded, so
        retry polls don't re-send dest funds and don't re-call the contract.
        Cache entries live until ``cleanup_stale_sends`` clears them when the
        swap leaves the active set.
        """
        state = self._sent.get(swap.id)
        if state is not None and state[2]:
            # mark_fulfilled already succeeded; contract state will catch up.
            return True

        bt.logging.info(f'Processing swap {swap.id}: {swap.source_chain} -> {swap.dest_chain}')

        # Step 1: Verify swap safety (timeout, rate, collateral)
        safety_result = self._verify_swap_safety(swap)
        if safety_result is None:
            bt.logging.warning(f'Swap {swap.id}: failed safety checks, skipping')
            return False

        dest_amount, my_source_address = safety_result

        # Step 2: Verify user sent source funds
        if not self.verify_user_sent_funds(swap, my_source_address):
            bt.logging.debug(f'Swap {swap.id}: waiting for source funds confirmation')
            return False

        # Step 3: Send destination funds (with double-send prevention)
        if state is not None:
            dest_tx_hash, dest_tx_block, _ = state
            bt.logging.info(f'Swap {swap.id}: retrying mark_fulfilled for cached send tx {dest_tx_hash[:16]}...')
        else:
            send_result = self.send_dest_funds(swap, dest_amount)
            if not send_result:
                bt.logging.error(f'Swap {swap.id}: failed to send dest funds')
                return False
            dest_tx_hash, dest_tx_block = send_result
            self._sent[swap.id] = (dest_tx_hash, dest_tx_block, False)
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
            self._sent[swap.id] = (dest_tx_hash, dest_tx_block, True)
            self._save_sent_cache()
            bt.logging.success(f'Swap {swap.id}: marked as fulfilled')
            return True
        except ContractError as e:
            bt.logging.error(f'Swap {swap.id}: failed to mark fulfilled on contract: {e}')
            return False
