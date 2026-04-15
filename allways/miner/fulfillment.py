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


def load_timeout_cushion_blocks() -> int:
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
    4. Mark swap as fulfilled on contract (with to_tx_hash, to_amount)
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
        self.timeout_cushion_blocks = load_timeout_cushion_blocks()
        # Chain → miner's own deposit/fulfillment address, populated at
        # startup from this miner's own commitment and refreshed by the
        # miner loop when a new rate is posted. Shared dict so the miner
        # neuron's reload mutates what we read here.
        self.my_addresses: Dict[str, str] = my_addresses if my_addresses is not None else {}
        # swap_id → (to_tx_hash, to_tx_block, marked_fulfilled)
        self.sent: Dict[int, Tuple[str, int, bool]] = {}
        self.sent_cache_path = sent_cache_path
        self.load_sent_cache()

    def load_sent_cache(self):
        """Load persisted send results from disk to prevent double-sends after restart."""
        if not self.sent_cache_path or not self.sent_cache_path.exists():
            return
        try:
            data = json.loads(self.sent_cache_path.read_text())
            for swap_id_str, entry in data.items():
                self.sent[int(swap_id_str)] = (entry[0], entry[1], bool(entry[2]))
            if self.sent:
                bt.logging.info(f'Restored {len(self.sent)} cached send(s) from disk')
        except Exception as e:
            bt.logging.warning(f'Failed to load sent cache: {e}')

    def save_sent_cache(self):
        """Persist send results to disk immediately after any change."""
        if not self.sent_cache_path:
            return
        try:
            self.sent_cache_path.parent.mkdir(parents=True, exist_ok=True)
            data = {str(k): [v[0], v[1], v[2]] for k, v in self.sent.items()}
            tmp = self.sent_cache_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data))
            tmp.rename(self.sent_cache_path)
        except Exception as e:
            bt.logging.error(f'CRITICAL: Failed to persist sent cache: {e}')

    def cleanup_stale_sends(self, active_swap_ids: Set[int]):
        """Remove cached send results for swaps no longer active."""
        stale = [sid for sid in self.sent if sid not in active_swap_ids]
        for sid in stale:
            self.sent.pop(sid)
            bt.logging.debug(f'Cleaned up stale send cache for swap {sid}')
        if stale:
            self.save_sent_cache()

    def verify_swap_safety(self, swap: Swap) -> Optional[Tuple[int, str]]:
        """Verify the swap is safe to fulfill.

        Returns ``(user_receives_amount, miner_from_address)`` or ``None`` if
        the swap isn't safe to fulfill. ``user_receives_amount`` is the
        POST-FEE amount the miner must actually send to the user; it is
        distinct from ``swap.to_amount`` (which at fulfill time gets set to
        this same value, but at initiate time is the full pre-fee amount).
        """
        # Hot-reload the cushion on every call so an operator can tune
        # MINER_TIMEOUT_CUSHION_BLOCKS without restarting the miner.
        self.timeout_cushion_blocks = load_timeout_cushion_blocks()

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
        if not swap.rate or not swap.miner_from_address:
            bt.logging.error(f'Swap {swap.id}: missing rate or miner_from_address on swap struct')
            return None

        _, user_receives_amount = expected_swap_amounts(swap, self.fee_divisor)
        if user_receives_amount == 0:
            bt.logging.error(f'Swap {swap.id}: rate produces 0 user-receives amount after fees')
            return None

        return user_receives_amount, swap.miner_from_address

    def verify_user_sent_funds(self, swap: Swap, miner_from_address: str) -> bool:
        """Verify that the user sent funds on the source chain."""
        provider = self.providers.get(swap.from_chain)
        if not provider:
            bt.logging.error(f'No provider for chain: {swap.from_chain}')
            return False

        if not swap.from_tx_hash:
            bt.logging.warning(f'Swap {swap.id}: no source tx hash')
            return False

        try:
            tx_info = provider.verify_transaction(
                tx_hash=swap.from_tx_hash,
                expected_recipient=miner_from_address,
                expected_amount=swap.from_amount,
                block_hint=swap.from_tx_block,
                expected_sender=swap.user_from_address,
                require_confirmed=True,
            )
            if tx_info is None:
                bt.logging.debug(f'Swap {swap.id}: source tx not ready (not found, unconfirmed, or sender mismatch)')
                return False

            bt.logging.info(f'Swap {swap.id}: source funds verified ({tx_info.amount} from {tx_info.sender})')
            return True

        except ProviderUnreachableError as e:
            bt.logging.warning(f'Swap {swap.id}: provider unreachable, will retry: {e}')
            return False
        except Exception as e:
            bt.logging.error(f'Swap {swap.id}: verification error: {e}')
            return False

    def send_dest_funds(self, swap: Swap, user_receives_amount: int) -> Optional[Tuple[str, int]]:
        """Send the post-fee amount to the user. Returns (tx_hash, block_number) or None."""
        provider = self.providers.get(swap.to_chain)
        if not provider:
            bt.logging.error(f'Swap {swap.id}: no provider for dest chain: {swap.to_chain}')
            return None

        # Miner's own dest-chain sending address — cached from this miner's
        # committed pair at startup (refreshed when the CLI signals a new
        # post). For TAO we don't need a from_address hint because the wallet
        # keypair fully identifies the sender. For non-TAO chains we pass the
        # cached address so the provider can skip UTXO probing. Signing
        # credentials live on the provider itself.
        from_address = None if swap.to_chain == 'tao' else self.my_addresses.get(swap.to_chain)

        result = provider.send_amount(swap.user_to_address, user_receives_amount, from_address=from_address)
        if result:
            tx_hash, block_num = result
            bt.logging.info(
                f'Swap {swap.id}: sent {user_receives_amount} to {swap.user_to_address} '
                f'on {swap.to_chain} (tx: {tx_hash}, block: {block_num})'
            )
        else:
            bt.logging.error(
                f'Swap {swap.id}: failed to send {user_receives_amount} to {swap.user_to_address} '
                f'on {swap.to_chain} — check wallet balance and node connectivity'
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
        state = self.sent.get(swap.id)
        if state is not None and state[2]:
            # mark_fulfilled already succeeded; contract state will catch up.
            return True

        bt.logging.info(f'Processing swap {swap.id}: {swap.from_chain} -> {swap.to_chain}')

        # Step 1: Verify swap safety (timeout, rate, collateral)
        safety_result = self.verify_swap_safety(swap)
        if safety_result is None:
            bt.logging.warning(f'Swap {swap.id}: failed safety checks, skipping')
            return False

        user_receives_amount, my_source_address = safety_result

        # Step 2: Verify user sent source funds
        if not self.verify_user_sent_funds(swap, my_source_address):
            bt.logging.debug(f'Swap {swap.id}: waiting for source funds confirmation')
            return False

        # Step 3: Send destination funds. The ``sent`` cache serves two
        # purposes: skip the send on a retry (dest tx already broadcast) and
        # skip the mark_fulfilled call on a retry (contract already accepted).
        if state is not None:
            to_tx_hash, to_tx_block, _ = state
            bt.logging.info(f'Swap {swap.id}: retrying mark_fulfilled for cached send tx {to_tx_hash[:16]}...')
        else:
            send_result = self.send_dest_funds(swap, user_receives_amount)
            if not send_result:
                bt.logging.error(f'Swap {swap.id}: failed to send dest funds')
                return False
            to_tx_hash, to_tx_block = send_result
            self.sent[swap.id] = (to_tx_hash, to_tx_block, False)
            self.save_sent_cache()

        # Step 4: Mark fulfilled on contract. We pass user_receives_amount as
        # to_amount because at mark_fulfilled time the contract expects the
        # actual sent amount (post-fee), which is what `swap.to_amount` will
        # be set to after the call.
        try:
            self.client.mark_fulfilled(
                wallet=self.wallet,
                swap_id=swap.id,
                to_tx_hash=to_tx_hash,
                to_amount=user_receives_amount,
                to_tx_block=to_tx_block,
            )
            self.sent[swap.id] = (to_tx_hash, to_tx_block, True)
            self.save_sent_cache()
            bt.logging.success(f'Swap {swap.id}: marked as fulfilled')
            return True
        except ContractError as e:
            bt.logging.error(f'Swap {swap.id}: failed to mark fulfilled on contract: {e}')
            return False
