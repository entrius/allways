"""Swap fulfillment engine - verifies receipt and sends funds (Solana-sourced).

Fee model = Option A: the miner delivers **99% of the pinned ``to_amount``** (``apply_fee_deduction``);
the protocol's 1% is skimmed from the miner's SOL collateral by ``confirm_swap``. The validator's swap
loop checks the dest leg delivered exactly this 99%, so the miner MUST match it. The
fee saved on the dest leg offsets the collateral skim → the user bears the fee, the miner is a
pass-through. ``mark_fulfilled`` records only the dest tx hash/block (``to_amount`` is pinned on-chain).
Deadlines are unix-seconds (``Swap.timeout_at``).
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import bittensor as bt

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError
from allways.constants import FEE_DIVISOR, MINER_TIMEOUT_CUSHION_SECS, SENT_CACHE_DISCARD_MARGIN_SECS
from allways.solana.client import SolanaClientError, SolanaSwap
from allways.utils.logging import log_on_change
from allways.utils.rate import apply_fee_deduction


@dataclass
class SentSwap:
    """Persistent record of a destination-chain send for a single swap (keyed by ``swap_key`` hex).

    Created when ``send_dest_funds`` succeeds; ``marked_fulfilled`` flips to True after the program
    accepts ``mark_fulfilled``. A retry after crash finds this record, skips re-sending (prevents
    double-sends), and only re-calls mark_fulfilled if it didn't already succeed.

    ``timeout_at`` is the swap's last-known (possibly extended) unix deadline, snapshotted so
    ``cleanup_stale_sends`` can bound how long an unmarked entry is retained. 0 means unknown.
    """

    to_tx_hash: str
    to_tx_block: int
    marked_fulfilled: bool
    timeout_at: int = 0


class SwapFulfiller:
    """Handles the miner's side of swap fulfillment.

    1. Verify swap safety (timeout cushion, dest amount, source address)
    2. Verify user sent source funds
    3. Send destination funds to user (full pinned ``to_amount``)
    4. Mark swap as fulfilled on-chain (dest tx hash/block)
    """

    def __init__(
        self,
        solana_client,
        chain_providers: Dict[str, ChainProvider],
        sent_cache_path: Optional[Path] = None,
        my_addresses: Optional[Dict[str, str]] = None,
        fee_divisor: int = FEE_DIVISOR,
    ):
        self.client = solana_client
        self.providers = chain_providers
        self.fee_divisor = fee_divisor
        # Chain → miner's own deposit/fulfillment address, populated at startup from this miner's own
        # quotes and refreshed by the miner loop when a new quote is posted. Shared dict so the miner
        # neuron's reload mutates what we read here.
        self.my_addresses: Dict[str, str] = my_addresses if my_addresses is not None else {}
        self.sent: Dict[str, SentSwap] = {}
        self.mark_fulfilled_attempts: Dict[str, int] = {}
        self.cushion_warned: Set[str] = set()
        self.unmarked_stale_warned: Set[str] = set()
        self.sent_cache_path = sent_cache_path
        self.load_sent_cache()

    def load_sent_cache(self):
        """Load persisted send results from disk to prevent double-sends after restart."""
        if not self.sent_cache_path or not self.sent_cache_path.exists():
            return
        try:
            data = json.loads(self.sent_cache_path.read_text())
            for key_hex, entry in data.items():
                self.sent[key_hex] = SentSwap(
                    to_tx_hash=entry[0],
                    to_tx_block=entry[1],
                    marked_fulfilled=bool(entry[2]),
                    timeout_at=entry[3] if len(entry) > 3 else 0,
                )
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
            data = {
                key_hex: [s.to_tx_hash, s.to_tx_block, s.marked_fulfilled, s.timeout_at]
                for key_hex, s in self.sent.items()
            }
            tmp = self.sent_cache_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data))
            tmp.rename(self.sent_cache_path)
        except Exception as e:
            bt.logging.error(f'CRITICAL: Failed to persist sent cache: {e}')

    def cleanup_stale_sends(self, active_swap_keys: Set[str]):
        """Drop cached send results that are safe to forget.

        An unmarked send (dest funds out, ``mark_fulfilled`` not yet landed) must be retained:
        dropping it would let a rediscovered swap send funds a second time. We keep unmarked entries
        until either they're marked fulfilled, or the clock is provably past their last-known deadline
        (``SENT_CACHE_DISCARD_MARGIN_SECS`` beyond ``timeout_at``), at which point the swap can't still
        be active and retention only leaks.
        """
        stale = [k for k in self.sent if k not in active_swap_keys]
        removable = [k for k in stale if self.sent[k].marked_fulfilled]
        unmarked_stale = [k for k in stale if not self.sent[k].marked_fulfilled]

        now = int(time.time())
        expired = [
            k
            for k in unmarked_stale
            if self.sent[k].timeout_at > 0 and now > self.sent[k].timeout_at + SENT_CACHE_DISCARD_MARGIN_SECS
        ]

        for k in removable + expired:
            self.sent.pop(k)
            self.mark_fulfilled_attempts.pop(k, None)
            self.unmarked_stale_warned.discard(k)
        self.cushion_warned &= active_swap_keys
        self.unmarked_stale_warned -= active_swap_keys

        if removable or expired:
            self.save_sent_cache()
        if removable:
            bt.logging.info(f'Cleaned up stale send cache for {len(removable)} marked swap(s): {removable}')
        if expired:
            bt.logging.warning(
                f'Discarded stale send(s) past deadline without confirmed mark_fulfilled — funds may have '
                f'been sent without on-chain credit: {expired}'
            )

        retained = [k for k in unmarked_stale if k not in expired]
        newly_retained = [k for k in retained if k not in self.unmarked_stale_warned]
        if newly_retained:
            bt.logging.warning(
                f'Retaining unmarked send(s) to avoid duplicate destination sends if the swap reappears: '
                f'{newly_retained}'
            )
            self.unmarked_stale_warned.update(newly_retained)

    def verify_swap_safety(self, swap: SolanaSwap) -> Optional[Tuple[int, str]]:
        """Verify the swap is safe to fulfill.

        Returns ``(user_receives_amount, miner_from_address)`` or ``None`` if the swap isn't safe to
        fulfill. ``user_receives_amount`` is 99% of the pinned ``swap.to_amount`` (Option A — the 1% the
        validator expects withheld here; the protocol then skims 1% of collateral at confirm).
        """
        # Timeout check — bail out MINER_TIMEOUT_CUSHION_SECS before the hard deadline so slow
        # dest-chain inclusion can't turn a legitimate fulfillment into a timeout and a slash. Sized to
        # match the validator's extension runway: starting a fulfill inside this window leaves no rescue.
        now = int(time.time())
        effective_deadline = swap.timeout_at - MINER_TIMEOUT_CUSHION_SECS
        if now >= effective_deadline:
            if swap.key_hex not in self.cushion_warned:
                bt.logging.warning(
                    f'Swap {swap.key_hex[:16]}: inside cushion window '
                    f'(now {now} >= {swap.timeout_at} - {MINER_TIMEOUT_CUSHION_SECS})'
                )
                self.cushion_warned.add(swap.key_hex)
            return None

        if not swap.miner_from_addr:
            bt.logging.error(f'Swap {swap.key_hex[:16]}: missing miner_from_addr on swap')
            return None

        user_receives = apply_fee_deduction(swap.to_amount, self.fee_divisor)
        if user_receives == 0:
            bt.logging.error(f'Swap {swap.key_hex[:16]}: pinned to_amount yields 0 after the fee haircut')
            return None

        return user_receives, swap.miner_from_addr

    def verify_user_sent_funds(self, swap: SolanaSwap, miner_from_address: str) -> bool:
        """Verify that the user sent funds on the source chain."""
        provider = self.providers.get(swap.from_chain)
        if not provider:
            bt.logging.error(f'No provider for chain: {swap.from_chain}')
            return False

        if not swap.from_tx_hash:
            bt.logging.warning(f'Swap {swap.key_hex[:16]}: no source tx hash')
            return False

        try:
            tx_info = provider.verify_transaction(
                tx_hash=swap.from_tx_hash,
                expected_recipient=miner_from_address,
                expected_amount=swap.from_amount,
                block_hint=swap.from_tx_block,
                expected_sender=swap.user_from_addr,
                require_confirmed=True,
            )
            if tx_info is None:
                log_on_change(
                    f'src_waiting:{swap.key_hex}',
                    True,
                    f'Swap {swap.key_hex[:16]}: source tx not yet ready, will retry',
                )
                return False

            bt.logging.info(f'Swap {swap.key_hex[:16]}: source funds verified ({tx_info.amount} from {tx_info.sender})')
            return True

        except ProviderUnreachableError as e:
            bt.logging.warning(f'Swap {swap.key_hex[:16]}: provider unreachable, will retry: {e}')
            return False
        except Exception as e:
            bt.logging.error(f'Swap {swap.key_hex[:16]}: verification error: {type(e).__name__}: {e}')
            return False

    def send_dest_funds(self, swap: SolanaSwap, user_receives_amount: int) -> Optional[Tuple[str, int]]:
        """Send the pinned amount to the user. Returns (tx_hash, block_number) or None."""
        provider = self.providers.get(swap.to_chain)
        if not provider:
            bt.logging.error(f'Swap {swap.key_hex[:16]}: no provider for dest chain: {swap.to_chain}')
            return None

        # Miner's own dest-chain sending address — cached from this miner's posted quote at startup, passed
        # as a hint so UTXO-based providers can skip probing. Providers that identify their own sender from
        # a wallet keypair (e.g. subtensor) ignore it, so this is uniform across chains.
        from_address = self.my_addresses.get(swap.to_chain)

        bt.logging.info(
            f'Swap {swap.key_hex[:16]}: initiating dest send of {user_receives_amount} to {swap.user_to_addr} '
            f'on {swap.to_chain}'
        )
        result = provider.send_amount(swap.user_to_addr, user_receives_amount, from_address=from_address)
        if result:
            tx_hash, block_num = result
            bt.logging.info(
                f'Swap {swap.key_hex[:16]}: sent {user_receives_amount} to {swap.user_to_addr} '
                f'on {swap.to_chain} (tx: {tx_hash}, block: {block_num})'
            )
        else:
            reason = getattr(provider, 'last_send_error', None) or 'no provider error captured'
            bt.logging.error(
                f'Swap {swap.key_hex[:16]}: failed to send {user_receives_amount} to {swap.user_to_addr} '
                f'on {swap.to_chain}: {reason}'
            )
        return result

    def process_swap(self, swap: SolanaSwap) -> bool:
        """Run the full swap lifecycle for one assigned swap.

        Idempotent across forward steps — the ``sent`` cache (keyed by ``swap_key`` hex) tracks both the
        dest-tx outcome and whether ``mark_fulfilled`` has landed, so retry polls never double-send and
        never double-call the program. Three possible starting states:
          - no prior record → send dest funds, then mark fulfilled
          - prior send, not yet marked → skip send, retry mark fulfilled
          - prior send, already marked → nothing to do
        """
        key = swap.key_hex
        sent = self.sent.get(key)
        if sent and sent.marked_fulfilled:
            bt.logging.debug(f'Swap {key[:16]}: already marked fulfilled locally, awaiting validator confirm')
            return True

        bt.logging.info(f'Processing swap {key[:16]}: {swap.from_chain} -> {swap.to_chain}')

        if sent is None:
            # First pass — gate the send on safety (timeout cushion, dest amount, source funds).
            safety_result = self.verify_swap_safety(swap)
            if safety_result is None:
                bt.logging.warning(f'Swap {key[:16]}: failed safety checks, skipping')
                return False
            user_receives_amount, my_source_address = safety_result

            if not self.verify_user_sent_funds(swap, my_source_address):
                return False

            send_result = self.send_dest_funds(swap, user_receives_amount)
            if not send_result:
                bt.logging.error(f'Swap {key[:16]}: failed to send dest funds')
                return False
            to_tx_hash, to_tx_block = send_result
            sent = SentSwap(
                to_tx_hash=to_tx_hash,
                to_tx_block=to_tx_block,
                marked_fulfilled=False,
                timeout_at=swap.timeout_at,
            )
            self.sent[key] = sent
            self.save_sent_cache()
        else:
            # Funds are already out — skip the cushion/safety gate (scoped to STARTING a fulfill);
            # retrying mark_fulfilled to the deadline only helps and avoids a timeout slash of a miner
            # that paid (#462). Keep the retained deadline current with any extension seen while active.
            if swap.timeout_at > sent.timeout_at:
                sent.timeout_at = swap.timeout_at
                self.save_sent_cache()
            bt.logging.info(f'Swap {key[:16]}: retrying mark_fulfilled for cached send tx {sent.to_tx_hash[:16]}...')

        # Mark fulfilled on-chain — records only the dest tx hash/block (to_amount is the pinned value).
        try:
            self.client.mark_fulfilled(
                swap_key=swap.swap_key,
                to_tx_hash=sent.to_tx_hash,
                to_tx_block=sent.to_tx_block,
            )
            sent.marked_fulfilled = True
            self.save_sent_cache()
            self.mark_fulfilled_attempts.pop(key, None)
            bt.logging.success(f'Swap {key[:16]}: marked as fulfilled')
            return True
        except SolanaClientError as e:
            attempts = self.mark_fulfilled_attempts.get(key, 0) + 1
            self.mark_fulfilled_attempts[key] = attempts
            secs_to_deadline = swap.timeout_at - int(time.time())
            log = bt.logging.warning if attempts >= 3 else bt.logging.error
            log(f'Swap {key[:16]}: mark_fulfilled failed (attempt {attempts}, {secs_to_deadline}s to deadline): {e}')
            return False
