"""Finds this miner's swaps: pushed by the program feed (SwapInitiated) and followed by point read, with a
MinerState-gated getProgramAccounts snapshot as the catch-up whenever push can't be trusted. Whether a swap
was already handled is ``SwapFulfiller``'s send cache — this poller reports raw on-chain state only."""

import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt

from allways.solana.client import SolanaSwap, _as_pubkey, swap_from_solana
from allways.utils.logging import log_on_change

# Statuses the miner acts on: ACTIVE needs fulfillment; FULFILLED is awaiting validator confirm but is
# still reported so the fulfiller retains its send-cache entry until the swap closes on-chain.
ACTIVE_STATUSES = ('Active', 'Fulfilled')
# A pushed swap_key that no point read can find yet (a lagging node) is retried this long, then synced.
ANNOUNCE_GRACE_SECS = 60.0
# A followed swap must read absent this many passes in a row before it counts as closed.
VANISH_MISSES = 2


class SwapPoller:
    """Reports this miner's live swaps as (active, fulfilled) each pass."""

    def __init__(self, solana_client, miner_pubkey, feed=None, wake: Optional[Callable[[], None]] = None):
        self.client = solana_client
        self.miner_pubkey = _as_pubkey(miner_pubkey)
        self.known: Set[str] = set()  # swap_key hexes of this miner's live swaps, followed by point read
        self.last_poll_ok: bool = True
        self._counters = None  # (successful_swaps, failed_swaps) baseline for naming terminal outcomes
        self.feed = feed
        self.wake = wake
        self._announced: Dict[str, float] = {}  # pushed swap_key hex -> first-seen monotonic time
        self._announced_lock = threading.Lock()
        self._misses: Dict[str, int] = {}
        self._synced_session: Optional[int] = None
        self._resync = True
        if feed is not None:
            feed.on('SwapInitiated', self.on_swap_initiated)

    # ── push ────────────────────────────────────────────────────────────────

    def on_swap_initiated(self, _name: str, event) -> None:
        """Feed-thread handler: remember our new swap and wake the loop to fulfill it now."""
        if _as_pubkey(event.miner) != self.miner_pubkey:
            return
        key_hex = bytes(event.swap_key).hex()
        with self._announced_lock:
            self._announced.setdefault(key_hex, time.monotonic())
        bt.logging.info(f'Swap {key_hex[:16]}: initiated (pushed)')
        if self.wake is not None:
            self.wake()

    # ── poll ────────────────────────────────────────────────────────────────

    def poll(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        """Returns (active, fulfilled) for this miner. On RPC failure returns ([], []) with
        ``last_poll_ok`` False so the caller skips send-cache cleanup against an empty set."""
        try:
            result = self.poll_inner()
            self.last_poll_ok = True
            return result
        except Exception as e:
            bt.logging.error(f'SwapPoller poll error: {type(e).__name__}: {e}')
            self.last_poll_ok = False
            self._resync = True
            return [], []

    def poll_inner(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        if self.needs_sync():
            return self.sync()
        return self.follow()

    def needs_sync(self) -> bool:
        if self.feed is None:
            return True
        live = self.feed.connected
        log_on_change(
            'swap_poller:feed',
            live,
            'Program feed live — following swaps by push'
            if live
            else 'Program feed down — syncing from chain every pass until it reconnects',
        )
        return not live or self.feed.session != self._synced_session or self._resync

    def sync(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        """Authoritative catch-up. `has_active_swap` is set in the same instruction that makes a swap Active,
        so with it clear and nothing followed here there is nothing to find — skip the snapshot."""
        session = self.feed.session if self.feed is not None else None
        try:
            ms = self.client.get_miner_state(self.miner_pubkey)
        except Exception as e:
            bt.logging.debug(f'SwapPoller: MinerState read failed ({e}); taking the snapshot')
            ms = None
        if ms is not None and not ms.has_active_swap and not self.known and not self._pending():
            active, fulfilled = [], []
        else:
            rows = self.client.get_swaps()
            active = self._mine(rows, 'Active')
            fulfilled = self._mine(rows, 'Fulfilled')
        live = {s.key_hex for s in active} | {s.key_hex for s in fulfilled}
        self._settle(live)
        with self._announced_lock:
            for k in live:
                self._announced.pop(k, None)
        self._misses.clear()
        self._synced_session = session
        self._resync = False
        return active, fulfilled

    def follow(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        """Push mode: point-read each followed or pushed swap. No keys → no calls."""
        now = time.monotonic()
        pending = self._pending()
        active: List[SolanaSwap] = []
        fulfilled: List[SolanaSwap] = []
        live: Set[str] = set()
        for key_hex in self.known | set(pending):
            acct = self.client.get_swap(key_hex)
            status = type(acct.status).__name__ if acct is not None else None
            if status in ACTIVE_STATUSES and _as_pubkey(acct.miner) == self.miner_pubkey:
                swap = self._adopt(swap_from_solana(acct))
                (active if status == 'Active' else fulfilled).append(swap)
                live.add(key_hex)
                self._misses.pop(key_hex, None)
                with self._announced_lock:
                    self._announced.pop(key_hex, None)
            elif key_hex in self.known:
                self._misses[key_hex] = self._misses.get(key_hex, 0) + 1
                if self._misses[key_hex] < VANISH_MISSES:
                    live.add(key_hex)  # one miss can be a lagging node; keep following it
                else:
                    self._misses.pop(key_hex, None)
                    self._resync = True  # confirm the close against the snapshot next pass
            elif now - pending[key_hex] > ANNOUNCE_GRACE_SECS:
                with self._announced_lock:
                    self._announced.pop(key_hex, None)
                self._resync = True
        self._settle(live)
        return active, fulfilled

    # ── helpers ─────────────────────────────────────────────────────────────

    def _pending(self) -> Dict[str, float]:
        with self._announced_lock:
            return dict(self._announced)

    def _adopt(self, swap: SolanaSwap) -> SolanaSwap:
        if swap.key_hex not in self.known:
            bt.logging.info(
                f'Discovered swap {swap.key_hex[:16]}: {swap.from_chain} -> {swap.to_chain}, '
                f'collateral_amount={swap.collateral_amount}, status={swap.status}'
            )
        return swap

    def _mine(self, rows, status: str) -> List[SolanaSwap]:
        out = []
        for _pubkey, acct in rows:
            if type(acct.status).__name__ != status:
                continue
            if _as_pubkey(acct.miner) != self.miner_pubkey:
                continue
            out.append(self._adopt(swap_from_solana(acct)))
        return out

    def _settle(self, live: Set[str]) -> None:
        """Forget keys no longer live (so a reused-tx swap re-logs) and name how each one ended."""
        gone = self.known - live
        if gone or self._counters is None:
            self._log_terminal(gone)
        self.known = set(live)

    def _read_counters(self):
        ms = self.client.get_miner_state(self.miner_pubkey)
        return (int(ms.successful_swaps), int(ms.failed_swaps))

    def _log_terminal(self, gone: Set[str]) -> None:
        """Name each closed swap's terminal outcome — paid vs slashed — from the on-chain lifetime
        counters, so the miner's own log tells the whole story (the Swap account is already gone).
        Counters only move on closures, so the delta since the last baseline attributes exactly."""
        prev = self._counters
        try:
            self._counters = self._read_counters()
        except Exception as e:
            for g in gone:
                bt.logging.warning(f'Swap {g[:16]}: resolved (Completed or TimedOut — outcome read failed: {e})')
            return
        if not gone:
            return  # first poll: baseline seeded
        if prev is None:
            for g in gone:
                bt.logging.info(f'Swap {g[:16]}: left active set — resolved (Completed or TimedOut)')
            return
        ok, failed = self._counters[0] - prev[0], self._counters[1] - prev[1]
        if len(gone) == 1 and ok + failed == 1:
            g = next(iter(gone))
            if ok:
                bt.logging.success(f'Swap {g[:16]}: COMPLETED — paid out (successful_swaps={self._counters[0]})')
            else:
                bt.logging.error(f'Swap {g[:16]}: TIMED OUT — collateral slashed (failed_swaps={self._counters[1]})')
            return
        bt.logging.info(
            f'{len(gone)} swap(s) resolved: +{ok} completed, +{failed} timed out '
            f'(lifetime {self._counters[0]} ok / {self._counters[1]} failed)'
        )
