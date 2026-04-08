"""Persist the scoring window and voted set across validator restarts.

Without persistence, SwapTracker.window starts empty after a restart.
With SCORING_EMA_ALPHA=1.0 (instantaneous scoring), the first scoring
cycle zeros all miner weights because the window contains no completed
swaps.  This module writes the window to a JSON file after every update
and restores it on cold start so scoring is continuous.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import bittensor as bt

from allways.classes import Swap, SwapStatus

# Bump when the on-disk schema changes in a backwards-incompatible way.
_SCHEMA_VERSION = 1


def _swap_to_dict(swap: Swap) -> dict:
    return {
        'id': swap.id,
        'user_hotkey': swap.user_hotkey,
        'miner_hotkey': swap.miner_hotkey,
        'source_chain': swap.source_chain,
        'dest_chain': swap.dest_chain,
        'source_amount': swap.source_amount,
        'dest_amount': swap.dest_amount,
        'tao_amount': swap.tao_amount,
        'user_source_address': swap.user_source_address,
        'user_dest_address': swap.user_dest_address,
        'miner_source_address': swap.miner_source_address,
        'miner_dest_address': swap.miner_dest_address,
        'rate': swap.rate,
        'source_tx_hash': swap.source_tx_hash,
        'source_tx_block': swap.source_tx_block,
        'dest_tx_hash': swap.dest_tx_hash,
        'dest_tx_block': swap.dest_tx_block,
        'status': swap.status.value,
        'initiated_block': swap.initiated_block,
        'timeout_block': swap.timeout_block,
        'fulfilled_block': swap.fulfilled_block,
        'completed_block': swap.completed_block,
    }


def _dict_to_swap(d: dict) -> Optional[Swap]:
    try:
        return Swap(
            id=d['id'],
            user_hotkey=d['user_hotkey'],
            miner_hotkey=d['miner_hotkey'],
            source_chain=d['source_chain'],
            dest_chain=d['dest_chain'],
            source_amount=d['source_amount'],
            dest_amount=d['dest_amount'],
            tao_amount=d['tao_amount'],
            user_source_address=d['user_source_address'],
            user_dest_address=d['user_dest_address'],
            miner_source_address=d.get('miner_source_address', ''),
            miner_dest_address=d.get('miner_dest_address', ''),
            rate=d.get('rate', ''),
            source_tx_hash=d.get('source_tx_hash', ''),
            source_tx_block=d.get('source_tx_block', 0),
            dest_tx_hash=d.get('dest_tx_hash', ''),
            dest_tx_block=d.get('dest_tx_block', 0),
            status=SwapStatus(d['status']),
            initiated_block=d.get('initiated_block', 0),
            timeout_block=d.get('timeout_block', 0),
            fulfilled_block=d.get('fulfilled_block', 0),
            completed_block=d.get('completed_block', 0),
        )
    except (KeyError, ValueError, TypeError) as e:
        bt.logging.debug(f'Failed to restore swap from cache: {e}')
        return None


class ScoringWindowStore:
    """Atomic JSON persistence for the scoring window and voted-id set.

    Uses write-to-tmp-then-rename for crash safety (same pattern as
    SwapFulfiller._save_sent_cache).
    """

    def __init__(self, path: Path):
        self._path = path

    def save(self, window: List[Swap], voted_ids: Set[int]) -> None:
        """Persist current window and voted set to disk."""
        data: Dict = {
            'version': _SCHEMA_VERSION,
            'window': [_swap_to_dict(s) for s in window],
            'voted_ids': sorted(voted_ids),
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix('.tmp')
            tmp.write_text(json.dumps(data))
            # os.replace works cross-platform (unlike Path.rename which
            # fails on Windows when the destination already exists).
            os.replace(tmp, self._path)
        except Exception as e:
            bt.logging.warning(f'Failed to persist scoring window: {e}')

    def load(self, window_blocks: int, current_block: int) -> Tuple[List[Swap], Set[int]]:
        """Restore window and voted set, pruning entries older than window_blocks."""
        if not self._path.exists():
            return [], set()

        try:
            raw = json.loads(self._path.read_text())
        except Exception as e:
            bt.logging.warning(f'Failed to read scoring window cache: {e}')
            return [], set()

        version = raw.get('version', 0)
        if version != _SCHEMA_VERSION:
            bt.logging.warning(
                f'Scoring cache version mismatch (got {version}, expected {_SCHEMA_VERSION}), starting fresh'
            )
            return [], set()

        window_start = current_block - window_blocks
        raw_window = raw.get('window', [])
        raw_voted = raw.get('voted_ids', [])

        window: List[Swap] = []
        for entry in raw_window:
            swap = _dict_to_swap(entry)
            if swap is None:
                continue
            if resolved_block(swap) < window_start:
                continue
            window.append(swap)

        voted_ids: Set[int] = {v for v in raw_voted if isinstance(v, int)}

        # Re-persist if stale entries were pruned
        if len(window) != len(raw_window) or len(voted_ids) != len(raw_voted):
            self.save(window, voted_ids)

        if window:
            bt.logging.info(f'Restored {len(window)} swap(s) and {len(voted_ids)} voted ID(s) from scoring cache')

        return window, voted_ids

    def remove(self) -> None:
        """Delete the cache file (for tests or manual reset)."""
        try:
            os.remove(self._path)
        except FileNotFoundError:
            pass


def resolved_block(swap: Swap) -> int:
    """Block when a terminal swap was resolved."""
    if swap.completed_block > 0:
        return swap.completed_block
    if swap.timeout_block > 0:
        return swap.timeout_block
    return swap.initiated_block
