"""Scoring: the crown ledger fed from program events, one reward vector per round, and the weights.

A round reads the ledger and the World (``rewards.score_round``); nothing in it reads a chain."""

from __future__ import annotations

import time
from functools import partial
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple

import bittensor as bt
import numpy as np

from allways.constants import MAX_SCORING_BACKFILL_SECS, RECYCLE_UID, SCORING_WINDOW_SECS
from allways.eligibility import quote_addresses_valid
from allways.validator.chains.subtensor_view import Head
from allways.validator.chains.watcher import ChainWatcher, Probe
from allways.validator.feed.events import FeedStatus
from allways.validator.runtime.bus import Latest, Subscription
from allways.validator.runtime.component import Component
from allways.validator.scoring.ledger import Ledger
from allways.validator.scoring.rewards import (
    AlphaPrices,
    RoundResult,
    crown_now,
    fill_held_crown,
    halted_round,
    score_round,
)
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World
from neurons.base.utils.weight_utils import convert_weights_and_uids_for_emit, process_weights_for_netuid

LAST_SCORED = 'scoring.last_scored_time'
JUDGE_WAIT_SECS = 60.0  # a quote whose address check hasn't come back by then ingests unjudged
CROWN_NOW_SECS = 30.0  # how often the live crown is replayed for the dashboard


class Scoring(Component):
    """Not critical: a late round delays weights, never a swap. A round runs only once the feed has caught
    up and every event up to it is in the ledger."""

    name = 'scoring'
    interval_secs = 5.0
    stall_secs = 900.0

    def __init__(
        self,
        world: Latest[World],
        head: Latest[Head],
        events: Subscription,
        gaps: Subscription,
        feed_status: Latest[FeedStatus],
        store: Store,
        out: Latest[RoundResult],
        assets: Dict[str, Any],
        watchers: Dict[str, ChainWatcher],
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.world, self.head, self.events, self.gaps = world, head, events, gaps
        self.feed_status, self.store, self.out = feed_status, store, out
        self.assets, self.watchers = assets, watchers
        self.ledger = Ledger(store)
        self.prices = AlphaPrices(assets, lambda: (self.head.get()[0] or SimpleNamespace(prices=None)).prices)
        self._held: List[Tuple[Any, str, float]] = []  # QuoteSets waiting on an address check
        self._dropped = 0
        row = store.conn().execute("SELECT slot FROM acks WHERE consumer = 'scoring'").fetchone()
        self._acked_slot = int(row['slot']) if row else 0  # the feed may replay from an older consumer's ack
        self.crown: Latest[Dict] = Latest()  # who holds each quoted lane's crown right now (crown_now rows)
        self._crown_at = 0.0

    def tick(self, now: float) -> None:
        world, _ = self.world.get()
        head, _ = self.head.get()
        if world is None or world.config is None or head is None:
            return
        status, _ = self.feed_status.get()  # read before draining: everything up to its cursor is drained below
        for gap in self.gaps.drain():
            self.ledger.mark_gap(int(gap.complete_since))
        if self.events.dropped != self._dropped:  # fell behind the feed: what it dropped is a gap
            self._dropped = self.events.dropped
            self.ledger.mark_gap(int(now))
        self._ingest(self.events.drain(), world, head, now, status)
        caught_up = world.complete and status is not None and not status.catching_up and not self._held
        last = int(self.store.get_meta(LAST_SCORED) or now - SCORING_WINDOW_SECS)
        if caught_up and now - last >= SCORING_WINDOW_SECS:
            self._round(world, head, int(now), max(last, int(now) - MAX_SCORING_BACKFILL_SECS))
        if caught_up and now - self._crown_at >= CROWN_NOW_SECS:
            self._crown_at = now
            self.crown.set(crown_now(world, tuple(head.metagraph.hotkeys), self.ledger, self.prices, int(now)))

    def _ingest(self, records: List[Any], world: World, head: Head, now: float, status: Optional[FeedStatus]) -> None:
        fill = partial(fill_held_crown, world, set(head.metagraph.hotkeys), self.ledger, self.prices)
        ttl = int(getattr(world.config, 'reservation_ttl_secs', 0) or 0)
        fresh = [(r, _hotkey(r, world), now) for r in records if int(r.slot or 0) >= self._acked_slot]
        pending, self._held = self._held + fresh, []
        with self.store.tx() as c:
            for record, hotkey, since in pending:
                if hotkey is None or record.block_time is None:
                    continue  # unbound: no uid to credit; the round's anchors heal what it changed
                usable = self._quote_usable(record) if record.name == 'QuoteSet' else None
                if record.name == 'QuoteSet' and usable is None and now - since < JUDGE_WAIT_SECS:
                    self._held.append((record, hotkey, since))
                    continue
                try:
                    self.ledger.ingest(record, hotkey, ttl, usable, fill)
                except Exception as e:  # one malformed event must not hold back the rest
                    bt.logging.warning(f'scoring: {record.name} at {record.signature} not ingested: {e}')
            if status is not None and status.cursor is not None:
                Store.ack(c, 'scoring', *status.cursor)

    def _quote_usable(self, record: Any) -> Optional[bool]:
        if 'miner_from_addr' not in record.fields:
            return True  # a pre-upgrade event without the addresses: unjudged
        keys = ('from_chain', 'to_chain', 'miner_from_addr', 'miner_to_addr')
        return quote_usable(SimpleNamespace(**{k: str(record.fields[k]) for k in keys}), self.assets, self._refused)

    def _refused(self, chain: str, address: str) -> Optional[bool]:
        """The chain's own answer, through its watcher (asked once per address while quotes name it)."""
        if chain not in self.watchers:
            return False  # no provider here: unjudged
        probe = Probe(
            chain, 'address_refused', (address,), run=lambda a: a.address_refused(address), final=lambda _: True
        )
        verdict = self.watchers[chain].verdict(probe)
        if verdict is None:
            return None
        return verdict.error is None and bool(verdict.info)  # a failed check judges nothing

    def _round(self, world: World, head: Head, now: int, last: int) -> None:
        hotkeys = tuple(head.metagraph.hotkeys)
        if world.config.halted:
            result = halted_round(hotkeys, last, now)
        else:
            result = score_round(world, hotkeys, self.ledger, self.assets, self.prices, last, now)
        self.ledger.prune(now)
        with self.store.tx() as c:
            Store.set_meta(c, LAST_SCORED, str(now))
        self.out.set(result)
        bt.logging.info(f'scoring: round [{last}, {now}] scored' + (' (halted: recycled)' if result.halted else ''))


def quote_usable(
    quote: Any, providers: Dict[str, Any], refused: Callable[[str, str], Optional[bool]]
) -> Optional[bool]:
    """False if either address can't parse for its chain or its chain refuses it (an issuer-frozen address,
    a contract that reverts on receipt); None while ``refused`` hasn't answered."""
    if not quote_addresses_valid(quote, providers):
        return False
    answers = [
        refused(str(c).lower(), a)
        for c, a in ((quote.from_chain, quote.miner_from_addr), (quote.to_chain, quote.miner_to_addr))
    ]
    if any(answers):
        return False
    return None if None in answers else True


def _hotkey(record: Any, world: World) -> Optional[str]:
    miner = record.fields.get('miner') if hasattr(record.fields, 'get') else None
    return world.hotkey_for(miner) if miner is not None else None


class WeightSetter(Component):
    """Folds each round into the moving average and sets weights once per epoch, on its own subtensor
    connection. Scores live in the store, so a restart keeps them."""

    name = 'weights'
    interval_secs = 30.0
    stall_secs = 1800.0

    def __init__(
        self,
        rounds: Latest[RoundResult],
        head: Latest[Head],
        subtensor_factory: Callable[[], Any],
        store: Store,
        wallet: Any = None,  # None: fold scores but never set weights
        netuid: int = 0,
        alpha: float = 1.0,
        epoch_length: int = 150,
        version_key: int = 0,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.rounds, self.head, self._factory, self.store = rounds, head, subtensor_factory, store
        self.wallet, self.netuid, self.alpha = wallet, netuid, alpha
        self.epoch_length, self.version_key = epoch_length, version_key
        store.declare('weights', 1, [('weights_scores', _SCORES_DDL)])
        rows = store.conn().execute('SELECT hotkey, score FROM weights_scores ORDER BY uid').fetchall()
        self.hotkeys: List[str] = [r['hotkey'] for r in rows]
        self.scores = np.array([r['score'] for r in rows], dtype=np.float32)
        self._applied = 0
        self._set_at_block = 0
        self._subtensor: Any = None

    def setup(self) -> None:
        if self.wallet is not None:
            self._subtensor = self._factory()

    def tick(self, now: float) -> None:
        result, version = self.rounds.get()
        if result is not None and version != self._applied:
            self._applied = version
            self.fold(result)
        head, _ = self.head.get()
        if self._subtensor is not None and head is not None and self.hotkeys and self._due(head):
            self._set(head)

    def fold(self, result: RoundResult) -> None:
        """EMA over the round, aligned to its metagraph: a uid whose hotkey changed starts from zero."""
        old = self._aligned(result.hotkeys)
        rewards = np.nan_to_num(np.asarray(result.rewards, dtype=np.float32), nan=0.0)
        self.scores = self.alpha * rewards + (1 - self.alpha) * old
        self.hotkeys = list(result.hotkeys)
        with self.store.tx() as c:
            c.execute('DELETE FROM weights_scores')
            c.executemany(
                'INSERT INTO weights_scores VALUES (?, ?, ?)',
                [(uid, hk, float(s)) for uid, (hk, s) in enumerate(zip(self.hotkeys, self.scores))],
            )

    def _aligned(self, hotkeys) -> np.ndarray:
        """Scores laid over `hotkeys` by uid; a uid whose hotkey changed (or is new) gets zero."""
        return np.array(
            [
                self.scores[uid] if uid < len(self.hotkeys) and self.hotkeys[uid] == hk else 0.0
                for uid, hk in enumerate(hotkeys)
            ],
            dtype=np.float32,
        )

    def _due(self, head: Head) -> bool:
        hotkeys = list(head.metagraph.hotkeys)
        me = self.wallet.hotkey.ss58_address
        if me not in hotkeys:
            return False
        last = max(int(head.metagraph.last_update[hotkeys.index(me)]), self._set_at_block)
        return head.block - last > self.epoch_length

    def _set(self, head: Head) -> None:
        metagraph = head.metagraph
        n = len(metagraph.hotkeys)
        scores = self._aligned(metagraph.hotkeys)
        norm = float(np.sum(np.abs(scores)))
        if norm == 0 or np.isnan(norm):
            # Without this, an empty vector would spread emissions uniformly over every uid.
            weights = np.zeros(n, dtype=np.float32)
            weights[RECYCLE_UID if RECYCLE_UID < n else 0] = 1.0
            bt.logging.warning('weights: no scores, routing the full pool to the recycle uid')
        else:
            weights = scores / norm
        uids, processed = process_weights_for_netuid(
            uids=metagraph.uids, weights=weights, netuid=self.netuid, subtensor=self._subtensor, metagraph=metagraph
        )
        uint_uids, uint_weights = convert_weights_and_uids_for_emit(uids=uids, weights=processed)
        ok, msg = self._subtensor.set_weights(
            wallet=self.wallet,
            netuid=self.netuid,
            uids=uint_uids,
            weights=uint_weights,
            wait_for_finalization=False,
            wait_for_inclusion=False,
            version_key=self.version_key,
        )
        if ok is True:
            self._set_at_block = head.block
            bt.logging.info('weights: set on chain')
        else:
            bt.logging.error(f'weights: set_weights failed: {msg}')


_SCORES_DDL = (
    'CREATE TABLE IF NOT EXISTS weights_scores (uid INTEGER PRIMARY KEY, hotkey TEXT NOT NULL, score REAL NOT NULL)'
)
