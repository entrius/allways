"""SQLite-backed store for all validator-local state.

Tables: ``rate_events`` + ``active_events`` + ``activity_events`` +
``collateral_events`` (the crown-time event series, sourced from Solana program
events via ``SolanaEventIndex`` and keyed by unix ``blockTime``),
``clearing_rates`` (per-swap realized legs from ``SwapCompleted``, backing the
windowed volume read), ``swap_outcomes`` (terminal completed/timed_out
truth per swap_key, backing the seam's stage disambiguation after the swap PDA
closes), ``routed_requests`` (queued on-behalf reservation details awaiting
finalize — the one table NOT rebuildable from chain),
``relay_swaps``/``relay_fees``/``relay_slashes``/``relay_meta`` (the W3 bond
relay's ledger of what the vault still owes), and
``solana_event_meta`` (the event-ingest cursor).
Single connection guarded by one lock; opened with ``check_same_thread=False``.
``busy_timeout`` is set before ``journal_mode=WAL`` because the WAL flip takes a
brief exclusive lock that concurrent openers would otherwise hit as "database is
locked" — the local dev env runs two validators against the same file.
"""

import sqlite3
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from allways.classes import ActivityTransition, MinerActivity, next_activity


class ValidatorStateStore:
    def __init__(
        self,
        db_path: Path | str | None = None,
        current_block_fn: Optional[Callable[[], int]] = None,
    ):
        self.db_path = Path(db_path or Path.home() / '.allways' / 'validator' / 'state.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.conn: Optional[sqlite3.Connection] = sqlite3.connect(self.db_path, check_same_thread=False)
        # busy_timeout must be set before journal_mode: the WAL switch takes a
        # brief exclusive lock that a concurrent opener would otherwise hit as
        # an immediate "database is locked" error.
        self.conn.execute('PRAGMA busy_timeout=5000')
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.row_factory = sqlite3.Row
        self.current_block_fn = current_block_fn
        self.init_db()

    # ─── rate_events ────────────────────────────────────────────────────

    def insert_rate_event(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        rate: float,
        block: int,
        collateral_chain: str = 'sol',
    ) -> bool:
        """Insert a rate event, skipping same-rate duplicates. Dedup is per (hotkey, direction,
        collateral_chain) so a tao-backed quote and a sol-backed one on the same direction never
        overwrite or shadow each other (F4)."""
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(
                """
                SELECT rate FROM rate_events
                WHERE hotkey = ? AND from_chain = ? AND to_chain = ? AND collateral_chain = ?
                ORDER BY block DESC, id DESC
                LIMIT 1
                """,
                (hotkey, from_chain, to_chain, collateral_chain),
            ).fetchone()
            if row is not None and row['rate'] == rate:
                return False
            conn.execute(
                """
                INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block, collateral_chain)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (hotkey, from_chain, to_chain, rate, block, collateral_chain),
            )
            conn.commit()
            return True

    def get_latest_rate_before(
        self,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        block: int,
        collateral_chain: Optional[str] = None,
    ) -> Optional[Tuple[float, int]]:
        sql = """
            SELECT rate, block FROM rate_events
            WHERE hotkey = ? AND from_chain = ? AND to_chain = ? AND block <= ?
        """
        params: Tuple = (hotkey, from_chain, to_chain, block)
        if collateral_chain is not None:
            sql += ' AND collateral_chain = ?'
            params += (collateral_chain,)
        row = self._fetchone(sql + ' ORDER BY block DESC, id DESC LIMIT 1', params)
        return (row['rate'], row['block']) if row is not None else None

    def get_latest_rates_before(
        self,
        from_chain: str,
        to_chain: str,
        block: int,
        collateral_chain: Optional[str] = None,
    ) -> Dict[str, Tuple[float, int]]:
        """Batched form of get_latest_rate_before — one query per direction
        instead of one per (hotkey, direction). Returns {hotkey: (rate, block)}
        for every hotkey that has at least one rate event in that direction
        at-or-before ``block``. ``collateral_chain`` restricts to one backing
        (F4: a direction is scored against its own hub's quotes). Caller filters
        by membership in the rewardable set after.

        Ordering matches the single-row form: ``block DESC, id DESC`` so a
        same-block re-emit (id is monotonic) picks the latest write.
        """
        sql = """
            SELECT hotkey, rate, block FROM (
                SELECT hotkey, rate, block,
                       ROW_NUMBER() OVER (
                           PARTITION BY hotkey
                           ORDER BY block DESC, id DESC
                       ) AS rn
                FROM rate_events
                WHERE from_chain = ? AND to_chain = ? AND block <= ?{backing}
            ) WHERE rn = 1
        """
        params: Tuple = (from_chain, to_chain, block)
        if collateral_chain is not None:
            params += (collateral_chain,)
        sql = sql.format(backing=' AND collateral_chain = ?' if collateral_chain is not None else '')
        with self.lock:
            conn = self.require_connection()
            rows = conn.execute(sql, params).fetchall()
        return {r['hotkey']: (r['rate'], r['block']) for r in rows}

    def get_rate_events_in_range(
        self,
        from_chain: str,
        to_chain: str,
        start_block: int,
        end_block: int,
        collateral_chain: Optional[str] = None,
    ) -> List[dict]:
        """Rate events in ``(start_block, end_block]`` for a direction, oldest first.
        ``collateral_chain`` restricts to one backing (F4)."""
        sql = """
            SELECT id, hotkey, rate, block FROM rate_events
            WHERE from_chain = ? AND to_chain = ? AND block > ? AND block <= ?
        """
        params: Tuple = (from_chain, to_chain, start_block, end_block)
        if collateral_chain is not None:
            sql += ' AND collateral_chain = ?'
            params += (collateral_chain,)
        rows = self._fetchall(sql + ' ORDER BY block ASC, id ASC', params)
        return [{'id': r['id'], 'hotkey': r['hotkey'], 'rate': r['rate'], 'block': r['block']} for r in rows]

    def directions_with_live_rate(self, hotkey: str, collateral_chain: str, block: int) -> List[Tuple[str, str]]:
        """Directions where ``hotkey`` has a positive latest rate under ``collateral_chain`` at-or-before
        ``block`` (F4). Used to zero exactly one backing's quotes on a silent purse drop — the event
        that drops the purse names no direction, so the live ones are read back here."""
        rows = self._fetchall(
            """
            SELECT from_chain, to_chain, rate FROM (
                SELECT from_chain, to_chain, rate,
                       ROW_NUMBER() OVER (
                           PARTITION BY from_chain, to_chain
                           ORDER BY block DESC, id DESC
                       ) AS rn
                FROM rate_events
                WHERE hotkey = ? AND collateral_chain = ? AND block <= ?
            ) WHERE rn = 1 AND rate > 0
            """,
            (hotkey, collateral_chain, block),
        )
        return [(r['from_chain'], r['to_chain']) for r in rows]

    def lanes_with_live_rate(self, block: int) -> Dict[Tuple[str, str, str, str], float]:
        """Cross-hotkey variant of ``directions_with_live_rate``: every
        ``(hotkey, from_chain, to_chain, collateral_chain)`` lane whose latest
        rate at-or-before ``block`` is positive, mapped to that rate. This is
        the event-derived side of the per-round quote-existence reconcile."""
        rows = self._fetchall(
            """
            SELECT hotkey, from_chain, to_chain, collateral_chain, rate FROM (
                SELECT hotkey, from_chain, to_chain, collateral_chain, rate,
                       ROW_NUMBER() OVER (
                           PARTITION BY hotkey, from_chain, to_chain, collateral_chain
                           ORDER BY block DESC, id DESC
                       ) AS rn
                FROM rate_events
                WHERE block <= ?
            ) WHERE rn = 1 AND rate > 0
            """,
            (block,),
        )
        return {(r['hotkey'], r['from_chain'], r['to_chain'], r['collateral_chain']): r['rate'] for r in rows}

    def rate_lanes_touched_in_range(self, start_block: int, end_block: int) -> Set[Tuple[str, str, str, str]]:
        """Lanes with any rate event in ``(start_block, end_block]`` — the
        reconcile's per-lane quiet-window guard reads this so a stale live
        view never overwrites a fresher real event."""
        rows = self._fetchall(
            """
            SELECT DISTINCT hotkey, from_chain, to_chain, collateral_chain
            FROM rate_events WHERE block > ? AND block <= ?
            """,
            (start_block, end_block),
        )
        return {(r['hotkey'], r['from_chain'], r['to_chain'], r['collateral_chain']) for r in rows}

    # ─── crown event tables (Solana-sourced via SolanaEventIndex) ───────

    def insert_active_event(self, block_num: int, hotkey: str, active: bool) -> None:
        self._execute(
            'INSERT INTO active_events (block_num, hotkey, active) VALUES (?, ?, ?)',
            (block_num, hotkey, 1 if active else 0),
        )

    def insert_activity_event(
        self, block_num: int, hotkey: str, transition: ActivityTransition, hub: Optional[str] = None
    ) -> None:
        """Record one edge of a miner's ``MinerActivity`` machine (RESERVE_START,
        FULFILL_START, FULFILL_END, or the synthetic RESERVE_EXPIRE). ``hub`` is the
        purse the swap draws against (v3.1 per-hub busy); NULL applies to every hub —
        the pre-v3.1.1 global-busy reading, kept for legacy rows and reconcile writes."""
        self._execute(
            'INSERT INTO activity_events (block_num, hotkey, kind, hub) VALUES (?, ?, ?, ?)',
            (block_num, hotkey, int(transition), hub),
        )

    def restamp_reservation_expiry(
        self, hotkey: str, hub: Optional[str], new_block_num: int, not_before: int = 0
    ) -> None:
        """Move the miner's most-recent synthetic RESERVE_EXPIRE (this hub) to the chain's real
        ``reserved_until``. PoolResolved stamps a draw+ttl guess before the true deadline is known;
        ReservationFilled/Extended carry it, so a busy miner isn't freed before its swap initiates.
        ``not_before`` bounds the move: a row already fired before the triggering event belongs to a
        PRIOR reservation (this one's PoolResolved was dropped) — clobbering it strands the hub busy."""
        self._execute(
            """
            UPDATE activity_events SET block_num = ?
            WHERE id = (
                SELECT id FROM activity_events
                WHERE hotkey = ? AND kind = ? AND hub IS ? AND block_num >= ?
                ORDER BY id DESC LIMIT 1
            )
            """,
            (int(new_block_num), hotkey, int(ActivityTransition.RESERVE_EXPIRE), hub, int(not_before)),
        )

    def load_all_active_events(self) -> List[dict]:
        rows = self._fetchall('SELECT block_num, hotkey, active FROM active_events ORDER BY block_num ASC, id ASC')
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'active': bool(r['active'])} for r in rows]

    def load_all_activity_events(self) -> List[dict]:
        rows = self._fetchall('SELECT block_num, hotkey, kind, hub FROM activity_events ORDER BY block_num ASC, id ASC')
        return [{'block_num': r['block_num'], 'hotkey': r['hotkey'], 'kind': r['kind'], 'hub': r['hub']} for r in rows]

    def insert_collateral_event(self, block_num: int, hotkey: str, collateral_rao: int, backing: str = 'sol') -> None:
        self._execute(
            'INSERT INTO collateral_events (block_num, hotkey, collateral_rao, backing) VALUES (?, ?, ?, ?)',
            (block_num, hotkey, int(collateral_rao), backing),
        )

    def load_all_collateral_events(self) -> List[dict]:
        rows = self._fetchall(
            'SELECT block_num, hotkey, collateral_rao FROM collateral_events ORDER BY block_num ASC, id ASC'
        )
        return [
            {'block_num': r['block_num'], 'hotkey': r['hotkey'], 'collateral_rao': int(r['collateral_rao'])}
            for r in rows
        ]

    # ─── crown read interface (B3.4 SolanaEventIndex) ───────────────────
    #
    # At-time + in-range queries over the active/activity/collateral event tables,
    # the SQL twins of the rate_events readers above. ``block_num`` here is a
    # unix ``blockTime`` (seconds), not a substrate block — the Solana crown
    # axis. ``SolanaEventIndex`` wraps these into the read interface scoring's
    # crown replay consumes.

    def get_active_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Active-flag transitions in ``(start_time, end_time]``, oldest first."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, active FROM active_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [{'hotkey': r['hotkey'], 'active': bool(r['active']), 'block': r['block_num']} for r in rows]

    def get_active_state_at(self, at_time: int) -> Set[str]:
        """Active set at ``at_time`` — latest transition per hotkey at-or-before
        ``at_time``, keeping those whose latest flag is True."""
        rows = self._fetchall(
            """
            SELECT hotkey, active FROM (
                SELECT hotkey, active,
                       ROW_NUMBER() OVER (PARTITION BY hotkey ORDER BY block_num DESC, id DESC) AS rn
                FROM active_events WHERE block_num <= ?
            ) WHERE rn = 1
            """,
            (at_time,),
        )
        return {r['hotkey'] for r in rows if r['active']}

    def get_activity_events_in_range(self, start_time: int, end_time: int) -> List[dict]:
        """Activity transitions in ``(start_time, end_time]``. Ordered ``block_num,
        kind`` so coincident-instant edges replay in machine-precedence order
        (closers/openers before a reservation lapse). ``hub`` NULL = every hub."""
        rows = self._fetchall(
            """
            SELECT id, block_num, hotkey, kind, hub FROM activity_events
            WHERE block_num > ? AND block_num <= ?
            ORDER BY block_num ASC, kind ASC, id ASC
            """,
            (start_time, end_time),
        )
        return [{'hotkey': r['hotkey'], 'kind': r['kind'], 'block': r['block_num'], 'hub': r['hub']} for r in rows]

    def get_activity_state_at(self, at_time: int) -> Dict[str, Dict[str, MinerActivity]]:
        """Per-(hotkey, hub) ``MinerActivity`` at ``at_time``, reduced over each
        miner's per-hub transition timelines (v3.1: busy is per purse). Only busy
        entries are returned — ``{hotkey: {hub: state}}`` with AVAILABLE hubs
        omitted; callers default absences to AVAILABLE."""
        rows = self._fetchall(
            """
            SELECT block_num, hotkey, kind, hub FROM activity_events
            WHERE block_num <= ?
            ORDER BY block_num ASC, kind ASC, id ASC
            """,
            (at_time,),
        )
        return self._reduce_activity(rows)

    @staticmethod
    def _reduce_activity(rows: Sequence[sqlite3.Row]) -> Dict[str, Dict[str, MinerActivity]]:
        """Fold ordered transition rows into ``{hotkey: {hub: state}}`` keeping only
        non-AVAILABLE hubs. A NULL-hub row (legacy global-busy, reconcile) steps every
        hub's machine. An undefined transition holds the current state (defensive)."""
        from allways.solana.pdas import BACKING_BITS

        states: Dict[str, Dict[str, MinerActivity]] = {}
        for r in rows:
            hk = r['hotkey']
            hub = r['hub'] if 'hub' in r.keys() else None
            per_hub = states.setdefault(hk, {})
            for h in [hub] if hub else list(BACKING_BITS):
                cur = per_hub.get(h, MinerActivity.AVAILABLE)
                nxt = next_activity(cur, ActivityTransition(r['kind']))
                per_hub[h] = cur if nxt is None else nxt
        return {
            hk: busy
            for hk, per_hub in states.items()
            if (busy := {h: st for h, st in per_hub.items() if st is not MinerActivity.AVAILABLE})
        }

    def get_collateral_events_in_range(
        self, start_time: int, end_time: int, backing: Optional[str] = None
    ) -> List[dict]:
        """Collateral transitions in ``(start_time, end_time]``, oldest first.
        ``collateral_rao`` is the post-event total. ``backing`` restricts to one
        purse (F4: the SOL local purse vs the attested TAO bond)."""
        sql = (
            'SELECT id, block_num, hotkey, collateral_rao FROM collateral_events WHERE block_num > ? AND block_num <= ?'
        )
        params: Tuple = (start_time, end_time)
        if backing is not None:
            sql += ' AND backing = ?'
            params += (backing,)
        rows = self._fetchall(sql + ' ORDER BY block_num ASC, id ASC', params)
        return [
            {'hotkey': r['hotkey'], 'collateral_rao': int(r['collateral_rao']), 'block': r['block_num']} for r in rows
        ]

    def get_collaterals_at(self, at_time: int, backing: Optional[str] = None) -> Dict[str, int]:
        """Per-hotkey posted collateral at ``at_time`` — latest transition
        at-or-before ``at_time``. Hotkeys with no event are absent (caller
        treats as unknown, not zero). ``backing`` restricts to one purse (F4)."""
        sql = """
            SELECT hotkey, collateral_rao FROM (
                SELECT hotkey, collateral_rao,
                       ROW_NUMBER() OVER (PARTITION BY hotkey ORDER BY block_num DESC, id DESC) AS rn
                FROM collateral_events WHERE block_num <= ?{backing}
            ) WHERE rn = 1
        """
        params: Tuple = (at_time,)
        if backing is not None:
            params += (backing,)
        sql = sql.format(backing=' AND backing = ?' if backing is not None else '')
        rows = self._fetchall(sql, params)
        return {r['hotkey']: int(r['collateral_rao']) for r in rows}

    # ─── clearing_rates (per-swap realized legs) ────────────────────────

    def insert_clearing_rate(
        self,
        block_num: int,
        hotkey: str,
        from_chain: str,
        to_chain: str,
        from_amount: int,
        to_amount: int,
        swap_key: str,
    ) -> None:
        """Persist one completed swap's realized legs, keyed by ``swap_key`` hex so a
        cursor-reset / RPC-prune re-ingest can't double-count volume. ``block_num`` is
        the unix ``blockTime``; the legs are stored as decimal strings (u128-safe)."""
        self._execute(
            """
            INSERT INTO clearing_rates (block_num, hotkey, from_chain, to_chain, from_amount, to_amount, swap_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(swap_key) DO NOTHING
            """,
            (block_num, hotkey, from_chain, to_chain, str(int(from_amount)), str(int(to_amount)), swap_key),
        )

    def get_clearing_volumes(self, start_time: int, end_time: int) -> Dict[Tuple[str, str], Dict[str, Tuple[int, int]]]:
        """``{(from_chain, to_chain): {hotkey: (from_amount_sum, to_amount_sum)}}``
        over ``(start_time, end_time]`` — the windowed realized-volume read.
        Scoring no longer consumes it; the dashboard and treasury reporting do.
        Summed in Python: the legs are stored as TEXT (u128-safe) and SQL SUM
        would coerce them to float."""
        rows = self._fetchall(
            """
            SELECT from_chain, to_chain, hotkey, from_amount, to_amount FROM clearing_rates
            WHERE block_num > ? AND block_num <= ?
            """,
            (start_time, end_time),
        )
        volumes: Dict[Tuple[str, str], Dict[str, Tuple[int, int]]] = {}
        for r in rows:
            direction = volumes.setdefault((r['from_chain'], r['to_chain']), {})
            from_sum, to_sum = direction.get(r['hotkey'], (0, 0))
            direction[r['hotkey']] = (from_sum + int(r['from_amount']), to_sum + int(r['to_amount']))
        return volumes

    def prune_clearing_rates(self, cutoff_block: int) -> None:
        """Drop clearing-rate rows older than ``cutoff_block``. No anchor row is
        preserved — each row is an independent sample, not a state-reconstruction
        baseline (unlike rate/active/collateral events)."""
        if cutoff_block <= 0:
            return
        self._execute('DELETE FROM clearing_rates WHERE block_num < ?', (cutoff_block,))

    # ─── swap_outcomes (terminal per-swap truth for the seam) ───────────

    def record_swap_outcome(self, swap_key: str, outcome: str, block_time: int) -> None:
        """Persist a swap's terminal outcome (``completed`` | ``timed_out``) keyed by
        swap_key hex. Upsert: a cursor-reset re-ingest of the same event is a no-op."""
        self._execute(
            """
            INSERT INTO swap_outcomes (swap_key, outcome, block_time) VALUES (?, ?, ?)
            ON CONFLICT(swap_key) DO UPDATE SET outcome = excluded.outcome, block_time = excluded.block_time
            """,
            (swap_key, outcome, block_time),
        )

    def get_swap_outcome(self, swap_key: str) -> Optional[str]:
        row = self._fetchone('SELECT outcome FROM swap_outcomes WHERE swap_key = ?', (swap_key,))
        return row['outcome'] if row is not None else None

    def prune_swap_outcomes(self, cutoff_block: int) -> None:
        """Drop outcome (and fulfillment-hash) rows older than ``cutoff_block``. No anchor row —
        each row is an independent terminal fact, only queried while the offering still polls."""
        if cutoff_block <= 0:
            return
        self._execute('DELETE FROM swap_outcomes WHERE block_time < ?', (cutoff_block,))
        self._execute('DELETE FROM swap_fulfillments WHERE block_time < ?', (cutoff_block,))

    # ─── swap_fulfillments (delivery-leg hash for post-close receipts) ──

    def record_swap_fulfillment(self, swap_key: str, to_tx_hash: str, block_time: int) -> None:
        """Persist the miner's delivery tx hash (``SwapFulfilled`` event) keyed by swap_key hex.
        The Swap PDA — and its ``to_tx_hash`` — closes at terminal, so this index is the seam's
        only post-close source of the delivery leg. Upsert: a cursor-reset re-ingest is a no-op."""
        self._execute(
            """
            INSERT INTO swap_fulfillments (swap_key, to_tx_hash, block_time) VALUES (?, ?, ?)
            ON CONFLICT(swap_key) DO UPDATE SET to_tx_hash = excluded.to_tx_hash, block_time = excluded.block_time
            """,
            (swap_key, to_tx_hash, block_time),
        )

    def get_swap_fulfillment(self, swap_key: str) -> Optional[str]:
        row = self._fetchone('SELECT to_tx_hash FROM swap_fulfillments WHERE swap_key = ?', (swap_key,))
        return row['to_tx_hash'] if row is not None else None

    # ─── routed_requests (on-behalf reservation queue) ──────────────────
    # The ONLY table not rebuildable from chain events: a routed user's details
    # exist nowhere else until the won seat is finalized on-chain.

    def upsert_routed_request(
        self,
        miner: str,
        from_chain: str,
        to_chain: str,
        backing: str,
        user_pubkey: str,
        user_from_addr: str,
        user_to_addr: str,
        from_amount: int,
        created_at: int,
    ) -> None:
        """Persist one routed reservation request. A retry from the same user for the
        same (miner, direction, backing) refreshes addresses/amount but keeps its
        original ``created_at`` — a retry never loses its FIFO position."""
        self._execute(
            """
            INSERT INTO routed_requests
                (miner, from_chain, to_chain, backing, user_pubkey, user_from_addr, user_to_addr, from_amount, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(miner, from_chain, to_chain, backing, user_pubkey) DO UPDATE SET
                user_from_addr = excluded.user_from_addr,
                user_to_addr = excluded.user_to_addr,
                from_amount = excluded.from_amount
            """,
            (
                miner,
                from_chain,
                to_chain,
                backing,
                user_pubkey,
                user_from_addr,
                user_to_addr,
                str(int(from_amount)),
                created_at,
            ),
        )

    def pending_routed_requests(self, miner: str, from_chain: str, to_chain: str, backing: str = 'sol') -> List[dict]:
        """A (miner, direction, backing) queue, oldest first (the FIFO order
        ``draw_pool_winner`` selects from)."""
        rows = self._fetchall(
            """
            SELECT user_pubkey, user_from_addr, user_to_addr, from_amount, created_at FROM routed_requests
            WHERE miner = ? AND from_chain = ? AND to_chain = ? AND backing = ?
            ORDER BY created_at ASC, id ASC
            """,
            (miner, from_chain, to_chain, backing),
        )
        return [
            {
                'user_pubkey': r['user_pubkey'],
                'user_from_addr': r['user_from_addr'],
                'user_to_addr': r['user_to_addr'],
                'from_amount': int(r['from_amount']),
                'created_at': r['created_at'],
            }
            for r in rows
        ]

    def distinct_routed_pools(self) -> List[Tuple[str, str, str, str]]:
        """The (miner, from_chain, to_chain, backing) keys with pending requests —
        the finalize sweep's iteration set."""
        rows = self._fetchall('SELECT DISTINCT miner, from_chain, to_chain, backing FROM routed_requests')
        return [(r['miner'], r['from_chain'], r['to_chain'], r['backing']) for r in rows]

    def delete_routed_requests(self, miner: str, from_chain: str, to_chain: str, backing: str = 'sol') -> None:
        """Drop one (miner, direction, backing) queue — called on any terminal outcome
        (finalized, lost, expired). Non-selected users re-request via their client."""
        self._execute(
            'DELETE FROM routed_requests WHERE miner = ? AND from_chain = ? AND to_chain = ? AND backing = ?',
            (miner, from_chain, to_chain, backing),
        )

    def prune_routed_requests(self, cutoff_time: int) -> None:
        """Staleness backstop: drop rows older than ``cutoff_time`` so a dead
        miner (pool never drawn, reservation never seen) can't pin a queue."""
        self._execute('DELETE FROM routed_requests WHERE created_at < ?', (cutoff_time,))

    # ─── W3 bond relay (cross-chain vault relayer) ──────────────────────

    def record_relay_swap(
        self,
        swap_key: str,
        miner: str,
        backing: str,
        user_addr: str,
        seen_at: int,
        hotkey: Optional[str] = None,
    ) -> None:
        """Snapshot a live off-chain-backed swap's reimbursement target and bonded hotkey. First
        sighting wins: the address is pinned at finalize and immutable, and the hotkey is captured
        here (F1) so a mid-swap rebind can't redirect the seizure to a fresh, unbonded account."""
        self._execute(
            """
            INSERT INTO relay_swaps (swap_key, miner, backing, user_addr, seen_at, hotkey)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(swap_key) DO NOTHING
            """,
            (swap_key, miner, backing, user_addr, seen_at, hotkey),
        )

    def get_relay_swap(self, swap_key: str) -> Optional[dict]:
        row = self._fetchone(
            'SELECT swap_key, miner, backing, user_addr, seen_at, hotkey FROM relay_swaps WHERE swap_key = ?',
            (swap_key,),
        )
        return dict(row) if row is not None else None

    def prune_relay_swaps(self, cutoff_time: int) -> None:
        """Drop snapshots for swaps long since terminal. Rows whose slash is still unapplied are
        kept — the relay's obligation outlives the swap."""
        self._execute(
            """
            DELETE FROM relay_swaps WHERE seen_at < ?
              AND swap_key NOT IN (SELECT swap_key FROM relay_slashes WHERE applied = 0)
            """,
            (cutoff_time,),
        )

    def record_relay_fee(
        self, swap_key: str, miner: str, backing: str, fee: int, block_time: int, vault_generation: int = 0
    ) -> None:
        """One completed off-chain-backed swap's absolute protocol fee (rao), tagged with the vault
        generation that will collect it. Keyed by swap_key so a re-ingested event can never
        double-count the accrual."""
        self._execute(
            """
            INSERT INTO relay_fees (swap_key, miner, backing, fee, block_time, vault_generation)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(swap_key) DO NOTHING
            """,
            (swap_key, miner, backing, int(fee), block_time, int(vault_generation)),
        )

    def accrued_fee_total(
        self, miner: str, backing: str, at_time: Optional[int] = None, vault_generation: Optional[int] = None
    ) -> int:
        """Cumulative fees this miner owes THIS vault generation on ``backing``, optionally as of a
        boundary. Scoped by generation because the vault's settled counter restarts at 0 on a
        replacement — an unscoped sum re-charges what the retired vault already collected. The
        cadence batch reads at the aligned boundary so every validator derives the identical vector."""
        sql = 'SELECT COALESCE(SUM(fee), 0) AS total FROM relay_fees WHERE miner = ? AND backing = ?'
        params: Tuple = (miner, backing)
        if vault_generation is not None:
            sql += ' AND vault_generation = ?'
            params += (int(vault_generation),)
        if at_time is not None:
            sql += ' AND block_time <= ?'
            params += (at_time,)
        row = self._fetchone(sql, params)
        return int(row['total']) if row is not None else 0

    def accrued_fee_totals(
        self, backing: str, at_time: Optional[int] = None, vault_generation: Optional[int] = None
    ) -> Dict[str, int]:
        """``{miner: cumulative_fee}`` on ``backing``, scoped to one vault generation when given
        (accounting) and across all of them when not (miner discovery)."""
        sql = 'SELECT miner, COALESCE(SUM(fee), 0) AS total FROM relay_fees WHERE backing = ?'
        params: Tuple = (backing,)
        if vault_generation is not None:
            sql += ' AND vault_generation = ?'
            params += (int(vault_generation),)
        if at_time is not None:
            sql += ' AND block_time <= ?'
            params += (at_time,)
        rows = self._fetchall(sql + ' GROUP BY miner', params)
        return {r['miner']: int(r['total']) for r in rows}

    def record_relay_slash(
        self,
        swap_key: str,
        miner: str,
        backing: str,
        penalty: int,
        reimbursement: int,
        user_addr: str,
        block_time: int,
        hotkey: Optional[str] = None,
    ) -> None:
        """A timeout verdict owed to the vault. Stored verbatim from the event — the figures are
        hash-bound into the vault round, so a reconstructed number would conflict, not co-count.
        ``hotkey`` is the observe-time snapshot (F1); NULL falls back to the live lookup."""
        self._execute(
            """
            INSERT INTO relay_slashes
                (swap_key, miner, backing, penalty, reimbursement, user_addr, block_time, applied, hotkey)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
            ON CONFLICT(swap_key) DO NOTHING
            """,
            (swap_key, miner, backing, int(penalty), int(reimbursement), user_addr, block_time, hotkey),
        )

    def open_relay_slashes(self, backing: str, miner: Optional[str] = None) -> List[dict]:
        """Verdicts the vault has not confirmed applied, oldest first."""
        sql = """
            SELECT swap_key, miner, backing, penalty, reimbursement, user_addr, block_time, hotkey
            FROM relay_slashes WHERE applied = 0 AND backing = ?
        """
        params: Tuple = (backing,)
        if miner is not None:
            sql += ' AND miner = ?'
            params += (miner,)
        return [dict(r) for r in self._fetchall(sql + ' ORDER BY block_time ASC, swap_key ASC', params)]

    def mark_relay_slash_applied(self, swap_key: str) -> None:
        self._execute('UPDATE relay_slashes SET applied = 1 WHERE swap_key = ?', (swap_key,))

    def pending_slash_totals(self, backing: str) -> Dict[str, int]:
        """``{miner: unapplied penalty total}`` — the netting-at-verdict subtraction, and the
        off-chain initiate backstop's "has any pending debit" answer."""
        rows = self._fetchall(
            """
            SELECT miner, COALESCE(SUM(penalty), 0) AS total FROM relay_slashes
            WHERE applied = 0 AND backing = ? GROUP BY miner
            """,
            (backing,),
        )
        return {r['miner']: int(r['total']) for r in rows}

    def get_relay_meta(self, key: str) -> Optional[str]:
        row = self._fetchone('SELECT value FROM relay_meta WHERE key = ?', (key,))
        return row['value'] if row is not None else None

    def relay_meta_prefix(self, prefix: str) -> Dict[str, str]:
        """Every relay_meta row whose key starts with ``prefix`` — the armed exit set, which has to
        outlive a restart because "deactivated" and "never activated" look identical on chain."""
        rows = self._fetchall('SELECT key, value FROM relay_meta WHERE key LIKE ?', (f'{prefix}%',))
        return {r['key'][len(prefix) :]: r['value'] for r in rows}

    def delete_relay_meta(self, key: str) -> None:
        self._execute('DELETE FROM relay_meta WHERE key = ?', (key,))

    def set_relay_meta(self, key: str, value: str) -> None:
        self._execute(
            """
            INSERT INTO relay_meta (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, str(value)),
        )

    def get_solana_event_cursor(self) -> Optional[str]:
        """Last ingested Solana tx signature (the SolanaEventIngest cursor).
        ``None`` on a fresh DB so the first poll starts from the prune horizon."""
        row = self._fetchone('SELECT value FROM solana_event_meta WHERE key = ?', ('cursor',))
        return row['value'] if row is not None else None

    def set_solana_event_cursor(self, signature: str) -> None:
        self._execute(
            """
            INSERT INTO solana_event_meta (key, value) VALUES ('cursor', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (signature,),
        )

    def get_relay_event_cursor(self) -> Optional[str]:
        """The relay's OWN event cursor (F3), kept apart from the crown cursor so a failed verdict
        write holds it and the window is re-read, without wedging (or being wedged by) the crown."""
        return self.get_relay_meta('event_cursor')

    def set_relay_event_cursor(self, signature: str) -> None:
        self.set_relay_meta('event_cursor', signature)

    def prune_active_events(self, cutoff_block: int) -> None:
        """Drop active events older than ``cutoff_block``, preserving the latest
        row per hotkey as a state-reconstruction anchor (mirrors the in-memory
        prune's anchor-preservation rule)."""
        if cutoff_block <= 0:
            return
        self._execute(
            """
            DELETE FROM active_events
            WHERE block_num < ?
              AND id NOT IN (SELECT MAX(id) FROM active_events GROUP BY hotkey)
            """,
            (cutoff_block,),
        )

    def prune_activity_events(self, cutoff_block: int) -> None:
        """Drop activity transitions older than ``cutoff_block`` except for hotkeys
        still mid-reservation/swap (reduced state != AVAILABLE) — their full
        timeline is kept so a later FULFILL_END / RESERVE_EXPIRE isn't orphaned.
        Read + reduce + delete under one lock so no writer interleaves."""
        if cutoff_block <= 0:
            return
        with self.lock:
            conn = self.require_connection()
            all_rows = conn.execute(
                'SELECT block_num, hotkey, kind, hub FROM activity_events ORDER BY block_num ASC, kind ASC, id ASC'
            ).fetchall()
            # Busy on ANY hub keeps the hotkey's full timeline (the reducer's keys).
            open_hotkeys = set(self._reduce_activity(all_rows))
            if open_hotkeys:
                placeholders = ','.join('?' * len(open_hotkeys))
                conn.execute(
                    f'DELETE FROM activity_events WHERE block_num < ? AND hotkey NOT IN ({placeholders})',
                    (cutoff_block, *open_hotkeys),
                )
            else:
                conn.execute('DELETE FROM activity_events WHERE block_num < ?', (cutoff_block,))
            conn.commit()

    def prune_collateral_events(self, cutoff_block: int) -> None:
        """Drop collateral events older than ``cutoff_block``, preserving the
        latest row per ``(hotkey, backing)`` as a reconstruction anchor.

        The anchor MUST be keyed by backing: a dual-backing miner posts one
        collateral stream per purse (sol local, tao bond), and grouping by
        hotkey alone kept only the globally-newest row — so a fresher sol row
        silently deleted the tao anchor, ``get_collaterals_at(backing='tao')``
        found nothing, and the miner dropped off every tao lane's crown. Same
        per-lane keying the rate prune uses (#668)."""
        if cutoff_block <= 0:
            return
        self._execute(
            """
            DELETE FROM collateral_events
            WHERE block_num < ?
              AND id NOT IN (SELECT MAX(id) FROM collateral_events GROUP BY hotkey, backing)
            """,
            (cutoff_block,),
        )

    # ─── cross-table maintenance ────────────────────────────────────────

    def delete_hotkey(self, hotkey: str) -> None:
        with self.lock:
            conn = self.require_connection()
            conn.execute('DELETE FROM rate_events WHERE hotkey = ?', (hotkey,))
            conn.commit()

    def prune_events_older_than(self, cutoff_block: int) -> None:
        """Delete rate events older than ``cutoff_block``, preserving the
        latest row per ``(hotkey, from_chain, to_chain, collateral_chain)``
        lane as a state-reconstruction anchor for
        ``get_latest_rate_before(window_start)``. The backing is part of the
        anchor key (F4): on a dual-backing direction each backing's lane is
        scored on its own rate stream, so keeping only the direction's newest
        row would delete the sibling backing's anchor and silently drop that
        lane out of the crown at window start."""
        self._execute(
            """
            DELETE FROM rate_events
            WHERE block < ?
              AND id NOT IN (
                  SELECT MAX(id) FROM rate_events
                  GROUP BY hotkey, from_chain, to_chain, collateral_chain
              )
            """,
            (cutoff_block,),
        )

    def close(self) -> None:
        with self.lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def require_connection(self) -> sqlite3.Connection:
        if self.conn is None:
            raise RuntimeError('ValidatorStateStore is closed')
        return self.conn

    # ─── crud helpers ───────────────────────────────────────────────────
    # Single-statement boilerplate. Methods that hold the lock across
    # multiple statements (insert_rate_event, delete_hotkey) bypass these.

    def _execute(self, sql: str, params: Tuple = ()) -> None:
        """Single-statement write under lock with commit."""
        with self.lock:
            conn = self.require_connection()
            conn.execute(sql, params)
            conn.commit()

    def _execute_returning_rowcount(self, sql: str, params: Tuple = ()) -> int:
        """Single-statement write under lock; returns affected row count."""
        with self.lock:
            conn = self.require_connection()
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor.rowcount

    def _fetchone(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        """Read a single row under lock. Caller is responsible for mapping
        the row to a domain type (often via a ``row_to_X`` helper)."""
        with self.lock:
            conn = self.require_connection()
            return conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Read all matching rows under lock. Caller maps."""
        with self.lock:
            conn = self.require_connection()
            return conn.execute(sql, params).fetchall()

    def _fetch_and_delete(self, select_sql: str, delete_sql: str, params: Tuple) -> Optional[sqlite3.Row]:
        """Atomic snapshot-then-delete under a single lock acquisition.
        Returns the pre-delete row, or None if no row matched."""
        with self.lock:
            conn = self.require_connection()
            row = conn.execute(select_sql, params).fetchone()
            if row is None:
                return None
            conn.execute(delete_sql, params)
            conn.commit()
            return row

    def init_db(self) -> None:
        with self.lock:
            conn = self.require_connection()
            # The pre-B3.5 scoring ledger squatted this name; IF NOT EXISTS keeps its dead schema.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(swap_outcomes)')]
            if cols and 'outcome' not in cols:
                conn.execute('DROP TABLE swap_outcomes')
            # Pre-M2 deployments lack the clearing_rates idempotency key. Purge unkeyed rows:
            # NULL keys never collide with ON CONFLICT(swap_key), so a post-upgrade replay would
            # double-count them — a one-time <=2h volume gap buys a safe dedup invariant.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(clearing_rates)')]
            if cols and 'swap_key' not in cols:
                conn.execute('ALTER TABLE clearing_rates ADD COLUMN swap_key TEXT')
                conn.execute('DELETE FROM clearing_rates')
            # v3.1: routed queues are keyed per (miner, direction, BACKING) — one hub's residue must
            # not dead-end another hub's queue. Pre-v3.1 rows were all sol-backed by construction.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(routed_requests)')]
            if cols and 'backing' not in cols:
                conn.execute("ALTER TABLE routed_requests ADD COLUMN backing TEXT NOT NULL DEFAULT 'sol'")
                conn.execute('DROP INDEX IF EXISTS idx_routed_requests_key')
            # F1: pin the bond hotkey at observe time so a mid-swap rebind can't redirect the
            # seizure to a fresh, unbonded account. Pre-F1 rows carry NULL and fall back to the
            # live lookup, which is the pre-F1 behaviour.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(relay_swaps)')]
            if cols and 'hotkey' not in cols:
                conn.execute('ALTER TABLE relay_swaps ADD COLUMN hotkey TEXT')
            cols = [row[1] for row in conn.execute('PRAGMA table_info(relay_slashes)')]
            if cols and 'hotkey' not in cols:
                conn.execute('ALTER TABLE relay_slashes ADD COLUMN hotkey TEXT')
            # A fee is owed to the vault in service when it was earned, and a replacement vault
            # restarts its settled counter at 0 — so an accrual must never be summed against a vault
            # that did not collect it. Pre-existing rows default to generation 0, which is exactly
            # where they were earned (the generation only exists from the first swap onward).
            cols = [row[1] for row in conn.execute('PRAGMA table_info(relay_fees)')]
            if cols and 'vault_generation' not in cols:
                conn.execute('ALTER TABLE relay_fees ADD COLUMN vault_generation INTEGER NOT NULL DEFAULT 0')
                conn.execute('DROP INDEX IF EXISTS idx_relay_fees_miner')
            # F4: the crown must score each direction against its OWN backing. rate_events gains the
            # quote's collateral_chain and collateral_events a backing, so a tao-hub quote/purse is
            # never confused with the sol one. Pre-F4 rows were all sol-backed by construction.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(rate_events)')]
            if cols and 'collateral_chain' not in cols:
                conn.execute("ALTER TABLE rate_events ADD COLUMN collateral_chain TEXT NOT NULL DEFAULT 'sol'")
            cols = [row[1] for row in conn.execute('PRAGMA table_info(collateral_events)')]
            if cols and 'backing' not in cols:
                conn.execute("ALTER TABLE collateral_events ADD COLUMN backing TEXT NOT NULL DEFAULT 'sol'")
            # v3.1.1: activity gains the per-hub dimension. Pre-upgrade rows stay NULL =
            # global-busy on every hub — exactly the behavior they were written under.
            cols = [row[1] for row in conn.execute('PRAGMA table_info(activity_events)')]
            if cols and 'hub' not in cols:
                conn.execute('ALTER TABLE activity_events ADD COLUMN hub TEXT')
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS rate_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    hotkey      TEXT NOT NULL,
                    from_chain  TEXT NOT NULL,
                    to_chain    TEXT NOT NULL,
                    rate        REAL NOT NULL,
                    block       INTEGER NOT NULL,
                    collateral_chain TEXT NOT NULL DEFAULT 'sol'
                );
                CREATE INDEX IF NOT EXISTS idx_rate_events_block
                    ON rate_events(block);
                CREATE INDEX IF NOT EXISTS idx_rate_events_dir_block
                    ON rate_events(from_chain, to_chain, block);
                CREATE INDEX IF NOT EXISTS idx_rate_events_hotkey
                    ON rate_events(hotkey);

                CREATE TABLE IF NOT EXISTS active_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    active      INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_active_events_block
                    ON active_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_active_events_hotkey
                    ON active_events(hotkey);

                -- MinerActivity transitions (D4): kind is an ActivityTransition
                -- value; the crown replay reduces these into per-instant PER-HUB
                -- state (v3.1 busy is per purse), so a reserved/fulfilling miner
                -- forfeits crown only on directions its busy hub serves. hub NULL
                -- = every hub (legacy rows, reconcile writes).
                CREATE TABLE IF NOT EXISTS activity_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    kind        INTEGER NOT NULL,
                    hub         TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_activity_events_block
                    ON activity_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_activity_events_hotkey
                    ON activity_events(hotkey);

                CREATE TABLE IF NOT EXISTS collateral_events (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num       INTEGER NOT NULL,
                    hotkey          TEXT NOT NULL,
                    collateral_rao  INTEGER NOT NULL,
                    backing         TEXT NOT NULL DEFAULT 'sol'
                );
                CREATE INDEX IF NOT EXISTS idx_collateral_events_block
                    ON collateral_events(block_num);
                CREATE INDEX IF NOT EXISTS idx_collateral_events_hotkey
                    ON collateral_events(hotkey);

                -- Per-swap realized legs from SwapCompleted. One row per completed
                -- swap; the windowed volume read sums these for reporting.
                -- from_amount/to_amount are TEXT because the on-chain legs are
                -- u128 and overflow SQLite's signed-64 INTEGER.
                CREATE TABLE IF NOT EXISTS clearing_rates (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num   INTEGER NOT NULL,
                    hotkey      TEXT NOT NULL,
                    from_chain  TEXT NOT NULL,
                    to_chain    TEXT NOT NULL,
                    from_amount TEXT NOT NULL,
                    to_amount   TEXT NOT NULL,
                    swap_key    TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_clearing_rates_dir_block
                    ON clearing_rates(from_chain, to_chain, block_num);
                -- Idempotency key: NULL only on pre-migration rows (SQLite UNIQUE
                -- admits multiple NULLs); every new insert carries the swap_key.
                CREATE UNIQUE INDEX IF NOT EXISTS idx_clearing_rates_swap_key
                    ON clearing_rates(swap_key);

                -- Terminal outcome per swap (SwapCompleted | SwapTimedOut), keyed by
                -- swap_key hex. Terminal swap PDAs are closed on-chain, so this is the
                -- seam's only way to tell a slash from a completion after close. Not
                -- the old B3.5 scoring ledger — scoring reads on-chain counters.
                CREATE TABLE IF NOT EXISTS swap_outcomes (
                    swap_key    TEXT PRIMARY KEY,
                    outcome     TEXT NOT NULL,
                    block_time  INTEGER NOT NULL
                );

                -- Delivery-leg tx hash per swap (SwapFulfilled), keyed by swap_key hex.
                -- The Swap PDA closes at terminal, so post-close receipts read the
                -- delivery hash from here. Same retention sweep as swap_outcomes.
                CREATE TABLE IF NOT EXISTS swap_fulfillments (
                    swap_key    TEXT PRIMARY KEY,
                    to_tx_hash  TEXT NOT NULL,
                    block_time  INTEGER NOT NULL
                );

                -- Routed reservation requests awaiting their draw (on-behalf flow).
                -- The ONLY table not rebuildable from chain events: the user's
                -- details live here alone until finalize_reservation publishes
                -- them. from_amount is TEXT (u128-safe), created_at is the FIFO key.
                CREATE TABLE IF NOT EXISTS routed_requests (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    miner          TEXT NOT NULL,
                    from_chain     TEXT NOT NULL,
                    to_chain       TEXT NOT NULL,
                    backing        TEXT NOT NULL DEFAULT 'sol',
                    user_pubkey    TEXT NOT NULL,
                    user_from_addr TEXT NOT NULL,
                    user_to_addr   TEXT NOT NULL,
                    from_amount    TEXT NOT NULL,
                    created_at     INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_routed_requests_key
                    ON routed_requests(miner, from_chain, to_chain, backing, user_pubkey);

                -- W3 bond relay. relay_swaps snapshots a live off-chain-backed swap's
                -- backing-chain user address: it is the vote_slash reimbursement target, the
                -- Swap PDA closes at the verdict, and no event carries it. relay_fees is the
                -- per-swap accrual half of "effective bond" (the vault's settled_total is the
                -- other). relay_slashes holds verdicts until the vault's permanent swap_ref
                -- marker confirms them — an unapplied row keeps netting AND blocks the miner's
                -- initiates. Amounts are rao (u64 on the vault's wire), SQLite-INTEGER-safe.
                CREATE TABLE IF NOT EXISTS relay_swaps (
                    swap_key    TEXT PRIMARY KEY,
                    miner       TEXT NOT NULL,
                    backing     TEXT NOT NULL,
                    user_addr   TEXT NOT NULL,
                    seen_at     INTEGER NOT NULL,
                    hotkey      TEXT
                );

                CREATE TABLE IF NOT EXISTS relay_fees (
                    swap_key    TEXT PRIMARY KEY,
                    miner       TEXT NOT NULL,
                    backing     TEXT NOT NULL,
                    fee         INTEGER NOT NULL,
                    block_time  INTEGER NOT NULL,
                    -- Which vault the fee is owed to. The vault's settled counter restarts at 0 on a
                    -- replacement, so summing across generations re-charges fees the retired vault
                    -- already collected.
                    vault_generation INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_relay_fees_miner
                    ON relay_fees(backing, miner, vault_generation);

                CREATE TABLE IF NOT EXISTS relay_slashes (
                    swap_key      TEXT PRIMARY KEY,
                    miner         TEXT NOT NULL,
                    backing       TEXT NOT NULL,
                    penalty       INTEGER NOT NULL,
                    reimbursement INTEGER NOT NULL,
                    user_addr     TEXT NOT NULL,
                    block_time    INTEGER NOT NULL,
                    applied       INTEGER NOT NULL DEFAULT 0,
                    hotkey        TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_relay_slashes_open
                    ON relay_slashes(backing, applied, miner);

                CREATE TABLE IF NOT EXISTS relay_meta (
                    key     TEXT PRIMARY KEY,
                    value   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS solana_event_meta (
                    key     TEXT PRIMARY KEY,
                    value   TEXT NOT NULL
                );
                """
            )
            conn.commit()
