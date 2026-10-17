"""What serving keeps: the routed-request queue, and each swap's terminal outcome after its account closes.

Served to the lifted request ops under the old store's method names. Facts the World already holds
(claimed deposits, reported deliveries, payout routes) and the actuator already priced (collateral
verdicts) are read from there, not kept twice."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from allways.constants import SWAP_OUTCOME_RETENTION_SECS
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import World, tx_key

OWNER, VERSION = 'serving', 1
TABLES = [
    (
        'serving_routed',
        'CREATE TABLE IF NOT EXISTS serving_routed (miner TEXT NOT NULL, from_chain TEXT NOT NULL, '
        'to_chain TEXT NOT NULL, backing TEXT NOT NULL, user_pubkey TEXT NOT NULL, user_from_addr TEXT NOT NULL, '
        'user_to_addr TEXT NOT NULL, from_amount TEXT NOT NULL, created_at INTEGER NOT NULL, '
        'PRIMARY KEY (miner, from_chain, to_chain, backing, user_pubkey))',
    ),
    (
        'serving_outcomes',
        'CREATE TABLE IF NOT EXISTS serving_outcomes (swap_key TEXT PRIMARY KEY, outcome TEXT, to_tx_hash TEXT, '
        'refund_chain TEXT, refund_amount TEXT, refund_tx TEXT, at INTEGER NOT NULL)',
    ),
]

# Terminal events → the stage /status reports once the Swap account is closed.
OUTCOMES = {'SwapCompleted': 'completed', 'SwapTimedOut': 'timed_out', 'SwapCancelled': 'cancelled'}
OUTCOMES['StaleClaimClosed'] = 'expired'  # a claim reaped before attestation: no funds moved


class ServingStore:
    def __init__(
        self, store: Store, world: Callable[[], Optional[World]], memory: Any, providers: Dict[str, Any]
    ) -> None:
        self.store, self._world, self.memory, self.providers = store, world, memory, providers
        store.declare(OWNER, VERSION, TABLES)
        self._claimed: Dict[str, Tuple[str, int]] = {}  # deposits this validator claimed, until the World has them

    def _rows(self, sql: str, params=()) -> List[Any]:
        return self.store.conn().execute(sql, params).fetchall()

    def _exec(self, sql: str, params=()) -> None:
        self.store.conn().execute(sql, params)

    # ── outcomes, from program events ───────────────────────────────────────

    def ingest(self, record: Any) -> None:
        f, at = record.fields, int(record.block_time or 0)
        if record.name == 'SwapFulfilled':
            key = bytes(f['swap_key']).hex()
            self._exec(
                'INSERT INTO serving_outcomes (swap_key, to_tx_hash, at) VALUES (?, ?, ?) '
                'ON CONFLICT(swap_key) DO UPDATE SET to_tx_hash = excluded.to_tx_hash',
                (key, str(f['to_tx_hash']), at),
            )
        elif record.name in OUTCOMES:
            refund: Tuple[Any, ...] = (None, None, None)
            if record.name == 'SwapTimedOut':
                chain = str(f.get('collateral_chain') or 'sol').lower()
                # An off-chain backing refunds on its own chain: no Solana signature proves that payout.
                refund = (chain, str(int(f['reimbursement'])), record.signature if chain == 'sol' else None)
            self._exec(
                'INSERT INTO serving_outcomes (swap_key, outcome, refund_chain, refund_amount, refund_tx, at) '
                'VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(swap_key) DO UPDATE SET outcome = excluded.outcome, '
                'refund_chain = COALESCE(excluded.refund_chain, refund_chain), '
                'refund_amount = COALESCE(excluded.refund_amount, refund_amount), '
                'refund_tx = COALESCE(excluded.refund_tx, refund_tx), at = excluded.at',
                (bytes(f['swap_key']).hex(), OUTCOMES[record.name], *refund, at),
            )

    def get_swap_outcome(self, swap_key: str) -> Optional[str]:
        rows = self._rows('SELECT outcome FROM serving_outcomes WHERE swap_key = ?', (swap_key,))
        return rows[0]['outcome'] if rows else None

    def get_swap_fulfillment(self, swap_key: str) -> Optional[str]:
        rows = self._rows('SELECT to_tx_hash FROM serving_outcomes WHERE swap_key = ?', (swap_key,))
        return rows[0]['to_tx_hash'] if rows else None

    def get_swap_refund(self, swap_key: str) -> Optional[dict]:
        rows = self._rows('SELECT * FROM serving_outcomes WHERE swap_key = ?', (swap_key,))
        if not rows or rows[0]['refund_amount'] is None:
            return None
        row = rows[0]
        refund: Dict[str, Any] = {'refund_amount': int(row['refund_amount']), 'refund_chain': row['refund_chain']}
        if row['refund_tx'] is not None:
            refund['refund_tx_hash'] = row['refund_tx']
        return refund

    # ── the routed-request queue ────────────────────────────────────────────

    def upsert_routed_request(
        self, miner, from_chain, to_chain, backing, user_pubkey, user_from_addr, user_to_addr, from_amount, created_at
    ):
        """A retry from the same user keeps its place in the queue: only its addresses and amount refresh."""
        self._exec(
            'INSERT INTO serving_routed VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(miner, from_chain, to_chain, '
            'backing, user_pubkey) DO UPDATE SET user_from_addr = excluded.user_from_addr, '
            'user_to_addr = excluded.user_to_addr, from_amount = excluded.from_amount',
            (
                miner,
                from_chain,
                to_chain,
                backing,
                user_pubkey,
                user_from_addr,
                user_to_addr,
                str(int(from_amount)),
                int(created_at),
            ),
        )

    def pending_routed_requests(self, miner, from_chain, to_chain, backing='sol') -> List[dict]:
        rows = self._rows(
            'SELECT * FROM serving_routed WHERE miner = ? AND from_chain = ? AND to_chain = ? AND backing = ? '
            'ORDER BY created_at, rowid',
            (miner, from_chain, to_chain, backing),
        )
        keys = ('user_pubkey', 'user_from_addr', 'user_to_addr', 'created_at')
        return [{**{k: r[k] for k in keys}, 'from_amount': int(r['from_amount'])} for r in rows]

    def distinct_routed_pools(self) -> List[Tuple[str, str, str, str]]:
        rows = self._rows('SELECT DISTINCT miner, from_chain, to_chain, backing FROM serving_routed')
        return [(r['miner'], r['from_chain'], r['to_chain'], r['backing']) for r in rows]

    def delete_routed_requests(self, miner, from_chain, to_chain, backing='sol') -> None:
        self._exec(
            'DELETE FROM serving_routed WHERE miner = ? AND from_chain = ? AND to_chain = ? AND backing = ?',
            (miner, from_chain, to_chain, backing),
        )

    def delete_routed_request(self, miner, from_chain, to_chain, backing, user_pubkey) -> None:
        self._exec(
            'DELETE FROM serving_routed WHERE miner = ? AND from_chain = ? AND to_chain = ? AND backing = ? '
            'AND user_pubkey = ?',
            (miner, from_chain, to_chain, backing, user_pubkey),
        )

    def prune_routed_requests(self, cutoff: int) -> None:
        self._exec('DELETE FROM serving_routed WHERE created_at < ?', (int(cutoff),))

    def prune(self, now: int) -> None:
        self._exec('DELETE FROM serving_outcomes WHERE at < ?', (now - SWAP_OUTCOME_RETENTION_SECS,))
        self._claimed = {k: v for k, v in self._claimed.items() if v[1] >= now - 3600}

    # ── read through to the World and the actuator ──────────────────────────

    def deposit_claim(self, tx_hash: str) -> Optional[Tuple[str, bool]]:
        world = self._world()
        held = world.deposits.get(tx_key(tx_hash)) if world is not None else None
        if held is not None:
            return held.swap_key, held.initiated
        mine = self._claimed.get(tx_key(tx_hash))
        return (mine[0], False) if mine else None

    def record_deposit_claim(self, tx_hash: str, swap_key: str, seen_at: int) -> None:
        """Held until the SwapClaimed event reaches the World, so a second request can't race it."""
        self._claimed[tx_key(tx_hash)] = (swap_key, int(seen_at))

    def is_payout_route(self, chain: str, sender: str, recipient: str) -> bool:
        """``sender`` and ``recipient`` come normalized; the World keeps routes as posted."""
        world, provider = self._world(), self.providers.get(chain)
        norm = provider.chain.normalize_address if provider is not None else (lambda a: a)
        routes = world.routes.values() if world is not None else ()
        return any(r.chain == chain and norm(r.sender) == sender and norm(r.recipient) == recipient for r in routes)

    def is_recorded_payout(self, tx_hash: str) -> bool:
        key, world = tx_key(tx_hash), self._world()
        reported: Set[str] = (
            {tx_key(f.to_tx_hash) for f in world.fulfillments.values() if f.to_tx_hash} if world else set()
        )
        return key in reported or any(k == key for _, k in self.memory.deliveries)

    def collateral_verdict(self, miner, backing, created_at, collateral_amount) -> Optional[bool]:
        return self.memory.collateral.get((str(miner), str(backing), int(created_at), int(collateral_amount)))

    def record_collateral_verdict(self, miner, backing, created_at, collateral_amount, ok) -> None:
        self.memory.record_collateral((str(miner), str(backing), int(created_at), int(collateral_amount)), bool(ok))
