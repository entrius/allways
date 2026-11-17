"""What one hub's relay owes its vault: slash verdicts not yet applied, and the recent fee events.

A miner's cumulative fee total is not kept here: the program accrues it on ``MinerState.accrued_fees``
at confirm quorum, so every validator reads the same figure off chain whatever history it has seen
(``BondRelay.accrued_fees``). The fee rows here are only what that read can't give: the fees confirmed
after a past boundary, so the true-up can name the total AS OF its boundary. They are pruned past the
event feed's history horizon, which a fresh validator replays, so an old and a fresh database agree."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from allways.validator.store.db import Store


def _tables(hub: str):
    p = f'relay_{hub}'
    return [
        (
            f'{p}_slashes',
            f'CREATE TABLE IF NOT EXISTS {p}_slashes (swap_key TEXT PRIMARY KEY, miner TEXT NOT NULL, '
            'backing TEXT NOT NULL, penalty INTEGER NOT NULL, reimbursement INTEGER NOT NULL, user_addr TEXT NOT NULL, '
            'block_time INTEGER NOT NULL, hotkey TEXT, applied INTEGER NOT NULL DEFAULT 0)',
        ),
        (
            f'{p}_fees',
            f'CREATE TABLE IF NOT EXISTS {p}_fees (swap_key TEXT PRIMARY KEY, miner TEXT NOT NULL, '
            'generation INTEGER NOT NULL, fee INTEGER NOT NULL, at INTEGER NOT NULL)',
        ),
    ]


class RelayLedger:
    """The relay engine's store, under the method names it calls."""

    def __init__(self, store: Store, hub: str) -> None:
        self.store, self.hub = store, hub
        self._t = f'relay_{hub}'
        store.declare(self._t, 1, _tables(hub), consumer=f'relay-{hub}')  # the relay component's feed position
        # The pre-v17 fee base (a local guess at what the vault had settled) is gone with the counters.
        store.conn().execute(f'DROP TABLE IF EXISTS {self._t}_fee_base')

    def _rows(self, sql: str, params=()) -> List[Any]:
        return self.store.conn().execute(sql, params).fetchall()

    def _exec(self, sql: str, params=()) -> None:
        self.store.conn().execute(sql, params)

    # ── slash verdicts ──────────────────────────────────────────────────────

    def record_relay_slash(self, swap_key, miner, backing, penalty, reimbursement, user_addr, block_time, hotkey=None):
        self._exec(
            f'INSERT OR IGNORE INTO {self._t}_slashes VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)',
            (swap_key, miner, backing, int(penalty), int(reimbursement), user_addr, int(block_time), hotkey),
        )

    def open_relay_slashes(self, backing: str, miner: Optional[str] = None) -> List[dict]:
        sql = f'SELECT * FROM {self._t}_slashes WHERE applied = 0 AND backing = ?'
        params: tuple = (backing,)
        if miner is not None:
            sql, params = sql + ' AND miner = ?', params + (miner,)
        return [dict(r) for r in self._rows(sql + ' ORDER BY block_time, swap_key', params)]

    def mark_relay_slash_applied(self, swap_key: str) -> None:
        self._exec(f'UPDATE {self._t}_slashes SET applied = 1 WHERE swap_key = ? AND applied = 0', (swap_key,))

    # A verdict's ``applied`` runs 0 → 1 → 2: owed to the vault, seized on the vault but not yet visible
    # in Solana's BondAttestation, and reflected there too. Only the last state frees the miner's entry.

    def unreflected_relay_slashes(self, backing: str, miner: Optional[str] = None) -> List[dict]:
        sql = f'SELECT * FROM {self._t}_slashes WHERE applied = 1 AND backing = ?'
        params: tuple = (backing,)
        if miner is not None:
            sql, params = sql + ' AND miner = ?', params + (miner,)
        return [dict(r) for r in self._rows(sql + ' ORDER BY block_time, swap_key', params)]

    def mark_relay_slashes_reflected(self, backing: str, miner: str) -> None:
        self._exec(
            f'UPDATE {self._t}_slashes SET applied = 2 WHERE applied = 1 AND backing = ? AND miner = ?',
            (backing, miner),
        )

    def owes_relay_debit(self, backing: str, miner: str) -> bool:
        """Whether a verdict against ``miner`` is unapplied on the vault or not yet reflected on Solana."""
        sql = f'SELECT 1 FROM {self._t}_slashes WHERE applied < 2 AND backing = ? AND miner = ? LIMIT 1'
        return bool(self._rows(sql, (backing, miner)))

    def get_relay_swap(self, swap_key: str) -> None:
        return None  # the verdict carries its payee and pinned hotkey; no live snapshot is kept

    def prune_relay_swaps(self, cutoff: int) -> None:
        self._exec(f'DELETE FROM {self._t}_slashes WHERE applied >= 1 AND block_time < ?', (int(cutoff),))

    # ── fees ────────────────────────────────────────────────────────────────

    def record_relay_fee(self, swap_key, miner, backing, fee, block_time, vault_generation=0) -> None:
        self._exec(
            f'INSERT OR IGNORE INTO {self._t}_fees VALUES (?, ?, ?, ?, ?)',
            (swap_key, miner, int(vault_generation), int(fee), int(block_time)),
        )

    def fees_after(self, miner: str, generation: int, after: int) -> int:
        """The fees ``miner`` was confirmed for in ``generation`` after ``after``: what the on-chain
        counter holds beyond the total as of that time."""
        sql = f'SELECT COALESCE(SUM(fee), 0) AS s FROM {self._t}_fees WHERE miner = ? AND generation = ? AND at > ?'
        return int(self._rows(sql, (miner, int(generation), int(after)))[0]['s'])

    def prune_relay_fees(self, cutoff: int) -> None:
        self._exec(f'DELETE FROM {self._t}_fees WHERE at <= ?', (int(cutoff),))

    # ── meta ────────────────────────────────────────────────────────────────

    def _key(self, key: str) -> str:
        return f'{self._t}.{key}'

    def get_relay_meta(self, key: str) -> Optional[str]:
        return self.store.get_meta(self._key(key))

    def set_relay_meta(self, key: str, value: Any) -> None:
        self._exec('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)', (self._key(key), str(value)))

    def delete_relay_meta(self, key: str) -> None:
        self._exec('DELETE FROM meta WHERE key = ?', (self._key(key),))

    def relay_meta_prefix(self, prefix: str) -> Dict[str, str]:
        full = self._key(prefix)
        rows = self._rows('SELECT key, value FROM meta WHERE key LIKE ?', (f'{full}%',))
        return {r['key'][len(full) :]: r['value'] for r in rows}
