"""What one hub's relay owes its vault: slash verdicts not yet applied, and each miner's fee total.

The fee total is cumulative per (miner, vault generation), because the vault's fee batch takes
cumulative totals and every validator must vote the same vector. It is kept as a base plus the fee
rows since the base was last folded, not as all of history: a miner's base is seeded from the vault's
settled counter (which covers every fee up to the last fee boundary) and only rises when the vault
has settled more than this ledger counted. A validator with a fresh database may count short until the
next boundary settles; it never counts a fee twice."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

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
        (
            f'{p}_fee_base',
            f'CREATE TABLE IF NOT EXISTS {p}_fee_base (miner TEXT NOT NULL, generation INTEGER NOT NULL, '
            'base INTEGER NOT NULL, as_of INTEGER NOT NULL, PRIMARY KEY (miner, generation))',
        ),
    ]


class RelayLedger:
    """The relay engine's store, under the method names it calls. ``settled`` reads the vault's settled
    counter for a miner (None when unreadable); ``boundary`` is the last fee boundary at or before a time."""

    def __init__(
        self,
        store: Store,
        hub: str,
        settled: Optional[Callable[[str], Optional[int]]] = None,
        boundary: Callable[[int], int] = lambda t: t,
        clock: Callable[[], float] = lambda: 0.0,
    ) -> None:
        self.store, self.hub = store, hub
        self.settled, self.boundary, self.clock = settled, boundary, clock
        self._t = f'relay_{hub}'
        store.declare(self._t, 1, _tables(hub))

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
        self._exec(f'UPDATE {self._t}_slashes SET applied = 1 WHERE swap_key = ?', (swap_key,))

    def get_relay_swap(self, swap_key: str) -> None:
        return None  # the verdict carries its payee and pinned hotkey; no live snapshot is kept

    def prune_relay_swaps(self, cutoff: int) -> None:
        self._exec(f'DELETE FROM {self._t}_slashes WHERE applied = 1 AND block_time < ?', (int(cutoff),))

    # ── fees ────────────────────────────────────────────────────────────────

    def record_relay_fee(self, swap_key, miner, backing, fee, block_time, vault_generation=0) -> None:
        self._exec(
            f'INSERT OR IGNORE INTO {self._t}_fees VALUES (?, ?, ?, ?, ?)',
            (swap_key, miner, int(vault_generation), int(fee), int(block_time)),
        )

    def _base(self, miner: str, generation: int) -> Optional[Any]:
        rows = self._rows(f'SELECT * FROM {self._t}_fee_base WHERE miner = ? AND generation = ?', (miner, generation))
        return rows[0] if rows else None

    def _counted(self, miner: str, generation: int, after: int, upto: Optional[int] = None) -> int:
        sql = f'SELECT COALESCE(SUM(fee), 0) AS s FROM {self._t}_fees WHERE miner = ? AND generation = ? AND at > ?'
        params: tuple = (miner, generation, after)
        if upto is not None:
            sql, params = sql + ' AND at <= ?', params + (upto,)
        return int(self._rows(sql, params)[0]['s'])

    def accrued_fee_total(self, miner, backing, at_time=None, vault_generation=None) -> int:
        """Cumulative fees ``miner`` owes this vault generation, as of ``at_time`` if given. Seeds the base
        from the vault on first use, and raises it when the vault has settled more than counted. With no
        generation, the sum over every generation (who has ever owed, not what is owed now)."""
        if vault_generation is None:
            return sum(self._total(miner, g, at_time) for g in self._generations(miner))
        generation = int(vault_generation)
        settled = self.settled(miner) if self.settled is not None else None
        base = self._base(miner, generation)
        if base is None:
            if settled is None:
                raise RuntimeError(f'vault settled total unreadable for {miner[:8]}: cannot seed its fee total')
            as_of = self.boundary(int(self.clock()))
            self._exec(f'INSERT INTO {self._t}_fee_base VALUES (?, ?, ?, ?)', (miner, generation, int(settled), as_of))
            base = self._base(miner, generation)
        last = self.boundary(int(self.clock()))
        # Settled covers fees up to the last boundary, except an exit's residual, which settles past it.
        if settled is not None and base['as_of'] <= last and self.get_relay_meta(f'exit:{miner}') is None:
            short = int(settled) - (base['base'] + self._counted(miner, generation, base['as_of'], last))
            if short > 0:  # the vault settled fees this ledger never saw: count them from here on
                self._exec(
                    f'UPDATE {self._t}_fee_base SET base = base + ? WHERE miner = ? AND generation = ?',
                    (short, miner, generation),
                )
                base = self._base(miner, generation)
        return self._total(miner, generation, at_time)

    def _generations(self, miner: str) -> List[int]:
        sql = f'SELECT generation FROM {self._t}_fee_base WHERE miner = ? UNION SELECT generation FROM {self._t}_fees WHERE miner = ?'
        return [r['generation'] for r in self._rows(sql, (miner, miner))]

    def _total(self, miner: str, generation: int, at_time: Optional[int] = None) -> int:
        base = self._base(miner, generation)
        if base is None:
            return self._counted(miner, generation, -1, at_time)
        return int(base['base']) + self._counted(miner, generation, base['as_of'], at_time)

    def accrued_fee_totals(self, backing, at_time=None, vault_generation=None) -> Dict[str, int]:
        """Every miner's total for the fee batch: each one with a base or a fee row (seeding the latter)."""
        sql = f'SELECT miner FROM {self._t}_fee_base UNION SELECT miner FROM {self._t}_fees'
        if vault_generation is not None:
            sql = (
                f'SELECT miner FROM {self._t}_fee_base WHERE generation = ? '
                f'UNION SELECT miner FROM {self._t}_fees WHERE generation = ?'
            )
        params = () if vault_generation is None else (int(vault_generation),) * 2
        miners = [r['miner'] for r in self._rows(sql, params)]
        if vault_generation is None:  # discovery only: no seeding
            return {m: sum(self._total(m, g, at_time) for g in self._generations(m)) for m in miners}
        return {m: self.accrued_fee_total(m, backing, at_time, vault_generation) for m in miners}

    def fold(self, cutoff: int) -> None:
        """Move fee rows at or before ``cutoff`` into their miner's base and drop them. Rows of a miner
        with no base yet are kept: its seed decides which of them the vault already holds."""
        with self.store.tx() as c:
            for row in c.execute(f'SELECT * FROM {self._t}_fee_base WHERE as_of < ?', (cutoff,)).fetchall():
                add = self._counted(row['miner'], row['generation'], row['as_of'], cutoff)
                c.execute(
                    f'UPDATE {self._t}_fee_base SET base = base + ?, as_of = ? WHERE miner = ? AND generation = ?',
                    (add, cutoff, row['miner'], row['generation']),
                )
            c.execute(
                f'DELETE FROM {self._t}_fees WHERE at <= ? AND EXISTS (SELECT 1 FROM {self._t}_fee_base b '
                f'WHERE b.miner = {self._t}_fees.miner AND b.generation = {self._t}_fees.generation)',
                (cutoff,),
            )

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
