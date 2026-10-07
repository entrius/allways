"""One journal row per alpha swap, on disk before every chain submit."""

import threading
import time
from copy import deepcopy

from allways.miner.alpha.ledger import Ledger

ALPHA_JOURNAL_NAME = 'alpha.json'
SIZED, BUY_SENT, HELD, XFER_SENT, SETTLED, FAILED = 'SIZED', 'BUY_SENT', 'HELD', 'XFER_SENT', 'SETTLED', 'FAILED'
STATES = frozenset({SIZED, BUY_SENT, HELD, XFER_SENT, SETTLED, FAILED})
TERMINAL = frozenset({SETTLED, FAILED})
RESERVING = frozenset({SIZED, BUY_SENT})  # a buy's reserved free TAO is spent once the position is held
_PRUNE_GRACE_SECS = 86_400


def _checked(fields: dict) -> dict:
    if 'state' in fields and fields['state'] not in STATES:
        raise ValueError(f'unknown alpha journal state {fields["state"]!r}')
    return fields


def _loaded(ledger: Ledger) -> dict[str, dict]:
    rows = ledger.load()
    if rows or not ledger.path.exists() or ledger.path.stat().st_size <= len('{}'):
        return rows
    raise RuntimeError(f'alpha journal unreadable path={ledger.path}: a recorded send may still land')


class AlphaJournal:
    """Thread-safe rows keyed by swap key; callers only ever hold copies."""

    def __init__(self, ledger: Ledger) -> None:
        self.ledger = ledger
        self._lock = threading.Lock()
        self._rows = _loaded(ledger)

    def row(self, key: str) -> dict | None:
        with self._lock:
            return deepcopy(self._rows.get(key))

    def open(self, key: str, netuid: int, need: int, hotkey: str, deadline: int) -> dict:
        """Start a swap in SIZED once; an existing row, whatever its state, comes back unchanged."""
        with self._lock:
            if key not in self._rows:
                self._rows[key] = {
                    'netuid': netuid,
                    'need': need,
                    'hotkey': hotkey,
                    'deadline': deadline,
                    'state': SIZED,
                    'buys': [],
                    'xfer': None,
                    'reserved_rao': 0,
                    'tops': 0,
                }
                self._commit(key)
            return deepcopy(self._rows[key])

    def update(self, key: str, **fields) -> dict:
        with self._lock:
            self._rows[key].update(_checked(fields))
            self._commit(key)
            return deepcopy(self._rows[key])

    def transition(self, key: str, from_state: str, **fields) -> bool:
        """Compare-and-set: the fields are saved only while the row is still in from_state."""
        with self._lock:
            if self._rows[key]['state'] != from_state:
                return False
            self._rows[key].update(_checked(fields))
            self._commit(key)
            return True

    def add_buy(self, key: str, buy: dict) -> None:
        with self._lock:
            self._rows[key]['buys'].append(dict(buy))
            self._commit(key)

    def set_buy(self, key: str, index: int, **fields) -> None:
        with self._lock:
            self._rows[key]['buys'][index].update(fields)
            self._commit(key)

    def open_rows(self) -> dict[str, dict]:
        with self._lock:
            return {key: deepcopy(row) for key, row in self._rows.items() if row['state'] not in TERMINAL}

    def open_need(self, netuid: int) -> int:
        return sum(row['need'] for row in self.open_rows().values() if row['netuid'] == netuid)

    def reserved_rao(self) -> int:
        with self._lock:
            return sum(row['reserved_rao'] for row in self._rows.values() if row['state'] in RESERVING)

    def prune(self, now: float) -> None:
        """Forget terminal rows a day past their deadline; a live row stays whatever its age."""
        with self._lock:
            stale = [
                key
                for key, row in self._rows.items()
                if row['state'] in TERMINAL and row['deadline'] + _PRUNE_GRACE_SECS < now
            ]
            for key in stale:
                del self._rows[key]
            if stale:
                self.ledger.save(self._rows)

    def _commit(self, key: str) -> None:
        self._rows[key]['updated_at'] = int(time.time())
        self.ledger.save(self._rows)
