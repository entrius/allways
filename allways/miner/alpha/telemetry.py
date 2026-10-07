"""Miner telemetry: a live state.json for the dashboard, an events.jsonl ledger and the guarded operator alert.
No-op until configured."""

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from allways.miner.alpha.ledger import Ledger

EVENTS_FILE = 'events.jsonl'
Alert = Callable[[str, str, str], None]  # (key, title, body)


def guarded_alert(alert: Alert | None, logger: logging.Logger) -> Alert:
    """The operator alert as a call that never aborts the tick raising it: a notifier error is a warning."""

    def guarded(key: str, title: str, body: str) -> None:
        if alert is None:
            return
        try:
            alert(key, title, body)
        except Exception as error:
            logger.warning('Alert failed key=%s: %s', key, error)

    return guarded


class Telemetry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, dict[str, Any]] = {}
        self._state_path: Path | None = None
        self._events_path: Path | None = None
        self.started_at = time.time()

    def configure(self, log_dir: Path) -> None:
        self._state_path = log_dir / 'state.json'
        self._events_path = log_dir / EVENTS_FILE
        previous = Ledger(self._state_path).load()
        health = previous.get('health') if isinstance(previous, dict) else None
        if isinstance(health, dict):
            with self._lock:
                self._state.setdefault('health', {}).update(health)  # last known lanes table; the probes overwrite it

    def put(self, section: str, key: str, value: Any) -> None:
        with self._lock:
            self._state.setdefault(section, {})[key] = value

    def drop(self, section: str, key: str) -> None:
        with self._lock:
            self._state.get(section, {}).pop(key, None)

    def event(self, kind: str, **fields: Any) -> None:
        if self._events_path is None:
            return
        line = json.dumps({'ts': int(time.time()), 'kind': kind, **fields}, default=str)
        with self._lock, self._events_path.open('a') as handle:
            handle.write(line + '\n')

    def flush(self, **top: Any) -> None:
        if self._state_path is None:
            return
        with self._lock:
            payload = {'ts': int(time.time()), 'uptime_secs': int(time.time() - self.started_at), **top, **self._state}
            # Serialise under the lock: the copy above is shallow, so a live section dict can mutate mid-dump.
            encoded = json.dumps(payload, default=str)
        tmp = self._state_path.with_suffix('.tmp')
        tmp.write_text(encoded)
        os.replace(tmp, self._state_path)


telemetry = Telemetry()
