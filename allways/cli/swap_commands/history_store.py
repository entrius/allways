"""Local swap history persistence for CLI receipts and listing."""

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from allways.classes import SwapStatus

HISTORY_FILE = Path.home() / '.allways' / 'swap_history.json'


def _read_history() -> List[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    try:
        data = json.loads(HISTORY_FILE.read_text())
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _write_history(records: List[Dict[str, Any]]) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(records, indent=2)
    fd, tmp_path = tempfile.mkstemp(dir=HISTORY_FILE.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(payload)
        os.replace(tmp_path, HISTORY_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def _status_name(value: Any) -> str:
    if isinstance(value, SwapStatus):
        return value.name
    if isinstance(value, str):
        return value.upper()
    return str(value)


def make_pending_key(miner_hotkey: str, created_at: float) -> str:
    return f'{miner_hotkey}:{int(created_at)}'


def _find_index(
    records: List[Dict[str, Any]], swap_id: Optional[int] = None, pending_key: Optional[str] = None
) -> Optional[int]:
    if swap_id is not None:
        for idx, record in enumerate(records):
            if record.get('swap_id') == swap_id:
                return idx
    if pending_key:
        for idx, record in enumerate(records):
            if record.get('pending_key') == pending_key:
                return idx
    return None


def upsert_history(
    *,
    swap_id: Optional[int] = None,
    pending_key: Optional[str] = None,
    data: Dict[str, Any],
) -> None:
    records = _read_history()
    idx = _find_index(records, swap_id=swap_id, pending_key=pending_key)
    now = int(time.time())

    if idx is None:
        record = {'created_at': now, 'updated_at': now}
        if swap_id is not None:
            record['swap_id'] = swap_id
        if pending_key:
            record['pending_key'] = pending_key
        record.update(data)
        records.append(record)
    else:
        existing = records[idx]
        if swap_id is not None:
            existing['swap_id'] = swap_id
        if pending_key:
            existing['pending_key'] = pending_key
        existing.update(data)
        existing['updated_at'] = now
        records[idx] = existing

    _write_history(records)


def get_history(limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
    records = _read_history()
    if status:
        expected = status.upper()
        records = [r for r in records if _status_name(r.get('status', '')) == expected]
    records.sort(key=lambda r: r.get('updated_at', 0), reverse=True)
    return records[:limit]


def get_receipt(swap_id: int) -> Optional[Dict[str, Any]]:
    records = _read_history()
    for record in records:
        if record.get('swap_id') == swap_id:
            return record
    return None
