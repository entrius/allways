import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

import bittensor as bt

EVENTS_LEVEL_NUM = 38
DEFAULT_LOG_BACKUP_COUNT = 10


def setup_events_logger(full_path, events_retention_size):
    logging.addLevelName(EVENTS_LEVEL_NUM, 'EVENT')

    logger = logging.getLogger('event')
    logger.setLevel(EVENTS_LEVEL_NUM)

    def event(self, message, *args, **kws):
        if self.isEnabledFor(EVENTS_LEVEL_NUM):
            self.log(EVENTS_LEVEL_NUM, message, args, **kws)

    logging.Logger.event = event

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    file_handler = RotatingFileHandler(
        os.path.join(full_path, 'events.log'),
        maxBytes=events_retention_size,
        backupCount=DEFAULT_LOG_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(EVENTS_LEVEL_NUM)
    logger.addHandler(file_handler)

    return logger


_last_seen: dict[str, Any] = {}


def log_on_change(key: str, value: Any, message: str) -> None:
    """Log a message only when ``value`` differs from the last call with the same ``key``."""
    if _last_seen.get(key) != value:
        _last_seen[key] = value
        bt.logging.info(message)


def miner_label(metagraph: Optional[Any], hotkey: str) -> str:
    """Return ``UID N / hotkey[:8]`` so a glance at any log line ties it to a UID.

    ``metagraph`` is optional so unit tests and offline tools can pass None
    and fall back to the hotkey-only label."""
    if not hotkey:
        return 'UID ? / ????????'
    uid: Any = '?'
    if metagraph is not None:
        try:
            uid = metagraph.hotkeys.index(hotkey)
        except (ValueError, IndexError, AttributeError):
            uid = '?'
    return f'UID {uid} / {hotkey[:8]}'


def swap_label(swap: Any, metagraph: Optional[Any] = None) -> str:
    """Format a swap log prefix: ``Swap #N [DIR UID … / hotkey]``."""
    direction = f'{(swap.from_chain or "?").upper()}->{(swap.to_chain or "?").upper()}'
    miner = miner_label(metagraph, getattr(swap, 'miner_hotkey', ''))
    return f'Swap #{swap.id} [{direction} {miner}]'


def log_crown_winners(
    metagraph: Any,
    block: int,
    snapshot: dict[tuple[str, str], list[tuple[str, str, str, float, float, int]]],
) -> None:
    """One greppable info line per forward pass naming the current crown holder
    UID and rate per direction:

        forward: crown holders @ block=N | btc->tao uid=42 rate=326.42 | tao->btc uid=17 rate=339.62

    Ties render as ``uid=42,55``. Empty directions render as ``btc->tao none``.
    ASCII arrows so ``grep 'crown holders'`` and ``grep 'btc->tao'`` work
    without copy-pasting unicode."""
    hotkey_to_uid = {hk: uid for uid, hk in enumerate(metagraph.hotkeys)}
    parts = [f'forward: crown holders @ block={block}']
    for (from_chain, to_chain), rows in snapshot.items():
        direction = f'{from_chain}->{to_chain}'
        if not rows:
            parts.append(f'{direction} none')
            continue
        uids = ','.join(str(hotkey_to_uid.get(row[2], '?')) for row in rows)
        rate = rows[0][4]
        parts.append(f'{direction} uid={uids} rate={rate:g}')
    bt.logging.info(' | '.join(parts))
