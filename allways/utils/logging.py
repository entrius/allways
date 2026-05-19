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
