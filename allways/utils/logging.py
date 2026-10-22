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


_last_crown_holders: Optional[str] = None


def log_crown_winners(
    metagraph: Any,
    block: int,
    snapshot: dict[tuple[str, str, str], list[tuple[str, str, str, str, float, float, int]]],
) -> None:
    """One greppable line per forward pass naming the current crown holder UID and
    rate per HELD lane (direction + backing, F4), plus a count of the empty ones:

        forward: crown holders @ block=N | btc->sol[sol] uid=42 rate=326.42 | sol->tao[tao] uid=17 rate=2.1 | 4,118 empty lanes

    Ties render as ``uid=42,55``. ASCII arrows so ``grep 'crown holders'`` and
    ``grep 'btc->sol'`` work without copy-pasting unicode.

    INFO when the held lanes changed since the previous pass, DEBUG otherwise. Empty lanes
    are counted, not listed: the lane set is every direction × backing, thousands of them,
    and listing each as ``none`` made this line 145 KB every 20 s — 99.8% of the validator's
    log volume, rotating a 30 MB window down to ~90 minutes (2026-10-22, the #3901 post-mortem
    had no log to read)."""
    global _last_crown_holders
    hotkey_to_uid = {hk: uid for uid, hk in enumerate(metagraph.hotkeys)}
    held: list[str] = []
    empty = 0
    for (from_chain, to_chain, backing), rows in snapshot.items():
        if not rows:
            empty += 1
            continue
        uids = ','.join(str(hotkey_to_uid.get(row[3], '?')) for row in rows)
        rate = rows[0][5]
        held.append(f'{from_chain}->{to_chain}[{backing}] uid={uids} rate={rate:g}')
    body = ' | '.join(held) if held else 'no lane held'
    line = f'forward: crown holders @ block={block} | {body} | {empty:,} empty lanes'
    if body != _last_crown_holders:
        _last_crown_holders = body
        bt.logging.info(line)
    else:
        bt.logging.debug(line)
