"""Pushed program events over a Solana WebSocket: `logsSubscribe(mentions=[account])` on the RPC's wss twin,
decoded and dispatched per event name, reconnecting with jittered backoff. The account defaults to the program;
a miner narrows it to its own pubkey. The feed is latency, not truth — consumers keep a catch-up for gaps."""

import asyncio
import base64
import json
import random
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

import bittensor as bt

from allways.solana.events import PROGRAM_DATA_PREFIX, decode_event

Handler = Callable[[str, Any], None]

RECONNECT_MIN_SECS = 1.0
RECONNECT_MAX_SECS = 30.0


def _mask(url: str) -> str:
    head, sep, _ = url.partition('api-key=')
    return f'{head}{sep}…' if sep else url


def events_from_logs(logs: List[str]) -> List[tuple]:
    """Decode the program's `Program data:` lines of one tx into [(name, event)], skipping foreign/unknown."""
    out = []
    for line in logs or []:
        if not line.startswith(PROGRAM_DATA_PREFIX):
            continue
        try:
            decoded = decode_event(base64.b64decode(line[len(PROGRAM_DATA_PREFIX) :]))
        except Exception:
            continue
        if decoded is not None:
            out.append(decoded)
    return out


class ProgramEventFeed:
    """Handlers run on the feed thread (keep them short). `connected` is set once the node acks the subscribe;
    `session` counts acks so a consumer can catch up after each one. ``max_session_secs`` resubscribes on a
    schedule — pings prove the socket, not that the node still delivers."""

    def __init__(self, ws_url: str, program_id, mentions=None, max_session_secs: Optional[float] = None) -> None:
        self.ws_url = ws_url
        self.program_id = str(program_id)
        self.mentions = str(mentions) if mentions is not None else self.program_id
        self.max_session_secs = max_session_secs
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._connected = threading.Event()
        self._session_count = 0
        self._thread: Optional[threading.Thread] = None

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def session(self) -> int:
        """Acknowledged subscriptions so far; a change means events may have been missed in between."""
        return self._session_count

    def on(self, event_name: str, handler: Handler) -> None:
        self._handlers[event_name].append(handler)

    def start(self) -> 'ProgramEventFeed':
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name='program-feed', daemon=True)
            self._thread.start()
        return self

    # ── dispatch ────────────────────────────────────────────────────────────

    def handle_notification(self, msg: dict) -> int:
        """Dispatch one logsNotification frame; a failed tx commits no events. Returns events dispatched."""
        value = (((msg or {}).get('params') or {}).get('result') or {}).get('value') or {}
        if value.get('err') is not None:
            return 0
        n = 0
        for name, event in events_from_logs(value.get('logs')):
            for handler in self._handlers.get(name, ()):
                try:
                    handler(name, event)
                except Exception as e:
                    bt.logging.warning(f'program feed: {name} handler failed: {e}')
            n += 1
        return n

    # ── transport ───────────────────────────────────────────────────────────

    def handle_frame(self, msg: dict) -> None:
        """One inbound frame: the subscribe ack (id 1) marks the feed live; notifications dispatch."""
        if msg.get('id') == 1:
            if 'error' in msg:
                raise ConnectionError(f'logsSubscribe rejected: {msg["error"]}')
            self._session_count += 1
            self._connected.set()
            log = bt.logging.info if self._session_count == 1 else bt.logging.debug
            log(f'program feed: logsSubscribe({self.mentions}) @ {_mask(self.ws_url)} (session {self._session_count})')
        elif msg.get('method') == 'logsNotification':
            self.handle_notification(msg)

    async def _session(self) -> None:
        import websockets

        sub = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'logsSubscribe',
            'params': [{'mentions': [self.mentions]}, {'commitment': 'confirmed'}],
        }
        deadline = time.monotonic() + self.max_session_secs if self.max_session_secs else None
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
            await ws.send(json.dumps(sub))
            while True:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return  # scheduled resubscribe
                try:
                    raw = await asyncio.wait_for(ws.recv(), remaining)
                except asyncio.TimeoutError:
                    return
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                self.handle_frame(msg)

    def _run(self) -> None:
        backoff = RECONNECT_MIN_SECS
        failures = 0
        while True:
            try:
                asyncio.run(self._session())
                backoff = RECONNECT_MIN_SECS
                failures = 0
            except Exception as e:
                failures += 1
                # An endpoint with no WebSocket fails every attempt; say so once, not every 30 s.
                log = bt.logging.warning if failures == 1 else bt.logging.debug
                log(f'program feed: socket down ({e}); reconnecting in ~{backoff:.0f}s')
            finally:
                self._connected.clear()
            threading.Event().wait(backoff * random.uniform(0.8, 1.2))
            backoff = min(backoff * 2, RECONNECT_MAX_SECS)
