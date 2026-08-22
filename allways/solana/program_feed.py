"""Pushed program events over a Solana WebSocket (`logsSubscribe`).

One daemon thread holds a `logsSubscribe(mentions=[program])` session on the RPC's wss twin (a keyed Helius
HTTP URL derives its keyed wss endpoint), decodes every `Program data:` line through the canonical event
decoder, and hands (name, event) to the handlers registered per event name. A dropped socket reconnects
with jittered backoff; a handler exception is logged, never fatal. Consumers that need "did I miss
something while the socket was down" keep their own poll as the backstop — the feed is latency, not truth.
"""

import asyncio
import base64
import json
import random
import threading
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
    """`logsSubscribe` on one program, dispatched to per-event handlers on the feed thread (keep them short —
    hand real work to a timer/executor). `connected` tells a consumer whether to trust the push path or
    fall back to polling."""

    def __init__(self, ws_url: str, program_id) -> None:
        self.ws_url = ws_url
        self.program_id = str(program_id)
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._connected = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

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

    async def _session(self) -> None:
        import websockets

        sub = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'logsSubscribe',
            'params': [{'mentions': [self.program_id]}, {'commitment': 'confirmed'}],
        }
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
            await ws.send(json.dumps(sub))
            self._connected.set()
            bt.logging.info(f'program feed: logsSubscribe({self.program_id}) @ {_mask(self.ws_url)}')
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if msg.get('method') == 'logsNotification':
                    self.handle_notification(msg)

    def _run(self) -> None:
        backoff = RECONNECT_MIN_SECS
        while True:
            try:
                asyncio.run(self._session())
                backoff = RECONNECT_MIN_SECS
            except Exception as e:
                bt.logging.warning(f'program feed: socket down ({e}); reconnecting in ~{backoff:.0f}s')
            finally:
                self._connected.clear()
            threading.Event().wait(backoff * random.uniform(0.8, 1.2))
            backoff = min(backoff * 2, RECONNECT_MAX_SECS)
