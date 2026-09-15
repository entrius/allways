"""Pushed program state over a Solana WebSocket, reconnecting with jittered backoff.

``ProgramEventFeed``: `logsSubscribe(mentions=[account])` on the RPC's wss twin, decoded and dispatched per event
name. The account defaults to the program; a miner narrows it to its own pubkey. The feed is latency, not truth —
consumers keep a catch-up for gaps.

``SubscriptionFeed``: many `programSubscribe` / `accountSubscribe` subscriptions on one connection, notifications
routed by subscription id. Its scheduled resubscribe is make-before-break, so a consumer that seeded once keeps
an unbroken picture across it."""

import asyncio
import base64
import json
import random
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Tuple

import bittensor as bt

from allways.solana.events import PROGRAM_DATA_PREFIX, decode_event

Handler = Callable[[str, Any], None]

RECONNECT_MIN_SECS = 1.0
RECONNECT_MAX_SECS = 30.0
PING_INTERVAL_SECS = 20
PING_TIMEOUT_SECS = 20
# A session whose subscriptions aren't all acknowledged by then is abandoned and retried.
SUBSCRIBE_ACK_TIMEOUT_SECS = 20.0


def _mask(url: str) -> str:
    head, sep, _ = url.partition('api-key=')
    return f'{head}{sep}…' if sep else url


class Backoff:
    """Jittered exponential reconnect delay. An endpoint with no WebSocket fails every attempt, so only the first
    failure in a row is a warning."""

    def __init__(self) -> None:
        self.delay = RECONNECT_MIN_SECS
        self.failures = 0

    def reset(self) -> None:
        self.delay = RECONNECT_MIN_SECS
        self.failures = 0

    def failed(self, name: str, error: Exception) -> float:
        self.failures += 1
        log = bt.logging.warning if self.failures == 1 else bt.logging.debug
        log(f'{name}: socket down ({error}); reconnecting in ~{self.delay:.0f}s')
        wait = self.delay * random.uniform(0.8, 1.2)
        self.delay = min(self.delay * 2, RECONNECT_MAX_SECS)
        return wait


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
        backoff = Backoff()
        while True:
            wait = RECONNECT_MIN_SECS * random.uniform(0.8, 1.2)
            try:
                asyncio.run(self._session())
                backoff.reset()
            except Exception as e:
                wait = backoff.failed('program feed', e)
            finally:
                self._connected.clear()
            threading.Event().wait(wait)


class Subscription(NamedTuple):
    """One subscription, named by ``key`` so notifications route to it on whichever session carries it."""

    key: str
    method: str
    params: list


class FeedSession:
    """One connection's subscriptions: request ids map to keys until acknowledged, then subscription ids do."""

    def __init__(self, ws=None) -> None:
        self.ws = ws
        self.next_id = 0
        self.pending: Dict[int, str] = {}
        self.keys: Dict[int, str] = {}
        self.requested: Set[str] = set()
        self.acked: Set[str] = set()
        self.dead = False
        self.changed: Optional[asyncio.Event] = None
        self.reader: Optional[asyncio.Task] = None

    def request(self, sub: Subscription) -> dict:
        self.next_id += 1
        self.pending[self.next_id] = sub.key
        self.requested.add(sub.key)
        return {'jsonrpc': '2.0', 'id': self.next_id, 'method': sub.method, 'params': sub.params}

    def handle_frame(self, msg: dict) -> Optional[Tuple[str, dict]]:
        """An ack records the subscription id and returns None; a notification returns ``(key, result)``.
        A rejected subscribe raises, so the session is abandoned rather than silently missing a feed."""
        if msg.get('id') in self.pending:
            key = self.pending.pop(msg['id'])
            if 'error' in msg:
                raise ConnectionError(f'subscribe {key} rejected: {msg["error"]}')
            self.keys[int(msg['result'])] = key
            self.acked.add(key)
            return None
        params = msg.get('params') or {}
        key = self.keys.get(params.get('subscription'))
        if key is None:
            return None
        return key, params.get('result') or {}

    def covers(self, keys) -> bool:
        return not self.dead and set(keys) <= self.acked

    async def close(self) -> None:
        self.dead = True
        try:
            await self.ws.close()
        except Exception:
            pass
        if self.reader is not None:
            self.reader.cancel()


class SubscriptionFeed:
    """Keeps every added subscription live on one connection and hands each notification to
    ``handler(key, result)`` on the feed thread (keep it short). ``live`` is true while the current session has
    acknowledged every subscription. Every ``max_session_secs`` a new session is opened and fully acknowledged
    before the old one closes, so no push falls in a gap; the overlap delivers some pushes twice, which consumers
    order by slot. ``generation`` counts the times the feed went live after being down — a change means pushes
    may have been missed."""

    def __init__(
        self,
        ws_url: str,
        handler: Callable[[str, dict], None],
        max_session_secs: Optional[float] = None,
        name: str = 'subscription feed',
        connect=None,
    ) -> None:
        self.ws_url = ws_url
        self.handler = handler
        self.max_session_secs = max_session_secs
        self.name = name
        self._connect = connect
        self._subs: Dict[str, Subscription] = {}
        self._lock = threading.Lock()
        self._current: Optional[FeedSession] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self.bytes_received = 0
        self.connections_opened = 0
        self.generation = 0
        self.live_since: Optional[float] = None
        self.down_since: float = time.time()

    @property
    def live(self) -> bool:
        session = self._current
        return session is not None and session.covers(self.keys())

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._subs)

    def add(self, sub: Subscription) -> bool:
        """Subscribe (once per key), on the live session right away. Returns whether the key was new."""
        with self._lock:
            if sub.key in self._subs:
                return False
            self._subs[sub.key] = sub
        loop, session = self._loop, self._current
        if loop is not None and session is not None:
            asyncio.run_coroutine_threadsafe(self._sync(session), loop)
        return True

    def start(self) -> 'SubscriptionFeed':
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name=self.name.replace(' ', '-'), daemon=True)
            self._thread.start()
        return self

    # ── transport ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        try:
            asyncio.run(self._supervise())
        except Exception as e:  # the supervisor loops forever; a crash here leaves the consumer's dead-man to act
            bt.logging.error(f'{self.name}: stopped on {type(e).__name__}: {e}')

    def _dispatch(self, key: str, result: dict) -> None:
        try:
            self.handler(key, result)
        except Exception as e:
            bt.logging.warning(f'{self.name}: {key} handler failed: {e}')

    async def _sync(self, session: FeedSession) -> None:
        """Send every subscription this session hasn't requested yet."""
        with self._lock:
            missing = [sub for key, sub in self._subs.items() if key not in session.requested]
        for sub in missing:
            await session.ws.send(json.dumps(session.request(sub)))

    async def _read(self, session: FeedSession) -> None:
        try:
            async for raw in session.ws:
                self.bytes_received += len(raw)
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                routed = session.handle_frame(msg)
                if routed is None:
                    session.changed.set()
                else:
                    self._dispatch(*routed)
        except Exception as e:
            if not session.dead:
                bt.logging.debug(f'{self.name}: session ended ({type(e).__name__}: {e})')
        finally:
            session.dead = True
            session.changed.set()

    async def _open(self) -> FeedSession:
        """Connect, subscribe everything, and return once every subscription is acknowledged."""
        connect = self._connect
        if connect is None:
            import websockets

            connect = websockets.connect
        ws = await connect(self.ws_url, ping_interval=PING_INTERVAL_SECS, ping_timeout=PING_TIMEOUT_SECS, max_size=None)
        self.connections_opened += 1
        session = FeedSession(ws)
        session.changed = asyncio.Event()
        session.reader = asyncio.ensure_future(self._read(session))
        deadline = time.monotonic() + SUBSCRIBE_ACK_TIMEOUT_SECS
        try:
            while not session.covers(self.keys()):
                if session.dead:
                    raise ConnectionError('socket closed before every subscription was acknowledged')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('subscriptions not acknowledged in time')
                session.changed.clear()
                await self._sync(session)
                try:
                    await asyncio.wait_for(session.changed.wait(), remaining)
                except asyncio.TimeoutError:
                    pass
        except BaseException:
            await session.close()
            raise
        return session

    def _went_down(self) -> None:
        if self.live_since is not None:
            self.live_since = None
            self.down_since = time.time()
            bt.logging.warning(f'{self.name}: down @ {_mask(self.ws_url)}')

    async def _supervise(self) -> None:
        self._loop = asyncio.get_running_loop()
        backoff = Backoff()
        while True:
            try:
                session = await self._open()
            except Exception as e:
                current = self._current
                if current is None or current.dead:
                    self._went_down()
                await asyncio.sleep(backoff.failed(self.name, e))
                continue
            backoff.reset()
            old, self._current = self._current, session
            if old is None or old.dead:
                self.generation += 1
                self.live_since = time.time()
                bt.logging.info(
                    f'{self.name}: live with {len(session.acked)} subscriptions @ {_mask(self.ws_url)} '
                    f'(generation {self.generation})'
                )
            await self._sync(session)  # anything added while the session was opening
            if old is not None:
                await old.close()
            await asyncio.wait({session.reader}, timeout=self.max_session_secs)
            if session.dead:
                self._current = None
                self._went_down()
                await asyncio.sleep(RECONNECT_MIN_SECS * random.uniform(0.8, 1.2))
