"""A websocket carrying many ``programSubscribe`` / ``accountSubscribe`` subscriptions, for the quote optimizer.

The base miner's ``allways.solana.program_feed.ProgramEventFeed`` holds one ``logsSubscribe`` for its swaps; the
optimizer decides on account state, so it runs this on a connection of its own. Notifications route by
subscription id. The scheduled renewal is make-before-break, so a consumer that seeded once keeps an unbroken
picture across it; ``stop`` closes the connection and ``start`` opens a new one.
"""

import asyncio
import json
import random
import threading
import time
from typing import Callable, Dict, List, NamedTuple, Optional, Set, Tuple

import bittensor as bt

RECONNECT_MIN_SECS = 1.0
RECONNECT_MAX_SECS = 30.0
PING_INTERVAL_SECS = 20
PING_TIMEOUT_SECS = 20
# A session whose subscriptions aren't all acknowledged by then is abandoned and retried.
SUBSCRIBE_ACK_TIMEOUT_SECS = 20.0


def mask_url(url: str) -> str:
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
        self._task: Optional[asyncio.Task] = None
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

    def stop(self) -> None:
        """Close the connection and stop reconnecting. ``start`` opens a fresh one, which counts as a new
        generation — whatever was pushed meanwhile was missed."""
        thread, loop, task = self._thread, self._loop, self._task
        if loop is not None and task is not None:
            loop.call_soon_threadsafe(task.cancel)
        if thread is not None:
            thread.join(timeout=5)
        self._thread = self._loop = self._task = None

    # ── transport ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        try:
            asyncio.run(self._supervise())
        except asyncio.CancelledError:
            pass  # stop()
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
            bt.logging.warning(f'{self.name}: down @ {mask_url(self.ws_url)}')

    async def _supervise(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        backoff = Backoff()
        try:
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
                        f'{self.name}: live with {len(session.acked)} subscriptions @ {mask_url(self.ws_url)} '
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
        finally:  # stop(): close the connection on the way out, quietly — it was asked for
            current, self._current = self._current, None
            if current is not None:
                await current.close()
            if self.live_since is not None:
                self.live_since, self.down_since = None, time.time()
