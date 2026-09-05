"""Localhost HTTP seam — the offering transport onto the shared kernel ops (reserve/confirm/rate/status).

Loopback-only by default, shared-secret auth. A validator's product offering (a separate process) calls IN
here to enter the reservation lottery on a user's behalf and to read swap status; the kernel never calls
out. Not for public exposure. Off by default: starts only when ``ALLWAYS_SEAM_SECRET`` is set (see
``maybe_start_seam``). A containerized validator sets ``ALLWAYS_SEAM_HOST=0.0.0.0`` — a container-loopback
bind is unreachable even from the offering — and relies on the docker network + secret for isolation,
publishing no port.
"""

import json
import os
import threading
import time
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import bittensor as bt

from allways.validator.reserve_engine import (
    confirm_deposit,
    rate_quote,
    reserve_on_behalf,
    scan_deposit,
    swap_status,
    verdict_epoch,
)

SEAM_HOST = os.environ.get('ALLWAYS_SEAM_HOST', '127.0.0.1')

# The offering polls /status and /deposit-scan per active swap on a tight loop, and its
# list view reconciles every live row per poll — so identical reads arrive from many tabs and
# many rows at once, each costing uncached getAccountInfo. Sustained hard enough that earns the
# validator an RPC 429, and a validator that cannot read the chain cannot verify a swap: the
# miner holding the reservation is the one who eats the timeout. A chatty consumer must not be
# able to price the validator out of its own RPC budget.
#
# Sized to the consumer's fastest poll, not longer: past that the TTL — not the caller's
# cadence — sets how fast a stage transition surfaces, buying no extra dedup (a burst of tabs
# and reconciled rows lands within ~1s) at the cost of latency on every transition. Cadence is
# the lever for fewer reads; this one only collapses simultaneous ones.
SEAM_READ_TTL_SECS = 3.0


def _ttl_bucket() -> int:
    """Cache key component that rolls every TTL — a new bucket is a miss, old ones age out of
    the LRU. Far shorter than any transition the offering reacts to (reservations live minutes,
    dest legs need tens of seconds of confirmations), so a read this stale changes no decision."""
    return int(time.monotonic() / SEAM_READ_TTL_SECS)


def _make_handler(validator, secret: str):
    # Memos are per server, closing over this validator rather than keying on it: the validator
    # is not required to be hashable, and no module-level table pins it alive. lru_cache does not
    # store exceptions, so a transient RPC fault is retried rather than pinned for the bucket;
    # maxsize caps the table so a long-lived seam can't grow an entry per swap ever polled.
    # `_epoch` keys the memo on the pool-verdict epoch: a read still in flight when a 'rejected' verdict
    # is published or cleared lands under the old key, so the next poll re-reads instead of inheriting it.
    @lru_cache(maxsize=512)
    def cached_status(miner_hotkey: str, swap_key: str, from_chain: str, to_chain: str, _bucket: int, _epoch: int):
        return swap_status(validator, miner_hotkey, swap_key, from_chain, to_chain)

    @lru_cache(maxsize=512)
    def cached_deposit_scan(miner_hotkey: str, _bucket: int):
        return scan_deposit(validator, miner_hotkey)

    class SeamHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence default stderr access log
            pass

        def _send(self, code: int, payload: dict):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authed(self) -> bool:
            return self.headers.get('X-Seam-Secret', '') == secret

        def _body(self) -> dict:
            length = int(self.headers.get('Content-Length', 0) or 0)
            if not length:
                return {}
            return json.loads(self.rfile.read(length) or b'{}')

        def do_GET(self):
            if not self._authed():
                return self._send(401, {'error': 'unauthorized'})
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                if url.path == '/rate':
                    # Depth rides along on hit AND miss — the offering's size copy ("min/max right
                    # now is X") needs the true bounds exactly when no quote fits the asked size.
                    rq = rate_quote(validator, q['from'], q['to'], int(q['amount']))
                    depth = {
                        'levels': rq.levels,
                        'max_from_amount': rq.max_from_amount,
                        'min_from_amount': rq.min_from_amount,
                        'candidates': rq.candidates,
                    }
                    if rq.quote is None:
                        return self._send(404, {'error': rq.reason, **depth})
                    return self._send(200, {**rq.quote.__dict__, **depth})
                if url.path == '/status':
                    # Optional swap_key (hex, persisted by the consumer at claim time) resolves the
                    # swap directly — the only route to post-attestation stages, since vote_initiate
                    # consumes the reservation at quorum. Optional from_chain/to_chain pick the
                    # reservation slot carrying that pair (v3.1 holds one per hub).
                    status = cached_status(
                        q['miner_hotkey'],
                        q.get('swap_key', ''),
                        q.get('from_chain', ''),
                        q.get('to_chain', ''),
                        _ttl_bucket(),
                        verdict_epoch(),
                    )
                    return self._send(200, status.__dict__)
                if url.path == '/deposit-scan':
                    # Hash-finder for the offering's deposit watcher — /confirm stays the verifier.
                    tx_hash = cached_deposit_scan(q['miner_hotkey'], _ttl_bucket())
                    return self._send(200, {'tx_hash': tx_hash})
                if url.path == '/health':
                    return self._send(200, {'ok': True})
            except (KeyError, ValueError) as e:
                return self._send(400, {'error': f'bad request: {e}'})
            except Exception as e:
                bt.logging.error(f'seam GET {url.path} failed: {e}')
                return self._send(500, {'error': str(e)})
            return self._send(404, {'error': 'not found'})

        def do_POST(self):
            if not self._authed():
                return self._send(401, {'error': 'unauthorized'})
            url = urlparse(self.path)
            try:
                body = self._body()
                if url.path == '/reserve':
                    r = reserve_on_behalf(
                        validator,
                        body['miner_hotkey'],
                        body['from_chain'],
                        body['to_chain'],
                        body['user_pubkey'],
                        body['user_from_addr'],
                        body['user_to_addr'],
                        int(body['from_amount']),
                    )
                    return self._send(200 if r.ok else 422, r.__dict__)
                if url.path == '/confirm':
                    r = confirm_deposit(
                        validator, body['miner_hotkey'], body['from_tx_hash'], int(body.get('from_tx_block', 0) or 0)
                    )
                    return self._send(200 if r.ok else 422, r.__dict__)
            except (KeyError, ValueError) as e:
                return self._send(400, {'error': f'bad request: {e}'})
            except Exception as e:
                bt.logging.error(f'seam POST {url.path} failed: {e}')
                return self._send(500, {'error': str(e)})
            return self._send(404, {'error': 'not found'})

    return SeamHandler


def start_seam(validator, port: int, secret: str) -> ThreadingHTTPServer:
    """Start the loopback seam server in a daemon thread. Returns the server (call shutdown() to stop)."""
    server = ThreadingHTTPServer((SEAM_HOST, port), _make_handler(validator, secret))
    threading.Thread(target=server.serve_forever, name='offering-seam', daemon=True).start()
    bt.logging.info(f'Offering seam listening on {SEAM_HOST}:{port}')
    return server


def maybe_start_seam(validator):
    """Start the seam iff an operator opted in (ALLWAYS_SEAM_SECRET set). Generic validators run without it."""
    secret = os.environ.get('ALLWAYS_SEAM_SECRET', '')
    if not secret:
        return None
    port = int(os.environ.get('ALLWAYS_SEAM_PORT', '8710'))
    return start_seam(validator, port, secret)
