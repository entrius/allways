"""Localhost HTTP seam — the offering transport onto the shared kernel ops (reserve/confirm/rate/status).

Loopback-only, shared-secret auth. A validator's product offering (a separate process) calls IN here to
enter the reservation lottery on a user's behalf and to read swap status; the kernel never calls out. Not
for public exposure — bind 127.0.0.1 and front it with the product server. Off by default: starts only when
``ALLWAYS_SEAM_SECRET`` is set (see ``maybe_start_seam``).
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import bittensor as bt

from allways.validator.reserve_engine import best_quote, confirm_deposit, reserve_on_behalf, swap_status

SEAM_HOST = '127.0.0.1'


def _make_handler(validator, secret: str):
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
                    bq = best_quote(validator, q['from'], q['to'], int(q['amount']))
                    if bq is None:
                        return self._send(404, {'error': 'no executable quote for that pair/amount'})
                    return self._send(200, bq.__dict__)
                if url.path == '/status':
                    return self._send(200, swap_status(validator, q['miner_hotkey']).__dict__)
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
