"""Dev-only structured signal (T3 assertion surface for the e2e harness).

``emit`` is a no-op unless ``ALLWAYS_DEV_SIGNAL`` is set to a writable path; then each call
appends one NDJSON line ``{"ts": ..., "event": ..., ...fields}``. It mirrors existing log
lines — it never replaces them — and never raises.

``fault`` gates harness-injected failure paths (e.g. withholding a dest send to force the
slash path). ``ALLWAYS_DEV_FAULTS`` names a file whose content is a comma/newline-separated
flag list, so the harness can toggle faults on a running neuron. Both env vars are unset in
production, making this module inert.
"""

import json
import os
import time


def emit(event: str, **fields) -> None:
    path = os.environ.get('ALLWAYS_DEV_SIGNAL')
    if not path:
        return
    try:
        with open(path, 'a') as f:
            f.write(json.dumps({'ts': time.time(), 'event': event, **fields}, default=str) + '\n')
    except Exception:
        pass


def fault(name: str) -> bool:
    path = os.environ.get('ALLWAYS_DEV_FAULTS')
    if not path:
        return False
    try:
        flags = open(path).read().replace('\n', ',')
    except OSError:
        return False
    return name in {f.strip() for f in flags.split(',') if f.strip()}
