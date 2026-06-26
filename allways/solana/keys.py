"""Solana keypair loading for the validator/miner.

Separate from the Bittensor wallet. Reads the standard solana-CLI keypair file (a JSON array of 64
ints) from `SOLANA_KEYPAIR_PATH` env / explicit path / `~/.solana/id.json`. `load_or_create` generates
one for dev/localnet.
"""

import json
import os
from pathlib import Path
from typing import Optional

from solders.keypair import Keypair


def _default_path() -> str:
    return os.environ.get('SOLANA_KEYPAIR_PATH') or str(Path.home() / '.solana' / 'id.json')


def load_keypair(path: Optional[str] = None) -> Keypair:
    p = Path(path or _default_path())
    data = json.loads(p.read_text())
    return Keypair.from_bytes(bytes(data))


def load_or_create(path: Optional[str] = None) -> Keypair:
    p = Path(path or _default_path())
    if p.exists():
        return load_keypair(str(p))
    kp = Keypair()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(list(bytes(kp))))  # solana-CLI format: JSON array of 64 ints
    return kp
