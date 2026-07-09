"""Single resolution point for the allways_swap_manager program address.

Precedence: ALLWAYS_PROGRAM_ID env > CLI config (`program-id`, legacy `contract`) > constants.PROGRAM_ID.

Resolved lazily on every call, never at import. Import-time resolution silently froze the address
before `load_dotenv()` ran in the neurons, so a `.env` override was ignored there.
"""

import os
from typing import Optional

from solders.pubkey import Pubkey

from allways.constants import PROGRAM_ID as DEFAULT_PROGRAM_ID

ENV_VAR = 'ALLWAYS_PROGRAM_ID'
# `contract` is the legacy ink!-era key name, still honored for existing ~/.allways/config.json.
CONFIG_KEYS = ('program-id', 'contract')


def _warn(message: str) -> None:
    try:
        import bittensor as bt

        bt.logging.warning(message)
    except Exception:
        print(f'WARNING: {message}')


def resolve_program_id(config: Optional[dict] = None) -> Pubkey:
    """Program address from env, else CLI config, else the committed default.

    A malformed env value raises: it is explicit operator intent, and silently falling back to the
    devnet default would point a mainnet node at the wrong program. A malformed config value only
    warns and falls through, preserving the CLI's existing behavior.
    """
    raw = os.environ.get(ENV_VAR)
    if raw:
        try:
            return Pubkey.from_string(raw)
        except (ValueError, TypeError) as exc:
            raise ValueError(f'{ENV_VAR}={raw!r} is not a valid Solana address') from exc

    for key in CONFIG_KEYS:
        configured = (config or {}).get(key)
        if configured:
            try:
                return Pubkey.from_string(configured)
            except (ValueError, TypeError):
                _warn(f'Ignoring invalid {key} config {configured!r}; using default {DEFAULT_PROGRAM_ID}')
            break

    return Pubkey.from_string(DEFAULT_PROGRAM_ID)
