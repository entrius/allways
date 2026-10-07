"""Alpha-lane knobs, read from the environment with the same defaults the lift came with.

Validation mirrors the source's `_require` lines: a bad value is an operator error worth refusing at
boot, not a surprise mid-fill.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

ENV_PREFIX = 'ALLWAYS_ALPHA_'

# Dropped by hand (or by a re-registration) to pull alpha quotes. A fill on a quote that is already
# standing still delivers — pausing stops new exposure, it never abandons an obligation.
ALPHA_PAUSED_FLAG = 'alpha-paused'


def _float(name: str, default: float) -> float:
    raw = os.environ.get(f'{ENV_PREFIX}{name}')
    return default if raw is None or raw == '' else float(raw)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(f'{ENV_PREFIX}{name}')
    return default if raw is None or raw == '' else int(raw)


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(f'{ENV_PREFIX}{name}')
    return default if raw is None or raw == '' else raw.strip().lower() in ('1', 'true', 'yes', 'on')


@dataclass(frozen=True)
class AlphaConfig:
    """Field names match what the lifted modules read, so `dataclasses.replace` works in tests."""

    alpha_enabled: bool = False
    alpha_cushion: float = 0.02
    alpha_reprice: float = 0.01
    alpha_depth_mult: float = 200.0
    alpha_prune_skip: int = 5
    alpha_jump_limit: int = 3
    alpha_buy_buffer: float = 0.003
    alpha_fee_reserve_tao: float = 0.05
    log_dir: Path = field(default_factory=lambda: Path.home() / '.allways')


def load() -> AlphaConfig:
    log_dir = os.environ.get(f'{ENV_PREFIX}LOG_DIR') or os.environ.get('ALLWAYS_LOG_DIR')
    config = AlphaConfig(
        alpha_enabled=_bool('ENABLED', False),
        alpha_cushion=_float('CUSHION', 0.02),
        alpha_reprice=_float('REPRICE', 0.01),
        alpha_depth_mult=_float('DEPTH_MULT', 200),
        alpha_prune_skip=_int('PRUNE_SKIP', 5),
        alpha_jump_limit=_int('JUMP_LIMIT', 3),
        alpha_buy_buffer=_float('BUY_BUFFER', 0.003),
        alpha_fee_reserve_tao=_float('FEE_RESERVE_TAO', 0.05),
        log_dir=Path(log_dir).expanduser() if log_dir else Path.home() / '.allways',
    )
    validate(config)
    return config


def validate(config: AlphaConfig) -> None:
    checks = {
        'CUSHION': 0 < config.alpha_cushion < 0.1,
        'REPRICE': 0 < config.alpha_reprice < config.alpha_cushion,
        'DEPTH_MULT': config.alpha_depth_mult >= 50,
        'PRUNE_SKIP': config.alpha_prune_skip >= 1,
        'JUMP_LIMIT': config.alpha_jump_limit >= 1,
        'BUY_BUFFER': 0 < config.alpha_buy_buffer < 0.05,
        'FEE_RESERVE_TAO': config.alpha_fee_reserve_tao > 0,
    }
    bad = [name for name, ok in checks.items() if not ok]
    if bad:
        raise ValueError(f'invalid alpha config: {", ".join(f"{ENV_PREFIX}{n}" for n in bad)}')


def alpha_live(config: AlphaConfig) -> bool:
    """Enabled and not paused by the flag file."""
    return config.alpha_enabled and not (config.log_dir / ALPHA_PAUSED_FLAG).exists()
