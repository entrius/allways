"""The fee-scaled pool: each round pays every lane pool × ``miner_share``, and the rest recycles with the
shortfall.

``miner_share = min(1, FEE_POOL_MULTIPLE × fees / miner_emission)`` over the trailing ``POOL_VOLUME_WINDOW_SECS``:
the swap fees (``SwapCompleted.fee``) in TAO against the miners' alpha emission in TAO. Lamport fees convert at
the fixed-hour market rate (``sol_tao_price``), so every validator derives the same share. A failed emission read
keeps the last share; a validator with none yet starts at 0.0 (full recycle)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

import bittensor as bt

from allways.chains import get_chain_def
from allways.constants import (
    FEE_POOL_MULTIPLE,
    MINER_EMISSION_FRACTION,
    POOL_VOLUME_WINDOW_SECS,
    SECONDS_PER_BLOCK,
    SOL_TAO_RATE_STALE_SECS,
)
from allways.validator import sol_tao_price
from allways.validator.store.db import Store

MINER_SHARE = 'scoring.fee_pool_miner_share'
SOL_TAO_RATE = 'scoring.fee_pool_sol_tao_rate'
SOL_TAO_RATE_HOUR = 'scoring.fee_pool_sol_tao_rate_hour'


@dataclass(frozen=True)
class FeePoolShare:
    """One round's fee-scaled pool inputs, in TAO over ``POOL_VOLUME_WINDOW_SECS``. ``emission_tao`` is None
    when it could not be read; ``share`` is then the last round's."""

    fees_tao: float
    emission_tao: Optional[float]
    share: float

    @property
    def fresh(self) -> bool:
        return self.emission_tao is not None


def fee_pool_share(fees_tao: float, emission_tao: float) -> float:
    """``min(1, FEE_POOL_MULTIPLE × fees / emission)``: the fraction of every lane pool a round pays."""
    if emission_tao <= 0:
        raise ValueError(f'miner emission must be positive, got {emission_tao}')
    return min(1.0, max(0.0, FEE_POOL_MULTIPLE * fees_tao / emission_tao))


def window_fees_tao(fees: Dict[str, int], sol_tao_rate: Callable[[], Optional[float]]) -> float:
    """``{backing: fee}`` (each in its backing's smallest unit) in TAO: rao as-is, lamports at ``sol_tao_rate()``
    (TAO per SOL, read only when there are lamport fees). A backing with no conversion is skipped with a warning."""
    total = 0.0
    for backing, fee in sorted(fees.items()):
        if fee <= 0:
            continue
        rate = 1.0 if backing == 'tao' else sol_tao_rate() if backing == 'sol' else None
        if rate is None:
            bt.logging.warning(f'fee pool: no {backing}→tao conversion, skipping {fee} units of {backing} fees')
            continue
        total += fee / 10 ** get_chain_def(backing).decimals * rate
    return total


def miner_emission_tao(subnet: Optional[Any]) -> Optional[float]:
    """The miners' alpha emission over ``POOL_VOLUME_WINDOW_SECS`` in TAO at the subnet's on-chain moving alpha
    price, from a ``subtensor.subnet(netuid)`` read: per-block alpha emission × ``MINER_EMISSION_FRACTION`` ×
    blocks in the window × price. None when there is no read or it values to nothing."""
    if subnet is None:
        return None
    try:
        alpha_per_block = float(subnet.alpha_out_emission.tao)
        price = float(subnet.moving_price)
    except Exception as e:
        bt.logging.warning(f'fee pool: unreadable subnet info: {e}')
        return None
    value = alpha_per_block * MINER_EMISSION_FRACTION * (POOL_VOLUME_WINDOW_SECS / SECONDS_PER_BLOCK) * price
    if not value > 0:
        bt.logging.warning(f'fee pool: miner emission reads {value} (alpha/block={alpha_per_block}, price={price})')
        return None
    return value


class FeePool:
    """The round's share and the hourly SOL→TAO rate, both kept in the store's meta across restarts."""

    def __init__(self, store: Store, fetch: Optional[Callable[[int], Optional[float]]] = None) -> None:
        self.store = store
        self._fetch = fetch

    def last_share(self) -> float:
        """The last share a round derived; 0.0 (full recycle) before the first one."""
        stored = self.store.get_meta(MINER_SHARE)
        return float(stored) if stored is not None else 0.0

    def stored_rate(self) -> Optional[Tuple[float, int]]:
        """``(rate, hour)``: the last good TAO-per-SOL rate and the UTC hour it is for; None before the first."""
        rate, hour = self.store.get_meta(SOL_TAO_RATE), self.store.get_meta(SOL_TAO_RATE_HOUR)
        if rate is None or hour is None:
            return None
        return float(rate), int(hour)

    def _put(self, **meta: str) -> None:
        with self.store.tx() as c:
            for key, value in meta.items():
                Store.set_meta(c, key, value)

    def sol_tao_rate(self, now: int) -> Optional[float]:
        """TAO per SOL at ``rate_hour(now)``, fetched once per hour. A failed fetch keeps the last good rate
        (WARNING with its hour and age, ERROR once older than ``SOL_TAO_RATE_STALE_SECS``) and retries next
        round; None = no rate fetched yet."""
        hour = sol_tao_price.rate_hour(now)
        last = self.stored_rate()
        if last is not None and last[1] >= hour:
            return last[0]
        rate = (self._fetch or sol_tao_price.fetch_sol_tao_rate)(hour)
        if rate is not None:
            self._put(**{SOL_TAO_RATE: repr(float(rate)), SOL_TAO_RATE_HOUR: str(int(hour))})
            return rate
        if last is None:
            bt.logging.warning(f'sol/tao rate for hour {hour} unavailable and none stored yet')
            return None
        last_rate, last_hour = last
        age = now - last_hour
        log = bt.logging.error if age > SOL_TAO_RATE_STALE_SECS else bt.logging.warning
        log(f'sol/tao rate for hour {hour} unavailable, keeping {last_rate:g} from hour {last_hour} ({age}s old)')
        return last_rate

    def round(self, fees: Dict[str, int], subnet: Optional[Any], now: int) -> FeePoolShare:
        """This round's share from the window's ``{backing: fee}`` and the latest subnet read, persisted for the
        next round. A failed emission read keeps the last share."""
        fees_tao = window_fees_tao(fees, lambda: self.sol_tao_rate(now))
        emission = miner_emission_tao(subnet)
        if emission is None:
            share = self.last_share()
            bt.logging.warning(f'fee pool: input unavailable (fees={fees_tao}, emission=None), keeping {share:.6f}')
            return FeePoolShare(fees_tao, None, share)
        share = fee_pool_share(fees_tao, emission)
        self._put(**{MINER_SHARE: repr(float(share))})
        return FeePoolShare(fees_tao, emission, share)
