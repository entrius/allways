"""One 10-min read of every subnet: price, reserves, flags, prune order, top validators; the quotable set."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from allways.miner.alpha.config import AlphaConfig as Config
from allways.miner.alpha.ledger import Ledger
from allways.miner.alpha.logbook import get_logger
from allways.miner.alpha.telemetry import guarded_alert

ALPHA_MARKET_NAME = 'alpha-market.json'
ROOT_NETUID = 0
BLOCK_SECS = 12
HEAD_MAX_AGE_SECS = 60  # a head this old is a frozen endpoint: its reads are wrong, not late
PRICE_SAMPLES = 145  # 24 h of 10-min samples, the jumpy window
TOGGLE_MEMORY_SECS = 7 * 86_400
IMMUNITY_HORIZON_SECS = 3 * 86_400  # a subnet leaving immunity this soon already stands in the prune order
VALIDATORS_REFRESH_SECS = 3600
SIM_BUY_CALLS = 8
RAO = 10**9
MIN_STAKE_RAO = 2_100_000  # 0.0021 τ: the chain's 0.002 τ stake minimum plus the swap fee
_SNAPSHOT_WAITS, _SNAPSHOT_TIMEOUT = (5, 15), 15  # retry waits and per-call timeout for the snapshot read
_SIM_WAITS, _SIM_TIMEOUT = (1, 1), 5
_READ_WAITS, _READ_TIMEOUT = (1, 2, 4), 5
_METAGRAPH_FIELDS = [0, 52, 57, 67]  # Netuid, Hotkeys, ValidatorPermit, AlphaStake

_LOGGER = get_logger('c12.alpha_market')


@dataclass(frozen=True)
class Subnet:
    netuid: int
    price_rao: int
    moving_price: float
    tao_in: int
    transfers: bool
    subtoken: bool
    registered_at: int


@dataclass(frozen=True)
class Snapshot:
    block: int
    subnets: dict[int, Subnet]
    prune_order: tuple[int, ...]


class StaleHeadError(Exception):
    """The endpoint's head stopped moving: the read is dropped with the socket, never trusted."""


def fee_reserve_rao(config: Config) -> int:
    """The free TAO an alpha buy leaves untouched, for the fees of every coldkey send after it."""
    return int(config.alpha_fee_reserve_tao * RAO)


class AlphaMarket:
    """Every subnet's market state, read on the own socket; a failed read keeps the last snapshot, never guesses."""

    def __init__(
        self,
        connect: Callable[[str | None], Any],
        ledger: Ledger,
        config: Config,
        tao_max_swap: Callable[[], int | None],
        alert=None,
        tick_secs: float = 600,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.connect = connect  # receives the endpoint just dropped, so the next socket can rotate off it
        self.ledger = ledger
        self.config = config
        self.tao_max_swap = tao_max_swap
        self._alert = guarded_alert(alert, _LOGGER)
        self.tick_secs = tick_secs
        self.clock = clock
        self.sleep = sleep
        self._lock = threading.Lock()  # one socket, many threads: the router and seller read between ticks
        self._subtensor = None
        self._dropped_endpoint: str | None = None
        self._snapshot: Snapshot | None = None
        self._validators_read = False  # the first tick's validator pass is done: quotable() needs a top validator
        self._read_at = clock()
        self._validators: dict[int, str] = {}
        self._validators_at: dict[int, float] = {}  # per netuid: a failed read leaves no stamp, so it retries next tick
        state = ledger.load()
        self._prices: dict[int, list[int]] = {int(n): [int(p) for p in s] for n, s in state.get('prices', {}).items()}
        self._transfers_off: dict[int, float] = {int(n): float(at) for n, at in state.get('transfers_off', {}).items()}
        self._registered_at: dict[int, int] = {int(n): int(at) for n, at in state.get('registered_at', {}).items()}

    def tick(self) -> None:
        try:
            snapshot = self._read(self._read_snapshot, _SNAPSHOT_WAITS, _SNAPSHOT_TIMEOUT)
        except Exception as error:
            _LOGGER.warning('Alpha market read failed error=%s — the last snapshot stands', error)
            if self._stale():
                self._alert(
                    'alpha-price-stale', 'Alpha prices are stale', 'No subnet read for two ticks; alpha quotes pull'
                )
            return
        if not self._prices:
            self._backfill(snapshot.block)
        self._remember(snapshot)
        self._snapshot, self._read_at = snapshot, self.clock()
        self._refresh_validators(snapshot)
        self._validators_read = True
        _LOGGER.info(
            'Alpha market block=%d subnets=%d quotable=%d prune_line=%s',
            snapshot.block,
            len(snapshot.subnets),
            len(self.quotable()),
            snapshot.prune_order[: self.config.alpha_prune_skip],
        )

    def known(self) -> bool:
        """A first full read (snapshot and validators) and the TAO max swap; before that the set is unknown."""
        return self._validators_read and self.tao_max_swap() is not None

    def quotable(self) -> frozenset[int]:
        """Subnets that pass every gate in order; empty while the snapshot is stale or the TAO max swap is unknown."""
        max_swap = self.tao_max_swap()
        if self._snapshot is None or self._stale() or max_swap is None:
            return frozenset()
        return frozenset(n for n, subnet in self._snapshot.subnets.items() if self._gate(subnet, max_swap) is None)

    def price_rao(self, netuid: int) -> int | None:
        subnet = self._snapshot.subnets.get(netuid) if self._snapshot else None
        return subnet.price_rao if subnet and subnet.price_rao > 0 else None

    def spot_rao(self, netuid: int) -> int | None:
        """Live price read for the buy limit; None on failure or a zero price, never a stale number."""
        try:
            price = self._read(
                lambda s: int(s.query_runtime_api('SwapRuntimeApi', 'current_alpha_price', [netuid])),
                _READ_WAITS,
                _READ_TIMEOUT,
            )
        except Exception as error:
            _LOGGER.warning('Alpha spot read failed netuid=%d error=%s', netuid, error)
            return None
        return price if price > 0 else None

    def top_validator(self, netuid: int) -> str | None:
        return self._validators.get(netuid)

    def sim_buy(self, netuid: int, alpha_out: int) -> int | None:
        """Smallest TAO (rao) whose simulated alpha out meets ``alpha_out``, found in at most 8 sim calls."""
        try:
            block = self._read(lambda s: int(s.get_current_block()), _READ_WAITS, _READ_TIMEOUT)  # pool moves per block
        except Exception as error:
            _LOGGER.warning('Alpha sim head read failed error=%s', error)
            return None
        price = self.price_rao(netuid)
        tao = -(-alpha_out * price // RAO) if price else RAO
        short, enough = 0, None
        for _ in range(SIM_BUY_CALLS):
            out = self._sim('sim_swap_tao_for_alpha', {'netuid': netuid, 'tao': tao}, 'alpha_amount', block)
            if out is None:
                return None
            if out >= alpha_out:
                enough = tao
            else:
                short = tao
            if enough is None:
                tao = -(-tao * alpha_out // out) + 1 if out else tao * 2  # scale up, one rao past the estimate
            elif enough - short <= 1:
                break
            elif short == 0:
                tao = min(enough - 1, -(-enough * alpha_out // out))  # scale down, never past the known-good size
            else:
                tao = (short + enough) // 2
        if enough is None:
            _LOGGER.warning(
                'Alpha sim_buy never met the target netuid=%d alpha_out=%d calls=%d — no size',
                netuid,
                alpha_out,
                SIM_BUY_CALLS,
            )
        return enough

    def sim_sell(self, netuid: int, alpha_in: int) -> int | None:
        return self._sim('sim_swap_alpha_for_tao', {'netuid': netuid, 'alpha': alpha_in}, 'tao_amount')

    def _sim(self, method: str, params: dict, field: str, block: int | None = None) -> int | None:
        try:
            result = self._read(
                lambda s: s.query_runtime_api('SwapRuntimeApi', method, params, block=block), _SIM_WAITS, _SIM_TIMEOUT
            )
        except Exception as error:
            _LOGGER.warning('Alpha sim failed method=%s params=%s error=%s', method, params, error)
            return None
        return int(getattr(result, 'value', result)[field])

    def _gate(self, subnet: Subnet, max_swap: int) -> str | None:
        """The first gate a subnet fails, None when it passes them all."""
        if subnet.tao_in < self.config.alpha_depth_mult * max_swap:
            return 'depth'
        if not (subnet.transfers and subnet.subtoken):
            return 'flags'
        if subnet.netuid in self._snapshot.prune_order[: self.config.alpha_prune_skip]:
            return 'prune'
        if self._moves(self._prices.get(subnet.netuid, [])) >= self.config.alpha_jump_limit:
            return 'jumpy'
        if self.clock() - self._transfers_off.get(subnet.netuid, float('-inf')) < TOGGLE_MEMORY_SECS:
            return 'toggle'
        if subnet.price_rao <= 0:
            return 'price'
        if subnet.netuid not in self._validators:  # a fill needs a hotkey to buy into
            return 'validator'
        return None

    def _moves(self, samples: list[int]) -> int:
        """Moves past the cushion of the price we would have quoted, which reprices on every reprice-sized move."""
        moves, quoted = 0, None
        for price in samples:
            if price <= 0:
                continue
            if quoted is None:
                quoted = price
                continue
            drift = abs(price / quoted - 1)
            if drift > self.config.alpha_cushion:
                moves += 1
            if drift >= self.config.alpha_reprice:
                quoted = price
        return moves

    def _stale(self) -> bool:
        return self.clock() - self._read_at > 2 * self.tick_secs

    def _read_snapshot(self, subtensor) -> Snapshot:
        block = int(subtensor.get_current_block())
        self._require_fresh(subtensor, block)
        infos = subtensor.all_subnets(block=block)
        prices = _prices_at(subtensor, block)  # the SDK substitutes tao_in/alpha_in on a failed price read; we fail
        transfers = _flags(subtensor.query_map('SubtensorModule', 'TransferToggle', block=block))
        subtoken = _flags(subtensor.query_map('SubtensorModule', 'SubtokenEnabled', block=block))
        immunity = int(_value(subtensor.query_subtensor('NetworkImmunityPeriod', block=block)))
        subnets = {}
        for info in infos or ():
            netuid = int(info.netuid)
            if netuid == ROOT_NETUID:
                continue
            subnets[netuid] = Subnet(
                netuid=netuid,
                price_rao=prices[netuid],
                moving_price=float(info.moving_price),
                tao_in=int(info.tao_in.rao),
                transfers=transfers.get(netuid, True),  # TransferToggle defaults to true when unset
                subtoken=subtoken.get(netuid, False),
                registered_at=int(info.network_registered_at),
            )
        return Snapshot(block, subnets, _prune_order(subnets, block, immunity))

    def _require_fresh(self, subtensor, block: int) -> None:
        """A head that did not pass the last snapshot's, or whose Timestamp.Now is over a minute old, fails the read."""
        if self._snapshot is not None and block <= self._snapshot.block:
            raise StaleHeadError(f'head {block} did not advance past {self._snapshot.block}')
        age = self.clock() - int(_value(subtensor.query_module('Timestamp', 'Now', block=block))) / 1000
        if age > HEAD_MAX_AGE_SECS:
            raise StaleHeadError(f'head {block} is {age:.0f} s old')

    def _backfill(self, head: int) -> None:
        """One-time 24 h price history from archive blocks, oldest first; a failed backfill is skipped, not fatal."""
        step = int(self.tick_secs // BLOCK_SECS)
        prices: dict[int, list[int]] = {}
        try:
            for k in range(PRICE_SAMPLES - 1, 0, -1):
                at_block = self._read(
                    lambda s, block=head - k * step: _prices_at(s, block), _SNAPSHOT_WAITS, _SNAPSHOT_TIMEOUT
                )
                for netuid, price in at_block.items():
                    prices.setdefault(netuid, []).append(price)
        except Exception as error:
            _LOGGER.warning('Alpha price backfill failed head=%d error=%s — history starts now', head, error)
            return
        self._prices = prices
        _LOGGER.info('Alpha price history backfilled head=%d subnets=%d', head, len(prices))

    def _remember(self, snapshot: Snapshot) -> None:
        """Rebuild the histories as new lists and rebind them; readers on other threads never see a half-trimmed one."""
        now = self.clock()
        prices, transfers_off = {}, {}
        for netuid, subnet in snapshot.subnets.items():
            reborn = self._registered_at.get(netuid, subnet.registered_at) != subnet.registered_at
            if reborn:
                self._forget(netuid)
            history = [] if reborn else self._prices.get(netuid, [])
            last_off = float('-inf') if reborn else self._transfers_off.get(netuid, float('-inf'))
            prices[netuid] = [*history, subnet.price_rao][-PRICE_SAMPLES:]
            seen_off = now if not subnet.transfers else last_off
            if now - seen_off < TOGGLE_MEMORY_SECS:
                transfers_off[netuid] = seen_off
        self._prices, self._transfers_off = prices, transfers_off
        self._registered_at = {n: s.registered_at for n, s in snapshot.subnets.items()}
        self.ledger.save(
            {'prices': self._prices, 'transfers_off': self._transfers_off, 'registered_at': self._registered_at}
        )

    def _forget(self, netuid: int) -> None:
        """A dissolved and re-registered netuid is a new subnet: no toggle sighting, no top validator."""
        _LOGGER.info('Alpha subnet re-registered netuid=%d — its history starts over', netuid)
        self._validators.pop(netuid, None)
        self._validators_at.pop(netuid, None)

    def _refresh_validators(self, snapshot: Snapshot) -> None:
        """Largest alpha-stake hotkey with a validator permit, refreshed hourly; a failed read is retried next tick."""
        now = self.clock()
        self._validators = {n: hk for n, hk in self._validators.items() if n in snapshot.subnets}
        self._validators_at = {n: at for n, at in self._validators_at.items() if n in snapshot.subnets}
        for netuid in snapshot.subnets:
            if now - self._validators_at.get(netuid, float('-inf')) < VALIDATORS_REFRESH_SECS:
                continue
            try:
                info = self._read(
                    lambda s, n=netuid: s.get_metagraph_info(n, selected_indices=_METAGRAPH_FIELDS),
                    _READ_WAITS,
                    _READ_TIMEOUT,
                )
                best = _top_validator(info)
            except Exception as error:
                _LOGGER.warning('Alpha validators read failed netuid=%d error=%s', netuid, error)
                continue
            if best is None:
                self._validators.pop(netuid, None)
            else:
                self._validators[netuid] = best
            self._validators_at[netuid] = now

    def _read(self, call: Callable[[Any], Any], waits: tuple[int, ...], timeout: float) -> Any:
        """One locked RPC call, retried after each wait; any error drops the socket and the retry rotates off it."""
        for attempt, wait in enumerate((*waits, None)):
            with self._lock:
                try:
                    if self._subtensor is None:
                        self._subtensor = self.connect(self._dropped_endpoint)
                        self._subtensor.substrate.max_retries = 0  # the retries are ours, below
                    self._subtensor.substrate.retry_timeout = timeout  # per-call recv timeout, not the 60 s default
                    return call(self._subtensor)
                except Exception:
                    self._drop()
                    if wait is None:
                        raise
            _LOGGER.debug('Alpha market read retry attempt=%d wait=%ds', attempt + 1, wait)
            self.sleep(wait)

    def _drop(self) -> None:
        subtensor, self._subtensor = self._subtensor, None
        if subtensor is None:
            return
        self._dropped_endpoint = getattr(subtensor, 'chain_endpoint', None)
        try:
            subtensor.close()
        except Exception as error:
            _LOGGER.debug('Socket close failed error=%s', error)


def _prune_order(subnets: dict[int, Subnet], block: int, immunity: int) -> tuple[int, ...]:
    """Subtensor ``get_network_to_prune``: subnets out of immunity within 3 days by (moving price, registered_at)."""
    horizon = block + IMMUNITY_HORIZON_SECS // BLOCK_SECS
    candidates = [s for s in subnets.values() if s.registered_at + immunity <= horizon]
    return tuple(s.netuid for s in sorted(candidates, key=lambda s: (s.moving_price, s.registered_at)))


def _top_validator(info) -> str | None:
    if info is None or not info.hotkeys:
        return None
    permitted = [uid for uid, permit in enumerate(info.validator_permit) if permit]
    if not permitted:
        return None
    return info.hotkeys[max(permitted, key=lambda uid: int(info.alpha_stake[uid].rao))]


def _prices_at(subtensor, block: int) -> dict[int, int]:
    rows = subtensor.query_runtime_api('SwapRuntimeApi', 'current_alpha_price_all', block=block)
    return {int(row['netuid']): int(row['price']) for row in _value(rows)}


def _flags(records) -> dict[int, bool]:
    return {int(_value(netuid)): bool(_value(flag)) for netuid, flag in records}


def _value(scale: Any) -> Any:
    return getattr(scale, 'value', scale)
