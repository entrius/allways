"""Buy exactly the alpha a fill needs, into one hotkey, crash-safe via the journal."""

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import NamedTuple

from allways.constants import CONTRACT_MAX_TOTAL_EXTENSION_SECS
from allways.miner.alpha.config import AlphaConfig as Config
from allways.miner.alpha.journal import BUY_SENT, FAILED, HELD, SETTLED, SIZED, XFER_SENT, AlphaJournal
from allways.miner.alpha.logbook import get_logger, swap_log
from allways.miner.alpha.market import MIN_STAKE_RAO, AlphaMarket, fee_reserve_rao
from allways.miner.alpha.signer import DEAD, INCLUDED, POOL_REJECTED, Included, Signed, SubtensorSigner
from allways.miner.alpha.telemetry import guarded_alert, telemetry

TICK_SECS = 5
LATE_BUY_SECS = 60  # no buy this close to the deadline: the stake would not settle in time to send it on
MAX_TOPS = 3
STRANDED_SECS = 120
LIMIT_LADDER = (1.015, 1.05, 1.10)  # one rung per limit refusal, then no limit at all
NO_LIMIT = 2**64 - 1
LIMIT_ERRORS = frozenset({'SlippageTooHigh', 'PriceLimitExceeded'})  # subtensor and swap pallet names
BALANCE_ERRORS = frozenset({'NotEnoughBalanceToStake', 'BalanceWithdrawalError'})  # the TAO went elsewhere: resize
_RETRIED = LIMIT_ERRORS | BALANCE_ERRORS | {POOL_REJECTED}  # a limit miss climbs a rung; the rest resend as they were
_OWNING = frozenset({HELD, XFER_SENT})  # rows whose alpha is on the hotkey already
_DELIVERABLE = _OWNING | {SETTLED}  # rows delivery may send from, or already has
_EXPIRING = frozenset({SIZED, HELD})  # rows the seller may reclaim once the swap is past every extension


@dataclass(frozen=True)
class AlphaRequest:
    swap_key_hex: str
    netuid: int
    need: int
    to_address: str  # logged only
    deadline: int


class Position(NamedTuple):
    hotkey: str  # the one hotkey holding the most of our free alpha; the row follows it
    on_hotkey: int  # the raw alpha on it
    ours: int  # the free part of it, less what other held fills on that hotkey already own


class AlphaRouter:
    """C14 — one journal row per fill: size, buy with free TAO, top up until one hotkey holds the whole need."""

    def __init__(
        self,
        signer: SubtensorSigner,
        journal: AlphaJournal,
        market: AlphaMarket,
        assets: dict,
        coldkey: str,
        tao_free: Callable[[], int],
        config: Config,
        alert=None,
    ):
        self.signer = signer
        self.journal = journal
        self.market = market
        self.assets = assets
        self.coldkey = coldkey
        self.tao_free = tao_free
        self.config = config
        self.logger = get_logger('c14.alpha_router')
        self._alert = guarded_alert(alert, self.logger)
        self._refused: set[str] = set()
        self._unrouted: set[str] = set()  # requests waiting on a top validator, logged once each

    def request(self, req: AlphaRequest) -> None:
        """Open the journal row once. A reservation on our standing quote is a promise: only the clock refuses."""
        key = req.swap_key_hex
        if key in self._refused or self.journal.row(key) is not None:
            return
        log = swap_log(self.logger, key)
        if time.time() + LATE_BUY_SECS >= req.deadline:
            return self._refuse(key, log, f'deadline {req.deadline} is too close for a buy')
        hotkey = self.market.top_validator(req.netuid)
        if hotkey is None:
            if key not in self._unrouted:
                log.warning('No top validator known netuid=%d — the request waits', req.netuid)
            self._unrouted.add(key)
            return
        self._unrouted.discard(key)
        self.journal.open(key, req.netuid, req.need, hotkey, req.deadline)
        log.info(
            'Alpha request netuid=%d need=%d hotkey=%s to=%s deadline=%d',
            req.netuid,
            req.need,
            hotkey,
            req.to_address,
            req.deadline,
        )

    def outcome(self, key: str) -> str | None:
        if key in self._refused:
            return 'failed'
        row = self.journal.row(key)
        if row is None:
            return None
        if row['state'] == FAILED:
            return 'failed'
        return 'done' if row['state'] in _DELIVERABLE else None

    def tick(self) -> None:
        for key, row in self.journal.open_rows().items():
            log = swap_log(self.logger, key)
            try:
                self._step(key, row, log)
            except Exception as error:
                log.warning('Alpha step held state=%s: %s', row['state'], error)  # unknown is never zero: retry
        self.journal.prune(time.time())

    def _step(self, key: str, row: dict, log) -> None:
        if _unsettled_buy(row):  # whatever the state: a sent buy is settled before any new one is signed
            self._settle_buy(key, row, log)
        elif row['state'] == BUY_SENT:  # the buy is settled on disk: read what it bought
            self._landed(key, row, log)
        elif row['state'] in _EXPIRING and time.time() > row['deadline'] + CONTRACT_MAX_TOTAL_EXTENSION_SECS:
            self._expire(key, row, log)
        elif row['state'] == SIZED:
            self._size(key, row, log)
        elif row['state'] == HELD and self._position(key, row, log).ours < row['need']:
            if self.journal.transition(key, HELD, state=SIZED):  # a fee taken in alpha; not if a transfer beat the read
                log.warning('Alpha held fell short netuid=%d — topping up', row['netuid'])
        elif time.time() - row['updated_at'] > STRANDED_SECS:
            self._alert(
                f'alpha-stranded:{key}',
                f'Alpha bought, not sent: swap {key}',
                f'{row["state"]} for over {STRANDED_SECS} s. Check c8 delivery runs and the swap is still active; '
                'send the pinned amount by hand if it stays.',
            )

    def _size(self, key: str, row: dict, log) -> None:
        position = self._position(key, row, log)
        gap = row['need'] - position.ours
        if gap <= 0:
            self.journal.update(key, state=HELD, reserved_rao=0)
            log.info(
                'Alpha held netuid=%d hotkey=%s ours=%d need=%d',
                row['netuid'],
                position.hotkey,
                position.ours,
                row['need'],
            )
            return
        if time.time() + LATE_BUY_SECS >= row['deadline']:
            return self._fail(key, log, f'alpha-late:{key}', 'deadline too close for a buy')
        if not self.assets[f'sn{row["netuid"]}'].transfers_enabled():  # unreadable raises: the step waits
            log.warning('Alpha transfers off netuid=%d — no buy', row['netuid'])
            self._alert(
                f'alpha-transfers:{key}',
                f'Alpha transfers are off: swap {key}',
                f'netuid {row["netuid"]} cannot deliver, so nothing is bought. The validator defers: wait.',
            )
            return
        tao = self.market.sim_buy(row['netuid'], math.ceil(gap * (1 + self.config.alpha_buy_buffer)))
        if tao is None:
            log.warning('Alpha buy unsized netuid=%d gap=%d — waiting', row['netuid'], gap)
            return
        self._buy(key, row, position, max(tao, MIN_STAKE_RAO), log)

    def _buy(self, key: str, row: dict, position: Position, tao: int, log) -> None:
        """Under the signer lock, so no other coldkey spender moves between the free-TAO check and the submit."""
        with self.signer.lock:
            free = self.tao_free()
            others = self.journal.reserved_rao() - row['reserved_rao']
            reserve = fee_reserve_rao(self.config)
            if free - others - tao < reserve:
                log.warning(
                    'Alpha buy blocked: free=%d reserved=%d buy=%d keeps under %d rao', free, others, tao, reserve
                )
                self._alert(
                    'alpha-float', 'Free TAO is short for an alpha buy', f'Top up the TAO float: swap {key} waits.'
                )
                return
            spot = self.market.spot_rao(row['netuid'])  # L6: read at the sign, never the 10-min snapshot
            if spot is None:
                log.warning('Alpha spot unknown netuid=%d — waiting', row['netuid'])
                return
            limit = _limit(row, spot)
            call = self.signer.compose(
                'add_stake_limit',
                {
                    'hotkey': position.hotkey,
                    'netuid': row['netuid'],
                    'amount_staked': tao,
                    'limit_price': limit,
                    'allow_partial': False,
                },
            )
            self.journal.update(key, reserved_rao=tao)

            def record(signed: Signed) -> None:
                buy = {'nonce': signed.nonce, 'tx_hash': signed.tx_hash, 'head': signed.head, 'tao_in': tao}
                buy |= {'limit_price': limit, 'spot_rao': spot, 'position_rao': position.on_hotkey}
                self.journal.update(key, state=BUY_SENT, buys=[*row['buys'], buy])

            sent = self.signer.send(call, record)
        if sent is None:
            log.warning('Alpha buy not sent netuid=%d tao=%d — stays sized', row['netuid'], tao)
            return
        signed, included = sent
        log.info(
            'Alpha buy sent netuid=%d hotkey=%s tao=%d limit=%d tx=%s',
            row['netuid'],
            position.hotkey,
            tao,
            limit,
            signed.tx_hash,
        )
        if included is not None:
            self._landed(key, self._outcome(key, included), log)

    def _settle_buy(self, key: str, row: dict, log) -> None:
        buy = row['buys'][-1]
        verdict, included = self.signer.resolve(Signed(buy['nonce'], buy['tx_hash'], buy['head']))
        if verdict == INCLUDED:
            self._landed(key, self._outcome(key, included), log)
        elif verdict == DEAD:
            log.warning('Alpha buy died tx=%s — re-reading the position', buy['tx_hash'])
            self.journal.update(
                key, state=SIZED, reserved_rao=0, buys=_settled(row, {'success': False, 'error': 'dead'})
            )
        else:
            log.debug('Alpha buy pending tx=%s', buy['tx_hash'])

    def _outcome(self, key: str, included: Included) -> dict:
        """The landed buy on disk before anything reads what it bought, so a failed read retries from here."""
        outcome = {
            'block': included.block,
            'success': included.success,
            'error': included.error,
            'fee_rao': included.fee_rao,
        }
        return self.journal.update(key, buys=_settled(self.journal.row(key), outcome))

    def _landed(self, key: str, row: dict, log) -> None:
        buy = row['buys'][-1]
        if not buy['success']:
            return self._buy_refused(key, buy, log)
        position = self._position(key, row, log)
        telemetry.event(
            'alpha_bought',
            swap=key,
            netuid=row['netuid'],
            hotkey=position.hotkey,
            tao_in=buy['tao_in'],
            alpha_out=position.on_hotkey - buy['position_rao'],
            spot_rao=buy['spot_rao'],
            limit_rao=buy['limit_price'],
            fee_rao=buy['fee_rao'],
            tx=buy['tx_hash'],
        )
        log.info(
            'Alpha bought tao_in=%d ours=%d need=%d fee=%d tx=%s',
            buy['tao_in'],
            position.ours,
            row['need'],
            buy['fee_rao'],
            buy['tx_hash'],
        )
        if position.ours >= row['need']:
            self.journal.update(key, state=HELD, reserved_rao=0)
        elif row['tops'] >= MAX_TOPS:
            self.journal.update(key, state=FAILED, reserved_rao=0)
            self._alert(
                f'alpha-short:{key}',
                f'Alpha still short after {MAX_TOPS} top-ups: swap {key}',
                f'Held {position.ours} of {row["need"]}.',
            )
        else:
            self.journal.update(key, state=SIZED, reserved_rao=0, tops=row['tops'] + 1)

    def _buy_refused(self, key: str, buy: dict, log) -> None:
        """A limit miss climbs a rung, a pool rejection resends, short TAO resizes with an alert; the rest is final."""
        error = buy['error']
        if error not in _RETRIED:
            return self._fail(key, log, f'alpha-buy:{key}', f'buy failed on chain error={error}')
        self.journal.update(key, state=SIZED, reserved_rao=0)
        log.warning('Alpha buy refused tx=%s error=%s — retrying', buy['tx_hash'], error)
        if error in BALANCE_ERRORS:
            self._alert(
                'alpha-float', 'Free TAO is short for an alpha buy', f'Top up the TAO float: swap {key} retries.'
            )

    def _position(self, key: str, row: dict, log) -> Position:
        """Every hotkey on the netuid at one block no older than the last landed buy; the row follows the best one."""
        netuid, floor = row['netuid'], _landed_block(row)
        subtensor = self.assets[f'sn{netuid}'].subtensor
        block = subtensor.get_current_block()
        if block < floor:
            raise LookupError(f'node at block {block} is behind the landed buy at {floor}')
        on_hotkey, free = _stakes_at(subtensor, self.coldkey, netuid, block)
        claimed = self._claimed(key, row)
        ours = {hotkey: max(0, min(alpha, free) - claimed.get(hotkey, 0)) for hotkey, alpha in on_hotkey.items()}
        ours.setdefault(row['hotkey'], 0)
        hotkey = max(ours, key=lambda candidate: (ours[candidate], candidate == row['hotkey']))  # ties keep the row's
        if hotkey != row['hotkey'] and self.journal.transition(key, row['state'], hotkey=hotkey):  # not a sent transfer
            log.warning('Alpha row follows hotkey=%s holding ours=%d (was %s)', hotkey, ours[hotkey], row['hotkey'])
        return Position(hotkey, on_hotkey.get(hotkey, 0), ours[hotkey])

    def _claimed(self, key: str, row: dict) -> dict[str, int]:
        """Alpha other fills already hold on this netuid, by hotkey; a fill still buying owns nothing."""
        claimed: dict[str, int] = {}
        for other_key, other in self.journal.open_rows().items():
            if other_key != key and other['state'] in _OWNING and other['netuid'] == row['netuid']:
                claimed[other['hotkey']] = claimed.get(other['hotkey'], 0) + other['need']
        return claimed

    def _expire(self, key: str, row: dict, log) -> None:
        self.journal.update(key, state=FAILED, reserved_rao=0)
        log.warning('Alpha swap expired in %s — the seller reclaims what it holds', row['state'])
        self._alert(
            f'alpha-expired:{key}',
            f'Alpha swap expired: swap {key}',
            f'{row["state"]} past the deadline and every extension. Nothing to do: the seller sells what it holds.',
        )

    def _refuse(self, key: str, log, why: str) -> None:
        self._refused.add(key)
        log.warning('Alpha request refused: %s', why)

    def _fail(self, key: str, log, alert_key: str, why: str) -> None:
        self.journal.update(key, state=FAILED, reserved_rao=0)
        log.critical('Alpha sourcing failed — the swap risks a timeout slash and a strike: %s', why)
        self._alert(alert_key, f'Alpha sourcing failed: swap {key}', f'{why}. Deliver it by hand or take the strike.')


def _unsettled_buy(row: dict) -> bool:
    return bool(row['buys']) and 'success' not in row['buys'][-1]


def _settled(row: dict, outcome: dict) -> list[dict]:
    return [*row['buys'][:-1], row['buys'][-1] | outcome]


def _landed_block(row: dict) -> int:
    return max((buy['block'] for buy in row['buys'] if buy.get('success')), default=0)


def _stakes_at(subtensor, coldkey: str, netuid: int, block: int) -> tuple[dict[str, int], int]:
    """(alpha by hotkey, free alpha) of the coldkey on the netuid at one block: allways' Alpha reads, pinned."""
    infos = subtensor.get_stake_info_for_coldkey(coldkey, block=block)
    on_hotkey = {info.hotkey_ss58: int(info.stake.rao) for info in infos if int(info.netuid) == netuid}
    availability = subtensor.query_runtime_api(
        'StakeInfoRuntimeApi', 'get_stake_availability_for_coldkeys', [[coldkey], [netuid]], block=block
    )
    by_netuid = getattr(availability, 'value', availability).get(coldkey, {})
    return on_hotkey, int(by_netuid[netuid]['available']) if netuid in by_netuid else 0


def _limit(row: dict, spot: int) -> int:
    """The ladder rung is the count of limit refusals so far; past the ladder the buy takes any price."""
    rung = sum(buy.get('error') in LIMIT_ERRORS for buy in row['buys'])
    return math.ceil(spot * LIMIT_LADDER[rung]) if rung < len(LIMIT_LADDER) else NO_LIMIT
