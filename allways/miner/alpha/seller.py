"""Sell every alpha position no open fill needs back to TAO, at once: nothing is held between fills."""

import time
from collections.abc import Callable
from dataclasses import dataclass

from allways.constants import is_alpha
from allways.miner.alpha.journal import XFER_SENT, AlphaJournal
from allways.miner.alpha.logbook import get_logger
from allways.miner.alpha.market import MIN_STAKE_RAO, RAO, AlphaMarket
from allways.miner.alpha.signer import INCLUDED, WAIT, Included, Signed, SubtensorSigner
from allways.miner.alpha.telemetry import guarded_alert, telemetry

TICK_SECS = 60
TOLERANCE_LADDER = (0.01, 0.02, 0.05, 0.10, 0.20)
RUNG_WAIT_SECS = 600
HOTKEYS_CAP = 64
HOTKEYS_CAP_ALERT = 'hotkeys-cap'
HELD_ALERT = 'alpha-held'


@dataclass(frozen=True)
class _Sale:
    """A remove_stake_limit in flight; kept in memory only, since a sell is idempotent by re-read."""

    signed: Signed
    netuid: int
    hotkey: str
    alpha_in: int
    tao_out: int | None
    spot_rao: int
    tolerance: float


class AlphaSeller:
    """c13: one stake read, then one remove_stake_limit per position that no open fill needs; journal-free."""

    def __init__(
        self,
        signer: SubtensorSigner,
        journal: AlphaJournal,
        market: AlphaMarket,
        assets: dict,
        coldkey: str,
        tao_max_swap: Callable[[], int | None],
        alert=None,
        keep_hotkeys: frozenset[str] = frozenset(),
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.signer = signer
        self.journal = journal
        self.market = market
        self.alphas = {asset.netuid: asset for chain_id, asset in assets.items() if is_alpha(chain_id)}
        self.coldkey = coldkey
        self.tao_max_swap = tao_max_swap
        self.keep_hotkeys = keep_hotkeys  # our miner hotkey: SN7 pays its incentive as stake there, never for sale
        self.clock = clock
        self.logger = get_logger('c13.alpha_seller')
        self._alert = guarded_alert(alert, self.logger)
        self._rung: dict[tuple[int, str], int] = {}
        self._failed_at: dict[tuple[int, str], float] = {}
        self._pending: dict[tuple[int, str], _Sale] = {}
        self._unsold_rao: dict[int, int | None] = {}  # netuid → value of the alpha no fill needs; None = unpriced

    def tick(self) -> None:
        if not self.alphas:
            return
        self._settle_pending()
        positions = self._positions()
        if positions is None:
            return
        self._check_hotkeys_cap(positions)
        rows = list(self.journal.open_rows().values())
        unsold_rao: dict[int, int | None] = {}
        for netuid, held in positions.items():
            needs = [row for row in rows if row['netuid'] == netuid]
            need_by_hotkey: dict[str, int] = {}
            for row in needs:
                need_by_hotkey[row['hotkey']] = need_by_hotkey.get(row['hotkey'], 0) + row['need']
            open_need = self.journal.open_need(netuid)
            unsold = sum(held.values()) - open_need
            if unsold <= 0:
                continue
            spot = self.market.spot_rao(netuid)
            unsold_rao[netuid] = None if spot is None else unsold * spot // RAO
            if spot is None or any(row['state'] == XFER_SENT for row in needs):
                continue
            budget = self._free_to_send(netuid) - open_need
            for hotkey, alpha in held.items():
                amount = min(alpha - need_by_hotkey.get(hotkey, 0), budget)
                if amount * spot // RAO < MIN_STAKE_RAO or not self._due(netuid, hotkey):
                    continue
                if self._sell(netuid, hotkey, amount, spot):
                    budget -= amount
        self._unsold_rao = unsold_rao

    def alpha_in_blocked(self) -> frozenset[int]:
        """Netuids whose alpha no fill needs is worth at least one TAO max swap; an unknown value or max blocks."""
        max_swap = self.tao_max_swap()
        return frozenset(
            netuid
            for netuid, value in self._unsold_rao.items()
            if max_swap is None or value is None or value >= max_swap
        )

    def _positions(self) -> dict[int, dict[str, int]] | None:
        """{netuid: {hotkey: alpha}} of our positions for sale on the alpha subnets, one read; None when unreadable."""
        try:
            infos = self._any_alpha().subtensor.get_stake_info_for_coldkey(self.coldkey)
        except Exception as error:
            self.logger.warning('Stake read failed error=%s — no sell this tick', error)
            return None
        positions: dict[int, dict[str, int]] = {}
        for info in infos:
            netuid, alpha = int(info.netuid), int(info.stake.rao)
            if netuid in self.alphas and alpha > 0 and info.hotkey_ss58 not in self.keep_hotkeys:
                positions.setdefault(netuid, {})[info.hotkey_ss58] = alpha
        return positions

    def _free_to_send(self, netuid: int) -> int:
        """Subtensor's own availability on the netuid; an unreadable one sells nothing."""
        try:
            return self.alphas[netuid].free_to_send(self.coldkey)
        except Exception as error:
            self.logger.warning('Free-to-send read failed netuid=%d error=%s — no sell this tick', netuid, error)
            return 0

    def _due(self, netuid: int, hotkey: str) -> bool:
        if (netuid, hotkey) in self._pending:
            return False
        return self.clock() - self._failed_at.get((netuid, hotkey), float('-inf')) >= RUNG_WAIT_SECS

    def _sell(self, netuid: int, hotkey: str, amount: int, spot: int) -> bool:
        """One fill-or-kill remove_stake_limit at the current rung; True when the alpha left or may still leave."""
        key = (netuid, hotkey)
        tolerance = TOLERANCE_LADDER[self._rung.get(key, 0)]
        limit = int(spot * (1 - tolerance))
        call = self.signer.compose(
            'remove_stake_limit',
            {
                'hotkey': hotkey,
                'netuid': netuid,
                'amount_unstaked': amount,
                'limit_price': limit,
                'allow_partial': False,
            },
        )
        tao_out = self.market.sim_sell(netuid, amount)

        def record(signed: Signed) -> None:
            # The log line is the sell's only record: nonce and hash reach disk before submit.
            self.logger.info(
                'Selling alpha netuid=%d hotkey=%s alpha=%d tolerance=%.0f%% nonce=%d tx_id=%s',
                netuid,
                hotkey,
                amount,
                tolerance * 100,
                signed.nonce,
                signed.tx_hash,
            )
            self._pending[key] = _Sale(signed, netuid, hotkey, amount, tao_out, spot, tolerance)

        sent = self.signer.send(call, record)
        if sent is None:
            return False
        signed, included = sent
        if included is None:
            self.logger.warning(
                'Sell ambiguous netuid=%d hotkey=%s alpha=%d tx_id=%s — resolving before any resend',
                netuid,
                hotkey,
                amount,
                signed.tx_hash,
            )
            return True
        self._settle(self._pending[key], included)
        return included.success

    def _settle_pending(self) -> None:
        for key, sale in list(self._pending.items()):
            verdict, included = self.signer.resolve(sale.signed)
            if verdict == WAIT:
                continue
            if verdict == INCLUDED:
                self._settle(sale, included)
                continue
            del self._pending[key]
            self.logger.info(
                'Sell died netuid=%d hotkey=%s tx_id=%s — the fresh read decides',
                sale.netuid,
                sale.hotkey,
                sale.signed.tx_hash,
            )

    def _settle(self, sale: _Sale, included: Included) -> None:
        key = (sale.netuid, sale.hotkey)
        self._pending.pop(key, None)
        if included.success:
            self._rung.pop(key, None)
            self._failed_at.pop(key, None)
            telemetry.event(
                'alpha_sold',
                netuid=sale.netuid,
                hotkey=sale.hotkey,
                alpha_in=sale.alpha_in,
                tao_out=sale.tao_out,
                spot_rao=sale.spot_rao,
                tolerance=sale.tolerance,
                fee_rao=included.fee_rao,
                tx=included.tx_hash,
            )
            self.logger.info(
                'Sold alpha netuid=%d hotkey=%s alpha=%d tao_out=%s tolerance=%.0f%% tx_id=%s',
                sale.netuid,
                sale.hotkey,
                sale.alpha_in,
                sale.tao_out,
                sale.tolerance * 100,
                included.tx_hash,
            )
            return
        self._failed_at[key] = self.clock()
        rung = self._rung.get(key, 0)
        if rung < len(TOLERANCE_LADDER) - 1:
            self._rung[key] = rung + 1
        else:
            self._alert(
                f'{HELD_ALERT}:{sale.netuid}',
                f'Alpha unsold on netuid {sale.netuid}',
                f'remove_stake_limit fails at {sale.tolerance:.0%} tolerance ({included.error}); retrying every 10 min',
            )
        self.logger.warning(
            'Sell failed netuid=%d hotkey=%s alpha=%d tolerance=%.0f%% error=%s tx_id=%s — next rung in %d s',
            sale.netuid,
            sale.hotkey,
            sale.alpha_in,
            sale.tolerance * 100,
            included.error,
            included.tx_hash,
            RUNG_WAIT_SECS,
        )

    def _any_alpha(self):
        """Coldkey-wide reads live on every alpha asset alike; any one serves."""
        return next(iter(self.alphas.values()))

    def _check_hotkeys_cap(self, positions: dict[int, dict[str, int]]) -> None:
        """Entries past the cap, less the dust an outsider can park on us: under the sell minimum, it never leaves."""
        try:
            hotkeys = set(self._any_alpha().staking_hotkeys(self.coldkey))
        except Exception as error:
            self.logger.warning('StakingHotkeys read failed error=%s', error)
            return
        count = len(hotkeys - self._dust_hotkeys(positions))
        if count > HOTKEYS_CAP:
            self._alert(
                HOTKEYS_CAP_ALERT,
                f'Our StakingHotkeys list holds {count} entries',
                f'Above {HOTKEYS_CAP}; a full list at 128 blocks transfers. Sell the positions to zero.',
            )

    def _dust_hotkeys(self, positions: dict[int, dict[str, int]]) -> set[str]:
        """Hotkeys whose every position is worth under the sell minimum at the snapshot price; unpriced is not dust."""
        dust, worth = set(), set()
        for netuid, held in positions.items():
            price = self.market.price_rao(netuid)
            for hotkey, alpha in held.items():
                (dust if price is not None and alpha * price // RAO < MIN_STAKE_RAO else worth).add(hotkey)
        return dust - worth
