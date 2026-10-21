"""The production SwapContext: facts from the World, chain answers from the watchers, and the
actuator's own memory of what it verified. Nothing here waits on a chain: an answer not in yet is
``NotReady``."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Set, Tuple, Union

from allways.cli.swap_commands.swap_intake import collateral_matches, leg_value
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.actuator.swaps import UNKNOWN, Leg, NotReady, Unknown, is_tx_fresh, status_of, user_receives
from allways.validator.chains.watcher import ChainWatcher, Probe, Verdict, verify_probe
from allways.validator.feed.events import HISTORY_HORIZON_SECS
from allways.validator.store.db import Store
from allways.validator.world.snapshot import World, tx_key

OWNER, VERSION = 'actuator', 1
TABLES = [
    (
        'actuator_deliveries',
        'CREATE TABLE IF NOT EXISTS actuator_deliveries (miner TEXT NOT NULL, tx_key TEXT NOT NULL, '
        'swap_key TEXT NOT NULL, seen_at INTEGER NOT NULL, PRIMARY KEY (miner, tx_key))',
    ),
    (
        'actuator_collateral',
        'CREATE TABLE IF NOT EXISTS actuator_collateral (miner TEXT NOT NULL, backing TEXT NOT NULL, '
        'created_at INTEGER NOT NULL, collateral_amount INTEGER NOT NULL, ok INTEGER NOT NULL, '
        'PRIMARY KEY (miner, backing, created_at, collateral_amount))',
    ),
]


class SwapMemory:
    """What this validator itself verified, kept across restarts: the swap each miner's delivery tx confirmed here
    (never a second one), and alpha collateral verdicts priced at a fill block a lite node may forget."""

    def __init__(self, store: Store) -> None:
        self.store = store
        store.declare(OWNER, VERSION, TABLES)
        conn = store.conn()
        self.deliveries: Dict[Tuple[str, str], Tuple[str, int]] = {
            (r['miner'], r['tx_key']): (r['swap_key'], r['seen_at'])
            for r in conn.execute('SELECT * FROM actuator_deliveries')
        }
        self.delivered: Set[str] = {k for _, k in self.deliveries}  # tx_key of every delivery verified here
        self.collateral: Dict[Tuple[str, str, int, int], bool] = {
            (r['miner'], r['backing'], r['created_at'], r['collateral_amount']): bool(r['ok'])
            for r in conn.execute('SELECT * FROM actuator_collateral')
        }

    def claim_delivery(self, miner: str, key: str, swap_key: str, now: int) -> bool:
        held = self.deliveries.get((miner, key))
        if held is None:
            self.deliveries[(miner, key)] = (swap_key, now)
            self.delivered.add(key)
            with self.store.tx() as c:
                c.execute('INSERT OR IGNORE INTO actuator_deliveries VALUES (?, ?, ?, ?)', (miner, key, swap_key, now))
            return True
        return held[0] == swap_key

    def record_collateral(self, key: Tuple[str, str, int, int], ok: bool) -> None:
        self.collateral[key] = ok
        with self.store.tx() as c:
            c.execute('INSERT OR REPLACE INTO actuator_collateral VALUES (?, ?, ?, ?, ?)', (*key, int(ok)))

    def prune(self, now: int) -> None:
        floor = now - HISTORY_HORIZON_SECS
        stale = [k for k, (_, seen) in self.deliveries.items() if seen < floor]
        if not stale:
            return
        for k in stale:
            del self.deliveries[k]
        self.delivered = {k for _, k in self.deliveries}
        with self.store.tx() as c:
            c.execute('DELETE FROM actuator_deliveries WHERE seen_at < ?', (floor,))


class WorldContext:
    def __init__(
        self,
        world: World,
        watchers: Dict[str, ChainWatcher],
        providers: Dict[str, Any],
        memory: SwapMemory,
        now: int,
        owes_settlement: Optional[Callable[[Any], bool]] = None,
        program_id: Any = None,
    ) -> None:
        self.world = world
        self.watchers = watchers
        self.providers = providers
        self.memory = memory
        self.now = now
        self._owes = owes_settlement
        self.program_id = program_id
        self.legs: Dict[str, Leg] = {}  # chain → the leg read this decision, for the status report

    def provider(self, chain: str) -> Optional[Any]:
        return self.providers.get(chain)

    def reservation(self, swap: Any) -> Optional[Any]:
        return self.world.get(
            'Reservation', pdas.reservation_pda(swap.miner, str(swap.collateral_chain), self.program_id)
        )

    def _ask(self, chain: str, probe: Probe) -> Verdict:
        watcher = self.watchers.get(chain)
        verdict = watcher.verdict(probe) if watcher is not None else None
        if verdict is None:
            raise NotReady(f'{chain} {probe.name}')
        return verdict

    def leg(self, chain: str, tx_hash: str, recipient: str, amount: int, block_hint: int, sender: str) -> Leg:
        provider = self.providers.get(chain)
        if provider is None or chain not in self.watchers:
            return Leg('down')
        canonical = provider.chain.canonical_tx_hash(tx_hash)
        if canonical is None:
            return Leg('no')
        v = self._ask(chain, verify_probe(chain, canonical, recipient, amount, block_hint, sender or None))
        if v.error:
            if not v.unreachable:
                raise NotReady(f'{chain} verify ({v.error})')  # a bug or odd answer is never "down"
            leg = Leg('down', checked_at=v.checked_at)
        elif v.info is None:
            leg = Leg('no', checked_at=v.checked_at)
        else:
            leg = Leg('ok' if v.info.confirmed else 'pending', v.info, v.checked_at)
        self.legs[chain] = leg
        return leg

    def cancel_evidence(self, swap: Any, amount: int) -> Union[int, None, Unknown]:
        args = (
            swap.user_to_addr,
            int(amount),
            getattr(swap, 'to_tx_hash', '') or None,
            swap.miner_to_addr or None,
            int(getattr(swap, 'to_tx_block', 0) or 0),  # where the delivery landed: alpha reads refusals there
        )
        probe = Probe(
            swap.to_chain,
            'cancel_evidence',
            args,
            run=lambda a: a.cancel_evidence(args[0], args[1], args[2], from_address=args[3], block_hint=args[4]),
        )
        v = self._ask(swap.to_chain, probe)
        return UNKNOWN if v.error else v.info  # never cancel on a failed check, and never read it as "none"

    def delivery_refused(self, swap: Any) -> bool:
        args = (swap.user_to_addr, int(swap.initiated_at))
        probe = Probe(swap.to_chain, 'delivery_refused', args, run=lambda a: a.delivery_refused(*args))
        v = self._ask(swap.to_chain, probe)
        return True if v.error else bool(v.info)  # an unknown answer defers the slash

    def collateral_ok(self, swap: Any, reservation: Any) -> bool:
        backing = str(swap.collateral_chain)
        key = (str(swap.miner), backing, int(reservation.created_at), int(swap.collateral_amount))
        if key in self.memory.collateral:
            return self.memory.collateral[key]
        legs = (swap.from_chain, int(swap.from_amount), swap.to_chain, int(swap.to_amount))

        def price(_asset: Any) -> bool:
            value = leg_value(backing, *legs, self.providers, created_at=key[2])
            return collateral_matches(key[3], value)

        v = self._ask(backing, Probe(backing, 'collateral', (key, legs), run=price, final=lambda ok: True))
        if v.error:
            raise NotReady(f'alpha leg unpriceable at fill block: {v.error}')
        self.memory.record_collateral(key, bool(v.info))
        return bool(v.info)

    def deposit_claimed(self, swap: Any) -> bool:
        """Another swap claimed this deposit first, or a swap already spent it (a closed swap's key can be
        claimed again). The ledger covers every live swap: the World marks a live Swap's deposit spent
        whenever it reads one, under whichever spelling it used."""
        held = self.world.deposits.get(tx_key(swap.from_tx_hash))
        return held is not None and (held.swap_key != swap_key_from_tx_hash(swap.from_tx_hash).hex() or held.initiated)

    def payout_reuse(self, swap: Any) -> bool:
        """The deposit is a delivery this validator verified for some swap. A payout from a posted miner wallet
        can't be named as a taker's source at all (``finalize_reservation`` refuses it), and a delivery a miner
        only reported is not evidence: ``to_tx_hash`` is free text, so any miner could name a user's real
        deposit and get it refused."""
        return tx_key(swap.from_tx_hash) in self.memory.delivered

    def delivery_is_deposit(self, swap: Any) -> bool:
        """Some swap claimed this swap's delivery tx as its source deposit, so it pays that swap, not this one."""
        return tx_key(swap.to_tx_hash) in self.world.deposits

    def claim_delivery(self, swap: Any, info: Any, fee_divisor: int) -> bool:
        """This swap may use its verified delivery tx (``info``): no other swap of the miner completed on it, no
        live swap of the miner that the same tx would also pay was fulfilled before it, and this validator never
        used it for another swap. The World rule is what every validator agrees on — they read the same World,
        so a shared tx goes to the same swap, and a swap the tx doesn't pay (a mistaken fulfillment) never
        blocks the one it does. The memory is the backstop for a World that lagged: one tx, one swap, here."""
        miner, delivery = str(swap.miner), tx_key(swap.to_tx_hash)
        key = swap_key_from_tx_hash(swap.from_tx_hash).hex()
        for other_key, f in self.world.fulfillments.items():
            if (
                f.settled
                and f.miner == miner
                and other_key != key
                and f.to_tx_hash
                and tx_key(f.to_tx_hash) == delivery
            ):
                return False
        mine = (int(getattr(swap, 'fulfilled_at', 0) or 0), key)
        for _, other in self.world.all('Swap'):
            other_key = swap_key_from_tx_hash(other.from_tx_hash).hex()
            if (
                other_key != key
                and status_of(other) == 'Fulfilled'
                and str(other.miner) == miner
                and other.to_tx_hash
                and tx_key(other.to_tx_hash) == delivery
                and (int(getattr(other, 'fulfilled_at', 0) or 0), other_key) < mine
                and self._also_pays(other, swap, info, fee_divisor)
            ):
                return False
        return self.memory.claim_delivery(miner, delivery, key, self.now)

    def _also_pays(self, other: Any, swap: Any, info: Any, fee_divisor: int) -> bool:
        """The tx verified for ``swap`` would verify for ``other`` too: same dest chain, recipient and sender,
        enough for ``other``'s leg, and fresh for it."""
        provider = self.providers.get(swap.to_chain)
        if provider is None or other.to_chain != swap.to_chain:
            return False
        norm = provider.chain.normalize_address
        grace = getattr(provider.chain_def, 'replay_grace_secs', 0)
        return (
            norm(other.user_to_addr) == norm(swap.user_to_addr)
            and norm(other.miner_to_addr or '') == norm(swap.miner_to_addr or '')
            and user_receives(other, fee_divisor) <= int(info.amount)
            and is_tx_fresh(info, int(other.initiated_at), grace)
        )

    def owes_settlement(self, swap: Any) -> bool:
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if self._owes is None or not backing or backing == 'sol':
            return False
        return self._owes(swap)
