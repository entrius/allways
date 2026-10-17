"""The production SwapContext: facts from the World, chain answers from the watchers, and the
actuator's own memory of what it verified. Nothing here waits on a chain: an answer not in yet is
``NotReady``."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from allways.cli.swap_commands.swap_intake import collateral_matches, leg_value
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.validator_v2.actuator.swaps import Leg, NotReady, status_of
from allways.validator_v2.chains.watcher import ChainWatcher, Probe, Verdict, verify_probe
from allways.validator_v2.feed.events import HISTORY_HORIZON_SECS
from allways.validator_v2.store.db import Store
from allways.validator_v2.world.snapshot import World, tx_key

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
    """What this validator itself verified, kept across restarts: the delivery each miner's tx confirmed
    (first verified wins), and alpha collateral verdicts priced at a fill block a lite node may forget."""

    def __init__(self, store: Store) -> None:
        self.store = store
        store.declare(OWNER, VERSION, TABLES)
        conn = store.conn()
        self.deliveries: Dict[Tuple[str, str], Tuple[str, int]] = {
            (r['miner'], r['tx_key']): (r['swap_key'], r['seen_at'])
            for r in conn.execute('SELECT * FROM actuator_deliveries')
        }
        self.collateral: Dict[Tuple[str, str, int, int], bool] = {
            (r['miner'], r['backing'], r['created_at'], r['collateral_amount']): bool(r['ok'])
            for r in conn.execute('SELECT * FROM actuator_collateral')
        }

    def claim_delivery(self, miner: str, key: str, swap_key: str, now: int) -> bool:
        held = self.deliveries.get((miner, key))
        if held is None:
            self.deliveries[(miner, key)] = (swap_key, now)
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

    def cancel_evidence(self, swap: Any, amount: int) -> Optional[int]:
        args = (swap.user_to_addr, int(amount), getattr(swap, 'to_tx_hash', '') or None, swap.miner_to_addr or None)
        probe = Probe(
            swap.to_chain,
            'cancel_evidence',
            args,
            run=lambda a: a.cancel_evidence(args[0], args[1], args[2], from_address=args[3]),
        )
        v = self._ask(swap.to_chain, probe)
        return None if v.error else v.info  # no evidence when the check fails: never cancel on a failure

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
        claimed again)."""
        key, deposit = swap_key_from_tx_hash(swap.from_tx_hash).hex(), tx_key(swap.from_tx_hash)
        held = self.world.deposits.get(deposit)
        if held is not None and (held.swap_key != key or held.initiated):
            return True
        for _, other in self.world.all('Swap'):
            if status_of(other) in ('Active', 'Fulfilled') and tx_key(other.from_tx_hash) == deposit:
                if swap_key_from_tx_hash(other.from_tx_hash).hex() != key:
                    return True
        return False

    def payout_reuse(self, swap: Any) -> bool:
        """The deposit is some swap's payout: it matches a payout route, or it is a reported delivery."""
        provider = self.providers.get(swap.from_chain)
        if provider is None:
            return False
        norm = provider.chain.normalize_address
        sender, wallet = norm(swap.user_from_addr), norm(swap.miner_from_addr)
        for route in self.world.routes.values():
            if route.chain == swap.from_chain and norm(route.sender) == sender and norm(route.recipient) == wallet:
                return True
        deposit = tx_key(swap.from_tx_hash)
        if any(tx_key(f.to_tx_hash) == deposit for f in self.world.fulfillments.values() if f.to_tx_hash):
            return True
        return any(k == deposit for _, k in self.memory.deliveries)

    def claim_delivery(self, swap: Any) -> bool:
        """This swap may use its delivery tx: no other swap of the miner completed on it, and no other swap
        of the miner was verified on it here first."""
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
        return self.memory.claim_delivery(miner, delivery, key, self.now)

    def owes_settlement(self, swap: Any) -> bool:
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if self._owes is None or not backing or backing == 'sol':
            return False
        return self._owes(swap)
