"""One relay per vault-custody hub, built from ``hubs.relayed_hubs()``.

A relay mirrors its vault onto Solana (attestations, heartbeat) and carries Solana verdicts to its vault
(slashes, fee true-ups, exits). Solana writes go through the actuator's queue; vault writes go through
the hub's ``VaultAdapter``, so a new hub on a different chain is a new adapter, not a new relay."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Protocol

from allways.validator_v2.hubs import HubSpec
from allways.validator_v2.runtime.bus import Latest, Subscription
from allways.validator_v2.runtime.component import Component
from allways.validator_v2.runtime.deadline_queue import DeadlineQueue
from allways.validator_v2.world.snapshot import World


class VaultAdapter(Protocol):
    """What a relay needs from a hub's vault, whatever chain it lives on. The ink! TAO vault client
    (``allways.vault``) already provides these; an EVM hub implements the same surface."""

    def head(self) -> Optional[int]: ...

    def get_bond_state(self, hotkey: str) -> Any: ...

    def get_settled_total(self, hotkey: str) -> Optional[int]: ...

    def is_slashed(self, swap_ref: bytes) -> Optional[bool]: ...

    def poll_events(self, start: int, end: int) -> Any: ...

    def vote_slash(self, *args: Any) -> Any: ...

    def vote_collect_fees_batch(self, entries: Any) -> Any: ...

    def vote_unlock(self, *args: Any) -> Any: ...


# vault_kind → factory(hub) -> VaultAdapter
VAULT_ADAPTERS: Dict[str, Callable[[HubSpec], VaultAdapter]] = {}


class HubRelay(Component):
    """Not critical: a lagging relay keeps its hub's entry fuse shut (that is the fuse's job) but never
    delays the actuator. Every meta key it persists is namespaced by ``hub.chain``.

    Fee accrual: until the program keeps a per-(miner, backing) accrued-fee counter, the relay seeds each
    miner's accrued total from the vault's settled total at boot and adds fees seen since, which only
    needs history back to the last settled true-up instead of the vault's whole lifetime."""

    interval_secs = 6.0
    stall_secs = 600.0

    def __init__(
        self,
        hub: HubSpec,
        world: Latest[World],
        events: Subscription,
        vault_factory: Callable[[], VaultAdapter],
        actuator_queue: DeadlineQueue,
        store: Any,
        clock=time.time,
    ) -> None:
        super().__init__(clock)
        self.name = f'relay-{hub.chain}'
        self.hub = hub
        self.world = world
        self.events = events
        self._vault_factory = vault_factory
        self.vault: Optional[VaultAdapter] = None
        self.actuator_queue = actuator_queue
        self.store = store
        self.reconciled = False

    def setup(self) -> None:
        self.vault = self._vault_factory()

    def tick(self, now: float) -> None:
        """Order matters, as in ``allways.validator.relay.engine.BondRelay.step``: boot reconcile barrier,
        then fold events into the fee/slash ledgers, flush attestations, relay slashes, run exits, fee
        cadence, heartbeat. Lift those jobs as-is; swap their Solana sends for actuator intents."""
        for record in self.events.drain(limit=5_000):
            pass
