"""World: one consistent read of the program's live accounts, shared by every component.

No component scans the program on its own. The reader refreshes each account type on its own cadence
(live-swap state every tick, slow-moving books less often) and publishes an immutable World."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import bittensor as bt

from allways.validator.binding import build_attribution
from allways.validator_v2.runtime.bus import Latest
from allways.validator_v2.runtime.component import Component

Accounts = Tuple[Tuple[str, Any], ...]  # ((pda, decoded account), ...)

# Account type → refresh interval (seconds). 0 = every tick.
REFRESH_SECS: Dict[str, float] = {
    'Swap': 0,
    'Reservation': 0,
    'Pool': 0,
    'MinerState': 0,
    'MinerQuote': 60,
    'BondAttestation': 60,
    'Binding': 60,
}
CONFIG_REFRESH_SECS = 60


@dataclass(frozen=True)
class World:
    taken_at: float  # unix seconds the newest read in it completed
    config: Any
    accounts: Mapping[str, Accounts]
    read_at: Mapping[str, float]  # per account type, when it was last read
    attribution: Mapping[str, str] = field(default_factory=dict)  # miner pubkey → verified hotkey ss58

    def all(self, name: str) -> Accounts:
        return self.accounts.get(name, ())

    def hotkey_for(self, miner: Any) -> Optional[str]:
        return self.attribution.get(str(miner))

    def age(self, name: str, now: float) -> float:
        return now - self.read_at.get(name, 0.0)


class _BindingSource:
    """Lets build_attribution verify the Binding accounts already in hand instead of scanning again."""

    def __init__(self, bindings: Accounts) -> None:
        self._bindings = bindings

    def get_all(self, name: str) -> Accounts:
        assert name == 'Binding'
        return self._bindings


class WorldReader(Component):
    """Critical: the actuator decides off this World, so a stalled reader means stale decisions."""

    name = 'world'
    interval_secs = 6.0
    stall_secs = 60.0
    critical = True

    def __init__(self, client_factory: Callable[[], Any], out: Latest[World], clock=time.time) -> None:
        super().__init__(clock)
        self._client_factory = client_factory
        self.out = out
        self._client: Any = None
        self._accounts: Dict[str, Accounts] = {}
        self._read_at: Dict[str, float] = {}
        self._config: Any = None
        self._config_at = 0.0
        self._attribution: Dict[str, str] = {}

    def setup(self) -> None:
        self._client = self._client_factory()

    def tick(self, now: float) -> None:
        for name, every in REFRESH_SECS.items():
            if name in self._accounts and now - self._read_at[name] < every:
                continue
            try:
                self._accounts[name] = tuple(self._client.get_all(name))
                self._read_at[name] = now
            except Exception as e:
                # Keep the previous read; consumers see its age and decide whether it is too old to act on.
                bt.logging.warning(f'world: {name} read failed, keeping read from {self._read_at.get(name)}: {e}')
                continue
            if name == 'Binding':
                self._attribution = build_attribution(_BindingSource(self._accounts['Binding']))
        if self._config is None or now - self._config_at >= CONFIG_REFRESH_SECS:
            self._config = self._client.get_config()
            self._config_at = now
        self.out.set(
            World(
                taken_at=now,
                config=self._config,
                accounts=dict(self._accounts),
                read_at=dict(self._read_at),
                attribution=dict(self._attribution),
            )
        )
