"""Backing hubs as data. Everything per-hub (relay instances, pool families, eligibility purses) is
built by iterating ``HUBS``; nothing else in the validator names a hub."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple

from allways.solana import pdas


class Custody(str, Enum):
    LOCAL = 'local'  # collateral held by the Solana program itself; nothing to relay
    VAULT = 'vault'  # collateral held by a vault on another chain; a relay mirrors it


@dataclass(frozen=True)
class HubSpec:
    chain: str  # the backing chain id the program uses (Reservation/Swap.collateral_chain)
    backing_bit: int  # its bit in MinerState.active_backings
    custody: Custody
    vault_kind: Optional[str] = None  # adapter that reads/writes the vault: 'ink' today; e.g. 'evm' for an EVM hub

    @property
    def relayed(self) -> bool:
        return self.custody is Custody.VAULT


HUBS: Tuple[HubSpec, ...] = (
    HubSpec('sol', pdas.BACKING_BIT_SOL, Custody.LOCAL),
    HubSpec('tao', pdas.BACKING_BIT_TAO, Custody.VAULT, vault_kind='ink'),
)

_BY_CHAIN: Dict[str, HubSpec] = {h.chain: h for h in HUBS}

# The program's backing registry is the source of truth; a bit the validator has no spec for is a bug.
assert set(_BY_CHAIN) == set(pdas.BACKING_BITS), 'HUBS must cover exactly the program backing registry'
assert all(_BY_CHAIN[c].backing_bit == bit for c, bit in pdas.BACKING_BITS.items()), 'HUBS backing bits drifted'


def hub(chain: str) -> HubSpec:
    return _BY_CHAIN[str(chain).lower()]


def is_hub(chain: str) -> bool:
    return str(chain).lower() in _BY_CHAIN


def relayed_hubs() -> Tuple[HubSpec, ...]:
    return tuple(h for h in HUBS if h.relayed)
