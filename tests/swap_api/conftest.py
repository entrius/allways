"""Test scaffolding for swap-api routes.

Builds a fully-mocked AppState so route handlers can run end-to-end without a
live subtensor, contract, or wallet. Each test mutates the FakeContractClient
to script the responses it wants. Real ``build_app_state`` is not invoked.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from allways.classes import MinerPair, Reservation, Swap, SwapStatus
from allways.swap_api.deps import AppState
from allways.swap_api.routes import chains, health, miners, proofs, swap


@dataclass
class FakeContractClient:
    miners: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    reservations: Dict[str, Reservation] = field(default_factory=dict)
    miner_swaps: Dict[str, List[Swap]] = field(default_factory=dict)

    def add_miner(
        self,
        hotkey: str,
        *,
        active: bool = True,
        has_swap: bool = False,
        collateral: int = 1_000_000_000_000,
        reserved_until: int = 0,
    ) -> None:
        self.miners[hotkey] = {
            'active': active,
            'has_swap': has_swap,
            'collateral': collateral,
            'reserved_until': reserved_until,
        }

    def get_miner_active_flag(self, hotkey: str) -> bool:
        return self.miners.get(hotkey, {}).get('active', False)

    def get_miner_has_active_swap(self, hotkey: str) -> bool:
        return self.miners.get(hotkey, {}).get('has_swap', False)

    def get_miner_collateral(self, hotkey: str) -> int:
        return self.miners.get(hotkey, {}).get('collateral', 0)

    def get_miner_reserved_until(self, hotkey: str) -> int:
        return self.miners.get(hotkey, {}).get('reserved_until', 0)

    def get_pending_reserve_vote_count(self, hotkey: str) -> int:
        return 0

    def get_reservation(self, hotkey: str) -> Optional[Reservation]:
        return self.reservations.get(hotkey)

    def get_miner_active_swaps(self, hotkey: str) -> List[Swap]:
        return self.miner_swaps.get(hotkey, [])

    def is_validator(self, hotkey: str) -> bool:
        return True


@dataclass
class FakeSubtensor:
    block: int = 100
    commitments: Dict[str, str] = field(default_factory=dict)

    def get_current_block(self) -> int:
        return self.block

    # discover_validators calls subtensor.metagraph(netuid).
    def metagraph(self, netuid: int):
        return _empty_metagraph()


class _EmptyAxon:
    is_serving = False


def _empty_metagraph():
    class _MG:
        n = 0
        validator_permit: List[bool] = []
        axons: List[Any] = []
        hotkeys: List[str] = []

    return _MG()


@dataclass
class FakeWallet:
    name: str = 'ephemeral'
    hotkey_str: str = 'default'


def make_app(
    *,
    contract_client: FakeContractClient,
    subtensor: FakeSubtensor,
    miner_pairs: Optional[List[MinerPair]] = None,
    broadcast_factory: Optional[Callable[[List[Any]], List[Any]]] = None,
    commitments: Optional[Dict[str, str]] = None,
) -> FastAPI:
    """Build a FastAPI app whose lifespan installs a hand-built AppState."""

    state = AppState(
        subtensor=subtensor,  # type: ignore[arg-type]
        contract_client=contract_client,  # type: ignore[arg-type]
        ephemeral_wallet=FakeWallet(),  # type: ignore[arg-type]
        netuid=2,
        contract_address='5DjJmTpcHZvF3aZZEafKBdo3ksmdUSZ8bBBUSFhW3Ce3xf1J',
        quorum_timeout_s=0.05,
        quorum_poll_interval_s=0.01,
    )

    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['GET', 'POST'], allow_headers=['*'])
    app.state.allways = state
    app.include_router(health.router)
    app.include_router(chains.router)
    app.include_router(miners.router)
    app.include_router(proofs.router)
    app.include_router(swap.router)

    # The routes call module-level functions; monkeypatch the lookups here so
    # tests stay self-contained without each one re-wiring imports.
    import allways.swap_api.routes.miners as miners_mod
    import allways.swap_api.routes.swap as swap_mod

    miners_mod.read_miner_commitments = lambda _s, _n: list(miner_pairs or [])
    swap_mod.get_commitment = lambda _s, _n, hk: (commitments or {}).get(hk)
    swap_mod._discover = lambda _state: []  # default: no validators
    if broadcast_factory is not None:

        async def _bc(_wallet, _axons, synapse, timeout=60.0):
            return broadcast_factory(synapse)

        swap_mod.broadcast_synapse_async = _bc
        swap_mod._discover = lambda _state: [object()]  # non-empty axon stub

    return app


@pytest.fixture
def client_factory():
    """Pytest fixture returning a TestClient builder for one-off app configs."""

    def _build(**kwargs) -> TestClient:
        return TestClient(make_app(**kwargs))

    return _build


def make_pair(
    hotkey: str,
    *,
    from_chain: str = 'btc',
    from_address: str = 'bc1qsource',
    to_chain: str = 'tao',
    to_address: str = '5Cdest',
    rate: float = 345.0,
    counter_rate: float = 0.003,
    uid: int = 1,
) -> MinerPair:
    return MinerPair(
        uid=uid,
        hotkey=hotkey,
        from_chain=from_chain,
        from_address=from_address,
        to_chain=to_chain,
        to_address=to_address,
        rate=rate,
        rate_str=f'{rate:g}',
        counter_rate=counter_rate,
        counter_rate_str=f'{counter_rate:g}',
    )


def make_swap(swap_id: int, miner_hotkey: str) -> Swap:
    return Swap(
        id=swap_id,
        user_hotkey='5Cuser',
        miner_hotkey=miner_hotkey,
        from_chain='btc',
        to_chain='tao',
        from_amount=10_000,
        to_amount=3_450_000,
        tao_amount=3_450_000,
        user_from_address='bc1qsource',
        user_to_address='5Cdest',
        status=SwapStatus.ACTIVE,
    )


def make_reservation(miner_hotkey: str, *, request_hash: str = '0xabc', reserved_until: int = 200) -> Reservation:
    return Reservation(
        hash=request_hash,
        from_addr='bc1qsource',
        from_chain='btc',
        to_chain='tao',
        tao_amount=3_450_000,
        from_amount=10_000,
        to_amount=3_450_000,
        reserved_until=reserved_until,
    )
