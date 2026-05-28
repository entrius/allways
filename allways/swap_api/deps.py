"""Cached dependencies for swap-api routes.

Reads config from env once at startup and holds onto the subtensor, contract
client, and ephemeral wallet for the process lifetime. Routes pull these
through ``get_state()`` rather than rebuilding per-request — creating a fresh
wallet or subtensor on every call thrashes both the keystore and the WS pool.
"""

import os
from dataclasses import dataclass
from typing import Optional

import bittensor as bt
from fastapi import Request

from allways.cli.dendrite_lite import get_ephemeral_wallet
from allways.constants import CONTRACT_ADDRESS as DEFAULT_CONTRACT_ADDRESS
from allways.constants import NETUID_FINNEY
from allways.contract_client import AllwaysContractClient


@dataclass
class AppState:
    subtensor: bt.Subtensor
    contract_client: AllwaysContractClient
    ephemeral_wallet: bt.Wallet
    netuid: int
    contract_address: str
    quorum_timeout_s: float
    quorum_poll_interval_s: float


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def build_app_state(subtensor: Optional[bt.Subtensor] = None) -> AppState:
    """Construct the singleton state pinned to the FastAPI app.

    Subtensor connection target is the standard bittensor ``network`` arg
    (e.g. ``finney``, ``test``, or a ws URL); read from ``WS_ENDPOINT`` to
    match the spec's env contract.
    """
    network = os.environ.get('WS_ENDPOINT', 'finney')
    netuid = _env_int('NETUID', NETUID_FINNEY)
    contract_address = os.environ.get('CONTRACT_ADDRESS', DEFAULT_CONTRACT_ADDRESS)
    quorum_timeout_s = _env_float('SWAP_API_QUORUM_TIMEOUT_S', 60.0)
    quorum_poll_interval_s = _env_float('SWAP_API_QUORUM_POLL_S', 2.0)

    if subtensor is None:
        subtensor = bt.Subtensor(network=network)
    contract_client = AllwaysContractClient(contract_address=contract_address, subtensor=subtensor)
    ephemeral_wallet = get_ephemeral_wallet()

    return AppState(
        subtensor=subtensor,
        contract_client=contract_client,
        ephemeral_wallet=ephemeral_wallet,
        netuid=netuid,
        contract_address=contract_address,
        quorum_timeout_s=quorum_timeout_s,
        quorum_poll_interval_s=quorum_poll_interval_s,
    )


def get_state(request: Request) -> AppState:
    return request.app.state.allways
