"""B3.6 integration: drive the contract on localnet, ingest events, run a real
scoring round, and assert the eligibility gate + Solana-sourced crown.

Deploys the B3.0 program, activates three bound miners (A quotes a better btc→tao
rate than B; S quotes the highest rate of all but it is unexecutable against the
on-chain swap bounds — a boundary-squat), ingests the emitted events through
``SolanaEventIndex`` with real sr25519 attribution, then checks the full B3.6
wiring against live on-chain state: the index reconstructs the active set +
collateral, ``build_eligibility`` reads the on-chain ``MinerState`` counters, the
crown replay credits the better-rate holder A (never B or the unexecutable S), and
a real ``calculate_miner_rewards`` round gates the (0-success) miners to weight 0 —
then, with all marked eligible, credits the btc→tao pool to A while the non-crown
miner B and the sentinel S both stay at 0. S earning nothing proves the non-zero
swap bounds reached the on-chain Config AND that ``is_executable_rate`` fires.

``successful_swaps`` can't be driven to the eligibility floor on localnet yet —
that needs the Phase-9 reservation/pool flow — so the eligible-path assertion
patches ``build_eligibility`` for miner A; everything else runs against real
on-chain accounts (Config, MinerState, Binding, MinerQuote).

Gated behind @pytest.mark.integration. Run with:
    uv run pytest tests/test_solana_scoring_integration.py -m integration -s
"""

import json
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import bittensor as bt
import pytest
from solders.keypair import Keypair

from allways.constants import DIRECTION_POOLS, RATE_PRECISION, RECYCLE_UID
from allways.solana.client import AllwaysSolanaClient
from allways.solana.events import SolanaEventIngest
from allways.solana.rpc import SolanaRpc
from allways.validator.binding import build_attribution
from allways.validator.bounds_cache import SolanaConfigCache
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import (
    build_eligibility,
    calculate_miner_rewards,
    replay_crown_time_window,
)
from allways.validator.state_store import ValidatorStateStore

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[1]
SO = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager.so'
PROG_KP = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager-keypair.json'
RPC = 'http://127.0.0.1:8899'
POOL_BTC_SOL = DIRECTION_POOLS[('btc', 'sol')]
# Non-zero swap bounds so is_executable_rate actually fires (0/0 fails open).
MIN_SWAP_RAO = 1_000_000
MAX_SWAP_RAO = 100_000_000
# A btc→tao rate so high its smallest fundable sat overshoots max_swap → unexecutable.
SENTINEL_RATE = 1_000_000


def _wait_health(rpc: SolanaRpc, timeout: float = 60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if rpc._call('getHealth', []) == 'ok':
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError('validator RPC not healthy in time')


def _airdrop(pubkey, sol):
    subprocess.run(
        ['solana', 'airdrop', str(sol), str(pubkey), '--url', RPC], check=True, capture_output=True, timeout=60
    )


@pytest.fixture(scope='module')
def env(tmp_path_factory):
    work = tmp_path_factory.mktemp('solana_b36')
    payer = Keypair()
    (work / 'payer.json').write_text(json.dumps(list(bytes(payer))))
    proc = subprocess.Popen(
        ['solana-test-validator', '--reset', '--quiet', '--ledger', str(work / 'ledger'), '--rpc-port', '8899'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_health(SolanaRpc(RPC))
        _airdrop(payer.pubkey(), 100)
        subprocess.run(
            [
                'solana',
                'program',
                'deploy',
                str(SO),
                '--program-id',
                str(PROG_KP),
                '--keypair',
                str(work / 'payer.json'),
                '--url',
                RPC,
            ],
            check=True,
            capture_output=True,
            timeout=120,
        )
        yield {'payer': payer, 'work': work}
    finally:
        proc.terminate()
        proc.wait()


def _hotkey():
    return bt.Keypair.create_from_mnemonic(bt.Keypair.generate_mnemonic())


def _activate_miner(admin, validators, rate_int):
    """Fund, quote (btc→sol), bind a fresh hotkey, and vote-activate a new miner.
    Returns (miner_pubkey, hotkey_keypair, collateral)."""
    miner = Keypair()
    _airdrop(miner.pubkey(), 10)
    mclient = AllwaysSolanaClient(RPC, keypair=miner)
    collateral = MAX_SWAP_RAO  # == max_swap → capacity_factor 1.0
    mclient.post_collateral(collateral)
    mclient.set_quote('btc', 'sol', 'minerBTC', 'minerSOL', rate_int, 1_000)
    hk = _hotkey()
    mclient.bind_hotkey(bytes.fromhex(hk.public_key.hex()), hk.sign(bytes(miner.pubkey())))
    validators[0].vote_activate(miner.pubkey())
    validators[1].vote_activate(miner.pubkey())  # quorum → MinerActivated
    return miner, hk, collateral


def test_scoring_round_off_solana_events(env):
    admin = AllwaysSolanaClient(RPC, keypair=env['payer'])
    admin.initialize(
        min_collateral=1_000_000,
        max_collateral=0,
        fulfillment_timeout_secs=12_600,
        consensus_threshold_percent=66,
        min_swap_amount=MIN_SWAP_RAO,
        max_swap_amount=MAX_SWAP_RAO,
        reservation_ttl_secs=1_800,
    )
    validators = []
    for _ in range(3):
        kp = Keypair()
        _airdrop(kp.pubkey(), 10)
        admin.add_validator(kp.pubkey(), 1)
        validators.append(AllwaysSolanaClient(RPC, keypair=kp))

    # btc→sol is reverse-canonical (SOL is the canonical source), so the LOWER rate wins: A's 345 beats
    # B's 400. S quotes a far-higher rate — the best-looking number — but it's unexecutable against the SOL
    # swap bounds (boundary-squat), so the executable-rate gate must keep it out of the crown entirely.
    miner_a, hk_a, coll_a = _activate_miner(admin, validators, 345 * RATE_PRECISION)
    miner_b, hk_b, coll_b = _activate_miner(admin, validators, 400 * RATE_PRECISION)
    miner_s, hk_s, coll_s = _activate_miner(admin, validators, SENTINEL_RATE * RATE_PRECISION)
    hka, hkb, hks = hk_a.ss58_address, hk_b.ss58_address, hk_s.ss58_address

    # ── ingest the emitted program events through the crown index ──
    store = ValidatorStateStore(db_path=env['work'] / 'state.db')
    records, cursor = SolanaEventIngest(admin).poll(until_sig=None)
    attribution = build_attribution(admin)
    assert attribution[str(miner_a.pubkey())] == hka
    assert attribution[str(miner_b.pubkey())] == hkb
    assert attribution[str(miner_s.pubkey())] == hks
    index = SolanaEventIndex(store)
    written = index.ingest(records, attribution)
    assert written > 0 and cursor is not None

    now = int(time.time())

    # ── the index reconstructs live miner state from the events ──
    assert index.get_active_miners_at(now) == {hka, hkb, hks}
    assert index.get_miner_collaterals_at(now) == {hka: coll_a, hkb: coll_b, hks: coll_s}

    # ── crown replay credits the better-rate holder A, not B, and NOT the
    #    sentinel S — its rate is unexecutable against the on-chain swap bounds. ──
    crown = replay_crown_time_window(
        store=store,
        event_index=index,
        from_chain='btc',
        to_chain='sol',
        window_start=now - 600,
        window_end=now + 60,
        rewardable_hotkeys={hka, hkb, hks},
        min_swap_lamports=MIN_SWAP_RAO,
        max_swap_lamports=MAX_SWAP_RAO,
    )
    assert set(crown) == {hka}
    assert crown[hka] > 0

    # ── eligibility is read off the on-chain MinerState counters ──
    metagraph = _padded_metagraph([hka, hkb, hks])
    eligibility = build_eligibility(admin, metagraph)
    # No completed swaps on localnet yet → 0 successes → all ineligible.
    assert all(eligibility.get(hk) is False for hk in (hka, hkb, hks))

    v = _validator_ns(admin, store, index, metagraph, last_scored_time=now - 600)

    # Round 1 — real eligibility: every miner ineligible → full pool recycles.
    rewards, _ = calculate_miner_rewards(v, now + 60)
    assert rewards[0] == 0.0 and rewards[1] == 0.0 and rewards[2] == 0.0  # A, B, S
    assert rewards[RECYCLE_UID] == pytest.approx(1.0, abs=1e-5)

    # Round 2 — patch all eligible: the btc→sol pool is credited to crown holder A.
    # B (lower rate) and S (higher rate but unexecutable) both earn nothing — the
    # sentinel earning 0 proves the bounds reached the Config AND the gate fires.
    with patch('allways.validator.scoring.build_eligibility', return_value={hka: True, hkb: True, hks: True}):
        rewards2, _ = calculate_miner_rewards(v, now + 60)
    assert rewards2[0] == pytest.approx(POOL_BTC_SOL, abs=1e-5)
    assert rewards2[1] == 0.0
    assert rewards2[2] == 0.0
    store.close()


def _padded_metagraph(hotkeys):
    keys = list(hotkeys)
    while len(keys) <= RECYCLE_UID:
        keys.append(_hotkey().ss58_address)
    return SimpleNamespace(n=SimpleNamespace(item=lambda: len(keys)), hotkeys=keys)


def _validator_ns(admin, store, index, metagraph, last_scored_time):
    return SimpleNamespace(
        metagraph=metagraph,
        state_store=store,
        event_index=index,
        solana_client=admin,
        solana_config_cache=SolanaConfigCache(admin),
        database_storage=SimpleNamespace(is_enabled=lambda: False),
        last_scored_time=last_scored_time,
        block=0,
    )
