"""B2.0 integration: prove the consensus-vote encoding is runtime-correct against the deployed program.

Drives `vote_activate` to quorum from three distinct validator keypairs (each signing its own vote) and
asserts the miner's MinerState flips active=true — exercising the validator/config/miner_state/vote_round
account pattern that vote_initiate/confirm_swap/timeout_swap all share, plus the on-chain record_vote /
threshold path and the new has_voted reader.

The full swap lifecycle (submit_swap_claim → vote_initiate → mark_fulfilled → confirm_swap) needs a live
Reservation, which only the Phase-9 pool flow (resolve_pool) creates; that end-to-end check lands in B2.5
once reservation seeding exists. The Rust `e2e.sh` onchain_* tests already cover the contract side.

Gated behind @pytest.mark.integration. Run with:
    uv run pytest tests/test_solana_vote_integration.py -m integration -s
"""

import json
import subprocess
import time
from pathlib import Path

import pytest
from solders.keypair import Keypair

from allways.solana import pdas
from allways.solana.client import AllwaysSolanaClient
from allways.solana.rpc import SolanaRpc

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[1]
SO = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager.so'
PROG_KP = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager-keypair.json'
RPC = 'http://127.0.0.1:8899'
MIN_COLLATERAL = 1_000_000


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


def _airdrop(pubkey, sol: int):
    subprocess.run(
        ['solana', 'airdrop', str(sol), str(pubkey), '--url', RPC], check=True, capture_output=True, timeout=60
    )


@pytest.fixture(scope='module')
def env(tmp_path_factory):
    work = tmp_path_factory.mktemp('solana_b2')
    payer = Keypair()
    payer_path = work / 'payer.json'
    payer_path.write_text(json.dumps(list(bytes(payer))))
    proc = subprocess.Popen(
        ['solana-test-validator', '--reset', '--quiet', '--ledger', str(work / 'ledger'), '--rpc-port', '8899'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        rpc = SolanaRpc(RPC)
        _wait_health(rpc)
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
                str(payer_path),
                '--url',
                RPC,
            ],
            check=True,
            capture_output=True,
            timeout=120,
        )
        yield {'payer': payer}
    finally:
        proc.terminate()
        proc.wait()


def test_vote_activate_quorum(env):
    admin = AllwaysSolanaClient(RPC, keypair=env['payer'])
    admin.initialize(
        min_collateral=MIN_COLLATERAL,
        max_collateral=0,
        fulfillment_timeout_secs=12_600,
        consensus_threshold_percent=66,
        min_swap_amount=0,
        max_swap_amount=0,
        reservation_ttl_secs=1_800,
    )

    # Three whitelisted validators, each with its own funded client (signs its own vote).
    validators = []
    for _ in range(3):
        kp = Keypair()
        _airdrop(kp.pubkey(), 10)
        admin.add_validator(kp.pubkey(), 1)
        validators.append(AllwaysSolanaClient(RPC, keypair=kp))
    cfg = admin.get_config()
    assert len(cfg.validators) == 3

    # A miner with enough collateral to clear the vote_activate entry guard.
    miner = Keypair()
    _airdrop(miner.pubkey(), 10)
    AllwaysSolanaClient(RPC, keypair=miner).post_collateral(5_000_000)
    mpk = miner.pubkey()
    assert admin.get_miner_state(mpk).active is False

    # First vote: recorded, but quorum (66% of 3 → 2 votes) not yet reached.
    validators[0].vote_activate(mpk)
    assert admin.has_voted(pdas.REQ_ACTIVATE, mpk, validators[0].keypair.pubkey()) is True
    assert admin.get_miner_state(mpk).active is False

    # Second vote reaches quorum → miner activated; the round is reset on activation.
    validators[1].vote_activate(mpk)
    ms = admin.get_miner_state(mpk)
    assert ms.active is True
    assert ms.deactivation_at == 0
