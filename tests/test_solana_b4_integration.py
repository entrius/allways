"""B4 integration: drive the miner/admin Solana builders against the deployed program on localnet.

Deploys the program, activates a real bound miner (post_collateral + set_quote + bind_hotkey +
vote_activate — the B4 client methods the miner neuron/CLI use), then exercises everything B4 added that
is reachable without the Phase-9 reservation flow: the SwapPoller read path against live
getProgramAccounts, miner self-deactivate, quote retract, the mark_fulfilled rejection path, the admin
runtime setters + halt toggle, and withdraw_treasury's over-balance guard.

The mark_fulfilled HAPPY path (an Active miner-owned swap → Fulfilled) needs a live Reservation, which
only the Phase-9 pool flow (open_or_request → resolve_pool) creates — same gate B2.5/B3.6 documented. The
Rust e2e.sh onchain_* tests cover that contract-side lifecycle.

Gated behind @pytest.mark.integration. Run with:
    uv run pytest tests/test_solana_b4_integration.py -m integration -s
"""

import json
import subprocess
import time
from pathlib import Path

import bittensor as bt
import pytest
from solders.keypair import Keypair

from allways.constants import RATE_PRECISION
from allways.miner.swap_poller import SwapPoller
from allways.solana.client import AllwaysSolanaClient, SolanaClientError
from allways.solana.rpc import SolanaRpc

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[1]
SO = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager.so'
PROG_KP = REPO / 'smart-contracts/solana/target/deploy/allways_swap_manager-keypair.json'
RPC = 'http://127.0.0.1:8899'


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
    work = tmp_path_factory.mktemp('solana_b4')
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


def test_b4_miner_and_admin_builders_on_localnet(env):
    admin = AllwaysSolanaClient(RPC, keypair=env['payer'])
    admin.initialize(
        min_collateral=1_000_000,
        max_collateral=0,
        fulfillment_timeout_secs=12_600,
        consensus_threshold_percent=66,
        min_swap_amount=0,
        max_swap_amount=0,
        reservation_ttl_secs=1_800,
    )
    validators = []
    for _ in range(3):
        kp = Keypair()
        _airdrop(kp.pubkey(), 10)
        admin.add_validator(kp.pubkey(), 1)
        validators.append(AllwaysSolanaClient(RPC, keypair=kp))

    # ── activate a real bound miner via the B4 client methods ──
    miner = Keypair()
    _airdrop(miner.pubkey(), 10)
    mclient = AllwaysSolanaClient(RPC, keypair=miner)
    mclient.post_collateral(5_000_000)
    mclient.set_quote('btc', 'tao', 'minerBTC', 'minerTAO', 400 * RATE_PRECISION, 1_000)
    hk = _hotkey()
    mclient.bind_hotkey(bytes.fromhex(hk.public_key.hex()), hk.sign(bytes(miner.pubkey())))
    validators[0].vote_activate(miner.pubkey())
    validators[1].vote_activate(miner.pubkey())  # quorum → active

    ms = mclient.get_miner_state(miner.pubkey())
    assert ms is not None and ms.active is True and ms.collateral == 5_000_000
    assert mclient.get_binding(miner.pubkey()) is not None
    assert mclient.get_quote(miner.pubkey(), 'btc', 'tao') is not None

    # ── SwapPoller against live getProgramAccounts: no Active swaps without Phase-9 → ([], []) ──
    poller = SwapPoller(mclient, miner.pubkey())
    active, fulfilled = poller.poll()
    assert poller.last_poll_ok is True
    assert active == [] and fulfilled == []

    # ── mark_fulfilled rejection: no such swap → the program rejects the tx ──
    with pytest.raises(SolanaClientError):
        mclient.mark_fulfilled(swap_key=bytes([7] * 32), to_tx_hash='deadbeef', to_tx_block=1)

    # ── quote retract closes the MinerQuote PDA ──
    mclient.remove_quote('btc', 'tao')
    assert mclient.get_quote(miner.pubkey(), 'btc', 'tao') is None

    # ── miner self-deactivate flips MinerState.active ──
    mclient.deactivate()
    assert mclient.get_miner_state(miner.pubkey()).active is False

    # ── admin runtime setter + halt toggle land on Config ──
    admin.set_min_collateral(2_000_000)
    assert admin.get_config().min_collateral == 2_000_000
    admin.set_halted(True)
    assert admin.get_config().halted is True
    admin.set_halted(False)
    assert admin.get_config().halted is False

    # ── withdraw_treasury: over-balance is rejected (builder reaches the program) ──
    treasury = admin.get_treasury()
    total = treasury.total if treasury is not None else 0
    with pytest.raises(SolanaClientError):
        admin.withdraw_treasury(env['payer'].pubkey(), total + 1_000_000_000)
