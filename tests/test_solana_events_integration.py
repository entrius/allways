"""B3.1 integration: emit real program events on localnet and assert the ingest decodes them.

Drives set_quote (QuoteSet), post_collateral (CollateralPosted), and vote_activate→quorum (MinerActivated)
against the deployed program, then runs SolanaEventIngest.poll and checks the decoded records carry the
right fields + slot/block_time. Proves the discriminators + borsh layouts match the on-chain emission.

Gated behind @pytest.mark.integration. Run with:
    uv run pytest tests/test_solana_events_integration.py -m integration -s
"""

import json
import subprocess
import time
from pathlib import Path

import pytest
from solders.keypair import Keypair

from allways.solana.client import AllwaysSolanaClient
from allways.solana.events import SolanaEventIngest
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


def _airdrop(pubkey, sol):
    subprocess.run(['solana', 'airdrop', str(sol), str(pubkey), '--url', RPC], check=True,
                   capture_output=True, timeout=60)


@pytest.fixture(scope='module')
def env(tmp_path_factory):
    work = tmp_path_factory.mktemp('solana_b31')
    payer = Keypair()
    (work / 'payer.json').write_text(json.dumps(list(bytes(payer))))
    proc = subprocess.Popen(
        ['solana-test-validator', '--reset', '--quiet', '--ledger', str(work / 'ledger'), '--rpc-port', '8899'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        _wait_health(SolanaRpc(RPC))
        _airdrop(payer.pubkey(), 100)
        subprocess.run(
            ['solana', 'program', 'deploy', str(SO), '--program-id', str(PROG_KP),
             '--keypair', str(work / 'payer.json'), '--url', RPC],
            check=True, capture_output=True, timeout=120,
        )
        yield {'payer': payer}
    finally:
        proc.terminate()
        proc.wait()


def test_ingest_decodes_emitted_events(env):
    admin = AllwaysSolanaClient(RPC, keypair=env['payer'])
    admin.initialize(
        min_collateral=1_000_000, max_collateral=0, fulfillment_timeout_secs=12_600,
        consensus_threshold_percent=66, min_swap_amount=0, max_swap_amount=0, reservation_ttl_secs=1_800,
    )
    validators = []
    for _ in range(3):
        kp = Keypair()
        _airdrop(kp.pubkey(), 10)
        admin.add_validator(kp.pubkey(), 1)
        validators.append(AllwaysSolanaClient(RPC, keypair=kp))

    miner = Keypair()
    _airdrop(miner.pubkey(), 10)
    mclient = AllwaysSolanaClient(RPC, keypair=miner)
    mclient.post_collateral(5_000_000)                                  # → CollateralPosted
    mclient.set_quote('btc', 'tao', 'minerBTC', 'minerTAO', 345 * 10**18, 1_000)  # → QuoteSet

    validators[0].vote_activate(miner.pubkey())
    validators[1].vote_activate(miner.pubkey())                          # quorum → MinerActivated

    records, cursor = SolanaEventIngest(admin).poll(until_sig=None)
    by_name = {}
    for r in records:
        by_name.setdefault(r.name, []).append(r)

    assert 'CollateralPosted' in by_name
    assert by_name['CollateralPosted'][0].fields.miner == miner.pubkey()
    assert by_name['CollateralPosted'][0].fields.total == 5_000_000

    assert 'QuoteSet' in by_name
    q = by_name['QuoteSet'][0].fields
    assert q.miner == miner.pubkey() and q.from_chain == 'btc' and q.to_chain == 'tao' and q.rate == 345 * 10**18

    assert 'MinerActivated' in by_name
    assert by_name['MinerActivated'][0].fields.miner == miner.pubkey()

    # records carry ordering context + cursor advanced
    assert all(r.slot is not None for r in records)
    assert cursor is not None
