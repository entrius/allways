"""B0 integration: round-trip the Solana client against a fresh local solana-test-validator.

Proves the whole foundation against REAL on-chain bytes: deploy → write (initialize/post_collateral/
set_quote/bind_hotkey) → read back + assert (covers the Config/MinerState/MinerQuote/Binding/HotkeyBinding
layouts, PDA derivation, tx build/sign/send, the new keypair, discovery, and the event-log skeleton).

Gated behind @pytest.mark.integration (like the Rust #[ignore] tests). Run with:
    uv run pytest tests/test_solana_client_integration.py -m integration -s
"""

import json
import os
import subprocess
import time
from pathlib import Path

import pytest
from solders.keypair import Keypair

from allways.solana.client import AllwaysSolanaClient
from allways.solana.layouts import CONFIG_VERSION
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


@pytest.fixture(scope='module')
def client(tmp_path_factory):
    # External mode: validator already up + program deployed, payer keypair at SOLANA_KEYPAIR_PATH.
    if os.environ.get('SOLANA_B0_EXTERNAL') == '1':
        kp = Keypair.from_bytes(bytes(json.loads(Path(os.environ['SOLANA_KEYPAIR_PATH']).read_text())))
        yield AllwaysSolanaClient(os.environ.get('SOLANA_RPC_URL', RPC), keypair=kp)
        return
    work = tmp_path_factory.mktemp('solana_b0')
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
        subprocess.run(
            ['solana', 'airdrop', '100', str(payer.pubkey()), '--url', RPC], check=True, capture_output=True, timeout=60
        )
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
        yield AllwaysSolanaClient(RPC, keypair=payer)
    finally:
        proc.terminate()
        proc.wait()


def test_b0_round_trip(client):
    me = client.keypair.pubkey()

    # initialize -> Config (+ Treasury)
    client.initialize(
        min_collateral=1_000_000,
        max_collateral=0,
        fulfillment_timeout_secs=12_600,
        consensus_threshold_percent=66,
        min_swap_amount=1000,  # hard dust floor — 0-unbounded was removed (L4a)
        max_swap_amount=0,
        reservation_ttl_secs=1_800,
    )
    cfg = client.get_config()
    assert cfg.version == CONFIG_VERSION
    assert cfg.consensus_threshold_percent == 66
    assert cfg.admin == me
    assert cfg.halted is False
    assert list(cfg.validators) == []

    # post_collateral -> MinerState.collateral (the tracked, slashable, gating field)
    client.post_collateral(5_000_000)
    ms = client.get_miner_state(me)
    assert ms.miner == me
    assert ms.collateral == 5_000_000
    assert ms.active is False and ms.successful_swaps == 0 and ms.failed_swaps == 0
    # get_collateral_lamports returns exactly the tracked field — not vault lamports (rent-inclusive).
    assert client.get_collateral_lamports(me) == 5_000_000

    # set_quote -> MinerQuote (u128 fixed-point rate). A quote names the purse that answers for it, so
    # the SOL purse has to be lit first: this keypair is its own (sole) validator, and at a 66%
    # threshold one vote is quorum.
    client.add_validator(me, 1)
    client.vote_activate(me, backing='sol')
    assert client.get_miner_state(me).active is True

    rate = 15 * 10**17  # 1.5 * RATE_PRECISION
    client.set_quote('btc', 'sol', 'minerBTCaddr', 'minerSOLaddr', rate, 1_000, backing='sol')
    q = client.get_quote(me, 'btc', 'sol', 'sol')
    assert q.rate == rate
    assert q.from_chain == 'btc' and q.to_chain == 'sol'
    assert q.collateral_chain == 'sol'
    assert q.miner_from_addr == 'minerBTCaddr' and q.liquidity == 1_000

    # bind_hotkey -> Binding + set-once HotkeyBinding
    hotkey = bytes(range(32))
    sig = bytes(range(64))
    client.bind_hotkey(hotkey, sig)
    b = client.get_binding(me)
    assert b.miner == me and bytes(b.hotkey) == hotkey and bytes(b.hotkey_sig) == sig
    hb = client.get_hotkey_binding(hotkey)
    assert hb.miner == me

    # discovery: no swaps exist yet
    assert client.get_swaps() == []

    # missing account reads return None
    assert client.get_swap(bytes(32)) is None

    # event-log skeleton: the set_quote tx emitted a QuoteSet event (Program data log present)
    sigs = client.get_program_signatures(limit=20)
    assert sigs, 'expected program signatures'
    saw_event = any(client.get_event_logs(s['signature']) for s in sigs)
    assert saw_event, "expected at least one Anchor 'Program data:' event log"
