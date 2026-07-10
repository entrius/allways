"""Integration: verify a REAL SOL transfer on a local solana-test-validator (B7).

Spins up a throwaway validator, funds a sender, sends lamports via SolanaProvider.send_amount (a
SystemProgram transfer), then proves the provider reads it back: amount match, sender attribution,
and the on-chain blockTime (the replay-freshness floor). No swap-manager program needed — the SOL
swap leg is a peer-to-peer transfer, verified like a BTC deposit.

Gated behind @pytest.mark.integration. Run with:
    uv run pytest tests/test_solana_provider_integration.py -m integration -s
Tear down: pkill -9 -f solana-test-validator; ensure port 8899 is free.
"""

import subprocess
import time

import pytest
from solders.keypair import Keypair

from allways.chain_providers.solana import SolanaProvider
from allways.solana.rpc import SolanaRpc

pytestmark = pytest.mark.integration

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
def validator(tmp_path_factory):
    work = tmp_path_factory.mktemp('sol_provider')
    proc = subprocess.Popen(
        ['solana-test-validator', '--reset', '--quiet', '--ledger', str(work / 'ledger'), '--rpc-port', '8899'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_health(SolanaRpc(RPC))
        yield work
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _fund(pubkey: str, sol: int = 5):
    subprocess.run(['solana', 'airdrop', str(sol), pubkey, '--url', RPC], check=True, capture_output=True, timeout=60)


def test_send_then_verify_real_transfer(validator):
    sender = Keypair()
    recipient = Keypair()
    _fund(str(sender.pubkey()))

    sender_provider = SolanaProvider(solana_rpc_url=RPC, solana_keypair=sender)
    reader = SolanaProvider(solana_rpc_url=RPC)  # read-only, no keypair

    # Connection check (read-only path).
    reader.check_connection(require_send=False)

    amount = 1_500_000  # lamports
    out = sender_provider.send_amount(str(recipient.pubkey()), amount)
    assert out is not None, 'send_amount failed'
    sig, slot = out
    assert sig and slot >= 0

    # Verify the leg by signature, like the swap loop does. No confirmation requirement —
    # the tx is at least 'confirmed' (getTransaction commitment) but may be < 32 slots old.
    info = reader.fetch_matching_tx(sig, str(recipient.pubkey()), amount)
    assert info is not None, 'transfer not found / did not match'
    assert info.amount >= amount
    assert info.recipient == str(recipient.pubkey())
    assert info.sender == str(sender.pubkey())
    assert info.block_number == slot

    # The replay-freshness floor: a real on-chain blockTime in unix seconds (B2).
    assert info.block_time is not None
    assert abs(info.block_time - int(time.time())) < 600

    # Underpayment is rejected against the same tx.
    assert reader.fetch_matching_tx(sig, str(recipient.pubkey()), amount + 1) is None

    # Balance read reflects the credit.
    assert reader.get_balance(str(recipient.pubkey())) >= amount

    # A nonexistent signature returns None, not an error.
    fake_sig = str(Keypair().sign_message(b'x'))
    assert reader.fetch_matching_tx(fake_sig, str(recipient.pubkey()), amount) is None
