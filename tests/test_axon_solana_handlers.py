"""B2.4 — unit tests for the Solana-repointed axon handlers (claim relay + vote_activate).

Mocks the validator's solana_client + axon_subtensor + chain providers; no chain. Asserts the handler
resolves the miner pubkey via the HotkeyBinding, gates on on-chain state, and submits the right Solana
instruction (vote_activate / submit_swap_claim) — or rejects with a clear reason.
"""

import asyncio
import threading
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.chain_providers.base import TransactionInfo
from allways.solana.client import swap_key_from_tx_hash
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse
from allways.validator import axon_handlers

HK = bt.Keypair.create_from_seed('0x' + '11' * 32)
HOTKEY = HK.ss58_address
MINER_PK = SolKeypair().pubkey()
HOTKEY_BYTES = bytes.fromhex(HK.public_key.hex())
BINDING_SIG = HK.sign(bytes(MINER_PK))  # valid sr25519 binding: hotkey signs the miner pubkey bytes
NOW = 2_000_000_000
CREATED_AT = NOW - 600  # reservation created 10 min ago


class FakeSolanaClient:
    def __init__(self, *, binding=True, miner_state=None, reservation=None, min_collateral=1_000_000):
        self.binding = SimpleNamespace(miner=MINER_PK) if binding else None
        self.full_binding = (
            SimpleNamespace(miner=MINER_PK, hotkey=HOTKEY_BYTES, hotkey_sig=BINDING_SIG) if binding else None
        )
        self.miner_state = miner_state
        self.reservation = reservation
        self._min_collateral = min_collateral
        self.calls = []

    def get_hotkey_binding(self, hotkey_bytes):
        return self.binding

    def get_binding(self, miner):
        return self.full_binding

    def get_miner_state(self, miner):
        return self.miner_state

    def get_config(self):
        return SimpleNamespace(min_collateral=self._min_collateral)

    def get_reservation(self, miner):
        return self.reservation

    def vote_activate(self, miner):
        self.calls.append(('vote_activate', miner))

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block):
        self.calls.append(('submit_swap_claim', miner, swap_key, from_tx_hash, from_tx_block))


class FakeProvider:
    def __init__(self, tx_info, grace=0):
        self.tx_info = tx_info
        self.grace = grace

    def get_chain(self):
        return SimpleNamespace(replay_grace_secs=self.grace)

    def verify_transaction(self, **kw):
        return self.tx_info


def make_validator(solana_client, provider=None):
    return SimpleNamespace(
        solana_client=solana_client,
        axon_subtensor=SimpleNamespace(is_hotkey_registered=lambda netuid, hotkey_ss58: True),
        axon_lock=threading.RLock(),
        config=SimpleNamespace(netuid=1),
        metagraph=SimpleNamespace(hotkeys=[HOTKEY]),
        axon_chain_providers={'btc': provider} if provider else {},
    )


def activate_synapse():
    s = MinerActivateSynapse(hotkey=HOTKEY, signature='', message='')
    s.dendrite = bt.TerminalInfo(hotkey=HOTKEY)
    return s


def confirm_synapse(from_tx_hash='srctx', from_tx_block=800_000):
    # handle_swap_confirm keys off reservation_id (the miner hotkey), not dendrite.
    return SwapConfirmSynapse(
        reservation_id=HOTKEY,
        from_tx_hash=from_tx_hash,
        from_tx_proof='',
        from_address='userBTC',
        from_tx_block=from_tx_block,
    )


def run(coro):
    return asyncio.run(coro)


# ---- handle_miner_activate ----

def test_activate_unbound_hotkey_rejects():
    client = FakeSolanaClient(binding=False)
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is False and 'not bound' in s.rejection_reason
    assert client.calls == []


def test_activate_already_active_rejects():
    client = FakeSolanaClient(miner_state=SimpleNamespace(active=True, collateral=9_000_000))
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is False and 'already active' in s.rejection_reason
    assert client.calls == []


def test_activate_low_collateral_rejects():
    client = FakeSolanaClient(miner_state=SimpleNamespace(active=False, collateral=10), min_collateral=1_000_000)
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is False and 'Insufficient collateral' in s.rejection_reason
    assert client.calls == []


def test_activate_success_votes():
    client = FakeSolanaClient(miner_state=SimpleNamespace(active=False, collateral=5_000_000))
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is True
    assert client.calls == [('vote_activate', MINER_PK)]


# ---- handle_swap_confirm (claim relay) ----

def _reservation(claimed=b'\x00' * 32, reserved_until=NOW + 600):
    return SimpleNamespace(
        reserved_until=reserved_until,
        claimed_swap_key=claimed,
        from_chain='btc',
        miner_from_addr='minerBTC',
        from_amount=500,
        from_addr='userBTC',
        created_at=CREATED_AT,
    )


def _fresh_tx():
    return TransactionInfo(
        tx_hash='srctx', confirmed=True, sender='userBTC', recipient='minerBTC',
        amount=500, block_number=800_000, block_time=NOW - 60,  # mined after created_at
    )


def test_confirm_no_reservation_rejects():
    client = FakeSolanaClient(reservation=None)
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(_fresh_tx())), confirm_synapse()))
    assert s.accepted is False and 'No reservation' in s.rejection_reason
    assert client.calls == []


def test_confirm_already_claimed_rejects():
    client = FakeSolanaClient(reservation=_reservation(claimed=b'\x07' * 32))
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(_fresh_tx())), confirm_synapse()))
    assert s.accepted is False and 'already has a claimed swap' in s.rejection_reason
    assert client.calls == []


def test_confirm_tx_not_visible_rejects():
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(None)), confirm_synapse()))
    assert s.accepted is False and 'not yet visible' in s.rejection_reason
    assert client.calls == []


def test_confirm_stale_deposit_rejects():
    stale = TransactionInfo(
        tx_hash='srctx', confirmed=True, sender='userBTC', recipient='minerBTC',
        amount=500, block_number=700_000, block_time=CREATED_AT - 1,  # predates the reservation
    )
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(stale)), confirm_synapse()))
    assert s.accepted is False and 'freshness' in s.rejection_reason
    assert client.calls == []


def test_confirm_success_relays_claim():
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(_fresh_tx())), confirm_synapse()))
    assert s.accepted is True
    assert client.calls == [
        ('submit_swap_claim', MINER_PK, swap_key_from_tx_hash('srctx'), 'srctx', 800_000),
    ]
