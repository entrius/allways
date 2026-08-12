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

from allways.assets.asset import TransactionInfo
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse
from allways.validator import activation, axon_handlers

HK = bt.Keypair.create_from_seed('0x' + '11' * 32)
HOTKEY = HK.ss58_address
MINER_PK = SolKeypair().pubkey()
HOTKEY_BYTES = bytes.fromhex(HK.public_key.hex())
BINDING_SIG = HK.sign(bytes(MINER_PK))  # valid sr25519 binding: hotkey signs the miner pubkey bytes
NOW = 2_000_000_000
CREATED_AT = NOW - 600  # reservation created 10 min ago
TAO_FLOOR = 250_000_000  # 0.25 TAO in rao — the deployed tao_min_collateral
BOND = 50 * 1_000_000_000  # a healthy bond, well clear of the floor
EPOCH = 3  # the vault's lock epoch; the attestation must name the same one


class FakeSolanaClient:
    def __init__(
        self,
        *,
        binding=True,
        miner_state=None,
        reservation=None,
        min_collateral=1_000_000,
        attestation=None,
        tao_min_collateral=TAO_FLOOR,
        heartbeat=NOW,
        vault_generation=0,
    ):
        self.binding = SimpleNamespace(miner=MINER_PK) if binding else None
        self.full_binding = (
            SimpleNamespace(miner=MINER_PK, hotkey=HOTKEY_BYTES, hotkey_sig=BINDING_SIG) if binding else None
        )
        self.miner_state = miner_state
        self.reservation = reservation
        self._min_collateral = min_collateral
        self.attestation = attestation
        self._tao_min_collateral = tao_min_collateral
        self._heartbeat = heartbeat
        self._vault_generation = vault_generation
        self.calls = []

    def get_hotkey_binding(self, hotkey_bytes):
        return self.binding

    def get_binding(self, miner):
        return self.full_binding

    def get_miner_state(self, miner):
        return self.miner_state

    def get_config(self):
        return SimpleNamespace(
            min_collateral=self._min_collateral,
            tao_min_collateral=self._tao_min_collateral,
            last_attest_heartbeat=self._heartbeat,
            attest_max_age_secs=86_400,
            vault_generation=self._vault_generation,
        )

    def get_bond_attestation(self, miner, chain='tao'):
        return self.attestation

    def get_reservation(self, miner, backing='sol'):
        return self.reservation

    def vote_activate(self, miner, backing='sol'):
        self.calls.append(('vote_activate', miner, backing))

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block, backing='sol'):
        self.calls.append(('submit_swap_claim', miner, swap_key, from_tx_hash, from_tx_block, backing))


class FakeProvider:
    def __init__(self, tx_info, grace=0):
        self.tx_info = tx_info
        self.grace = grace

    @property
    def chain_def(self):
        return SimpleNamespace(replay_grace_secs=self.grace)

    def verify_transaction(self, **kw):
        return self.tx_info


class FakeVault:
    """The bond as the vault itself reports it. None from either read is "unknown", never zero."""

    def __init__(self, locked=True, epoch=EPOCH, gross=BOND):
        self.lock = None if locked is None else (locked, epoch)
        self.gross = gross

    def get_lock_state(self, hotkey):
        return self.lock

    def get_collateral(self, hotkey):
        return self.gross


def make_validator(solana_client, provider=None, vault=None):
    return SimpleNamespace(
        solana_client=solana_client,
        axon_subtensor=SimpleNamespace(is_hotkey_registered=lambda netuid, hotkey_ss58: True),
        axon_lock=threading.RLock(),
        config=SimpleNamespace(netuid=1),
        metagraph=SimpleNamespace(hotkeys=[HOTKEY]),
        axon_assets={'btc': provider} if provider else {},
        bond_relay=SimpleNamespace() if vault is not None else None,
        _vault=vault,
    )


def with_vault(monkeypatch, validator):
    """Serve the handler's vault reads off the validator's fake instead of a real subtensor."""
    monkeypatch.setattr(activation, 'axon_vault_client', lambda v: getattr(v, '_vault', None))
    return validator


def miner_state(active=False, collateral=5_000_000, mask=0):
    # `active` is the OR view of `active_backings`, exactly as the program writes it.
    return SimpleNamespace(active=active or bool(mask), collateral=collateral, active_backings=mask)


def attested(balance=BOND, locked=True, epoch=EPOCH):
    return SimpleNamespace(effective_balance=balance, locked=locked, epoch=epoch)


def activate_synapse(backing=None):
    s = MinerActivateSynapse(hotkey=HOTKEY, signature='', message='', backing=backing)
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
    client = FakeSolanaClient(miner_state=miner_state(collateral=9_000_000, mask=pdas.BACKING_BIT_SOL))
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is False and 'already active' in s.rejection_reason
    assert client.calls == []


def test_activate_low_collateral_rejects():
    client = FakeSolanaClient(miner_state=miner_state(collateral=10), min_collateral=1_000_000)
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is False and 'Insufficient collateral' in s.rejection_reason
    assert client.calls == []


def test_activate_success_votes():
    client = FakeSolanaClient(miner_state=miner_state(collateral=5_000_000))
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse()))
    assert s.accepted is True
    # No backing on the wire is "sol" — a pre-W2 miner's request still means what it always meant.
    assert client.calls == [('vote_activate', MINER_PK, 'sol')]


# ---- handle_miner_activate: the TAO purse (the production trigger for split collateral) ----


def test_activate_tao_votes_when_the_bond_checks_out(monkeypatch):
    client = FakeSolanaClient(miner_state=miner_state(collateral=10), attestation=attested())
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    # The lamport purse is far below its own floor and irrelevant: floors are per backing.
    assert s.accepted is True
    assert client.calls == [('vote_activate', MINER_PK, 'tao')]


def test_activate_tao_is_open_to_a_miner_already_serving_sol(monkeypatch):
    # The OR view says "active"; the TAO bit is what this request is about. D2's whole point.
    client = FakeSolanaClient(
        miner_state=miner_state(collateral=5_000_000, mask=pdas.BACKING_BIT_SOL), attestation=attested()
    )
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is True
    assert client.calls == [('vote_activate', MINER_PK, 'tao')]


def test_activate_tao_refuses_a_stale_attestation(monkeypatch):
    # The dangerous direction: a bond unlocked on the vault still reads LOCKED on Solana for a
    # cadence, and activating in that window puts swaps behind a bond its owner can withdraw.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(epoch=EPOCH))
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault(locked=True, epoch=EPOCH + 1)))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'stale' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_a_bond_unlocked_on_the_vault(monkeypatch):
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault(locked=False)))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'not locked on the TAO vault' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_an_unreadable_vault(monkeypatch):
    # None is "unknown", not "zero" — an undecodable dry-run must never be voted on either way.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault(locked=None)))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'could not be read off the TAO vault' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_when_no_attestation_exists_yet(monkeypatch):
    # Attestation-before-activation: the miner retries, the validator never hand-sequences the write.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=None)
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'attestation for your TAO purse is missing' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_an_unlocked_attestation(monkeypatch):
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(locked=False))
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'UNLOCKED' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_a_bond_under_its_own_floor(monkeypatch):
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(balance=TAO_FLOOR - 1))
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'Bond below the TAO floor' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_while_the_fuse_is_blown(monkeypatch):
    # heartbeat 0 = a fleet that has never proven itself live. Same gate the contract applies.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(), heartbeat=0)
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault()))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'fused off' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_on_a_validator_with_no_relay(monkeypatch):
    # A SOL-only deployment cannot verify a bond, so it declines instead of voting blind.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = with_vault(monkeypatch, make_validator(client, vault=None))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'Bond relay not configured' in s.rejection_reason
    assert client.calls == []


def test_activate_refuses_an_unknown_backing():
    client = FakeSolanaClient(miner_state=miner_state())
    s = run(axon_handlers.handle_miner_activate(make_validator(client), activate_synapse('btc')))
    assert s.accepted is False and 'Unknown backing' in s.rejection_reason
    assert client.calls == []


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
        tx_hash='srctx',
        confirmed=True,
        sender='userBTC',
        recipient='minerBTC',
        amount=500,
        block_number=800_000,
        block_time=NOW - 60,  # mined after created_at
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
    # Absent tx (verify → None): fast-fail with no claim, so the short TTL frees the miner.
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(None)), confirm_synapse()))
    assert s.accepted is False and 'not visible' in s.rejection_reason
    assert client.calls == []


def test_confirm_unconfirmed_mempool_deposit_relays_claim():
    # Deferred intake: a content-valid but unconfirmed (0-conf mempool) deposit still creates the claim;
    # the crank defers voting until confirmations accrue.
    mempool = TransactionInfo(
        tx_hash='srctx',
        confirmed=False,
        sender='userBTC',
        recipient='minerBTC',
        amount=500,
        block_number=None,
        block_time=None,  # unmined → no block_time; freshness deferred to the crank's 'ok' gate
    )
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_validator(client, FakeProvider(mempool)), confirm_synapse()))
    assert s.accepted is True
    assert client.calls == [('submit_swap_claim', MINER_PK, swap_key_from_tx_hash('srctx'), 'srctx', 0, 'sol')]


def test_confirm_stale_deposit_rejects():
    stale = TransactionInfo(
        tx_hash='srctx',
        confirmed=True,
        sender='userBTC',
        recipient='minerBTC',
        amount=500,
        block_number=700_000,
        block_time=CREATED_AT - 1,  # predates the reservation
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
        ('submit_swap_claim', MINER_PK, swap_key_from_tx_hash('srctx'), 'srctx', 800_000, 'sol'),
    ]


# ---- handle_swap_reserve: malformed caller input ----
def test_reserve_bad_hotkey_rejects_at_info_not_error(monkeypatch):
    from allways.synapses import SwapReserveSynapse

    def _raise(*_a, **_k):
        raise ValueError('Invalid SS58 address: Base 58 requirement is violated')

    monkeypatch.setattr(axon_handlers, 'reserve_on_behalf', _raise)
    errors = []
    monkeypatch.setattr(axon_handlers.bt.logging, 'error', lambda msg, *a, **k: errors.append(msg))

    syn = SwapReserveSynapse(
        miner_hotkey='not-an-ss58',
        from_chain='tao',
        to_chain='sol',
        user_pubkey='u',
        user_from_addr='x',
        user_to_addr='y',
        from_amount=1,
    )
    out = run(axon_handlers.handle_swap_reserve(make_validator(FakeSolanaClient()), syn))

    assert out.accepted is False
    assert 'Invalid SS58' in out.rejection_reason
    assert errors == []  # a garbage caller must not land on the validator's ERROR log


def test_activate_tao_accepts_an_attestation_from_the_current_vault_generation(monkeypatch):
    """After a vault swap the attested epoch carries the generation in its high half; the vault
    still reports the bare lock epoch, so the check must compare like for like."""
    from allways.solana.layouts import compose_attestation_epoch

    client = FakeSolanaClient(
        miner_state=miner_state(),
        attestation=attested(epoch=compose_attestation_epoch(1, EPOCH)),
        vault_generation=1,
    )
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault(locked=True, epoch=EPOCH)))
    assert activation.check(v, HOTKEY, MINER_PK, miner_state(), 'tao', NOW).ok


def test_activate_tao_refuses_an_attestation_from_a_retired_vault(monkeypatch):
    """A retired vault's mirror must not authorise activation just because its lock epoch happens
    to collide with the live vault's — the generation is what tells them apart."""
    client = FakeSolanaClient(
        miner_state=miner_state(),
        attestation=attested(epoch=EPOCH),  # generation 0, i.e. the retired vault
        vault_generation=1,
    )
    v = with_vault(monkeypatch, make_validator(client, vault=FakeVault(locked=True, epoch=EPOCH)))
    result = activation.check(v, HOTKEY, MINER_PK, miner_state(), 'tao', NOW)
    assert not result.ok
    assert 'retired' in result.reason.lower(), result.reason
