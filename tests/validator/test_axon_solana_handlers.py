"""Unit tests for the Solana-repointed axon handlers (claim relay + vote_activate).

Runs the handlers against a fake serving context (program reads + sends, subtensor, chain providers);
no chain. Asserts the handler resolves the miner pubkey via its binding, gates on on-chain state, and
submits the right Solana instruction (vote_activate / submit_swap_claim) — or rejects with a clear reason.
"""

import asyncio
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.assets.asset import TransactionInfo
from allways.solana import pdas
from allways.solana.client import swap_key_from_tx_hash
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse
from allways.validator.serving import activation
from allways.validator.serving import axon as axon_handlers
from tests.validator.fakes import Ctx

HK = bt.Keypair.create_from_seed('0x' + '11' * 32)
HOTKEY = HK.ss58_address
MINER_PK = SolKeypair().pubkey()
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
        self.binding = binding
        self.miner_state = miner_state
        self.reservation = reservation
        self._min_collateral = min_collateral
        self.attestation = attestation
        self._tao_min_collateral = tao_min_collateral
        self._heartbeat = heartbeat
        self._vault_generation = vault_generation
        self.calls = []

    def miner_for(self, hotkey_ss58):
        return MINER_PK if self.binding and hotkey_ss58 == HOTKEY else None

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

    def get_swap(self, swap_key):
        return None

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block, backing='sol'):
        self.calls.append(('submit_swap_claim', miner, swap_key, from_tx_hash, from_tx_block, backing))


class FakeProvider:
    def __init__(self, tx_info, grace=0):
        self.tx_info = tx_info
        self.grace = grace

    @property
    def chain_def(self):
        return SimpleNamespace(replay_grace_secs=self.grace)

    @property
    def chain(self):
        return SimpleNamespace(normalize_address=lambda a: a, canonical_tx_hash=lambda h: h)

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


def make_ctx(solana_client, provider=None, vault=None):
    return Ctx(
        solana_client,
        providers={'btc': provider} if provider else {},
        metagraph=SimpleNamespace(hotkeys=[HOTKEY]),
        subtensor=SimpleNamespace(is_hotkey_registered=lambda netuid, hotkey_ss58: True),
        vault=vault,
        netuid=1,
    )


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
    s = run(axon_handlers.handle_miner_activate(make_ctx(client), activate_synapse()))
    assert s.accepted is False and 'not bound' in s.rejection_reason
    assert client.calls == []


def test_activate_low_collateral_rejects():
    client = FakeSolanaClient(miner_state=miner_state(collateral=10), min_collateral=1_000_000)
    s = run(axon_handlers.handle_miner_activate(make_ctx(client), activate_synapse()))
    assert s.accepted is False and 'Insufficient collateral' in s.rejection_reason
    assert client.calls == []


def test_activate_success_votes():
    client = FakeSolanaClient(miner_state=miner_state(collateral=5_000_000))
    s = run(axon_handlers.handle_miner_activate(make_ctx(client), activate_synapse()))
    assert s.accepted is True
    # No backing on the wire is "sol" — an older miner's request still means what it always meant.
    assert client.calls == [('vote_activate', MINER_PK, 'sol')]


# ---- handle_miner_activate: the TAO purse (the production trigger for split collateral) ----


def test_activate_tao_votes_when_the_bond_checks_out():
    client = FakeSolanaClient(miner_state=miner_state(collateral=10), attestation=attested())
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    # The lamport purse is far below its own floor and irrelevant: floors are per backing.
    assert s.accepted is True
    assert client.calls == [('vote_activate', MINER_PK, 'tao')]


def test_activate_tao_is_open_to_a_miner_already_serving_sol():
    # The OR view says "active"; the TAO bit is what this request is about. Purses activate separately.
    client = FakeSolanaClient(
        miner_state=miner_state(collateral=5_000_000, mask=pdas.BACKING_BIT_SOL), attestation=attested()
    )
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is True
    assert client.calls == [('vote_activate', MINER_PK, 'tao')]


def test_activate_tao_refuses_a_stale_attestation():
    # The dangerous direction: a bond unlocked on the vault still reads LOCKED on Solana for a
    # cadence, and activating in that window puts swaps behind a bond its owner can withdraw.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(epoch=EPOCH))
    v = make_ctx(client, vault=FakeVault(locked=True, epoch=EPOCH + 1))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'stale' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_a_bond_unlocked_on_the_vault():
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = make_ctx(client, vault=FakeVault(locked=False))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'not locked on the TAO vault' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_an_unreadable_vault():
    # None is "unknown", not "zero" — an undecodable dry-run must never be voted on either way.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = make_ctx(client, vault=FakeVault(locked=None))
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'could not be read off the TAO vault' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_when_no_attestation_exists_yet():
    # Attestation-before-activation: the miner retries, the validator never hand-sequences the write.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=None)
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'attestation for your TAO purse is missing' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_an_unlocked_attestation():
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(locked=False))
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'UNLOCKED' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_a_bond_under_its_own_floor():
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(balance=TAO_FLOOR - 1))
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'Bond below the TAO floor' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_while_the_fuse_is_blown():
    # heartbeat 0 = a fleet that has never proven itself live. Same gate the contract applies.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested(), heartbeat=0)
    v = make_ctx(client, vault=FakeVault())
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'fused off' in s.rejection_reason
    assert client.calls == []


def test_activate_tao_refuses_on_a_validator_with_no_relay():
    # A SOL-only deployment cannot verify a bond, so it declines instead of voting blind.
    client = FakeSolanaClient(miner_state=miner_state(), attestation=attested())
    v = make_ctx(client, vault=None)
    s = run(axon_handlers.handle_miner_activate(v, activate_synapse('tao')))
    assert s.accepted is False and 'Bond relay not configured' in s.rejection_reason
    assert client.calls == []


def test_activate_refuses_an_unknown_backing():
    client = FakeSolanaClient(miner_state=miner_state())
    s = run(axon_handlers.handle_miner_activate(make_ctx(client), activate_synapse('btc')))
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
    s = run(axon_handlers.handle_swap_confirm(make_ctx(client, FakeProvider(stale)), confirm_synapse()))
    assert s.accepted is False and 'freshness' in s.rejection_reason
    assert client.calls == []


def test_confirm_success_relays_claim():
    client = FakeSolanaClient(reservation=_reservation())
    s = run(axon_handlers.handle_swap_confirm(make_ctx(client, FakeProvider(_fresh_tx())), confirm_synapse()))
    assert s.accepted is True
    assert client.calls == [
        ('submit_swap_claim', MINER_PK, swap_key_from_tx_hash('srctx'), 'srctx', 800_000, 'sol'),
    ]


def test_activate_tao_accepts_an_attestation_from_the_current_vault_generation():
    """After a vault swap the attested epoch carries the generation in its high half; the vault
    still reports the bare lock epoch, so the check must compare like for like."""
    from allways.solana.layouts import compose_attestation_epoch

    client = FakeSolanaClient(
        miner_state=miner_state(),
        attestation=attested(epoch=compose_attestation_epoch(1, EPOCH)),
        vault_generation=1,
    )
    v = make_ctx(client, vault=FakeVault(locked=True, epoch=EPOCH))
    assert activation.check(v, HOTKEY, MINER_PK, miner_state(), 'tao', NOW).ok


def test_activate_tao_refuses_an_attestation_from_a_retired_vault():
    """A retired vault's mirror must not authorise activation just because its lock epoch happens
    to collide with the live vault's — the generation is what tells them apart."""
    client = FakeSolanaClient(
        miner_state=miner_state(),
        attestation=attested(epoch=EPOCH),  # generation 0, i.e. the retired vault
        vault_generation=1,
    )
    v = make_ctx(client, vault=FakeVault(locked=True, epoch=EPOCH))
    result = activation.check(v, HOTKEY, MINER_PK, miner_state(), 'tao', NOW)
    assert not result.ok
    assert 'retired' in result.reason.lower(), result.reason
