"""A fresh fee ledger has to start counting where the vault last stopped, or two validators vote
different fee vectors forever. Every test here is a way a second validator could disagree with the first."""

from types import SimpleNamespace

from allways.constants import TAO_HUB_VAULT_ADDRESSES
from allways.validator.feed.events import HISTORY_HORIZON_SECS
from allways.validator.relay import stop_point
from allways.validator.relay.ledger import RelayLedger
from allways.validator.store.db import Store
from tests.validator.test_relay_engine import HOTKEY, MINER, NOW, RAO, SWAP, USER_TAO, FakeVault, _relay

HOUR = 3600
CADENCE = 6 * HOUR


# --- the lookup ---------------------------------------------------------------------------------


def test_the_window_reaches_back_two_boundaries():
    now = 10 * CADENCE + 2 * HOUR
    assert stop_point.boundaries(now, CADENCE) == [10 * CADENCE, 9 * CADENCE]
    assert stop_point.boundaries(10 * CADENCE + 30, CADENCE) == [10 * CADENCE, 9 * CADENCE, 8 * CADENCE]
    assert all(b >= now - HISTORY_HORIZON_SECS for b in stop_point.boundaries(now, CADENCE))


def _reads(history):
    """settled_at off a {hotkey: [(from_time, value), ...]} timeline, counting reads."""
    calls = []

    def settled_at(hotkey, t):
        calls.append((hotkey, t))
        value = 0
        for since, v in history.get(hotkey, []):
            if since <= t:
                value = v
        return value

    return settled_at, calls


def test_a_settle_after_the_latest_boundary_means_the_vault_stands_there():
    now = 10 * CADENCE + 2 * HOUR
    settled_at, _ = _reads({HOTKEY: [(0, 5), (10 * CADENCE + HOUR, 8)]})
    assert stop_point.resolve(now, CADENCE, 2, [HOTKEY], settled_at) == 10 * CADENCE


def test_a_round_still_waiting_on_our_vote_leaves_the_vault_one_boundary_back():
    now = 10 * CADENCE + 2 * HOUR
    settled_at, _ = _reads({HOTKEY: [(0, 2), (9 * CADENCE + HOUR, 5)]})  # boundary 9 applied, 10 is waiting
    assert stop_point.resolve(now, CADENCE, 2, [HOTKEY], settled_at) == 9 * CADENCE


def test_no_settle_in_the_window_answers_the_oldest_boundary_this_validator_can_count_from():
    now = 10 * CADENCE + 2 * HOUR
    settled_at, _ = _reads({HOTKEY: [(0, 5)]})
    assert stop_point.resolve(now, CADENCE, 2, [HOTKEY], settled_at) == 9 * CADENCE


def test_a_failed_archive_read_is_unknown_never_a_guess():
    now = 10 * CADENCE + 2 * HOUR
    assert stop_point.resolve(now, CADENCE, 2, [HOTKEY], lambda hk, t: None) is None
    assert stop_point.resolve(now, CADENCE, None, [HOTKEY], lambda hk, t: 0) is None


# --- two validators, one vault ------------------------------------------------------------------


def _ledger(vault, clock, seed_as_of=None):
    ledger = RelayLedger(
        Store(':memory:'),
        'tao',
        settled=lambda m: vault.get(m, 0),
        boundary=lambda t: (t // CADENCE) * CADENCE,
        clock=lambda: clock['now'],
    )
    ledger.seed_as_of = seed_as_of
    return ledger


def test_a_fresh_validator_joining_while_a_round_waits_on_it_counts_what_the_first_one_counts():
    # The vault settled M to 5 at boundary 9. A fee of 3 lands, A votes 8 at boundary 10, and the
    # round waits for B — which then boots with an empty database.
    vault, clock = {'M': 5}, {'now': 9 * CADENCE + HOUR}
    a = _ledger(vault, clock)
    a.accrued_fee_total('M', 'tao', vault_generation=1)  # A has been counting since before the fee
    a.record_relay_fee('s1', 'M', 'tao', 3, 9 * CADENCE + 2 * HOUR, vault_generation=1)
    clock['now'] = 10 * CADENCE + HOUR
    fee_rows = [('s1', 'M', 'tao', 3, 9 * CADENCE + 2 * HOUR)]

    naive = _ledger(vault, clock)  # the old seed: assumes the vault stands at the latest boundary
    stopped = _ledger(vault, clock, seed_as_of=9 * CADENCE)  # what the lookup answers
    for ledger in (naive, stopped):
        for row in fee_rows:
            ledger.record_relay_fee(*row, vault_generation=1)
        ledger.accrued_fee_total('M', 'tao', vault_generation=1)

    for boundary in (10 * CADENCE, 11 * CADENCE, 12 * CADENCE):
        clock['now'] = boundary + 300
        want = a.accrued_fee_total('M', 'tao', at_time=boundary, vault_generation=1)
        assert want == 8
        assert stopped.accrued_fee_total('M', 'tao', at_time=boundary, vault_generation=1) == want
        assert naive.accrued_fee_total('M', 'tao', at_time=boundary, vault_generation=1) == 5, 'short forever'


def test_a_miner_the_vault_never_settled_counts_every_fee_row_whenever_it_is_first_seen():
    vault, clock = {}, {'now': 10 * CADENCE + HOUR}
    ledger = _ledger(vault, clock)
    ledger.record_relay_fee('s1', 'M', 'tao', 3, 9 * CADENCE + HOUR, vault_generation=1)
    assert ledger.accrued_fee_total('M', 'tao', vault_generation=1) == 3


# --- the boot barrier ---------------------------------------------------------------------------


class _Archive:
    def __init__(self, history=None, up=True):
        self.history, self.up, self.reads = history or {}, up, []

    def settled_at(self, hotkey, t):
        self.reads.append((hotkey, t))
        if not self.up:
            return None
        value = 0
        for since, v in self.history.get(hotkey, []):
            if since <= t:
                value = v
        return value


def _fresh_relay(archive, validators=('us', 'them'), settled=None):
    vault = FakeVault(settled=settled if settled is not None else {HOTKEY: 5 * RAO})
    vault.validators = list(validators)
    relay = _relay(vault=vault, fee_cadence_secs=CADENCE)
    relay.store.settled = lambda miner: relay.settled_total(relay.hotkey_for(miner))  # as the hub component wires it
    relay.archive = archive
    return relay


def test_a_fresh_ledger_holds_the_barrier_until_the_archive_answers_and_seeds_nothing_meanwhile():
    archive = _Archive(up=False)
    relay = _fresh_relay(archive)
    relay.step(NOW)
    assert not relay._reconciled
    assert not relay.store.has_fee_bases(), 'no miner seeded from a guessed starting point'
    assert not [c for c in relay.chain.calls if c[0] == 'vote_set_attestation']

    archive.up = True
    relay.step(NOW + 12)
    assert relay._reconciled
    assert relay.store.get_relay_meta('fee_stop_point') is not None
    assert relay.store.has_fee_bases()
    assert relay.store.seed_as_of is None, 'the lookup only steers the boot seeding'


def test_a_verdict_is_relayed_while_the_barrier_waits_on_the_archive():
    # Only the fee seed needs the stop point: a slash carries its own figures and touches no fee base.
    archive = _Archive(up=False)
    relay = _fresh_relay(archive)
    relay.store.record_relay_slash(SWAP, MINER, 'tao', 22 * RAO, 22 * RAO, USER_TAO, NOW, HOTKEY)
    relay.step(NOW)
    assert ('vote_slash', HOTKEY, SWAP, 22 * RAO, USER_TAO, 22 * RAO) in relay.vault.calls
    assert relay.store.open_relay_slashes('tao') == []
    assert relay.has_pending_debit(MINER), 'entry stays held until the netted attestation is on Solana'
    assert not relay._reconciled, 'the attestations are still owed, so the heartbeat stays down'
    assert not relay.store.has_fee_bases()


def test_a_lone_validator_boots_without_the_archive():
    archive = _Archive(up=False)
    relay = _fresh_relay(archive, validators=('us',))
    relay.step(NOW)
    assert relay._reconciled
    assert not archive.reads


def test_the_seeded_vector_matches_the_long_running_validator_after_a_fresh_boot():
    # End to end through the relay: boundary 9 settled M to 5, a 3-fee is owed, boundary 10's round waits.
    t9, t10 = 300_000 * CADENCE, 300_001 * CADENCE
    archive = _Archive({HOTKEY: [(0, 2 * RAO), (t9 + HOUR, 5 * RAO)]})
    relay = _fresh_relay(archive)
    relay.store.record_relay_fee(SWAP, MINER, 'tao', 3 * RAO, t9 + 2 * HOUR, vault_generation=0)
    relay.step(t10 + HOUR)
    assert relay.store.get_relay_meta('fee_stop_point') == str(t9)
    assert ('vote_collect_fees_batch', [(HOTKEY, 8 * RAO)]) in relay.vault.calls, 'the same 8 the first validator voted'


# --- the archive connection ---------------------------------------------------------------------


def test_the_archive_is_picked_by_the_vault_of_record_not_the_endpoint(monkeypatch):
    monkeypatch.delenv('SUBTENSOR_ARCHIVE_ENDPOINTS', raising=False)

    def reader(net):
        return stop_point.ArchiveReader(SimpleNamespace(address=TAO_HUB_VAULT_ADDRESSES[net]))

    assert reader('test').endpoints() == ['test']
    assert reader('finney').endpoints() == ['archive']
    assert stop_point.ArchiveReader(SimpleNamespace(address='5Unknown')).endpoints() == []
    monkeypatch.setenv('SUBTENSOR_ARCHIVE_ENDPOINTS', 'wss://a.example, wss://b.example')
    assert reader('finney').endpoints() == ['wss://a.example', 'wss://b.example']


class _Chain:
    """A substrate whose block n was produced at t0 + 12n, with an occasional slow block."""

    def __init__(self, head, t0=1_000_000):
        self.head, self.t0, self.queries = head, t0, 0

    def time(self, n):
        return self.t0 + 12 * n + (7 if n > 500 else 0)

    def get_chain_head(self):
        return f'0x{self.head}'

    def get_block_number(self, block_hash):
        return int(block_hash[2:])

    def get_block_hash(self, n):
        return f'0x{n}'

    def query(self, module, name, block_hash=None):
        self.queries += 1
        return SimpleNamespace(value=self.time(int(block_hash[2:])) * 1000)


def test_the_block_search_lands_on_the_first_block_at_or_after_the_time_in_a_few_reads():
    chain = _Chain(head=5_000)
    reader = stop_point.ArchiveReader(SimpleNamespace(address='x'), connect=lambda url: chain)
    reader.endpoints = lambda: ['x']
    for target in (chain.time(1234), chain.time(1234) - 5, chain.time(600) + 1):
        n = int(reader.block_at(target)[2:])
        assert chain.time(n) >= target > chain.time(n - 1)
    assert chain.queries <= 3 * stop_point.MAX_PROBES


# --- the signer's balance -----------------------------------------------------------------------
