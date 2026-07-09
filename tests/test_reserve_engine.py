"""Unit tests for the shared on-behalf reserve op (kernel core behind both the axon + HTTP seam).

Mocks the solana_client; no chain. Asserts eligibility gating, SOL-numeraire amount derivation, and that
a joiner quotes against the PINNED pool rate (not the live quote) so it stays rate-consistent for D1.
"""

import threading
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.constants import RATE_PRECISION
from allways.validator.reserve_engine import reserve_on_behalf

HK = bt.Keypair.create_from_seed('0x' + '11' * 32)
HOTKEY = HK.ss58_address
MINER_PK = SolKeypair().pubkey()
HOTKEY_BYTES = bytes.fromhex(HK.public_key.hex())
BINDING_SIG = HK.sign(bytes(MINER_PK))
USER_PK = str(SolKeypair().pubkey())
FUTURE = 9_999_999_999


def _rate_fixed(display: float) -> int:
    return int(display * RATE_PRECISION)


class FakeClient:
    def __init__(self, *, active=True, has_active_swap=False, quote_rate=0.0021, pool=None, collateral=10**12):
        self.miner_state = SimpleNamespace(active=active, has_active_swap=has_active_swap)
        self.quote = SimpleNamespace(rate=_rate_fixed(quote_rate), from_chain='sol', to_chain='btc')
        self._pool = pool
        self.collateral = collateral
        self.calls = []

    # binding resolution (valid sr25519 binding)
    def get_hotkey_binding(self, hotkey_bytes):
        return SimpleNamespace(miner=MINER_PK)

    def get_binding(self, miner):
        return SimpleNamespace(miner=MINER_PK, hotkey=HOTKEY_BYTES, hotkey_sig=BINDING_SIG)

    def get_miner_state(self, miner):
        return self.miner_state

    def get_pool(self, miner):
        return self._pool

    def get_quote(self, miner, from_chain, to_chain):
        return self.quote

    def get_config(self):
        return SimpleNamespace(min_swap_amount=0, max_swap_amount=0)

    def get_collateral_lamports(self, miner):
        return self.collateral

    def open_or_request(self, miner, from_chain, to_chain, user, ufa, uta, sol_amount, from_amount, to_amount):
        self.calls.append(('open_or_request', sol_amount, from_amount, to_amount, str(user), ufa, uta))
        self._pool = SimpleNamespace(
            opened_at=1, closes_at=FUTURE, from_chain=from_chain, to_chain=to_chain, rate=self.quote.rate
        )
        return 'sig123'


def _validator(client):
    return SimpleNamespace(solana_client=client, axon_lock=threading.RLock())


def _reserve(client, from_amount=1_000_000_000):
    # sol->btc: user sends 1 SOL, receives btc
    return reserve_on_behalf(
        _validator(client), HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', from_amount
    )


def test_open_happy_path():
    client = FakeClient()
    r = _reserve(client)
    assert r.ok and r.pool_closes_at == FUTURE
    assert client.calls and client.calls[0][0] == 'open_or_request'
    _, sol_amount, from_amount, to_amount, user, _, uta = client.calls[0]
    assert (
        sol_amount == 1_000_000_000 and from_amount == 1_000_000_000
    )  # sol is the source leg → sol_amount == from_amount
    assert to_amount > 0 and user == USER_PK and uta == 'userBTCaddr'


def test_inactive_miner_rejects():
    r = _reserve(FakeClient(active=False))
    assert not r.ok and 'not active' in r.reason


def test_busy_miner_open_rejects():
    r = _reserve(FakeClient(has_active_swap=True))
    assert not r.ok and 'busy' in r.reason


def test_contract_rejection_returns_reject_not_raise():
    # A race can reserve the miner between our pre-check and the tx; the contract rejects (MinerReserved).
    # That must surface as ok=False (seam → 422), NOT bubble as an exception (seam → 500 crash).
    client = FakeClient()

    def _raise(*_a, **_k):
        raise RuntimeError(
            'send failed: AnchorError ... Error Code: MinerReserved. Error Number: 6022. '
            'Error Message: Miner already has an active reservation. custom program error: 0x1786'
        )

    client.open_or_request = _raise
    r = _reserve(client)
    assert not r.ok and 'active reservation' in r.reason.lower()


def test_transport_error_still_raises():
    # A genuine RPC/transport fault is NOT a domain rejection — it must propagate (seam → 500), not be
    # silently swallowed as a normal rejection.
    client = FakeClient()

    def _raise(*_a, **_k):
        raise RuntimeError('connection refused')

    client.open_or_request = _raise
    try:
        _reserve(client)
        assert False, 'expected transport error to propagate'
    except RuntimeError as e:
        assert 'connection refused' in str(e)


def test_no_quote_rejects():
    client = FakeClient()
    client.quote = None
    r = _reserve(client)
    assert not r.ok and 'no quote' in r.reason.lower()


def test_low_collateral_rejects():
    r = _reserve(FakeClient(collateral=1))
    assert not r.ok and 'collateral' in r.reason.lower()


def test_join_uses_pinned_pool_rate_not_live_quote():
    # Pool already open at a pinned rate; a live quote that drifted must be ignored for the joiner's amounts.
    pinned = SimpleNamespace(opened_at=1, closes_at=FUTURE, from_chain='sol', to_chain='btc', rate=_rate_fixed(0.0021))
    client = FakeClient(quote_rate=0.0099, pool=pinned)  # live quote drifted away from the pinned 0.0021
    r = _reserve(client)
    assert r.ok
    _, sol_amount, _, to_amount, _, _, _ = client.calls[0]
    # to_amount derived from pinned 0.0021 (≈ 0.0021 BTC for 1 SOL = 210000 sat), not the 0.0099 live quote.
    assert to_amount == 210_000


# ─── _swap_stage: closed-PDA terminal disambiguation ────────────────────────
# Terminal swaps (Completed AND TimedOut) close their PDA on-chain, so a None swap
# account alone can't tell a completion from a slash — the validator's own
# swap_outcomes index (written on SwapCompleted/SwapTimedOut ingest) must.


def _stage_validator(tmp_path):
    from allways.validator.state_store import ValidatorStateStore

    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return SimpleNamespace(state_store=store), store


def test_closed_pda_with_recorded_slash_reports_timed_out(tmp_path):
    from allways.validator.reserve_engine import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x01' * 32
    store.record_swap_outcome(key.hex(), 'timed_out', 100)
    assert _swap_stage(validator, None, key) == 'timed_out'
    store.close()


def test_closed_pda_with_recorded_completion_reports_completed(tmp_path):
    from allways.validator.reserve_engine import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x02' * 32
    store.record_swap_outcome(key.hex(), 'completed', 100)
    assert _swap_stage(validator, None, key) == 'completed'
    store.close()


def test_closed_pda_with_unrecorded_outcome_reports_fulfilled(tmp_path):
    # Ingest lag: another validator's quorum closed the PDA but this validator hasn't
    # ingested the terminal event yet. The fallback must be NON-terminal so the consumer
    # keeps polling and picks up the real outcome next ingest — a 'completed' guess for a
    # fresh slash would resurrect the original bug through a one-forward-step window.
    from allways.validator.reserve_engine import _swap_stage

    validator, store = _stage_validator(tmp_path)
    assert _swap_stage(validator, None, b'\x03' * 32) == 'fulfilled'
    store.close()


def test_live_pda_status_maps_by_variant_name(tmp_path):
    # A still-open PDA never consults the outcome index — the borsh status variant wins.
    from allways.validator.reserve_engine import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x04' * 32
    store.record_swap_outcome(key.hex(), 'completed', 100)  # must be ignored while the PDA is live
    for variant, stage in [('Active', 'active'), ('Fulfilled', 'fulfilled'), ('TimedOut', 'timed_out')]:
        swap = SimpleNamespace(status=type(variant, (), {})())
        assert _swap_stage(validator, swap, key) == stage
    store.close()


# ─── swap_status by swap_key (post-attestation resolution) ──────────────────
# vote_initiate consumes the reservation at attestation quorum (reserved_until=0,
# claimed_swap_key cleared), so post-attestation stages are only reachable by key —
# the consumer persists the swap_key from /confirm and polls /status with it.


class StatusClient:
    """Minimal client for the status paths: swap-by-key + reservation + a valid binding."""

    def __init__(self, swap=None, reservation=None):
        self._swap = swap
        self._reservation = reservation
        self.swap_keys_queried = []

    def get_swap(self, swap_key):
        self.swap_keys_queried.append(swap_key)
        return self._swap

    def get_reservation(self, miner):
        return self._reservation

    def get_hotkey_binding(self, hotkey_bytes):
        return SimpleNamespace(miner=MINER_PK)

    def get_binding(self, miner):
        return SimpleNamespace(miner=MINER_PK, hotkey=HOTKEY_BYTES, hotkey_sig=BINDING_SIG)


def _live_swap(variant: str):
    return SimpleNamespace(
        status=type(variant, (), {})(),
        user='userSOLpk',
        from_chain='sol',
        to_chain='btc',
        from_amount=1_000_000_000,
        to_amount=210_000,
        miner_from_addr='minerSOLaddr',
    )


def _status_validator(tmp_path, client):
    validator, store = _stage_validator(tmp_path)
    validator.solana_client = client
    return validator, store


def _unclaimed_reservation(reserved_until: int):
    return SimpleNamespace(
        reserved_until=reserved_until,
        claimed_swap_key=b'\x00' * 32,
        user='staleUserSOLpk',
        from_chain='btc',
        to_chain='sol',
        from_amount=10_000,
        to_amount=47_000_000,
        miner_from_addr='tb1qminer',
    )


def test_expired_unclaimed_reservation_reports_none(tmp_path):
    """A dead (expired, never-claimed) reservation must not surface as 'reserved' with its stale
    user — the offering's win-detection would read it as another user holding the miner."""
    import time as _time

    from allways.validator.reserve_engine import swap_status

    client = StatusClient(reservation=_unclaimed_reservation(int(_time.time()) - 5))
    validator, _ = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'none'


def test_live_unclaimed_reservation_reports_reserved(tmp_path):
    from allways.validator.reserve_engine import swap_status

    client = StatusClient(reservation=_unclaimed_reservation(FUTURE))
    validator, _ = _status_validator(tmp_path, client)
    s = swap_status(validator, HOTKEY)
    assert s.stage == 'reserved' and s.user == 'staleUserSOLpk'


def test_initiated_swap_resolves_by_key_after_reservation_consumed(tmp_path):
    from allways.validator.reserve_engine import swap_status

    key = b'\x05' * 32
    consumed = SimpleNamespace(reserved_until=0)  # vote_initiate zeroed it at quorum
    client = StatusClient(swap=_live_swap('Active'), reservation=consumed)
    validator, store = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'none'  # reservation path is blind post-attestation
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'active' and s.swap_key == key.hex() and s.reserved_until == 0
    assert s.detail['from_chain'] == 'sol' and s.detail['to_amount'] == 210_000
    assert client.swap_keys_queried == [key]
    store.close()


def test_closed_pda_by_key_with_recorded_slash_reports_timed_out(tmp_path):
    from allways.validator.reserve_engine import swap_status

    key = b'\x06' * 32
    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    store.record_swap_outcome(key.hex(), 'timed_out', 100)
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'timed_out' and s.swap_key == key.hex() and s.detail == {}
    store.close()


def test_closed_pda_by_key_with_unrecorded_outcome_reports_fulfilled(tmp_path):
    from allways.validator.reserve_engine import swap_status

    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    assert swap_status(validator, HOTKEY, (b'\x07' * 32).hex()).stage == 'fulfilled'
    store.close()


def test_malformed_swap_key_raises_value_error(tmp_path):
    # Non-hex or wrong-length keys must raise ValueError (seam maps it to a 400).
    from allways.validator.reserve_engine import swap_status

    validator, store = _status_validator(tmp_path, StatusClient())
    for bad in ('zz', 'abcd'):
        try:
            swap_status(validator, HOTKEY, bad)
            assert False, f'expected ValueError for swap_key={bad!r}'
        except ValueError:
            pass
    store.close()


# ── confirm_deposit: deferred-confirmation intake. Accepts a content-valid deposit even before it fully
# confirms (the crank defers voting until confirmations accrue); fast-fails without a claim on absent/mismatch
# (None) or a stale MINED deposit, so the short reservation TTL frees the miner.
from allways.chain_providers.base import ProviderUnreachableError, TransactionInfo  # noqa: E402
from allways.validator.reserve_engine import confirm_deposit  # noqa: E402

CONFIRM_CREATED_AT = 1000


class _ConfirmClient(FakeClient):
    def __init__(self, reservation, **kw):
        super().__init__(**kw)
        self._reservation = reservation
        self.claims = []

    def get_reservation(self, miner):
        return self._reservation

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block):
        self.claims.append((swap_key, from_tx_hash, from_tx_block))
        return 'claimsig'


class _FakeProvider:
    def __init__(self, tx_info, *, unreachable=False, grace=0):
        self._tx = tx_info
        self._unreachable = unreachable
        self._grace = grace

    def verify_transaction(self, **kw):
        if self._unreachable:
            raise ProviderUnreachableError('down')
        return self._tx

    def get_chain(self):
        return SimpleNamespace(replay_grace_secs=self._grace)


def _confirm_reservation(**over):
    d = dict(
        reserved_until=FUTURE,
        claimed_swap_key=b'\x00' * 32,
        from_chain='btc',
        miner_from_addr='minerBTC',
        from_amount=100_000,
        from_addr='userBTC',
        created_at=CONFIRM_CREATED_AT,
    )
    d.update(over)
    return SimpleNamespace(**d)


def _tx(*, confirmed, block_time, confirmations=0):
    return TransactionInfo(
        tx_hash='abc',
        confirmed=confirmed,
        sender='userBTC',
        recipient='minerBTC',
        amount=100_000,
        block_number=(None if block_time is None else 500),
        confirmations=confirmations,
        block_time=block_time,
    )


def _confirm(reservation, tx_info, *, unreachable=False):
    client = _ConfirmClient(reservation)
    provider = _FakeProvider(tx_info, unreachable=unreachable)
    validator = SimpleNamespace(
        solana_client=client, axon_chain_providers={'btc': provider}, axon_lock=threading.RLock()
    )
    return confirm_deposit(validator, HOTKEY, 'srctxhash'), client


def test_confirm_accepts_unconfirmed_mempool_deposit():
    # KEY new behavior: a content-valid 0-conf mempool tx (no block_time) still creates the claim.
    r, client = _confirm(_confirm_reservation(), _tx(confirmed=False, block_time=None))
    assert r.ok and client.claims


def test_confirm_accepts_mined_low_conf_fresh_deposit():
    # Mined but below min_confirmations, block_time present + fresh → accepted; crank defers the rest.
    r, client = _confirm(
        _confirm_reservation(), _tx(confirmed=False, block_time=CONFIRM_CREATED_AT + 5, confirmations=1)
    )
    assert r.ok and client.claims


def test_confirm_accepts_deeply_confirmed_fast_chain_deposit():
    # Regression: a deeply-confirmed source still creates the claim (unchanged path for SOL/TAO fast chains).
    r, client = _confirm(
        _confirm_reservation(), _tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6)
    )
    assert r.ok and client.claims


def test_confirm_rejects_absent_or_mismatch_without_claim():
    # verify_transaction None (absent OR content mismatch) → fast-fail, no claim, TTL frees the miner.
    r, client = _confirm(_confirm_reservation(), None)
    assert not r.ok and not client.claims


def test_confirm_rejects_stale_mined_deposit_without_claim():
    # A MINED tx older than the reservation floor is a replay → freshness fast-fail (block_time checkable).
    r, client = _confirm(
        _confirm_reservation(), _tx(confirmed=True, block_time=CONFIRM_CREATED_AT - 1, confirmations=6)
    )
    assert not r.ok and not client.claims


def test_confirm_rejects_when_reservation_expired():
    r, client = _confirm(_confirm_reservation(reserved_until=1), _tx(confirmed=False, block_time=None))
    assert not r.ok and not client.claims


def test_confirm_rejects_when_reservation_already_claimed():
    r, client = _confirm(_confirm_reservation(claimed_swap_key=b'\x07' * 32), _tx(confirmed=True, block_time=FUTURE))
    assert not r.ok and not client.claims


def test_confirm_provider_unreachable_resends_without_claim():
    r, client = _confirm(_confirm_reservation(), None, unreachable=True)
    assert not r.ok and not client.claims and 'unreachable' in r.reason.lower()
