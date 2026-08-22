"""Unit tests for the shared on-behalf reserve op (kernel core behind both the axon + HTTP seam).

Mocks the solana_client; no chain. Asserts eligibility gating, SOL-numeraire amount derivation, and that
a joiner quotes against the PINNED pool rate (not the live quote) so it stays rate-consistent for D1.
"""

import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.constants import RATE_PRECISION
from allways.validator.reserve_engine import reserve_on_behalf
from allways.validator.state_store import ValidatorStateStore

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
        self.miner_state = SimpleNamespace(
            active=active,
            has_active_swap=has_active_swap,
            active_swap_backings=1 if has_active_swap else 0,
            collateral=collateral,
        )
        self.quote = SimpleNamespace(
            rate=_rate_fixed(quote_rate), from_chain='sol', to_chain='btc', collateral_chain='sol'
        )
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

    def get_pool(self, miner, backing='sol'):
        return self._pool

    def get_quote(self, miner, from_chain, to_chain, backing='sol'):
        return self.quote if backing == self.quote.collateral_chain else None

    def get_quotes_for_direction(self, miner, from_chain, to_chain):
        return [self.quote]

    def get_bond_attestation(self, miner, chain='tao'):
        return None

    def get_config(self):
        return SimpleNamespace(min_swap_amount=0, max_swap_amount=0, tao_min_swap_amount=0, tao_max_swap_amount=0)

    def get_collateral_lamports(self, miner):
        return self.collateral

    def open_or_request(self, miner, from_chain, to_chain, backing='sol'):
        # Two-phase: a bid carries only the pair + the backing (the winner names the fill at finalize).
        self.calls.append(('open_or_request', from_chain, to_chain, backing))
        self._pool = SimpleNamespace(
            opened_at=1,
            closes_at=FUTURE,
            from_chain=from_chain,
            to_chain=to_chain,
            rate=self.quote.rate,
            collateral_chain=backing,
        )
        return 'sig123'


def _validator(client):
    store = ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db')
    scheduler = SimpleNamespace(
        scheduled=[], schedule=lambda miner, closes_at: scheduler.scheduled.append((miner, closes_at))
    )
    return SimpleNamespace(
        solana_client=client, axon_lock=threading.RLock(), state_store=store, crank_scheduler=scheduler
    )


def _reserve(client, from_amount=1_000_000_000):
    # sol->btc: user sends 1 SOL, receives btc
    validator = _validator(client)
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', from_amount)
    return result, validator.state_store


def test_open_happy_path_persists_routed_request():
    # Two-phase: reserve_on_behalf places a BID after a viability pre-check, then queues the
    # user's details for finalize_won_seats (the winner names the fill at finalize).
    client = FakeClient()
    r, store = _reserve(client)
    assert r.ok and r.pool_closes_at == FUTURE
    assert client.calls == [('open_or_request', 'sol', 'btc', 'sol')]
    queued = store.pending_routed_requests(str(MINER_PK), 'sol', 'btc')
    assert len(queued) == 1
    assert queued[0]['user_pubkey'] == USER_PK
    assert queued[0]['from_amount'] == 1_000_000_000
    store.close()


def test_open_normalizes_the_source_address_at_intake():
    # V-C2: the queued source address is the string later hashed into the source-lock PDA —
    # canonicalize at intake so a checksummed EVM-style entry can't mint a divergent lock.
    client = FakeClient()
    validator = _validator(client)
    validator.axon_assets = {'sol': SimpleNamespace(chain=SimpleNamespace(normalize_address=lambda a: a.lower()))}
    r = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, 'MiXeDcAsE', 'userBTCaddr', 1_000_000_000)
    assert r.ok
    queued = validator.state_store.pending_routed_requests(str(MINER_PK), 'sol', 'btc')
    assert queued[0]['user_from_addr'] == 'mixedcase'
    validator.state_store.close()


def _gate_asset(can_deliver, valid=lambda addr: True):
    """Duck-typed asset for the reserve deliverability gates (can_deliver_to + chain format check)."""
    return SimpleNamespace(
        can_deliver_to=lambda addr, amt, from_address=None: can_deliver(addr, amt),
        chain=SimpleNamespace(is_valid_address=valid, normalize_address=lambda addr: addr),
    )


def test_reserve_does_not_predict_dest_deliverability():
    # Reserve-time deliverability is NOT a security boundary (a dest can pass here then revert later),
    # so a dest that WOULD refuse is no longer bounced here — the sound check is the delivery-time
    # reverted-tx proof (cancel_swap). Validity is still screened (see the malformed test below).
    client = FakeClient()
    validator = _validator(client)
    validator.axon_assets = {'btc': _gate_asset(lambda addr, amt: False)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert 'rejects incoming transfers' not in (result.reason or '')


def test_missing_spoke_provider_rejects_before_bid():
    client = FakeClient()
    validator = _validator(client)
    validator.axon_assets = {'sol': _gate_asset(lambda addr, amt: True)}
    validator.assets = {'sol': object()}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'cannot verify btc' in result.reason
    assert client.calls == []


def test_malformed_destination_address_rejects_before_any_bid():
    # F4: address FORMAT is screened first — offline, and a malformed dest can never deliver.
    client = FakeClient()
    validator = _validator(client)
    validator.axon_assets = {'btc': _gate_asset(lambda addr, amt: True, valid=lambda addr: False)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'not-an-addr', 10**9)
    assert not result.ok
    assert 'not a valid btc address' in result.reason
    assert client.calls == []


def test_malformed_miner_receive_address_rejects():
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.axon_assets = {'sol': _gate_asset(lambda addr, amt: True, valid=lambda addr: addr != 'minerSOLaddr')}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'miner receive address' in result.reason
    assert client.calls == []


def test_user_to_addr_equal_miner_delivery_address_rejects():
    # V-H1: user_to_addr == miner_to_addr makes every delivery a from==to self-transfer the anti-wash
    # verifier rejects → the miner never confirms the leg and is force-slashed with no cancel escape.
    # Bounce it at reserve before any bid.
    client = FakeClient()
    client.quote.miner_to_addr = 'minerBTCdeliver'
    validator = _validator(client)
    validator.axon_assets = {'btc': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'minerBTCdeliver', 10**9)
    assert not result.ok
    assert 'differ from the miner delivery address' in result.reason
    assert client.calls == []


def test_distinct_dest_addr_with_miner_to_addr_set_reserves():
    # The guard must not over-reject: a normal dest address different from the miner's still reserves.
    client = FakeClient()
    client.quote.miner_to_addr = 'minerBTCdeliver'
    validator = _validator(client)
    validator.axon_assets = {'btc': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert result.ok


def test_cased_chain_id_rejects_at_intake():
    # F4: chain ids are lowercase everywhere — a cased id must bounce before it can derive
    # a mismatched quote PDA (the program enforces the same at its own intake).
    client = FakeClient()
    validator = _validator(client)
    result = reserve_on_behalf(validator, HOTKEY, 'SOL', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'lowercase' in result.reason
    assert client.calls == []


def test_miner_receive_address_screened_on_source_chain():
    # T18: a miner quoting a rejecting/blacklisted receive address griefs takers into a
    # burned entry fee — the reserve gate screens the SOURCE side too, before any bid.
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.axon_assets = {'sol': _gate_asset(lambda addr, amt: addr != 'minerSOLaddr')}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'miner receive address' in result.reason
    assert client.calls == []


def test_miner_receive_address_clean_still_bids():
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.axon_assets = {'sol': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert result.ok
    assert client.calls == [('open_or_request', 'sol', 'btc', 'sol')]


def test_inactive_miner_rejects():
    r, store = _reserve(FakeClient(active=False))
    assert not r.ok and 'not active' in r.reason
    assert store.distinct_routed_pools() == []  # nothing queued on rejection


def test_busy_miner_open_rejects():
    r, _ = _reserve(FakeClient(has_active_swap=True))
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
    r, store = _reserve(client)
    assert not r.ok and 'active reservation' in r.reason.lower()
    assert store.distinct_routed_pools() == []  # a failed entry queues nothing


def test_contract_rejection_code_only_form_returns_reject():
    # Same race, but the reject tx LANDS failed instead of failing pre-flight: the confirm path
    # surfaces only `{'InstructionError': [0, {'Custom': 6022}]}` — no Anchor name, no 'custom program
    # error' text. Must still be a 422 domain reject, not a 500 crash (the F2-class code-only miss).
    client = FakeClient()

    def _raise(*_a, **_k):
        raise RuntimeError("tx 5abc failed: {'InstructionError': [0, {'Custom': 6022}]}")

    client.open_or_request = _raise
    r, _ = _reserve(client)
    assert not r.ok and '6022' in r.reason


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
    r, _ = _reserve(client)
    assert not r.ok and 'no quote' in r.reason.lower()


def test_low_collateral_rejects():
    r, store = _reserve(FakeClient(collateral=1))
    assert not r.ok and 'collateral' in r.reason.lower()
    assert store.distinct_routed_pools() == []


def test_join_uses_pinned_pool_rate_not_live_quote():
    # Joining an already-open pool bids successfully even when the live quote has drifted from the
    # pinned rate: the joiner's viability pre-check computes against pool.rate (0.0021), not the live
    # 0.0099. Under two-phase the settlement guarantee (fill honors the pinned rate) is enforced by the
    # contract at finalize (Rust suite) — the bid itself carries no amounts.
    pinned = SimpleNamespace(opened_at=1, closes_at=FUTURE, from_chain='sol', to_chain='btc', rate=_rate_fixed(0.0021))
    client = FakeClient(quote_rate=0.0099, pool=pinned)  # live quote drifted away from the pinned 0.0021
    r, _ = _reserve(client)
    assert r.ok
    assert client.calls == [('open_or_request', 'sol', 'btc', 'sol')]


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

    def get_reservation(self, miner, backing='sol'):
        # A dict models a dual-purse miner's per-hub slots (v3.1); a bare value is the same on every hub.
        if isinstance(self._reservation, dict):
            return self._reservation.get(backing)
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
        from_tx_hash='srcTxHash',
        to_tx_hash='',
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
import allways.validator.reserve_engine as rc  # noqa: E402
from allways.assets.asset import ProviderUnreachableError, TransactionInfo  # noqa: E402
from allways.validator.reserve_engine import confirm_deposit  # noqa: E402

CONFIRM_CREATED_AT = 1000


class _ConfirmClient(FakeClient):
    def __init__(self, reservation, **kw):
        super().__init__(**kw)
        self._reservation = reservation
        self.claims = []
        self.extensions = []
        self.extend_backings = []
        self.extend_raises = False

    def get_reservation(self, miner, backing='sol'):
        # A dict models a dual-purse miner's per-hub slots (v3.1); a bare value is the same on every hub.
        if isinstance(self._reservation, dict):
            return self._reservation.get(backing)
        return self._reservation

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block, backing='sol'):
        self.claims.append((swap_key, from_tx_hash, from_tx_block, backing))
        return 'claimsig'

    def extend_reservation(self, miner, target_at, backing='sol', *, from_chain=None, from_addr=None):
        if self.extend_raises:
            raise RuntimeError('rpc down')
        self.extensions.append(target_at)
        self.extend_backings.append(backing)
        return 'extendsig'


class _FakeProvider:
    def __init__(self, tx_info, *, unreachable=False, grace=0, normalize=None):
        self._tx = tx_info
        self._unreachable = unreachable
        self._grace = grace
        self._normalize = normalize or (lambda a: a)

    def verify_transaction(self, **kw):
        if self._unreachable:
            raise ProviderUnreachableError('down')
        return self._tx

    @property
    def chain_def(self):
        return SimpleNamespace(replay_grace_secs=self._grace)

    @property
    def chain(self):
        return SimpleNamespace(normalize_address=self._normalize)


def _confirm_reservation(**over):
    d = dict(
        reserved_until=FUTURE,
        claimed_swap_key=b'\x00' * 32,
        from_chain='btc',
        miner_from_addr='minerBTC',
        from_amount=100_000,
        from_addr='userBTC',
        created_at=CONFIRM_CREATED_AT,
        max_extend_at=FUTURE,
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
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
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


def test_confirm_prefers_canonical_form_slot_over_case_variant():
    # V-C2 residual: a case variant of a live source dodges the byte-keyed source lock. When one
    # deposit content-matches two slots, the canonical-form (honest-lane) reservation wins the claim
    # even when the variant slot is older — the variant can only come from bypassing our tooling.
    variant = _confirm_reservation(from_addr='0xABC', created_at=500)
    canonical = _confirm_reservation(from_addr='0xabc', created_at=900)
    client = _ConfirmClient({'sol': variant, 'tao': canonical})
    provider = _FakeProvider(
        _tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6), normalize=str.lower
    )
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and len(client.claims) == 1
    assert client.claims[0][3] == 'tao'  # the canonical slot's backing claimed, not the variant's


def test_confirm_prefers_the_oldest_slot_on_a_form_tie():
    # Dual-backing (V-I1): the same source legitimately backs two hubs. Deterministic pick = oldest.
    younger = _confirm_reservation(created_at=900)
    older = _confirm_reservation(created_at=500)
    client = _ConfirmClient({'sol': younger, 'tao': older})
    provider = _FakeProvider(_tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6))
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims[0][3] == 'tao'


def test_confirm_fresh_match_on_one_hub_survives_stale_match_on_another():
    # A stale match is terminal for ITS hub only: a fresh match on the miner's other hub still claims
    # (the old first-match-wins scan could fail the whole confirm on slot-scan order).
    stale_hub = _confirm_reservation(created_at=CONFIRM_CREATED_AT)
    fresh_hub = _confirm_reservation(from_chain='sol', created_at=CONFIRM_CREATED_AT)
    client = _ConfirmClient({'sol': stale_hub, 'tao': fresh_hub})
    providers = {
        'btc': _FakeProvider(_tx(confirmed=True, block_time=CONFIRM_CREATED_AT - 1, confirmations=6)),
        'sol': _FakeProvider(_tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6)),
    }
    validator = SimpleNamespace(solana_client=client, axon_assets=providers, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims[0][3] == 'tao'


# ── claim runway: a verified deposit must not lose its window mid-relay ──────
# submit_swap_claim needs reserved_until >= now. If it lapses between the taker sending and the
# relay landing there is no claim, no Swap, no timeout and no refund — the deposit is just gone.
# So a deposit that has already verified against the pinned reservation buys runway first.
def _near_expiry(secs_left, **over):
    return _confirm_reservation(reserved_until=int(time.time()) + secs_left, **over)


def test_confirm_extends_reservation_when_runway_is_short():
    r, client = _confirm(_near_expiry(20), _tx(confirmed=False, block_time=None))
    assert r.ok and client.claims, 'the claim must still be submitted'
    assert len(client.extensions) == 1
    # Extended to a real margin ahead of now, not merely one second past the old deadline.
    assert client.extensions[0] >= int(time.time()) + rc.CLAIM_RELAY_MARGIN_SECS - 5


def test_confirm_measures_runway_after_the_source_rpc(monkeypatch):
    # verify_transaction is a source-chain RPC that can burn seconds on BTC. Runway read before it
    # runs can say "ample" while the real window is already short, and an extension computed off that
    # stale clock buys less than the margin — so the helper re-reads the clock.
    start = int(time.time())
    clock = {'t': start}
    resv = _confirm_reservation(reserved_until=start + rc.CLAIM_RELAY_MARGIN_SECS + 30, max_extend_at=start + 10_000)
    client = _ConfirmClient(resv)

    class _SlowProvider(_FakeProvider):
        def verify_transaction(self, **kw):
            clock['t'] += 60  # the RPC hung; the window shrank while we waited
            return super().verify_transaction(**kw)

    validator = SimpleNamespace(
        solana_client=client,
        axon_assets={'btc': _SlowProvider(_tx(confirmed=False, block_time=None))},
        axon_lock=threading.RLock(),
    )
    monkeypatch.setattr(rc.time, 'time', lambda: clock['t'])
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims
    # Off the pre-RPC clock this reservation looks ample and never extends.
    assert client.extensions == [clock['t'] + rc.CLAIM_RELAY_MARGIN_SECS]


def test_confirm_does_not_extend_when_runway_is_ample():
    # Don't burn an extension (or the ceiling budget) on a reservation that has plenty left.
    r, client = _confirm(_near_expiry(rc.CLAIM_RELAY_MARGIN_SECS + 60), _tx(confirmed=False, block_time=None))
    assert r.ok and client.claims
    assert client.extensions == []


def test_confirm_does_not_extend_past_the_contract_ceiling():
    # max_extend_at is frozen at creation; the contract rejects a target above it, so don't try.
    now = int(time.time())
    resv = _confirm_reservation(reserved_until=now + 20, max_extend_at=now + 20)
    r, client = _confirm(resv, _tx(confirmed=False, block_time=None))
    assert r.ok and client.claims, 'no headroom left, but the claim is still worth attempting'
    assert client.extensions == []


def test_confirm_claims_even_if_the_extension_fails():
    # Best-effort: the reservation may still have just enough runway, and a claim that lands beats a
    # clean error path. A failed extension must never sink the deposit.
    client = _ConfirmClient(_near_expiry(20))
    client.extend_raises = True
    provider = _FakeProvider(_tx(confirmed=False, block_time=None))
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims


# ── V-1: the deposit-confirm seam must scan per-hub slots, not the SOL default ──
# v3.1 seeds a reservation per (miner, backing). A SOL-defaulted get_reservation reads the empty SOL
# slot for a TAO-backed deposit, so no claim lands and the deposit is silently lost.
def test_confirm_uses_tao_slot_when_sol_slot_empty():
    resv = _near_expiry(20)  # near expiry so the runway extend also travels on the right hub
    client = _ConfirmClient({'sol': None, 'tao': resv})
    provider = _FakeProvider(_tx(confirmed=False, block_time=None))
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims
    assert client.claims[0][3] == 'tao'  # claim submitted against the TAO hub, not the empty SOL slot
    assert client.extend_backings == ['tao']  # runway bought on the same hub


def test_confirm_no_live_unclaimed_slot_on_any_hub_rejects():
    # Empty SOL + expired TAO: nothing live+unclaimed anywhere → no claim, TTL frees the miner.
    client = _ConfirmClient({'sol': None, 'tao': _confirm_reservation(reserved_until=1)})
    provider = _FakeProvider(_tx(confirmed=False, block_time=None))
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert not r.ok and not client.claims


class _MatchingProvider:
    """Verifies a deposit ONLY against the reservation whose pinned params it actually matches — unlike
    _FakeProvider, which returns the same tx for every slot. Models the real per-hub matcher, so a
    first-match scan that guesses the wrong hub is exposed."""

    def __init__(self, amount, recipient):
        self._amount = amount
        self._recipient = recipient

    def verify_transaction(self, *, tx_hash, expected_recipient, expected_amount, block_hint, expected_sender):
        if int(expected_amount) != self._amount or expected_recipient != self._recipient:
            return None
        return _tx(confirmed=False, block_time=None)

    @property
    def chain_def(self):
        return SimpleNamespace(replay_grace_secs=0)

    @property
    def chain(self):
        return SimpleNamespace(normalize_address=lambda a: a)


def test_confirm_matches_the_right_hub_when_both_slots_are_live_unclaimed():
    # V-1 dual-live: a miner holds a live-unclaimed SOL reservation AND a live-unclaimed TAO reservation
    # at once (v3.1 simultaneous swaps). A TAO deposit must be verified against the TAO slot — the old
    # first-match scan checked SOL first, failed the content match, and stranded the deposit (or an
    # attacker sitting on the SOL slot could shadow every TAO confirm on the miner).
    sol = _confirm_reservation(from_amount=100_000, miner_from_addr='minerBTC_sol')
    tao = _near_expiry(20, from_amount=777, miner_from_addr='minerBTC_tao')
    client = _ConfirmClient({'sol': sol, 'tao': tao})
    provider = _MatchingProvider(amount=777, recipient='minerBTC_tao')  # the real deposit is the TAO one
    validator = SimpleNamespace(solana_client=client, axon_assets={'btc': provider}, axon_lock=threading.RLock())
    r = confirm_deposit(validator, HOTKEY, 'taoDeposit')
    assert r.ok and client.claims
    assert client.claims[0][3] == 'tao'  # claimed against the hub the deposit matched, not the first live slot
    assert client.extend_backings == ['tao']


# --- scan_deposit: the deposit watcher's hash-finder (confirm_deposit stays the verifier) ---


class _ScanProvider:
    def __init__(self, tx_hash=None):
        self.tx_hash = tx_hash
        self.calls = []

    def find_recent_outgoing(self, from_addr, to_addr, amount):
        self.calls.append((from_addr, to_addr, amount))
        return self.tx_hash


def _scan_reservation(reserved_until, claimed=False):
    return SimpleNamespace(
        reserved_until=reserved_until,
        claimed_swap_key=(b'\x09' * 32) if claimed else b'\x00' * 32,
        user='userSOLpk',
        from_chain='btc',
        to_chain='sol',
        from_amount=10_000,
        to_amount=47_000_000,
        miner_from_addr='tb1qminer',
        from_addr='tb1quser',
    )


def _scan_validator(tmp_path, reservation, provider):
    from allways.validator.reserve_engine import scan_deposit

    client = StatusClient(reservation=reservation)
    validator, store = _status_validator(tmp_path, client)
    validator.axon_assets = {'btc': provider} if provider else {}
    return scan_deposit, validator, store


def test_scan_deposit_finds_hash_for_live_unclaimed_reservation(tmp_path):
    provider = _ScanProvider('depositTx')
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(FUTURE), provider)
    assert scan_deposit(validator, HOTKEY) == 'depositTx'
    # Scans against the PINNED reservation triple — declared sender, miner deposit addr, exact amount.
    assert provider.calls == [('tb1quser', 'tb1qminer', 10_000)]
    store.close()


def test_scan_deposit_none_when_provider_cannot_scan(tmp_path):
    # A provider without find_recent_outgoing (hypothetical new chain) degrades to manual/wallet paths.
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(FUTURE), object())
    validator.axon_assets = {'btc': object()}
    assert scan_deposit(validator, HOTKEY) is None
    store.close()


def test_scan_deposit_none_when_reservation_expired_claimed_or_absent(tmp_path):
    import time as _time

    provider = _ScanProvider('depositTx')
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(int(_time.time()) - 5), provider)
    assert scan_deposit(validator, HOTKEY) is None  # expired — no late auto-claims, ever
    validator.solana_client._reservation = _scan_reservation(FUTURE, claimed=True)
    assert scan_deposit(validator, HOTKEY) is None  # already claimed
    validator.solana_client._reservation = None
    assert scan_deposit(validator, HOTKEY) is None  # nothing reserved
    assert provider.calls == []
    store.close()


def test_scan_deposit_finds_hash_in_tao_slot_when_sol_empty(tmp_path):
    # V-1: the scanner must find the live unclaimed TAO reservation, not read the empty SOL slot.
    provider = _ScanProvider('depositTx')
    client = StatusClient(reservation={'sol': None, 'tao': _scan_reservation(FUTURE)})
    validator, store = _status_validator(tmp_path, client)
    validator.axon_assets = {'btc': provider}
    from allways.validator.reserve_engine import scan_deposit

    assert scan_deposit(validator, HOTKEY) == 'depositTx'
    store.close()


def test_scan_deposit_scans_every_live_hub_not_just_the_first(tmp_path):
    # V-1 dual-live: SOL slot (btc, nothing to find) + TAO slot (eth, the real deposit). The scanner must
    # try every live hub — a first-match scan would stop at the SOL slot and miss the deposit entirely.
    from allways.validator.reserve_engine import scan_deposit

    sol = SimpleNamespace(
        reserved_until=FUTURE,
        claimed_swap_key=b'\x00' * 32,
        from_chain='btc',
        from_amount=10_000,
        miner_from_addr='tb1qminer',
        from_addr='tb1quser',
    )
    tao = SimpleNamespace(
        reserved_until=FUTURE,
        claimed_swap_key=b'\x00' * 32,
        from_chain='eth',
        from_amount=5,
        miner_from_addr='0xminer',
        from_addr='0xuser',
    )
    client = StatusClient(reservation={'sol': sol, 'tao': tao})
    validator, store = _status_validator(tmp_path, client)
    validator.axon_assets = {'btc': _ScanProvider(None), 'eth': _ScanProvider('ethDepositTx')}
    assert scan_deposit(validator, HOTKEY) == 'ethDepositTx'
    store.close()


def test_swap_status_reads_tao_slot_when_sol_empty(tmp_path):
    # V-1: status must surface a live TAO-hub reservation, not report 'none' off the empty SOL slot.
    from allways.validator.reserve_engine import swap_status

    client = StatusClient(reservation={'sol': None, 'tao': _unclaimed_reservation(FUTURE)})
    validator, store = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'reserved'
    store.close()


def test_status_by_key_detail_carries_leg_hashes(tmp_path):
    from allways.validator.reserve_engine import swap_status

    client = StatusClient(swap=_live_swap('Fulfilled'), reservation=SimpleNamespace(reserved_until=0))
    validator, store = _status_validator(tmp_path, client)
    s = swap_status(validator, HOTKEY, (b'\x05' * 32).hex())
    assert s.detail['from_tx_hash'] == 'srcTxHash' and s.detail['to_tx_hash'] == ''
    store.close()


def test_closed_pda_by_key_serves_delivery_hash_from_fulfillment_index(tmp_path):
    # The Swap PDA (and its to_tx_hash) is gone at terminal — the receipt's delivery link must
    # survive via the SwapFulfilled ingest, served on the closed-PDA by-key path.
    from allways.validator.reserve_engine import swap_status

    key = b'\x08' * 32
    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    store.record_swap_outcome(key.hex(), 'completed', 100)
    store.record_swap_fulfillment(key.hex(), 'destTx', 90)
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'completed' and s.detail == {'to_tx_hash': 'destTx'}
    store.close()


# ── event-driven crank ───────────────────────────────────────────────────────


class _Loop:
    def __init__(self, lock, calls):
        self.lock, self.calls = lock, calls

    def resolve_pools_once(self, now):
        assert self.lock.locked()
        self.calls.append('resolve')
        return []


class _FakeFeed:
    """Stands in for ProgramEventFeed: records handlers, lets a test push decoded events."""

    def __init__(self):
        self.handlers = {}

    def on(self, name, handler):
        self.handlers[name] = handler

    def push(self, name, **fields):
        self.handlers[name](name, SimpleNamespace(**fields))


def _wait(cond, secs=2.0):
    deadline = time.time() + secs
    while not cond() and time.time() < deadline:
        time.sleep(0.02)
    return cond()


def _crank_validator(calls):
    lock = threading.Lock()
    return SimpleNamespace(
        solana_swap_loop=_Loop(lock, calls),
        crank_lock=lock,
        solana_client=SimpleNamespace(rpc=SimpleNamespace(get_slot=lambda: 100)),
    )


def _crank_crank_validator(calls):
    lock = threading.Lock()
    return SimpleNamespace(
        solana_swap_loop=_Loop(lock, calls),
        crank_lock=lock,
        solana_client=SimpleNamespace(rpc=SimpleNamespace(get_slot=lambda: 100)),
    )


def test_feed_events_drive_arm_draw_finalize(monkeypatch):
    """One crank at close (arm), one at the seed slot (+ one retry), one on PoolResolved (finalize)."""
    import allways.validator.reserve_engine as engine

    monkeypatch.setattr(engine, 'CRANK_SKEW_SECS', 0)
    monkeypatch.setattr(engine, 'SLOT_SECS', 0.01)
    monkeypatch.setattr(engine, 'DRAW_SKEW_SECS', 0)
    monkeypatch.setattr(engine, 'DRAW_RETRY_SECS', 0.05)
    calls = []
    monkeypatch.setattr(engine, 'finalize_won_seats', lambda v, now: (calls.append('finalize'), [])[1])
    feed = _FakeFeed()
    validator = _crank_validator(calls)
    scheduler = engine.CrankScheduler(validator, feed)
    assert set(feed.handlers) == {'PoolDrawArmed', 'PoolResolved'}

    scheduler.schedule(MINER_PK, int(time.time()))
    assert _wait(lambda: calls.count('resolve') == 1)
    time.sleep(0.2)
    assert calls.count('resolve') == 1, 'no blind retries after the arm'
    feed.push('PoolDrawArmed', miner=MINER_PK, seed_slot=110, collateral_chain='sol')
    assert _wait(lambda: calls.count('resolve') == 3)  # draw + its one retry, ~10 slots later
    feed.push('PoolResolved', miner=MINER_PK, winner=MINER_PK, requests=1, collateral_chain='sol')
    assert _wait(lambda: calls.count('resolve') == 4)
    assert scheduler._pools == {}
    time.sleep(0.2)
    assert calls.count('resolve') == 4, calls


def test_feed_events_for_untracked_pools_are_ignored():
    import allways.validator.reserve_engine as engine

    feed = _FakeFeed()
    calls = []
    engine.CrankScheduler(_crank_validator(calls), feed)
    feed.push('PoolDrawArmed', miner=MINER_PK, seed_slot=110, collateral_chain='sol')
    feed.push('PoolResolved', miner=MINER_PK, winner=MINER_PK, requests=1, collateral_chain='sol')
    time.sleep(0.1)
    assert calls == []


def test_reserve_schedules_a_crank_at_the_pool_close():
    client = FakeClient()
    closes = int(time.time()) + 30
    client.get_pool = lambda miner, backing='sol': SimpleNamespace(
        miner=MINER_PK, opened_at=0, requests=[], closes_at=closes
    )
    validator = _validator(client)
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 1_000_000_000)
    assert result.ok
    assert validator.crank_scheduler.scheduled == [(MINER_PK, closes)]
