"""Unit tests for the shared on-behalf reserve op (kernel core behind both the axon + HTTP seam).

Mocks the solana_client; no chain. Asserts eligibility gating, SOL-numeraire amount derivation, and that
a joiner quotes against the PINNED pool rate (not the live quote) so it stays rate-consistent for D1.
"""

import time
from types import SimpleNamespace

import bittensor as bt
import pytest
from solders.keypair import Keypair as SolKeypair

from allways.constants import MAX_FAILED_SWAPS, RATE_PRECISION
from allways.validator_v2.actuator.planners import SwapReport
from allways.validator_v2.serving.requests import _best_offer, reserve_on_behalf
from tests.validator_v2.fakes import Ctx, record_fulfillment, record_outcome, serving_store

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
            failed_swaps=0,
            has_active_swap=has_active_swap,
            active_swap_backings=1 if has_active_swap else 0,
            collateral=collateral,
        )
        self.quote = SimpleNamespace(
            rate=_rate_fixed(quote_rate),
            from_chain='sol',
            to_chain='btc',
            collateral_chain='sol',
            miner_from_addr='minerSOLaddr',
            miner_to_addr='minerBTCdeliver',
        )
        self._pool = pool
        self.collateral = collateral
        self.calls = []

    def miner_for(self, hotkey):
        return MINER_PK if hotkey == HOTKEY else None

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
    return Ctx(client, providers=_ALL_ASSETS())


def _reserve(client, from_amount=1_000_000_000):
    # sol->btc: user sends 1 SOL, receives btc
    validator = _validator(client)
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', from_amount)
    return result, validator.store


def test_reserve_refuses_a_fill_the_in_flight_obligations_leave_uncovered():
    # 10**12 lamports gross, all but 1 SOL already obligated on the sol hub: a 1 SOL fill needs 1.1.
    client = FakeClient()
    client.miner_state.reserved_collateral = [10**12 - 10**9, 0]
    r, _ = _reserve(client)
    assert not r.ok and 'collateral too low' in r.reason


def test_a_new_request_clears_the_pool_rejection_verdict():
    from allways.validator_v2.serving import requests as reserve_engine

    key = (str(MINER_PK), 'sol', 'btc')
    reserve_engine._routed_rejections[key] = reserve_engine.RoutedRejection('Insufficient collateral', ['u'])
    try:
        r, _ = _reserve(FakeClient())
        assert r.ok and key not in reserve_engine._routed_rejections
    finally:
        reserve_engine._routed_rejections.clear()


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


def test_open_normalizes_the_source_address_at_intake():
    # V-C2: the queued source address is the string later hashed into the source-lock PDA —
    # canonicalize at intake so a checksummed EVM-style entry can't mint a divergent lock.
    client = FakeClient()
    validator = _validator(client)
    validator.providers = {
        **_ALL_ASSETS(),
        'sol': SimpleNamespace(
            transfers_enabled=lambda: True,
            send_blocker=lambda frm, to, amt: None,
            can_deliver_to=lambda address, amount: True,
            chain=SimpleNamespace(normalize_address=lambda a: a.lower(), is_valid_address=lambda address: True),
        ),
    }
    r = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, 'MiXeDcAsE', 'userBTCaddr', 1_000_000_000)
    assert r.ok
    queued = validator.store.pending_routed_requests(str(MINER_PK), 'sol', 'btc')
    assert queued[0]['user_from_addr'] == 'mixedcase'


def _ALL_ASSETS():
    return {'sol': _gate_asset(lambda addr, amt: True), 'btc': _gate_asset(lambda addr, amt: True)}


def _gate_asset(can_deliver, valid=lambda addr: True, enabled=True, blocker=None):
    """Duck-typed asset for the reserve deliverability gates (can_deliver_to + chain format check)."""
    return SimpleNamespace(
        transfers_enabled=lambda: enabled,
        send_blocker=lambda frm, to, amt: blocker,
        can_deliver_to=lambda addr, amt, from_address=None: can_deliver(addr, amt),
        chain=SimpleNamespace(
            is_valid_address=valid, normalize_address=lambda addr: addr, canonical_tx_hash=lambda h: h
        ),
    )


def test_reserve_does_not_predict_dest_deliverability():
    # Reserve-time deliverability is NOT a security boundary (a dest can pass here then revert later),
    # so a dest that WOULD refuse is no longer bounced here — the sound check is the delivery-time
    # reverted-tx proof (cancel_swap). Validity is still screened (see the malformed test below).
    client = FakeClient()
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'btc': _gate_asset(lambda addr, amt: False)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert 'rejects incoming transfers' not in (result.reason or '')


def test_missing_spoke_provider_rejects_before_bid():
    client = FakeClient()
    validator = _validator(client)
    validator.providers = {'sol': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'cannot verify btc' in result.reason
    assert client.calls == []


def test_leg_with_transfers_switched_off_rejects_before_any_bid():
    # A subnet owner's TransferToggle / SubtokenEnabled: no dest address can take it, so never open the swap.
    client = FakeClient()
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'btc': _gate_asset(lambda addr, amt: True, enabled=False)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'BTC transfers are switched off on-chain; SOL→BTC cannot settle' in result.reason
    assert client.calls == []


def test_malformed_destination_address_rejects_before_any_bid():
    # F4: address FORMAT is screened first — offline, and a malformed dest can never deliver.
    client = FakeClient()
    validator = _validator(client)
    validator.providers = {
        **_ALL_ASSETS(),
        'btc': _gate_asset(lambda addr, amt: True, valid=lambda addr: addr != 'not-an-addr'),
    }
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'not-an-addr', 10**9)
    assert not result.ok
    assert 'not a valid btc address' in result.reason
    assert client.calls == []


def test_malformed_miner_receive_address_rejects():
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.providers = {
        **_ALL_ASSETS(),
        'sol': _gate_asset(lambda addr, amt: True, valid=lambda addr: addr != 'minerSOLaddr'),
    }
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'no eligible quote' in result.reason
    assert client.calls == []


def test_source_that_cannot_go_out_as_one_transfer_rejects_before_any_bid():
    # A taker's alpha split across hotkeys: partial sends each miss the amount and strand with the miner.
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'sol': _gate_asset(lambda addr, amt: True, blocker='split across hotkeys')}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert (result.ok, result.reason, client.calls) == (False, 'split across hotkeys', [])


def test_user_to_addr_equal_miner_delivery_address_rejects():
    # V-H1: user_to_addr == miner_to_addr makes every delivery a from==to self-transfer the anti-wash
    # verifier rejects → the miner never confirms the leg and is force-slashed with no cancel escape.
    # Bounce it at reserve before any bid.
    client = FakeClient()
    client.quote.miner_to_addr = 'minerBTCdeliver'
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'btc': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'minerBTCdeliver', 10**9)
    assert not result.ok
    assert 'differ from the miner delivery address' in result.reason
    assert client.calls == []


def test_distinct_dest_addr_with_miner_to_addr_set_reserves():
    # The guard must not over-reject: a normal dest address different from the miner's still reserves.
    client = FakeClient()
    client.quote.miner_to_addr = 'minerBTCdeliver'
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'btc': _gate_asset(lambda addr, amt: True)}
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
    validator.providers = {**_ALL_ASSETS(), 'sol': _gate_asset(lambda addr, amt: addr != 'minerSOLaddr')}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert not result.ok
    assert 'miner receive address' in result.reason
    assert client.calls == []


def test_miner_receive_address_clean_still_bids():
    client = FakeClient()
    client.quote.miner_from_addr = 'minerSOLaddr'
    validator = _validator(client)
    validator.providers = {**_ALL_ASSETS(), 'sol': _gate_asset(lambda addr, amt: True)}
    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 10**9)
    assert result.ok
    assert client.calls == [('open_or_request', 'sol', 'btc', 'sol')]


def test_inactive_miner_rejects():
    r, store = _reserve(FakeClient(active=False))
    assert not r.ok and 'not active' in r.reason
    assert store.distinct_routed_pools() == []  # nothing queued on rejection


def test_struck_miner_rejects():
    # Same reading as scoring: a miner past the strike cap earns nothing, so routing never lands on it.
    client = FakeClient()
    client.miner_state.failed_swaps = MAX_FAILED_SWAPS + 1
    r, store = _reserve(client)
    assert not r.ok and 'no eligible quote' in r.reason
    assert store.distinct_routed_pools() == []


def test_struck_miner_cannot_take_joiners_on_its_open_pool():
    pinned = SimpleNamespace(opened_at=1, closes_at=FUTURE, from_chain='sol', to_chain='btc', rate=_rate_fixed(0.0021))
    client = FakeClient(pool=pinned)
    client.miner_state.failed_swaps = MAX_FAILED_SWAPS + 1
    r, _ = _reserve(client)
    assert not r.ok and 'no longer eligible' in r.reason
    assert client.calls == []


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
    assert not r.ok and 'no eligible quote' in r.reason


@pytest.mark.parametrize('bad_field', ['miner_from_addr', 'miner_to_addr'])
def test_best_offer_skips_the_best_rate_with_a_bad_address(bad_field):
    client = FakeClient()
    best = client.quote
    best.to_chain = 'tao'
    best.miner_from_addr = 'from-best'
    best.miner_to_addr = 'to-best'
    next_best = SimpleNamespace(
        rate=_rate_fixed(0.002),
        from_chain='sol',
        to_chain='tao',
        collateral_chain='tao',
        miner_from_addr='from-next',
        miner_to_addr='to-next',
    )
    setattr(best, bad_field, 'bad')
    client.get_quotes_for_direction = lambda *args: [best, next_best]
    client.get_bond_attestation = lambda *args: SimpleNamespace(locked=True, effective_balance=10**12)
    provider = SimpleNamespace(chain=SimpleNamespace(is_valid_address=lambda address: address != 'bad'))

    offer, reason = _best_offer(
        client,
        MINER_PK,
        client.miner_state,
        'sol',
        'tao',
        10**9,
        {'sol': (0, 0), 'tao': (0, 0)},
        {'sol': provider, 'tao': provider},
    )

    assert reason == '' and offer == (next_best, 'tao')


def test_joining_pool_rejects_an_unusable_pinned_quote():
    pool = SimpleNamespace(opened_at=1, closes_at=FUTURE, from_chain='sol', to_chain='btc', rate=_rate_fixed(0.0021))
    client = FakeClient(pool=pool)
    client.quote.miner_to_addr = 'bad'
    validator = _validator(client)
    validator.providers = {
        **_ALL_ASSETS(),
        'btc': _gate_asset(lambda addr, amt: True, valid=lambda addr: addr != 'bad'),
    }

    result = reserve_on_behalf(validator, HOTKEY, 'sol', 'btc', USER_PK, str(USER_PK), 'userBTCaddr', 1_000_000_000)

    assert not result.ok and result.reason == 'the pooled quote has an address its chain rejects'


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
    store = serving_store(tmp_path / 'state.db')
    return Ctx(None, store=store), store


def test_closed_pda_with_recorded_slash_reports_timed_out(tmp_path):
    from allways.validator_v2.serving.requests import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x01' * 32
    record_outcome(store, key.hex(), 'timed_out', 100)
    assert _swap_stage(validator, None, key) == 'timed_out'


def test_closed_pda_with_recorded_completion_reports_completed(tmp_path):
    from allways.validator_v2.serving.requests import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x02' * 32
    record_outcome(store, key.hex(), 'completed', 100)
    assert _swap_stage(validator, None, key) == 'completed'


def test_closed_pda_with_unrecorded_outcome_reports_fulfilled(tmp_path):
    # Ingest lag: another validator's quorum closed the PDA but this validator hasn't
    # ingested the terminal event yet. The fallback must be NON-terminal so the consumer
    # keeps polling and picks up the real outcome next ingest — a 'completed' guess for a
    # fresh slash would resurrect the original bug through a one-forward-step window.
    from allways.validator_v2.serving.requests import _swap_stage

    validator, store = _stage_validator(tmp_path)
    assert _swap_stage(validator, None, b'\x03' * 32) == 'fulfilled'


def test_live_pda_status_maps_by_variant_name(tmp_path):
    # A still-open PDA never consults the outcome index — the borsh status variant wins.
    from allways.validator_v2.serving.requests import _swap_stage

    validator, store = _stage_validator(tmp_path)
    key = b'\x04' * 32
    record_outcome(store, key.hex(), 'completed', 100)  # must be ignored while the PDA is live
    for variant, stage in [('Active', 'active'), ('Fulfilled', 'fulfilled'), ('TimedOut', 'timed_out')]:
        swap = SimpleNamespace(status=type(variant, (), {})())
        assert _swap_stage(validator, swap, key) == stage


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

    def miner_for(self, hotkey):
        return MINER_PK if hotkey == HOTKEY else None


def _live_swap(variant: str):
    return SimpleNamespace(
        status=type(variant, (), {})(),
        user='userSOLpk',
        from_chain='sol',
        to_chain='btc',
        from_amount=1_000_000_000,
        to_amount=210_000,
        miner_from_addr='minerSOLaddr',
        miner_to_addr='minerBTCaddr',
        user_from_addr='userSOLaddr',
        user_to_addr='userBTCaddr',
        rate='0.0021',
        from_tx_hash='srcTxHash',
        to_tx_hash='',
    )


def _status_validator(tmp_path, client):
    validator, store = _stage_validator(tmp_path)
    validator.client = client
    validator.providers = {**_ALL_ASSETS(), 'btc': _gate_asset(lambda addr, amt: True)}
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
        from_addr='tb1quser',
        collateral_chain='sol',
    )


def test_expired_unclaimed_reservation_reports_none(tmp_path):
    """A dead (expired, never-claimed) reservation must not surface as 'reserved' with its stale
    user — the offering's win-detection would read it as another user holding the miner."""
    import time as _time

    from allways.validator_v2.serving.requests import swap_status

    client = StatusClient(reservation=_unclaimed_reservation(int(_time.time()) - 5))
    validator, _ = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'none'


def test_pool_rejection_reports_rejected_with_its_users_until_a_seat_is_live(tmp_path):
    from allways.validator_v2.serving import requests as reserve_engine
    from allways.validator_v2.serving.requests import swap_status

    key = (str(MINER_PK), 'btc', 'sol')
    reserve_engine._routed_rejections[key] = reserve_engine.RoutedRejection('Insufficient collateral', ['userA'])
    try:
        validator, _ = _status_validator(tmp_path, StatusClient(reservation=None))
        s = swap_status(validator, HOTKEY, from_chain='btc', to_chain='sol')
        assert s.stage == 'rejected' and s.detail == {'reason': 'Insufficient collateral', 'users': ['userA']}
        assert swap_status(validator, HOTKEY, from_chain='sol', to_chain='btc').stage == 'none'  # other pool
        # A live reservation is a newer round — it speaks, the stale verdict does not.
        validator.client = StatusClient(reservation=_unclaimed_reservation(FUTURE))
        assert swap_status(validator, HOTKEY, from_chain='btc', to_chain='sol').stage == 'reserved'
    finally:
        reserve_engine._routed_rejections.clear()


def test_live_unclaimed_reservation_reports_reserved(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    client = StatusClient(reservation=_unclaimed_reservation(FUTURE))
    validator, _ = _status_validator(tmp_path, client)
    s = swap_status(validator, HOTKEY)
    assert s.stage == 'reserved' and s.user == 'staleUserSOLpk'
    assert s.detail['user_from_addr'] == 'tb1quser'


def _declared_reservation(reserved_until: int):
    """sn7→btc with TAO behind the sn7 leg: a declared backing, whose verdict ingest pins at fill."""
    return SimpleNamespace(
        reserved_until=reserved_until,
        claimed_swap_key=b'\x00' * 32,
        user='userSOLpk',
        from_chain='sn7',
        to_chain='btc',
        from_amount=5_000_000_000,
        to_amount=5_000_000,
        miner_from_addr='minerColdkey',
        from_addr='userColdkey',
        collateral_chain='tao',
        collateral_amount=10**9,
        created_at=1_200,
    )


def test_reserved_seat_serves_the_pinned_collateral_verdict(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    validator, store = _status_validator(tmp_path, StatusClient(reservation=_declared_reservation(FUTURE)))
    assert swap_status(validator, HOTKEY).detail['collateral_ok'] is None  # pending: the consumer must hold
    store.record_collateral_verdict(str(MINER_PK), 'tao', 1_200, 10**9, False)
    assert swap_status(validator, HOTKEY).detail['collateral_ok'] is False
    store.record_collateral_verdict(str(MINER_PK), 'tao', 1_200, 10**9, True)
    assert swap_status(validator, HOTKEY).detail['collateral_ok'] is True


def test_exact_backing_leg_carries_no_collateral_verdict(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    exact = _declared_reservation(FUTURE)
    exact.from_chain = 'tao'  # the backing IS a leg: bound on-chain, nothing to verify
    validator, store = _status_validator(tmp_path, StatusClient(reservation=exact))
    store.record_collateral_verdict(str(MINER_PK), 'tao', 1_200, 10**9, False)
    assert 'collateral_ok' not in swap_status(validator, HOTKEY).detail


def test_claimed_swap_surfaces_the_loops_collateral_reject_reason(tmp_path):
    from allways.validator_v2.actuator.swaps import COLLATERAL_REJECT_REASON
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x16' * 32
    swap = _live_swap('PendingAttestation')
    validator, store = _status_validator(tmp_path, StatusClient(swap=swap))
    assert 'reject_reason' not in swap_status(validator, HOTKEY, key.hex()).detail  # loop not there yet
    validator._report[key.hex()] = SwapReport('PendingAttestation', 'reject', COLLATERAL_REJECT_REASON, {}, 'sol', 0)
    assert swap_status(validator, HOTKEY, key.hex()).detail['reject_reason'] == COLLATERAL_REJECT_REASON
    swap.status = type('Active', (), {})()
    assert 'reject_reason' not in swap_status(validator, HOTKEY, key.hex()).detail


def test_initiated_swap_resolves_by_key_after_reservation_consumed(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x05' * 32
    consumed = SimpleNamespace(reserved_until=0)  # vote_initiate zeroed it at quorum
    client = StatusClient(swap=_live_swap('Active'), reservation=consumed)
    validator, store = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'none'  # reservation path is blind post-attestation
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'active' and s.swap_key == key.hex() and s.reserved_until == 0
    assert s.detail['from_chain'] == 'sol' and s.detail['to_amount'] == 210_000
    assert s.detail['user_from_addr'] == 'userSOLaddr'
    assert client.swap_keys_queried == [key]


def test_reject_reason_only_surfaces_for_invalid_pending_attestation(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x15' * 32
    reservation = SimpleNamespace(
        reserved_until=FUTURE,
        claimed_swap_key=key,
        user='userSOLpk',
        from_chain='sol',
        to_chain='btc',
        from_amount=1_000_000_000,
        to_amount=210_000,
        miner_from_addr='minerSOLaddr',
        from_addr='userSOLaddr',
    )
    swap = _live_swap('PendingAttestation')
    validator, store = _status_validator(tmp_path, StatusClient(swap=swap, reservation=reservation))

    validator.providers['btc'].chain.is_valid_address = lambda address: False
    reason = 'dest address invalid — refusing to attest'
    assert swap_status(validator, HOTKEY).detail['reject_reason'] == reason
    assert swap_status(validator, HOTKEY, key.hex()).detail['reject_reason'] == reason

    validator.providers['btc'].chain.is_valid_address = lambda address: True
    assert 'reject_reason' not in swap_status(validator, HOTKEY).detail

    swap.status = type('Active', (), {})()
    validator.providers['btc'].chain.is_valid_address = lambda address: False
    assert 'reject_reason' not in swap_status(validator, HOTKEY, key.hex()).detail


def test_closed_pda_by_key_with_recorded_slash_reports_timed_out(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x06' * 32
    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    record_outcome(store, key.hex(), 'timed_out', 100)
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'timed_out' and s.swap_key == key.hex() and s.detail == {}


def test_closed_pda_by_key_carries_refund_facts(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x16' * 32
    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    record_outcome(store, key.hex(), 'timed_out', 100, refund=('sol', 123, 'refundTx'))
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'timed_out'
    assert s.detail == {'refund_chain': 'sol', 'refund_amount': 123, 'refund_tx_hash': 'refundTx'}


def test_closed_pda_by_key_with_unrecorded_outcome_reports_fulfilled(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    assert swap_status(validator, HOTKEY, (b'\x07' * 32).hex()).stage == 'fulfilled'


def test_malformed_swap_key_raises_value_error(tmp_path):
    # Non-hex or wrong-length keys must raise ValueError (seam maps it to a 400).
    from allways.validator_v2.serving.requests import swap_status

    validator, store = _status_validator(tmp_path, StatusClient())
    for bad in ('zz', 'abcd'):
        try:
            swap_status(validator, HOTKEY, bad)
            assert False, f'expected ValueError for swap_key={bad!r}'
        except ValueError:
            pass


# ── confirm_deposit: deferred-confirmation intake. Accepts a content-valid deposit even before it fully
# confirms (the crank defers voting until confirmations accrue); fast-fails without a claim on absent/mismatch
# (None) or a stale MINED deposit, so the short reservation TTL frees the miner.
import allways.validator_v2.serving.requests as rc  # noqa: E402
from allways.assets.asset import ProviderUnreachableError, TransactionInfo  # noqa: E402
from allways.validator_v2.serving.requests import confirm_deposit  # noqa: E402

CONFIRM_CREATED_AT = 1000


def _store():
    return serving_store()


class _ConfirmClient(FakeClient):
    def __init__(self, reservation, **kw):
        super().__init__(**kw)
        self._reservation = reservation
        self.claims = []
        self.extensions = []
        self.extend_backings = []
        self.extend_raises = False
        self.swaps = {}

    def get_swap(self, swap_key):
        return self.swaps.get(bytes(swap_key))

    def get_reservation(self, miner, backing='sol'):
        # A dict models a dual-purse miner's per-hub slots (v3.1); a bare value is the same on every hub.
        if isinstance(self._reservation, dict):
            return self._reservation.get(backing)
        return self._reservation

    def submit_swap_claim(self, miner, swap_key, from_tx_hash, from_tx_block, backing='sol'):
        if bytes(swap_key) in self.swaps:
            raise RuntimeError('swap PDA already in use')
        self.claims.append((swap_key, from_tx_hash, from_tx_block, backing))
        self.swaps[bytes(swap_key)] = SimpleNamespace(from_tx_hash=from_tx_hash)
        return 'claimsig'

    def extend_reservation(
        self, miner, target_at, backing='sol', *, from_chain=None, from_addr=None, miner_from_addr=None
    ):
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
        return SimpleNamespace(normalize_address=self._normalize, canonical_tx_hash=lambda h: h)


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
    validator = Ctx(client, providers={'btc': provider})
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
    validator = Ctx(client, providers={'btc': provider})
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and len(client.claims) == 1
    assert client.claims[0][3] == 'tao'  # the canonical slot's backing claimed, not the variant's


def test_confirm_prefers_the_oldest_slot_on_a_form_tie():
    # Dual-backing (V-I1): the same source legitimately backs two hubs. Deterministic pick = oldest.
    younger = _confirm_reservation(created_at=900)
    older = _confirm_reservation(created_at=500)
    client = _ConfirmClient({'sol': younger, 'tao': older})
    provider = _FakeProvider(_tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6))
    validator = Ctx(client, providers={'btc': provider})
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
    validator = Ctx(client, providers=providers)
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

    validator = Ctx(client, providers={'btc': _SlowProvider(_tx(confirmed=False, block_time=None))})
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
    validator = Ctx(client, providers={'btc': provider})
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims


# ── V-1: the deposit-confirm seam must scan per-hub slots, not the SOL default ──
# v3.1 seeds a reservation per (miner, backing). A SOL-defaulted get_reservation reads the empty SOL
# slot for a TAO-backed deposit, so no claim lands and the deposit is silently lost.
def test_confirm_uses_tao_slot_when_sol_slot_empty():
    resv = _near_expiry(20)  # near expiry so the runway extend also travels on the right hub
    client = _ConfirmClient({'sol': None, 'tao': resv})
    provider = _FakeProvider(_tx(confirmed=False, block_time=None))
    validator = Ctx(client, providers={'btc': provider})
    r = confirm_deposit(validator, HOTKEY, 'srctxhash')
    assert r.ok and client.claims
    assert client.claims[0][3] == 'tao'  # claim submitted against the TAO hub, not the empty SOL slot
    assert client.extend_backings == ['tao']  # runway bought on the same hub


def test_confirm_no_live_unclaimed_slot_on_any_hub_rejects():
    # Empty SOL + expired TAO: nothing live+unclaimed anywhere → no claim, TTL frees the miner.
    client = _ConfirmClient({'sol': None, 'tao': _confirm_reservation(reserved_until=1)})
    provider = _FakeProvider(_tx(confirmed=False, block_time=None))
    validator = Ctx(client, providers={'btc': provider})
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
        return SimpleNamespace(normalize_address=lambda a: a, canonical_tx_hash=lambda h: h)


def test_confirm_matches_the_right_hub_when_both_slots_are_live_unclaimed():
    # V-1 dual-live: a miner holds a live-unclaimed SOL reservation AND a live-unclaimed TAO reservation
    # at once (v3.1 simultaneous swaps). A TAO deposit must be verified against the TAO slot — the old
    # first-match scan checked SOL first, failed the content match, and stranded the deposit (or an
    # attacker sitting on the SOL slot could shadow every TAO confirm on the miner).
    sol = _confirm_reservation(from_amount=100_000, miner_from_addr='minerBTC_sol')
    tao = _near_expiry(20, from_amount=777, miner_from_addr='minerBTC_tao')
    client = _ConfirmClient({'sol': sol, 'tao': tao})
    provider = _MatchingProvider(amount=777, recipient='minerBTC_tao')  # the real deposit is the TAO one
    validator = Ctx(client, providers={'btc': provider})
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
    from allways.validator_v2.serving.requests import scan_deposit

    client = StatusClient(reservation=reservation)
    validator, store = _status_validator(tmp_path, client)
    validator.providers = {**_ALL_ASSETS(), 'btc': provider} if provider else {}
    return scan_deposit, validator, store


def test_scan_deposit_finds_hash_for_live_unclaimed_reservation(tmp_path):
    provider = _ScanProvider('depositTx')
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(FUTURE), provider)
    assert scan_deposit(validator, HOTKEY) == 'depositTx'
    # Scans against the PINNED reservation triple — declared sender, miner deposit addr, exact amount.
    assert provider.calls == [('tb1quser', 'tb1qminer', 10_000)]


def test_scan_deposit_none_when_provider_cannot_scan(tmp_path):
    # A provider without find_recent_outgoing (hypothetical new chain) degrades to manual/wallet paths.
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(FUTURE), object())
    validator.providers = {**_ALL_ASSETS(), 'btc': object()}
    assert scan_deposit(validator, HOTKEY) is None


def test_scan_deposit_none_when_reservation_expired_claimed_or_absent(tmp_path):
    import time as _time

    provider = _ScanProvider('depositTx')
    scan_deposit, validator, store = _scan_validator(tmp_path, _scan_reservation(int(_time.time()) - 5), provider)
    assert scan_deposit(validator, HOTKEY) is None  # expired — no late auto-claims, ever
    validator.client._reservation = _scan_reservation(FUTURE, claimed=True)
    assert scan_deposit(validator, HOTKEY) is None  # already claimed
    validator.client._reservation = None
    assert scan_deposit(validator, HOTKEY) is None  # nothing reserved
    assert provider.calls == []


def test_scan_deposit_finds_hash_in_tao_slot_when_sol_empty(tmp_path):
    # V-1: the scanner must find the live unclaimed TAO reservation, not read the empty SOL slot.
    provider = _ScanProvider('depositTx')
    client = StatusClient(reservation={'sol': None, 'tao': _scan_reservation(FUTURE)})
    validator, store = _status_validator(tmp_path, client)
    validator.providers = {**_ALL_ASSETS(), 'btc': provider}
    from allways.validator_v2.serving.requests import scan_deposit

    assert scan_deposit(validator, HOTKEY) == 'depositTx'


def test_scan_deposit_scans_every_live_hub_not_just_the_first(tmp_path):
    # V-1 dual-live: SOL slot (btc, nothing to find) + TAO slot (eth, the real deposit). The scanner must
    # try every live hub — a first-match scan would stop at the SOL slot and miss the deposit entirely.
    from allways.validator_v2.serving.requests import scan_deposit

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
    validator.providers = {**_ALL_ASSETS(), 'btc': _ScanProvider(None), 'eth': _ScanProvider('ethDepositTx')}
    assert scan_deposit(validator, HOTKEY) == 'ethDepositTx'


def test_swap_status_reads_tao_slot_when_sol_empty(tmp_path):
    # V-1: status must surface a live TAO-hub reservation, not report 'none' off the empty SOL slot.
    from allways.validator_v2.serving.requests import swap_status

    client = StatusClient(reservation={'sol': None, 'tao': _unclaimed_reservation(FUTURE)})
    validator, store = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).stage == 'reserved'


def test_swap_status_by_pair_reads_that_pairs_slot_not_the_freshest(tmp_path):
    # The offering tracks ONE seat; answering with the miner's other hub made it refund a won seat
    # ("another user holds this miner") or adopt a stranger's reservation under the same pubkey.
    from allways.validator_v2.serving.requests import swap_status

    sol = _unclaimed_reservation(FUTURE + 500)  # fresher, btc->sol, a stranger
    tao = SimpleNamespace(
        **{
            **vars(_unclaimed_reservation(FUTURE)),
            'user': 'ourTaoUser',
            'from_chain': 'tao',
            'to_chain': 'btc',
            'collateral_chain': 'tao',
        }
    )
    client = StatusClient(reservation={'sol': sol, 'tao': tao})
    validator, store = _status_validator(tmp_path, client)
    assert swap_status(validator, HOTKEY).user == 'staleUserSOLpk'  # no pair: freshest, as before
    assert swap_status(validator, HOTKEY, from_chain='tao', to_chain='btc').user == 'ourTaoUser'
    assert swap_status(validator, HOTKEY, from_chain='eth', to_chain='sol').stage == 'none'


def test_status_by_key_detail_carries_leg_hashes(tmp_path):
    from allways.validator_v2.serving.requests import swap_status

    client = StatusClient(swap=_live_swap('Fulfilled'), reservation=SimpleNamespace(reserved_until=0))
    validator, store = _status_validator(tmp_path, client)
    s = swap_status(validator, HOTKEY, (b'\x05' * 32).hex())
    assert s.detail['from_tx_hash'] == 'srcTxHash' and s.detail['to_tx_hash'] == ''


def test_closed_pda_by_key_serves_delivery_hash_from_fulfillment_index(tmp_path):
    # The Swap PDA (and its to_tx_hash) is gone at terminal — the receipt's delivery link must
    # survive via the SwapFulfilled ingest, served on the closed-PDA by-key path.
    from allways.validator_v2.serving.requests import swap_status

    key = b'\x08' * 32
    validator, store = _status_validator(tmp_path, StatusClient(swap=None))
    record_outcome(store, key.hex(), 'completed', 100)
    record_fulfillment(store, key.hex(), 'destTx', 90)
    s = swap_status(validator, HOTKEY, key.hex())
    assert s.stage == 'completed' and s.detail == {'to_tx_hash': 'destTx'}


def test_confirm_resolves_an_extrinsic_id_through_the_source_chain_and_claims_the_inner_hash():
    # The app relays btcli's `<block>-<idx>` unchanged; the seam resolves, claims and echoes the inner hash.
    from allways.assets.tao import Tao

    INNER_HASH = '0x' + 'ab' * 32

    class _AlphaProvider(_FakeProvider):
        chain = Tao(SimpleNamespace())
        verified = None

        def locate_transfer(self, block_num, ext_idx):
            if (block_num, ext_idx) != (8083554, 7):
                raise ValueError('no creditable transfer')
            return INNER_HASH, block_num

        def verify_transaction(self, **kw):
            self.verified = kw
            return self._tx

    client = _ConfirmClient(_confirm_reservation(from_chain='sn19'))
    provider = _AlphaProvider(_tx(confirmed=True, block_time=CONFIRM_CREATED_AT + 5, confirmations=6))
    validator = Ctx(client, providers={'sn19': provider})
    r = confirm_deposit(validator, HOTKEY, '8083554-7')
    assert r.ok and r.from_tx_hash == INNER_HASH
    assert (provider.verified['tx_hash'], provider.verified['block_hint']) == (INNER_HASH, 8083554)
    assert client.claims[0][1] == INNER_HASH and client.claims[0][0] == rc.swap_key_from_tx_hash(INNER_HASH)
    assert not confirm_deposit(validator, HOTKEY, '8083554-6').ok  # names no transfer → not visible, no claim
