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
