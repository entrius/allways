"""B3.4 — SolanaEventIndex: persist decoded program events into the state store and expose the crown's
per-instant read interface over them, on the unix-``blockTime`` axis, attributing each Solana pubkey to its
bound hotkey at write time."""

from pathlib import Path

from allways.classes import MinerActivity
from allways.constants import RATE_PRECISION
from allways.solana.events import EventRecord
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import replay_crown_time_window
from allways.validator.state_store import ValidatorStateStore

# pubkey str -> hotkey ss58 (what binding.build_attribution returns).
ATTR = {'pk_a': 'hk_a', 'pk_b': 'hk_b'}
RESERVATION_TTL = 300


DEFAULT_SWAP_KEY = b'\x2a' * 32


def rec(name: str, *, miner: str = 'pk_a', block_time, slot: int = 0, **fields) -> EventRecord:
    # swap_key defaults so swap-lifecycle recs match the on-chain layouts (Hash32); harmless extra
    # field on events that don't carry one.
    fields = {'miner': miner, 'swap_key': DEFAULT_SWAP_KEY, **fields}
    return EventRecord(name=name, fields=fields, slot=slot, block_time=block_time, signature=f'sig{slot}')


def make_store(tmp_path: Path) -> ValidatorStateStore:
    return ValidatorStateStore(db_path=tmp_path / 'state.db')


def make_index(store: ValidatorStateStore, ttl: int = RESERVATION_TTL) -> SolanaEventIndex:
    """Index wired with a constant reservation TTL (the config-cache getter in
    production) so PoolResolved can synthesize RESERVE_EXPIRE."""
    return SolanaEventIndex(store, reservation_ttl_fn=lambda: ttl)


class TestIngestActive:
    def test_activated_deactivated_build_the_active_series(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        n = idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=100, at=100),
                rec('MinerDeactivated', miner='pk_a', block_time=500, at=500),
            ],
            ATTR,
        )
        assert n == 2
        assert idx.get_active_miners_at(50) == set()
        assert idx.get_active_miners_at(100) == {'hk_a'}
        assert idx.get_active_miners_at(499) == {'hk_a'}
        assert idx.get_active_miners_at(500) == set()
        # Range is half-open on the left: an event at exactly start is excluded.
        assert [e['block'] for e in idx.get_active_events_in_range(100, 500)] == [500]
        store.close()


class TestIngestActivity:
    def test_reservation_then_swap_lifecycle_drives_activity(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_router', user='pk_user', requests=1),
                rec('SwapInitiated', miner='pk_a', block_time=250),
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=400,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100_000,
                    to_amount=500_000_000,
                ),
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(100) == {}  # AVAILABLE before the reservation
        assert idx.get_activity_state_at(200) == {'hk_a': MinerActivity.RESERVED}
        assert idx.get_activity_state_at(250) == {'hk_a': MinerActivity.FULFILLING}
        assert idx.get_activity_state_at(400) == {}  # completed → AVAILABLE
        store.close()

    def test_pool_resolved_synthesizes_reserve_expire(self, tmp_path: Path):
        """A reservation with no swap forfeits the crown until block_time + ttl,
        then RESERVE_EXPIRE returns the miner to AVAILABLE."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=300)
        idx.ingest(
            [rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_router', user='pk_user', requests=1)],
            ATTR,
        )
        assert idx.get_activity_state_at(200) == {'hk_a': MinerActivity.RESERVED}
        assert idx.get_activity_state_at(499) == {'hk_a': MinerActivity.RESERVED}
        assert idx.get_activity_state_at(500) == {}  # 200 + 300 ttl → AVAILABLE
        kinds = [e['kind'] for e in idx.get_activity_events_in_range(0, 1000)]
        assert kinds == [1, 3]  # RESERVE_START then synthetic RESERVE_EXPIRE
        store.close()

    def test_busy_miner_is_the_reserved_miner_not_the_router(self, tmp_path: Path):
        """PoolResolved.miner (not .winner, the router) is busy-gated."""
        store = make_store(tmp_path)
        idx = make_index(store)
        idx.ingest(
            [rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_b', user='pk_user', requests=1)],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': MinerActivity.RESERVED}  # not hk_b
        store.close()

    def test_pool_resolved_dropped_without_ttl_source(self, tmp_path: Path):
        """No TTL getter wired → PoolResolved is dropped (a reservation never
        opens without its matching expiry)."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)  # no reservation_ttl_fn
        n = idx.ingest(
            [rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_b', user='pk_user', requests=1)],
            ATTR,
        )
        assert n == 0
        assert idx.get_activity_state_at(250) == {}
        store.close()


class TestIngestCollateral:
    def test_posted_and_withdrawn_track_total(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('CollateralPosted', miner='pk_a', block_time=100, amount=100_000_000, total=100_000_000),
                rec('CollateralPosted', miner='pk_a', block_time=500, amount=150_000_000, total=250_000_000),
                rec('CollateralWithdrawn', miner='pk_a', block_time=800, amount=200_000_000, total=50_000_000),
            ],
            ATTR,
        )
        assert idx.get_miner_collaterals_at(50) == {}
        assert idx.get_miner_collaterals_at(100) == {'hk_a': 100_000_000}
        assert idx.get_miner_collaterals_at(499) == {'hk_a': 100_000_000}
        assert idx.get_miner_collaterals_at(500) == {'hk_a': 250_000_000}
        assert idx.get_miner_collaterals_at(800) == {'hk_a': 50_000_000}
        store.close()


class TestIngestRate:
    def test_quote_set_converts_fixed_point_to_display(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # On-chain rate is display × RATE_PRECISION; the index must divide it back.
        idx.ingest(
            [
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=100,
                    from_chain='btc',
                    to_chain='tao',
                    rate=326 * RATE_PRECISION,
                    liquidity=0,
                )
            ],
            ATTR,
        )
        latest = store.get_latest_rate_before('hk_a', 'btc', 'tao', 200)
        assert latest == (326.0, 100)
        store.close()

    def test_quote_removed_writes_zero_optout(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=100,
                    from_chain='btc',
                    to_chain='tao',
                    rate=200 * RATE_PRECISION,
                    liquidity=0,
                ),
                rec('QuoteRemoved', miner='pk_a', block_time=500, from_chain='btc', to_chain='tao'),
            ],
            ATTR,
        )
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', 999) == (0.0, 500)
        store.close()

    def test_chain_strings_lowercased(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=100,
                    from_chain='BTC',
                    to_chain='TAO',
                    rate=200 * RATE_PRECISION,
                    liquidity=0,
                )
            ],
            ATTR,
        )
        # Stored under the lowercased direction the crown queries by.
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', 999) == (200.0, 100)
        store.close()


class TestIngestClearingRate:
    def test_swap_completed_persists_clearing_rate_and_activity(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec('PoolResolved', miner='pk_a', block_time=150, winner='pk_router', user='pk_user', requests=1),
                rec('SwapInitiated', miner='pk_a', block_time=200),
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=400,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100_000,
                    to_amount=500_000_000,
                ),
            ],
            ATTR,
        )
        # Both effects fire: FULFILL_END returns AVAILABLE AND a clearing-rate sample lands.
        assert idx.get_activity_state_at(400) == {}
        rows = store.get_clearing_rates_in_range('btc', 'tao', 0, 1000)
        assert rows == [{'hotkey': 'hk_a', 'from_amount': 100_000, 'to_amount': 500_000_000, 'block': 400}]
        store.close()

    def test_u128_legs_survive_text_storage(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        big = (1 << 100) + 7  # well past signed-64 INTEGER
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=10,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=big,
                    to_amount=big - 1,
                )
            ],
            ATTR,
        )
        rows = store.get_clearing_rates_in_range('btc', 'tao', 0, 100)
        assert rows[0]['from_amount'] == big and rows[0]['to_amount'] == big - 1
        store.close()

    def test_chain_strings_lowercased(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=10,
                    from_chain='BTC',
                    to_chain='TAO',
                    from_amount=1,
                    to_amount=2,
                )
            ],
            ATTR,
        )
        assert store.get_clearing_rates_in_range('btc', 'tao', 0, 100)  # found under lowercased direction
        store.close()

    def test_unbound_and_unstamped_skip_clearing_rate(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_c',
                    block_time=10,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=1,
                    to_amount=2,
                ),  # unbound → dropped
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=None,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=1,
                    to_amount=2,
                ),  # unstamped tip → skipped
            ],
            ATTR,
        )
        assert store.get_clearing_rates_in_range('btc', 'tao', 0, 100) == []
        store.close()

    def test_prune_drops_old_samples(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=100,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=1,
                    to_amount=2,
                ),
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=900,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=3,
                    to_amount=4,
                ),
            ],
            ATTR,
        )
        store.prune_clearing_rates(500)
        blocks = [r['block'] for r in store.get_clearing_rates_in_range('btc', 'tao', 0, 1000)]
        assert blocks == [900]  # no anchor preservation — old sample is gone
        store.close()


class TestIngestSwapOutcomes:
    def test_swap_completed_records_completed_outcome(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=400,
                    swap_key=key,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100_000,
                    to_amount=500_000_000,
                )
            ],
            ATTR,
        )
        assert store.get_swap_outcome(key.hex()) == 'completed'
        store.close()

    def test_swap_timed_out_records_timed_out_outcome(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        idx.ingest(
            [rec('SwapTimedOut', miner='pk_a', block_time=400, swap_key=key, sol_amount=10, slash=1)],
            ATTR,
        )
        assert store.get_swap_outcome(key.hex()) == 'timed_out'
        assert store.get_swap_outcome(DEFAULT_SWAP_KEY.hex()) is None  # only the event's key lands
        store.close()

    def test_stale_claim_closed_records_expired_outcome(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        idx.ingest([rec('StaleClaimClosed', miner='pk_a', block_time=400, swap_key=key)], ATTR)
        assert store.get_swap_outcome(key.hex()) == 'expired'
        store.close()

    def test_reingest_of_same_event_is_a_noop_upsert(self, tmp_path: Path):
        """A cursor reset can replay history — the outcome row upserts instead of erroring."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        event = rec('SwapTimedOut', miner='pk_a', block_time=400, swap_key=key, sol_amount=10, slash=1)
        idx.ingest([event], ATTR)
        idx.ingest([event], ATTR)
        assert store.get_swap_outcome(key.hex()) == 'timed_out'
        store.close()

    def test_legacy_b35_table_is_dropped_and_recreated(self, tmp_path: Path):
        # The pre-B3.5 scoring ledger squatted the swap_outcomes name in long-lived state DBs.
        import sqlite3

        db = tmp_path / 'state.db'
        with sqlite3.connect(db) as conn:
            conn.execute('CREATE TABLE swap_outcomes (swap_id INTEGER, completed INTEGER, resolved_block INTEGER)')
        store = ValidatorStateStore(db_path=db)
        store.record_swap_outcome('ab' * 32, 'timed_out', 100)
        assert store.get_swap_outcome('ab' * 32) == 'timed_out'
        store.close()

    def test_prune_drops_old_outcomes(self, tmp_path: Path):
        store = make_store(tmp_path)
        old, recent = b'\x01' * 32, b'\x02' * 32
        store.record_swap_outcome(old.hex(), 'completed', 100)
        store.record_swap_outcome(recent.hex(), 'timed_out', 900)
        store.prune_swap_outcomes(500)
        assert store.get_swap_outcome(old.hex()) is None
        assert store.get_swap_outcome(recent.hex()) == 'timed_out'
        store.close()


class TestAttributionAndSkips:
    def test_unbound_pubkey_is_dropped(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # pk_c has no binding → no UID to credit → its event is skipped.
        n = idx.ingest([rec('MinerActivated', miner='pk_c', block_time=100, at=100)], ATTR)
        assert n == 0
        assert idx.get_active_miners_at(100) == set()
        store.close()

    def test_unstamped_tip_tx_is_skipped(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # block_time None (not-yet-stamped tip tx) → skipped, cursor stays behind it.
        n = idx.ingest([rec('MinerActivated', miner='pk_a', block_time=None, at=0)], ATTR)
        assert n == 0
        assert idx.get_active_miners_at(10_000) == set()
        store.close()

    def test_unknown_event_name_is_ignored(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = make_index(store)
        n = idx.ingest([rec('ValidatorWeightsUpdated', miner='pk_a', block_time=100)], ATTR)
        assert n == 0
        store.close()

    def test_attribution_maps_pubkey_to_bound_hotkey(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=100, at=100),
                rec('MinerActivated', miner='pk_b', block_time=100, at=100),
            ],
            ATTR,
        )
        # Events keyed on-chain by pubkey land on the bound hotkeys.
        assert idx.get_active_miners_at(100) == {'hk_a', 'hk_b'}
        store.close()


class TestIngestEndToEndCrown:
    def test_ingested_events_drive_the_crown_to_funded_best_rate_holder(self, tmp_path: Path):
        """The full B3.4 path: decode-shaped records → index persistence →
        scoring's crown replay credits the active, funded, best-rate miner."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                # Both miners active + funded from t=0; btc→tao (higher rate wins).
                rec('MinerActivated', miner='pk_a', block_time=0, at=0),
                rec('MinerActivated', miner='pk_b', block_time=0, at=0),
                rec('CollateralPosted', miner='pk_a', block_time=0, amount=0, total=500_000_000),
                rec('CollateralPosted', miner='pk_b', block_time=0, amount=0, total=500_000_000),
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    rate=300 * RATE_PRECISION,
                    liquidity=0,
                ),
                rec(
                    'QuoteSet',
                    miner='pk_b',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    rate=200 * RATE_PRECISION,
                    liquidity=0,
                ),
                # A is reserved then takes a swap mid-window — crown flips to B while busy.
                rec('PoolResolved', miner='pk_a', block_time=400, winner='pk_router', user='pk_user', requests=1),
                rec('SwapInitiated', miner='pk_a', block_time=400),
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=800,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100_000,
                    to_amount=500_000_000,
                ),
            ],
            ATTR,
        )
        crown = replay_crown_time_window(
            store=store,
            event_index=idx,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
            min_swap_lamports=100_000_000,
            max_swap_lamports=500_000_000,
        )
        # A: (100,400] + (800,1100] = 600. B: (400,800] = 400.
        assert crown == {'hk_a': 600.0, 'hk_b': 400.0}
        store.close()
