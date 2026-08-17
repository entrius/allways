"""B3.4 — SolanaEventIndex: persist decoded program events into the state store and expose the crown's
per-instant read interface over them, on the unix-``blockTime`` axis, attributing each Solana pubkey to its
bound hotkey at write time."""

from pathlib import Path

from allways.classes import ActivityTransition, MinerActivity
from allways.constants import RATE_PRECISION
from allways.solana.events import EventRecord
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import replay_crown_time_window
from allways.validator.state_store import ValidatorStateStore

# pubkey str -> hotkey ss58 (what binding.build_attribution returns).
ATTR = {'pk_a': 'hk_a', 'pk_b': 'hk_b'}
RESERVATION_TTL = 300


def _both(state):
    # A hub-less (legacy-shape) event steps every hub's machine — the global-busy reading.
    return {'sol': state, 'tao': state}


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
        assert idx.get_activity_state_at(200) == {'hk_a': _both(MinerActivity.RESERVED)}
        assert idx.get_activity_state_at(250) == {'hk_a': _both(MinerActivity.FULFILLING)}
        assert idx.get_activity_state_at(400) == {}  # completed → AVAILABLE
        store.close()

    def test_swap_cancelled_frees_the_hub(self, tmp_path: Path):
        """The no-fault cancel is a terminal like completion/timeout: without its FULFILL_END
        the hub stays FULFILLING (crownless) until an unrelated swap's terminal repairs it."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_router', user='pk_user', requests=1),
                rec('SwapInitiated', miner='pk_a', block_time=250),
                rec('SwapCancelled', miner='pk_a', block_time=400, collateral_chain='', collateral_amount=10, reason=0),
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': _both(MinerActivity.FULFILLING)}
        assert idx.get_activity_state_at(400) == {}  # cancelled → AVAILABLE
        # The synthetic RESERVE_EXPIRE at 1200 then no-ops in AVAILABLE.
        assert idx.get_activity_state_at(1200) == {}
        store.close()

    def test_swap_cancelled_frees_only_its_hub(self, tmp_path: Path):
        """A v3.1 cancel carries the backing hub — the sibling hub's machine must not step."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec(
                    'PoolResolved',
                    miner='pk_a',
                    block_time=200,
                    winner='pk_router',
                    user='pk_user',
                    requests=1,
                    collateral_chain='tao',
                ),
                rec('SwapInitiated', miner='pk_a', block_time=250, collateral_chain='tao'),
                rec(
                    'SwapCancelled',
                    miner='pk_a',
                    block_time=400,
                    collateral_chain='tao',
                    collateral_amount=10,
                    reason=0,
                ),
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': {'tao': MinerActivity.FULFILLING}}
        assert idx.get_activity_state_at(400) == {}  # only the tao machine ever stepped
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
        assert idx.get_activity_state_at(200) == {'hk_a': _both(MinerActivity.RESERVED)}
        assert idx.get_activity_state_at(499) == {'hk_a': _both(MinerActivity.RESERVED)}
        assert idx.get_activity_state_at(500) == {}  # 200 + 300 ttl → AVAILABLE
        kinds = [e['kind'] for e in idx.get_activity_events_in_range(0, 1000)]
        assert kinds == [1, 3]  # RESERVE_START then synthetic RESERVE_EXPIRE
        store.close()

    def test_reservation_filled_restamps_expiry_so_late_initiate_stays_busy(self, tmp_path: Path):
        """V-H5: PoolResolved's draw+ttl expiry is a guess. When SwapInitiated lands after it
        (slow BTC confs), the miner must still be FULFILLING — not scored AVAILABLE and earning the
        crown while busy. ReservationFilled carries the real reserved_until; we re-stamp the guess."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=30)  # tiny ttl → synthetic expiry at 230, before SwapInitiated
        idx.ingest(
            [
                rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_router', user='pk_user', requests=1),
                rec('ReservationFilled', miner='pk_a', block_time=210, reserved_until=600),
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
        # Re-stamped to 600, so the miner is NOT freed at the old 230 guess.
        assert idx.get_activity_state_at(230) == {'hk_a': _both(MinerActivity.RESERVED)}
        # The bug regression: without the re-stamp this would be {} (AVAILABLE) — FULFILL_START
        # would have hit AVAILABLE (no edge) and held there through the whole swap.
        assert idx.get_activity_state_at(250) == {'hk_a': _both(MinerActivity.FULFILLING)}
        assert idx.get_activity_state_at(399) == {'hk_a': _both(MinerActivity.FULFILLING)}
        assert idx.get_activity_state_at(400) == {}  # completed → AVAILABLE
        # The single RESERVE_EXPIRE row was moved (not duplicated) from 230 to 600.
        expiries = [e['block'] for e in idx.get_activity_events_in_range(0, 2000) if e['kind'] == 3]
        assert expiries == [600]
        store.close()

    def test_reservation_extended_pushes_expiry(self, tmp_path: Path):
        """A claim-runway extend (validator slides reserved_until for slow source confs) emits
        ReservationExtended — the expiry must follow it, else the miner is freed mid-fulfillment."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=30)
        idx.ingest(
            [
                rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_router', user='pk_user', requests=1),
                rec('ReservationFilled', miner='pk_a', block_time=210, reserved_until=300),
                rec('ReservationExtended', miner='pk_a', block_time=280, reserved_until=900, validator='pk_router'),
                rec('SwapInitiated', miner='pk_a', block_time=350),  # past the pre-extend 300 deadline
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=600,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100_000,
                    to_amount=500_000_000,
                ),
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': _both(MinerActivity.RESERVED)}
        assert idx.get_activity_state_at(350) == {'hk_a': _both(MinerActivity.FULFILLING)}
        assert idx.get_activity_state_at(600) == {}  # completed → AVAILABLE
        expiries = [e['block'] for e in idx.get_activity_events_in_range(0, 2000) if e['kind'] == 3]
        assert expiries == [900]  # one row, pushed guess(230)→fill(300)→extend(900)
        store.close()

    def test_restamp_targets_only_its_own_hub(self, tmp_path: Path):
        """The re-stamp matches on hub, so a fill on one purse must not move the sibling's expiry."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=30)
        idx.ingest(
            [
                rec(
                    'PoolResolved',
                    miner='pk_a',
                    block_time=200,
                    winner='pk_router',
                    user='pk_user',
                    requests=1,
                    collateral_chain='sol',
                ),
                rec(
                    'PoolResolved',
                    miner='pk_a',
                    block_time=200,
                    winner='pk_router',
                    user='pk_user',
                    requests=1,
                    collateral_chain='tao',
                ),
                rec('ReservationFilled', miner='pk_a', block_time=210, reserved_until=600, collateral_chain='tao'),
            ],
            ATTR,
        )
        # tao expiry moved to 600; sol expiry untouched at the 230 guess.
        rows = idx.get_activity_events_in_range(0, 2000)
        expiries = sorted((e['hub'], e['block']) for e in rows if e['kind'] == 3)
        assert expiries == [('sol', 230), ('tao', 600)]
        assert idx.get_activity_state_at(400) == {'hk_a': {'tao': MinerActivity.RESERVED}}  # sol freed, tao held
        store.close()

    def test_busy_miner_is_the_reserved_miner_not_the_router(self, tmp_path: Path):
        """PoolResolved.miner (not .winner, the router) is busy-gated."""
        store = make_store(tmp_path)
        idx = make_index(store)
        idx.ingest(
            [rec('PoolResolved', miner='pk_a', block_time=200, winner='pk_b', user='pk_user', requests=1)],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': _both(MinerActivity.RESERVED)}  # not hk_b
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
        assert store.get_clearing_volumes(0, 1000)[('btc', 'tao')]['hk_a'] == (100_000, 500_000_000)
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
        assert store.get_clearing_volumes(0, 100)[('btc', 'tao')]['hk_a'] == (big, big - 1)
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
        assert ('btc', 'tao') in store.get_clearing_volumes(0, 100)  # found under lowercased direction
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
        assert store.get_clearing_volumes(0, 100) == {}
        store.close()

    def test_reingested_swap_counts_volume_once(self, tmp_path: Path):
        """M2: a cursor-reset replay of the same SwapCompleted must not double volume."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        completed = rec(
            'SwapCompleted',
            miner='pk_a',
            block_time=10,
            from_chain='btc',
            to_chain='tao',
            from_amount=100,
            to_amount=200,
        )
        idx.ingest([completed], ATTR)
        idx.ingest([completed], ATTR)
        assert store.get_clearing_volumes(0, 100)[('btc', 'tao')]['hk_a'] == (100, 200)
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
                    swap_key=bytes([1] * 32),
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=1,
                    to_amount=2,
                ),
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=900,
                    swap_key=bytes([2] * 32),
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=3,
                    to_amount=4,
                ),
            ],
            ATTR,
        )
        store.prune_clearing_rates(500)
        # No anchor preservation — the old sample is gone, only the 900 legs remain.
        assert store.get_clearing_volumes(0, 1000)[('btc', 'tao')]['hk_a'] == (3, 4)
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
            [rec('SwapTimedOut', miner='pk_a', block_time=400, swap_key=key, collateral_amount=10, slash=1)],
            ATTR,
        )
        assert store.get_swap_outcome(key.hex()) == 'timed_out'
        assert store.get_swap_outcome(DEFAULT_SWAP_KEY.hex()) is None  # only the event's key lands
        store.close()

    def test_swap_cancelled_records_cancelled_outcome(self, tmp_path: Path):
        """Post-close, the seam must distinguish a cancel from a completion — both close the PDA."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        idx.ingest(
            [
                rec(
                    'SwapCancelled',
                    miner='pk_a',
                    block_time=400,
                    swap_key=key,
                    collateral_chain='sol',
                    collateral_amount=10,
                    reason=0,
                )
            ],
            ATTR,
        )
        assert store.get_swap_outcome(key.hex()) == 'cancelled'
        store.close()

    def test_stale_claim_closed_records_expired_outcome(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        idx.ingest([rec('StaleClaimClosed', miner='pk_a', block_time=400, swap_key=key)], ATTR)
        assert store.get_swap_outcome(key.hex()) == 'expired'
        store.close()

    def test_unbound_miner_still_records_terminal_outcome(self, tmp_path: Path):
        """V-M5: a miner that deregs mid-swap is unbound at ingest, but the terminal outcome +
        delivery hash are keyed purely by swap_key — record them so /status can resolve, while
        the UID-crediting clearing volume stays empty."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        n = idx.ingest(
            [
                rec('SwapFulfilled', miner='pk_c', block_time=390, swap_key=key, to_tx_hash='0xdead'),
                rec(
                    'SwapCompleted',
                    miner='pk_c',  # unbound
                    block_time=400,
                    swap_key=key,
                    from_chain='btc',
                    to_chain='tao',
                    from_amount=100,
                    to_amount=200,
                ),
            ],
            ATTR,
        )
        assert n == 2
        assert store.get_swap_outcome(key.hex()) == 'completed'
        assert store.get_swap_fulfillment(key.hex()) == '0xdead'
        assert store.get_clearing_volumes(0, 1000) == {}  # no UID → no volume credited
        store.close()

    def test_reingest_of_same_event_is_a_noop_upsert(self, tmp_path: Path):
        """A cursor reset can replay history — the outcome row upserts instead of erroring."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        key = bytes(range(32))
        event = rec('SwapTimedOut', miner='pk_a', block_time=400, swap_key=key, collateral_amount=10, slash=1)
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
                # Both miners active + funded from t=0; btc→tao is the reverse leg of the
                # TAO-hub pair (canonical 'BTC per TAO'), so the LOWER canonical rate wins.
                # A tao-hub direction is funded by the attested TAO bond, not the SOL purse (F4),
                # and the quotes are tao-backed.
                rec('MinerActivated', miner='pk_a', block_time=0, at=0),
                rec('MinerActivated', miner='pk_b', block_time=0, at=0),
                rec(
                    'BondAttested', miner='pk_a', block_time=0, chain='tao', effective_balance=500_000_000, locked=True
                ),
                rec(
                    'BondAttested', miner='pk_b', block_time=0, chain='tao', effective_balance=500_000_000, locked=True
                ),
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    collateral_chain='tao',
                    rate=int(0.002 * RATE_PRECISION),
                    liquidity=0,
                ),
                rec(
                    'QuoteSet',
                    miner='pk_b',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    collateral_chain='tao',
                    rate=int(0.003 * RATE_PRECISION),
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
            min_swap_hub=100_000_000,
            max_swap_hub=500_000_000,
        )
        # A: (100,400] + (800,1100] = 600. B: (400,800] = 400.
        assert crown == {'hk_a': 600.0, 'hk_b': 400.0}
        store.close()

    def test_a_tao_miner_earns_only_when_its_bond_funds_the_quote(self, tmp_path: Path):
        """F4 (4a): a tao-hub direction is purse-scored against the attested TAO bond, not run
        neutral. The bonded miner earns; an equal-rate rival with no bond can't fund and earns 0
        (before F4 the purse axis ran neutral and both split the window)."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=0, at=0),
                rec('MinerActivated', miner='pk_b', block_time=0, at=0),
                # A is bonded on TAO; B posts SOL collateral, which does not fund a tao-hub quote.
                rec(
                    'BondAttested', miner='pk_a', block_time=0, chain='tao', effective_balance=500_000_000, locked=True
                ),
                rec('CollateralPosted', miner='pk_b', block_time=0, amount=0, total=500_000_000),
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    collateral_chain='tao',
                    rate=int(0.002 * RATE_PRECISION),
                    liquidity=0,
                ),
                rec(
                    'QuoteSet',
                    miner='pk_b',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    collateral_chain='tao',
                    rate=int(0.002 * RATE_PRECISION),
                    liquidity=0,
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
            min_swap_hub=100_000_000,
            max_swap_hub=500_000_000,
        )
        assert crown == {'hk_a': 1000.0}  # only the bonded miner earns
        store.close()

    def test_dropping_the_tao_purse_stops_tao_crown_but_not_sol_crown(self, tmp_path: Path):
        """F4 (4b): a silent tao-purse drop (bit off, no MinerDeactivated) must end the miner's
        tao-hub crown, while its sol-hub quote keeps earning."""
        store = make_store(tmp_path)
        idx = make_index(store, ttl=1000)
        idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=0, at=0),
                rec(
                    'BondAttested', miner='pk_a', block_time=0, chain='tao', effective_balance=500_000_000, locked=True
                ),
                rec('CollateralPosted', miner='pk_a', block_time=0, amount=0, total=500_000_000),
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=0,
                    from_chain='btc',
                    to_chain='tao',
                    collateral_chain='tao',
                    rate=int(0.002 * RATE_PRECISION),
                    liquidity=0,
                ),
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=0,
                    from_chain='btc',
                    to_chain='sol',
                    collateral_chain='sol',
                    rate=int(0.002 * RATE_PRECISION),
                    liquidity=0,
                ),
                # Mid-window the TAO purse bit goes down, with NO MinerDeactivated.
                rec(
                    'MinerBackingChanged', miner='pk_a', block_time=600, backing='tao', enabled=False, active_backings=1
                ),
            ],
            ATTR,
        )
        tao_crown = replay_crown_time_window(
            store=store,
            event_index=idx,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
            min_swap_hub=100_000_000,
            max_swap_hub=500_000_000,
        )
        sol_crown = replay_crown_time_window(
            store=store,
            event_index=idx,
            from_chain='btc',
            to_chain='sol',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a'},
            min_swap_hub=100_000_000,
            max_swap_hub=500_000_000,
        )
        assert tao_crown == {'hk_a': 500.0}  # (100,600] only — the tao quote is zeroed at 600
        assert sol_crown == {'hk_a': 1000.0}  # sol quote unaffected by the tao drop
        store.close()


class TestReconcileLiveState:
    """Scoring-round backstop: divergence between live MinerState and the
    event-derived view is corrected with events stamped ``now`` — but only for
    miners whose event stream has been quiet for RECONCILE_QUIET_SECS."""

    NOW = 100_000

    @staticmethod
    def _ms(active: bool = True, collateral: int = 0):
        from types import SimpleNamespace

        return SimpleNamespace(active=active, collateral=collateral)

    def test_corrects_missed_deactivation(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_active_event(100, 'hk_a', True)  # activation seen, deactivation lost
        idx.reconcile_live_state({'hk_a': self._ms(active=False)}, now=self.NOW)
        assert idx.get_active_miners_at(self.NOW) == set()
        store.close()

    def test_corrects_pre_binding_activation_drop(self, tmp_path: Path):
        # Chain says active with collateral, but the events landed before the
        # miner bound its hotkey and were dropped — reconcile makes them earnable.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=550)}, now=self.NOW)
        assert idx.get_active_miners_at(self.NOW) == {'hk_a'}
        assert idx.get_miner_collaterals_at(self.NOW) == {'hk_a': 550}
        store.close()

    def test_seeds_missing_collateral_baseline(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_active_event(100, 'hk_a', True)  # active agrees; collateral unknown
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=42)}, now=self.NOW)
        assert idx.get_miner_collaterals_at(self.NOW) == {'hk_a': 42}
        assert idx.get_active_miners_at(self.NOW) == {'hk_a'}  # no spurious active row
        store.close()

    def test_quiet_guard_defers_to_recent_event(self, tmp_path: Path):
        # A real active event landed seconds ago; a stale live read disagreeing
        # with it must NOT be written over the fresher event truth.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_active_event(self.NOW - 60, 'hk_a', True)
        store.insert_collateral_event(self.NOW - 60, 'hk_a', 999)
        idx.reconcile_live_state({'hk_a': self._ms(active=False, collateral=1)}, now=self.NOW)
        assert idx.get_active_miners_at(self.NOW) == {'hk_a'}
        assert idx.get_miner_collaterals_at(self.NOW) == {'hk_a': 999}
        store.close()

    def test_noop_when_consistent(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_active_event(100, 'hk_a', True)
        store.insert_collateral_event(100, 'hk_a', 550)
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=550)}, now=self.NOW)
        assert len(store.load_all_active_events()) == 1
        assert len(store.load_all_collateral_events()) == 1
        store.close()

    def test_reseeds_missing_tao_collateral_from_live_bond(self, tmp_path: Path):
        # The tao anchor was lost (missed BondAttested, or a mis-keyed prune) so the purse
        # reads 0 and the miner is off every tao crown — the live bond re-seeds it. Regression
        # for the live divergence: 0.85 τ attested on-chain, no tao collateral row in the store.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_collateral_event(100, 'hk_a', 2_000_000_000, backing='sol')  # sol purse intact
        idx.reconcile_live_state(
            {'hk_a': self._ms(active=True, collateral=2_000_000_000)},
            now=self.NOW,
            live_bonds={'hk_a': 850_000_000},
        )
        assert idx.get_miner_collaterals_at(self.NOW, backing='tao') == {'hk_a': 850_000_000}
        assert idx.get_miner_collaterals_at(self.NOW, backing='sol') == {'hk_a': 2_000_000_000}  # untouched
        store.close()

    def test_tao_backstop_skipped_when_bonds_absent(self, tmp_path: Path):
        # Default (no live_bonds) keeps the sol-only behaviour — no spurious tao rows.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=42)}, now=self.NOW)
        assert idx.get_miner_collaterals_at(self.NOW, backing='tao') == {}
        store.close()

    def test_tao_quiet_guard_defers_to_recent_bond_event(self, tmp_path: Path):
        # A real BondAttested landed seconds ago; a stale live bond read must not overwrite it.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_collateral_event(self.NOW - 60, 'hk_a', 900_000_000, backing='tao')
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=0)}, now=self.NOW, live_bonds={'hk_a': 1})
        assert idx.get_miner_collaterals_at(self.NOW, backing='tao') == {'hk_a': 900_000_000}
        store.close()

    def test_unlocked_bond_reconciles_to_zero(self, tmp_path: Path):
        # An unlocked bond backs nothing → 0 (live_bond_balances maps it so); a stale nonzero
        # tao anchor is corrected down, matching the BondAttested ingest.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        store.insert_collateral_event(100, 'hk_a', 850_000_000, backing='tao')
        idx.reconcile_live_state({'hk_a': self._ms(active=True, collateral=0)}, now=self.NOW, live_bonds={'hk_a': 0})
        assert idx.get_miner_collaterals_at(self.NOW, backing='tao') == {'hk_a': 0}
        store.close()


class TestReconcileLiveQuotes:
    """Rate-liveness backstop: a lane whose event-derived rate is positive but
    which has no matching live on-chain quote is zeroed once per round — the
    safety net for a lost ``QuoteRemoved`` (which would otherwise freeze the
    lane's last nonzero rate as its prune anchor forever). Quiet-window guarded
    per lane; an empty quote book reads as a failed RPC and skips the sweep."""

    NOW = 100_000

    @staticmethod
    def _seed_quote(
        idx,
        *,
        block_time: int,
        from_chain: str = 'btc',
        to_chain: str = 'tao',
        rate: int = 200,
        collateral_chain: str = 'sol',
    ):
        idx.ingest(
            [
                rec(
                    'QuoteSet',
                    miner='pk_a',
                    block_time=block_time,
                    from_chain=from_chain,
                    to_chain=to_chain,
                    rate=rate * RATE_PRECISION,
                    liquidity=0,
                    collateral_chain=collateral_chain,
                )
            ],
            ATTR,
        )

    def test_zeroes_lane_whose_quote_removed_was_dropped(self, tmp_path: Path):
        # The quote died on-chain but its QuoteRemoved never reached ingest; the
        # book still has an unrelated live quote, so the sweep runs and zeroes it.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=100)
        idx.reconcile_live_quotes({('hk_b', 'sol', 'tao', 'sol'): 1.0}, now=self.NOW)
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', self.NOW, collateral_chain='sol') == (0.0, self.NOW)
        store.close()

    def test_zeroes_only_the_missing_backing_on_dual_lane(self, tmp_path: Path):
        # Dual-backing direction: the sol-backed quote vanished, the tao-backed
        # sibling is still live — only the sol lane is zeroed.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=100, from_chain='sol', to_chain='tao', rate=1, collateral_chain='sol')
        self._seed_quote(idx, block_time=200, from_chain='sol', to_chain='tao', rate=2, collateral_chain='tao')
        idx.reconcile_live_quotes({('hk_a', 'sol', 'tao', 'tao'): 2.0}, now=self.NOW)
        assert store.get_latest_rate_before('hk_a', 'sol', 'tao', self.NOW, collateral_chain='sol') == (0.0, self.NOW)
        assert store.get_latest_rate_before('hk_a', 'sol', 'tao', self.NOW, collateral_chain='tao') == (2.0, 200)
        store.close()

    def test_quiet_guard_defers_to_recent_rate_event(self, tmp_path: Path):
        # The lane's quote was set seconds ago; a stale (pre-set) book read that
        # doesn't carry it yet must not zero the fresher event truth.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=self.NOW - 60)
        idx.reconcile_live_quotes({('hk_b', 'sol', 'tao', 'sol'): 1.0}, now=self.NOW)
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', self.NOW, collateral_chain='sol') == (
            200.0,
            self.NOW - 60,
        )
        store.close()

    def test_corrects_diverged_rate_to_chain_value(self, tmp_path: Path):
        # A missed QuoteSet: the chain says 250, the event stream still says 200.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=100, rate=200)
        idx.reconcile_live_quotes({('hk_a', 'btc', 'tao', 'sol'): 250.0}, now=self.NOW)
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', self.NOW, collateral_chain='sol') == (
            250.0,
            self.NOW,
        )
        store.close()

    def test_empty_book_skips_the_sweep(self, tmp_path: Path):
        # An empty bulk read is indistinguishable from a failed one — fail open
        # rather than zeroing every lane at once.
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=100)
        idx.reconcile_live_quotes({}, now=self.NOW)
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', self.NOW, collateral_chain='sol') == (200.0, 100)
        store.close()

    def test_noop_when_consistent(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        self._seed_quote(idx, block_time=100)
        idx.reconcile_live_quotes({('hk_a', 'btc', 'tao', 'sol'): 200.0}, now=self.NOW)
        events = store.get_rate_events_in_range('btc', 'tao', 0, self.NOW, collateral_chain='sol')
        assert [e['block'] for e in events] == [100]
        store.close()


class TestSwapFulfillmentHash:
    def test_swap_fulfilled_persists_delivery_hash(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest([rec('SwapFulfilled', block_time=300, to_tx_hash='destTx', to_amount=500)], ATTR)
        assert store.get_swap_fulfillment(DEFAULT_SWAP_KEY.hex()) == 'destTx'
        # Re-ingest (cursor reset) is a no-op upsert, and unknown keys read None.
        idx.ingest([rec('SwapFulfilled', block_time=300, to_tx_hash='destTx', to_amount=500)], ATTR)
        assert store.get_swap_fulfillment(DEFAULT_SWAP_KEY.hex()) == 'destTx'
        assert store.get_swap_fulfillment('ff' * 32) is None
        store.close()

    def test_fulfillment_hash_pruned_with_outcomes(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest([rec('SwapFulfilled', block_time=100, to_tx_hash='destTx', to_amount=500)], ATTR)
        store.prune_swap_outcomes(200)
        assert store.get_swap_fulfillment(DEFAULT_SWAP_KEY.hex()) is None
        store.close()


class TestIngestActivityPerHub:
    """v3.1.1: lifecycle events carry collateral_chain, and the activity machine
    runs per (hotkey, hub) so a sol-busy miner stays crownable on its tao quotes."""

    def test_pool_resolved_scopes_the_reservation_to_its_hub(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = make_index(store, ttl=300)
        idx.ingest(
            [
                rec(
                    'PoolResolved',
                    miner='pk_a',
                    block_time=200,
                    winner='pk_router',
                    user='pk_user',
                    requests=1,
                    collateral_chain='tao',
                )
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(250) == {'hk_a': {'tao': MinerActivity.RESERVED}}
        assert idx.get_activity_state_at(500) == {}  # the synthetic expire inherits the hub

    def test_swap_lifecycle_stays_on_its_hub(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = make_index(store, ttl=10**9)
        idx.ingest(
            [
                rec(
                    'PoolResolved',
                    miner='pk_a',
                    block_time=200,
                    winner='w',
                    user='u',
                    requests=1,
                    collateral_chain='sol',
                ),
                rec('SwapInitiated', miner='pk_a', block_time=250, collateral_chain='sol'),
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(300) == {'hk_a': {'sol': MinerActivity.FULFILLING}}
        idx.ingest(
            [
                rec(
                    'SwapCompleted',
                    miner='pk_a',
                    block_time=400,
                    from_chain='sol',
                    to_chain='btc',
                    from_amount=1,
                    to_amount=1,
                    collateral_chain='sol',
                )
            ],
            ATTR,
        )
        assert idx.get_activity_state_at(450) == {}


def test_activity_events_hub_column_migrates_in_place(tmp_path: Path):
    """A pre-v3.1.1 state.db (no hub column) gains it on open; its legacy rows read
    NULL = busy on every hub, exactly the meaning they were written under."""
    import sqlite3

    db = tmp_path / 'state.db'
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE activity_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            block_num   INTEGER NOT NULL,
            hotkey      TEXT NOT NULL,
            kind        INTEGER NOT NULL
        );
        INSERT INTO activity_events (block_num, hotkey, kind) VALUES (100, 'hk_a', 1);
        """
    )
    conn.commit()
    conn.close()
    store = ValidatorStateStore(db_path=db)
    assert store.get_activity_state_at(150) == {'hk_a': {'sol': MinerActivity.RESERVED, 'tao': MinerActivity.RESERVED}}
    store.insert_activity_event(200, 'hk_a', ActivityTransition(2), hub='sol')  # FULFILL_START, sol purse
    state = store.get_activity_state_at(250)
    assert state['hk_a']['sol'] == MinerActivity.FULFILLING
    assert state['hk_a']['tao'] == MinerActivity.RESERVED
    store.close()


class TestMinerlessEvents:
    def test_by_design_minerless_events_skip_without_debug_noise(self, tmp_path: Path, monkeypatch):
        """AttestHeartbeat is {at} by construction — skipping it must not log the
        'missing miner field' line that reads like a decode defect in validator logs."""
        from allways.validator import event_index as ei

        calls = []
        monkeypatch.setattr(ei.bt.logging, 'debug', lambda *a, **k: calls.append(a))
        heartbeat = EventRecord(
            name='AttestHeartbeat', fields={'at': 1_000}, slot=1, block_time=1_000, signature='sig-hb'
        )
        halt = EventRecord(name='HaltSet', fields={'halted': True}, slot=2, block_time=1_001, signature='sig-halt')

        written = make_index(make_store(tmp_path)).ingest([heartbeat, halt], ATTR)

        assert written == 0
        assert calls == []
