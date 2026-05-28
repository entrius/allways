"""Unit tests for ContractEventWatcher.

Covers three layers:
  1. ``load_event_registry`` — metadata JSON → registry dict
  2. ``decode_data_fields`` / ``decode_topic_fields`` — raw bytes → values
     (driven by hand-encoded SCALE fixtures so we don't need a live node)
  3. ``apply_event`` — state transitions once events are decoded
"""

import struct
from pathlib import Path
from unittest.mock import MagicMock, patch

from bittensor.utils import ss58_decode

from allways.validator.event_watcher import (
    ContractEventWatcher,
    EventDef,
    FieldDef,
    decode_data_fields,
    decode_topic_fields,
    load_event_registry,
)
from allways.validator.state_store import ValidatorStateStore

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'

# Well-known test SS58 — Alice from the substrate dev keyring. Used as
# contract_address in fixtures so the decoder's address comparison doesn't
# receive a garbage string that could bypass validation on future codepaths.
TEST_CONTRACT_ADDRESS = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_watcher(tmp_path: Path, *, netuid=None, subtensor=None) -> ContractEventWatcher:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return ContractEventWatcher(
        substrate=MagicMock(),
        contract_address=TEST_CONTRACT_ADDRESS,
        metadata_path=METADATA_PATH,
        state_store=store,
        netuid=netuid,
        subtensor=subtensor,
    )


def encode_u64_le(v: int) -> bytes:
    return struct.pack('<Q', v)


def encode_u128_le(v: int) -> bytes:
    return struct.pack('<QQ', v & 0xFFFFFFFFFFFFFFFF, v >> 64)


def encode_bool(v: bool) -> bytes:
    return b'\x01' if v else b'\x00'


def ss58_to_bytes(addr: str) -> bytes:
    return bytes.fromhex(ss58_decode(addr))


class TestRegistryLoad:
    def test_registry_has_expected_events(self):
        registry = load_event_registry(METADATA_PATH)
        names = {e.name for e in registry.values()}
        for expected in (
            'CollateralPosted',
            'CollateralWithdrawn',
            'CollateralSlashed',
            'MinerActivated',
            'SwapCompleted',
            'SwapTimedOut',
            'ConfigUpdated',
        ):
            assert expected in names, f'missing event {expected}'


class TestActiveFlag:
    def test_activation_adds_to_set(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        assert 'hk_a' in w.active_miners
        w.apply_event(200, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        assert 'hk_a' not in w.active_miners
        w.state_store.close()


class TestSwapOutcomePersistence:
    def test_completed_writes_ledger(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (1, 0)
        w.state_store.close()

    def test_timed_out_writes_ledger(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapTimedOut', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (0, 1)
        w.state_store.close()

    def test_mixed_outcomes_counted(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        w.apply_event(101, 'SwapCompleted', {'swap_id': 2, 'miner': 'hk_a'})
        w.apply_event(102, 'SwapTimedOut', {'swap_id': 3, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (2, 1)
        w.state_store.close()

    def test_completed_event_resolves_tracker(self, tmp_path: Path):
        from allways.validator.swap_tracker import SwapTracker

        w = make_watcher(tmp_path)
        tracker = SwapTracker(client=MagicMock())
        from allways.classes import Swap, SwapStatus

        tracker.active[42] = Swap(
            id=42,
            user_hotkey='u',
            miner_hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            from_amount=1,
            to_amount=1,
            tao_amount=1,
            user_from_address='u',
            user_to_address='u',
            status=SwapStatus.FULFILLED,
            initiated_block=1,
            timeout_block=100,
        )
        w.swap_tracker = tracker

        w.apply_event(150, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a'})

        assert 42 not in tracker.active
        w.state_store.close()

    def test_timed_out_event_resolves_tracker(self, tmp_path: Path):
        from allways.classes import Swap, SwapStatus
        from allways.validator.swap_tracker import SwapTracker

        w = make_watcher(tmp_path)
        tracker = SwapTracker(client=MagicMock())
        tracker.active[43] = Swap(
            id=43,
            user_hotkey='u',
            miner_hotkey='hk_a',
            from_chain='btc',
            to_chain='tao',
            from_amount=1,
            to_amount=1,
            tao_amount=1,
            user_from_address='u',
            user_to_address='u',
            status=SwapStatus.FULFILLED,
            initiated_block=1,
            timeout_block=100,
        )
        w.swap_tracker = tracker

        w.apply_event(200, 'SwapTimedOut', {'swap_id': 43, 'miner': 'hk_a'})

        assert 43 not in tracker.active
        w.state_store.close()


class TestBusyIntervals:
    def test_initiate_marks_busy_then_complete_frees(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        assert 'hk_a' in w.get_busy_miners_at(100)
        assert w.open_swap_count['hk_a'] == 1

        w.apply_event(150, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 0
        assert 'hk_a' not in w.get_busy_miners_at(150)
        w.state_store.close()

    def test_timeout_frees_busy_miner(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        w.apply_event(500, 'SwapTimedOut', {'swap_id': 1, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 0
        assert 'hk_a' not in w.get_busy_miners_at(500)
        w.state_store.close()

    def test_get_busy_events_in_range_is_block_filtered(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        w.apply_event(200, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        w.apply_event(300, 'SwapInitiated', {'swap_id': 2, 'miner': 'hk_b'})
        # Range is (start, end]: block 100 is excluded, 200/300 included
        events = w.get_busy_events_in_range(100, 300)
        assert [(e['block'], e['hotkey'], e['delta']) for e in events] == [
            (200, 'hk_a', -1),
            (300, 'hk_b', +1),
        ]
        w.state_store.close()

    def test_count_never_goes_negative(self, tmp_path: Path):
        """A terminal event with no matching initiate (e.g. bootstrap gap)
        is dropped rather than letting count go negative."""
        w = make_watcher(tmp_path)
        w.apply_event(500, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        assert w.open_swap_count.get('hk_a', 0) == 0
        # And no event was recorded
        assert w.busy_events == []
        w.state_store.close()

    def test_bootstrap_seeds_busy_from_active_swaps(self, tmp_path: Path):
        from unittest.mock import MagicMock

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.return_value = False
        client.get_active_swaps.return_value = [
            type('S', (), {'miner_hotkey': 'hk_a', 'initiated_block': 50})(),
            type('S', (), {'miner_hotkey': 'hk_b', 'initiated_block': 80})(),
        ]
        w.initialize(current_block=100, metagraph_hotkeys=['hk_a', 'hk_b'], contract_client=client)

        assert w.open_swap_count == {'hk_a': 1, 'hk_b': 1}
        busy_now = w.get_busy_miners_at(100)
        assert busy_now == {'hk_a': 1, 'hk_b': 1}
        w.state_store.close()

    def test_bootstrapped_swap_initiated_replay_is_idempotent(self, tmp_path: Path):
        """Restart scenario: a swap that was live at bootstrap is seeded with
        +1 from the contract, then its SwapInitiated event replays during
        sync_to. The replay must NOT add a second +1."""
        from unittest.mock import MagicMock

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.return_value = False
        client.get_active_swaps.return_value = [
            type('S', (), {'id': 42, 'miner_hotkey': 'hk_a', 'initiated_block': 50})(),
        ]
        w.initialize(current_block=100, metagraph_hotkeys=['hk_a'], contract_client=client)
        assert w.open_swap_count['hk_a'] == 1

        # sync_to replays the SwapInitiated event for swap 42 — must be a no-op.
        w.apply_event(50, 'SwapInitiated', {'swap_id': 42, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 1, 'bootstrapped swap must not double-count on replay'

        # And when the matching terminal event fires, count returns to 0.
        w.apply_event(120, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 0
        assert 42 not in w.bootstrapped_swap_ids
        w.state_store.close()

    def test_non_bootstrapped_initiated_still_increments(self, tmp_path: Path):
        """A SwapInitiated whose id wasn't in the bootstrap set (new swap
        after startup) applies the +1 normally."""
        from unittest.mock import MagicMock

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.return_value = False
        client.get_active_swaps.return_value = [
            type('S', (), {'id': 42, 'miner_hotkey': 'hk_a', 'initiated_block': 50})(),
        ]
        w.initialize(current_block=100, metagraph_hotkeys=['hk_a'], contract_client=client)

        # swap_id 99 was not in the bootstrap set — treat normally.
        w.apply_event(110, 'SwapInitiated', {'swap_id': 99, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 2
        w.state_store.close()

    def test_bootstrapped_swap_timeout_also_clears_set(self, tmp_path: Path):
        from unittest.mock import MagicMock

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.return_value = False
        client.get_active_swaps.return_value = [
            type('S', (), {'id': 7, 'miner_hotkey': 'hk_a', 'initiated_block': 50})(),
        ]
        w.initialize(current_block=100, metagraph_hotkeys=['hk_a'], contract_client=client)
        assert 7 in w.bootstrapped_swap_ids

        w.apply_event(130, 'SwapTimedOut', {'swap_id': 7, 'miner': 'hk_a'})
        assert w.open_swap_count['hk_a'] == 0
        assert 7 not in w.bootstrapped_swap_ids
        w.state_store.close()


class TestSCALEDecoder:
    """Decoder fixtures: hand-build event bytes and feed them through.

    ink! v5 emits all event fields in the data blob in declaration order,
    with topic_fields getting a second copy in the topics array. These
    fixtures mirror what substrate.get_events would produce.
    """

    ALICE = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'

    def test_decode_miner_activated(self):
        event = EventDef(
            name='MinerActivated',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('active', 'bool')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        # data = AccountId (32b, the indexed-field copy) + active bool
        data = miner_bytes + encode_bool(True)
        topics = [b'\x00' * 32, miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['miner'] == self.ALICE
        assert values['active'] is True

    def test_decode_collateral_posted(self):
        event = EventDef(
            name='CollateralPosted',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('amount', 'u128'), FieldDef('total', 'u128')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        amount = 250_000_000
        total = 750_000_000
        data = miner_bytes + encode_u128_le(amount) + encode_u128_le(total)
        topics = [b'\x00' * 32, miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['miner'] == self.ALICE
        assert values['amount'] == amount
        assert values['total'] == total

    def test_decode_swap_completed(self):
        event = EventDef(
            name='SwapCompleted',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('swap_id', 'u64'), FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('tao_amount', 'u128'), FieldDef('fee_amount', 'u128')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        data = encode_u64_le(42) + miner_bytes + encode_u128_le(500_000_000) + encode_u128_le(5_000_000)
        topics = [b'\x00' * 32, encode_u64_le(42), miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['swap_id'] == 42
        assert values['miner'] == self.ALICE
        assert values['tao_amount'] == 500_000_000
        assert values['fee_amount'] == 5_000_000

    def test_decoder_stops_on_unknown_type(self):
        """A FieldDef with a type the decoder doesn't know halts decoding
        cleanly — partial values are kept, the rest are skipped."""
        event = EventDef(
            name='Weird',
            signature_topic='0x' + '00' * 32,
            topic_fields=[],
            data_fields=[
                FieldDef('first', 'u128'),
                FieldDef('second', 'FloatNobodySupports'),
                FieldDef('third', 'u128'),
            ],
        )
        data = encode_u128_le(1) + encode_u128_le(2) + encode_u128_le(3)
        values = decode_data_fields(event, data)
        # First decodes fine; decoder bails at 'second' and never reaches third
        assert 'first' in values
        assert 'second' not in values
        assert 'third' not in values


def make_pinned_commitment(
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    from_address: str = 'bc1-miner',
    to_address: str = '5miner',
    rate_str: str = '345',
    counter_rate_str: str = '0.0029',
):
    """A MinerPair as read_miner_commitment would return it."""
    from allways.classes import MinerPair

    return MinerPair(
        uid=1,
        hotkey='hk_a',
        from_chain=from_chain,
        from_address=from_address,
        to_chain=to_chain,
        to_address=to_address,
        rate=float(rate_str),
        rate_str=rate_str,
        counter_rate=float(counter_rate_str) if counter_rate_str else 0.0,
        counter_rate_str=counter_rate_str,
    )


class TestReservationPin:
    """MinerReserved → reservation pin index, plus the lifecycle clears that
    drop or refresh the pin."""

    def test_miner_reserved_writes_expected_pin(self, tmp_path: Path):
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        commitment = make_pinned_commitment()
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=commitment,
        ) as mock_read:
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        # Commitment read pinned to the reservation block so every validator
        # derives a byte-identical pin.
        assert mock_read.call_args.kwargs['block'] == 900

        pin = w.state_store.get_reservation_pin('hk_a')
        assert pin is not None
        assert pin.reserve_block == 900
        assert pin.reserved_until == 1000
        assert pin.from_chain == 'btc'
        assert pin.to_chain == 'tao'
        assert pin.rate_str == '345'
        assert pin.counter_rate_str == '0.0029'
        assert pin.miner_from_address == 'bc1-miner'
        assert pin.miner_to_address == '5miner'
        w.state_store.close()

    def test_commitment_read_raising_writes_no_pin(self, tmp_path: Path):
        """A transient RPC error or a pruned block must not write a pin and
        must not let the exception escape apply_event."""
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            side_effect=RuntimeError('rpc down'),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        assert w.state_store.get_reservation_pin('hk_a') is None
        w.state_store.close()

    def test_none_commitment_writes_no_pin(self, tmp_path: Path):
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=None,
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        assert w.state_store.get_reservation_pin('hk_a') is None
        w.state_store.close()

    def test_watcher_without_subtensor_is_noop(self, tmp_path: Path):
        """The test helper (and any watcher built without subtensor/netuid)
        must no-op the MinerReserved handler rather than crash."""
        w = make_watcher(tmp_path)  # no netuid/subtensor
        with patch('allways.validator.event_watcher.read_miner_commitment') as mock_read:
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        mock_read.assert_not_called()
        assert w.state_store.get_reservation_pin('hk_a') is None
        w.state_store.close()

    def test_swap_initiated_clears_pin(self, tmp_path: Path):
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})
        assert w.state_store.get_reservation_pin('hk_a') is not None

        w.apply_event(950, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})
        assert w.state_store.get_reservation_pin('hk_a') is None
        w.state_store.close()

    def test_swap_timed_out_clears_pin(self, tmp_path: Path):
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        w.apply_event(1100, 'SwapTimedOut', {'swap_id': 1, 'miner': 'hk_a'})
        assert w.state_store.get_reservation_pin('hk_a') is None
        w.state_store.close()

    def test_reservation_extension_finalized_bumps_pin_ttl(self, tmp_path: Path):
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        w.apply_event(990, 'ReservationExtensionFinalized', {'miner': 'hk_a', 'applied_target': 1400})
        pin = w.state_store.get_reservation_pin('hk_a')
        assert pin is not None
        assert pin.reserved_until == 1400
        w.state_store.close()

    def test_miner_reserved_emits_scoring_pin_events_for_both_directions(self, tmp_path: Path):
        """The scoring overlay needs to see a 'start' event in each direction
        the miner offers a positive rate for. ``MinerReserved`` reads the
        commitment's primary + counter rate and emits one event per direction
        with a positive quote."""
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        # commitment has rate_str=345 (btc→tao) and counter_rate_str=0.0029 (tao→btc).
        starts = [ev for ev in w.reservation_pin_events if ev.hotkey == 'hk_a' and ev.kind == 'start']
        assert len(starts) == 2
        primary = next(ev for ev in starts if ev.from_chain == 'btc' and ev.to_chain == 'tao')
        counter = next(ev for ev in starts if ev.from_chain == 'tao' and ev.to_chain == 'btc')
        assert primary.rate == 345.0
        assert counter.rate == 0.0029
        assert primary.block_num == 900 and counter.block_num == 900
        w.state_store.close()

    def test_swap_initiated_emits_pin_end_events(self, tmp_path: Path):
        """Open pins in any direction must close on SwapInitiated so the
        scoring overlay drops to live rates once the swap consumes the
        reservation slot."""
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})
        w.apply_event(950, 'SwapInitiated', {'swap_id': 1, 'miner': 'hk_a'})

        end_events = [
            ev for ev in w.reservation_pin_events if ev.hotkey == 'hk_a' and ev.kind == 'end' and ev.block_num == 950
        ]
        # One 'end' per direction that had been pinned.
        assert {(ev.from_chain, ev.to_chain) for ev in end_events} == {('btc', 'tao'), ('tao', 'btc')}
        # After end, get_reservation_pins_at returns empty for both directions.
        assert w.get_reservation_pins_at(950, 'btc', 'tao') == {}
        assert w.get_reservation_pins_at(950, 'tao', 'btc') == {}
        w.state_store.close()

    def test_get_reservation_pins_at_filters_by_direction(self, tmp_path: Path):
        """get_reservation_pins_at returns only pins active for the requested
        direction. A miner pinned in both legs is visible in both calls."""
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=make_pinned_commitment(),
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})

        primary = w.get_reservation_pins_at(950, 'btc', 'tao')
        counter = w.get_reservation_pins_at(950, 'tao', 'btc')
        assert primary == {'hk_a': 345.0}
        assert counter == {'hk_a': 0.0029}
        # A direction the miner doesn't quote returns nothing.
        assert w.get_reservation_pins_at(950, 'btc', 'eth') == {}
        w.state_store.close()

    def test_consecutive_miner_reserved_supersedes_prior_pin(self, tmp_path: Path):
        """If a fresh MinerReserved arrives while a pin from a prior reservation
        is still open (e.g. terminal event missed), the helper emits an end
        for the stale pin before laying down the new 'start'."""
        w = make_watcher(tmp_path, netuid=2, subtensor=MagicMock())
        first = make_pinned_commitment(rate_str='200', counter_rate_str='0.005')
        second = make_pinned_commitment(rate_str='250', counter_rate_str='0.004')
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=first,
        ):
            w.apply_event(900, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1000})
        with patch(
            'allways.validator.event_watcher.read_miner_commitment',
            return_value=second,
        ):
            w.apply_event(1100, 'MinerReserved', {'miner': 'hk_a', 'reserved_until': 1200})

        # At block 1100 the active pin should be the second one's rate, not
        # the first's. The first pin must have been closed by an 'end' before
        # the second 'start'.
        primary = w.get_reservation_pins_at(1150, 'btc', 'tao')
        assert primary == {'hk_a': 250.0}
        end_events = [
            ev for ev in w.reservation_pin_events if ev.hotkey == 'hk_a' and ev.kind == 'end' and ev.block_num == 1100
        ]
        assert {(ev.from_chain, ev.to_chain) for ev in end_events} == {('btc', 'tao'), ('tao', 'btc')}
        w.state_store.close()


class TestBootstrap:
    """initialize() snapshotting behavior — the M1 fix."""

    def test_bootstrap_seeds_active_from_contract(self, tmp_path: Path):
        from allways.constants import SCORING_WINDOW_BLOCKS

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.side_effect = lambda hk: hk == 'hk_a'

        current_block = SCORING_WINDOW_BLOCKS + 500  # well past the backfill floor
        w.initialize(current_block=current_block, metagraph_hotkeys=['hk_a', 'hk_b'], contract_client=client)

        assert w.active_miners == {'hk_a'}
        # Cursor rewinds one scoring window so sync_to backfills the crown-time history.
        assert w.cursor == current_block - SCORING_WINDOW_BLOCKS
        w.state_store.close()

    def test_bootstrap_tolerates_contract_read_failures(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.side_effect = RuntimeError('rpc down')

        # Pre-window start (current_block < SCORING_WINDOW_BLOCKS) — cursor clamps at 0.
        from allways.constants import SCORING_WINDOW_BLOCKS

        w.initialize(current_block=SCORING_WINDOW_BLOCKS // 2, metagraph_hotkeys=['hk_a'], contract_client=client)

        # Everything defaults to empty/starting state, no exception propagated
        assert w.active_miners == set()
        assert w.cursor == 0
        w.state_store.close()


class TestStateStoreEventTables:
    """Direct exercise of the new event-watcher tables on ValidatorStateStore."""

    def test_init_db_creates_event_tables(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        conn = store.require_connection()
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for expected in ('active_events', 'busy_events', 'event_watcher_meta', 'bootstrapped_swaps'):
            assert expected in names
        store.close()

    def test_init_db_is_idempotent(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_active_event(100, 'hk_a', True)
        store.close()
        store2 = ValidatorStateStore(db_path=tmp_path / 'state.db')
        assert store2.load_all_active_events() == [{'block_num': 100, 'hotkey': 'hk_a', 'active': True}]
        store2.close()

    def test_insert_and_load_active_events_round_trip(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_active_event(100, 'hk_a', True)
        store.insert_active_event(200, 'hk_b', False)
        store.insert_active_event(150, 'hk_a', False)
        loaded = store.load_all_active_events()
        assert loaded == [
            {'block_num': 100, 'hotkey': 'hk_a', 'active': True},
            {'block_num': 150, 'hotkey': 'hk_a', 'active': False},
            {'block_num': 200, 'hotkey': 'hk_b', 'active': False},
        ]
        store.close()

    def test_insert_and_load_busy_events_round_trip(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_busy_event(100, 'hk_a', +1, 7)
        store.insert_busy_event(150, 'hk_a', -1, 7)
        store.insert_busy_event(120, 'hk_b', +1, None)
        loaded = store.load_all_busy_events()
        assert loaded == [
            {'block_num': 100, 'hotkey': 'hk_a', 'delta': 1, 'swap_id': 7},
            {'block_num': 120, 'hotkey': 'hk_b', 'delta': 1, 'swap_id': None},
            {'block_num': 150, 'hotkey': 'hk_a', 'delta': -1, 'swap_id': 7},
        ]
        store.close()

    def test_event_cursor_default_is_none_then_round_trips(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        assert store.get_event_cursor() is None
        store.set_event_cursor(1234)
        assert store.get_event_cursor() == 1234
        store.set_event_cursor(5678)
        assert store.get_event_cursor() == 5678
        store.close()

    def test_prune_active_events_preserves_latest_per_hotkey(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_active_event(100, 'hk_a', True)
        store.insert_active_event(200, 'hk_a', False)
        store.insert_active_event(100, 'hk_b', True)
        store.prune_active_events(cutoff_block=300)
        remaining = store.load_all_active_events()
        # hk_a's (100, True) is dropped; (200, False) is its latest anchor.
        # hk_b's only row is preserved as its own anchor even though < cutoff.
        assert remaining == [
            {'block_num': 100, 'hotkey': 'hk_b', 'active': True},
            {'block_num': 200, 'hotkey': 'hk_a', 'active': False},
        ]
        store.close()

    def test_prune_busy_events_preserves_open_swaps(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_busy_event(100, 'hk_a', +1, 1)
        store.insert_busy_event(150, 'hk_a', -1, 1)  # hk_a SUM=0 → fully prunable
        store.insert_busy_event(100, 'hk_b', +1, 2)  # hk_b SUM=+1 → keep
        store.prune_busy_events(cutoff_block=200)
        remaining = store.load_all_busy_events()
        assert remaining == [{'block_num': 100, 'hotkey': 'hk_b', 'delta': 1, 'swap_id': 2}]
        store.close()

    def test_bootstrapped_swaps_add_remove_load(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.add_bootstrapped_swap(7)
        store.add_bootstrapped_swap(9)
        store.add_bootstrapped_swap(7)  # idempotent
        assert store.load_bootstrapped_swaps() == {7, 9}
        store.remove_bootstrapped_swap(7)
        assert store.load_bootstrapped_swaps() == {9}
        store.close()

    def test_reset_event_watcher_state_wipes_all_four_tables(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.insert_active_event(100, 'hk_a', True)
        store.insert_busy_event(100, 'hk_a', +1, 1)
        store.set_event_cursor(123)
        store.add_bootstrapped_swap(1)
        store.reset_event_watcher_state()
        assert store.load_all_active_events() == []
        assert store.load_all_busy_events() == []
        assert store.get_event_cursor() is None
        assert store.load_bootstrapped_swaps() == set()
        store.close()


class TestEventWatcherWarmRestart:
    """Persisted cursor branches initialize() into hydrate-from-DB vs. cold."""

    def make_swap(self, swap_id: int, hotkey: str, initiated_block: int):
        s = MagicMock()
        s.id = swap_id
        s.miner_hotkey = hotkey
        s.initiated_block = initiated_block
        return s

    def test_cold_bootstrap_writes_anchors_and_cursor(self, tmp_path: Path):
        from allways.constants import SCORING_WINDOW_BLOCKS

        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.side_effect = lambda hk: hk in {'hk_a', 'hk_b'}
        client.get_active_swaps.return_value = [self.make_swap(42, 'hk_a', initiated_block=950)]

        current_block = SCORING_WINDOW_BLOCKS + 500
        w.initialize(current_block=current_block, metagraph_hotkeys=['hk_a', 'hk_b'], contract_client=client)

        cursor_expected = current_block - SCORING_WINDOW_BLOCKS
        assert w.state_store.get_event_cursor() == cursor_expected
        loaded_active = w.state_store.load_all_active_events()
        assert {(r['hotkey'], r['active']) for r in loaded_active} == {('hk_a', True), ('hk_b', True)}
        loaded_busy = w.state_store.load_all_busy_events()
        assert loaded_busy == [{'block_num': 950, 'hotkey': 'hk_a', 'delta': 1, 'swap_id': 42}]
        assert w.state_store.load_bootstrapped_swaps() == {42}
        w.state_store.close()

    def test_warm_restart_hydrates_without_contract_reads(self, tmp_path: Path):
        from allways.constants import SCORING_WINDOW_BLOCKS

        # First boot: cold.
        w1 = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_active_flag.side_effect = lambda hk: hk == 'hk_a'
        client.get_active_swaps.return_value = [self.make_swap(99, 'hk_a', initiated_block=900)]
        current_block = SCORING_WINDOW_BLOCKS + 500
        w1.initialize(current_block=current_block, metagraph_hotkeys=['hk_a'], contract_client=client)
        w1.state_store.close()

        # Second boot: contract_client must NOT be called.
        w2 = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address=TEST_CONTRACT_ADDRESS,
            metadata_path=METADATA_PATH,
            state_store=ValidatorStateStore(db_path=tmp_path / 'state.db'),
        )
        strict_client = MagicMock()
        strict_client.get_miner_active_flag.side_effect = AssertionError('warm restart must not call contract')
        strict_client.get_active_swaps.side_effect = AssertionError('warm restart must not call contract')
        # Second boot at the same head — keeps gap within SCORING_WINDOW_BLOCKS.
        w2.initialize(current_block=current_block, metagraph_hotkeys=['hk_a'], contract_client=strict_client)

        assert w2.active_miners == {'hk_a'}
        assert w2.open_swap_count == {'hk_a': 1}
        assert w2.bootstrapped_swap_ids == {99}
        assert w2.cursor == current_block - SCORING_WINDOW_BLOCKS
        w2.state_store.close()

    def test_cold_bootstrap_is_idempotent_against_crashed_prior_boot(self, tmp_path: Path):
        """A cold boot that wrote anchors but died before the cursor write must
        not leave duplicate anchors when the next cold boot runs."""
        from allways.constants import SCORING_WINDOW_BLOCKS

        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        # Simulate a crashed prior cold boot: anchor rows present, no cursor.
        anchor_block = (SCORING_WINDOW_BLOCKS + 500) - SCORING_WINDOW_BLOCKS
        store.insert_active_event(anchor_block, 'hk_a', True)
        store.add_bootstrapped_swap(7)
        assert store.get_event_cursor() is None  # cursor never landed
        store.close()

        w = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address=TEST_CONTRACT_ADDRESS,
            metadata_path=METADATA_PATH,
            state_store=ValidatorStateStore(db_path=tmp_path / 'state.db'),
        )
        client = MagicMock()
        client.get_miner_active_flag.side_effect = lambda hk: hk == 'hk_a'
        client.get_active_swaps.return_value = []
        w.initialize(current_block=SCORING_WINDOW_BLOCKS + 500, metagraph_hotkeys=['hk_a'], contract_client=client)

        # Exactly one anchor for hk_a — the stale row was wiped, not duplicated.
        rows = w.state_store.load_all_active_events()
        assert rows == [{'block_num': anchor_block, 'hotkey': 'hk_a', 'active': True}]
        # Orphaned bootstrapped swap from the crashed boot is gone.
        assert w.state_store.load_bootstrapped_swaps() == set()
        w.state_store.close()

    def test_warm_restart_rebuilds_open_swap_count_with_multiple_swaps(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.set_event_cursor(1000)
        store.insert_busy_event(500, 'hk_a', +1, 1)
        store.insert_busy_event(600, 'hk_a', +1, 2)
        store.close()

        w = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address=TEST_CONTRACT_ADDRESS,
            metadata_path=METADATA_PATH,
            state_store=ValidatorStateStore(db_path=tmp_path / 'state.db'),
        )
        w.initialize(current_block=1100)
        assert w.open_swap_count == {'hk_a': 2}
        w.state_store.close()

    def test_warm_restart_drops_closed_hotkeys_from_open_swap_count(self, tmp_path: Path):
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.set_event_cursor(1000)
        store.insert_busy_event(500, 'hk_a', +1, 1)
        store.insert_busy_event(600, 'hk_a', -1, 1)
        store.close()

        w = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address=TEST_CONTRACT_ADDRESS,
            metadata_path=METADATA_PATH,
            state_store=ValidatorStateStore(db_path=tmp_path / 'state.db'),
        )
        w.initialize(current_block=1100)
        assert 'hk_a' not in w.open_swap_count
        w.state_store.close()

    def test_long_outage_falls_back_to_cold_bootstrap(self, tmp_path: Path):
        from allways.constants import SCORING_WINDOW_BLOCKS

        # Persist a stale cursor far behind head.
        store = ValidatorStateStore(db_path=tmp_path / 'state.db')
        store.set_event_cursor(100)
        store.insert_active_event(50, 'hk_old', True)
        store.close()

        w = ContractEventWatcher(
            substrate=MagicMock(),
            contract_address=TEST_CONTRACT_ADDRESS,
            metadata_path=METADATA_PATH,
            state_store=ValidatorStateStore(db_path=tmp_path / 'state.db'),
        )
        client = MagicMock()
        client.get_miner_active_flag.side_effect = lambda hk: hk == 'hk_new'
        client.get_active_swaps.return_value = []
        current_block = 100 + SCORING_WINDOW_BLOCKS + 50
        w.initialize(current_block=current_block, metagraph_hotkeys=['hk_new'], contract_client=client)

        # Stale rows were wiped; new cold-bootstrap anchor for hk_new exists.
        loaded = w.state_store.load_all_active_events()
        assert {r['hotkey'] for r in loaded} == {'hk_new'}
        assert w.active_miners == {'hk_new'}
        assert w.cursor == current_block - SCORING_WINDOW_BLOCKS
        w.state_store.close()

    def test_terminal_event_removes_bootstrapped_swap_from_db(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.state_store.add_bootstrapped_swap(42)
        w.bootstrapped_swap_ids.add(42)
        w.apply_event(2000, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a', 'tao_amount': 100})
        assert 42 not in w.state_store.load_bootstrapped_swaps()
        assert 42 not in w.bootstrapped_swap_ids
        w.state_store.close()


class TestEventWatcherWriteThrough:
    """In-memory transitions also persist."""

    def test_record_active_transition_writes_to_db(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.record_active_transition(500, 'hk_a', True)
        loaded = w.state_store.load_all_active_events()
        assert loaded == [{'block_num': 500, 'hotkey': 'hk_a', 'active': True}]
        w.state_store.close()

    def test_apply_busy_delta_writes_to_db_with_swap_id(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_busy_delta(500, 'hk_a', +1, swap_id=7)
        loaded = w.state_store.load_all_busy_events()
        assert loaded == [{'block_num': 500, 'hotkey': 'hk_a', 'delta': 1, 'swap_id': 7}]
        w.state_store.close()

    def test_process_block_advances_cursor_per_block(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.substrate.get_block_hash.side_effect = lambda b: f'0x{b:064x}'
        w.substrate.get_events.return_value = []
        w.cursor = 99
        w.sync_to(101)
        assert w.cursor == 101
        assert w.state_store.get_event_cursor() == 101
        w.state_store.close()


class TestEventWatcherLogHygiene:
    """Pruned-block errors collapse into a single summary line."""

    def test_pruned_block_error_increments_counter_silently(self, tmp_path: Path, caplog):
        import logging

        w = make_watcher(tmp_path)
        w.substrate.get_block_hash.side_effect = RuntimeError(
            'Other error: -32603: Unable to fetch block at hash 0x...: State already discarded'
        )
        w.cursor = 0
        caplog.set_level(logging.INFO)
        w.sync_to(5)
        assert w.pruned_block_count == 5
        assert w.pruned_block_first == 1
        assert w.pruned_block_last == 5
        # Cursor MUST advance through pruned blocks — they are permanently
        # unavailable, so stalling here would never reach the live region.
        assert w.cursor == 5
        assert w.state_store.get_event_cursor() == 5
        w.state_store.close()

    def test_cursor_does_not_stall_on_pruned_cold_start_region(self, tmp_path: Path):
        """Regression: a cold start whose first blocks are all pruned must
        still march the cursor forward until it reaches a live block, rather
        than looping on the first pruned block forever."""
        from allways.validator import event_watcher as ew_module

        w = make_watcher(tmp_path)
        original_chunk = ew_module.MAX_BLOCKS_PER_SYNC
        ew_module.MAX_BLOCKS_PER_SYNC = 100
        try:
            live_from = 60

            def events_for(block_hash):
                block = int(block_hash, 16)
                if block < live_from:
                    raise RuntimeError('State already discarded')
                return []

            w.substrate.get_block_hash.side_effect = lambda block: f'0x{block:064x}'
            w.substrate.get_events.side_effect = events_for
            w.cursor = 0
            w.sync_to(80)
            # Walked through the 1..59 pruned zone and on to the live tail.
            assert w.cursor == 80
            assert w.state_store.get_event_cursor() == 80
        finally:
            ew_module.MAX_BLOCKS_PER_SYNC = original_chunk
        w.state_store.close()

    def test_unrelated_exception_holds_cursor_for_retry(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.substrate.get_block_hash.side_effect = RuntimeError('connection refused')
        w.cursor = 0
        w.sync_to(2)
        # Transient error is not pruned-state — counter stays at zero AND the
        # cursor holds so the block is retried next sync.
        assert w.pruned_block_count == 0
        assert w.cursor == 0
        w.state_store.close()

    def test_pruned_block_counter_resets_between_sync_calls(self, tmp_path: Path):
        from allways.validator import event_watcher as ew_module

        w = make_watcher(tmp_path)
        original_chunk = ew_module.MAX_BLOCKS_PER_SYNC
        ew_module.MAX_BLOCKS_PER_SYNC = 10
        try:
            calls = {'n': 0}

            def hash_for(block):
                calls['n'] += 1
                if calls['n'] <= 3:
                    raise RuntimeError('State already discarded')
                return f'0x{block:064x}'

            w.substrate.get_block_hash.side_effect = hash_for
            w.substrate.get_events.return_value = []
            w.cursor = 0
            w.sync_to(10)
            assert w.pruned_block_count == 3
            w.sync_to(20)
            # All later blocks succeed → counter should reset to zero.
            assert w.pruned_block_count == 0
        finally:
            ew_module.MAX_BLOCKS_PER_SYNC = original_chunk
        w.state_store.close()


class TestSwapOutcomesIdempotency:
    """Re-applying terminal events doesn't duplicate swap_outcomes rows."""

    def test_replaying_swap_completed_does_not_duplicate_outcome(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(1000, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a', 'tao_amount': 500})
        w.apply_event(1000, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a', 'tao_amount': 500})
        rows = w.state_store.get_success_rates_since(0)
        # Two SwapCompleted apply()s, but swap_outcomes is keyed by swap_id (INSERT OR REPLACE).
        assert rows.get('hk_a') == (1, 0)
        w.state_store.close()
