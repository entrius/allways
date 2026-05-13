"""Unit tests for ContractEventWatcher.

Covers three layers:
  1. ``load_event_registry`` — metadata JSON → registry dict
  2. ``decode_data_fields`` / ``decode_topic_fields`` — raw bytes → values
     (driven by hand-encoded SCALE fixtures so we don't need a live node)
  3. ``apply_event`` — state transitions once events are decoded
"""

import struct
from pathlib import Path
from unittest.mock import MagicMock

from bittensor.utils import ss58_decode

from allways.validator.event_watcher import (
    ContractEventWatcher,
    EventDef,
    FieldDef,
    decode_data_fields,
    decode_topic_fields,
    load_event_registry,
)
from allways.validator.state_store import PendingConfirm, ValidatorStateStore

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'

# Well-known test SS58 — Alice from the substrate dev keyring. Used as
# contract_address in fixtures so the decoder's address comparison doesn't
# receive a garbage string that could bypass validation on future codepaths.
TEST_CONTRACT_ADDRESS = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_watcher(tmp_path: Path) -> ContractEventWatcher:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return ContractEventWatcher(
        substrate=MagicMock(),
        contract_address=TEST_CONTRACT_ADDRESS,
        metadata_path=METADATA_PATH,
        state_store=store,
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


class TestReservationExtended:
    def test_event_bumps_queued_reserved_until(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.state_store.enqueue(
            PendingConfirm(
                miner_hotkey='hk_a',
                from_tx_hash='tx',
                from_chain='btc',
                to_chain='tao',
                from_address='u',
                to_address='u',
                tao_amount=1,
                from_amount=1,
                to_amount=1,
                miner_from_address='m',
                miner_to_address='m',
                rate_str='1',
                reserved_until=100,
            )
        )

        w.apply_event(101, 'ReservationExtended', {'miner': 'hk_a', 'reserved_until': 300})

        assert w.state_store.get_all()[0].reserved_until == 300
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
        w.initialize(current_block=500, metagraph_hotkeys=['hk_a'], contract_client=client)

        # Everything defaults to empty/starting state, no exception propagated
        assert w.active_miners == set()
        assert w.cursor == 0
        w.state_store.close()
