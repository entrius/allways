from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.classes import MinerPair
from allways.constants import SCORING_WINDOW_BLOCKS
from allways.validator.forward import poll_commitments
from allways.validator.state_store import ValidatorStateStore


def make_pair(hotkey: str, rate: float, counter_rate: float, source='tao', dest='btc') -> MinerPair:
    return MinerPair(
        uid=0,
        hotkey=hotkey,
        from_chain=source,
        from_address='src_addr',
        to_chain=dest,
        to_address='dst_addr',
        rate=rate,
        rate_str=str(rate),
        counter_rate=counter_rate,
        counter_rate_str=str(counter_rate),
    )


def make_validator(tmp_path: Path, hotkeys=None) -> SimpleNamespace:
    """Construct a validator stub with the fields used by poll_commitments.

    Includes a MagicMock event_watcher defensively — poll_commitments
    doesn't touch it today, but if a helper ever does the tests should
    AttributeError loud, not silently pass or break confusingly.
    """
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    metagraph = SimpleNamespace(hotkeys=list(hotkeys or ['hk_a', 'hk_b']))
    config = SimpleNamespace(netuid=2)
    return SimpleNamespace(
        block=1000,
        subtensor=MagicMock(),
        config=config,
        metagraph=metagraph,
        state_store=store,
        contract_client=MagicMock(),
        event_watcher=MagicMock(),
        last_known_rates={},
    )


class TestPollCommitmentsBasic:
    def test_first_poll_inserts_both_directions_per_miner(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [
            make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        # 4 rate_events: 2 miners * 2 directions
        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 2000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 2000)
        assert len(tao_btc) == 2
        assert len(btc_tao) == 2
        assert v.last_known_rates == {
            ('hk_a', 'tao', 'btc'): 0.00015,
            ('hk_a', 'btc', 'tao'): 6500.0,
            ('hk_b', 'tao', 'btc'): 0.00016,
            ('hk_b', 'btc', 'tao'): 6400.0,
        }
        v.state_store.close()


class TestPollCommitmentsChanges:
    def test_no_changes_across_polls_inserts_nothing_extra(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert len(tao_btc) == 1
        v.state_store.close()

    def test_rate_change_inserts_new_event_every_block(self, tmp_path: Path):
        """Per-block polling — a rate change is recorded immediately with no
        throttle delay."""
        v = make_validator(tmp_path)
        pairs_v1 = [make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]
        pairs_v2 = [make_pair('hk_a', rate=0.00020, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v1):
            poll_commitments(v)

        # A single block later — the throttle is gone, so the change lands.
        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v2):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.00020]
        # counter rate unchanged → still only one event
        assert [e['rate'] for e in btc_tao] == [6500.0]
        v.state_store.close()


class TestPollCommitmentsZeroRate:
    def test_zero_rate_skips_only_that_direction(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [make_pair('hk_a', rate=0.00015, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert len(tao_btc) == 1
        assert len(btc_tao) == 0
        assert ('hk_a', 'tao', 'btc') in v.last_known_rates
        assert ('hk_a', 'btc', 'tao') not in v.last_known_rates
        v.state_store.close()


class TestPollCommitmentsDereg:
    def test_dereg_removes_hotkey_from_store_and_cache(self, tmp_path: Path):
        v = make_validator(tmp_path, hotkeys=['hk_a', 'hk_b'])
        pairs = [
            make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        # hk_b deregistered
        v.metagraph.hotkeys = ['hk_a']
        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        assert v.state_store.get_latest_rate_before('hk_b', 'tao', 'btc', block=10_000) is None
        assert v.state_store.get_latest_rate_before('hk_a', 'tao', 'btc', block=10_000) is not None
        assert all(k[0] != 'hk_b' for k in v.last_known_rates.keys())
        v.state_store.close()


class TestPollCommitmentsErrors:
    def test_read_raises_logs_and_returns_cleanly(self, tmp_path: Path):
        v = make_validator(tmp_path)

        def raiser(*args, **kwargs):
            raise RuntimeError('websocket dead')

        with patch('allways.validator.forward.read_miner_commitments', side_effect=raiser):
            poll_commitments(v)

        # No events persisted on a failed read.
        assert v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) == []
        v.state_store.close()


class TestPollCommitmentsPruning:
    def test_prune_runs_via_scoring_pass_not_commitment_poll(self, tmp_path: Path):
        """Pruning moved out of the per-tick path and into the scoring round —
        verify both parts of that contract: commitment polling does NOT prune,
        and run_scoring_pass DOES. The latest row per (hotkey, direction) is
        preserved as a state-reconstruction anchor even when it's older than
        the cutoff, so this test uses a hotkey with two rows to exercise the
        "older one drops, newer one survives" path."""
        from allways.validator.scoring import prune_aged_rate_events

        v = make_validator(tmp_path)
        v.block = SCORING_WINDOW_BLOCKS + 1_000
        ancient_block = 1
        recent_block = v.block - 100

        conn = v.state_store.require_connection()
        # Two rows for the same direction — the ancient one must drop on prune
        # while the recent one survives.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00010, ancient_block),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, recent_block),
        )
        conn.commit()

        # 1. Commitment polling no longer prunes — both rows survive.
        with patch('allways.validator.forward.read_miner_commitments', return_value=[]):
            poll_commitments(v)
        surviving_blocks = {e['block'] for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, v.block + 1)}
        assert ancient_block in surviving_blocks, 'poll_commitments should not prune'
        assert recent_block in surviving_blocks

        # 2. Scoring pass prunes the ancient row; the latest row stays as anchor.
        prune_aged_rate_events(v)
        surviving_blocks = {e['block'] for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, v.block + 1)}
        assert ancient_block not in surviving_blocks
        assert recent_block in surviving_blocks

        v.state_store.close()
