"""dev_signal — the T3 NDJSON emitter and file-backed fault flags. Both must be inert
(no file writes, no faults) when their env vars are unset, and must never raise."""

import json
from unittest.mock import MagicMock

from allways import dev_signal
from allways.miner.fulfillment import SwapFulfiller

from .test_fulfillment import make_swap


class TestEmit:
    def test_noop_without_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_SIGNAL', raising=False)
        dev_signal.emit('vote_cast', swap_key='ab', sig='s1')
        assert list(tmp_path.iterdir()) == []

    def test_appends_ndjson_lines(self, tmp_path, monkeypatch):
        out = tmp_path / 'signal.ndjson'
        monkeypatch.setenv('ALLWAYS_DEV_SIGNAL', str(out))
        dev_signal.emit('vote_cast', swap_key='ab', sig='s1')
        dev_signal.emit('d1_reject', swap_key='cd', expected=100, got=99)
        lines = [json.loads(line) for line in out.read_text().splitlines()]
        assert [line['event'] for line in lines] == ['vote_cast', 'd1_reject']
        assert lines[0]['swap_key'] == 'ab' and 'ts' in lines[0]
        assert lines[1]['expected'] == 100

    def test_non_json_fields_stringified(self, tmp_path, monkeypatch):
        out = tmp_path / 'signal.ndjson'
        monkeypatch.setenv('ALLWAYS_DEV_SIGNAL', str(out))
        dev_signal.emit('decision', key=b'\x2a')
        assert json.loads(out.read_text())['event'] == 'decision'

    def test_unwritable_path_never_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv('ALLWAYS_DEV_SIGNAL', str(tmp_path / 'no' / 'such' / 'dir' / 'f'))
        dev_signal.emit('vote_cast')  # must not raise


class TestFault:
    def test_false_without_env(self, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_FAULTS', raising=False)
        assert not dev_signal.fault('withhold_dest')

    def test_false_when_file_missing_or_empty(self, tmp_path, monkeypatch):
        flags = tmp_path / 'faults'
        monkeypatch.setenv('ALLWAYS_DEV_FAULTS', str(flags))
        assert not dev_signal.fault('withhold_dest')
        flags.write_text('')
        assert not dev_signal.fault('withhold_dest')

    def test_toggles_at_runtime_via_file(self, tmp_path, monkeypatch):
        flags = tmp_path / 'faults'
        monkeypatch.setenv('ALLWAYS_DEV_FAULTS', str(flags))
        flags.write_text('withhold_dest\nother_flag')
        assert dev_signal.fault('withhold_dest')
        assert dev_signal.fault('other_flag')
        assert not dev_signal.fault('unrelated')
        flags.write_text('')
        assert not dev_signal.fault('withhold_dest')


class TestWithholdDestFault:
    def test_withhold_blocks_send_and_emits_refuse(self, tmp_path, monkeypatch):
        flags, out = tmp_path / 'faults', tmp_path / 'signal.ndjson'
        flags.write_text('withhold_dest')
        monkeypatch.setenv('ALLWAYS_DEV_FAULTS', str(flags))
        monkeypatch.setenv('ALLWAYS_DEV_SIGNAL', str(out))
        provider = MagicMock()
        f = SwapFulfiller(solana_client=MagicMock(), chain_providers={'tao': provider})
        assert f.send_dest_funds(make_swap(), 341_550_000) is None
        provider.send_amount.assert_not_called()
        refuse = json.loads(out.read_text())
        assert refuse['event'] == 'refuse' and refuse['reason'] == 'withhold_dest'

    def test_send_proceeds_without_fault(self, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_FAULTS', raising=False)
        provider = MagicMock()
        provider.send_amount.return_value = ('txhash', 7)
        f = SwapFulfiller(solana_client=MagicMock(), chain_providers={'tao': provider})
        assert f.send_dest_funds(make_swap(), 341_550_000) == ('txhash', 7)
