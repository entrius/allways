"""dev_signal — the T3 NDJSON emitter and file-backed fault flags. Both must be inert
(no file writes, no faults) when their env vars are unset, and must never raise."""

from unittest.mock import MagicMock

from allways import dev_signal
from allways.miner.fulfillment import SwapFulfiller

from .test_fulfillment import make_swap


class TestEmit:
    def test_noop_without_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_SIGNAL', raising=False)
        dev_signal.emit('vote_cast', swap_key='ab', sig='s1')
        assert list(tmp_path.iterdir()) == []

    def test_unwritable_path_never_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv('ALLWAYS_DEV_SIGNAL', str(tmp_path / 'no' / 'such' / 'dir' / 'f'))
        dev_signal.emit('vote_cast')  # must not raise


class TestFault:
    def test_false_without_env(self, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_FAULTS', raising=False)
        assert not dev_signal.fault('withhold_dest')


class TestWithholdDestFault:
    def test_send_proceeds_without_fault(self, monkeypatch):
        monkeypatch.delenv('ALLWAYS_DEV_FAULTS', raising=False)
        provider = MagicMock()
        provider.send_amount.return_value = ('txhash', 7)
        f = SwapFulfiller(solana_client=MagicMock(), assets={'tao': provider})
        assert f.send_dest_funds(make_swap(), 341_550_000) == ('txhash', 7)
