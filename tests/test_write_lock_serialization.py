"""Tests for shared write_lock serialization in AllwaysContractClient (issue #457).

Verifies that:
- Two clients sharing a write_lock can be created (wiring test).
- A client without a write_lock still works (backward compat).
- The write_lock is held during exec_contract_raw's submit path.
- The account balance read (pre-flight) is NOT blocked by the write_lock.
"""

import threading
from contextlib import contextmanager
from unittest.mock import MagicMock, patch, call


def make_client(write_lock=None, substrate_lock=None):
    from allways.contract_client import AllwaysContractClient

    subtensor = MagicMock()
    subtensor.substrate = MagicMock()
    return AllwaysContractClient(
        contract_address='5FakeContractAddress',
        subtensor=subtensor,
        write_lock=write_lock,
        substrate_lock=substrate_lock,
    )


class TestWriteLockWiring:
    """Shared and private write_lock construction."""

    def test_no_write_lock_creates_client(self):
        client = make_client()
        assert client._write_lock is None

    def test_shared_write_lock_stored(self):
        lock = threading.Lock()
        client = make_client(write_lock=lock)
        assert client._write_lock is lock

    def test_two_clients_share_same_write_lock(self):
        lock = threading.Lock()
        c1 = make_client(write_lock=lock)
        c2 = make_client(write_lock=lock)
        assert c1._write_lock is c2._write_lock is lock


class TestWriteLockHeldDuringSubmit:
    """exec_contract_raw must hold write_lock across nonce-fetch → submit → inclusion."""

    def _make_receipt(self, success=True):
        receipt = MagicMock()
        receipt.is_success = success
        receipt.extrinsic_hash = '0xabc'
        return receipt

    def test_write_lock_is_acquired_during_submit(self):
        """write_lock must be held when substrate_call(submit_extrinsic) runs."""
        lock = threading.Lock()
        lock_held_during_submit = []

        def fake_substrate_call(fn):
            lock_held_during_submit.append(not lock.acquire(blocking=False))
            if not lock_held_during_submit[-1]:
                lock.release()
            return self._make_receipt()

        client = make_client(write_lock=lock)
        client.initialized = True

        # Patch balance read and the actual substrate_call used for submit
        with (
            patch.object(client, 'substrate_call', side_effect=fake_substrate_call),
            patch.object(client, 'encode_args', return_value=b''),
        ):
            from allways.contract_client import CONTRACT_SELECTORS
            CONTRACT_SELECTORS.setdefault('vote_reserve', b'\x01\x02\x03\x04')
            client.exec_contract_raw('vote_reserve', args={}, keypair=MagicMock(ss58_address='5Fake'))

        # The lock was held (acquire returned False = already locked) when submit ran
        assert any(lock_held_during_submit), 'write_lock must be held during substrate_call submit'

    def test_no_write_lock_does_not_raise(self):
        """Client without write_lock must still complete exec_contract_raw normally."""
        client = make_client()
        client.initialized = True

        receipt = self._make_receipt()

        with (
            patch.object(client, 'substrate_call', return_value=receipt),
            patch.object(client, 'encode_args', return_value=b''),
        ):
            from allways.contract_client import CONTRACT_SELECTORS
            CONTRACT_SELECTORS.setdefault('vote_reserve', b'\x01\x02\x03\x04')
            result = client.exec_contract_raw('vote_reserve', args={}, keypair=MagicMock(ss58_address='5Fake'))

        assert result == '0xabc'


class TestReadPathNotBlocked:
    """Account balance read (pre-flight) must not require the write_lock."""

    def test_substrate_call_read_runs_before_write_lock(self):
        """substrate_call for the balance read fires before the write_lock is acquired.

        Uses a recording wrapper around threading.Lock to track acquire order.
        """
        class _RecordingLock:
            """Thin wrapper around threading.Lock that records acquire/release."""

            def __init__(self):
                self._inner = threading.Lock()
                self.events = []

            def acquire(self, *args, **kwargs):
                self.events.append('lock_acquire')
                return self._inner.acquire(*args, **kwargs)

            def release(self):
                self._inner.release()

            def __enter__(self):
                self.acquire()
                return self

            def __exit__(self, *_):
                self.release()

        rec_lock = _RecordingLock()
        call_count = [0]

        def fake_substrate_call(fn):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call is the balance read — lock should NOT be held yet
                rec_lock.events.append('balance_read')
                account_info = MagicMock()
                account_info.value = {'data': {'free': 10**12}}
                return account_info
            # Second call is the submit — lock IS held
            rec_lock.events.append('submit')
            receipt = MagicMock()
            receipt.is_success = True
            receipt.extrinsic_hash = '0xdef'
            return receipt

        client = make_client(write_lock=rec_lock)
        client.initialized = True

        with (
            patch.object(client, 'substrate_call', side_effect=fake_substrate_call),
            patch.object(client, 'encode_args', return_value=b''),
        ):
            from allways.contract_client import CONTRACT_SELECTORS
            CONTRACT_SELECTORS.setdefault('vote_reserve', b'\x01\x02\x03\x04')
            client.exec_contract_raw('vote_reserve', args={}, keypair=MagicMock(ss58_address='5Fake'))

        # balance_read must come before lock_acquire
        events = rec_lock.events
        assert 'balance_read' in events, 'balance read substrate_call not observed'
        assert 'lock_acquire' in events, 'write_lock was never acquired'
        assert events.index('balance_read') < events.index('lock_acquire'), (
            f'balance read should precede write_lock acquisition; order={events}'
        )
