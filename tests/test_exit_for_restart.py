import os
import threading

import pytest

from neurons.base.neuron import exit_for_restart


def test_exit_for_restart_raises_and_arms_a_daemon_hard_exit(monkeypatch):
    armed = {}

    class FakeTimer:
        def __init__(self, secs, fn):
            armed['secs'], armed['fn'] = secs, fn
            self.daemon = False

        def start(self):
            armed['daemon'] = self.daemon

    hard = []
    monkeypatch.setattr(threading, 'Timer', FakeTimer)
    monkeypatch.setattr(os, '_exit', lambda code: hard.append(code))

    with pytest.raises(SystemExit) as raised:
        exit_for_restart('Forward progress stalled for 655s', grace_secs=30)

    assert raised.value.code == 1
    assert armed['secs'] == 30 and armed['daemon'] is True
    armed['fn']()
    assert hard == [1]


def test_exit_for_restart_kills_a_process_held_by_a_wedged_thread(tmp_path):
    import subprocess
    import sys

    script = tmp_path / 'wedged.py'
    script.write_text(
        'import threading\n'
        'from neurons.base.neuron import exit_for_restart\n'
        'threading.Thread(target=threading.Event().wait).start()\n'
        'exit_for_restart("stalled", grace_secs=0.5)\n'
    )
    done = subprocess.run([sys.executable, str(script)], timeout=20, capture_output=True)
    assert done.returncode == 1
