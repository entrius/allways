"""fetch_miner_reliability / reliability_text: per-miner swap success aggregation.

Pins the Reliability column in `alw view rates` and the `swap now` miner picker:
per-direction completed/total reshaped from the API's pre-aggregated
`/miners/reliability` response, a graceful None when the API is down, and the
shape guard against a JSON error object served instead of a list.
"""

from unittest.mock import MagicMock, patch

import requests

from allways.cli.swap_commands import helpers
from allways.cli.swap_commands.helpers import fetch_miner_reliability, reliability_text


def _row(miner: str, src: str, dst: str, completed: int, total: int) -> dict:
    return {'minerHotkey': miner, 'sourceChain': src, 'destChain': dst, 'completed': completed, 'total': total}


def _mock_get(payload):
    """Fake requests.get serving a fixed JSON payload from /miners/reliability."""

    def _get(url, params=None, headers=None, timeout=None):
        resp = MagicMock()
        resp.json.return_value = payload
        resp.raise_for_status.return_value = None
        return resp

    return _get


def test_reshapes_per_miner_per_direction(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    payload = [
        _row('5A', 'tao', 'btc', 2, 3),
        _row('5A', 'btc', 'tao', 1, 1),
        _row('5B', 'tao', 'btc', 0, 1),
    ]
    with patch.object(helpers.requests, 'get', _mock_get(payload)):
        stats = fetch_miner_reliability(use_cache=False)
    assert stats['5A']['tao->btc'] == (2, 3)
    assert stats['5A']['btc->tao'] == (1, 1)
    assert stats['5B']['tao->btc'] == (0, 1)


def test_skips_rows_missing_hotkey_or_direction(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    payload = [
        _row('5A', 'tao', 'btc', 1, 1),
        {'minerHotkey': None, 'sourceChain': 'tao', 'destChain': 'btc', 'completed': 1, 'total': 1},
        {'minerHotkey': '5B', 'sourceChain': 'tao', 'completed': 1, 'total': 1},  # no destChain
    ]
    with patch.object(helpers.requests, 'get', _mock_get(payload)):
        stats = fetch_miner_reliability(use_cache=False)
    assert stats == {'5A': {'tao->btc': (1, 1)}}


def test_returns_none_when_api_unreachable(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)

    def _boom(*args, **kwargs):
        raise requests.ConnectionError('indexer down')

    with patch.object(helpers.requests, 'get', _boom):
        assert fetch_miner_reliability(use_cache=False) is None


def test_json_error_object_does_not_crash(tmp_path, monkeypatch):
    """A dict error body instead of a list must be treated as no data, not iterated."""
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    with patch.object(helpers.requests, 'get', _mock_get({'error': 'Not Found', 'statusCode': 404})):
        assert fetch_miner_reliability(use_cache=False) == {}


def test_reliability_text_colors_and_formats():
    rel = {'5A': {'tao->btc': (9, 10), 'btc->tao': (1, 4)}}

    high = reliability_text('5A', 'tao', 'btc', rel)
    assert high.plain == '9/10'
    assert high.style == 'green'

    low = reliability_text('5A', 'btc', 'tao', rel)
    assert low.plain == '1/4'
    assert low.style == 'red'

    # no resolved swap for this miner/direction → dim dash
    assert reliability_text('5Z', 'tao', 'btc', rel).plain == '—'
    # API unavailable → dim dash
    assert reliability_text('5A', 'tao', 'btc', None).plain == '—'
