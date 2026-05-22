"""fetch_miner_reliability / reliability_text: per-miner swap success aggregation.

Pins the aggregation behind the Reliability column in `alw view rates` and the
`swap now` miner picker: per-direction completed/total over a 30-day window,
unresolved statuses excluded, a graceful None when the API is down, and the
page-shape guard against a JSON error object served instead of a list.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import requests

from allways.cli.swap_commands import helpers
from allways.cli.swap_commands.helpers import fetch_miner_reliability, reliability_text


def _iso(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def _swap(miner: str, src: str, dst: str, status: str, age_days: float = 1) -> dict:
    ts = _iso(datetime.now(timezone.utc) - timedelta(days=age_days))
    return {
        'minerHotkey': miner,
        'sourceChain': src,
        'destChain': dst,
        'status': status,
        'createdAt': ts,
        'resolvedAt': ts,
    }


def _mock_get(pages: list):
    """Fake requests.get that serves `pages` by offset, then empty pages."""

    def _get(url, params=None, headers=None, timeout=None):
        idx = (params or {}).get('offset', 0) // 50
        resp = MagicMock()
        resp.json.return_value = pages[idx] if idx < len(pages) else []
        resp.raise_for_status.return_value = None
        return resp

    return _get


def test_aggregates_per_miner_per_direction(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    page = [
        _swap('5A', 'tao', 'btc', 'COMPLETED'),
        _swap('5A', 'tao', 'btc', 'COMPLETED'),
        _swap('5A', 'tao', 'btc', 'TIMED_OUT'),
        _swap('5A', 'btc', 'tao', 'COMPLETED'),
        _swap('5B', 'tao', 'btc', 'TIMED_OUT'),
    ]
    with patch.object(helpers.requests, 'get', _mock_get([page])):
        stats = fetch_miner_reliability(use_cache=False)
    assert stats['5A']['tao->btc'] == (2, 3)
    assert stats['5A']['btc->tao'] == (1, 1)
    assert stats['5B']['tao->btc'] == (0, 1)


def test_excludes_swaps_outside_window(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    page = [
        _swap('5A', 'tao', 'btc', 'COMPLETED', age_days=2),
        _swap('5A', 'tao', 'btc', 'TIMED_OUT', age_days=40),  # older than the 30d window
    ]
    with patch.object(helpers.requests, 'get', _mock_get([page])):
        stats = fetch_miner_reliability(window_days=30, use_cache=False)
    assert stats['5A']['tao->btc'] == (1, 1)  # the 40-day-old swap is dropped


def test_ignores_unresolved_statuses(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)
    page = [
        _swap('5A', 'tao', 'btc', 'COMPLETED'),
        _swap('5A', 'tao', 'btc', 'ACTIVE'),
        _swap('5A', 'tao', 'btc', 'REFUNDED'),
    ]
    with patch.object(helpers.requests, 'get', _mock_get([page])):
        stats = fetch_miner_reliability(use_cache=False)
    assert stats['5A']['tao->btc'] == (1, 1)  # only COMPLETED/TIMED_OUT count


def test_returns_none_when_api_unreachable(tmp_path, monkeypatch):
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)

    def _boom(*args, **kwargs):
        raise requests.ConnectionError('indexer down')

    with patch.object(helpers.requests, 'get', _boom):
        assert fetch_miner_reliability(use_cache=False) is None


def test_json_error_object_does_not_crash(tmp_path, monkeypatch):
    """A dict error body instead of a list must be treated as no data, not iterated."""
    monkeypatch.setattr(helpers, 'ALLWAYS_DIR', tmp_path)

    def _get(url, params=None, headers=None, timeout=None):
        resp = MagicMock()
        resp.json.return_value = {'error': 'Not Found', 'statusCode': 404}
        resp.raise_for_status.return_value = None
        return resp

    with patch.object(helpers.requests, 'get', _get):
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
