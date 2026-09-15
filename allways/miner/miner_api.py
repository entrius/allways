"""Read-only calls to the allways API for the quote optimizer's emissions-eligibility alerts.

Validators keep two gates off chain: a completed fill per purse inside ``ELIGIBILITY_FILL_WINDOW_SECS``,
and whether a lane's pair is live (a qualified fill in the pool window, so its pool is above 0). The
optimizer reads them here. Every call returns None when the API is unreachable or answers with
something unexpected, and the caller skips those reasons rather than guess.
"""

import os
from typing import Dict, Optional, Tuple

import bittensor as bt
import requests

import allways
from allways.constants import NETUID_FINNEY

MAINNET_API_URL = 'https://api.all-ways.io'
TESTNET_API_URL = 'https://test-api.all-ways.io'
API_TIMEOUT_SECS = 10


def resolve_api_url(netuid: int) -> str:
    """``ALLWAYS_API_URL`` (the CLI's override) wins; otherwise the deployment matching the netuid."""
    override = os.environ.get('ALLWAYS_API_URL')
    if override:
        return override.rstrip('/')
    return MAINNET_API_URL if int(netuid) == NETUID_FINNEY else TESTNET_API_URL


class AllwaysApi:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        # The API rejects unknown user agents; identify ourselves explicitly.
        self.headers = {'User-Agent': f'allways-miner/{allways.__version__}'}

    def get(self, path: str, params: Optional[dict] = None):
        try:
            resp = requests.get(f'{self.base_url}{path}', params=params, headers=self.headers, timeout=API_TIMEOUT_SECS)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            bt.logging.debug(f'allways API {path} unavailable: {e}')
            return None

    def last_fill_times(self, hotkey: str) -> Optional[Dict[str, int]]:
        """``{backing: unix seconds of the miner's latest completed swap on that purse}``."""
        data = self.get(f'/miners/{hotkey}/swaps', params={'limit': 50})
        rows = data.get('rows') if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return None
        latest: Dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get('status') != 'COMPLETED':
                continue
            backing = row.get('backing') or 'sol'
            latest[backing] = max(latest.get(backing, 0), int(row.get('completedAt') or 0))
        return latest

    def live_lanes(self) -> Optional[Dict[Tuple[str, str, str], bool]]:
        """``{(from, to, backing): live}`` from each lane's latest pool round."""
        data = self.get('/crown/pools/history')
        lanes = data.get('lanes') if isinstance(data, dict) else None
        if not isinstance(lanes, list):
            return None
        out: Dict[Tuple[str, str, str], bool] = {}
        for lane in lanes:
            points = lane.get('points') if isinstance(lane, dict) else None
            if not points:
                continue
            out[(lane.get('from'), lane.get('to'), lane.get('backing'))] = bool(points[-1].get('live'))
        return out
