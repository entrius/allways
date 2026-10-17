"""Parity: the bad-address lane tests (tests/test_scoring_bad_address_lanes.py) with the v2 ledger and v2's
quote judgment (``scoring.component.quote_usable``) in place of the old event index. Left out: the
refusal memo's size cap, which v2 doesn't have (the chain watcher forgets an address nobody asked about
for ten minutes)."""

from tests.validator_v2.parity import under_ledger

globals().update(
    under_ledger('test_scoring_bad_address_lanes', skip={'test_a_full_refusal_memo_still_probes_a_new_address'})
)
