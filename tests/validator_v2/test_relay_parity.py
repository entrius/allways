"""Parity: the bond relay tests (tests/test_bond_relay.py) with the v2 relay ledger as the relay's store.

Left out: the forward loop's relay cursor, which v2 doesn't have (a failed event write is held and
retried by HubRelay, with the heartbeat held down meanwhile; see test_relay.py)."""

from tests.validator_v2.parity import under_relay_ledger

globals().update(
    under_relay_ledger('test_bond_relay', skip={'test_a_failed_verdict_write_holds_the_relay_cursor_until_it_lands'})
)
