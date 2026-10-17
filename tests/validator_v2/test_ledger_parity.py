"""Parity: the event-index tests (tests/test_event_index.py) against the v2 ledger's ingest and anchors.

Left out, each owned elsewhere in v2: swap outcomes and delivery hashes (the seam's post-close truth, ported
with serving), the alpha collateral verdict pinned at fill (the actuator's SwapMemory, priced through the
chain watcher), the event cursor (the feed's acks), and the old tables' in-place migrations (v2 drops and
refills a table whose shape changed)."""

from tests.validator_v2.parity import under_ledger

globals().update(
    under_ledger(
        'test_event_index',
        skip={
            'TestIngestSwapOutcomes',
            'TestSwapFulfillmentHash',
            'test_fill_ingest_pins_declared_collateral_from_reservation_created_at',
            'test_stale_fill_event_does_not_pin_newer_reservation',
            'test_collateral_verdicts_prune_at_caller_cutoff',
            'test_activity_events_hub_column_migrates_in_place',
        },
    )
)
