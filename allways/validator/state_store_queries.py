"""SQL constants for ValidatorStateStore (SQLite).

New queries added to state_store should land here as named constants
and be imported, not inlined in method bodies. The bulk of the
existing state_store SQL still lives inline; new additions follow
this convention so over time the surface migrates out.
"""

# Latest rate per hotkey in one direction at-or-before `block`. ROW_NUMBER
# matches the per-hotkey `ORDER BY block DESC, id DESC LIMIT 1` semantics
# of get_latest_rate_before. Same-block re-emit → latest id wins (id is
# monotonic in this table).
BATCH_LATEST_RATES_BEFORE = """
SELECT hotkey, rate, block FROM (
    SELECT hotkey, rate, block,
           ROW_NUMBER() OVER (
               PARTITION BY hotkey
               ORDER BY block DESC, id DESC
           ) AS rn
    FROM rate_events
    WHERE from_chain = ? AND to_chain = ? AND block <= ?
) WHERE rn = 1
"""
