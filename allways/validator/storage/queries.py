# Storage queries — INSERT/UPDATE/DELETE only. Read paths stay in das-allways.
#
# Lifted directly from alw-utils/sync-validator-state/sync_validator_state.py
# (`write_cursor`, `insert_rate_changes`, `delete_crown_in_range`,
# `upsert_crown_rows`). Same SQL, same conflict keys — the validator writes
# rows shaped exactly like what the daemon writes today so the dashboard's
# read path is unchanged.

# rate_history: per-miner on-chain rate commitments, append-only by (hotkey,
# direction, block). Last-write-wins on the rate column if the same block is
# re-emitted (matches existing sync-daemon behavior).
BULK_UPSERT_RATE_HISTORY = """
INSERT INTO rate_history (hotkey, from_chain, to_chain, rate, block)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (hotkey, from_chain, to_chain, block)
DO UPDATE SET rate = EXCLUDED.rate
"""

# crown_holders window wipe: crown derivation operates on a moving block
# window, so an old window's rows are deleted before the recomputed ones are
# upserted. Bounded by the caller (typically SCORING_WINDOW_BLOCKS wide).
DELETE_CROWN_IN_RANGE = """
DELETE FROM crown_holders
WHERE from_chain = %s AND to_chain = %s AND block >= %s AND block < %s
"""

# crown_holders: per-block winners with fractional credit. k-way ties emit
# k rows summing to 1.0; DOUBLE PRECISION column avoids drift at aggregation.
BULK_UPSERT_CROWN_HOLDERS = """
INSERT INTO crown_holders (block, from_chain, to_chain, hotkey, credit, rate)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (block, from_chain, to_chain, hotkey)
DO UPDATE SET credit = EXCLUDED.credit, rate = EXCLUDED.rate
"""

# sync_cursor: bookkeeping watermarks. The validator advances these in the
# same transaction as the data they describe, so a partial write never leaves
# the cursor ahead of (or behind) the rows.
SET_SYNC_CURSOR = """
INSERT INTO sync_cursor (name, value, updated_at)
VALUES (%s, %s, NOW())
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
"""

# current_crown_holders: live "who holds the crown right now" per direction,
# refreshed per forward step (~12s). Distinct from crown_holders, which is
# the historical per-block ledger flushed at end-of-round (~2h). A direction's
# rows are deleted before re-insertion so a k-way tied holder set is
# consistent — readers never see a partial tie.
DELETE_CURRENT_CROWN_BY_DIRECTION = """
DELETE FROM current_crown_holders
WHERE from_chain = %s AND to_chain = %s
"""

BULK_UPSERT_CURRENT_CROWN_HOLDERS = """
INSERT INTO current_crown_holders (from_chain, to_chain, hotkey, credit, rate, block, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (from_chain, to_chain, hotkey)
DO UPDATE SET credit = EXCLUDED.credit,
              rate   = EXCLUDED.rate,
              block  = EXCLUDED.block,
              updated_at = NOW()
"""
