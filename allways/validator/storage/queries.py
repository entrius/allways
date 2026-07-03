# Storage queries — INSERT/UPDATE/DELETE only. Read paths stay in das-allways.
#
# The validator writes the time-native crown ledger its scoring already
# produces: rate quotes and crown intervals timestamped in unix seconds
# (blockTime axis), not block numbers.

# rate_history: per-miner on-chain rate quotes, append-only by (hotkey,
# direction, ts). Last-write-wins on the rate column if the same timestamp is
# re-emitted.
BULK_UPSERT_RATE_HISTORY = """
INSERT INTO rate_history (hotkey, from_chain, to_chain, rate, ts)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (hotkey, from_chain, to_chain, ts)
DO UPDATE SET rate = EXCLUDED.rate
"""

# crown_holders window wipe: crown derivation operates on a moving time
# window, so an old window's intervals are deleted before the recomputed ones
# are upserted. Bounded by the caller (the scoring window, in unix seconds).
DELETE_CROWN_IN_RANGE = """
DELETE FROM crown_holders
WHERE from_chain = %s AND to_chain = %s AND started_at >= %s AND started_at < %s
"""

# crown_holders: per-interval winners with fractional credit. k-way ties emit
# k rows summing to 1.0; DOUBLE PRECISION column avoids drift at aggregation.
BULK_UPSERT_CROWN_HOLDERS = """
INSERT INTO crown_holders (started_at, ended_at, from_chain, to_chain, hotkey, credit, rate)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (started_at, from_chain, to_chain, hotkey)
DO UPDATE SET ended_at = EXCLUDED.ended_at, credit = EXCLUDED.credit, rate = EXCLUDED.rate
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
# the historical interval ledger flushed at end-of-round (~1h). A direction's
# rows are deleted before re-insertion so a k-way tied holder set is
# consistent — readers never see a partial tie.
DELETE_CURRENT_CROWN_BY_DIRECTION = """
DELETE FROM current_crown_holders
WHERE from_chain = %s AND to_chain = %s
"""

BULK_UPSERT_CURRENT_CROWN_HOLDERS = """
INSERT INTO current_crown_holders (from_chain, to_chain, hotkey, credit, rate, ts, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (from_chain, to_chain, hotkey)
DO UPDATE SET credit = EXCLUDED.credit,
              rate   = EXCLUDED.rate,
              ts     = EXCLUDED.ts,
              updated_at = NOW()
"""
