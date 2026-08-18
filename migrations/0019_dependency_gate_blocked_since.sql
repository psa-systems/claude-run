-- CLAUDE-225: record when a dependency-gate row FIRST blocked its issue.
--
-- `blocked_until` is re-armed on every re-evaluation and `updated_at` moves with
-- it, so neither says how long an issue has been held. The row lives for exactly
-- as long as the block does (the gate deletes it once the blockers clear), so a
-- first-blocked stamp makes the age of a block directly queryable, which is what
-- the dashboard's stale-gate banner reports. Written on insert and preserved
-- across every later upsert.
--
-- Backfill: rows written before this migration have no first-blocked stamp, so
-- they take `updated_at` (their last evaluation, an understatement of the true
-- age) and fall back to `blocked_until` if that fails to parse. Understating is
-- deliberate: it can only delay the banner for pre-existing rows, never invent
-- an alarm.
ALTER TABLE dependency_gate ADD COLUMN blocked_since INTEGER NOT NULL DEFAULT 0;

UPDATE dependency_gate
SET blocked_since = IFNULL(CAST(strftime('%s', updated_at) AS INTEGER) * 1000, blocked_until)
WHERE blocked_since = 0;
