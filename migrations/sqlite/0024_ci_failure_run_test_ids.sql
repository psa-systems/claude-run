-- CLAUDE-371: record each sighting's own failing-id fingerprint, not just the
-- failure's aggregate set in `ci_failure_tests`, so `route_agent_pr_failure`
-- can compare an agent PR's latest attempt against its immediately previous
-- one and stop re-queueing a cycle instead of burning every attempt on it.
-- Newline-joined (fingerprint ids never contain a newline) rather than a
-- second table: one row per sighting already exists, and this is always read
-- back whole, never queried by individual id.
ALTER TABLE ci_failure_runs ADD COLUMN test_ids TEXT;
