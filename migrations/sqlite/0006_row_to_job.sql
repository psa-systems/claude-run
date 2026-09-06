-- CLAUDE-23: rename the per-run unit of work from "row" to "job".
--
-- This is a pure DDL rename: no data is transformed, so every existing row in
-- every table is preserved. Only schema identifiers change so the runtime
-- vocabulary (action / run / job) is consistent with the UI, code, and docs.
--
-- "row" had two meanings in this schema; only the unit-of-work one is renamed
-- here. The standard-SQL sense (a row of a table) is untouched: the
-- attempt_counters / daily_ledger / actions tables keep their shape, and only
-- the columns that named the unit of work move to "job".
--
-- SQLite carries indexes and the primary-key definition along through
-- RENAME TO / RENAME COLUMN, but keeps the old index *names*; the indexes on
-- the renamed outcomes table are therefore dropped and recreated under their
-- job names at the end. RENAME COLUMN requires SQLite 3.25.0+ (bundled
-- libsqlite3-sys is well past that).

-- runs: the per-run job counters and in-flight markers.
ALTER TABLE runs RENAME COLUMN rows_processed TO jobs_processed;
ALTER TABLE runs RENAME COLUMN row_keys TO job_keys;
ALTER TABLE runs RENAME COLUMN current_row_key TO current_job_key;

-- attempt_counters: the per-key attempt ledger. Its key is the job key.
ALTER TABLE attempt_counters RENAME COLUMN row_key TO job_key;

-- row_outcomes -> job_outcomes, plus its job-key column. The table rename
-- carries the indexes with it (under their old names); they are recreated
-- below.
ALTER TABLE row_outcomes RENAME TO job_outcomes;
ALTER TABLE job_outcomes RENAME COLUMN row_key TO job_key;

DROP INDEX idx_row_outcomes_action_row;
DROP INDEX idx_row_outcomes_run;
CREATE INDEX IF NOT EXISTS idx_job_outcomes_action_job ON job_outcomes(action_name, job_key);
CREATE INDEX IF NOT EXISTS idx_job_outcomes_run ON job_outcomes(run_id);
