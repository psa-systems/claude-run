-- CLAUDE-371: same end schema as migrations/sqlite/0024_ci_failure_run_test_ids.sql;
-- see there for the column-by-column rationale.
ALTER TABLE ci_failure_runs ADD COLUMN test_ids TEXT;
