-- CLAUDE-601 / SPEC-3.14 items 1 and 7: record the stage and the outcome on
-- each attempt so "succeeded on the first / second / third try" is a query on
-- `job_outcomes`. Historical rows are backfilled from what they stored: the
-- stage from the attempt number, the outcome from `ok`.
ALTER TABLE job_outcomes ADD COLUMN stage TEXT;
ALTER TABLE job_outcomes ADD COLUMN outcome TEXT;

UPDATE job_outcomes SET
    stage = CASE WHEN attempt <= 1 THEN 'initial' WHEN attempt = 2 THEN 'retry-2' ELSE 'retry-3' END,
    outcome = CASE WHEN ok = 1 THEN 'success' ELSE 'failed' END;

CREATE INDEX IF NOT EXISTS idx_job_outcomes_stage_outcome ON job_outcomes(stage, outcome);
