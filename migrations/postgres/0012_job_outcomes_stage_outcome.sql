-- CLAUDE-601 / SPEC-3.14 items 1 and 7: see
-- migrations/sqlite/0031_job_outcomes_stage_outcome.sql for the rationale.
ALTER TABLE job_outcomes ADD COLUMN IF NOT EXISTS stage TEXT;
ALTER TABLE job_outcomes ADD COLUMN IF NOT EXISTS outcome TEXT;

UPDATE job_outcomes SET
    stage = CASE WHEN attempt <= 1 THEN 'initial' WHEN attempt = 2 THEN 'retry-2' ELSE 'retry-3' END,
    outcome = CASE WHEN ok = 1 THEN 'success' ELSE 'failed' END;

CREATE INDEX IF NOT EXISTS idx_job_outcomes_stage_outcome ON job_outcomes(stage, outcome);
