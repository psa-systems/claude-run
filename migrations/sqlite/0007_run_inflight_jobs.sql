-- CLAUDE-28: set of in-flight job keys per run.
--
-- The single `runs.current_job_key` marker (migration 0004 / 0006) assumed at
-- most one job in flight at a time, which held while jobs ran strictly
-- sequentially. CLAUDE-28 runs distinct-key jobs in parallel, so a run can have
-- several jobs in flight at once. This table tracks that set: a row per
-- (run_id, job_key) currently being processed. The run-detail UI badges a
-- planned job "running" when its key is in this set (and the run is still
-- active), replacing the equality check against `current_job_key`.
--
-- `runs.current_job_key` is left in place for back-compat (older rows, and the
-- column is still selected into the RunRow) but is no longer written.

CREATE TABLE IF NOT EXISTS run_inflight_jobs (
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    job_key TEXT NOT NULL,
    PRIMARY KEY (run_id, job_key)
);
