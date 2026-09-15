-- CLAUDE-172: index run_events(ts) so the Phase 6 retention prune seeks the
-- rows past the cutoff instead of scanning the whole table.
--
-- The daemon retention sweep (see `src/retention.rs`) deletes rows with
-- `ts < <cutoff>`. `run_events` already indexes (action_name, ts) and (run_id),
-- but idx_run_events_action_ts leads with action_name, so a bare `ts <` range
-- predicate cannot use it and would full-scan an unbounded table. This
-- single-column index turns that DELETE into an index range seek, mirroring
-- migration 0017 for job_outcomes so the two prunes share the same shape.
CREATE INDEX IF NOT EXISTS idx_run_events_ts ON run_events(ts);
