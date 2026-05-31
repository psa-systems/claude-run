-- CLAUDE-52: distinguish a claude timeout (exit 124) from a hard failure.
--
-- job_outcomes previously recorded only `ok`, so the web UI derived "failed"
-- purely from `ok = 0` and could not tell a timeout apart from a crash. A
-- timed-out job sets `timed_out = 1` (still with `ok = 0`); the UI renders it
-- with a distinct badge and excludes it from the per-run failed-job count.
-- Outcomes created before this migration default to 0 (not timed out).

ALTER TABLE job_outcomes ADD COLUMN timed_out INTEGER NOT NULL DEFAULT 0;
