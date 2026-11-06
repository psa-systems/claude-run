-- CLAUDE-575 / SPEC-3.14: time an attempt as a sequence of steps instead of
-- one `job_outcomes.duration_ms`. `kind` is the SPEC-3.14 closed set (clone,
-- hook_install, postgres_start, postgres_healthy, coding, check, build,
-- checkpoint, push, pr_create, review, ci_wait, conflict_resolution, gate,
-- other); a gap between recorded steps is written back as its own
-- `kind = 'other', label = 'unaccounted'` row so `SUM(ended_at - started_at)`
-- over an attempt's rows always equals its wall time (SPEC-3.14 item 6), never
-- a separate column that could drift from the rows.
CREATE TABLE IF NOT EXISTS attempt_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    action_name TEXT NOT NULL,
    job_key TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    stage TEXT NOT NULL,
    kind TEXT NOT NULL,
    label TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    ok INTEGER NOT NULL,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempt_steps_action_job ON attempt_steps(action_name, job_key, attempt);
CREATE INDEX IF NOT EXISTS idx_attempt_steps_run ON attempt_steps(run_id);
