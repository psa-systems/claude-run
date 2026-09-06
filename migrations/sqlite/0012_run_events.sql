-- CLAUDE-147: input/poll-phase diagnostics, persisted so they surface in the
-- web UI instead of being log-only and invisible.
--
-- `gather()` resolves each project's VCS before `run_begin`. A probe tool error
-- (e.g. `yt project vcs list <PROJECT>` exiting non-zero) or a "no VCS
-- integration" drop was previously only `warn!`/`info!` to the daemon journal:
-- the affected issues vanish from the poll and the operator sees nothing.
--
-- One row per diagnostic (not a single blob column on `runs`) so multiple
-- per-project probe failures in one poll are each recorded. `run_id` is
-- nullable: events are recorded keyed by `action_name` + the poll `ts` before
-- any run row exists, then backfilled with the run id once `run_begin` creates
-- the row for that poll. A poll that yields zero jobs still surfaces its events
-- on the action page under that timestamp even if no run row is ever attached.
--
-- Retention rides on the Phase 6 `job_outcomes` retention sweep (see the Phases
-- table in CLAUDE.md); like `job_outcomes`, this table is unbounded until that
-- lands. The web read path bounds display with a LIMIT.
CREATE TABLE IF NOT EXISTS run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES runs(id) ON DELETE CASCADE,
    action_name TEXT NOT NULL,
    ts TEXT NOT NULL,
    level TEXT NOT NULL,
    context TEXT,
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_events_action_ts ON run_events(action_name, ts);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id);
