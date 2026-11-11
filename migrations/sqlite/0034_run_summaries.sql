-- SPEC-3.17 items 9 and 10 / CLAUDE-616: one immutable highlight row per job
-- outcome, own and replicated, plus the per-peer pull cursor. `seq` is
-- monotonic per `instance_id`; the unique index makes a concurrent writer
-- retry instead of reusing a number.
CREATE TABLE run_summaries (
    run_uuid TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    action_name TEXT NOT NULL,
    job_key TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    ok INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    cost_usd REAL NOT NULL,
    timed_out INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_run_summaries_origin_seq ON run_summaries(instance_id, seq);
CREATE INDEX idx_run_summaries_ended_at ON run_summaries(ended_at);

CREATE TABLE peer_cursors (
    peer_instance_id TEXT PRIMARY KEY,
    after_seq INTEGER NOT NULL,
    last_success_at TEXT,
    last_error TEXT
);

-- Backfill: this instance's id is created here when history exists and no id
-- is stored yet (the same `settings` row `ensure_instance_id` would create).
INSERT INTO settings (key, value, updated_at)
SELECT 'instance.id',
       lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
       substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) ||
       substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))),
       strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = 'instance.id')
  AND EXISTS (SELECT 1 FROM job_outcomes);

INSERT INTO run_summaries (
    run_uuid, instance_id, seq, action_name, job_key, attempt, outcome, ok,
    started_at, ended_at, duration_ms, cost_usd, timed_out
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) ||
        substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))),
    (SELECT value FROM settings WHERE key = 'instance.id'),
    ROW_NUMBER() OVER (ORDER BY o.ts, o.id),
    o.action_name, o.job_key, o.attempt, o.outcome, o.ok,
    strftime('%Y-%m-%dT%H:%M:%SZ', o.ts, '-' || (COALESCE(o.duration_ms, 0) / 1000) || ' seconds'),
    o.ts,
    COALESCE(o.duration_ms, 0),
    o.claude_cost_usd,
    o.timed_out
FROM job_outcomes o;
