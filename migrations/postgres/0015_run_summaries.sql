-- SPEC-3.17 items 9 and 10 / CLAUDE-616: see
-- migrations/sqlite/0034_run_summaries.sql for the rationale.
CREATE TABLE run_summaries (
    run_uuid TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    action_name TEXT NOT NULL,
    job_key TEXT NOT NULL,
    attempt BIGINT NOT NULL,
    outcome TEXT NOT NULL,
    ok BIGINT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    duration_ms BIGINT NOT NULL,
    cost_usd DOUBLE PRECISION NOT NULL,
    timed_out BIGINT NOT NULL
);
CREATE UNIQUE INDEX idx_run_summaries_origin_seq ON run_summaries(instance_id, seq);
CREATE INDEX idx_run_summaries_ended_at ON run_summaries(ended_at);

CREATE TABLE peer_cursors (
    peer_instance_id TEXT PRIMARY KEY,
    after_seq BIGINT NOT NULL,
    last_success_at TEXT,
    last_error TEXT
);

INSERT INTO settings (key, value, updated_at)
SELECT 'instance.id', gen_random_uuid()::text, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = 'instance.id')
  AND EXISTS (SELECT 1 FROM job_outcomes);

INSERT INTO run_summaries (
    run_uuid, instance_id, seq, action_name, job_key, attempt, outcome, ok,
    started_at, ended_at, duration_ms, cost_usd, timed_out
)
SELECT
    gen_random_uuid()::text,
    (SELECT value FROM settings WHERE key = 'instance.id'),
    ROW_NUMBER() OVER (ORDER BY o.ts, o.id),
    o.action_name, o.job_key, o.attempt, o.outcome, o.ok,
    to_char((o.ts::timestamptz - COALESCE(o.duration_ms, 0) * interval '1 millisecond') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    o.ts,
    COALESCE(o.duration_ms, 0),
    o.claude_cost_usd,
    o.timed_out
FROM job_outcomes o;
