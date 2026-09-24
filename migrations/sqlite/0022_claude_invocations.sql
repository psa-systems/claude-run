-- CLAUDE-315: one row per `claude` invocation, so plan usage (rate-limit
-- utilization, retries) is queryable history rather than only a log line.
-- Every rate-limit column is nullable: an invocation whose CLI predates the
-- `rate_limit_event` stream event, or that made no API call reporting one,
-- leaves them all NULL rather than a fabricated zero. `job_key` and `run_id`
-- are nullable because a classification invocation (CLAUDE-274's at-ingest
-- triage) has neither: it belongs to no scheduled job and opens no run row.

CREATE TABLE IF NOT EXISTS claude_invocations (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                        TEXT NOT NULL,
    action                    TEXT NOT NULL,
    job_key                   TEXT,
    run_id                    INTEGER REFERENCES runs(id),
    exit_code                 INTEGER,
    timed_out                 INTEGER NOT NULL,
    ok                        INTEGER NOT NULL,
    cost_usd                  REAL NOT NULL,
    total_tokens              INTEGER NOT NULL,
    rate_limit_status         TEXT,
    rate_limit_type           TEXT,
    five_hour_utilization     REAL,
    five_hour_resets_at       TEXT,
    seven_day_utilization     REAL,
    seven_day_resets_at       TEXT,
    max_five_hour_utilization REAL,
    max_seven_day_utilization REAL,
    overage_status            TEXT,
    is_using_overage          INTEGER,
    api_retries_json          TEXT,
    stream_log_path           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claude_invocations_ts ON claude_invocations(ts);
