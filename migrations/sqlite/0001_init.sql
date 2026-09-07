-- Phase 1 schema. Carries the parts of state.json that drive runtime
-- decisions: attempt counters, daily ledger, plus per-row outcome history.
-- The web UI / daemon (Phase 2+) extend this with actions metadata, runs,
-- tokens, settings - all in additive migrations.

CREATE TABLE IF NOT EXISTS attempt_counters (
    action_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    count INTEGER NOT NULL,
    last_at TEXT NOT NULL,
    PRIMARY KEY (action_name, row_key)
);

CREATE TABLE IF NOT EXISTS daily_ledger (
    action_name TEXT NOT NULL,
    date TEXT NOT NULL,
    run_count INTEGER NOT NULL DEFAULT 0,
    cost_usd REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY (action_name, date)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    exit_code INTEGER,
    rows_processed INTEGER NOT NULL DEFAULT 0,
    triggered_by TEXT NOT NULL DEFAULT 'cli'
);

CREATE INDEX IF NOT EXISTS idx_runs_action_started ON runs(action_name, started_at);

CREATE TABLE IF NOT EXISTS row_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    action_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    ts TEXT NOT NULL,
    ok INTEGER NOT NULL,
    claude_ran INTEGER NOT NULL,
    claude_ok INTEGER NOT NULL,
    claude_cost_usd REAL NOT NULL DEFAULT 0.0,
    attempt_counted INTEGER NOT NULL DEFAULT 1,
    summary TEXT,
    stream_log_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_row_outcomes_action_row ON row_outcomes(action_name, row_key);
CREATE INDEX IF NOT EXISTS idx_row_outcomes_run ON row_outcomes(run_id);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
