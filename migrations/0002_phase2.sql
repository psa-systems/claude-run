-- Phase 2: daemon + scheduler tables.
-- - `actions` indexes the YAML files on disk so the daemon and web UI can
--   query them without re-parsing every tick. The on-disk YAML is still the
--   source of truth; the daemon refreshes this table on startup and when it
--   notices a `yaml_sha256` drift.
-- - `paused_actions` holds the per-action pause state. The Phase 1 PAUSED
--   filesystem flag is replaced by rows here.
-- - `global_pause` is a single-row table for the global pause flag.

CREATE TABLE IF NOT EXISTS actions (
    name TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    yaml_sha256 TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    schedule_kind TEXT,           -- 'interval' | 'cron' | 'manual'
    schedule_spec TEXT,           -- '30m' / '*/30 * * * *' / NULL
    last_seen_at TEXT NOT NULL,
    parse_error TEXT              -- non-null = YAML failed to load
);

CREATE TABLE IF NOT EXISTS paused_actions (
    action_name TEXT PRIMARY KEY,
    paused_at TEXT NOT NULL,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS global_pause (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    paused_at TEXT NOT NULL,
    reason TEXT
);
