-- SPEC-3.17.21: one row per lifecycle notice a peer sent this instance. Idempotent per
-- (instance_id, run_id, event). Pruned after 24 hours by `received_at` (SPEC-3.8.14).
CREATE TABLE peer_work_events (
    instance_id TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    event TEXT NOT NULL,
    key TEXT NOT NULL,
    pr_number INTEGER,
    at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (instance_id, run_id, event)
);
CREATE INDEX idx_peer_work_events_received_at ON peer_work_events(received_at);

-- SPEC-3.17.20: what a peer's snapshot says about this instance's live run. NULL until known:
-- `stage` is set when the attempt starts, `branch` and `pr_number` once a PR exists.
ALTER TABLE run_inflight_jobs ADD COLUMN stage TEXT;
ALTER TABLE run_inflight_jobs ADD COLUMN branch TEXT;
ALTER TABLE run_inflight_jobs ADD COLUMN pr_number INTEGER;
