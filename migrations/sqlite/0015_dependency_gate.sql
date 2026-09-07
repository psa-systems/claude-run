-- CLAUDE-161: per-issue re-check backoff for the pre-clone dependency gate.
--
-- When a queued issue is blocked by an unmet dependency the gate leaves it
-- Queued and records `blocked_until` (epoch ms) plus a `signature` of the unmet
-- blocker set. While `now < blocked_until` the gate skips re-evaluating the
-- issue entirely (no `yt` read, no comment); the signature drives comment
-- idempotency (re-comment only when the unmet-blocker set changes). Keyed by
-- (action_name, job_key). Rows are deleted once the issue's dependencies clear.
CREATE TABLE dependency_gate (
    action_name   TEXT    NOT NULL,
    job_key       TEXT    NOT NULL,
    blocked_until INTEGER NOT NULL, -- epoch ms; gate re-evaluates once now >= this
    signature     TEXT    NOT NULL, -- canonical unmet-blocker set (change => re-comment)
    updated_at    TEXT    NOT NULL, -- RFC 3339, last write
    PRIMARY KEY (action_name, job_key)
);
