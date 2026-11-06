-- CLAUDE-550: bound the automerge conflict auto-resolve (CLAUDE-380) per PR.
-- `conflict_resolve_attempts` counts dispatched resolve runs across head
-- commits; `conflict_resolve_started_at` marks one still in flight (cleared
-- when it finishes); `conflict_files` holds the newline-separated paths the
-- last attempt left unresolved, named in the delivery-blocked comment.
ALTER TABLE agent_pull_requests ADD COLUMN conflict_resolve_attempts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE agent_pull_requests ADD COLUMN conflict_resolve_started_at TEXT;
ALTER TABLE agent_pull_requests ADD COLUMN conflict_files TEXT;
