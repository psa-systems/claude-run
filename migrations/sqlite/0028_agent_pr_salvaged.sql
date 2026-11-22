-- A PR opened by the timeout salvage path must never be merged
-- by automerge until a resumed run finishes the work, however its draft
-- status changes. Persist that provenance on the row instead of
-- inferring it from the `WIP: ` prefix, which anyone can clear. Existing rows
-- default to false: only PRs salvaged after this migration are flagged.
ALTER TABLE agent_pull_requests ADD COLUMN salvaged INTEGER NOT NULL DEFAULT 0;
