-- CLAUDE-630: once a run's own completion criteria are met (`Finalized::CodeReview`,
-- success with the PR promoted to ready), the agent has nothing left to do on this
-- PR. Persist that on the row so the automerge reconciler and the Action-Run
-- webhook decision path (`apply_automerge_decision`) stop re-syncing it (no more
-- behind-base branch updates, no more status comments) and leave it for a human
-- to merge. Existing rows default to false: only a finalize that observes
-- `CodeReview` after this migration sets it.
ALTER TABLE agent_pull_requests ADD COLUMN finished INTEGER NOT NULL DEFAULT 0;
