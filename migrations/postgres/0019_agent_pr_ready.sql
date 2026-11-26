-- The `finished` flag only recorded that `finalize` promoted a successful
-- run's PR from draft to ready; it did not mean the PR had reached any kind
-- of end. Rename it to `ready` so a `ready` PR can be evaluated for
-- automerge instead of being treated as done the moment it is promoted.
-- Existing `finished = 1` rows keep that value as `ready = 1`.
ALTER TABLE agent_pull_requests RENAME COLUMN finished TO ready;
