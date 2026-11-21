-- SPEC-3.3.1: every `Failed` finalize outcome whose issue has a
-- recorded PR sets this flag, including a gate failure (`SPEC-3.3.2`), so
-- automerge (the webhook decision path and the periodic reconciler) takes no
-- action on the PR while it is set. Cleared only when a later run for the
-- issue ends in Code Review. Existing rows default to false.
-- Boolean-ish columns stay integer 0/1 across both backends (see
-- migrations/postgres/0001_init.sql), so this is BIGINT, not BOOLEAN.
ALTER TABLE agent_pull_requests ADD COLUMN failed BIGINT NOT NULL DEFAULT 0;
