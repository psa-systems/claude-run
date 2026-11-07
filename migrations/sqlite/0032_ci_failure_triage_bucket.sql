-- SPEC-4.3.15 / CLAUDE-598: persist the triage bucket
-- (`code_fix`/`human_action`/`transient`/`auto_fix`, `src/triage.rs`'s
-- `TriageBucket`) on the `ci_failures` row it was computed for, so the
-- dashboard can render it as its own column distinct from lifecycle `state`.
-- Nullable: a legacy-adopted row (adopted via YouTrack full-text search, no
-- triage ever ran) and existing rows before this migration have no bucket.
ALTER TABLE ci_failures ADD COLUMN triage_bucket TEXT;
