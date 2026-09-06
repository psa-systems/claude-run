-- CLAUDE-14: persist the row a run is currently executing.
--
-- Rows are processed strictly sequentially (one `claude --print` at a time), so
-- at most one row is ever in flight. The web UI previously inferred "running"
-- from the absence of a row_outcomes record, which mislabeled every queued row
-- as running. runs.current_row_key is the authoritative marker: set to the key
-- of the row being processed and cleared to NULL when no row is in flight (run
-- finished, or a legacy row created before this migration).

ALTER TABLE runs ADD COLUMN current_row_key TEXT;
