-- CLAUDE-20: persist the full per-row pipeline elapsed time.
--
-- row_outcomes previously recorded only `ts` (when the outcome landed), so a
-- per-row duration could not be derived. `duration_ms` is measured in
-- `process_row` from the start of the row through the outcome, covering
-- filter / pre / prompt / claude / post, not just the claude invocation.
-- Rows created before this migration have NULL `duration_ms` and render as "-".

ALTER TABLE row_outcomes ADD COLUMN duration_ms INTEGER;
