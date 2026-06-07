-- CLAUDE-80: live pipeline stage for an in-flight run. The webhook fast-path
-- consumer advances it through "downloading forgejo log" / "triaging" /
-- "classifying with claude" / "minting issue" so the run-detail page can show
-- what a run is doing before any outcome lands. NULL means no stage info
-- (scheduled runs, finished runs, rows predating the column); `run_finish`
-- clears it together with `current_job_key`.
ALTER TABLE runs ADD COLUMN stage TEXT;
