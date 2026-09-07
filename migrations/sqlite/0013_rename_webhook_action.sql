-- CLAUDE-153: rename the synthetic webhook action from `webhook-fast-path` to
-- `webhook-forgejo` in the persisted state, so existing run history, caps, and
-- any pause row stay attached to the action under its new name (the constant
-- `WEBHOOK_MINT_ACTION` now reads `webhook-forgejo`). A clean-break rename would
-- orphan every past webhook run; migrating the value keeps the dashboard's run
-- list, durations chart, and last-run recency intact.
--
-- One UPDATE per table that keys on `action_name` and that the webhook path can
-- write: `runs` (the history rows), `job_outcomes` (per-job records),
-- `paused_actions` (the pause row, if the operator paused before the upgrade),
-- `attempt_counters` / `daily_ledger` (the reused DEV-335 caps ledger), and
-- `run_events` (poll/pipeline diagnostics). On a fresh DB every UPDATE matches
-- zero rows and is a no-op. `paused_actions.action_name` is a PRIMARY KEY, but
-- `webhook-forgejo` cannot pre-exist this rename, so no key collision is
-- possible. The on-disk log directory is renamed separately at serve start
-- (`crate::daemon`), since SQLite migrations cannot touch the filesystem.
UPDATE runs             SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
UPDATE job_outcomes     SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
UPDATE paused_actions   SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
UPDATE attempt_counters SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
UPDATE daily_ledger     SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
UPDATE run_events       SET action_name = 'webhook-forgejo' WHERE action_name = 'webhook-fast-path';
