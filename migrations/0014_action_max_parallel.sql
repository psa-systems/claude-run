-- CLAUDE-162: cache each action's configured `concurrency.max_parallel` in the
-- action index.
--
-- The scheduler resolves the daemon-wide auto default for `max_parallel_global`
-- (unset -> the maximum enabled-action `max_parallel`) on every tick. Reading it
-- off the index avoids re-parsing every action's YAML each 5s tick; `scan_actions`
-- already loads the YAML and writes this column alongside the schedule fields.
-- Defaults to 1 (the `Concurrency::max_parallel` default) so pre-existing rows and
-- the back-compat path read a sane value before the next scan rewrites them.

ALTER TABLE actions ADD COLUMN max_parallel INTEGER NOT NULL DEFAULT 1;
