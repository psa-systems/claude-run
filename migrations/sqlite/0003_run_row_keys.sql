-- CLAUDE-1: surface in-flight row keys + per-action link template.
--
-- - runs.row_keys: JSON array of row keys captured at run start, so the web UI
--   can show the planned batch immediately rather than waiting for each row's
--   outcome record. Null on rows created before this migration.
-- - actions.link_template: optional URL template with `{key}` substitution. The
--   web UI renders row keys as clickable links to the external resource
--   (YouTrack issue, Forgejo PR, etc.) when set.

ALTER TABLE runs ADD COLUMN row_keys TEXT;
ALTER TABLE actions ADD COLUMN link_template TEXT;
