-- The `version` self-update was removed (CLAUDE-400), so its DB-backed
-- `config.update` settings document has no reader. Delete it rather than leave
-- a dead setting behind.
DELETE FROM settings WHERE key = 'config.update';
