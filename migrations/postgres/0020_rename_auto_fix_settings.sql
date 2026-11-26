-- SPEC-4.4.6: the two settings both named `auto_fix` are renamed for what each
-- does. A stored `config.mint` document moves `auto_fix` to `detect_fixable`
-- and a stored `config.webhook` document moves it to `queue_fix_issues`, with
-- the value kept. A document without the key is left alone.
UPDATE settings
SET value = CASE key
    WHEN 'config.mint' THEN
        ((value::jsonb - 'auto_fix') || jsonb_build_object('detect_fixable', value::jsonb -> 'auto_fix'))::text
    WHEN 'config.webhook' THEN
        ((value::jsonb - 'auto_fix') || jsonb_build_object('queue_fix_issues', value::jsonb -> 'auto_fix'))::text
END
WHERE key IN ('config.mint', 'config.webhook')
  AND CASE WHEN key IN ('config.mint', 'config.webhook')
           THEN jsonb_exists(value::jsonb, 'auto_fix')
           ELSE FALSE END;
