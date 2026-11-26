-- SPEC-4.4.6: the two settings both named `auto_fix` are renamed for what each
-- does. A stored `config.mint` document moves `auto_fix` to `detect_fixable`
-- and a stored `config.webhook` document moves it to `queue_fix_issues`, with
-- the value kept. A document without the key is left alone.
UPDATE settings
SET value = CASE key
    WHEN 'config.mint' THEN json_remove(
        json_set(value, '$.detect_fixable', json(json_extract(value, '$.auto_fix'))),
        '$.auto_fix')
    WHEN 'config.webhook' THEN json_remove(
        json_set(
            value,
            '$.queue_fix_issues',
            json(CASE json_type(value, '$.auto_fix') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' END)),
        '$.auto_fix')
END
WHERE key IN ('config.mint', 'config.webhook')
  AND CASE WHEN key IN ('config.mint', 'config.webhook')
           THEN json_type(value, '$.auto_fix') IS NOT NULL
           ELSE 0 END;
