-- `mint.detect_fixable.checks_workflows` no longer exists (SPEC-4.4.6):
-- `detect_fixable` has only `enabled`, and a failed workflow with no
-- mechanical signature goes to the analysis pass whatever its name. Remove
-- the stale key from the stored `config.mint` document, only when it is
-- present, and leave every other row alone. The CASE guards the jsonb cast
-- so another row's non-JSON value is never parsed.
UPDATE settings
SET value = (value::jsonb #- '{detect_fixable,checks_workflows}')::text
WHERE key = 'config.mint'
  AND CASE WHEN key = 'config.mint'
           THEN jsonb_exists(value::jsonb -> 'detect_fixable', 'checks_workflows')
           ELSE FALSE END;
