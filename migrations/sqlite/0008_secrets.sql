-- CLAUDE-26: encrypted action secrets.
--
-- Stores credentials an action references by name (`token_ref`) instead of
-- carrying a raw token in its YAML. Values are encrypted at rest with the `age`
-- crate (X25519 + ChaCha20-Poly1305) using a key read from the file named by
-- `CLAUDE_RUN_SECRETS_KEY_FILE`. The key is NEVER stored in the DB beside the
-- ciphertext, so a DB leak alone does not reveal any secret.
--
-- `name` is the human handle a config's `token_ref` points at; it is unique so a
-- reference resolves to exactly one secret. `last_used_at` is bumped whenever a
-- secret is decrypted at run time (NULL until first use).

CREATE TABLE IF NOT EXISTS secrets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    ciphertext BLOB NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT
);
