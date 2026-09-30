-- Mandatory TOTP 2FA for the web UI's single administrator (CLAUDE-368).
--
-- `totp_secret` holds the RFC 6238 secret (base32) encrypted with the same
-- age key that protects the `secrets` table (`crate::secrets::SecretsKey`),
-- never plaintext. It is written as soon as enrollment starts, before it is
-- confirmed: `totp_enrolled_at` is what actually marks the account enrolled,
-- so a page reload during setup does not read as "already 2FA'd".
-- `totp_last_step` is the last accepted 30s time step, refused on replay so a
-- captured code cannot be reused within its own window.
ALTER TABLE web_admin ADD COLUMN totp_secret BLOB;
ALTER TABLE web_admin ADD COLUMN totp_enrolled_at TEXT;
ALTER TABLE web_admin ADD COLUMN totp_last_step BIGINT;

-- Ten single-use recovery codes, minted once when enrollment is confirmed.
-- `code_hash` is an argon2id hash (the same scheme as `web_admin.password_hash`),
-- never the code itself; `used_at` marks consumption so a code cannot be
-- replayed.
CREATE TABLE IF NOT EXISTS web_admin_recovery_codes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    code_hash  TEXT NOT NULL,
    used_at    TEXT,
    created_at TEXT NOT NULL
);
