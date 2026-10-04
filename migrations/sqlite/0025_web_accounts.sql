-- Multiple web accounts with invites (CLAUDE-369), replacing the single-row
-- `web_admin` credential with a real account table. There are no roles yet:
-- every account has full admin rights, but `created_by` and the shape here
-- leave room for a role column later without a further migration.
--
-- `username_key` is the lowercased username, UNIQUE, so sign-in and invite
-- acceptance are case-insensitive on uniqueness while `username` keeps the
-- casing the account was created with for display.
CREATE TABLE IF NOT EXISTS web_accounts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL,
    username_key  TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    session_key   TEXT NOT NULL,
    totp_secret       BLOB,
    totp_enrolled_at  TEXT,
    totp_last_step    BIGINT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    created_by    INTEGER REFERENCES web_accounts(id)
);

-- Recovery codes now key on the owning account instead of the single
-- `id = 1` row `web_admin_recovery_codes` assumed.
CREATE TABLE IF NOT EXISTS web_account_recovery_codes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES web_accounts(id),
    code_hash  TEXT NOT NULL,
    used_at    TEXT,
    created_at TEXT NOT NULL
);

-- A single-use invite link. Only `token_hash` is stored; the token itself is
-- shown once to the inviter and never persisted. `expires_at` enforces the 72
-- hour window; `used_at` makes acceptance single-use.
CREATE TABLE IF NOT EXISTS web_invites (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash TEXT NOT NULL UNIQUE,
    created_by INTEGER NOT NULL REFERENCES web_accounts(id),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at    TEXT
);

-- Carry a deployed instance's existing admin (including its TOTP enrollment
-- and recovery codes) over into the new table so it keeps its login with no
-- re-setup. `web_admin` is left in place, unread, in case this needs undoing;
-- a later cleanup that drops it is its own issue if wanted.
INSERT INTO web_accounts (id, username, username_key, password_hash, session_key,
                           totp_secret, totp_enrolled_at, totp_last_step,
                           created_at, updated_at, created_by)
SELECT id, username, LOWER(username), password_hash, session_key,
       totp_secret, totp_enrolled_at, totp_last_step,
       created_at, updated_at, NULL
FROM web_admin;

INSERT INTO web_account_recovery_codes (account_id, code_hash, used_at, created_at)
SELECT 1, code_hash, used_at, created_at
FROM web_admin_recovery_codes
WHERE EXISTS (SELECT 1 FROM web_admin WHERE id = 1);
