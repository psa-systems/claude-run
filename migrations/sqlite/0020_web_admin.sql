-- The web UI's own administrator credential, so a fresh instance can be brought
-- up entirely from the browser.
--
-- Before this, the only web login was `web.auth_username` plus the plaintext of
-- the `web.auth_token_ref` secret, which lives age-encrypted in `secrets` and
-- therefore needs CLAUDE_RUN_SECRETS_KEY_FILE, a key file on disk, and the
-- `claude-run secret set` CLI. In a container that is a chicken-and-egg: the
-- daemon refuses to expose the TCP surface without a resolvable token, so the
-- operator cannot reach the UI to configure the very thing that would let them
-- in. This table holds an argon2id PHC hash instead, written by the first-run
-- setup page, and needs neither the secrets store nor the CLI.
--
-- One row, enforced by the CHECK: this is the instance's admin, not a user
-- table. `session_key` signs the login session cookies, so rotating the
-- password (which rotates the key) invalidates every outstanding session.
CREATE TABLE IF NOT EXISTS web_admin (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    session_key   TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
