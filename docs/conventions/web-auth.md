# Web UI authentication

Full text of the `CLAUDE.md` conventions on this topic, moved there verbatim by CLAUDE-370. The index in `CLAUDE.md` links each heading below.

## Setup bootstrap

The web UI bootstraps itself: an exposed instance with no credential serves only `/setup`, which writes an argon2id admin hash to the `web_admin` table (`src/web_admin.rs`, migration `0020`) and is refused once one exists. `auth::resolve_gate` now returns a gate carrying whichever credentials exist (bearer token from `auth_token_ref`, DB administrator, or neither), and an unresolvable `auth_token_ref` is a `warn` with bearer auth disabled for the run rather than the old fail-closed that dropped the whole TCP listener. That old behaviour was unrecoverable in a container: the CLI that would store the secret is not the interface a container operator has. The gate holds the administrator behind an `Arc<RwLock<..>>` the setup handler swaps, so a first-run setup authenticates the next request with no restart. Sessions are HMAC'd with the admin's `session_key`, rotated on a password change, falling back to the token where no administrator exists. Where no administrator exists the pre-setup `auth_username` + token-plaintext login still works. The container image seeds `deploy/container-config.toml` (`bind = "0.0.0.0"`) into an empty config volume, because a loopback bind in a container is reachable from nothing outside it.

## Mandatory TOTP (CLAUDE-368)

CLAUDE-368 makes TOTP mandatory for every `web_admin` row, not optional, because a password alone was the whole story for the browser account that can pause the daemon and edit every action. `web_admin` gained `totp_secret` (encrypted at rest through `SecretsKey`, the same age-based store `secrets.rs` already uses for the token/service-login ciphertext), `totp_enrolled_at`, and `totp_last_step`, plus a new `web_admin_recovery_codes` table of argon2id-hashed, single-use codes (`migrations/{sqlite,postgres}/00{23,4}_web_admin_totp.sql`; `src/import.rs`'s `TABLES` and `IDENTITY_COLUMNS` carry the new table so a backend switch preserves it). `src/web_admin.rs` owns enrollment (`begin_totp_enrollment` mints a `totp-rs` secret and an inline-SVG QR via `qrcodegen`, `confirm_totp_enrollment` verifies the first code and returns one-time recovery codes) and verification (`verify_totp_or_recovery`, `try_consume_recovery_code`), plus `reset_totp` for the CLI escape hatch. Replay protection is one field doing double duty: `totp_last_step` advances on both enrollment confirmation and every successful login, and a code is refused whenever the matched step is not strictly newer, so the exact code that confirmed enrollment can never be replayed as a login. `src/auth.rs` gates on it: `login_submit` no longer mints a session on a correct password alone when the admin is enrolled, it mints a short-lived HMAC'd pending-2FA cookie (`PENDING_2FA_COOKIE`, keyed by the admin's `session_key` so a password change invalidates it too, capped at 5 attempts) and redirects to `/login/2fa`, which is the only place a session cookie is minted for an enrolled admin. `require_token`'s existing session-cookie branch gained an enforcement block: a signed-in but unenrolled admin is redirected to `/account/2fa/setup` on every HTML GET and 403'd otherwise, so no other page is reachable until enrollment completes; `setup_submit` sends a freshly created admin straight there instead of to `/`. A `totp_secret` the daemon cannot decrypt (a rotated `secrets.key`, corrupted ciphertext) fails login visibly, logged at `error`, rather than silently. `claude-run access reset-2fa` (`src/bin/claude-run.rs`, DB-direct like `access check`) clears enrollment and recovery codes and rotates the session key, for an operator recovering a locked-out administrator; the next login is sent back to `/account/2fa/setup`.

## Invite by email (CLAUDE-394)

The Accounts page invite form (`create_invite_submit` in `src/auth.rs`) takes an optional `email`. Blank keeps the
original one-time-link flow unchanged. A non-blank address first has to parse as a `lettre::message::Mailbox`
(refused `400`, nothing minted) and the `config.email` document has to be `EmailConfig::configured()` (`smtp_host`,
`from`, and `public_url` all set; refused `400` naming the Settings card, nothing minted) before anything happens.
Only then is the invite minted and the send attempted, in that order: a send failure keeps the invite rather than
rolling it back, answers `502` with the SMTP error, and falls back to showing the link, so a configured-but-broken
mailer never strands an invite nobody can reach. `render_accounts` carries a `StatusCode` on every call site for
exactly this: every refused or failed path above answers non-2xx, not just a visually-flagged `200`.

`src/mailer.rs`'s `Mailer` is built fresh per send from the DB-backed `EmailConfig` plus the resolved SMTP password
(`crate::secrets::resolve_secret`), not cached, so a saved settings change (including a rotated password) applies to
the very next invite with no daemon restart; it deliberately does not derive `Debug`, since it holds that password in
memory for the call's lifetime. The invite link in the email body is always built from the configured `public_url`,
never from the request's `Host` header, because an invite emailed from one browser session is not necessarily
accepted from the same one. Logging around a send names the recipient address but never the invite token, the full
link, or the SMTP password; a send failure's log line carries the lettre/SMTP error text only.
