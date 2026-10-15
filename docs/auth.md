# Authentication and secrets

## Web UI / API auth

The daemon's TCP web UI and JSON API (`/api/*`) bind to loopback (`127.0.0.1:7321`) by default and are unauthenticated
there. When you expose the surface beyond loopback (a non-loopback `web.bind`, or a reverse proxy that forwards to it),
the daemon requires a bearer token on every request to the web UI and `/api/*`:

```toml
# config.toml
[web]
bind = "0.0.0.0"
port = 7321
# Optional: name of a stored secret (see "Secrets store" below) holding a bearer token for API
# clients. The browser login does not need it: that credential is created at /setup on first run.
auth_token_ref = "web-ui-token"
# Optional, and only used while no account exists yet: the username for the pre-setup login form.
auth_username = "admin"
# Optional: require the token even on a loopback bind (off by default; useful for local testing).
require_auth_on_loopback = false
```

Two credentials answer that gate, for two kinds of caller.

**The browser: any number of accounts, created by setup or by invite.** An exposed instance with no account yet serves
exactly one page: `/setup`, which asks for a username and a password (minimum 12 characters) and creates the first
account. Everything else redirects there, and non-browser callers get `503` naming the state. The credential is an
argon2id hash in the `web_accounts` table, so it needs neither the secrets store, nor a key file, nor the CLI, and the
setup page closes itself the moment any account exists: a second POST to `/setup` is refused and redirected to
`/login`. The new credential applies to the next request, with no restart. This is what makes a container reachable
out of the box; before it, a non-loopback bind with no resolvable token took the whole web UI down, which no browser
could recover from.

Setup is trust-on-first-use: whoever reaches the exposed port first claims the instance. Bring it up behind the
reverse proxy (and whatever the proxy already enforces) and complete setup promptly. `no account yet` is logged at
`warn` on every start while the instance is unclaimed.

Every account after the first comes from an invite, not from `/setup`. Any signed-in account can open **Accounts**
(`/settings/accounts`) and create a single-use invite link, valid for 72 hours and shown exactly once. The invitee
opens `/invite/<token>`, picks their own username and password, and is signed in and sent straight to TOTP enrollment,
the same as first-run setup. Accepting an invite marks it used; a second visit to the same link is refused. The same
page lists every account and outstanding invite, and lets a signed-in account remove another account or revoke an
invite; removing the last remaining account, or removing yourself, is refused rather than silently ignored. There are
no roles yet: every account has full access.

Giving an email address on the invite form sends the link by mail instead of showing it: once SMTP is configured
through the `CLAUDE_RUN_SMTP_*` / `CLAUDE_RUN_PUBLIC_URL` environment variables (CLAUDE-428; the read-only **Email
(SMTP)** card in Settings shows the effective values), the field accepts an address and the page reports "Invite sent
to \<address\>" with no link on screen. The field is disabled, with a note pointing at that card, until SMTP is
configured; an invalid address is refused with `400` before anything is minted. If the send itself fails (bad
credentials, an unreachable host), the invite is kept rather than discarded, the page answers `502` with the error,
and the link is shown as a fallback so the invite is not stranded. Leaving the field blank keeps the original
link-only flow. See "Email (SMTP)" in [`docs/configuration.md`](configuration.md) for the mailer's own fields and
`src/mailer.rs` for the transport; the link it sends always comes from the configured `public_url`, never from the
request's `Host` header.

**Every account requires TOTP.** Setup and invite acceptance do not end at account creation: the new account is signed
in and sent straight to `/account/2fa/setup`, which shows a QR code (and the manual-entry `otpauth://` URL as a
fallback) and asks for a current code before enrollment completes. Confirming it shows a one-time set of recovery
codes, each usable exactly once, and returns to the dashboard. An account that is signed in but not yet enrolled cannot
reach any other page: every browser request redirects back to enrollment until it completes. Login itself is two steps
once enrolled: a correct password never mints a session by itself, it mints a short-lived pending cookie and redirects
to `/login/2fa`, and only a correct TOTP code (or an unused recovery code) turns that into a real session, scoped to
that one account. A stored TOTP secret the daemon cannot decrypt refuses the login with a visible error rather than
failing silently. Losing both the authenticator and the recovery codes is an operator's job to fix, not the browser's:
run `claude-run access reset-2fa <username>` against the daemon's database to clear that account's enrollment, and its
next login is sent back to `/account/2fa/setup` to enroll again.

**API clients: the bearer token.** Callers send `Authorization: Bearer <token>`. The value lives encrypted in the
secrets store (resolved once at daemon startup); only its name appears in `config.toml`. `auth_token_ref` is optional
now: when it is unset, or its secret cannot be decrypted, the reason is logged at `warn` and bearer auth is simply
unavailable for the run, while the browser login and setup are unaffected. Where no account exists, the older pairing
still signs in: `auth_username` plus the token plaintext as the password, so existing host deployments are unchanged.

Run the daemon behind a TLS-terminating reverse proxy when exposed; the token is bearer credentials and must not cross
the network in plaintext. The `/webhooks/` route keeps its own HMAC verification and is exempt from the gate, before and
after setup.

### Service auth (Settings -> Auth)

The daemon needs its own logins to do its own work: the scheduler polls YouTrack for queued issues and reads Forgejo
for CI failures long before any action-level `token_ref` is involved. Historically those calls used whatever the host
operator had logged in with (`~/.config/youtrack-cli`, `~/.local/share/forgejo-cli/keys.json`), which a container does
not have, so every poll failed with `no instance configured. Run 'yt auth login --base-url <url>'`.

The **Auth** card at the top of `/settings` takes those credentials in the browser:

| Credential | Fields | Equivalent CLI | Where it lands |
|---|---|---|---|
| Claude | interactive sign-in, or an API key | `claude auth login` / `ANTHROPIC_API_KEY` | the agent's own config dir / that env var on each child |
| YouTrack | base URL, token | `yt auth login --base-url <url>` | single-file `yt` config at `<CLAUDE_RUN_CONFIG_DIR>/auth/youtrack.yml`, named by `YOUTRACK_CLI_CONFIG` |
| Forgejo | host, token | `fj auth add-key` | `keys.json` in the daemon's XDG data dir, the same file `fj` writes |
| GitHub | host (default `github.com`), token | `gh auth login --with-token` | `gh`'s own `hosts.yml` under `<CLAUDE_RUN_CONFIG_DIR>/auth/gh`, named by `GH_CONFIG_DIR` |

Each value is encrypted into the secrets store under a fixed name (`auth-anthropic-api-key`, `auth-youtrack-token`,
`auth-forgejo-token`, `auth-github-token`); the `config.auth` settings document holds only the URL, the host, and those
names, and the page never renders a stored value back. Saving applies to the next invocation with no restart:
`YOUTRACK_CLI_CONFIG` and `GH_CONFIG_DIR` are exported once at serve start and point at fixed paths whose contents the
save rewrites, so every `yt`, `fj` and `gh` child (the runner's own calls, and every `claude` session it spawns) picks
up a saved login with no restart.

The GitHub token must be a **classic PAT**. `gh auth login --with-token` refuses one without `repo`, `read:org` and
`gist` ([gh manual](https://cli.github.com/manual/gh_auth_login)). GitHub automerge reads this login (not
`github.token_ref` or a webhook source's `write_token_ref`) and calls `gh pr view --json statusCheckRollup`, which a
fine-grained PAT cannot satisfy because it cannot read check runs; the call fails with `Resource not accessible by
personal access token (...statusCheckRollup...contexts.nodes.N)`. A fine-grained PAT is enough for opening PRs and reads.
For a personal-account repository there is no organization approval or SSO step. Replacing the token fixes PRs that
already exist, because it is read on every pass.

The YouTrack token needs **Update Project** permission on every project the daemon polls, not just issue read/write
access. YouTrack only returns a GitHub-bound project's VCS integration `path` (the `owner/repo` a `yt project vcs
list` resolves, see [Access check](usage.md#access-check-claude-216)) to a token with that permission; without it the field comes back empty and
the job's clone URL loses its repo path (`https://github.com/.git`), failing deep inside `git clone` with no
indication of the missing permission. A Forgejo/Gitea VCS integration is unaffected, since its `path` is a stored
value rather than one gated by a live external-app connection.

**Signing in to Claude from the page.** `claude auth login` is an OAuth flow, but its redirect is a paste-the-code
callback rather than a `localhost` listener, and with pipes on stdin/stdout it prints the authorize URL and then reads
the code from stdin. The Auth card brokers exactly that: **Sign in with Anthropic** spawns the CLI under the agent's
config dir, shows you the authorization link, and takes the code back through a form. A refused code shows the CLI's own
message and keeps the session open for another paste; **Cancel sign-in** kills the child. The child stays alive between
the two requests because it holds the PKCE verifier the code is redeemed against, so exactly one sign-in runs at a time
and an abandoned one expires after 15 minutes. The API key remains the right credential for API-billed deployments and
needs no browser at all.

**No Claude credential means nothing runs.** The runner's whole job is spawning `claude`, so an instance without a
Claude credential is not finished being set up: the scheduler polls no source, dispatches no action, and the trigger
endpoint answers `409` naming the reason. That is deliberate. Before it, a fresh container queried YouTrack every tick
and logged an auth error per action per tick, which buries real failures and would have burned attempts against
`max_attempts_per_key` on runs that could only fail. The dashboard carries a banner while it lasts, `/api/status`
reports `ready` plus `not_ready_reason`, and the transition is logged once in each direction rather than every tick. A
credential counts when any of these hold: a `.credentials.json` in the agent config dir, a stored API key, or
`ANTHROPIC_API_KEY` in the daemon's environment.

Precedence is unchanged where an action carries its own credentials: an action's `youtrack.token_ref` /
`forgejo.token_ref` is materialized per job and sets the same env in the child, so it still wins. These are the
defaults for everything else, including the daemon's own polling. An operator who prefers the CLIs can still run
`claude auth login`, `yt auth login` and `fj auth add-key` inside the container; leaving the Auth card empty writes
nothing and does not disturb those files.

**An expired or revoked login is a second, independent not-ready state (CLAUDE-269): `login_required`, distinct from
the `no_credential` state above.** `.credentials.json` stays on disk when a login expires, so the credential check
alone keeps answering ready; the runner instead notices the CLI itself saying so. Detection matches only the CLI's
structured `error: "authentication_failed"` field, never its human-readable wording (which is reworded between
releases): every `claude` invocation is scanned for it (a terminal `result`/`assistant` event, or three repeated
`system`/`api_retry` events, whichever comes first, so a dead login is caught well below `claude_timeout_seconds`
rather than exhausting it), and a background probe additionally runs `claude auth status --json` at startup, every 10
minutes while ready, and every 60 seconds while login-required (skipped entirely on the `no_credential` state, since
there is nothing to probe yet). A spawn failure, a timeout, or unparseable probe output is logged and changes
nothing: it is neither evidence of a login nor of its absence. While login-required, exactly like `no_credential`,
the scheduler dispatches nothing, the trigger endpoint answers `409`, the CI-triage webhook accepts and drops each
delivery (recording why as a `run_events` row), which is not recovered once signed in again, and the `/claude` comment webhook replies on the thread naming the
reason instead of running anything; unlike a global pause, no job spends an attempt because of it. A run already in
flight stops picking up new jobs and closes with `exit::LOGIN_REQUIRED` (8), distinct from `exit::PAUSED`: an operator
cancel wins over both, and a global pause wins over login-required when both hold. The dashboard-and-every-page banner,
the Settings Auth card (badged `login expired`, with the CLI's own message), and `/api/status`'s `not_ready_kind` /
`login_required_since` fields all cover this state alongside `no_credential`. It clears with no restart the moment any
of the following happens: a brokered sign-in completes, an Anthropic API key is saved, or the status probe reports
`loggedIn: true`.

### Secrets store

Auth (and every action `token_ref`) reads from an encrypted secrets store. Two things are required before
`auth_token_ref` resolves:

1. **A key file, which the runner provides.** The store is encrypted with an
   [`age`](https://github.com/FiloSottile/age) X25519 identity at `<CLAUDE_RUN_CONFIG_DIR>/secrets.key`, generated at
   serve start (mode `0600`) when the instance has none. You do not run `age-keygen` to get started.

   The key lives with the config, not the data, on purpose: the ciphertext it decrypts is in the state store (the
   `state.db` file under the data dir, or the `secrets` table on a PostgreSQL backend), and a key stored beside its
   ciphertext buys nothing. Keeping them apart is what lets the database be copied, snapshotted or backed up without
   carrying the tokens along. **Back the key up separately**, because every stored
   secret is unrecoverable without it. A key already at the old `<CLAUDE_RUN_DATA_DIR>/secrets.key` is moved to the new
   path on the next start rather than replaced, so an existing deployment keeps reading what it stored.

   Generation only happens while the store is empty. A missing key file with secrets already in the database means the
   key was lost, and the daemon says so, naming the file and the secrets at risk, rather than minting a replacement that
   would turn all of them into permanently undecryptable bytes.

   `CLAUDE_RUN_SECRETS_KEY_FILE` overrides the path when you want to supply the key yourself, from a secret store or a
   mounted file. That file does not have to be `age-keygen` output: the key is read from wherever it appears on a line,
   so a SOPS-decrypted `data: AGE-SECRET-KEY-...` or an env-style `CLAUDE_RUN_AGE_KEY=AGE-SECRET-KEY-...` works as-is. A
   key file readable beyond its owner is logged at `warn` (every process that can read it can decrypt every stored
   token) but still loads, since an operator's own delivery may hand it over `0644` deliberately. In the container,
   deliver it as a compose secret and point `CLAUDE_RUN_SECRETS_KEY_FILE` at it; see [self-hosting.md](self-hosting.md).
   A missing/unset key file is exactly the `secret: CLAUDE_RUN_SECRETS_KEY_FILE is not set` error that disables the TCP
   web UI on an exposed bind.

   Back this file up. If you lose it, every stored secret becomes undecryptable and must be re-entered under a new key.

2. **The secret itself.** Create it with the CLI (no daemon required - it writes straight to the state DB, so it works
   while the daemon is down for exactly this bootstrap):

   ```sh
   printf %s 'a-strong-random-string' | claude-run secret set web-ui-token
   ```

   The value is read from stdin (never argv, so it stays out of shell history and `ps`) and this same value is both the
   bearer token and the browser-login password. Then set `web.auth_token_ref = "web-ui-token"` (the secret **name**, not
   its value), optionally `web.auth_username`, switch `web.bind` to the exposed address, and restart so the daemon
   decrypts the secret and brings up the gated TCP listener. `claude-run secret list` shows stored names (never values);
   `claude-run secret delete <name>` removes one.

   Alternatively, secrets can be created on the web UI Secrets page, but on an exposed bind that page is gated by the
   very token you are setting up, so you would first have to start on a loopback bind (the default `bind = "127.0.0.1"`,
   unauthenticated) and create the secret at `http://127.0.0.1:7321/secrets`. The CLI avoids that loopback round-trip.

   `auth_token_ref` is the **name** of a stored secret. It is not the token value and not the `age` public key.
