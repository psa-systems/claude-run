# claude-run

YAML-driven runner for Claude Code. Poll an external source (YouTrack issues, Forgejo PRs, whatever produces a JSON array), feed each job to `claude --print`, and do something with the result. Single Rust binary, doubles as a CLI and a long-running daemon.

## What it gives you

- **Per-action YAML files** describe input (a nu snippet that emits a JSON array), an optional filter, pre and post nu snippets, and the prompt sent to claude. See [`actions/SCHEMA.md`](actions/SCHEMA.md).
- **Scheduling**: each action declares `schedule.kind: interval | cron | manual`; the daemon's built-in scheduler fires on time without external cron.
- **Per-job streaming logs**: claude is invoked with `--output-format stream-json` and the NDJSON events land in `$STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`. `tail -f` to watch live.
- **Cost ledger + caps**: per-day Claude-invocation cap, per-day USD budget, per-job attempt cap. All in SQLite.
- **Skip-attempt marker**: an action's post step can write `$workdir/.claude-run-skip-attempt` for outcomes that park a job pending human input (so the attempt cap doesn't trip).
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`, `/resume`, ... The CLI client speaks that API when the daemon is reachable, else operates on the DB directly.

## Install

Pull the latest binary from the Forgejo Generic Packages registry:

```nu
http get https://dev.a8n.run/api/packages/psa-systems/generic/claude-run/latest/claude-run-linux-x86_64 | save claude-run
chmod +x claude-run
sudo mv claude-run /usr/local/bin/
```

Runtime dependencies (host-installed, not bundled):

- `nu` 0.112.2+ - executes the action snippets.
- `claude` (Claude Code CLI) - the AI invocation itself.
- `timeout` (coreutils) - wall-clock cap around claude.
- Whatever CLIs the actions themselves invoke (`yt`, `fj`, `git`, ...).

## Use

```nu
# One-shot manual run (cli-driven; standalone). Same exit codes as the old nu script.
claude-run run youtrack

# Long-running daemon: scheduler + unix-socket API. --detach forks into the background.
claude-run serve --detach

# Manual out-of-cycle run via the daemon (counts as triggered_by=trigger in the runs table).
claude-run trigger youtrack

# Status (delegates to daemon if it's up, else reads DB directly).
claude-run status

# Pause / resume.
claude-run pause youtrack --reason "addressing a blocker"
claude-run resume youtrack

# Action management.
claude-run action list
claude-run action show youtrack

# Secrets (encrypted at rest under CLAUDE_RUN_SECRETS_KEY_FILE; value read from stdin, never argv).
printf %s $token | claude-run secret set web-ui-token
claude-run secret list
claude-run secret delete web-ui-token

# Import legacy ~/.local/share/claude-run/state/*.json into SQLite (one-time migration off the nu runner).
claude-run migrate

# Self-update. `check` reports whether a newer build is published; `update` swaps the binary in place.
claude-run version                         # print the build-stamped banner (same as `version show`)
claude-run version check                   # uses the [update].train default (release)
claude-run version check --train latest    # track the moving main build instead
claude-run version update --yes            # install the newest release-train build
claude-run version update --train latest   # install the moving `latest` build
claude-run version update --version v0.2.0 # pin an exact tag (independent of train)
```

## Filesystem layout

XDG-compliant; each path overridable by the matching env var.

| Default | Env override | What |
|---|---|---|
| `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR` | `config.toml`, `actions/*.yml` |
| `$XDG_DATA_HOME/claude-run/` | `CLAUDE_RUN_DATA_DIR` | `state.db`, `workdirs/` |
| `$XDG_STATE_HOME/claude-run/` | `CLAUDE_RUN_STATE_DIR` | `logs/<action>/YYYY-MM-DD.jsonl`, `logs/<action>/streams/<key>-attempt-<n>.jsonl`, `daemon.log` |
| `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` | `daemon.pid`, `daemon.sock`, `locks/<action>.lock` |

`workdir_base` (action YAML field or `CLAUDE_RUN_WORKDIR_BASE` env) overrides where per-job workdirs live; defaults to `$DATA_DIR/workdirs/`. Supports `~`, `$VAR`, `${VAR}` expansion.

## Logging

Log verbosity is a [tracing `EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives) directive string. Precedence is `CLAUDE_RUN_LOG` (env var) > `[daemon].log_filter` (config.toml) > the built-in default `info,claude_run=info,sqlx=warn`.

```toml
# config.toml
[daemon]
# Persistent log filter. Same syntax as the CLAUDE_RUN_LOG env var.
log_filter = "info,claude_run=debug,sqlx=warn"
```

Set `[daemon].log_filter` for a persistent change (no systemd override needed); it is applied right after config load and logs a `log filter applied from config.toml` line. Set `CLAUDE_RUN_LOG=...` for one-shot debugging: when it is present it wins and the config field is ignored. An invalid `log_filter` value is not fatal: the daemon logs a warning naming the value and the parse error, and keeps running on the current filter.

Known limitation: the handful of startup lines emitted before config load (e.g. the config-source line) are governed by the initial filter, not `[daemon].log_filter`; use `CLAUDE_RUN_LOG` when those specific lines are needed at `debug`.

## Self-update (release trains)

`claude-run version {show,check,update}` self-updates the binary from the Forgejo Generic Package registry. CI publishes two channels ("trains"), and `[update].train` in `config.toml` picks the default; `--train <release|latest>` overrides it per invocation:

```toml
# config.toml
[update]
# Default channel: "release" (newest semver vX.Y.Z, the default) or "latest" (moving main build).
train = "release"
# Optional: name of a stored secret (see "Secrets store") holding the registry token,
# used for private packages. Unset stays anonymous-first.
token_ref = "registry-token"
```

- **`release`** resolves the newest semver `vX.Y.Z` package, ignoring the moving `latest` tag. `check` compares it against the running version by semver.
- **`latest`** tracks the moving `latest` package (re-published on every push to main). Because that tag is non-semver and moving, `check` reports update-available by comparing the running binary's SHA-256 against the published `latest.sha256` sidecar rather than by semver.
- `--version <tag>` pins an exact published tag and is independent of the train.

Auth is anonymous-first. When `[update].token_ref` is set, the named secret is decrypted from the store (same mechanism as `web.auth_token_ref` and action `token_ref`s; the raw token never appears in `config.toml`) and used for the registry requests. When it is unset, a private registry still falls back to the host `fj` token on a 401/403 with `claude-run version check --use-fj-token`. The download is always verified against the published SHA-256 sidecar before the binary is atomically swapped in place.

## Web UI / API auth

The daemon's TCP web UI and JSON API (`/api/*`) bind to loopback (`127.0.0.1:7321`) by default and are unauthenticated there. When you expose the surface beyond loopback (a non-loopback `web.bind`, or a reverse proxy that forwards to it), the daemon requires a bearer token on every request to the web UI and `/api/*`:

```toml
# config.toml
[web]
bind = "0.0.0.0"
port = 7321
# Name of a stored secret (see "Secrets store" below) holding the bearer token / web password.
auth_token_ref = "web-ui-token"
# Optional: enable the browser login form with this username (lets a password manager sign in).
auth_username = "admin"
# Optional: require the token even on a loopback bind (off by default; useful for local testing).
require_auth_on_loopback = false
```

Callers send `Authorization: Bearer <token>`. The token value lives encrypted in the secrets store (resolved once at daemon startup); only its name appears in `config.toml`. If `web.bind` is non-loopback but `auth_token_ref` is unset or its secret cannot be decrypted, the daemon fails closed and does not bring up the TCP listener at all, rather than serve an unauthenticated surface. Run the daemon behind a TLS-terminating reverse proxy when exposed; the token is bearer credentials and must not cross the network in plaintext. The `/webhooks/` route keeps its own HMAC verification and is exempt from the token gate.

### Non-loopback bind and the boot race

A non-loopback `web.bind` (e.g. `198.51.100.11`) can only bind once the kernel has assigned that IP to an interface. At boot the daemon frequently starts before the NIC is up, so the first bind fails with `EADDRNOTAVAIL`. The daemon classifies this case and retries the bind every 5s indefinitely (logging a warn on the first failure and a throttled progress line about once per minute) until the address is available, then logs `web UI listening`; the unix-socket API and scheduler run normally the whole time. Other bind failures are not a transient race, so they are not retried: `AddrInUse` (another process holds the port) and `PermissionDenied` (privileged port) log a classified reason with a hint and leave the web UI disabled for that run while the rest of the daemon keeps running.

The retry loop is the in-process safety net; the proper fix is to order the unit after the network is up. For any non-loopback `web.bind`, add the network-online ordering to the systemd unit so the IP is assigned before the daemon starts:

```ini
[Unit]
After=network-online.target
Wants=network-online.target
```

Every startup also logs which config file was read: an `info` line `config loaded path=<file>` when `config.toml` was found, or a `warn` line `config.toml not found; built-in defaults in use (web bind 127.0.0.1:7321)` when it was absent (under systemd this commonly means the resolved `CLAUDE_RUN_CONFIG_DIR` / `XDG_CONFIG_HOME` / `HOME` differs from your shell, so the file the shell sees is not the one the service loads). The effective bind is logged before the attempt, so `journalctl` alone answers where the listening address came from.

### Browser login (for password managers)

A bearer header is awkward in a browser (a password manager such as Bitwarden fills a username/password form, not an `Authorization` header). Set `web.auth_username` to enable a form login: an unauthenticated browser request (a GET that accepts HTML) is redirected to `/login`, where the configured **username** plus the **`auth_token_ref` secret value as the password** mint a session cookie (`HttpOnly`, `Secure`, `SameSite=Lax`, 12-hour lifetime). `GET /logout` clears it. The session is an HMAC over the token, so rotating the secret invalidates every outstanding session. `Authorization: Bearer <token>` continues to work for API clients regardless of `auth_username`. Because the cookie is `Secure`, the login flow only works over HTTPS, i.e. behind the reverse proxy (which is the only context where the surface is exposed anyway).

### Secrets store

Auth (and every action `token_ref`) reads from an encrypted secrets store. Two things are required before `auth_token_ref` resolves:

1. **A key file.** The store is encrypted with an [`age`](https://github.com/FiloSottile/age) X25519 identity. Generate one and point `CLAUDE_RUN_SECRETS_KEY_FILE` at it (the daemon reads only this env var; it is never put in `config.toml`):

   ```sh
   age-keygen -o /etc/claude-run/secrets.key
   chmod 600 /etc/claude-run/secrets.key   # readable only by the service user
   ```

   Under systemd, set the env var in the unit (e.g. `systemctl edit claude-run` -> `[Service]` -> `Environment=CLAUDE_RUN_SECRETS_KEY_FILE=/etc/claude-run/secrets.key`, then `systemctl daemon-reload`). A missing/unset key file is exactly the `secret: CLAUDE_RUN_SECRETS_KEY_FILE is not set` error that disables the TCP web UI on an exposed bind.

   Back this file up. If you lose it, every stored secret becomes undecryptable and must be re-entered under a new key.

2. **The secret itself.** Create it with the CLI (no daemon required - it writes straight to the state DB, so it works while the daemon is down for exactly this bootstrap):

   ```sh
   printf %s 'a-strong-random-string' | claude-run secret set web-ui-token
   ```

   The value is read from stdin (never argv, so it stays out of shell history and `ps`) and this same value is both the bearer token and the browser-login password. Then set `web.auth_token_ref = "web-ui-token"` (the secret **name**, not its value), optionally `web.auth_username`, switch `web.bind` to the exposed address, and restart so the daemon decrypts the secret and brings up the gated TCP listener. `claude-run secret list` shows stored names (never values); `claude-run secret delete <name>` removes one.

   Alternatively, secrets can be created on the web UI Secrets page, but on an exposed bind that page is gated by the very token you are setting up, so you would first have to start on a loopback bind (the default `bind = "127.0.0.1"`, unauthenticated) and create the secret at `http://127.0.0.1:7321/secrets`. The CLI avoids that loopback round-trip.

   `auth_token_ref` is the **name** of a stored secret. It is not the token value and not the `age` public key.

## Webhook fast-path

The daemon turns a Forgejo `action_run_failure` delivery into a triaged YouTrack issue in seconds, instead of waiting for the next scheduled poll. The pipeline is `POST /webhooks/forgejo` (HMAC-verified, replay-protected, rate-limited) -> internal-route log fetch + redaction -> hybrid triage -> issue mint, all inside the running daemon. The scheduled `youtrack` / `forgejo-ci-fix` actions remain the backstop and dedupe against webhook-minted issues through the same hidden markers, so a poll that races the webhook never files a second issue.

Triage sorts each verified failure into one of three buckets: a failing PR check that a code change fixes (`code_fix`) mints an issue and sets `AI Agent = Queued` so `forgejo-ci-fix` opens the fix PR; a failure no PR fixes (`human_action`, e.g. a main-branch / tag / release / missing-secret failure) mints a human-assigned issue with no PR; a transient / infra / flaky failure (`transient`) mints nothing. A failing run triggered by the fix bot's own login mints nothing (loop safety), and the reused per-key + daily caps throttle a storm of failures.

A minted issue is routed to a YouTrack project in this order (CLAUDE-86): an explicit `repo_project_map` override, then the project whose **enabled VCS integration** claims the failing repo, then `fallback_project`. The VCS lookup is the primary mechanism: the repo-to-project binding already lives in YouTrack (each project's enabled `GiteaChangesProcessor` carries the forge `serverUrl` + `path`), so adding a repo to a project's VCS settings is the only step needed for routing to follow. There is no per-repo config to keep in sync. The inverse map is built from `yt project list --json` + `yt project vcs <KEY> --enabled-only --json` (non-archived projects only), cached for 15 minutes, and refreshed once on a lookup miss so a just-added integration is picked up without a daemon restart. Matching is case-insensitive on both host and path and ignores trailing slashes. When more than one project claims the same repo the first by short name (ascending) wins, with a warning naming all candidates; a single project's failing `vcs` call is logged and skipped rather than disabling routing. The mint journal line names the routing source (`override` / `vcs` / `fallback`) so a wrong destination is diagnosable.

```toml
# config.toml

[webhook]
# Name of the stored secret (see CLAUDE-26 secrets) holding the Forgejo HMAC key.
secret_ref = "forgejo-webhook-secret"
# Repo allow-list. Only failures from matching repos are acted on; every other
# authenticated delivery is acked and dropped. Patterns: exact "owner/repo",
# per-owner "owner/*", or "*" for every repo (pair "*" with a Forgejo SYSTEM
# webhook so all repos deliver). Empty list denies all.
allowed_repos = ["pandoras-box/forgejo-cli"]
replay_window_secs = 300
rate_limit_per_min = 60

[mint]
# Project for any repo no VCS integration claims. Routing is normally driven by
# each YouTrack project's VCS settings, so this is only the last-resort default.
fallback_project = "CLAUDE"
# bot_logins defaults to ["claude-run"] (the fix bot's login); add others here.

# repo_project_map is an OPTIONAL override, consulted before the VCS lookup. Add
# an entry only to force a repo to a project that does not (or should not) own
# the repo's VCS integration; otherwise leave it empty and let YouTrack's VCS
# settings drive routing.
[mint.repo_project_map]
# "pandoras-box/forgejo-cli" = "FJ"
```

The mint needs the `yt` (and, for the `has_open_pr` triage signal and the fix PR, `fj`) CLIs on the daemon host. With no usable backend the consumer still ingests and logs failures but mints nothing.

Register the webhook on the pilot repo with **only** the Action Run Failure event (Forgejo: repo Settings -> Webhooks -> Add Webhook -> Forgejo), pointing at the daemon's `/webhooks/forgejo` URL, with the same HMAC secret stored under `secret_ref`. Selecting only the Failure event keeps `success` / `recover` deliveries off the wire entirely; any that do arrive are acked and dropped.

## Development

```nu
just --list           # show all recipes
just check            # fmt + clippy + build + tests (what CI runs)
just test             # cargo test
just build            # debug build into target/debug/
just build-static     # musl-static binary via the Docker builder into ./output/claude-run
just build-docker     # full OCI build (builder stage)
just build-docker-export   # extract the static binary out of the OCI build
just install-hooks    # one-time: install pre-commit hook that runs CI's checks
```

See [CLAUDE.md](CLAUDE.md) for the project-specific deviations from `niceguyit/governance` and the phased rollout plan.

## License

MIT. See [LICENSE.md](LICENSE.md).
