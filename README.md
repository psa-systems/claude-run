# claude-run

YAML-driven runner for Claude Code. Poll an external source (YouTrack issues, Forgejo PRs, whatever produces a JSON
array), feed each job to `claude --print`, and do something with the result. Single Rust binary, doubles as a CLI and a
long-running daemon.

## What it gives you

- **Per-action YAML files** describe input (a nu snippet that emits a JSON array), an optional filter, pre and post nu
  snippets, and the prompt sent to claude. See [`actions/SCHEMA.md`](actions/SCHEMA.md).
- **Scheduling**: each action declares `schedule.kind: interval | cron | manual`; the daemon's built-in scheduler fires
  on time without external cron.
- **Per-job streaming logs**: claude is invoked with `--output-format stream-json` and the NDJSON events land in
  `$STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`. `tail -f` to watch live.
- **Cost ledger + caps**: per-day Claude-invocation cap, per-day USD budget, per-job attempt cap. All in SQLite.
- **Skip-attempt marker**: an action's post step can write `$workdir/.claude-run-skip-attempt` for outcomes that park a
  job pending human input (so the attempt cap doesn't trip).
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick.
- **Duplicate-source guardrail**: at scan time the daemon flags (but never merges or disables) two enabled actions that
  do redundant work - names that differ only by case, or two native actions of the same `kind` polling the same source.
  Both would gather the same jobs and silently compete on the shared concurrency caps; you get a WARN log and a notice
  on each colliding action instead of discovering the contention during an incident.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`,
  `/resume`, ... The CLI client speaks that API when the daemon is reachable, else operates on the DB directly.

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

| Default                        | Env override             | What                                                                                            |
|--------------------------------|--------------------------|-------------------------------------------------------------------------------------------------|
| `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR`  | `config.toml`, `actions/*.yml`                                                                  |
| `$XDG_DATA_HOME/claude-run/`   | `CLAUDE_RUN_DATA_DIR`    | `state.db`, `workdirs/`                                                                         |
| `$XDG_STATE_HOME/claude-run/`  | `CLAUDE_RUN_STATE_DIR`   | `logs/<action>/YYYY-MM-DD.jsonl`, `logs/<action>/streams/<key>-attempt-<n>.jsonl`, `daemon.log` |
| `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` | `daemon.pid`, `daemon.sock`, `locks/<action>.lock`                                              |

`workdir_base` (action YAML field or `CLAUDE_RUN_WORKDIR_BASE` env) overrides where per-job workdirs live; defaults to
`$DATA_DIR/workdirs/`. Supports `~`, `$VAR`, `${VAR}` expansion.

## Settings: bootstrap file vs the Settings page (CLAUDE-101)

Configuration splits in two. `config.toml` is the **bootstrap** file: only what the daemon must know before it can open
the DB and bind the listener, namely `[web]` (bind / port / `auth_token_ref` / login) and `[daemon]` binary overrides (
`nu_binary`, `claude_binary`) plus `[daemon].claude_config_dir` (CLAUDE-165: where the spawned agent reads its
user-level `CLAUDE.md` / credentials from; defaults to `<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`, seeded at serve
start with the slim bot `CLAUDE.md` and a symlink to the host `~/.claude/.credentials.json`). Everything runtime-tunable, namely issue minting (`[mint]`), the update train (
`[update]`), the webhook sources (`[webhook]`), and the log filter (`[daemon].log_filter`), now lives in the database
and is edited in the app on the **Settings** page (`/settings`, in the nav next to Dashboard), behind the same auth gate
as the rest of the web UI. A `config.toml` containing only `[web]` and `[daemon]` binary overrides is a complete, valid
configuration.

Each section is one JSON document validated against the same serde struct the file parser used (`deny_unknown_fields`),
so an invalid edit is rejected inline with the parser's own error and nothing is persisted. Secret values are never
shown or stored on the page: fields ending in `_ref` (`auth_token_ref`, a webhook source's `secret_ref` /
`log_token_ref`, `[update].token_ref`) name a credential in the Secrets store and are rendered as that name only. Each
section (and field) carries an apply badge: `live` for the log filter and webhook source edits (allow-list, rate limit,
secret refs, adding a source), `next event` / `next invocation` for mint / update, and `restart` for the webhook
`replay_window_secs` (the replay store is built once at start). The same documents round-trip over the JSON API at
`GET` / `PUT /api/settings/{mint,update,webhook,log_filter,workdir_gc,preferences,concurrency}`. The `workdir_gc`
(CLAUDE-106), `preferences` (CLAUDE-154), and `concurrency` (CLAUDE-162) sections are DB-only from birth (never
`config.toml` keys), so they are not part of the one-time import below; each simply
defaults until edited.

**One-time import, then breaking.** The first daemon start after upgrading imports any of the four moved sections still
present in `config.toml` into the database and logs a warning naming each one and telling you to remove it from the
file. After that, remove the migrated sections from `config.toml`: a later start that finds a section in **both** the
file and the database refuses to run, naming the section, so there is never silent drift between two sources of truth (
the same breaking-migration signal CLAUDE-87 used). A `config.toml` with no moved sections starts cleanly with the
database as the only source.

## Logging

Log verbosity is a [tracing
`EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives)
directive string. Precedence is `CLAUDE_RUN_LOG` (env var) > the stored log filter (Settings page, formerly
`[daemon].log_filter`) > the built-in default `info,claude_run=info,sqlx=warn`.

The persistent filter is now edited on the **Settings** page (the `Log filter` section, or`PUT /api/settings/log_filter`
with `{"filter": "info,claude_run=debug,sqlx=warn"}`). Saving applies it live: the daemon swaps the active filter
through the tracing reload handle and logs a `log filter applied from settings` line, no restart needed. Set
`CLAUDE_RUN_LOG=...` for one-shot debugging: when it is present it wins, and a saved filter persists but is not applied
for that run. An invalid filter is rejected inline (and at startup logs a warning naming the value and the parse error,
keeping the current filter rather than aborting).

## Workdir GC (CLAUDE-106)

The YouTrack runner intentionally keeps some workdirs rather than deleting them: a directory that holds unpushed commits
is preserved when no PR was opened, and a leftover workdir is moved aside (`<key>.kept-<n>`) out of the way of a fresh
attempt. Nothing else ever deletes these, so preserved and aside workdirs accumulate without bound under
`$CLAUDE_RUN_DATA_DIR/workdirs/<source>/<key>` (a single preserved Rust workdir can be tens of GB of `target/` output).

A time-based sweeper bounds that disk use. It runs on `serve` start and then hourly, walking `workdirs/*/*` and deleting
any directory whose last-modified time is older than the retention window. A directory that still holds unpushed commits
is respected (kept) until a separate, longer hard-cap age, so genuinely un-saved work is not discarded the moment the
window passes. Every deletion logs the action, key, path, age, and bytes reclaimed, so a sweep is auditable.

The windows are a DB-backed runtime setting edited on the **Settings** page (the `Workdir GC` section, or `GET` /
`PUT /api/settings/workdir_gc`). Fields: `enabled` (master toggle, default `true`), `retention_days` (default `14`), and
`unpushed_hard_cap_days` (default `60`). Edits apply on the next sweep. Set `enabled: false` to park the sweeper without
losing the configured windows.

Known limitation: the handful of startup lines emitted before the stored filter is applied (e.g. the config-source line)
are governed by the initial filter, not the saved one; use `CLAUDE_RUN_LOG` when those specific lines are needed at
`debug`.

## Display preferences (CLAUDE-154)

Every timestamp is stored as an RFC3339 UTC string; how it is displayed in the web UI is a single global setting (this is a single-operator daemon with no user table, so there are no per-user rows). The `preferences` section is DB-backed and edited on the **Settings** page (the `Display preferences` section, or `GET` / `PUT /api/settings/preferences`). Fields:

- `timezone`: an IANA name (e.g. `America/New_York`, default `UTC`). Every displayed date is converted into it before formatting.
- `datetime_format`: a chrono strftime pattern (default `%Y-%m-%d %H:%M:%S %Z`, e.g. `2026-07-01 00:00:00 UTC`). The Settings page offers preset buttons (ISO-seconds `%Y-%m-%d %H:%M:%S`, ISO-minutes `%Y-%m-%d %H:%M`, US `%m/%d/%Y %I:%M:%S %p`, EU `%d.%m.%Y %H:%M:%S`, RFC3339 `%Y-%m-%dT%H:%M:%S%:z`) that fill the field, plus free-form editing.
- `locale`: a BCP-47 locale (default `en-US`). Advisory / reserved for now: it is stored and round-trips through the API but does NOT yet localize month / weekday names (that needs chrono's `unstable-locales` feature). Wiring it live is tracked in CLAUDE-155.

Edits apply live (the dashboard, run and action detail pages, the secrets table, and the live job-stream log all reformat on the next render). Saving an unknown timezone or an unparseable strftime format is rejected inline and nothing is persisted. With the defaults (`UTC` + ISO-with-seconds) displayed times equal today's stored UTC values; only the surface format changes, never the underlying data. Elapsed durations (run duration, the +offset in the live log) and `report` analytics are wall-clock spans, not dates, and are left unformatted.

## Continuous pickup and concurrency caps (CLAUDE-162)

The daemon does not serialize runs per action. An issue queued while a run is in flight is picked up by a new,
**overlapping** run within `regather_interval` (default 60s) rather than waiting for the in-flight run to drain, and it is
never blocked behind an unrelated in-flight run of another action. A manual `trigger` during an in-flight run likewise
starts an overlapping pickup run instead of being silently dropped. Concurrency is bounded by three limits:

- **Per-action `max_parallel`** (the action's `concurrency.max_parallel`, default 1), now enforced **daemon-wide**: the
  total number of concurrent jobs for an action stays `<= max_parallel` across all its overlapping runs, not just within
  one run.
- **Global `max_parallel_global`** caps total concurrent jobs across every action, so overlapping runs cannot spawn an
  unbounded number of concurrent `claude` processes. Unset (`0`) it defaults to the maximum enabled-action
  `max_parallel`, so a single action still reaches its full per-action cap out of the box.
- **Per-key serialization** (unchanged): same-repo / same-project jobs still serialize by the daemon-wide key gate, so
  distinct repos parallelize but two jobs against the same repo never run at once, even across overlapping runs.

Each job acquires a global permit, then the per-action permit, then the key gate, in that fixed order, so the pipeline is
deadlock-free. Two overlapping runs never both work the same job: each run drops issues already in flight in another open
run (an admission check that also suppresses empty, redundant runs), the native `youtrack` handler re-reads the live `AI
Agent` field per job, and the key gate serializes same-repo work. The daily cap and budget stay best-effort; because up
to `max_parallel_global` jobs can pass the pre-launch check before any advances the ledger, the documented overshoot
bound is `max_parallel_global - 1`.

`max_parallel_global` and `regather_interval` are a DB-backed runtime setting (the `concurrency` section) edited on the
**Settings** page or via `GET` / `PUT /api/settings/concurrency`; fields are `max_parallel_global` (default `0` = auto)
and `regather_interval_secs` (default `60`, must be `>= 1`). Both apply on the next scheduler tick.

> The whole-run per-action `flock` that used to force a full drain is gone on the daemon path. The standalone
> `claude-run run <action>` path still takes it as a cross-process guard; running standalone `run` for an action the
> daemon also schedules is unsupported concurrent use.

## Self-update (release trains)

`claude-run version {show,check,update}` self-updates the binary from the Forgejo Generic Package registry. CI publishes
two channels ("trains"), and the stored update settings pick the default; `--train <release|latest>` overrides it per
invocation. The `train` and `token_ref` are edited on the **Settings** page (`Update train` section, formerly `[update]`
in `config.toml`), or over the API:

```jsonc
// PUT /api/settings/update
{
  // Default channel: "release" (newest semver vX.Y.Z, the default) or "latest" (moving main build).
  "train": "release",
  // Optional: name of a stored secret (see "Secrets store") holding the registry
  // token, used for private packages. null stays anonymous-first.
  "token_ref": "registry-token"
}
```

- **`release`** resolves the newest semver `vX.Y.Z` package, ignoring the moving `latest` tag. `check` compares it
  against the running version by semver.
- **`latest`** tracks the moving `latest` package (re-published on every push to main). Because that tag is non-semver
  and moving, `check` reports update-available by comparing the running binary's SHA-256 against the published
  `latest.sha256` sidecar rather than by semver.
- `--version <tag>` pins an exact published tag and is independent of the train.

Auth is anonymous-first. The registry is queried with no credentials first; a token is consulted only when the registry answers 401/403. When `[update].token_ref` is set, the named secret is decrypted from the store (same mechanism as `web.auth_token_ref` and action `token_ref`s; the raw token never appears in `config.toml`) and used for the registry requests. When it is unset, a private registry still falls back to the host `fj` token on a 401/403 with `claude-run version check --use-fj-token` (the interactive `fj`-token escalation is CLI-only). The download is always verified against the published SHA-256 sidecar before the binary is atomically swapped in place.

The `age` key file is only consulted on registry escalation (CLAUDE-158): a configured `token_ref` is decrypted lazily inside the 401/403 handler, not up front, so an anonymously-readable registry succeeds with no key file even when `token_ref` is set, and `--use-fj-token` escalates without touching the age store at all. This means the CLI process (not just the daemon) must have `CLAUDE_RUN_SECRETS_KEY_FILE` in its environment when the configured `token_ref` is the escalation path: the daemon gets it from its unit's `Environment=` line, but an interactive shell running `claude-run version check` / `version update` does not inherit that, so export it (same value as the daemon unit) or pass `--use-fj-token`. When escalation genuinely needs the key file and it is missing, the error names both remedies rather than the bare `CLAUDE_RUN_SECRETS_KEY_FILE is not set`.

The same check / apply is available from the browser on the **About** page (linked in the nav). "Check for updates" runs
the on-demand registry check (anonymous-first, escalating only to the configured `token_ref`) and shows current vs
newest with whether an update is available; a registry/network failure is surfaced as a non-fatal message rather than an
error page. "Update now" downloads the newest build on the configured train, verifies its SHA-256, and atomically
replaces the binary. It does **not** restart the daemon: after a successful apply the page shows the applied tag and "
Restart Claude Run service to use the new version", which you do yourself (systemd, container, nohup, ...). A failed
apply (checksum mismatch, unwritable binary path, registry unreachable) leaves the running binary untouched. Version
pinning (`--version <tag>`) stays a CLI-only option; the web applies the newest build on the configured train. There is
no background or periodic update check; checks happen only on demand.

## Web UI / API auth

The daemon's TCP web UI and JSON API (`/api/*`) bind to loopback (`127.0.0.1:7321`) by default and are unauthenticated
there. When you expose the surface beyond loopback (a non-loopback `web.bind`, or a reverse proxy that forwards to it),
the daemon requires a bearer token on every request to the web UI and `/api/*`:

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

Callers send `Authorization: Bearer <token>`. The token value lives encrypted in the secrets store (resolved once at
daemon startup); only its name appears in `config.toml`. If `web.bind` is non-loopback but `auth_token_ref` is unset or
its secret cannot be decrypted, the daemon fails closed and does not bring up the TCP listener at all, rather than serve
an unauthenticated surface. Run the daemon behind a TLS-terminating reverse proxy when exposed; the token is bearer
credentials and must not cross the network in plaintext. The `/webhooks/` route keeps its own HMAC verification and is
exempt from the token gate.

### Secrets store

Auth (and every action `token_ref`) reads from an encrypted secrets store. Two things are required before
`auth_token_ref` resolves:

1. **A key file.** The store is encrypted with an [`age`](https://github.com/FiloSottile/age) X25519 identity. Generate
   one and point `CLAUDE_RUN_SECRETS_KEY_FILE` at it (the daemon reads only this env var; it is never put in
   `config.toml`):

   ```sh
   age-keygen -o /etc/claude-run/secrets.key
   chmod 600 /etc/claude-run/secrets.key   # readable only by the service user
   ```

   Under systemd, set the env var in the unit (e.g. `systemctl edit claude-run` -> `[Service]` ->
   `Environment=CLAUDE_RUN_SECRETS_KEY_FILE=/etc/claude-run/secrets.key`, then `systemctl daemon-reload`). A
   missing/unset key file is exactly the `secret: CLAUDE_RUN_SECRETS_KEY_FILE is not set` error that disables the TCP
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

## Webhook (Forgejo)

The daemon turns a Forgejo `action_run_failure` delivery into a triaged YouTrack issue in seconds, instead of waiting
for the next scheduled poll. The pipeline is `POST /webhooks/forgejo/<source>` (HMAC-verified, replay-protected,
rate-limited) -> internal-route log fetch + redaction -> hybrid triage -> issue mint, all inside the running daemon. The
scheduled `youtrack` / `forgejo-ci-fix` actions remain the backstop and dedupe against webhook-minted issues through the
same hidden markers, so a poll that races the webhook never files a second issue.

The listener is multi-source (CLAUDE-87): each `<source>` is a `[webhook.sources.<source>]` config block with its own
HMAC secret and allow-list, served at its own URL path. One Forgejo org webhook per source means a leaked org secret
authenticates only that org's allow-listed payloads, and each source has its own rate-limit bucket so one org's failure
storm cannot rate-starve another's deliveries. The replay window is shared across sources (the `X-Forgejo-Delivery` id
is globally unique, so a duplicate delivered to two sources is still caught). The source name is the URL path segment
verbatim, so use lowercase `[a-z0-9_-]`; a name needing URL escaping is warned about at load.

Triage sorts each verified failure into one of three buckets: a failing PR check that a code change fixes (`code_fix`)
mints an issue and sets `AI Agent = Queued` so the runner opens the fix PR; a failure no PR fixes (`human_action`,
e.g. a main-branch / tag / release / missing-secret failure) mints a human-assigned issue with no PR; a transient /
infra / flaky failure (`transient`) mints nothing. A failing run triggered by the fix bot's own login mints nothing (
loop safety), and the reused per-key + daily caps throttle a storm of failures.

**CLAUDE-149: a human's failing PR is left to its author; the bot only fixes its own.** When the failing run already has
an open PR, the webhook `code_fix` (queue-for-fix) route is suppressed: no issue is minted and no fix is queued, because
the author owns the red CI on their own branch. A `human_action` failure on an open PR still mints (it needs a human, not
the bot). The `forgejo-ci-fix` action is retargeted to match: it now auto-fixes ONLY the bot's own failing PRs (those
authored by `bot_author`, the PRs the bot opened working a queued issue) and skips everyone else's. On its own PR the bot
keeps pushing corrective commits until CI passes; after `max_pr_attempts` distinct fix pushes it stops and flags the PR
Needs Review (a PR comment, the `needs-review` label, and best-effort sets the source issue's State and assignee). The
`enabled` toggle defaults to `true` and is a master kill switch.

**CLAUDE-153: the webhook is a configurable action.** It is named `webhook-forgejo` (formerly `webhook-fast-path`), and a per-webhook `auto_fix` toggle on the `Webhook` settings gates whether a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) queues a corrective fixup PR. It defaults to `false` (opt-in): with `auto_fix` off, a red PR check mints nothing and queues no fixup PR, so the failure stays reported on the original PR for its author to fix in place (a `human_action` failure still mints); with it on, the corrective-fixup behavior above applies, still subject to the CLAUDE-149 open-PR suppression. This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. The `webhook-forgejo` action also participates in **Pause** like every other action: Pause/Resume controls and a Paused badge appear on the dashboard, and while it is paused an inbound `POST /webhooks/forgejo/<source>` is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, but the delivery is dropped without minting or queuing so Forgejo does not retry. Edit / export / delete remain rejected (it is synthetic and has no backing YAML).

A minted issue is routed to a YouTrack project in this order (CLAUDE-86): an explicit `repo_project_map` override, then
the project whose **enabled VCS integration** claims the failing repo, then `fallback_project`. The VCS lookup is the
primary mechanism: the repo-to-project binding already lives in YouTrack (each project's enabled `GiteaChangesProcessor`
carries the forge `serverUrl` + `path`), so adding a repo to a project's VCS settings is the only step needed for
routing to follow. There is no per-repo config to keep in sync. The inverse map is built from `yt project list --json` +
`yt project vcs list <KEY> --enabled-only --json` (non-archived projects only), cached for 15 minutes, and refreshed
once on a lookup miss so a just-added integration is picked up without a daemon restart. Matching is case-insensitive on
both host and path and ignores trailing slashes. When more than one project claims the same repo the first by short
name (ascending) wins, with a warning naming all candidates; a single project's failing `vcs` call is logged and skipped
rather than disabling routing. The mint journal line names the routing source (`override` / `vcs` / `fallback`) so a
wrong destination is diagnosable.

The webhook sources and the mint routing are edited on the **Settings** page (the `Webhook sources` and `Issue minting`
sections, formerly `[webhook]` and `[mint]` in `config.toml`; CLAUDE-101), or over the API. Adding / editing a source (
allow-list, rate limit, `secret_ref`, `log_token_ref`) and the webhook `auto_fix` toggle (CLAUDE-153) take effect live;
`replay_window_secs` applies on restart. A mint edit is honored by the next event.

### Auto-fix for mechanically-fixable failures (CLAUDE-104)

Some CI failures are verification-only and have a known, judgement-free fix. The canonical case is `cargo fmt --check`:
the fix is `cargo fmt --all`. The general case is any failure of a configured checks workflow (default `check.yml`),
whose steps (fmt, clippy, build, tests) check code rather than produce it. The old triage classified a
`cargo fmt --check` failure on a push to `main` (no open PR) as `human_action` with a "human must commit the result
directly to main" analysis. That is backwards: it manufactures manual toil for a mechanical fix.

The auto-fix framework (the `auto_fix` block in the `mint` settings) adds a deterministic detector layer that runs
inside triage AFTER the transient/no-log rules (so a flaky run of a checks workflow stays transient, never auto-fixed)
but BEFORE the LLM classifier. Two detectors ship: `cargo-fmt` (matches the rustfmt `Diff in ...` signature, fix
`cargo fmt --all`) and `checks-workflow` (matches `workflow_id` against `checks_workflows`, handing the runner the
failing step). A match short-circuits to the `auto_fix` triage bucket, which routes exactly like `code_fix` (mint a
queued issue, `AI Agent=Queued`, no human assignee) but applies regardless of `has_open_pr` and on a push to a
protected/main branch, so it never reaches the classifier prompt that would label it `human_action`. The minted issue's
summary (`Auto-fix: cargo fmt on <repo> (run #N, <sha>)`) and description name the concrete problem, the fix command,
and the delivery instruction: commit on a fresh `chore/<kind>-<short-sha>` branch and open a PR against `main` via the
configured PR strategy (CLAUDE-103), never a direct push to main. The `has_open_pr == true` case is handled by CLAUDE-149
instead: this auto-fix path applies only when there is no open PR. When a PR is already open the webhook `code_fix` route
is suppressed (a human's PR is left to its author), and `forgejo-ci-fix` separately fixes the bot's own failing PRs,
iterating until green or flagging Needs Review. Dedupe markers are unchanged, so repeats of the same
`(repo, workflow_id, commit_sha)` still group.

### Migration from the single-source listener (CLAUDE-87)

This is a **breaking config change**: the old top-level `webhook.secret_ref` / `webhook.allowed_repos` /
`webhook.rate_limit_per_min` keys are gone, and `[webhook.sources.*]` is the only shape. A config still carrying an old
key fails to parse at serve start with serde's unknown-field error naming the offending key. That is the intended
migration signal. To switch from the single SITE-level system webhook to one ORG-level webhook per org:

1. Generate and store one secret per org:
   `printf %s '<distinct-random-value>' | claude-run secret set forgejo-hmac-<org>` for each org that sends
   webhooks (e.g. `acme`, `globex`).
2. In Forgejo, on each org's Settings -> Webhooks, add a **Forgejo** webhook with URL
   `https://<daemon-host>/webhooks/forgejo/<source>`, Secret set to that org's stored value, and **only** the Action Run
   Failure event selected. Selecting only the Failure event keeps `success` / `recover` deliveries off the wire
   entirely; any that do arrive are acked and dropped.
3. Replace the old `[webhook]` block in `config.toml` with the sources shape above and restart the daemon. It refuses to
   start while any old key is present, naming it.
4. Delete the SITE-level system webhook (and any old per-repo pilot webhook).
5. Verify: one failing run per org produces a `verified action_run_failure received` log line carrying the right`source`
   field. The old shared secret can be deleted after a burn-in period.

## Containerized deployment

Running the daemon on the host means every `claude` child it spawns inherits the operator's `$HOME`, so Claude Code
loads the operator's interactive `~/.claude/CLAUDE.md` (which prescribes a full Git + YouTrack workflow) and contradicts
the local-only agent contract. The container runtime (CLAUDE-37) fixes that: the whole daemon runs in a container with a
clean `$HOME`, so each child inherits a slim, bot-specific user memory instead.

**Host (non-container) deployments (CLAUDE-165).** When the daemon runs directly on the host it now achieves the same
isolation without a clean `$HOME`: at serve start it seeds `[daemon].claude_config_dir` (default
`<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`) with the embedded slim bot `CLAUDE.md` (the same `deploy/bot-CLAUDE.md` the
container bakes in) and, on Linux, a symlink `<dir>/.credentials.json` to the host login at `~/.claude/.credentials.json`,
then spawns every `claude` child with `CLAUDE_CONFIG_DIR=<dir>`. That relocates where Claude Code reads the user-level
`CLAUDE.md` and credentials, so the child loads the bot memory instead of the operator's global `~/.claude/CLAUDE.md`
while staying authenticated with the shared host login; `HOME` is left inherited so git/ssh keep working, and (unlike
`--bare`) MCP servers, hooks, and skills stay enabled. If the host credential file is absent at serve start the daemon
fails loudly naming the path (run `claude` once to log in first). On macOS credentials live in the Keychain rather than
`.credentials.json`, so the symlink step is Linux-only.

**Image.** `oci-build/Dockerfile` has a `runtime` target built on the CI image (`dev.a8n.run/pandoras-box/rust-builder-musl`).
It carries `claude-run` plus the pinned runtime toolchain: `nu`, the `claude` CLI, coreutils `timeout`, `git`, `yt` (
youtrack-cli), and `fj` (forgejo-cli). Versions are build args (`NU_VERSION`, `CLAUDE_CODE_VERSION`, `YT_VERSION`,
`FJ_VERSION` / `FJ_URL`); override any one with `--build-arg`. The container runs as the non-root user `claudebot` with
`$HOME=/home/claudebot`.

**Published image (CLAUDE-88).** CI publishes the runtime image to the Forgejo OCI registry on every push to main (
`:latest`) and every release tag (`:vX.Y.Z`), the same tag set as the binary's Generic Package. The image publish runs
in its own workflow, `.forgejo/workflows/build-image.yml` (split out of `build-binary.yml` in CLAUDE-94), so the binary
and image publish independently. Pull it with:

```nu
docker pull dev.a8n.run/psa-systems/claude-run:latest
```

Pin `:vX.Y.Z` in production; `:latest` tracks main. Container deployments update by pulling a newer tag and recreating
the service; the binary's `claude-run version update` self-update does not apply inside a container:

```nu
docker compose pull
docker compose up --detach
```

**Slim bot CLAUDE.md.** `deploy/bot-CLAUDE.md` is versioned in this repo and `COPY`d to
`/home/claudebot/.claude/CLAUDE.md`. It contains only the local-only agent contract (no push, no PR, no YouTrack writes;
leave the feature branch as HEAD; the runner finalizes), the em-dash ban, the Nushell 0.112.2 note, and the
simplest-solution preference. It has no Git/YouTrack workflow and no lifecycle section, so the agent is never told to
push, open PRs, or transition issue state.

**compose.yml.** Consumes the published image (`image: dev.a8n.run/psa-systems/claude-run:latest`; the local `build:`block
stays as a commented development alternative). App-prefixed names (service `claude-bot`, network`claude-bot-private`,
volumes `claude-bot-{config,data,state}`; prefix each with `dev-` for a development stack).

**Mounts.** What each one is for:

| Mount                                            | Container path                                       | Purpose                                                                                             |
|--------------------------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| volume `claude-bot-config`                       | `/home/claudebot/.config/claude-run`                 | `config.toml` and the actions dir (`CLAUDE_RUN_CONFIG_DIR`)                                         |
| volume `claude-bot-data`                         | `/home/claudebot/.local/share/claude-run`            | `state.db` (runs, outcomes, encrypted secrets store) and per-job workdirs (`CLAUDE_RUN_DATA_DIR`)   |
| volume `claude-bot-state`                        | `/home/claudebot/.local/state/claude-run`            | logs, per-attempt stream JSONL, webhook log artifacts (`CLAUDE_RUN_STATE_DIR`)                      |
| bind `./actions` (ro)                            | `/home/claudebot/.config/claude-run/actions`         | action YAML under version control                                                                   |
| bind `~/.claude/.credentials.json` (ro)          | `/home/claudebot/.claude/.credentials.json`          | Claude CLI auth (alternative: `ANTHROPIC_API_KEY` env)                                              |
| bind `~/.config/youtrack-cli` (ro)               | `/home/claudebot/.config/youtrack-cli`               | `yt` auth/config                                                                                    |
| bind `~/.local/share/forgejo-cli/keys.json` (ro) | `/home/claudebot/.local/share/forgejo-cli/keys.json` | `fj` auth                                                                                           |
| bind `~/.gitconfig` (ro)                         | `/home/claudebot/.gitconfig`                         | git identity for clones/commits                                                                     |
| secret `claude-run-age-key`                      | `/run/secrets/claude-run-age-key`                    | age identity decrypting the secrets store                                                           |
| tmpfs                                            | `/home/claudebot/.runtime/claude-run`                | daemon unix socket (`CLAUDE_RUN_RUNTIME_DIR`); ephemeral so a stale socket never survives a restart |

Only the single Claude credentials file is mounted, never the whole `~/.claude`. Mounting `~/.claude` wholesale would
re-shadow the baked slim CLAUDE.md (the exact regression this removes). To use an API key instead of mounted
credentials, drop that bind and set `ANTHROPIC_API_KEY` in the service `environment`.

**Secrets.** The store itself (web auth token, webhook HMACs, registry token refs) lives encrypted inside `state.db` on
the data volume; the only material the container needs injected is the age identity that decrypts it. It travels as a
compose `secrets:` file mount, and `CLAUDE_RUN_SECRETS_KEY_FILE` in the `environment:` block points at
`/run/secrets/claude-run-age-key`. The env var carries the PATH, never the key itself: key material in a plain
environment variable is deliberately unsupported (visible to `docker inspect` and `/proc/<pid>/environ`).
`ANTHROPIC_API_KEY` is the one secret that MAY go through plain env, as the Claude-auth alternative above. Before first
start:

```nu
mkdir secrets
chmod 700 secrets
# migrating from a host install: copy the EXISTING key instead of generating,
# or the secrets already in state.db cannot be decrypted
age-keygen | save secrets/age-key.txt
```

**Networking.** To expose the web UI / webhook listener, set `[web].bind` to a non-loopback address in the container's
`config.toml` (e.g. `0.0.0.0`) and publish the port (`ports:` mapping or a reverse-proxy network). A non-loopback bind
requires token auth: `web.auth_token_ref` must name a secret stored via `claude-run secret set` (see "Web UI / API
auth").

```nu
docker compose up --detach      # start the daemon from the published image
docker compose logs --follow    # watch the scheduler
```

See [CLAUDE.md](CLAUDE.md) for the project-specific deviations from `niceguyit/governance` and the phased rollout plan.

## License

MIT. See [LICENSE.md](LICENSE.md).
