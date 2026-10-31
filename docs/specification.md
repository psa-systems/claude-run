# claude-run Specification

This document is normative. It describes claude-run as implemented in the source tree (Rust crate, SQL migrations,
templates, container build files), and code that disagrees with it is a defect. Requirements that code cites carry a
`SPEC-<section>` identifier, for example `SPEC-3.3.1`.

## 1. Purpose and scope

claude-run is a single-binary daemon plus CLI that drives the Claude Code CLI (`claude`) from work sources. It feeds
three kinds of input to Claude sessions and acts on the results:

- Issue-tracker issues: a YouTrack issue marked queued is cloned, worked on by Claude, and turned into a pull request.
- CI failures: signed webhook deliveries from a Forgejo or GitHub instance are triaged, and a tracker issue is created
  or updated for them.
- Pull-request comments: a comment starting with a configurable trigger (default `/claude`) runs Claude against the
  pull-request branch or turns a plain-issue comment into a queued tracker issue.

Users are operators of one runner instance. The web UI, the HTTP API and the CLI control a single daemon with a single
state store.

It does not: host a git forge or tracker, terminate TLS (a reverse proxy is expected), provide per-user permissions
beyond "authenticated account", write to the tracker state outside the configured fields, or poll for CI failures
(failures arrive only by webhook).

## 2. Architecture

### 2.1 Build and runtime

- One Cargo package `claude-run` with a library (`src/lib.rs`) and one binary (`src/bin/claude-run.rs`). No workspace.
- Rust 2021, Tokio multi-thread runtime, axum 0.8 for HTTP, sqlx 0.9 (`any` driver, SQLite and PostgreSQL), askama
  templates for the web UI, clap 4 for the CLI, `tracing` with JSON output for logs, `age` for secret encryption,
  `argon2` and `totp-rs` for web accounts, `lettre` for SMTP, `rmcp` for MCP, `reqwest` (rustls) for outbound HTTP.
- `build.rs` embeds the 12-character git SHA and the UTC build date (from `CLAUDE_RUN_GIT_SHA` / `CLAUDE_RUN_BUILD_DATE`
  or a local probe, else `unknown`); both appear in `claude_run::VERSION`.
- Migrations, templates, `assets/htmx.min.js`, `assets/idiomorph-ext.min.js` and `deploy/bot-CLAUDE.md` are embedded at
  compile time.

### 2.2 Process model

`claude-run serve` starts one daemon. It holds a single-instance pidfile lock (`flock`, non-blocking) in the runtime
directory and runs these concurrent tasks:

- Scheduler: a 5-second tick. For each enabled, unpaused, non-`manual` action it fires a pickup run when the
  re-gather interval (setting `concurrency.regather_interval_secs`, default 60) has elapsed, no gather for that action
  is in flight, and the action's `concurrency.max_parallel` cap has a free slot. The scheduler does nothing while the
  daemon is globally paused or while the Claude credential is not ready.
- Trigger consumer: runs actions requested by `trigger`.
- HTTP servers: an optional unix-socket API server (disabled by `--no-socket`), a TCP server (web UI, `/api`,
  webhook routes), and an optional MCP listener.
- Webhook consumers: CI-failure events, pull-request events, action-success events (automerge) and comment commands.
- Background loops: automerge reconciler (300-second tick), garbage-collection sweeps (hourly tick), history-table
  retention sweep, Claude login status probe (every 600 seconds when ready, every 60 seconds when login is required).
- On start the daemon reaps run rows left open by a dead process (closed with the `UNEXPECTED` exit code), reconciles
  tracker issues stranded in the working state, scans the actions directory, and records the serve start time.
- On SIGTERM the daemon shuts down gracefully with a 30-second grace window. Runs found open at shutdown are closed
  with exit code 130 (cancelled).

The TCP bind retries every 5 seconds while the address is not yet assigned. Any other bind failure disables the TCP
surface for that run and is logged.

### 2.3 Data flow

```
tracker issue (queued) -> youtrack action -> clone -> claude session -> draft PR -> review session -> ready PR
forge CI failure -> webhook -> verify/replay/rate-limit -> log fetch + redact -> triage -> tracker issue
PR comment "/claude ..." -> webhook -> authorize -> claude session -> push within caps -> reply
```

Every external effect goes through external CLIs resolved from `PATH`: `yt` (YouTrack), `fj` (Forgejo), `gh` and a
GitHub MCP endpoint (GitHub), `git`, `claude`, `nu` (default interpreter for scripted actions), `timeout` and `docker`.

### 2.4 Source layout

| Path | Role |
|---|---|
| `src/bin/claude-run.rs` | CLI definition and subcommand handlers |
| `src/daemon.rs` | serve loop, scheduler, listeners, background loops |
| `src/action.rs` | action YAML schema and validation |
| `src/native/` | native action handlers (`youtrack`, `closed-issue-summary`, audit prompt) |
| `src/job.rs`, `src/claude.rs` | per-job pipeline, `claude` invocation, usage and budget accounting |
| `src/provider/` | tracker and forge backends (`yt`, `fj`, `gh` command wrappers) |
| `src/web/` | web UI handlers and the two webhook listeners |
| `src/auth.rs`, `src/web_admin.rs` | TCP-surface authentication, accounts, TOTP, invites |
| `src/api.rs` | JSON API |
| `src/mcp.rs`, `src/mcp_http.rs` | read-only troubleshooting MCP server (stdio and HTTP) |
| `src/triage.rs`, `src/autofix.rs`, `src/mint.rs`, `src/ci_failure/` | CI-failure triage, issue mint, failure store |
| `src/secrets.rs`, `src/service_auth.rs` | encrypted secrets store, service credential materialization |
| `src/settings.rs`, `src/config.rs` | `config.toml` and DB-backed settings |
| `src/state.rs`, `src/db.rs` | state store queries, pool and migrations |
| `src/gc.rs`, `src/retention.rs` | file and row retention |
| `migrations/sqlite`, `migrations/postgres` | schema |
| `templates/`, `assets/` | web UI |
| `oci-build/`, `compose.yml`, `deploy/` | container image and deployment |
| `actions/` | shipped example action files |

## 3. Functional requirements

### 3.1 Actions

- An action is a YAML file `*.yml` in `<config dir>/actions/` (override: `--actions-dir` / `CLAUDE_RUN_ACTIONS_DIR`).
  Parsing rejects unknown fields (`deny_unknown_fields`). A file that fails to parse is recorded with a parse error and
  never scheduled. The directory is rescanned every scheduler tick, so new files need no restart.
- Action kinds: `scripted` (default), `youtrack`, `closed-issue-summary`. A native kind requires a matching `native`
  block.
- `scripted` runs `input.command` (emits a JSON array) -> `filter` -> `pre` -> `prompt` -> `claude` -> `post`; each
  script is a plain string or `{run, shell}`, with `nu` as the default interpreter.
- Schedule: `kind: interval` (`humantime` duration, e.g. `30m`), `kind: cron` (5-field or 6-field expression), or
  `kind: manual` (only `trigger` fires it).
- Per-action `limits`: `max_attempts_per_key` (default 3), `max_runs_per_day` (20), `claude_timeout_seconds` (600),
  `daily_budget_usd` (5.00), `run_budget_usd`, `budget_warn_fraction`, `max_issues_per_run`, `review_timeout_seconds`
  (1800). `concurrency` has `max_parallel` and a `key` list; two jobs sharing a concurrency key never overlap, across
  scheduled runs and manual triggers.
- `claude` block: `cwd`, `allowed_tools`, `permission_mode`, `model`, `effort`, `autocompact`. Precedence for model,
  effort and autocompact: action value, then the `claude` settings section, then the Claude CLI default (autocompact
  falls back to a built-in default window instead).
- The daemon refuses a daily run beyond `max_runs_per_day` (exit code 4) and a run over budget (exit code 6).
- A per-key attempt counter parks a job once it reaches the cap; `action reset-attempts` clears it.

### 3.2 Claude invocation

Each job runs `timeout <seconds>s claude --print --verbose --output-format=stream-json --permission-mode=... --allowed-tools=... [--model] [--effort] --autocompact=... [--max-budget-usd] --disallowedTools=Monitor,ScheduleWakeup
<prompt>` with stdin closed, in its own process group (cancellation signals the group). Environment for the child sets
`CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` and sets `BASH_DEFAULT_TIMEOUT_MS` / `BASH_MAX_TIMEOUT_MS` to the invocation's
timeout. `CLAUDE_RUN_DATABASE_URL`, `CLAUDE_RUN_DATABASE_URL_FILE`, `CLAUDE_RUN_SMTP_PASSWORD` and
`CLAUDE_RUN_SMTP_PASSWORD_FILE` are removed from the child environment. Stdout events are timestamped into a per-attempt
stream log under `<state dir>/logs/<action>/streams/`. Exit code 124 from `timeout` is recorded as a timed-out outcome.
Each run appends one JSON record (tokens, estimated cost, duration, tool counts) to `<state dir>/logs/usage.jsonl`, and
each invocation is recorded in `claude_invocations`.

### 3.3 YouTrack action (`kind: youtrack`)

- Gather: query the tracker with `native.youtrack.query` (default `-Resolved AI Agent: Queued`, with the placeholder
  `{{audit_project}}` replaced by `audit_project`, default `AUDIT`), resolve each issue's repository from its project's
  enabled VCS integration, drop issues with no integration, and drop issues whose repository the forge token cannot
  read (they stay queued).
- Eligibility, checked at gather time and again after winning the concurrency gate: the custom field (default `AI Agent`)
  must still equal the queued value, and the dependency gate must pass. The dependency gate blocks on open issues linked
  by the configured verbs (defaults: `depends on`, `parent for`), leaves the issue queued, comments the blockers once,
  and re-evaluates after `recheck_timeout`.
- Pre: set the field to working and `State` to the in-progress value, then `git clone --depth 50`.
- Claude works in the local clone; the runner pushes the result (see 3.3.1). The agent may create issues in
  `write_projects` (default: the triggering issue's project), linked `relates to` / `depends on` / `is required for`,
  never as a subtask.
- Finalize runs on every path and is the only place external mutations happen. Every path first applies 3.3.1, then:
  - marker file `.claude-run-needs-review`: commits pushed, field set to needs-review, no PR, `Needs-Info` tag added (name
    configurable under `tags.needs_info`), attempt not counted.
  - marker file `.claude-run-no-code-change`: field set to `states.no_code` (default: the done label), marker text posted
    as a comment, no PR. Any commits the attempt made are pushed.
  - success with commits: a draft PR is created (branch strategy `branch` by default, `agit` opt-in), a fresh read-only
    review session runs, and a pass marker `.claude-run-review-pass` promotes the PR to ready and sets the field to
    code-review. A fail marker `.claude-run-review-fail` or no verdict keeps the PR draft, posts the findings on the PR and
    the issue, and sets the field to queued (below the attempt cap) or needs-review.
  - hard failure: commits pushed, field set to failed. A push or PR-create failure with a recorded open PR below the
    attempt cap requeues instead.
  - timeout or cancel: the final push in 3.3.1 runs; the checkpoint push (every 60 seconds while `claude` runs) is the
    backstop if the runner dies first. A draft PR is opened and the issue lands in needs-review.

#### 3.3.1 Push invariant (`SPEC-3.3.1`)

- Every attempt that produces one or more commits ends with them on a branch on the forge, on every path: success, hard
  failure, timeout, cancel, needs-review, no-code-change and failed review. A run is never recorded finished while its
  commits exist only in the local clone.
- The branch name is deterministic per issue, so a requeued issue continues the same branch. A pushed branch does not
  imply a PR; PR creation follows the path rules above.
- A failed final push is a failure: non-zero exit, field set to failed, a `run_events` row naming the branch and the
  redacted git error. It is never reported as success.
- When the repository's `pre-push` hook rejects the final push, the runner pushes the same commits with `--no-verify`,
  opens the PR as a draft, and posts the hook output on the PR and the issue. The hook's verdict is reported, not used
  to withhold the work. The checkpoint push already uses `--no-verify`.
- A fresh clone carries the remote's tags (CLAUDE-484), so a repository check that reads tags gives the same verdict in a
  run as on a developer machine.
- The runner never sets `State` to done. A tracker-side workflow resolves the issue when its PR merges
  (`docs/yt-workflow-resolve-on-pr-merge.js`).
- Optional `automerge` (default off): wait up to `wait_seconds` (1800) polling checks every `check_interval_seconds`
  (30), then merge, and set the field to done. A reconciler every 300 seconds and action-success webhooks drive the same
  decision for recorded agent PRs.
- Targets: `forgejo` (`base_url`, `token_ref`, `pr_strategy`) and `github` (`hosts`, `mcp_endpoint`, `token_ref`). A job
  whose repository host is `github.com` or listed in `github.hosts` uses the GitHub backend; any other host uses `fj`.
- Prompt: `prompt` overrides; otherwise `prompt_preset` is `default` or `audit`.

### 3.4 Closed-issue summary action (`kind: closed-issue-summary`)

Starts no Claude session and clones nothing. For each nominated parent issue (`parents`, plus issues matching `query`), it
lists issues resolved in the last `window_days` across `projects`, renders counts and a per-project listing, files the
report as a new issue in `report_project` (default: the parent's project), links it with `link_type`, and posts a hidden
marker comment (`Report-Window:`) on the parent so one report is filed per window.

### 3.5 CI-failure webhook

- Accepts Action-run deliveries (Forgejo `action_run_*`, GitHub `workflow_run`). Only a failure for an allow-listed
  repository is processed. A `success` or `recover` action goes to the automerge pipeline. A `pull_request` delivery
  with action `closed` reconciles recorded agent PRs; other PR actions are acknowledged and dropped.
- Processing: fetch the job logs (`fj actions logs`), redact secrets, fingerprint the failure, store it in
  `ci_failures`, then triage into a bucket and act:
  - `code_fix`: create a tracker issue; set the queued value only when `webhook.auto_fix` is true.
  - `auto_fix`: set only by deterministic detectors (`cargo-fmt` signature; failing workflows named in
    `mint.auto_fix.checks_workflows`, default `check.yml`) when the failing branch has no open PR; skipped when
    `mint.auto_fix.enabled` is false.
  - `human_action`: create an issue assigned to `mint.human_assignee` when set, no queue.
  - `transient`: no issue. An infrastructure failure kind is stored on the failure row.
- Branches matching `mint.bot_branch_prefixes` (default `renovate/`) never route to `code_fix` or `auto_fix`.
- Failures from logins listed in `mint.bot_logins` do not mint (loop safety). Open-PR routing: a failure on the agent's own
  open PR requeues its issue until `webhook.agent_pr_fix_attempts` (default 3) is reached, then the issue is set to
  needs-review; a failure on a human's PR is commented on that PR.
- Issues are created in the project whose VCS integration claims the repository, with `mint.repo_project_map` consulted
  first and `mint.fallback_project` (default `CLAUDE`) last. State on creation is `mint.target_state` (default `To do`).
  The redacted log is attached.
- Throttling uses the synthetic action `webhook-triage`: `webhook.max_runs_per_day` (default 20) and a per-key attempt
  counter.

### 3.6 Comment-command webhook

- A `created` issue or PR comment from a non-bot author whose first line starts with `comment.trigger` (default `/claude`)
  and whose body is at most `max_instruction_chars` (default 4000) characters.
- The commenter's repository permission is read with the write-scoped token and must match `allowed_permissions`
  (default `write`, `admin`; a Forgejo level also admits every higher level).
- Pull request: clone the head branch, run Claude with the instruction, commit, and push only when the diff is within
  `max_files` (10) and `max_lines` (300) and has no unresolved conflicts. Otherwise the commits are pushed to a side
  branch (3.3.1), not to the PR head, and the reply says so. The commit carries `skip_marker` (default `[skip claude]`) and the configured bot identity. The outcome is posted as a PR reply.
- Plain issue: create a tracker issue carrying the instruction and a back link, set it to queued, and reply with its key.
- Throttling: synthetic action `webhook-comment` with `max_runs_per_day` (20); session timeout
  `claude_timeout_seconds` (600).

### 3.7 Pause, cancel and readiness

- Global and per-action pause are stored in the database and survive restart. Paused webhook deliveries are acknowledged
  with 2xx and dropped; paused scheduling skips pickup.
- `cancel` stops an in-flight run by id through the daemon.
- Readiness has two not-ready kinds: `no_credential` and `login_required`. While login is required, scheduling stops,
  webhook deliveries are acknowledged and dropped with a `run_events` row, and runs stop picking up jobs (exit code 8).
  The Settings page can run an interactive Claude sign-in through the daemon.

### 3.8 Garbage collection and retention

All sweeps are DB-backed settings, enabled by default flags in each section, and run at serve start and hourly:

- Workdir GC: preserved or aside workdirs older than `retention_days` (14) are deleted. A workdir holding unpushed
  commits violates 3.3.1; it is kept until `unpushed_hard_cap_days` (60) and logged as a defect, never treated as normal.
- Stream-log GC: stream logs older than `retention_days` (30); `daemon.log`, per-action `output.file` and `usage.jsonl`
  are bounded by size; snippets and `webhook-logs` have their own retention days.
- DB retention: rows older than `retention_days` (180) are deleted from `job_outcomes`, `run_events`, `ci_failure_runs`
  and `claude_invocations`.
- Docker GC: named Docker volumes unreferenced by any container and older than `volume_retention_days` (3) are removed.

## 4. Interfaces

### 4.1 CLI

Global flags: `--actions-dir <dir>` (env `CLAUDE_RUN_ACTIONS_DIR`), `--color auto|always|never`.

| Command | Behavior |
|---|---|
| `run <action>` | Run one action once, holding the per-action lock |
| `serve [--detach] [--no-socket]` | Start the daemon |
| `stop` | SIGTERM the daemon; no-op when not running; never force-kills |
| `restart [--no-socket] [--detach]` | Stop then serve |
| `trigger <action>` | Ask the daemon to run an action now |
| `cancel <run_id>` | Cancel an in-flight run (needs a daemon) |
| `pause [action] [--reason]`, `resume [action]` | Global or per-action pause |
| `status` | Print daemon status JSON |
| `action list`, `action show <name>`, `action delete <name>`, `action reset-attempts <name> <key>` | Action management |
| `access check [--repo owner/name] [--host] [--project] [--secret]` | Read-only credential and access probes; non-zero exit when any probe fails |
| `access reset-2fa <username>` | Clear TOTP enrollment and recovery codes, rotate the session key (DB-direct) |
| `secret set <name>`, `secret list`, `secret delete <name>` | Encrypted secrets; `set` reads the value from a hidden prompt or stdin |
| `test-postgres [--image] [--init-sql ...] -- <command>` | Run a command against a throwaway Postgres container |
| `mcp stdio` | Read-only troubleshooting MCP server over stdio |
| `migrate [--from]` | Import legacy JSON state files into SQLite |
| `import [--from] [--allow-nonempty]` | Copy a SQLite store into the configured database |

Exit codes: 0 ok, 1 paused, 2 locked, 3 config error, 4 daily cap, 5 unexpected, 6 over budget, 7 conflict, 8 login
required, 130 cancelled.

CLI mutations delegate to the daemon over the unix socket when it is reachable and otherwise operate on the database.

### 4.2 JSON API

Served at the root of the unix socket (unless `--no-socket`) and under `/api` on the TCP listener.

| Method and path | Purpose |
|---|---|
| `GET /status` | Version, global pause, activity snapshot, readiness, stale dependency gates, attempt-cap hits, actions |
| `GET /actions` | List actions |
| `POST /actions/{name}/trigger` | Queue a trigger (400 for synthetic actions, 404 unknown, 409 when paused or not ready, 503 queue full) |
| `POST /actions/{name}/pause`, `/resume` | Per-action pause |
| `POST /actions/{name}/attempts/{key}/reset` | Clear an attempt counter (404 when none) |
| `POST /actions/{name}/delete` | Delete (403 synthetic, 409 when a run is in flight) |
| `POST /runs/{id}/cancel` | Cancel a run |
| `POST /pause`, `POST /resume` | Global pause |
| `GET`/`PUT /settings/{section}` | Settings sections `mint`, `webhook`, `webhook_comment`, `log_filter`, `workdir_gc`, `stream_log_gc`, `preferences`, `concurrency`, `db_retention`, `docker_gc`, `claude`, `auth` |
| `GET /report/summary`, `/report/daily`, `/report/weekly`, `/report/by-weekday`, `/report/by-hour` | Aggregations; query `range` (`30d`, `90d`, other values mean all time) and `action` |
| `POST /access/check` | Same typed report as `access check` |

### 4.3 Web UI (TCP, default port 7321)

Pages: `/` dashboard; `/actions/new`, `/actions/new/raw`, `/actions/new/youtrack`, `/actions/new/closed-issue-summary`;
`/actions/{name}` with `/durations`, `/events` (redirect), `/edit`, `/form`, `/export`, `/delete`; `/logs`; `/secrets` with
rotate, delete and check; `/access`; `/settings` with per-section saves and the Claude, YouTrack, Forgejo, GitHub and git
auth cards; `/reports`; `/about` and `/about/activity`; `/runs/{id}`; `/runs/{id}/jobs/{key}/attempt/{attempt}`;
`/sse/runs/{id}/jobs/{key}/attempt/{attempt}` (live stream); `/static/htmx.min.js`, `/static/idiomorph-ext.min.js`.
Auth routes: `/login`, `/login/2fa`, `/logout`, `/setup`, `/account/2fa/setup`, `/invite/{token}`, `/settings/accounts` with
invite, remove and revoke.

### 4.4 Webhooks

| Path | Purpose |
|---|---|
| `POST /webhooks/{provider}/triage/{source}` | CI-failure, success and pull-request deliveries |
| `POST /webhooks/{provider}/comment/{source}` | Comment commands |

`{provider}` is `forgejo` or `github`; `{source}` names a configured source. Unknown provider, unknown source, a source
configured for a different provider, or a disabled listener returns 404. Pipeline order and responses:

1. Per-source fixed-window rate limit (`rate_limit_per_min`, default 60): 429.
2. Resolve the source's HMAC secret from the secrets store; missing or unresolvable: 401.
3. Verify the HMAC-SHA256 hex signature over the raw body, constant time. Forgejo: `X-Forgejo-Signature`. GitHub:
   `X-Hub-Signature-256` with a `sha256=` prefix. Mismatch or missing: 401.
4. Require the delivery id header (`X-Forgejo-Delivery` / `X-GitHub-Delivery`): missing 400; seen within
   `replay_window_secs` (default 300, shared across both listeners): 409.
5. Event type header (`X-Forgejo-Event` / `X-GitHub-Event`) selects the handler. Unparseable payload: 400.
6. Accepted failure events: 202. Authenticated events that are not actionable: 200 and dropped.

### 4.5 MCP tools

Seven read-only tools, served over stdio (`mcp stdio`) and, when enabled, Streamable HTTP at `/mcp`: `get_status`,
`list_runs`, `get_run`, `get_attempts`, `get_stream_log`, `list_ci_failures`, `get_ci_failure`. Limits above the maximum are
rejected, not clamped. `get_stream_log` rejects paths that resolve outside the logs directory. The HTTP listener adds the
`mcp-web` crate's account, OAuth 2.1 and bearer layer, serves OAuth discovery under `/.well-known/`, and restricts the
`Host` header to the authority of `public_url` plus `allowed_hosts`.

## 5. Data model

State store: SQLite (default, `<data dir>/state.db`, WAL mode, busy timeout 5 seconds) or PostgreSQL. Migrations live in
`migrations/sqlite` (chain) and `migrations/postgres` (squashed chain) and run at open. A test asserts both sets end on the
same schema. Pool: 16 connections, 30-second acquire timeout. Timestamps are RFC 3339 text.

| Table | Purpose and key columns |
|---|---|
| `actions` | Action index: `name` (PK), `file_path`, `yaml_sha256`, `enabled`, `schedule_kind`, `schedule_spec`, `parse_error`, `max_parallel` |
| `runs` | One row per run: `id`, `action_name`, `started_at`, `ended_at`, `exit_code`, `jobs_processed`, `triggered_by`, `current_job_key`, `stage`, `job_links` |
| `job_outcomes` | Per-attempt result: `run_id` (FK, cascade), `job_key`, `attempt`, `ok`, `claude_ran`, `claude_ok`, `claude_cost_usd`, `attempt_counted`, `summary`, `stream_log_path`, `duration_ms`, `timed_out` |
| `run_inflight_jobs` | In-flight jobs: PK (`run_id`, `job_key`), `concurrency_key` |
| `run_events` | Diagnostics: `run_id`, `action_name`, `ts`, `level`, `context`, `message` |
| `attempt_counters` | PK (`action_name`, `job_key`), `count`, `last_at` |
| `daily_ledger` | PK (`action_name`, `date`), `run_count`, `cost_usd` |
| `paused_actions`, `global_pause` | Pause state (`global_pause` is a single row, `id = 1`) |
| `dependency_gate` | PK (`action_name`, `job_key`), `blocked_until`, `signature`, `blocked_since` |
| `settings` | `key` (PK) to JSON `value` |
| `secrets` | `name` (unique), age `ciphertext`, `created_at`, `last_used_at` |
| `ci_failures`, `ci_failure_tests`, `ci_failure_runs` | Failure store: repo, workflow, job, `infra_kind`, `issue_key`, `state`; unique (`failure_id`, `test_id`); run index, commit, branch, PR number |
| `agent_pull_requests` | Recorded agent PRs: unique (`host`, `repo`, `pr_number`), `issue_key`, `head_branch`, `fix_attempts`, `state`, `salvaged` |
| `claude_invocations` | Per-invocation cost, token and rate-limit utilization record |
| `web_accounts` | `username`, unique `username_key`, argon2 `password_hash`, `session_key`, TOTP secret (encrypted), `totp_enrolled_at`, `totp_last_step`, `created_by` |
| `web_account_recovery_codes` | Hashed one-time recovery codes per account |
| `web_invites` | `token_hash` (unique), `created_by`, `expires_at`, `used_at`, `email` |
| `web_admin`, `web_admin_recovery_codes` | Legacy single-admin tables, copied into `web_accounts` by migration |

Settings keys: `config.mint`, `config.webhook`, `config.webhook_comment`, `config.log_filter`, `config.workdir_gc`,
`config.stream_log_gc`, `config.preferences`, `config.concurrency`, `config.db_retention`, `config.docker_gc`,
`config.claude`, `config.auth`.

On-disk layout: `<config dir>` (`config.toml`, `actions/`, `secrets.key`, MCP key files), `<data dir>` (`state.db`,
`workdirs/<action>/<key>`, `mcp-web.db`), `<state dir>` (`logs/`, `snippets/`, `webhook-logs/`), `<runtime dir>`
(pidfile, `daemon.sock`).

## 6. Configuration

### 6.1 Environment variables

| Variable | Purpose |
|---|---|
| `CLAUDE_RUN_CONFIG_DIR`, `CLAUDE_RUN_DATA_DIR`, `CLAUDE_RUN_STATE_DIR`, `CLAUDE_RUN_RUNTIME_DIR` | Override the four directories (defaults: XDG config, data, state, runtime under `claude-run/`) |
| `CLAUDE_RUN_ACTIONS_DIR` | Actions directory override |
| `CLAUDE_RUN_WORKDIR_BASE` | Workdir base when the action sets none |
| `CLAUDE_RUN_LOG` | Tracing filter; wins over `[daemon].log_filter` |
| `CLAUDE_RUN_DATABASE_URL_FILE`, `CLAUDE_RUN_DATABASE_URL` | Database URL (file form wins, then variable, then `[database] url`) |
| `CLAUDE_RUN_SECRETS_KEY_FILE` | Path of the age key (default `<config dir>/secrets.key`) |
| `CLAUDE_RUN_PUBLIC_URL` | Externally reachable base URL for invite links and run-link footers (absolute http or https) |
| `CLAUDE_RUN_SMTP_HOST`, `_PORT`, `_ENCRYPTION` (`starttls` default, `tls`, `none`), `_USERNAME`, `_PASSWORD_FILE`, `_PASSWORD`, `_FROM` | SMTP for emailed invites; configured only when host, from and public URL are all set; a username without a password is refused |
| `CLAUDE_RUN_TEST_DATABASE_URL` | Test-only: run the suite against PostgreSQL |
| `CLAUDE_RUN_GIT_USERNAME`, `CLAUDE_RUN_GIT_PASSWORD` | Set by the runner on its own git child processes for an inline credential helper; not operator settings |
| `CLAUDE_RUN_GIT_SHA`, `CLAUDE_RUN_BUILD_DATE` | Build-time metadata |

`~`, `$VAR` and `${VAR}` are expanded in path values.

### 6.2 `config.toml` (bootstrap, unknown keys rejected)

| Key | Default | Purpose |
|---|---|---|
| `[web] bind` / `port` | `127.0.0.1` / `7321` | TCP listener |
| `[web] auth_token_ref` | unset | Secret name of the bearer token |
| `[web] require_auth_on_loopback` | `false` | Force auth on a loopback bind |
| `[web] auth_username` | unset | Legacy form-login username (used with the token while no account exists) |
| `[daemon] log_filter` | unset | Tracing filter |
| `[database] url` | unset (SQLite) | `sqlite://`, `postgres://` or `postgresql://`; any other scheme is a startup error |
| `[webhook]`, `[mint]` | see 3.5 | Bootstrap copies of the DB-backed sections |
| `[mcp] enabled` / `bind` / `public_url` / `admin_email` / `allowed_hosts` | `false` / `127.0.0.1:7322` / unset / unset / empty | MCP HTTP listener; `public_url` is required when enabled |

`[webhook.sources.<name>]`: `provider` (`forgejo` default, `github`), `secret_ref`, `allowed_repos` (exact `owner/repo`,
`owner/*` or `*`; empty denies all), `rate_limit_per_min`, `log_token_ref`, `write_token_ref`. Enabling the listener
requires `secret_ref` and `write_token_ref` on every source.

### 6.3 DB-backed settings sections

Edited on the Settings page or through `PUT /api/settings/{section}`; changes apply without restart. Fields not listed in
3.x: `webhook.analysis_timeout_secs` (600), `webhook.analysis_concurrency` (2); `concurrency.max_parallel_global` (0 =
largest enabled action cap); `preferences` (`locale`, `timezone`, `datetime_format`); `claude` (`model`, `effort`,
`autocompact`); `auth` (service base URLs, secret names, git author name and email). `webhook_comment.sources.<name>`
adds `allowed_permissions`, `max_files`, `max_lines`, `bot_author`, `bot_email`, `skip_marker`.

## 7. Security

- TCP surface authentication is required for any non-loopback bind (or `require_auth_on_loopback`). Credentials: bearer
  token (constant-time compare) or a session cookie `claude_run_session` (HttpOnly, Secure, SameSite=Lax, 12-hour TTL,
  HMAC-signed with the account's rotating session key). `/webhooks/`, the login, setup and invite routes are exempt;
  webhooks authenticate by HMAC.
- With no credential and no account, browsers are sent to `/setup` and other clients receive 503; the surface is never
  served unauthenticated.
- Accounts: argon2id password hashes, mandatory TOTP enrollment (an unenrolled session may only reach `/account/2fa/setup`
  and static assets), one-time recovery codes, 5 wrong second-factor attempts per 5-minute pending cookie, invites that
  expire after 72 hours and are stored as hashes. `access reset-2fa` rotates the session key.
- Secrets are stored age-encrypted (X25519 identity) in `secrets`. The key file is created with mode 0600 on first start,
  a missing key with stored secrets refuses to generate a new one, and a key readable beyond its owner is warned about.
- Webhook hardening: per-source HMAC secret, rate limit before crypto, shared replay store, repository allow-list that
  denies by default, and a comment-command permission check plus decision-gate caps.
- Logs fetched from CI are redacted before storage or attachment. Run summaries rendered in the UI are Markdown with raw
  HTML demoted to text.
- The unix-socket API has no authentication and is disabled by `--no-socket` in the container image.
- The agent may mutate only through the bounded tracker writes the prompt permits; the runner owns the `AI Agent` field
  and every write to the triggering issue except comments.

## 8. Integrations and dependencies

| Role | Integration |
|---|---|
| Issue tracker | YouTrack through the `yt` CLI |
| Forge | Forgejo through the `fj` CLI; GitHub through `gh`, git, and the GitHub MCP endpoint |
| Agent | Claude Code CLI `claude` |
| Scripted actions | `nu` (default interpreter) |
| State store | SQLite or PostgreSQL |
| Email | SMTP (STARTTLS, TLS or none) |
| Test databases | Docker (`test-postgres` subcommand) |
| MCP web layer | `mcp-web` crate (accounts, OAuth 2.1) |

Credentials for `yt`, `fj` and `gh` come from the `auth` settings (materialized into the CLIs' credential files) or an
action's `token_ref`, which is injected per job.

## 9. Deployment and operations

- Image (`oci-build/Dockerfile`): multi-stage. The builder compiles a static musl binary with a locked dependency
  build. The runtime stage is a Linux image carrying `claude`, `yt`, `fj`, `gh`, `nu`, `just`, Docker CLI, git and Rust
  tooling, plus `gosu` and `catatonit`. Entrypoint: `catatonit -- gosu-entrypoint.sh claude-run`, default command
  `serve --no-socket`. The image starts as root and drops to uid/gid 1001 through the entrypoint.
- Entrypoint hooks seed `config.toml` and the shipped action files into an empty config volume and chown the data
  directory.
- Image environment: `CLAUDE_RUN_CONFIG_DIR=/config`, `CLAUDE_RUN_DATA_DIR=/data`, `CLAUDE_RUN_STATE_DIR=/state`,
  `CLAUDE_RUN_RUNTIME_DIR=/run/claude-run`, `CLAUDE_CONFIG_DIR=/config/claude-agent-home`. The agent's `CLAUDE.md` is baked
  from `deploy/bot-CLAUDE.md`.
- Seeded container config binds the web UI to `0.0.0.0:7321`. The MCP listener is off; when enabled it binds
  `0.0.0.0:7322`.
- `compose.yml`: the app service with three named volumes (`/config`, `/data`, `/state`), a tmpfs at `/run/claude-run`, a
  privileged Docker-in-Docker sidecar sharing the app's network namespace (daemon on `tcp://127.0.0.1:2375`, healthcheck
  `nc -z`), and an optional PostgreSQL service behind the `postgres` profile (healthcheck `pg_isready`). No host bind
  mount is used. No port is published by default.
- The app service defines no container healthcheck. Liveness is available from `GET /api/status` and the `status`
  command.
- Shipped action files: `youtrack.yml` (interval 5m), `audit-run.yml` (interval 5m), `closed-issue-summary.yml` (interval
  24h). `examples/release-notes.yml` (cron weekly) is an example and is not seeded into a new install.
- Restart safety: `GET /api/status` and the About page expose a `quiet` flag that is true only when no run is open and
  no run is dispatched.

## 10. Non-functional behavior

- Logging: JSON lines through `tracing`. Default filter `info,claude_run=info,sqlx=warn`. Destination is stdout, except
  `mcp stdio` which logs to stderr so stdout carries only protocol frames. `serve --detach` redirects both to
  `<state dir>/logs/daemon.log`. The first line of `serve` is `Server started` with version and pid.
- Diagnostics: warnings and errors that concern a run or delivery are also stored in `run_events` and shown on `/logs`.
- Error handling: invalid action YAML, unknown config keys and unsupported database schemes fail with a message naming the
  value. API errors carry a JSON message and a non-2xx status for conflicts, unknown names and unavailable queues.
- Database connection to PostgreSQL retries for a bounded deadline, then fails naming the host.
- Concurrency: a global permit cap plus per-action caps; a key gate serializes jobs that share a concurrency key.
- Timeouts: per-job `claude` wall clock (default 600 seconds), review session (1800), CI-failure analysis (600).
- Time rendering: stored UTC timestamps are rendered through the `preferences` timezone, locale and format.
