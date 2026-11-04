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
  daemon is globally paused or while the Claude credential is not ready. An action's own schedule (3.1) decides
  whether it is due at all: `interval` fires once that duration has elapsed since its last run's `started_at` (or
  immediately if it has never run), `cron` fires at the next matching instant (UTC) after that same timestamp (or,
  if it has never run, after the scheduler first saw it), and `manual` never auto-fires. A re-gather that finds no
  work creates no run, so a due action keeps re-gathering at the cadence until one starts. The re-gather cadence itself is tracked by the scheduler's own last-fire time in
  memory, per action, not by run rows, since a re-gather that finds only already-in-flight work creates no run row.
  The "no gather in flight" check is advisory and scheduler-only: a manual trigger always gathers regardless, and a
  scheduled pickup that narrowly loses the race still gathers rather than erroring.
- Trigger consumer: runs actions requested by `trigger`.
- HTTP servers: an optional unix-socket API server (disabled by `--no-socket`), a TCP server (web UI, `/api`,
  webhook routes), and an optional MCP listener.
- Webhook consumers: CI-failure events, pull-request events, action-success events (automerge) and comment commands.
- Background loops: automerge reconciler (300-second tick), garbage-collection sweeps (hourly tick), history-table
  retention sweep, Claude login status probe (every 600 seconds when ready, every 60 seconds when login is required).
- On start the daemon reaps run rows left open by a dead process (closed with the `UNEXPECTED` exit code), reconciles
  tracker issues stranded in the working state, scans the actions directory, and records the serve start time. A scan
  also prunes the action index of rows whose backing YAML file no longer exists (run history is kept), and flags two
  enabled actions doing redundant work (a case-insensitive name collision, or the same native kind and effective
  source) with a WARN log and a persisted `run_events` notice on each; detection is observability-only and never
  disables either action.
- On SIGTERM the daemon shuts down gracefully with a 30-second grace window, driven by tripping every in-flight run's
  cancellation token (3.7) at once and waiting for the run registry to drain. Runs found open at shutdown are closed
  with exit code 130 (cancelled).
- `serve --detach` forks into the background before any tokio runtime is built (forking after would hand the child a
  corrupted scheduler), and keeps the daemonize dependency from managing the pidfile itself so it never races the
  daemon's own `flock` (sole authority: the pidfile lock taken above). It also writes a marker file next to the
  pidfile for the run's lifetime (removed on clean shutdown and recreated at the next `serve` start). A CLI command
  with no reachable daemon socket classifies daemon presence from the pidfile's liveness and this marker: a live
  pidfile with the marker present reads as running with the socket disabled, a live pidfile with no marker reads as
  undetermined (starting up, or its socket listener crashed) rather than guessing not-running, and no live pidfile
  reads as not running.

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
  block. A retired kind (`forgejo-ci-fix`, `ci-failure-analysis`, `ci-failure-reconcile`) fails to load with an
  error naming the retired kind, checked before the general YAML parse so it reads as "retired", not as an unknown
  value.
- An unknown `native.youtrack.prompt_preset` value fails to load with an error naming it and the valid presets,
  checked before the general YAML parse so it reads as an invalid preset, not an untagged-enum mismatch naming no
  field.
- `scripted` runs `input.command` (emits a JSON array) -> `filter` -> `pre` -> `prompt` -> `claude` -> `post`; each
  script is a plain string or `{run, shell}`, with `nu` as the default interpreter.
- Schedule: `kind: interval` (`humantime` duration, e.g. `30m`), `kind: cron` (5-field or 6-field expression), or
  `kind: manual` (only `trigger` fires it).
- Per-action `limits`: `max_attempts_per_key` (default 3), `max_runs_per_day` (20), `claude_timeout_seconds` (600),
  `daily_budget_usd` (5.00), `run_budget_usd`, `budget_warn_fraction`, `max_issues_per_run`, `review_timeout_seconds`
  (1800). `concurrency` has `max_parallel` and a `key` list; two jobs sharing a concurrency key never overlap, across
  scheduled runs and manual triggers. `concurrency.max_ci_in_flight_per_repo` (default 3, `0` disables) caps the runner
  PRs on one repo with CI pending (`SPEC-3.3`). The key is the literal joined value of those fields, not prefixed by the
  action name, and is held in one process-wide gate: two jobs from *different* actions that resolve to the same key
  value also never overlap, not only two jobs of the same action.
- `claude` block: `cwd`, `allowed_tools`, `permission_mode`, `model`, `effort`, `autocompact`. Precedence for model,
  effort and autocompact: action value, then the `claude` settings section, then the Claude CLI default (autocompact
  falls back to a built-in default window instead).
- The daemon refuses a daily run beyond `max_runs_per_day` (exit code 4) and a run over budget (exit code 6).
- A per-key attempt counter parks a job once it reaches the cap; `action reset-attempts` clears it.
- Optional `link_template` renders a job key as a clickable link in the web UI, substituting `{key}`; other `{...}`
  tokens pass through unchanged.
- Optional `output.file` (relative to the state dir) appends one JSON record per job outcome, the same shape the DB's
  `job_outcomes` table stores; unset writes nothing. Bounded by size, see 3.8.
- Optional `env` and `secrets` blocks (`scripted` only; a native kind uses `token_ref` instead): each key is a
  shell-safe variable name (`A-Z0-9_`), named in at most one of the two blocks, and a `secrets` value names a
  non-empty secret.

### 3.2 Claude invocation

Each job runs `timeout <seconds>s claude --print --verbose --output-format=stream-json --permission-mode=... --allowed-tools=... [--model] [--effort] --autocompact=... [--max-budget-usd] --disallowedTools=Monitor,ScheduleWakeup
<prompt>` with stdin closed, in its own process group (cancellation signals the group). Environment for the child sets
`CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` and sets `BASH_DEFAULT_TIMEOUT_MS` / `BASH_MAX_TIMEOUT_MS` to the invocation's
timeout. For a repo workdir it also sets `CARGO_TARGET_DIR` to that repo's shared target directory (`SPEC-3.10`).
`CLAUDE_RUN_DATABASE_URL`, `CLAUDE_RUN_DATABASE_URL_FILE`, `CLAUDE_RUN_SMTP_PASSWORD` and
`CLAUDE_RUN_SMTP_PASSWORD_FILE` are removed from the child environment. Stdout events are timestamped into a per-attempt
stream log under `<state dir>/logs/<action>/streams/`. Exit code 124 from `timeout` is recorded as a timed-out outcome.
Each run appends one JSON record (tokens, estimated cost, duration, tool counts) to `<state dir>/logs/usage.jsonl`, and
each invocation is recorded in `claude_invocations`.

#### 3.2.1 Model, effort and autocompact resolution (`SPEC-3.2.1`)

`--model` and `--effort` resolve action value, then the live `claude` settings section, then omission (the `claude`
CLI's own default applies); `--autocompact` resolves the same way but falls back to a fixed window (200000 tokens)
rather than omission, because the CLI's own `auto` scales to the model's whole context window, and on a large-context
model a long tool-heavy session would then never compact, resending every completed tool payload on every turn so
spend grows with the square of the tool-call count rather than linearly in it. A resolved effort level outside the
CLI's documented set (`low`, `medium`, `high`, `xhigh`, `max`) is passed through unchanged after one warning naming the
value and its source: the CLI itself only ignores an unknown level and falls back to its own default, so without the
warning a typo would silently downgrade effort for every run.

#### 3.2.2 Per-invocation spend ceiling (`SPEC-3.2.2`)

The run's remaining budget (`limits.run_budget_usd`, see 3.1, minus what the run has already spent) is passed as
`--max-budget-usd` on every invocation, so a single runaway session cannot overshoot the run's ceiling between the
runner's own between-job checks; an unset remaining budget passes no flag. Each invocation's reported cost is folded
into the run's shared ledger before any other branch can discard the result, including a cancelled invocation, so a
cancelled run still bills what it actually spent. The ledger's ceiling is resolved once, before the run starts, and
is immutable for the run's lifetime: no job can raise it mid-run.

#### 3.2.3 Cancellation and the process group (`SPEC-3.2.3`)

`claude` and the `timeout` wrapper around it run in a process group whose id equals the wrapped child's pid
(`setpgid(0)`), so a cancellation or a timeout signals the whole tree (the wrapper, `claude`, and anything it spawned)
at once rather than leaving a descendant orphaned. Termination is SIGTERM, then (if the group has not exited within a
short grace window) SIGKILL. The wait for the child is a poll loop rather than a blocking `wait`, so an operator
cancel is observed promptly without a dedicated thread per invocation.

#### 3.2.4 Result-exit grace and the background-task backstop (`SPEC-3.2.4`)

Once `claude`'s terminal `result` event appears in the stream log the turn is functionally done; the child gets a
grace window (30 seconds) to exit on its own, after which the runner force-terminates the group and finalizes from
the already-captured result rather than waiting out the job's full timeout. This is the backstop behind the
disallowed-tools restriction below: a session is not expected to still be working once it has reported its result, so
a result followed by a hang is treated as a leftover background task, not as more legitimate work in progress.

A `claude` child always runs with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` and `--disallowedTools=Monitor,ScheduleWakeup`,
because a session run under `--print` is over the instant its turn ends: a turn that ends waiting on a background
`Bash` task, a `Monitor`, or a `ScheduleWakeup` never resumes. As a further backstop, the stream log is scanned for a
`system`/`task_updated` event with `patch.status: killed` that appears AFTER the terminal `result` event; a match
means a background task outlived the session's own report of success, and the runner overrides that into a
non-transient failure naming the task id rather than reporting it as a clean success.

#### 3.2.5 Failure classification (`SPEC-3.2.5`)

An invocation's outcome is one of: a normal result (`ok` from the terminal event's `is_error`), a non-transient
failure (a nonzero exit with no authentication signal, or a terminal event present but marked an error), a transient
failure (the terminal event's `api_error_status` or `error` names a rate limit; never a timeout), a timed-out run
(`timeout`'s exit code 124), a killed run (exit code 137, SIGKILL, with no terminal result captured; typically an
out-of-memory kill), or a login-required run.

A run is login-required when the CLI reports a structured `authentication_failed`: either on a single event anywhere
in the stream (checked against the nonzero-exit path), or via three or more `system`/`api_retry` events carrying that
same structured error observed during the wait loop, which force-terminates the child well below the job's timeout
rather than waiting it out. Detection keys only on the structured `error` field, never on human-readable text the CLI
may reword between releases. A login-required outcome is always transient (no attempt spent) and trips the daemon's
readiness gate (3.7) so no further Claude work starts until a fresh sign-in.

#### 3.2.6 Stream-log relay and read failures (`SPEC-3.2.6`)

The stream log is the only record of an invocation's events, written by a relay thread that copies the child's piped
stdout line by line, stamping each parseable JSON event with an arrival-time `ts`; a line that fails to parse passes
through unstamped rather than being dropped. A relay I/O failure (for example a full log partition) is carried out as
an explicit error rather than discarded: if the terminal result was already captured before the relay died, the run
still finalizes from it, with the truncation logged; if not, the run is reported as a transient failure naming the
write error and the log path, so it spends no attempt rather than being blamed for an agent silence the runner itself
produced. Reading the stream log back (to find the terminal result or the last assistant text) carries its own
read failure out the same way: an unreadable log finalizes as a transient failure, while a log that reads fine but
simply has no terminal `result` event is a real, non-transient claude-side outcome.

#### 3.2.7 Usage, cost and rate-limit accounting (`SPEC-3.2.7`)

Each invocation's stream log is replayed once to attribute tokens, tool-call counts and context growth per tool (a
turn is attributed to the tool whose result most recently entered its context), and to capture the last-seen Claude
plan rate-limit reading (`five_hour` / `seven_day` utilization) plus a count of `api_retry` events per reason; every
rate-limit field stays absent rather than defaulting to zero when the CLI reported none. One `claude usage` log line
is emitted per invocation, at `warn` instead of `info` when the plan status is not `allowed` or either window's
utilization is at least 90%, so an operator sees a tightening limit before the next invocation is throttled outright.
Each invocation is also persisted to `claude_invocations` (3.2's closing sentence) immediately after it returns,
independent of the outcome, since the invocation already succeeded or failed on its own terms; a persistence failure
is logged and never fails the job. Context growth that stays flat as the number of tool calls grows is linear
accounting (3.2.1's fixed autocompact window doing its job); growth that rises with the tool-call count is quadratic.

#### 3.2.8 Job pipeline orchestration (`SPEC-3.2.8`)

A scripted job's per-job pipeline is: `filter` -> workdir creation -> `pre` steps -> `prompt` -> `claude` -> `post`
steps -> outcome record -> guarded workdir cleanup (3.2.9). `post` runs on every path that reached the workdir,
including a `filter`/`pre`/`prompt` failure or an operator cancel, with a synthesized `$claude_result` describing
why `claude` did not run, so a `post` script always sees a uniform shape. A job's attempt is not counted, and its
daily run/cost ledger does not advance, on three paths: a transient `claude` result (3.2.5), an operator cancel taken
before or during the invocation, and a global pause (or a login-required stop) taken after the job started but
before `claude` ran; a `post` script can additionally suppress attempt counting itself by writing
`$workdir/.claude-run-skip-attempt` (a parked or blocked job). A job's key is its `dedup_key` field value, or a
positional fallback when unset or absent; two jobs sharing a concurrency key (3.1) never overlap, computed by joining
the configured key fields with a unit separator, falling back to the job's own key when none of those fields are
present on it.

#### 3.2.9 Workdir resolution and the local-only-work guard (`SPEC-3.2.9`)

A job's workdir base resolves in order: the action's own `workdir_base` field, then `CLAUDE_RUN_WORKDIR_BASE`, then
`<data dir>/workdirs`; the job's workdir is `<base>/<action name>/<job key>`. Every runner deletes a job's workdir
through one guarded path that first checks whether it holds local-only work: a local branch tip the remote does not
have under that same branch name (excluding a tip that merely equals the remote default branch's tip, which has no
commits of its own), a tracked or staged modification, an untracked and non-ignored file, or a stash. Any local-only
work, or a git or network failure while checking, preserves the workdir (after pruning gitignored build artifacts)
rather than risking a wrong delete; only a clean check with nothing local-only found deletes it. A caller with a forge
token in scope authenticates the one network call this check makes (`ls-remote origin`) per invocation via the
token's own userinfo, since the clone's `origin` remote carries no embedded credential.

#### 3.2.10 Snippet execution (`SPEC-3.2.10`)

Each `filter` / `pre` / `prompt` / `post` step, and a native kind's own snippets, run as a separate interpreter
process rather than in-process. For the `nu` interpreter (the default), bindings (`$job`, `$workdir`,
`$claude_result`, ...) are each written to a sibling JSON tempfile and loaded by a generated `let`-binding prelude,
avoiding any quoting issue from embedding a value directly in the script source; for any other declared interpreter,
each binding is instead exposed as an environment variable holding its compact-JSON value. An action's `env` and
`secrets` blocks (3.1) are set on the snippet's own process only, never on the `claude` child, and every secret value
is replaced with a fixed placeholder in both the snippet's stdout and stderr before either is logged, returned, or
folded into a later prompt, so a script that prints a secret cannot leak it.

### 3.3 YouTrack action (`kind: youtrack`)

- Gather: query the tracker with `native.youtrack.query` (default `-Resolved AI Agent: Queued`, with the placeholder
  `{{audit_project}}` replaced by `audit_project`, default `AUDIT`), resolve each issue's repository from its project's
  enabled VCS integration, drop issues with no integration, and drop issues whose repository the forge token cannot
  read (they stay queued).
- Eligibility, checked at gather time and again after winning the concurrency gate: the custom field (default `AI Agent`)
  must still equal the queued value, and the dependency gate must pass. The dependency gate blocks on open issues linked
  by the configured verbs (defaults: `depends on`, `parent for`), leaves the issue queued, comments the blockers once,
  and re-evaluates after `recheck_timeout`.
- CI in-flight cap (CLAUDE-535): after the dependency gate, a pickup is deferred while
  `concurrency.max_ci_in_flight_per_repo` of the repo's other open agent PRs have CI `pending`: the issue stays queued,
  no attempt is consumed and nothing is cloned. A PR whose status lookup fails is logged at `warn` and not counted,
  and a store or forge read failure lets the pickup proceed. `0` disables the cap.
- Pre: set the field to working and `State` to the in-progress value, optionally set `Assignee`, then
  `git clone --depth 50` and, unless `native.youtrack.install_hooks` is `false` (default `true`), install the
  repo's own git hooks (`just install-hooks`, when the clone has that recipe) so a commit or push that fails
  the repo's local checks is blocked locally, not only caught later by CI (A8N-69). Hook install is
  best-effort: a repo with no `install-hooks` recipe, or a host without `just`, is left unchanged and pickup
  continues. `install_hooks: false` (CLAUDE-537) skips the install entirely and logs the skip at `info`, naming
  the action and the issue key; this does not affect the `SPEC-3.3.2` verification gate, which still runs the
  repo's checks independently of any installed hook.
- Claude works in the local clone; the runner pushes the result (see 3.3.1). The agent may create issues in
  `write_projects` (default: the triggering issue's project), linked `relates to` / `depends on` / `is required for`,
  never as a subtask.
- Finalize runs on every path and is the only place external mutations happen. Every path first applies 3.3.1, then:
  - marker file `.claude-run-needs-review`: commits pushed, field set to needs-review, no PR, `Needs-Info` tag added (name
    configurable under `tags.needs_info`), attempt not counted.
  - marker file `.claude-run-no-code-change`: field set to `states.no_code` (default: the done label), marker text posted
    as a comment, no PR. Any commits the attempt made are pushed.
  - success with commits: a draft PR is created (branch strategy `branch` by default, `agit` opt-in), then the
    verification gate (`SPEC-3.3.2`) runs; a gate failure sets the field to failed and leaves the PR draft instead of
    reaching the review session below. Otherwise a fresh read-only review session runs, and a pass marker
    `.claude-run-review-pass` promotes the PR to ready and sets the field to code-review. A fail marker
    `.claude-run-review-fail` or no verdict keeps the PR draft, posts the findings on the PR and the issue, and sets
    the field to queued (below the attempt cap) or needs-review.
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
- Every runner push of an agent's feature branch (checkpoint, branch-strategy create-PR push, and the final push above)
  always uses `--no-verify`: the repository's own `pre-push` hook never runs on a runner push. The SPEC-3.3.2
  verification gate is the sole post-agent full-suite check (CLAUDE-533).
- A fresh clone carries the remote's tags (CLAUDE-484), so a repository check that reads tags gives the same verdict in a
  run as on a developer machine.
- The runner never sets `State` to done. A tracker-side workflow resolves the issue when its PR merges
  (`docs/yt-workflow-resolve-on-pr-merge.js`). The legacy `states.state_done` key is deprecated and ignored (a
  load-time warning names it); loading still succeeds so an existing action file keeps working.
- Optional `automerge` (default off): merge and set the field to done once checks pass. Finalize never polls for this;
  a reconciler every 300 seconds and action-success webhooks drive the decision for recorded agent PRs. The legacy
  `wait_seconds` / `check_interval_seconds` keys are deprecated and ignored (a load-time warning names them); loading
  still succeeds so an existing action file keeps working.
- Automerge separates a behind-base PR (its branch is simply behind its base, `behind_base: true`) from a real
  conflict: a green, behind-base PR is updated through the forge's update-branch endpoint, which retriggers CI, and a
  later success event re-evaluates and merges once that run goes green; a PR whose checks are green but the forge
  still reports it not mergeable for another reason (an actual conflict, still-checking, or WIP) is left at Code
  Review for a human, as is a merge call that fails after the mergeable check passed. When the behind-base update
  itself fails because of a merge conflict, the runner dispatches one `/claude` conflict-resolution run instead of
  leaving it to a human on the first attempt per PR head commit; a second failure for the same head commit falls
  back to leaving it at Code Review. When the forge reports no `behind_base` at all, automerge takes no merge action:
  on Forgejo this means the runtime's `fj` predates FJ-99 and a human must act; on GitHub, `mergeStateStatus` is
  still `UNKNOWN` (computed lazily after a push) and the reconciler re-checks on its next pass. Each of these
  outcomes posts one comment naming the reason. The behind-base update runs only when the repo is below the
  `concurrency.max_ci_in_flight_per_repo` cap and no other PR's branch update on the repo is still in CI; otherwise it
  is deferred with no comment, and the next success event or reconciler pass asks again.
- Targets: `forgejo` (`base_url`, `token_ref`, `pr_strategy`) and `github` (`hosts`, `mcp_endpoint`, `token_ref`). A job
  whose repository host is `github.com` or listed in `github.hosts` uses the GitHub backend; any other host uses `fj`.
- Prompt: `prompt` overrides; otherwise `prompt_preset` is `default` or `audit`.

#### 3.3.2 Verification gate (`SPEC-3.3.2`)

- Before the runner records an attempt as successful, it re-runs the repository's own pre-push entrypoint in the
  clone itself, rather than trusting the agent's own report that checks passed: `just pre-push` when the clone has
  that recipe, else `just check`, else no command at all. This runs after the final push (`SPEC-3.3.1`), so the
  commits this run produced are already on the branch before the gate's own verdict is known.
- A non-zero exit from the chosen command is recorded as a failure, with its redacted, bounded output attached to the
  run and posted on the PR and the issue, never reported as success. The PR the push already opened stays a draft.
- A report that the checks passed must name the command that actually ran (`just pre-push`, `just check`, or that
  neither recipe exists and no command ran); a report naming no command is not evidence the checks passed.
- A file change a failing check requires is allowed in the diff and is justified in the PR body; the gate checks the
  state of the clone after the agent's own commits, not whether the agent's diff was minimal.
- The clone's own commit hooks (`SPEC-3.3`'s `just install-hooks` step) already run locally on every commit; the
  `pre-push` hook never runs on a runner push (`SPEC-3.3.1`). This gate is independent of both and runs even when the
  clone has no hooks installed, or the host has no `just install-hooks` recipe to run.

### 3.4 Closed-issue summary action (`kind: closed-issue-summary`)

Starts no Claude session and clones nothing. For each nominated parent issue (`parents`, plus issues matching `query`), it
lists issues resolved in the last `window_days` across `projects`, renders counts and a per-project listing, files the
report as a new issue in `report_project` (default: the parent's project), links it with `link_type`, and posts a hidden
marker comment (`Report-Window:`) on the parent so one report is filed per window.

### 3.5 CI-failure webhook

- Accepts Action-run deliveries (Forgejo `action_run_*`, GitHub `workflow_run`). Only a failure for an allow-listed
  repository is processed. A `success` or `recover` action goes to the automerge pipeline. A `pull_request` delivery
  with action `closed` reconciles recorded agent PRs; other PR actions are acknowledged and dropped.
- Processing: fetch the job logs (`fj actions logs`), redact secrets, fingerprint the failure (3.5.2), match or store it
  in `ci_failures`, then triage into a bucket (3.5.1) and act:
  - `code_fix`: create a tracker issue; set the queued value only when `webhook.auto_fix` is true.
  - `auto_fix`: set only by deterministic detectors (`cargo-fmt` signature; failing workflows named in
    `mint.auto_fix.checks_workflows`, default `check.yml`) when the failing branch has no open PR; skipped when
    `mint.auto_fix.enabled` is false.
  - `human_action`: create an issue assigned to `mint.human_assignee` when set, no queue.
  - `transient`: no issue. An infrastructure failure kind is stored on the failure row.
- Branches matching `mint.bot_branch_prefixes` (default `renovate/`) never route to `code_fix` or `auto_fix`.
- `mint.bot_logins` is not a mint-time loop guard: loop safety is the open-PR routing below, decided from the
  `agent_pull_requests` store (3.5.2) before a failure on the agent's own PR ever reaches the mint. `bot_logins` is
  read only by the comment-command webhook (3.6). Open-PR routing: a failure on the agent's own open PR requeues its
  issue until `webhook.agent_pr_fix_attempts` (default 3) is reached, then the issue is set to needs-review; a
  failure on a human's PR is commented on that PR; a resolved match reopens as a regression, comments the run and
  its failing tests, and is re-triaged so `AI Agent` is set exactly as a freshly minted issue of that bucket would.
- Issues are created in the project whose VCS integration claims the repository, with `mint.repo_project_map` consulted
  first and `mint.fallback_project` (default `CLAUDE`) last. State on creation is `mint.target_state` (default `To do`).
  The redacted log is attached.
- Throttling uses the synthetic action `webhook-triage`: `webhook.max_runs_per_day` (default 20) and a per-key attempt
  counter.
- The whole mint pipeline degrades to log-only (no tracker issue created) when no YouTrack/Forgejo/GitHub backend is
  usable, or its on-disk analysis directory cannot be created.

#### 3.5.1 Triage classification (`SPEC-3.5.1`)

Hybrid: a cheap deterministic pass runs first and only the residue it cannot label reaches the LLM.

- Deterministic rules, in order: a log that could not be fetched at all routes to `human_action` (never silently
  dropped as transient), naming the reason; a job the runner skipped (`Skipping job '...' due to '...'`, its
  workflow `if` guard evaluated false) routes to `transient`, since nothing ran; a `runner-unavailable` infrastructure
  condition (below) routes to `human_action`; a curated set of transient/infra log substrings (no runner available,
  registry 401/429/manifest-unknown, connection reset/refused, DNS/TLS timeouts, 502/503/504) routes to `transient`
  with no LLM call.
- A branch matching `mint.bot_branch_prefixes` always routes to `human_action` before auto-fix or the classifier run:
  the owning bot force-pushes/rebases that branch on its own schedule, so a fix pushed there would be wiped.
- Deterministic auto-fix detectors (cargo-fmt signature; a failing workflow named in `mint.auto_fix.checks_workflows`)
  run next, only when the failing branch has no open PR, and only when `mint.auto_fix.enabled`; a match routes to
  `auto_fix` with no LLM call.
- The residue goes to one tool-using `claude` session (`Read,Grep,Bash`) over the complete redacted log plus the
  failing workflow YAML, ending with a fenced JSON verdict: `code_defect` maps to `code_fix`, `needs_human` to
  `human_action`, and `flaky` / `infra` both to `transient`. An unparseable or missing verdict, or a session that does
  not complete, degrades to `human_action` and is logged; it must never become `code_fix` on its own. The session's
  Markdown report becomes the minted issue's analysis text, capped at 24 KiB.
- Infrastructure-failure detection (a full disk, an out-of-memory kill, or an unreachable package/container registry)
  is deterministic phrase matching over the complete log, most-specific-first, case-insensitive, with cargo's own
  test-result and `failures:` lines stripped first so a test NAMED after a detector phrase can never match. The
  matched `kind` id (not the matched line or its wording) is what downstream dedupe hashes, so two differently-worded
  reports of the same host condition collapse identically; an undetected condition is the deliberately preferred
  failure mode over a false positive that would merge two unrelated code failures across branches.

#### 3.5.2 CI-failure store and dedupe (`SPEC-3.5.2`)

- `ci_failures` (one row per distinct failure, keyed on `(repo, workflow_id, job_name)`), `ci_failure_tests` (its
  failing-test fingerprint), `ci_failure_runs` (one row per sighting: commit, branch, PR number, timestamp), and
  `agent_pull_requests` (every PR the runner's own `youtrack` action opens, carrying `fix_attempts` and a `salvaged`
  flag) replace a YouTrack full-text search for dedupe: an unindexed issue is invisible to that search, and a
  failure every PR inherits from `main` would otherwise mint one issue per PR.
- The fingerprint is the sorted, deduplicated `<target>::<test>` ids parsed from cargo test output (a target that
  failed to build or run at all, with no parsed test, contributes a bare `<target>` id), or `infra:<kind>` when the
  infrastructure detector (3.5.1) matches, or a single normalized first-diagnostic-line signature when neither
  parses anything.
- A failure matches an existing `ci_failures` row by failing-test id overlap within the same `(repo, workflow_id,
  job_name)`, preferring `open` over `resolved` over `pr_only` and the newest on a tie.
- A hidden `Dedupe-Key:` marker in the issue body (searched only by the legacy-adoption lookup, for an issue minted
  before this store existed; nothing else searches YouTrack for it) carries three hashed markers: a primary key
  `(repo, run index)` for webhook-redelivery idempotency, a secondary key `(repo, commit, workflow)` kept for
  back-compat with issues minted before the root-cause key existed, and the cross-run collapse key, which is either
  the root-cause key `(repo, branch, workflow, job, failure signature)` (falling back to `(repo, branch, workflow,
  job)` when no signature is derivable) or, when an infrastructure condition was detected, the branch-agnostic
  `(repo, workflow, job, infra kind)` key in its place: a host condition is identical on every branch and is not
  fixable on any of them, so it must collapse onto one issue across every PR branch it hits, while two unrelated code
  failures on two branches must never merge.
- `agent_pull_requests` tracks every PR the runner's own `youtrack` action opens so a re-queued issue continues that
  same branch (3.3.1) instead of opening a second PR, and so the open-PR loop-safety routing (3.5) can bump and read
  back a fix-attempt counter keyed on the PR rather than on a login.

### 3.6 Comment-command webhook

- A `created` issue or PR comment from a non-bot author whose first line starts with `comment.trigger` (default `/claude`)
  and whose body is at most `max_instruction_chars` (default 4000) characters.
- The commenter's repository permission is read with the write-scoped token and must match `allowed_permissions`
  (default `write`, `admin`; a Forgejo level also admits every higher level).
- Pull request: clone the head branch, run Claude with the instruction, commit, and push only when the diff is within
  `max_files` (10) and `max_lines` (300) and has no unresolved conflicts. Otherwise the commits are pushed to a side
  branch (3.3.1), not to the PR head, and the reply says so. The commit carries `skip_marker` (default `[skip claude]`) and the configured bot identity. The outcome is posted as a PR reply.
- Before the fix request runs, the head branch's most recent failing check run is resolved and its failing jobs' tailed
  logs are written to a scratch file as delimited, clearly-marked-as-data diagnostic context (never an instruction) so
  the fix addresses the actual CI failure; a PR with no resolvable failing run proceeds with no CI context rather than
  failing the request.
- Plain issue: create a tracker issue carrying the instruction and a back link, set it to queued, and reply with its key.
- Throttling: synthetic action `webhook-comment` with `max_runs_per_day` (20); session timeout
  `claude_timeout_seconds` (600).

### 3.7 Pause, cancel and readiness

- Global and per-action pause are stored in the database and survive restart. Paused webhook deliveries are acknowledged
  with 2xx and dropped; paused scheduling skips pickup. A run already dispatched re-checks the global-pause flag at
  each per-job checkpoint (TTL-cached, ~1.5s), separately from the up-front scheduler check, so a pause taken mid-run
  stops it from picking up further new jobs without waiting for the whole run to drain; the job already executing
  finishes normally. A read failure of that flag is treated as not-paused and logged, so a transient DB error never
  silently halts the daemon. When a run is stopped mid-flight, its exit code names which axis stopped it: `exit::PAUSED`
  (1) when a global pause tripped it, `exit::LOGIN_REQUIRED` (8) when only the login-required axis did; an operator
  cancel (130) wins over both, and a global pause wins over a login-required trip when both hold during the same run.
- `cancel` stops an in-flight run by id through the daemon: it trips a per-run cancellation token held in a
  process-wide registry (no cross-process cancellation; only a run inside the same daemon process can be cancelled).
  Tripping it, in order: the in-flight `claude` child is sent SIGTERM then, after a grace period, SIGKILL to its whole
  process group (3.2); the job still in flight runs its `post` phase with a synthesized cancelled result; the run loop
  stops dispatching further jobs, and a job still waiting for a concurrency permit stops without waiting for one, and
  finalizes with exit code 130.
- Readiness has two not-ready kinds: `no_credential` and `login_required`. A credential counts, in this order: an
  agent-config-dir `.credentials.json` file (the host login, or one a container `/setup` sign-in wrote), a stored
  Anthropic API key (Settings -> Auth), or `ANTHROPIC_API_KEY` in the daemon's own environment; none of the three
  present is the `no_credential` kind. The login-required axis, once tripped, overrides a credential that is
  otherwise still present on disk, since an expired or revoked login does not delete `.credentials.json`. While login
  is required, scheduling stops, webhook deliveries are acknowledged and dropped with a `run_events` row, and runs
  stop picking up jobs (exit code 8). The Settings page can run an interactive Claude sign-in through the daemon.

### 3.8 Garbage collection and retention

All sweeps are DB-backed settings, enabled by default flags in each section, and run at serve start and hourly.

#### 3.8.1 Workdir GC (`SPEC-3.8.1`)

- Preserved or aside workdirs under `workdirs/<source>/<key>` older than `retention_days` (14), measured from the
  directory's last-modified time, are deleted. A workdir whose mtime is unreadable or newer than "now" is treated as
  age zero (kept).
- A workdir holding unpushed commits, a dirty tracked/untracked tree, or a stash violates 3.3.1; it is kept past
  `retention_days` until the longer `unpushed_hard_cap_days` (60), and logged as a defect, never treated as normal.
  Past the hard cap it is deleted even with local-only work still inside it.
- A filesystem error deleting one directory is logged and that directory is skipped; the pass continues sweeping the
  rest rather than aborting.
- A `target/` directory inside any workdir older than one day is removed even when the workdir itself is kept
  (`SPEC-3.10` item 5).
- A per-repo shared Cargo target directory over `cargo_cache.max_bytes_per_repo` is deleted once no run for that repo
  is in flight (`SPEC-3.10` item 6).

#### 3.8.2 Stream-log, snippet and webhook-log age sweeps (`SPEC-3.8.2`)

- Per-attempt stream logs under `logs/<action>/streams/*.jsonl` older than `retention_days` (30) are deleted. A log
  whose `(action, job_key)` pair is still in flight (joined from `run_inflight_jobs` to runs that have not ended) is
  kept regardless of age, because age alone is not evidence a run has finished. A file not shaped
  `<key>-attempt-<n>.jsonl` is not a stream log and is swept on age alone without the in-flight check.
- `snippets/<action>/<job_key>/` directories older than `snippet_retention_days` are deleted the same way, reusing the
  exact in-flight set the stream-log sweep reads; a failed read of that set skips both sweeps, since sweeping with an
  empty set would delete a live run's own stream log or snippet directory.
- Files directly under `webhook-logs/` older than `webhook_log_retention_days` are deleted by age alone, with no
  in-flight guard: a webhook run is not a scheduled job and never appears in `run_inflight_jobs`. Only files directly
  under the directory are considered; a subdirectory (the mint classifier's own per-event working directory) is never
  descended into or deleted by this sweep.

#### 3.8.3 Size-bounded logs (`SPEC-3.8.3`)

- `logs/daemon.log`, each `logs/<action>/<output.file>`, and `logs/usage.jsonl` are append-forever files with no
  per-unit-of-work age to expire, so each is bounded by SIZE instead: past its configured ceiling (`daemon_log_max_bytes`,
  `output_file_max_bytes`, `usage_log_max_bytes`; `0` disables a ceiling) the file is rolled to one `<name>.1`
  predecessor holding the full prior content, then truncated in place back to its newest half, cut at the first line
  boundary inside that half so every retained line is whole.
- The truncation is in place, never a rename: `daemon.log` is held open for append, and a rename would leave new
  writes landing in the predecessor while the live file stayed empty.
- A predecessor (`<name>.1`) is itself never rolled over into a second generation.
- A missing file is silent and uncounted (the daemon may not have run yet, or an action may have logged no output);
  any other error bounding a file is logged and that file is left as-is, and the pass continues to the rest.

#### 3.8.4 DB retention (`SPEC-3.8.4`)

- Rows older than `retention_days` (180), by their stored `ts`, are deleted from `job_outcomes`, `run_events`,
  `ci_failure_runs` and `claude_invocations`. Each pruned table has a single-column index on `ts` so the cutoff
  predicate seeks rather than scans.
- `ci_failures`, `ci_failure_tests` and `agent_pull_requests` are never pruned by this sweep: a resolved failure or a
  merged PR must still be matchable against a regression months later, so only the per-sighting/per-invocation
  history tables above are bounded by age.
- The default `retention_days` stays `>=` the longest reporting range the web UI exposes (90 days), so a bounded
  report is never silently truncated by the prune running ahead of it. A save that enables the sweep with
  `retention_days` below that same 90-day floor is rejected outright, not just defaulted away from, so the floor
  holds for every saved value, not only the shipped default.

#### 3.8.5 Docker volume GC (`SPEC-3.8.5`)

- A named volume in the sidecar's Docker daemon that no container currently references and that is older than
  `volume_retention_days` (3), measured from Docker's own `CreatedAt` (Docker records no last-used time), is removed.
  A `buildx_buildkit_*` state volume is kept at any age; recreating buildkit's own cache costs far more than the disk
  it uses.
- When the Docker daemon cannot be reached (including `DOCKER_HOST` unset), the pass is skipped and logged, and
  nothing is removed; this is never reported as a clean sweep.
- A remove Docker refuses because the volume is now in use (a race between listing and removing it) is logged and the
  volume is left in place; any other remove failure is logged and the pass continues to the rest. Neither `--force`
  nor `--all` is ever passed: Docker's own in-use refusal is the safety net this sweep relies on, and `--all` has no
  age filter.
- A save setting `volume_retention_days` below 1 is rejected: Docker records no last-used time, so a 0-day window
  would remove a volume a session created moments ago while its container sat between restarts.
- The sidecar's total Docker storage is also bounded by `docker_gc.max_bytes` (`SPEC-3.11`).

### 3.9 Run comment footer (`SPEC-3.9`)

Every runner-posted comment (job-lifecycle comments on 3.3/3.3.1's paths, the closed-issue summary's marker comment,
the dependency-gate blocker comment, the attempt-cap comment, webhook replies) appends a footer rather than relying on
callers to build one themselves:

- A `Run:` line linking back to the run: `<public_url>/runs/<id>` for a run-level comment, or
  `<public_url>/runs/<id>/jobs/<key>/attempt/<n>` for a comment about one job's attempt. `public_url` is resolved
  independently of SMTP configuration, so the footer still links out when SMTP is unset or broken; when it is unset
  the line instead names `CLAUDE_RUN_PUBLIC_URL` as missing, with the run id and attempt still stated in words. When
  no run id is available at all (a daemon-restart reconcile, or the run row itself failed to open) the line says so
  explicitly rather than being omitted.
- `Started:`, `Finished:` (RFC 3339, second precision) and `Duration:` (`<minutes>m <seconds>s`, or `<seconds>s` under
  a minute) lines, computed from the attempt's or run's own start time and "now" as the finish time.

### 3.10 Shared Cargo target directory (`SPEC-3.10`)

A run clones its repo into a fresh workdir, so a Rust repo starts with an empty `target/` and rebuilds every
dependency. Runs of the same repo share one compiled-dependency cache instead.

1. **Purpose.** Cargo keys each dependency artifact by package, version, features, profile and compiler version, so two
   clones using the same dependency versions reuse the same compiled output in one target directory.
2. **Location.** For each repo the runner owns `<data dir>/cache/cargo-target/<repo-slug>`. `<repo-slug>` is
   `<owner>__<repo>` taken from the clone URL path, lowercased, with every character outside `[a-z0-9._-]` replaced by
   `_`. Two repos never share a directory. The directory is outside `workdirs/`, so workdir GC (3.8) never reaches it.
3. **Injection.** Every `claude` child spawned for a repo workdir, and every runner-spawned build subprocess in that
   workdir, gets `CARGO_TARGET_DIR` set to that repo's directory. A repo that is not Rust is unaffected. If the
   directory cannot be created or written, the attempt fails with the path and the OS error; the runner never falls
   back to a per-workdir `target/`.
4. **Incremental builds.** Incremental compilation stays enabled; the runner does not set `CARGO_INCREMENTAL`.
5. **Workdir `target/`.** A workdir's `target/` is removed when the workdir is preserved or set aside, and by the
   workdir sweep (3.8) from any workdir older than one day, including one kept for local-only work. This covers a
   `target/` left by a build that ignored the variable (for example a project that sets its own `build.target-dir`).
   Each removal is logged at `info` with the key, path and bytes reclaimed.
6. **Bound.** Each repo directory has a size ceiling, `cargo_cache.max_bytes_per_repo` (default 60 GB; assume this
   value and revise if measurement shows otherwise). A directory over its ceiling is deleted once no run for that repo
   is in flight, on the existing GC tick, with a `warn` naming the repo, size and ceiling. The next run for that repo
   rebuilds cold.
7. **Concurrency.** Cargo serializes builds on one target directory, so simultaneous runs of one repo take turns
   compiling; waiting is not a failure. Runs of different repos do not wait on each other.
8. **Scope.** Docker builds the agent starts through `just check-docker` run in the Docker-in-Docker sidecar with their
   own build cache (9). This requirement does not change them.
9. **Visibility.** A run's start log line names the `CARGO_TARGET_DIR` it was given. The Status page (4.3) lists each
   repo directory with its size and last-used time.
10. **Management.** The Status page has a Management part listing each directory with its size, last-used time and
    whether a run for that repo is in flight. Each row has a Delete button, and a Delete all button covers every row;
    both ask for confirmation naming the directory and its size. A delete is a `POST` (`/status/cargo-cache/{repo-slug}/delete`
    and `/status/cargo-cache/delete`, mirrored under `/api` as the UI's own buttons use it, 4.2). While a run for the repo
    is in flight the delete is refused with `409` and the page shows the refusal where the person clicked. Each delete
    is logged at `info` with the repo, path and bytes reclaimed; an I/O failure is logged at `error` and shown on the
    page. The controls need the same authentication as the other Status page controls and add no route to the unix
    socket.

### 3.11 Sidecar Docker storage (`SPEC-3.11`)

The Docker-in-Docker sidecar keeps its images, containers, build cache and named volumes in the `/var/lib/docker`
volume (9). Only unreferenced named volumes were bounded before this requirement.

1. **Measure.** The runner reads the sidecar's usage by category (images, containers, local volumes, build cache)
   through the Docker CLI against the sidecar daemon (`tcp://127.0.0.1:2375`). A failed read is logged at `error` and
   shown as "unknown", never as zero.
2. **Ceiling.** The setting `docker_gc.max_bytes` (default 250 GB; assume this value and revise if measurement shows
   otherwise) bounds the total. When it is exceeded on the Docker GC tick and no run is in flight, the runner reclaims
   in this order and stops once under the ceiling: stopped containers older than 1 day, dangling images, build cache
   older than 7 days, unused images older than 7 days, then unreferenced named volumes older than
   `volume_retention_days` (3.8). A pass with a run in flight is skipped and logged.
3. **Logging.** Each step logs the category, bytes reclaimed, and the size before and after. A failed step logs at
   `error` and the pass continues with the next step; a failed pass is never reported as clean.
4. **Management.** The Status page's Management part (3.10 item 10) has one row per category with its size and a Prune
   button. A prune is a `POST` (`/status/docker/{category}/prune`, mirrored under `/api` as the UI's own buttons use it,
   4.2), refused with `409` while any run is in flight because any run may be using the sidecar, and shown where the
   person clicked. It logs bytes reclaimed at `info`; a failure is logged at `error` and shown. It needs the same
   authentication as the other Status controls and adds no route to the unix socket.
5. **Scope.** The runner never removes an image, container or volume that a running container references, never uses a
   prune command that needs a force flag to skip its confirmation (removal is by explicit id from a listing), and never
   touches the app's own `/data`, `/config` or `/state` volumes.

## 4. Interfaces

### 4.1 CLI

Global flags: `--actions-dir <dir>` (env `CLAUDE_RUN_ACTIONS_DIR`), `--color auto|always|never`.

#### 4.1.1 JSON syntax highlighting (`SPEC-4.1.1`)

JSON printed to a terminal (`--color auto`, a TTY) or forced on is syntax-highlighted token by token (keys, strings,
numbers, `true`/`false`/`null`) with the surrounding structure and whitespace copied through unchanged; stripping the
color codes back out always yields byte-identical output to the plain `serde_json::to_string_pretty` form, so a
scripted consumer piping the output (`claude-run status | from json`) is unaffected by color being on or off.

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

CLI mutations delegate to the daemon over the unix socket when it is reachable (a bare `connect()` to the socket path
succeeds; this does not confirm the daemon answers HTTP) and otherwise operate on the database. `trigger` and
`cancel` are the two exceptions: a scheduled pickup or an in-flight run exists only inside the daemon process, so
with no reachable socket both fail outright rather than falling back to the database. A socket-dependent command
that finds the socket unreachable but the daemon's pidfile alive and `serve --no-socket`'s marker present (2.2) tells
the operator to use the TCP web UI / `/api` instead of reporting the daemon as not running. `action list` / `action
show` rescan the actions directory themselves first when no daemon is reachable (3.1's rescan never happens
otherwise); `action delete` / `action reset-attempts` apply the same guards (in-flight-run refusal, missing-row 404)
directly against the database that the daemon's own handlers enforce. `stop`'s own poll grace (5s) is the CLI's
patience waiting for the daemon to finish its separate 30-second shutdown grace (2.2); timing out reports failure
without ever sending SIGKILL. `restart` is implemented as stop, then `exec()` into a fresh `serve` invocation (not an
in-process restart); a stop that times out aborts the restart without execing. `migrate`'s source defaults to
`~/.local/share/claude-run/state` when `--from` is unset; its target is always the configured database, never
SQLite-only. `import` refuses a self-import (its resolved source and target are the same store) independent of the
`--allow-nonempty` guard. `secret set` never accepts an empty value; piped input has exactly one trailing newline
(`\n` or `\r\n`) stripped, and interactive input is prompted twice with echo disabled and must match.

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

A trigger is refused before it is queued, never accepted and silently dropped: a synthetic action (event-driven, no
`prompt`/`input`) 400s by name from the one shared predicate the web UI also uses; an unknown name 404s; a global
pause or a not-ready daemon 409s naming the reason (the pause's own annotation, or the readiness gate's message), so
the caller is never told a run was queued when the dispatcher would have dropped it; a full trigger queue 503s. Delete
refuses (409) a name with a run in flight, checked both in the daemon's in-memory dispatch set (a run dispatched
before its `runs` row exists) and the database (a recorded run with no `ended_at`); otherwise it removes the YAML
file (tolerating one already gone), the action index row, and derived control state (`paused_actions`,
`attempt_counters`, `daily_ledger`), keeping run history (`runs`, `job_outcomes`, stream logs) for audit. An
attempt-counter reset 404s a key with no counter, otherwise clears it and records a `warn`-level `run_events` row
naming the discarded count, so the mutation is never invisible. A pause reason is read from either `{"reason":...}`
JSON or the `reason=...` urlencoded body htmx's Pause form posts; an unparseable body is logged at `warn` and the
pause proceeds with no reason rather than being refused. A settings `PUT` validates the body against the section's
config struct before persisting anything: a validation failure 400s with the message and nothing is saved; a secret
field in the response names only its `*_ref`, never its value. `/status`'s `activity` field is the same `Activity`
snapshot (9) the `status` command and the Status page's quiet indicator read, so the three cannot disagree about
whether a restart is safe; its `stale_gates` and `attempt_cap_hits` are the dependency-gate (3.3) and per-key
attempt-cap (3.1) backlogs an operator without the web UI would otherwise have no machine-readable view of.
`/access/check` is the typed report defined in 4.6.

### 4.3 Web UI (TCP, default port 7321)

Pages: `/` dashboard; `/actions/new`, `/actions/new/raw`, `/actions/new/youtrack`, `/actions/new/closed-issue-summary`;
`/actions/{name}` with `/durations`, `/events` (redirect), `/edit`, `/form`, `/export`, `/delete`; `/logs`; `/secrets` with
rotate, delete and check; `/access`; `/settings` with per-section saves and the Claude, YouTrack, Forgejo, GitHub and git
auth cards; `/reports`; `/status` and `/status/activity`, with `/about` and `/about/activity` redirecting permanently to
them; `/status/cargo-cache/{repo-slug}/delete` and `/status/cargo-cache/delete`; `/status/docker/{category}/prune`; `/runs/{id}`; `/runs/{id}/jobs/{key}/attempt/{attempt}`;
`/sse/runs/{id}/jobs/{key}/attempt/{attempt}` (live stream); `/static/htmx.min.js`, `/static/idiomorph-ext.min.js`.
Auth routes: `/login`, `/login/2fa`, `/logout`, `/setup`, `/account/2fa/setup`, `/invite/{token}`, `/settings/accounts` with
invite, remove and revoke; these and their handlers in `src/web_admin.rs` / `src/auth.rs` are specified separately
(`SPEC-7`), this section's subsections cover only the pages owned by `src/web/mod.rs`, `src/web/native_forms.rs`,
`src/report.rs` and `src/paint.rs`.

#### 4.3.1 Dashboard and run search (`SPEC-4.3.1`)

- The dashboard lists every configured action plus the two always-present synthetic rows (`audit-run`,
  `closed-issue-summary` reporting), each annotated with its live pause/readiness/dependency-gate state and its most
  recent run; a synthetic row that resolves disabled hides itself rather than rendering a dead trigger button.
- Job search matches a free-text needle against job keys and, when given, the matching portion of the job's stream log
  (a short snippet around the first match), filterable by action, trigger source and a recent-run window.
- The duration and plan-usage charts on the dashboard are built from the same `report::` series the Reports page uses,
  scaled to the widget's own bar/line geometry rather than the page's full chart.

#### 4.3.2 Action detail, durations and events (`SPEC-4.3.2`)

- `/actions/{name}/events` is a redirect to the dashboard's events view scoped to that action, not a page of its own.
- The events view's level/action/context filters are populated from the distinct values actually present in
  `run_events`, not a fixed list, so a filter never offers a value with zero matching rows.
- `/actions/{name}/durations` charts one action's own run-duration history; an action with no runs yet renders the page
  with an empty chart rather than 404ing.

#### 4.3.3 Action create, edit, export and delete (`SPEC-4.3.3`)

- `/actions/new/raw` and `/actions/{name}/edit` both validate the submitted YAML with the same `validate_str` path the
  loader uses and re-index the action on a successful save; a rejected save re-renders the editor with the validation
  message instead of discarding the submission.
- `/actions/{name}/delete` and `/actions/{name}/export` operate on the file indexed for that action name, resolved
  through the action index rather than a path built from the name directly, so a deleted or renamed file on disk cannot
  be reached through a stale name.
- A synthetic action (`audit-run`, `closed-issue-summary` reporting) rejects edit, export and delete: there is no file
  backing it to touch.

#### 4.3.4 Native action editor forms (`SPEC-4.3.4`)

- A kind with a structured editor (`youtrack`, `closed-issue-summary`) is edited through `GET`/`POST
  /actions/{name}/form` and created through `GET`/`POST /actions/new/{kind}`, instead of the raw YAML editor; a kind
  with no structured editor (`scripted`) has no `/form` route and uses the raw editor only.
- Every submitted form is serialized to YAML and run through the same `validate_str` + write + re-index path the raw
  editor uses, so a structured save can never write a file the raw editor would reject; a rejected save re-renders the
  form inline with the validation message rather than discarding the submission.
- An edit overlays only the fields the form owns onto the existing YAML mapping: every key the structured editor does
  not expose (for example `dedup_key`, `concurrency`, or a hand-written `claude:` block on a kind that runs no agent
  session) is left untouched.
- A checkbox absent from the submitted body is a false value, never a fallback to the loaded action's or the kind's
  default; a cleared text field whose config key has a documented default is written back as that default rather than
  as an empty string, and a cleared field with no default removes the key instead.
- The new-action filename is derived from the posted name through `safe_action_stem` (letters, digits, `-`, `_`, `.`
  only); a save is rejected if that file already exists rather than silently overwritten.
- If the on-disk YAML for an action being edited fails to parse, the save is rejected with a message pointing at the
  raw editor rather than silently treated as an empty mapping.

#### 4.3.5 Logs page (`SPEC-4.3.5`)

- `/logs` renders `run_events` filtered by level, action and context, windowed to a configurable recent period, newest
  first.

#### 4.3.6 Secrets page (`SPEC-4.3.6`)

- A secret name is restricted to a safe identifier charset; `/secrets` create, rotate and delete all reject a name
  outside that charset before touching the store.
- Rotating or deleting a secret still in use by an action's `token_ref` or the `auth` settings is allowed; the page
  does not track or block on referential use.
- The secrets page never renders a stored secret's value, only its name and `last_used_at`; `check` (`SPEC-7.1`)
  reports whether a name resolves, not what it resolves to.

#### 4.3.7 Access page (`SPEC-4.3.7`)

- `/access` runs the same typed report as `access check` / `POST /api/access/check` (`SPEC-4.2`) and renders it as a
  page instead of JSON.

#### 4.3.8 Settings page and service auth cards (`SPEC-4.3.8`)

- Each settings section (`SPEC-6.3`) saves independently; saving one section never touches another section's stored
  value, and a save takes effect without a daemon restart.
- The Claude, YouTrack, Forgejo, GitHub and git auth cards each validate and materialize their own credential
  (`SPEC-7.3`) on save; a card left blank clears that credential's configured secret reference rather than leaving a
  stale reference pointed at a deleted secret.
- The email test action (`auth_test_email`) sends a real test message through the configured SMTP settings and
  reports delivery failure inline, without persisting anything.
- The Claude login card starts, submits a code to, and can cancel, the interactive broker session (`SPEC-7.5`); the
  rendered banner reflects the broker's own session state, not a separate poll of `claude auth status`.

#### 4.3.9 About and activity pages (`SPEC-4.3.9`)

- `/about` and `/about/activity` render the version, build metadata and the same `quiet` restart-safety flag `GET
  /api/status` exposes (`SPEC-9`), plus a recent-activity feed across all actions.

#### 4.3.10 Run detail, job stream and live SSE (`SPEC-4.3.10`)

- `/runs/{id}` 404s an unknown run id, otherwise lists its jobs sortable by key, outcome, duration and cost, each
  linked through the action's configured `link_template` (falling back to a bare issue-key heuristic when none is
  set) so a job key that looks like a tracker issue becomes a clickable link.
- `/runs/{id}/jobs/{key}/attempt/{attempt}` resolves the attempt's on-disk stream-log path and renders it with the
  run's blocker signature, if any, rendered through the same link-template resolution.
- `/sse/runs/{id}/jobs/{key}/attempt/{attempt}` is a server-sent-events feed over that same on-disk `.jsonl` log: it
  404s an unknown run, waits briefly for the file to appear on a still-running job, reports a permanently absent file
  as `stream-log-gone` on an ended run versus `no-stream-yet` on a live one, resumes from `Last-Event-ID` (or a
  `lastEventId` query parameter on a manual reconnect) by line index, and closes with a `done` event on a `type:
  result` line or a matching `job_outcomes` row.
- Each SSE line carries a server-formatted `ts_fmt` alongside its raw `ts` (the browser cannot run the daemon's
  strftime), honoring the operator's configured locale/timezone/datetime format (`SPEC-6.3`); a line that is not a JSON
  object, or carries no string `ts`, passes through unchanged.

#### 4.3.11 Static assets (`SPEC-4.3.11`)

- `/static/htmx.min.js` and `/static/idiomorph-ext.min.js` are served from the binary's own embedded copy, not fetched
  from a CDN, with a one-year immutable cache-control header so the browser never re-requests them.

#### 4.3.12 Web error responses (`SPEC-4.3.12`)

- Every fallible page handler returns one `WebError` type: a status code plus a plain message, with `From`
  conversions from the error types handlers actually hit (the crate's own error, a database error, a filesystem
  error, a template error), so a stray `?` can never bypass the page's error rendering. A `WebError` renders as a
  plain-text response at its status code.

#### 4.3.13 Reports page and aggregation (`SPEC-4.3.13`)

- `/reports` and its JSON siblings `GET /report/{summary,daily,weekly,by-weekday,by-hour}` (`SPEC-4.2`) share one
  aggregation layer (`src/report.rs`); the `range` (`30d`, `90d`, other values mean all time) and `action` query
  parameters filter every series identically.
- `daily_ledger` is already local-dated and is the source for the daily and weekly spend/run-count series;
  `job_outcomes.ts` and `runs.started_at`/`ended_at` are UTC and are converted to the daemon's local timezone before
  weekday/hour bucketing.
- `by-weekday` and `by-hour` always return a fixed 7- or 24-bucket series (Sunday first, respectively `0..=23`), even
  when a bucket has no data, rather than omitting an empty bucket.
- The top-line `summary` totals spend and run count from `daily_ledger` and run minutes from `runs` (the only table
  with both a start and an end timestamp; a run with a NULL `ended_at` is excluded from the minutes total).
- The Claude plan-usage chart reads its series oldest first (so the chart draws left to right); the most-recent-
  invocations table is a separate newest-first query capped at 20 rows, not a reverse-and-slice of the chart's rows.

The Status page (`/status`, the former About page) carries build and version information, the quiet indicator, the
global Pause and Resume control, the activity list, and the Management part (3.10 item 10). The nav entry "About" is
"Status".

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
7. Two gates run between replay (point 4) and event dispatch (point 5), both accept-and-drop (2xx) rather than reject:
   a hit on global pause, the action's own pause, or a failed pause read; and a configured Claude login that needs
   renewing. Both still record the delivery id as seen, since a retry could not help either way.

#### 4.4.1 Ingress pipeline, types and shared primitives (`SPEC-4.4.1`)

- `WebhookProvider` (`forgejo` or `github`, the `{provider}` path segment) owns its own signature header, delivery-id
  header, event-type header and signature-verification scheme; both providers share the pipeline in SPEC-4.4 (auth,
  replay, rate limit, pause) and converge on the same internal event types downstream.
- A GitHub delivery sent with GitHub's default content type arrives form-encoded (`payload=<url-encoded JSON>`); it is
  decoded back to raw JSON only after signature verification, since the signature covers the original raw body.
- A GitHub `workflow_run` delivery is mapped onto the same internal action-run payload shape a Forgejo Action-run
  delivery produces, so one dispatch path serves both forges.
- One fixed-window rate limiter is built per configured source, so one source exhausting its budget never 429s
  another.

#### 4.4.2 CI-failure triage and mint dispatch (`SPEC-4.4.2`)

- Before triage runs, a failing run's fingerprinted tests are checked against the `ci_failures` store: a match on an
  `open` row attaches a new sighting with no `claude` call and no mint; a match on a `resolved` row reopens it and
  re-triages it as a regression; no match falls through to classification, after one legacy-adoption lookup (an issue
  minted before the store existed).
- Whether the failing commit sits on an open PR is resolved before the store-match dispatch; a lookup failure here is
  a routing failure, never silently folded into "no open PR" the way the advisory open-PR check fed to the classifier
  degrades to `false` on a lookup failure or an underivable forge host.
- An issue's live YouTrack `State` is re-checked before treating an `open` store row's reappearance as a regression,
  so the store never reopens an issue the resolve-on-merge workflow (`SPEC-3.3.1`) already closed; a `State` read
  failure is never read as "still open".
- A fresh mint or a legacy adoption records a new `ci_failures` row together with its failing tests and the
  triggering run; the caps/attempt ledger advances only on a genuine new mint, never on an attach or a reopen.

#### 4.4.3 PR-comment routing and automerge (`SPEC-4.4.3`)

- A PR comment is posted through credentials materialized and scoped to the call (`SPEC-7.2`), using the GitHub or
  Forgejo backend the source is configured for.
- A failure on the agent's own open PR either re-queues the issue or, past a fix-attempt ceiling, flags it `Needs
  Review`; a re-queue is suppressed in favor of `Needs Review` when the new failure's fingerprint set repeats the
  immediately preceding attempt (the loop-breaker).
- A failure on a human-authored open PR routes by whether its tests overlap an existing `ci_failures` row: an open
  match attaches (after re-checking the issue is not already resolved); a resolved match reopens and comments; a
  `pr_only` match on a second distinct branch promotes to a real minted issue, pointing the existing row at the new
  issue rather than duplicating it, and comments the issue link onto every open PR the failure was sighted on; a
  first sighting records a `pr_only` row and always comments the PR, minting no issue yet.
- On an automerge conflict (an `update_pr_branch` failure), a configured conflict-auto-resolve dispatch sends a
  synthetic command to the comment listener (`SPEC-4.4.5`) instead of leaving a "needs a human" comment.
- An automerge outcome (conflict dispatch, behind-base update/merge, or clean merge) is recorded and its comment
  posted exactly once per decision, guarded against double-posting.

#### 4.4.4 Action-run success and pull-request delivery consumption (`SPEC-4.4.4`)

- A verified terminal (`closed`) pull-request delivery and a verified `success`/`recover` Action-run delivery are
  each dispatched onto their own channel; with no channel configured the handler acks and drops with a log rather than
  failing the delivery.
- A merged agent PR records the outcome and leaves the issue `State` to the YouTrack resolve-on-merge workflow
  (`SPEC-3.3.1`); a closed-but-unmerged agent PR records the outcome so it stops counting toward open-PR routing.
- A success/recover event resolves to its agent PR, if any, and re-triggers the SPEC-4.4.3 automerge decision now
  that a check just went green.
- A repository-level fault (for example an unreachable forge) notifies every open agent PR tracked for that
  repository rather than failing silently per event.

#### 4.4.5 Comment-command webhooks (`SPEC-4.4.5`)

`POST /webhooks/{provider}/comment/{source}` is a second, independent listener sharing the pipeline mechanics in
SPEC-4.4 (provider/source resolution, rate limit, HMAC verify, delivery/replay, pause gate) and the same replay store,
rate-limiter type and allow-list matcher as the triage listener, but its own write token and pause switch. A GitHub
delivery that is not `issue_comment` (except `ping`) is acked and dropped before parsing.

- A comment is a command only when its first line opens with the configured `trigger` followed by whitespace or
  end-of-line; an empty or `help` instruction is a help request, and one over `max_instruction_chars` is refused by
  name. A delivery is dropped (2xx, logged, never a command) when its action is not `created`, its repository is not
  allow-listed, or its author is a configured bot login.
- Authorization is a live read of the commenter's repository permission against the forge, never cached; the read
  failing, or ranking below `allowed_permissions`, denies (fail closed). A synthetic (runner-dispatched) command skips
  only this check. At most one denial reply is sent per (repository, commenter) per window.
- A comment command is one-shot: its effective per-key attempt cap is one, and that same counter is the redelivery
  guard, keyed by a hash of (repository, PR/issue number, comment id). Admission also checks the shared daily run
  ledger. A refusal before admission (login required, over cap) advances no counter and runs no `claude`, and replies
  naming the reason. At most one comment job per thread runs at a time; a second command is told so and dropped.
- A pull-request command clones the PR's head branch, writes the instruction verbatim to a file outside the work
  tree, runs Claude, then a decision gate classifies the result as no changes, unresolved conflicts, a measurement
  failure, too big (over `max_files` / `max_lines`), or a push; only a push pushes and posts a push comment
  (`SPEC-4.4.4`-adjacent, `CLAUDE-447`).
- A plain-issue command clones nothing and runs no agent: it mints a YouTrack issue carrying the instruction verbatim,
  sets `State` then the `AI Agent` field to the queued value last, and replies naming the minted key.
- Every outcome replies on the originating thread; a served command records one `job_outcomes` row and finalizes
  through the run guard.

### 4.5 MCP tools

Seven read-only tools, served over stdio (`mcp stdio`) and, when enabled, Streamable HTTP at `/mcp`: `get_status`,
`list_runs`, `get_run`, `get_attempts`, `get_stream_log`, `list_ci_failures`, `get_ci_failure`. Each is annotated
`read_only_hint = true, destructive_hint = false`; none writes anything. `list_runs` / `list_ci_failures`'s `limit`
defaults to 20, capped at 100; `get_stream_log`'s `tail_lines` defaults to 200, capped at 2000. Limits above the
maximum are rejected, not clamped, so a caller is told its request was too large rather than silently handed a
truncated answer it could mistake for the whole picture. `get_status`'s result carries `"source": "database"` and
makes no claim about whether the daemon process itself is running, distinguishing it from the CLI `status` command's
daemonless path (same `state::status_blocks` assembly, with its own `daemon` / `socket_expected_at` keys layered on
top) and from `/api/status` (4.2).

`get_stream_log` rejects a recorded path that, canonicalized, resolves outside the canonicalized logs directory,
the same symlink-escape defense a write path would need. It returns one of three distinct error cases: no job
outcome recorded for the run/job/attempt, a recorded path with no stream log, or a file the retention sweep has
since removed; none of the three is a bare "not found". A successful read tails the content to `tail_lines` lines
and then, if still over the whole-response cap (256 KiB), trims further from the front to the nearest UTF-8
character boundary; the result states `total_lines`, `returned_lines` and whether either step truncated. Stream
logs are returned unredacted (the caller already has full container access via `docker compose exec`, over stdio) or
is otherwise relying on the authenticated bearer grant below standing in for that access, over HTTP.

The HTTP listener (`[mcp].enabled`) wraps the same tool set behind the `mcp-web` crate's own local-account, OAuth 2.1
and bearer layer, a separate account store from the web UI's `web_admin`/`web_accounts` (7.9): granting MCP access is
a narrower, read-only grant that must not share credentials or sessions with a full dashboard login. It serves OAuth
discovery under `/.well-known/`, and restricts the `Host` header to the authority of `public_url` plus
`[mcp].allowed_hosts`. Two key files (`mcp-master.key` encrypting its own `mcp-web.db`, `mcp-session.key` signing its
sessions) are generated once under the config directory on first start with the listener enabled, mode `0600`,
`create_new` so a concurrent first start cannot race a second generation; a present but corrupt file fails loudly
rather than silently regenerating, the same discipline `secrets.key` follows (7.1) applied to an unrelated key pair.
Its account/session/OAuth state lives in its own `mcp-web.db` under the data directory, entirely separate from
`state.db` or the configured `[database]` backend. A missing `public_url` or a corrupt key file fails only this
listener, logged at `error`; the rest of the daemon runs unaffected. See `docs/conventions/mcp.md` for the full
narrative (dependency footprint, the `AlwaysConnected` upstream-directory trivial implementation, TOTP being
optional for this account store where it is mandatory for the dashboard's).

### 4.6 On-demand access check

One service answers, without queueing a job, whether the runner has the access it would need, behind three surfaces
that cannot drift because they call the same function: the `/access` page, `POST /api/access/check` (4.2), and
`claude-run access check` (4.1, non-zero exit on any failed probe). No secret value ever leaves the module: a row
names the secret, the probe and the failure text only, never a token.

A request names zero or more of `secret`, `host`, `repo`, `project`; naming neither `repo` nor `project` is the
aggregate form, checking every enabled `youtrack` action's credential, the projects its own query reaches (capped at
10 projects per action; reaching more adds its own row saying so, rather than truncating silently, and names a
project to check the rest), and the repos those projects' VCS bindings resolve to. Each row's `kind` is one of
`identity` (is the credential still valid), `project` (a YouTrack project reads), `vcs` (a project's VCS binding
resolves to a host and repo path), `query` (the action's own query executes under the credential), `repo` (a forge
repo passes both read-scope categories `Forge::repo_access` checks), `write` (a capability only a mutation could
prove), or `setup` (the check itself could not be set up). Its `outcome` is `pass`, `fail`, or `unverifiable`:
`unverifiable` is deliberately not a failure, since it is the honest answer for a capability (opening a PR, writing
the tracker's `AI Agent` field, a `State` transition, a pickup comment) that only a real write would prove, and a
write probe would leave a real issue or PR behind. A missing `yt` / `fj` binary is reported as a `setup` row rather
than a 500; with no enabled `youtrack` action to check at all, that "nothing to check" row wins over reporting the
CLI absent, since it is the more useful answer.

Credentials are materialized into their own scratch directory exactly as a job's `JobCredentials::materialize`
does (7.2), keyed by the resolved host, and scrubbed when the probe's guard drops, so the check exercises the real
per-job credential path rather than falling back to ambient host auth by accident. A repo probe auto-routes to the
GitHub or Forgejo backend by host, the same routing a job's own forge selection uses (3.3.1): `github.com` or a
listed `github.hosts` entry uses the GitHub token, any other host uses the Forgejo token. The probe order per
action is identity, then the write row, then (aggregate) the project list, then per project: project read, VCS
binding, and the project-scoped query; discovered repos are probed last. A YouTrack project the token cannot read
and one that does not exist are reported identically by the tracker (HTTP 400 `invalid_query`, not 401/403); the
row's remediation says so explicitly rather than leaving the raw text to read as a query syntax error.

Recovery markers: `retain_access_recoveries` drops a poll-time access-recovery event when the repo it names is not
currently in an open failure, so a success marker is recorded only on a fail-to-pass transition (3.3) instead of
once per poll forever, while still clearing the dashboard's open-failure banner.

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

### 5.1 Query portability layer (`SPEC-5.1`)

`src/db.rs` opens the ONE `sqlx::AnyPool` (`DbPool`) either backend uses, so no module outside it names a backend
type; `Any` forwards SQL unchanged, so `db::q` is the chokepoint every statement passes through. It renumbers `?` /
`?N` placeholders into Postgres's `$n` form and expands the two dialect-neutral functions call sites are written in,
`strpos(haystack, needle)` and `elapsed_seconds(from, to)`; everything else (`COALESCE`, `ON CONFLICT DO NOTHING`,
`CAST(... AS BIGINT)` around a `SUM()`, `<table>.<col>` on the right of a `DO UPDATE SET`, no negative `LIMIT`) is
spelled portably in place at the call site instead. `db::flag` decodes an integer 0/1 column to `bool`, since `Any`
will not coerce that conversion itself.

### 5.2 Schema reference (`SPEC-5.2`)

The table above is the canonical column reference every `src/state.rs` read and write implements; a function there
that inserts, updates, selects or deletes rows of one of those tables implements that table's row lifecycle, unless
a more specific requirement elsewhere in this document (for example 3.1, 3.7, 7.1, 7.9 or 7.10) already governs that
behavior, in which case the more specific section is the citation of record. The `settings` and `secrets` tables are
further specified in 6.3 and 7.1 respectively.

### 5.3 Backend import (`SPEC-5.3`)

`claude-run import` (`src/import.rs`) carries the data over when an operator switches backends, since selecting
Postgres only opens an empty instance. The copy is schema-driven, not typed per column: the source's own
`pragma_table_info` gives the column list and the declared type that decides the Rust type each value round-trips
as. `import::TABLES` is the hand-maintained foreign-key order, checked against the live catalog on every import (so
a new table fails the copy instead of being silently left behind); `IDENTITY_COLUMNS` lists the Postgres sequences
advanced past the copied ids, without which the daemon's first insert after the import would collide with imported
history. The source opens `mode=ro` and is never written; the whole copy runs in one transaction; a target already
holding rows is refused unless `--allow-nonempty`; per-table source/copied/target counts are printed and checked
before the commit; and a source whose schema is older than the running build is refused, naming every missing
column, because a read-only open cannot apply the pending SQLite migrations.

### 5.4 Schema migrations and parity (`SPEC-5.4`)

`claude-run migrate` and every other entry point that opens the pool run the embedded migrators in `migrations/sqlite/`
and `migrations/postgres/` at open. The SQLite chain is the original incremental files, byte-identical so a deployed
`state.db` matches its recorded checksums and re-runs nothing; the Postgres chain is a hand-written squash of the
same end state (`BIGINT` for every integer, `DOUBLE PRECISION` for `REAL`, `BYTEA` for `BLOB`,
`GENERATED BY DEFAULT AS IDENTITY` for the autoincrement keys, boolean-ish columns left as integer 0/1). A schema
change adds a new incremental file to BOTH directories with the same end schema on both sides; a test asserts the
two chains end on an identical schema, so a one-sided migration fails the suite instead of surfacing only against
the backend nobody tested.

### 5.5 Unified error type (`SPEC-5.5`)

`src/error.rs` defines the one `Error` enum, and its `Result<T>` alias, every fallible path in the binary returns:
`#[from]` conversions fold `io`, `serde_yaml`, `serde_json`, `sqlx::Error`, `sqlx::migrate::MigrateError` and
`anyhow::Error` into it so a leaf call site can propagate with `?` without naming its own error type, while the
domain-specific variants (action validation, snippet and provider-CLI failures, the per-run issue-creation cap,
secret resolution) carry structured fields (name, field, path, exit code, stderr) so the closed failure message a
caller renders names the specific thing that went wrong instead of a generic wrapper message.

### 5.6 Postgres test fixture (`SPEC-5.6`)

With `CLAUDE_RUN_TEST_DATABASE_URL` set, every test fixture pool runs on that PostgreSQL server (`db::test_pool`). The
shared contract for such fixtures is section 12 of the `common` specification, which isolates each test in a schema of
its own; claude-run deviates to a database-per-test clone (CLAUDE-540) because replaying the ~130-statement migration
set per test was 94% of the CI job's wall time and failed under load. The requirements below are claude-run's
implementation.

- **SPEC-5.6.1**: Each fixture pool MUST get a disposable database of its own on the server, named from the process id
  and a per-process counter, reached by replacing the database path of `CLAUDE_RUN_TEST_DATABASE_URL`. Its tables live
  in the default `public` schema, so no `search_path` is set. `CLAUDE_RUN_TEST_DATABASE_URL` itself names the
  maintenance database, on which the clone and drop statements run.
- **SPEC-5.6.2**: The fixture MUST build one template database per test process, migrated once by running the SQL of
  `POSTGRES_MIGRATOR` joined in version order (not the migrator itself, so no `_sqlx_migrations` rows), and MUST NOT
  rebuild it per test. Each test MUST get its database with `CREATE DATABASE <name> TEMPLATE <template>`, a block copy,
  rather than replaying the schema.
- **SPEC-5.6.3**: The clone MUST run on one connection to the maintenance database (`CREATE DATABASE` cannot run in a
  transaction). A fixture database therefore holds the schema but no `_sqlx_migrations` rows.
- **SPEC-5.6.4**: The fixture MUST drop its database with `DROP DATABASE IF EXISTS` when the last handle of the pool is
  dropped or the pool is closed. One worker thread per test process does all the dropping over a single connection to
  the maintenance database, so cleanup adds one server connection however many tests run in parallel. `DROP DATABASE`
  names one database and cannot run in a transaction, so a drained backlog is dropped one statement at a time; a drop
  that loses the race with the pool's own connection teardown (`55006`, "being accessed by other users") is retried
  briefly, with no `WITH (FORCE)`. It is a thread of its own because the test's runtime is gone by then.
- **SPEC-5.6.5**: A failed drop MUST print one line to standard error naming the database it was dropping and the
  database error, then leak it (harmless: the server is a throwaway container discarded on teardown).
- **SPEC-5.6.6**: Once per test process, before the template is created, the fixture MUST fail naming the database when
  the maintenance database's `public._sqlx_migrations` exists, since that marks a database a runner has migrated
  (CLAUDE-403).
- **SPEC-5.6.7**: A test MUST apply `POSTGRES_MIGRATOR` itself into a fresh database of its own and assert that it
  recorded every migration, so the production path stays exercised while fixtures clone the template.
- **SPEC-5.6.8**: The Postgres CI leg MUST re-run only the test files that build a DB pool
  (`scripts/postgres-test-filters.txt`); the SQLite leg runs the whole suite. SQL runs only through a DB pool, so this
  keeps every SQL-executing test on both backends while sparing the pure-logic tests a redundant second run. A guard
  test MUST fail if a source file gains a DB test no filter covers, or if a filter matches no DB-touching module.

## 6. Configuration

### 6.1 Environment variables (`SPEC-6.1`)

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

### 6.2 `config.toml` (bootstrap, unknown keys rejected) (`SPEC-6.2`)

| Key | Default | Purpose |
|---|---|---|
| `[web] bind` / `port` | `127.0.0.1` / `7321` | TCP listener |
| `[web] auth_token_ref` | unset | Secret name of the bearer token |
| `[web] require_auth_on_loopback` | `false` | Force auth on a loopback bind |
| `[web] auth_username` | unset | Legacy form-login username (used with the token while no account exists) |
| `[daemon] log_filter` | unset | Tracing filter |
| `[daemon] claude_config_dir` | unset (`<config dir>/claude-agent-home`) | Agent `CLAUDE_CONFIG_DIR`; see `SPEC-8.1` |
| `[database] url` | unset (SQLite) | `sqlite://`, `postgres://` or `postgresql://`; any other scheme is a startup error |
| `[webhook]`, `[mint]` | see 3.5 | Bootstrap copies of the DB-backed sections |
| `[mcp] enabled` / `bind` / `public_url` / `admin_email` / `allowed_hosts` | `false` / `127.0.0.1:7322` / unset / unset / empty | MCP HTTP listener; `public_url` is required when enabled |

`[webhook.sources.<name>]`: `provider` (`forgejo` default, `github`), `secret_ref`, `allowed_repos` (exact `owner/repo`,
`owner/*` or `*`; empty denies all), `rate_limit_per_min`, `log_token_ref`, `write_token_ref`. Enabling the listener
requires `secret_ref` and `write_token_ref` on every source.

### 6.3 DB-backed settings sections (`SPEC-6.3`)

Edited on the Settings page or through `PUT /api/settings/{section}`; changes apply without restart. Fields not listed in
3.x: `webhook.analysis_timeout_secs` (600), `webhook.analysis_concurrency` (2); `concurrency.max_parallel_global` (0 =
largest enabled action cap); `preferences` (`locale`, `timezone`, `datetime_format`); `claude` (`model`, `effort`,
`autocompact`); `cargo_cache` (`enabled`, `max_bytes_per_repo`, 3.10); `docker_gc.max_bytes` (250 GB, 3.11); `auth` (service base URLs, secret names, git author name and email). `webhook_comment.sources.<name>`
adds `allowed_permissions`, `max_files`, `max_lines`, `bot_author`, `bot_email`, `skip_marker`.

Each section is one JSON document keyed `config.<section>` in the `settings` table (5.2); the serde struct in
`src/config.rs` IS the schema, so saving a section runs the identical `deny_unknown_fields` validation the
`config.toml` parser uses, and the web Settings page and the `/api/settings/<section>` JSON endpoints share this
one path. `Section` (`src/settings.rs`) enumerates the twelve sections (`Section::ALL`) and maps each to its
settings key, its URL path segment, its page title, and an "apply" badge (`next event` / `live` / `next tick` /
`next sweep` / `next invocation`) describing when a save takes effect; a section-specific `validate_*` function
rejects a value the serde schema alone would accept but that would be unsafe to apply (for example a DB-retention
window shorter than the longest UI report range, or a zero-second concurrency regather interval) before it is
persisted. `EffectiveRuntime` resolves each section once at bootstrap for `LiveSettings::new`: the DB document when
present, else the parsed `config.toml` value for a section still eligible to live there, else the built-in default;
a section that was always DB-only (never a `config.toml` key) has no file fallback.

### 6.4 Live settings apply (`SPEC-6.4`)

`LiveSettings` holds the runtime copy of every DB-backed section behind `Arc<RwLock<..>>` handles shared with the
webhook listener, the comment-webhook listener, the mint context, the GC sweepers, the scheduler and
`ClaudeInvoker`, so a save through the Settings page or the JSON API applies without a daemon restart: the log
filter swaps through the `tracing` reload handle (unless `CLAUDE_RUN_LOG` is set, which always wins, or no handle is
available), the webhook and comment-webhook source maps and their per-source rate limiters are rebuilt together with
the config they gate, and every GC sweeper and the scheduler read the live handle on their own next tick. The
`claude` section's handle is a plain `std::sync::RwLock` rather than the tokio one every other section uses, because
its reader (`ClaudeInvoker::invoke`) is synchronous and cannot await. `replay_window_secs` (part of `webhook`) is the
one documented exception: it applies on restart only, since the webhook replay store is constructed once at serve
start. `LiveSettings::detached` builds a standalone instance with built-in defaults and no live log-filter hook, for
tests and any context without a daemon.

### 6.5 Config-file to DB one-time import (`SPEC-6.5`)

At serve start, `migrate_config_file` parses `config.toml` raw and, for each section that was MOVED out of the file
(`[mint]`, `[webhook]`, `[daemon].log_filter`; bootstrap keys like `[web]` and `[daemon]`'s binary overrides are
ignored here) and is still present in it, either imports that section into the DB when the DB does not yet hold it,
or refuses to start naming the section when the DB already holds it: that is the breaking-migration signal that
prevents two sources of truth for the same setting once an operator has saved it through the Settings page. A
missing `config.toml`, or one with none of the three moved sections, is a clean no-op. A section born DB-only (never
a `config.toml` key, for example `webhook_comment` or `docker_gc`) has no import path at all.

### 6.6 Throwaway test Postgres (`SPEC-6.6`)

`claude-run test-postgres [--image <ref>] [--init-sql <file>]... -- <command> [args...]` starts a private,
non-durable PostgreSQL container in the Docker-in-Docker sidecar, runs `<command>` against it with `DATABASE_URL`,
`CLAUDE_RUN_TEST_DATABASE_URL` and `CLAUDE_RUN_TEST_POSTGRES_CONTAINER` set, and tears the container down however
the command ends (normal exit, command failure, `SIGINT`/`SIGTERM`, or a setup error), propagating the command's
exit code (128 + signal if it was killed by one) and failing the run if teardown itself fails even when the command
succeeded. It sweeps orphaned containers from earlier runs whose owning pid is no longer alive first, starts the
container with a loopback-only ephemeral published port, `--rm`, no volume (the writable layer and data
directory are both discarded on teardown) and a raised `max_connections` (the parallel fixture leg needs more than the
default 100, CLAUDE-540), waits for an actual `psql` connection (not `pg_isready`, which can report
ready during the official image's post-`initdb` restart) up to 60s, then disables `fsync`, `synchronous_commit` and
`full_page_writes` and verifies the change by reading `SHOW fsync` back, retrying for up to 20s against a
restart-shaped connection failure. Each `--init-sql` file then runs in order with `ON_ERROR_STOP`, through the same
retry. The generated password never appears in a log line or an error message.

## 7. Security

### 7.1 Secrets store and key lifecycle (`SPEC-7.1`)

- The symmetric key is an `age` X25519 identity read from a file: `CLAUDE_RUN_SECRETS_KEY_FILE` when set, else
  `<config dir>/secrets.key`. The key is configuration, never written beside the `secrets` table's ciphertext. The key
  line (`AGE-SECRET-KEY-...`) may sit anywhere on a line of the file, not only alone on one, so a SOPS-decrypted
  document or an env-style line works unmodified; blank lines and `#` comments are skipped.
- A missing key file is generated only when the instance has nothing to lose: no key file exists yet, no legacy
  data-dir key file exists to migrate, and the `secrets` table is empty. The generated file is mode `0600`. A key file
  readable beyond its owner loads anyway but logs a `warn` naming the mode, since every process able to read it can
  decrypt every stored token.
- An older key found at `<data dir>/secrets.key` is moved (not copied) to the config-dir path on first start after the
  change, so an existing deployment keeps decrypting what it already stored.
- A missing key file with one or more rows already in `secrets` is a hard error naming every secret at risk; a
  replacement key is never generated in that case, since it would make the existing ciphertext permanently
  undecryptable while the store kept listing it as present.
- Resolving a secret by name decrypts its ciphertext under the loaded key and bumps `last_used_at`. An unknown name or
  ciphertext that fails to decrypt under the loaded key is a closed failure, never an empty token.

### 7.2 Per-job credential materialization (`SPEC-7.2`)

- An action's `youtrack.token_ref` / `forgejo.token_ref` / `github.token_ref` are resolved to `ResolvedCredentials`
  together; the key file is loaded only when at least one is set, so the host-auth path (no block sets a `token_ref`)
  needs no key configured and resolves nothing. `youtrack.token_ref` additionally requires `youtrack.base_url`, needed
  to build the single-file `yt` config.
- `JobCredentials::materialize` writes the resolved credentials into a per-job directory (mode `0700`): a two-line
  `yt-config.yml` (`url:` + `token:`, mode `0600`) pointed at by a `YOUTRACK_CLI_CONFIG` env entry for the `yt` child
  only, and/or a `forgejo-cli/keys.json` (mode `0600`) keyed by the job's resolved forge host, pointed at by an
  `XDG_DATA_HOME` env entry for the `fj` child only. A resolved `fj_token` with no host passed in (e.g. the gather step,
  which never invokes `fj`) materializes nothing for `fj`. A decrypted GitHub token is carried in memory only, never
  written to a file, so it leaves no on-disk plaintext to scrub. Dropping the guard removes the whole per-job directory,
  scoping every materialized plaintext to that job's lifetime; concurrent jobs each get their own directory and never
  share a credential file.
- The `fj` host entry written for a per-job or daemon-service credential clones the operator's own key-store entry
  for that host when one exists, keeping every non-token field (`type`, `name`, and any future addition) verbatim and
  substituting only the token, so claude-run never re-derives `fj`'s store schema by hand; with no operator entry for
  that host it synthesizes a complete `{type: "Application", name: "claude-run", token}` entry, since `fj` requires
  `type` and `name` and rejects a token-only entry. The operator's own token for that host, if any, is never carried
  into the per-job or service store.
- A throwaway single-host `fj` key store (used for a decrypted `log_token_ref` around one `fj actions logs` call) lives
  under its own temp `XDG_DATA_HOME` directory and is removed when its guard drops, so that credential's plaintext
  lifetime is scoped to the one call that needs it.

### 7.3 Daemon service logins (`SPEC-7.3`)

- At serve start the daemon materializes its own logins to Claude, YouTrack, Forgejo and GitHub from the `auth`
  settings document: for each of `anthropic_key_ref` / `youtrack_token_ref` (with `youtrack_base_url`) /
  `forgejo_token_ref` (with `forgejo_host`) / `github_token_ref` (with `github_host`) that is fully configured, the
  named secret is resolved and the credential materialized; a secret that fails to resolve, or resolves to an empty
  value, is a `warn` naming it, and every other configured credential is still materialized. With none of the four
  configured, every file this step would otherwise touch is left alone, so a login entered by hand inside the
  container is never clobbered.
- The git commit identity (`git_author_name` / `git_author_email`) is the one credential written unconditionally,
  defaulted to `claude-run` / `claude-run@a8n.run` when unset, so a fresh deployment with nothing configured in
  Settings -> Auth still has a working commit identity with no exec step. With a Forgejo host configured, the same
  write adds a `credential "https://<host>"` stanza naming `fj auth git-credential` as the helper, scoped to that host
  only; a host value that could close the quoted config subsection or start a new config line (whitespace, control
  characters, `"`, `\`, `[`, `]`) is refused rather than written.
- The materialized destinations are fixed paths under the config dir (the single-file `yt` config, `gh`'s own config
  dir, the git identity config) so `YOUTRACK_CLI_CONFIG`, `GH_CONFIG_DIR` and `GIT_CONFIG_GLOBAL` can be exported once
  at serve start, before any child is spawned, and a credential saved later in the web UI needs no restart to reach the
  next child.
- `gh auth login --hostname <host> --with-token --git-protocol https --insecure-storage` receives the token on stdin
  from a throwaway `0600` file passed by explicit shell redirection (never a bare pipe, and never an argv value, which
  would leak it into `ps`), under a `30s` timeout. A failing login's error carries the exit code and `gh`'s stderr with
  the token redacted, so it never reaches a log line or a rendered page.

### 7.4 Claude login status probe (`SPEC-7.4`)

- `claude auth status --json` runs under the agent's own `CLAUDE_CONFIG_DIR` (plus `ANTHROPIC_API_KEY` when one is
  stored) with a `30s` wall-clock timeout. Only `{"loggedIn": true}` and `{"loggedIn": false}` are decisive outcomes; a
  spawn failure, a timeout, or output that does not parse as that shape is a third, indeterminate outcome that must
  never be collapsed into either decided answer, so a transient probe failure never flips readiness in either
  direction.

### 7.5 Interactive Claude sign-in broker (`SPEC-7.5`)

- Starting a broker-driven `claude auth login` replaces any session already in flight (an operator pressing the button
  again means the old authorize URL is stale) and spawns the child with piped stdio under the agent's
  `CLAUDE_CONFIG_DIR`. The child must print its authorize URL within `30s` or it is killed and the attempt fails; once
  started, its combined output is drained continuously so the pipe never fills and blocks the child.
- Submitting a code writes it to the child's stdin. The child exiting `0` is `LoggedIn`. The CLI answering a code it
  rejects without exiting is a `Retry` carrying the CLI's own stderr wording, and the broker keeps the session so the
  operator can paste another code into the same child, which still holds the PKCE verifier the first code was redeemed
  against. Any I/O failure writing or flushing the code, or a wait error, terminates the session rather than leaving a
  half-dead child. Submitting with no session in flight is an error, not a silent no-op. A session that waits longer
  than 15 minutes between requests is dropped the next time it is viewed.
- An operator can also abandon an in-flight session explicitly, killing its child immediately rather than waiting for
  the staleness drop above; this is the manual counterpart to the automatic drop, triggered from the Settings Auth
  card.

### 7.6 TCP surface authentication (`SPEC-7.6`)

- TCP surface authentication is required for any non-loopback bind, or for a loopback bind with `require_auth_on_loopback`
  set; a loopback bind with the flag unset serves unauthenticated, as before. An unparseable, non-`localhost` bind value
  is treated as non-loopback, so the check fails safe (requires auth) rather than open.
- Credentials: `Authorization: Bearer <token>` (constant-time compare, scheme match case-insensitive per RFC 7235)
  naming the secret `auth_token_ref`, or the session cookie (7.8). A configured `auth_token_ref` that fails to resolve
  (no key file, unknown secret, empty value) is a warning, not a fatal startup error: bearer auth is then unavailable
  for the run, but the browser login or first-run setup still gets the operator in.
- The legacy pair `auth_username` plus the `auth_token_ref` plaintext authenticates the login form only while no DB
  account exists; once any account exists, only a DB account (7.9) can sign in through the form. The legacy pair's
  session is signed with the bearer token as its HMAC key under the sentinel account id `0`; every other session signs
  with that account's own rotating `session_key`, so rotating either invalidates exactly the sessions it signed.
- `/webhooks/`, `/login`, `/login/2fa`, `/logout`, `/setup` and `/invite/*` are exempt from this gate: webhooks
  authenticate by HMAC (4.4) and cannot carry a bearer header, and the auth routes themselves must stay reachable
  unauthenticated or there is no way to obtain a credential. `/account/2fa/setup` still requires a valid credential; it
  is exempt only from the enrollment check in 7.10, not from this gate.

### 7.7 Unconfigured instance (`SPEC-7.7`)

- With no credential and no account at all, a browser GET that accepts `text/html` is redirected to `/setup`; every
  other request is refused with 503 naming the state (`claude-run has no account yet`). The surface is never served
  unauthenticated by falling through to the protected routes.
- `/setup` is a dead end once any account exists: a further GET or POST redirects to `/login` instead of minting a
  second account, so setup runs exactly once and only an accepted invite (7.11) can add another account.

### 7.8 Session and pending-2FA cookies (`SPEC-7.8`)

- The session cookie (`claude_run_session`) carries plaintext claims `"<account id>.<expiry>.<hmac>"`, set HttpOnly,
  Secure, SameSite=Lax, scoped to `/`, with a 12-hour TTL, HMAC-SHA256-signed with the key named in 7.6. An expired
  cookie, or one whose recomputed signature does not match, is rejected.
- The pending-2FA cookie proves a password check already passed and a second factor is still owed: the same claim
  shape plus a wrong-attempt counter, a 5-minute TTL, and a cap of 5 wrong codes before the cookie is discarded and the
  operator is sent back to `/login` to start over.

### 7.9 Accounts (`SPEC-7.9`)

- Any number of accounts live in `web_accounts`, each with its own argon2id password hash and its own rotating
  `session_key`; there are no roles yet, so every account has full rights.
- Username lookup and the uniqueness check are case-insensitive. Creating or accepting an invite refuses an empty
  username, a password under `MIN_PASSWORD_LEN` (12) characters, or a username already in use, each surfaced as a
  visible reason rather than a generic failure, and writes nothing on refusal.
- Removing an account is refused for the last remaining account and for removing one's own account, both as visible
  errors rather than a silent no-op; a successful removal also deletes that account's recovery codes.
- Changing a password rotates the session key with it, signing every existing browser session for that account out
  immediately.

### 7.10 TOTP enrollment and login (`SPEC-7.10`)

- TOTP enrollment is mandatory: an authenticated but unenrolled session may reach only `/account/2fa/setup` and
  `/static/*`; every other route refuses it (a browser GET is redirected to the enrollment page, anything else gets a
  plain refusal), so no other page loads until enrollment is confirmed.
- Enrollment: beginning (or restarting) it generates a fresh RFC 6238 secret, stores it encrypted but unconfirmed, and
  returns the `otpauth://` URI and its rendered QR code; reloading before confirmation just replaces the pending
  secret. Confirming it with a valid current code marks the account enrolled and mints `RECOVERY_CODE_COUNT` (10)
  single-use recovery codes, rendered once and never stored in the clear.
- Login-time verification accepts the current or an adjacent TOTP time step, or an unused recovery code (consumed on
  success); the same TOTP step is never accepted twice (replay refused). A wrong code bumps the pending cookie's
  attempt counter (7.8); a corrupt stored secret is a surfaced error, never silently treated as "not enrolled" or as a
  wrong code.
- `access reset-2fa` clears enrollment and every recovery code and rotates the session key, forcing re-enrollment and
  signing out every existing session for that account.

### 7.11 Invites and email (`SPEC-7.11`)

- An invite is a single-use token in `web_invites`, stored only as a hash, minted by a signed-in account and expiring
  `INVITE_TTL_HOURS` (72) hours after creation. Accepting it creates the account and marks the invite used inside one
  transaction, so a race between two acceptances of the same token can create at most one account; a used, expired or
  unknown token resolves to nothing.
- With no email address given, the invite link is shown once in the accounts UI. With an address, the link is emailed
  instead through `src/mailer.rs` (CLAUDE-394), built from `CLAUDE_RUN_PUBLIC_URL` rather than any request's `Host`
  header, and not displayed, unless the send fails, in which case the invite is kept (not discarded) and the link is
  shown once as a fallback. The emailed body never carries the SMTP credential.
- Sending by email requires SMTP to be configured: `CLAUDE_RUN_SMTP_HOST`, `_FROM` and `CLAUDE_RUN_PUBLIC_URL` must all
  be set, and a `_USERNAME` without a password is refused (CLAUDE-428); the transport is built fresh from the process
  environment on every send, never from a Settings-page form, so a changed environment applies on the next daemon
  restart, not live. An invalid recipient address is refused with 400 and mints no invite; an unconfigured SMTP
  environment is refused with 400 naming the missing variables rather than silently falling back to the link-only
  flow.

### 7.12 Webhook hardening (`SPEC-7.12`)

- Webhook hardening: per-source HMAC secret, rate limit before crypto, shared replay store, repository allow-list that
  denies by default, and a comment-command permission check plus decision-gate caps.

### 7.13 CI log and run-summary rendering (`SPEC-7.13`)

- Logs fetched from CI are redacted before storage or attachment. Run summaries rendered in the UI are Markdown with raw
  HTML demoted to text.

### 7.14 Unix-socket API (`SPEC-7.14`)

- The unix-socket API has no authentication and is disabled by `--no-socket` in the container image.

### 7.15 Agent write boundary (`SPEC-7.15`)

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

### 8.1 Agent config dir isolation (`SPEC-8.1`)

- Every spawned `claude` child, scheduled or interactive, runs with `CLAUDE_CONFIG_DIR` pointed at a dedicated agent
  config dir: the configured `[daemon].claude_config_dir` (`~`/`$VAR` expanded) when set, else
  `<config dir>/claude-agent-home`. `HOME` stays inherited so git and ssh keep working; only the user-level
  `CLAUDE.md`, `.credentials.json` and `settings.json` are relocated, and (unlike `--bare`) MCP servers, hooks and
  skills stay enabled.
- The dir is seeded before any `claude` child is spawned. `<dir>/CLAUDE.md` is overwritten on every start from the
  binary's embedded copy of `deploy/bot-CLAUDE.md`, so a bot-file update ships with the binary with no separate sync
  step.
- On Linux, `<dir>/.credentials.json` is symlinked to `$HOME/.claude/.credentials.json` when that host file exists, so
  an OAuth refresh the child writes persists back to the shared host login instead of silently diverging in a copy.
  Three cases leave the agent dir's credentials file untouched, none of them an error: no host credentials file at
  all (logged as a `warn`, since the agent then has no credential until one is supplied), a real (non-symlink) file
  already present in the agent dir (logged as a `warn`, since it is left in place rather than replaced), and an agent
  config dir that resolves (after symlinks) to the same directory as `$HOME/.claude` (nothing to report; source and
  target are one path). On macOS this step does not run; credentials live in the Keychain instead.

### 8.2 Provider abstraction and backend resolution (`SPEC-8.2`)

- Two traits capture every operation a native handler needs: `IssueTracker` (list queued issues, read/write a custom
  field, set state/assignee, comment, inspect, resolve/map VCS, create/attach/link/tag an issue, verify auth, run a
  bounded query, read a project) and `Forge` (open or adopt a PR, mark it ready, refresh its text, checkpoint-push,
  resolve a clone URL, probe repo access, read identity/permission/CI state/mergeability, comment on a PR or issue,
  merge or update a PR branch, add labels). A `Backend` is both at once.
- `resolve`/`resolve_runner` walk an ordered `BackendKind` priority list and return the first backend whose `probe()`
  succeeds, erroring with every attempt's own failure when none is usable. The YouTrack/Forgejo path is `[Cli]`
  (CLI-first, CLI-only for this phase); the GitHub `Forge` prefers the hosted MCP endpoint, falling back to the `gh`
  CLI (`[Mcp, Gh]`).
- `CliBackend` implements both traits over `yt`/`fj`; `GithubForge` implements `Forge` (and the subset of
  `IssueTracker` that makes sense, each method erroring "not supported") over `gh`/MCP/git. A job whose repository
  host is `github.com` or listed in `github.hosts` uses `GithubForge`; any other host uses `CliBackend` (also see
  SPEC-3.3.1's Targets bullet).
- A CI-failure webhook read (log fetch, run list/timing, workflow file) routes per-call by host (`github.com` or a
  configured host uses the `gh`-backed reader, everything else the `fj`-backed one), not by a per-source config
  lookup, so one reader pair serves every configured repository (see SPEC-3.5).

### 8.3 Audited tracker writes (`SPEC-8.3`)

- Every YouTrack write a run makes goes through one run-scoped wrapper, `AuditedTracker`, built from an
  `IssueWriteAudit` and wrapped around whichever `IssueTracker` the run resolved (see
  [convention](conventions/actions.md#audited-youtrack-writes-claude-234)).
- `create_issue` spends one slot of a per-run ceiling (`limits.max_issues_per_run`, default 25) BEFORE the tracker is
  invoked, and refuses with a typed error past the ceiling; a claimed slot is never released, even when the create
  itself then fails. Reaching the ceiling logs a `warn` and leaves an operator-visible notice the caller persists as
  a `run_events` row, so a truncated filing run never reads as clean.
- Every create and every update is journaled with the run id (or "none" outside a run) and the issue key, win or
  lose, so an unexpected issue resolves to a run in one query. Reads pass straight through uncounted and unlogged.
- The ledger is per RUN, not per backend: several backends serving one run (the youtrack action builds one per job)
  count against a single shared ceiling. `IssueTracker` exposes no delete, so no write path through this wrapper can
  issue one.

### 8.4 Forge and tracker read retries; writes never retry (`SPEC-8.4`)

- Every read-only forge/tracker call (`fj`'s `repo view`/`pr search`/`pr status`/`whoami`/`repo collaborator
  ... permission` and the CI-failure log/run/file reads; `gh`'s equivalents) is retried up to four attempts total on
  a non-zero exit, never by classifying `stderr` wording, and never on a spawn failure (the binary is simply absent).
  The schedule is `[0s, 2s, 5s]` between attempts 2-4 (worst case 7s added delay); a failed non-final attempt logs a
  `warn` naming the command, attempt number and a redacted stderr tail, a recovery logs `info`, and a call that fails
  every attempt returns the last attempt's error unchanged.
- Every forge WRITE (`pr create`, `pr merge`, `pr update`, PR/issue comments, label edits) is excluded on purpose:
  retrying a write risks a duplicate comment, PR, merge or branch update. The one deliberate exception is the git
  `push` network call (SPEC-8.5), which is idempotent in a way an `fj`/`gh` write is not. The AGit `fj pr create -aA`
  path is excluded from every retry, since it pushes and opens a PR in one write.
- See [convention](conventions/forge.md#read-only-fj-retries-claude-366) for the full per-call-site mapping.

### 8.5 Git network retries and per-invocation authentication (`SPEC-8.5`)

- Every git network call the runner makes (clone, submodule update, the pinned-branch default-ref restore's
  `ls-remote`/`fetch`, a feature branch's remote-sha read/fetch/push, and the comment-webhook's push) is retried up
  to four attempts total on the same `[0s, 2s, 5s]` schedule as SPEC-8.4, retried only on a completed process with a
  non-zero exit. `push` is the one WRITE that retries here: pushing a fixed commit to a fixed ref is idempotent, so a
  retry landing after an earlier attempt already succeeded exits 0 ("Everything up-to-date") rather than duplicating
  anything, and no site ever adds `--force` or a force-equivalent refspec. A clone retry removes whatever a failed
  attempt left at the target directory first, since `git clone` refuses a non-empty directory.
- Every git subprocess authenticates per invocation through an inline credential helper built from the resolved
  clone URL's userinfo (`-c credential.helper=` to clear any inherited helper, then the inline helper), never a
  token in argv or `.git/config`; a URL with no userinfo runs unauthenticated so a host-level credential helper still
  applies. A fresh clone's remote URLs (origin, every `submodule.*.url`, and each submodule's own `origin`,
  recursively) are stripped of userinfo immediately after cloning, so a token is never left sitting in a workdir's
  git config; a scrub failure fails the clone outright rather than leaving a half-scrubbed tree behind a warning.
- A shallow clone pins `--single-branch --branch <branch>` when given; a pinned clone's missing `origin/HEAD` (and
  missing default-branch ref) is restored right after by resolving the remote's real default branch via
  `ls-remote --symref` and fetching it, never assumed, so a base-branch lookup against a pinned-branch workdir
  resolves exactly as it would against a default-branch clone.

### 8.6 Git failure reporting and secret redaction (`SPEC-8.6`)

- A failed git subprocess reports the exit code plus a bounded, redacted tail of its stderr (the last few non-empty
  lines, skipping the `Cloning into ...` progress banner) instead of a bare exit code, so a missing credential, an
  unknown host, a deleted branch, a rejected non-fast-forward and a protected branch are told apart. A redacted,
  bounded tail of stdout is appended too, since some git subcommands write their diagnostic text there instead of
  stderr.
- Redaction (`forge_log::redact`) masks a clone URL's userinfo verbatim (passed as a known secret) on top of a
  shape-based scan for well-known token prefixes (`ghp_`, `glpat-`, `xoxb-`, ...) and for a value following an auth
  keyword (`Bearer`/`token`/`Basic`) above a minimum length, so ordinary prose is never masked. The redacted string
  (never the raw URL) is what reaches `run_events` and, from there, a YouTrack comment.
- The same `redact` function serves the CI-failure log fetch (SPEC-3.5) and every git-failure path here, so a secret
  pattern recognized in one place is masked in both.

### 8.7 PR subject and body integrity (`SPEC-8.7`)

- `validate_pr_subject` rejects an empty subject or one starting with `#` (a template heading like `## Summary`, or
  a bare issue reference, mistaken for the commit's one-line subject) before every push or PR-create call on every
  path (branch, AGit, GitHub), naming the branch and the offending subject; a bad tip therefore opens no PR at all.
- A commit body intended as a PR description is reflowed before it is used as one: a hard-wrapped paragraph is
  rejoined onto one line (preserving fenced code blocks, headings, list items, block quotes and trailer lines as
  their own lines), mirroring the join the runner's own posting path performs, so a PR body built from a commit
  written with hard line breaks reads the same as one written as single long lines.
- `refresh_pr_text` re-derives a PR's title and body from its branch's current tip (running the same subject
  validation first) and overwrites the forge-side PR, used when a resumed run replaces a timeout-salvage
  placeholder (CLAUDE-398) with the finished tip's own text; it is a no-op on a PR that is not a draft.
- `bare_issue_key` reads the bare `#<KEY>-N` reference every runner-made commit body ends with out of arbitrary
  text, for a push-comment call site with no YouTrack job key passed in directly.

### 8.8 Push comments on every PR-branch write (`SPEC-8.8`)

- Every runner-owned write to a PR's branch (the initial push, a resumed-run push, a push onto a diverged or merged
  remote branch, a timeout-salvage push, a checkpoint push, the comment-webhook fix push, and the automerge
  behind-base branch update) posts one PR comment naming the event, the issue key, the PR's resulting draft/ready
  state, the old/new SHA range, and the pushed commits' subjects (capped at 20, with the remaining count named
  beyond that), ending with the SPEC-3.9 run footer. A write with no local clone to read commit subjects from (the
  automerge branch update) says so explicitly rather than omitting the line.
- Posting is best-effort and never retried (forge writes do not retry, SPEC-8.4): a failed post is logged at `error`
  and returned as a line the caller folds into whichever outcome comment it already owns, never a second, separate
  comment; the push or branch update it reports on is already done and is never undone, failed, or rolled back
  because the comment failed.

### 8.9 Forge preflight access check (`SPEC-8.9`)

- `Forge::repo_access` verifies the configured identity can read AND work with pull requests on a repository before
  any clone or Claude session is paid for, covering both capability categories the PR path needs (a repository read
  and a pull-request read) since a token scoped to only one passes a repository-only probe and then fails at the
  first PR-related call. `identity` names which account is authenticated, read-only, never returning the token.
- A denied probe is rendered naming which call failed, the exit status, and the scope or permission grant an
  operator needs to add, distinguishing an HTTP 403 (visible but refused: a scope or unit-permission gap) from an
  HTTP 404 (not visible at all: a missing grant) from a status-less failure (most likely transient or the same gap
  as the 403 case), since `fj` and `gh` phrase these differently in their own stderr.

### 8.10 Open-PR lookup degradation (`SPEC-8.10`)

- `create_pr` looks up an already-open PR for the branch before creating one, on both backends, so a re-queued or
  resumed run adopts the existing PR instead of a second one failing to open. A failed lookup degrades to "assume no
  open PR" (a `warn` plus an operator-visible notice) rather than discarding the run; a create then rejected because
  a PR for that head already exists retries the lookup exactly once and adopts the result, or names both the lookup
  and create failures if the retry also fails. See
  [convention](conventions/forge.md#open-pr-lookup-degradation-claude-206-claude-469).
- A single-branch shallow clone's `remote.origin.fetch` refspec covers only the branch it was pinned to
  (SPEC-8.5); a divergence check that merges in the remote tip of the job's OWN feature branch therefore fetches it
  with an explicit destination tracking ref (`refs/heads/<branch>:refs/remotes/origin/<branch>`, no leading `+`,
  since the ref does not exist yet in a fresh workdir) rather than a bare `git fetch origin <branch>`, which would
  write `FETCH_HEAD` only and leave `origin/<branch>` unresolvable to the merge that follows.

## 9. Deployment and operations

- Image (`oci-build/Dockerfile`): multi-stage. The builder compiles a static musl binary with a locked dependency
  build. The runtime stage is a Linux image carrying `claude`, `yt`, `fj`, `gh`, `nu`, `just`, Docker CLI, git and Rust
  tooling, plus `gosu` and `catatonit`. Entrypoint: `catatonit -- gosu-entrypoint.sh claude-run`, default command
  `serve --no-socket`. The image starts as root and drops to uid/gid 1001 through the entrypoint.
- The runtime stage smoke-checks the base image's toolchain at build time (a version/help invocation of every CLI the
  image is expected to carry); a base image that lost a tool fails the image build there instead of failing the first
  job that needs it.
- `gosu` has no openSUSE Leap package: the runtime stage downloads the upstream per-architecture release binary and
  verifies it against that release's published SHA256SUMS before installing it.
- Entrypoint hooks seed `config.toml` and the shipped action files into an empty config volume and chown the data
  directory. See 9.1 for the entrypoint's own root/non-root and UID/GID behavior.
- Image environment: `CLAUDE_RUN_CONFIG_DIR=/config`, `CLAUDE_RUN_DATA_DIR=/data`, `CLAUDE_RUN_STATE_DIR=/state`,
  `CLAUDE_RUN_RUNTIME_DIR=/run/claude-run`, `CLAUDE_CONFIG_DIR=/config/claude-agent-home`. The agent's `CLAUDE.md` is baked
  from `deploy/bot-CLAUDE.md`.
- Seeded container config binds the web UI to `0.0.0.0:7321`. The MCP listener is off; when enabled it binds
  `0.0.0.0:7322`.
- `compose.yml`: the app service with three named volumes (`/config`, `/data`, `/state`), a tmpfs at `/run/claude-run`, a
  privileged Docker-in-Docker sidecar sharing the app's network namespace (daemon on `tcp://127.0.0.1:2375`, healthcheck
  `nc -z`), and an optional PostgreSQL service behind the `postgres` profile (healthcheck `pg_isready`). No host bind
  mount is used. No port is published by default.
- `compose.yml` passes the same `RUNS_ON_*` image references the CI workflows resolve through `${{ vars.RUNS_ON_* }}`
  into the app service as environment variables, so a `claude` session the daemon spawns (running `just check-docker` or
  `just pre-commit` against the Docker-in-Docker sidecar) builds against the identical images CI uses instead of a
  guessed tag.
- The app service defines no container healthcheck. Liveness is available from `GET /api/status` and the `status`
  command.
- Shipped action files: `youtrack.yml` (interval 5m), `audit-run.yml` (interval 5m), `closed-issue-summary.yml` (interval
  24h). `examples/release-notes.yml` (weekly, gated in its input) is an example and is not seeded into a new install.
- The shared Cargo target directories (`SPEC-3.10`) live under the `/data` volume, which is disk-backed.
- Restart safety: `GET /api/status` and the Status page expose a `quiet` flag that is true only when no run is open and
  no run is dispatched.

### 9.1 Entrypoint and UID/GID remap (`SPEC-9.1`)

`oci-build/gosu-entrypoint.sh` is the shared, byte-identical entrypoint; image-specific root steps run as hooks under
`/etc/entrypoint.d/` instead of being added to this file.

- Started as a non-root user (for example a compose `user:` pin): the remap, hooks and chown below are all skipped. Any
  `PUID`/`PGID` given must match the uid/gid the container was actually started as, or the entrypoint refuses to start.
  Every path named in `DATA_DIRS` must already exist and be writable by that uid/gid, or the entrypoint refuses to
  start naming the unwritable path; it then execs the command directly.
- Started as root: `PUID`/`PGID` resolve to, in order, an explicit non-zero environment value, else the owner of the
  first `DATA_DIRS` entry when that owner is not root, else 1001/1001. A non-numeric or zero `PUID`/`PGID` refuses to
  start rather than run the workload as root.
- The resolved uid/gid is applied to the image's `APP_USER` (`usermod`/`groupmod -o`, allowing an id already present in
  the image), the root hooks under `/etc/entrypoint.d/` run in filename order and a non-zero exit from any hook aborts
  startup, then every `DATA_DIRS` entry is chowned recursively to the resolved uid/gid, skipping paths that already
  match. The command then execs under `gosu "$APP_USER" "$@"` (the user form, not `uid:gid`, so supplementary groups
  come from `/etc/group`).
- `CLAUDE_RUN_DATA_DIR` (`/data`) is deliberately not a member of `DATA_DIRS`: it holds `workdirs/`, a tree of job git
  clones the runtime user populates itself, which a recursive chown on every boot would walk. It gets its own shallow
  chown from the `20-data-dir.sh` hook instead: the directory itself, every entry directly under it except `workdirs`
  (chowned recursively), and the `workdirs` directory entry itself (not its contents).

### 9.2 Image publish trains (`SPEC-9.2`)

The CI image-publish workflow resolves a single publish train from its own trigger (`oci-build/get-tags.nu`), never
from `git describe`, because a release commit is simultaneously a push to `main` and a `v*` tag push and both workflow
runs fire:

- `release` train (an exact `v*` tag push): publishes only the immutable `<version>` artifact.
- `latest` train (a push to `main`): publishes only the rolling `latest` artifact.
- `dry-run` (a `workflow_dispatch`): builds and prints without mutating the registry; it resolves to the `release` train
  when given a `--simulate-tag` version to exercise, else to the `latest` train.

The two real trains publish disjoint tag sets, so the branch-push run and the tag-push run of the same release commit
never write the same registry destination.

## 10. Non-functional behavior

- Logging: JSON lines through `tracing`. Default filter `info,claude_run=info,sqlx=warn`. Destination is stdout, except
  `mcp stdio` which logs to stderr so stdout carries only protocol frames. `serve --detach` redirects both to
  `<state dir>/logs/daemon.log`. The first line of `serve` is `Server started` with version and pid. The container
  image runs its default command foregrounded (`serve --no-socket`, no `--detach`), so its logs are its stdout, left
  for the container runtime's own log driver to capture rather than redirected to a file. Two argv pre-scans run
  before clap parses the real CLI (`mcp stdio` detection for the trace writer, and `--color`): clap colors its own
  help/usage/error output as part of parsing itself, so the color choice must be known before that call, not read
  back from its result.
- Diagnostics: warnings and errors that concern a run or delivery are also stored in `run_events` and shown on `/logs`.
- Error handling: invalid action YAML, unknown config keys and unsupported database schemes fail with a message naming the
  value. API errors carry a JSON message and a non-2xx status for conflicts, unknown names and unavailable queues.
- Database connection to PostgreSQL retries for a bounded deadline, then fails naming the host.
- Concurrency: a global permit cap plus per-action caps, each floored to 1 even when configured as 0; a key gate
  serializes jobs that share a concurrency key. Changing a cap at runtime (global resize, or an action's
  `max_parallel` edited on disk) swaps in a new semaphore sized to the new cap, and permits already held continue
  draining against the old one, so the new size is only exactly enforced once those in-flight jobs finish. No
  fairness or ordering is guaranteed among jobs waiting on the same key or a saturated cap: a deferred job is simply
  left queued for the next pickup, and which deferred job wins the key or a permit next is undetermined.
- Blocking: every synchronous subprocess exec or blocking sleep reached from async code runs through a
  `block_in_place` wrapper, so a long-running child (`yt`/`fj`/`git`/`claude`/`nu`, or a poll loop) never pins a
  multi-thread tokio worker or holds a pooled DB connection unpollable for its duration; the wrapper falls through to
  a direct call under a current-thread runtime (e.g. `#[tokio::test]`), which `block_in_place` would otherwise panic
  on.
- Timeouts: per-job `claude` wall clock (default 600 seconds), review session (1800), CI-failure analysis (600).
- Time rendering: stored UTC timestamps are rendered through the `preferences` timezone, locale and format.
- Storage latency (`SPEC-10.1`):
  - **SPEC-10.1.1**: Storage with the latency and throughput of spinning disks is a supported environment, including a
    volume that mixes SSD, HDD and NVMe devices (for example a RAID logical volume). Slower disk I/O MUST make an
    operation take longer and MUST NOT make it fail or give a wrong result.
  - **SPEC-10.1.2**: A deadline, pool acquire timeout or retry budget on an operation backed by disk I/O (database
    access, git, builds, log and state writes) MUST be sized to catch a stalled operation, not an expected-slow one, and
    MUST NOT be the only thing standing between a slow disk and a failure.
  - **SPEC-10.1.3**: The unit-test suite, on SQLite and on PostgreSQL, MUST pass on such storage without a test-specific
    timeout increase, retry or skip. A test that fails only when disk I/O is slow is a defect in the code or the fixture.
  - **SPEC-10.1.4**: A timeout, pool exhaustion or similar failure caused by slow disk I/O MUST be reported as that
    failure with the operation and its elapsed time, and MUST NOT be retried silently or recorded as success.

## 11. CI

- **SPEC-11.1**: The PostgreSQL pass of the unit-test job in `.forgejo/workflows/check.yml` MUST run through
  `claude-run test-postgres` (`SPEC-6.6`) and MUST NOT use a `postgres` service container: a service's `options`
  cannot carry the server flags that disable durability, and a durable container makes the pass depend on disk flush
  latency (`SPEC-10.1.3`). With no service there is no health check, so the `CI_POSTGRES_HEALTH_*` variables
  (`common` `SPEC-13.1`) do not apply to this job.
