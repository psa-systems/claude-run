# Action YAML Schema

`scripts/claude-run.nu` is a generic runner. Behavior is defined by an action
YAML file passed as the only argument:

```nu
claude-run.nu /path/to/action.yml
# or, if the action lives in $CLAUDE_RUN_ACTIONS_DIR:
claude-run.nu youtrack
```

Each job produced by `input.command` becomes one Claude invocation. The runner
provides locking, daily caps, per-job attempt caps, timeouts, and a cost-based
usage check. The action YAML supplies everything specific to the job.

## File shape

```yaml
name: <kebab-id>            # required
kind: scripted              # optional discriminator: scripted (default) | youtrack
workdir_base: /path/to/dir  # optional, overrides $CLAUDE_RUN_WORKDIR_BASE for this action
input:
  command: |                # required, nu snippet, stdout = JSON array
    #!/usr/bin/env nu
    ...
dedup_key: <field>          # optional, job field used to track attempts
filter: |                   # optional, nu snippet; exit non-zero to skip job
  ...
pre:                        # optional, ordered list of nu snippets
  - |
    ...
prompt: |                   # required, nu snippet that returns the prompt string
  ...
claude:                     # optional, all keys optional
  cwd: ...
  allowed_tools: "Read,Edit,Write,Bash,Glob,Grep"
  permission_mode: acceptEdits
  output_format: json
post:                       # optional, ordered list of nu snippets
  - |
    ...
output:
  file: <name>.log          # optional, appended JSONL of per-job outcomes
limits:                     # optional per-action overrides of env defaults
  max_attempts_per_key: 3
  max_runs_per_day: 20
  claude_timeout_seconds: 600
  daily_budget_usd: 5.00
concurrency:                # optional; default is strictly sequential
  max_parallel: 1           # distinct-key jobs in flight at once (default 1)
  key: [project, repo, workflow]   # job fields composing the concurrency key
schedule:                   # optional; omit for trigger-only (manual)
  kind: interval            # interval | cron | manual
  spec: 30m                 # interval/cron spec; omit for manual
```

## Script fields: `run` / `shell`

The script-bearing fields (`input.command`, `filter`, each `pre`, `prompt`,
each `post`) each accept two forms:

- a plain YAML string holding the code (implicit `shell: nu`), or
- a `{run:, shell:}` mapping, following Docker Compose's `run` / `shell`
  convention: `run` is the code, `shell` is the interpreter.

`shell` defaults to `nu` when omitted, so existing string-form actions keep
working unchanged. `shell` is a bare binary name resolved on `PATH` (no
arguments); e.g. `nu`, `bash`, `sh`.

```yaml
filter: exit 0                 # string form, runs with nu

prompt:                        # mapping form
  run: |
    print $"Work ($job.id)"
  shell: nu

pre:
  - run: |                     # this step runs with bash
      set -euo pipefail
      echo "$job" | jq -r .id
    shell: bash
```

**Bindings across interpreters.** For `nu` (the default), bindings (`$job`,
`$workdir`, `$claude_result`) are loaded by a generated prelude, exactly as
before. For any other interpreter the prelude does not apply, so each binding
is exposed instead as an environment variable of the same name holding the
compact-JSON value (decode with `jq`): in a `bash` snippet, `$job` is the
JSON string `{"id":"..."}`.

## Field reference

### `name` (string, required)

Identifier used for the lock file, state file, log directory, and PAUSED flag.
Should be kebab-case and unique across all actions on the host.

### `kind` (enum, optional, default `scripted`)

The dispatch discriminator (CLAUDE-30). Selects how the runner executes the
action:

- `scripted` (default): runs the generic nu-snippet pipeline
  (`input.command` -> `filter` -> `pre` -> `prompt` -> `claude` -> `post`)
  described in the rest of this document. **Deprecated**: it is kept for
  back-compat, but new actions should prefer a native kind once one exists for
  their source.
- `youtrack`: a built-in Rust handler for YouTrack. The script-bearing fields
  (`input.command`, `prompt`, `filter`, `pre`, `post`) are not used; the action
  instead supplies a typed [`native`](#native-record-required-for-native-kinds)
  block.

`scripted` and native kinds share all the run accounting (locking, daily caps,
per-job attempt caps, budget, job-outcome records); only job production and the
per-job pipeline differ.

### `native` (record, required for native kinds)

Typed configuration for a native `kind`. Required when `kind` names a native
handler, validated against that handler. Because `kind` is already the
discriminator, the block holds the handler's config directly (no nested variant
tag). Omit `native` entirely for `kind: scripted`.

#### `kind: youtrack`

Two sub-records, `youtrack` (the issue source) and `forgejo` (the PR target).
Every field defaults, so a bare `native: {}` validates and reproduces the
behavior the scripted youtrack action had. The generic blocks (`workdir_base`,
`link_template`, `limits`, `schedule`, `claude`, `output`) stay at the top level
of the action, not inside `native`.

```yaml
kind: youtrack
native:
  youtrack:
    base_url: https://yt.example     # optional; default uses host-configured yt
    token: <token>                   # optional; inline plaintext (CLAUDE-26 encrypts)
    query: "-Resolved AI Agent: Queued"   # issues to pick up
    custom_field: "AI Agent"         # field holding the state machine
    states:                          # AI Agent / State labels the handler drives
      queued: Queued
      working: Working
      code_review: Code Review
      needs_review: Needs Review
      failed: Failed
      in_progress: In Progress       # the standard State field value set in pre
  forgejo:
    base_url: https://forge.example  # optional; default derives host per issue VCS
    token: <token>                   # optional; inline plaintext (CLAUDE-26 encrypts)
```

| Key                        | Default                      | Meaning                                                      |
|----------------------------|------------------------------|--------------------------------------------------------------|
| `youtrack.base_url`        | host-configured `yt`         | YouTrack base URL. Consumed once tokens move to config (CLAUDE-26). |
| `youtrack.token`           | none                         | API token, inline plaintext for now (CLAUDE-26 encrypts).    |
| `youtrack.query`           | `-Resolved AI Agent: Queued` | Issue query selecting the jobs this action picks up.         |
| `youtrack.custom_field`    | `AI Agent`                   | Custom field holding the AI Agent state machine.             |
| `youtrack.states.*`        | see above                    | Labels the handler reads/writes for each AI Agent / State value. |
| `forgejo.base_url`         | per-issue VCS host           | Forgejo base URL. Consumed once tokens move to config (CLAUDE-26). |
| `forgejo.token`            | none                         | API token, inline plaintext for now (CLAUDE-26 encrypts).    |

`base_url` / `token` are placeholders for CLAUDE-26 (encrypted secrets); the
CLI backend uses host-configured `yt` / `fj` credentials today and ignores them.

### `workdir_base` (string, optional)

Per-job working directories are rooted under `<workdir_base>/<name>/<key>`.
Defaults to `$CLAUDE_RUN_WORKDIR_BASE` (which itself defaults to
`/tmp/claude-run`). Setting this on the action overrides the env for that
action only - useful when one action needs more disk than `/tmp` allows
(e.g., a workspace mounted on a large data volume) or when you want
per-action isolation on disk for cleanup or quota reasons.

**Resolution rules** (apply to both the YAML value and the env value):

- If the value starts with `$`, it is evaluated as a nu expression and the
  result is used. Lets you do things like:

  ```yaml
  workdir_base: $nu.home-dir | path join workspace
  workdir_base: $"($env.HOME)/workspace"
  workdir_base: $"($env.XDG_DATA_HOME)/claude-run"
  ```

- Otherwise, the value is treated as a literal path. `path expand` is applied
  in either case, so `~` and `$env` references that `path expand` understands
  still work for literal values.

```yaml
workdir_base: ~/workspace                          # literal, ~ expanded
workdir_base: /mnt/data/claude-run                 # literal, absolute
workdir_base: $nu.home-dir | path join workspace   # evaluated
```

### `input.command` (nu snippet, required for `kind: scripted`)

A Nushell snippet executed by the runner with `nu --commands "$snippet"`. Must
write a JSON array to stdout. Each element of the array becomes one Claude
invocation, bound to `$job` for downstream snippets.

The snippet inherits the runner's environment. Use host-configured CLIs
(`yt`, `fj`, `git`, etc.) directly; the runner does not parse or inject
secrets.

If the snippet exits non-zero, or stdout is not a JSON array, the run aborts
with exit code 5.

### `dedup_key` (string, optional)

The name of a field on each job whose value uniquely identifies the unit of
work. The runner tracks attempts keyed on this value and skips a job whose
attempt count has reached `max_attempts_per_key`.

Omit to disable per-job attempt tracking (every run processes every job).

### `filter` (nu snippet, optional)

Run once per job, before any other per-job work. The job JSON is piped to
stdin and bound as `$job` in the snippet. Exit non-zero to skip the job
without counting an attempt. Exit zero to proceed.

### `pre` (list of nu snippets, optional)

Ordered steps executed before the Claude invocation. Each step is a separate
nu snippet with `$job`, `$workdir`, and `$env` in scope. Steps share the
runner's working directory unless they `cd` themselves.

If a step exits non-zero, the runner logs the failure, **counts the attempt**,
skips remaining pre steps, skips the prompt + claude invocation, and **still
runs post steps** so the action can roll back any side effects an earlier
pre step had (e.g. resetting an external state field that pre step 0 set).
Post receives a synthesized `$claude_result` with `ok: false`, an `error`
field naming the failed pre step, and the step's stderr as `summary`.
Poison jobs eventually hit the per-key attempt cap and stop being retried.

### `prompt` (nu snippet, required for `kind: scripted`)

A nu snippet whose return value is the prompt string passed to Claude. Has
`$job`, `$workdir`, and `$env` in scope. Plain text without `$` interpolation
is fine; nu treats it as a string literal.

```yaml
prompt: |
  $"Work YouTrack issue ($job.id): ($job.summary)"
```

### `claude` (record, optional)

Tuning for the `claude` CLI call. All keys are optional.

| Key               | Default                                  | Meaning                                                   |
|-------------------|------------------------------------------|-----------------------------------------------------------|
| `cwd`             | per-job `$workdir`                       | Working directory for the claude process. Plain string.   |
| `allowed_tools`   | `Read,Edit,Write,Bash,Glob,Grep`         | Passed as `--allowed-tools`.                              |
| `permission_mode` | `acceptEdits`                            | Passed as `--permission-mode`.                            |

The runner always passes `--print --verbose --output-format stream-json`. It
wraps the call in `timeout $claude_timeout_seconds`. Stdout+stderr are
redirected to a per-job NDJSON stream log:

```
$CLAUDE_RUN_LOG_DIR/<name>/streams/<key>-attempt-<n>.jsonl
```

`tail -f` that file from another terminal to watch Claude work in real time.
The final `result` event on the last line is parsed for cost (`total_cost_usd`)
and outcome (`result`, `is_error`) once claude exits.

### `post` (list of nu snippets, optional)

Ordered steps that run after every code path that reached the workdir:
Claude success, Claude non-transient failure, prompt-snippet failure,
empty-prompt, and pre-step failure all run post. The single exception is
transient Claude errors (rate limit, auth) - those skip post and return
early so the job can be retried cleanly on the next firing.

When a run is cancelled while this job is in flight, its claude child is
terminated and post still runs, with a synthesized cancelled `$claude_result`
(`ok: false`, `error: "run cancelled by operator"`, `cost: 0`), so end-of-job
cleanup (resetting an external state field, etc.) happens. A cancelled job does
not count an attempt, and jobs not yet started are skipped without running post.

This makes post the right place for end-of-job cleanup that must happen
unconditionally - resetting an external state field, posting a status
comment, releasing a soft lock, etc.

Each step has `$job`, `$workdir`, `$env`, and `$claude_result` in scope.
On pre/prompt failure `$claude_result` is synthesized (`ok: false`,
`error` names the failing phase, `summary` carries the step stderr) so
post sees a uniform shape regardless of which upstream step failed.
The action's post step is expected to branch on `$claude_result.ok` to do
success vs failure cleanup.

`$claude_result` shape:

```
{
  ok: bool,               # false if claude exited non-zero (timeout, error)
  summary: string,        # claude's final message on success, error text or
                          #   stream-log tail on failure
  cost: float,            # total_cost_usd from the result event (0 if claude
                          #   never got that far)
  raw: record,            # the parsed final "result" NDJSON event, or {} on
                          #   failure before the result event was emitted
  error: string,          # set on failure, empty on success
  transient: bool,        # always false at this point - transient is handled
                          #   before post runs
}
```

Same step-failure handling as `pre`: if a post step exits non-zero, the
runner logs the failure, **counts the attempt**, and stops running
subsequent post steps. The job's overall outcome record (the "done" line in
the per-action JSONL log) has `ok: false` if either claude or any post step
failed.

#### Skip-attempt marker (parked / blocker outcomes)

By default every job processed past pre counts toward `max_attempts_per_key`.
A post step can opt this job out of attempt counting by writing a sentinel
file:

```nu
touch $"($workdir)/.claude-run-skip-attempt"
```

The runner checks for that file after the post loop. If present, the
per-key attempt counter is **not** incremented. Daily Claude-invocation
count still increments when claude actually ran (real budget was spent).

Use this for outcomes where the job is **parked pending external input**,
not failed:

- The work depends on information that has to come from a human.
- The work depends on another issue that has to land first.
- The work was deliberately deferred (out of scope, wrong priority).

Do **not** use it for hard failures (timeout, exception, claude can't even
investigate). Those should consume an attempt slot so poison jobs eventually
stop being retried.

The `done` outcome record gains an `attempt_counted: bool` field reflecting
the runner's decision, so downstream tooling can see at a glance which jobs
were parked vs failed.

#### Needs-review marker (agent-signalled blocker)

A complement to the skip-attempt marker, used by the `youtrack` action so the
agent can signal a blocker without writing to the external system itself. When
the claude agent decides it cannot safely proceed, it makes no code change and
writes the human-facing reason to a second sentinel:

```nu
"Need decision X from a human." | save $"($workdir)/.claude-run-needs-review"
touch $"($workdir)/.claude-run-skip-attempt"
```

The action's finalize post step then reads this marker after claude exits: if
`.claude-run-needs-review` exists it parks the job (ensuring the skip-attempt
marker is present), posts the file's contents verbatim as the issue comment, and
sets the status field to the project's "needs human input" value, creating no
PR. This keeps every external mutation in one post step gated on the run
outcome, rather than having the agent mutate the external system mid-run. The
marker is a plain convention between an action's prompt and its post step; the
runner itself only special-cases `.claude-run-skip-attempt`.

### `output.file` (string, optional)

If set, the runner appends one JSON line per job to
`$CLAUDE_RUN_LOG_DIR/<name>/<output.file>` summarizing the outcome (job key,
attempt number, claude cost, push/comment results from post steps, etc.).

If omitted, per-job outcomes are only visible in the daily JSONL log.

### `limits` (record, optional)

Per-action overrides for the runner-wide defaults. Any subset of:

| Key                        | Env default                          |
|----------------------------|--------------------------------------|
| `max_attempts_per_key`     | `$env.CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY` |
| `max_runs_per_day`         | `$env.CLAUDE_RUN_MAX_RUNS_PER_DAY`     |
| `claude_timeout_seconds`   | `$env.CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS` |
| `daily_budget_usd`         | `$env.CLAUDE_RUN_DAILY_BUDGET_USD`     |

### `concurrency` (record, optional)

Controls keyed parallel execution of a run's jobs. By default a run processes
its jobs strictly one at a time. With `concurrency` you can run independent jobs
in parallel while guaranteeing that two jobs touching the same unit of work
never run at once.

```yaml
concurrency:
  max_parallel: 4                  # up to 4 distinct-key jobs at a time
  key: [project, repo, workflow]   # default; the per-job concurrency key
```

| Key            | Default                      | Meaning                                                                 |
|----------------|------------------------------|-------------------------------------------------------------------------|
| `max_parallel` | `1`                          | Maximum distinct-key jobs in flight at once within a run.                |
| `key`          | `[project, repo, workflow]`  | Job fields whose values compose the concurrency key.                     |

Semantics:

- **Same key serializes.** At most one job per resolved key runs at a time, so
  two jobs against the same repo never collide on the clone dir, branch, or PR.
  Per-key concurrency is fixed at 1 and is not configurable.
- **Distinct keys parallelize**, up to `max_parallel`. `max_parallel: 1` (the
  default) reproduces the original strictly-sequential behavior exactly.
- **Key resolution.** For a scripted action the key is built from the listed
  fields on each job's JSON object; for the native `youtrack` action the handler
  maps `project` to the issue-id prefix and `repo` to the resolved repo path.
  `workflow` is forward-looking (reserved for a future Forgejo-failed-workflow
  action) and is unset on YouTrack jobs today. When none of the configured
  fields resolve, the job's own key is used so it is treated as independent.
- **Daemon-wide gate.** The key gate is held at the daemon level, so two
  executions sharing a key never overlap even across a scheduled run and a
  manual trigger. (It is in-memory per process; a standalone `claude-run run`
  uses its own gate but the per-action lock still prevents it from overlapping a
  daemon-driven run of the same action.)
- **Caps stay best-effort.** The daily cap and budget are checked before each
  job launches; because up to `max_parallel` jobs can pass the check before any
  of them advances the ledger, the cap may overshoot by at most
  `max_parallel - 1` jobs.
- **`max_parallel` is bounded by available worker threads.** Each running job
  occupies a thread for the duration of its `claude` invocation, so set
  `max_parallel` no higher than the host has cores to spare.

### `schedule` (record, optional)

Controls when the daemon auto-fires the action. A record with a required
`kind` (one of `interval`, `cron`, `manual`) and an optional `spec`:

```yaml
schedule:
  kind: interval
  spec: 30m
```

Only the daemon (`claude-run serve`) acts on `schedule:`; a bare
`claude-run run <action>` ignores it and processes the action once
immediately.

#### `kind: interval`

`spec` is a [humantime](https://docs.rs/humantime) duration string
(`90s`, `30m`, `2h`, `1h30m`). The action fires every N measured from the
**last run's `started_at`**, not from a fixed wall-clock grid. An action
that has never run fires on the next scheduler tick.

```yaml
schedule:
  kind: interval
  spec: 2h          # fire ~2h after each run starts
```

#### `kind: cron`

`spec` is a cron expression: either the common **5-field** form
(`minute hour day-of-month month day-of-week`) or a **6-field**
seconds-precision form (`second minute hour dom month dow`). A 5-field
expression is internally prefixed with `0 `, so it fires on the 0th second
of the matching minute. The action fires at the next instant matching the
expression after the last run.

```yaml
schedule:
  kind: cron
  spec: "*/30 * * * *"   # every 30 minutes, on the minute
```

```yaml
schedule:
  kind: cron
  spec: "15 9 * * 1-5"   # 09:15 Mon-Fri
```

#### `kind: manual`

Never auto-fires. The action runs only when triggered explicitly (see
below). Omitting the `schedule:` block entirely is equivalent to
`kind: manual`: the scheduler skips any action with no `schedule.kind`.

```yaml
schedule:
  kind: manual
```

#### Triggering and the one-block rule

Manual triggering via `claude-run trigger <action>` (and the web UI trigger
button) works **regardless of the schedule kind**, because the trigger path
is independent of `schedule:`. An interval- or cron-scheduled action can
still be fired on demand at any time.

Only **one** `schedule:` block is allowed per action (YAML rejects a
duplicate mapping key). There is therefore no "interval plus manual" mode:
just use `kind: interval` (or `cron`), and trigger it on demand whenever you
need an extra run. Declaring two `schedule:` blocks to get both is the
common gotcha and fails to parse.

#### Resolution

The daemon evaluates schedules on a fixed 5-second tick, so an action fires
on the first tick at or after its due time. Intervals shorter than 5s are
not meaningful, and cron seconds-precision is honored only to 5s
granularity.

## Runner environment variables

| Variable                              | Default                          | Purpose                                                 |
|---------------------------------------|----------------------------------|---------------------------------------------------------|
| `CLAUDE_RUN_STATE_DIR`                | `~/.local/share/claude-run`      | State, locks, PAUSED flags, cost ledger.                |
| `CLAUDE_RUN_LOG_DIR`                  | `$CLAUDE_RUN_STATE_DIR/logs`     | Daily JSONL logs and per-action output files.           |
| `CLAUDE_RUN_ACTIONS_DIR`              | `./actions`                      | Resolves bare action names to YAML files.               |
| `CLAUDE_RUN_WORKDIR_BASE`             | `/tmp/claude-run`                | Per-job workdir root. Final path = `<base>/<name>/<key>`. Overridable per action via `workdir_base`. |
| `CLAUDE_RUN_MAX_RUNS_PER_DAY`         | `20`                             | Hard cap on Claude invocations per action per day.      |
| `CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY`     | `3`                              | Per-job attempt cap when `dedup_key` is set.            |
| `CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS`   | `600`                            | `timeout` wrapper around the `claude` CLI call.         |
| `CLAUDE_RUN_DAILY_BUDGET_USD`         | `5.00`                           | Skip run when today's cost ledger meets/exceeds this.   |

## State + lock layout

```
$CLAUDE_RUN_STATE_DIR/
  PAUSED                       # global pause, applies to all actions
  <name>.PAUSED                # per-action pause
  locks/<name>.lock            # PID lock keyed on action name
  state/<name>.json            # {attempts, daily_count, daily_cost_usd}
  logs/<name>/YYYY-MM-DD.log   # JSONL runner log
  logs/<name>/<output.file>    # JSONL per-job outcomes if output.file set
  logs/<name>/streams/<key>-attempt-<n>.jsonl  # claude stream-json output per attempt
```

`state/<name>.json` shape:

```json
{
  "attempts": {
    "<dedup_key value>": {
      "count": 2,
      "last_at": "2026-05-23T12:34:56-0400"
    }
  },
  "daily_count": { "2026-05-23": 7 },
  "daily_cost_usd": { "2026-05-23": 1.23 }
}
```

## Exit codes

| Code | Meaning                                                                 |
|------|-------------------------------------------------------------------------|
| 0    | Run completed (with or without finding work).                            |
| 1    | Paused (global `PAUSED` or per-action `<name>.PAUSED` present).         |
| 2    | Lock contention (previous run still going).                             |
| 3    | Config error (action YAML missing, malformed, or missing required field). |
| 4    | Daily cap reached (`max_runs_per_day`).                                 |
| 5    | Unexpected error (see logs).                                            |
| 6    | Over daily budget (`daily_budget_usd`).                                 |
| 130  | Run cancelled in flight (operator cancel via the API / web UI / CLI).   |

Exit code 130 is recorded on the `runs` row, not returned by the `run`
subcommand; the web UI and CLI render it as a distinct "cancelled" badge.

## Snippet execution model

`input.command`, `filter`, `pre`, `prompt`, and `post` snippets run with the
interpreter declared by the field's `shell` (see
[Script fields: `run` / `shell`](#script-fields-run--shell)), defaulting to
Nushell. The runner writes the code to a file and executes
`<interpreter> <file>`.

Bindings available to snippets (the `$`-prefixed nu names below; under a
non-nu `shell` the same names are environment variables holding compact JSON):

| Binding          | `input.command` | `filter` | `pre` | `prompt` | `post` |
|------------------|:---------------:|:--------:|:-----:|:--------:|:------:|
| `$env`           | yes             | yes      | yes   | yes      | yes    |
| `$job`           | -               | yes      | yes   | yes      | yes    |
| `$workdir`       | -               | yes      | yes   | yes      | yes    |
| `$claude_result` | -               | -        | -     | -        | yes    |

`$workdir` is `<workdir_base>/<name>/<dedup_key-or-job-index>`, where
`workdir_base` is the action's `workdir_base` field if set, else
`$CLAUDE_RUN_WORKDIR_BASE`, else `/tmp/claude-run`. The runner creates it
before the first `pre` step and **removes it at end of job regardless of
outcome** (success, claude failure, pre/post step failure, transient error).
Plain `rm -r` is used, not `rm -rf`, so permission or read-only issues
surface as real errors instead of being silently force-deleted.

To inspect what happened in a failed run, look at the per-attempt stream log
(`$CLAUDE_RUN_LOG_DIR/<name>/streams/<key>-attempt-<n>.jsonl`); it survives
across runs since it lives outside `$workdir`.

For `filter` and `pre`/`post`, exit code 0 = continue, non-zero = step
failure (skip job, count attempt). The snippet's stdout is captured into the
daily log; stderr passes through.
