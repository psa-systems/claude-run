# Action YAML Schema

`scripts/claude-run.nu` is a generic runner. Behavior is defined by an action
YAML file passed as the only argument:

```nu
claude-run.nu /path/to/action.yml
# or, if the action lives in $CLAUDE_RUN_ACTIONS_DIR:
claude-run.nu youtrack
```

Each row produced by `input.command` becomes one Claude invocation. The runner
provides locking, daily caps, per-row attempt caps, timeouts, and a cost-based
usage check. The action YAML supplies everything specific to the job.

## File shape

```yaml
name: <kebab-id>            # required
workdir_base: /path/to/dir  # optional, overrides $CLAUDE_RUN_WORKDIR_BASE for this action
input:
  command: |                # required, nu snippet, stdout = JSON array
    #!/usr/bin/env nu
    ...
dedup_key: <field>          # optional, row field used to track attempts
filter: |                   # optional, nu snippet; exit non-zero to skip row
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
  file: <name>.log          # optional, appended JSONL of per-row outcomes
limits:                     # optional per-action overrides of env defaults
  max_attempts_per_key: 3
  max_runs_per_day: 20
  claude_timeout_seconds: 600
  daily_budget_usd: 5.00
```

## Field reference

### `name` (string, required)

Identifier used for the lock file, state file, log directory, and PAUSED flag.
Should be kebab-case and unique across all actions on the host.

### `workdir_base` (string, optional)

Per-row working directories are rooted under `<workdir_base>/<name>/<key>`.
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

### `input.command` (nu snippet, required)

A Nushell snippet executed by the runner with `nu --commands "$snippet"`. Must
write a JSON array to stdout. Each element of the array becomes one Claude
invocation, bound to `$row` for downstream snippets.

The snippet inherits the runner's environment. Use host-configured CLIs
(`yt`, `fj`, `git`, etc.) directly; the runner does not parse or inject
secrets.

If the snippet exits non-zero, or stdout is not a JSON array, the run aborts
with exit code 5.

### `dedup_key` (string, optional)

The name of a field on each row whose value uniquely identifies the unit of
work. The runner tracks attempts keyed on this value and skips a row whose
attempt count has reached `max_attempts_per_key`.

Omit to disable per-row attempt tracking (every run processes every row).

### `filter` (nu snippet, optional)

Run once per row, before any other per-row work. The row JSON is piped to
stdin and bound as `$row` in the snippet. Exit non-zero to skip the row
without counting an attempt. Exit zero to proceed.

### `pre` (list of nu snippets, optional)

Ordered steps executed before the Claude invocation. Each step is a separate
nu snippet with `$row`, `$workdir`, and `$env` in scope. Steps share the
runner's working directory unless they `cd` themselves.

If a step exits non-zero, the runner logs the failure, **counts the attempt**,
and moves on to the next row. (Poison rows eventually hit the attempt cap and
stop being retried.)

### `prompt` (nu snippet, required)

A nu snippet whose return value is the prompt string passed to Claude. Has
`$row`, `$workdir`, and `$env` in scope. Plain text without `$` interpolation
is fine; nu treats it as a string literal.

```yaml
prompt: |
  $"Work YouTrack issue ($row.id): ($row.summary)"
```

### `claude` (record, optional)

Tuning for the `claude` CLI call. All keys are optional.

| Key               | Default                                  | Meaning                                                   |
|-------------------|------------------------------------------|-----------------------------------------------------------|
| `cwd`             | per-row `$workdir`                       | Working directory for the claude process. Plain string.   |
| `allowed_tools`   | `Read,Edit,Write,Bash,Glob,Grep`         | Passed as `--allowed-tools`.                              |
| `permission_mode` | `acceptEdits`                            | Passed as `--permission-mode`.                            |

The runner always passes `--print --verbose --output-format stream-json`. It
wraps the call in `timeout $claude_timeout_seconds`. Stdout+stderr are
redirected to a per-row NDJSON stream log:

```
$CLAUDE_RUN_LOG_DIR/<name>/streams/<key>-attempt-<n>.jsonl
```

`tail -f` that file from another terminal to watch Claude work in real time.
The final `result` event on the last line is parsed for cost (`total_cost_usd`)
and outcome (`result`, `is_error`) once claude exits.

### `post` (list of nu snippets, optional)

Ordered steps executed after Claude returns successfully. Each step has
`$row`, `$workdir`, `$env`, and `$claude_result` in scope.

`$claude_result` shape:

```
{
  ok: bool,
  summary: string,        # claude's final message (from the result event)
  cost: float,            # total_cost_usd from the result event
  raw: record,            # the parsed final "result" NDJSON event
}
```

Same failure handling as `pre`: a non-zero step logs the failure, counts the
attempt, and continues to the next row.

### `output.file` (string, optional)

If set, the runner appends one JSON line per row to
`$CLAUDE_RUN_LOG_DIR/<name>/<output.file>` summarizing the outcome (row key,
attempt number, claude cost, push/comment results from post steps, etc.).

If omitted, per-row outcomes are only visible in the daily JSONL log.

### `limits` (record, optional)

Per-action overrides for the runner-wide defaults. Any subset of:

| Key                        | Env default                          |
|----------------------------|--------------------------------------|
| `max_attempts_per_key`     | `$env.CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY` |
| `max_runs_per_day`         | `$env.CLAUDE_RUN_MAX_RUNS_PER_DAY`     |
| `claude_timeout_seconds`   | `$env.CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS` |
| `daily_budget_usd`         | `$env.CLAUDE_RUN_DAILY_BUDGET_USD`     |

## Runner environment variables

| Variable                              | Default                          | Purpose                                                 |
|---------------------------------------|----------------------------------|---------------------------------------------------------|
| `CLAUDE_RUN_STATE_DIR`                | `~/.local/share/claude-run`      | State, locks, PAUSED flags, cost ledger.                |
| `CLAUDE_RUN_LOG_DIR`                  | `$CLAUDE_RUN_STATE_DIR/logs`     | Daily JSONL logs and per-action output files.           |
| `CLAUDE_RUN_ACTIONS_DIR`              | `./actions`                      | Resolves bare action names to YAML files.               |
| `CLAUDE_RUN_WORKDIR_BASE`             | `/tmp/claude-run`                | Per-row workdir root. Final path = `<base>/<name>/<key>`. Overridable per action via `workdir_base`. |
| `CLAUDE_RUN_MAX_RUNS_PER_DAY`         | `20`                             | Hard cap on Claude invocations per action per day.      |
| `CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY`     | `3`                              | Per-row attempt cap when `dedup_key` is set.            |
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
  logs/<name>/<output.file>    # JSONL per-row outcomes if output.file set
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

## Snippet execution model

All `input.command`, `filter`, `pre`, `prompt`, and `post` snippets run as
Nushell code with `nu --commands "$snippet"`.

Bindings available to snippets:

| Binding          | `input.command` | `filter` | `pre` | `prompt` | `post` |
|------------------|:---------------:|:--------:|:-----:|:--------:|:------:|
| `$env`           | yes             | yes      | yes   | yes      | yes    |
| `$row`           | -               | yes      | yes   | yes      | yes    |
| `$workdir`       | -               | yes      | yes   | yes      | yes    |
| `$claude_result` | -               | -        | -     | -        | yes    |

`$workdir` is `<workdir_base>/<name>/<dedup_key-or-row-index>`, where
`workdir_base` is the action's `workdir_base` field if set, else
`$CLAUDE_RUN_WORKDIR_BASE`, else `/tmp/claude-run`. The runner creates it
before the first `pre` step and does not clean it up; if a prior attempt
left state behind the next `pre` step (typically `git clone`) is expected
to fail naturally, which trips the per-key attempt counter so a human can
investigate.

For `filter` and `pre`/`post`, exit code 0 = continue, non-zero = step
failure (skip row, count attempt). The snippet's stdout is captured into the
daily log; stderr passes through.
