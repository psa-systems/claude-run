# Overview

## What it gives you

- **Per-action YAML files** describe input (a nu snippet that emits a JSON array), an optional filter, pre and post nu
  snippets, and the prompt sent to claude. See [`actions/SCHEMA.md`](../actions/SCHEMA.md).
- **Scheduling**: each action declares `schedule.kind: interval | cron | manual`; the daemon's built-in scheduler fires
  on time without external cron.
- **Per-job streaming logs**: claude is invoked with `--output-format stream-json` and the NDJSON events land in
  `$STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`. `tail -f` to watch live.
- **No background work survives a turn** (CLAUDE-280): every `claude` child runs with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`
  and `--disallowedTools Monitor,ScheduleWakeup`, because a session run under `--print` is over the moment its turn ends,
  so nothing it starts in the background can be waited on afterwards. As a backstop, if a stream log's terminal `result`
  event is followed by a `system`/`task_updated` event reporting `patch.status: killed` (a leftover background task the
  runner had to terminate), the run finalizes as a non-transient failure naming the task, never as the success the
  `result` event claimed.
- **Cost ledger + caps**: per-day Claude-invocation cap, per-day USD budget, per-RUN USD ceiling, per-job attempt cap. All in SQLite.
- **Per-run usage accounting** (DEV-707): every run ends with a `run usage` summary line and appends one newline-delimited JSON record to `$STATE_DIR/logs/usage.jsonl` carrying tokens by category, estimated cost against the budget, wall-clock duration, tool call counts, and per-tool attribution, so "what did this run cost, and which tool dominated it" is answerable without a provider console.
- **Skip-attempt marker**: an action's post step can write `$workdir/.claude-run-skip-attempt` for outcomes that park a
  job pending human input (so the attempt cap doesn't trip).
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick. The global switch is also on the **About** page's Activity card, next to a live quiet indicator (see [Global pause and the restart-safety workflow](operations.md#global-pause-and-the-restart-safety-workflow-claude-223)).
- **Duplicate-source guardrail**: at scan time the daemon flags (but never merges or disables) two enabled actions that
  do redundant work - names that differ only by case, or two native actions of the same `kind` polling the same source.
  Both would gather the same jobs and silently compete on the shared concurrency caps; you get a WARN log and a notice
  on each colliding action instead of discovering the contention during an incident.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`,
  `/resume`, `/runs/<id>/cancel`, ... The CLI client speaks that API when the daemon is reachable, else operates on the
  DB directly. It is the CLI's control plane, so `serve --no-socket` turns it off for a deployment that has no CLI
  operator; the same handlers are always mounted under `/api` on the TCP listener, which is what the web UI drives.
