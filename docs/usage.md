# Usage

## Use

The container runs the daemon (`serve --no-socket`). Run CLI commands inside it, as the runtime user:

```nu
docker compose exec --user claudebot claude-bot claude-run status
```

`trigger` and `cancel` talk to the daemon over its unix socket, which the container does not open, so they fail naming
that reason; use the web UI's buttons instead. Every other command below works directly on the state store.

```nu
# Manual out-of-cycle run (needs the unix socket; in the container use the web UI).
claude-run trigger youtrack

# Status (delegates to daemon if it's up, else reads DB directly). With no socket to ask - the container's
# `serve --no-socket` - `"daemon"` still reports whether the daemon is up: "running (unix-socket API disabled by
# `serve --no-socket`)" when a marker next to the pidfile confirms it deliberately has none, "not running" when the
# pidfile shows no live process, or `{"state": "undetermined", "reason": ...}` when a live process holds the pidfile
# but neither the socket nor the marker explain why it was unreachable. `socket_expected_at` is present only in the
# "not running" and "undetermined" cases, since a confirmed `--no-socket` daemon has none to expect.
claude-run status

# Pause / resume. With no action name, it is the GLOBAL pause.
claude-run pause youtrack --reason "addressing a blocker"
claude-run resume youtrack
claude-run pause --reason "rebooting the host"
claude-run resume

# Cancel an in-flight run by id (needs a running daemon; the run lives in the daemon's process table).
claude-run cancel 42

# Action management.
claude-run action list
claude-run action show youtrack

# Secrets (encrypted at rest under CLAUDE_RUN_SECRETS_KEY_FILE; value read from stdin, never argv).
printf %s $token | claude-run secret set web-ui-token
claude-run secret list
claude-run secret delete web-ui-token

# Access check (read-only). No target checks every credential, project, and repo the actions reach.
claude-run access check
claude-run access check --project CLAUDE
claude-run access check --repo owner/repo --host forgejo.example.com

# Clear a locked-out account's TOTP enrollment (DB-direct); the next login re-enrolls.
claude-run access reset-2fa admin

# Print the build-stamped version banner. Update by pulling a newer image (see self-hosting.md).
claude-run --version
```

## Web UI pages

The daemon's TCP web UI (loopback by default; see [Web UI / API auth](auth.md#web-ui--api-auth)) is more than the Dashboard. The pages an operator
will want to know about:

- **Dashboard** (`/`) leads with the conditions that make work stop without anything failing: the global-pause card, the
  24h diagnostics banner (CLAUDE-216 / CLAUDE-235, see "Access check"), the stale-dependency-gate banner
  (CLAUDE-225), and the attempt-cap banner (CLAUDE-232). The diagnostics banner is one line: how many `warn`/`error`
  events the last 24h holds, how many of those
  are still-open repo-access failures (whose remedy is the access check, so they are named separately), and a link to
  the Logs screen for the messages themselves. The
  stale-gate banner lists every queued issue the CLAUDE-161 dependency gate has held for over an hour, with its blocker
  set, how
  long it has been held, and whether the gate's own re-evaluation deadline has passed (a "recheck overdue" gate is not
  being re-evaluated at all, so its blockers may already be resolved). A held issue keeps its Queued field value and a
  poll whose candidates are all blocked writes no run row, so without the banner the only symptom is an issue that never
  moves. The same list is on `/api/status` as `stale_gates`, so `claude-run status` reports it too. It clears itself
  when the blockers resolve. See `actions/SCHEMA.md`, "Pre-clone dependency gate".

  The attempt-cap banner is the same signal for the sibling condition: every job whose attempt counter has reached its
  action's `limits.max_attempts_per_key`, with the action, the key, the count against the cap, and the last counted
  attempt. For the `youtrack` native action (CLAUDE-301), reaching the cap is no longer a silent skip: the run posts
  one comment naming the attempt count and the cap and stating that re-queueing alone will not retry it, then sets
  `AI Agent` to the configured `needs_review` label - the same label the needs-review marker and a timeout use - so the
  issue leaves Queued instead of being skipped again on every poll. A failed comment or field write logs at `error`
  naming the key and the cause, and the cap is still reported as hit either way, so the banner keeps showing the key
  until a write actually lands. The generic scripted runner has no tracker field to write, so a scripted action's cap
  hit still just skips the job with a `warn` and no outcome row, exactly as before. The same list is on `/api/status`
  as `attempt_cap_hits`, with or without the daemon. Unlike a dependency gate, the counter row does not clear itself:
  it is the durable record of the attempts, so a key stays listed until the counter is gone. CLAUDE-238 gives the
  operator that reset directly, per (action, key), without raising the action's whole cap or deleting the action: a
  Reset control on each banner row (`POST /api/actions/<name>/attempts/<key>/reset`, 404s an unknown key), and the
  equivalent `claude-run action reset-attempts <name> <key>` on the CLI, with or without the daemon. A reset is
  recorded at `warn` naming the discarded count, so it is not an invisible mutation, and the banner clears itself the
  moment the row is gone. Raising the action's cap instead still grants one more attempt without a reset. See
  `actions/SCHEMA.md`, "`limits`".

  A third cap bounds what a single run may WRITE rather than how often it fires: `limits.max_issues_per_run`
  (CLAUDE-234, default 25) is the ceiling on issues one run may create in YouTrack. It is enforced at the provider
  boundary rather than in the agent prompt, so it holds whatever the prompt says; a run that reaches it stops creating,
  logs at `warn`, and records a run event, so a truncated filing run does not read as a clean one. Every issue create
  and every issue update the runner performs is journalled as one `youtrack issue write` line carrying the originating
  run id and the issue key. See `actions/SCHEMA.md`, "`limits`".

  A fourth cap bounds what a single run may SPEND: `limits.run_budget_usd` (DEV-705, unset = the action's own
  `daily_budget_usd`). It is read from the action's YAML before the run starts and never re-read, so it cannot be raised
  while the run works; raising it means editing the action and starting a new run. A run that reaches it starts no
  further job, closes its row with exit code `6`, and logs a reason naming the budget and the spend. Nothing offers to
  raise it. The runner also hands the run's remaining dollars to each `claude` child as `--max-budget-usd`, so a single
  session cannot overshoot the ceiling between the runner's own between-job checks, and warns once when the spend passes
  `limits.budget_warn_fraction` (default 0.80) of the ceiling. See `actions/SCHEMA.md`, "`limits`".

- **New action** (`/actions/new`, CLAUDE-211) is a kind picker: it lists every action kind with a one-line description
  and a link to its editor. Every native kind (`youtrack`, `closed-issue-summary`) has a structured form at
  `/actions/new/<kind>`, so no native action has to be hand-written as YAML. `scripted` is **raw YAML only** (its fields are nu snippets, which a form cannot
  structure); the raw editor is
  the explicit escape hatch at `/actions/new/raw` and also handles the few advanced fields no form covers. An existing
  native action's form is at `/actions/<name>/form` (offered from the raw editor, which stays at
  `/actions/<name>/edit`). Every form saves through the same validation and write path as the raw editor, so a form can
  never write a file the raw editor would reject; keys no form covers are preserved untouched on save.
  The two webhook listeners (`webhook-triage`, `webhook-comment`) are not action kinds and are not created
  here: they are configured on the Settings page (Webhook sources, Comment webhook) and appear on the dashboard as
  soon as their section has a source, with no restart.
- **Action detail** (`/actions/<name>`) carries an **Input errors / warnings** line (CLAUDE-147 / CLAUDE-235): how many
  input / poll-phase diagnostics (probe failures, no-VCS drops) this action has recorded since the current serve
  started, linking to the Logs screen filtered to it. It is a signal, not a log.
- **Logs** (`/logs`, CLAUDE-171 / CLAUDE-235) is the one diagnostics screen, in the top nav: every `run_events` row for
  every action, undeduplicated and paginated (50 per page), filterable by a whitelisted Level / Action / Context /
  message substring and a current-serve / last-24h / all-time window. Above the table, two summary cards count the last
  24h of `warn` and `error` events grouped by `context` and by `action_name`, and each row links back to the same page
  with that filter applied over the same window. `?action=<name>` narrows it to one action, which is where the
  Dashboard banner, the action page's **View logs** buttons, and the permanent redirect from the former
  `/actions/<name>/events` URL all land.
- **Access** (`/access`, CLAUDE-216) runs the access check on demand: "Check all" probes every enabled `youtrack`
  action, and the form probes a named repo (`owner/name`, optional host) and/or a YouTrack project key. Results are one
  row per probe. `/secrets` grows a per-secret **Check** button that lands here with that credential's identity probe
  already run. See "Access check".
- **Reports** (`/reports`, CLAUDE-117) is the historical reporting page (spend / runs / time-of-day / day-of-week),
  global by default and narrowable with `action` + `range` query params. Its "Claude plan usage" section (CLAUDE-315)
  charts the Claude subscription's own rate-limit utilization (five-hour and seven-day windows, 0-100%) per `claude`
  invocation, separate from the dollar spend above: a solid line for the five-hour window, a dashed line for the
  seven-day window, a red marker for a timed-out or failed invocation, and a gap in the line (with a stated count)
  wherever an invocation's log carried no rate-limit reading at all rather than a fabricated zero. A table below the
  chart lists the 20 most recent invocations with their status and API-retry counts.
- **About** (`/about`, CLAUDE-129) shows version + build info. Its **Activity** card (CLAUDE-223) carries the global Pause / Resume control and the live quiet
  indicator (see [Global pause and the restart-safety workflow](operations.md#global-pause-and-the-restart-safety-workflow-claude-223)).

## Access check (CLAUDE-216)

`/secrets` lists secret NAMES only, so an expired, revoked, or under-scoped token looks exactly like a healthy one, and
the poll-time repo-access preflight leaves its issues Queued with no field change. The access check answers, on demand
and without queueing a job, both halves: is each credential still valid, and is a named repo or YouTrack project
workable by the credential that would actually be used for it.

One service (`src/access.rs`) backs three surfaces, so they cannot drift:

- **Page** `/access` (see "Web UI pages").
- **API** `POST /api/access/check` with `{"secret"?, "host"?, "repo"?, "project"?}` (an empty body checks everything),
  returning the typed report as JSON.
- **CLI** `claude-run access check [--repo owner/name] [--host <host>] [--project <KEY>] [--secret <name>]`, which
  delegates to the daemon over the unix socket when it is up and runs the same service against the DB when it is not.
  It prints the report and **exits non-zero when any probe failed**, so it works as a deployment smoke test.

The probes, per enabled `youtrack` action:

| Probe | Command | What it proves |
|---|---|---|
| Tracker identity | `yt auth verify` | the token is valid; the row names the authenticated login |
| Project read | `yt project show <KEY>` | the project is readable by that token |
| VCS binding | `yt project vcs list <KEY> --enabled-only` | an enabled binding exists, and which host / repo it resolves to |
| Query | `yt list --query="<action query> project: <KEY>" --top 1` | the action's own query executes under that credential |
| Forge identity | `fj whoami` (or the GitHub identity read) | which forge account the PR would be opened as |
| Repo access | `Forge::repo_access` | both scope categories the PR path crosses, the same probe the poll-time preflight runs |

With no target named, the check resolves the projects the action's own query reaches (up to 10; a larger set says so in
its own row rather than truncating silently) and the repos those projects bind to. Credentials are materialized exactly
as a job materializes them (`JobCredentials::materialize`, keyed by the resolved host) and scrubbed when the probe ends,
so the check exercises the real per-job credential path instead of quietly falling back to host auth. **No secret value
is ever returned or rendered**: a row names the secret, the probe command, and the failure text only.

Every probe is read-only. A capability only a write could prove (opening a PR, writing the `AI Agent` field, commenting)
is reported as **`unverifiable`** with the reason, never as a pass; a write probe would leave real PRs and real issue
edits behind. `unverifiable` rows do not count as failures for the CLI exit code.

A YouTrack project the token cannot read comes back as HTTP 400 `invalid_query`, not a 401, which reads as a broken
query; the row's remediation says what it really is. A failed repo probe carries the `probe_denied` text verbatim, which
names the scope or the permission grant to add (see "Forge repo-access preflight" in `actions/SCHEMA.md`).

For a GitHub-bound project, the VCS binding probe resolving to an empty repo (no `owner/repo` after the host) means the
token can read the project but lacks **Update Project** permission, which is what gates YouTrack's own visibility into
the GitHub VCS integration's resolved path (see [Service auth](auth.md#service-auth-settings---auth)); it is not a broken or missing VCS integration.

**Dashboard banner.** A poll-time repo-access preflight failure is recorded as a `run_events` row (CLAUDE-147); the
Dashboard counts those from the last 24 hours in its diagnostics banner and links to `/access`, so the operator is told
rather than having to go looking. A later poll whose preflight passes records a recovery marker for the same repo and
that count drops to zero; the marker is written only on that fail -> pass transition, so a healthy poll adds nothing to
the event log. CLAUDE-235 replaced the banner's table with the count: one repeating failure used to fill the operator's
first screen with byte-identical rows, and the rows now live on the Logs screen.
