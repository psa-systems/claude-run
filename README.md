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
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick. The global switch is also on the **About** page's Activity card, next to a live quiet indicator (see "Global pause and the restart-safety workflow").
- **Duplicate-source guardrail**: at scan time the daemon flags (but never merges or disables) two enabled actions that
  do redundant work - names that differ only by case, or two native actions of the same `kind` polling the same source.
  Both would gather the same jobs and silently compete on the shared concurrency caps; you get a WARN log and a notice
  on each colliding action instead of discovering the contention during an incident.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`,
  `/resume`, `/runs/<id>/cancel`, ... The CLI client speaks that API when the daemon is reachable, else operates on the
  DB directly. It is the CLI's control plane, so `serve --no-socket` turns it off for a deployment that has no CLI
  operator; the same handlers are always mounted under `/api` on the TCP listener, which is what the web UI drives.

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

**Forgejo bot token scopes.** The token `fj` authenticates with - the host `keys.json` entry, or the encrypted secret an
action names in `forgejo.token_ref` - must carry both **`write:repository`** and **`write:issue`**. Forgejo scopes its
API by endpoint category rather than by `fj` command name, and the PR path crosses both: `pr create`, `pr merge`,
`pr status`, `pr view`, and `repo view` are repository-category, while the open-PR lookup (`fj pr search`) and PR
comments / labels (`fj pr comment`, `fj pr edit labels`) go through the issues API. A repository-only token is rejected
at gather time by the repo-access preflight, which probes one read per category (`repo view` and `pr search`) so the
missing scope is named before a Claude run is paid for. If the open-PR lookup fails anyway (a mid-run permission
change, or a transient forge error) the runner creates the PR instead of failing the run, logging a `warn` run event
with the CLI error. The read-only log-fetch token
(`log_token_ref`, used by the CI-failure sources' log fetch) needs only `read:repository`. Two, separate write-scoped
tokens exist for two, separate reasons: the CI-failure webhook source's own `write_token_ref` (CLAUDE-276) posts the
PR comment a CI-failure routing decision needs (the analysis on a human's PR, the agent's own re-queued issue, the
"already tracked" note), needing `write:issue` (PR comments go through the issues API) and nothing else - it never
pushes a branch or opens a PR. The comment-command token (`write_token_ref`, CLAUDE-220, on the SEPARATE
`webhook_comment` section) reads the commenter's effective repository permission (`fj repo collaborator ...
permission`), posts the PR replies, and authenticates the branch push, so it needs `write:repository` plus
`write:issue`. Per-command breakdown with the endpoint each call maps to: `actions/SCHEMA.md`, "Forgejo token scopes".

**Resuming a PR on re-queue (CLAUDE-275).** Every PR the `youtrack` action opens is recorded in the `agent_pull_requests`
table (`src/ci_failure/store.rs`), keyed on `(host, repo, pr_number)`. If its issue is re-queued (`AI Agent = Queued`
again, e.g. after a CI failure sends it back) while that PR is still open, the runner clones its head branch instead of
the default branch and tells the agent, in the prompt, to fix the issue on that same branch rather than opening a
second PR; pushing to it lands on the existing PR through the same branch-strategy idempotency reconcile that already
adopts a pre-existing remote branch. A PR that is no longer open is marked closed and pickup falls back to the default
branch. The AGit PR strategy has no server branch to resume, so it always opens a fresh PR. See `actions/SCHEMA.md`,
"Resuming an open PR on re-queue".

## Use

```nu
# Long-running daemon: scheduler + unix-socket API. --detach forks into the background.
# --no-socket skips the unix-socket API (the container image runs that way).
claude-run serve --detach

# Manual out-of-cycle run via the daemon (counts as triggered_by=trigger in the runs table).
claude-run trigger youtrack

# Status (delegates to daemon if it's up, else reads DB directly).
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

# Self-update. `check` reports whether a newer build is published; `update` swaps the binary in place.
claude-run version                         # print the build-stamped banner (same as `version show`)
claude-run version check                   # uses the [update].train default (release)
claude-run version check --train latest    # track the moving main build instead
claude-run version update --yes            # install the newest release-train build
claude-run version update --train latest   # install the moving `latest` build
claude-run version update --version v0.2.0 # pin an exact tag (independent of train)
```

## Web UI pages

The daemon's TCP web UI (loopback by default; see "Web UI / API auth") is more than the Dashboard. The pages an operator
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
  The two webhook listeners (`webhook-forgejo`, `webhook-forgejo-comment`) are not action kinds and are not created
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
  global by default and narrowable with `action` + `range` query params.
- **About** (`/about`, CLAUDE-129) shows version + build info and drives the on-demand self-update check / apply (see
  "Self-update"). Its **Activity** card (CLAUDE-223) carries the global Pause / Resume control and the live quiet
  indicator (see "Global pause and the restart-safety workflow").

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

**Dashboard banner.** A poll-time repo-access preflight failure is recorded as a `run_events` row (CLAUDE-147); the
Dashboard counts those from the last 24 hours in its diagnostics banner and links to `/access`, so the operator is told
rather than having to go looking. A later poll whose preflight passes records a recovery marker for the same repo and
that count drops to zero; the marker is written only on that fail -> pass transition, so a healthy poll adds nothing to
the event log. CLAUDE-235 replaced the banner's table with the count: one repeating failure used to fill the operator's
first screen with byte-identical rows, and the rows now live on the Logs screen.

## Filesystem layout

XDG-compliant; each path overridable by the matching env var.

| Default                        | Env override             | What                                                                                            |
|--------------------------------|--------------------------|-------------------------------------------------------------------------------------------------|
| `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR`  | `config.toml`, `actions/*.yml`                                                                  |
| `$XDG_DATA_HOME/claude-run/`   | `CLAUDE_RUN_DATA_DIR`    | `state.db` (SQLite backend only; see "State store backend"), `workdirs/`                        |
| `$XDG_STATE_HOME/claude-run/`  | `CLAUDE_RUN_STATE_DIR`   | `logs/daemon.log`, `logs/<action>/<output.file>`, `logs/<action>/streams/<key>-attempt-<n>.jsonl`, `snippets/`, `webhook-logs/` |
| `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` | `daemon.pid`, `daemon.sock` (absent under `serve --no-socket`), `locks/<action>.lock`           |

`workdir_base` (action YAML field or `CLAUDE_RUN_WORKDIR_BASE` env) overrides where per-job workdirs live; defaults to
`$CLAUDE_RUN_DATA_DIR/workdirs` (`/data/workdirs` in the container). Supports `~`, `$VAR`, `${VAR}` expansion.

## State store backend: SQLite or PostgreSQL (CLAUDE-250)

Runtime state (runs, job outcomes, caps, settings, the encrypted secrets store) lives in SQLite at
`<CLAUDE_RUN_DATA_DIR>/state.db` by default. A deployment that already runs a database server can point the daemon at
PostgreSQL instead. Both backends run through one sqlx `Any` pool and one set of statements, so nothing else about the
runner changes with the choice.

**Three inputs, highest precedence first.** The backend has to be known before the DB is open, so this is a bootstrap
setting, never one of the DB-backed Settings sections:

| Input | Form |
|---|---|
| `CLAUDE_RUN_DATABASE_URL_FILE` | path to a file whose contents are the URL |
| `CLAUDE_RUN_DATABASE_URL` | the URL itself |
| `[database] url` in `config.toml` | the URL itself |

All three unset means SQLite at `<CLAUDE_RUN_DATA_DIR>/state.db`, exactly as before this existed: an existing install
needs no config edit, moves no data, and re-runs no migration. A `postgres://` or `postgresql://` URL selects
PostgreSQL; `sqlite://<path>` selects SQLite explicitly. Any other scheme stops startup with an error naming the scheme,
and an unreachable PostgreSQL is a startup error naming the host and the driver's own message, retried on a bounded
deadline (compose can start the daemon first) and then fatal. Neither case ever falls back to SQLite, which would
silently start a second, empty state store.

The file form exists because the URL carries the password, and an environment value is readable through
`docker inspect` and `/proc/<pid>/environ`. `CLAUDE_RUN_DATABASE_URL` remains available for a deployment where that
exposure is acceptable.

```toml
# config.toml
[database]
url = "postgres://claude_run:secret@db.internal:5432/claude_run"
```

**Compose.** `compose.yml` carries the server behind a `postgres` profile, so `docker compose up` starts exactly the
service set it started before, and `docker compose --profile postgres up` adds `claude-bot-postgres` (its data on the
`claude-bot-postgres` volume, its password from a compose secret, a `pg_isready` healthcheck the daemon waits on).
Point the daemon at it by uncommenting the `CLAUDE_RUN_DATABASE_URL_FILE` environment line and the
`claude-run-database-url` secret in the `claude-bot` service, then creating the two secret files:

```nu
openssl rand -base64 32 | save secrets/postgres-password.txt
"postgres://claude_run:<that password>@claude-bot-postgres:5432/claude_run" | save secrets/database-url.txt
docker compose --profile postgres up --detach
```

**Nothing is carried over by switching alone.** Pointing an existing install at PostgreSQL and starting it opens a
fresh, empty instance: no run history, no caps ledger, no stored secrets, and no web administrator, so the browser lands
on `/setup` again. Move the data first with `claude-run import`.

**Adding a table.** `migrations/sqlite/` is a chain (each file byte-identical once shipped, so an existing `state.db`
matches its recorded checksums); `migrations/postgres/0001_init.sql` is a squash of that chain's end state, spelled per
the type mapping at the top of that file. A schema change is therefore a new incremental file in EACH directory (e.g.
`migrations/sqlite/0021_ci_failures.sql` and `migrations/postgres/0002_ci_failures.sql` for CLAUDE-272's CI-failure
store), with the same end schema on both sides, never an edit to an existing file. `db::tests::the_two_migration_sets_end_on_the_same_schema`
is what catches the two directories drifting apart.

### Importing an existing SQLite store (CLAUDE-251)

`claude-run import` copies every table out of a SQLite state store and into the database the configuration resolves to,
so a switched backend keeps the same history, the same attempt and daily caps, the same settings documents, the same
encrypted secrets and the same web administrator. Stop the daemon, point the configuration at the new database, and run
it:

```nu
claude-run stop
$env.CLAUDE_RUN_DATABASE_URL = "postgres://claude_run:secret@db.internal:5432/claude_run"
claude-run import
claude-run serve --detach
```

```
table                  source      copied      target
-----------------  ----------  ----------  ----------
actions                     3           3           3
attempt_counters          412         412         412
...
```

- **The target is the configured database**, never a flag of its own, so the import and the daemon cannot disagree about
  where the rows went. Set `[database] url`, `CLAUDE_RUN_DATABASE_URL` or `CLAUDE_RUN_DATABASE_URL_FILE` first; the
  import runs the target's migrations itself, so the database only has to exist and be reachable.
- **`--from <file>` reads any SQLite store**, defaulting to `<CLAUDE_RUN_DATA_DIR>/state.db`. Importing a file into
  itself is refused, which is what the default invocation with no database URL set would otherwise be.
- **The source is opened read-only** and is never written, so a refused or failed import leaves you exactly where you
  started, still able to run the daemon off the same file. Stop the daemon first: a read-only open cannot recover a
  write-ahead log left behind by a killed process, and the open fails naming the file if one is there.
- **A target that already holds rows is refused**, naming each populated table and its count, unless you pass
  `--allow-nonempty`. The whole copy is one transaction, so a failure at any table commits nothing.
- **Both sides are printed per table** (source rows, rows copied, rows the target holds afterwards) rather than a bare
  success line, and a table whose counts do not add up aborts the import before the commit. On PostgreSQL the identity
  sequences behind `runs`, `job_outcomes`, `run_events` and `secrets` are advanced past the copied ids, so the daemon's
  first insert does not collide with imported history.
- **The two schemas must match exactly.** A source older than the build doing the import is refused, listing every
  missing column; open that file once with this build (`CLAUDE_RUN_DATABASE_URL=sqlite://<file> claude-run status`) to
  apply its pending SQLite migrations, then import.

The age key is not in the database and is not copied: back it up as usual and keep it where the new deployment reads it
(`<CLAUDE_RUN_CONFIG_DIR>/secrets.key`), or the imported ciphertext cannot be decrypted.

In the container the same command runs as a one-off, with the daemon stopped so nothing writes either side:

```nu
docker compose stop claude-bot
docker compose --profile postgres run --rm claude-bot import
docker compose --profile postgres up --detach
```

## Settings: bootstrap file vs the Settings page (CLAUDE-101)

Configuration splits in two. `config.toml` is the **bootstrap** file: only what the daemon must know before it can open
the DB and bind the listener, namely `[database]` (CLAUDE-250: which state store to open, see
[State store backend](#state-store-backend-sqlite-or-postgresql-claude-250)), `[web]` (bind / port / `auth_token_ref` /
login) and `[daemon]` binary overrides (
`nu_binary`, `claude_binary`) plus `[daemon].claude_config_dir` (CLAUDE-165: where the spawned agent reads its
user-level `CLAUDE.md` / credentials from; defaults to `<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`, seeded at serve
start with the slim bot `CLAUDE.md`, and with a symlink to the host `~/.claude/.credentials.json` when the host has
one). Everything runtime-tunable, namely issue minting (`[mint]`), the update train (
`[update]`), the webhook sources (`[webhook]`), and the log filter (`[daemon].log_filter`), now lives in the database
and is edited in the app on the **Settings** page (`/settings`, in the nav next to Dashboard), behind the same auth gate
as the rest of the web UI. A `config.toml` containing only `[web]` and `[daemon]` binary overrides is a complete, valid
configuration, and so is an empty one.

Each section is one JSON document validated against the same serde struct the file parser used (`deny_unknown_fields`),
so an invalid edit is rejected inline with the parser's own error and nothing is persisted. Secret values are never
shown or stored on the page: fields ending in `_ref` (`auth_token_ref`, a webhook source's `secret_ref` /
`log_token_ref` / `write_token_ref`, a comment source's `write_token_ref`, `[update].token_ref`) name a credential in
the Secrets store and are rendered as that name only. Each
section (and field) carries an apply badge: `live` for the log filter and webhook source edits (allow-list, rate limit,
secret refs, adding a source), `next event` for mint / update, and `restart` for the webhook
`replay_window_secs` (the replay store is built once at start). The same documents round-trip over the JSON API at
`GET` / `PUT /api/settings/{mint,update,webhook,webhook_comment,log_filter,workdir_gc,stream_log_gc,preferences,concurrency,db_retention,claude}`.
The `webhook_comment` (CLAUDE-220), `workdir_gc` (CLAUDE-106), `stream_log_gc` (CLAUDE-245), `preferences` (CLAUDE-154),
`concurrency` (CLAUDE-162), `db_retention` (CLAUDE-175), and `claude` (CLAUDE-239 `model`, CLAUDE-259 `effort`: the instance-wide `--model` and
`--effort` defaults an action inherits when it pins neither in its own `claude:` block, each one null by default, which passes no flag and leaves
the `claude` CLI's own default in force) sections are DB-only from birth (never `config.toml` keys), so
they are not part of the one-time import below; each simply defaults until edited. `webhook_comment` is additionally rejected by the file parser:
a `config.toml` declaring `[webhook_comment]` fails to start with serde's unknown-field error naming the section, so
there is no way to end up with two sources of truth for it.

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

## Workdir preservation (CLAUDE-102, CLAUDE-193, CLAUDE-281)

A job workdir is deleted only after `crate::workdir::local_only_work` proves nothing in it exists only locally, and every
message saying where the work is comes from that same check. "Only locally" means any of: commits on a local branch the
remote does not have under that branch name (a branch whose tip merely equals the remote default branch's tip has no
commits of its own, so it does not count); tracked or staged modifications; untracked files that are not ignored; or
stashes. A git or network failure preserves rather than guesses. Every runner (`youtrack`, the scripted-action pipeline,
the PR-comment webhook route) calls `crate::workdir::cleanup_or_preserve`, which runs this check and, on
anything local-only, prunes gitignored build artifacts (`git clean -fdX`) and keeps the directory rather than deleting it.
A PR-creation failure's issue comment (`pr_failure_comment`) is computed from the same check, so it can never call an
unpushed branch "safe on the remote" or claim work is preserved when the check could not run.

## Workdir GC (CLAUDE-106)

The YouTrack runner intentionally keeps some workdirs rather than deleting them: a directory that holds local-only work
(see above) is preserved when no PR was opened, and a leftover workdir is moved aside (`<key>.kept-<n>`) out of the way of
a fresh attempt. Nothing else ever deletes these, so preserved and aside workdirs accumulate without bound under
`$CLAUDE_RUN_DATA_DIR/workdirs/<source>/<key>` (a single preserved Rust workdir can be tens of GB of `target/` output).

A time-based sweeper bounds that disk use. It runs on `serve` start and then hourly, walking `workdirs/*/*` and deleting
any directory whose last-modified time is older than the retention window. A directory that still holds local-only work
(CLAUDE-281 / CLAUDE-284: unpushed commits, a dirty tracked/untracked tree, or a stash) is respected (kept) until a
separate, longer hard-cap age, so genuinely un-saved work is not discarded the moment the window passes. Every deletion
logs the action, key, path, age, and bytes reclaimed, so a sweep is auditable.

The windows are a DB-backed runtime setting edited on the **Settings** page (the `Workdir GC` section, or `GET` /
`PUT /api/settings/workdir_gc`). Fields: `enabled` (master toggle, default `true`), `retention_days` (default `14`), and
`unpushed_hard_cap_days` (default `60`). Edits apply on the next sweep. Set `enabled: false` to park the sweeper without
losing the configured windows.

Known limitation: the handful of startup lines emitted before the stored filter is applied (e.g. the config-source line)
are governed by the initial filter, not the saved one; use `CLAUDE_RUN_LOG` when those specific lines are needed at
`debug`.

## Log retention and size ceilings (CLAUDE-245, CLAUDE-257, CLAUDE-262, CLAUDE-268)

Every claude invocation writes a per-attempt stream log to `$CLAUDE_RUN_STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`, and until this sweep nothing ever deleted one: the workdir GC walks `workdirs/*/*` and the DB retention prune touches only database rows, so `logs/` only ever grew. That is not a cosmetic leak. A full log partition is what made the stream-log relay's write fail in the incident diagnosed in CLAUDE-233, which truncated 33 logs mid-line and failed the runs that produced them.

An age-based sweeper (`crate::gc::sweep_stream_logs`) bounds it. It runs on `serve` start and then hourly, walking `logs/<action>/streams/` and deleting every `.jsonl` whose last-modified time is older than the retention window. Every pass logs its `deleted` / `kept` / `bytes_reclaimed` tally at `info`, so the sweep is auditable even when it deletes nothing.

A log whose job is still in flight is never deleted, at any age. Age alone would not prove a run has finished: a job with a long `claude_timeout_seconds` can outlive any window while its log is still being appended to. The sweep runs inside the daemon that owns the runs, so it reads the authoritative set of `(action, job key)` pairs from `run_inflight_jobs` joined to the runs that have not ended, and keeps everything that set names. If that read fails the whole pass is skipped rather than run with an empty set, because the wrong guess would delete a live run's only transcript.

The other three writers under `logs/` are bounded by SIZE, not age (CLAUDE-257, CLAUDE-268). None is a per-run artifact: `daemon.log` is the single file `serve --detach` hands the daemon as stdout and stderr, `logs/<action>/<output.file>` is the single JSONL sink an action's `output.file` field turns on, and `usage.jsonl` is DEV-707's per-run token and cost record, so each is appended to for the life of the install and an age window would never fire. Past its ceiling a file is copied to one rolled-over predecessor (`daemon.log.1`, `<output.file>.1`, `usage.jsonl.1`) and truncated in place back to its newest half, starting at a line boundary. That line boundary is what keeps `usage.jsonl` parseable after a sweep: the survivor starts at a whole record rather than mid-object. In place, not renamed: the daemon holds `daemon.log` open in append mode, so a rename would leave it writing into the predecessor while the live log stayed empty.

The `output.file` sink is bounded by size and never deleted by age, unlike the stream logs beside it. An operator turns `output.file` on deliberately to produce a data sink, often read by something outside the runner, so the pass keeps the newest records where that consumer expects them instead of destroying the record it exists to keep.

Two more trees under the state dir grow the same way and are bounded by the same section (CLAUDE-262). `snippets/<action>/<job_key>/` holds the rendered snippet script plus one `binding-<name>.json` per `$`-binding for every `filter` / `pre` / `prompt` / `post` step of every job (`binding-job.json` carries the whole job payload), and nothing removed it before: `crate::gc::sweep_snippets` bounds it by age, on the same hourly tick, reusing the same `run_inflight_jobs` in-flight guard the stream-log sweep uses so a still-running job's snippet directory is never deleted out from under it. `webhook-logs/` holds the redacted, concatenated CI job logs the Forgejo webhook triage fetches, one file per triaged run, and `crate::gc::sweep_webhook_logs` bounds the files directly under it by age on the same tick; `webhook-logs/analysis/<owner>_<repo>-run<N>/` (CLAUDE-274), the per-event working directory the at-ingest analysis classifier creates and removes itself, is a subdirectory rather than a per-run file and needs no sweep of its own.

All of it is one DB-backed runtime setting edited on the **Settings** page (the `Stream log GC` section, or `GET` / `PUT /api/settings/stream_log_gc`). Fields: `enabled` (master toggle, default `true`), `retention_days` (default `30`), `daemon_log_max_bytes` (default 50 MiB), `output_file_max_bytes` (default 50 MiB), `usage_log_max_bytes` (default 8 MiB), `snippet_retention_days` (default `30`), and `webhook_log_retention_days` (default `30`). Edits apply on the next sweep. Set `enabled: false` to park all of it without losing the configured values, but nothing then bounds any of these trees; set any of the three size ceilings to `0` to leave that one file unbounded and rotate it yourself. The section keeps its original `stream_log_gc` key even though it now covers more than stream logs: the key is the primary key of the stored document, so renaming it would silently reset a configured window back to the default.

The default is deliberately longer than the workdir GC's 14 days and shorter than DB retention's 180: a failure reported weeks late still has its transcript, while at the ~800 MB/month this directory was observed growing at, 30 days holds it near 1 GB. The two windows are independent, so a `job_outcomes` row (180 days) outlives its stream log (30 days): run history, cost, and the reports are unaffected, but the transcript behind an older run is gone. The stream-log search skips a swept log silently, as an absent file rather than a read failure; CLAUDE-246's `unreadable` list is for logs that exist and fail mid-read.

## DB retention (CLAUDE-175)

The history tables append forever: every job outcome (`job_outcomes`) and every poll-phase diagnostic (`run_events`) is written and nothing deletes them, so the state store grows without bound. An age-based retention prune (`src/retention.rs`) bounds that growth. A daemon sweep runs on `serve` start and then hourly, deleting rows whose stored `ts` is older than the configured window. The delete predicate is an index range seek on the per-table single-column `ts` index (`migrations/sqlite/0017_job_outcomes_ts_index.sql` for `job_outcomes`, `migrations/sqlite/0018_run_events_ts_index.sql` for `run_events`, and both indexes in `migrations/postgres/0001_init.sql`), so it never full-scans a large table. `job_outcomes` is the driver; `run_events` (CLAUDE-172) and `ci_failure_runs` (CLAUDE-272, one row per sighting of a CI failure) share the same helper, config, and loop through the `RETENTION_TABLES` list rather than a divergent implementation. The other three CI-failure tables (`ci_failures`, `ci_failure_tests`, `agent_pull_requests`) are never pruned: a resolved failure or a merged PR must still be matchable against a regression months later.

The window is a DB-backed runtime setting edited on the **Settings** page (the `DB retention` section, or `GET` / `PUT /api/settings/db_retention`). Fields: `enabled` (master toggle, default `true`) and `retention_days` (default `180`). Edits apply on the next sweep. Set `enabled: false` to park the prune without losing the configured window.

Reporting interaction: the reports layer aggregates spend from `job_outcomes.ts`, and the web UI exposes 30-day and 90-day (and all-time) ranges. `retention_days` must stay `>=` the longest bounded range (90 days), so a save below 90 (while enabled) is rejected inline; the 180-day default leaves comfortable margin. The all-time range is inherently bounded by the retention window once a prune runs (its lower bound is whatever the oldest surviving row is), which is expected: retention is the layer that caps unbounded history.

## Display preferences (CLAUDE-154)

Every timestamp is stored as an RFC3339 UTC string; how it is displayed in the web UI is a single global setting (this is a single-operator daemon with no user table, so there are no per-user rows). The `preferences` section is DB-backed and edited on the **Settings** page (the `Display preferences` section, or `GET` / `PUT /api/settings/preferences`). Fields:

- `timezone`: an IANA name (e.g. `America/New_York`, default `UTC`). Every displayed date is converted into it before formatting.
- `datetime_format`: a chrono strftime pattern (default `%Y-%m-%d %H:%M:%S %Z`, e.g. `2026-07-01 00:00:00 UTC`). The Settings page offers preset buttons (ISO-seconds `%Y-%m-%d %H:%M:%S`, ISO-minutes `%Y-%m-%d %H:%M`, US `%m/%d/%Y %I:%M:%S %p`, EU `%d.%m.%Y %H:%M:%S`, RFC3339 `%Y-%m-%dT%H:%M:%S%:z`) that fill the field, plus free-form editing.
- `locale`: a BCP-47 locale (default `en-US`, CLAUDE-155). Name-emitting format specifiers (`%A`, `%a`, `%B`, `%b`, `%p`) render month / weekday / AM-PM names in this locale; it is validated on save and rejected inline if unknown.

Edits apply live (the dashboard, run and action detail pages, the secrets table, and the live job-stream log all reformat on the next render). Saving an unknown timezone or an unparseable strftime format is rejected inline and nothing is persisted. With the defaults (`UTC` + ISO-with-seconds) displayed times equal today's stored UTC values; only the surface format changes, never the underlying data. Elapsed durations (run duration, the +offset in the live log) and `report` analytics are wall-clock spans, not dates, and are left unformatted.

## Continuous pickup and concurrency caps (CLAUDE-162)

The daemon does not serialize runs per action. An issue queued while a run is in flight is picked up by a new,
**overlapping** run within `regather_interval` (default 60s) rather than waiting for the in-flight run to drain, and it is
never blocked behind an unrelated in-flight run of another action. A manual `trigger` during an in-flight run likewise
starts an overlapping pickup run instead of being silently dropped. Concurrency is bounded by four limits:

- **Per-action `max_parallel`** (the action's `concurrency.max_parallel`, default 1), now enforced **daemon-wide**: the
  total number of concurrent jobs for an action stays `<= max_parallel` across all its overlapping runs, not just within
  one run.
- **Global `max_parallel_global`** caps total concurrent jobs across every action, so overlapping runs cannot spawn an
  unbounded number of concurrent `claude` processes. Unset (`0`) it defaults to the maximum enabled-action
  `max_parallel`, so a single action still reaches its full per-action cap out of the box.
- **Per-key serialization** (unchanged): same-repo / same-project jobs still serialize by the daemon-wide key gate, so
  distinct repos parallelize but two jobs against the same repo never run at once, even across overlapping runs.
- **One gather at a time per action** (CLAUDE-228): a scheduled pickup is skipped while that action's previous gather is
  still running. A gather is the `yt list` / `fj` scan that runs before `run_begin`, and when the source slows down it can
  outlast the 60s cadence; each stacked fire then re-ran the same query against the same source for no new work. The skip
  does not advance the cadence stamp, so the next tick fires as soon as the gather finishes, and overlapping runs PAST the
  gather are unchanged. A manual `trigger` is never deferred by it.

Each job first gates on its concurrency key with a non-blocking try, then acquires a global permit, then the per-action
permit, in that fixed order (CLAUDE-163). A job whose key is already held by a running sibling takes no permit at all and
defers, staying queued for the next pickup gather, so a permit is only ever held by a job that is actually running.
Because the key is only ever taken by that non-blocking try, no job ever waits on a permit while another waits on that
key, so the pipeline is deadlock-free. Two overlapping runs never both work the same job: each run drops issues already
in flight in another open run (an admission check that also suppresses empty, redundant runs), the native `youtrack`
handler re-reads the live `AI Agent` field per job, and the key gate serializes same-repo work. The daily cap and budget
stay best-effort; because up to `max_parallel_global` jobs can pass the pre-launch check before any advances the ledger,
the documented overshoot bound is `max_parallel_global - 1`.

A run row is open **if and only if** a run is live (CLAUDE-228). Every runner closes its row on every exit path, including
an early error return between `run_begin` and `run_finish`, and a row closed that way carries exit code `5` (unexpected).
A row still open when the daemon starts belonged to a process that died without finalizing, so startup reaps it with the
same code and logs the count at `warn` (an operator cancel stays `130`). Without that, one transient database error left
a row the run list rendered as a running job indefinitely.

`max_parallel_global` and `regather_interval` are a DB-backed runtime setting (the `concurrency` section) edited on the
**Settings** page or via `GET` / `PUT /api/settings/concurrency`; fields are `max_parallel_global` (default `0` = auto)
and `regather_interval_secs` (default `60`, must be `>= 1`). Both apply on the next scheduler tick.

> The whole-run per-action `flock` that used to force a full drain is gone on the daemon path. The standalone
> `claude-run run <action>` path still takes it as a cross-process guard; running standalone `run` for an action the
> daemon also schedules is unsupported concurrent use.

## What the agent may write to the tracker (CLAUDE-214, CLAUDE-297)

The agent never publishes code: no `git push`, no pull request. The runner's finalize step opens the PR from the local
commits and owns every field it sets. What the agent *may* do to YouTrack used to be nothing at all; the built-in
prompt now permits a bounded set, because the `No orphan notes` rule requires a run that finds a gap to file it as a
tracked issue rather than bury it in prose in a PR body.

Permitted by the built-in default prompt (`prompt_preset: default`, `default_youtrack_prompt`, `src/native/youtrack.rs`):

- Read anything the token can see, across every project.
- Create an issue with `yt issue create` in exactly two cases. A gap, deferred item, or descoped piece of the
  triggering issue: restricted to `native.youtrack.write_projects` (defaulting to empty, which resolves to the
  **triggering issue's own project**, so the common case needs no configuration and no site specifics are baked into
  the binary), linked `yt issue link <new-issue> <KEY> --type "subtask of"`. A bug found outside the triggering
  issue's scope, in this repo or another: filed in the project whose VCS integration binds the buggy repo, falling
  back to `native.youtrack.write_projects` when none or more than one project binds it, linked
  `yt issue link <new-issue> <KEY> --type "relates to"`; fixed in this run only if it blocks the triggering issue's
  acceptance criteria and lives in the same repo, otherwise filed and left unfixed.
- Update, comment on and link **only the issues the run itself created** during that attempt.
- On the triggering issue, the only writes are the two link commands above, one per issue the run created.

Banned outright, stated as hard rules in the prompt: setting `AI Agent` on **any** issue including ones the run created
(setting it queues an agent run, so an agent that could set it could spawn runs), every other write to the triggering
issue (description, summary, comment, tag, assignee, `State`), any write to an issue the run did not create, and
`yt issue delete` anywhere.

The permitted list is a prompt instruction. The enforced boundary is the YouTrack token's own permissions: a create the
token cannot perform fails loudly and the run parks at Needs Review quoting the rejected command. An action that wants a
different policy selects another built-in preset with `native.youtrack.prompt_preset`, or replaces the prompt wholesale
with a `native.youtrack.prompt` override. `actions/audit-run.yml` selects `prompt_preset: audit`, the built-in audit
prompt: it adds the audit write-back and lets the run set `AI Agent = Queued` on the finding issues it files
(CLAUDE-295), the one place a built-in prompt permits that field.

**No-code completion path.** A run whose deliverable is tracker output rather than code makes no commits, and the
CLAUDE-212 ahead check fails such a run (`AI Agent = Failed`, no PR) so a silently-empty engineering run cannot look
successful. To finish deliberately with no commits, the agent writes its human-facing summary to
`$workdir/.claude-run-no-code-change`. Finalize then skips the PR, posts that text verbatim as the issue comment, and
sets `AI Agent` to `native.youtrack.states.no_code` (defaulting to the `done` label, since there is no PR to review).
`State` is left untouched, so the issue stays In Progress until a human closes it. Without the marker the CLAUDE-212
guard is unchanged, and the marker is never an escape hatch for a code task the agent could not finish: that stays
`.claude-run-needs-review`.

## Scheduled repository audits (CLAUDE-214, CLAUDE-215)

Two actions share `kind: youtrack` and partition the queued issues between them:

- `actions/youtrack.yml` works code issues. Query: `(project: -AUDIT or tag: -Audit-*) and -Resolved AI Agent: Queued`.
- `actions/audit-run.yml` works read-only repository audits. Query:
  `-Resolved AI Agent: Queued project: AUDIT tag: Audit-*`.

The split exists because the handler resolves a job's repository from the triggering issue's project VCS processor, one
project to one repo, and an audit writes its deliverable into a **different** repo from the one it reads. Audit issues
live in the `AUDIT` project, whose processor points at the audits repo (e.g. `owner/audits`), so the handler clones the audits repo into the
job workdir and finalize opens the pull request there; the agent clones the audited repo read-only to a path outside the
workdir and never writes to it. An audit issue picked up by the code-issue action would clone the audited repo and open
a pull request against it, which is why the exclusion is not optional: both actions poll on the same interval, so
without it the winner is a race. The audit side also requires an `Audit-*` tag: the YouTrack workflow tags the daily
audit parent and every per-target Epic `Audit-<Type>`, and that tag is what routes them to `audit-run.yml`. The worker
query is the exact complement, so an `AUDIT` issue with no `Audit-*` tag is ordinary engineering work on the audits repo
and goes to `youtrack.yml`. Note the query syntax - YouTrack rejects `-project: AUDIT` with a parse error, so the
negation sits on the value (`project: -AUDIT`), and a parenthesized group must be followed by an explicit `and`
(`#Unresolved (a or b)` is rejected). Name the project by its short name `AUDIT`: `AUD` is a legacy alias YouTrack still
resolves after the rename, but it appears in no project listing and must not be used in a query.

`audit-run.yml` sets `prompt_preset: audit`, which selects the audit prompt built into the binary
(`src/native/audit_prompt.rs`) instead of the engineering default; it carries no YAML `prompt` override, so an image
upgrade updates the audit prompt. `claude-run action show audit-run` prints `"prompt_source": "builtin:audit"`. The
preset handles both run shapes (a per-target Epic and the daily parent) and permits the tracker writes the default
prompt forbids: one summary comment per target on the parent issue, a `## Result` block on the run's own issue
description, the standing table on the anchor issue (daily parent run only), and filing the run's findings
(CLAUDE-295). Findings are
created only in `native.youtrack.write_projects` (the seven product projects the audits repo maps its targets to), at
most 20 finding issues per target, and each gets `AI Agent = Queued` as its last write, so the code-issue action works
it. `AI Agent = Needs Review` is the one other value the run may set, and only on an issue it created this run whose
content it later finds wrong (CLAUDE-304); `AI Agent` and `State` on the audit issue itself remain the runner's
alone, and delete stays banned regardless. The cap is a prompt instruction, not an enforced limit: `AuditedTracker`
counts the runner's own provider calls, not the `yt` calls an agent makes. The procedure itself (audit phases, the
coverage and carry-forward ledgers, dedup, grouping, priority, links) lives in the audits repo under `prompts/`, not in
this prompt, so it changes by pull request there.

**Filing from disk, not from a compacted context (CLAUDE-304).** A context compaction mid-run (`claude.autocompact`,
default 200000 tokens) replaces earlier conversation with a summary; an earlier audit run hit this and refiled nine findings
whose citations did not exist because the run rebuilt them from that summary instead of re-reading them. The audit
prompt now writes the dated report, with every finding's full block, before it files a single issue, then files each
one by re-reading its block from that on-disk report and re-opening its first cited `file:line` in the clone,
appending `<finding id><TAB><issue key>` to a filing log at `/tmp/audit-filing-{{key}}.tsv` right after each create so
a resumed or compacted run can tell what it already filed and never refile it. Independently, `finalize` counts
`compact_boundary` events in the run's own stream log and, when non-zero, appends `Context was compacted <n>
time(s) during this run; check the issues it created.` to the success or needs-review outcome comment (and logs a
`warn` naming the key and the count), so a compacted run is visible on the triggering issue even when everything it
filed turned out fine.

The dependency gate (CLAUDE-161) is left enabled, which is what sequences a daily rotation: the workflow files one
parent plus one Epic per target and queues all of them, and `parent for` keeps the parent blocked until every Epic
resolves, so the run summary has something to summarize. Each finding is filed as a subtask of its Epic, which makes the
Epic `parent for` open issues in the product projects. `dependencies.block_cross_project: false` makes the gate ignore
those, or an Epic retried after a transient failure would wait until every one of its findings was worked.
`concurrency.max_parallel: 1` keeps one audit in flight at a time; per-target caps and the coverage ledger, not
serialization, are what keep a rotation affordable. `automerge.enabled: true` merges an audit pull request once its CI
is green, which is what resolves its Epic and so releases the gated daily parent; the merge is event-driven, so the
webhook must send Action Run Success and Recover (see "Event-driven automerge").

**Migrating an existing deployment to the audit preset.** The container entrypoint copies the shipped action files into
`/config/actions/` only when that directory holds no `*.yml` file, so an existing deployment keeps its own
`/config/actions/audit-run.yml`, including the old YAML `prompt:` override, and an override wins over any preset. After
the image that ships `prompt_preset` is running (an older binary rejects the key as unknown), edit that file: delete the
whole `native.youtrack.prompt` block, add `prompt_preset: audit` under `native.youtrack`, and carry over the rest of the
shipped file (the `tag: Audit-*` query and `automerge.enabled: true`). Change the query in `/config/actions/youtrack.yml`
to the complement in the same step: with only one of the two queries changed, an `AUDIT` issue without an `Audit-*` tag
is matched by both actions or by neither. Then confirm the source:

```nu
docker compose exec --user claudebot claude-bot claude-run action show audit-run | from json | get prompt_source
```

It must print `builtin:audit`. `override` means a `prompt:` key is still in the file.

## Global pause and the restart-safety workflow (CLAUDE-223)

Applying an update does not restart the service, and restarting while jobs are running kills them mid-flight. The
**Activity** card at the top of `/about` is the control for doing it safely, and it is the same state
`claude-run pause` / `claude-run resume` / `claude-run status` read and write.

The workflow:

1. **Pause all activity.** The card's button posts to `/api/pause` with an optional free-text reason. From then on
   nothing new starts: the scheduler tick returns immediately, a manual trigger is refused with `409` naming the pause
   (instead of the old "queued trigger" ack for a run that was then silently dropped), an inbound delivery to either
   webhook listener is accepted with a `2xx` and dropped (a dropped delivery is not recovered after Resume), and any run
   still in flight stops picking up its NEXT job.
2. **Watch the indicator drain.** It polls itself every 2s and reads exactly one of three states: **busy** (each active
   run listed and linked), **idle** ("safe to restart or reboot"), or **unknown**. `unknown` means the activity check
   itself failed; it is never rendered as idle, and the underlying error is logged at `error`. Do not restart on
   `unknown`.
3. **Restart / reboot** once it reads idle.
4. **Resume all activity**, from the same card or from the "Globally paused" banner on the Dashboard.

Two properties worth knowing:

- **Pause never interrupts a `claude --print` child that is already executing.** Gating the next job pickup bounds the
  wait to that one child rather than to every job still queued in the run; the indicator is what tells you when it has
  finished. A run that stops this way ends with exit code `1` and badges as **paused** in the run list, distinct from an
  operator cancel (`130`) and from a clean finish (`0`). Its unprocessed jobs consume no attempt and are re-gathered on
  the first run after Resume.
- **The pause is stored in the database, so it survives a restart.** A daemon that comes up paused logs one `info` line
  saying so and stays idle until you resume; the Dashboard banner and the About card are the visual reminder.

Scripting a reboot instead of clicking: `/api/status` (and `claude-run status`) carries an `activity` block with
`quiet`, `active_runs`, and `dispatched_actions`. `quiet` is true only when BOTH signals are clear: the daemon's
in-flight map is empty AND no `runs` row has `ended_at IS NULL`. Neither alone is sufficient, because the map catches a
run dispatched before its row exists and the row catches a run started by a daemonless `claude-run run`.

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
# Optional: name of a stored secret (see "Secrets store" below) holding a bearer token for API
# clients. The browser login does not need it: that credential is created at /setup on first run.
auth_token_ref = "web-ui-token"
# Optional, and only used while no administrator exists: the username for the pre-setup login form.
auth_username = "admin"
# Optional: require the token even on a loopback bind (off by default; useful for local testing).
require_auth_on_loopback = false
```

Two credentials answer that gate, for two kinds of caller.

**The browser: a first-run administrator.** An exposed instance with no credential yet serves exactly one page: `/setup`,
which asks for a username and a password (minimum 12 characters) and creates the administrator. Everything else
redirects there, and non-browser callers get `503` naming the state. The credential is an argon2id hash in the
`web_admin` table, so it needs neither the secrets store, nor a key file, nor the CLI, and the setup page closes itself
the moment an administrator exists: a second POST to `/setup` is refused and redirected to `/login`. The new credential
applies to the next request, with no restart. This is what makes a container reachable out of the box; before it, a
non-loopback bind with no resolvable token took the whole web UI down, which no browser could recover from.

Setup is trust-on-first-use: whoever reaches the exposed port first can claim the instance. Bring it up behind the
reverse proxy (and whatever the proxy already enforces) and complete setup promptly. `no administrator yet` is logged at
`warn` on every start while the instance is unclaimed.

**API clients: the bearer token.** Callers send `Authorization: Bearer <token>`. The value lives encrypted in the
secrets store (resolved once at daemon startup); only its name appears in `config.toml`. `auth_token_ref` is optional
now: when it is unset, or its secret cannot be decrypted, the reason is logged at `warn` and bearer auth is simply
unavailable for the run, while the browser login and setup are unaffected. Where no administrator exists, the older
pairing still signs in: `auth_username` plus the token plaintext as the password, so existing host deployments are
unchanged.

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
credential counts when any of these hold, so a host install with an existing login is unaffected: a
`.credentials.json` in the agent config dir (on a host, the symlink to `~/.claude/.credentials.json`), a stored API
key, or `ANTHROPIC_API_KEY` in the daemon's environment.

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
   token) but still loads, since an operator's own delivery may hand it over `0644` deliberately. To generate one
   yourself:

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

The daemon turns a Forgejo `action_run_failure` delivery into a triaged YouTrack issue in seconds. The pipeline is
`POST /webhooks/forgejo/<source>` (HMAC-verified, replay-protected, rate-limited) -> internal-route log fetch +
redaction -> hybrid triage -> issue mint, all inside the running daemon. The webhook is the only path that turns a CI
failure into an issue: nothing has polled for CI failures since CLAUDE-279 retired the scheduled CI-failure actions,
so a delivery the daemon never processes (daemon down, hook misconfigured, or dropped while paused) is not picked up
later.

**CLAUDE-273: dedupe is a database lookup by failing-test overlap, not a YouTrack search.** Before triage runs, the
webhook consumer fingerprints the redacted log (`ci_failure::fingerprint`, the same cargo-test-output parser CLAUDE-272
introduced for the `ci_failure` store) into a set of `<target>::<test>` ids, or a single infra/signature id when nothing
cargo-shaped parses, and looks it up in `ci_failures` by `(repo, workflow_id, job_name)` overlap:

- **A hit on an `open` row** attaches this run (`attach_run`, `add_tests` for any new failing test, a comment naming the
  run, branch, and failing tests) and mints nothing - no triage, no `claude` call at all.
- **A hit on a `resolved` row** is a regression: the issue reopens (`set_state(mint.target_state)`), a "regression"
  comment posts immediately, the store row flips back to `open`, and only THEN does triage run so the analysis lands as
  a follow-up comment and `AI Agent` is set exactly as a freshly minted issue of that bucket would be.
- **No row matches** falls through to one legacy lookup (a YouTrack `#Unresolved` full-text search on the pre-store
  collapse marker, adopting an issue minted before the store existed) and, on a miss there too, the ordinary triage +
  mint pipeline; a `Created` outcome (or the legacy adoption) is then recorded into the store so the next sighting
  matches by test overlap instead of searching YouTrack again.

A store read or write failure is logged at `error` and fails the run rather than falling through to a mint, which
would recreate the very duplicate this issue removes. `mint::mint` itself no longer searches YouTrack or ever declines
for a duplicate or a resolved issue: dedupe is entirely the caller's job now. The issue body still carries a hidden
`Dedupe-Key:` marker (the pre-store `hash(repo, branch, workflow_id, job_name, failure_signature)` collapse key,
falling back to `hash(repo, branch, workflow_id, job_name)` with no derivable signature), because the legacy-adoption
lookup above still reads it.

**CLAUDE-274: a new failure gets one tool-using analysis at ingest, on a bounded worker pool.** A failure the store does
not already track (the `none` / `pr_only` case above) is analyzed by `AnalysisClassifier`: one `claude` session with
`Read,Grep,Bash` over the COMPLETE redacted log and the failing workflow YAML, in its own
`webhook-logs/analysis/<owner>_<repo>-run<N>/` working directory (removed once the session ends), timeout
`config.webhook.analysis_timeout_secs` (default 600s). This replaces the old split between a tool-less classifier at
ingest (bounded to a 48 KiB copy of the log) and a separate scheduled deep-analysis action (retired in CLAUDE-279) that
ran later over the full log with no way to route the failure: the analysis and the routing decision now come from the
same session.
The prompt ends with a fenced JSON verdict, `{"verdict": "code_defect" | "needs_human" | "flaky" | "infra", ...}`,
parsed the way the old classifier parsed its reply; an unparseable or missing verdict routes `needs_human` and logs at
`warn` (it never becomes `code_defect`). `code_defect` maps to the code-fix route, `needs_human` to the human-assigned
route, and `flaky` / `infra` both mint nothing (transient). The session's own Markdown report becomes the minted
issue's `## Triage` analysis text (capped at 24 KiB, the same cap the old scheduled analysis used), so the issue
carries the deep, log-grounded analysis from the moment it is filed. `consume_events` spawns each event's processing
as its own task behind a semaphore sized by `config.webhook.analysis_concurrency` (default 2, at least 1), so a burst
of failures runs at most that many 600s analyses at once rather than one blocking every later event (the old inline
loop) or an unbounded number starting together. The store lookup, analysis, and mint for one event additionally run
under a `concurrency::KeyGate` keyed on `(repo, workflow_id, job_name)`: a second failure of the same job waits for the
first's gate to release, then matches its store row and attaches instead of starting a second analysis.

**CLAUDE-264: a host condition keys without the branch, for the `Dedupe-Key:` marker and the legacy lookup alike.** The
collapse key normally folds in `branch`, which is right for a code failure (two unrelated bugs on two branches must
not merge, and the auto-fix route needs one branch to deliver onto) and wrong for a failure that is a property of the
RUNNER. A full disk is identical on every branch and fixable on none of them, so keying on it would mint (or search
for) a fresh marker per branch. When `infra_failure::detect` recognizes such a condition in the log, the key becomes
`hash(repo, workflow_id, job_name, infra_kind)` instead: no branch and no commit. The detector is pure string work over
the FULL redacted log (no LLM, no I/O) and reads the same text `failure_signature` does, not the bounded copy the
classifier prompt gets, because a capacity abort is post-steps and is exactly what that bound cuts. It ships three
deliberately conservative kinds, each requiring an unambiguous phrase: `disk-full`, `out-of-memory`, and
`runner-unavailable`. An over-broad pattern would merge unrelated code failures across branches, so an undetected
condition is the preferred failure mode. The `ci_failure` store dedupes an infra failure the same way as a code failure
now (by its `infra:<kind>` fingerprint id, CLAUDE-272's `fingerprint::fingerprint`), so the branch-agnostic collapse
described here matters mainly to the legacy-adoption fallback, not to the primary path.

Nothing about triage buckets, routing, or the auto-fix path changes: an infra failure is still classified and assigned
the same way, because reclaiming disk is a human action.

The listener is multi-source (CLAUDE-87): each `<source>` is a `[webhook.sources.<source>]` config block with its own
HMAC secret and allow-list, served at its own URL path. One Forgejo org webhook per source means a leaked org secret
authenticates only that org's allow-listed payloads, and each source has its own rate-limit bucket so one org's failure
storm cannot rate-starve another's deliveries. The replay window is shared across sources (the `X-Forgejo-Delivery` id
is globally unique, so a duplicate delivered to two sources is still caught). The source name is the URL path segment
verbatim, so use lowercase `[a-z0-9_-]`; a name needing URL escaping is warned about at load.

Triage sorts each verified failure into one of three buckets: a failing PR check that a code change fixes (`code_fix`)
mints an issue and sets `AI Agent = Queued` so the runner opens the fix PR; a failure no PR fixes (`human_action`,
e.g. a main-branch / tag / release / missing-secret failure) mints a human-assigned issue with no PR; a transient /
infra / flaky failure (`transient`) mints nothing. The reused per-key + daily caps throttle a storm of failures.

**CLAUDE-208: a stopped run is not a broken one.** Forgejo fires `action_run_failure` for every non-success done status, and it reports run-level `failure` for a run whose jobs were merely CANCELLED, which is what a newer push to the same ref produces when it supersedes an in-flight run. The run-level status alone therefore cannot tell a broken build from a stopped one, so the consumer reads the per-job statuses that come back with the log: when no job failed and every job ended `cancelled` or `skipped`, the delivery is acked and the run closed with a summary naming those statuses, with no triage and no issue. Any other status set (including an all-`success` run under a run-level `failure`) still triages and mints, and a job list that did not resolve at all (a rejected log token, a pruned run) fails open and mints as before.

**CLAUDE-276: a failure on an open PR routes by who opened the PR, not by matching a bot login.** Before triage runs,
the consumer resolves whether the failing commit itself is the head of some open PR (`resolve_open_pr`, listing the
repo's open PRs and matching by `head_sha`); a lookup failure is never read as "no PR" - it fails the run (`ok =
false`) with an `error` log, and nothing is minted or commented, because the old CLAUDE-149 `resolve_has_open_pr`
degrading a forge error to "no PR" is exactly the kind of silent miss this issue closes. `None` (no open PR at all,
e.g. a push to `main`) runs the ordinary store-match / triage / mint pipeline below unchanged. `Some(pr)` routes by
whether `agent_pull_requests` has an `open` row for `(host, repo, pr.number)`:

- **The agent's own PR** (an `open` `agent_pull_requests` row): below `config.webhook.agent_pr_fix_attempts` (default
  3), the run comments the recorded issue with the run link, the failing tests, and the analysis, bumps
  `fix_attempts`, and sets `AI Agent = Queued` so the runner picks the SAME issue back up on the SAME branch (CLAUDE-275)
  instead of a second issue ever being filed for it. At the ceiling, the issue is flagged `AI Agent = Needs Review`
  instead, with a comment listing every fix attempt's run. Either way: no PR comment, no new issue - the failure
  belongs to an issue that already exists. This replaces matching `trigger_user` against `bot_logins`, which is what
  silently dropped a real incident (run 5874 on PR 766, opened by the bot as `Claude-Run`, never matched the
  lowercase-only `claude-run` in `bot_logins`).
- **A human's PR** always gets the analysis, one way or another: sharing a failing test with an already-`open` failure
  gets a PR comment naming the issue ("This failure is already tracked in \<KEY\>; it is not caused by this PR.") and
  the run attached, with no analysis (the failure is already fully triaged); sharing a failing test with a `pr_only`
  failure whose sightings now span two distinct branches means the failure belongs to `main`, not either PR, so an
  issue is minted for real (triage + `mint::mint`, DEV-335 caps applied and advanced exactly like a fresh mint) and
  both PRs get a comment linking it; anything else (no store match at all) runs the analysis and comments it on the PR
  - always - then records a `pr_only` row (branch, PR number) with no issue yet, so a second branch hitting the same
  failure is what promotes it.

PR comments post through a webhook source's own `write_token_ref` (a Forgejo PAT that can comment on pull requests),
reaching `fj` the same temp single-host `keys.json` way `log_token_ref` and the comment listener's own `write_token_ref`
do. A source with none configured is a hard failure the moment a PR comment is needed: logged at `error`, recorded as
a `run_events` row naming the source, and the run finishes `ok = false` - it is never silently skipped.

`mint::mint` itself no longer applies a bot-loop or open-PR guard of its own (the old `SkippedBotLoop` /
`MintOutcome::SkippedOpenPr`, CLAUDE-149's `is_bot_loop`): the caller now decides open-PR routing, including the
agent's-own-PR case, before `mint` is ever reached. `config.mint.bot_logins` is unchanged and still used by the
`/claude` PR-comment listener's own, separate bot-login check.

**CLAUDE-153/CLAUDE-273: the webhook is a configurable action.** It is named `webhook-forgejo` (formerly `webhook-fast-path`), and a per-webhook `auto_fix` toggle on the `Webhook` settings decides only whether a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) gets `AI Agent = Queued`. It defaults to `false` (opt-in): with `auto_fix` off, a red PR check still creates an issue but with no `AI Agent` write, so no fixup PR is queued and the failure stays reported on the original PR for its author to fix in place (a `human_action` failure is unaffected either way); with it on, the corrective-fixup behavior above applies. This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. The `webhook-forgejo` action also participates in **Pause** like every other action: Pause/Resume controls and a Paused badge appear on the dashboard, and while it is paused an inbound `POST /webhooks/forgejo/<source>` is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, but the delivery is dropped without minting or queuing so Forgejo does not retry. A dropped delivery is not recovered after Resume. CLAUDE-223: the GLOBAL pause gets the same accept-and-drop treatment, checked first. Edit / export / delete remain rejected (it is synthetic and has no backing YAML).

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
allow-list, rate limit, `secret_ref`, `log_token_ref`, `write_token_ref`) and the webhook `auto_fix` toggle (CLAUDE-153)
take effect live, as does `agent_pr_fix_attempts` (CLAUDE-276, default 3, applies to the next event); `replay_window_secs`
applies on restart. A mint edit is honored by the next event.

### Closing a CI-failure issue on merge (CLAUDE-277)

Closing a CI-failure issue by finding its failing job green needs a run of that workflow on the repo's `default_branch`,
which never happens for a workflow that only ever runs on pull requests: it has no run on `main` at all. CLAUDE-277 adds
a second, event-driven close: `POST /webhooks/forgejo/<source>` now also accepts Forgejo `pull_request` deliveries,
dispatched by the delivery's event-type header (`X-Forgejo-Event`, mirroring Gitea's `X-Gitea-Event`; a deployment must
confirm this against its own Forgejo's actual delivery, since Forgejo's webhook headers are undocumented upstream - see
the operator note below). A `pull_request` delivery runs through the SAME signature / replay / rate-limit / pause /
allow-list gates the Action-run path does; only a terminal `action: "closed"` delivery does anything, everything else is
acked and dropped.

- **Merged, with the failing workflow green at the PR's head commit:** every open `ci_failures` row the PR references
  (its own `agent_pull_requests` issue, plus every key `Forge::pr_referenced_issues` finds) is closed -
  `set_state(mint.resolved_state)` (a new setting, default `Done`, separate from `mint.target_state`, which is what a
  fresh mint or a regression reopen sets) plus a comment linking the PR and the green run - and the store row is marked
  `resolved`. "Green" is the run's own status, not a specific job's: the acceptance criterion speaks of the workflow, not
  one job within it.
- **Merged, with that workflow red or no run found at the head commit:** the issue is left open with a comment saying
  so; nothing else changes. A later run of the same failing test still reopens it the ordinary CLAUDE-273 way.
- **Closed without merging:** the only effect is marking the PR's own `agent_pull_requests` row `closed` (a human's PR,
  with no such row, changes nothing); the runner's own re-queue logic (CLAUDE-275) then treats it as needing a fresh
  branch rather than resuming a dead one.

The failing workflow's run at the merge commit is found by paging `fj actions runs` for the commit (`CommitRunsClient`,
a second seam over the same JSON `ActionRunsClient::list_runs` reads, needed because `RunEntry` carries no commit sha).

**Operator setup:** each Forgejo webhook pointed at `/webhooks/forgejo/<source>` must also send pull-request events
(**Pull request events > Modification** in Forgejo's webhook form, which covers opened, closed, reopened and edited),
alongside the Action-run events it already sends. Until it does, nothing closes on merge, and the run list shows no
pull-request deliveries.

### Event-driven automerge (CLAUDE-126, CLAUDE-278)

A `youtrack` action with `automerge.enabled: true` opts a successful run into merging its own PR once CI passes, instead
of leaving it at Code Review for a human. Before CLAUDE-278, that wait was a blocking poll inside `finalize`: it held a
job slot for up to `automerge.wait_seconds` (1800s in the shipped action), and a suite slower than that budget - as
happened for billing-service's ~25-minute integration suite (GAMMA-1174) - timed out with the PR left at Code Review and
nothing watching it afterward.

`finalize` no longer waits for CI at all: a successful run opens the PR and stops at Code Review immediately, whether or
not automerge is enabled. The merge instead happens later, driven by a `success` or `recover` Action-run webhook event
on `POST /webhooks/forgejo/<source>` - the same endpoint the CI-failure listener uses, dispatched on the payload's
`action` field rather than dropped as "non-failure" the way it used to be. On such an event the daemon:

1. Finds the open PR (if any) whose head matches the run's commit.
2. Looks it up in the `agent_pull_requests` store (CLAUDE-275); a PR the runner did not open, or one no longer marked
   `open` there, is left alone.
3. Loads that PR's own action from disk to check `automerge.enabled`; if it is not (or the action no longer exists),
   nothing happens.
4. Reads the PR's combined CI status (`fj pr status --json`, CLAUDE-309: including its `behind_base` field), and
   dispatches on `behind_base` first, because Forgejo's `mergeable` never reflects whether the branch is behind its
   base - the two are unrelated:
   - **`behind_base` missing** (an `fj` predating FJ-99): takes no merge action, comments that the runtime's `fj` needs
     upgrading, and fails the run so the gap is visible rather than silently never automerging.
   - **`behind_base: true`:** runs `fj pr update <repo>#<n> --style merge` (`Forge::update_pr_branch`, CLAUDE-309) to
     merge the base into the branch, instead of racing into Forgejo's 405 refusal to merge a behind-base PR. A
     successful update comments that the branch was updated and leaves the issue at Code Review; updating a branch
     retriggers CI, and the next `success` event tries again. A failed update (e.g. a real conflict) comments the
     failure and fails the run.
   - **`behind_base: false` and not mergeable** (a real conflict, still-checking, or a WIP PR): comments that the PR
     is not mergeable and leaves the issue at Code Review.
   - **`behind_base: false` and mergeable:** merges it as a merge-commit, sets State to `state_done` and AI Agent to
     `done`, and posts a "runner merged" comment - the same sequence the old poll loop made on green. A merge failure
     here (Forgejo refusing for a reason `mergeable` did not predict) comments the failure and fails the run.
   - **Anything else** (checks not `success`): no action. There is no budget to exhaust and nothing to time out - a
     later `success` event on the same PR is what retries, for as long as CI takes.

Because the wait is event-driven, no job slot, timeout, or `check_interval_seconds` poll is involved any more; both
`automerge.wait_seconds` and `automerge.check_interval_seconds` are deprecated (parsed, for a still-loading action file,
but otherwise ignored - one deprecation warning is logged per action at load if either is set away from its default).

**Operator setup:** each Forgejo webhook pointed at `/webhooks/forgejo/<source>` must send Action Run **Success** and
**Recover** events, in addition to Failure, for any org running a `youtrack` action with automerge enabled (see the
migration note above). Without them, a PR opened by such an action sits at Code Review forever, exactly as if automerge
were disabled.

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
queued issue, `AI Agent=Queued`, no human assignee) and never reaches the classifier prompt that would label it
`human_action`. The minted issue's summary (`Auto-fix: cargo fmt on <repo> (run #N, <sha>)`) and description name the
concrete problem, the fix command, and the delivery instruction: commit on a fresh `chore/<kind>-<short-sha>` branch
and open a PR against `main` via the configured PR strategy (CLAUDE-103), never a direct push to main. In practice this
mint only happens on a push with no open PR (a direct push to `main`, or a protected branch): CLAUDE-276's open-PR
routing (above) decides everything before triage even runs when the failing commit IS on an open PR, so an auto-fix
verdict there reaches a PR comment (as this issue's analysis) rather than a fresh mint - the agent's own PR is re-queued
onto its existing issue instead, and a human's PR is left to its author with the fix described in the comment. Repeats
of the same failure group by the CLAUDE-273 `ci_failure` store's failing-test overlap, not the commit SHA, so an
auto-fix failure that recurs on a later commit attaches to the same open issue instead of minting a sibling.

### Cross-project closed-issue summaries (CLAUDE-218)

The same runner can be used as a reporting tool rather than as an issue worker. The `closed-issue-summary` native action
(`kind: closed-issue-summary`, `src/native/closed_issue_summary.rs`) answers "what did we close this week, across these
projects" and files the answer where the record already lives: it queries each configured project for the issues
resolved inside a rolling window, renders per-project counts plus a total and a per-project listing, creates that report
as a new issue, and links it to a nominated parent (typically the milestone being reported against) as a subtask.

It changes no code and opens no pull request. The run clones no repository and starts no agent session, so there is no
forge call and no git call on any path, and the only writes are the tracker's: one `issue create`, one `issue link`, and
one marker comment on the parent. That contract is why it is a separate action rather than a second job taught to
`actions/youtrack.yml`, following the audit split of CLAUDE-215.

Parents come from `parents` (nominated by id, never field-written) unioned with whatever the optional `query` selects. A
`query` must select an `AI Agent` value that `actions/youtrack.yml` does not, or the two actions race for the same
issue; the shipped reporting action leaves `query` empty, and `shipped_action_queries_are_disjoint` in `src/action.rs`
guards the partition. Counts are exact and `max_listed_per_project` caps only the listing, which then states how many
rows it did not print. The marker comment on the parent carries a hidden `Report-Window:` line and a parent already
carrying the current window's marker is skipped, so a poll interval shorter than the window still files one report per
window.

Configure it in `actions/closed-issue-summary.yml` (`parents` nominates the milestone, `projects` names what to count,
`window_days` sets the window). See `actions/SCHEMA.md` (`kind: closed-issue-summary`).

### Migration from the single-source listener (CLAUDE-87)

This is a **breaking config change**: the old top-level `webhook.secret_ref` / `webhook.allowed_repos` /
`webhook.rate_limit_per_min` keys are gone, and `[webhook.sources.*]` is the only shape. A config still carrying an old
key fails to parse at serve start with serde's unknown-field error naming the offending key. That is the intended
migration signal. To switch from the single SITE-level system webhook to one ORG-level webhook per org:

1. Generate and store one secret per org:
   `printf %s '<distinct-random-value>' | claude-run secret set forgejo-hmac-<org>` for each org that sends
   webhooks (e.g. `acme`, `globex`).
2. In Forgejo, on each org's Settings -> Webhooks, add a **Forgejo** webhook with URL
   `https://<daemon-host>/webhooks/forgejo/<source>`, Secret set to that org's stored value, and the Action Run Failure
   event selected. If any `youtrack` action in this org has `automerge.enabled: true`, also select Action Run Success
   and Action Run Recover (CLAUDE-278: that is what triggers the merge once CI goes green - see "Event-driven automerge"
   below); otherwise leave them unselected so `success` / `recover` deliveries stay off the wire entirely (any that do
   arrive are acked and dropped, same as before).
3. Replace the old `[webhook]` block in `config.toml` with the sources shape above and restart the daemon. It refuses to
   start while any old key is present, naming it.
4. Delete the SITE-level system webhook (and any old per-repo pilot webhook).
5. Verify: one failing run per org produces a `verified action_run_failure received` log line carrying the right`source`
   field. The old shared secret can be deleted after a burn-in period.

## Comment commands on a pull request or an issue (CLAUDE-220, CLAUDE-221)

A second inbound listener turns a `/claude <instruction>` comment on a Forgejo thread into work. The endpoint is
`POST /webhooks/forgejo-comment/<source>`, served alongside (and independently of) the CI-failure webhook above. It has
two routes, chosen by whether the comment's thread is a pull request:

- **Pull request (CLAUDE-220):** a bounded Claude run against that PR's head branch, pushed back to the branch.
- **Plain issue (CLAUDE-221):** an issue has no head branch to clone or push, so nothing runs here. The command mints a
  YouTrack issue carrying the instruction and a link back to the Forgejo issue, queued for the agent.

The second route is a ROUTE, not a second listener: the request pipeline, authorization gate, trigger parsing,
bot-login loop drop, dedupe key, and caps ledger are the same implementations both paths run through.

It is a SEPARATE listener, section, and synthetic action rather than a second event type on `webhook-forgejo`,
because the tokens differ in power: the comment listener's `write_token_ref` pushes branches (`write:repository`
plus `write:issue`), while a CI-failure source holds a read-only `log_token_ref` and, since CLAUDE-276, a
`write_token_ref` that only comments on pull requests (`write:issue`). Separate sections keep the push-capable token
out of the CI-failure source block, and each listener gets its own Pause switch. What the two DO share, by reference rather than by copy: HMAC
verification, the single replay-store instance (so one `X-Forgejo-Delivery` id fanned out to both endpoints is still
caught), the per-source rate-limiter type and builder, the `owner/repo` allow-list matcher, and the accept-and-drop
pause gate (CLAUDE-223), which checks the GLOBAL pause before each listener's own synthetic action.

**Request pipeline**, in the same order as the CI-failure endpoint: unknown source `404` -> per-source rate limit `429`
-> missing / unresolvable `secret_ref` `401` -> bad or missing signature `401` -> missing `X-Forgejo-Delivery` `400` ->
replayed delivery id `409` -> Pause check, global then per-action, accept-and-drop with `2xx` (CLAUDE-223) -> parse. Every post-authentication drop is acked
with `2xx` (so Forgejo does not retry) and logged with its reason: a comment `action` other than `created`, a repo
outside `allowed_repos`, a first line that does not open with the trigger, or an author in the mint config's
loop-safety `bot_logins` (so the bot's own reply can never re-trigger a run).

**Trigger.** The FIRST line of the comment, trimmed, must start with the configured `trigger` (default `/claude`)
followed by whitespace or end-of-line; everything after it on that line plus the remaining lines is the instruction.
`/claude help` (and a bare `/claude`) replies with usage and runs nothing, counting no attempt (the usage text differs
by route). An instruction over `max_instruction_chars` (default 4000) is refused with a reply naming the limit. An
edited or deleted comment does nothing: re-running work from an edit expresses no new human intent, so post a new
comment instead.

**Authorization** is a LIVE read of the commenter's effective repository permission through
`fj repo collaborator <owner>/<repo> permission <username> --format json` (a new `Forge::repo_permission`; no raw REST
anywhere on this path). The command runs only when the returned permission is in the source's `allowed_permissions`
(default `["write", "admin"]`) or outranks an entry in it. Forgejo ranks its access levels `none` < `read` < `write` <
`admin` < `owner` (`models/perm/access_mode.go`), so the default also admits a repository `owner`. Any other value is
matched exactly and case-insensitively, so a future Forgejo role can still be allowed by name.
A permission lookup that ERRORS denies (fail closed) and replies "could not verify permission". A denied commenter gets
ONE reply stating the required set, then the delivery is acked and dropped; denial replies are throttled per
`(repo, login)` so a scripted denied commenter cannot flood a PR.

**Execution on a pull request** shallow-clones the PR head branch, sets the bot git identity in the clone (so any
commit the agent itself makes, such as a merge of the base branch, is authored and committed as the bot), records the
cloned head commit, and writes every runner scratch file (`.claude-run-instruction.md`, `.claude-run-commit-msg`,
`.claude-run-pr-comment.md`, and the `fj pr create` body file) into the clone's own git directory (`git rev-parse
--absolute-git-dir`, joined with a `claude-run` subdirectory) rather than its work tree (CLAUDE-305), so a scratch
file can never be tracked, staged, or committed, whatever the repository already tracks at the same name in the work
tree; the instruction file is removed as soon as Claude's session returns and the commit-message file as soon as the
commit runs. The human's instruction is written VERBATIM to that file (it is never interpolated into a shell
command or a command line: the agent is told to read the file by its full path). The runner then runs Claude and
applies the decision gate (CLAUDE-302, CLAUDE-303). The prompt names the PR's base branch and allows exactly one kind of commit: a merge of
that base branch, made with `git fetch --unshallow origin <base>` then `git merge FETCH_HEAD`, which is how a
`/claude merge main and resolve the conflicts` request resolves a real conflict; every other change is left
uncommitted for the runner. The gate first checks for unmerged paths (`git diff --diff-filter=U`) BEFORE staging
anything: a conflict left behind, markers and all, is reported as unresolved and nothing is staged, committed, or
pushed. Otherwise the runner stages the tree (`git add -A`) and commits only if the index still differs from `HEAD`
or a merge is in progress (an agent that already committed everything is not re-committed), concluding a merge the
agent staged but did not commit. The change is then measured over every commit made since the cloned head: an
ordinary commit by its own `--numstat`, a merge commit (more than one parent) by `--remerge-diff --numstat`, so a
base-branch merge counts only the conflict resolution the agent actually wrote, never the lines the base branch
brought in; `files` is the count of distinct paths touched across every commit. A run with no commits since the
clone, or whose measured diff exceeds the source's `max_files` / `max_lines`, is reported on the PR and nothing is
pushed. A failure anywhere in the gate (for example an unreadable checkout) is reported as a failure, never as "no
changes". Within the caps the runner pushes the branch. A PR comment is posted on EVERY outcome path: pushed, push
failed, no changes, unresolved conflicts, too big, agent error, and measurement error.

**Execution on a plain issue (CLAUDE-221)** clones nothing, starts no agent session, and pushes nothing. It resolves
the repo's YouTrack project through the SAME routing the CI-failure mint uses (the `[mint]` `repo_project_map`
override, then the project whose enabled VCS integration claims the repo via the TTL-cached inverse map, then
`fallback_project`), creates an issue whose summary names the Forgejo issue and whose body carries the instruction
verbatim in a fenced block plus a link back and the requester, sets `State` to `[mint].target_state`, and then sets
`[mint].ai_agent_field` to `[mint].queued_value` (`AI Agent = Queued`) LAST so the `youtrack` native action only picks
the issue up once it is fully formed. The work then arrives as that action's own pull request. A comment is posted on
the Forgejo issue on EVERY outcome path: minted (naming the key and project), created-but-not-queued (naming the key
and the field to set by hand, so the request is never lost), and create-failed. Replies on this route go through
`fj issue comment <owner>/<repo>#<n> --body-file <path>` (`Forge::issue_comment`), never `fj pr comment` and never raw
REST.

**Accounting and concurrency.** Runs are recorded under the synthetic action `webhook-forgejo-comment` with the same
per-key attempt counter and daily run ledger every other action uses, so caps, the durations chart, and run history all
work. The attempt key is `hash(repo, number, comment_id)` on both routes (a Forgejo comment id is unique across a
repo's issues and pull requests), and because a comment is a one-shot request its effective per-key cap is one attempt:
a redelivery of the same comment never runs the work twice, and never mints a second YouTrack issue. The daily run
ledger advances for a mint as well as for a Claude run, so a flood of plain-issue commands is bounded by the same cap.
At most one comment job per thread is in flight, through the daemon-wide key gate; the gate key carries the thread kind,
so a plain issue can never block the like-numbered pull request. A second command while one is running gets a reply
saying so and is dropped.

**Configuration** is the `Comment webhook` section on the Settings page (`GET` / `PUT
/api/settings/webhook_comment`, DB key `config.webhook_comment`). Top level: `trigger`, `max_instruction_chars`, and
`sources`. Per source: `secret_ref` (HMAC; unset means the endpoint `401`s everything), `allowed_repos` (the same three
pattern forms as the CI-failure allow-list; empty denies all), `rate_limit_per_min`, `write_token_ref`,
`allowed_permissions`, the `max_files` / `max_lines` decision-gate caps, and the commit identity (`bot_author`,
`bot_email`, `skip_marker`). The plain-issue route adds no settings of its own: it reads the existing `Mint` section
(`config.mint`) for project routing, target state, and the `AI Agent` field / queued value. Source and rate-limiter
changes apply live, with no daemon restart. The `write_token_ref`
secret reaches `fj` exactly the way `log_token_ref` does: a temp single-host `keys.json` carrying that one secret and
nothing else, removed when the command finishes. The token is never in YAML, never an env var in the daemon's own
environment, never a process argument, and never rendered in a settings response.

**Forgejo setup.** On the repo or org's Settings -> Webhooks, add a **Forgejo** webhook with URL
`https://<daemon-host>/webhooks/forgejo-comment/<source>`, the Secret set to that source's stored `secret_ref` value,
and exactly two events selected: **Issue events > Comments** (plain-issue comments) and **Pull request events >
Comments** (pull-request comments). Forgejo sends these as separate event types, `issue_comment` and
`pull_request_comment` (`services/webhook/notifier.go`, `CreateIssueComment`), each behind its own checkbox, so a hook
with only the issue box ticked never receives a pull-request comment. Both carry the same payload, and the listener
tells them apart by `issue.pull_request`. Leave **Reviews** unticked: review comments are a different event the
listener does not handle. The bot account that owns the write PAT needs the **Admin** role on each allowed
repository, because Forgejo answers a permission read only for a site admin, a repo admin, or the user being
queried (`GetRepoPermissions`, `routers/api/v1/repo/collaborators.go`); without it every command replies "could not
verify permission". Store the write PAT first, e.g.
`printf %s '<pat>' | claude-run secret set forgejo-write-<org>`, and name it in the source's `write_token_ref`.

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
`--bare`) MCP servers, hooks, and skills stay enabled. If the host credential file is absent at serve start the daemon logs a
`warn` naming the path and continues (CLAUDE-247, so a container the operator has not logged into yet still comes up);
run `claude` once to log in, or set `ANTHROPIC_API_KEY`. A regular `.credentials.json` already in the agent config dir
is left alone rather than replaced by the link, because in a container that file is the login itself. On macOS
credentials live in the Keychain rather than `.credentials.json`, so the symlink step is Linux-only.

**Image.** `oci-build/Dockerfile` has a `runtime` target built on `dev.a8n.run/pandoras-box/opensuse-dev:v1.10.0-leap-16.0`
(CLAUDE-287), the org's dev image. It already carries the whole runtime toolchain: `nu`, the `claude` CLI, coreutils
`timeout`, `git`, `yt` (youtrack-cli), `fj` (forgejo-cli), `just`, `kingfisher`, and `docker` (client, Compose, buildx).
Tool versions come from the base image tag; moving a tool means bumping that tag, not editing a Dockerfile `RUN` block.
The runtime stage smoke-checks every one of those tools in a single `RUN`, so a base image that loses a tool fails the
image build instead of the first job. The container runs as the non-root user `claudebot` (uid 100, gid 101, matching
the ids the earlier Alpine-based image used) with `$HOME=/home/claudebot`, which holds nothing persistent: everything
the operator cares about lives on the three flat volumes below.

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

**Slim bot CLAUDE.md.** `deploy/bot-CLAUDE.md` is versioned in this repo, `COPY`d to
`/home/claudebot/.claude/CLAUDE.md` in the image, and written into the agent's own config dir
(`/config/claude-agent-home/CLAUDE.md`) on every serve start, which is the copy the agent actually reads. It contains only the local-only agent contract (no push, no PR; leave the feature
branch as HEAD; the runner finalizes), the bounded tracker writes from
[What the agent may write to the tracker](#what-the-agent-may-write-to-the-tracker-claude-214), the em-dash ban, the
Nushell 0.112.2 note, and the simplest-solution preference. It has no Git workflow and no lifecycle section, so the
agent is never told to push, open PRs, or transition issue state.

**compose.yml.** Consumes the published image (`image: dev.a8n.run/psa-systems/claude-run:latest`; the local `build:` block stays as a commented development alternative). App-prefixed names (service `claude-bot`, network `claude-bot-private`, volumes `claude-bot-config`, `claude-bot-data` and `claude-bot-state`, plus `claude-bot-postgres` which only the `postgres` profile mounts; prefix each with `dev-` for a development stack). Each tool's store is a subdirectory of one of the three roots rather than a volume of its own (CLAUDE-253); see the volume table below.

**Self-contained by design (CLAUDE-247, image and isolation superseded by CLAUDE-287).** The container bind-mounts nothing from the host and reads no host credential. The operator logs Claude, YouTrack, Forgejo and git in INSIDE the container once; each login lands in its own subdirectory of the config or data volume (`claude-agent-home/`, `youtrack-cli/` and `git/` under `/config`, `forgejo-cli/` under `/data`, per the volume table below) and survives restarts and image upgrades. Two reasons: the runner's identity stays separate from the operator's, and the stack deploys on a host where neither `$HOME` nor the invocation directory is available to the compose file. The default stack reads no path relative to this repo at all, including the secrets key, which the daemon generates itself; the only repo-relative paths in the file are the two `secrets/` files the `postgres` profile needs. CLAUDE-287 supersedes CLAUDE-247's image decisions (the Alpine base, the per-tool downloads, `su-exec`) and ends the property that the agent has no path to anything privileged (see "Docker access" above); the no-host-mount and in-container-login properties themselves are unchanged.

**Volumes.** Three, flat, named for what they hold:

| Volume              | Container path   | Purpose                                                                                                    |
|---------------------|------------------|------------------------------------------------------------------------------------------------------------|
| `claude-bot-config` | `/config`        | `config.toml`, `actions/`, `secrets.key`, `claude-agent-home/` (the agent's Claude login), `youtrack-cli/` (`yt`), `git/` (commit identity) |
| `claude-bot-data`   | `/data`          | `state.db` (runs, outcomes, the encrypted secrets store; unused on the `postgres` profile), per-job `workdirs/`, `forgejo-cli/` (`fj` keys) |
| `claude-bot-state`  | `/state`         | logs and per-attempt stream JSONL                                                                          |
| tmpfs               | `/run/claude-run`| pidfile; ephemeral so nothing stale survives a restart                                                     |

Each tool's store is a subdirectory rather than a mount of its own, because the image points every tool at these roots: `CLAUDE_RUN_{CONFIG,DATA,STATE,RUNTIME}_DIR` for the runner, `XDG_CONFIG_HOME=/config` for `yt`, `XDG_DATA_HOME=/data` for `fj` (which has no config-dir variable of its own, and which claude-run resolves the same way through `dirs::data_dir()`), `CLAUDE_CONFIG_DIR=/config/claude-agent-home` for `claude`, and `GIT_CONFIG_GLOBAL=/config/git/config` for git. `$HOME` holds nothing persistent; it is scratch.

`CLAUDE_CONFIG_DIR` deliberately names the same directory the daemon seeds and points its agent children at, so a `claude auth login` run through `docker compose exec` writes the credential where the agent reads it rather than into a second Claude config dir that looks fine and authenticates nothing.

A host install is unaffected by all of this: none of these variables is set there, and every path still resolves through XDG exactly as before.

**Running user.** The runtime stage stays on `USER root` (the base image's last `USER` is `dev`) so `oci-build/entrypoint.sh` can chown the mounted volumes to `claudebot`, then drops to that user with `setpriv --reuid=claudebot --regid=claudebot --init-groups` (pattern 3 in `governance/CONTAINER_UID_GID.md`); Leap packages neither `su-exec` nor `gosu`, and `setpriv` (util-linux, already in the base) is the equivalent gosu's own README names. The daemon itself runs non-root. `catatonit` (already in the base image) is PID 1 and execs `entrypoint.sh` as its child, so any process an agent session leaves running when its own session exits is reaped instead of accumulating as a zombie for the life of the container (CLAUDE-310). Do NOT set `user:` in compose: the entrypoint would lose the privileges the chown needs and a volume owned by another uid could never be repaired from inside. Two consequences for the operator:

- `docker compose exec` lands as root unless you pass `--user claudebot`. Always pass it for the login commands below, or the credentials are written root-owned and only become usable after the next restart's chown.
- The chown skips `$CLAUDE_RUN_DATA_DIR/workdirs`, which is a tree of job git clones the runtime user created itself; walking it on every boot would be slow for no gain.

**Docker access (CLAUDE-287).** Agent sessions run against a Docker-in-Docker sidecar, `claude-bot-docker`
(`docker.io/docker:29.6.2-dind`, the same pin the org's Forgejo runners use), rather than the host's own daemon: a
mounted host socket resolves bind paths against the host filesystem, where the volume in `/data/workdirs/...` does not
exist. The sidecar shares `claude-bot`'s network namespace (`network_mode: service:claude-bot`) and listens on
`tcp://127.0.0.1:2375` with no TLS, since nothing outside that shared namespace can reach it; `claude-bot` sets
`DOCKER_HOST` to that address, and every `claude` child inherits it (nothing in `src/` calls `env_clear` or
`env_remove`). A port a job publishes through that daemon answers on `claude-bot`'s own `127.0.0.1`. The sidecar mounts
the same `claude-bot-data` volume at `/data` that `claude-bot` does, which is what makes a bind like
`docker run --volume /data/workdirs/<job>:/build ...` from inside a job's workdir resolve to the right files; its own
Docker state lives on a separate named volume, `claude-bot-docker` at `/var/lib/docker`, so its growth is visible and
prunable:

```nu
docker compose exec --env DOCKER_HOST=tcp://127.0.0.1:2375 claude-bot-docker docker system df
docker compose exec --env DOCKER_HOST=tcp://127.0.0.1:2375 claude-bot-docker docker system prune
```

The `--env` override is required because `-H` in the sidecar's own `dockerd` command replaces its default unix socket.
There is no `depends_on` between the two services (the sidecar joins `claude-bot`'s namespace, so the dependency would
be a cycle); a job that runs `docker` before the sidecar's healthcheck passes simply fails with the CLI's own
connection error in that job's log. This ends the property CLAUDE-247 established that the agent has no path to
anything privileged: the agent now drives a privileged Docker daemon, which is root-equivalent on the host if a
container escapes it. That trade was accepted when CLAUDE-287 was filed; the container still bind-mounts nothing from
the host, and logins still happen inside the container.

**Actions.** The `./actions` bind is gone with the other host mounts. The image bakes this repo's action YAML at `/usr/share/claude-run/actions`, and the entrypoint copies it into the config volume the first time that volume holds no `*.yml`, so a fresh container starts with the shipped action set. It never overwrites after that: an action edited in the web UI survives an image upgrade, and a changed shipped action reaches an existing deployment through the same editor (`/actions`), not by recreating the container.

**No unix-socket API.** The image runs `serve --no-socket`, so the daemon binds only the TCP listener. That socket is the CLI's control plane on a host install, and this container has no CLI operator: the web UI and its `/api` routes are the whole interface, and they carry the same handlers (`/api/actions/<n>/trigger`, `/api/pause`, `/api/resume`, `/settings/<section>`). Binding it here would only leave a second copy of that control plane with no token gate on it, reachable by the `claude` agent the daemon spawns as the same user. `claude-run trigger` and `claude-run cancel` inside the container therefore fail, naming the reason rather than claiming no daemon is running; every other subcommand (`secret set`, `action list`, `status`) works as before because it goes to the database directly. Override `command: ["serve"]` in compose if a deployment really does want the socket.

**Web bind and first-run setup.** The image seeds a bootstrap `config.toml` (from `deploy/container-config.toml`) that binds `0.0.0.0:7321`, because a loopback bind inside a container is reachable from nothing outside it, published port or reverse proxy included. A non-loopback bind requires auth, and that credential is created in the browser: open the URL and the daemon serves `/setup`, which takes a username and a password and creates the administrator. No token, no key file, no CLI. The seeded `config.toml` is copied only when the config volume has none, so operator edits survive restarts and image upgrades. See "Web UI / API auth" for what setup is and is not (it is trust-on-first-use; bring the instance up behind the proxy).

**First-run logins.** Everything is in the browser: after creating the administrator, open **Settings -> Auth**, press **Sign in with Anthropic** (or store an API key), and enter the YouTrack base URL plus token, the Forgejo host plus token, and the GitHub host plus token. No restart, and no CLI. Until the Claude credential exists the scheduler runs nothing and says so on the dashboard. The `exec` route below remains for operators who would rather the CLIs own their own config. The daemon comes up before any login exists (a missing Claude credential is a `warn`, not a startup failure), so the container is there to exec into:

```nu
docker compose up --detach

# Create the web administrator at https://<your-host>/setup, then the tool logins:
docker compose exec --user claudebot claude-bot claude auth login
docker compose exec --user claudebot claude-bot yt auth login --base-url https://yt.example.com
docker compose exec --user claudebot claude-bot fj auth add-key
docker compose exec --user claudebot claude-bot gh auth login
docker compose exec --user claudebot claude-bot git config --global user.name "claude-bot"
docker compose exec --user claudebot claude-bot git config --global user.email "claude-bot@example.com"

docker compose restart
```

`claude auth login` needs the interactive TTY that `docker compose exec` provides; `claude setup-token` is the long-lived-token alternative. Verify with `claude auth status`, `yt auth list`, `fj auth list` and `gh auth status` through the same `exec --user claudebot` form.

To use an API key instead of an interactive Claude login, set `ANTHROPIC_API_KEY` in the service `environment:` and skip `claude auth login`. Nothing else changes: since CLAUDE-247 the daemon no longer requires a credentials file to start.

**Secrets.** The store itself (web auth token, webhook HMACs, registry token refs) lives encrypted in the state store,
`state.db` on the data volume or the `secrets` table on a PostgreSQL backend. The age identity that decrypts it needs no
injection: the daemon generates it at `/config/secrets.key` (mode `0600`) on the `claude-bot-config` volume the first
time it starts without one, so there is no `age-keygen` step before first start and the shipped `compose.yml` declares
no age-key secret. **Back that key up separately**, off the volume: every stored secret is unrecoverable without it, and
a key the operator never created by hand is easier to lose than one they did. Generation happens only while the store is
empty; a missing key with secrets already in the database is a hard error naming them rather than a silent replacement
that would orphan every one (see "Secrets store").

`CLAUDE_RUN_SECRETS_KEY_FILE` is the bring-your-own-key path, for a key that comes from your own secret store or has to
be shared with another instance. Its value is a PATH, never the key itself: key material in a plain environment variable
is deliberately unsupported (visible to `docker inspect` and `/proc/<pid>/environ`), so deliver the file with a bind
mount or a compose `secrets:` entry you add yourself, and set the variable to where it lands.
`ANTHROPIC_API_KEY` is the one secret that MAY go through plain env, as the Claude-auth alternative above.

```yaml
services:
  claude-bot:
    environment:
      CLAUDE_RUN_SECRETS_KEY_FILE: /run/secrets/claude-run-age-key
    secrets:
      - claude-run-age-key

secrets:
  claude-run-age-key:
    file: ./secrets/age-key.txt
```

A deployment that cannot use a path relative to the compose file (one whose config is piped through `docker compose --file -`, where relative paths resolve against the caller's working directory) declares that same secret from an environment value instead, and supplies the value from its own secret store. The key still reaches the container as a file at `/run/secrets/claude-run-age-key`; only the source changes:

```yaml
secrets:
  claude-run-age-key:
    environment: CLAUDE_RUN_AGE_KEY
```

**Migrating from a host install.** Carry the EXISTING key across, or the secrets already in the state store cannot be
decrypted, and do it before the first start so the daemon finds a key instead of generating one. Either copy it onto the
config volume:

```nu
docker compose create
docker cp ~/.config/claude-run/secrets.key claude-bot:/config/secrets.key
docker compose up --detach
```

or leave the host file where it is, mount it, and point `CLAUDE_RUN_SECRETS_KEY_FILE` at the mount as above. `docker cp`
keeps the source's uid and mode; the entrypoint chowns `/config` to `claudebot` on every start, and a key readable
beyond its owner still loads with a `warn`, so `chmod 600` the copy if you want that warning gone.

**Networking.** The seeded `config.toml` already binds `0.0.0.0:7321`; publish the port (`ports:` mapping or a
reverse-proxy network) to reach it. A non-loopback bind requires auth, which the first-run `/setup` page creates. A
bearer token for API clients stays optional and separate (`web.auth_token_ref`, see "Web UI / API auth").

```nu
docker compose up --detach      # start the daemon from the published image
docker compose logs --follow    # watch the scheduler
```

See [CLAUDE.md](CLAUDE.md) for the project-specific deviations from `niceguyit/governance` and the phase history of the completed Rust port.

## License

MIT. See [LICENSE.md](LICENSE.md).
