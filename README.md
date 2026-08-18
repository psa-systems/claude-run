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
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick. The global switch is also on the **About** page's Activity card, next to a live quiet indicator (see "Global pause and the restart-safety workflow").
- **Duplicate-source guardrail**: at scan time the daemon flags (but never merges or disables) two enabled actions that
  do redundant work - names that differ only by case, or two native actions of the same `kind` polling the same source.
  Both would gather the same jobs and silently compete on the shared concurrency caps; you get a WARN log and a notice
  on each colliding action instead of discovering the contention during an incident.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`,
  `/resume`, `/runs/<id>/cancel`, ... The CLI client speaks that API when the daemon is reachable, else operates on the
  DB directly.

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
(`log_token_ref`, used by the CI-failure actions) needs only `read:repository`. The comment-command token
(`write_token_ref`, CLAUDE-220) is a third, separate credential: it reads the commenter's effective repository
permission (`fj repo collaborator ... permission`), posts the PR replies, and authenticates the branch push, so it needs
`write:repository` plus `write:issue` (PR comments go through the issues API). Keeping it in its own settings section is
the whole reason the comment listener is a separate section from `webhook`: the CI-failure sources never hold a
write-scoped token. Per-command breakdown with the endpoint each call maps to: `actions/SCHEMA.md`, "Forgejo token
scopes".

## Use

```nu
# Long-running daemon: scheduler + unix-socket API. --detach forks into the background.
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
  24h access-failure banner (CLAUDE-216, see "Access check"), and the stale-dependency-gate banner (CLAUDE-225). The
  last one lists every queued issue the CLAUDE-161 dependency gate has held for over an hour, with its blocker set, how
  long it has been held, and whether the gate's own re-evaluation deadline has passed (a "recheck overdue" gate is not
  being re-evaluated at all, so its blockers may already be resolved). A held issue keeps its Queued field value and a
  poll whose candidates are all blocked writes no run row, so without the banner the only symptom is an issue that never
  moves. The same list is on `/api/status` as `stale_gates`, so `claude-run status` reports it too. It clears itself
  when the blockers resolve. See `actions/SCHEMA.md`, "Pre-clone dependency gate".

- **New action** (`/actions/new`, CLAUDE-211) is a kind picker: it lists every action kind with a one-line description
  and a link to its editor. Every native kind (`youtrack`, `forgejo-ci-fix`, `ci-failure-analysis`,
  `ci-failure-reconcile`, `closed-issue-summary`) has a structured form at `/actions/new/<kind>`, so no native action
  has to be hand-written as YAML. `scripted` is **raw YAML only** (its fields are nu snippets, which a form cannot
  structure); the raw editor is
  the explicit escape hatch at `/actions/new/raw` and also handles the few advanced fields no form covers. An existing
  native action's form is at `/actions/<name>/form` (offered from the raw editor, which stays at
  `/actions/<name>/edit`). Every form saves through the same validation and write path as the raw editor, so a form can
  never write a file the raw editor would reject; keys no form covers are preserved untouched on save.
- **Action detail** (`/actions/<name>`) carries an **Input errors / warnings** summary region (CLAUDE-147 / CLAUDE-171):
  the first few (`ACTION_EVENT_SUMMARY_LIMIT = 5`) distinct input / poll-phase diagnostics (probe failures, no-VCS drops)
  seen since the current serve started, each with its repeat count. It is a signal, not a log.
- **Action error log** (`/actions/<name>/events`, CLAUDE-171) is the full list behind that summary: every matching
  `run_events` row, undeduplicated and paginated (50 per page), filterable by a whitelisted Level / Action / Context /
  message substring and an optional "since boot" bound. `<name>` seeds the Action filter, which can be widened to any
  action or all.
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
Dashboard now surfaces those from the last 24 hours in a banner linking to `/access`, so the operator is told rather
than having to find the action's event list. A later poll whose preflight passes records a recovery marker for the same
repo and the banner clears itself; the marker is written only on that fail -> pass transition, so a healthy poll adds
nothing to the event log.

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
`log_token_ref`, a comment source's `write_token_ref`, `[update].token_ref`) name a credential in the Secrets store and
are rendered as that name only. Each
section (and field) carries an apply badge: `live` for the log filter and webhook source edits (allow-list, rate limit,
secret refs, adding a source), `next event` for mint / update, and `restart` for the webhook
`replay_window_secs` (the replay store is built once at start). The same documents round-trip over the JSON API at
`GET` / `PUT /api/settings/{mint,update,webhook,webhook_comment,log_filter,workdir_gc,preferences,concurrency,db_retention}`.
The `webhook_comment` (CLAUDE-220), `workdir_gc` (CLAUDE-106), `preferences` (CLAUDE-154), `concurrency` (CLAUDE-162),
and `db_retention` (CLAUDE-175) sections are DB-only from birth (never `config.toml` keys), so they are not part of the
one-time import below; each simply defaults until edited. `webhook_comment` is additionally rejected by the file parser:
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

## DB retention (CLAUDE-175)

The history tables append forever: every job outcome (`job_outcomes`) and every poll-phase diagnostic (`run_events`) is written and nothing deletes them, so the SQLite database grows without bound. An age-based retention prune (`src/retention.rs`) bounds that growth. A daemon sweep runs on `serve` start and then hourly, deleting rows whose stored `ts` is older than the configured window. The delete predicate is an index range seek on the per-table single-column `ts` index (`migrations/0017_job_outcomes_ts_index.sql` for `job_outcomes`, `migrations/0018_run_events_ts_index.sql` for `run_events`), so it never full-scans a large table. `job_outcomes` is the driver; `run_events` (CLAUDE-172) shares the same helper, config, and loop through the `RETENTION_TABLES` list rather than a divergent implementation.

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

## What the agent may write to the tracker (CLAUDE-214)

The agent never publishes code: no `git push`, no pull request. The runner's finalize step opens the PR from the local
commits and owns every field it sets. What the agent *may* do to YouTrack used to be nothing at all; the built-in
prompt now permits a bounded set, because the `No orphan notes` rule requires a run that finds a gap to file it as a
tracked issue rather than bury it in prose in a PR body.

Permitted by the built-in prompt (`default_youtrack_prompt`, `src/native/youtrack.rs`):

- Read anything the token can see, across every project.
- Create issues with `yt issue create`, restricted to `native.youtrack.write_projects`. That list defaults to empty,
  which resolves to the **triggering issue's own project**, so the common case needs no configuration and no site
  specifics are baked into the binary.
- Update, comment on and link **only the issues the run itself created** during that attempt.
- On the triggering issue, exactly one write: `yt issue link <new-issue> "subtask of" <KEY>`, so a report issue hangs
  off the issue that asked for it.

Banned outright, stated as hard rules in the prompt: setting `AI Agent` on **any** issue including ones the run created
(setting it queues an agent run, so an agent that could set it could spawn runs), every other write to the triggering
issue (description, summary, comment, tag, assignee, `State`), any write to an issue the run did not create, and
`yt issue delete` anywhere.

The permitted list is a prompt instruction. The enforced boundary is the YouTrack token's own permissions: a create the
token cannot perform fails loudly and the run parks at Needs Review quoting the rejected command. An action that wants a
tighter policy than the built-in one overrides `native.youtrack.prompt` wholesale (`actions/audit-run.yml` does exactly
that, permitting three named writes and banning issue creation).

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

- `actions/youtrack.yml` works code issues. Query: `-Resolved AI Agent: Queued project: -AUD`.
- `actions/audit-run.yml` works read-only repository audits. Query: `-Resolved AI Agent: Queued project: AUD`.

The split exists because the handler resolves a job's repository from the triggering issue's project VCS processor, one
project to one repo, and an audit writes its deliverable into a **different** repo from the one it reads. Audit issues
live in the `AUD` project, whose processor points at the audits repo (e.g. `owner/audits`), so the handler clones the audits repo into the
job workdir and finalize opens the pull request there; the agent clones the audited repo read-only to a path outside the
workdir and never writes to it. An audit issue picked up by the code-issue action would clone the audited repo and open
a pull request against it, which is why the exclusion is not optional: both actions poll on the same interval, so
without it the winner is a race. Note the query syntax - YouTrack rejects `-project: AUD` with a parse error, so the
negation sits on the value (`project: -AUD`).

`audit-run.yml` carries a full-replacement prompt (CLAUDE-125) rather than the built-in one, to permit exactly three
tracker writes the default prompt forbids outright: the run-summary comment on the parent issue, a `## Result` block on
the run's own Epic description, and the standing table on the anchor issue. Everything else stays banned, **including
issue creation**: an unattended run proposes findings in its report and a human asks for the filing separately. `AI
Agent` and `State` remain the runner's alone, exactly as for the code-issue action. The procedure itself (audit phases,
the coverage and carry-forward ledgers, the per-target cap) lives in the audits repo under `prompts/`, not in this
prompt, so it changes by pull request there.

The dependency gate (CLAUDE-161) is left enabled, which is what sequences a daily rotation: the workflow files one
parent plus one Epic per target and queues all of them, and `parent for` keeps the parent blocked until every Epic
resolves, so the run summary has something to summarize. `concurrency.max_parallel: 1` keeps one audit in flight at a
time; per-target caps and the coverage ledger, not serialization, are what keep a rotation affordable.

## Global pause and the restart-safety workflow (CLAUDE-223)

Applying an update does not restart the service, and restarting while jobs are running kills them mid-flight. The
**Activity** card at the top of `/about` is the control for doing it safely, and it is the same state
`claude-run pause` / `claude-run resume` / `claude-run status` read and write.

The workflow:

1. **Pause all activity.** The card's button posts to `/api/pause` with an optional free-text reason. From then on
   nothing new starts: the scheduler tick returns immediately, a manual trigger is refused with `409` naming the pause
   (instead of the old "queued trigger" ack for a run that was then silently dropped), an inbound delivery to either
   webhook listener is accepted with a `2xx` and dropped, and any run still in flight stops picking up its NEXT job.
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

**CLAUDE-190: dedup keys on the failure, not the commit.** The cross-run collapse key is
`hash(repo, branch, workflow_id, job_name, failure_signature)`, so N commits that each fail the same job on the same
branch with the same failure collapse onto ONE open issue (later runs append a run-association comment instead of minting
a duplicate), while a genuinely different failure signature on the same job stays a separate issue. The
`failure_signature` is a deterministic, pure-string signature of the first diagnostic line in the redacted log
(lowercased, path prefixes dropped, digit runs masked, whitespace collapsed): no LLM, computed on every event. When no
diagnostic line is derivable (a truncated / diagnostic-free / logless failure) the key falls back to
`hash(repo, branch, workflow_id, job_name)`, which still collapses a "branch is broken" storm onto one issue (two
indistinguishable logless failures on the same job are intentionally grouped; the CLAUDE-189 analysis can later split a
mis-grouped issue). The branch is read from the run's log environment dump (`GITHUB_HEAD_REF` on a PR run, else the `refs/heads/` short name, in either the `setupEnv => map[...]` or the `KEY=value` form; the deployed
Forgejo returns `head_branch: null` from run detail) and the job name from the jobs `fj actions logs` returns with the
log, both resolved in Rust. The issue body carries hidden `Dedupe-Key:`, `Root-Cause-Key:`, `Branch:`, and `Job:`
markers so the CLAUDE-191 auto-close reconciler can find every issue for a `(repo, branch, workflow, job)`; a resolved
issue is never reopened.

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

**CLAUDE-208: a stopped run is not a broken one.** Forgejo fires `action_run_failure` for every non-success done status, and it reports run-level `failure` for a run whose jobs were merely CANCELLED, which is what a newer push to the same ref produces when it supersedes an in-flight run. The run-level status alone therefore cannot tell a broken build from a stopped one, so the consumer reads the per-job statuses that come back with the log: when no job failed and every job ended `cancelled` or `skipped`, the delivery is acked and the run closed with a summary naming those statuses, with no triage and no issue. Any other status set (including an all-`success` run under a run-level `failure`) still triages and mints, and a job list that did not resolve at all (a rejected log token, a pruned run) fails open and mints as before.

**CLAUDE-149: a human's failing PR is left to its author; the bot only fixes its own.** When the failing run already has
an open PR, the webhook `code_fix` (queue-for-fix) route is suppressed: no issue is minted and no fix is queued, because
the author owns the red CI on their own branch. A `human_action` failure on an open PR still mints (it needs a human, not
the bot). The `forgejo-ci-fix` action is retargeted to match: it now auto-fixes ONLY the bot's own failing PRs (those
authored by `bot_author`, the PRs the bot opened working a queued issue) and skips everyone else's. On its own PR the bot
keeps pushing corrective commits until CI passes; after `max_pr_attempts` distinct fix pushes it stops and flags the PR
Needs Review (a PR comment, the `needs-review` label, and best-effort sets the source issue's State and assignee). The
`enabled` toggle defaults to `true` and is a master kill switch.

**CLAUDE-153: the webhook is a configurable action.** It is named `webhook-forgejo` (formerly `webhook-fast-path`), and a per-webhook `auto_fix` toggle on the `Webhook` settings gates whether a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) queues a corrective fixup PR. It defaults to `false` (opt-in): with `auto_fix` off, a red PR check mints nothing and queues no fixup PR, so the failure stays reported on the original PR for its author to fix in place (a `human_action` failure still mints); with it on, the corrective-fixup behavior above applies, still subject to the CLAUDE-149 open-PR suppression. This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. The `webhook-forgejo` action also participates in **Pause** like every other action: Pause/Resume controls and a Paused badge appear on the dashboard, and while it is paused an inbound `POST /webhooks/forgejo/<source>` is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, but the delivery is dropped without minting or queuing so Forgejo does not retry. CLAUDE-223: the GLOBAL pause gets the same accept-and-drop treatment, checked first. Edit / export / delete remain rejected (it is synthetic and has no backing YAML).

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
iterating until green or flagging Needs Review. Repeats of the same failure group by the CLAUDE-190 root-cause key
(`hash(repo, branch, workflow_id, job_name, failure_signature)`), not the commit SHA, so an auto-fix failure that recurs
on a later commit lands on the same open issue.

### Deep CI-failure analysis (CLAUDE-189)

A minted "CI failure" issue's `## Triage` prose is a one-shot classification: it does not read the complete run log, does
not cross-reference the failing workflow YAML to name the exact step that failed, and does not distinguish a
truncated / diagnostic-free log from a real compiler/clippy/test error. The `ci-failure-analysis` native action
(`kind: ci-failure-analysis`, `src/native/ci_failure_analysis.rs`) closes that gap: on a schedule it enriches every open,
unresolved "CI failure" / "Auto-fix" issue with a deep, log-grounded root-cause comment, produced automatically and
idempotently, including for issues already open when the feature ships (the CLAUDE-185 backfill case).

For each issue it: parses the run coordinates from the `## Failure` block; fetches the complete redacted run log
through `fj actions logs <run_number>` (the per-repo run number the block already carries, which is what `fj` takes)
plus the failing workflow YAML into a workdir; runs one agentic
`claude` session (tools `Read,Grep,Bash`) over both; and posts the analysis as a YouTrack comment carrying a hidden marker
so the issue is never analyzed twice. The analysis names the exact failing workflow step (by mapping log position to
workflow step order), the root-cause diagnostic or an explicit statement that the stored log is truncated / diagnostic
-free, and concrete remediation.

Selection is by summary prefix + analyzed-marker ONLY; it **never** selects by `AI Agent: Queued` (that field belongs to
the code-fix runner, so analysis, a read-only enrichment, must not collide with it). A log miss (auth denied or a pruned
run) posts a short "unavailable" note once and leaves the issue un-marked so a later run retries; it never crashes or
marks analyzed on a miss. Log reads go through `fj actions logs` and the workflow-YAML read through `fj repo file`
(CLAUDE-205); both are in Rust inside `forge_log`, never a raw REST call and never one from a nu snippet, and redaction
is preserved end to end via `forge_log::redact`, so no token lands in a posted comment. Configure it in `actions/ci-failure-analysis.yml` (`log_token_ref` picks the Forgejo PAT per repo, see
below; `query` / `summary_prefixes` / `max_analysis_bytes` tune selection and the comment cap). See `actions/SCHEMA.md`
(`kind: ci-failure-analysis`).

### Auto-close of fixed and duplicate CI-failure issues (CLAUDE-191, CLAUDE-198)

The mint path keeps only `action_run_failure` events and drops every success, so nothing in claude-run ever observes a
run going green: a "CI failure" / "Auto-fix" issue whose root cause is later fixed stays open until a human closes it by
hand. The `ci-failure-reconcile` native action (`kind: ci-failure-reconcile`, `src/native/ci_failure_reconcile.rs`) closes
that gap. On a schedule it scans open CI-failure issues, reads each issue's hidden `Branch:` / `Job:` markers (the
CLAUDE-190 rekey markers; a pre-rekey issue with no parseable branch/job is left for manual close), then for each
allow-listed repo pages the Forgejo runs list, reduces it to the LATEST run per `(branch, workflow_id)`, and reads that
run's per-job status from the jobs endpoint. Every open issue whose `(repo, branch, workflow, job)` is `success` on its
latest run is closed: State set to `resolved_state` (default `Done`) and a run-linked comment posted ("Auto-closed: run
#N on `<branch>` succeeded ...; the failing `<job>` is green, root cause resolved.").

Only the LATEST run decides, so a newer failure after an older green run leaves the job red and the issue open; matching is
exact on all four of `(repo, branch, workflow, job)`, so a green sibling branch or job never closes a still-broken issue.
The close is idempotent (a resolved issue drops out of the `#Unresolved` scan, and a hidden auto-closed marker guards
against a re-comment). When the runs list carries no plain branch name (`fj`'s `head_branch` is Forgejo's `prettyref`,
which is `#<pr-number>` for a PR run and the bare commit sha for a ref-deleted push), the branch is resolved from the
run's log environment dump, bounded by `max_branch_resolves`. The reconciler is polling by design (a success webhook
can be missed or arrive out of order, and success events are dropped at the ingress; polling reflects current truth); it
performs no git/PR actions and does not depend on the CLAUDE-189 analysis having run. Log and per-job status reads go
through `fj actions logs` and the runs list through `fj actions runs` (CLAUDE-205). Both stay inside `forge_log` (Rust),
never a raw REST call and never one from a nu snippet.

Duplicates are collapsed in the same pass (CLAUDE-198). Mint already declines to create a second issue for a root cause
that has an open one, but that check is a tracker full-text search on the hidden `Root-Cause-Key:` marker, so a lagging
index lets two failures of the same job mint two issues. Before any Forgejo read the reconciler groups the gathered
issues by `(repo, root_cause_key)` and closes every issue of a group but the OLDEST, posting `duplicate_comment` (which
names the survivor) and carrying the same hidden auto-closed marker; only the survivor goes on to the now-green check, so
a failing-then-passing sequence leaves no duplicate open. An issue with no `Root-Cause-Key:` marker (minted before the
CLAUDE-190 rekey) is never grouped, and grouping keys on the repo too, so a marker collision across repos closes nothing.
Set `close_duplicates: false` to leave duplicates open.

Configure it in `actions/ci-failure-reconcile.yml` (`repos` names the allow-list, `log_token_ref` picks the Forgejo PAT
per repo, see below). See `actions/SCHEMA.md` (`kind: ci-failure-reconcile`).

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

### Per-repo log tokens for the CI-failure actions (CLAUDE-200)

Both CI-failure actions scan a global `#Unresolved` query, so their candidate issues span every org while each org has its
own Forgejo PAT. `log_token_ref` therefore accepts either one secret name for every repo or an ordered per-repo mapping
(`repos:` patterns in the webhook allow-list forms plus a `token_ref:`, first match wins). Omitting the key derives the
mapping from `config.webhook.sources`, which already carries one `log_token_ref` per org: a token is rotated in one place
and ONE deployed copy of each action covers every org. A repo that matches no entry is skipped with a warning, never read
with another org's token, so `ci-failure-analysis` posts no "log unavailable" note on an issue it holds no token for and
`ci-failure-reconcile` spends no request that would 401. See `actions/SCHEMA.md` ("Per-repo log tokens").

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

## Comment commands on a pull request or an issue (CLAUDE-220, CLAUDE-221)

A second inbound listener turns a `/claude <instruction>` comment on a Forgejo thread into work. The endpoint is
`POST /webhooks/forgejo-comment/<source>`, served alongside (and independently of) the CI-failure webhook above. It has
two routes, chosen by whether the comment's thread is a pull request:

- **Pull request (CLAUDE-220):** a bounded Claude run against that PR's head branch, pushed back to the branch.
- **Plain issue (CLAUDE-221):** an issue has no head branch to clone or push, so nothing runs here. The command mints a
  YouTrack issue carrying the instruction and a link back to the Forgejo issue, queued for the agent.

The second route is a ROUTE, not a second listener: the request pipeline, authorization gate, trigger parsing,
bot-login loop drop, dedupe key, and caps ledger are the same implementations both paths run through.

It is a SEPARATE listener, section, and synthetic action rather than a second event type on `webhook-forgejo`, because
the credentials differ in kind: comment handling needs a WRITE token (post replies, push a branch) while the CI-failure
sources need only a read-only log token. Separate sections keep the write token out of the block holding the read-only
one, and each listener gets its own Pause switch. What the two DO share, by reference rather than by copy: HMAC
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
(default `["write", "admin"]`, matched case-insensitively and configurable because Forgejo's role model keeps growing).
A permission lookup that ERRORS denies (fail closed) and replies "could not verify permission". A denied commenter gets
ONE reply stating the required set, then the delivery is acked and dropped; denial replies are throttled per
`(repo, login)` so a scripted denied commenter cannot flood a PR.

**Execution on a pull request** reuses the `forgejo-ci-fix` pipeline shape: shallow-clone the PR head branch, write the human's
instruction VERBATIM to `.claude-run-instruction.md` in the checkout (it is never interpolated into a shell command or
a command line: the agent is told to read the file), run Claude, then apply the decision gate. A run that made no
changes, or whose diff exceeds the source's `max_files` / `max_lines`, is reported on the PR and nothing is pushed;
within the caps the runner commits as the bot identity with the CI skip marker (so the push does not re-trigger
`forgejo-ci-fix`) and pushes the branch. A PR comment is posted on EVERY outcome path: pushed, push failed, no changes,
too big, and agent error.

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
and **only** the Issue Comment event selected (Forgejo delivers pull-request and plain-issue comments through that one
event; the listener tells them apart from the payload). Store the write PAT first, e.g.
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
`--bare`) MCP servers, hooks, and skills stay enabled. If the host credential file is absent at serve start the daemon
fails loudly naming the path (run `claude` once to log in first). On macOS credentials live in the Keychain rather than
`.credentials.json`, so the symlink step is Linux-only.

**Image.** `oci-build/Dockerfile` has a `runtime` target built on the CI image (`dev.a8n.run/pandoras-box/rust-builder-musl`).
It carries `claude-run` plus the pinned runtime toolchain: `nu`, the `claude` CLI, coreutils `timeout`, `git`, `yt` (
youtrack-cli), and `fj` (forgejo-cli). Versions are build args (`NU_VERSION`, `CLAUDE_CODE_VERSION`, `YT_VERSION`,
`FJ_VERSION`); override any one with `--build-arg`. The two CI-failure actions read the forge only through `fj`, so they
need an `fj` carrying `fj actions logs`, `fj actions runs` (FJ-66), and `fj repo file` (FJ-67); the latter two landed on
`pandoras-box/forgejo-cli` `main` after the `v0.13.0` tag (merge `b08d8bc`) and are in no released version yet, so
`FJ_VERSION` must point at a build from that commit or newer for those actions to work. The container runs as the
non-root user `claudebot` with `$HOME=/home/claudebot`.

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
`/home/claudebot/.claude/CLAUDE.md`. It contains only the local-only agent contract (no push, no PR; leave the feature
branch as HEAD; the runner finalizes), the bounded tracker writes from
[What the agent may write to the tracker](#what-the-agent-may-write-to-the-tracker-claude-214), the em-dash ban, the
Nushell 0.112.2 note, and the simplest-solution preference. It has no Git workflow and no lifecycle section, so the
agent is never told to push, open PRs, or transition issue state.

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
| bind `~/.local/share/forgejo-cli/keys.json` (ro) | `/home/claudebot/.local/share/forgejo-cli/keys.json` | `fj` auth; the token needs `write:repository` + `write:issue` (see Install)                          |
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

See [CLAUDE.md](CLAUDE.md) for the project-specific deviations from `niceguyit/governance` and the phase history of the completed Rust port.

## License

MIT. See [LICENSE.md](LICENSE.md).
