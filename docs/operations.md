# Operations

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

## MCP key backup (CLAUDE-442)

The two key files the optional MCP HTTP listener generates on first start (`mcp-master.key`, `mcp-session.key`, both
under `CLAUDE_RUN_CONFIG_DIR`; see [configuration.md](configuration.md#mcp-over-http-claude-442)) back up the same way
`secrets.key` does and for the same reason: `mcp-master.key` decrypts `mcp-web.db`'s own encrypted-at-rest columns, so
losing it makes that database's contents permanently unreadable, and the daemon refuses to mint a silent replacement
on a corrupt or unreadable file rather than risk doing that by accident. Back these up alongside `secrets.key`
whenever `[mcp].enabled = true`; a deployment that leaves the listener off never generates them and has nothing to
back up.

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


## Global pause and the restart-safety workflow (CLAUDE-223)

Applying an update does not restart the service, and restarting while jobs are running kills them mid-flight. The
**Activity** card at the top of `/about` is the control for doing it safely, and it is the same state
`claude-run pause` / `claude-run resume` / `claude-run status` read and write.

The workflow:

1. **Pause all activity.** The card's button posts to `/api/pause` with an optional free-text reason. From then on
   nothing new starts: the scheduler tick returns immediately, a manual trigger is refused with `409` naming the pause
   (instead of the old "queued trigger" ack for a run that was then silently dropped), an inbound delivery to either
   webhook listener is accepted with a `2xx` and dropped (a dropped delivery is not recovered after Resume), the automerge
   reconciler tick and every queued event (Action Run failure, success, and pull request events, and `/claude` comment
   commands, which check `webhook-comment`) do nothing while the daemon or their listener's action is paused (an
   unreadable pause flag also skips them, CLAUDE-332, CLAUDE-335; a skipped queued event is not replayed after Resume), and any run still in flight stops picking up its NEXT job.
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
