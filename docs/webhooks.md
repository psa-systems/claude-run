# Webhooks

## Webhook (Forgejo or GitHub)

**CLAUDE-361: the route carries an explicit `{provider}` segment.** The CI-failure listener now lives at
`POST /webhooks/{provider}/triage/<source>` and the comment listener at `POST /webhooks/{provider}/comment/<source>`,
with `{provider}` naming the sending forge: `forgejo` or `github` (CLAUDE-362 adds `github`). Each source's own
`provider` config field, not the URL, is authoritative - a request against `/webhooks/github/<segment>` for a source
configured `provider: forgejo` 404s the same way an unconfigured source does. This replaces the earlier
`/webhooks/forgejo/<source>` and `/webhooks/forgejo-comment/<source>` paths, and the synthetic action names moved with
them: `webhook-forgejo` is now `webhook-triage`, and `webhook-forgejo-comment` is now `webhook-comment`. The old
routes 404; update every configured Forgejo webhook URL to the new paths (e.g.
`https://<daemon-host>/webhooks/forgejo/triage/<source>`, `https://<daemon-host>/webhooks/forgejo/comment/<source>`)
after upgrading. On-disk stream logs under either action's earlier name are carried forward to the new name
automatically at the next `serve` start.

The daemon turns a Forgejo `action_run_failure` delivery into a triaged YouTrack issue in seconds. The pipeline is
`POST /webhooks/forgejo/triage/<source>` (HMAC-verified, replay-protected, rate-limited) -> internal-route log fetch +
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
`runner-unavailable`. Before matching, the detector drops cargo test-result lines (`test ... ... ok` / `FAILED` /
`ignored[, ...]`, with or without a leading timestamp) and the bare `<target>::<test>` paths cargo lists under
`failures:`, so a test whose NAME happens to contain a detector phrase (for example a test asserting on `ENOSPC`
handling) never matches, whether that test passed or failed; only text the runner or toolchain itself emitted as a
diagnostic counts. An over-broad pattern would merge unrelated code failures across branches, so an undetected
condition is the preferred failure mode. The `ci_failure` store dedupes an infra failure the same way as a code failure
now (by its `infra:<kind>` fingerprint id, CLAUDE-272's `fingerprint::fingerprint`), so the branch-agnostic collapse
described here matters mainly to the legacy-adoption fallback, not to the primary path.

Routing changes only for `runner-unavailable` (CLAUDE-266): `triage_rules` runs the detector before its transient scan and
routes that kind to `human_action` with no LLM call, so an unreachable registry mints one grouped, human-assigned issue
instead of vanishing as transient. `could not resolve host` is no longer a transient signature; a bare `connection reset`
with no registry on the line stays transient. Every other infra failure is still classified and assigned the same way,
because reclaiming disk is a human action.

The listener is multi-source (CLAUDE-87): each `<source>` is a `[webhook.sources.<source>]` config block with its own
HMAC secret and allow-list, served at its own URL path. One Forgejo org webhook per source means a leaked org secret
authenticates only that org's allow-listed payloads, and each source has its own rate-limit bucket so one org's failure
storm cannot rate-starve another's deliveries. The replay window is shared across sources (the `X-Forgejo-Delivery` id
is globally unique, so a duplicate delivered to two sources is still caught). The source name is the URL path segment
verbatim, so use lowercase `[a-z0-9_-]`; a name needing URL escaping is warned about at load.

Triage sorts each verified failure into one of three buckets: a failing PR check that a code change fixes (`code_fix`)
mints an issue and sets `AI Agent = Queued` so the runner opens the fix PR; a failure no PR fixes (`human_action`,
e.g. a main-branch / tag / release / missing-secret failure) mints a human-assigned issue with no PR; a transient /
infra / flaky failure (`transient`) mints nothing, except a `runner-unavailable` infra failure, which is `human_action`. The reused per-key + daily caps throttle a storm of failures; the daily cap is `config.webhook.max_runs_per_day` (default 20, at least 1), editable on the Settings page and applied to the next event with no restart.

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

**CLAUDE-360: the listener has its own `enabled` switch.** `config.webhook.enabled` defaults `false`; while off, `POST /webhooks/forgejo/triage/<source>` 404s every request regardless of `sources` (logged as `"webhook rejected: disabled"`, distinct from an unknown-source 404), ahead of any source lookup. A Settings save that sets it `true` is rejected unless `sources` is non-empty and every source carries `secret_ref`, naming every offending source, so "enabled" can never mean "accepting requests with no real authentication configured behind it." At `serve` start, a stored document with `sources` already configured but `enabled` unset (the state an upgrade from a version with no such flag deserializes to) logs one `warn` naming the section.

**CLAUDE-153/CLAUDE-273: the webhook is a configurable action.** It is named `webhook-triage` (formerly `webhook-fast-path`), and a per-webhook `auto_fix` toggle on the `Webhook` settings decides only whether a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) gets `AI Agent = Queued`. It defaults to `false` (opt-in): with `auto_fix` off, a red PR check still creates an issue but with no `AI Agent` write, so no fixup PR is queued and the failure stays reported on the original PR for its author to fix in place (a `human_action` failure is unaffected either way); with it on, the corrective-fixup behavior above applies. This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. The `webhook-triage` action also participates in **Pause** like every other action: Pause/Resume controls and a Paused badge appear on the dashboard, and while it is paused an inbound `POST /webhooks/forgejo/triage/<source>` is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, but the delivery is dropped without minting or queuing so Forgejo does not retry. A dropped delivery is not recovered after Resume. CLAUDE-223: the GLOBAL pause gets the same accept-and-drop treatment, checked first. Edit / export / delete remain rejected (it is synthetic and has no backing YAML).

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

### Resolving a CI-failure issue on merge (CLAUDE-277, CLAUDE-327)

`POST /webhooks/forgejo/triage/<source>` accepts Forgejo `pull_request` deliveries, dispatched by the delivery's event-type
header (`X-Forgejo-Event`, mirroring Gitea's `X-Gitea-Event`; a deployment must confirm this against its own Forgejo's
actual delivery, since Forgejo's webhook headers are undocumented upstream - see the operator note below). A
`pull_request` delivery runs through the SAME signature / replay / rate-limit / pause / allow-list gates the Action-run
path does; only a terminal `action: "closed"` delivery does anything, everything else is acked and dropped.

The runner itself never resolves an issue on a PR merge. A YouTrack workflow does that instead
(`docs/yt-workflow-resolve-on-pr-merge.js`), for any issue whose own PR merges - not only ones the runner opened - by
setting `State` to `Done` when the merged PR's title or body claims the issue (a title token, a bare `#KEY-N` line, or
`Closes KEY-N`; the runner's own PR bodies already end with `Closes` and the bare issue key). On a merged or closed
delivery the runner only updates its own `agent_pull_requests` bookkeeping: a merge marks the row `merged`, a close
without a merge marks it `closed`. Neither makes a tracker call or a forge run-list call.

Because the store's own `ci_failures` row for a matched failure no longer flips to `resolved` on a merge the runner
sees, a CI-failure dispatch that matches an `open` row reads that issue's live YouTrack `State` before deciding what to
do: if it already equals `mint.resolved_state` (default `Done`, case-insensitive), the match is a regression and is
reopened exactly as a `resolved` store row is; otherwise it attaches or comments as before. A failed read never falls
back to "still open": it logs an error, records a `run_events` row, and fails the run rather than risking a false
attach or a duplicate comment on an issue that is actually done.

**Operator setup:** each Forgejo webhook pointed at `/webhooks/forgejo/triage/<source>` must also send pull-request events
(**Pull request events > Modification** in Forgejo's webhook form, which covers opened, closed, reopened and edited),
alongside the Action-run events it already sends, so the `agent_pull_requests` bookkeeping stays current. Every project
the runner works must also have the YouTrack workflow attached, or an issue there stays open after its PR merges.

### Event-driven automerge (CLAUDE-126, CLAUDE-278)

A `youtrack` action with `automerge.enabled: true` opts a successful run into merging its own PR once CI passes, instead
of leaving it at Code Review for a human. Before CLAUDE-278, that wait was a blocking poll inside `finalize`: it held a
job slot for up to `automerge.wait_seconds` (1800s in the shipped action), and a suite slower than that budget - as
happened for billing-service's ~25-minute integration suite (GAMMA-1174) - timed out with the PR left at Code Review and
nothing watching it afterward.

`finalize` no longer waits for CI at all: a successful run opens the PR and stops at Code Review immediately, whether or
not automerge is enabled. The merge instead happens later, normally driven by a `success` or `recover` Action-run
webhook event on `POST /webhooks/forgejo/triage/<source>` - the same endpoint the CI-failure listener uses, dispatched on the
payload's `action` field rather than dropped as "non-failure" the way it used to be - and, as a backstop for a webhook
that is missed, dropped, or delivered before the PR's `agent_pull_requests` row exists, by a reconciler pass on the
daemon's own tick (CLAUDE-328, see below). Both entry points make the same decision through one shared function,
`apply_automerge_decision`. On a webhook event, the daemon:

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
   - **`behind_base: false` and mergeable:** merges it as a merge-commit and posts a "runner merged" comment; it makes
     no `State` or `AI Agent` write (CLAUDE-327: the YouTrack workflow resolves the issue once it sees the merge, not
     the runner). A merge failure here (Forgejo refusing for a reason `mergeable` did not predict) comments the failure
     and fails the run.
   - **Anything else** (checks not `success`): no action. There is no budget to exhaust and nothing to time out - a
     later `success` event on the same PR is what retries, for as long as CI takes.

Because the wait is event-driven, no job slot, timeout, or `check_interval_seconds` poll is involved any more; both
`automerge.wait_seconds` and `automerge.check_interval_seconds` are deprecated (parsed, for a still-loading action file,
but otherwise ignored - one deprecation warning is logged per action at load if either is set away from its default).

**Reconciler backstop (CLAUDE-328):** the webhook is the low-latency path, not the only one. Every
`AUTOMERGE_RECONCILE_TICK_SECS` (300s) the daemon lists every `open` row in `agent_pull_requests`, skips any whose
action is not a `youtrack` action or has automerge disabled, and runs the same `apply_automerge_decision` a webhook
event would run - so a PR still merges within one tick even if its `success` event was never delivered, was dropped, or
arrived before the PR's row was recorded. The reconciler is idempotent: a PR that is not yet green is left exactly where the
webhook path would have left it, and a PR that `pr_status` reports as `merged` (or that the runner just merged) has its
row marked `merged` so later ticks skip it (CLAUDE-331). Each tick also lists the forge's open PRs once per repo and
marks a row `closed` when its PR is no longer listed (a PR closed without merge, for which `fj pr status` fails), so it is
not asked about again (CLAUDE-334); a failed listing leaves that repo's rows `open` and is logged at `error`. The daemon's reconciler loop tracks, in memory, how many consecutive passes each `(host, repo)` listing has failed; the first later pass where that pair's listing succeeds logs at `info` and writes an `info` `run_events` row reading `automerge reconciler: list_open_prs for <repo> recovered after <n> failed pass(es)`, so a cleared fault is as visible as the fault itself (CLAUDE-365). The tracking is in-memory only, so a daemon restart never reports a false recovery for a failure that predates it. Every fault this pass or the webhook path can hit while a
recorded, automerge-enabled PR is involved (a forge-host lookup failure, a `list_open_prs` failure, a failed action
load, a failed `pr_status` call, a failed merge or branch update) is logged at `error`, written to `run_events`, and
posted as a PR comment naming the failure, rather than left as a silent `warn`. A `pr_status` failure that repeats with an identical message for the same PR (for example `no commits in pr`) is still logged and recorded on every tick, but commented on the issue only once (CLAUDE-333); a changed message comments again.

**Operator setup:** each Forgejo webhook pointed at `/webhooks/forgejo/triage/<source>` should still send Action Run
**Success** and **Recover** events, in addition to Failure, for any org running a `youtrack` action with automerge
enabled (see the migration note above), so a green PR merges within seconds rather than waiting for the next
reconciler tick. Without them, the reconciler is what keeps a PR opened by such an action from sitting at Code Review
forever; automerge itself does not depend on the webhook.

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
   `https://<daemon-host>/webhooks/forgejo/triage/<source>`, Secret set to that org's stored value, and the Action Run Failure
   event selected. If any `youtrack` action in this org has `automerge.enabled: true`, also select Action Run Success
   and Action Run Recover (CLAUDE-278: that is the low-latency trigger for merging once CI goes green - see
   "Event-driven automerge" below; the CLAUDE-328 reconciler backstop merges within one tick either way, but the
   webhook avoids that wait); otherwise leave them unselected so `success` / `recover` deliveries stay off the wire
   entirely (any that do arrive are acked and dropped, same as before).
3. Replace the old `[webhook]` block in `config.toml` with the sources shape above and restart the daemon. It refuses to
   start while any old key is present, naming it.
4. Delete the SITE-level system webhook (and any old per-repo pilot webhook).
5. Verify: one failing run per org produces a `verified action_run_failure received` log line carrying the right`source`
   field. The old shared secret can be deleted after a burn-in period.

### GitHub setup (CLAUDE-362)

A source's `provider` field, not the URL, decides which signature scheme and event names the listener expects; set
`provider: github` on the source and point the URL at `https://<daemon-host>/webhooks/github/triage/<source>` instead
of `/webhooks/forgejo/...`. A Forgejo source and a GitHub source can share one `[webhook]` config, one daily run cap,
and one Pause switch, each keyed by its own `sources.<name>` entry.

1. Generate and store one secret: `printf %s '<distinct-random-value>' | claude-run secret set github-hmac-<org>`.
2. In GitHub, on the repo or org's Settings -> Webhooks, add a webhook with Payload URL
   `https://<daemon-host>/webhooks/github/triage/<source>`, Content type `application/json` or
   `application/x-www-form-urlencoded` (GitHub's default; both are accepted), Secret set to that stored value, and
   select the single **Workflow runs** event (`workflow_run`). GitHub folds `requested` /
   `in_progress` / `completed` into that one event type rather than Forgejo's separate failure/success/recover
   events, and the listener only acts on a `completed` delivery, reading the outcome from
   `workflow_run.conclusion` rather than from the event name. If any `youtrack` action in this org has
   `automerge.enabled: true`, no extra event selection is needed: the same `workflow_run` delivery covers a
   successful conclusion too.
3. Set `secret_ref` to the stored secret's name and `log_token_ref` (a token with the `actions:read` and
   `contents:read` scopes) on the source.
4. Verify: GitHub signs each delivery with `X-Hub-Signature-256: sha256=<hex hmac>` rather than Forgejo's bare-hex
   `X-Forgejo-Signature`, and identifies the delivery with `X-GitHub-Delivery` / `X-GitHub-Event` rather than
   `X-Forgejo-Delivery` / `X-Forgejo-Event`; the listener verifies and parses accordingly for a `github`-provider
   source, so a failing workflow run should still produce a `verified action_run_failure received` log line
   carrying the right `source` field.

Fetching a GitHub-hosted run's own log (the text the triage classifier reads) goes over `gh`/the GitHub API for a
`github.com` host; see the `GitHub log fetch (CLAUDE-363)` note in `actions/SCHEMA.md`.
