# Action YAML Schema

`scripts/claude-run.nu` is a generic runner. Behavior is defined by an action YAML file passed as the only argument:

```nu
claude-run.nu /path/to/action.yml
# or, if the action lives in $CLAUDE_RUN_ACTIONS_DIR:
claude-run.nu youtrack
```

Each job produced by `input.command` becomes one Claude invocation. The runner provides locking, daily caps, per-job
attempt caps, timeouts, and a cost-based usage check. The action YAML supplies everything specific to the job.

## File shape

```yaml
name: <kebab-id>            # required
kind: scripted              # optional: scripted (default) | youtrack | closed-issue-summary
enabled: true               # optional, default true; false skips the action in the scheduler
workdir_base: /path/to/dir  # optional, overrides $CLAUDE_RUN_WORKDIR_BASE for this action
link_template: https://tracker.example.com/issue/{key}  # optional override; {key} -> job key (render_link). Default for YouTrack actions: the Settings -> Auth card's YouTrack URL + /issue/{key}. Drives every tracker-keyed job-key link in the web UI: the run-detail tables, the Dashboard's dependency-gate and attempt-cap banners (Issue/Job columns and the gate's blocker keys), /logs (Context column and the row's own key inside Message), the usage report's Job key column, and the job stream page's heading/breadcrumb.
input:
  command: |                # required, nu snippet, stdout = JSON array
    #!/usr/bin/env nu
    ...
dedup_key: <field>          # optional, job field used to track attempts
filter: |                   # optional, nu snippet; exit non-zero to skip job
  ...
pre: # optional, ordered list of nu snippets
  - |
    ...
prompt: |                   # required, nu snippet that returns the prompt string
  ...
claude: # optional, all keys optional
  cwd: ...
  allowed_tools: "Read,Edit,Write,Bash,Glob,Grep"
  permission_mode: acceptEdits
  model: opus
  effort: high
  autocompact: 200000       # compaction window: auto, or 100000-1000000 tokens
post: # optional, ordered list of nu snippets
  - |
    ...
output:
  file: <name>.log          # optional, appended JSONL of per-job outcomes
limits: # optional per-action overrides of the built-in defaults
  max_attempts_per_key: 3
  max_runs_per_day: 20
  claude_timeout_seconds: 600
  daily_budget_usd: 5.00
  run_budget_usd: 5.00      # spend ceiling for ONE run; unset = daily_budget_usd
  budget_warn_fraction: 0.8 # warn once at this fraction of run_budget_usd
  max_issues_per_run: 25
concurrency: # optional; default is strictly sequential
  max_parallel: 1           # distinct-key jobs in flight at once, daemon-wide (default 1)
  key: [ project, repo, workflow ]   # job fields composing the concurrency key
schedule: # optional; omit for trigger-only (manual)
  kind: interval            # interval | cron | manual
  spec: 30m                 # interval/cron spec; omit for manual
```

## Script fields: `run` / `shell`

The script-bearing fields (`input.command`, `filter`, each `pre`, `prompt`, each `post`) each accept two forms:

- a plain YAML string holding the code (implicit `shell: nu`), or
- a `{run:, shell:}` mapping, following Docker Compose's `run` / `shell` convention: `run` is the code, `shell` is the interpreter.

`shell` defaults to `nu` when omitted, so existing string-form actions keep working unchanged. `shell` is a bare binary
name resolved on `PATH` (no arguments); e.g. `nu`, `bash`, `sh`.

```yaml
filter: exit 0                 # string form, runs with nu

prompt: # mapping form
  run: |
    print $"Work ($job.id)"
  shell: nu

pre:
  - run: |                     # this step runs with bash
      set -euo pipefail
      echo "$job" | jq -r .id
    shell: bash
```

**Bindings across interpreters.** For `nu` (the default), bindings (`$job`, `$workdir`, `$claude_result`) are loaded by a
*generated prelude, exactly as before. For any other interpreter the prelude does not apply, so each binding is exposed
*instead as an environment variable of the same name holding the compact-JSON value (decode with `jq`): in a `bash`
*snippet, `$job` is the JSON string `{"id":"..."}`.

## Field reference

### `name` (string, required)

Identifier used for the lock file, the log directory, and every per-action row in the state store (index, pause,
attempt counters, daily ledger). Should be kebab-case and unique across all actions on the host.

The action index is keyed by this exact string, so two files whose `name:` differ only by case (`youtrack` vs
`YouTrack`) are indexed as two independent, separately-scheduled actions. At scan time the daemon flags (but does not
merge or disable) two kinds of redundant configuration among enabled actions (CLAUDE-164): (1) names equal ignoring
case, and (2) two native actions of the same `kind` polling the same source (for `kind: youtrack`, the same
`native.youtrack.query` and `base_url`). Either condition
means both actions gather the same jobs and silently compete on the shared daemon-wide concurrency caps. The daemon
emits a WARN log and a persisted notice on each colliding action (visible on the action page); no action is
auto-disabled - rename, consolidate, or disable one yourself.

### `kind` (enum, optional, default `scripted`)

The dispatch discriminator (CLAUDE-30). Selects how the runner executes the action:

- `scripted` (default): runs the generic nu-snippet pipeline (`input.command` -> `filter` -> `pre` -> `prompt` ->
  `claude` -> `post`) described in the rest of this document. **Deprecated**: it is kept for back-compat, but new
  actions should prefer a native kind once one exists for their source.
- `youtrack`: a built-in Rust handler for YouTrack. The script-bearing fields (`input.command`, `prompt`, `filter`,
  `pre`, `post`) are not used; the action instead supplies a typed [`native`](#native-record-required-for-native-kinds)
  block.
- `closed-issue-summary`: a built-in Rust handler that counts and lists every issue closed in a rolling window across a
  configured project list, files that report as a new issue, and hangs it off a nominated parent as a subtask
  (CLAUDE-218). The automation used as a reporting tool rather than as an issue worker: it clones no repository, starts
  no agent session, and opens no pull request. Config only, supplying a typed
  [`native`](#native-record-required-for-native-kinds) block.

`scripted` and native kinds share all the run accounting (locking, daily caps, per-job attempt caps, budget, job-outcome
records); only job production and the per-job pipeline differ.

Every native kind is editable from the web UI through a structured form (CLAUDE-211): pick it at `/actions/new` to
create one, or open `/actions/<name>/form` for an existing action. `scripted` has no form and is **raw YAML only** (its
fields are nu snippets); the raw editor at `/actions/<name>/edit` remains available for every kind, and for the few
advanced fields a form does not surface (they are preserved untouched when the form saves). Forms and the raw editor
share one validation and write path, so neither accepts what the other would reject.

### `native` (record, required for native kinds)

Typed configuration for a native `kind`. Required when `kind` names a native handler, validated against that handler.
Because `kind` is already the discriminator, the block holds the handler's config directly (no nested variant tag). Omit
`native` entirely for `kind: scripted`.

#### `kind: youtrack`

Three sub-records: `youtrack` (the issue source), `forgejo` (the Forgejo PR target), and `github` (the GitHub PR
target). Every field defaults, so a bare `native: {}` validates and reproduces the behavior the scripted youtrack action
had. The generic blocks (`workdir_base`, `link_template`, `limits`, `schedule`, `claude`, `output`) stay at the top
level of the action, not inside `native`.

```yaml
kind: youtrack
native:
  youtrack:
    base_url: https://yt.example          # optional; required when token_ref is set
    token_ref: yt-prod                    # optional; name of an encrypted secret
    query: "-Resolved AI Agent: Queued"   # issues to pick up
    custom_field: "AI Agent"              # field holding the state machine
    assignee: octocat                     # optional; assign the issue on pickup (login, me, or unassigned)
    prompt_preset: default                # optional; built-in prompt used when `prompt` is unset: default | audit
    prompt: |                             # optional; full-replacement override, wins over prompt_preset
      Work issue {{key}}: {{summary}}
      Repo cloned at {{workdir}} ({{clone_url}}).
    states: # AI Agent / State labels the handler drives
      queued: Queued
      working: Working
      code_review: Code Review
      needs_review: Needs Review
      failed: Failed
      done: Done                          # unused unless a no_code label falls back to it (states.no_code)
      in_progress: In Progress            # the standard State field value set in pre
      state_done: Done                    # deprecated and ignored (CLAUDE-327); a YouTrack workflow
                                           # resolves the issue on merge, not the runner
    automerge:                            # optional; CI-gated automerge of the opened PR (CLAUDE-126)
      enabled: false                      # off by default; finalize stops at Code Review either way.
                                           # when true, a later success/recover webhook event merges
                                           # once checks are green (CLAUDE-278; finalize never polls);
                                           # merging writes no State or AI Agent field (CLAUDE-327)
    dependencies:                         # optional; pre-clone dependency gate (CLAUDE-161)
      enabled: true                       # on by default; false makes the gate a no-op
      recheck_timeout: 5m                 # backoff before a blocked issue is re-evaluated
      block_on:                           # direction-resolved link verbs that block
        - depends on                      # Depend INWARD: an open blocker gates this issue
        - parent for                      # Subtask OUTWARD: an open child gates this parent
      block_cross_project: true           # false: a link to an issue in another project never blocks
    tags:                                 # optional; tags written alongside an AI Agent value (CLAUDE-373)
      needs_info: Needs-Info              # tag added with the needs-review-marker park; empty writes no tag
  forgejo:
    base_url: https://forge.example       # optional; default derives host per issue VCS
    token_ref: fj-prod                    # optional; name of an encrypted secret
    pr_strategy: branch                   # optional; branch (default) | agit
  github:                                 # optional; all fields default
    hosts:                                # extra GitHub hosts beyond github.com (GHE)
      - ghe.example.com
    mcp_endpoint: https://api.githubcopilot.com/mcp/  # override the remote GitHub MCP URL
    token_ref: gh-prod                    # optional; name of an encrypted secret (GitHub PAT)
```

| Key                     | Default                            | Meaning                                                                                             |
|-------------------------|------------------------------------|-----------------------------------------------------------------------------------------------------|
| `youtrack.base_url`     | host-configured `yt`               | YouTrack base URL. Required when `token_ref` is set (the injected `yt` config needs a `url`).       |
| `youtrack.token_ref`    | none                               | Name of an encrypted secret holding the YouTrack API token (CLAUDE-26). `None` uses host `yt` auth. |
| `youtrack.query`        | `-Resolved AI Agent: Queued`       | Issue query selecting the jobs this action picks up. Two youtrack actions must never both match one issue: they poll on the same interval, so the winner is a race and the loser's repo or prompt is the wrong one. The shipped pair partitions on project plus tag (CLAUDE-215): `audit-run.yml` selects `-Resolved AI Agent: Queued project: AUDIT tag: Audit-*` (the YouTrack workflow tags the daily audit parent and every per-target Epic `Audit-<Type>`), and `youtrack.yml` selects the exact complement, `(project: -AUDIT or tag: -Audit-*) and -Resolved AI Agent: Queued`. Two syntax rules: YouTrack rejects `-project: AUDIT` as a parse error, so the negation goes on the value, and a parenthesized group must be followed by an explicit `and` (`#Unresolved (a or b)` is rejected). |
| `youtrack.custom_field` | `AI Agent`                         | Custom field holding the AI Agent state machine.                                                    |
| `youtrack.assignee`     | none                               | Optional Assignee set on the issue during pickup (CLAUDE-131). A literal `yt` assignee argument: a login, `me`, or `unassigned`. When unset (default), the Assignee is never touched. An assignee write failure aborts pickup before the clone. Editable in the structured web form. |
| `youtrack.prompt`       | none                               | Full-replacement agent prompt override (CLAUDE-125). Resolution order: this text when set, otherwise the built-in preset named by `prompt_preset`. When set, the text fully replaces the preset (so the operator owns the runner contract: the commit body used as the PR description (the runner joins any hard-wrapped prose lines back together before posting it), ending with a `Closes` line followed by the bare `#<KEY>` line, the `.claude-run-needs-review` / `.claude-run-skip-attempt` blocker markers, the `.claude-run-no-code-change` no-code marker, no push / no PR, and whichever tracker writes the operator chooses to permit), and it no longer changes when the binary's preset improves. Editable in the structured web form, which shows the prompt source (`override` or `builtin:<preset>`) and stores no override when the text equals the action's preset. See the placeholders below. |
| `youtrack.prompt_preset` | `default`                         | Built-in prompt used when `prompt` is unset. `default` is the engineering prompt (`default_youtrack_prompt`, `src/native/youtrack.rs`); `audit` is the scheduled-audit prompt (`default_audit_prompt`, `src/native/audit_prompt.rs`), selected by `actions/audit-run.yml`. Preset text is compiled into the binary, so an upgrade updates it. Omitted on serialize when `default`. Any other value is rejected at load, naming `native.youtrack.prompt_preset`. No structured-form UI; the value is carried forward across form saves. `claude-run action show <name>` prints `prompt_source` (`override`, `builtin:default`, `builtin:audit`) and `prompt_preset` read from the YAML. |
| `youtrack.write_projects` | empty                            | Projects the agent may create issues in for a descoped piece of the triggering issue (CLAUDE-214), and the fallback for an out-of-scope bug when no project or more than one project binds the buggy repo (CLAUDE-297); rendered into the prompt as `{{write_projects}}`. Empty (the default) resolves to the **triggering issue's own project**, so filing a deferred item satisfies the `No orphan notes` rule with no site-specific configuration and none is baked into the binary. The list is a prompt instruction, not an enforced grant: the hard boundary is the YouTrack token's own permissions. Exposed in the structured editor as a comma-separated "Write projects" field (CLAUDE-298). |
| `youtrack.states.*`     | see above                          | Labels the handler reads/writes for each AI Agent / State value. `done` is unused except as the fallback default for `states.no_code`; the runner writes no `State` or `AI Agent` on merge, whether by automerge or otherwise, whether through the webhook or the CLAUDE-328 reconciler (CLAUDE-327: a YouTrack workflow resolves the issue instead). |
| `youtrack.states.state_done` | `Done`                        | **Deprecated and ignored (CLAUDE-327):** the runner no longer writes `State` on merge; a YouTrack workflow resolves the issue instead. Still parsed (an action file setting it still loads) but otherwise ignored, with one deprecation warning at load when it is set away from its default. Saving the action's structured form resets it to the default; absent from the structured web editor. |
| `youtrack.states.no_code` | the `done` label                 | AI Agent label set when a successful run deliberately produced no commits and left `.claude-run-no-code-change` (CLAUDE-214). Defaults to whatever `states.done` is, because there is no PR for a human to review. `State` is left untouched, so the issue stays In Progress until a human closes it. Exposed in the structured editor as the "No-code label" field, blank meaning inherit the Done label (CLAUDE-298). |
| `youtrack.automerge.enabled` | `false`                       | Master switch for CI-gated automerge (CLAUDE-126). When `false`, the PR is never merged by the runner (unchanged behavior). When `true`, `finalize` still stops at Code Review immediately; the merge happens later, normally driven by a `success`/`recover` Action-run webhook event (CLAUDE-278) and, if that event is missed, by the reconciler's own periodic pass (CLAUDE-328) - the webhook is a low-latency trigger, not the only one. |
| `youtrack.automerge.wait_seconds` | `1800`                   | **Deprecated (CLAUDE-278):** the runner no longer polls, so this is parsed (an action file setting it still loads) but otherwise ignored; setting it away from its default logs one deprecation warning at load. Saving the action's structured form resets it to the default. |
| `youtrack.automerge.check_interval_seconds` | `30`           | **Deprecated (CLAUDE-278):** same as `wait_seconds`.                                                  |
| `youtrack.dependencies.enabled` | `true`                    | Master switch for the pre-clone dependency gate (CLAUDE-161). When `true`, a queued issue with any unmet dependency is left Queued (not cloned, no claude spend) and re-evaluated after `recheck_timeout`. When `false`, the gate is a no-op and pickup proceeds exactly as before. See [Pre-clone dependency gate](#pre-clone-dependency-gate-claude-161). Editable in the structured web form (CLAUDE-298). |
| `youtrack.dependencies.recheck_timeout` | `5m`              | Humantime backoff before a blocked issue is re-evaluated. While the window is open the issue stays Queued with no `yt` read and no comment (no per-poll spam). Validated at action load, and by the structured web form on save (CLAUDE-298). |
| `youtrack.dependencies.block_on` | `["depends on", "parent for"]` | Direction-resolved link verbs (YT-40) that block. A queued issue is blocked when it has any link whose `verb` is listed here and whose target is unresolved. The defaults block `depends on` (Depend INWARD, an open blocker) and `parent for` (Subtask OUTWARD, an open child); the reverse verbs `is required for` and `subtask of` never block. Editable in the structured web form as a comma-separated list (CLAUDE-298). |
| `youtrack.dependencies.block_cross_project` | `true`        | When `false`, a link to an issue in a different project (compared by key prefix: `GAMMA` in `GAMMA-12`) never blocks, whatever its verb. `actions/audit-run.yml` sets it (CLAUDE-295): an audit Epic in `AUDIT` is `parent for` the findings it filed into the product projects, and must not wait on them. Editable in the structured web form (CLAUDE-298). |
| `youtrack.tags.needs_info` | `Needs-Info`                  | Tag attached (`yt issue tag add <key> <tag> --create`) alongside the `Needs Review` AI Agent value, but only when the agent itself parked the issue via `.claude-run-needs-review` (CLAUDE-373). The attempt-cap and timeout parks also set `Needs Review`, but never write this tag; neither does a normal or no-code completion. Empty disables the tag write. A failed tag write is logged and does not block the `AI Agent` write. Editable in the structured web form as "Needs-Info tag". |
| `forgejo.base_url`      | per-issue VCS host                 | Forgejo base URL.                                                                                   |
| `forgejo.token_ref`     | none                               | Name of an encrypted secret holding the Forgejo API token (CLAUDE-26). `None` uses host `fj` auth. The token must carry the **`write:repository`** and **`write:issue`** scopes (see [Forgejo token scopes](#forgejo-token-scopes-claude-196)). |
| `forgejo.pr_strategy`   | `branch`                           | PR-creation strategy (CLAUDE-103). `branch`: resolve the feature branch by name (independent of the current `HEAD`; CLAUDE-166), push it as `git push --set-upstream origin <branch>:<branch>`, and open the PR over the API with an explicit `--head`, so the PR stays mergeable and updatable. `agit`: `fj pr create -aA`, a branchless PR whose head lives only at `refs/pull/<N>/head` and loses its merge button if it falls behind an up-to-date base. The selected strategy is authoritative (no silent cross-strategy fallback). Both strategies are gated by the same pre-dispatch ahead check (CLAUDE-212): PR creation is attempted only when exactly one local branch has commits ahead of the base, so a run that produced no commits fails loudly instead of opening an empty PR. |
| `github.hosts`          | empty                              | Extra GitHub host(s) beyond `github.com` to treat as GitHub (e.g. GitHub Enterprise). Bare hosts.   |
| `github.mcp_endpoint`   | `https://api.githubcopilot.com/mcp/` | Remote GitHub MCP endpoint used to open the PR (no local server to install).                      |
| `github.token_ref`      | none                               | Name of an encrypted secret holding the GitHub PAT (CLAUDE-26). `None` falls back to the daemon's own GitHub service login (CLAUDE-307), resolved lazily via `gh auth token --hostname <host>`. |

##### Prompt placeholders (CLAUDE-125)

The prompt is a template, whether it comes from a `youtrack.prompt` override or from any built-in `prompt_preset`. These
`{{...}}` placeholders are substituted with each job's values before the prompt is sent to the agent; an unknown
placeholder (or an unterminated `{{`) is left literal and logged at warn:

| Placeholder              | Substituted with                                                              |
|--------------------------|-------------------------------------------------------------------------------|
| `{{key}}`                | Issue id (`idReadable`, e.g. `MK-16`).                                         |
| `{{summary}}`            | Issue summary.                                                                 |
| `{{clone_url}}`          | Full clone URL of the resolved repo.                                          |
| `{{workdir}}`            | Absolute path of the per-job working directory (the repo is cloned here).     |
| `{{timeout_minutes}}`    | The action's effective `claude_timeout_seconds` divided by 60, rounded down (CLAUDE-377). |
| `{{needs_review_marker}}`| The `.claude-run-needs-review` marker filename.                               |
| `{{skip_attempt_marker}}`| The `.claude-run-skip-attempt` marker filename.                               |
| `{{no_code_marker}}`     | The `.claude-run-no-code-change` marker filename (CLAUDE-214).                |
| `{{write_projects}}`     | Comma-separated `youtrack.write_projects`, or the triggering issue's own project when that list is empty (CLAUDE-214). Scopes a descoped piece of the triggering issue, and is the fallback project for an out-of-scope bug whose repo no project or more than one project binds (CLAUDE-297). |
| `{{existing_pr_url}}`    | Empty on a fresh pickup. On a re-queue with an open recorded PR (CLAUDE-275), the URL of that PR. |
| `{{existing_branch}}`    | Empty on a fresh pickup. On a re-queue with an open recorded PR (CLAUDE-275), that PR's head branch, already checked out in `{{workdir}}`. |

##### Audit prompt: filing from disk, not from a compacted context (CLAUDE-304)

On 2026-09-15 an `audit-run` job compacted mid-run (`claude.autocompact`'s default 200000-token window) and, after the
boundary, rebuilt 14 of its 20 finding write-ups from the compacted summary instead of re-reading them, filing nine
issues whose `file:line` citations did not exist. The audit prompt (`default_audit_prompt`,
`src/native/audit_prompt.rs`) now orders its per-target steps so that never has to happen:

- **P4 writes the dated report, with every finding block, before P5 files anything.** The report is the on-disk record
  a compacted run re-reads instead of reconstructing a finding from memory.
- **P5 files each finding from that report, never from conversation memory:** it re-reads the finding's block from the
  report on disk and re-opens its first cited `file:line` in the clone before building the issue body and calling
  `yt issue create`. Immediately after each create, it appends `<finding id><TAB><issue key>` to a filing log outside
  `{{workdir}}`, at `/tmp/audit-filing-{{key}}.tsv`. If the run notices its earlier conversation was replaced by a
  compaction summary, or it cannot account for a finding's text from its own prior turns, it re-reads the report and
  the filing log from disk before any further tracker write, and never files a finding id the log already lists.
- **`AI Agent = Needs Review` is now permitted on a self-created issue**, in addition to `Queued`: an issue the run
  created and filed this run, that it later finds wrong (a citation that does not exist, a duplicate, anything the
  filing log or a re-read of the report contradicts), gets `Needs Review` instead, taking it out of the `youtrack`
  action's queue for a person. Delete stays banned either way. The run records the key and its reason in the
  `## Result` block and in its final checklist.

Separately, `finalize` (`src/native/youtrack.rs`) reads its own run's stream log after every job and counts
`compact_boundary` events (`crate::claude::count_compact_boundaries`). A non-zero count appends one line to the
success or needs-review outcome comment, `Context was compacted <n> time(s) during this run; check the issues it
created.`, and logs a `warn` naming the key and the count; a run with none gets neither. A stream-log read failure
logs at `error` and the outcome comment still posts, without that line - the same "the comment is best-effort, the
field write is authoritative" posture `finalize` already takes everywhere else.

##### Resuming an open PR on re-queue (CLAUDE-275)

Every PR the `youtrack` action opens is recorded in the `agent_pull_requests` store (action, issue key, host, repo, PR
number, head branch), keyed unique on `(host, repo, pr_number)`. Before `pre` runs, `process_job` looks up the
triggering issue's most recently recorded PR:

- **No record, or a record already `merged` / `closed`:** unchanged behavior - clone the repo's default branch and
  open a fresh PR.
- **A record still marked `open`:** confirmed live against `Forge::list_open_prs`. Still open: `{{existing_branch}}`
  is cloned instead of the default branch, and `{{existing_pr_url}}` / `{{existing_branch}}` are rendered into the
  prompt so the agent fixes the SAME branch (reading the issue's newest comments for what to fix) instead of opening a
  second PR. No longer in the open list: the record is marked `closed` (a closed-vs-merged distinction is not
  derivable from an open-PR listing alone) and pickup falls back to the default branch.
- **The `agit` PR strategy:** always skipped, logged at `info`. AGit pushes to `refs/for/<base>` and produces no
  server branch, so there is nothing to resume; a re-queue always opens a fresh PR under this strategy.

A successful `create_pr` on a NON-resumed run (fresh PR) records one new `agent_pull_requests` row; a resumed run's
`create_pr` call adopts the same PR number again (the branch-strategy idempotency reconcile below), so it is never
re-recorded (the store's `UNIQUE(host, repo, pr_number)` would reject a duplicate insert). A failed record write is
logged at `error`, surfaced as a `run_events` row (the same channel a degraded open-PR lookup uses), and named in a
follow-up issue comment; the PR itself still exists and the issue still advances to Code Review, since the record is
bookkeeping for the NEXT re-queue, not a condition of this one succeeding.

##### Routing a CI failure on an open PR (CLAUDE-276)

The `agent_pull_requests` store this section built is what the CI-failure webhook (`webhook-triage`) reads to decide
whether a failing run's open PR is this action's own. Before triage, the webhook consumer resolves the open PR (if
any) whose head matches the failing commit, then looks that PR up in `agent_pull_requests` by `(host, repo,
pr_number)`:

- **An `open` row (the agent's own PR):** below `config.webhook.agent_pr_fix_attempts` (default 3), the failure
  comments the recorded issue (run link, failing tests, analysis), bumps `fix_attempts`, and sets `AI Agent = Queued`
  - the exact write that re-queues the `youtrack` action's own pickup above, so the SAME branch gets fixed instead of
  a second issue being filed. At the ceiling, `AI Agent = Needs Review` instead, with a comment listing every fix
  attempt's run.
- **No row, or one not `open` (a human's PR):** the failure is never the agent's to fix. It always gets the analysis
  as a PR comment, one way or another; see "Webhook (Forgejo or GitHub)" in `docs/webhooks.md` for the three cases (shares an `open`
  failure / shares a `pr_only` failure across two branches / no match at all).

This replaces matching `trigger_user` against `config.mint.bot_logins` for loop safety: that comparison is
case-sensitive, and the runner's own Forgejo login (`Claude-Run`) never matched the lowercase-only default
`bot_logins` entry (`claude-run`), which is what silently dropped a real agent-PR failure before this issue.
`config.mint.bot_logins` is unchanged and still used by the separate `/claude` PR-comment listener's own bot-login
check.

##### Resolving a CI-failure issue on merge (CLAUDE-277, CLAUDE-327)

Closing a CI-failure issue by finding its failing job green needed a run on `default_branch`, which a PR-only workflow
never satisfies (it has no run on `main` at all). The CI-failure webhook (`webhook-triage`) closed
that gap with a second delivery kind on the same endpoint: `POST /webhooks/forgejo/triage/<source>` also accepts Forgejo
`pull_request` deliveries, dispatched by the delivery's `X-Forgejo-Event` header before parsing (a value the deployed
Forgejo's Action-run notifier does not send; an operator adding this must confirm the header against their own
instance's actual delivery). A `pull_request` delivery runs through the same signature / replay / rate-limit / pause /
allow-list gates the Action-run path does; only a terminal `action: "closed"` delivery does anything.

CLAUDE-327 removes the rest of what this delivery used to decide. The runner never writes `State` on merge, whether
the issue was minted by the runner or not: a YouTrack workflow (`docs/yt-workflow-resolve-on-pr-merge.js`, attached
per-project by an operator; see the operator-setup note in `docs/webhooks.md`) resolves the issue once it observes the merge,
matching by title token, a bare `#KEY-N` line, or a `Closes/Fixes/Resolves KEY-N` line in the PR body - the same tail
`mint::build_description` already writes.

- **Merged:** the PR's own `agent_pull_requests` row (if any) is marked `merged`. No tracker call, no forge run-list
  call.
- **Closed without merging:** the PR's own `agent_pull_requests` row (if any) is marked `closed`; a human's PR, with
  no such row, is unaffected.

Because the store's `open`/`resolved` column no longer changes on merge, an `open` `ci_failures` row can still name an
issue the workflow already resolved by the time a later CI failure on the same failing tests is triaged. Both
CI-failure dispatch sites (the no-open-PR path in `triage_and_mint`, and `route_human_pr_failure`) read the issue's
live `State` at match time before deciding: equal to `config.mint.resolved_state` (default `Done`, compared
case-insensitively) is a regression, handled exactly as a `resolved` store row already is (`reopen_resolved_failure`);
not equal is the ordinary open-and-still-open attach; a failed read fails the run (an `error` log, a `run_events` row,
no comment or attach) rather than risking a false attach on a state it could not confirm.

Finding the failing workflow's run at the head commit reads `fj actions runs` again, through the same `log_token_ref`
this listener already resolves for the Action-run path - no new credential.

##### The no-code completion path (CLAUDE-214)

A run whose deliverable is tracker output rather than code (survey several repos, file the findings as issues, link them
under the triggering issue) makes zero commits by design. The CLAUDE-212 ahead check would fail such a run
(`AI Agent = Failed`, no PR), so the agent marks the outcome deliberate by writing
`$workdir/.claude-run-no-code-change`. Its contents are the human-facing summary.

When `finalize` sees a **successful** claude result and that marker, it skips the PR entirely, posts the marker text
verbatim as the issue comment, and sets `AI Agent` to `states.no_code`. `State` is never touched. The blocker marker
still wins: a workdir carrying both parks at `Needs Review`.

Without the marker, a zero-commit run keeps the CLAUDE-212 behaviour unchanged (`AI Agent = Failed`, no PR). That guard
is what stops a silently-empty engineering run from looking successful, so the marker is the only thing that relaxes it,
and never an escape hatch for a code task the agent could not finish (that stays `.claude-run-needs-review`).

The built-in prompt correspondingly permits a bounded set of tracker writes: read across projects; create an issue in
one of two cases (CLAUDE-297) - a descoped piece of the triggering issue, filed in `write_projects` and linked
`subtask of` the triggering issue, or a bug found outside the triggering issue's scope, filed in the project whose
VCS integration binds the buggy repo (falling back to `write_projects`) and linked `relates to` the triggering issue;
and update, comment on and link only the issues the run itself created. Setting `AI Agent` on **any** issue stays
banned, since setting it queues an agent run.

##### Forgejo token scopes (CLAUDE-196)

The token named by `forgejo.token_ref` (or the host `keys.json` entry `fj` falls back to) must carry
**`write:repository`** and **`write:issue`**, in the labels the deployed Forgejo's token UI uses (verified against
Forgejo `16.0.0+gitea-1.22.0`, `models/auth/access_token_scope.go`). Forgejo scopes API endpoints by category, not by
`fj` command name, and the runner's PR path crosses two categories: `fj pr search` and `fj pr comment` are backed by the
**issues** API even though they name a PR. A repository-only token can still `fj pr create` and fails the
CLAUDE-170 open-PR lookup with `403 Forbidden`; since CLAUDE-207 the gather-time preflight exercises one read per
category, so such a token is rejected before any Claude spend (see
[Forge repo-access preflight](#forge-repo-access-preflight-claude-194-claude-207)). Should the lookup fail anyway
(a permission change mid-run, or a transient forge error), CLAUDE-206 degrades it rather than discarding the run: on
the create-capable arms the runner assumes no open PR exists, opens the PR, and records a `warn` run event naming the
CLI error. See [Open-PR lookup degradation](#open-pr-lookup-degradation-claude-206).

Every `fj` command the runner issues, the fj-source call it makes, and the category Forgejo routes that endpoint under
(`routers/api/v1/api.go`: the repository group, and the separate `/repos` group carrying
`tokenRequiresScopes(...CategoryIssue)`):

| `fj` command (`src/provider/cli.rs`)             | fj source call                                             | Forgejo endpoint                                     | Scope category    |
|--------------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------|
| `repo view <repo> --json` (`repo_access`)        | `fj_core::repo::get` -> `repo_get`                          | `GET /repos/{o}/{r}`                                   | repository (read)  |
| `pr create <title> --body-file .. --head <b> --json` (`create_pr_branch`) | `fj_core::prs::create` -> `repo_create_pull_request` | `POST /repos/{o}/{r}/pulls`                            | repository (write) |
| `pr create -aA --json` (AGit, `create_pr_agit`)  | same as above                                                | `POST /repos/{o}/{r}/pulls`                            | repository (write) |
| `pr search --repo <r> --state open --json` (`list_open_prs`) | `fj_core::issues::list_page` -> `issue_list_issues` (`type=pulls`), then one `prs::get` per hit | `GET /repos/{o}/{r}/issues?type=pulls`, `GET /repos/{o}/{r}/pulls/{i}` | **issue (read)** + repository (read) |
| `pr status <id> --json` (`pr_status`)            | `prs::get` + `prs::get_commit_statuses`                      | `GET /repos/{o}/{r}/pulls/{i}`, `GET /repos/{o}/{r}/commits/{sha}/status` | repository (read)  |
| `pr merge <id> --method merge --delete` (`merge_pr`) | `repo::get` + `prs::get` + `prs::merge` -> `repo_merge_pull_request` | `POST /repos/{o}/{r}/pulls/{i}/merge`          | repository (write) |
| `pr update <id> --style merge` (`update_pr_branch`, CLAUDE-309/FJ-99) | `fj_core::prs::update_branch` -> `repo_update_pull_request` | `POST /repos/{o}/{r}/pulls/{i}/update` | repository (write) |
| `pr comment <id> --body-file <f>` (`pr_comment`) | `fj_core::issues::create_comment` -> `issue_create_comment`  | `POST /repos/{o}/{r}/issues/{i}/comments`              | **issue (write)**  |
| `issue comment <repo>#<n> --body-file <f>` (`issue_comment`) | `fj_core::issues::create_comment` -> `issue_create_comment` | `POST /repos/{o}/{r}/issues/{i}/comments` | **issue (write)** |
| `pr edit <id> labels --add <label>` (`pr_add_labels`) | `fj_core::issues::edit_labels` -> `issue_list_labels` + `issue_add_label` | `GET /repos/{o}/{r}/labels`, `POST /repos/{o}/{r}/issues/{i}/labels` | **issue (write)**  |
| `pr view <id> --json` (`pr_referenced_issues`)   | `prs::get` + `prs::list_files` + `repo::get`                 | `GET /repos/{o}/{r}/pulls/{i}`, `.../files`            | repository (read)  |
| `repo collaborator <repo> permission <user> --format json` (`repo_permission`) | `fj_core::repo::collaborator_permission` | `GET /repos/{o}/{r}/collaborators/{user}/permission` | repository (read); caller must be a site admin, a repo admin, or the queried user |

The comment-command listener (CLAUDE-220) uses a THIRD credential, the `write_token_ref` on a `webhook_comment` source, distinct from `forgejo.token_ref` and from the read-only `log_token_ref`. It issues `repo collaborator ... permission` (the authorization read), `pr search` (resolving the PR's head branch), `pr comment` (every PR reply), and `issue comment` (every plain-issue reply), and it authenticates the branch push, so it needs `write:repository` plus `write:issue` by the same table above. Forgejo answers the permission read only for a site admin, a repo admin, or the user being queried (`GetRepoPermissions`, `routers/api/v1/repo/collaborators.go`), so the bot account that owns this token needs the **Admin** role on every allowed repository; without it every command fails closed with a "could not verify permission" reply rather than running unauthorized work.

The CI-failure webhook (`webhook-triage`) uses a FOURTH credential (CLAUDE-276): `write_token_ref` on a `[webhook.sources.<source>]` block, distinct from that same source's own `log_token_ref` and from the comment listener's `write_token_ref` above. It issues only `pr comment` (routing a CI failure on an open PR: the analysis on a human's PR, a re-queue comment on the agent's own PR, or the "already tracked" note), so it needs only **`write:issue`** - never `write:repository`, since this listener never pushes a branch or opens a PR. A source with none configured fails the run the moment a PR comment is needed (logged at `error`, recorded as a `run_events` row naming the source), rather than silently posting nothing.

Two things the token does **not** have to cover: the `git push` in front of `pr create` authenticates with the daemon
host's own git credentials, not this token (CLAUDE-194); and the org-label lookup inside `pr edit labels`
(`org_list_labels`, `GET /orgs/{org}/labels`, organization category) is failure-tolerant in fj, so `read:organization`
is optional and only needed to attach an org-level label.

Adding a new `fj` call to the runner requires re-checking the endpoint's category in fj's source and updating this
table, since a new category is a silent 403 on every existing deployment until each token is re-minted.

##### Forge auto-routing by host (CLAUDE-50)

The PR target is chosen automatically from each job's resolved VCS `repo_host`; there is **no** per-action forge
selector. A job whose repo host is GitHub (`github.com` plus any host listed in `github.hosts`) opens its PR through the
GitHub backend; every other host opens through `fj` exactly as before, so no existing Forgejo action changes behavior.

The GitHub backend prefers GitHub's remote MCP (`create_pull_request`) and falls back to `gh pr create` when the MCP
endpoint is unreachable; with neither available it errors. Unlike Forgejo's AGit flow (`fj -aA`), GitHub has no AGit
equivalent, so the backend pushes the working branch before opening the PR. The PAT supplied via `github.token_ref`
must carry the **`repo`** and **`pull_request`** scopes (used for the authenticated push, the MCP call, and `gh`). The
gather-time preflight probes one capability per scope, so a PAT short of either is rejected before the run (CLAUDE-207).

##### CI-gated automerge (CLAUDE-126, event-driven since CLAUDE-278, reconciler backstop since CLAUDE-328)

By default a successful run opens the PR, sets AI Agent to `code_review`, and stops; the issue stays In Progress until a
human merges. Setting `youtrack.automerge.enabled: true` opts each successful run into merging the PR itself once its CI
checks pass. `finalize` itself never waits for CI: once the PR is opened it stops at Code Review exactly as when automerge
is disabled. The merge instead happens later, normally driven by a Forgejo webhook `success` or `recover` Action-run
event on that PR's head commit (`POST /webhooks/forgejo/triage/<source>`, the same endpoint the CI-failure listener uses) - so
the wait holds no job slot no matter how long the suite takes, and there is nothing to time out. The webhook is not the
only trigger: a reconciler pass on the daemon's own tick (every 300s, CLAUDE-328) lists every `open` `agent_pull_requests`
row, skips any whose action is not automerge-enabled, and runs the exact same decision through the shared
`apply_automerge_decision` function a webhook event runs, so a PR still merges within one tick when its `success` event
was never delivered, was dropped, or arrived before the PR's row was recorded. On either entry point, once the daemon has
resolved which recorded agent PR it belongs to and confirmed that PR's action still has automerge enabled, it reads the
PR's combined CI status (`fj pr status --json`, CLAUDE-309: including its `behind_base` field) and dispatches on
`behind_base` first, since Forgejo's `mergeable` never reflects whether the branch is behind its base:

- **`behind_base` missing** (an `fj` predating FJ-99): no merge action is taken; the handler comments that the
  runtime's `fj` must include FJ-99 and fails the run so a stale deployment does not silently stop automerging.
- **Green and `behind_base: true`:** the branch is updated with `fj pr update <repo>#<n> --style merge`
  (`update_pr_branch`, CLAUDE-309/FJ-99), which merges the base into the head branch through Forgejo's own
  Update-branch endpoint, instead of racing straight into a 405 from `pr merge`. A successful update comments that the
  branch was updated and leaves the issue at Code Review; updating a branch retriggers CI, and the next `success`
  event tries again. A failed update comments the failure and fails the run.
- **Green, `behind_base: false`, and not mergeable** (a real conflict, still-checking, or a WIP PR): the handler
  comments that the PR is not mergeable and leaves the issue at Code Review.
- **Green, `behind_base: false`, and mergeable:** the PR is merged as a **merge-commit** (never squash/rebase, so the
  single commit's bare `#<KEY>` reference survives in history), and a "runner merged" comment is posted. CLAUDE-327
  removed the `State`/`AI Agent` write that used to follow the merge here: a YouTrack workflow
  (`docs/yt-workflow-resolve-on-pr-merge.js`) resolves the issue once it observes the merge, not the runner. A merge
  failure here comments the failure and fails the run.
- **Anything else** (checks not `success`): no action. A later `success` event on the same PR is what retries; a real
  CI failure is handled by the existing CI-failure webhook routing, not by this path.

Operators should point their Forgejo webhook at "Action Run" success **and** recover events, in addition to failure, so
this fires within seconds instead of waiting for the next reconciler tick (see "Webhook (Forgejo or GitHub)" in `docs/webhooks.md`
operator-setup note); without them the reconciler backstop is what keeps a green PR from sitting at Code Review
forever. Every fault either entry point can hit while a recorded, automerge-enabled PR is involved (a forge-host lookup
failure, a `list_open_prs` failure, a failed action load, a failed status read, a failed merge or branch update) is
logged at `error`, written to `run_events`, and posted as a PR comment naming the failure. `automerge.wait_seconds` and
`automerge.check_interval_seconds` are deprecated: the runner no longer polls, so an action file still setting them keeps
loading (with one deprecation warning at load) but the values are otherwise ignored.

CLAUDE-397: a reconciler pass records a `webhook-triage` run only when it merges a PR, marks an already-merged PR
merged, updates a behind-base branch, or fails; a pass whose decision changes nothing (checks not green yet, automerge
disabled, the action isn't a youtrack action, or not mergeable) opens no run row, so the 300s sweep over every open
agent PR no longer floods run history with one row per PR per pass.

Automerge works on both backends: a Forgejo-hosted PR goes through `fj pr status` / `fj pr merge` / `fj pr update`, and a
GitHub-hosted PR (CLAUDE-405) goes through `gh pr view --json` / `gh pr merge` / `gh pr update-branch` via `GithubForge`,
selected automatically by `resolve_automerge_forge` from the PR's host.

##### Pre-clone dependency gate (CLAUDE-161)

When issues are mass-assigned to the runner (AI Agent = Queued), an issue whose blockers are still open would otherwise be picked up, cloned, and handed to the agent only to discover it cannot proceed. The dependency gate moves that check before the clone, where it is knowable for free. The gate reads the issue's links (`yt issue inspect --json`, which since YT-40 carries each link's resolved `direction` / `verb` and each target's nullable `resolved` timestamp) and blocks the issue when it has any unmet dependency.

CLAUDE-182: the gate (and the live AI-Agent re-read) run at **gather time**, before the daemon-wide concurrency KeyGate is acquired, so a blocked or no-longer-Queued issue is dropped from the candidate set and never competes for its project's single per-poll work slot. Before CLAUDE-182 the gate ran only after an issue won the KeyGate, so a blocked issue could win the shared project key each poll, hit the gate, and consume the slot that a genuinely-workable same-project sibling needed, starving the sibling indefinitely. The gate is re-run once more after the winning issue takes the key (the authoritative pre-clone re-check for the cross-run TOCTOU); its `blocked_until` backoff and idempotent side effects make the second read cheap.

A link blocks when its direction-resolved `verb` is listed in `dependencies.block_on` and its target is unresolved (`resolved` is null/absent). The defaults are `depends on` (Depend INWARD: an open blocker gates this issue) and `parent for` (Subtask OUTWARD: an open child gates this parent). The reverse verbs never block: `is required for` (Depend OUTWARD, issues this one blocks) and `subtask of` (Subtask INWARD, this issue's parent). A blocker or child counts as **met** iff its `resolved` timestamp is non-null. With `block_cross_project: false`, a link whose target sits in another project (by key prefix) never blocks, whatever its verb (CLAUDE-295).

When blocked:

- The issue is left at AI Agent = `queued` (no transition to Working); it is **not** cloned and `claude` is **not** invoked.
- No `max_attempts_per_key` slot is consumed (the same no-attempt semantics as the skip-attempt / parked path); the run outcome is `Blocked`, distinct from filtered / failed / needs-review.
- A comment listing the unmet blockers (id + relationship) is posted on entering the blocked state, and re-posted only when the unmet-blocker set changes, so repeated re-checks do not spam the issue.
- A per-issue backoff is recorded: `blocked_until = now + recheck_timeout`. While the window is open the **scheduler's** periodic pickup skips re-evaluating the issue entirely (it stays Queued; no `yt` read, no comment). The state persists across polls and daemon restarts in the `dependency_gate` table (migration `0015_dependency_gate.sql`), keyed by (action, issue key).
- CLAUDE-195: a **human-initiated** run always ignores an open backoff window and re-reads the links live. "Manual" is any origin other than the scheduler's periodic pickup: `claude-run trigger <action>` (dispatch origin `trigger`) and a direct `claude-run run` (origin `cli`). An operator who just resolved a blocker and triggers expects an evaluation against the live tracker, not a replay of the cached signature, so the manual trigger picks the issue up in the same run instead of waiting out `recheck_timeout`. Only the periodic `scheduler` origin keeps the throttle. The bypass changes only whether an open window suppresses the read; every persistence and comment side effect (fresh signature / `blocked_until` when still blocked, row clear when unblocked, re-comment only on a changed blocker set) is unchanged.

When dependencies are met, the issue has none, or `enabled: false`, the gate is a no-op and pickup proceeds exactly as before (Working, clone, claude). No "unblocked" comment is posted; the normal Working transition and eventual PR are the signal. If the link read itself fails, the gate does not strand the issue: it falls back to normal pickup (the agent self-parks if it truly cannot proceed), since the gate is an optimization over that correct baseline.

CLAUDE-225: a block that lasts is **announced**, because everything about the blocked path is otherwise silent. The issue keeps its Queued field value, its comment says it "will be re-evaluated automatically", and a poll whose every candidate is blocked returns before `run_begin`, so no run row is written either. The gate row therefore carries `blocked_since` (migration `0019_dependency_gate_blocked_since.sql`), stamped when the block starts and preserved across every re-arm, so the age measured is the age of the whole block rather than of the last evaluation; the row is deleted when the blockers clear, so the signal clears itself with no acknowledgement. Once a block passes `STALE_GATE_AFTER_MS` (one hour, `src/state.rs`) it surfaces in three places: a `warn` line in the daemon journal on each evaluation, a Dashboard banner listing the action, issue, age, blocker set, and last evaluation, and a `stale_gates` block on `/api/status` (so `claude-run status` reports it with or without the daemon). The banner separately flags a **recheck overdue** gate, whose `blocked_until` deadline passed more than the threshold ago: that means the promised re-evaluation is not running at all, so the blockers may already be resolved, which is a different fault from a dependency that is simply still open.

For that signal to stay trustworthy, each poll also prunes the gate rows whose issue the action's query no longer matches (resolved by hand, re-fielded, or dropped by a query change). A pruned row held nothing, and left in place it would sit in the banner permanently. The prune runs only after a **successful** query, and it keys off what the query matched rather than what survived the VCS / access filters, so a project skipped for one poll keeps its row (and is not re-commented when it comes back).

##### Forge repo-access preflight (CLAUDE-194, CLAUDE-207)

The runner uses two identities against the same forge host: `fj` / `gh` authenticate with the action's `token_ref` (the bot), while `git push` authenticates from the daemon host's own git credentials. A repo the forge token cannot see therefore clones and pushes fine and fails only at `pr create`, with a bare 404 that names neither the repo, the host, nor the identity, after a whole Claude run has been paid for.

At gather time, once per repo per poll (not once per issue), the runner asks the forge whether the token can do the PR path on the resolved repo. Forgejo scopes its API by endpoint category and the PR path crosses two of them, so the preflight is **two** reads, one per category, and both must pass:

| Backend | Repository-category probe                        | Issue / pull-request-category probe                            |
|---------|--------------------------------------------------|-----------------------------------------------------------------|
| Forgejo | `fj --host <host> repo view <owner/repo> --json`  | `fj --host <host> pr search --repo <owner/repo> --state open --json` |
| GitHub  | `gh repo view <owner/repo> --json nameWithOwner`  | `gh pr list --repo <owner/repo> --state open --limit 1 --json number` |

The second probe is the exact call the PR path makes first (the CLAUDE-170 open-PR lookup), so a token carrying the repository category alone now fails at gather instead of 403-ing mid-run with the Claude session already paid for. An empty result is a pass; only a non-zero exit is a failure. The repository probe is still needed because an invisible repo and a repo with no open PRs both search as an empty array with exit 0. A GitHub fine-grained PAT scopes pull requests separately from repo metadata, so the same asymmetry exists there and is probed the same way. **Deployment prerequisite:** the runner's `fj` must expose `repo view --json` and `pr search --json`.

A failing probe is reported with the capability that failed (`repo view` / `pr search`), the CLI's exit status, and the grant to add, and the two failure modes read differently because they need different grants: a **403** means the token can see the repo but is refused the call (add the named scope, `read:repository` / `read:issue` for Forgejo and `repo` / `pull_request` for GitHub, or the matching unit permission), while a **404** means the repo is not visible to the token at all (a repo permission grant for the bot; no scope reveals it). See [Forgejo token scopes](#forgejo-token-scopes-claude-196) for the per-command category breakdown.

When either probe fails, every issue of that repo is dropped for the poll and one `warn` input event (visible on the action page) records the host, the `owner/repo`, and the CLI's cause. `AI Agent` is left at **`Queued`**: the condition is a missing permission grant, not a defect in the issue, so the next poll after the grant picks the issues up with no manual field change. This is deliberately unlike the `Failed` path a mid-run failure takes.

CLAUDE-216: the same failure also raises a banner on the Dashboard for 24 hours, linking to the `/access` page, so the silent skip is announced rather than waiting to be found on the action page. When a later poll's preflight passes for that repo, the runner records a one-off `forge repo-access preflight passed` info event, which supersedes the open failure and clears the banner; it is written only on that fail -> pass transition, so a healthy poll adds nothing to the event log. `/access` (and `claude-run access check`) runs this exact probe on demand against a named repo, so the check and the preflight can never disagree; see "Access check" in `docs/usage.md`.

The probe is skipped entirely when the action sets no forge `token_ref`, and skipped per host when the action carries no token for that host's forge: `fj` / `gh` then read the operator's own credential store, so the probe would test the wrong identity. A permission change *after* gather still fails at `pr create`, so that error is also restated with the repo, the host, and the possible-permission cause instead of the bare CLI text. A permission change that costs only the open-PR lookup is handled by the degradation below.

##### Open-PR lookup degradation (CLAUDE-206)

The CLAUDE-170 idempotency reconcile asks the forge for an already-open PR on the branch before creating one (`fj pr search`, an issue-category call). That lookup is an optimization: `fj pr create` (repository-category) is what delivers the work, and the two can fail independently, so a failed lookup must not discard a paid Claude run.

On every create-capable arm (remote branch absent, remote equal to local, remote fast-forwardable, and - since CLAUDE-382 - remote diverged after a clean merge), a failed lookup is treated as "unknown, assume none": the runner logs a `warn`, records a `warn` run event naming the CLI error and the repo (visible on the action page under the issue key), and proceeds to `pr create`. The adopt path is unchanged when the lookup succeeds.

That degraded path can race a PR that already exists. Forgejo answers such a create with 409 and the message `pull request already exists for these targets [...]`; on that specific failure the runner retries the lookup **exactly once** and adopts the PR when the retry succeeds. If it still cannot produce one, the error names both causes (the lookup failure and the create failure) rather than reporting a bare "PR creation failed".

CLAUDE-382: the diverged arm (neither SHA is an ancestor of the other, typically because the runner itself pushed its own "branch was behind its base" update onto the branch while the run was working) first fetches `origin/<branch>` and merges it into the local work with `git merge --no-edit`, then pushes the fast-forward result and falls through to the same lookup-or-create path as every other arm. `--force` / `--force-with-lease` is never issued on any arm. Only a merge CONFLICT is fatal: the merge is aborted immediately and the error names the branch, both SHAs, and the workdir, stating that any unpushed work is preserved only if the cleanup check finds it there.

#### Credential injection (CLAUDE-26)

`token_ref` names an entry in the encrypted secrets store (managed under `/secrets` in the web UI, encrypted at rest
with the `age` key file named by `CLAUDE_RUN_SECRETS_KEY_FILE`). A raw token is **never** written to the action YAML,
exported, or logged. When a block sets `token_ref`, the runner decrypts the secret at run time and materializes per-job,
mode-`0600` credential files (a single-file `yt` config and a temp forgejo `keys.json`), injecting them into the `yt` /
`fj` children for that job only and scrubbing them when the job finishes. The GitHub PAT (`github.token_ref`) is
injected as the `gh` child's `GH_TOKEN`, the MCP bearer, and the authenticated push URL for that job only; being an
env-var / URL component it leaves no on-disk plaintext to scrub and is dropped with the job. The gather step and the forge preflight materialize their own short-lived stores; every such directory carries a process-wide unique suffix, so two overlapping polls of one action never share a path and one poll finishing can never scrub another's live credential files (CLAUDE-209). When no block sets
`token_ref`, `yt` and `fj` fall back to host-configured credentials exactly as the scripted action did; `github` falls
back instead to the daemon's own GitHub service login (CLAUDE-307, Settings -> Auth), resolved lazily and cached
per `GithubForge` via `gh auth token --hostname <host>` rather than a `GH_TOKEN` env var, so a job with no
`github.token_ref` still clones, pushes and opens its PR authenticated as that login.

#### `kind: closed-issue-summary`

A single flat config record (no sub-records). The automation used as a REPORTING tool rather than as an issue worker
(CLAUDE-218): on each run it takes every nominated parent, queries each configured project for the issues resolved
inside a rolling window, renders counts plus a per-project listing, creates that report as a new issue, and links it to
the parent as a subtask. The dedup key is the parent issue id.

**No code, no pull request.** The run clones no repository, starts no agent session, and opens no pull request, so there
is no forge call and no git call on any path. That is why it is a separate action rather than a second job taught to
`youtrack.yml`, following the audit split of CLAUDE-215: the worker action's query, custom field, states, assignee
handling and labels are all for code work, and every one of them differs here.

**Parents.** Two independent sources, unioned. `parents` nominates issues by id and needs no tracker configuration;
those issues are never field-written. `query` (empty by default, so no query runs at all) selects parents dynamically,
and each parent it selects gets `custom_field` advanced to `done_label` (or `failed_label`) once its report is filed,
which is what drops it back out of the query. **A `query` MUST select a `custom_field` value the worker action does
not**, or the two actions race for the same issue: `actions/youtrack.yml` pins `AI Agent: Queued`, so a reporting query
needs its own distinct value. Note that the value has to exist in the YouTrack `AI Agent` bundle first, because YouTrack
rejects a query naming an unknown enum value with HTTP 400 rather than returning an empty result; that is also why
`youtrack.yml` documents the partition instead of negating the reporting value in its own query, which would break its
poll until the value was created. `shipped_action_queries_are_disjoint` in `src/action.rs` is the mechanical guard.

**Window and idempotency.** The window ends today and is `window_days` long inclusive, so `7` covers today plus the
previous six days; it is applied server-side as the tracker's own `resolved date: <start> .. <end>` range. Counts are
exact (the query is fully paginated); `max_listed_per_project` caps the LISTING only, and a capped section prints an
explicit "and N more" line rather than truncating silently. The marker comment posted on the parent carries a hidden
`Report-Window:` line, and a parent already carrying the current window's marker is skipped entirely, so a poll interval
shorter than the window files one report per window rather than one per poll.

```yaml
kind: closed-issue-summary
native:
  enabled: true                       # master toggle (default true)
  parents: [CLAUDE-100]               # issues to hang each report off; never field-written
  query: ""                           # optional dynamic parent query; MUST NOT collide with the worker's AI Agent value
  custom_field: "AI Agent"            # advanced on a query-selected parent once its report is filed
  done_label: Done
  failed_label: Failed
  projects: [CLAUDE, DELTA]              # project short names the report counts closed issues across
  window_days: 7                      # rolling window, ending today inclusive
  report_project: RPT                 # omit to create the report in the parent's own project
  link_type: "subtask of"             # applied as `<report> subtask of <parent>`
  summary_template: "Closed-issue summary: {start} .. {end}"
  max_listed_per_project: 100         # caps the listing only; counts stay exact
```

| Key                      | Default                                     | Meaning                                                                                 |
|--------------------------|---------------------------------------------|-----------------------------------------------------------------------------------------|
| `enabled`                | `true`                                      | Master toggle. While false the handler no-ops.                                          |
| `parents`                | `[]`                                        | Issue ids each report hangs off, typically the milestone being reported against. Never field-written. |
| `query`                  | `""`                                        | Optional tracker query selecting further parents. Empty runs no query. MUST select a `custom_field` value the worker action does not. |
| `custom_field`           | `AI Agent`                                  | Field advanced on a query-selected parent once its report is filed.                      |
| `done_label`             | `Done`                                      | Value written to `custom_field` after a query-selected parent's report is filed.          |
| `failed_label`           | `Failed`                                    | Value written to `custom_field` when a query-selected parent's report could not be filed. |
| `projects`               | `[]`                                        | Project short names the report counts closed issues across. Empty reports nothing.        |
| `window_days`            | `7`                                         | Rolling window length in days, ending today inclusive.                                    |
| `report_project`         | (the parent's project)                      | Project the report issue is created in.                                                   |
| `link_type`              | `subtask of`                                | Link type attaching the report to its parent.                                             |
| `summary_template`       | `Closed-issue summary: {start} .. {end}`    | Summary line of the report issue; `{start}` / `{end}` / `{days}` / `{total}` / `{parent}` are substituted. |
| `max_listed_per_project` | `100`                                       | Cap on how many issues are LISTED per project. Counts stay exact; a capped section prints an "and N more" line. |

`window_days` is the distinctive key that disambiguates this variant from the other native configs under the untagged
`native:` block. It is a number, so it never prunes away on save, and the structured form always writes it.

**Deployment prerequisite:** a host `yt` on `PATH` only. This kind reaches no forge and needs no Forgejo PAT, since it
neither clones a repository nor opens a pull request.

### `workdir_base` (string, optional)

Per-job working directories are rooted under `<workdir_base>/<name>/<key>`. Defaults to `$CLAUDE_RUN_WORKDIR_BASE`
(which itself defaults to `$CLAUDE_RUN_DATA_DIR/workdirs`, i.e. `/data/workdirs` in the container). The shipped actions set none. Setting this on the action overrides the env for that action only - useful
when one action needs more disk than `/tmp` allows (e.g., a workspace mounted on a large data volume) or when you want
per-action isolation on disk for cleanup or quota reasons.

**Resolution rules** (apply to both the YAML value and the env value):

The value is a literal path. `~`, `$VAR` and `${VAR}` are expanded; no nu evaluation happens.

```yaml
workdir_base: /data/workdirs-large               # literal, absolute
workdir_base: ${CLAUDE_RUN_DATA_DIR}/workdirs    # literal, ${VAR} expanded
```

A path outside `/data` is not on the volume the Docker sidecar shares, so bind mounts from the workdir will not
resolve (see CLAUDE-325).

### `input.command` (nu snippet, required for `kind: scripted`)

A Nushell snippet executed by the runner with `nu --commands "$snippet"`. Must write a JSON array to stdout. Each
element of the array becomes one Claude invocation, bound to `$job` for downstream snippets.

The snippet inherits the runner's environment. Use host-configured CLIs (`yt`, `fj`, `git`, etc.) directly; the runner
does not parse or inject secrets.

If the snippet exits non-zero, or stdout is not a JSON array, the run aborts with exit code 5.

### `dedup_key` (string, optional)

The name of a field on each job whose value uniquely identifies the unit of work. The runner tracks attempts keyed on
this value and skips a job whose attempt count has reached `max_attempts_per_key`.

Omit to disable per-job attempt tracking (every run processes every job).

### `filter` (nu snippet, optional)

Run once per job, before any other per-job work. The job JSON is piped to stdin and bound as `$job` in the snippet. Exit
non-zero to skip the job without counting an attempt. Exit zero to proceed.

### `pre` (list of nu snippets, optional)

Ordered steps executed before the Claude invocation. Each step is a separate nu snippet with `$job`, `$workdir`, and
`$env` in scope. Steps share the runner's working directory unless they `cd` themselves.

If a step exits non-zero, the runner logs the failure, **counts the attempt**, skips remaining pre steps, skips the
prompt + claude invocation, and **still runs post steps** so the action can roll back any side effects an earlier pre
step had (e.g. resetting an external state field that pre step 0 set). Post receives a synthesized `$claude_result` with
`ok: false`, an `error` field naming the failed pre step, and the step's stderr as `summary`. Poison jobs eventually hit
the per-key attempt cap and stop being retried.

### `prompt` (nu snippet, required for `kind: scripted`)

A nu snippet whose return value is the prompt string passed to Claude. Has `$job`, `$workdir`, and `$env` in scope.
Plain text without `$` interpolation is fine; nu treats it as a string literal.

```yaml
prompt: |
  $"Work YouTrack issue ($job.id): ($job.summary)"
```

### `claude` (record, optional)

Tuning for the `claude` CLI call. All keys are optional.

| Key               | Default                          | Meaning                                                 |
|-------------------|----------------------------------|---------------------------------------------------------|
| `cwd`             | per-job `$workdir`               | Working directory for the claude process. Plain string. |
| `allowed_tools`   | `Read,Edit,Write,Bash,Glob,Grep` | Passed as `--allowed-tools`.                            |
| `permission_mode` | `acceptEdits`                    | Passed as `--permission-mode`.                          |
| `model`           | `config.claude.model`, else the `claude` CLI's own default | Passed as `--model`, verbatim (CLAUDE-239). |
| `effort`          | `config.claude.effort`, else the `claude` CLI's own default | Passed as `--effort`, verbatim: `low`, `medium`, `high`, `xhigh`, `max` (CLAUDE-259). |
| `autocompact`     | `config.claude.autocompact`, else `200000`                  | Passed as `--autocompact`, verbatim: `auto`, or a window of 100000-1000000 tokens (DEV-706). |

**Model selection (CLAUDE-239).** The effective model is resolved per invocation in this order: this action's `model`, else the global default in the `Claude invocation` settings section (DB key `config.claude`, field `model`, edited on `/settings` or via `GET` / `PUT /api/settings/claude`), else nothing at all - no `--model` flag is passed and the `claude` CLI applies its own default. The runner never validates the value at any layer: it is handed to `--model` untouched, and the CLI is the source of truth for which models exist, so new aliases and dated ids (`sonnet`, `opus`, `claude-sonnet-4-5-20250929`, ...) work without a runner change. Like every DB-backed section, `config.claude` is DB-only from birth (never a `config.toml` key) and applies to the next invocation, with no daemon restart.

**Effort selection (CLAUDE-259).** The effective effort level resolves on exactly the same three-step precedence: this action's `effort`, else `config.claude.effort` (same settings section, same API path), else nothing at all - no `--effort` flag is passed and the `claude` CLI applies its own default. No layer rejects a value and no layer substitutes one, so a level Anthropic adds after this release works without a runner change. What the runner does add is one `warn` per invocation when the resolved value is outside the documented set (`low`, `medium`, `high`, `xhigh`, `max`), naming the value, whether it came from the action or the global default, and the known set. That warn exists because the CLI does NOT fail on an unknown level: it prints `Warning: Unknown --effort value '<x>' - ignoring it and using the default effort` on stderr and carries on at its default, which inside a run lands unstamped in the stream log where nothing notices it. Without the runner's own warn, a typo would silently downgrade effort for every run of that action.

**Compaction window (DEV-706).** The effective `--autocompact` window resolves on the same three-step precedence: this action's `autocompact`, else `config.claude.autocompact`, else the runner's own `200000` (`crate::claude::DEFAULT_AUTOCOMPACT`). Unlike `model` and `effort`, the last step is a VALUE rather than nothing: the CLI's own `auto` scales the window to the model's whole context, so on a 1M-context model a long tool-heavy session never compacts and resends every completed operation's tool payloads on every turn. That is what made cumulative spend grow with the square of the number of tool calls rather than linearly, with tool output rather than reasoning dominating the bill. A bounded window keeps full results for the operation in flight and a short carry-forward summary for the ones already finished. Set `autocompact: auto` on an action to opt back into the CLI's behavior. Values are passed through untouched; the CLI validates them.

The runner always passes `--print --verbose --output-format stream-json`. It wraps the call in `timeout
$claude_timeout_seconds`. Stdout+stderr are redirected to a per-job NDJSON stream log:

**Background work is disallowed, unconditionally (CLAUDE-280).** No action setting turns this off. Every `claude`
child runs with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` in its environment and `--disallowedTools=Monitor,ScheduleWakeup`
on its command line, because a session run under `--print` is over the instant its turn ends: a turn that ends
"waiting" on a backgrounded `Bash` command, a `Monitor` call, or a `ScheduleWakeup` call never gets to resume, and the
work it left running is not part of the outcome the runner reads. As a backstop, if a stream log's terminal `result`
event is followed by a `system`/`task_updated` event reporting `patch.status: killed` (the shape produced when the
runner terminates a leftover background task that outlived the result), the run finalizes as a non-transient failure
naming the task id rather than the success the `result` event reported.

```
$CLAUDE_RUN_STATE_DIR/logs/<name>/streams/<key>-attempt-<n>.jsonl
```

`tail -f` that file from another terminal to watch Claude work in real time. The final `result` event on the last line
is parsed for cost (`total_cost_usd`) and outcome (`result`, `is_error`) once claude exits.

A relay thread inside the runner is that file's only writer, so if writing to it fails (a full log partition is the
usual cause) every later event is lost, including the terminal `result`. CLAUDE-233: that is reported as what it is - a
runner-side I/O failure naming the underlying error and the log path - rather than as claude failing to produce a
result, and it is classified `transient`, so it costs the job no attempt and the job is retried on the next poll. When
the `result` event was already captured before the write failed, the run is still finalized from it and the truncation
is logged at `warn`.

**Retention (CLAUDE-245).** Stream logs are bounded, not permanent. A daemon sweep runs at `serve` start and hourly
thereafter, deleting every `logs/<name>/streams/<key>-attempt-<n>.jsonl` whose last-modified time is older than the
configured window; every pass logs its `deleted` / `kept` / `bytes_reclaimed` tally at `info`. A log whose job is still
in flight is kept no matter how old it is: the sweep reads the in-flight job set from the same database the daemon
records its runs in, so age is never the only evidence a run has finished. The window is the `Stream log GC` settings
section (DB key `config.stream_log_gc`, fields `enabled` (default `true`) and `retention_days` (default `30`)), edited
on `/settings` or via `GET` / `PUT /api/settings/stream_log_gc`. Like every DB-backed section it is DB-only from birth
(never a `config.toml` key) and applies on the next sweep. Set `enabled: false` to park the sweep without losing the
window, but note that nothing then bounds the directory: an unbounded `logs/` filling its partition is exactly what
truncated the stream logs described above.

### `post` (list of nu snippets, optional)

Ordered steps that run after every code path that reached the workdir: Claude success, Claude non-transient failure,
prompt-snippet failure, empty-prompt, and pre-step failure all run post. The single exception is transient Claude errors
(rate limit, auth, and the stream-log write failure above) - those skip post and return early so the job can be retried
cleanly on the next firing.

When a run is canceled while this job is in flight, its claude child is terminated and post still runs, with a
synthesized cancelled `$claude_result` (`ok: false`, `error: "run cancelled by operator"`, `cost: 0`), so end-of-job
cleanup (resetting an external state field, etc.) happens. A cancelled job does not count an attempt, and jobs not yet
started are skipped without running post.

This makes post the right place for end-of-job cleanup that must happen unconditionally - resetting an external state
field, posting a status comment, releasing a soft lock, etc.

Each step has `$job`, `$workdir`, `$env`, and `$claude_result` in scope. On pre/prompt failure `$claude_result` is
synthesized (`ok: false`, `error` names the failing phase, `summary` carries the step stderr) so post sees a uniform
shape regardless of which upstream step failed. The action's post step is expected to branch on `$claude_result.ok` to
do success vs failure cleanup.

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

Same step-failure handling as `pre`: if a post step exits non-zero, the runner logs the failure, **counts the attempt**,
and stops running subsequent post steps. The job's overall outcome record (the "done" line in the per-action JSONL log)
has `ok: false` if either claude or any post step failed.

#### Skip-attempt marker (parked / blocker outcomes)

By default, every job processed past pre counts toward `max_attempts_per_key`. A post step can opt this job out of
attempt counting by writing a sentinel file:

```nu
touch $"($workdir)/.claude-run-skip-attempt"
```

The runner checks for that file after the post loop. If present, the per-key attempt counter is **not** incremented.
Daily Claude-invocation count still increments when claude actually ran (real budget was spent).

Use this for outcomes where the job is **parked pending external input**, not failed:

- The work depends on information that has to come from a human.
- The work depends on another issue that has to land first.
- The work was deliberately deferred (out of scope, wrong priority).

Do **not** use it for hard failures (timeout, exception, Claude can't even investigate). Those should consume an attempt
slot so poison jobs eventually stop being retried.

The `done` outcome record gains an `attempt_counted: bool` field reflecting the runner's decision, so downstream tooling
can see at a glance which jobs were parked vs failed.

#### Needs-review marker (agent-signalled blocker)

A complement to the skip-attempt marker, used by the `youtrack` action so the agent can signal a blocker without writing
to the external system itself. When the Claude agent decides it cannot safely proceed, it makes no code change and
writes the human-facing reason to a second sentinel:

```nu
"Need decision X from a human." | save $"($workdir)/.claude-run-needs-review"
touch $"($workdir)/.claude-run-skip-attempt"
```

The action's finalize post step then reads this marker after claude exits: if `.claude-run-needs-review` exists it parks
the job (ensuring the skip-attempt marker is present), posts the file's contents verbatim as the issue comment, attaches
the `youtrack.tags.needs_info` tag (CLAUDE-373; empty skips the write, and a failed tag write is logged and does not
block the field write below), and sets the status field to the project's "needs human input" value, creating no PR. This
keeps every external mutation in one post step gated on the run outcome, rather than having the agent mutate the
external system mid-run. The marker is a plain convention between an action's prompt and its post step; the runner
itself only special-cases `.claude-run-skip-attempt`.

The tag is written ONLY on this path: the attempt-cap park (CLAUDE-301) and a timeout park (CLAUDE-232) both also set
`AI Agent = needs_review`, but neither is the agent asking a question, so neither writes the tag. The built-in prompt's
blocker-path step requires the marker text to end with exactly one explicit question a human can answer, so the tag's
presence lines up with "there's a question here" rather than "ran out of attempts" or "timed out".

### `output.file` (string, optional)

If set, the runner appends one JSON line per job to `$CLAUDE_RUN_STATE_DIR/logs/<name>/<output.file>` summarizing the outcome
(job key, attempt number, claude cost, push/comment results from post steps, etc.).

If omitted, no JSONL file is written; per-job outcomes are still recorded in the `job_outcomes` table and rendered on the
run detail page. The file is bounded by size, never by age: see [State + lock layout](#state--lock-layout).

### `limits` (record, optional)

Per-action overrides for the runner-wide defaults. Any subset of:

| Key                      | Default |
|--------------------------|---------|
| `max_attempts_per_key`   | `3`     |
| `max_runs_per_day`       | `20`    |
| `claude_timeout_seconds` | `600`   |
| `daily_budget_usd`       | `5.00`  |
| `run_budget_usd`         | the effective `daily_budget_usd` |
| `budget_warn_fraction`   | `0.80`  |
| `max_issues_per_run`     | `25`    |

The defaults are the built-in `RunnerDefaults::default()` (`src/job.rs`); the Rust runner reads no `CLAUDE_RUN_MAX_*` environment variable for them, so an action's YAML is the only override.

CLAUDE-232: reaching `max_attempts_per_key` is **announced**, for the same reason a lasting dependency gate is (see "Pre-clone dependency gate"). It surfaces in three places: a `warn` line naming the count and the cap at each cap-check site, a Dashboard banner listing the action, key, count against the cap, and last counted attempt, and an `attempt_cap_hits` block on `/api/status` (so `claude-run status` reports it with or without the daemon). The read is the live `attempt_counters` table compared against each action's OWN effective cap, so lowering the cap parks a key immediately and raising it releases the key and grants one more attempt.

CLAUDE-301: for the `youtrack` native action (`src/native/youtrack.rs`), the cap check no longer just skips the job silently. It posts one comment naming the attempt count and the cap and stating that re-queueing alone will not retry it, then sets `AI Agent` to the configured `needs_review` label as the last write - the same label the needs-review marker and a timeout use for "parked pending a human". This is why the issue no longer stays Queued forever: it leaves the candidate set the moment the cap is reported, and the comment is posted exactly once, on the poll that reports it. A failed comment or field write is logged at `error` naming the key and the cause; the cap is still reported as hit either way, so the Dashboard banner keeps showing the key until a write actually lands. The generic scripted runner (`src/job.rs`) has no tracker field to write - a scripted action's cap hit still just skips the job with a `warn`, since there is nothing in the action's own model to move.

Unlike a gate row, the counter row is not deleted when the trouble clears: it is the durable record of the attempts, and nothing (not even a successful run) resets it. So the runner cannot retry a capped key by itself. CLAUDE-238 adds a per-key reset for exactly this: `POST /api/actions/<name>/attempts/<key>/reset` (404s a key with no counter row), a `claude-run action reset-attempts <name> <key>` CLI subcommand working with or without the daemon, and a Reset control on each row of the Dashboard banner. A reset is recorded at `warn` naming the action, the key, and the discarded count, so it is not an invisible mutation. Raising `limits.max_attempts_per_key` for the whole action, or deleting the action (which purges its whole control state), still work as before; the reset is the narrower, per-key alternative to both.

CLAUDE-379: a run that hits `claude_timeout_seconds` (exit 124) no longer just parks the issue with whatever the agent built left behind in the workdir. `finalize`'s timeout branch commits any dirty tree as a `WIP: uncommitted changes at timeout` commit, pushes, and opens a **draft** PR from it (Forgejo: `WIP: `-prefixed title, fj's own draft convention; GitHub: the real draft flag) with a comment on the PR explaining the timeout and that re-queuing resumes this branch. A clean tree with nothing ahead of the remote default branch salvages nothing, and the timeout comment says so instead. Every salvage step (the git-state read, the WIP commit, the push/PR-create, the PR comment) that fails is logged at `error` and named in the issue's timeout comment separately, never swallowed. The issue still ends on `AI Agent = needs_review` and the attempt counter still increments exactly as a timeout always did; the salvaged PR is recorded into the CLAUDE-275 `agent_pull_requests` store the same way a successful run's PR is, so a re-queue resumes onto it, and the built-in prompt's resume step tells the agent to also read that PR's own comments. A later resumed run that finishes successfully strips the draft/`WIP: ` status and overwrites the salvage's placeholder title/body with the finished tip's own text (CLAUDE-398). See [Timeout salvages work as a draft PR](../docs/conventions/ci-failures.md#timeout-salvages-work-as-a-draft-pr-claude-379) for the full mechanism.

CLAUDE-234: `max_issues_per_run` bounds a different axis from the other caps. `max_runs_per_day` bounds how often an action FIRES; this bounds how many issues ONE firing may create in the tracker, so a misfiring filing run opens a handful of issues rather than a hundred. It is enforced at the provider boundary (`src/provider/audit.rs`, `AuditedTracker` wrapping the `yt` backend for the duration of a run), never in a prompt: a prompt is guidance, and the ceiling exists for the case where the guidance is ignored. Reaching it refuses the create BEFORE `yt issue create` runs, logs at `warn`, and records a `run_events` row on the run, so a truncated filing run does not read as a clean one. `0` means "create nothing" rather than "unlimited". The same wrapper writes one `youtrack issue write` line per create AND per update, carrying the originating `run_id` and the issue key, so an unexpected issue resolves to a run in one journal query.

The ceiling covers the writes the RUNNER itself makes through the provider (the mint, the closed-issue-summary filing, every field / state / comment write the native actions perform). It does not reach the `yt` calls an agent session makes inside its own workdir: those are a separate process with its own credentials, bounded by the agent prompt rather than by this cap (tracked separately).

Deletes are denied on this surface by omission: `IssueTracker` exposes no delete, the CLI backend invokes no `yt issue delete`, and `no_delete_on_the_tracker_surface` (`src/provider/audit.rs`) fails if either reappears.

DEV-705: `run_budget_usd` bounds a third axis. `max_runs_per_day` bounds how often an action fires and `daily_budget_usd` bounds a whole day across runs; `run_budget_usd` bounds what ONE firing may spend, so a single runaway run cannot eat the day. It is read from this file BEFORE the run starts, snapshotted into the run's `EffectiveLimits` (which is `Copy` and is moved into each per-job task by value), and never re-read, so it cannot be raised while the run works: raising it means editing the action and starting a new run, which makes the decision deliberate and auditable. Nothing offers to raise it, and there is no interactive path that could. Unset, it falls back to the effective `daily_budget_usd`, so adding the ceiling loosens nothing and changes no shipped action.

Enforcement is two-layered. The runner accumulates each invocation's `total_cost_usd` into one per-run ledger (`crate::usage::RunSpend`, shared by every job of the run) and checks it before every job pickup; a run that has reached the ceiling starts no further job, closes its `runs` row with exit code 6, and logs a reason naming the budget and the spend. Between those checks a single session could still overshoot, so the runner also passes the run's REMAINING dollars to the child as `--max-budget-usd`, which stops the session itself. `budget_warn_fraction` (default `0.80`) emits one `warn` per run when the spend first passes that fraction of the ceiling. A cancel or a global pause keeps its own exit code; the over-budget code only overrides a run that would otherwise have finished cleanly.

DEV-707: every run also reports what it spent. The runner replays each invocation's stream log for input / output / cache-read / cache-write tokens per turn, tool call counts, and per-tool attribution (a turn is attributed to the tool whose result most recently entered its context, so a dominant tool is identifiable), and emits one `run usage` summary line plus one newline-delimited JSON record per run at `$CLAUDE_RUN_STATE_DIR/logs/usage.jsonl`. The record carries the run id, action, exit code, wall-clock duration, invocation count, cost against the budget, tokens by category, turns, tool calls, `peak_context_tokens`, `cumulative_context_tokens`, `context_per_tool_call`, and the per-tool breakdown. `context_per_tool_call` is the DEV-706 measurement: flat as the number of tool calls grows is the linear signature, rising with it is the quadratic one.

CLAUDE-315: every `claude` invocation also reports what the CLAUDE SUBSCRIPTION itself thinks of its own rate limit, separate from the dollar spend above. The runner replays the same stream log for a trailing `rate_limit_event` (a `status` plus five-hour and seven-day window utilization) and every `api_retry` event, logs one `claude usage` line per invocation (`warn` instead of `info` when the status is not `allowed` or a window is at or past 90% utilization, and `rate_limit = "absent"` when the log carried no such event at all), and persists one row to the `claude_invocations` table. Every field is nullable and is never defaulted to zero: an invocation with no rate-limit reading is missing data, not a 0% reading. `/reports`' "Claude plan usage" section charts it as two utilization lines (solid five-hour, dashed seven-day), breaking the line at any invocation with no reading rather than interpolating through it, and flagging a timed-out or failed invocation's marker.

### `concurrency` (record, optional)

Controls keyed parallel execution of a run's jobs. By default a run processes its jobs strictly one at a time. With
`concurrency` you can run independent jobs in parallel while guaranteeing that two jobs touching the same unit of work
never run at once.

```yaml
concurrency:
  max_parallel: 4                  # up to 4 distinct-key jobs at a time
  key: [ project, repo, workflow ]   # default; the per-job concurrency key
```

| Key            | Default                     | Meaning                                                   |
|----------------|-----------------------------|-----------------------------------------------------------|
| `max_parallel` | `1`                         | Maximum distinct-key jobs in flight at once for this action, enforced daemon-wide. |
| `key`          | `[project, repo, workflow]` | Job fields whose values compose the concurrency key.      |

Semantics:

- **Same key serializes.** At most one job per resolved key runs at a time, so
  two jobs against the same repo never collide on the clone dir, branch, or PR. Per-key concurrency is fixed at 1 and is
  not configurable. This holds across **overlapping runs** too (see below): an issue queued into a second run against a
  repo a first run is still working never runs concurrently with it.
- **Distinct keys parallelize**, up to `max_parallel`. `max_parallel: 1` (the default) reproduces the original
  strictly-sequential behavior, now enforced daemon-wide (at most one job for the action across all its overlapping runs).
- **Key resolution.** For a scripted action the key is built from the listed fields on each job's JSON object; for the
  native `youtrack` action the handler maps `project` to the issue-id prefix and `repo` to the resolved repo path.
  `workflow` is forward-looking (reserved for a future Forgejo-failed-workflow action) and is unset on YouTrack jobs
  today. When none of the configured fields resolve, the job's own key is used so it is treated as independent.
- **Daemon-wide gate.** The key gate is held at the daemon level, so two executions sharing a key never overlap even
  across a scheduled run and a manual trigger. (It is in-memory per process; a standalone `claude-run run` uses its own
  gate but the per-action lock still prevents it from overlapping a daemon-driven run of the same action.)
- **`max_parallel` is a daemon-wide cap (CLAUDE-162), not a per-run one.** The daemon holds one shared semaphore per
  action, so the total number of concurrent jobs for an action stays `<= max_parallel` even when several **overlapping**
  pickup runs are in flight at once. A separate global cap (`max_parallel_global`, a runtime setting) bounds total
  concurrent jobs across all actions. Each job first gates on its concurrency key with a non-blocking try, then acquires a
  global permit, then the per-action permit, in that fixed order, so the pipeline is deadlock-free.
- **A same-key backlog never starves other keys or actions (CLAUDE-163).** A permit (global or per-action) is only ever
  held by a job that actually holds its concurrency key and is running, never by one merely waiting on a key. A job whose
  key is already held by a running sibling takes no permit and defers, staying Queued for the next pickup gather. So one
  project with a large queued backlog drains one issue at a time without consuming the caps: other projects (distinct
  keys) and other actions keep running, and the scheduler no longer logs `pickup deferred: concurrency cap saturated` when
  the only in-flight work is a single serialized key. (Before CLAUDE-163 the caps were exhausted by same-key siblings
  parked on the key gate while holding permits.)
- **Caps stay best-effort.** The daily cap and budget are checked before each job launches; because up to
  `max_parallel_global` jobs can pass the check before any of them advances the ledger, the cap may overshoot by at most
  `max_parallel_global - 1` jobs.
- **`max_parallel` is bounded by available worker threads.** Each running job occupies a thread for the duration of its
  `claude` invocation, so set `max_parallel` no higher than the host has cores to spare.

#### Overlapping runs and continuous pickup (CLAUDE-162)

The daemon no longer serializes runs per action. An issue queued while a run is in flight is picked up by a new,
**overlapping** run within `regather_interval` (a runtime setting, default 60s) rather than waiting for the in-flight run
to drain, subject only to the concurrency caps above. The whole-run per-action file lock that used to force a full drain
is gone on the daemon path; a manual `trigger` during an in-flight run now starts an overlapping pickup run instead of
being dropped. Two overlapping runs never both work the same job: each run drops gathered jobs whose **concurrency key**
is already in flight in another open run (CLAUDE-163 widened this admission check from issue id to concurrency key, so a
run never gathers a second same-project issue while that project is in flight; the check also suppresses empty, redundant
runs), the native `youtrack` handler re-reads the live `AI Agent` field per job, and the daemon-wide key gate serializes
same-repo work. `max_parallel_global` and
`regather_interval` are edited on the web **Settings** page (the Concurrency section) or via
`GET`/`PUT /api/settings/concurrency`.

> The standalone `claude-run run <action>` path still takes the per-action cross-process `flock`. Running standalone
> `run` for an action the daemon also schedules is unsupported concurrent use.

### `schedule` (record, optional)

Controls when the daemon auto-fires the action. A record with a required `kind` (one of `interval`, `cron`, `manual`)
and an optional `spec`:

```yaml
schedule:
  kind: interval
  spec: 30m
```

Only the daemon (`claude-run serve`) acts on `schedule:`; a bare `claude-run run <action>` ignores it and processes the
action once immediately.

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

`spec` is a cron expression: either the common **5-field** form (`minute hour day-of-month month day-of-week`) or a
**6-field** seconds-precision form (`second minute hour dom month dow`). A 5-field expression is internally prefixed
with `0 `, so it fires on the 0th second of the matching minute. The action fires at the next instant matching the
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

Never auto-fires. The action runs only when triggered explicitly (see below). Omitting the `schedule:` block entirely is
equivalent to `kind: manual`: the scheduler skips any action with no `schedule.kind`.

```yaml
schedule:
  kind: manual
```

#### Triggering and the one-block rule

Manual triggering via `claude-run trigger <action>` (and the web UI trigger button) works **regardless of the schedule
kind**, because the trigger path is independent of `schedule:`. An interval- or cron-scheduled action can still be fired
on demand at any time.

Only **one** `schedule:` block is allowed per action (YAML rejects a duplicate mapping key). There is therefore no
"interval plus manual" mode: just use `kind: interval` (or `cron`), and trigger it on demand whenever you need an extra
run. Declaring two `schedule:` blocks to get both is the common gotcha and fails to parse.

#### Resolution

The daemon evaluates schedules on a fixed 5-second tick, so an action fires on the first tick at or after its due time.
Intervals shorter than 5s are not meaningful, and cron seconds-precision is honored only to 5s granularity.

## Runner environment variables

These are the variables an ACTION author meets. Each one below is read by the Rust binary; verify with
`grep -rn CLAUDE_RUN_ src/`. The daemon-level variables (`CLAUDE_RUN_DATABASE_URL`, `CLAUDE_RUN_DATABASE_URL_FILE`,
`CLAUDE_RUN_SECRETS_KEY_FILE`, `CLAUDE_RUN_LOG`) are the operator's and are documented in `docs/configuration.md` and `docs/auth.md`, not repeated here.

| Variable                  | Default                          | Purpose                                                                                          |
|---------------------------|----------------------------------|--------------------------------------------------------------------------------------------------|
| `CLAUDE_RUN_CONFIG_DIR`   | `$XDG_CONFIG_HOME/claude-run`    | `config.toml` and `actions/*.yml`.                                                               |
| `CLAUDE_RUN_DATA_DIR`     | `$XDG_DATA_HOME/claude-run`      | `state.db` (SQLite backend only) and `workdirs/`.                                                |
| `CLAUDE_RUN_STATE_DIR`    | `$XDG_STATE_HOME/claude-run`     | `logs/`: `daemon.log`, per-action `output.file`, and the per-attempt stream logs. Also `snippets/` and `webhook-logs/`. |
| `CLAUDE_RUN_RUNTIME_DIR`  | `$XDG_RUNTIME_DIR/claude-run`    | `daemon.pid`, `daemon.sock`, and `locks/<name>.lock`.                                            |
| `CLAUDE_RUN_ACTIONS_DIR`  | `$CLAUDE_RUN_CONFIG_DIR/actions` | Resolves bare action names to YAML files (same as `--actions-dir`).                              |
| `CLAUDE_RUN_WORKDIR_BASE` | `$CLAUDE_RUN_DATA_DIR/workdirs`  | Per-job workdir root. Final path = `<base>/<name>/<key>`. The action's `workdir_base` field wins. |

There is no environment variable for any `limits` key. CLAUDE-244: the table above used to carry
`CLAUDE_RUN_MAX_RUNS_PER_DAY`, `CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY`, `CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS`,
`CLAUDE_RUN_DAILY_BUDGET_USD`, and `CLAUDE_RUN_LOG_DIR`, all inherited from the removed `scripts/claude-run.nu` runner
and read by nothing in the Rust binary, so an operator setting one changed no behaviour and got no warning. The limit
defaults come from `RunnerDefaults::default()` (`src/job.rs`) and an action's YAML `limits` block is the only override;
logs live under the state dir as shown above.

## State + lock layout

```
$CLAUDE_RUN_STATE_DIR/
  logs/daemon.log                              # serve --detach stdout + stderr
  logs/daemon.log.1                            # its one rolled-over predecessor
  logs/<name>/<output.file>                    # JSONL per-job outcomes if output.file set
  logs/<name>/<output.file>.1                  # its one rolled-over predecessor
  logs/<name>/streams/<key>-attempt-<n>.jsonl  # claude stream-json output per attempt
  logs/usage.jsonl                             # DEV-707: one per-run token / cost record per line
  snippets/<name>/<key>/                       # rendered snippet scripts + one binding-<name>.json per $-binding
  webhook-logs/                                # redacted CI job logs the Forgejo webhook triage fetched
```

What bounds each path:

- `logs/daemon.log`: size, `config.stream_log_gc.daemon_log_max_bytes` (default 50 MiB). Past the ceiling the daemon's log GC pass copies the file to `daemon.log.1` and truncates it in place back to its newest half, so the daemon's open append handle keeps writing to the live file.
- `logs/<name>/<output.file>`: size, `config.stream_log_gc.output_file_max_bytes` (default 50 MiB), same rollover. It is bounded by size and never deleted by age: `output.file` is an operator-configured data sink, so the sweep keeps the newest records in place instead of destroying the record.
- `logs/<name>/streams/*.jsonl`: age, `config.stream_log_gc.retention_days` (default 30). A log whose job is still in flight is kept at any age.
- `snippets/<name>/<key>/`: age, `config.stream_log_gc.snippet_retention_days` (default 30). A directory whose job is still in flight is kept at any age, the same guard the stream logs use.
- `webhook-logs/`: age, `config.stream_log_gc.webhook_log_retention_days` (default 30), applied to files directly under it. `webhook-logs/analysis/<owner>_<repo>-run<N>/` (CLAUDE-274, replacing the old `webhook-logs/triage/`) is a PER-EVENT working directory the [`crate::triage::AnalysisClassifier`] creates and removes itself once the classification returns, so it needs no sweep of its own; only files directly under `webhook-logs/` (the redacted log artifacts) are swept.
- `logs/usage.jsonl`: size, `config.stream_log_gc.usage_log_max_bytes` (default 8 MiB), same rollover to `usage.jsonl.1`. The truncation cuts at a line boundary, so what survives is still parseable newline-delimited JSON rather than a part-record (CLAUDE-268).

Pause state and per-key accounting are NOT files here. Global pause is the `global_pause` flag and per-action pause the
`paused_actions` table (`src/pause.rs`); attempt counters, daily counts, and daily cost are the `attempt_counters` and
`daily_ledger` tables. All of them live in the state store (`state.db`, or PostgreSQL). The legacy nu runner's
`state/<name>.json` files are read once by `claude-run migrate` and are never written.

The per-action lock is a file, but it lives in the runtime dir, not here:

```
$CLAUDE_RUN_RUNTIME_DIR/
  daemon.pid                   # daemon pidfile lock
  daemon.sock                  # CLI control socket (absent under serve --no-socket)
  locks/<name>.lock            # flock keyed on action name, held for one run
```

## Synthetic webhook action (`webhook-triage`)

The inbound webhook (`POST /webhooks/{provider}/triage/<source>`; `forgejo` and `github` are both implemented, CLAUDE-362 adding `github`) surfaces on the dashboard as a synthetic action named `webhook-triage` (renamed from `webhook-fast-path` in CLAUDE-153, then from `webhook-forgejo` and its route `/webhooks/forgejo/<source>` in CLAUDE-361, when the listener's route and name stopped being Forgejo-specific: an on-disk stream log under either older name is carried forward at serve start, and the old route 404s). It is not a YAML file under `actions/`: it is Rust-defined, injected by the web layer only when at least one webhook source is configured, and reserved (an on-disk YAML claiming the name `webhook-triage` is refused at scan). Its per-key attempt counter and daily run ledger are recorded under this name like any other action, so its run history, durations chart, and last-run recency render on the dashboard. A one-time startup migration (SQL migration `0013` plus a log-directory rename) carries any pre-rename history across.

- **Read-only:** edit, export, and delete are rejected (HTTP 403); there is no backing YAML to operate on, and a manual trigger is rejected (HTTP 400) because it is event-driven, not scheduled.
- **Pausable (CLAUDE-153):** it participates in the same per-action Pause as every other action. Pause/Resume controls and a Paused badge appear on the dashboard, and `POST /api/actions/webhook-triage/pause` / `/resume` write and clear its `paused_actions` row. While paused, an inbound delivery is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, then dropped without minting or queuing, so Forgejo does not treat it as a failed delivery and retry. A dropped delivery is not recovered after Resume. CLAUDE-223: the GLOBAL pause is checked first and gets the identical accept-and-drop treatment.
- **Enabled switch (CLAUDE-360):** the `Webhook` settings section carries an `enabled` boolean, default `false`. While `false`, `/webhooks/forgejo/triage/<source>` 404s every request regardless of `sources`, logged as `"webhook rejected: disabled"` (distinct from the unknown-source rejection). A save that sets `enabled: true` is rejected unless `sources` is non-empty and every source has `secret_ref` set, naming every offending source key, so "enabled" can never mean "accepting requests with no real authentication configured behind it." At `serve` start, a stored document with `sources` configured but `enabled` unset (the upgrade-time default) logs one `warn` naming the section rather than going quiet with no explanation.
- **Auto-fix toggle (CLAUDE-153/CLAUDE-273):** the `Webhook` settings section carries an `auto_fix` boolean, default `false` (opt-in), that decides queuing ONLY. When off, a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) still creates an issue but with no `AI Agent` write, so no corrective fixup PR is queued: the failure stays reported on the original PR for its author. A `human_action` failure is unaffected either way. When on, the corrective-fixup behavior applies. This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. It is DB-backed (`config.webhook`) and edited live on the Settings page or via `PUT /api/settings/webhook`.
- **Open-PR routing by who opened the PR (CLAUDE-276):** before triage, the consumer resolves whether the failing commit is the head of some open PR (`resolve_open_pr`, listing the repo's open PRs); a lookup failure fails the run (`ok = false`, an `error` log) rather than being read as "no PR". No PR at all runs the dedupe-then-mint pipeline below unchanged. An open PR routes by whether `agent_pull_requests` has an `open` row for it: the agent's own PR re-queues its issue (comment + bump `fix_attempts` + `AI Agent = Queued`, or `Needs Review` at `config.webhook.agent_pr_fix_attempts`, default 3) with no PR comment and no new issue; anyone else's PR always gets the analysis as a PR comment (sharing an already-`open` failure gets a "this is already tracked" comment and an attach instead of a fresh analysis; the same failure appearing on a SECOND branch's PR mints one issue and comments both PRs). PR comments need the source's own `write_token_ref` (`write:issue` only); a source without one fails the run the moment a comment is needed rather than posting nothing silently. See "Routing a CI failure on an open PR (CLAUDE-276)" above and "Webhook (Forgejo or GitHub)" in `docs/webhooks.md`.
- **One tool-using analysis at ingest, no separate scheduled pass (CLAUDE-274):** a failure the store does not already track gets ONE `claude` session (`Read,Grep,Bash`, timeout `config.webhook.analysis_timeout_secs`, default 600s) over the COMPLETE redacted log and the failing workflow YAML, in its own `webhook-logs/analysis/<owner>_<repo>-run<N>/` working directory (removed once the session returns). The session ends with a fenced JSON verdict (`code_defect` | `needs_human` | `flaky` | `infra`) that routes the failure exactly as the old two-value classifier did (`code_defect` -> code-fix, `needs_human` -> human-assigned, `flaky` / `infra` -> transient, no issue), and the session's own Markdown report becomes the issue's `## Triage` analysis text (capped at 24 KiB), so the issue carries the deep, log-grounded analysis from the moment it is filed. `consume_events` processes events through a worker pool bounded by `config.webhook.analysis_concurrency` (default 2, at least 1): each event is a spawned task acquiring a semaphore permit, so a burst of failures runs at most that many analyses at once instead of one 600s session blocking every later event or an unbounded burst starting them all at once. The store lookup, the analysis, and the mint for one event additionally run under a `concurrency::KeyGate` on `(repo, workflow_id, job_name)`, so two concurrent failures of the same job never both analyze: the second waits, then finds the first's store row and attaches instead.
- **Dedupe by failing-test overlap (CLAUDE-273, store-backed):** before triage runs, the consumer fingerprints the redacted log into `<target>::<test>` ids (`ci_failure::fingerprint`) and looks them up in the `ci_failures` store by `(repo, workflow_id, job_name)` overlap. A hit on an `open` row attaches this run (no triage, no `claude` call); a hit on a `resolved` row reopens it (`set_state`, a regression comment, then triage for the analysis comment and the `AI Agent` write); no hit falls through to one legacy YouTrack `#Unresolved` search on the pre-store collapse marker (adopting an issue minted before the store existed), then the ordinary mint. A store read or write failure fails the run rather than falling through to a mint. `mint::mint` itself no longer searches YouTrack or declines for a duplicate or a resolved issue. The issue body still carries a hidden `Dedupe-Key:` marker (the pre-store cross-run collapse key `hash(repo, branch, workflow_id, job_name, failure_signature)`, falling back to `hash(repo, branch, workflow_id, job_name)` with no derivable signature), because the legacy-adoption lookup above still reads it.
- **GitHub log fetch (CLAUDE-363):** the webhook accepts, authenticates, and dispatches a GitHub `workflow_run` delivery exactly like a Forgejo one (same `VerifiedFailureEvent`, same triage/dedupe/mint pipeline), and the daemon's log-fetch client (`ActionLogClient`, wired in `daemon.rs`) is a `HostRoutedForgeReads` that serves a `github.com` host over `GhForgeReads` (`gh`/the GitHub API), falling back to `FjForgeReads` otherwise. No per-action `github.hosts` list is known at this point in the pipeline, so a GHE host configured only there still falls back to `fj`.
- **Dedupe (infrastructure keying, CLAUDE-264):** the key above contains `branch`, so a failure caused by the RUNNER rather than the commit never collapsed: a full disk is identical on every branch and fixable on none of them, and with one PR branch per tracker issue every run minted a fresh issue. When `src/infra_failure.rs`'s `detect` recognizes such a condition, the cross-run collapse key becomes `hash(repo, workflow_id, job_name, infra_kind)` instead: no `branch` and no commit, and the stable kind id rather than the matched line, so the same condition keys identically however the message is worded or however many bytes free it reports. N branches hitting one host condition on one `(repo, workflow, job)` collapse onto ONE open issue; two different kinds on the same job stay two issues. The detector is pure string work over the FULL redacted log (no LLM, no I/O), reading the same text `failure_signature` does rather than the 48 KB copy the classifier prompt gets, because a capacity abort is post-steps and is exactly what that bound cuts. Three deliberately conservative kinds ship, each requiring an unambiguous phrase: `disk-full` (`no space left on device`, `enospc`, a capacity guard naming insufficient free space), `out-of-memory` (`out of memory`, `oom-kill`, `cannot allocate memory`, `killed (signal 9)`), and `runner-unavailable` (`could not resolve host`, or a reset connection on a line naming a package or container registry). An over-broad pattern would merge unrelated code failures across branches, so an undetected condition is the preferred failure mode. Triage routing is unchanged except for `runner-unavailable` (CLAUDE-266): `triage_rules` runs the detector before the transient scan and routes that kind to `human_action` (no LLM call), so it mints one grouped human-assigned issue; `could not resolve host` is no longer in `TRANSIENT_SIGNATURES`, and a bare `connection reset` with no registry marker stays transient and mints nothing (reclaiming disk is a human action). With no infra kind detected, every key and every decision is exactly as it was.

## Synthetic comment-webhook action (`webhook-comment`)

The inbound `/claude <instruction>` comment listener (`POST /webhooks/{provider}/comment/<source>`; `forgejo` and `github` are both implemented, CLAUDE-362 adding `github`; CLAUDE-220 for pull requests, CLAUDE-221 for plain issues) surfaces on the dashboard as a SECOND synthetic action named `webhook-comment` (renamed from `webhook-forgejo-comment`, and its route from `/webhooks/forgejo-comment/<source>`, in CLAUDE-361; the old route 404s), alongside `webhook-triage`. Same shape as its sibling above: no YAML file under `actions/`, Rust-defined, injected by the web layer only when at least one comment source is configured, and reserved (an on-disk `webhook-comment.yml` is refused at scan with a logged reason). Both reserved names come from ONE shared predicate (`crate::web::SYNTHETIC_ACTIONS`), read by the daemon's scan guard, the API's trigger / delete rejections, and the web layer's edit / export / delete rejections, so a new event-driven listener is reserved everywhere by adding one entry.

It is a separate action from `webhook-triage`, not a second event type on it, because the tokens differ in power: this listener's `write_token_ref` pushes branches (`write:repository` plus `write:issue`), while a CI-failure source's tokens only read logs (`log_token_ref`) and comment on pull requests (`write_token_ref`, `write:issue`). Separate actions also mean separate Pause switches.

- **Read-only:** edit, export, and delete are rejected (HTTP 403); a manual trigger is rejected (HTTP 400) because it is event-driven, not scheduled.
- **Enabled switch (CLAUDE-360):** mirrors the CI-failure listener's own switch above: `config.webhook_comment.enabled`, default `false`, gates `/webhooks/forgejo/comment/<source>` to a 404 (`"webhook rejected: disabled"`) regardless of `sources` while `false`, a save enabling it requires every source to carry `secret_ref`, and `serve` start warns once when `sources` is configured but this is unset.
- **Pausable:** it participates in the same per-action Pause as every other action, and in the GLOBAL pause (CLAUDE-223), which is checked first. While either is set, an inbound delivery is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, then dropped without running anything, so Forgejo does not retry. A dropped delivery is not recovered after Resume.
- **Two routes, one action (CLAUDE-221):** the route is chosen by whether the comment's thread is a pull request. A PR comment clones the head branch, runs Claude, and pushes within the decision-gate caps. A PLAIN ISSUE comment has no branch to clone or push, so it clones nothing and starts no agent session: it mints a YouTrack issue in the project the repo routes to (the same `[mint]` override / VCS-integration / fallback order the CI-failure mint uses), carrying the instruction verbatim plus a link back to the Forgejo issue, sets `State` to `[mint].target_state`, and sets `[mint].ai_agent_field` to `[mint].queued_value` (`AI Agent = Queued`) LAST, so the `youtrack` action picks it up only once the issue is fully formed. Replies on that route go through `fj issue comment <owner>/<repo>#<n> --body-file <path>` (`Forge::issue_comment`), and every outcome replies: minted, created-but-not-queued (naming the key and the field to set by hand), and create-failed. It is a ROUTE, not a second listener: the authorization gate, trigger parsing, bot-login loop drop, dedupe key, and caps are the same implementations both paths run through.
- **Accounting:** runs are recorded under this name with the same per-key attempt counter and daily run ledger as every other action. The attempt key is `hash(repo, number, comment_id)` on both routes (a Forgejo comment id is unique across a repo's issues and pull requests); because a comment is a one-shot request its effective per-key cap is one attempt, so a redelivery of the same comment never runs the work twice and never mints a second YouTrack issue. The daily run ledger advances for a mint as well as for a Claude run. `runs.triggered_by` stays `webhook` (the action name distinguishes the two listeners).
- **Concurrency:** at most one comment job per thread, through the daemon-wide key gate. The gate key carries the thread kind, so a plain issue can never block the like-numbered pull request. A second command while one is running gets a reply saying so and is dropped.
- **Authorization:** the commenter's effective repository permission is read LIVE per command via `fj repo collaborator <owner>/<repo> permission <username> --format json` (`Forge::repo_permission`); no raw REST on this path. Only a permission in the source's `allowed_permissions` (default `["write", "admin"]`), or one that outranks an entry in it (`none` < `read` < `write` < `admin` < `owner`), may run a command, and a lookup that errors denies (fail closed). Denial replies are throttled per `(repo, login)`.
- **Config:** the `Comment webhook` settings section, DB key `config.webhook_comment`, edited on `/settings` or via `GET` / `PUT /api/settings/webhook_comment`. The plain-issue route adds no settings of its own; it reads the existing `Mint` section (`config.mint`) for project routing, target state, and the `AI Agent` field / queued value. It is DB-only from day one: it is absent from `OnDiskConfig`, so a `config.toml` declaring `[webhook_comment]` fails to parse as an unknown key, and there is no file-to-DB import path. Source and rate-limiter edits apply live. See "Comment commands on a pull request" in `docs/comment-commands.md` for the full field list and the Forgejo webhook setup.

## Deleting an action

An action can be deleted from all three surfaces: the CLI (`claude-run action delete <name>`), the web UI (the Delete button on an action's detail page), and the JSON API (`POST /actions/<name>/delete`).

Delete is a config-and-control purge, not a history wipe. It removes the on-disk YAML file, the `actions` index row, the `paused_actions` row, and the `attempt_counters` / `daily_ledger` rows for that action. It KEEPS the action's run history: the `runs` and `job_outcomes` rows and the on-disk stream logs under `logs/<name>/` survive, each for as long as its own retention window allows (`config.db_retention` for the rows, `config.stream_log_gc` for the stream logs). Re-creating an action with the same name re-associates that retained history, which is the intended audit behavior.

Delete is refused while a run for the action is in flight, so removing a config can never orphan a running job. The CLI returns exit code 7 (conflict) and the API / web UI return HTTP 409; cancel the run (`claude-run cancel <run-id>`) or wait for it to finish, then retry. An unknown action returns exit code 3 from the CLI and HTTP 404 from the API / web UI.

The index is self-healing: `scan_actions` prunes any `actions` row whose backing YAML no longer exists on disk, so an out-of-band `rm` of an action file is reconciled on the next scan rather than leaving a stale dashboard row. Pruning removes only the index row; retained run history is untouched.

## Exit codes

| Code | Meaning                                                                   |
|------|---------------------------------------------------------------------------|
| 0    | Run completed (with or without finding work).                             |
| 1    | Paused. Returned by `run` when a pause is already set at start, and recorded on the `runs` row of a run the global pause stopped mid-flight (CLAUDE-223). |
| 2    | Lock contention (previous run still going).                               |
| 3    | Config error (action YAML missing, malformed, or missing required field). |
| 4    | Daily cap reached (`max_runs_per_day`).                                   |
| 5    | Unexpected error (see logs).                                              |
| 6    | Over budget: the daily budget (`daily_budget_usd`) was already spent when the run started, or the run reached its own ceiling (`run_budget_usd`) while working (DEV-705). |
| 7    | Conflict: a mutating command was refused (e.g. `action delete` while a run for that action is in flight). |
| 8    | Login required: the run stopped picking up new jobs because the Claude login needs renewing (CLAUDE-269). |
| 130  | Run cancelled in flight (operator cancel via the API / web UI / CLI).     |

Exit code 130 is recorded on the `runs` row, not returned by the `run` subcommand; the web UI and CLI render it as a
distinct "canceled" badge. CLAUDE-223: exit code 1 is likewise recorded on the `runs` row of a run that stopped picking
up new jobs because the operator paused globally mid-run, and badges as "paused"; it is excluded from the run list's
"failed" filter, because the run did not fail.

CLAUDE-269: exit code 8 is recorded the same way, on a run that stopped picking up new jobs because the CLI itself
reported an authentication failure (a structured `authentication_failed`, on an invocation or the periodic `claude
auth status --json` probe), distinct from a pause (an operator decision) and badged "login required" rather than
"paused" or "failed". An operator cancel wins over both; a global pause wins over login-required when both apply to
the same run.

## Snippet execution model

`input.command`, `filter`, `pre`, `prompt`, and `post` snippets run with the interpreter declared by the field's `shell`
(see [Script fields: `run` / `shell`](#script-fields-run--shell)), defaulting to Nushell. The runner writes the code to
a file and executes `<interpreter> <file>`.

Bindings available to snippets (the `$`-prefixed nu names below; under a non-nu `shell` the same names are environment
variables holding compact JSON):

| Binding          | `input.command` | `filter` | `pre` | `prompt` | `post` |
|------------------|:---------------:|:--------:|:-----:|:--------:|:------:|
| `$env`           |       yes       |   yes    |  yes  |   yes    |  yes   |
| `$job`           |        -        |   yes    |  yes  |   yes    |  yes   |
| `$workdir`       |        -        |   yes    |  yes  |   yes    |  yes   |
| `$claude_result` |        -        |    -     |   -   |    -     |  yes   |

`$workdir` is `<workdir_base>/<name>/<dedup_key-or-job-index>`, where `workdir_base` is the action's `workdir_base`
field if set, else `$CLAUDE_RUN_WORKDIR_BASE`, else `<data_dir>/workdirs`. The runner creates it before the first `pre` step
and **removes it at end of job regardless of outcome** (success, claude failure, pre/post step failure, transient
error). Plain `rm -r` is used, not `rm -rf`, so permission or read-only issues surface as real errors instead of being
silently force-deleted.

To inspect what happened in a failed run, look at the per-attempt stream log
(`$CLAUDE_RUN_STATE_DIR/logs/<name>/streams/<key>-attempt-<n>.jsonl`); it survives across runs since it lives outside
`$workdir`, up to the `config.stream_log_gc` retention window (default 30 days) past which the daemon sweep deletes it.

For `filter` and `pre`/`post`, exit code 0 = continue, non-zero = step failure (skip job, count attempt). The runner
captures each snippet's stdout: `input.command`'s is parsed as the job array and `prompt`'s becomes the prompt text,
while a `filter` / `pre` / `post` step's is read only for its exit code. A failing step's stderr is logged with the
failure.
