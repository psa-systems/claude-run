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
kind: scripted              # optional: scripted (default) | youtrack | forgejo-ci-fix | ci-failure-analysis | ci-failure-reconcile
enabled: true               # optional, default true; false skips the action in the scheduler
workdir_base: /path/to/dir  # optional, overrides $CLAUDE_RUN_WORKDIR_BASE for this action
link_template: https://youtrack/issue/{key}  # optional; {key} -> job key, builds a per-job link (render_link)
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
post: # optional, ordered list of nu snippets
  - |
    ...
output:
  file: <name>.log          # optional, appended JSONL of per-job outcomes
limits: # optional per-action overrides of env defaults
  max_attempts_per_key: 3
  max_runs_per_day: 20
  claude_timeout_seconds: 600
  daily_budget_usd: 5.00
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

Identifier used for the lock file, state file, log directory, and PAUSED flag. Should be kebab-case and unique across
all actions on the host.

The action index is keyed by this exact string, so two files whose `name:` differ only by case (`youtrack` vs
`YouTrack`) are indexed as two independent, separately-scheduled actions. At scan time the daemon flags (but does not
merge or disable) two kinds of redundant configuration among enabled actions (CLAUDE-164): (1) names equal ignoring
case, and (2) two native actions of the same `kind` polling the same source (for `kind: youtrack`, the same
`native.youtrack.query` and `base_url`; for `kind: forgejo-ci-fix`, the same `host` and `repos`). Either condition
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
- `forgejo-ci-fix`: a built-in Rust handler that fixes failing CI on Forgejo PRs (CLAUDE-29, supersedes DEV-330). Like
  `youtrack` it is config only and supplies a typed [`native`](#native-record-required-for-native-kinds) block.
- `ci-failure-analysis`: a built-in Rust handler that enriches each open "CI failure" / "Auto-fix" issue with a deep,
  log-grounded root-cause comment (CLAUDE-189). Config only, supplying a typed
  [`native`](#native-record-required-for-native-kinds) block.
- `ci-failure-reconcile`: a built-in Rust handler that auto-closes each open "CI failure" / "Auto-fix" issue once the
  failing `(repo, branch, workflow, job)` it tracks is green again on its latest run (CLAUDE-191), and collapses
  duplicate issues that share a root-cause key (CLAUDE-198). Config only, supplying a typed
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
    prompt: |                             # optional; full replacement of the built-in agent prompt
      Work issue {{key}}: {{summary}}
      Repo cloned at {{workdir}} ({{clone_url}}).
    states: # AI Agent / State labels the handler drives
      queued: Queued
      working: Working
      code_review: Code Review
      needs_review: Needs Review
      failed: Failed
      done: Done                          # AI Agent value set when automerge merges the PR
      in_progress: In Progress            # the standard State field value set in pre
      state_done: Done                    # the standard State value set when automerge merges the PR
    automerge:                            # optional; CI-gated automerge of the opened PR (CLAUDE-126)
      enabled: false                      # off by default; finalize stops at Code Review
      wait_seconds: 1800                  # max total seconds to wait for checks to go green
      check_interval_seconds: 30          # seconds between CI status polls
    dependencies:                         # optional; pre-clone dependency gate (CLAUDE-161)
      enabled: true                       # on by default; false makes the gate a no-op
      recheck_timeout: 5m                 # backoff before a blocked issue is re-evaluated
      block_on:                           # direction-resolved link verbs that block
        - depends on                      # Depend INWARD: an open blocker gates this issue
        - parent for                      # Subtask OUTWARD: an open child gates this parent
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
| `youtrack.query`        | `-Resolved AI Agent: Queued`       | Issue query selecting the jobs this action picks up.                                                |
| `youtrack.custom_field` | `AI Agent`                         | Custom field holding the AI Agent state machine.                                                    |
| `youtrack.assignee`     | none                               | Optional Assignee set on the issue during pickup (CLAUDE-131). A literal `yt` assignee argument: a login, `me`, or `unassigned`. When unset (default), the Assignee is never touched. An assignee write failure aborts pickup before the clone. Editable in the structured web form. |
| `youtrack.prompt`       | built-in default                   | Full-replacement agent prompt (CLAUDE-125). When unset, the built-in CLAUDE-24 prompt is used. When set, the text fully replaces it (so the operator owns the runner contract: bare `#<KEY>` commit reference, the `.claude-run-needs-review` / `.claude-run-skip-attempt` blocker markers, no push / no PR / no YouTrack write). Editable in the structured web form. See the placeholders below. |
| `youtrack.states.*`     | see above                          | Labels the handler reads/writes for each AI Agent / State value. `done` (AI Agent) and `state_done` (State) are set only when automerge merges the PR (CLAUDE-126). |
| `youtrack.automerge.enabled` | `false`                       | Master switch for CI-gated automerge (CLAUDE-126). When `false`, finalize stops at Code Review with no CI wait and no merge (unchanged behavior). |
| `youtrack.automerge.wait_seconds` | `1800`                   | Maximum total seconds to wait for the PR's checks to go green (and the branch to be up to date) before giving up and leaving the PR open. |
| `youtrack.automerge.check_interval_seconds` | `30`           | Seconds between combined-CI-status polls.                                                            |
| `youtrack.dependencies.enabled` | `true`                    | Master switch for the pre-clone dependency gate (CLAUDE-161). When `true`, a queued issue with any unmet dependency is left Queued (not cloned, no claude spend) and re-evaluated after `recheck_timeout`. When `false`, the gate is a no-op and pickup proceeds exactly as before. See [Pre-clone dependency gate](#pre-clone-dependency-gate-claude-161). |
| `youtrack.dependencies.recheck_timeout` | `5m`              | Humantime backoff before a blocked issue is re-evaluated. While the window is open the issue stays Queued with no `yt` read and no comment (no per-poll spam). Validated at action load. |
| `youtrack.dependencies.block_on` | `["depends on", "parent for"]` | Direction-resolved link verbs (YT-40) that block. A queued issue is blocked when it has any link whose `verb` is listed here and whose target is unresolved. The defaults block `depends on` (Depend INWARD, an open blocker) and `parent for` (Subtask OUTWARD, an open child); the reverse verbs `is required for` and `subtask of` never block. |
| `forgejo.base_url`      | per-issue VCS host                 | Forgejo base URL.                                                                                   |
| `forgejo.token_ref`     | none                               | Name of an encrypted secret holding the Forgejo API token (CLAUDE-26). `None` uses host `fj` auth. The token must carry the **`write:repository`** and **`write:issue`** scopes (see [Forgejo token scopes](#forgejo-token-scopes-claude-196)). |
| `forgejo.pr_strategy`   | `branch`                           | PR-creation strategy (CLAUDE-103). `branch`: resolve the feature branch by name (independent of the current `HEAD`; CLAUDE-166), push it as `git push --set-upstream origin <branch>:<branch>`, and open the PR over the API with an explicit `--head`, so the PR stays mergeable and updatable. `agit`: `fj pr create -aA`, a branchless PR whose head lives only at `refs/pull/<N>/head` and loses its merge button if it falls behind an up-to-date base. The selected strategy is authoritative (no silent cross-strategy fallback). |
| `github.hosts`          | empty                              | Extra GitHub host(s) beyond `github.com` to treat as GitHub (e.g. GitHub Enterprise). Bare hosts.   |
| `github.mcp_endpoint`   | `https://api.githubcopilot.com/mcp/` | Remote GitHub MCP endpoint used to open the PR (no local server to install).                      |
| `github.token_ref`      | none                               | Name of an encrypted secret holding the GitHub PAT (CLAUDE-26). `None` uses host `gh` auth / `GH_TOKEN`. |

##### Prompt placeholders (CLAUDE-125)

`youtrack.prompt` is a template. These `{{...}}` placeholders are substituted with each job's values before the
prompt is sent to the agent; an unknown placeholder (or an unterminated `{{`) is left literal and logged at warn:

| Placeholder              | Substituted with                                                              |
|--------------------------|-------------------------------------------------------------------------------|
| `{{key}}`                | Issue id (`idReadable`, e.g. `MK-16`).                                         |
| `{{summary}}`            | Issue summary.                                                                 |
| `{{clone_url}}`          | Full clone URL of the resolved repo.                                          |
| `{{workdir}}`            | Absolute path of the per-job working directory (the repo is cloned here).     |
| `{{needs_review_marker}}`| The `.claude-run-needs-review` marker filename.                               |
| `{{skip_attempt_marker}}`| The `.claude-run-skip-attempt` marker filename.                               |

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
| `repo view <repo> --json` (`:1284`)              | `fj_core::repo::get` -> `repo_get`                          | `GET /repos/{o}/{r}`                                   | repository (read)  |
| `pr create <title> --body-file .. --head <b> --json` (`:771`) | `fj_core::prs::create` -> `repo_create_pull_request` | `POST /repos/{o}/{r}/pulls`                            | repository (write) |
| `pr create -aA --json` (AGit, `:843`)            | same as above                                                | `POST /repos/{o}/{r}/pulls`                            | repository (write) |
| `pr search --repo <r> --state open --json` (`:1316`) | `fj_core::issues::list_page` -> `issue_list_issues` (`type=pulls`), then one `prs::get` per hit | `GET /repos/{o}/{r}/issues?type=pulls`, `GET /repos/{o}/{r}/pulls/{i}` | **issue (read)** + repository (read) |
| `pr status <id> --json` (`:1335`)                | `prs::get` + `prs::get_commit_statuses`                      | `GET /repos/{o}/{r}/pulls/{i}`, `GET /repos/{o}/{r}/commits/{sha}/status` | repository (read)  |
| `pr merge <id> --method merge --delete` (`:1365`) | `repo::get` + `prs::get` + `prs::merge` -> `repo_merge_pull_request` | `POST /repos/{o}/{r}/pulls/{i}/merge`          | repository (write) |
| `pr comment <id> --body-file <f>` (`:1407`)      | `fj_core::issues::create_comment` -> `issue_create_comment`  | `POST /repos/{o}/{r}/issues/{i}/comments`              | **issue (write)**  |
| `pr edit <id> labels --add <label>` (`:1427`)    | `fj_core::issues::edit_labels` -> `issue_list_labels` + `issue_add_label` | `GET /repos/{o}/{r}/labels`, `POST /repos/{o}/{r}/issues/{i}/labels` | **issue (write)**  |
| `pr view <id> --json` (`:1443`)                  | `prs::get` + `prs::list_files` + `repo::get`                 | `GET /repos/{o}/{r}/pulls/{i}`, `.../files`            | repository (read)  |

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

##### CI-gated automerge (CLAUDE-126)

By default a successful run opens the PR, sets AI Agent to `code_review`, and stops; the issue stays In Progress until a
human merges. Setting `youtrack.automerge.enabled: true` opts each successful run into merging the PR itself once its CI
checks pass. After the PR is opened, finalize polls the PR's combined CI status (`fj pr status`) every
`check_interval_seconds` up to a total budget of `wait_seconds`, honoring the run's cancel token so a `claude-run stop`
never hangs on a long wait. The outcomes:

- **Green and up to date:** the PR is merged as a **merge-commit** (never squash/rebase, so the single commit's bare
  `#<KEY>` reference survives in history), State is set to `state_done` and AI Agent to `done`, and a "runner merged"
  comment is posted. The run's outcome is `Merged`.
- **Branch behind base:** if the checks are green but the forge reports the PR as not mergeable (its branch is behind a
  base that requires an up-to-date branch), the runner performs the Forgejo "Update Branch" step once (it merges the base
  into the head with the git CLI and pushes, retriggering CI) and keeps polling within the same budget. `fj` exposes no
  `pr update-branch` subcommand, so this is done with git rather than the REST API. A branch that stays unmergeable after
  one update is left open for a human.
- **CI failure:** no merge; the PR is left open at Code Review and a failure comment is posted.
- **Budget exhausted while still pending:** no merge; the PR is left open at Code Review and a timeout comment is posted.

Automerge is currently implemented for the Forgejo (`fj`) backend only; a GitHub-hosted job with automerge enabled logs
the unsupported merge and leaves the PR at Code Review (GitHub automerge is a follow-up).

##### Pre-clone dependency gate (CLAUDE-161)

When issues are mass-assigned to the runner (AI Agent = Queued), an issue whose blockers are still open would otherwise be picked up, cloned, and handed to the agent only to discover it cannot proceed. The dependency gate moves that check before the clone, where it is knowable for free. The gate reads the issue's links (`yt issue inspect --json`, which since YT-40 carries each link's resolved `direction` / `verb` and each target's nullable `resolved` timestamp) and blocks the issue when it has any unmet dependency.

CLAUDE-182: the gate (and the live AI-Agent re-read) run at **gather time**, before the daemon-wide concurrency KeyGate is acquired, so a blocked or no-longer-Queued issue is dropped from the candidate set and never competes for its project's single per-poll work slot. Before CLAUDE-182 the gate ran only after an issue won the KeyGate, so a blocked issue could win the shared project key each poll, hit the gate, and consume the slot that a genuinely-workable same-project sibling needed, starving the sibling indefinitely. The gate is re-run once more after the winning issue takes the key (the authoritative pre-clone re-check for the cross-run TOCTOU); its `blocked_until` backoff and idempotent side effects make the second read cheap.

A link blocks when its direction-resolved `verb` is listed in `dependencies.block_on` and its target is unresolved (`resolved` is null/absent). The defaults are `depends on` (Depend INWARD: an open blocker gates this issue) and `parent for` (Subtask OUTWARD: an open child gates this parent). The reverse verbs never block: `is required for` (Depend OUTWARD, issues this one blocks) and `subtask of` (Subtask INWARD, this issue's parent). A blocker or child counts as **met** iff its `resolved` timestamp is non-null.

When blocked:

- The issue is left at AI Agent = `queued` (no transition to Working); it is **not** cloned and `claude` is **not** invoked.
- No `max_attempts_per_key` slot is consumed (the same no-attempt semantics as the skip-attempt / parked path); the run outcome is `Blocked`, distinct from filtered / failed / needs-review.
- A comment listing the unmet blockers (id + relationship) is posted on entering the blocked state, and re-posted only when the unmet-blocker set changes, so repeated re-checks do not spam the issue.
- A per-issue backoff is recorded: `blocked_until = now + recheck_timeout`. While the window is open the **scheduler's** periodic pickup skips re-evaluating the issue entirely (it stays Queued; no `yt` read, no comment). The state persists across polls and daemon restarts in the `dependency_gate` table (migration `0015_dependency_gate.sql`), keyed by (action, issue key).
- CLAUDE-195: a **human-initiated** run always ignores an open backoff window and re-reads the links live. "Manual" is any origin other than the scheduler's periodic pickup: `claude-run trigger <action>` (dispatch origin `trigger`) and a direct `claude-run run` (origin `cli`). An operator who just resolved a blocker and triggers expects an evaluation against the live tracker, not a replay of the cached signature, so the manual trigger picks the issue up in the same run instead of waiting out `recheck_timeout`. Only the periodic `scheduler` origin keeps the throttle. The bypass changes only whether an open window suppresses the read; every persistence and comment side effect (fresh signature / `blocked_until` when still blocked, row clear when unblocked, re-comment only on a changed blocker set) is unchanged.

When dependencies are met, the issue has none, or `enabled: false`, the gate is a no-op and pickup proceeds exactly as before (Working, clone, claude). No "unblocked" comment is posted; the normal Working transition and eventual PR are the signal. If the link read itself fails, the gate does not strand the issue: it falls back to normal pickup (the agent self-parks if it truly cannot proceed), since the gate is an optimization over that correct baseline.

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

The probe is skipped entirely when the action sets no forge `token_ref`, and skipped per host when the action carries no token for that host's forge: `fj` / `gh` then read the operator's own credential store, so the probe would test the wrong identity. A permission change *after* gather still fails at `pr create`, so that error is also restated with the repo, the host, and the possible-permission cause instead of the bare CLI text. A permission change that costs only the open-PR lookup is handled by the degradation below.

##### Open-PR lookup degradation (CLAUDE-206)

The CLAUDE-170 idempotency reconcile asks the forge for an already-open PR on the branch before creating one (`fj pr search`, an issue-category call). That lookup is an optimization: `fj pr create` (repository-category) is what delivers the work, and the two can fail independently, so a failed lookup must not discard a paid Claude run.

On the three create-capable arms (remote branch absent, remote equal to local, remote fast-forwardable), a failed lookup is treated as "unknown, assume none": the runner logs a `warn`, records a `warn` run event naming the CLI error and the repo (visible on the action page under the issue key), and proceeds to `pr create`. The adopt path is unchanged when the lookup succeeds.

That degraded path can race a PR that already exists. Forgejo answers such a create with 409 and the message `pull request already exists for these targets [...]`; on that specific failure the runner retries the lookup **exactly once** and adopts the PR when the retry succeeds. If it still cannot produce one, the error names both causes (the lookup failure and the create failure) rather than reporting a bare "PR creation failed".

The diverged arm keeps the lookup fatal: there the lookup result is the only alternative to a force-push, so an assumed "none" would raise the refusal for the wrong reason. Its error distinguishes a failed search ("the open-PR lookup failed ..., so whether an open PR exists is UNKNOWN") from a genuine "no open PR exists for it". No `--force` is issued on any arm.

#### Credential injection (CLAUDE-26)

`token_ref` names an entry in the encrypted secrets store (managed under `/secrets` in the web UI, encrypted at rest
with the `age` key file named by `CLAUDE_RUN_SECRETS_KEY_FILE`). A raw token is **never** written to the action YAML,
exported, or logged. When a block sets `token_ref`, the runner decrypts the secret at run time and materializes per-job,
mode-`0600` credential files (a single-file `yt` config and a temp forgejo `keys.json`), injecting them into the `yt` /
`fj` children for that job only and scrubbing them when the job finishes. The GitHub PAT (`github.token_ref`) is
injected as the `gh` child's `GH_TOKEN`, the MCP bearer, and the authenticated push URL for that job only; being an
env-var / URL component it leaves no on-disk plaintext to scrub and is dropped with the job. The gather step and the forge preflight materialize their own short-lived stores; every such directory carries a process-wide unique suffix, so two overlapping polls of one action never share a path and one poll finishing can never scrub another's live credential files (CLAUDE-209). When no block sets
`token_ref`, the CLIs fall back to host-configured credentials (`gh` host auth / `GH_TOKEN` for GitHub) exactly as the
scripted action did.

#### `kind: forgejo-ci-fix`

A single flat config record (no sub-records). The handler scans `repos` on `host` for open PRs whose head-commit
combined CI state is `failure` and that were updated within `window_minutes`, keeps ONLY PRs authored by `bot_author`
(the bot's own PRs), clones the PR branch, runs Claude to fix the failing CI, applies a diff-size decision gate, commits
as the bot identity with `skip_marker` and pushes on `push`, and posts a PR comment on every outcome path. The dedup /
attempt key is the PR head SHA (a per-commit attempt cap). The generic blocks (`workdir_base`, `claude`, `limits`,
`schedule`, `output`) stay at the top level of the action, not inside `native`.

**CLAUDE-149: only the bot's own PRs, iterate then flag Needs Review.** The author filter is inverted from the old
anti-feedback-loop skip: a human's failing PR is left to its author (the bot never pushes onto someone else's branch),
and only PRs authored by `bot_author` (the PRs the bot opened working a queued issue) are worked. The bot keeps pushing
corrective commits to its own PR until CI passes; because the per-commit cap resets on every new head SHA, a separate
per-PR counter bounds the iteration. After `max_pr_attempts` distinct fix pushes on one PR the handler stops and flags it
Needs Review: it posts a PR comment, adds the `needs_review_label` Forgejo label (`fj pr edit <repo>#<n> labels --add`),
and, when the PR body references a `#<KEY>-N` issue, best-effort sets that source issue's State to `needs_review_state`
and assigns `review_assignee` (when set). `enabled` defaults to `true` and is a master kill switch. The complementary
webhook-side change suppresses the mint's queued auto-fix route for any failure that already has an open PR (a
`human_action` failure on an open PR still mints).

```yaml
kind: forgejo-ci-fix
native:
  enabled: true                       # CLAUDE-149 master kill switch (default on)
  host: dev.a8n.run                   # forge host passed to `fj --host`
  repos: # allow-list of owner/name repos to scan
    - contoso/claude-fix-sandbox
  window_minutes: 45                  # only PRs updated this recently
  max_files: 10                       # decision gate: too_big over this many files
  max_lines: 300                      # decision gate: too_big over this many lines
  bot_author: claude-run              # ONLY this author's PRs are fixed + git commit author name
  bot_email: claude-run@a8n.run       # git commit author email
  skip_marker: "[skip claude]"        # appended to the fix commit message
  max_pr_attempts: 3                  # per-PR fix-push ceiling, then Needs Review
  needs_review_label: needs-review    # label added at the ceiling
  needs_review_state: "Needs Review"  # source-issue State set at the ceiling (best-effort)
  review_assignee: nate               # optional source-issue assignee at the ceiling
  token: <token>                      # optional; inline plaintext (CLAUDE-26 encrypts)
```

| Key                  | Default              | Meaning                                                              |
|----------------------|----------------------|----------------------------------------------------------------------|
| `enabled`            | `true`               | CLAUDE-149 master kill switch. While false the handler no-ops.       |
| `host`               | `dev.a8n.run`        | Forgejo host (bare, no scheme) passed to `fj --host`.                |
| `repos`              | `[]`                 | Allow-list of `owner/name` repos to scan. Empty means scan none.     |
| `window_minutes`     | `45`                 | Only consider PRs updated within this many minutes of the run.       |
| `max_files`          | `10`                 | Decision gate: a diff touching more files is `too_big`.              |
| `max_lines`          | `300`                | Decision gate: a diff changing more lines is `too_big`.              |
| `bot_author`         | `claude-run`         | ONLY this author's PRs are auto-fixed (CLAUDE-149) + git commit author. |
| `bot_email`          | `claude-run@a8n.run` | Git commit author email.                                             |
| `skip_marker`        | `[skip claude]`      | Appended to the fix commit so the bot's push does not re-trigger.    |
| `max_pr_attempts`    | `3`                  | Per-PR fix-push ceiling before the PR is flagged Needs Review.       |
| `needs_review_label` | `needs-review`       | Forgejo label added to the PR at the ceiling.                        |
| `needs_review_state` | `Needs Review`       | YouTrack State set on the PR's source issue at the ceiling (best-effort). |
| `review_assignee`    | none                 | Optional YouTrack login assigned to the source issue at the ceiling. |
| `token`              | none                 | API token, inline plaintext for now (CLAUDE-26 encrypts).            |

All Forgejo reads (open PRs, head-commit CI status, the PR body for the source-issue link) and the comment / label writes
go through the `fj` CLI (no raw REST); clone / push auth comes from `fj`'s own credential store. **Deployment
prerequisite:** the runner's `fj` must be a build that exposes `--json` on `pr search` (FJ-1), `pr status` (FJ-2), and
`pr view`, plus `pr edit <PR> labels --add`. An older `fj` without these cannot drive the gather reads or the Needs
Review flag.

#### `kind: ci-failure-analysis`

A single flat config record (no sub-records). The handler scans the tracker for open, unresolved issues whose summary
starts with one of `summary_prefixes` and that are not yet marked analyzed, then for each: parses the run coordinates from
the issue's `## Failure` block, fetches the complete redacted run log through `fj actions logs <run_number>` (the
per-repo run number the block already carries; PAT from `log_token_ref`) plus the failing workflow YAML into the workdir,
runs one agentic `claude` session
(tools `Read,Grep,Bash`) over both files, and posts a deep root-cause comment before marking the issue analyzed (a hidden
comment marker) so it is never analyzed twice. The dedup / attempt key is the issue id. The generic blocks
(`workdir_base`, `claude`, `limits`, `schedule`, `output`) stay at the top level of the action, not inside `native`.

**Selection is by summary prefix + analyzed-marker ONLY; it NEVER selects by `AI Agent: Queued`.** That field belongs to
the code-fix runner in `actions/youtrack.yml`; analysis is read-only enrichment and must not collide with it, so `query`
must not filter on `AI Agent`. A log miss (`AuthDenied` / `TransientMiss` / `Failed`) posts a short "unavailable" note
once and leaves the issue un-marked so a later run retries; it never crashes or marks analyzed on a miss. Redaction is
preserved end to end via `forge_log::redact`. Log reads shell out to `fj actions logs` and the workflow-YAML read to
`fj repo file` (CLAUDE-205). Both go through `forge_log` (Rust), never a raw REST call and never one from a nu snippet
(tooling-gap rule).

```yaml
kind: ci-failure-analysis
native:
  enabled: true                       # master toggle (default true)
  query: "#Unresolved"                # candidate query; MUST NOT filter by AI Agent
  summary_prefixes: # summary prefixes an issue must start with
    - "CI failure:"
    - "Auto-fix:"
  log_token_ref:                      # per-repo Forgejo PAT; omit to inherit config.webhook.sources
    - repos: ["contoso/*"]          # exact owner/repo, owner/*, or *; first match wins
      token_ref: forgejo-logs-contoso
    - repos: ["initech/*", "globex/*"]
      token_ref: forgejo-logs-initech
  max_analysis_bytes: 24576           # cap on the posted analysis comment
```

| Key                  | Default                        | Meaning                                                              |
|----------------------|--------------------------------|----------------------------------------------------------------------|
| `enabled`            | `true`                         | Master toggle. While false the handler no-ops.                       |
| `query`              | `#Unresolved`                  | YouTrack candidate query. MUST NOT filter by `AI Agent` (see above). |
| `summary_prefixes`   | `["CI failure:", "Auto-fix:"]` | Summary prefixes an issue must start with to be analyzed.            |
| `log_token_ref`      | the webhook sources            | Forgejo PAT with the **`read:repository`** scope, used to fetch the log and read the workflow YAML. Either one encrypted-secret name (CLAUDE-26) for every repo or a per-repo mapping (CLAUDE-200, see **Per-repo log tokens**). Reaches `fj` through a temp single-host `keys.json` carrying that one secret and nothing else (CLAUDE-204/CLAUDE-205), never a process argument. Read-only and repository-category only: every endpoint `fj` touches on its behalf (the three `fj actions logs` calls `/repos/{o}/{r}/actions/runs`, `.../runs/{id}/jobs`, `.../jobs/{id}/logs`, plus `/repos/{o}/{r}/raw/{path}` behind `fj repo file`) is in Forgejo's repository group behind `reqRepoReader(unit.TypeActions)`, so unlike `forgejo.token_ref` this one needs no issue scope (CLAUDE-196). Omitted, the mapping is derived from `config.webhook.sources`. |
| `max_analysis_bytes` | `24576`                        | Cap (bytes) on the posted analysis comment.                          |

**Deployment prerequisite:** a host `fj` on `PATH` carrying both `fj actions logs` and `fj repo file` (FJ-67), and a
Forgejo exposing the Actions read endpoints it calls. `fj repo file` landed on `pandoras-box/forgejo-cli` `main` after
the `v0.13.0` tag (merge `b08d8bc`), so no released version ships it yet: build from that commit or newer. Where they
are absent the fetch reports a miss and the handler posts an "unavailable" note rather than crashing.

##### Per-repo log tokens (CLAUDE-200)

Both CI-failure actions select issues across a global `#Unresolved` query, so their candidate set spans every org and no
single PAT can serve it. `log_token_ref` therefore takes either form:

```yaml
log_token_ref: forgejo-logs-contoso   # one secret for every repo
```

```yaml
log_token_ref:                          # per-repo; the first matching entry wins
  - repos: ["contoso/*"]              # exact owner/repo, per-owner owner/*, or the global *
    token_ref: forgejo-logs-contoso
  - repos: ["initech/*", "globex/*"]
    token_ref: forgejo-logs-initech
```

The scalar form is exactly a single `*` entry, so a pre-CLAUDE-200 config keeps working. Patterns use the same three
literal forms as the webhook allow-list (`crate::config::repo_matches`); ordering, not specificity, decides, so put the
exact entries first.

**Omitting the key** derives the mapping from `config.webhook.sources`: each source contributes its `allowed_repos` and
its own `log_token_ref` (a source carrying no log token contributes nothing), in source-name order. That keeps one place
to rotate a token and lets ONE deployed copy of each action cover every org.

Both shapes are editable in each CI-failure kind's structured web form (CLAUDE-211): a source selector picks derive
(writes no key), one secret for every repo, or repeatable `{repos, token_ref}` rows.

A repo that matches no entry is **skipped with a warning**: `ci-failure-analysis` leaves the issue untouched and posts no
"log unavailable" note (that note is reserved for a real log miss on a repo the deployment does hold a token for), and
`ci-failure-reconcile` skips the repo before any Forgejo read rather than spending a request that would 401.

#### `kind: ci-failure-reconcile`

A single flat config record (no sub-records). The mint path only ever observes CI failures, so a stale open "CI failure"
/ "Auto-fix" issue is closed by hand today; this handler closes it automatically once its failing job is green again. On
each run it scans the tracker for open issues whose summary starts with one of `summary_prefixes`, inspects each and reads
the hidden `Branch:` / `Job:` markers the CLAUDE-190 rekey embeds (plus the `## Failure` block's `- Repo:` / `- Workflow:`
/ `- Run:`); a pre-rekey issue whose branch/job are not parseable is skipped and left for manual close. Then for each
allow-listed repo it pages the Forgejo runs list, reduces it to the LATEST run per `(branch, workflow_id)` (resolving a
a branch the runs list does not carry as a plain name from the run's log environment dump, bounded by
`max_branch_resolves`), and reads each latest run's
per-job status from the jobs endpoint. Every open issue whose `(repo, branch, workflow, job)` exactly matches a now-green
job (job status `success`) is closed: State set to `resolved_state` and a run-linked comment posted. The dedup key is the
issue id.

**Duplicate collapse (CLAUDE-198).** Mint's cross-run collapse is a tracker full-text search on the hidden
`Root-Cause-Key:` marker, so a lagging tracker index lets two failures of the same job mint two issues. Before any
Forgejo read, the handler groups the gathered issues by `(repo, root_cause_key)` and closes every issue of a group but
the OLDEST (lowest issue number, the one mint's association comments already append to): State set to `resolved_state`
and the `duplicate_comment` posted, carrying the same hidden auto-closed marker. Only the survivor goes on to the
now-green check. Grouping is on the repo as well as the key, so a marker collision across repos closes nothing, and an
issue with no `Root-Cause-Key:` marker (pre-rekey) is never grouped and never closed as a duplicate. Set
`close_duplicates: false` to leave duplicates open.

**Only the LATEST run decides.** A newer failure after an older green run leaves the job red and the issue open. Matching
is exact on all four of `(repo, branch, workflow, job)`, so a green sibling branch or job never closes a still-broken
issue. The close is idempotent: an already-closed issue (its resolved State drops it from `#Unresolved`, and a hidden
auto-closed comment marker) is never re-closed or re-commented. **Selection is by summary prefix + markers ONLY; it NEVER
selects by `AI Agent`** (reconcile is independent of the code-fix runner), so `query` must not filter on `AI Agent`. The
reconciler performs no git/PR actions and does not depend on the CLAUDE-189 analysis having run. Log and per-job status
reads shell out to `fj actions logs` and the runs list to `fj actions runs` (CLAUDE-205). Both go through `forge_log`
(Rust), never a raw REST call and never one from a nu snippet (tooling-gap rule).

```yaml
kind: ci-failure-reconcile
native:
  enabled: true                       # master toggle (default true)
  query: "#Unresolved"                # candidate query; MUST NOT filter by AI Agent
  summary_prefixes: # summary prefixes an issue must start with
    - "CI failure:"
    - "Auto-fix:"
  repos: # allow-list: exact owner/repo, owner/*, or *
    - contoso/claude-run
  log_token_ref:                      # per-repo Forgejo PAT; omit to inherit config.webhook.sources
    - repos: ["contoso/*"]          # exact owner/repo, owner/*, or *; first match wins
      token_ref: forgejo-logs-contoso
    - repos: ["initech/*", "globex/*"]
      token_ref: forgejo-logs-initech
  resolved_state: Done                # State set on a closed issue
  close_comment: "Auto-closed: run #{run_index} on `{branch}` succeeded ({run_url}); the failing `{job}` is green, root cause resolved."
  max_branch_resolves: 40             # cap on per-repo branch-from-log resolutions
  close_duplicates: true              # collapse issues sharing a Root-Cause-Key onto the oldest
  duplicate_comment: "Auto-closed as a duplicate of {canonical}: both issues track the same failing job (root-cause key `{root_cause_key}`). Follow {canonical} for the fix."
```

| Key                   | Default                        | Meaning                                                                                 |
|-----------------------|--------------------------------|-----------------------------------------------------------------------------------------|
| `enabled`             | `true`                         | Master toggle. While false the handler no-ops.                                          |
| `query`               | `#Unresolved`                  | YouTrack candidate query. MUST NOT filter by `AI Agent`.                                 |
| `summary_prefixes`    | `["CI failure:", "Auto-fix:"]` | Summary prefixes an issue must start with to be reconciled.                              |
| `repos`               | `[]`                           | Allow-list of `owner/repo` full names to scan: exact `owner/repo`, `owner/*`, or `*`. Empty scans nothing. |
| `log_token_ref`       | the webhook sources            | Forgejo PAT with the **`read:repository`** scope, used to page the runs list and to read each latest run's per-job status and log (which also resolves a null-branch run's branch). Either one encrypted-secret name (CLAUDE-26) for every repo or a per-repo mapping (CLAUDE-200, see **Per-repo log tokens**). Reaches `fj` through a temp single-host `keys.json` (CLAUDE-204). Repository-category only, same as the `ci-failure-analysis` log token (CLAUDE-196). Omitted, the mapping is derived from `config.webhook.sources`. |
| `resolved_state`      | `Done`                         | State set on a closed issue (the CLAUDE project's only resolved State).                  |
| `close_comment`       | (the auto-closed sentence)     | Run-linked close-comment template; `{run_index}` / `{branch}` / `{run_url}` / `{job}` are substituted, and a hidden auto-closed marker is appended. |
| `max_branch_resolves` | `40`                           | Cap on per-repo branch-from-log resolutions for runs whose branch the runs list does not carry as a plain name (`fj`'s `head_branch` is Forgejo's `prettyref`: `#<pr-number>` on a PR run, the bare sha on a ref-deleted push). Reaching it is logged, never silent. |
| `close_duplicates`    | `true`                         | Collapse open issues sharing a `(repo, Root-Cause-Key:)` onto the oldest and close the rest as duplicates, before any Forgejo read (CLAUDE-198). |
| `duplicate_comment`   | (the duplicate sentence)       | Duplicate close-comment template; `{canonical}` (the surviving issue id) and `{root_cause_key}` are substituted, and the same hidden auto-closed marker is appended. |

`repos` and `resolved_state` are the distinctive keys that disambiguate this variant from the other native configs under
the untagged `native:` block, so a shipped action must set at least one of them (the shipped `ci-failure-reconcile.yml`
sets both). The generic blocks (`schedule`, `output`, `limits`) stay at the top level of the action.

**Deployment prerequisite:** a host `fj` on `PATH` carrying both `fj actions logs` and `fj actions runs` (FJ-66), and a
Forgejo exposing the Actions read endpoints they call. `fj actions runs` landed on `pandoras-box/forgejo-cli` `main`
after the `v0.13.0` tag (merge `b08d8bc`), so no released version ships it yet: build from that commit or newer. On a
deployment lacking either, a repo scan is skipped with a warning rather than crashing.

### `workdir_base` (string, optional)

Per-job working directories are rooted under `<workdir_base>/<name>/<key>`. Defaults to `$CLAUDE_RUN_WORKDIR_BASE`
(which itself defaults to `/tmp/claude-run`). Setting this on the action overrides the env for that action only - useful
when one action needs more disk than `/tmp` allows (e.g., a workspace mounted on a large data volume) or when you want
per-action isolation on disk for cleanup or quota reasons.

**Resolution rules** (apply to both the YAML value and the env value):

- If the value starts with `$`, it is evaluated as a nu expression and the result is used. Lets you do things like:

  ```yaml
  workdir_base: $nu.home-dir | path join workspace
  workdir_base: $"($env.HOME)/workspace"
  workdir_base: $"($env.XDG_DATA_HOME)/claude-run"
  ```

- Otherwise, the value is treated as a literal path. `path expand` is applied in either case, so `~` and `$env`
  references that `path expand` understands still work for literal values.

  ```yaml
  workdir_base: ~/workspace                          # literal, ~ expanded
  workdir_base: /mnt/data/claude-run                 # literal, absolute
  workdir_base: $nu.home-dir | path join workspace   # evaluated
  ```

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

The runner always passes `--print --verbose --output-format stream-json`. It wraps the call in `timeout
$claude_timeout_seconds`. Stdout+stderr are redirected to a per-job NDJSON stream log:

```
$CLAUDE_RUN_LOG_DIR/<name>/streams/<key>-attempt-<n>.jsonl
```

`tail -f` that file from another terminal to watch Claude work in real time. The final `result` event on the last line
is parsed for cost (`total_cost_usd`) and outcome (`result`, `is_error`) once claude exits.

### `post` (list of nu snippets, optional)

Ordered steps that run after every code path that reached the workdir: Claude success, Claude non-transient failure,
prompt-snippet failure, empty-prompt, and pre-step failure all run post. The single exception is transient Claude errors
(rate limit, auth) - those skip post and return early so the job can be retried cleanly on the next firing.

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
the job (ensuring the skip-attempt marker is present), posts the file's contents verbatim as the issue comment, and sets
the status field to the project's "needs human input" value, creating no PR. This keeps every external mutation in one
post step gated on the run outcome, rather than having the agent mutate the external system mid-run. The marker is a
plain convention between an action's prompt and its post step; the runner itself only special-cases
`.claude-run-skip-attempt`.

### `output.file` (string, optional)

If set, the runner appends one JSON line per job to `$CLAUDE_RUN_LOG_DIR/<name>/<output.file>` summarizing the outcome
(job key, attempt number, claude cost, push/comment results from post steps, etc.).

If omitted, per-job outcomes are only visible in the daily JSONL log.

### `limits` (record, optional)

Per-action overrides for the runner-wide defaults. Any subset of:

| Key                      | Env default                              |
|--------------------------|------------------------------------------|
| `max_attempts_per_key`   | `$env.CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY`   |
| `max_runs_per_day`       | `$env.CLAUDE_RUN_MAX_RUNS_PER_DAY`       |
| `claude_timeout_seconds` | `$env.CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS` |
| `daily_budget_usd`       | `$env.CLAUDE_RUN_DAILY_BUDGET_USD`       |

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

| Variable                            | Default                      | Purpose                                                                                              |
|-------------------------------------|------------------------------|------------------------------------------------------------------------------------------------------|
| `CLAUDE_RUN_STATE_DIR`              | `~/.local/share/claude-run`  | State, locks, PAUSED flags, cost ledger.                                                             |
| `CLAUDE_RUN_LOG_DIR`                | `$CLAUDE_RUN_STATE_DIR/logs` | Daily JSONL logs and per-action output files.                                                        |
| `CLAUDE_RUN_ACTIONS_DIR`            | `./actions`                  | Resolves bare action names to YAML files.                                                            |
| `CLAUDE_RUN_WORKDIR_BASE`           | `/tmp/claude-run`            | Per-job workdir root. Final path = `<base>/<name>/<key>`. Overridable per action via `workdir_base`. |
| `CLAUDE_RUN_MAX_RUNS_PER_DAY`       | `20`                         | Hard cap on Claude invocations per action per day.                                                   |
| `CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY`   | `3`                          | Per-job attempt cap when `dedup_key` is set.                                                         |
| `CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS` | `600`                        | `timeout` wrapper around the `claude` CLI call.                                                      |
| `CLAUDE_RUN_DAILY_BUDGET_USD`       | `5.00`                       | Skip run when today's cost ledger meets/exceeds this.                                                |

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
  "daily_count": {
    "2026-05-23": 7
  },
  "daily_cost_usd": {
    "2026-05-23": 1.23
  }
}
```

## Synthetic webhook action (`webhook-forgejo`)

The inbound Forgejo webhook (`POST /webhooks/forgejo/<source>`) surfaces on the dashboard as a synthetic action named `webhook-forgejo` (renamed from `webhook-fast-path` in CLAUDE-153). It is not a YAML file under `actions/`: it is Rust-defined, injected by the web layer only when at least one webhook source is configured, and reserved (an on-disk YAML claiming the name `webhook-forgejo` is refused at scan). Its per-key attempt counter and daily run ledger are recorded under this name like any other action, so its run history, durations chart, and last-run recency render on the dashboard. A one-time startup migration (SQL migration `0013` plus a log-directory rename) carries any pre-rename history across.

- **Read-only:** edit, export, and delete are rejected (HTTP 403); there is no backing YAML to operate on, and a manual trigger is rejected (HTTP 400) because it is event-driven, not scheduled.
- **Pausable (CLAUDE-153):** it participates in the same per-action Pause as every other action. Pause/Resume controls and a Paused badge appear on the dashboard, and `POST /api/actions/webhook-forgejo/pause` / `/resume` write and clear its `paused_actions` row. While paused, an inbound delivery is still authenticated (signature, replay, and rate-limit checks run) and acked with a 2xx, then dropped without minting or queuing, so Forgejo does not treat it as a failed delivery and retry.
- **Auto-fix toggle (CLAUDE-153):** the `Webhook` settings section carries an `auto_fix` boolean, default `false` (opt-in). When off, a failure the webhook triages into a fixup bucket (`code_fix` / `auto_fix`) mints nothing and queues no corrective fixup PR: the failure stays reported on the original PR for its author. A `human_action` failure still mints. When on, the corrective-fixup behavior applies (subject to the CLAUDE-149 open-PR suppression). This toggle is webhook-scoped and independent of the framework-wide `[mint].auto_fix.enabled` detector switch. It is DB-backed (`config.webhook`) and edited live on the Settings page or via `PUT /api/settings/webhook`.
- **Dedupe (root-cause keying, CLAUDE-190):** the cross-run collapse key is `hash(repo, branch, workflow_id, job_name, failure_signature)`, not the commit SHA, so N commits that each fail the same job on the same branch with the same failure collapse onto ONE open issue (the second and later runs append a run-association comment instead of minting a sibling), while a different failure signature on the same job stays a separate issue. The `failure_signature` is a deterministic, pure-string signature of the first diagnostic line in the redacted log (lowercased, path prefixes dropped, digit runs masked to `#`, whitespace collapsed): no LLM, no I/O. When no signature is derivable (truncated / diagnostic-free / logless failure) the key falls back to `hash(repo, branch, workflow_id, job_name)`, which still collapses a "branch is broken" storm onto one issue (a logless failure cannot distinguish two root causes on the same job, so it intentionally groups them; the CLAUDE-189 analysis can later split a mis-grouped issue). `branch` is resolved from the run's log environment dump (`GITHUB_HEAD_REF` on a PR run, else the `refs/heads/` short name, in either the `setupEnv => map[...]` or the `KEY=value` form; run detail returns `head_branch: null` on the deployed Forgejo), `job_name` from the jobs `fj actions logs` returns with the log, both in Rust (never a raw REST call from a nu snippet). The issue body carries hidden `Dedupe-Key:`, `Root-Cause-Key:`, `Branch:`, and `Job:` markers; a per-run key is retained only for webhook-redelivery idempotency, and a resolved issue is never reopened.

## Deleting an action

An action can be deleted from all three surfaces: the CLI (`claude-run action delete <name>`), the web UI (the Delete button on an action's detail page), and the JSON API (`POST /actions/<name>/delete`).

Delete is a config-and-control purge, not a history wipe. It removes the on-disk YAML file, the `actions` index row, the `paused_actions` row, and the `attempt_counters` / `daily_ledger` rows for that action. It KEEPS the action's run history: the `runs` and `job_outcomes` rows and the on-disk stream logs under `logs/<name>/` survive. Re-creating an action with the same name re-associates that retained history, which is the intended audit behavior.

Delete is refused while a run for the action is in flight, so removing a config can never orphan a running job. The CLI returns exit code 7 (conflict) and the API / web UI return HTTP 409; cancel the run (`claude-run cancel <run-id>`) or wait for it to finish, then retry. An unknown action returns exit code 3 from the CLI and HTTP 404 from the API / web UI.

The index is self-healing: `scan_actions` prunes any `actions` row whose backing YAML no longer exists on disk, so an out-of-band `rm` of an action file is reconciled on the next scan rather than leaving a stale dashboard row. Pruning removes only the index row; retained run history is untouched.

## Exit codes

| Code | Meaning                                                                   |
|------|---------------------------------------------------------------------------|
| 0    | Run completed (with or without finding work).                             |
| 1    | Paused (global `PAUSED` or per-action `<name>.PAUSED` present).           |
| 2    | Lock contention (previous run still going).                               |
| 3    | Config error (action YAML missing, malformed, or missing required field). |
| 4    | Daily cap reached (`max_runs_per_day`).                                   |
| 5    | Unexpected error (see logs).                                              |
| 6    | Over daily budget (`daily_budget_usd`).                                   |
| 7    | Conflict: a mutating command was refused (e.g. `action delete` while a run for that action is in flight). |
| 130  | Run cancelled in flight (operator cancel via the API / web UI / CLI).     |

Exit code 130 is recorded on the `runs` row, not returned by the `run` subcommand; the web UI and CLI render it as a
distinct "canceled" badge.

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
field if set, else `$CLAUDE_RUN_WORKDIR_BASE`, else `/tmp/claude-run`. The runner creates it before the first `pre` step
and **removes it at end of job regardless of outcome** (success, claude failure, pre/post step failure, transient
error). Plain `rm -r` is used, not `rm -rf`, so permission or read-only issues surface as real errors instead of being
silently force-deleted.

To inspect what happened in a failed run, look at the per-attempt stream log
(`$CLAUDE_RUN_LOG_DIR/<name>/streams/<key>-attempt-<n>.jsonl`); it survives across runs since it lives outside
`$workdir`.

For `filter` and `pre`/`post`, exit code 0 = continue, non-zero = step failure (skip job, count attempt). The snippet's
stdout is captured into the daily log; stderr passes through.
