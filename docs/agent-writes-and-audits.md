# Agent tracker writes and audits

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

- `actions/youtrack.yml` works code issues. Query: `(project: -{{audit_project}} or tag: -Audit-*) and -Resolved AI Agent: Queued`.
- `actions/audit-run.yml` works read-only repository audits. Query:
  `-Resolved AI Agent: Queued project: {{audit_project}} tag: Audit-*`.

Both `{{audit_project}}` placeholders are substituted with `native.youtrack.audit_project` (default `AUDIT`) at query
time (CLAUDE-409); the two action files carry this field, not the project key spelled out inside the query string, so
moving the audit project to a different short name is one field edited in each file instead of two independently
hand-written query literals. `YoutrackSource::resolved_query` (`src/action.rs`) does the substitution.

The split exists because the handler resolves a job's repository from the triggering issue's project VCS processor, one
project to one repo, and an audit writes its deliverable into a **different** repo from the one it reads. Audit issues
live in the configured audit project, whose processor points at the audits repo (e.g. `owner/audits`), so the handler clones the audits repo into the
job workdir and finalize opens the pull request there; the agent clones the audited repo read-only to a path outside the
workdir and never writes to it. An audit issue picked up by the code-issue action would clone the audited repo and open
a pull request against it, which is why the exclusion is not optional: both actions poll on the same interval, so
without it the winner is a race. The audit side also requires an `Audit-*` tag: the YouTrack workflow tags the daily
audit parent and every per-target Epic `Audit-<Type>`, and that tag is what routes them to `audit-run.yml`. The worker
query is the exact complement, so an issue in the audit project with no `Audit-*` tag is ordinary engineering work on
the audits repo and goes to `youtrack.yml`. Note the query syntax - YouTrack rejects `-project: AUDIT` with a parse
error, so the negation sits on the value (`project: -AUDIT`), and a parenthesized group must be followed by an explicit
`and` (`#Unresolved (a or b)` is rejected). Name the project by its short name: `AUD` is a legacy alias YouTrack still
resolves after the rename, but it appears in no project listing and must not be used in a query.

`audit-run.yml` sets `prompt_preset: audit`, which selects the audit prompt built into the binary
(`src/native/audit_prompt.rs`) instead of the engineering default; it carries no YAML `prompt` override, so an image
upgrade updates the audit prompt. `claude-run action show audit-run` prints `"prompt_source": "builtin:audit"`. The
preset handles both run shapes (a per-target Epic and the daily parent) and permits the tracker writes the default
prompt forbids: one summary comment per target on the parent issue, a `## Result` block on the run's own issue
description, the standing table on the anchor issue (daily parent run only), and filing the run's findings
(CLAUDE-295). Findings are
created only in `native.youtrack.write_projects` (empty by default, which resolves to the triggering issue's own project; an operator sets the projects findings should file into), at
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
Epic `parent for` open issues in other projects. `dependencies.block_cross_project: false` makes the gate ignore
those, or an Epic retried after a transient failure would wait until every one of its findings was worked.
`concurrency.max_parallel: 1` keeps one audit in flight at a time; per-target caps and the coverage ledger, not
serialization, are what keep a rotation affordable. `automerge.enabled: true` merges an audit pull request once its CI
is green, which is what resolves its Epic and so releases the gated daily parent; the merge is normally event-driven,
so the webhook should send Action Run Success and Recover for low latency, and the reconciler backstop merges it
within one tick either way (see [Event-driven automerge](webhooks.md#event-driven-automerge-claude-126-claude-278)).

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
