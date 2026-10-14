# Comment commands

## Comment commands on a pull request or an issue (CLAUDE-220, CLAUDE-221)

A second inbound listener turns a `/claude <instruction>` comment on a Forgejo thread into work. The endpoint is
`POST /webhooks/forgejo/comment/<source>`, served alongside (and independently of) the CI-failure webhook above. It has
two routes, chosen by whether the comment's thread is a pull request:

- **Pull request (CLAUDE-220):** a bounded Claude run against that PR's head branch, pushed back to the branch.
- **Plain issue (CLAUDE-221):** an issue has no head branch to clone or push, so nothing runs here. The command mints a
  YouTrack issue carrying the instruction and a link back to the Forgejo issue, queued for the agent.

The second route is a ROUTE, not a second listener: the request pipeline, authorization gate, trigger parsing,
bot-login loop drop, dedupe key, and caps ledger are the same implementations both paths run through.

It is a SEPARATE listener, section, and synthetic action rather than a second event type on `webhook-triage`,
because the tokens differ in power: the comment listener's `write_token_ref` pushes branches (`write:repository`
plus `write:issue`), while a CI-failure source holds a read-only `log_token_ref` and, since CLAUDE-276, a
`write_token_ref` that only comments on pull requests (`write:issue`). Separate sections keep the push-capable token
out of the CI-failure source block, and each listener gets its own Pause switch. What the two DO share, by reference rather than by copy: HMAC
verification, the single replay-store instance (so one `X-Forgejo-Delivery` id fanned out to both endpoints is still
caught), the per-source rate-limiter type and builder, the `owner/repo` allow-list matcher, and the accept-and-drop
pause gate (CLAUDE-223), which checks the GLOBAL pause before each listener's own synthetic action.

**Request pipeline**, in the same order as the CI-failure endpoint: disabled listener `404` (CLAUDE-360:
`config.webhook_comment.enabled = false`, ahead of the source lookup) -> unknown source `404` -> per-source rate limit
`429` -> missing / unresolvable `secret_ref` `401` -> bad or missing signature `401` -> missing `X-Forgejo-Delivery`
`400` -> replayed delivery id `409` -> Pause check, global then per-action, accept-and-drop with `2xx` (CLAUDE-223) ->
parse. Every post-authentication drop is acked
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

**Accounting and concurrency.** Runs are recorded under the synthetic action `webhook-comment` with the same
per-key attempt counter and daily run ledger every other action uses, so caps, the durations chart, and run history all
work. The attempt key is `hash(repo, number, comment_id)` on both routes (a Forgejo comment id is unique across a
repo's issues and pull requests), and because a comment is a one-shot request its effective per-key cap is one attempt:
a redelivery of the same comment never runs the work twice, and never mints a second YouTrack issue. The daily run
ledger advances for a mint as well as for a Claude run, so a flood of plain-issue commands is bounded by the same cap.
At most one comment job per thread is in flight, through the daemon-wide key gate; the gate key carries the thread kind,
so a plain issue can never block the like-numbered pull request. A second command while one is running gets a reply
saying so and is dropped.

**Configuration** is the `Comment webhook` section on the Settings page (`GET` / `PUT
/api/settings/webhook_comment`, DB key `config.webhook_comment`). Top level: `trigger`, `max_instruction_chars`,
`max_runs_per_day` (the daily run cap of this listener's ledger, default 20, at least 1, live), `claude_timeout_seconds`
(the per-run `claude` session timeout for the PR-comment route, default 600, at least 1, read live per command with
no restart), and `sources`. Per source: `secret_ref` (HMAC; unset means the endpoint `401`s everything), `allowed_repos` (the same three
pattern forms as the CI-failure allow-list; empty denies all), `rate_limit_per_min`, `write_token_ref`,
`allowed_permissions`, the `max_files` / `max_lines` decision-gate caps, and the commit identity (`bot_author`,
`bot_email`, `skip_marker`). The plain-issue route adds no settings of its own: it reads the existing `Mint` section
(`config.mint`) for project routing, target state, and the `AI Agent` field / queued value. Source and rate-limiter
changes apply live, with no daemon restart. The `write_token_ref`
secret reaches `fj` exactly the way `log_token_ref` does: a temp single-host `keys.json` carrying that one secret and
nothing else, removed when the command finishes. The token is never in YAML, never an env var in the daemon's own
environment, never a process argument, and never rendered in a settings response.

**Forgejo setup.** On the repo or org's Settings -> Webhooks, add a **Forgejo** webhook with URL
`https://<daemon-host>/webhooks/forgejo/comment/<source>`, the Secret set to that source's stored `secret_ref` value,
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

**GitHub setup.** Set `provider: github` on the source and use
`https://<daemon-host>/webhooks/github/comment/<source>` as the Payload URL, Content type `application/json` or
`application/x-www-form-urlencoded` (GitHub's default; both are accepted), Secret set to the source's stored
`secret_ref` value. GitHub has no separate pull-request-comment event: select the
single **Issue comments** event (`issue_comment`), which fires for a comment on either a plain issue or a pull
request, and the listener tells them apart the same way GitHub's own payload does, by whether `issue.pull_request`
is present. Store the write PAT first, e.g. `printf %s '<pat>' | claude-run secret set github-write-<org>`, and name
it in the source's `write_token_ref`; it needs `contents:write` (to push the branch), `pull_requests:write`, and
`issues:write` scopes. GitHub automerge does not use this secret: it uses the GitHub login saved in Settings -> Auth
(`auth-github-token`), which must be a classic PAT because a fine-grained PAT cannot read check runs. To serve both with
one classic PAT, grant `repo`, `read:org` (needed by `gh auth login --with-token`), `read:user`, and `workflow` only when
the agent edits `.github/workflows/*`, then save it in both places: Settings -> Auth and the secrets store under the
`write_token_ref` name. The Settings token and `write_token_ref` stay separate secrets. Authorization is a live read of the commenter's permission via `GithubForge::repo_permission`
(`gh api repos/<owner>/<repo>/collaborators/<username>/permission --jq .permission`), matched against the source's
`allowed_permissions` the same way as the Forgejo route; the token used for that read needs read access to the
repository's collaborators, which a token with `contents:write` on a repo the bot administers already has. An
org-level webhook may subscribe to more events than Issue comments: the listener acknowledges and ignores the
`ping` GitHub sends on webhook creation or "Redeliver", and any other delivered event that is not `issue_comment`,
with `200` and an `info` log rather than failing the delivery.
