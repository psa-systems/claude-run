# Install

## Install

Pull the latest binary from the Forgejo Generic Packages registry. It is published under the private
`psa-systems-private` package owner, so the request needs a Forgejo PAT that can read it:

```nu
let token = "<your Forgejo PAT>"
http get --headers [Authorization $"token ($token)"] https://dev.a8n.run/api/packages/psa-systems-private/generic/claude-run/latest/claude-run-linux-x86_64 | save claude-run
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
with the CLI error. Every read-only `fj` call the runner makes (`repo view`, `pr search`, `pr status`, `whoami`, `repo
collaborator ... permission`, and the CI-failure log/run/file reads) is retried up to four attempts total on a non-zero
exit, waiting 0s/2s/5s before attempts 2-4 (CLAUDE-366), so a transient forge blip costs at most 7 seconds rather than
failing the run outright; a deterministic failure (a 404 or a scope error) still exhausts all four attempts before its
error surfaces, since the retry never inspects the CLI's error text to decide whether to give up early. Every `fj`
write (`pr create`, `pr merge`, `pr update`, comments, label edits) is never retried, since a retried write could
double-post a comment or a PR. The read-only log-fetch token
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
