//! Provider abstraction for the external services native handlers talk to.
//!
//! Native handlers need a typed, testable interface over the host CLIs (`yt`,
//! `fj`) instead of the raw nu snippets the scripted youtrack action uses. Two
//! traits capture the operations they need:
//!
//! - [`IssueTracker`]: the YouTrack operations (list queued issues, read/write
//!   a custom field, set state, comment, resolve a project's VCS repo).
//! - [`Forge`]: the Forgejo operations (open a PR from local commits, resolve an
//!   authenticated clone URL).
//!
//! A single [`Backend`] is both. [`CliBackend`] (in [`cli`]) implements it over
//! `yt` / `fj`, parsing their `--json` output and surfacing typed errors
//! ([`Error::CliNotFound`], [`Error::CliFailed`], [`Error::CliParse`]). The
//! [`resolve`] factory walks a [`BackendKind`] priority list and returns the
//! first usable backend, erroring clearly (with what it tried) when none is.
//!
//! **CLI-first.** The priority list holds only [`BackendKind::Cli`] today. A
//! future `Mcp` backend slots in as another variant without reshaping callers:
//! they depend on the traits and the factory, never on `CliBackend` directly.
//! No raw REST anywhere; the CLI backend shells out to `yt` / `fj` only.
//!
//! The trait methods are synchronous (they shell out via [`std::process`], like
//! [`crate::snippet`]). The async native handler wraps a backend
//! call in `tokio::task::spawn_blocking`.

pub mod audit;
pub mod cli;
pub mod github;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub use audit::{AuditedTracker, IssueWriteAudit, DEFAULT_MAX_ISSUES_PER_RUN};
pub use cli::{CliBackend, CmdOutput, CommandRunner, FjForgeReads, ProcessRunner};
pub use github::{is_github_host, GhForgeReads, GithubForge, HttpMcpTransport, McpTransport};

/// One project-to-repo binding flattened from a YouTrack project's enabled VCS
/// integration, the row of the inverse routing map. Each enabled
/// `GiteaChangesProcessor` on a non-archived project contributes one entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsMapEntry {
    /// Forge server URL as the processor stores it, e.g. `https://dev.a8n.run`.
    pub server_url: String,
    /// Repository path the processor watches, e.g. `contoso/claude-run`
    /// (stored lowercased by YouTrack; match case-insensitively).
    pub path: String,
    /// Short name of the project that owns the processor, e.g. `CLAUDE`.
    pub project_key: String,
}

/// A repository resolved from a YouTrack project's enabled VCS integration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRepo {
    /// Forge server URL with any trailing slash trimmed, e.g.
    /// `https://dev.a8n.run`.
    pub repo_host: String,
    /// Repository path, e.g. `contoso/claude-run`.
    pub repo_path: String,
    /// Full clone URL: `<repo_host>/<repo_path>.git`.
    pub clone_url: String,
}

/// An open pull request as `fj pr search --json` reports it. Only the fields
/// the CI-fix handler consumes are modelled; unknown fields are ignored so a
/// newer `fj` adding columns does not break parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgePullRequest {
    /// PR number.
    pub number: u64,
    #[serde(default)]
    pub title: String,
    /// Head-commit SHA; the CI-fix dedup / attempt key.
    #[serde(default)]
    pub head_sha: String,
    /// Head branch name, cloned by the handler.
    #[serde(default)]
    pub head_ref: String,
    /// Base branch name (the branch the PR merges into), used by the comment
    /// webhook's PR route to let an agent bring the base branch in to resolve
    /// merge conflicts.
    #[serde(default)]
    pub base_ref: String,
    /// Author login, matched against the bot-author skip.
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub html_url: String,
    /// Last-update timestamp (RFC 3339), used for the scan window.
    #[serde(default)]
    pub updated_at: String,
}

/// The mergeability snapshot of a PR as `fj pr status --json` reports it.
/// `state` is the combined CI status (`"success"` / `"failure"` /
/// `"pending"`); `mergeable` is Forgejo's verdict that the PR has no conflict,
/// is not still being checked, and is not a WIP PR - it says nothing about
/// whether the branch is up to date with its base (that used to be
/// the assumption here, and it is wrong - Forgejo's `mergeable` ignores being
/// behind). `behind_base` answers that separately: `Some(true)` /
/// `Some(false)` when both Forgejo's `merge_base` and `base.sha` are known,
/// `None` when either is missing or the PR is merged, meaning unknown and
/// never collapsed to `false`. Only the fields automerge consumes are
/// modelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrStatus {
    /// Combined CI status: `"success"` / `"failure"` / `"pending"`.
    pub state: String,
    /// Whether Forgejo will merge the PR as-is: no conflict, not still being
    /// checked, not WIP. Does not reflect whether the branch is behind base.
    /// `None` is unknown: `fj` reports an explicit `null` for a merged PR.
    #[serde(default)]
    pub mergeable: Option<bool>,
    /// Whether the PR's head is behind its base. `None` means unknown (an
    /// `fj` predating that field, or a value Forgejo has not computed yet).
    #[serde(default)]
    pub behind_base: Option<bool>,
    /// The PR head commit SHA. `None` for a merged PR or an `fj` predating
    /// this field; used to bound a conflict auto-resolve dispatch to one
    /// attempt per head commit (CLAUDE-380).
    #[serde(default)]
    pub head_sha: Option<String>,
}

/// A pull request opened by a [`Forge`] backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    /// PR number from the forge CLI's JSON envelope, when present.
    pub number: Option<u64>,
    /// PR web URL as the forge CLI reported it (the `html_url` field of
    /// `fj pr create --json`).
    pub url: String,
    /// The server-side head branch this PR was opened from, when the
    /// strategy produced one (the Forgejo branch strategy, and GitHub, which is
    /// always branch-backed). `None` for the AGit strategy, which pushes to
    /// `refs/for/<base>` and never creates a server branch, so a re-queued issue
    /// has nothing to resume there.
    #[serde(default)]
    pub head_branch: Option<String>,
}

/// Direction of an issue link, mirroring the YouTrack REST `IssueLink.direction`
/// and the `direction` field `yt issue inspect --json` emits (YT-40). `Both` is
/// the undirected case (e.g. `Relates`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkDirection {
    Outward,
    Inward,
    Both,
}

impl LinkDirection {
    /// Parse the `direction` string `yt` emits. Unknown values map to `Both`
    /// (the least-committal, non-directional reading); the dependency gate keys
    /// off the resolved `verb`, so an unexpected direction never mis-blocks.
    pub fn from_yt(s: &str) -> Self {
        match s {
            "OUTWARD" => LinkDirection::Outward,
            "INWARD" => LinkDirection::Inward,
            _ => LinkDirection::Both,
        }
    }
}

/// One directed link from an issue to a single linked target, flattened from the
/// `links[].issues[]` shape of `yt issue inspect --json` (YT-40).
/// Each linked issue in a `links[]` entry becomes its own row so the dependency
/// gate can filter by `verb` (the direction-resolved relationship) and test each
/// target's `resolved` timestamp independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueLink {
    /// Resolved link direction (`OUTWARD` / `INWARD` / `BOTH`).
    pub direction: LinkDirection,
    /// Direction-resolved relationship verb, e.g. `depends on`, `parent for`,
    /// `is required for`, `subtask of`, `relates to`.
    pub verb: String,
    /// The linked issue's `idReadable`, e.g. `YT-40`.
    pub target: String,
    /// The linked issue's `resolved` epoch-ms timestamp. `None` (absent or null
    /// in the JSON) means the target is unresolved, i.e. an unmet dependency.
    pub resolved: Option<i64>,
}

/// An issue's body plus its comment texts, from
/// `yt issue inspect --json`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IssueInspect {
    /// The issue's Markdown description / body.
    pub description: String,
    /// The text of each comment on the issue, in order.
    pub comments: Vec<String>,
}

/// The YouTrack-side operations native handlers need. Backed by `yt` today; an
/// MCP backend can implement the same surface later.
pub trait IssueTracker: Send + Sync {
    /// Queued issues matching `query` (e.g. `-Resolved AI Agent: Queued`),
    /// returned as raw JSON objects (the shape `yt list --json` emits).
    fn list_queued(&self, query: &str) -> Result<Vec<serde_json::Value>>;

    /// Read a single custom field's value as text, e.g. `("MK-16", "AI Agent")`.
    fn get_field(&self, key: &str, field: &str) -> Result<String>;

    /// Set a custom field, e.g. `("MK-16", "AI Agent", "Working")`.
    fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()>;

    /// Set the standard `State` field, e.g. `("MK-16", "In Progress")`.
    fn set_state(&self, key: &str, state: &str) -> Result<()>;

    /// Set the issue Assignee, e.g. `("MK-16", "octocat")`. `login` is passed
    /// straight to the backend; `me` and `unassigned` are valid values. Backs the
    /// optional assignee-on-pickup write.
    fn set_assignee(&self, key: &str, login: &str) -> Result<()>;

    /// Add a comment to an issue.
    fn comment(&self, key: &str, body: &str) -> Result<()>;

    /// The issue's directed links, flattened one row per linked target, with each
    /// link's resolved `direction` / `verb` and each target's nullable `resolved`
    /// timestamp (YT-40). Backs the pre-clone dependency gate, which
    /// blocks a queued issue on any unmet dependency before it is cloned or handed
    /// to claude.
    fn issue_links(&self, key: &str) -> Result<Vec<IssueLink>>;

    /// Read an issue's description and comment texts via
    /// `yt issue inspect --json`.
    fn inspect_issue(&self, key: &str) -> Result<IssueInspect>;

    /// Resolve a project's enabled VCS integration to a clonable repo. Errors
    /// [`Error::NoVcsIntegration`] when the project has none.
    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo>;

    /// Build the inverse VCS routing map (repo -> project): list every
    /// non-archived project and flatten each one's enabled VCS processors into
    /// `(server_url, path, project_key)` rows. A single project's `vcs` lookup
    /// failing is logged and skipped, not propagated, so one broken project never
    /// disables routing. Backs mint routing without a hand-kept repo map.
    fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>>;

    /// Create a new issue in `project` (short name, e.g. `CLAUDE`) with `summary`
    /// and a Markdown `description`, optionally pre-assigning `assignee` (a login).
    /// Returns the new issue's `idReadable` (e.g. `CLAUDE-47`). Backs the
    /// triage-driven issue mint.
    ///
    /// A run reaches this through [`AuditedTracker`], which spends
    /// one slot of the run's issue-creation ceiling per call and refuses once
    /// the ceiling is reached. Implementations do NOT count; the ledger is
    /// per-run, and a backend can outlive many runs.
    fn create_issue(
        &self,
        project: &str,
        summary: &str,
        description: &str,
        assignee: Option<&str>,
    ) -> Result<String>;

    /// Upload `file` as an attachment on issue `key`. Backs attaching the
    /// redacted failing-job log to a minted issue.
    fn attach_file(&self, key: &str, file: &std::path::Path) -> Result<()>;

    /// Link `key` to `target` with `link_type`, read source-first
    /// (`("CLAUDE-99", "subtask of", "CLAUDE-1")` makes CLAUDE-99 a subtask of
    /// CLAUDE-1). Backs hanging a filed report off its nominated parent.
    fn link_issue(&self, key: &str, link_type: &str, target: &str) -> Result<()> {
        let _ = (key, link_type, target);
        Err(Error::Other(anyhow::anyhow!(
            "link_issue not supported by this tracker backend"
        )))
    }

    /// Attach an existing tag to an issue, creating the instance-wide tag when
    /// missing (`yt issue tag add <key> <tag> --create`). Backs the
    /// `Needs-Info` tag written alongside the `Needs Review` AI Agent value
    /// when the agent parks an issue pending a human answer (CLAUDE-373). The
    /// default is a no-op `Ok(())`, for a backend with no tag support.
    fn add_tag(&self, key: &str, tag: &str) -> Result<()> {
        let _ = (key, tag);
        Ok(())
    }

    /// Verify the tracker token and return the authenticated login
    /// (`yt auth verify`, GET /api/users/me). The single read-only answer to "is
    /// this token still valid". Never returns the token itself.
    fn auth_verify(&self) -> Result<String> {
        Err(Error::Other(anyhow::anyhow!(
            "auth_verify not supported by this tracker backend"
        )))
    }

    /// Run an arbitrary issue query, bounded to `top` results. Backs
    /// the access check's "does the action's query execute under this
    /// credential" probe, where only the exit status matters and the full result
    /// set would be wasted work.
    fn list_query_top(&self, query: &str, top: u32) -> Result<Vec<serde_json::Value>> {
        let _ = (query, top);
        Err(Error::Other(anyhow::anyhow!(
            "list_query_top not supported by this tracker backend"
        )))
    }

    /// Read a project's metadata by short name (`yt project show`),
    /// returning its full name. Errors when the project is absent OR unreadable
    /// by this token; YouTrack reports both the same way, so the caller renders
    /// the failure as a permission gap rather than a missing project.
    fn project_show(&self, project_key: &str) -> Result<String> {
        let _ = project_key;
        Err(Error::Other(anyhow::anyhow!(
            "project_show not supported by this tracker backend"
        )))
    }
}

/// Render a failed [`Forge::repo_access`] probe as an error an
/// operator can act on: which capability failed (`cmd`), the exit status, and
/// the grant to add (`scope`). A 403 and a 404 need different grants, so they
/// are told apart from the CLI's own text: `fj` says `unexpected status code
/// 403` / `not found`, `gh` says `Resource not accessible` / `Could not resolve`.
/// Only the CLI's stderr is quoted; it carries no token.
pub(crate) fn probe_denied(
    identity: &str,
    tool: &str,
    cmd: &str,
    scope: &str,
    host: &str,
    repo: &str,
    out: &CmdOutput,
) -> Error {
    let detail = {
        let t = out.stderr.trim();
        if t.is_empty() {
            "no stderr"
        } else {
            t
        }
    };
    let lower = detail.to_ascii_lowercase();
    let cause =
        if lower.contains("403") || lower.contains("forbidden") || lower.contains("not accessible")
        {
            format!(
            "HTTP 403: the token can see `{repo}` but is refused this call, so it is missing the \
             `{scope}` scope or the matching unit permission on `{repo}`. Add `{scope}` to the \
             token (or enable the unit) and the next poll picks the issues up."
        )
        } else if lower.contains("404")
            || lower.contains("not found")
            || lower.contains("could not resolve")
        {
            format!(
                "HTTP 404: `{repo}` is not visible to this token at all, so this is a missing \
             permission grant for the bot on `{repo}` rather than a scope problem (adding the \
             `{scope}` scope alone will not reveal it)."
            )
        } else {
            format!(
            "the CLI reported no HTTP status; if this is not transient the token is most likely \
             missing the `{scope}` scope or a permission grant on `{repo}`."
        )
        };
    Error::Other(anyhow::anyhow!(
        "{identity} cannot `{cmd}` on `{repo}` at {host}: `{tool} {cmd}` exited {} ({detail}). \
         {cause}",
        out.exit_code
    ))
}

/// Reject a PR tip subject that is empty or a template heading/bare issue
/// reference rather than a one-line subject (CLAUDE-398). Called before any
/// push or PR-create call, so a bad tip opens nothing: the error names the
/// branch and the offending subject, and reaches the issue comment through
/// the existing PR-creation failure path in `finalize`.
pub fn validate_pr_subject(subject: &str, branch: &str) -> Result<()> {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        return Err(Error::Other(anyhow::anyhow!(
            "branch `{branch}`'s tip commit has no subject line, so no PR can open from it"
        )));
    }
    if trimmed.starts_with('#') {
        return Err(Error::Other(anyhow::anyhow!(
            "branch `{branch}`'s tip commit subject is `{trimmed}`, a template heading or a bare \
             issue reference rather than a one-line subject, so no PR can open from it"
        )));
    }
    Ok(())
}

/// The Forgejo-side operations native handlers need.
pub trait Forge: Send + Sync {
    /// Open a PR from the local commits in `workdir` against `host`. `strategy`
    /// selects branch-backed (push a real server branch, open via
    /// the API) or AGit (`fj --host <host> pr create -aA`); the selected
    /// strategy is authoritative, with no silent cross-strategy fallback that
    /// changes whether a server branch is produced. Returns the parsed PR
    /// number and URL.
    fn create_pr(
        &self,
        workdir: &std::path::Path,
        host: &str,
        strategy: crate::action::PrStrategy,
    ) -> Result<PullRequest>;

    /// Like [`Self::create_pr`], but `draft` selects whether the opened PR is a
    /// draft (Forgejo: a `WIP: ` title prefix, the convention `fj pr search
    /// --json` reports through its `draft` field; GitHub: the real draft flag).
    /// Used by the timeout salvage path (CLAUDE-379) to open a draft PR for
    /// work rescued from a timed-out run. `body`, when `Some`, is written
    /// verbatim as the PR body instead of the tip commit's own body
    /// (CLAUDE-398): a killed run's tip is often an agent layer commit whose
    /// body is only the bare `#<KEY>` line, which tells a reader nothing. The
    /// title still comes from the tip subject, carrying the `WIP: ` prefix.
    /// The default ignores `draft` and `body` and falls back to
    /// [`Self::create_pr`], so a backend with no draft notion (including
    /// every test-only stub) keeps compiling unchanged.
    fn create_pr_with_draft(
        &self,
        workdir: &std::path::Path,
        host: &str,
        strategy: crate::action::PrStrategy,
        draft: bool,
        body: Option<&str>,
    ) -> Result<PullRequest> {
        let _ = (draft, body);
        self.create_pr(workdir, host, strategy)
    }

    /// Strip PR `pr_number` in `repo` on `host` of its draft status (Forgejo:
    /// remove the `WIP: ` title prefix; GitHub: `gh pr ready`). Used when a
    /// re-queued run resumes onto a draft PR a prior timed-out run salvaged
    /// (CLAUDE-379) and now succeeds. The default errors so a backend without
    /// draft support (including every test-only stub) reports clearly rather
    /// than silently no-op'ing.
    fn mark_pr_ready(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "mark_pr_ready not supported by this forge backend"
        )))
    }

    /// Overwrite PR `pr_number` in `repo` on `host` with the title and body
    /// derived from `workdir`'s current tip commit (subject and, for
    /// Forgejo, the reflowed body; GitHub's `create_pr_impl` already reads
    /// `HEAD` the same way `create_pr` does). Used when a re-queued run
    /// (CLAUDE-275) resumes onto a draft PR a prior timed-out run salvaged
    /// (CLAUDE-379) and now finishes: the placeholder title/body the salvage
    /// wrote (CLAUDE-398) is replaced with the finished tip's own text,
    /// validated through [`validate_pr_subject`] first so a bad tip refuses
    /// here exactly as it would at PR-create time. The default errors so a
    /// backend without support (including every test-only stub) reports
    /// clearly rather than silently no-op'ing.
    fn refresh_pr_text(
        &self,
        workdir: &std::path::Path,
        host: &str,
        repo: &str,
        pr_number: u64,
    ) -> Result<()> {
        let _ = (workdir, host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "refresh_pr_text not supported by this forge backend"
        )))
    }

    /// Drain the operator-visible notices this backend accumulated
    /// while serving the job, e.g. an open-PR lookup that failed and was
    /// degraded to a create. The caller persists them as `run_events` so they
    /// show on the action page, not only in the daemon journal. Backends with
    /// nothing to report keep the default (empty).
    fn take_notices(&self) -> Vec<String> {
        Vec::new()
    }

    /// Record an operator-visible notice from OUTSIDE this backend
    /// (the native handler, which knows about the `agent_pull_requests` store
    /// this trait has no knowledge of), so a DB-write failure after a successful
    /// `create_pr` surfaces through the same [`Self::take_notices`] /
    /// `run_events` path as a lookup degradation rather than only the daemon
    /// journal. The default is a no-op, matching a backend with no notice queue.
    fn note(&self, _message: String) {}

    /// Resolve an authenticated clone URL for `repo` on `host` using the forge
    /// CLI's stored credentials (no token in YAML or env). `repo` is the
    /// `owner/name` path.
    fn clone_url(&self, host: &str, repo: &str) -> Result<String>;

    /// Verify the FORGE identity can work `repo` (`owner/name`) on
    /// `host`. `git push` authenticates from the host's own git credentials, so a
    /// repo the forge token cannot see still clones and pushes and only fails at
    /// `pr create` time, after a whole Claude run is paid for. This is the
    /// preflight that catches it first; the error names the host and `owner/repo`.
    ///
    /// The probe covers BOTH capability categories the PR path uses,
    /// a repository read and a pull-request read, because a token carrying only
    /// the repository category passes a repository-only probe and then 403s on
    /// the first open-PR lookup. Implementations report which probe failed and
    /// the scope label the operator has to add (see [`probe_denied`]).
    fn repo_access(&self, host: &str, repo: &str) -> Result<()>;

    /// The forge identity this backend authenticates as on `host`
    /// (`fj whoami`, or the GitHub identity read). Read-only, and it never
    /// returns the token. Backs the access check's "which account is this?" row.
    fn identity(&self, host: &str) -> Result<String> {
        let _ = host;
        Err(Error::Other(anyhow::anyhow!(
            "identity not supported by this forge backend"
        )))
    }

    /// List open PRs for `repo` (`owner/name`) on `host`. Backs the CI-fix scan
    /// (`fj pr search --repo <repo> --state open --json`).
    fn list_open_prs(&self, host: &str, repo: &str) -> Result<Vec<ForgePullRequest>>;

    /// Combined CI status of PR `pr_number` in `repo` on `host`, e.g.
    /// `"success"` / `"failure"` / `"pending"` (`fj pr status <repo>#<n> --json`).
    /// The PR is addressed as `owner/repo#n` so no local checkout is
    /// needed to resolve the repo.
    fn pr_ci_state(&self, host: &str, repo: &str, pr_number: u64) -> Result<String>;

    /// The commenter's EFFECTIVE repository permission on `repo` at
    /// `host`, read live from the forge
    /// (`fj repo collaborator <repo> permission <username> --format json`, whose
    /// `permission` field is returned verbatim, e.g. `read` / `write` / `admin`).
    /// Backs the `/claude` comment-command authorization gate, which accepts only
    /// a permission in the source's configured `allowed_permissions`. An error is
    /// a DENY at the call site (fail closed); no raw REST anywhere on this path.
    /// The default errors so a backend without the read denies rather than
    /// silently allowing.
    fn repo_permission(&self, host: &str, repo: &str, username: &str) -> Result<String> {
        let _ = (host, repo, username);
        Err(Error::Other(anyhow::anyhow!(
            "repo_permission not supported by this forge backend"
        )))
    }

    /// Post a comment on PR `pr_number` in `repo` on `host`, body read from
    /// `body_file` (`fj pr comment <repo>#<n> --body-file <path>`).
    fn pr_comment(
        &self,
        host: &str,
        repo: &str,
        pr_number: u64,
        body_file: &std::path::Path,
    ) -> Result<()>;

    /// Post a comment on plain ISSUE `issue_number` in `repo` on
    /// `host`, body read from `body_file` (`fj issue comment <repo>#<n>
    /// --body-file <path>`). The issue-thread mirror of [`Forge::pr_comment`],
    /// used to reply to a `/claude` command left on an issue that has no pull
    /// request (and therefore no branch to push). No raw REST on this path.
    fn issue_comment(
        &self,
        host: &str,
        repo: &str,
        issue_number: u64,
        body_file: &std::path::Path,
    ) -> Result<()>;

    /// Combined CI status plus mergeability of PR `pr_number` in
    /// `repo` on `host` (`fj pr status <repo>#<n> --json`). Backs the automerge
    /// poll, which needs both the CI verdict and whether the branch is up to
    /// date in a single query. The default errors so a backend that has not
    /// implemented automerge (the GitHub `Forge`, a follow-up) reports clearly
    /// rather than silently succeeding.
    fn pr_status(&self, host: &str, repo: &str, pr_number: u64) -> Result<PrStatus> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "pr_status (automerge) not supported by this forge backend"
        )))
    }

    /// Merge PR `pr_number` in `repo` on `host` as a merge-commit
    /// (never squash/rebase, so the single commit's bare `#<KEY>` reference
    /// survives in history). The default errors for backends without automerge.
    fn merge_pr(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "merge_pr (automerge) not supported by this forge backend"
        )))
    }

    /// Bring PR `pr_number` in `repo` on `host` up to date with its
    /// base through Forgejo's Update-branch endpoint (`fj pr update <repo>#<n>
    /// --update-style merge`; this flag was renamed in a later Forgejo release,
    /// so an older doc or script may still name the old flag),
    /// retriggering CI. `--update-style merge` matches [`Self::merge_pr`]'s
    /// merge-commit policy and never rewrites the agent's branch.
    /// The default errors for backends without automerge.
    fn update_pr_branch(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "update_pr_branch (automerge) not supported by this forge backend"
        )))
    }

    /// Add Forgejo labels to PR `pr_number` in `repo` on `host`
    /// (`fj pr edit <repo>#<n> labels --add <label>`). Used to flag a PR Needs
    /// Review when the per-PR auto-fix ceiling is reached. The default errors so
    /// a backend without label support reports clearly.
    fn pr_add_labels(
        &self,
        host: &str,
        repo: &str,
        pr_number: u64,
        labels: &[String],
    ) -> Result<()> {
        let _ = (host, repo, pr_number, labels);
        Err(Error::Other(anyhow::anyhow!(
            "pr_add_labels not supported by this forge backend"
        )))
    }
}

/// A backend implementing the full provider surface. Marker super-trait so the
/// factory can hand callers one object exposing both halves.
pub trait Backend: IssueTracker + Forge {
    /// Stable name for diagnostics / the `NoBackend` error message.
    fn name(&self) -> &'static str;
}

/// Ordered backend preference. The YouTrack / Forgejo provider resolution
/// ([`resolve`], [`resolve_runner`]) walks `[Cli]`. The GitHub `Forge`
/// walks `[Mcp, Gh]`: prefer the remote MCP, fall back to the `gh`
/// CLI. Variants are shared so one ordered priority type covers every backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// Shell out to the host `yt` / `fj` CLIs.
    Cli,
    /// Talk to a remote MCP server over HTTPS (the GitHub hosted MCP).
    Mcp,
    /// Shell out to the host `gh` CLI.
    Gh,
}

/// Default GitHub `Forge` priority: prefer the remote MCP, fall back to `gh`.
pub const GITHUB_PRIORITY: &[BackendKind] = &[BackendKind::Mcp, BackendKind::Gh];

/// Default priority: CLI-first, and CLI-only for this phase.
pub const DEFAULT_PRIORITY: &[BackendKind] = &[BackendKind::Cli];

/// Env var carrying the credential username for the inline helper
/// [`CREDENTIAL_HELPER`] builds. Set only on the child git process's own
/// environment by [`authed_git_output`] / [`run_authed_git`], never written to
/// argv or any git config file.
const GIT_CRED_USER_ENV: &str = "CLAUDE_RUN_GIT_USERNAME";
/// Env var carrying the credential password/token for [`CREDENTIAL_HELPER`].
const GIT_CRED_PASS_ENV: &str = "CLAUDE_RUN_GIT_PASSWORD";

/// An inline git credential helper (the `!<shell>` form) that answers git's
/// credential protocol by echoing back whatever this process set in its own
/// environment under [`GIT_CRED_USER_ENV`] / [`GIT_CRED_PASS_ENV`]. Passed as
/// `-c credential.helper=<this>` on the git invocation itself, so the
/// credential lives only in that one child's environment: never in argv,
/// never in `.git/config`, never inherited by anything git spawns afterwards.
pub(crate) const CREDENTIAL_HELPER: &str = concat!(
    "credential.helper=!f() { echo \"username=$",
    "CLAUDE_RUN_GIT_USERNAME",
    "\"; echo \"password=$",
    "CLAUDE_RUN_GIT_PASSWORD",
    "\"; }; f"
);

/// The two leading `-c` arguments that make a git invocation use
/// [`CREDENTIAL_HELPER`] instead of whatever the host or workdir already
/// configured. `credential.helper=` (empty value) first CLEARS any inherited
/// helper list: git accumulates `credential.helper` entries, so without this
/// an inherited helper could still answer (or cache) ahead of this one.
fn credential_helper_args() -> [&'static str; 4] {
    ["-c", "credential.helper=", "-c", CREDENTIAL_HELPER]
}

/// The `user:pass` pair from a token-bearing clone URL's userinfo
/// (`https://user:pass@host/...`), for handing to the per-invocation
/// credential helper. `None` when the URL carries no userinfo at all, which
/// callers treat as "no credential resolves" and fall back to an
/// unauthenticated invocation.
fn url_userinfo(url: &str) -> Option<(String, String)> {
    let rest = url.split_once("://").map(|(_, r)| r)?;
    let (userinfo, _) = rest.split_once('@')?;
    if userinfo.is_empty() {
        return None;
    }
    let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    Some((user.to_string(), pass.to_string()))
}

/// `url` with its userinfo stripped (`https://user:pass@host/x` ->
/// `https://host/x`), unchanged when it carries none. This is what every
/// remote URL in a job workdir is rewritten to after a clone, so a forge
/// token is never left sitting in `.git/config` or `.git/modules/**/config`.
fn strip_userinfo(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, hostpath)) => format!("{scheme}://{hostpath}"),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

/// Run `git <args>` in `dir` (the whole workdir when `None`), authenticated
/// per invocation via [`credential_helper_args`] when `token_url` resolves a
/// credential, else unauthenticated (any host git credential helper still
/// applies). Used by call sites that shell out directly with
/// [`std::process::Command`] rather than through a [`CommandRunner`] (this
/// module's own clone/scrub machinery, and [`crate::workdir`]).
pub fn authed_git_output(
    dir: Option<&std::path::Path>,
    args: &[&str],
    token_url: Option<&str>,
) -> std::io::Result<std::process::Output> {
    let mut cmd = std::process::Command::new("git");
    if let Some(d) = dir {
        cmd.arg("-C").arg(d);
    }
    if let Some((user, pass)) = token_url.and_then(url_userinfo) {
        cmd.args(credential_helper_args())
            .env(GIT_CRED_USER_ENV, user)
            .env(GIT_CRED_PASS_ENV, pass);
    }
    cmd.args(args).output()
}

/// Same per-invocation authentication as [`authed_git_output`], but through a
/// [`CommandRunner`] so call sites built on it (notably [`cli::CliBackend`])
/// stay testable against a [`cli::StubRunner`] instead of a real `git`.
pub fn run_authed_git(
    runner: &dyn CommandRunner,
    dir: Option<&std::path::Path>,
    args: &[&str],
    token_url: Option<&str>,
) -> Result<CmdOutput> {
    match token_url.and_then(url_userinfo) {
        Some((user, pass)) => {
            let mut full: Vec<&str> = Vec::with_capacity(4 + args.len());
            full.extend(credential_helper_args());
            full.extend_from_slice(args);
            runner.run_in_env(
                dir,
                "git",
                &full,
                &[
                    (GIT_CRED_USER_ENV, user.as_str()),
                    (GIT_CRED_PASS_ENV, pass.as_str()),
                ],
            )
        }
        None => runner.run_in_env(dir, "git", args, &[]),
    }
}

/// Rewrite every remote URL a fresh clone could have persisted with
/// credentials: `origin` in `workdir` itself, `submodule.<name>.url` in the
/// superproject config, and `origin` inside every submodule's own git
/// directory, recursively. Called once at the end of [`shallow_clone`], after
/// `init_submodules` has populated any submodule checkouts, so nothing ever
/// observes a token-bearing remote even transiently between clone and scrub.
///
/// A rewrite failure fails the clone outright (through [`git_error`], same as
/// every other failure in this module) rather than leaving a half-scrubbed
/// workdir behind a warning: a swallowed failure here is exactly the leak
/// this function exists to prevent.
fn scrub_workdir_credentials(url: &str, workdir: &std::path::Path) -> Result<()> {
    if !workdir.join(".git").exists() {
        return Ok(());
    }
    scrub_remote_origin_url(url, workdir)?;
    scrub_submodule_config_urls(url, workdir)?;
    recurse_into_submodules(url, workdir)
}

/// Strip userinfo from `origin`'s URL in `workdir`, if it has one and if the
/// URL carries any.
fn scrub_remote_origin_url(url: &str, workdir: &std::path::Path) -> Result<()> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("remote")
        .arg("get-url")
        .arg("origin")
        .output()?;
    if !out.status.success() {
        // No `origin` remote (shouldn't happen for a fresh clone, but a
        // submodule-only directory with none has nothing to scrub).
        return Ok(());
    }
    let current = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stripped = strip_userinfo(&current);
    if stripped == current {
        return Ok(());
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("remote")
        .arg("set-url")
        .arg("origin")
        .arg(&stripped)
        .output()?;
    if !out.status.success() {
        return Err(git_error("remote set-url", &out, Some(url)));
    }
    Ok(())
}

/// Strip userinfo from every `submodule.<name>.url` entry recorded in
/// `workdir`'s own config (written there by `git submodule init`/`update`
/// when `.gitmodules` carries a relative URL, resolved against the
/// token-bearing `origin`).
fn scrub_submodule_config_urls(url: &str, workdir: &std::path::Path) -> Result<()> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("config")
        .arg("--get-regexp")
        .arg(r"^submodule\..*\.url$")
        .output()?;
    if !out.status.success() {
        // Exit 1 just means no matching keys (no submodules initialized).
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    for line in stdout.lines() {
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        let stripped = strip_userinfo(value);
        if stripped == value {
            continue;
        }
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(workdir)
            .arg("config")
            .arg(key)
            .arg(&stripped)
            .output()?;
        if !out.status.success() {
            return Err(git_error("config submodule url", &out, Some(url)));
        }
    }
    Ok(())
}

/// Recurse into every submodule `.gitmodules` names (by path, not URL, since
/// the URL may be relative) and scrub its own `origin` and any
/// submodules-of-submodules the same way.
fn recurse_into_submodules(url: &str, workdir: &std::path::Path) -> Result<()> {
    if !workdir.join(".gitmodules").is_file() {
        return Ok(());
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("config")
        .arg("--file")
        .arg(".gitmodules")
        .arg("--get-regexp")
        .arg(r"^submodule\..*\.path$")
        .output()?;
    if !out.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    for line in stdout.lines() {
        let Some((_, path)) = line.split_once(' ') else {
            continue;
        };
        let sub_dir = workdir.join(path.trim());
        if !sub_dir.join(".git").exists() {
            continue;
        }
        scrub_remote_origin_url(url, &sub_dir)?;
        scrub_submodule_config_urls(url, &sub_dir)?;
        recurse_into_submodules(url, &sub_dir)?;
    }
    Ok(())
}

/// Shallow-clone `url` into `workdir`, pinning `branch` when given.
///
/// Captures git's output instead of letting it inherit the daemon's
/// stderr, so a failure reports WHY. Before this every clone site reported only
/// `git clone exited 128`, which in a container is indistinguishable between a
/// missing credential, an unknown host and a deleted branch, and diagnosing the
/// real one needed a hand-run `git clone` inside the container.
///
/// The URL is token-bearing (see [`Forge::clone_url`]), and git echoes it in its
/// own messages, so the captured text goes through [`crate::forge_log::redact`]
/// with the URL's userinfo passed as a known secret. The URL itself never enters
/// the error: the reason string is persisted to `run_events` and can reach a
/// YouTrack comment.
pub fn shallow_clone(
    url: &str,
    branch: Option<&str>,
    workdir: &std::path::Path,
    depth: u32,
) -> Result<()> {
    let mut cmd = std::process::Command::new("git");
    // A detached auto-maintenance repack would race the push and the
    // workdir removal; an ephemeral clone never benefits from it.
    cmd.arg("clone")
        .arg("--config")
        .arg("maintenance.auto=false");
    if let Some(b) = branch {
        cmd.arg("--branch").arg(b);
    }
    let out = cmd
        .arg("--depth")
        .arg(depth.to_string())
        .arg(url)
        .arg(workdir)
        .output()?;
    if !out.status.success() {
        return Err(git_error("clone", &out, Some(url)));
    }
    if let Some(pinned) = branch {
        restore_default_branch_refs(url, pinned, workdir, depth)?;
    }
    init_submodules(url, workdir)?;
    scrub_workdir_credentials(url, workdir)?;
    Ok(())
}

/// Check out every submodule the clone pinned, when the repo has any.
///
/// A repo that vendors its task runner as a submodule (for example `common`,
/// pulled in via a `justfile` import) is otherwise cloned with an empty
/// submodule directory, and the repo's own gate recipes (`just pre-commit`)
/// fail on the agent's very first call. Skipped silently when there is no
/// `.gitmodules`, so a repo with none behaves exactly as before. `--depth 1`
/// assumes the pinned submodule commit is reachable from the submodule's
/// default branch tip, matching the shallow top-level clone.
///
/// `protocol.file.allow=always` only widens what a LOCAL `file://` submodule
/// may do (git otherwise refuses to follow one from an untrusted superproject,
/// CVE-2022-39253); every real submodule here is fetched over the same
/// `https`/`ssh` host as the superproject, which this never restricts.
///
/// A submodule fetch failure is a clone failure: it surfaces through the same
/// [`git_error`] path as every other git failure in this module, with
/// redacted stderr, never a warning followed by a half-populated tree.
fn init_submodules(url: &str, workdir: &std::path::Path) -> Result<()> {
    if !workdir.join(".gitmodules").is_file() {
        return Ok(());
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("-c")
        .arg("protocol.file.allow=always")
        .arg("submodule")
        .arg("update")
        .arg("--init")
        .arg("--recursive")
        .arg("--depth")
        .arg("1")
        .output()?;
    if !out.status.success() {
        return Err(git_error("submodule update", &out, Some(url)));
    }
    Ok(())
}

/// A pinned-branch clone (`--branch` implies `--single-branch`)
/// carries only `refs/remotes/origin/<pinned>`, so it has no
/// `refs/remotes/origin/HEAD` and no `refs/remotes/origin/<default>`. That
/// breaks every base-branch lookup a resumed-PR workdir needs
/// (`CliBackend::resolve_feature_branch`, `GithubForge::detect_base`), because
/// there is nothing for `origin/HEAD` to resolve to. This fetches the
/// remote's actual default branch (never assumed) and restores
/// `origin/HEAD`, so a pinned clone ends up with the same base refs as a
/// default-branch clone.
fn restore_default_branch_refs(
    url: &str,
    pinned: &str,
    workdir: &std::path::Path,
    depth: u32,
) -> Result<()> {
    let out = std::process::Command::new("git")
        .arg("ls-remote")
        .arg("--symref")
        .arg(url)
        .arg("HEAD")
        .output()?;
    if !out.status.success() {
        return Err(git_error("ls-remote", &out, Some(url)));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let default_branch = stdout
        .lines()
        .find_map(|l| l.strip_prefix("ref: refs/heads/"))
        .and_then(|rest| rest.split_whitespace().next());
    let Some(default_branch) = default_branch else {
        let secrets = url_secrets(url);
        return Err(Error::Other(anyhow::anyhow!(
            "remote has no resolvable default branch: {}",
            crate::forge_log::redact(stdout.trim(), &secrets)
        )));
    };
    let default_branch = default_branch.to_string();

    if default_branch != pinned {
        let refspec = format!("+refs/heads/{default_branch}:refs/remotes/origin/{default_branch}");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(workdir)
            .arg("fetch")
            .arg("--depth")
            .arg(depth.to_string())
            .arg("origin")
            .arg(&refspec)
            .output()?;
        if !out.status.success() {
            return Err(git_error("fetch", &out, Some(url)));
        }
    }

    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("remote")
        .arg("set-head")
        .arg("origin")
        .arg(&default_branch)
        .output()?;
    if !out.status.success() {
        return Err(git_error("remote set-head", &out, Some(url)));
    }
    Ok(())
}

/// The error for a failed git subprocess: the exit code plus the tail of git's
/// own stderr, redacted.
///
/// Every git call site used to report only an exit code, so `git
/// clone exited 128` and `git push exited 1` were the whole diagnosis for a
/// missing credential, an unknown host, a deleted branch, a rejected
/// non-fast-forward and a protected branch alike. `url` is the token-bearing URL
/// when the command had one, so its credential is masked verbatim on top of the
/// shape-based redaction; the URL itself is never included, because this string
/// is persisted to `run_events` and can reach a YouTrack comment.
pub fn git_error(context: &str, out: &std::process::Output, url: Option<&str>) -> Error {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let secrets = url.map(url_secrets).unwrap_or_default();
    let detail = clone_error_tail(&crate::forge_log::redact(stderr.trim(), &secrets));
    Error::Other(anyhow::anyhow!(
        "git {context} exited {}{}",
        out.status.code().unwrap_or(-1),
        if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        }
    ))
}

/// The credential inside a clone URL's userinfo (`https://user:TOKEN@host/...`),
/// so it can be masked verbatim wherever git echoed it. Both halves are returned:
/// the password is the secret, and the whole `user:pass` pair is masked too in
/// case git printed it unsplit.
fn url_secrets(url: &str) -> Vec<String> {
    let Some(rest) = url.split_once("//").map(|(_, r)| r) else {
        return Vec::new();
    };
    let Some((userinfo, _)) = rest.split_once('@') else {
        return Vec::new();
    };
    let mut out = vec![userinfo.to_string()];
    if let Some((_, pass)) = userinfo.split_once(':') {
        out.push(pass.to_string());
    }
    out
}

/// Keep the last few lines of git's stderr: the cause is at the end, and the
/// banner above it ("Cloning into ...") is noise in a failure reason that lands
/// on an action page.
fn clone_error_tail(stderr: &str) -> String {
    stderr
        .lines()
        .map(str::trim)
        // "Cloning into '<path>'..." is git's progress banner, not a cause, and
        // it carries the workdir path into a message an operator reads.
        .filter(|l| !l.is_empty() && !l.starts_with("Cloning into"))
        .rev()
        .take(3)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("; ")
}

/// Resolve a usable [`Backend`] from `priority`, walking the list in order and
/// returning the first backend that probes successfully. Errors
/// [`Error::NoBackend`] (listing every attempt) when none is usable, e.g. when
/// `yt` / `fj` are absent from `PATH`.
///
/// Uses the real [`ProcessRunner`]; tests drive [`resolve_with`] with a stub.
pub fn resolve(priority: &[BackendKind]) -> Result<Box<dyn Backend>> {
    resolve_with(priority, Arc::new(ProcessRunner::new()))
}

/// [`resolve`] with an injectable [`CommandRunner`], so the factory's
/// priority-walk and the "no usable backend" path can be unit-tested without
/// touching real binaries.
pub fn resolve_with(
    priority: &[BackendKind],
    runner: Arc<dyn CommandRunner>,
) -> Result<Box<dyn Backend>> {
    let mut tried: Vec<String> = Vec::new();
    for kind in priority {
        match kind {
            BackendKind::Cli => {
                let backend = CliBackend::new(runner.clone());
                match backend.probe() {
                    Ok(()) => return Ok(Box::new(backend)),
                    Err(e) => tried.push(format!("cli: {e}")),
                }
            }
            // Mcp / Gh resolve the GitHub `Forge` (see [`github`]), not the
            // YouTrack/Forgejo `Backend`; they never appear in this priority list.
            BackendKind::Mcp | BackendKind::Gh => {}
        }
    }
    Err(Error::NoBackend(tried.join("; ")))
}

/// Resolve the [`CommandRunner`] of the first usable backend, walking `priority`
/// and probing exactly as [`resolve`] does. Callers that need to build per-job
/// [`CliBackend`]s with injected credentials use this to obtain a
/// shareable runner instead of an opaque `Box<dyn Backend>`. CLI-only today.
pub fn resolve_runner(priority: &[BackendKind]) -> Result<Arc<dyn CommandRunner>> {
    resolve_runner_with(priority, Arc::new(ProcessRunner::new()))
}

/// [`resolve_runner`] with an injectable runner (tests).
pub fn resolve_runner_with(
    priority: &[BackendKind],
    runner: Arc<dyn CommandRunner>,
) -> Result<Arc<dyn CommandRunner>> {
    let mut tried: Vec<String> = Vec::new();
    for kind in priority {
        match kind {
            BackendKind::Cli => {
                let backend = CliBackend::new(runner.clone());
                match backend.probe() {
                    Ok(()) => return Ok(runner),
                    Err(e) => tried.push(format!("cli: {e}")),
                }
            }
            BackendKind::Mcp | BackendKind::Gh => {}
        }
    }
    Err(Error::NoBackend(tried.join("; ")))
}

/// Routes every [`crate::forge_log::ActionLogClient`] /
/// [`crate::forge_log::ActionRunsClient`] / [`crate::forge_log::RepoFileClient`]
/// call to `gh` ([`GhForgeReads`]) for a GitHub host (`github.com` or a
/// configured GHE host, [`is_github_host`]) and to `fj` ([`FjForgeReads`]) for
/// everything else, mirroring the per-host `Forge` dispatch
/// `resolve_automerge_forge` (`src/web/webhooks.rs`) already uses. Built once
/// per daemon lifetime - `AnalysisClassifier` and the webhook failure-log
/// fetch each hold one shared client across every webhook source - so the
/// `host` argument on each call, not a per-source config lookup, decides
/// which client answers.
pub struct HostRoutedForgeReads {
    fj: Arc<dyn crate::forge_log::ForgeReads>,
    gh: Arc<dyn crate::forge_log::ForgeReads>,
    gh_hosts: Vec<String>,
}

impl HostRoutedForgeReads {
    pub fn new(
        fj: Arc<dyn crate::forge_log::ForgeReads>,
        gh: Arc<dyn crate::forge_log::ForgeReads>,
        gh_hosts: Vec<String>,
    ) -> Self {
        Self { fj, gh, gh_hosts }
    }

    fn pick(&self, host: &str) -> &Arc<dyn crate::forge_log::ForgeReads> {
        if is_github_host(host, &self.gh_hosts) {
            &self.gh
        } else {
            &self.fj
        }
    }
}

impl crate::forge_log::ActionLogClient for HostRoutedForgeReads {
    fn run_logs(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> crate::forge_log::RunLogsOutcome {
        self.pick(host).run_logs(host, repo, run_number, token)
    }
}

impl crate::forge_log::ActionRunsClient for HostRoutedForgeReads {
    fn list_runs(
        &self,
        host: &str,
        repo: &str,
        max_pages: u32,
        token: Option<&str>,
    ) -> crate::forge_log::RunsListOutcome {
        self.pick(host).list_runs(host, repo, max_pages, token)
    }

    fn run_timing(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> std::result::Result<crate::forge_log::RunTiming, String> {
        self.pick(host).run_timing(host, repo, run_number, token)
    }
}

impl crate::forge_log::RepoFileClient for HostRoutedForgeReads {
    fn read_file(
        &self,
        host: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        token: Option<&str>,
    ) -> std::result::Result<String, String> {
        self.pick(host).read_file(host, repo, path, git_ref, token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forge_log::ActionLogClient;
    use cli::testutil::StubRunner;

    #[test]
    fn validate_pr_subject_rejects_empty() {
        let err = validate_pr_subject("", "feat/x").unwrap_err().to_string();
        assert!(err.contains("feat/x"), "{err}");
    }

    #[test]
    fn validate_pr_subject_rejects_template_heading() {
        let err = validate_pr_subject("## Summary", "feat/x")
            .unwrap_err()
            .to_string();
        assert!(err.contains("feat/x"), "{err}");
        assert!(err.contains("## Summary"), "{err}");
    }

    #[test]
    fn validate_pr_subject_rejects_bare_issue_reference() {
        let err = validate_pr_subject("#KEY-1", "feat/x")
            .unwrap_err()
            .to_string();
        assert!(err.contains("#KEY-1"), "{err}");
    }

    #[test]
    fn validate_pr_subject_accepts_a_real_subject() {
        validate_pr_subject("Pin base image digests", "feat/x").unwrap();
    }

    /// A failed clone must say WHY. `git clone exited 128` alone is
    /// the same message for a missing credential, an unknown host and a deleted
    /// branch, and diagnosing the real one cost a hand-run clone in the
    /// container.
    #[test]
    fn a_failed_clone_reports_gits_own_reason() {
        let tmp = tempfile::tempdir().unwrap();
        let err = shallow_clone("/nonexistent/repo.git", None, &tmp.path().join("out"), 50)
            .unwrap_err()
            .to_string();
        assert!(err.contains("git clone exited"), "{err}");
        assert!(
            err.to_lowercase().contains("repository")
                || err.to_lowercase().contains("does not exist"),
            "the reason git printed must survive into the error: {err}"
        );
    }

    /// The clone URL carries a token, git echoes it, and the reason string is
    /// persisted to `run_events` and can reach a YouTrack comment. Neither the
    /// token nor the URL may appear.
    #[test]
    fn a_failed_clone_never_leaks_the_url_credential() {
        let tmp = tempfile::tempdir().unwrap();
        let token = "abcdef0123456789abcdef0123456789";
        let url = format!("https://oauth2:{token}@127.0.0.1:1/owner/repo.git");
        let err = shallow_clone(&url, None, &tmp.path().join("out"), 50)
            .unwrap_err()
            .to_string();
        assert!(
            !err.contains(token),
            "the token must not survive redaction: {err}"
        );
        assert!(
            !err.contains("oauth2:"),
            "the userinfo must not survive redaction: {err}"
        );
    }

    /// The key must land in the clone's own config, so every later
    /// git command in the workdir (runner and agent) skips auto-maintenance.
    #[test]
    fn a_clone_disables_auto_maintenance() {
        let tmp = tempfile::tempdir().unwrap();
        let origin = tmp.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["add", "."],
            vec![
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t.example",
                "commit",
                "-q",
                "-m",
                "init",
            ],
        ] {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&origin)
                .args(&args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let clone = tmp.path().join("clone");
        let url = format!("file://{}", origin.display());
        shallow_clone(&url, None, &clone, 50).unwrap();

        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(&clone)
            .args(["config", "--local", "--get", "maintenance.auto"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "maintenance.auto must be set in the clone: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "false");
    }

    /// The userinfo is pulled out of every URL shape the forge can produce, so
    /// redaction has the verbatim secret to mask and does not rely on shape
    /// heuristics alone.
    #[test]
    fn url_secrets_extracts_the_credential() {
        assert_eq!(
            url_secrets("https://oauth2:tok@host/owner/repo.git"),
            vec!["oauth2:tok".to_string(), "tok".to_string()]
        );
        assert_eq!(
            url_secrets("https://tok@host/owner/repo.git"),
            vec!["tok".to_string()]
        );
        assert!(url_secrets("https://host/owner/repo.git").is_empty());
        assert!(url_secrets("/local/path.git").is_empty());
    }

    /// The tail keeps the cause and drops git's banner, which is what makes the
    /// reason readable on an action page.
    #[test]
    fn the_error_tail_keeps_the_cause_not_the_banner() {
        let stderr = "Cloning into '/tmp/x'...\n\nfatal: could not read Username for 'https://h': terminal prompts disabled\n";
        let tail = clone_error_tail(stderr);
        assert!(tail.contains("could not read Username"), "{tail}");
        assert!(!tail.contains("Cloning into"), "{tail}");
    }

    #[test]
    fn resolve_returns_cli_when_binaries_present() {
        // Both probes succeed -> CLI backend resolves.
        let runner = StubRunner::new().ok("yt", &["--version"], "yt 0.3.0").ok(
            "fj",
            &["--version"],
            "fj 1.0.0",
        );
        let backend = resolve_with(DEFAULT_PRIORITY, Arc::new(runner)).unwrap();
        assert_eq!(backend.name(), "cli");
    }

    #[test]
    fn resolve_errors_clearly_when_yt_absent() {
        // `yt` missing -> probe fails -> no usable backend, message names yt.
        let runner = StubRunner::new().missing("yt");
        // `Box<dyn Backend>` is not Debug, so match rather than `unwrap_err`.
        let err = match resolve_with(DEFAULT_PRIORITY, Arc::new(runner)) {
            Ok(_) => panic!("expected no usable backend"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("no usable provider backend"), "got {msg}");
        assert!(msg.contains("yt"), "got {msg}");
    }

    // ---- a pinned clone still carries origin/HEAD -------------

    /// A real origin repo (over `file://`) whose default branch is
    /// `default_branch`, with one commit on it and a `feat` branch one commit
    /// ahead. Returns the `file://` URL `shallow_clone` can pull from.
    fn origin_with_default_branch(base: &std::path::Path, default_branch: &str) -> String {
        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        let git_in = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&origin)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git_in(&["init", "-q", "-b", default_branch]);
        git_in(&["config", "user.name", "t"]);
        git_in(&["config", "user.email", "t@t.example"]);
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&["add", "."]);
        git_in(&["commit", "-q", "-m", "init"]);
        git_in(&["checkout", "-q", "-b", "feat"]);
        std::fs::write(origin.join("g.txt"), "y").unwrap();
        git_in(&["add", "."]);
        git_in(&["commit", "-q", "-m", "feat commit"]);
        // The origin repo's own checked-out branch is what a `file://` clone's
        // remote HEAD follows; leave it back on the default branch, or every
        // clone (pinned or not) would see `origin/HEAD -> origin/feat`.
        git_in(&["checkout", "-q", default_branch]);
        format!("file://{}", origin.display())
    }

    fn rev_parse_abbrev(workdir: &std::path::Path, rev: &str) -> Option<String> {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(workdir)
            .args(["rev-parse", "--abbrev-ref", rev])
            .output()
            .unwrap();
        if out.status.success() {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            None
        }
    }

    #[test]
    fn pinned_clone_restores_origin_head_when_default_is_main() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_default_branch(tmp.path(), "main");
        let clone = tmp.path().join("clone");
        shallow_clone(&url, Some("feat"), &clone, 50).unwrap();
        assert_eq!(
            rev_parse_abbrev(&clone, "origin/HEAD").as_deref(),
            Some("origin/main")
        );
        assert_eq!(
            rev_parse_abbrev(&clone, "origin/main").as_deref(),
            Some("origin/main"),
            "refs/remotes/origin/main must resolve"
        );
    }

    #[test]
    fn pinned_clone_restores_origin_head_when_default_is_not_main() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_default_branch(tmp.path(), "trunk");
        let clone = tmp.path().join("clone");
        shallow_clone(&url, Some("feat"), &clone, 50).unwrap();
        assert_eq!(
            rev_parse_abbrev(&clone, "origin/HEAD").as_deref(),
            Some("origin/trunk")
        );
    }

    #[test]
    fn pinning_the_default_branch_itself_still_resolves_origin_head() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_default_branch(tmp.path(), "main");
        let clone = tmp.path().join("clone");
        shallow_clone(&url, Some("main"), &clone, 50).unwrap();
        assert_eq!(
            rev_parse_abbrev(&clone, "origin/HEAD").as_deref(),
            Some("origin/main")
        );
    }

    #[test]
    fn an_unpinned_clone_is_unaffected() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_default_branch(tmp.path(), "main");
        let clone = tmp.path().join("clone");
        shallow_clone(&url, None, &clone, 50).unwrap();
        assert_eq!(
            rev_parse_abbrev(&clone, "origin/HEAD").as_deref(),
            Some("origin/main")
        );
    }

    // ---- submodule init on clone -------------

    /// A bare submodule repo with one commit, plus a superproject that vendors
    /// it at `sub/` and commits the `.gitmodules` entry. Returns the
    /// superproject's `file://` URL `shallow_clone` can pull from. `git
    /// submodule add` over `file://` needs the same
    /// `protocol.file.allow=always` escape hatch as `init_submodules` itself,
    /// since it is the same CVE-2022-39253 guard.
    fn origin_with_submodule(base: &std::path::Path) -> String {
        let git_in = |dir: &std::path::Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };

        let sub = base.join("sub-origin");
        std::fs::create_dir_all(&sub).unwrap();
        git_in(&sub, &["init", "-q", "-b", "main"]);
        git_in(&sub, &["config", "user.name", "t"]);
        git_in(&sub, &["config", "user.email", "t@t.example"]);
        std::fs::write(sub.join("s.txt"), "s").unwrap();
        git_in(&sub, &["add", "."]);
        git_in(&sub, &["commit", "-q", "-m", "sub init"]);

        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_in(&origin, &["init", "-q", "-b", "main"]);
        git_in(&origin, &["config", "user.name", "t"]);
        git_in(&origin, &["config", "user.email", "t@t.example"]);
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-q", "-m", "init"]);
        let sub_url = format!("file://{}", sub.display());
        git_in(
            &origin,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                &sub_url,
                "sub",
            ],
        );
        git_in(&origin, &["commit", "-q", "-m", "add submodule"]);
        format!("file://{}", origin.display())
    }

    /// Acceptance: a clone of a repo with a `.gitmodules` entry leaves the
    /// submodule checked out (non-empty), for a fresh (unpinned) clone.
    #[test]
    fn a_fresh_clone_checks_out_its_submodule() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_submodule(tmp.path());
        let clone = tmp.path().join("clone");
        shallow_clone(&url, None, &clone, 50).unwrap();
        assert_eq!(
            std::fs::read_to_string(clone.join("sub").join("s.txt")).unwrap(),
            "s",
            "the submodule's tracked file must be checked out, not an empty directory"
        );
    }

    /// Acceptance: the pinned-branch resume clone also checks out submodules,
    /// not just the fresh-pickup path.
    #[test]
    fn a_pinned_resume_clone_checks_out_its_submodule() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_submodule(tmp.path());
        let clone = tmp.path().join("clone");
        shallow_clone(&url, Some("main"), &clone, 50).unwrap();
        assert_eq!(
            std::fs::read_to_string(clone.join("sub").join("s.txt")).unwrap(),
            "s"
        );
    }

    /// Acceptance: a repo with no `.gitmodules` runs no submodule command and
    /// behaves exactly as before - the existing unpinned-clone coverage above
    /// (e.g. `an_unpinned_clone_is_unaffected`) already clones a repo with no
    /// submodules successfully; this asserts the negative directly; no `sub/`
    /// directory and no `.gitmodules` file ever appear.
    #[test]
    fn a_clone_with_no_gitmodules_runs_no_submodule_command() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_default_branch(tmp.path(), "main");
        let clone = tmp.path().join("clone");
        shallow_clone(&url, None, &clone, 50).unwrap();
        assert!(!clone.join(".gitmodules").exists());
        assert!(!clone.join("sub").exists());
    }

    /// Acceptance: a submodule fetch failure fails the clone with the
    /// redacted git stderr, never a silent half-populated tree. Removing the
    /// submodule's origin after it is committed into `.gitmodules` leaves the
    /// pointer unreachable, so `submodule update --init` fails.
    #[test]
    fn a_submodule_fetch_failure_fails_the_clone() {
        let tmp = tempfile::tempdir().unwrap();
        let url = origin_with_submodule(tmp.path());
        std::fs::remove_dir_all(tmp.path().join("sub-origin")).unwrap();
        let clone = tmp.path().join("clone");
        let err = shallow_clone(&url, None, &clone, 50)
            .unwrap_err()
            .to_string();
        assert!(err.contains("git submodule update exited"), "{err}");
        assert!(
            clone.join("sub").read_dir().unwrap().next().is_none(),
            "a failed submodule fetch must not leave a populated submodule directory"
        );
    }

    // ---- post-clone credential scrub -------------

    /// `file://user:pass@localhost/abs/path` round-trips through git exactly
    /// like a real HTTPS forge URL with userinfo: git persists it verbatim in
    /// `remote.origin.url`, so it is a faithful fixture for "a clone from a
    /// URL carrying fake userinfo" without a real network credential.
    fn with_fake_userinfo(url: &str) -> String {
        let path = url.strip_prefix("file://").expect("file:// url");
        format!("file://user:pass@localhost{path}")
    }

    /// Every URL `git config --list` can report for `workdir` (and
    /// recursively for every submodule), for asserting none carries userinfo.
    fn all_configured_urls(workdir: &std::path::Path) -> Vec<String> {
        let mut urls = Vec::new();
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(workdir)
            .arg("config")
            .arg("--get-regexp")
            .arg(r"\.url$")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        for line in stdout.lines() {
            if let Some((_, value)) = line.split_once(' ') {
                urls.push(value.to_string());
            }
        }
        if workdir.join(".gitmodules").is_file() {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(workdir)
                .arg("config")
                .arg("--file")
                .arg(".gitmodules")
                .arg("--get-regexp")
                .arg(r"^submodule\..*\.path$")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            for line in stdout.lines() {
                if let Some((_, path)) = line.split_once(' ') {
                    let sub = workdir.join(path.trim());
                    if sub.join(".git").exists() {
                        urls.extend(all_configured_urls(&sub));
                    }
                }
            }
        }
        urls
    }

    /// Acceptance: after a plain (unpinned) clone from a userinfo-bearing URL,
    /// `origin` and every other configured URL in the workdir carry none.
    #[test]
    fn a_plain_clone_strips_userinfo_from_origin() {
        let tmp = tempfile::tempdir().unwrap();
        let url = with_fake_userinfo(&origin_with_default_branch(tmp.path(), "main"));
        let clone = tmp.path().join("clone");
        shallow_clone(&url, None, &clone, 50).unwrap();
        for configured in all_configured_urls(&clone) {
            assert!(
                !configured.contains("user:pass@"),
                "userinfo survived in a configured URL: {configured}"
            );
        }
    }

    /// Acceptance: a pinned-branch clone (which also runs
    /// `restore_default_branch_refs`, fetching and setting `origin/HEAD`
    /// before the scrub) ends up with the same unauthenticated `origin`.
    #[test]
    fn a_pinned_clone_strips_userinfo_from_origin() {
        let tmp = tempfile::tempdir().unwrap();
        let url = with_fake_userinfo(&origin_with_default_branch(tmp.path(), "main"));
        let clone = tmp.path().join("clone");
        shallow_clone(&url, Some("feat"), &clone, 50).unwrap();
        for configured in all_configured_urls(&clone) {
            assert!(
                !configured.contains("user:pass@"),
                "userinfo survived in a configured URL: {configured}"
            );
        }
    }

    /// Acceptance: a clone with a submodule added via a RELATIVE URL (the
    /// common `git submodule add ../name` shape) resolves that relative URL
    /// against the token-bearing `origin` at clone time, so the resolved
    /// `submodule.<name>.url` AND the submodule's own `remote.origin.url`
    /// both carry the credential until the scrub runs.
    fn origin_with_relative_submodule(base: &std::path::Path) -> String {
        let git_in = |dir: &std::path::Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        let sub = base.join("sub-origin");
        std::fs::create_dir_all(&sub).unwrap();
        git_in(&sub, &["init", "-q", "-b", "main"]);
        git_in(&sub, &["config", "user.name", "t"]);
        git_in(&sub, &["config", "user.email", "t@t.example"]);
        std::fs::write(sub.join("s.txt"), "s").unwrap();
        git_in(&sub, &["add", "."]);
        git_in(&sub, &["commit", "-q", "-m", "sub init"]);

        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_in(&origin, &["init", "-q", "-b", "main"]);
        git_in(&origin, &["config", "user.name", "t"]);
        git_in(&origin, &["config", "user.email", "t@t.example"]);
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-q", "-m", "init"]);
        // A relative URL resolves against the superproject's own `origin`,
        // which is what later carries the userinfo this test cares about.
        git_in(
            &origin,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                "../sub-origin",
                "sub",
            ],
        );
        git_in(&origin, &["commit", "-q", "-m", "add submodule"]);
        format!("file://{}", origin.display())
    }

    #[test]
    fn a_clone_with_a_relative_url_submodule_strips_userinfo_everywhere() {
        let tmp = tempfile::tempdir().unwrap();
        let url = with_fake_userinfo(&origin_with_relative_submodule(tmp.path()));
        let clone = tmp.path().join("clone");
        shallow_clone(&url, None, &clone, 50).unwrap();
        assert_eq!(
            std::fs::read_to_string(clone.join("sub").join("s.txt")).unwrap(),
            "s",
            "the submodule must still be checked out"
        );
        let configured = all_configured_urls(&clone);
        assert!(
            !configured.is_empty(),
            "the fixture must actually configure URLs for this assertion to mean anything"
        );
        for url in &configured {
            assert!(
                !url.contains("user:pass@"),
                "userinfo survived in a configured URL: {url}"
            );
        }
    }

    /// Acceptance: a failure rewriting a remote URL fails the clone through
    /// the same redacted [`git_error`] path as every other git failure here,
    /// never a half-scrubbed workdir behind a warning. A read-only
    /// `.git/config` makes `git remote set-url` fail with a permission error.
    #[test]
    fn a_remote_url_rewrite_failure_fails_the_clone() {
        let tmp = tempfile::tempdir().unwrap();
        let url = with_fake_userinfo(&origin_with_default_branch(tmp.path(), "main"));
        let clone = tmp.path().join("clone");
        std::process::Command::new("git")
            .arg("clone")
            .arg("-q")
            .arg(&url)
            .arg(&clone)
            .output()
            .unwrap();
        // A pre-existing `config.lock` makes `git remote set-url` fail with
        // "Unable to create ... File exists", with no permission bit involved
        // (CLAUDE-255 bars `set_mode` from a test; this needs none).
        let lock = clone.join(".git").join("config.lock");
        std::fs::write(&lock, b"").unwrap();
        let result = scrub_workdir_credentials(&url, &clone);
        std::fs::remove_file(&lock).unwrap();
        let err = result.unwrap_err().to_string();
        assert!(err.contains("git remote set-url exited"), "{err}");
        assert!(
            !err.contains("user:pass"),
            "the credential must stay redacted: {err}"
        );
    }

    #[test]
    fn ls_remote_failure_reports_gits_own_reason() {
        let tmp = tempfile::tempdir().unwrap();
        let err = restore_default_branch_refs(
            "/nonexistent/repo.git",
            "feat",
            &tmp.path().join("clone"),
            50,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("git ls-remote exited"), "{err}");
    }

    #[test]
    fn a_remote_with_no_head_symref_errors_rather_than_assuming_main() {
        // An empty bare repo has no HEAD to report: `ls-remote --symref`
        // succeeds with no `ref:` line, and that must never be read as "main".
        let tmp = tempfile::tempdir().unwrap();
        let origin = tmp.path().join("origin");
        std::process::Command::new("git")
            .arg("init")
            .arg("-q")
            .arg("--bare")
            .arg(&origin)
            .output()
            .unwrap();
        let url = format!("file://{}", origin.display());
        let err = restore_default_branch_refs(&url, "feat", &tmp.path().join("clone"), 50)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no resolvable default branch"),
            "must never guess main: {err}"
        );
    }

    /// A GitHub-hosted source's log fetch must go through `GhForgeReads`
    /// (shelling out to `gh`), never `FjForgeReads` (`fj`): the `fj` stub has
    /// no canned response at all, so any `fj` call would panic the test via
    /// `StubRunner::lookup`'s "no canned response" error surfacing as a
    /// `Failed` outcome instead of `Fetched`.
    #[test]
    fn host_routed_forge_reads_dispatches_a_github_host_to_gh_not_fj() {
        let gh_runner = StubRunner::new()
            .ok(
                "gh",
                &[
                    "api",
                    "--hostname",
                    "github.com",
                    "repos/octo/cat/actions/runs?per_page=100&page=1",
                ],
                r#"{"workflow_runs":[{"id":999,"run_number":42,"path":".github/workflows/ci.yml","head_branch":"main","status":"completed","conclusion":"failure","html_url":"https://github.com/octo/cat/actions/runs/999"}]}"#,
            )
            .ok(
                "gh",
                &[
                    "api",
                    "--hostname",
                    "github.com",
                    "repos/octo/cat/actions/runs/999/jobs?per_page=100",
                ],
                r#"{"jobs":[{"id":1,"name":"test","status":"completed","conclusion":"failure"}]}"#,
            )
            .ok(
                "gh",
                &[
                    "api",
                    "--hostname",
                    "github.com",
                    "repos/octo/cat/actions/jobs/1/logs",
                ],
                "boom\n",
            );
        let fj_runner = StubRunner::new();
        let logs = HostRoutedForgeReads::new(
            Arc::new(crate::provider::FjForgeReads::new(Arc::new(fj_runner))),
            Arc::new(crate::provider::GhForgeReads::new(Arc::new(gh_runner))),
            Vec::new(),
        );
        let out = logs.run_logs("github.com", "octo/cat", 42, Some("tok"));
        match out {
            crate::forge_log::RunLogsOutcome::Fetched(jobs) => {
                assert_eq!(jobs.len(), 1);
                assert_eq!(jobs[0].status, "failure");
                assert_eq!(jobs[0].log, "boom\n");
            }
            other => panic!("expected Fetched, got {other:?}"),
        }
    }
}
