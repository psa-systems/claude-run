//! Provider abstraction for the external services native handlers talk to
//! (CLAUDE-31, part of the native-action epic CLAUDE-25).
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
//! [`crate::snippet`]). The async native handler (CLAUDE-32) wraps a backend
//! call in `tokio::task::spawn_blocking`.

pub mod audit;
pub mod cli;
pub mod github;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub use audit::{AuditedTracker, IssueWriteAudit, DEFAULT_MAX_ISSUES_PER_RUN};
pub use cli::{CliBackend, CmdOutput, CommandRunner, FjForgeReads, ProcessRunner};
pub use github::{is_github_host, GithubForge, HttpMcpTransport, McpTransport};

/// One project-to-repo binding flattened from a YouTrack project's enabled VCS
/// integration, the row of the inverse routing map (CLAUDE-86). Each enabled
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
    /// merge conflicts (CLAUDE-303).
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

/// The mergeability snapshot of a PR as `fj pr status --json` reports it
/// (CLAUDE-126). `state` is the combined CI status (`"success"` / `"failure"` /
/// `"pending"`); `mergeable` is Forgejo's verdict that the PR has no conflict,
/// is not still being checked, and is not a WIP PR - it says nothing about
/// whether the branch is up to date with its base (CLAUDE-309: that used to be
/// the assumption here, and it is wrong - Forgejo's `mergeable` ignores being
/// behind). `behind_base` answers that separately: `Some(true)` /
/// `Some(false)` when both Forgejo's `merge_base` and `base.sha` are known,
/// `None` when either is missing or the PR is merged, meaning unknown and
/// never collapsed to `false` (FJ-99). Only the fields automerge consumes are
/// modelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrStatus {
    /// Combined CI status: `"success"` / `"failure"` / `"pending"`.
    pub state: String,
    /// Whether Forgejo will merge the PR as-is: no conflict, not still being
    /// checked, not WIP. Does not reflect whether the branch is behind base.
    pub mergeable: bool,
    /// Whether the PR's head is behind its base. `None` means unknown (an
    /// `fj` predating FJ-99, or a value Forgejo has not computed yet).
    #[serde(default)]
    pub behind_base: Option<bool>,
}

/// A pull request opened by a [`Forge`] backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    /// PR number from the forge CLI's JSON envelope, when present (CLAUDE-136).
    pub number: Option<u64>,
    /// PR web URL as the forge CLI reported it (the `html_url` field of
    /// `fj pr create --json`).
    pub url: String,
    /// CLAUDE-275: the server-side head branch this PR was opened from, when the
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
/// `links[].issues[]` shape of `yt issue inspect --json` (YT-40 / CLAUDE-161).
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

/// CLAUDE-189: an issue's body plus its comment texts, from
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
    /// optional assignee-on-pickup write (CLAUDE-131).
    fn set_assignee(&self, key: &str, login: &str) -> Result<()>;

    /// Add a comment to an issue.
    fn comment(&self, key: &str, body: &str) -> Result<()>;

    /// The issue's directed links, flattened one row per linked target, with each
    /// link's resolved `direction` / `verb` and each target's nullable `resolved`
    /// timestamp (YT-40). Backs the pre-clone dependency gate (CLAUDE-161), which
    /// blocks a queued issue on any unmet dependency before it is cloned or handed
    /// to claude.
    fn issue_links(&self, key: &str) -> Result<Vec<IssueLink>>;

    /// CLAUDE-189: read an issue's description and comment texts via
    /// `yt issue inspect --json`.
    fn inspect_issue(&self, key: &str) -> Result<IssueInspect>;

    /// Resolve a project's enabled VCS integration to a clonable repo. Errors
    /// [`Error::NoVcsIntegration`] when the project has none.
    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo>;

    /// Build the inverse VCS routing map (repo -> project, CLAUDE-86): list every
    /// non-archived project and flatten each one's enabled VCS processors into
    /// `(server_url, path, project_key)` rows. A single project's `vcs` lookup
    /// failing is logged and skipped, not propagated, so one broken project never
    /// disables routing. Backs mint routing without a hand-kept repo map.
    fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>>;

    /// Create a new issue in `project` (short name, e.g. `CLAUDE`) with `summary`
    /// and a Markdown `description`, optionally pre-assigning `assignee` (a login).
    /// Returns the new issue's `idReadable` (e.g. `CLAUDE-47`). Backs the
    /// triage-driven issue mint (CLAUDE-46).
    ///
    /// CLAUDE-234: a run reaches this through [`AuditedTracker`], which spends
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
    /// redacted failing-job log to a minted issue (CLAUDE-46).
    fn attach_file(&self, key: &str, file: &std::path::Path) -> Result<()>;

    /// CLAUDE-218: link `key` to `target` with `link_type`, read source-first
    /// (`("CLAUDE-99", "subtask of", "CLAUDE-1")` makes CLAUDE-99 a subtask of
    /// CLAUDE-1). Backs hanging a filed report off its nominated parent.
    fn link_issue(&self, key: &str, link_type: &str, target: &str) -> Result<()> {
        let _ = (key, link_type, target);
        Err(Error::Other(anyhow::anyhow!(
            "link_issue not supported by this tracker backend"
        )))
    }

    /// CLAUDE-216: verify the tracker token and return the authenticated login
    /// (`yt auth verify`, GET /api/users/me). The single read-only answer to "is
    /// this token still valid". Never returns the token itself.
    fn auth_verify(&self) -> Result<String> {
        Err(Error::Other(anyhow::anyhow!(
            "auth_verify not supported by this tracker backend"
        )))
    }

    /// CLAUDE-216: run an arbitrary issue query, bounded to `top` results. Backs
    /// the access check's "does the action's query execute under this
    /// credential" probe, where only the exit status matters and the full result
    /// set would be wasted work.
    fn list_query_top(&self, query: &str, top: u32) -> Result<Vec<serde_json::Value>> {
        let _ = (query, top);
        Err(Error::Other(anyhow::anyhow!(
            "list_query_top not supported by this tracker backend"
        )))
    }

    /// CLAUDE-216: read a project's metadata by short name (`yt project show`),
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

/// CLAUDE-207: render a failed [`Forge::repo_access`] probe as an error an
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

/// The Forgejo-side operations native handlers need.
pub trait Forge: Send + Sync {
    /// Open a PR from the local commits in `workdir` against `host`. `strategy`
    /// (CLAUDE-103) selects branch-backed (push a real server branch, open via
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

    /// CLAUDE-206: drain the operator-visible notices this backend accumulated
    /// while serving the job, e.g. an open-PR lookup that failed and was
    /// degraded to a create. The caller persists them as `run_events` so they
    /// show on the action page, not only in the daemon journal. Backends with
    /// nothing to report keep the default (empty).
    fn take_notices(&self) -> Vec<String> {
        Vec::new()
    }

    /// CLAUDE-275: record an operator-visible notice from OUTSIDE this backend
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

    /// CLAUDE-194: verify the FORGE identity can work `repo` (`owner/name`) on
    /// `host`. `git push` authenticates from the host's own git credentials, so a
    /// repo the forge token cannot see still clones and pushes and only fails at
    /// `pr create` time, after a whole Claude run is paid for. This is the
    /// preflight that catches it first; the error names the host and `owner/repo`.
    ///
    /// CLAUDE-207: the probe covers BOTH capability categories the PR path uses,
    /// a repository read and a pull-request read, because a token carrying only
    /// the repository category passes a repository-only probe and then 403s on
    /// the first open-PR lookup. Implementations report which probe failed and
    /// the scope label the operator has to add (see [`probe_denied`]).
    fn repo_access(&self, host: &str, repo: &str) -> Result<()>;

    /// CLAUDE-216: the forge identity this backend authenticates as on `host`
    /// (`fj whoami`, or the GitHub identity read). Read-only, and it never
    /// returns the token. Backs the access check's "which account is this?" row.
    fn identity(&self, host: &str) -> Result<String> {
        let _ = host;
        Err(Error::Other(anyhow::anyhow!(
            "identity not supported by this forge backend"
        )))
    }

    /// List open PRs for `repo` (`owner/name`) on `host`. Backs the CI-fix scan
    /// (`fj pr search --repo <repo> --state open --json`, FJ-1).
    fn list_open_prs(&self, host: &str, repo: &str) -> Result<Vec<ForgePullRequest>>;

    /// Combined CI status of PR `pr_number` in `repo` on `host`, e.g.
    /// `"success"` / `"failure"` / `"pending"` (`fj pr status <repo>#<n> --json`,
    /// FJ-2). The PR is addressed as `owner/repo#n` so no local checkout is
    /// needed to resolve the repo.
    fn pr_ci_state(&self, host: &str, repo: &str, pr_number: u64) -> Result<String>;

    /// CLAUDE-220: the commenter's EFFECTIVE repository permission on `repo` at
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

    /// CLAUDE-221: post a comment on plain ISSUE `issue_number` in `repo` on
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

    /// CLAUDE-126: combined CI status plus mergeability of PR `pr_number` in
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

    /// CLAUDE-126: merge PR `pr_number` in `repo` on `host` as a merge-commit
    /// (never squash/rebase, so the single commit's bare `#<KEY>` reference
    /// survives in history). The default errors for backends without automerge.
    fn merge_pr(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "merge_pr (automerge) not supported by this forge backend"
        )))
    }

    /// CLAUDE-309: bring PR `pr_number` in `repo` on `host` up to date with its
    /// base through Forgejo's Update-branch endpoint (`fj pr update <repo>#<n>
    /// --style merge`, FJ-99), retriggering CI. `--style merge` matches
    /// CLAUDE-126's merge-commit policy and never rewrites the agent's branch.
    /// The default errors for backends without automerge.
    fn update_pr_branch(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let _ = (host, repo, pr_number);
        Err(Error::Other(anyhow::anyhow!(
            "update_pr_branch (automerge) not supported by this forge backend"
        )))
    }

    /// CLAUDE-149: add Forgejo labels to PR `pr_number` in `repo` on `host`
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

    /// CLAUDE-160: the owning issue keys found in PR `pr_number`'s body
    /// (`fj pr view <repo>#<n> --json`) - only the bare `#<KEY>-N` references
    /// alone on their own line (the commit convention's trailer), never IDs
    /// cross-linked in prose. Used to flag the source issue Needs Review when the
    /// per-PR auto-fix ceiling is reached; scoping to the owning reference keeps
    /// the flag off related / depends / relates mentions. The default returns an
    /// empty list (no linkage), so a backend without PR-body access degrades to
    /// the PR-side flag only.
    fn pr_referenced_issues(&self, host: &str, repo: &str, pr_number: u64) -> Result<Vec<String>> {
        let _ = (host, repo, pr_number);
        Ok(Vec::new())
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
/// (CLAUDE-50) walks `[Mcp, Gh]`: prefer the remote MCP, fall back to the `gh`
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

/// Shallow-clone `url` into `workdir`, pinning `branch` when given.
///
/// CLAUDE-252: captures git's output instead of letting it inherit the daemon's
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
    // CLAUDE-306: a detached auto-maintenance repack would race the push and the
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
    Ok(())
}

/// CLAUDE-311: a pinned-branch clone (`--branch` implies `--single-branch`)
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
/// CLAUDE-252: every git call site used to report only an exit code, so `git
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
/// [`CliBackend`]s with injected credentials (CLAUDE-26) use this to obtain a
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

#[cfg(test)]
mod tests {
    use super::*;
    use cli::testutil::StubRunner;

    /// CLAUDE-252: a failed clone must say WHY. `git clone exited 128` alone is
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

    /// CLAUDE-306: the key must land in the clone's own config, so every later
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

    // ---- CLAUDE-311: a pinned clone still carries origin/HEAD -------------

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
}
