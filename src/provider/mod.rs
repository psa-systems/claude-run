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

pub mod cli;
pub mod github;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub use cli::{CliBackend, CmdOutput, CommandRunner, ProcessRunner};
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
/// `"pending"`); `mergeable` is the forge's verdict on whether the PR can be
/// merged right now (false when behind a base that requires an up-to-date
/// branch). Only the fields automerge consumes are modelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrStatus {
    /// Combined CI status: `"success"` / `"failure"` / `"pending"`.
    pub state: String,
    /// Whether the forge will merge the PR as-is (false when behind base).
    pub mergeable: bool,
}

/// A pull request opened by a [`Forge`] backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    /// PR number from the forge CLI's JSON envelope, when present (CLAUDE-136).
    pub number: Option<u64>,
    /// PR web URL as the forge CLI reported it (the `html_url` field of
    /// `fj pr create --json`).
    pub url: String,
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

    /// Resolve an authenticated clone URL for `repo` on `host` using the forge
    /// CLI's stored credentials (no token in YAML or env). `repo` is the
    /// `owner/name` path.
    fn clone_url(&self, host: &str, repo: &str) -> Result<String>;

    /// List open PRs for `repo` (`owner/name`) on `host`. Backs the CI-fix scan
    /// (`fj pr search --repo <repo> --state open --json`, FJ-1).
    fn list_open_prs(&self, host: &str, repo: &str) -> Result<Vec<ForgePullRequest>>;

    /// Combined CI status of PR `pr_number` in `repo` on `host`, e.g.
    /// `"success"` / `"failure"` / `"pending"` (`fj pr status <repo>#<n> --json`,
    /// FJ-2). The PR is addressed as `owner/repo#n` so no local checkout is
    /// needed to resolve the repo.
    fn pr_ci_state(&self, host: &str, repo: &str, pr_number: u64) -> Result<String>;

    /// Post a comment on PR `pr_number` in `repo` on `host`, body read from
    /// `body_file` (`fj pr comment <repo>#<n> --body-file <path>`).
    fn pr_comment(
        &self,
        host: &str,
        repo: &str,
        pr_number: u64,
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

    /// CLAUDE-126: bring the PR branch checked out in `workdir` up to date with
    /// its base (the Forgejo "Update Branch" step), retriggering CI. `fj`
    /// exposes no `pr update-branch` subcommand, so the Forgejo backend performs
    /// it with the git CLI (merge the base into the head and push) rather than
    /// hitting the REST API directly. The default errors for backends without
    /// automerge.
    fn update_branch(&self, workdir: &std::path::Path) -> Result<()> {
        let _ = workdir;
        Err(Error::Other(anyhow::anyhow!(
            "update_branch (automerge) not supported by this forge backend"
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
}
