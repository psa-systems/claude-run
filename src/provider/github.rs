//! GitHub `Forge` backend.
//!
//! The native YouTrack handler opens PRs through the [`Forge`](super::Forge)
//! trait. The CLI backend ([`super::CliBackend`]) speaks only the Forgejo/Gitea
//! API via `fj`, so a GitHub-hosted repo cannot open a PR through it. This
//! module adds [`GithubForge`], selected automatically when a job's resolved
//! `repo_host` is a GitHub host ([`is_github_host`]); every other host keeps
//! using `fj` unchanged.
//!
//! Unlike `fj -aA` (AGit + autofill, no separate push), GitHub has no AGit
//! equivalent, so [`GithubForge::create_pr`] pushes the working branch first,
//! then opens the PR. PR creation walks [`super::GITHUB_PRIORITY`] (`[Mcp, Gh]`):
//! it prefers GitHub's remote MCP (`create_pull_request`) and falls back to the
//! `gh` CLI when the MCP endpoint is unreachable. With neither usable it returns
//! [`Error::NoBackend`] listing both attempts.
//!
//! Credentials follow the `token_ref` pattern: an injected GitHub PAT
//! (`repo` + `pull_request` scope) is used as the MCP bearer, the `gh` child's
//! `GH_TOKEN`, and the authenticated push URL. When unset, the push and `gh`
//! fall back to host git / `gh` auth.
//!
//! The HTTP/TLS stack is `reqwest` + rustls (no second stack, keeping the
//! static musl build OpenSSL-free). The `Forge` trait is
//! synchronous, so [`HttpMcpTransport`] runs the blocking request on a dedicated
//! thread to stay clear of the handler's async runtime.

use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use super::cli::{CliBackend, CmdOutput, CommandRunner};
use super::{
    Backend, BackendKind, Forge, ForgePullRequest, IssueInspect, IssueLink, IssueTracker,
    PullRequest, VcsMapEntry, VcsRepo, GITHUB_PRIORITY,
};
use crate::error::{Error, Result};

/// The canonical public GitHub host.
pub const GITHUB_COM: &str = "github.com";

/// True when `host` (a bare host, no scheme) is GitHub: `github.com` or any host
/// in `extra` (configured GitHub Enterprise hosts). Case-insensitive.
pub fn is_github_host(host: &str, extra: &[String]) -> bool {
    host.eq_ignore_ascii_case(GITHUB_COM) || extra.iter().any(|h| h.eq_ignore_ascii_case(host))
}

/// Run `gh auth token --hostname <host>`, returning the trimmed token or a
/// formatted error naming the failure. Shared by [`GithubForge`] (its own
/// PR/comment push fallback) and [`GhForgeReads`] (its CI-failure log reads),
/// since both fall back to the daemon's own `gh` login the same way.
fn gh_auth_token(
    runner: &dyn CommandRunner,
    gh: &str,
    host: &str,
) -> std::result::Result<String, String> {
    let out = runner
        .run(gh, &["auth", "token", "--hostname", host])
        .map_err(|e| format!("gh auth token --hostname {host} failed: {e}"))?;
    if !out.ok {
        return Err(format!(
            "gh auth token --hostname {host} exited {}: {}",
            out.exit_code,
            out.stderr.trim()
        ));
    }
    let token = out.stdout.trim().to_string();
    if token.is_empty() {
        return Err(format!("gh auth token --hostname {host} returned no token"));
    }
    Ok(token)
}

/// Transport for the embedded MCP client. Seam so the synchronous `Forge` path
/// can be unit-tested with a stub instead of reaching the network.
pub trait McpTransport: Send + Sync {
    /// Probe the endpoint with an `initialize` request. `Ok(())` means reachable;
    /// any error means unreachable, so [`GithubForge`] falls through to `gh`.
    fn initialize(&self, endpoint: &str, token: Option<&str>) -> Result<()>;

    /// Call tool `name` with `arguments`, returning the JSON-RPC `result` value.
    fn call_tool(
        &self,
        endpoint: &str,
        token: Option<&str>,
        name: &str,
        arguments: Value,
    ) -> Result<Value>;
}

/// Real [`McpTransport`]: JSON-RPC over HTTPS with `reqwest` + rustls.
#[derive(Debug, Default, Clone)]
pub struct HttpMcpTransport;

impl HttpMcpTransport {
    pub fn new() -> Self {
        Self
    }

    /// POST a JSON-RPC request and return the parsed envelope. Runs the blocking
    /// reqwest call on a dedicated thread so it never blocks the async runtime
    /// the handler runs under.
    fn rpc(
        &self,
        endpoint: &str,
        token: Option<&str>,
        method: &str,
        params: Value,
    ) -> Result<Value> {
        let endpoint = endpoint.to_string();
        let token = token.map(str::to_string);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        })
        .to_string();
        let method = method.to_string();

        let payload = std::thread::spawn(move || -> Result<Value> {
            let client = reqwest::blocking::Client::builder()
                .build()
                .map_err(|e| Error::Other(anyhow::anyhow!("mcp http client: {e}")))?;
            let mut req = client
                .post(&endpoint)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .body(body);
            if let Some(t) = &token {
                req = req.bearer_auth(t);
            }
            let resp = req
                .send()
                .map_err(|e| Error::Other(anyhow::anyhow!("mcp request to {endpoint}: {e}")))?;
            let status = resp.status();
            let text = resp
                .text()
                .map_err(|e| Error::Other(anyhow::anyhow!("mcp response body: {e}")))?;
            if !status.is_success() {
                return Err(Error::Other(anyhow::anyhow!(
                    "mcp endpoint {endpoint} returned {status}: {}",
                    text.trim()
                )));
            }
            extract_json(&text)
        })
        .join()
        .map_err(|_| Error::Other(anyhow::anyhow!("mcp transport thread panicked")))??;

        if let Some(err) = payload.get("error") {
            return Err(Error::Other(anyhow::anyhow!("mcp {method} error: {err}")));
        }
        Ok(payload.get("result").cloned().unwrap_or(Value::Null))
    }
}

impl McpTransport for HttpMcpTransport {
    fn initialize(&self, endpoint: &str, token: Option<&str>) -> Result<()> {
        let params = json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "claude-run", "version": env!("CARGO_PKG_VERSION") },
        });
        self.rpc(endpoint, token, "initialize", params)?;
        Ok(())
    }

    fn call_tool(
        &self,
        endpoint: &str,
        token: Option<&str>,
        name: &str,
        arguments: Value,
    ) -> Result<Value> {
        self.rpc(
            endpoint,
            token,
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        )
    }
}

/// Extract a JSON object from an MCP HTTP response, which may be a plain JSON
/// body or a `text/event-stream` (SSE) frame carrying the JSON in `data:` lines.
fn extract_json(text: &str) -> Result<Value> {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed)
            .map_err(|e| Error::Other(anyhow::anyhow!("mcp response not JSON: {e}")));
    }
    // SSE: keep the last `data:` frame that parses as a JSON object.
    let mut last = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            let data = rest.trim();
            if data.is_empty() || data == "[DONE]" {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<Value>(data) {
                last = Some(v);
            }
        }
    }
    last.ok_or_else(|| Error::Other(anyhow::anyhow!("no JSON object in MCP response")))
}

/// GitHub `Forge`: pushes the working branch then opens a PR, preferring the
/// remote MCP and falling back to `gh`.
pub struct GithubForge {
    runner: Arc<dyn CommandRunner>,
    transport: Arc<dyn McpTransport>,
    /// Remote MCP endpoint (e.g. `https://api.githubcopilot.com/mcp/`).
    endpoint: String,
    /// Injected GitHub PAT. `None` falls back to host git / `gh` auth.
    token: Option<String>,
    /// Backend priority for opening the PR. Defaults to [`GITHUB_PRIORITY`].
    priority: Vec<BackendKind>,
    git: String,
    gh: String,
    /// Lazy `gh auth token` fallback result, cached after the first lookup.
    /// Only consulted when `token` is `None`.
    resolved_token: std::sync::Mutex<Option<std::result::Result<String, String>>>,
}

impl GithubForge {
    /// Build a GitHub forge with the real HTTP transport.
    pub fn new(runner: Arc<dyn CommandRunner>, endpoint: String, token: Option<String>) -> Self {
        Self::with_transport(runner, Arc::new(HttpMcpTransport::new()), endpoint, token)
    }

    /// Build a GitHub forge with an injectable [`McpTransport`] (tests).
    pub fn with_transport(
        runner: Arc<dyn CommandRunner>,
        transport: Arc<dyn McpTransport>,
        endpoint: String,
        token: Option<String>,
    ) -> Self {
        Self {
            runner,
            transport,
            endpoint,
            token,
            priority: GITHUB_PRIORITY.to_vec(),
            git: "git".to_string(),
            gh: "gh".to_string(),
            resolved_token: std::sync::Mutex::new(None),
        }
    }

    /// Override the backend priority (tests).
    pub fn with_priority(mut self, priority: Vec<BackendKind>) -> Self {
        self.priority = priority;
        self
    }

    /// Resolve the token to authenticate as. Returns `self.token` directly
    /// when set (never invoking `gh auth token`); otherwise falls back to the
    /// daemon's own `gh` login for `host` via `gh auth token`, caching the
    /// result (success or failure) after the first lookup.
    fn effective_token(&self, host: &str) -> std::result::Result<String, String> {
        if let Some(token) = &self.token {
            return Ok(token.clone());
        }
        let mut cached = self.resolved_token.lock().unwrap();
        if let Some(result) = cached.as_ref() {
            return result.clone();
        }
        let result = gh_auth_token(self.runner.as_ref(), &self.gh, host);
        *cached = Some(result.clone());
        result
    }

    /// Run `git args...` in `workdir`, returning trimmed stdout or a typed error.
    fn git(&self, workdir: &Path, args: &[&str]) -> Result<String> {
        let out = self.runner.run_in(workdir, &self.git, args)?;
        if out.ok {
            Ok(out.stdout.trim().to_string())
        } else {
            Err(Error::CliFailed {
                binary: self.git.clone(),
                args: args.join(" "),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            })
        }
    }

    /// Resolve the base branch from `origin/HEAD`, falling back to `main` when
    /// the symbolic ref is unset (a shallow clone may not carry it).
    fn detect_base(&self, workdir: &Path) -> String {
        match self.git(workdir, &["rev-parse", "--abbrev-ref", "origin/HEAD"]) {
            Ok(s) => match s.strip_prefix("origin/").filter(|b| !b.is_empty()) {
                Some(b) => b.to_string(),
                None => {
                    tracing::warn!(
                        origin_head = %s,
                        "origin/HEAD resolved to an empty branch name; falling back to main"
                    );
                    "main".to_string()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "could not resolve origin/HEAD; falling back to main");
                "main".to_string()
            }
        }
    }

    /// Push the working branch to the GitHub remote. With a resolvable token
    /// (the action's own, or the daemon's `gh` login), push to an
    /// authenticated HTTPS URL; otherwise fall back to `origin` (host git
    /// auth), warning with both misses named rather than failing silently.
    fn push(&self, workdir: &Path, host: &str, repo: &str, branch: &str) -> Result<()> {
        let refspec = format!("HEAD:{branch}");
        let out = match self.effective_token(host) {
            Ok(token) => {
                let url = format!("https://x-access-token:{token}@{host}/{repo}.git");
                self.runner
                    .run_in(workdir, &self.git, &["push", &url, &refspec])?
            }
            Err(e) => {
                tracing::warn!(
                    host,
                    repo,
                    "no github.token_ref on the action and {e}; falling back to origin"
                );
                self.runner
                    .run_in(workdir, &self.git, &["push", "origin", &refspec])?
            }
        };
        if out.ok {
            Ok(())
        } else {
            Err(Error::CliFailed {
                binary: self.git.clone(),
                args: format!("push <remote> {refspec}"),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            })
        }
    }

    /// Open the PR via the MCP `create_pull_request` tool.
    fn open_via_mcp(
        &self,
        repo: &str,
        branch: &str,
        base: &str,
        title: &str,
        body: &str,
    ) -> Result<PullRequest> {
        let (owner, name) = split_repo(repo)?;
        let result = self.transport.call_tool(
            &self.endpoint,
            self.token.as_deref(),
            "create_pull_request",
            json!({
                "owner": owner,
                "repo": name,
                "title": title,
                "head": branch,
                "base": base,
                "body": body,
            }),
        )?;
        parse_pr_from_mcp(&result)
    }

    /// Shared `gh <kind> comment <number> --repo <repo> --body-file <path>`
    /// runner backing [`Forge::pr_comment`] / [`Forge::issue_comment`]. `kind`
    /// is `"pr"` or `"issue"`; both subcommands take an identical shape.
    fn gh_comment(&self, repo: &str, kind: &str, number: u64, body_file: &Path) -> Result<()> {
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let number_str = number.to_string();
        let body_path = body_file.to_string_lossy().into_owned();
        let args = [
            kind,
            "comment",
            &number_str,
            "--repo",
            repo,
            "--body-file",
            &body_path,
        ];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(Error::CliFailed {
                binary: self.gh.clone(),
                args: args.join(" "),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(())
    }

    /// Open the PR via `gh pr create`, parsing the printed PR URL.
    fn open_via_gh(
        &self,
        workdir: &Path,
        branch: &str,
        base: &str,
        title: &str,
        body: &str,
    ) -> Result<PullRequest> {
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let args = [
            "pr", "create", "--head", branch, "--base", base, "--title", title, "--body", body,
        ];
        let out = self
            .runner
            .run_in_env(Some(workdir), &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(Error::CliFailed {
                binary: self.gh.clone(),
                args: "pr create".to_string(),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(parse_pr_from_gh(&out.stdout))
    }
}

impl Forge for GithubForge {
    fn create_pr(
        &self,
        workdir: &Path,
        host: &str,
        _strategy: crate::action::PrStrategy,
    ) -> Result<PullRequest> {
        // GitHub has no AGit equivalent: PRs are always branch-backed, so the
        // branch-strategy selector does not apply here and is ignored.
        // Push the branch before opening the PR.
        let repo = parse_owner_repo(&self.git(workdir, &["remote", "get-url", "origin"])?)
            .ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "cannot parse owner/repo from origin remote in {}",
                    workdir.display()
                ))
            })?;
        let branch = self.git(workdir, &["rev-parse", "--abbrev-ref", "HEAD"])?;
        let base = self.detect_base(workdir);
        let title = self.git(workdir, &["log", "-1", "--pretty=format:%s"])?;
        let body = self.git(workdir, &["log", "-1", "--pretty=format:%b"])?;

        self.push(workdir, host, &repo, &branch)?;

        // Walk [Mcp, Gh]: prefer the remote MCP, fall back to `gh`.
        let mut tried: Vec<String> = Vec::new();
        for kind in &self.priority {
            match kind {
                BackendKind::Mcp => {
                    match self
                        .transport
                        .initialize(&self.endpoint, self.token.as_deref())
                    {
                        // A create failure names the repo, the host, and the
                        // possible-permission cause; the push succeeding
                        // proves nothing about the API identity's access.
                        Ok(()) => {
                            return self
                                .open_via_mcp(&repo, &branch, &base, &title, &body)
                                .map(|mut pr| {
                                    // GitHub PRs are always branch-backed;
                                    // this is the branch just pushed above.
                                    pr.head_branch = Some(branch.clone());
                                    pr
                                })
                                .map_err(|e| pr_create_error(&repo, host, &branch, e));
                        }
                        Err(e) => tried.push(format!("mcp: {e}")),
                    }
                }
                BackendKind::Gh => match self.runner.run(&self.gh, &["--version"]) {
                    Ok(out) if out.ok => {
                        return self
                            .open_via_gh(workdir, &branch, &base, &title, &body)
                            .map(|mut pr| {
                                pr.head_branch = Some(branch.clone());
                                pr
                            })
                            .map_err(|e| pr_create_error(&repo, host, &branch, e))
                    }
                    Ok(out) => tried.push(format!("gh: --version exited {}", out.exit_code)),
                    Err(e) => tried.push(format!("gh: {e}")),
                },
                BackendKind::Cli => {}
            }
        }
        Err(Error::NoBackend(tried.join("; ")))
    }

    fn clone_url(&self, host: &str, repo: &str) -> Result<String> {
        match self.effective_token(host) {
            Ok(token) => Ok(format!("https://x-access-token:{token}@{host}/{repo}.git")),
            Err(e) => Err(Error::Other(anyhow::anyhow!(
                "no github.token_ref on the action to build an authenticated clone URL for \
                 {host}/{repo}, and {e}"
            ))),
        }
    }

    /// Two probes, run with the action's PAT as `GH_TOKEN` exactly as
    /// `open_via_gh` does, so they test the identity that will open the PR.
    /// The MCP transport exposes neither read, so this is
    /// `gh`-only; a host without `gh` reports that by name rather than silently
    /// passing.
    ///
    /// 1. `gh repo view <owner/repo> --json nameWithOwner` (classic `repo` scope
    ///    / fine-grained Metadata read).
    /// 2. `gh pr list --repo <owner/repo> --state open --limit 1 --json number`
    ///    (fine-grained `pull_request` read). A fine-grained PAT scopes pull
    ///    requests separately from repo metadata, so the repo read alone does not
    ///    prove the PAT can open a PR; probing both keeps the GitHub backend
    ///    symmetric with the Forgejo one.
    fn repo_access(&self, host: &str, repo: &str) -> Result<()> {
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let args = ["repo", "view", repo, "--json", "nameWithOwner"];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(super::probe_denied(
                "GitHub token",
                &self.gh,
                "repo view",
                "repo",
                host,
                repo,
                &out,
            ));
        }
        let args = [
            "pr", "list", "--repo", repo, "--state", "open", "--limit", "1", "--json", "number",
        ];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(super::probe_denied(
                "GitHub token",
                &self.gh,
                "pr list",
                "pull_request",
                host,
                repo,
                &out,
            ));
        }
        Ok(())
    }

    /// `gh api user --jq .login`, run with the action's PAT as `GH_TOKEN`
    /// so the row names the identity `repo_access` just probed with.
    fn identity(&self, _host: &str) -> Result<String> {
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let args = ["api", "user", "--jq", ".login"];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(Error::CliFailed {
                binary: self.gh.clone(),
                args: args.join(" "),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(out.stdout.trim().to_string())
    }

    // The remaining CI-fix-only operations are Forgejo-specific; `list_open_prs`
    // is the one exception, needed by the PR-resume lookup for every backend.
    // A GitHub repo never enters the Forgejo-only CI-fix scan (auto-route-by-host),
    // so the rest are unsupported on the GitHub backend; split the `Forge` trait
    // later if a GitHub CI-fix handler is wanted.
    //
    // `gh pr list --json ...`, run with the action's PAT as `GH_TOKEN`
    // exactly as `repo_access` does. `--limit 1000` bounds a single
    // page; exactly 1000 entries back means the listing may be truncated, so
    // that is reported as an error rather than answered from a partial list.
    fn list_open_prs(&self, _host: &str, repo: &str) -> Result<Vec<ForgePullRequest>> {
        const LIMIT: usize = 1000;
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let limit_str = LIMIT.to_string();
        let args = [
            "pr",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--limit",
            &limit_str,
            "--json",
            "number,title,headRefOid,headRefName,baseRefName,author,url",
        ];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(Error::CliFailed {
                binary: self.gh.clone(),
                args: "pr list --json".to_string(),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        let entries: Vec<GhPrEntry> =
            serde_json::from_str(&out.stdout).map_err(|source| Error::CliParse {
                binary: self.gh.clone(),
                context: "gh pr list --json".to_string(),
                source,
            })?;
        if entries.len() == LIMIT {
            return Err(Error::Other(anyhow::anyhow!(
                "gh pr list for `{repo}` returned exactly the {LIMIT}-entry limit; the open-PR \
                 listing may be truncated"
            )));
        }
        Ok(entries.into_iter().map(GhPrEntry::into_forge_pr).collect())
    }

    fn pr_ci_state(&self, _host: &str, _repo: &str, _pr_number: u64) -> Result<String> {
        Err(unsupported("pr_ci_state"))
    }

    /// `gh api repos/<repo>/collaborators/<username>/permission --jq
    /// .permission`, run with the action's PAT as `GH_TOKEN` exactly as
    /// `repo_access` does. GitHub's REST response's `permission` field is one
    /// of `admin` / `write` / `read` / `none` (GitHub has no separate `owner`
    /// level; `admin` already ranks highest against the comment
    /// listener's `allowed_permissions` check), returned verbatim like the
    /// Forgejo backend's own `permission` field.
    fn repo_permission(&self, _host: &str, repo: &str, username: &str) -> Result<String> {
        let envs: Vec<(&str, &str)> = match &self.token {
            Some(t) => vec![("GH_TOKEN", t.as_str())],
            None => Vec::new(),
        };
        let path = format!("repos/{repo}/collaborators/{username}/permission");
        let args = ["api", &path, "--jq", ".permission"];
        let out = self.runner.run_in_env(None, &self.gh, &args, &envs)?;
        if !out.ok {
            return Err(Error::CliFailed {
                binary: self.gh.clone(),
                args: args.join(" "),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(out.stdout.trim().to_string())
    }

    fn pr_comment(&self, _host: &str, repo: &str, pr_number: u64, body_file: &Path) -> Result<()> {
        self.gh_comment(repo, "pr", pr_number, body_file)
    }

    fn issue_comment(
        &self,
        _host: &str,
        repo: &str,
        issue_number: u64,
        body_file: &Path,
    ) -> Result<()> {
        self.gh_comment(repo, "issue", issue_number, body_file)
    }
}

/// Forward every [`IssueTracker`] operation to a fresh, cheap
/// [`CliBackend`] (default `yt`/`fj` binaries, no per-call state, so a new one
/// per call costs nothing). A YouTrack write needs no GitHub credential or
/// state, only ambient `yt` auth exactly like the Forgejo path; this is what
/// lets a GitHub-provider webhook source's `Box<dyn Backend>` mint and comment
/// on YouTrack issues while its `Forge` half (`pr_comment` / `issue_comment` /
/// `repo_permission` / `create_pr` / ...) talks to GitHub.
impl IssueTracker for GithubForge {
    fn list_queued(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        CliBackend::new(self.runner.clone()).list_queued(query)
    }

    fn get_field(&self, key: &str, field: &str) -> Result<String> {
        CliBackend::new(self.runner.clone()).get_field(key, field)
    }

    fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()> {
        CliBackend::new(self.runner.clone()).set_field(key, field, value)
    }

    fn set_state(&self, key: &str, state: &str) -> Result<()> {
        CliBackend::new(self.runner.clone()).set_state(key, state)
    }

    fn set_assignee(&self, key: &str, login: &str) -> Result<()> {
        CliBackend::new(self.runner.clone()).set_assignee(key, login)
    }

    fn comment(&self, key: &str, body: &str) -> Result<()> {
        CliBackend::new(self.runner.clone()).comment(key, body)
    }

    fn issue_links(&self, key: &str) -> Result<Vec<IssueLink>> {
        CliBackend::new(self.runner.clone()).issue_links(key)
    }

    fn inspect_issue(&self, key: &str) -> Result<IssueInspect> {
        CliBackend::new(self.runner.clone()).inspect_issue(key)
    }

    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo> {
        CliBackend::new(self.runner.clone()).resolve_vcs(project_key)
    }

    fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>> {
        CliBackend::new(self.runner.clone()).vcs_repo_map()
    }

    fn create_issue(
        &self,
        project: &str,
        summary: &str,
        description: &str,
        assignee: Option<&str>,
    ) -> Result<String> {
        CliBackend::new(self.runner.clone()).create_issue(project, summary, description, assignee)
    }

    fn attach_file(&self, key: &str, file: &Path) -> Result<()> {
        CliBackend::new(self.runner.clone()).attach_file(key, file)
    }

    fn link_issue(&self, key: &str, link_type: &str, target: &str) -> Result<()> {
        CliBackend::new(self.runner.clone()).link_issue(key, link_type, target)
    }

    fn auth_verify(&self) -> Result<String> {
        CliBackend::new(self.runner.clone()).auth_verify()
    }

    fn list_query_top(&self, query: &str, top: u32) -> Result<Vec<serde_json::Value>> {
        CliBackend::new(self.runner.clone()).list_query_top(query, top)
    }

    fn project_show(&self, project_key: &str) -> Result<String> {
        CliBackend::new(self.runner.clone()).project_show(project_key)
    }
}

impl Backend for GithubForge {
    fn name(&self) -> &'static str {
        "github"
    }
}

/// Restate a PR-create failure with the repo, the host, and the
/// permission hypothesis (the `fj` counterpart lives in `cli.rs`). The push
/// authenticates with the PAT-in-URL or host git auth, so a successful push does
/// not prove the API identity can see the repo.
fn pr_create_error(repo: &str, host: &str, branch: &str, e: Error) -> Error {
    Error::Other(anyhow::anyhow!(
        "creating the pull request for `{repo}` on {host} (head branch `{branch}`) failed: {e}. \
         The GitHub token may lack access to `{repo}`: a repo the token cannot see answers 404 \
         `not found` here even though the push succeeded."
    ))
}

fn unsupported(op: &str) -> Error {
    Error::Other(anyhow::anyhow!(
        "GitHub forge does not support `{op}` (Forgejo CI-fix only)"
    ))
}

/// Split `owner/name` into its parts.
fn split_repo(repo: &str) -> Result<(&str, &str)> {
    repo.split_once('/')
        .filter(|(o, n)| !o.is_empty() && !n.is_empty())
        .ok_or_else(|| Error::Other(anyhow::anyhow!("repo `{repo}` is not `owner/name`")))
}

/// Parse `owner/name` from a git remote URL, handling both HTTPS
/// (`https://github.com/owner/name.git`) and SSH (`git@github.com:owner/name.git`)
/// forms, with or without the `.git` suffix.
fn parse_owner_repo(url: &str) -> Option<String> {
    let url = url.trim();
    let tail = if let Some(rest) = url.strip_prefix("https://") {
        // `host/owner/name` -> drop the host segment.
        rest.split_once('/').map(|(_, t)| t)?
    } else if let Some(rest) = url.strip_prefix("http://") {
        rest.split_once('/').map(|(_, t)| t)?
    } else if let Some((_, rest)) = url.split_once('@') {
        // `git@host:owner/name` -> drop `host:`.
        rest.split_once(':').map(|(_, t)| t)?
    } else {
        url
    };
    let repo = tail.trim_end_matches('/').trim_end_matches(".git");
    if repo.split('/').filter(|s| !s.is_empty()).count() >= 2 {
        Some(repo.to_string())
    } else {
        None
    }
}

/// Parse a [`PullRequest`] from the MCP `create_pull_request` result. The PR data
/// may be the result object itself, its `structuredContent`, or JSON embedded in
/// the first `content[].text` entry.
fn parse_pr_from_mcp(result: &Value) -> Result<PullRequest> {
    for candidate in mcp_pr_candidates(result) {
        let number = candidate.get("number").and_then(Value::as_u64);
        let url = candidate
            .get("html_url")
            .or_else(|| candidate.get("url"))
            .and_then(Value::as_str);
        if number.is_some() || url.is_some() {
            return Ok(PullRequest {
                number,
                url: url.unwrap_or_default().to_string(),
                // Filled in by the caller, which knows the branch it
                // pushed; the MCP result carries no head-branch field of its own.
                head_branch: None,
            });
        }
    }
    Err(Error::Other(anyhow::anyhow!(
        "MCP create_pull_request result carried no PR number or URL: {result}"
    )))
}

/// The JSON objects that may carry the PR fields, in priority order.
fn mcp_pr_candidates(result: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(sc) = result.get("structuredContent") {
        out.push(sc.clone());
    }
    if let Some(text) = result
        .get("content")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(|c| c.get("text"))
        .and_then(Value::as_str)
    {
        if let Ok(v) = serde_json::from_str::<Value>(text) {
            out.push(v);
        }
    }
    out.push(result.clone());
    out
}

/// Parse a [`PullRequest`] from `gh pr create` stdout, whose last non-empty line
/// is the PR URL (e.g. `https://github.com/owner/name/pull/42`).
fn parse_pr_from_gh(stdout: &str) -> PullRequest {
    let url = stdout
        .lines()
        .map(str::trim)
        .rev()
        .find(|l| l.contains("/pull/"))
        .unwrap_or_else(|| stdout.trim())
        .to_string();
    let number = url.rsplit_once("/pull/").and_then(|(_, rest)| {
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        digits.parse::<u64>().ok()
    });
    PullRequest {
        number,
        url,
        // Filled in by the caller, which knows the branch it pushed.
        head_branch: None,
    }
}

/// One entry of `gh pr list --json number,title,headRefOid,headRefName,baseRefName,author,url`.
#[derive(Debug, serde::Deserialize)]
struct GhPrEntry {
    number: u64,
    #[serde(default)]
    title: String,
    #[serde(default, rename = "headRefOid")]
    head_ref_oid: String,
    #[serde(default, rename = "headRefName")]
    head_ref_name: String,
    #[serde(default, rename = "baseRefName")]
    base_ref_name: String,
    #[serde(default)]
    author: GhPrAuthor,
    #[serde(default)]
    url: String,
}

#[derive(Debug, Default, serde::Deserialize)]
struct GhPrAuthor {
    #[serde(default)]
    login: String,
}

impl GhPrEntry {
    fn into_forge_pr(self) -> ForgePullRequest {
        ForgePullRequest {
            number: self.number,
            title: self.title,
            head_sha: self.head_ref_oid,
            head_ref: self.head_ref_name,
            base_ref: self.base_ref_name,
            author: self.author.login,
            html_url: self.url,
            updated_at: String::new(),
        }
    }
}

/// Seam over the GitHub Actions REST API for the CI-failure webhook pipeline
/// ([`crate::forge_log::ActionLogClient`] / [`crate::forge_log::ActionRunsClient`]
/// / [`crate::forge_log::RepoFileClient`]), selected instead of
/// [`crate::provider::cli::FjForgeReads`] for a GitHub host
/// ([`is_github_host`]).
///
/// Every read goes through `gh api ... --hostname <host>`, never `gh run
/// view --log` (its own help text admits an unreliable job/log association
/// once a run has more than 25 jobs). GitHub's webhook `run_number` (the
/// trait methods' `run_number` parameter, from `workflow_run.run_number`) is
/// not the id the REST API addresses a run by (`workflow_run.id`), so
/// [`GhForgeReads::find_run`] pages `GET /repos/{repo}/actions/runs`
/// newest-first and matches `run_number` locally, reusing
/// [`crate::forge_log::parse_runs_list_entries`]'s tolerant field-name
/// matching (GitHub's `run_number` / `id` / `path` / `updated_at` are among
/// its candidate keys). From the matched run's `global_id`, jobs and each
/// job's plaintext log are two further calls (`.../runs/{id}/jobs`, then
/// `.../jobs/{job_id}/logs` per job), unlike `fj actions logs`, which
/// returns every job's log in one response.
///
/// Unlike `fj`, `gh api` requires a credential even for a public repo
/// (verified empirically against a live public repo), so a `None` token
/// falls back to `gh auth token --hostname <host>` exactly as
/// [`GithubForge::effective_token`] does; with neither, every trait method
/// fails naming that.
pub struct GhForgeReads {
    runner: Arc<dyn CommandRunner>,
    gh: String,
}

/// Runs requested per `gh api .../actions/runs` page.
const GH_RUNS_PER_PAGE: u32 = 100;

/// Pages searched for one target `run_number` before giving up: enough to
/// cover the 500 most recent runs, far more than the near-real-time webhook
/// delivery ever needs to look past.
const GH_RUN_LOOKUP_MAX_PAGES: u32 = 5;

impl GhForgeReads {
    /// Client using the default `gh` binary name.
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            runner,
            gh: "gh".to_string(),
        }
    }

    /// Override the binary name (tests point this at a stub).
    pub fn with_binary(mut self, gh: impl Into<String>) -> Self {
        self.gh = gh.into();
        self
    }

    /// `token` when given, else the daemon's own `gh auth token --hostname
    /// <host>` login.
    fn effective_token(
        &self,
        host: &str,
        token: Option<&str>,
    ) -> std::result::Result<String, String> {
        match token {
            Some(t) => Ok(t.to_string()),
            None => gh_auth_token(self.runner.as_ref(), &self.gh, host),
        }
    }

    /// `gh api --hostname <host> <endpoint> [extra...]`, GET by default.
    fn api_get(
        &self,
        host: &str,
        endpoint: &str,
        gh_token: &str,
        extra: &[&str],
    ) -> Result<CmdOutput> {
        let envs = [("GH_TOKEN", gh_token)];
        let mut args: Vec<&str> = vec!["api", "--hostname", host, endpoint];
        args.extend_from_slice(extra);
        self.runner.run_in_env(None, &self.gh, &args, &envs)
    }

    /// Page `GET /repos/{repo}/actions/runs`, newest-first, until the page
    /// carrying `run_number` is found or [`GH_RUN_LOOKUP_MAX_PAGES`] is
    /// exhausted. Returns the matched [`crate::forge_log::RunEntry`]; its
    /// `global_id` is the id `/actions/runs/{id}` addresses.
    fn find_run(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        gh_token: &str,
    ) -> std::result::Result<crate::forge_log::RunEntry, crate::forge_log::RunLogsOutcome> {
        use crate::forge_log::RunLogsOutcome;
        let per_page = GH_RUNS_PER_PAGE.to_string();
        for page in 1..=GH_RUN_LOOKUP_MAX_PAGES {
            let page_s = page.to_string();
            let endpoint = format!("repos/{repo}/actions/runs?per_page={per_page}&page={page_s}");
            let out = self.api_get(host, &endpoint, gh_token, &[]).map_err(|e| {
                RunLogsOutcome::Failed(format!("running {} api {endpoint}: {e}", self.gh))
            })?;
            if !out.ok {
                return Err(classify_gh_api_error(&out.stderr, out.exit_code));
            }
            let entries = crate::forge_log::parse_runs_list_entries(&out.stdout)
                .map_err(|e| RunLogsOutcome::Failed(format!("parsing gh api {endpoint}: {e}")))?;
            if let Some(entry) = entries.iter().find(|e| e.index == run_number) {
                return Ok(entry.clone());
            }
            if entries.len() < GH_RUNS_PER_PAGE as usize {
                break;
            }
        }
        Err(RunLogsOutcome::NotFound(format!(
            "no workflow run with run_number {run_number} found in the {} most recent runs of {repo}",
            GH_RUNS_PER_PAGE * GH_RUN_LOOKUP_MAX_PAGES
        )))
    }
}

/// Map a failed `gh api` invocation onto a [`crate::forge_log::RunLogsOutcome`].
/// `gh api` prints `gh: <message> (HTTP <code>)` on stderr (verified against a
/// live 403/404 response), so the code is the discriminator: 401/403 is a
/// rejected or under-scoped token (terminal), 404 is "not visible" (a private
/// repo without a token, a pruned run, or a job whose log already expired),
/// and anything else is a fault. Only `gh`'s own stderr is quoted; it carries
/// no token.
fn classify_gh_api_error(stderr: &str, exit_code: i32) -> crate::forge_log::RunLogsOutcome {
    use crate::forge_log::RunLogsOutcome;
    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no stderr")
        .to_string();
    if detail.contains("HTTP 401") {
        return RunLogsOutcome::AuthDenied(
            "HTTP 401: the log token is not a valid GitHub token (wrong value, expired, or revoked)"
                .to_string(),
        );
    }
    if detail.contains("HTTP 403") {
        return RunLogsOutcome::AuthDenied(
            "HTTP 403: the log token is valid but lacks Actions read access on this repo"
                .to_string(),
        );
    }
    if detail.contains("HTTP 404") {
        return RunLogsOutcome::NotFound(format!(
            "HTTP 404: not visible (private repo without a token, or pruned): {detail}"
        ));
    }
    RunLogsOutcome::Failed(format!("gh api exited {exit_code}: {detail}"))
}

/// Map a failed `gh api .../actions/runs` invocation onto a
/// [`crate::forge_log::RunsListOutcome`], on the same discriminator
/// [`classify_gh_api_error`] uses.
fn classify_gh_runs_error(stderr: &str, exit_code: i32) -> crate::forge_log::RunsListOutcome {
    use crate::forge_log::RunsListOutcome;
    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no stderr")
        .to_string();
    if detail.contains("HTTP 401") {
        return RunsListOutcome::AuthDenied(401);
    }
    if detail.contains("HTTP 403") {
        return RunsListOutcome::AuthDenied(403);
    }
    if detail.contains("HTTP 404") {
        return RunsListOutcome::NotFound;
    }
    RunsListOutcome::Failed(format!("gh api exited {exit_code}: {detail}"))
}

/// One `GET /repos/{repo}/actions/runs/{id}/jobs` document.
#[derive(Debug, Default, serde::Deserialize)]
struct GhJobsDoc {
    #[serde(default)]
    jobs: Vec<GhJobEntry>,
}

/// One job of a `GET .../jobs` document.
#[derive(Debug, serde::Deserialize)]
struct GhJobEntry {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    conclusion: Option<String>,
}

impl crate::forge_log::ActionLogClient for GhForgeReads {
    fn run_logs(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> crate::forge_log::RunLogsOutcome {
        use crate::forge_log::RunLogsOutcome;
        let gh_token = match self.effective_token(host, token) {
            Ok(t) => t,
            Err(e) => {
                return RunLogsOutcome::Failed(format!(
                    "no GitHub credential for host `{host}`: {e}"
                ))
            }
        };
        let run = match self.find_run(host, repo, run_number, &gh_token) {
            Ok(r) => r,
            Err(outcome) => return outcome,
        };
        let jobs_endpoint = format!(
            "repos/{repo}/actions/runs/{}/jobs?per_page=100",
            run.global_id
        );
        let out = match self.api_get(host, &jobs_endpoint, &gh_token, &[]) {
            Ok(out) => out,
            Err(e) => {
                return RunLogsOutcome::Failed(format!(
                    "running {} api {jobs_endpoint}: {e}",
                    self.gh
                ))
            }
        };
        if !out.ok {
            return classify_gh_api_error(&out.stderr, out.exit_code);
        }
        let doc: GhJobsDoc = match serde_json::from_str(out.stdout.trim()) {
            Ok(d) => d,
            Err(e) => {
                return RunLogsOutcome::Failed(format!("parsing gh api {jobs_endpoint}: {e}"))
            }
        };
        let mut jobs = Vec::with_capacity(doc.jobs.len());
        for job in doc.jobs {
            let log_endpoint = format!("repos/{repo}/actions/jobs/{}/logs", job.id);
            let (log, log_state, log_reason) =
                match self.api_get(host, &log_endpoint, &gh_token, &[]) {
                    Ok(out) if out.ok => (out.stdout, "available".to_string(), None),
                    Ok(out) => {
                        let reason = match classify_gh_api_error(&out.stderr, out.exit_code) {
                            RunLogsOutcome::NotFound(r) => {
                                (crate::forge_log::LOG_STATE_MISSING.to_string(), r)
                            }
                            RunLogsOutcome::AuthDenied(r) | RunLogsOutcome::Failed(r) => {
                                ("unavailable".to_string(), r)
                            }
                            RunLogsOutcome::Fetched(_) => unreachable!(
                            "classify_gh_api_error never returns Fetched from an !out.ok result"
                        ),
                        };
                        (String::new(), reason.0, Some(reason.1))
                    }
                    Err(e) => (
                        String::new(),
                        "unavailable".to_string(),
                        Some(format!("running {} api {log_endpoint}: {e}", self.gh)),
                    ),
                };
            let status = job
                .conclusion
                .filter(|c| !c.is_empty())
                .unwrap_or(job.status);
            jobs.push(crate::forge_log::JobLog {
                name: job.name,
                status,
                log,
                log_state,
                log_reason,
            });
        }
        RunLogsOutcome::Fetched(jobs)
    }
}

impl crate::forge_log::ActionRunsClient for GhForgeReads {
    fn list_runs(
        &self,
        host: &str,
        repo: &str,
        max_pages: u32,
        token: Option<&str>,
    ) -> crate::forge_log::RunsListOutcome {
        use crate::forge_log::RunsListOutcome;
        let gh_token = match self.effective_token(host, token) {
            Ok(t) => t,
            Err(e) => {
                return RunsListOutcome::Failed(format!(
                    "no GitHub credential for host `{host}`: {e}"
                ))
            }
        };
        let per_page = GH_RUNS_PER_PAGE.to_string();
        let mut all = Vec::new();
        for page in 1..=max_pages {
            let page_s = page.to_string();
            let endpoint = format!("repos/{repo}/actions/runs?per_page={per_page}&page={page_s}");
            let out = match self.api_get(host, &endpoint, &gh_token, &[]) {
                Ok(out) => out,
                Err(e) => {
                    if !all.is_empty() {
                        break;
                    }
                    return RunsListOutcome::Failed(format!(
                        "running {} api {endpoint}: {e}",
                        self.gh
                    ));
                }
            };
            if !out.ok {
                if !all.is_empty() {
                    break;
                }
                return classify_gh_runs_error(&out.stderr, out.exit_code);
            }
            let entries = match crate::forge_log::parse_runs_list_entries(&out.stdout) {
                Ok(e) => e,
                Err(e) => {
                    if !all.is_empty() {
                        break;
                    }
                    return RunsListOutcome::Failed(format!("parsing gh api {endpoint}: {e}"));
                }
            };
            let short = entries.len() < GH_RUNS_PER_PAGE as usize;
            all.extend(entries);
            if short {
                break;
            }
        }
        RunsListOutcome::Resolved(all)
    }

    fn run_timing(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> std::result::Result<crate::forge_log::RunTiming, String> {
        let gh_token = self
            .effective_token(host, token)
            .map_err(|e| format!("no GitHub credential for host `{host}`: {e}"))?;
        let per_page = GH_RUNS_PER_PAGE.to_string();
        for page in 1..=GH_RUN_LOOKUP_MAX_PAGES {
            let page_s = page.to_string();
            let endpoint = format!("repos/{repo}/actions/runs?per_page={per_page}&page={page_s}");
            let out = self
                .api_get(host, &endpoint, &gh_token, &[])
                .map_err(|e| format!("running {} api {endpoint}: {e}", self.gh))?;
            if !out.ok {
                return Err(format!(
                    "gh api exited {}: {}",
                    out.exit_code,
                    out.stderr
                        .lines()
                        .map(str::trim)
                        .find(|l| !l.is_empty())
                        .unwrap_or("no stderr")
                ));
            }
            if let Ok(t) = crate::forge_log::parse_run_timing(&out.stdout, run_number) {
                return Ok(t);
            }
            let entries =
                crate::forge_log::parse_runs_list_entries(&out.stdout).unwrap_or_default();
            if entries.len() < GH_RUNS_PER_PAGE as usize {
                break;
            }
        }
        Err(format!(
            "no workflow run with run_number {run_number} found in the {} most recent runs of {repo}",
            GH_RUNS_PER_PAGE * GH_RUN_LOOKUP_MAX_PAGES
        ))
    }
}

impl crate::forge_log::RepoFileClient for GhForgeReads {
    fn read_file(
        &self,
        host: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        token: Option<&str>,
    ) -> std::result::Result<String, String> {
        let gh_token = self
            .effective_token(host, token)
            .map_err(|e| format!("no GitHub credential for host `{host}`: {e}"))?;
        let endpoint = format!("repos/{repo}/contents/{path}?ref={git_ref}");
        let out = self
            .api_get(
                host,
                &endpoint,
                &gh_token,
                &["-H", "Accept: application/vnd.github.raw+json"],
            )
            .map_err(|e| format!("running {} api {endpoint}: {e}", self.gh))?;
        if out.ok {
            Ok(out.stdout)
        } else {
            Err(format!(
                "gh api exited {}: {}",
                out.exit_code,
                out.stderr
                    .lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty())
                    .unwrap_or("no stderr")
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;
    use std::sync::Mutex;

    /// Stub transport recording calls; `initialize` succeeds/fails per config and
    /// `call_tool` returns a canned result.
    struct StubTransport {
        init_ok: bool,
        result: Value,
        pub calls: Mutex<Vec<String>>,
    }

    impl StubTransport {
        fn new(init_ok: bool, result: Value) -> Self {
            Self {
                init_ok,
                result,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl McpTransport for StubTransport {
        fn initialize(&self, _endpoint: &str, _token: Option<&str>) -> Result<()> {
            self.calls.lock().unwrap().push("initialize".to_string());
            if self.init_ok {
                Ok(())
            } else {
                Err(Error::Other(anyhow::anyhow!("endpoint unreachable")))
            }
        }

        fn call_tool(
            &self,
            _endpoint: &str,
            _token: Option<&str>,
            name: &str,
            _arguments: Value,
        ) -> Result<Value> {
            self.calls.lock().unwrap().push(format!("call:{name}"));
            Ok(self.result.clone())
        }
    }

    /// Register the git interrogation + push every `create_pr` issues.
    fn with_git(runner: StubRunner, branch: &str, push_remote: &str) -> StubRunner {
        runner
            .ok(
                "git",
                &["remote", "get-url", "origin"],
                "https://github.com/octo/widget.git\n",
            )
            .ok("git", &["rev-parse", "--abbrev-ref", "HEAD"], branch)
            .ok(
                "git",
                &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                "origin/main\n",
            )
            .ok("git", &["log", "-1", "--pretty=format:%s"], "feat: x")
            .ok("git", &["log", "-1", "--pretty=format:%b"], "body text")
            .ok("git", &["push", push_remote, &format!("HEAD:{branch}")], "")
    }

    #[test]
    fn is_github_host_matches_dotcom_and_configured_ghe() {
        assert!(is_github_host("github.com", &[]));
        assert!(is_github_host("GitHub.com", &[]));
        assert!(!is_github_host("dev.a8n.run", &[]));
        let ghe = vec!["ghe.example.com".to_string()];
        assert!(is_github_host("ghe.example.com", &ghe));
        assert!(!is_github_host("other.example.com", &ghe));
    }

    #[test]
    fn parse_owner_repo_handles_https_and_ssh() {
        assert_eq!(
            parse_owner_repo("https://github.com/octo/widget.git").as_deref(),
            Some("octo/widget")
        );
        assert_eq!(
            parse_owner_repo("https://github.com/octo/widget").as_deref(),
            Some("octo/widget")
        );
        assert_eq!(
            parse_owner_repo("git@github.com:octo/widget.git").as_deref(),
            Some("octo/widget")
        );
        assert_eq!(parse_owner_repo("not-a-url"), None);
    }

    #[test]
    fn parse_pr_from_gh_pulls_url_and_number() {
        let pr = parse_pr_from_gh("\nhttps://github.com/octo/widget/pull/42\n");
        assert_eq!(pr.number, Some(42));
        assert_eq!(pr.url, "https://github.com/octo/widget/pull/42");
    }

    #[test]
    fn parse_pr_from_mcp_reads_structured_then_text() {
        // structuredContent wins.
        let v = json!({"structuredContent": {"number": 7, "html_url": "u7"}});
        let pr = parse_pr_from_mcp(&v).unwrap();
        assert_eq!(pr.number, Some(7));
        assert_eq!(pr.url, "u7");
        // content[].text JSON is parsed when no structuredContent.
        let v =
            json!({"content": [{"type": "text", "text": "{\"number\":9,\"html_url\":\"u9\"}"}]});
        let pr = parse_pr_from_mcp(&v).unwrap();
        assert_eq!(pr.number, Some(9));
        assert_eq!(pr.url, "u9");
    }

    #[test]
    fn create_pr_prefers_mcp() {
        // MCP reachable -> create_pull_request used; the PR itself never
        // consults gh, though the push below falls back to the daemon's own
        // `gh` login since no action token_ref is set.
        let push_remote = "https://x-access-token:ghp_from_login@github.com/octo/widget.git";
        let runner = Arc::new(with_git(StubRunner::new(), "feat/x", push_remote).ok(
            "gh",
            &["auth", "token", "--hostname", "github.com"],
            "ghp_from_login\n",
        ));
        let transport = Arc::new(StubTransport::new(
            true,
            json!({"structuredContent": {"number": 11, "html_url": "https://github.com/octo/widget/pull/11"}}),
        ));
        let forge = GithubForge::with_transport(
            runner.clone(),
            transport.clone(),
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        let pr = forge
            .create_pr(
                Path::new("/tmp/wd"),
                "github.com",
                crate::action::PrStrategy::Branch,
            )
            .unwrap();
        assert_eq!(pr.number, Some(11));
        assert_eq!(pr.url, "https://github.com/octo/widget/pull/11");
        let tcalls = transport.calls.lock().unwrap();
        assert!(
            tcalls.iter().any(|c| c == "call:create_pull_request"),
            "{tcalls:?}"
        );
        // The branch was pushed to the authenticated URL resolved through the
        // daemon's own `gh` login, and PR creation never consulted `gh` itself.
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| *c == format!("git push {push_remote} HEAD:feat/x")),
            "{calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c.starts_with("gh ") && c != "gh auth token --hostname github.com"),
            "{calls:?}"
        );
    }

    #[test]
    fn create_pr_falls_back_to_gh_when_mcp_unreachable() {
        let runner = Arc::new(
            with_git(StubRunner::new(), "feat/x", "origin")
                .ok("gh", &["--version"], "gh version 2.0.0")
                .ok(
                    "gh",
                    &[
                        "pr",
                        "create",
                        "--head",
                        "feat/x",
                        "--base",
                        "main",
                        "--title",
                        "feat: x",
                        "--body",
                        "body text",
                    ],
                    "https://github.com/octo/widget/pull/5\n",
                ),
        );
        let transport = Arc::new(StubTransport::new(false, Value::Null));
        let forge = GithubForge::with_transport(
            runner.clone(),
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        let pr = forge
            .create_pr(
                Path::new("/tmp/wd"),
                "github.com",
                crate::action::PrStrategy::Branch,
            )
            .unwrap();
        assert_eq!(pr.number, Some(5));
        assert_eq!(pr.url, "https://github.com/octo/widget/pull/5");
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c.starts_with("gh pr create")),
            "{calls:?}"
        );
    }

    #[test]
    fn create_pr_errors_no_backend_when_neither_available() {
        // MCP unreachable and gh absent -> NoBackend listing both attempts.
        let runner = Arc::new(with_git(StubRunner::new(), "feat/x", "origin").missing("gh"));
        let transport = Arc::new(StubTransport::new(false, Value::Null));
        let forge = GithubForge::with_transport(
            runner,
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        match forge
            .create_pr(
                Path::new("/tmp/wd"),
                "github.com",
                crate::action::PrStrategy::Branch,
            )
            .unwrap_err()
        {
            Error::NoBackend(tried) => {
                assert!(tried.contains("mcp:"), "{tried}");
                assert!(tried.contains("gh:"), "{tried}");
            }
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    /// The two GitHub preflight probes for `repo`, as arg lists.
    fn view_args(repo: &str) -> Vec<&str> {
        vec!["repo", "view", repo, "--json", "nameWithOwner"]
    }

    fn pr_list_args(repo: &str) -> Vec<&str> {
        vec![
            "pr", "list", "--repo", repo, "--state", "open", "--limit", "1", "--json", "number",
        ]
    }

    #[test]
    fn repo_access_probes_gh_with_the_pat_and_names_host_and_repo() {
        // Both reads run `gh` with the action's PAT as GH_TOKEN, so
        // they test the identity that will open the PR. The second
        // read exercises the pull-request capability a fine-grained PAT scopes
        // separately from repo metadata. A denial names the host and `owner/repo`.
        let ok_runner = Arc::new(
            StubRunner::new()
                .ok(
                    "gh",
                    &view_args("octo/widget"),
                    r#"{"nameWithOwner":"octo/widget"}"#,
                )
                .ok("gh", &pr_list_args("octo/widget"), "[]"),
        );
        let forge = GithubForge::new(
            ok_runner.clone(),
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        forge.repo_access("github.com", "octo/widget").unwrap();
        let envs = ok_runner.envs.lock().unwrap();
        assert_eq!(envs.len(), 2);
        for (bin, env) in envs.iter() {
            assert_eq!(bin, "gh");
            assert_eq!(
                *env,
                vec![("GH_TOKEN".to_string(), "ghp_secret".to_string())]
            );
        }
        drop(envs);

        let denied = GithubForge::new(
            Arc::new(StubRunner::new().fail(
                "gh",
                &view_args("octo/secret"),
                1,
                "GraphQL: Could not resolve to a Repository",
            )),
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let err = denied
            .repo_access("github.com", "octo/secret")
            .unwrap_err()
            .to_string();
        assert!(err.contains("octo/secret"), "got {err}");
        assert!(err.contains("github.com"), "got {err}");
        assert!(err.contains("permission"), "got {err}");
        assert!(err.contains("not visible"), "got {err}");
    }

    #[test]
    fn repo_access_fails_when_only_the_pull_request_probe_is_denied() {
        // A PAT with repo metadata but no pull-request read passes
        // `gh repo view` and is refused `gh pr list`. The error names the failing
        // capability, the status, and the `pull_request` scope to add.
        let forge = GithubForge::new(
            Arc::new(
                StubRunner::new()
                    .ok(
                        "gh",
                        &view_args("octo/widget"),
                        r#"{"nameWithOwner":"octo/widget"}"#,
                    )
                    .fail(
                        "gh",
                        &pr_list_args("octo/widget"),
                        1,
                        "HTTP 403: Resource not accessible by personal access token",
                    ),
            ),
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let err = forge
            .repo_access("github.com", "octo/widget")
            .unwrap_err()
            .to_string();
        assert!(err.contains("pr list"), "got {err}");
        assert!(err.contains("403"), "got {err}");
        assert!(err.contains("pull_request"), "got {err}");
        assert!(err.contains("unit permission"), "got {err}");
        assert!(!err.contains("not visible"), "got {err}");
    }

    #[test]
    fn create_pr_pushes_with_token_to_authenticated_url() {
        // A token routes the push to an authenticated HTTPS URL (not `origin`).
        let push_remote = "https://x-access-token:ghp_secret@github.com/octo/widget.git";
        let runner = Arc::new(with_git(StubRunner::new(), "feat/x", push_remote));
        let transport = Arc::new(StubTransport::new(
            true,
            json!({"structuredContent": {"number": 1, "html_url": "u1"}}),
        ));
        let forge = GithubForge::with_transport(
            runner.clone(),
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        forge
            .create_pr(
                Path::new("/tmp/wd"),
                "github.com",
                crate::action::PrStrategy::Branch,
            )
            .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "git push https://x-access-token:ghp_secret@github.com/octo/widget.git HEAD:feat/x"),
            "{calls:?}"
        );
    }

    /// The args `list_open_prs` passes to `gh pr list`.
    fn list_open_prs_args(repo: &str) -> Vec<&str> {
        vec![
            "pr",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--limit",
            "1000",
            "--json",
            "number,title,headRefOid,headRefName,baseRefName,author,url",
        ]
    }

    #[test]
    fn list_open_prs_parses_gh_json_and_maps_every_field() {
        let json = r#"[{"number":52,"title":"feat: x","headRefOid":"abc123","headRefName":"feat/x","baseRefName":"main","author":{"login":"octocat"},"url":"https://github.com/octo/widget/pull/52"}]"#;
        let runner = Arc::new(StubRunner::new().ok("gh", &list_open_prs_args("octo/widget"), json));
        let forge = GithubForge::new(
            runner,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let prs = forge.list_open_prs("github.com", "octo/widget").unwrap();
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].number, 52);
        assert_eq!(prs[0].title, "feat: x");
        assert_eq!(prs[0].head_sha, "abc123");
        assert_eq!(prs[0].head_ref, "feat/x");
        assert_eq!(prs[0].base_ref, "main");
        assert_eq!(prs[0].author, "octocat");
        assert_eq!(prs[0].html_url, "https://github.com/octo/widget/pull/52");
    }

    #[test]
    fn list_open_prs_passes_the_token_as_gh_token() {
        let runner = Arc::new(StubRunner::new().ok("gh", &list_open_prs_args("octo/widget"), "[]"));
        let forge = GithubForge::new(
            runner.clone(),
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        forge.list_open_prs("github.com", "octo/widget").unwrap();
        let envs = runner.envs.lock().unwrap();
        assert_eq!(envs.len(), 1);
        let (bin, env) = &envs[0];
        assert_eq!(bin, "gh");
        assert_eq!(
            *env,
            vec![("GH_TOKEN".to_string(), "ghp_secret".to_string())]
        );
    }

    #[test]
    fn clone_url_falls_back_to_gh_auth_token_when_no_token_ref() {
        // No action token: `clone_url` resolves through `gh auth token`.
        let runner = Arc::new(StubRunner::new().ok(
            "gh",
            &["auth", "token", "--hostname", "github.com"],
            "ghp_from_login\n",
        ));
        let transport = Arc::new(StubTransport::new(true, json!({})));
        let forge = GithubForge::with_transport(
            runner.clone(),
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        let url = forge.clone_url("github.com", "octo/widget").unwrap();
        assert_eq!(
            url,
            "https://x-access-token:ghp_from_login@github.com/octo/widget.git"
        );
        let calls = runner.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("gh auth token"))
                .count(),
            1
        );
    }

    #[test]
    fn list_open_prs_errors_on_nonzero_exit() {
        let runner = Arc::new(StubRunner::new().fail(
            "gh",
            &list_open_prs_args("octo/widget"),
            1,
            "HTTP 404: Not Found",
        ));
        let forge = GithubForge::new(
            runner,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let err = forge
            .list_open_prs("github.com", "octo/widget")
            .unwrap_err();
        match err {
            Error::CliFailed { exit_code, .. } => assert_eq!(exit_code, 1),
            other => panic!("expected CliFailed, got {other:?}"),
        }
    }

    #[test]
    fn list_open_prs_errors_on_unparseable_json() {
        let runner =
            Arc::new(StubRunner::new().ok("gh", &list_open_prs_args("octo/widget"), "not json"));
        let forge = GithubForge::new(
            runner,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let err = forge
            .list_open_prs("github.com", "octo/widget")
            .unwrap_err()
            .to_string();
        assert!(err.contains("gh pr list --json"), "got {err}");
    }

    #[test]
    fn list_open_prs_errors_when_exactly_at_the_limit() {
        let entries: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    r#"{{"number":{i},"title":"","headRefOid":"","headRefName":"","baseRefName":"","author":{{"login":""}},"url":""}}"#
                )
            })
            .collect();
        let json = format!("[{}]", entries.join(","));
        let runner =
            Arc::new(StubRunner::new().ok("gh", &list_open_prs_args("octo/widget"), &json));
        let forge = GithubForge::new(
            runner,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_secret".into()),
        );
        let err = forge
            .list_open_prs("github.com", "octo/widget")
            .unwrap_err()
            .to_string();
        assert!(err.contains("truncated"), "got {err}");
    }

    #[test]
    fn clone_url_with_action_token_never_invokes_gh_auth_token() {
        // A token_ref on the action is used directly; `gh auth token` must not run.
        let runner = Arc::new(StubRunner::new());
        let transport = Arc::new(StubTransport::new(true, json!({})));
        let forge = GithubForge::with_transport(
            runner.clone(),
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            Some("ghp_action".into()),
        );
        let url = forge.clone_url("github.com", "octo/widget").unwrap();
        assert_eq!(
            url,
            "https://x-access-token:ghp_action@github.com/octo/widget.git"
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.starts_with("gh auth token")),
            "{calls:?}"
        );
    }

    #[test]
    fn clone_url_reports_gh_auth_token_failure_when_no_token_ref() {
        // No action token and `gh auth token` fails: the error names both misses.
        let runner = Arc::new(StubRunner::new().fail(
            "gh",
            &["auth", "token", "--hostname", "github.com"],
            1,
            "no oauth token found",
        ));
        let transport = Arc::new(StubTransport::new(true, json!({})));
        let forge = GithubForge::with_transport(
            runner,
            transport,
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        let err = forge
            .clone_url("github.com", "octo/widget")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no github.token_ref"), "got {err}");
        assert!(err.contains("gh auth token"), "got {err}");
        assert!(err.contains("no oauth token found"), "got {err}");
    }
}
