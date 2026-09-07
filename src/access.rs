//! CLAUDE-216: on-demand access check.
//!
//! Answers, without queueing a job, whether the runner has the access it needs:
//! every configured credential's identity and validity, and whether a NAMED repo
//! or YouTrack project is workable by the credential that would actually be used
//! for it.
//!
//! One service backs three surfaces (the `/access` page, `POST
//! /api/access/check`, and `claude-run access check`), so the three cannot
//! drift. Credentials are materialized exactly as a job materializes them
//! ([`JobCredentials::materialize`], keyed by the resolved host) and scrubbed
//! when the guard drops, so the check exercises the real per-job credential path
//! rather than falling back to host auth by accident.
//!
//! **No secret value ever leaves this module.** A row names the secret, the
//! probe, and the failure text only. The probes are read-only by construction: a
//! write probe would leave a real issue or PR behind, so a capability that
//! cannot be proven read-only is reported as [`Outcome::Unverifiable`] rather
//! than as a pass.

use std::path::Path;
use std::sync::Arc;

use crate::db::DbPool;
use serde::{Deserialize, Serialize};

use crate::action::{ActionKind, ForgejoTarget, GithubTarget, NativeConfig, YoutrackSource};
use crate::error::{Error, Result};
use crate::native::youtrack::{credentialed_backend, host_only};
use crate::provider::{self, CommandRunner, Forge, IssueTracker};
use crate::secrets::{self, JobCredentials, ResolvedCredentials};
use crate::state;

/// Ceiling on the projects an aggregate check probes per action. A run of the
/// action's own query can reach any number of projects; the cap keeps a "Check
/// all" bounded. Truncation is never silent: it emits its own row.
const MAX_PROJECTS: usize = 10;

/// What a row probed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckKind {
    /// Who the credential authenticates as, and whether it is still valid.
    Identity,
    /// A YouTrack project is readable.
    Project,
    /// A project's VCS binding resolves to a host + repo path.
    Vcs,
    /// The action's own issue query executes under the credential.
    Query,
    /// A forge repo passes both [`Forge::repo_access`] scope categories.
    Repo,
    /// A capability that only a write would prove.
    Write,
    /// The check itself could not be set up (config load, credential resolve).
    Setup,
}

impl CheckKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckKind::Identity => "identity",
            CheckKind::Project => "project",
            CheckKind::Vcs => "vcs",
            CheckKind::Query => "query",
            CheckKind::Repo => "repo",
            CheckKind::Write => "write",
            CheckKind::Setup => "setup",
        }
    }
}

/// A row's verdict. `Unverifiable` is distinct from `Pass` on purpose: it is the
/// honest answer for a capability (opening a PR, writing a tracker field) that
/// only a mutating probe could prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Pass,
    Fail,
    Unverifiable,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "fail",
            Outcome::Unverifiable => "unverifiable",
        }
    }

    /// CSS badge class for the web table.
    pub fn badge(self) -> &'static str {
        match self {
            Outcome::Pass => "ok",
            Outcome::Fail => "bad",
            Outcome::Unverifiable => "warn",
        }
    }
}

/// One typed check row. `detail` carries the probe's own output or, on failure,
/// the CLI's error text verbatim (for a repo probe that is the
/// `provider::probe_denied` message, which already names the scope or grant to
/// add). `remediation` carries guidance this service composes where the tool's
/// own text does not spell it out. Neither ever carries a secret value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRow {
    /// What was probed: an action name, a project key, or `host/owner/repo`.
    pub target: String,
    pub kind: CheckKind,
    /// The credential in effect, named (`secret \`yt-prod\``) or `host \`yt\` auth`.
    pub credential: String,
    /// The probe command, e.g. `yt auth verify`.
    pub probe: String,
    pub outcome: Outcome,
    pub detail: String,
    pub remediation: Option<String>,
}

/// The report all three surfaces render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessReport {
    pub rows: Vec<CheckRow>,
}

impl AccessReport {
    /// Render a setup error (no usable `yt` / `fj`, an unreadable DB) as the
    /// report itself. A surface that 500s instead would hide the answer: "the
    /// runner cannot even invoke its CLIs" IS an access finding.
    pub fn from_error(e: &Error) -> Self {
        Self {
            rows: vec![CheckRow {
                target: "claude-run".to_string(),
                kind: CheckKind::Setup,
                credential: "-".to_string(),
                probe: "start access check".to_string(),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(
                    "the check could not run at all; `yt` and `fj` must be on the daemon's PATH"
                        .to_string(),
                ),
            }],
        }
    }

    /// True when no row failed. `Unverifiable` is not a failure: it is a stated
    /// limit of a read-only check, not a missing grant.
    pub fn ok(&self) -> bool {
        !self.rows.iter().any(|r| r.outcome == Outcome::Fail)
    }

    pub fn failures(&self) -> usize {
        self.rows
            .iter()
            .filter(|r| r.outcome == Outcome::Fail)
            .count()
    }
}

/// What to check. Every field absent is the aggregate check: every enabled
/// YouTrack action's credentials, the projects its query reaches, and the repos
/// those projects bind to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessRequest {
    /// Restrict to actions referencing this secret name (the `/secrets` page's
    /// per-secret Check button).
    #[serde(default)]
    pub secret: Option<String>,
    /// Forge host for `repo`, bare or with a scheme. Absent uses the host the
    /// project's VCS binding resolves to.
    #[serde(default)]
    pub host: Option<String>,
    /// Repo path `owner/name` to probe.
    #[serde(default)]
    pub repo: Option<String>,
    /// YouTrack project short name to probe.
    #[serde(default)]
    pub project: Option<String>,
}

impl AccessRequest {
    /// True when the request names no target: check everything.
    fn is_aggregate(&self) -> bool {
        self.repo.is_none() && self.project.is_none()
    }

    /// Normalize the empty strings a form POST sends for untouched fields.
    pub fn normalized(mut self) -> Self {
        fn clean(v: &mut Option<String>) {
            if v.as_deref().map(str::trim).unwrap_or("").is_empty() {
                *v = None;
            } else if let Some(s) = v {
                *s = s.trim().to_string();
            }
        }
        clean(&mut self.secret);
        clean(&mut self.host);
        clean(&mut self.repo);
        clean(&mut self.project);
        self
    }

    /// Form-echo accessors: the value or the empty string, for the `/access`
    /// inputs (an `Option` would render as `Some("x")`).
    pub fn repo_value(&self) -> &str {
        self.repo.as_deref().unwrap_or("")
    }

    pub fn host_value(&self) -> &str {
        self.host.as_deref().unwrap_or("")
    }

    pub fn project_value(&self) -> &str {
        self.project.as_deref().unwrap_or("")
    }
}

/// One enabled YouTrack action's typed config, the unit a check runs against.
#[derive(Debug, Clone)]
struct ActionTargets {
    name: String,
    youtrack: YoutrackSource,
    forgejo: ForgejoTarget,
    github: GithubTarget,
}

impl ActionTargets {
    /// True when any block references `secret`.
    fn references(&self, secret: &str) -> bool {
        [
            self.youtrack.token_ref.as_deref(),
            self.forgejo.token_ref.as_deref(),
            self.github.token_ref.as_deref(),
        ]
        .contains(&Some(secret))
    }
}

/// Label a credential for a row: the secret name, or the host-auth fallback.
fn credential_label(token_ref: Option<&str>, tool: &str) -> String {
    match token_ref {
        Some(name) => format!("secret `{name}`"),
        None => format!("host `{tool}` auth"),
    }
}

/// The row for "there is nothing to check": no enabled `youtrack` action, so
/// the check has no credential and no query to work from.
fn no_actions_row(req: &AccessRequest) -> CheckRow {
    CheckRow {
        target: req.secret.clone().unwrap_or_else(|| "actions".to_string()),
        kind: CheckKind::Setup,
        credential: "-".to_string(),
        probe: "action index".to_string(),
        outcome: Outcome::Fail,
        detail: "no enabled `youtrack` action to check".to_string(),
        remediation: Some(
            "add or enable a `kind: youtrack` action; a check needs an action to know which \
             credential and query to use"
                .to_string(),
        ),
    }
}

/// Run the access check with the real process runner.
///
/// A missing `yt` / `fj` is reported as a row rather than propagated, so no
/// surface 500s on it. With nothing to probe, the no-actions row is the more
/// useful answer than "the CLI is absent", so it wins.
pub async fn check(pool: &DbPool, cred_base: &Path, req: &AccessRequest) -> Result<AccessReport> {
    match provider::resolve_runner(provider::DEFAULT_PRIORITY) {
        Ok(runner) => check_with(pool, cred_base, req, runner).await,
        Err(e) => {
            let (targets, mut rows) = action_targets(pool, req).await?;
            if targets.is_empty() {
                rows.push(no_actions_row(req));
            } else {
                rows.extend(AccessReport::from_error(&e).rows);
            }
            Ok(AccessReport { rows })
        }
    }
}

/// [`check`] with an injectable [`CommandRunner`] (tests).
pub async fn check_with(
    pool: &DbPool,
    cred_base: &Path,
    req: &AccessRequest,
    runner: Arc<dyn CommandRunner>,
) -> Result<AccessReport> {
    let (targets, mut rows) = action_targets(pool, req).await?;
    if targets.is_empty() {
        rows.push(no_actions_row(req));
        return Ok(AccessReport { rows });
    }
    for target in targets {
        // Resolve `token_ref`s exactly as a run does. A resolve failure (missing
        // key file, unknown secret name) is itself the answer.
        let creds = match secrets::resolve_youtrack_credentials(
            pool,
            &target.youtrack,
            &target.forgejo,
            &target.github,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                rows.push(CheckRow {
                    target: target.name.clone(),
                    kind: CheckKind::Setup,
                    credential: credential_label(target.youtrack.token_ref.as_deref(), "yt"),
                    probe: "resolve token_ref".to_string(),
                    outcome: Outcome::Fail,
                    detail: e.to_string(),
                    remediation: Some(
                        "check the secret name on the action and that \
                         `CLAUDE_RUN_SECRETS_KEY_FILE` names the age key the secret was encrypted \
                         under"
                            .to_string(),
                    ),
                });
                continue;
            }
        };
        let req = req.clone();
        let runner = runner.clone();
        let base = cred_base.to_path_buf();
        // The probes shell out to `yt` / `fj`, so they run off the reactor.
        let produced =
            tokio::task::spawn_blocking(move || probe_action(&target, &creds, &req, runner, &base))
                .await
                .map_err(|e| Error::Other(anyhow::anyhow!("access check task: {e}")))?;
        rows.extend(produced);
    }
    Ok(AccessReport { rows })
}

/// Load the enabled YouTrack actions to check from the action index, plus a row
/// per action whose YAML no longer loads (a check that silently skipped it would
/// report "all good" for an action it never probed).
async fn action_targets(
    pool: &DbPool,
    req: &AccessRequest,
) -> Result<(Vec<ActionTargets>, Vec<CheckRow>)> {
    let mut targets = Vec::new();
    let mut rows = Vec::new();
    for row in state::action_index_list(pool).await? {
        if !row.enabled {
            continue;
        }
        let loaded = match crate::action::load_from_path(Path::new(&row.file_path)) {
            Ok(l) => l,
            Err(e) => {
                rows.push(CheckRow {
                    target: row.name.clone(),
                    kind: CheckKind::Setup,
                    credential: "-".to_string(),
                    probe: format!("load {}", row.file_path),
                    outcome: Outcome::Fail,
                    detail: e.to_string(),
                    remediation: Some("fix the action YAML; it cannot be checked".to_string()),
                });
                continue;
            }
        };
        if loaded.action.kind != ActionKind::Youtrack {
            continue;
        }
        let Some(NativeConfig::Youtrack(cfg)) = &loaded.action.native else {
            continue;
        };
        let target = ActionTargets {
            name: loaded.action.name.clone(),
            youtrack: cfg.youtrack.clone(),
            forgejo: cfg.forgejo.clone(),
            github: cfg.github.clone(),
        };
        if let Some(secret) = &req.secret {
            if !target.references(secret) {
                continue;
            }
        }
        targets.push(target);
    }
    Ok((targets, rows))
}

/// Probe one action's credentials. Blocking: it shells out to `yt` / `fj`.
fn probe_action(
    target: &ActionTargets,
    creds: &ResolvedCredentials,
    req: &AccessRequest,
    runner: Arc<dyn CommandRunner>,
    cred_base: &Path,
) -> Vec<CheckRow> {
    let mut rows = Vec::new();
    let yt_cred = credential_label(target.youtrack.token_ref.as_deref(), "yt");

    // yt-only credential set, materialized and scrubbed exactly as the gather
    // step's is (`JobCredentials` removes the directory on drop).
    let dir = cred_base.join(format!("access-{}", secrets::unique_suffix()));
    let guard = match JobCredentials::materialize(&dir, creds, None) {
        Ok(g) => g,
        Err(e) => {
            rows.push(CheckRow {
                target: target.name.clone(),
                kind: CheckKind::Setup,
                credential: yt_cred,
                probe: "materialize credentials".to_string(),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(format!(
                    "the runner could not write its per-job credential files under {}",
                    cred_base.display()
                )),
            });
            return rows;
        }
    };
    let backend = credentialed_backend(&runner, &guard);

    // 1. Identity: the single read-only answer to "is this token still valid".
    match backend.auth_verify() {
        Ok(login) => rows.push(CheckRow {
            target: target.name.clone(),
            kind: CheckKind::Identity,
            credential: yt_cred.clone(),
            probe: "yt auth verify".to_string(),
            outcome: Outcome::Pass,
            detail: format!("authenticated as `{login}`{}", base_url_suffix(target)),
            remediation: None,
        }),
        Err(e) => rows.push(CheckRow {
            target: target.name.clone(),
            kind: CheckKind::Identity,
            credential: yt_cred.clone(),
            probe: "yt auth verify".to_string(),
            outcome: Outcome::Fail,
            detail: e.to_string(),
            remediation: Some(format!(
                "the YouTrack token is missing, expired, or revoked{}. Rotate it on /secrets, or \
                 set `youtrack.base_url` if the instance is wrong.",
                base_url_suffix(target)
            )),
        }),
    }

    // 2. The tracker writes the runner performs (the AI Agent field, the state
    //    transition, the PR comment) cannot be proven read-only.
    rows.push(CheckRow {
        target: target.name.clone(),
        kind: CheckKind::Write,
        credential: yt_cred.clone(),
        probe: "not probed (read-only check)".to_string(),
        outcome: Outcome::Unverifiable,
        detail: format!(
            "the runner writes `{}`, the State field, and a comment on pickup; proving that \
             read-only is impossible and a write probe would leave real edits on a real issue",
            target.youtrack.custom_field
        ),
        remediation: None,
    });

    // 3. Projects: the named one, or the ones the action's own query reaches.
    let mut projects: Vec<String> = Vec::new();
    if let Some(project) = &req.project {
        projects.push(project.clone());
    } else if req.is_aggregate() {
        match backend.list_queued(&target.youtrack.query) {
            Ok(issues) => {
                let mut keys: Vec<String> = issues
                    .iter()
                    .filter_map(|i| i.get("idReadable").and_then(|v| v.as_str()))
                    .filter_map(|k| k.rsplit_once('-').map(|(p, _)| p.to_string()))
                    .collect();
                keys.sort();
                keys.dedup();
                if keys.len() > MAX_PROJECTS {
                    rows.push(CheckRow {
                        target: target.name.clone(),
                        kind: CheckKind::Setup,
                        credential: yt_cred.clone(),
                        probe: "yt list --query=<action query>".to_string(),
                        outcome: Outcome::Unverifiable,
                        detail: format!(
                            "the query reaches {} projects; only the first {MAX_PROJECTS} are \
                             probed. Name a project to check the rest.",
                            keys.len()
                        ),
                        remediation: None,
                    });
                    keys.truncate(MAX_PROJECTS);
                }
                projects = keys;
            }
            Err(e) => rows.push(CheckRow {
                target: target.name.clone(),
                kind: CheckKind::Query,
                credential: yt_cred.clone(),
                probe: format!("yt list --query=\"{}\"", target.youtrack.query),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(query_remediation(&e.to_string(), None)),
            }),
        }
    }

    // Repos discovered from the projects' VCS bindings, probed after the loop.
    let mut discovered: Vec<(String, String)> = Vec::new();
    for project in &projects {
        rows.push(match backend.project_show(project) {
            Ok(name) => CheckRow {
                target: project.clone(),
                kind: CheckKind::Project,
                credential: yt_cred.clone(),
                probe: format!("yt project show {project}"),
                outcome: Outcome::Pass,
                detail: format!("readable: {name}"),
                remediation: None,
            },
            Err(e) => CheckRow {
                target: project.clone(),
                kind: CheckKind::Project,
                credential: yt_cred.clone(),
                probe: format!("yt project show {project}"),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(format!(
                    "YouTrack reports a project this token cannot READ exactly as it reports a \
                     missing one, so grant the authenticated user read access to project \
                     `{project}` (or check the short name)."
                )),
            },
        });

        match backend.resolve_vcs(project) {
            Ok(repo) => {
                let host = host_only(&repo.repo_host);
                rows.push(CheckRow {
                    target: project.clone(),
                    kind: CheckKind::Vcs,
                    credential: yt_cred.clone(),
                    probe: format!("yt project vcs list {project} --enabled-only"),
                    outcome: Outcome::Pass,
                    detail: format!("enabled binding -> {host}/{}", repo.repo_path),
                    remediation: None,
                });
                let pair = (host, repo.repo_path);
                if !discovered.contains(&pair) {
                    discovered.push(pair);
                }
            }
            Err(e) => rows.push(CheckRow {
                target: project.clone(),
                kind: CheckKind::Vcs,
                credential: yt_cred.clone(),
                probe: format!("yt project vcs list {project} --enabled-only"),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(format!(
                    "the runner clones the repo this binding resolves to; add an ENABLED VCS \
                     integration on project `{project}` (or grant the token access to read it)."
                )),
            }),
        }

        // The action's own query, scoped to the project. A project this token
        // cannot read comes back as HTTP 400 `invalid_query` rather than a 401,
        // which reads as a broken query unless the row says otherwise.
        let scoped = format!("{} project: {project}", target.youtrack.query);
        rows.push(match backend.list_query_top(&scoped, 1) {
            Ok(hits) => CheckRow {
                target: project.clone(),
                kind: CheckKind::Query,
                credential: yt_cred.clone(),
                probe: format!("yt list --query=\"{scoped}\" --top 1"),
                outcome: Outcome::Pass,
                detail: format!("query executes; {} issue(s) in the top 1", hits.len()),
                remediation: None,
            },
            Err(e) => CheckRow {
                target: project.clone(),
                kind: CheckKind::Query,
                credential: yt_cred.clone(),
                probe: format!("yt list --query=\"{scoped}\" --top 1"),
                outcome: Outcome::Fail,
                detail: e.to_string(),
                remediation: Some(query_remediation(&e.to_string(), Some(project))),
            },
        });
    }

    // 4. Repos: the named one, or the ones the bindings resolved to.
    let mut repos: Vec<(String, String)> = Vec::new();
    if let Some(repo) = &req.repo {
        match req.host.as_deref().map(host_only).or_else(|| {
            discovered
                .iter()
                .find(|(_, path)| path.eq_ignore_ascii_case(repo))
                .map(|(h, _)| h.clone())
        }) {
            Some(host) => repos.push((host, repo.clone())),
            None => rows.push(CheckRow {
                target: repo.clone(),
                kind: CheckKind::Setup,
                credential: "-".to_string(),
                probe: "resolve forge host".to_string(),
                outcome: Outcome::Fail,
                detail: "no host given and none resolved from a project VCS binding".to_string(),
                remediation: Some(
                    "pass the forge host (e.g. dev.a8n.run) alongside the repo, or check a \
                     project whose VCS binding names it"
                        .to_string(),
                ),
            }),
        }
    } else {
        repos = discovered;
    }
    for (host, repo) in repos {
        rows.extend(probe_repo(target, creds, &runner, cred_base, &host, &repo));
    }
    rows
}

/// Probe one `host`/`repo` with the credential the job would use for it: the
/// GitHub PAT for a GitHub host (auto-routed exactly as the per-job forge
/// selection is), the Forgejo token otherwise.
fn probe_repo(
    target: &ActionTargets,
    creds: &ResolvedCredentials,
    runner: &Arc<dyn CommandRunner>,
    cred_base: &Path,
    host: &str,
    repo: &str,
) -> Vec<CheckRow> {
    let is_github = provider::is_github_host(host, &target.github.hosts);
    let (cred, tool) = if is_github {
        (
            credential_label(target.github.token_ref.as_deref(), "gh"),
            "gh",
        )
    } else {
        (
            credential_label(target.forgejo.token_ref.as_deref(), "fj"),
            "fj",
        )
    };
    let full = format!("{host}/{repo}");
    let mut rows = Vec::new();

    // A materialized single-host `fj` store, keyed by the resolved host, so the
    // probe exercises the credential the job would use rather than the operator
    // store. Held for the whole probe; dropping it scrubs the files.
    let mut _guard: Option<JobCredentials> = None;
    let forge: Box<dyn Forge> = if is_github {
        Box::new(provider::GithubForge::new(
            runner.clone(),
            target.github.mcp_endpoint.clone(),
            creds.github_token.clone(),
        ))
    } else {
        let dir = cred_base.join(format!(
            "access-{}-{}",
            secrets::unique_suffix(),
            host.replace(['/', '\\', '.'], "_")
        ));
        match JobCredentials::materialize(&dir, creds, Some(host)) {
            Ok(g) => {
                let backend = credentialed_backend(runner, &g);
                _guard = Some(g);
                Box::new(backend) as Box<dyn Forge>
            }
            Err(e) => {
                rows.push(CheckRow {
                    target: full,
                    kind: CheckKind::Setup,
                    credential: cred,
                    probe: "materialize credentials".to_string(),
                    outcome: Outcome::Fail,
                    detail: e.to_string(),
                    remediation: None,
                });
                return rows;
            }
        }
    };

    rows.push(match forge.identity(host) {
        Ok(who) => CheckRow {
            target: full.clone(),
            kind: CheckKind::Identity,
            credential: cred.clone(),
            probe: format!("{tool} whoami"),
            outcome: Outcome::Pass,
            detail: who,
            remediation: None,
        },
        Err(e) => CheckRow {
            target: full.clone(),
            kind: CheckKind::Identity,
            credential: cred.clone(),
            probe: format!("{tool} whoami"),
            outcome: Outcome::Fail,
            detail: e.to_string(),
            remediation: Some(format!(
                "the forge token for {host} is missing, expired, or revoked; rotate it on /secrets"
            )),
        },
    });

    // The same probe the poll-time preflight runs, unchanged, so the check and
    // the preflight can never disagree. Its error text already names the scope
    // or grant to add (`provider::probe_denied`), so it is shown verbatim.
    rows.push(match forge.repo_access(host, repo) {
        Ok(()) => CheckRow {
            target: full.clone(),
            kind: CheckKind::Repo,
            credential: cred.clone(),
            probe: format!("{tool} repo view + pr search (Forge::repo_access)"),
            outcome: Outcome::Pass,
            detail: "repository read and pull-request read both pass".to_string(),
            remediation: None,
        },
        Err(e) => CheckRow {
            target: full.clone(),
            kind: CheckKind::Repo,
            credential: cred.clone(),
            probe: format!("{tool} repo view + pr search (Forge::repo_access)"),
            outcome: Outcome::Fail,
            detail: e.to_string(),
            remediation: None,
        },
    });

    rows.push(CheckRow {
        target: full,
        kind: CheckKind::Write,
        credential: cred,
        probe: "not probed (read-only check)".to_string(),
        outcome: Outcome::Unverifiable,
        detail: "opening a PR is a write; probing it would leave a real PR on the forge. The two \
                 reads above are the closest read-only proof."
            .to_string(),
        remediation: None,
    });
    rows
}

/// `" on <base_url>"` when the action pins one, else empty. The URL is public
/// config, never the token.
fn base_url_suffix(target: &ActionTargets) -> String {
    match &target.youtrack.base_url {
        Some(url) => format!(" on {url}"),
        None => String::new(),
    }
}

/// Turn a failed `yt list` into operator guidance. YouTrack answers a query that
/// names a project the token cannot read with HTTP 400 `invalid_query`, not a
/// 401/403, so the raw text reads as a syntax error; say what it really is.
fn query_remediation(detail: &str, project: Option<&str>) -> String {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("invalid_query") || lower.contains("isn't used for the project field") {
        match project {
            Some(p) => format!(
                "HTTP 400 `invalid_query` is how YouTrack reports a project the token cannot READ: \
                 the query itself is fine. Grant the authenticated user read access to project \
                 `{p}`."
            ),
            None => {
                "HTTP 400 `invalid_query` is how YouTrack reports a field or project value the \
                     token cannot READ, so check the grant before the query syntax."
                    .to_string()
            }
        }
    } else {
        "the action's query did not execute under this credential; the row's detail carries the \
         CLI's own error"
            .to_string()
    }
}

/// CLAUDE-216: drop the poll's recovery markers for repos that were not failing,
/// so the success marker is recorded only on a fail -> pass transition. Without
/// this every poll would append one info row per repo forever; with it, the
/// dashboard banner still clears (the marker supersedes the open failure) and
/// the event log stays readable.
pub async fn retain_access_recoveries(
    pool: &DbPool,
    action: &str,
    events: &[state::InputEvent],
) -> Result<Vec<state::InputEvent>> {
    let mut kept = Vec::with_capacity(events.len());
    for event in events {
        if event.message.starts_with(state::ACCESS_RECOVERY_PREFIX)
            && !state::access_failure_is_open(pool, action, event.context.as_deref()).await?
        {
            continue;
        }
        kept.push(event.clone());
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;

    fn targets() -> ActionTargets {
        ActionTargets {
            name: "youtrack".to_string(),
            youtrack: YoutrackSource {
                base_url: Some("https://yt.example".to_string()),
                token_ref: Some("yt-prod".to_string()),
                query: "-Resolved AI Agent: Queued".to_string(),
                ..Default::default()
            },
            forgejo: ForgejoTarget {
                token_ref: Some("fj-prod".to_string()),
                ..Default::default()
            },
            github: GithubTarget::default(),
        }
    }

    fn creds() -> ResolvedCredentials {
        ResolvedCredentials {
            yt: Some((
                "https://yt.example".to_string(),
                "yt-token-value".to_string(),
            )),
            fj_token: Some("fj-token-value".to_string()),
            github_token: None,
        }
    }

    /// A stub whose yt probes all pass, so a repo-only assertion is not drowned
    /// by unrelated failures.
    fn happy_yt(project: &str) -> StubRunner {
        StubRunner::new()
            .ok(
                "yt",
                &["auth", "verify", "--json"],
                r#"{"ok":true,"user":{"login":"claude-run"}}"#,
            )
            .ok(
                "yt",
                &["project", "show", project, "--json"],
                r#"{"shortName":"CLAUDE","name":"Claude Run"}"#,
            )
            .ok(
                "yt",
                &[
                    "project",
                    "vcs",
                    "list",
                    project,
                    "--enabled-only",
                    "--json",
                ],
                r#"[{"serverUrl":"https://dev.a8n.run","path":"contoso/claude-run"}]"#,
            )
            .ok(
                "yt",
                &[
                    "list",
                    "--query=-Resolved AI Agent: Queued project: CLAUDE",
                    "--top",
                    "1",
                    "--json",
                ],
                "[]",
            )
    }

    fn row<'a>(rows: &'a [CheckRow], kind: CheckKind, target: &str) -> &'a CheckRow {
        rows.iter()
            .find(|r| r.kind == kind && r.target == target)
            .unwrap_or_else(|| panic!("no {kind:?} row for {target}: {rows:#?}"))
    }

    #[test]
    fn passing_repo_probe_reports_both_scope_categories() {
        let dir = tempfile::tempdir().unwrap();
        let runner = happy_yt("CLAUDE")
            .ok(
                "fj",
                &["--host", "dev.a8n.run", "whoami"],
                "signed in as bot",
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "repo",
                    "view",
                    "contoso/claude-run",
                    "--json",
                ],
                "{}",
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "pr",
                    "search",
                    "--repo",
                    "contoso/claude-run",
                    "--state",
                    "open",
                    "--json",
                ],
                "[]",
            );
        let req = AccessRequest {
            project: Some("CLAUDE".to_string()),
            ..Default::default()
        };
        let rows = probe_action(&targets(), &creds(), &req, Arc::new(runner), dir.path());

        let repo = row(&rows, CheckKind::Repo, "dev.a8n.run/contoso/claude-run");
        assert_eq!(repo.outcome, Outcome::Pass, "{rows:#?}");
        assert!(repo.detail.contains("pull-request read"), "{repo:?}");
        assert_eq!(repo.credential, "secret `fj-prod`");
        // The identity row names the login, never the token.
        let id = row(&rows, CheckKind::Identity, "youtrack");
        assert_eq!(id.outcome, Outcome::Pass);
        assert!(id.detail.contains("claude-run"), "{id:?}");
        // The PR-open capability is stated as unverifiable, not passed.
        let write = row(&rows, CheckKind::Write, "dev.a8n.run/contoso/claude-run");
        assert_eq!(write.outcome, Outcome::Unverifiable);
        // No secret value anywhere in the report.
        for r in &rows {
            let blob = format!("{r:?}");
            assert!(!blob.contains("yt-token-value"), "{r:?}");
            assert!(!blob.contains("fj-token-value"), "{r:?}");
        }
    }

    #[test]
    fn scope_short_repo_probe_carries_the_remediation() {
        let dir = tempfile::tempdir().unwrap();
        // Repository read passes, pull-request read 403s: the CLAUDE-207 case a
        // repository-only probe would call healthy.
        let runner = happy_yt("CLAUDE")
            .ok(
                "fj",
                &["--host", "dev.a8n.run", "whoami"],
                "signed in as bot",
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "repo",
                    "view",
                    "contoso/claude-run",
                    "--json",
                ],
                "{}",
            )
            .fail(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "pr",
                    "search",
                    "--repo",
                    "contoso/claude-run",
                    "--state",
                    "open",
                    "--json",
                ],
                1,
                "unexpected status code 403 Forbidden",
            );
        let req = AccessRequest {
            project: Some("CLAUDE".to_string()),
            ..Default::default()
        };
        let rows = probe_action(&targets(), &creds(), &req, Arc::new(runner), dir.path());

        let repo = row(&rows, CheckKind::Repo, "dev.a8n.run/contoso/claude-run");
        assert_eq!(repo.outcome, Outcome::Fail, "{rows:#?}");
        // The `probe_denied` text, verbatim: which probe, and the scope to add.
        assert!(repo.detail.contains("pr search"), "{repo:?}");
        assert!(repo.detail.contains("read:issue"), "{repo:?}");
        assert!(repo.detail.contains("HTTP 403"), "{repo:?}");
    }

    #[test]
    fn unreadable_project_renders_invalid_query_as_a_permission_failure() {
        let dir = tempfile::tempdir().unwrap();
        let invalid_query = r#"HTTP 400 from https://yt.example/api/issues: {"error":"invalid_query","error_description":"Can't parse search query","error_children":[{"error":"The value \"CLAUDE\" isn't used for the project field."}]}"#;
        let runner = StubRunner::new()
            .ok(
                "yt",
                &["auth", "verify", "--json"],
                r#"{"ok":true,"user":{"login":"claude-run"}}"#,
            )
            .fail(
                "yt",
                &["project", "show", "CLAUDE", "--json"],
                1,
                "project 'CLAUDE' not found",
            )
            .fail(
                "yt",
                &[
                    "project",
                    "vcs",
                    "list",
                    "CLAUDE",
                    "--enabled-only",
                    "--json",
                ],
                1,
                "project 'CLAUDE' not found",
            )
            .fail(
                "yt",
                &[
                    "list",
                    "--query=-Resolved AI Agent: Queued project: CLAUDE",
                    "--top",
                    "1",
                    "--json",
                ],
                1,
                invalid_query,
            )
            // The failing VCS probe runs the CLAUDE-148 identity diagnostic.
            .ok("yt", &["user", "me"], "claude-run")
            .ok("yt", &["project", "list", "--json"], "[]");
        let req = AccessRequest {
            project: Some("CLAUDE".to_string()),
            ..Default::default()
        };
        let rows = probe_action(&targets(), &creds(), &req, Arc::new(runner), dir.path());

        let project = row(&rows, CheckKind::Project, "CLAUDE");
        assert_eq!(project.outcome, Outcome::Fail);
        assert!(
            project
                .remediation
                .as_deref()
                .unwrap()
                .contains("cannot READ"),
            "{project:?}"
        );
        let query = row(&rows, CheckKind::Query, "CLAUDE");
        assert_eq!(query.outcome, Outcome::Fail);
        let rem = query.remediation.as_deref().unwrap();
        assert!(rem.contains("invalid_query"), "{query:?}");
        assert!(rem.contains("read access to project `CLAUDE`"), "{query:?}");
    }

    #[test]
    fn materialized_credentials_are_scrubbed_after_the_check() {
        let dir = tempfile::tempdir().unwrap();
        let runner = happy_yt("CLAUDE")
            .ok(
                "fj",
                &["--host", "dev.a8n.run", "whoami"],
                "signed in as bot",
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "repo",
                    "view",
                    "contoso/claude-run",
                    "--json",
                ],
                "{}",
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "pr",
                    "search",
                    "--repo",
                    "contoso/claude-run",
                    "--state",
                    "open",
                    "--json",
                ],
                "[]",
            );
        let req = AccessRequest {
            project: Some("CLAUDE".to_string()),
            ..Default::default()
        };
        let rows = probe_action(&targets(), &creds(), &req, Arc::new(runner), dir.path());
        assert!(!rows.is_empty());
        // Both guards (the yt-only set and the per-host fj store) drop with
        // `probe_action`, so no plaintext credential file survives it.
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(left.is_empty(), "credential dirs left behind: {left:?}");
    }

    #[test]
    fn a_request_naming_a_repo_without_a_host_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let runner = StubRunner::new().ok(
            "yt",
            &["auth", "verify", "--json"],
            r#"{"ok":true,"user":{"login":"claude-run"}}"#,
        );
        let req = AccessRequest {
            repo: Some("contoso/claude-run".to_string()),
            ..Default::default()
        };
        let rows = probe_action(&targets(), &creds(), &req, Arc::new(runner), dir.path());
        let setup = row(&rows, CheckKind::Setup, "contoso/claude-run");
        assert_eq!(setup.outcome, Outcome::Fail);
        assert!(setup.detail.contains("no host given"), "{setup:?}");
    }

    #[test]
    fn report_ok_ignores_unverifiable_rows() {
        let unverifiable = CheckRow {
            target: "t".into(),
            kind: CheckKind::Write,
            credential: "-".into(),
            probe: "-".into(),
            outcome: Outcome::Unverifiable,
            detail: "-".into(),
            remediation: None,
        };
        let mut report = AccessReport {
            rows: vec![unverifiable.clone()],
        };
        assert!(report.ok());
        report.rows.push(CheckRow {
            outcome: Outcome::Fail,
            ..unverifiable
        });
        assert!(!report.ok());
        assert_eq!(report.failures(), 1);
    }
}
