//! Native CI-failure auto-close reconciler (CLAUDE-191).
//!
//! Closes each open CI-failure YouTrack issue once the failing job it tracks is
//! green again. The mint path only ever observes failures (`action_run_failure`),
//! so a stale open issue is closed by hand today; this action removes that toil.
//!
//! Pipeline (typed Rust, mirroring [`crate::native::ci_failure_analysis`]):
//!
//! - gather (YouTrack): `IssueTracker::list_queued(query)` -> keep issues whose
//!   summary starts with a configured prefix, then `inspect_issue` each and read
//!   its hidden `Branch:` / `Job:` markers (the CLAUDE-190 rekey markers) plus the
//!   `## Failure` block's `- Repo:` / `- Workflow:` / `- Run:`. A pre-rekey issue
//!   with no parseable branch/job is skipped (left for manual close); an issue
//!   already carrying the auto-closed marker is skipped (idempotency).
//! - gather (Forgejo): for each allow-listed repo, page the runs list
//!   ([`crate::forge_log::fetch_runs`]) and reduce it to the LATEST run per
//!   `(branch, workflow_id)`. When the runs list returns `head_branch` null (the
//!   deployed Forgejo), the branch is resolved from the run's log checkout section
//!   ([`crate::forge_log::resolve_branch`]), bounded by `max_branch_resolves`.
//! - now-green: read each latest run's per-job status from the jobs endpoint
//!   ([`crate::forge_log::fetch_run_jobs`]); a job whose status is `success` is a
//!   now-green `(branch, workflow_id, job)`. Because the reduction keeps the
//!   LATEST run, a newer failure after an older green run leaves the job red and
//!   the issue open.
//! - close: every open issue whose `(repo, branch, workflow, job)` exactly matches
//!   a now-green job is set to `resolved_state` and gets a run-linked comment.
//!
//! All Forgejo reads go through `forge_log` (Rust), never a raw REST call from a
//! nu snippet (tooling-gap rule, FJ-22). The reconciler performs no git/PR actions
//! and does not depend on the CLAUDE-189 analysis having run.

use std::collections::{HashMap, HashSet};

use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{CiFailureReconcileConfig, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::error::{Error, Result};
use crate::forge_log::{
    self, HttpLogTransport, JobInfo, JobsOutcome, LogFetchConfig, RunEntry, RunJobsApiTarget,
    RunLogsApiTarget, RunsListApiTarget, RunsListOutcome,
};
use crate::provider::{self, Backend, IssueInspect};
use crate::secrets::{self, SecretsKey};
use crate::state;

/// Hidden token embedded in the auto-close comment. Its presence in the issue's
/// description or any comment means the issue was already auto-closed, so the
/// reconciler never re-closes or re-comments (idempotency guard).
const AUTO_CLOSED_MARKER: &str = "crciautoclosed";

/// The runs-list paging ceiling for one repo scan. `50 * 40 = 2000` most-recent
/// runs, matching the resolver's reach.
const RECONCILE_MAX_PAGES: u32 = 40;

pub struct CiFailureReconcileHandler;

impl NativeHandler for CiFailureReconcileHandler {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move { run_ci_failure_reconcile(ctx).await })
    }
}

async fn run_ci_failure_reconcile(ctx: NativeContext<'_>) -> Result<()> {
    let action = &ctx.action.action;
    let pool = ctx.pool;

    let cfg = match &action.native {
        Some(NativeConfig::CiFailureReconcile(cfg)) => (**cfg).clone(),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    if !cfg.enabled {
        info!(action = %action.name, "ci-failure-reconcile is disabled; skipping run");
        return Ok(());
    }
    // A PAT is required to read the runs list / jobs endpoint (and to resolve a
    // null-branch run's branch from its log) on a private repo. CLAUDE-200: one
    // mapping, resolved per repo as the scan reaches it.
    let tokens = super::effective_log_tokens(pool, ctx.paths, cfg.log_token_ref.as_ref()).await;

    let backend = provider::resolve(provider::DEFAULT_PRIORITY)?;

    // gather issues BEFORE run_begin so a gather failure is a no-op run.
    let candidates = match backend.list_queued(&cfg.query) {
        Ok(rows) => rows,
        Err(e) => {
            warn!(action = %action.name, error = %e, "listing candidate issues failed; skipping run");
            return Ok(());
        }
    };
    let keys: Vec<String> = candidates
        .iter()
        .filter_map(|v| {
            let summary = v.get("summary").and_then(|s| s.as_str()).unwrap_or("");
            let key = v.get("idReadable").and_then(|s| s.as_str())?;
            matches_prefix(summary, &cfg.summary_prefixes).then(|| key.to_string())
        })
        .collect();
    if keys.is_empty() {
        info!(action = %action.name, "no CI-failure issues to reconcile");
        return Ok(());
    }

    // Inspect each candidate and parse its coordinates. Only issues with a
    // resolvable (repo, branch, workflow, job) on an allow-listed repo are
    // reconcilable; the rest are skipped (logged) and left for manual close.
    let mut targets: Vec<IssueTarget> = Vec::new();
    for key in &keys {
        let inspect = match backend.inspect_issue(key) {
            Ok(i) => i,
            Err(e) => {
                warn!(action = %action.name, key, error = %e, "inspecting the issue failed; skipping");
                continue;
            }
        };
        match parse_issue_coords(&inspect) {
            IssueParse::Matched(coords) => {
                if repo_allowed(&cfg.repos, &coords.repo) {
                    targets.push(IssueTarget {
                        key: key.clone(),
                        coords,
                    });
                } else {
                    info!(action = %action.name, key, repo = %coords.repo, "issue repo is not allow-listed; skipping");
                }
            }
            IssueParse::AlreadyClosed => {
                info!(action = %action.name, key, "issue already auto-closed; skipping");
            }
            IssueParse::Skip(reason) => {
                info!(action = %action.name, key, %reason, "issue not reconcilable; left for manual close");
            }
        }
    }
    if targets.is_empty() {
        info!(action = %action.name, "no reconcilable CI-failure issues");
        return Ok(());
    }

    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
    let cancel = match ctx.registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard {
        registry: ctx.registry,
        run_id,
    };
    let planned: Vec<String> = targets.iter().map(|t| t.key.clone()).collect();
    if let Err(e) = state::run_set_job_keys(pool, run_id, &planned).await {
        warn!(run_id, error = %e, "recording planned keys failed (continuing)");
    }

    let transport = HttpLogTransport::new();
    let mut closed: u32 = 0;
    // Scan one repo at a time; each repo's targets drive which runs matter.
    for repo in repos_in_order(&targets) {
        if cancel.is_cancelled() {
            break;
        }
        let repo_targets: Vec<&IssueTarget> =
            targets.iter().filter(|t| t.coords.repo == repo).collect();
        // CLAUDE-200: the token for THIS repo. A repo the mapping does not cover
        // is skipped before any Forgejo read, so no request goes out with another
        // org's token and its issues stay open and untouched.
        let Some(token_ref) = tokens.resolve(&repo) else {
            warn!(action = %action.name, repo = %repo, "no log token maps to this repo; skipping repo");
            continue;
        };
        let token = match resolve_token(pool, token_ref).await {
            Ok(t) => t,
            Err(e) => {
                warn!(action = %action.name, repo = %repo, token_ref, error = %e, "resolving the log token failed; skipping repo");
                continue;
            }
        };
        let green = gather_now_green(&transport, &cfg, &token, &repo, &repo_targets);

        for gt in &repo_targets {
            if green.iter().any(|g| issue_matches(&gt.coords, &repo, g)) {
                let g = green
                    .iter()
                    .find(|g| issue_matches(&gt.coords, &repo, g))
                    .expect("just matched");
                match close_issue(backend.as_ref(), &cfg, &gt.key, g) {
                    Ok(()) => {
                        info!(action = %action.name, key = %gt.key, repo = %repo, branch = %g.branch, job = %g.job_name, "auto-closed CI-failure issue (job now green)");
                        closed += 1;
                    }
                    Err(e) => {
                        warn!(action = %action.name, key = %gt.key, error = %e, "auto-closing the issue failed (continuing)");
                    }
                }
            }
        }
    }

    let exit_code = if cancel.is_cancelled() {
        crate::exit::CANCELLED
    } else {
        crate::exit::OK
    };
    state::run_finish(pool, run_id, exit_code, closed).await?;
    Ok(())
}

/// One reconcilable open issue: its key and parsed coordinates.
struct IssueTarget {
    key: String,
    coords: IssueCoords,
}

/// The `(repo, branch, workflow, job)` a CI-failure issue tracks, parsed from its
/// hidden markers / `## Failure` block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IssueCoords {
    repo: String,
    branch: String,
    workflow_id: String,
    job_name: String,
    /// The run web URL, used to derive the forge origin for the runs-list scan.
    html_url: String,
}

/// The result of parsing an open issue for reconciliation.
enum IssueParse {
    /// Reconcilable: a full `(repo, branch, workflow, job)` was resolved.
    Matched(IssueCoords),
    /// Already auto-closed (the marker is present): skip, never re-close.
    AlreadyClosed,
    /// Not reconcilable (a pre-rekey issue with no parseable branch/job): the
    /// reason is logged and the issue is left for manual close.
    Skip(String),
}

/// A now-green job on a repo: the latest run of `(branch, workflow_id)` succeeded
/// at `job_name`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GreenJob {
    branch: String,
    workflow_id: String,
    job_name: String,
    run_index: i64,
    run_url: String,
}

/// Resolve the decrypted, trimmed Forgejo PAT from the secrets store.
async fn resolve_token(pool: &sqlx::SqlitePool, name: &str) -> Result<String> {
    let key = SecretsKey::load()?;
    let token = secrets::resolve_secret(pool, &key, name).await?;
    Ok(token.trim().to_string())
}

/// True when `summary` starts with any of `prefixes` (both trimmed).
fn matches_prefix(summary: &str, prefixes: &[String]) -> bool {
    let s = summary.trim_start();
    prefixes.iter().any(|p| s.starts_with(p.trim()))
}

/// True when `repo` (an `owner/repo` full name) matches one of the allow-list
/// patterns: exact `owner/repo`, per-owner `owner/*`, or the global `*`. The
/// semantics live in [`crate::config::repo_matches`], shared with the webhook
/// allow-list and the per-repo log-token mapping (CLAUDE-200).
fn repo_allowed(allowed: &[String], repo: &str) -> bool {
    crate::config::repo_matches(allowed, repo)
}

/// The distinct repos across `targets`, in first-seen order.
fn repos_in_order(targets: &[IssueTarget]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for t in targets {
        if seen.insert(t.coords.repo.clone()) {
            out.push(t.coords.repo.clone());
        }
    }
    out
}

/// Strip surrounding whitespace and backticks from a `- Key: `value`` value.
fn unbacktick(v: &str) -> String {
    v.trim().trim_matches('`').trim().to_string()
}

/// Parse an open CI-failure issue into its reconcile coordinates. Reads the hidden
/// `Branch:` / `Job:` markers (CLAUDE-190) and the `## Failure` block's `- Repo:` /
/// `- Workflow:` / `- Run:`. Falls back to the `## Failure` block's `- Branch:`
/// bullet when the hidden marker is absent. Returns [`IssueParse::AlreadyClosed`]
/// when the auto-closed marker is present, and [`IssueParse::Skip`] when the
/// branch or job cannot be resolved (a pre-rekey issue), so it is never wrongly
/// closed.
fn parse_issue_coords(inspect: &IssueInspect) -> IssueParse {
    if inspect.description.contains(AUTO_CLOSED_MARKER)
        || inspect
            .comments
            .iter()
            .any(|c| c.contains(AUTO_CLOSED_MARKER))
    {
        return IssueParse::AlreadyClosed;
    }

    let mut repo = String::new();
    let mut workflow_id = String::new();
    let mut html_url = String::new();
    let mut branch_marker = String::new();
    let mut branch_bullet = String::new();
    let mut job_name = String::new();
    for line in inspect.description.lines() {
        let t = line.trim();
        if let Some(v) = t.strip_prefix("- Repo:") {
            repo = unbacktick(v);
        } else if let Some(v) = t.strip_prefix("- Workflow:") {
            workflow_id = unbacktick(v);
        } else if let Some(v) = t.strip_prefix("- Run:") {
            html_url = v.trim().to_string();
        } else if let Some(v) = t.strip_prefix("- Branch:") {
            branch_bullet = unbacktick(v);
        } else if let Some(v) = t.strip_prefix("Branch:") {
            branch_marker = v.trim().to_string();
        } else if let Some(v) = t.strip_prefix("Job:") {
            job_name = v.trim().to_string();
        }
    }
    // The hidden marker is authoritative; the `## Failure` bullet is the fallback.
    let branch = if !branch_marker.is_empty() {
        branch_marker
    } else {
        branch_bullet
    };

    if repo.is_empty() || workflow_id.is_empty() || html_url.is_empty() {
        return IssueParse::Skip("no parseable `## Failure` block".to_string());
    }
    if branch.is_empty() || job_name.is_empty() {
        return IssueParse::Skip(
            "no branch/job markers (pre-rekey issue); left for manual close".to_string(),
        );
    }
    IssueParse::Matched(IssueCoords {
        repo,
        branch,
        workflow_id,
        job_name,
        html_url,
    })
}

/// Gather every now-green `(branch, workflow, job)` for `repo` that a target issue
/// tracks. Pages the runs list, resolves any null branch from the run's log
/// (bounded), reduces to the latest run per `(branch, workflow_id)`, and reads
/// each latest run's per-job success from the jobs endpoint. Best-effort: any
/// Forgejo miss logs and yields no green jobs for the affected group (the issue
/// stays open), never an error.
fn gather_now_green(
    transport: &dyn forge_log::LogTransport,
    cfg: &CiFailureReconcileConfig,
    token: &str,
    repo: &str,
    repo_targets: &[&IssueTarget],
) -> Vec<GreenJob> {
    // A representative run URL for the repo (to derive the forge origin).
    let Some(any_url) = repo_targets.first().map(|t| t.coords.html_url.clone()) else {
        return Vec::new();
    };
    let Some(runs_target) = RunsListApiTarget::for_run(&any_url, repo, token.to_string()) else {
        warn!(repo = %repo, "could not derive the forge origin from the issue run URL; skipping repo");
        return Vec::new();
    };

    let mut runs = match forge_log::fetch_runs(transport, &runs_target, RECONCILE_MAX_PAGES) {
        Ok(RunsListOutcome::Resolved(runs)) => runs,
        Ok(RunsListOutcome::AuthDenied(status)) => {
            warn!(repo = %repo, status, "runs-list auth denied; skipping repo");
            return Vec::new();
        }
        Ok(RunsListOutcome::NotFound) => {
            warn!(repo = %repo, "runs list not found (endpoint absent or empty); skipping repo");
            return Vec::new();
        }
        Ok(RunsListOutcome::Failed(reason)) => {
            warn!(repo = %repo, %reason, "runs-list fetch failed; skipping repo");
            return Vec::new();
        }
        Err(e) => {
            warn!(repo = %repo, error = %e, "runs-list fetch errored; skipping repo");
            return Vec::new();
        }
    };

    // The workflows any open issue on this repo tracks: only these need branch
    // resolution / jobs reads, so an unrelated workflow's runs are not fetched.
    let target_workflows: HashSet<&str> = repo_targets
        .iter()
        .map(|t| t.coords.workflow_id.as_str())
        .collect();

    resolve_missing_branches(transport, cfg, token, repo, &mut runs, &target_workflows);

    // The set of (branch, workflow) pairs a target tracks, to skip jobs reads for
    // runs no open issue references.
    let target_pairs: HashSet<(&str, &str)> = repo_targets
        .iter()
        .map(|t| (t.coords.branch.as_str(), t.coords.workflow_id.as_str()))
        .collect();

    let latest = reduce_latest_runs(&runs);
    let fetch_cfg = LogFetchConfig::default();
    let mut green = Vec::new();
    for run in &latest {
        if !target_pairs.contains(&(run.branch.as_str(), run.workflow_id.as_str())) {
            continue;
        }
        let Some(jobs_target) =
            RunJobsApiTarget::for_run(&run.html_url, repo, run.global_id, Some(token.to_string()))
        else {
            continue;
        };
        match forge_log::fetch_run_jobs(transport, &fetch_cfg, &jobs_target, std::thread::sleep) {
            Ok(JobsOutcome::Resolved(jobs)) => green.extend(now_green_jobs(run, &jobs)),
            Ok(other) => {
                info!(repo = %repo, run = run.index, outcome = ?other, "jobs endpoint did not resolve; group left open")
            }
            Err(e) => {
                warn!(repo = %repo, run = run.index, error = %e, "jobs-list fetch failed; group left open")
            }
        }
    }
    green
}

/// Resolve the branch for runs the runs list left null (`head_branch` null on the
/// deployed Forgejo). Only runs of a target workflow are resolved, newest-first,
/// bounded by `max_branch_resolves`; reaching the cap is logged (never silent).
fn resolve_missing_branches(
    transport: &dyn forge_log::LogTransport,
    cfg: &CiFailureReconcileConfig,
    token: &str,
    repo: &str,
    runs: &mut [RunEntry],
    target_workflows: &HashSet<&str>,
) {
    let fetch_cfg = LogFetchConfig::default();
    let out_dir =
        std::env::temp_dir().join(format!("claude-run-reconcile-{repo}").replace('/', "_"));
    let mut resolved = 0usize;
    let mut cap_hit = false;
    for run in runs.iter_mut() {
        if !run.branch.is_empty() || !target_workflows.contains(run.workflow_id.as_str()) {
            continue;
        }
        if resolved >= cfg.max_branch_resolves {
            cap_hit = true;
            break;
        }
        resolved += 1;
        let Some(log_target) = RunLogsApiTarget::for_run(
            &run.html_url,
            repo,
            run.index,
            run.global_id,
            token.to_string(),
        ) else {
            continue;
        };
        match forge_log::fetch_run_logs_api(
            transport,
            &fetch_cfg,
            &log_target,
            &out_dir,
            &[],
            std::thread::sleep,
        ) {
            Ok(forge_log::ApiLogOutcome::Fetched(log)) => {
                if let Some(b) = forge_log::resolve_branch(None, &log.text) {
                    run.branch = b;
                }
            }
            _ => { /* a log miss leaves the branch empty; the group stays open. */ }
        }
    }
    let _ = std::fs::remove_dir_all(&out_dir);
    if cap_hit {
        warn!(repo = %repo, cap = cfg.max_branch_resolves, "branch-resolve cap reached; some runs left ungrouped (raise max_branch_resolves to cover them)");
    }
}

/// Reduce runs to the LATEST run per `(branch, workflow_id)`: the one with the
/// highest global id (ids increase with run recency). Runs missing a branch or
/// workflow are dropped (they cannot be grouped). A run's jobs are shared by the
/// whole run, so the latest run per `(branch, workflow)` is the latest run per
/// `(branch, workflow, job)` for every job it contains.
fn reduce_latest_runs(runs: &[RunEntry]) -> Vec<RunEntry> {
    let mut latest: HashMap<(String, String), RunEntry> = HashMap::new();
    for run in runs {
        if run.branch.is_empty() || run.workflow_id.is_empty() {
            continue;
        }
        let key = (run.branch.clone(), run.workflow_id.clone());
        match latest.get(&key) {
            Some(existing) if existing.global_id >= run.global_id => {}
            _ => {
                latest.insert(key, run.clone());
            }
        }
    }
    latest.into_values().collect()
}

/// True for a job/run status that denotes success.
fn is_success_status(s: &str) -> bool {
    s.eq_ignore_ascii_case("success")
}

/// The now-green jobs of `run`: every job whose status is success, expanded to a
/// [`GreenJob`] carrying the run's branch / workflow / index / URL.
fn now_green_jobs(run: &RunEntry, jobs: &[JobInfo]) -> Vec<GreenJob> {
    jobs.iter()
        .filter(|j| !j.name.is_empty() && is_success_status(&j.status))
        .map(|j| GreenJob {
            branch: run.branch.clone(),
            workflow_id: run.workflow_id.clone(),
            job_name: j.name.clone(),
            run_index: run.index,
            run_url: run.html_url.clone(),
        })
        .collect()
}

/// True when the issue's `(repo, branch, workflow, job)` exactly matches a
/// now-green job. Exact on all four so a green job for a sibling branch or job
/// never closes a still-broken issue.
fn issue_matches(coords: &IssueCoords, repo: &str, green: &GreenJob) -> bool {
    coords.repo == repo
        && coords.branch == green.branch
        && coords.workflow_id == green.workflow_id
        && coords.job_name == green.job_name
}

/// Set the issue's State to `resolved_state` and post the run-linked auto-close
/// comment. State is set FIRST: once resolved, the issue drops out of the
/// `#Unresolved` scan, so a later run never re-closes it even if the comment
/// failed to post. The comment carries the hidden auto-closed marker.
fn close_issue(
    backend: &dyn Backend,
    cfg: &CiFailureReconcileConfig,
    key: &str,
    green: &GreenJob,
) -> Result<()> {
    backend.set_state(key, &cfg.resolved_state)?;
    let body = build_close_comment(&cfg.close_comment, green);
    if let Err(e) = backend.comment(key, &body) {
        warn!(key, error = %e, "posting the auto-close comment failed (issue already resolved)");
    }
    Ok(())
}

/// The auto-close comment: the rendered `close_comment` template plus the hidden
/// marker. The `{run_index}` / `{branch}` / `{run_url}` / `{job}` placeholders are
/// substituted from the now-green job.
fn build_close_comment(template: &str, green: &GreenJob) -> String {
    let rendered = template
        .replace("{run_index}", &green.run_index.to_string())
        .replace("{branch}", &green.branch)
        .replace("{run_url}", &green.run_url)
        .replace("{job}", &green.job_name);
    [
        rendered,
        String::new(),
        "<!-- claude-run ci-failure-reconcile; do not edit -->".to_string(),
        format!("Auto-Closed: {AUTO_CLOSED_MARKER}"),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inspect(description: &str, comments: &[&str]) -> IssueInspect {
        IssueInspect {
            description: description.to_string(),
            comments: comments.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn run(global_id: i64, index: i64, workflow: &str, branch: &str, status: &str) -> RunEntry {
        RunEntry {
            global_id,
            index,
            workflow_id: workflow.to_string(),
            branch: branch.to_string(),
            status: status.to_string(),
            html_url: format!("https://dev.a8n.run/owner/repo/actions/runs/{index}"),
        }
    }

    fn job(name: &str, status: &str) -> JobInfo {
        JobInfo {
            name: name.to_string(),
            status: status.to_string(),
        }
    }

    /// A CI-failure issue body as `mint::build_description` emits it post-rekey:
    /// a `## Failure` block plus the hidden Branch / Job markers.
    const REKEY_BODY: &str = "\
## Triage
Bucket: `code_fix` (classified by Llm).

Root cause and next step.

## Failure
- Repo: `contoso/claude-run`
- Workflow: `check.yml`
- Branch: `main`
- Commit: `abc123def456`
- Run: https://dev.a8n.run/contoso/claude-run/actions/runs/4034

The redacted failing-job log is attached to this issue.

<!-- claude-run dedupe markers; do not edit -->
Dedupe-Key: crdedupeaaaa crdedupebbbb crdedupecccc
Root-Cause-Key: crdedupecccc
Branch: main
Job: check";

    // ---- AC 3: marker-based matching + pre-rekey fallback/skip -------------

    #[test]
    fn parses_the_rekey_markers() {
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(REKEY_BODY, &[])) else {
            panic!("expected Matched");
        };
        assert_eq!(coords.repo, "contoso/claude-run");
        assert_eq!(coords.workflow_id, "check.yml");
        assert_eq!(coords.branch, "main");
        assert_eq!(coords.job_name, "check");
        assert_eq!(
            coords.html_url,
            "https://dev.a8n.run/contoso/claude-run/actions/runs/4034"
        );
    }

    #[test]
    fn hidden_branch_marker_wins_over_the_failure_bullet() {
        // The `## Failure` bullet says `main` but the hidden marker says `dev`:
        // the authoritative marker is used.
        let body = REKEY_BODY.replace("Branch: main", "Branch: dev");
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(&body, &[])) else {
            panic!("expected Matched");
        };
        assert_eq!(coords.branch, "dev");
    }

    #[test]
    fn pre_rekey_issue_without_branch_job_markers_is_skipped() {
        // A pre-CLAUDE-190 body: a `## Failure` block with no Branch/Job markers.
        let pre = "\
## Failure
- Repo: `contoso/claude-run`
- Workflow: `check.yml`
- Commit: `abc123`
- Run: https://dev.a8n.run/contoso/claude-run/actions/runs/4034

<!-- claude-run dedupe markers; do not edit -->
Dedupe-Key: crdedupeaaaa crdedupebbbb";
        assert!(matches!(
            parse_issue_coords(&inspect(pre, &[])),
            IssueParse::Skip(_)
        ));
    }

    #[test]
    fn issue_without_a_failure_block_is_skipped() {
        assert!(matches!(
            parse_issue_coords(&inspect("just prose, no failure block", &[])),
            IssueParse::Skip(_)
        ));
    }

    // ---- AC 4: close idempotency ------------------------------------------

    #[test]
    fn already_auto_closed_issue_is_skipped() {
        // The marker in a prior comment means the issue was auto-closed already.
        let default = CiFailureReconcileConfig::default();
        let body = build_close_comment(
            &default.close_comment,
            &GreenJob {
                branch: "main".to_string(),
                workflow_id: "check.yml".to_string(),
                job_name: "check".to_string(),
                run_index: 5,
                run_url: "https://dev.a8n.run/o/r/actions/runs/5".to_string(),
            },
        );
        let after = inspect(REKEY_BODY, &[&body]);
        assert!(matches!(
            parse_issue_coords(&after),
            IssueParse::AlreadyClosed
        ));
    }

    #[test]
    fn close_comment_renders_template_and_embeds_marker() {
        let g = GreenJob {
            branch: "main".to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            run_index: 42,
            run_url: "https://dev.a8n.run/o/r/actions/runs/42".to_string(),
        };
        let body = build_close_comment(&CiFailureReconcileConfig::default().close_comment, &g);
        // Every placeholder is substituted; none survives verbatim.
        assert!(body.contains("run #42"));
        assert!(body.contains("`main`"));
        assert!(body.contains("actions/runs/42"));
        assert!(body.contains("`check`"));
        assert!(!body.contains("{run_index}"));
        assert!(!body.contains("{branch}"));
        assert!(!body.contains("{run_url}"));
        assert!(!body.contains("{job}"));
        assert!(body.contains(AUTO_CLOSED_MARKER));
    }

    // ---- AC 2: latest-run reduction per (branch, workflow) ----------------

    #[test]
    fn reduce_keeps_the_highest_id_run_per_branch_workflow() {
        let runs = vec![
            run(100, 10, "check.yml", "main", "failure"),
            run(105, 11, "check.yml", "main", "success"), // newer on main/check
            run(103, 12, "check.yml", "dev", "success"),  // a different branch
            run(90, 9, "other.yml", "main", "success"),   // a different workflow
        ];
        let mut latest = reduce_latest_runs(&runs);
        latest.sort_by_key(|r| r.global_id);
        assert_eq!(latest.len(), 3);
        // main/check.yml collapses to the highest id (105), not the older 100.
        let main_check = latest
            .iter()
            .find(|r| r.branch == "main" && r.workflow_id == "check.yml")
            .unwrap();
        assert_eq!(main_check.global_id, 105);
    }

    #[test]
    fn reduce_drops_runs_without_branch_or_workflow() {
        let runs = vec![
            run(1, 1, "check.yml", "", "success"), // no branch
            run(2, 2, "", "main", "success"),      // no workflow
        ];
        assert!(reduce_latest_runs(&runs).is_empty());
    }

    // ---- AC 2 + AC 5: now-green detection + newer-failure-keeps-open -------

    #[test]
    fn now_green_emits_only_success_jobs() {
        let r = run(105, 11, "check.yml", "main", "success");
        let jobs = vec![
            job("check", "success"),
            job("deploy", "failure"),
            job("", "success"), // an unnamed job is ignored
        ];
        let green = now_green_jobs(&r, &jobs);
        assert_eq!(green.len(), 1);
        assert_eq!(green[0].job_name, "check");
        assert_eq!(green[0].branch, "main");
        assert_eq!(green[0].run_index, 11);
    }

    #[test]
    fn newer_failure_after_older_green_keeps_the_issue_open() {
        // Older run 100 was green; newer run 105 is red at `check`. Reduction keeps
        // the newer run, whose failing `check` yields no green job, so the issue
        // (main/check.yml/check) is not matched and stays open.
        let runs = vec![
            run(100, 10, "check.yml", "main", "success"),
            run(105, 11, "check.yml", "main", "failure"),
        ];
        let latest = reduce_latest_runs(&runs);
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].global_id, 105);
        let green = now_green_jobs(&latest[0], &[job("check", "failure")]);
        assert!(green.is_empty(), "a red latest run yields no green job");

        let coords = IssueCoords {
            repo: "owner/repo".to_string(),
            branch: "main".to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            html_url: String::new(),
        };
        assert!(!green
            .iter()
            .any(|g| issue_matches(&coords, "owner/repo", g)));
    }

    // ---- AC 3 + safety: exact matching, sibling guard ---------------------

    #[test]
    fn matching_is_exact_on_repo_branch_workflow_job() {
        let coords = IssueCoords {
            repo: "owner/repo".to_string(),
            branch: "main".to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            html_url: String::new(),
        };
        let exact = GreenJob {
            branch: "main".to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            run_index: 1,
            run_url: String::new(),
        };
        assert!(issue_matches(&coords, "owner/repo", &exact));
        // A green sibling job on the same branch must NOT match a different job.
        let sibling_job = GreenJob {
            job_name: "deploy".to_string(),
            ..exact.clone()
        };
        assert!(!issue_matches(&coords, "owner/repo", &sibling_job));
        // A green run on a different branch must NOT match.
        let sibling_branch = GreenJob {
            branch: "dev".to_string(),
            ..exact.clone()
        };
        assert!(!issue_matches(&coords, "owner/repo", &sibling_branch));
        // A different repo must NOT match.
        assert!(!issue_matches(&coords, "owner/other", &exact));
    }

    #[test]
    fn success_status_is_case_insensitive_and_exact() {
        assert!(is_success_status("success"));
        assert!(is_success_status("SUCCESS"));
        assert!(!is_success_status("failure"));
        assert!(!is_success_status("completed")); // completed != success
        assert!(!is_success_status(""));
    }

    // ---- repo allow-list ---------------------------------------------------

    #[test]
    fn repo_allow_list_matches_exact_owner_glob_and_star() {
        assert!(repo_allowed(&["owner/repo".to_string()], "owner/repo"));
        assert!(!repo_allowed(&["owner/repo".to_string()], "owner/other"));
        assert!(repo_allowed(&["owner/*".to_string()], "owner/anything"));
        assert!(!repo_allowed(&["owner/*".to_string()], "other/anything"));
        assert!(repo_allowed(&["*".to_string()], "any/repo"));
        assert!(!repo_allowed(&[], "any/repo"), "empty list denies all");
    }

    // ---- CLAUDE-200: per-repo log token -----------------------------------

    /// A cross-org allow-list plus one mapping: each repo scan uses its own org's
    /// PAT, and a repo the mapping does not cover is skipped (no runs-list read
    /// with another org's token) even though the allow-list admits it.
    #[test]
    fn each_allow_listed_repo_resolves_its_own_org_token() {
        use crate::action::{LogTokenEntry, LogTokenRef};
        let repos = vec![
            "contoso/*".to_string(),
            "initech/*".to_string(),
            "fabrikam/*".to_string(),
        ];
        let tokens = LogTokenRef::PerRepo(vec![
            LogTokenEntry {
                repos: vec!["contoso/*".to_string()],
                token_ref: "forgejo-logs-contoso".to_string(),
            },
            LogTokenEntry {
                repos: vec!["initech/*".to_string()],
                token_ref: "forgejo-logs-initech".to_string(),
            },
        ]);
        for (repo, expected) in [
            ("contoso/claude-run", Some("forgejo-logs-contoso")),
            ("initech/chat", Some("forgejo-logs-initech")),
            // Allow-listed but unmapped: skipped, never read with a wrong token.
            ("fabrikam/forgejo-cli", None),
        ] {
            assert!(repo_allowed(&repos, repo), "{repo} allow-listed");
            assert_eq!(tokens.resolve(repo), expected, "token for {repo}");
        }
    }

    #[test]
    fn prefix_filter_matches_mint_summaries() {
        let prefixes = vec!["CI failure:".to_string(), "Auto-fix:".to_string()];
        assert!(matches_prefix(
            "CI failure: check.yml on contoso/claude-run (run #4034, abc123)",
            &prefixes
        ));
        assert!(matches_prefix(
            "Auto-fix: cargo fmt on x (run #1, abc)",
            &prefixes
        ));
        assert!(!matches_prefix("Implement the events page", &prefixes));
    }
}
