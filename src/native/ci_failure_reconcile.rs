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
//!   (`fj actions runs`, [`crate::forge_log::ActionRunsClient`]) and reduce it to
//!   the LATEST run per `(branch, workflow_id)`. When the runs list carries no
//!   plain branch name (`fj`'s `head_branch` is Forgejo's `prettyref`, which is
//!   `#<pr-number>` for a PR run and the bare sha for a ref-deleted push), the
//!   branch is resolved from the run's log environment dump
//!   ([`crate::forge_log::resolve_branch`]), bounded by `max_branch_resolves`.
//! - now-green: read each latest run's per-job status from the jobs
//!   `fj actions logs` returns ([`crate::forge_log::fetch_run_log`]); a job whose
//!   status is `success` is a now-green `(branch, workflow_id, job)`. Because the
//!   reduction keeps the LATEST run, a newer failure after an older green run
//!   leaves the job red and the issue open.
//! - dedupe (CLAUDE-198): before any Forgejo read, open issues sharing a hidden
//!   `Root-Cause-Key:` marker are collapsed onto the oldest of the group; the rest
//!   are closed as duplicates. Mint's cross-run collapse is a tracker full-text
//!   search, so a lagging index lets two failures of the same job mint two issues.
//! - close (CLAUDE-265): every open issue whose `(repo, workflow, job)` matches a
//!   now-green job STRICTLY newer than the issue's own recorded run is set to
//!   `resolved_state` and gets a run-linked comment. The branch is no longer
//!   compared: a code issue (no `Infra-Kind:` marker) requires the green run to
//!   be on the configured `default_branch`, an infra issue (carrying the
//!   CLAUDE-264 marker) accepts any branch, so a merged or deleted PR branch
//!   never strands an issue open forever.
//!
//! All Forgejo reads go through `forge_log` (Rust) and shell out to `fj`: the
//! runs list via `fj actions runs`, the logs via `fj actions logs` (CLAUDE-205).
//! Never a raw REST call, and never one from a nu snippet (tooling-gap rule). The
//! reconciler performs no git/PR actions and does not depend on the CLAUDE-189
//! analysis having run.

use std::collections::{HashMap, HashSet};

use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{CiFailureReconcileConfig, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::error::{Error, Result};
use crate::forge_log::{
    self, ForgeReads, JobLog, LogFetchConfig, LogOutcome, LogTarget, RunEntry, RunsListOutcome,
};
use crate::provider::{self, AuditedTracker, IssueInspect, IssueTracker, IssueWriteAudit};
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
    // CLAUDE-228: hold the gather marker across the `yt list` AND the per-issue
    // `yt issue inspect` loop below; that loop is the slow part a stacked pickup
    // would redo verbatim.
    let gather_guard = ctx.gather.try_acquire(&action.name);
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

    // CLAUDE-228: the gather marker covers the whole pre-`run_begin` phase (the
    // work a stacked pickup would redo); released here, so overlapping runs past
    // this point stay allowed exactly as CLAUDE-162 intends.
    drop(gather_guard);
    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
    // CLAUDE-228: from here the row MUST be closed on every exit path, including
    // an early `?` out of the bookkeeping below.
    let run_guard = state::RunGuard::new(pool, run_id);
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

    // CLAUDE-234: every tracker write from here on carries this run's id into
    // the journal, and any issue it creates spends the run's ceiling.
    let limits = crate::job::effective_limits(&crate::job::RunnerDefaults::default(), ctx.action);
    let audit = IssueWriteAudit::new(Some(run_id), limits.max_issues_per_run);
    let tracker = AuditedTracker::new(backend.as_ref(), audit);

    // CLAUDE-223: run-scoped global-pause gate, checked alongside cancel at the
    // per-repo pickup so a mid-run pause stops the next repo scan.
    let run_pause = ctx.pause.for_action_run(&action.name);
    let mut closed: u32 = 0;

    // CLAUDE-198: collapse duplicates first. It needs no Forgejo read, so a
    // duplicate is closed even for a repo whose runs cannot be scanned, and the
    // survivor alone goes on to the now-green check below.
    if cfg.close_duplicates {
        let dups = plan_duplicates(&targets);
        for dup in &dups {
            match close_duplicate(&tracker, &cfg, dup) {
                Ok(()) => {
                    info!(action = %action.name, key = %dup.key, canonical = %dup.canonical, "auto-closed duplicate CI-failure issue");
                    closed += 1;
                }
                Err(e) => {
                    warn!(action = %action.name, key = %dup.key, error = %e, "closing the duplicate failed (continuing)");
                }
            }
        }
        let dup_keys: HashSet<&str> = dups.iter().map(|d| d.key.as_str()).collect();
        targets.retain(|t| !dup_keys.contains(t.key.as_str()));
    }

    // CLAUDE-205: ONE forge client: `fj actions runs` for the runs list and
    // `fj actions logs` for every log read.
    let fj = crate::provider::FjForgeReads::new(std::sync::Arc::new(
        crate::provider::ProcessRunner::new(),
    ));
    // Scan one repo at a time; each repo's targets drive which runs matter.
    for repo in repos_in_order(&targets) {
        if cancel.is_cancelled() {
            break;
        }
        if run_pause.stop().await {
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
        // CLAUDE-228: shells out to `fj` per run and sleeps between retries, so
        // it must not run on the async worker thread.
        let green =
            crate::blocking::in_place(|| gather_now_green(&fj, &cfg, &token, &repo, &repo_targets));

        for gt in &repo_targets {
            if green
                .iter()
                .any(|g| issue_matches(&gt.coords, &repo, g, &cfg.default_branch))
            {
                let g = green
                    .iter()
                    .find(|g| issue_matches(&gt.coords, &repo, g, &cfg.default_branch))
                    .expect("just matched");
                match close_issue(&tracker, &cfg, &gt.key, g) {
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

    let exit_code = run_pause.exit_code(cancel.is_cancelled());
    run_guard.finish(exit_code, closed).await?;
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
    /// CLAUDE-190 `Root-Cause-Key:` marker; empty on a pre-rekey issue, which is
    /// then never treated as a duplicate of anything.
    root_cause_key: String,
    /// The issue's own failing run's per-repo index, parsed from the `- Run:`
    /// URL suffix (CLAUDE-265). A close is only valid against a green run
    /// STRICTLY newer than this one, so a steady-state green default branch
    /// never closes an issue for a failure it has not yet fixed.
    run_index: i64,
    /// CLAUDE-264 hidden `Infra-Kind:` marker, present only on an infra/external
    /// failure. `None` means a code issue: its close scope is the configured
    /// `default_branch`. `Some(_)` means an infra issue: its close scope is ANY
    /// branch, since a host condition is not a property of a branch.
    infra_kind: Option<String>,
}

/// CLAUDE-198: one duplicate issue and the canonical issue it collapses onto.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DuplicateClose {
    key: String,
    canonical: String,
    root_cause_key: String,
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
async fn resolve_token(pool: &crate::db::DbPool, name: &str) -> Result<String> {
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
    let mut root_cause_key = String::new();
    let mut infra_kind: Option<String> = None;
    for line in inspect.description.lines() {
        let t = line.trim();
        if let Some(v) = t.strip_prefix("Root-Cause-Key:") {
            root_cause_key = v.trim().to_string();
        } else if let Some(v) = t.strip_prefix("Infra-Kind:") {
            infra_kind = Some(v.trim().to_string());
        } else if let Some(v) = t.strip_prefix("- Repo:") {
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
    // CLAUDE-265: the issue's own run index, parsed from the `- Run:` URL suffix
    // (e.g. `.../actions/runs/4034` -> `4034`). Never guessed: an unparseable
    // index means the freshness comparison against a candidate green run cannot
    // be made, so the issue is left for manual close rather than closed on an
    // unverifiable comparison.
    let Some(run_index) = html_url
        .rsplit('/')
        .next()
        .and_then(|s| s.parse::<i64>().ok())
    else {
        return IssueParse::Skip(format!(
            "could not parse the issue's own run index from its Run: url `{html_url}`"
        ));
    };
    IssueParse::Matched(IssueCoords {
        repo,
        branch,
        workflow_id,
        job_name,
        html_url,
        root_cause_key,
        run_index,
        infra_kind,
    })
}

/// CLAUDE-198: the duplicates among `targets`. Issues sharing a `(repo,
/// root_cause_key)` are the same failure by construction (mint derives the key
/// from repo / branch / workflow / job / failure signature), so all but the
/// canonical one are duplicates. The canonical is the OLDEST of the group: it
/// carries the earliest history and is the issue mint's association comments
/// already append to. An issue with no `Root-Cause-Key:` marker (pre-rekey) is
/// never grouped, so it is never closed as a duplicate of anything.
fn plan_duplicates(targets: &[IssueTarget]) -> Vec<DuplicateClose> {
    let mut groups: HashMap<(&str, &str), Vec<&IssueTarget>> = HashMap::new();
    for t in targets {
        if t.coords.root_cause_key.is_empty() {
            continue;
        }
        groups
            .entry((t.coords.repo.as_str(), t.coords.root_cause_key.as_str()))
            .or_default()
            .push(t);
    }
    let mut out = Vec::new();
    for group in groups.values_mut() {
        if group.len() < 2 {
            continue;
        }
        group.sort_by_key(|t| issue_order(&t.key));
        let canonical = group[0].key.clone();
        for dup in &group[1..] {
            out.push(DuplicateClose {
                key: dup.key.clone(),
                canonical: canonical.clone(),
                root_cause_key: dup.coords.root_cause_key.clone(),
            });
        }
    }
    // Deterministic order regardless of the HashMap iteration order.
    out.sort_by(|a, b| issue_order(&a.key).cmp(&issue_order(&b.key)));
    out
}

/// Sort key for an issue id: `(project, number)` for a `KEY-123` id, so
/// `CLAUDE-9` sorts before `CLAUDE-10`. An id with no numeric suffix falls back
/// to the whole string with number 0, keeping the ordering total.
fn issue_order(key: &str) -> (String, i64) {
    match key.rsplit_once('-') {
        Some((project, num)) => match num.parse::<i64>() {
            Ok(n) => (project.to_string(), n),
            Err(_) => (key.to_string(), 0),
        },
        None => (key.to_string(), 0),
    }
}

/// Close one duplicate: set `resolved_state` and post the duplicate comment.
/// State is set FIRST, exactly as [`close_issue`] does, so a failed comment can
/// never leave the issue open and re-processed.
fn close_duplicate(
    backend: &dyn IssueTracker,
    cfg: &CiFailureReconcileConfig,
    dup: &DuplicateClose,
) -> Result<()> {
    backend.set_state(&dup.key, &cfg.resolved_state)?;
    let body = build_duplicate_comment(&cfg.duplicate_comment, dup);
    if let Err(e) = backend.comment(&dup.key, &body) {
        warn!(key = %dup.key, error = %e, "posting the duplicate comment failed (issue already resolved)");
    }
    Ok(())
}

/// The duplicate close comment: the rendered `duplicate_comment` template plus
/// the same hidden marker the now-green close uses, so a duplicate is never
/// re-closed or re-commented.
fn build_duplicate_comment(template: &str, dup: &DuplicateClose) -> String {
    let rendered = template
        .replace("{canonical}", &dup.canonical)
        .replace("{root_cause_key}", &dup.root_cause_key);
    [
        rendered,
        String::new(),
        "<!-- claude-run ci-failure-reconcile; do not edit -->".to_string(),
        format!("Auto-Closed: {AUTO_CLOSED_MARKER}"),
    ]
    .join("\n")
}

/// Gather every now-green `(branch, workflow, job)` for `repo` that a target issue
/// tracks. Pages the runs list, resolves any null branch from the run's log
/// (bounded), reduces to the latest run per `(branch, workflow_id)`, and reads
/// each latest run's per-job success from the jobs `fj actions logs` returns.
/// Best-effort: any Forgejo miss logs and yields no green jobs for the affected
/// group (the issue stays open), never an error.
fn gather_now_green(
    fj: &dyn ForgeReads,
    cfg: &CiFailureReconcileConfig,
    token: &str,
    repo: &str,
    repo_targets: &[&IssueTarget],
) -> Vec<GreenJob> {
    // A representative run URL for the repo (to derive the forge host).
    let Some(any_url) = repo_targets.first().map(|t| t.coords.html_url.clone()) else {
        return Vec::new();
    };
    let Some(host) = forge_log::host_from_html_url(&any_url) else {
        warn!(repo = %repo, "could not derive the forge host from the issue run URL; skipping repo");
        return Vec::new();
    };

    // CLAUDE-205: the runs list through `fj actions runs`, paged by the client.
    let mut runs = match fj.list_runs(&host, repo, RECONCILE_MAX_PAGES, Some(token)) {
        RunsListOutcome::Resolved(runs) => runs,
        RunsListOutcome::AuthDenied(status) => {
            warn!(repo = %repo, status, "runs-list auth denied; skipping repo");
            return Vec::new();
        }
        RunsListOutcome::NotFound => {
            warn!(repo = %repo, "runs list not found (repo not visible or empty); skipping repo");
            return Vec::new();
        }
        RunsListOutcome::Failed(reason) => {
            warn!(repo = %repo, %reason, "runs-list fetch failed; skipping repo");
            return Vec::new();
        }
    };

    // Scratch dir for the redacted log artifacts the fetch writes; removed below
    // so no run log outlives the scan.
    let out_dir =
        std::env::temp_dir().join(format!("claude-run-reconcile-{repo}").replace('/', "_"));

    // The workflows any open issue on this repo tracks: only these need branch
    // resolution / jobs reads, so an unrelated workflow's runs are not fetched.
    let target_workflows: HashSet<&str> = repo_targets
        .iter()
        .map(|t| t.coords.workflow_id.as_str())
        .collect();

    resolve_missing_branches(fj, cfg, token, repo, &out_dir, &mut runs, &target_workflows);

    // CLAUDE-265: the workflows an ANY-branch (infra-keyed) target tracks need a
    // jobs read for every branch; a default-branch (code) target needs one only
    // for the configured `default_branch`. Neither set is keyed on the issue's
    // OWN branch anymore, so a merged/deleted PR branch is never what decides
    // whether a run is worth reading.
    let any_branch_workflows: HashSet<&str> = repo_targets
        .iter()
        .filter(|t| t.coords.infra_kind.is_some())
        .map(|t| t.coords.workflow_id.as_str())
        .collect();
    let default_branch_workflows: HashSet<&str> = repo_targets
        .iter()
        .filter(|t| t.coords.infra_kind.is_none())
        .map(|t| t.coords.workflow_id.as_str())
        .collect();

    let latest = reduce_latest_runs(&runs);
    let mut green = Vec::new();
    for run in &latest {
        let in_scope = any_branch_workflows.contains(run.workflow_id.as_str())
            || (run.branch == cfg.default_branch
                && default_branch_workflows.contains(run.workflow_id.as_str()));
        if !in_scope {
            continue;
        }
        match fetch_run(fj, token, repo, run, &out_dir) {
            Some(jobs) => green.extend(now_green_jobs(run, &jobs)),
            None => {
                info!(repo = %repo, run = run.index, "run logs did not resolve; group left open")
            }
        }
    }
    let _ = std::fs::remove_dir_all(&out_dir);
    green
}

/// Fetch one run's redacted per-job logs through `fj actions logs`. `None` on any
/// miss (the caller leaves the affected group open); never errors.
fn fetch_run(
    fj: &dyn ForgeReads,
    token: &str,
    repo: &str,
    run: &RunEntry,
    out_dir: &std::path::Path,
) -> Option<Vec<JobLog>> {
    let target = LogTarget::for_run(&run.html_url, repo, run.index, Some(token.to_string()))?;
    match forge_log::fetch_run_log(
        fj,
        // CLAUDE-204: these runs finished long ago; one call decides.
        &LogFetchConfig::no_retry(),
        &target,
        out_dir,
        &[],
        std::thread::sleep,
    ) {
        Ok(LogOutcome::Fetched(log)) => Some(log.jobs),
        Ok(other) => {
            info!(repo = %repo, run = run.index, outcome = ?other, "run-log fetch did not resolve");
            None
        }
        Err(e) => {
            warn!(repo = %repo, run = run.index, error = %e, "run-log fetch failed");
            None
        }
    }
}

/// Resolve the branch for runs the runs list left null (`head_branch` null on the
/// deployed Forgejo). Only runs of a target workflow are resolved, newest-first,
/// bounded by `max_branch_resolves`; reaching the cap is logged (never silent).
fn resolve_missing_branches(
    fj: &dyn ForgeReads,
    cfg: &CiFailureReconcileConfig,
    token: &str,
    repo: &str,
    out_dir: &std::path::Path,
    runs: &mut [RunEntry],
    target_workflows: &HashSet<&str>,
) {
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
        let snapshot = run.clone();
        if let Some(jobs) = fetch_run(fj, token, repo, &snapshot, out_dir) {
            // The environment dump is in whichever job printed it; take the first hit.
            if let Some(b) = jobs
                .iter()
                .find_map(|j| forge_log::resolve_branch(None, &j.log))
            {
                run.branch = b;
            }
        }
    }
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
fn now_green_jobs(run: &RunEntry, jobs: &[JobLog]) -> Vec<GreenJob> {
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

/// True when the issue's `(repo, workflow, job)` matches a now-green job within
/// its close scope (CLAUDE-265). Always required: same repo, same
/// `workflow_id`, same `job_name`, and the green run STRICTLY newer than the
/// issue's own recorded run (`green.run_index > coords.run_index`), so a
/// steady-state green default branch never closes a freshly minted issue.
///
/// The branch scope differs by whether the issue is infra-keyed: a code issue
/// (no `Infra-Kind:` marker) requires `green.branch == default_branch`, since
/// the default branch is the authority on whether the code is fixed; an infra
/// issue (carrying the marker) accepts ANY branch, since a host condition is
/// not a property of a branch. Neither rule compares `coords.branch` to
/// `green.branch`, so a merged or deleted PR branch no longer strands the
/// issue open.
fn issue_matches(coords: &IssueCoords, repo: &str, green: &GreenJob, default_branch: &str) -> bool {
    let branch_in_scope = match &coords.infra_kind {
        Some(_) => true,
        None => green.branch == default_branch,
    };
    coords.repo == repo
        && branch_in_scope
        && coords.workflow_id == green.workflow_id
        && coords.job_name == green.job_name
        && green.run_index > coords.run_index
}

/// Set the issue's State to `resolved_state` and post the run-linked auto-close
/// comment. State is set FIRST: once resolved, the issue drops out of the
/// `#Unresolved` scan, so a later run never re-closes it even if the comment
/// failed to post. The comment carries the hidden auto-closed marker.
fn close_issue(
    backend: &dyn IssueTracker,
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
            stopped: "2026-08-20T20:27:31+02:00".to_string(),
        }
    }

    fn job(name: &str, status: &str) -> JobLog {
        JobLog {
            name: name.to_string(),
            status: status.to_string(),
            log: String::new(),
            ..Default::default()
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
            root_cause_key: String::new(),
            run_index: 5,
            infra_kind: None,
        };
        assert!(!green
            .iter()
            .any(|g| issue_matches(&coords, "owner/repo", g, "main")));
    }

    /// A code coords fixture: no `Infra-Kind:`, tracked failure at `run_index`.
    fn code_coords(run_index: i64) -> IssueCoords {
        IssueCoords {
            repo: "owner/repo".to_string(),
            branch: "some-pr-branch".to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            html_url: String::new(),
            root_cause_key: String::new(),
            run_index,
            infra_kind: None,
        }
    }

    fn green_job(branch: &str, run_index: i64) -> GreenJob {
        GreenJob {
            branch: branch.to_string(),
            workflow_id: "check.yml".to_string(),
            job_name: "check".to_string(),
            run_index,
            run_url: String::new(),
        }
    }

    // ---- AC 1 + AC 2: close scope no longer keys on coords.branch ---------

    #[test]
    fn code_issue_closes_on_default_branch_green_newer_than_its_own_run() {
        // The issue's OWN recorded branch (`some-pr-branch`, from `code_coords`)
        // is long gone; the close is decided by the default branch instead.
        let coords = code_coords(5);
        assert!(issue_matches(
            &coords,
            "owner/repo",
            &green_job("main", 6),
            "main"
        ));
    }

    #[test]
    fn code_issue_does_not_close_on_a_non_default_branch_green() {
        let coords = code_coords(5);
        assert!(!issue_matches(
            &coords,
            "owner/repo",
            &green_job("some-other-branch", 6),
            "main"
        ));
    }

    #[test]
    fn infra_issue_closes_on_any_branch_green_newer_than_its_own_run() {
        let mut coords = code_coords(5);
        coords.infra_kind = Some("disk-full".to_string());
        // Neither the issue's own branch nor the default branch: an infra
        // issue's scope is ANY branch.
        assert!(issue_matches(
            &coords,
            "owner/repo",
            &green_job("some-unrelated-branch", 6),
            "main"
        ));
    }

    // ---- AC 4: the green run must be strictly newer than the issue's own --

    #[test]
    fn a_green_run_not_newer_than_the_issue_never_closes_it_either_rule() {
        let code = code_coords(10);
        let mut infra = code_coords(10);
        infra.infra_kind = Some("disk-full".to_string());

        // Equal run_index: not newer, does not close.
        assert!(!issue_matches(
            &code,
            "owner/repo",
            &green_job("main", 10),
            "main"
        ));
        assert!(!issue_matches(
            &infra,
            "owner/repo",
            &green_job("any-branch", 10),
            "main"
        ));
        // Older run_index: not newer, does not close.
        assert!(!issue_matches(
            &code,
            "owner/repo",
            &green_job("main", 9),
            "main"
        ));
        assert!(!issue_matches(
            &infra,
            "owner/repo",
            &green_job("any-branch", 9),
            "main"
        ));
    }

    // ---- AC 3 + safety: workflow/job/repo remain exact --------------------

    #[test]
    fn matching_is_exact_on_repo_workflow_job() {
        let coords = code_coords(1);
        let exact = green_job("main", 2);
        assert!(issue_matches(&coords, "owner/repo", &exact, "main"));
        // A green sibling job on the default branch must NOT match a different job.
        let sibling_job = GreenJob {
            job_name: "deploy".to_string(),
            ..exact.clone()
        };
        assert!(!issue_matches(&coords, "owner/repo", &sibling_job, "main"));
        // A different repo must NOT match.
        assert!(!issue_matches(&coords, "owner/other", &exact, "main"));
    }

    // ---- AC 5: unparseable run index is skipped, never closed unverifiably

    #[test]
    fn an_issue_whose_run_index_cannot_be_parsed_is_skipped() {
        let body = REKEY_BODY.replace(
            "Run: https://dev.a8n.run/contoso/claude-run/actions/runs/4034",
            "Run: https://dev.a8n.run/contoso/claude-run/actions/runs/not-a-number",
        );
        assert!(matches!(
            parse_issue_coords(&inspect(&body, &[])),
            IssueParse::Skip(_)
        ));
    }

    #[test]
    fn infra_kind_marker_is_parsed() {
        let body = format!("{REKEY_BODY}\nInfra-Kind: disk-full");
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(&body, &[])) else {
            panic!("expected Matched");
        };
        assert_eq!(coords.infra_kind, Some("disk-full".to_string()));
        assert_eq!(coords.run_index, 4034);
    }

    #[test]
    fn no_infra_kind_marker_parses_as_a_code_issue() {
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(REKEY_BODY, &[])) else {
            panic!("expected Matched");
        };
        assert_eq!(coords.infra_kind, None);
        assert_eq!(coords.run_index, 4034);
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

    // ---- CLAUDE-198: duplicate collapse -----------------------------------

    /// A reconcile target built from an issue id and a body, as the gather step
    /// produces it.
    fn target(key: &str, body: &str) -> IssueTarget {
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(body, &[])) else {
            panic!("{key}: expected Matched");
        };
        IssueTarget {
            key: key.to_string(),
            coords,
        }
    }

    /// `REKEY_BODY` with its root-cause key (and, optionally, its run index)
    /// swapped, so a fixture can vary only the field under test.
    fn body_with(root_cause: &str, run_index: i64) -> String {
        REKEY_BODY
            .replace(
                "Root-Cause-Key: crdedupecccc",
                &format!("Root-Cause-Key: {root_cause}"),
            )
            .replace("actions/runs/4034", &format!("actions/runs/{run_index}"))
    }

    #[test]
    fn the_root_cause_key_marker_is_parsed() {
        let IssueParse::Matched(coords) = parse_issue_coords(&inspect(REKEY_BODY, &[])) else {
            panic!("expected Matched");
        };
        assert_eq!(coords.root_cause_key, "crdedupecccc");
    }

    #[test]
    fn duplicates_collapse_onto_the_oldest_issue() {
        // Three open issues for the same root-cause key (a lagging tracker index
        // let mint create siblings) plus one for a different key.
        let targets = vec![
            target("CLAUDE-310", &body_with("crdedupeaaaa", 11)),
            target("CLAUDE-9", &body_with("crdedupeaaaa", 9)),
            target("CLAUDE-42", &body_with("crdedupeaaaa", 10)),
            target("CLAUDE-50", &body_with("crdedupezzzz", 12)),
        ];
        let dups = plan_duplicates(&targets);
        // The oldest (CLAUDE-9, not the lexicographically smallest CLAUDE-310)
        // survives; the other two of its group close onto it.
        assert_eq!(
            dups,
            vec![
                DuplicateClose {
                    key: "CLAUDE-42".to_string(),
                    canonical: "CLAUDE-9".to_string(),
                    root_cause_key: "crdedupeaaaa".to_string(),
                },
                DuplicateClose {
                    key: "CLAUDE-310".to_string(),
                    canonical: "CLAUDE-9".to_string(),
                    root_cause_key: "crdedupeaaaa".to_string(),
                },
            ]
        );
    }

    #[test]
    fn a_lone_issue_per_root_cause_key_is_never_a_duplicate() {
        let targets = vec![
            target("CLAUDE-1", &body_with("crdedupeaaaa", 1)),
            target("CLAUDE-2", &body_with("crdedupebbbb", 2)),
        ];
        assert!(plan_duplicates(&targets).is_empty());
    }

    #[test]
    fn the_same_key_on_two_repos_is_not_a_duplicate() {
        // Grouping is on (repo, key), so a marker collision across repos (or a
        // hand-copied body) never closes another repo's issue.
        let a = body_with("crdedupeaaaa", 1);
        let b = a.replace("contoso/claude-run", "initech/chat");
        let targets = vec![target("CLAUDE-1", &a), target("LC-2", &b)];
        assert!(plan_duplicates(&targets).is_empty());
    }

    #[test]
    fn a_pre_rekey_issue_without_a_root_cause_key_is_never_a_duplicate() {
        // Two pre-rekey issues (Branch/Job markers, no Root-Cause-Key) are both
        // left open: an empty key groups nothing.
        let body = REKEY_BODY.replace("Root-Cause-Key: crdedupecccc\n", "");
        let targets = vec![target("CLAUDE-1", &body), target("CLAUDE-2", &body)];
        assert!(targets.iter().all(|t| t.coords.root_cause_key.is_empty()));
        assert!(plan_duplicates(&targets).is_empty());
    }

    #[test]
    fn duplicate_comment_renders_template_and_embeds_marker() {
        let dup = DuplicateClose {
            key: "CLAUDE-42".to_string(),
            canonical: "CLAUDE-9".to_string(),
            root_cause_key: "crdedupeaaaa".to_string(),
        };
        let body =
            build_duplicate_comment(&CiFailureReconcileConfig::default().duplicate_comment, &dup);
        assert!(body.contains("CLAUDE-9"));
        assert!(body.contains("crdedupeaaaa"));
        assert!(!body.contains("{canonical}"));
        assert!(!body.contains("{root_cause_key}"));
        // Same hidden marker as the now-green close: a closed duplicate is never
        // re-processed even if it is later reopened by hand.
        assert!(matches!(
            parse_issue_coords(&inspect(REKEY_BODY, &[&body])),
            IssueParse::AlreadyClosed
        ));
    }

    #[test]
    fn issue_order_sorts_by_project_then_number() {
        assert!(issue_order("CLAUDE-9") < issue_order("CLAUDE-10"));
        assert!(issue_order("CLAUDE-10") < issue_order("CLAUDE-310"));
        assert!(issue_order("CLAUDE-1") < issue_order("LC-1"));
        // An id with no numeric suffix still orders totally (no panic).
        assert_eq!(issue_order("weird"), ("weird".to_string(), 0));
    }

    /// AC 4: replay of a real failing-then-passing CI sequence, with a duplicate
    /// pair in the mix, asserting NO issue of the sequence stays open.
    ///
    /// Sequence: run #10 of `check.yml` on `main` fails at job `check`, minting
    /// CLAUDE-9; a tracker-index lag lets a second failure mint CLAUDE-42 for the
    /// same root-cause key. Run #11 then passes. The reconciler must close
    /// CLAUDE-42 as a duplicate of CLAUDE-9 and CLAUDE-9 as now-green.
    #[test]
    fn failing_then_passing_sequence_leaves_no_duplicate_open() {
        let open = vec![
            target("CLAUDE-9", &body_with("crdedupeaaaa", 10)),
            target("CLAUDE-42", &body_with("crdedupeaaaa", 10)),
        ];

        // 1. Duplicate collapse: CLAUDE-42 closes onto CLAUDE-9.
        let dups = plan_duplicates(&open);
        assert_eq!(dups.len(), 1);
        assert_eq!(dups[0].key, "CLAUDE-42");
        assert_eq!(dups[0].canonical, "CLAUDE-9");
        let dup_keys: HashSet<&str> = dups.iter().map(|d| d.key.as_str()).collect();
        let remaining: Vec<&IssueTarget> = open
            .iter()
            .filter(|t| !dup_keys.contains(t.key.as_str()))
            .collect();
        assert_eq!(remaining.len(), 1, "only the canonical issue is scanned");

        // 2. Forgejo sequence: run #10 failed, run #11 passed, same branch and
        // workflow. Reduction keeps #11, whose `check` job is success.
        let runs = vec![
            run(100, 10, "check.yml", "main", "failure"),
            run(105, 11, "check.yml", "main", "success"),
        ];
        let latest = reduce_latest_runs(&runs);
        assert_eq!(latest.len(), 1);
        let green = now_green_jobs(&latest[0], &[job("check", "success")]);

        // 3. The canonical issue matches the now-green job, so it closes too.
        let canonical = remaining[0];
        assert!(green.iter().any(|g| issue_matches(
            &canonical.coords,
            "contoso/claude-run",
            g,
            "main"
        )));

        // Nothing from the sequence is left open.
        let closed: HashSet<&str> = dup_keys
            .iter()
            .copied()
            .chain(std::iter::once(canonical.key.as_str()))
            .collect();
        assert_eq!(closed.len(), open.len(), "every open issue was closed");
    }
}
