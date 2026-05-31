//! Native YouTrack action handler (CLAUDE-32, Phase 3 of the native-action
//! epic CLAUDE-25).
//!
//! Ports the scripted `actions/youtrack.yml` pipeline to typed Rust on the
//! Phase 1 dispatch seam ([`super`]) and the Phase 2 provider abstraction
//! ([`crate::provider`]). The pipeline is:
//!
//! ```text
//! gather  -> IssueTracker::list_queued + resolve_vcs per project (drop no-VCS)
//! filter  -> live AI Agent re-read; skip unless still Queued
//! pre     -> AI Agent=Working, State=In Progress, git clone --depth 50
//! prompt  -> build the CLAUDE-24 local-only instruction text in Rust
//! claude  -> ClaudeInvoker with the action's claude block + timeout
//! post    -> finalize state machine, runs on EVERY path (success, claude
//!            error, timeout, pre/post failure, cancel):
//!              blocker  (.claude-run-needs-review) -> AI Agent=Needs Review, no PR
//!              success  -> Forge::create_pr, AI Agent=Code Review
//!                          (PR-create failure -> AI Agent=Failed)
//!              failure  -> AI Agent=Failed, no PR
//! ```
//!
//! State is never set to Done here; it stays In Progress until a human merges
//! the PR. Every external mutation (PR, comment, AI Agent transition) happens
//! in [`finalize`], after claude exits, gated on the outcome - matching the
//! merged CLAUDE-24 runner-creates-PR contract. The agent itself does only
//! local engineering work (no push, no PR, no YouTrack write).

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use sqlx::SqlitePool;
use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{NativeConfig, YoutrackSource};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::{ClaudeInvoker, ClaudeResult};
use crate::concurrency::Executor;
use crate::config::Paths;
use crate::error::{Error, Result};
use crate::job::{self, EffectiveLimits, ProcessedJob, RunnerDefaults};
use crate::provider::{self, Backend, CliBackend, CommandRunner, Forge, PullRequest};
use crate::secrets::{self, JobCredentials};
use crate::state;
use crate::workdir;

/// Workdir marker the agent writes to park a job pending human input.
const NEEDS_REVIEW_MARKER: &str = ".claude-run-needs-review";
/// Workdir marker that suppresses attempt counting for this run.
const SKIP_ATTEMPT_MARKER: &str = ".claude-run-skip-attempt";

/// A typed YouTrack job: an eligible issue plus the repo resolved from its
/// project's enabled VCS integration.
#[derive(Debug, Clone)]
struct YoutrackJob {
    /// Issue id (`idReadable`, e.g. `MK-16`); also the dedup / attempt key.
    key: String,
    summary: String,
    /// Forge server URL, trailing slash trimmed (e.g. `https://dev.a8n.run`).
    repo_host: String,
    /// Repo path (e.g. `contoso/claude-run`).
    repo_path: String,
    /// Full clone URL: `<repo_host>/<repo_path>.git`.
    clone_url: String,
}

/// Built-in YouTrack handler (CLAUDE-32). Routed to by [`super::handler_for`].
pub struct YoutrackHandler;

impl NativeHandler for YoutrackHandler {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move { run_youtrack(ctx).await })
    }
}

/// Drive a full YouTrack run: caps/budget, gather, then the per-job pipeline,
/// mirroring [`crate::daemon::run_action_inner`]'s run accounting (which native
/// actions bypass) while substituting the typed YouTrack pipeline for the
/// generic nu-snippet one.
async fn run_youtrack(ctx: NativeContext<'_>) -> Result<()> {
    let loaded = ctx.action;
    let action = &loaded.action;
    let pool = ctx.pool;

    // Typed config (CLAUDE-33). Validation guarantees a matching `native` block
    // for `kind: youtrack`, so anything else is a misrouted action.
    let (src, forge, gh) = match &action.native {
        Some(NativeConfig::Youtrack(cfg)) => (&cfg.youtrack, &cfg.forgejo, &cfg.github),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    let defaults = RunnerDefaults::default();
    let limits = job::effective_limits(&defaults, loaded);

    // Pre-run daily caps.
    let daily = state::daily_count_get(pool, &action.name).await?;
    if daily >= limits.max_runs_per_day {
        warn!(count = daily, "daily cap reached");
        return Ok(());
    }
    let cost = state::daily_cost_get(pool, &action.name).await?;
    if cost >= limits.daily_budget_usd {
        warn!(cost_usd = cost, "daily budget reached");
        return Ok(());
    }

    // Resolve a CLI runner (CLI-first). Errors clearly if yt/fj absent. The
    // runner is shared across the CLAUDE-28 per-job tasks, each of which builds
    // its own (optionally credentialed) backend from it.
    let runner = provider::resolve_runner(provider::DEFAULT_PRIORITY)?;

    // CLAUDE-26: resolve any `token_ref` to a decrypted credential once per run.
    // An all-None set (the shipped, host-auth config) materializes nothing and
    // every backend below falls back to host `yt` / `fj` auth.
    let creds = Arc::new(secrets::resolve_youtrack_credentials(pool, src, forge, gh).await?);
    let cred_base = ctx.paths.runtime_dir.join("credentials").join(&action.name);

    // gather (before run_begin: a gather failure is a no-op run, same as the
    // scripted path). gather only invokes `yt`, so its credentials are yt-only.
    let jobs = {
        let (gather_backend, _gather_guard) = if creds.is_empty() {
            (CliBackend::new(runner.clone()), None)
        } else {
            let dir = cred_base.join(format!("gather-{}", std::process::id()));
            let guard = JobCredentials::materialize(&dir, &creds, None)?;
            let backend = credentialed_backend(&runner, &guard);
            (backend, Some(guard))
        };
        gather(&gather_backend, &src.query)?
    };
    info!(action = %action.name, count = jobs.len(), "youtrack jobs gathered");

    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;

    // CLAUDE-9: register a cancellation token and ensure it is unregistered on
    // every exit path (including the `?` early returns below).
    let cancel = match ctx.registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard {
        registry: ctx.registry,
        run_id,
    };

    // CLAUDE-1: record planned job keys so the web UI shows what is queued.
    let planned_keys: Vec<String> = jobs.iter().map(|j| j.key.clone()).collect();
    if let Err(e) = state::run_set_job_keys(pool, run_id, &planned_keys).await {
        warn!(error = %e, "run_set_job_keys failed (non-fatal)");
    }

    // CLAUDE-28: keyed, bounded-parallel execution. Distinct repos (the
    // concurrency key) run in parallel; jobs against the same repo serialize so
    // they never collide on the clone dir / branch / PR. `max_parallel: 1`
    // reproduces the original strictly-sequential behavior.
    let max_parallel = action.concurrency.max_parallel.max(1) as usize;
    let key_fields = action.concurrency.key.clone();
    let loaded_arc = Arc::new(loaded.clone());
    let paths_owned = ctx.paths.clone();
    let pool_owned = pool.clone();
    let src_owned = src.clone();
    let gh_owned = gh.clone();

    let mut items: Vec<(String, crate::concurrency::JobFuture)> = Vec::with_capacity(jobs.len());
    for job in jobs.into_iter() {
        let gate_key = concurrency_key(&key_fields, &job);
        let loaded = loaded_arc.clone();
        let paths = paths_owned.clone();
        let pool = pool_owned.clone();
        let claude = ctx.claude.clone();
        let runner = runner.clone();
        let creds = creds.clone();
        let cred_base = cred_base.clone();
        let src = src_owned.clone();
        let gh = gh_owned.clone();
        let job_cancel = cancel.clone();
        let fut: crate::concurrency::JobFuture = Box::pin(async move {
            let action = &loaded.action;
            // CLAUDE-28: best-effort cap/budget check before launch; bounded
            // overshoot of at most `max_parallel - 1`.
            if state::daily_count_get(&pool, &action.name).await? >= limits.max_runs_per_day {
                warn!(action = %action.name, "daily cap reached, skipping job");
                return Ok(false);
            }
            if state::daily_cost_get(&pool, &action.name).await? >= limits.daily_budget_usd {
                warn!(action = %action.name, "daily budget reached, skipping job");
                return Ok(false);
            }
            // CLAUDE-26: materialize this job's own credential files (yt config +
            // forge keys.json keyed by the issue's resolved VCS host). The guard
            // scrubs them when the job's provider calls finish; concurrent jobs
            // never share a credential file. No token_ref -> host auth, no files.
            let (job_backend, _job_creds): (CliBackend, Option<JobCredentials>) =
                if creds.is_empty() {
                    (CliBackend::new(runner.clone()), None)
                } else {
                    let dir = cred_base
                        .join(format!("run-{run_id}"))
                        .join(sanitize_key(&job.key));
                    let host = host_only(&job.repo_host);
                    let guard = JobCredentials::materialize(&dir, &creds, Some(&host))?;
                    let backend = credentialed_backend(&runner, &guard);
                    (backend, Some(guard))
                };
            if let Err(e) = state::run_inflight_add(&pool, run_id, &job.key).await {
                warn!(error = %e, "run_inflight_add failed (non-fatal)");
            }
            // CLAUDE-50: pick the Forge by the job's resolved host. A GitHub host
            // (github.com plus any configured GHE host) routes to the GitHub
            // backend; every other host reuses the credentialed `fj` backend
            // unchanged. The GitHub PAT (when an action set `github.token_ref`)
            // comes from this job's materialized credentials.
            let host = host_only(&job.repo_host);
            let gh_forge = if provider::is_github_host(&host, &gh.hosts) {
                let token = _job_creds.as_ref().and_then(|c| c.github_token.clone());
                Some(provider::GithubForge::new(
                    runner.clone(),
                    gh.mcp_endpoint.clone(),
                    token,
                ))
            } else {
                None
            };
            let forge: &dyn Forge = match &gh_forge {
                Some(f) => f,
                None => &job_backend,
            };
            let outcome = process_job(
                loaded.as_ref(),
                &paths,
                &pool,
                claude.as_ref(),
                &job_backend,
                forge,
                &src,
                &limits,
                &job_cancel,
                &job,
            )
            .await;
            if let Err(e) = state::run_inflight_remove(&pool, run_id, &job.key).await {
                warn!(error = %e, "run_inflight_remove failed (non-fatal)");
            }
            let outcome = outcome?;
            record_outcome(loaded.as_ref(), &paths, &pool, run_id, &outcome).await?;
            Ok::<bool, crate::error::Error>(true)
        });
        items.push((gate_key, fut));
    }

    let exec = Executor {
        max_parallel,
        gate: ctx.keygate.clone(),
        cancel: cancel.clone(),
    };
    let results = exec.run(items).await;
    let mut processed: u32 = 0;
    let mut first_err: Option<crate::error::Error> = None;
    for r in results {
        match r {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    let exit_code = if cancel.is_cancelled() {
        crate::exit::CANCELLED
    } else {
        crate::exit::OK
    };
    state::run_finish(pool, run_id, exit_code, processed).await?;
    if let Some(e) = first_err {
        warn!(action = %action.name, error = %e, "one or more jobs errored during the run");
    }
    info!(action = %action.name, processed, exit_code, "run complete");
    Ok(())
}

/// CLAUDE-28: resolve a YouTrack job's concurrency key from the configured key
/// fields. `project` is the issue id's prefix, `repo` is the resolved repo path;
/// `workflow` is reserved for a future action and unset today (it contributes
/// nothing). Falls back to the issue id when no field resolves, so each job is
/// treated as independent rather than all sharing one empty key.
fn concurrency_key(fields: &[String], job: &YoutrackJob) -> String {
    let mut parts: Vec<String> = Vec::new();
    for field in fields {
        let value = match field.as_str() {
            "project" => project_key(&job.key),
            "repo" => job.repo_path.clone(),
            // `workflow` is forward-looking; the youtrack action does not
            // populate it, so it contributes nothing to the key today.
            _ => String::new(),
        };
        if !value.is_empty() {
            parts.push(value);
        }
    }
    if parts.is_empty() {
        job.key.clone()
    } else {
        parts.join("\u{1f}")
    }
}

/// CLAUDE-26: build a [`CliBackend`] from `runner`, applying a job's materialized
/// credentials (yt config env, fj store env, and the temp `keys.json` path so
/// `clone_url` reads the same store). An empty credential set leaves host auth.
fn credentialed_backend(runner: &Arc<dyn CommandRunner>, creds: &JobCredentials) -> CliBackend {
    let mut backend = CliBackend::new(runner.clone());
    if !creds.yt_env.is_empty() {
        backend = backend.with_yt_env(creds.yt_env.clone());
    }
    if !creds.fj_env.is_empty() {
        backend = backend.with_fj_env(creds.fj_env.clone());
    }
    if let Some(path) = &creds.fj_keys_path {
        backend = backend.with_keys_path(path.clone());
    }
    backend
}

/// Make a job key safe as a single path component for the per-job credential
/// dir (issue ids like `MK-16` are already safe; defensively strip separators).
fn sanitize_key(key: &str) -> String {
    key.replace(['/', '\\', '.'], "_")
}

/// gather: list queued issues, resolve each project's VCS once (cached), and
/// drop issues whose project has no usable VCS integration. Mirrors the
/// scripted `input.command`.
fn gather(backend: &dyn Backend, query: &str) -> Result<Vec<YoutrackJob>> {
    let issues = backend.list_queued(query)?;
    let mut vcs_cache: std::collections::HashMap<String, Option<provider::VcsRepo>> =
        std::collections::HashMap::new();
    let mut jobs = Vec::new();
    for issue in &issues {
        let Some(key) = issue.get("idReadable").and_then(|v| v.as_str()) else {
            warn!("skipping issue with no idReadable");
            continue;
        };
        let project = project_key(key);
        if !vcs_cache.contains_key(&project) {
            let resolved = match backend.resolve_vcs(&project) {
                Ok(r) => Some(r),
                Err(e) => {
                    info!(project = %project, error = %e, "dropping issues: no usable VCS integration");
                    None
                }
            };
            vcs_cache.insert(project.clone(), resolved);
        }
        // Drop issues whose project has no enabled VCS; they cannot be cloned.
        let Some(repo) = vcs_cache.get(&project).and_then(|r| r.clone()) else {
            continue;
        };
        let summary = issue
            .get("summary")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        jobs.push(YoutrackJob {
            key: key.to_string(),
            summary,
            repo_host: repo.repo_host,
            repo_path: repo.repo_path,
            clone_url: repo.clone_url,
        });
    }
    Ok(jobs)
}

/// Project key prefix of an issue id (`MK-16` -> `MK`).
fn project_key(idreadable: &str) -> String {
    idreadable
        .split('-')
        .next()
        .unwrap_or(idreadable)
        .to_string()
}

/// Process a single job through filter / pre / prompt / claude / finalize,
/// returning the outcome record. Mirrors [`crate::job::process_job`]'s
/// contracts: finalize runs on every path that reached the workdir, transient
/// claude errors skip finalize + attempt counting, the skip-attempt marker (or
/// a cancel) suppresses attempt counting, and the daily ledger advances only
/// when claude actually ran.
#[allow(clippy::too_many_arguments)]
async fn process_job(
    loaded: &crate::action::LoadedAction,
    paths: &Paths,
    pool: &SqlitePool,
    claude: &ClaudeInvoker,
    backend: &dyn Backend,
    forge: &dyn Forge,
    src: &YoutrackSource,
    limits: &EffectiveLimits,
    cancel: &CancellationToken,
    job: &YoutrackJob,
) -> Result<ProcessedJob> {
    let started = Instant::now();
    let action = &loaded.action;
    let key = &job.key;
    let attempts = state::attempts_get(pool, &action.name, key).await?;

    // YouTrack jobs always carry a stable key (idReadable), so the attempt cap
    // always applies.
    if attempts >= limits.max_attempts_per_key {
        info!(action = %action.name, key = %key, attempts, "skipping job, attempt cap reached");
        return Ok(ProcessedJob::CapHit { key: key.clone() });
    }

    info!(action = %action.name, key = %key, attempt = attempts + 1, "processing job");

    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), paths);
    let workdir_path = workdir::job_workdir(&wb_base, &action.name, key);

    // ---- filter: live AI Agent re-read ----
    match backend.get_field(key, &src.custom_field) {
        Ok(v) if v.trim() == src.states.queued => {}
        Ok(v) => {
            info!(action = %action.name, key = %key, ai_agent = %v.trim(), "job filtered out");
            return Ok(ProcessedJob::Filtered { key: key.clone() });
        }
        Err(e) => {
            // Conservative: skip on lookup failure so a transient error cannot
            // let an ineligible issue be worked. A still-Queued job retries on
            // the next firing.
            warn!(action = %action.name, key = %key, error = %e, "AI Agent lookup failed; skipping");
            return Ok(ProcessedJob::Filtered { key: key.clone() });
        }
    }

    std::fs::create_dir_all(&workdir_path)?;

    // ---- pre: mark Working / In Progress, then clone ----
    let pre_result = run_pre(backend, src, job, &workdir_path);

    // ---- prompt + claude (skipped on pre failure / cancel) ----
    let (claude_result, claude_ran) = match pre_result {
        Err(reason) => {
            warn!(action = %action.name, key = %key, %reason, "pre step failed (finalize still runs)");
            (ClaudeResult::synthesized("pre step failed", reason), false)
        }
        Ok(()) if cancel.is_cancelled() => {
            warn!(action = %action.name, key = %key, "run cancelled before claude started (finalize still runs)");
            (
                ClaudeResult::synthesized(
                    "run cancelled by operator",
                    "cancelled before claude started",
                ),
                false,
            )
        }
        Ok(()) => {
            let prompt = build_prompt(job, &workdir_path);
            let stream_log =
                ClaudeInvoker::stream_log_path(&paths.logs_dir(), &action.name, key, attempts + 1);
            info!(action = %action.name, key = %key, stream_log = %stream_log.display(), "invoking claude");
            let r = claude.invoke(
                &prompt,
                &action.claude,
                limits.claude_timeout,
                &workdir_path,
                &stream_log,
                cancel,
            )?;
            if cancel.is_cancelled() {
                // The token tripped mid-run; `invoke` already terminated the
                // child. Synthesize a cancelled result so finalize runs and no
                // real cost is charged.
                warn!(action = %action.name, key = %key, "run cancelled mid-claude (finalize still runs)");
                (
                    ClaudeResult::synthesized(
                        "run cancelled by operator",
                        "claude run cancelled before completion",
                    ),
                    false,
                )
            } else {
                if !r.transient {
                    if r.ok {
                        info!(action = %action.name, key = %key, cost = r.cost, "claude completed");
                    } else {
                        warn!(action = %action.name, key = %key, error = %r.error, "claude run failed (finalize still runs)");
                    }
                }
                (r, true)
            }
        }
    };

    // Transient claude error: no finalize, no attempt counted, no daily count.
    if claude_result.transient {
        info!(action = %action.name, key = %key, error = %claude_result.error, "transient claude error, not counting attempt");
        cleanup_workdir(&workdir_path);
        return Ok(ProcessedJob::Transient { key: key.clone() });
    }

    if claude_ran {
        state::daily_cost_add(pool, &action.name, claude_result.cost).await?;
    }

    // ---- finalize (the native "post"): every external mutation lives here ----
    let finalize_res = finalize(backend, forge, src, job, &workdir_path, &claude_result);
    let post_failed = match &finalize_res {
        Ok(Finalized::CodeReview { pr_url }) => {
            info!(action = %action.name, key = %key, pr_url = %pr_url, "finalized: Code Review");
            false
        }
        Ok(other) => {
            info!(action = %action.name, key = %key, outcome = %other.as_str(), "finalized");
            false
        }
        Err(e) => {
            warn!(action = %action.name, key = %key, error = %e, "finalize failed");
            true
        }
    };

    // ---- skip-attempt marker (parked / blocker) or cancel ----
    let skip_marker = workdir_path.join(SKIP_ATTEMPT_MARKER);
    let cancelled = cancel.is_cancelled();
    let skip_attempt = skip_marker.exists() || cancelled;
    if skip_attempt {
        info!(action = %action.name, key = %key, cancelled, "not counting attempt");
    }

    let ok = claude_result.ok && !post_failed;
    let stream_log_path = if claude_ran {
        Some(
            ClaudeInvoker::stream_log_path(&paths.logs_dir(), &action.name, key, attempts + 1)
                .display()
                .to_string(),
        )
    } else {
        None
    };

    if !skip_attempt {
        state::attempts_increment(pool, &action.name, key).await?;
    }
    if claude_ran {
        state::daily_count_increment(pool, &action.name).await?;
    }

    cleanup_workdir(&workdir_path);

    Ok(ProcessedJob::Done {
        key: key.clone(),
        attempt: attempts + 1,
        ok,
        claude_ran,
        claude_ok: claude_result.ok,
        claude_cost: claude_result.cost,
        attempt_counted: !skip_attempt,
        summary: claude_result.summary,
        stream_log_path,
        duration_ms: started.elapsed().as_millis() as i64,
    })
}

/// Pre steps: mark the issue Working + In Progress, then clone the repo into
/// `workdir`. Returns the failure reason on any step error so the caller can
/// synthesize a failed claude result and still finalize.
fn run_pre(
    backend: &dyn Backend,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
) -> std::result::Result<(), String> {
    backend
        .set_field(&job.key, &src.custom_field, &src.states.working)
        .map_err(|e| format!("set {}={}: {e}", src.custom_field, src.states.working))?;
    backend
        .set_state(&job.key, &src.states.in_progress)
        .map_err(|e| format!("set State={}: {e}", src.states.in_progress))?;
    clone_repo(&job.clone_url, workdir).map_err(|e| format!("git clone: {e}"))?;
    Ok(())
}

/// Shallow-clone `clone_url` into `workdir` (already created, empty). Host git
/// identity handles auth.
fn clone_repo(clone_url: &str, workdir: &Path) -> Result<()> {
    let status = Command::new("git")
        .arg("clone")
        .arg("--depth")
        .arg("50")
        .arg(clone_url)
        .arg(workdir)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::Other(anyhow::anyhow!(
            "git clone exited {}",
            status.code().unwrap_or(-1)
        )))
    }
}

/// Build the agent's instruction text. Local-only work: no push, no PR, no
/// YouTrack write. Matches the merged CLAUDE-24 prompt from the scripted
/// `actions/youtrack.yml`.
fn build_prompt(job: &YoutrackJob, workdir: &Path) -> String {
    let key = &job.key;
    let wd = workdir.display();
    [
        "You are working a YouTrack issue end-to-end. Follow the YouTrack and Git workflows from CLAUDE.md.".to_string(),
        String::new(),
        format!("Issue: {key}: {}", job.summary),
        format!("Repo: {}", job.clone_url),
        format!("Working directory: {wd} - the repo is already cloned here, you are inside it."),
        String::new(),
        "Do ONLY local engineering work. Do NOT push, do NOT open a PR, and do NOT touch YouTrack (no 'yt issue set-field', no 'yt issue set-state', no comments). The runner finalizes the issue after you exit: it opens the PR from your local commits and sets the AI Agent field based on the run outcome.".to_string(),
        String::new(),
        "Steps:".to_string(),
        format!("1. Run 'yt issue inspect {key}' to read the full description and acceptance criteria."),
        "2. Create a feature branch off the default branch per the Git workflow in CLAUDE.md.".to_string(),
        "3. Make the minimum change required to satisfy the acceptance criteria.".to_string(),
        format!("4. Commit with a message that references the issue on its own line as '#{key}' (a bare reference, with NO 'State ...' command after it). This links the PR to the issue in YouTrack without transitioning it. Do NOT write 'State Done' - the issue must stay In Progress until a human merges the PR."),
        "5. Run the target repo's check suite synchronously to green (e.g. the repo's 'just check', tests, or build). Run it in the foreground and wait for it inline; do NOT background a long-running job and poll/wait on it across turns - that is what blew the run timeout previously.".to_string(),
        "6. Stop. Do not push, do not open a PR, do not touch YouTrack. The runner opens the PR from your commits and transitions the issue.".to_string(),
        String::new(),
        "If you CANNOT safely complete the issue - missing context, ambiguous acceptance criteria, out-of-scope work, dependency on another issue:".to_string(),
        "  a. Do NOT change any code.".to_string(),
        "  b. Write the human-facing explanation of exactly what information or decision you need to proceed to this file (the file contents are the reason, posted verbatim as the issue comment by the runner):".to_string(),
        format!("       {wd}/{NEEDS_REVIEW_MARKER}"),
        "  c. Touch this marker file so the runner does NOT count this attempt toward the per-key cap (the job is parked pending human input, not failed):".to_string(),
        format!("       {wd}/{SKIP_ATTEMPT_MARKER}"),
        "  d. Do NOT touch YouTrack yourself; the runner reads the marker and sets AI Agent to 'Needs Review'.".to_string(),
        String::new(),
        "When you finish, summarize what you did in plain text. Do not include a PR URL - the runner creates the PR.".to_string(),
    ]
    .join("\n")
}

/// Outcome of [`finalize`]: which AI Agent transition was applied. Carried into
/// the log line and asserted in the unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Finalized {
    /// Blocker: agent parked the job; AI Agent=Needs Review, no PR.
    NeedsReview,
    /// Success: PR opened; AI Agent=Code Review.
    CodeReview { pr_url: String },
    /// Hard failure (claude error/timeout, pre failure, or PR-create failure);
    /// AI Agent=Failed, no PR (or a failed PR).
    Failed,
}

impl Finalized {
    fn as_str(&self) -> &'static str {
        match self {
            Finalized::NeedsReview => "Needs Review",
            Finalized::CodeReview { .. } => "Code Review",
            Finalized::Failed => "Failed",
        }
    }
}

/// The finalize state machine (the native "post"). Runs on every path. Mirrors
/// the single finalize `post` step in `actions/youtrack.yml`: the comment is
/// best-effort (a comment failure cannot strand the AI Agent field on Working),
/// and the AI Agent write is the authoritative, last operation. State is never
/// set to Done; it stays In Progress until a human merges the PR.
fn finalize(
    backend: &dyn Backend,
    forge: &dyn Forge,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    claude_result: &ClaudeResult,
) -> Result<Finalized> {
    let needs_review = workdir.join(NEEDS_REVIEW_MARKER);
    let skip_marker = workdir.join(SKIP_ATTEMPT_MARKER);

    if needs_review.exists() {
        // Blocker: ensure the skip marker is present so this attempt is not
        // counted, surface the reason verbatim, AI Agent=Needs Review, no PR.
        if !skip_marker.exists() {
            std::fs::write(&skip_marker, b"")?;
        }
        let reason = std::fs::read_to_string(&needs_review)?.trim().to_string();
        comment_then_field(backend, src, &job.key, &reason, &src.states.needs_review)?;
        Ok(Finalized::NeedsReview)
    } else if claude_result.ok {
        // Success: open the PR from the agent's local commits via the resolved
        // Forge (CLAUDE-50: `fj` agit+autofill for Forgejo hosts, the GitHub
        // backend for GitHub hosts), then report it. Both derive the title/body
        // from the commit messages, which already carry the bare #<KEY>-N
        // reference.
        let host = host_only(&job.repo_host);
        match forge.create_pr(workdir, &host) {
            Ok(pr) => {
                let url = pr_url(job, &pr);
                let body = success_comment(claude_result, &url);
                comment_then_field(backend, src, &job.key, &body, &src.states.code_review)?;
                Ok(Finalized::CodeReview { pr_url: url })
            }
            Err(e) => {
                let body = pr_failure_comment(claude_result, &e);
                comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
                Ok(Finalized::Failed)
            }
        }
    } else {
        // Failure: timeout (exit 124), crash, empty prompt, or pre failure.
        let body = failure_comment(claude_result);
        comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
        Ok(Finalized::Failed)
    }
}

/// Comment first (best-effort), then set AI Agent last as the authoritative
/// write. A comment failure is logged and ignored so it cannot prevent the
/// field from advancing off Working; a set_field failure propagates and marks
/// the job's finalize as failed.
fn comment_then_field(
    backend: &dyn Backend,
    src: &YoutrackSource,
    key: &str,
    body: &str,
    target: &str,
) -> Result<()> {
    if let Err(e) = backend.comment(key, body) {
        warn!(key = %key, error = %e, "issue comment failed (continuing to AI Agent write)");
    }
    backend.set_field(key, &src.custom_field, target)
}

/// Bare host of a forge server URL: `https://dev.a8n.run` -> `dev.a8n.run`,
/// for `fj --host <host>`.
fn host_only(repo_host: &str) -> String {
    repo_host
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(repo_host)
        .to_string()
}

/// PR URL for the issue comment. When the forge returned a real HTTP URL (the
/// GitHub backend reports the PR `html_url`), use it verbatim. Otherwise (the
/// `fj` backend reports a `Created pull request #<n>` line, not a URL) render the
/// parsed number against the Forgejo `pulls` path, falling back to the raw line.
fn pr_url(job: &YoutrackJob, pr: &PullRequest) -> String {
    if pr.url.starts_with("https://") || pr.url.starts_with("http://") {
        return pr.url.clone();
    }
    match pr.number {
        Some(n) => format!("{}/{}/pulls/{}", job.repo_host, job.repo_path, n),
        None => pr.url.clone(),
    }
}

fn success_comment(r: &ClaudeResult, pr_url: &str) -> String {
    [
        "Claude AI agent run summary:".to_string(),
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n")
}

fn pr_failure_comment(r: &ClaudeResult, err: &Error) -> String {
    [
        "Claude AI agent run completed but PR creation FAILED:".to_string(),
        String::new(),
        err.to_string(),
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n")
}

fn failure_comment(r: &ClaudeResult) -> String {
    [
        format!("Claude AI agent run FAILED ({}):", r.error),
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n")
}

/// Map a `ProcessedJob::Done` into the JSONL record + `job_outcomes` insert,
/// identical to the scripted path in [`crate::daemon::run_action_inner`].
async fn record_outcome(
    loaded: &crate::action::LoadedAction,
    paths: &Paths,
    pool: &SqlitePool,
    run_id: i64,
    outcome: &ProcessedJob,
) -> Result<()> {
    let Some(record) = job::done_record(outcome) else {
        return Ok(());
    };
    job::append_output_record(paths, loaded, &record);
    if let ProcessedJob::Done {
        key,
        attempt,
        ok,
        claude_ran,
        claude_ok,
        claude_cost,
        attempt_counted,
        summary,
        stream_log_path,
        duration_ms,
    } = outcome
    {
        state::job_outcome_insert(
            pool,
            &state::JobOutcome {
                run_id,
                action_name: &loaded.action.name,
                job_key: key,
                attempt: *attempt,
                ok: *ok,
                claude_ran: *claude_ran,
                claude_ok: *claude_ok,
                claude_cost_usd: *claude_cost,
                attempt_counted: *attempt_counted,
                summary: Some(summary.as_str()),
                stream_log_path: stream_log_path.as_deref(),
                duration_ms: *duration_ms,
            },
        )
        .await?;
    }
    Ok(())
}

fn cleanup_workdir(workdir: &Path) {
    if workdir.exists() {
        if let Err(e) = std::fs::remove_dir_all(workdir) {
            warn!(workdir = %workdir.display(), error = %e, "workdir cleanup failed (continuing)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;
    use crate::provider::CliBackend;
    use serde_json::Value;
    use std::sync::Arc;

    fn job() -> YoutrackJob {
        YoutrackJob {
            key: "MK-16".into(),
            summary: "Do the thing".into(),
            repo_host: "https://dev.a8n.run".into(),
            repo_path: "contoso/claude-run".into(),
            clone_url: "https://dev.a8n.run/contoso/claude-run.git".into(),
        }
    }

    fn src() -> YoutrackSource {
        YoutrackSource::default()
    }

    fn ok_result() -> ClaudeResult {
        ClaudeResult {
            ok: true,
            summary: "did the work".into(),
            cost: 0.5,
            raw: Value::Null,
            error: String::new(),
            transient: false,
        }
    }

    #[test]
    fn project_key_splits_idreadable() {
        assert_eq!(project_key("MK-16"), "MK");
        assert_eq!(project_key("CLAUDE-32"), "CLAUDE");
        assert_eq!(project_key("NODASH"), "NODASH");
    }

    #[test]
    fn host_only_strips_scheme_and_path() {
        assert_eq!(host_only("https://dev.a8n.run"), "dev.a8n.run");
        assert_eq!(host_only("http://x.test/"), "x.test");
        assert_eq!(host_only("dev.a8n.run"), "dev.a8n.run");
    }

    #[test]
    fn pr_url_prefers_number_then_falls_back() {
        let j = job();
        let with_num = PullRequest {
            number: Some(7),
            url: "raw line".into(),
        };
        assert_eq!(
            pr_url(&j, &with_num),
            "https://dev.a8n.run/contoso/claude-run/pulls/7"
        );
        let no_num = PullRequest {
            number: None,
            url: "fallback line".into(),
        };
        assert_eq!(pr_url(&j, &no_num), "fallback line");
        // CLAUDE-50: a real HTTP URL (the GitHub backend's html_url) is used
        // verbatim rather than rebuilt against the Forgejo `pulls` path.
        let gh = PullRequest {
            number: Some(11),
            url: "https://github.com/octo/widget/pull/11".into(),
        };
        assert_eq!(pr_url(&j, &gh), "https://github.com/octo/widget/pull/11");
    }

    #[test]
    fn gather_merges_repo_and_drops_projects_without_vcs() {
        let issues =
            r#"[{"idReadable":"MK-16","summary":"a"},{"idReadable":"ZZ-1","summary":"b"}]"#;
        let runner = StubRunner::new()
            .ok(
                "yt",
                &["list", "--query=-Resolved AI Agent: Queued", "--json"],
                issues,
            )
            .ok(
                "yt",
                &["project", "vcs", "MK", "--enabled-only", "--json"],
                r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#,
            )
            // ZZ has no enabled VCS -> its issue is dropped.
            .ok(
                "yt",
                &["project", "vcs", "ZZ", "--enabled-only", "--json"],
                "[]",
            );
        let backend = CliBackend::new(Arc::new(runner));
        let jobs = gather(&backend, &src().query).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].key, "MK-16");
        assert_eq!(jobs[0].summary, "a");
        assert_eq!(
            jobs[0].clone_url,
            "https://dev.a8n.run/contoso/claude-run.git"
        );
    }

    #[test]
    fn finalize_needs_review_parks_and_writes_skip_marker() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NEEDS_REVIEW_MARKER), "need a decision on X").unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Needs Review"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            wd,
            &ClaudeResult::synthesized("e", "s"),
        )
        .unwrap();
        assert_eq!(res, Finalized::NeedsReview);
        // The skip-attempt marker is created if the agent didn't already.
        assert!(wd.join(SKIP_ATTEMPT_MARKER).exists());
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
        // No PR on the park path.
        assert!(!calls.iter().any(|c| c.starts_with("fj ")), "{calls:?}");
    }

    #[test]
    fn finalize_success_creates_pr_and_sets_code_review() {
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA"],
                    "Created pull request #7",
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(&backend, &backend, &src(), &job(), dir.path(), &ok_result()).unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into()
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr create -aA"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Code Review"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_github_job_opens_pr_via_forge_and_sets_code_review() {
        // CLAUDE-50: a GitHub job finalizes through the GitHub `Forge`. The PR's
        // real html_url is linked exactly as the Forgejo path links its PR, and
        // AI Agent transitions to Code Review.
        use crate::provider::{GithubForge, McpTransport};
        use std::sync::Mutex;

        struct OkTransport(Mutex<Vec<String>>);
        impl McpTransport for OkTransport {
            fn initialize(&self, _e: &str, _t: Option<&str>) -> Result<()> {
                self.0.lock().unwrap().push("initialize".into());
                Ok(())
            }
            fn call_tool(
                &self,
                _e: &str,
                _t: Option<&str>,
                name: &str,
                _a: Value,
            ) -> Result<Value> {
                self.0.lock().unwrap().push(format!("call:{name}"));
                Ok(serde_json::json!({
                    "structuredContent": {
                        "number": 11,
                        "html_url": "https://github.com/octo/widget/pull/11"
                    }
                }))
            }
        }

        let gh_job = YoutrackJob {
            key: "GH-1".into(),
            summary: "do x".into(),
            repo_host: "https://github.com".into(),
            repo_path: "octo/widget".into(),
            clone_url: "https://github.com/octo/widget.git".into(),
        };
        // Tracker runner: only the authoritative set-field is registered; the
        // best-effort comment falls through (no canned response) and is ignored.
        let tracker_runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "GH-1", "AI Agent", "Code Review"],
            "",
        ));
        let tracker = CliBackend::new(tracker_runner.clone());
        // Forge runner: the git interrogation + push the GitHub backend issues.
        let forge_runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://github.com/octo/widget.git\n",
                )
                .ok("git", &["rev-parse", "--abbrev-ref", "HEAD"], "feat/gh")
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main\n",
                )
                .ok("git", &["log", "-1", "--pretty=format:%s"], "feat: x")
                .ok("git", &["log", "-1", "--pretty=format:%b"], "")
                .ok("git", &["push", "origin", "HEAD:feat/gh"], ""),
        );
        let forge = GithubForge::with_transport(
            forge_runner,
            Arc::new(OkTransport(Mutex::new(Vec::new()))),
            "https://api.githubcopilot.com/mcp/".into(),
            None,
        );
        let dir = tempfile::tempdir().unwrap();
        let res = finalize(&tracker, &forge, &src(), &gh_job, dir.path(), &ok_result()).unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://github.com/octo/widget/pull/11".into()
            }
        );
        let calls = tracker_runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field GH-1 AI Agent Code Review"),
            "{calls:?}"
        );
        // No `fj` was invoked for a GitHub job.
        assert!(!calls.iter().any(|c| c.starts_with("fj ")), "{calls:?}");
    }

    #[test]
    fn finalize_uses_configured_field_and_state_labels() {
        // A non-default custom_field / state label drives the yt write, proving
        // the config threads through finalize rather than using the old literals.
        let dir = tempfile::tempdir().unwrap();
        let s = YoutrackSource {
            custom_field: "Bot State".into(),
            states: crate::action::YoutrackStates {
                code_review: "Awaiting Review".into(),
                ..crate::action::YoutrackStates::default()
            },
            ..YoutrackSource::default()
        };
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA"],
                    "Created pull request #7",
                )
                .ok(
                    "yt",
                    &[
                        "issue",
                        "set-field",
                        "MK-16",
                        "Bot State",
                        "Awaiting Review",
                    ],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(&backend, &backend, &s, &job(), dir.path(), &ok_result()).unwrap();
        assert!(matches!(res, Finalized::CodeReview { .. }));
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 Bot State Awaiting Review"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_pr_create_failure_sets_failed() {
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA"],
                    1,
                    "no commits to push",
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Failed"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(&backend, &backend, &src(), &job(), dir.path(), &ok_result()).unwrap();
        assert_eq!(res, Finalized::Failed);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Failed"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_claude_error_sets_failed_without_pr() {
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Failed"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        // FJMCP-6 case: claude errored on the job.
        let claude = ClaudeResult::synthesized("claude exited 1, see stream.jsonl", "error tail");
        let res = finalize(&backend, &backend, &src(), &job(), dir.path(), &claude).unwrap();
        assert_eq!(res, Finalized::Failed);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Failed"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.starts_with("fj ")), "{calls:?}");
    }
}
