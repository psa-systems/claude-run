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
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{NativeConfig, YoutrackSource};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::{raw_event_details, ClaudeInvoker, ClaudeResult};
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

    // CLAUDE-103: capture the configured PR-creation strategy before the inner
    // loop shadows `forge` with the resolved `&dyn Forge`. `PrStrategy` is Copy.
    let pr_strategy = forge.pr_strategy;

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

    // CLAUDE-108: the canonical YouTrack issue URL is fully determined by the
    // source `base_url` and each job's `idReadable` key, so persist per-job
    // `job_links` (the same mechanism the Forgejo webhook uses) to light up the
    // key hyperlinks in the web UI with zero operator config. When `base_url`
    // is None (host `yt` default, instance host unknown) skip, so keys degrade
    // to plain text rather than a broken link.
    if let Some(base) = src.base_url.as_deref() {
        let base = base.trim_end_matches('/');
        let links: std::collections::BTreeMap<String, String> = jobs
            .iter()
            .map(|j| (j.key.clone(), format!("{}/issue/{}", base, j.key)))
            .collect();
        if let Err(e) = state::run_set_job_links(pool, run_id, &links).await {
            warn!(error = %e, "run_set_job_links failed (non-fatal)");
        }
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
                pr_strategy,
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
                // Project genuinely has no enabled VCS processor (empty
                // `list --json`): the legitimate drop path.
                Err(Error::NoVcsIntegration(_)) => {
                    info!(project = %project, "dropping issues: no usable VCS integration");
                    None
                }
                // A probe tool error (e.g. `yt` exit 2) is NOT the same as
                // "no integration": the project may well have one and the
                // probe failed. Surface it as a provider-CLI error rather
                // than silently collapsing it into the drop path (CLAUDE-124).
                Err(e) => {
                    warn!(project = %project, error = %e, "VCS probe failed (provider-CLI error); skipping project this poll, not treating as missing integration");
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
    pr_strategy: crate::action::PrStrategy,
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

    // CLAUDE-102: a preserved workdir from a failed finalize (unpushed commits)
    // must not be clobbered by this attempt's clone; move it aside first.
    preserve_aside(&workdir_path);
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
            let prompt = build_prompt(src, job, &workdir_path);
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
        cleanup_or_preserve(&workdir_path, &action.name, key);
        return Ok(ProcessedJob::Transient { key: key.clone() });
    }

    if claude_ran {
        state::daily_cost_add(pool, &action.name, claude_result.cost).await?;
    }

    // ---- finalize (the native "post"): every external mutation lives here ----
    let finalize_res = finalize(
        backend,
        forge,
        src,
        job,
        &workdir_path,
        &claude_result,
        pr_strategy,
        cancel,
    );
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

    // CLAUDE-102: a finalize failure can cost a retry, never the work. Only a
    // successfully opened PR makes deletion unconditional (the AGit push leaves
    // no remote-tracking ref, so the unpushed check cannot clear it); every
    // other outcome deletes only when no unpushed commits would be lost. A
    // panic anywhere above preserves trivially: deletion only ever happens
    // here, explicitly.
    match &finalize_res {
        // A merged PR (CLAUDE-126) is as safe to delete as a Code Review PR: the
        // work reached the server and, for Merged, was merged into the base.
        Ok(Finalized::CodeReview { .. }) | Ok(Finalized::Merged { .. }) => {
            cleanup_workdir(&workdir_path)
        }
        _ => cleanup_or_preserve(&workdir_path, &action.name, key),
    }

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
        timed_out: claude_result.timed_out,
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
    // CLAUDE-131: optionally assign the issue before cloning, so an assignee
    // failure aborts pickup before any repo work (same contract as the field /
    // state writes above). Unset leaves the Assignee untouched.
    if let Some(login) = &src.assignee {
        backend
            .set_assignee(&job.key, login)
            .map_err(|e| format!("set Assignee={login}: {e}"))?;
    }
    clone_repo(&job.clone_url, workdir).map_err(|e| format!("git clone: {e}"))?;
    install_hooks(workdir);
    Ok(())
}

/// CLAUDE-120: install the repo's git pre-commit hook on a fresh clone so a
/// commit that fails the repo's local checks is blocked locally instead of only
/// being caught by CI (the gap that produced A8N-69). Detects a `just
/// install-hooks` recipe and, when present, runs it; the recipe is idempotent
/// (it rewrites `.git/hooks/pre-commit`), so running it on every clone is safe.
///
/// Non-fatal by contract: this is an additional local backstop, not a hard
/// gate. A repo with no `install-hooks` recipe, or a host without `just`, is
/// left unchanged and the bootstrap continues - failing the job here would be
/// strictly worse than the CI-only status quo.
fn install_hooks(workdir: &Path) {
    // `just --show <recipe>` exits non-zero when the recipe (or the justfile
    // itself) is absent, so it doubles as the presence probe with no output
    // parsing. A spawn error means `just` is not installed: nothing to do.
    match Command::new("just")
        .current_dir(workdir)
        .arg("--show")
        .arg("install-hooks")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => {}
        Ok(_) => return, // no install-hooks recipe; leave the clone unchanged
        Err(e) => {
            info!(error = %e, "just not available; skipping pre-commit hook install");
            return;
        }
    }
    match Command::new("just")
        .current_dir(workdir)
        .arg("install-hooks")
        .status()
    {
        Ok(s) if s.success() => info!("installed repo pre-commit hook on fresh clone"),
        Ok(s) => warn!(
            code = s.code(),
            "just install-hooks failed; continuing without local pre-commit hook"
        ),
        Err(e) => warn!(
            error = %e,
            "running just install-hooks failed; continuing without local pre-commit hook"
        ),
    }
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

/// The built-in YouTrack agent prompt as a placeholder template (CLAUDE-125).
/// Returns the default instruction text - local-only work: no push, no PR, no
/// YouTrack write, matching the merged CLAUDE-24 prompt from the scripted
/// `actions/youtrack.yml` - with the documented `{{...}}` placeholders that
/// [`render_prompt_template`] substitutes per job. Exposed crate-wide so the web
/// form can prefill the prompt editor with the live default.
pub(crate) fn default_youtrack_prompt() -> String {
    [
        "You are working a YouTrack issue end-to-end. Follow the YouTrack and Git workflows from CLAUDE.md.",
        "",
        "Issue: {{key}}: {{summary}}",
        "Repo: {{clone_url}}",
        "Working directory: {{workdir}} - the repo is already cloned here, you are inside it.",
        "",
        "Do ONLY local engineering work. Do NOT push, do NOT open a PR, and do NOT touch YouTrack (no 'yt issue set-field', no 'yt issue set-state', no comments). The runner finalizes the issue after you exit: it opens the PR from your local commits and sets the AI Agent field based on the run outcome.",
        "",
        "Steps:",
        "1. Run 'yt issue inspect {{key}}' to read the full description and acceptance criteria.",
        "2. If any acceptance criterion depends on another issue, repo, endpoint, or symbol, verify that dependency exists in the source of truth NOW, before writing code: read the actual file, query the endpoint, or grep for the symbol on the current default branch. Do NOT trust another issue's Done or Resolved state - an issue marked Done may not have shipped its code. If the depended-on artifact is missing or contradicts this issue, take the blocker path below instead of building against it.",
        "3. Create a feature branch off the default branch per the Git workflow in CLAUDE.md.",
        "4. Make the minimum change required to satisfy the acceptance criteria.",
        "5. Commit with a message that references the issue on its own line as '#{{key}}' (a bare reference, with NO 'State ...' command after it). This links the PR to the issue in YouTrack without transitioning it. Do NOT write 'State Done' - the issue must stay In Progress until a human merges the PR.",
        "6. Run the target repo's check suite synchronously to green (e.g. the repo's 'just check', tests, or build). HARD RULE: run every long command in the FOREGROUND and wait for it inline. Do NOT background a build, check, or watch and poll/wait on it across turns. Leave NO background task running when you end your turn: any lingering background process keeps the run's process group alive and will get the whole run timeout-killed (exit 124) even after the functional work is complete - that is what blew the run timeout previously. Kill or wait out every spawned background job before you stop.",
        "7. Verify completion against the acceptance criteria before stopping. Re-read each criterion and state the concrete evidence that it is met: the file:line you changed, the command and its output, or the test that now passes. For any criterion that REMOVES or DELETES something, prove absence with a search (grep for the symbol or file) and confirm zero hits - never assert a removal you have not grep-verified - and add or extend a test or CI guard that fails if it reappears, so the removal is enforced mechanically. A green check suite is necessary but NOT sufficient: green checks do not prove the acceptance criteria are met. If you cannot produce objective evidence for EVERY criterion, you have NOT completed the issue - take the blocker path below instead of stopping as done.",
        "8. Stop. Do not push, do not open a PR, do not touch YouTrack. The runner opens the PR from your commits and transitions the issue.",
        "",
        "If you CANNOT safely complete the issue - missing context, ambiguous acceptance criteria, out-of-scope work, dependency on another issue, or you cannot produce objective evidence that every acceptance criterion is met:",
        "  a. Do NOT change any code.",
        "  b. Write the human-facing explanation of exactly what information or decision you need to proceed to this file (the file contents are the reason, posted verbatim as the issue comment by the runner):",
        "       {{workdir}}/{{needs_review_marker}}",
        "  c. Touch this marker file so the runner does NOT count this attempt toward the per-key cap (the job is parked pending human input, not failed):",
        "       {{workdir}}/{{skip_attempt_marker}}",
        "  d. Do NOT touch YouTrack yourself; the runner reads the marker and sets AI Agent to 'Needs Review'.",
        "",
        "When you finish, summarize what you did as an acceptance-criteria checklist: one line per criterion with the concrete evidence that proves it is met. Do not include a PR URL - the runner creates the PR.",
    ]
    .join("\n")
}

/// Build the agent's instruction text for `job`. Uses the action's full-override
/// prompt (`native.youtrack.prompt`, CLAUDE-125) when set, otherwise the
/// built-in [`default_youtrack_prompt`]; either way the `{{...}}` placeholders
/// are substituted with this job's values.
fn build_prompt(src: &YoutrackSource, job: &YoutrackJob, workdir: &Path) -> String {
    let template = src.prompt.clone().unwrap_or_else(default_youtrack_prompt);
    render_prompt_template(&template, job, workdir)
}

/// Substitute the documented `{{...}}` placeholders in `template` with this
/// job's values. An unknown `{{name}}` is left literal and logged at warn
/// (forgiving per CLAUDE-125), as is an unterminated `{{`.
fn render_prompt_template(template: &str, job: &YoutrackJob, workdir: &Path) -> String {
    let workdir = workdir.display().to_string();
    let vars: [(&str, &str); 6] = [
        ("key", job.key.as_str()),
        ("summary", job.summary.as_str()),
        ("clone_url", job.clone_url.as_str()),
        ("workdir", workdir.as_str()),
        ("needs_review_marker", NEEDS_REVIEW_MARKER),
        ("skip_attempt_marker", SKIP_ATTEMPT_MARKER),
    ];

    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // No closing `}}`: copy the remainder verbatim and stop.
            warn!("unterminated '{{{{' in prompt template left literal");
            out.push_str(&rest[start..]);
            return out;
        };
        let name = after[..end].trim();
        match vars.iter().find(|(k, _)| *k == name) {
            Some((_, val)) => out.push_str(val),
            None => {
                warn!(
                    placeholder = name,
                    "unknown prompt placeholder left literal"
                );
                out.push_str(&rest[start..start + 2 + end + 2]);
            }
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

/// Outcome of [`finalize`]: which AI Agent transition was applied. Carried into
/// the log line and asserted in the unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Finalized {
    /// Blocker: agent parked the job; AI Agent=Needs Review, no PR.
    NeedsReview,
    /// Success: PR opened; AI Agent=Code Review.
    CodeReview { pr_url: String },
    /// CLAUDE-126: automerge succeeded - the PR's checks went green, the branch
    /// was up to date, and the runner merged it; State=Done, AI Agent=Done.
    Merged { pr_url: String },
    /// Hard failure (claude error, pre failure, or PR-create failure);
    /// AI Agent=Failed, no PR (or a failed PR).
    Failed,
    /// CLAUDE-52: claude hit the wall-clock limit (exit 124). The work may be
    /// complete, partial, or absent, so the issue is NOT marked Failed: a
    /// timeout-specific comment is posted and the AI Agent field is advanced to
    /// Needs Review only when still Working (a human/agent-advanced state is left
    /// untouched).
    TimedOut,
}

impl Finalized {
    fn as_str(&self) -> &'static str {
        match self {
            Finalized::NeedsReview => "Needs Review",
            Finalized::CodeReview { .. } => "Code Review",
            Finalized::Merged { .. } => "Merged",
            Finalized::Failed => "Failed",
            Finalized::TimedOut => "Timed Out",
        }
    }
}

/// The finalize state machine (the native "post"). Runs on every path. Mirrors
/// the single finalize `post` step in `actions/youtrack.yml`: the comment is
/// best-effort (a comment failure cannot strand the AI Agent field on Working),
/// and the AI Agent write is the authoritative, last operation. State stays In
/// Progress until a human merges the PR, except when CLAUDE-126 automerge is
/// enabled and the runner merges the PR itself (then State / AI Agent go Done).
#[allow(clippy::too_many_arguments)]
fn finalize(
    backend: &dyn Backend,
    forge: &dyn Forge,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    claude_result: &ClaudeResult,
    pr_strategy: crate::action::PrStrategy,
    cancel: &CancellationToken,
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
        match forge.create_pr(workdir, &host, pr_strategy) {
            Ok(pr) => {
                let url = pr_url(job, &pr);
                let body = success_comment(claude_result, &url);
                comment_then_field(backend, src, &job.key, &body, &src.states.code_review)?;
                // CLAUDE-126: optional, CI-gated automerge. Off by default, so
                // the issue stays at Code Review exactly as before. When enabled
                // and the PR number is known, poll the checks and merge on green.
                if src.automerge.enabled {
                    match pr.number {
                        Some(pr_number) => {
                            match automerge(
                                backend, forge, src, job, workdir, &url, pr_number, cancel,
                            ) {
                                Ok(Some(merged)) => return Ok(merged),
                                // Failure / timeout / cancel: the PR is left open
                                // at Code Review and the reason was already
                                // commented inside `automerge`.
                                Ok(None) => {}
                                // A forge error during the merge attempt must not
                                // strand the issue: it already sits at Code Review
                                // for a human, so log and leave it there.
                                Err(e) => {
                                    warn!(key = %job.key, error = %e, "automerge failed; leaving PR at Code Review");
                                }
                            }
                        }
                        None => {
                            warn!(key = %job.key, "automerge enabled but PR number is unknown; leaving at Code Review");
                        }
                    }
                }
                Ok(Finalized::CodeReview { pr_url: url })
            }
            Err(e) => {
                // CLAUDE-102: the error reaches the journal here (the run loop
                // only records `outcome: Failed`), and the comment names the
                // preserved workdir so the committed branch is recoverable.
                warn!(key = %job.key, error = %e, "PR creation failed; finalizing as Failed");
                let preserved = has_unpushed_commits(workdir).then_some(workdir);
                let body = pr_failure_comment(claude_result, &e, preserved);
                comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
                Ok(Finalized::Failed)
            }
        }
    } else if claude_result.timed_out {
        // CLAUDE-52: timeout (exit 124). Ambiguous outcome: the agent may have
        // finished the work yet a lingering background task held the process
        // open past the limit. Do NOT mark Failed. Comment with the situation,
        // and advance AI Agent to Needs Review only when it is still Working;
        // if a human/agent already moved it on (Done, Code Review, ...), leave
        // the field untouched so we never stomp an advanced state.
        let body = timeout_comment(claude_result);
        if let Err(e) = backend.comment(&job.key, &body) {
            warn!(key = %job.key, error = %e, "timeout comment failed (continuing)");
        }
        let current = backend
            .get_field(&job.key, &src.custom_field)
            .unwrap_or_default();
        if current.trim() == src.states.working {
            backend.set_field(&job.key, &src.custom_field, &src.states.needs_review)?;
        } else {
            info!(key = %job.key, current = %current, "timeout: AI Agent already advanced off Working, leaving untouched");
        }
        Ok(Finalized::TimedOut)
    } else {
        // Failure: crash, empty prompt, or pre failure.
        let body = failure_comment(claude_result);
        comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
        Ok(Finalized::Failed)
    }
}

/// CLAUDE-126: the automerge poll loop. Runs only when `automerge.enabled` and
/// a PR number is known. Polls the PR's combined CI status every
/// `check_interval_seconds` up to a total budget of `wait_seconds`, honoring the
/// run's cancel token. Returns:
///
/// - `Ok(Some(Finalized::Merged))` on green + up-to-date: the PR is merged as a
///   merge-commit, State=Done and AI Agent=Done are set, and a "runner merged"
///   comment is posted.
/// - `Ok(None)` when the PR is left open at Code Review (checks failed, the
///   budget was exhausted while still pending, the branch could not be brought
///   up to date, or the run was cancelled); the reason is commented (except on
///   cancel, where the run is being torn down).
/// - `Err` only on a forge/tracker call error, which the caller logs while
///   leaving the issue at Code Review.
///
/// Before merging, a green-but-unmergeable PR (its branch behind the base) is
/// brought up to date once via [`Forge::update_branch`] (retriggering CI), and
/// the subsequent checks are awaited within the same budget; a branch that
/// stays unmergeable after one update is left for a human.
#[allow(clippy::too_many_arguments)]
fn automerge(
    backend: &dyn Backend,
    forge: &dyn Forge,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    pr_url: &str,
    pr_number: u64,
    cancel: &CancellationToken,
) -> Result<Option<Finalized>> {
    let host = host_only(&job.repo_host);
    let repo = job.repo_path.as_str();
    let cfg = &src.automerge;
    let budget = Duration::from_secs(cfg.wait_seconds);
    let start = Instant::now();
    // Bound the "behind base" recovery to a single Update Branch so a branch
    // that stays unmergeable (e.g. a merge conflict) cannot spin until the
    // deadline.
    let mut updated = false;

    info!(key = %job.key, pr = pr_number, wait_seconds = cfg.wait_seconds, "automerge: waiting for CI checks");
    loop {
        if cancel.is_cancelled() {
            info!(key = %job.key, "automerge: run cancelled; leaving PR at Code Review");
            return Ok(None);
        }
        let status = forge.pr_status(&host, repo, pr_number)?;
        match status.state.trim() {
            "failure" | "error" => {
                let body = automerge_failure_comment(pr_url);
                if let Err(e) = backend.comment(&job.key, &body) {
                    warn!(key = %job.key, error = %e, "automerge failure comment failed (continuing)");
                }
                info!(key = %job.key, pr = pr_number, "automerge: checks failed; leaving PR at Code Review");
                return Ok(None);
            }
            "success" if status.mergeable => {
                // Green and up to date: merge as a merge-commit, then advance the
                // issue. State=Done first, then comment + AI Agent=Done as the
                // authoritative last write (mirrors comment_then_field).
                forge.merge_pr(&host, repo, pr_number)?;
                backend.set_state(&job.key, &src.states.state_done)?;
                let body = merged_comment(pr_url);
                comment_then_field(backend, src, &job.key, &body, &src.states.done)?;
                info!(key = %job.key, pr = pr_number, "automerge: merged PR and advanced issue to Done");
                return Ok(Some(Finalized::Merged {
                    pr_url: pr_url.to_string(),
                }));
            }
            "success" => {
                // Green but not mergeable: the branch is behind its base. Update
                // it once (which retriggers CI) and keep polling; if it is still
                // not mergeable after that, leave it for a human.
                if updated {
                    let body = automerge_not_mergeable_comment(pr_url);
                    if let Err(e) = backend.comment(&job.key, &body) {
                        warn!(key = %job.key, error = %e, "automerge not-mergeable comment failed (continuing)");
                    }
                    info!(key = %job.key, pr = pr_number, "automerge: still not mergeable after Update Branch; leaving PR at Code Review");
                    return Ok(None);
                }
                info!(key = %job.key, pr = pr_number, "automerge: branch behind base; running Update Branch");
                forge.update_branch(workdir)?;
                updated = true;
            }
            // "pending" (or any other transient state): keep waiting.
            _ => {}
        }

        if start.elapsed() >= budget {
            let body = automerge_timeout_comment(pr_url, cfg.wait_seconds);
            if let Err(e) = backend.comment(&job.key, &body) {
                warn!(key = %job.key, error = %e, "automerge timeout comment failed (continuing)");
            }
            info!(key = %job.key, pr = pr_number, "automerge: timed out waiting for green checks; leaving PR at Code Review");
            return Ok(None);
        }
        sleep_with_cancel(cfg.check_interval_seconds, cancel);
    }
}

/// Sleep up to `secs` seconds, waking early (in <=500ms) if the run is
/// cancelled so a `claude-run stop` never hangs on a long `check_interval`.
fn sleep_with_cancel(secs: u64, cancel: &CancellationToken) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if cancel.is_cancelled() {
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(remaining.min(Duration::from_millis(500)));
    }
}

fn merged_comment(pr_url: &str) -> String {
    [
        "The runner merged this PR after its CI checks passed (automerge).".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "The issue has been advanced to Done.".to_string(),
    ]
    .join("\n")
}

fn automerge_failure_comment(pr_url: &str) -> String {
    [
        "Automerge did NOT merge this PR: its CI checks reported a failure.".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "The PR is left open for a human to resolve; AI Agent stays at Code Review.".to_string(),
    ]
    .join("\n")
}

fn automerge_timeout_comment(pr_url: &str, wait_seconds: u64) -> String {
    [
        format!(
            "Automerge timed out after {wait_seconds}s waiting for the PR's CI checks to pass."
        ),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "The PR is left open for a human to review and merge; AI Agent stays at Code Review."
            .to_string(),
    ]
    .join("\n")
}

fn automerge_not_mergeable_comment(pr_url: &str) -> String {
    [
        "Automerge could not bring the PR branch up to date with its base, so it was not merged."
            .to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "The PR is left open for a human to update and merge; AI Agent stays at Code Review."
            .to_string(),
    ]
    .join("\n")
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

/// PR URL for the issue comment. When the forge returned a real HTTP URL (both
/// the GitHub backend and `fj pr create --json` report the PR `html_url`), use
/// it verbatim. Otherwise render the parsed number against the Forgejo `pulls`
/// path, falling back to the raw line.
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

fn pr_failure_comment(r: &ClaudeResult, err: &Error, preserved_at: Option<&Path>) -> String {
    // CLAUDE-102: when the workdir holds unpushed commits it is preserved (not
    // cleaned), and this comment is the recovery pointer to the branch.
    let preserved_line = match preserved_at {
        Some(p) => format!(
            "The committed branch is PRESERVED on the daemon host at: `{}` (push it manually or re-run after the PR path is fixed).",
            p.display()
        ),
        None => "No local commits were found to preserve.".to_string(),
    };
    let mut body = [
        "Claude AI agent run completed but PR creation FAILED:".to_string(),
        String::new(),
        err.to_string(),
        String::new(),
        preserved_line,
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n");
    body.push_str(&raw_event_details(&r.raw));
    body
}

/// CLAUDE-52: timeout-specific comment. Surfaces the ambiguity (the work may be
/// complete despite the kill, e.g. a backgrounded command held the process open
/// past the limit), states the runner did NOT mark the issue Failed, and points
/// a human at the branch/PR and the run log (`r.error` carries the log path).
fn timeout_comment(r: &ClaudeResult) -> String {
    let mut body = [
        "Claude AI agent run TIMED OUT (the run hit its time limit and was killed):".to_string(),
        String::new(),
        format!("Details: {}", r.error),
        String::new(),
        "The agent may have completed all, some, or none of the work before being killed. A backgrounded command can hold the process open past the limit even after the functional work is done, so this is not necessarily a failure. The runner did NOT mark this issue Failed.".to_string(),
        String::new(),
        "A human should inspect the branch/PR and the run log above, then either resolve the issue manually or re-queue it (set AI Agent = Queued).".to_string(),
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n");
    body.push_str(&raw_event_details(&r.raw));
    body
}

fn failure_comment(r: &ClaudeResult) -> String {
    let mut body = [
        format!("Claude AI agent run FAILED ({}):", r.error),
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n");
    body.push_str(&raw_event_details(&r.raw));
    body
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
        timed_out,
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
                timed_out: *timed_out,
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

/// CLAUDE-105: true when any local branch tip in `workdir` is absent from the
/// live remote as reported by `git ls-remote origin` - exactly the work
/// deleting the directory would destroy. This asks the source of truth rather
/// than trusting remote-tracking refs: a `--depth` clone is `--single-branch`,
/// so its only tracking ref is `origin/main` and the old `--not --remotes`
/// heuristic false-positived on every non-`main` branch (CLAUDE-102's intent
/// was to save unpushed work, not preserve every feature-branch run). Any git
/// or network failure errs toward `true`: preserving costs disk, a wrong delete
/// costs the run.
pub(crate) fn has_unpushed_commits(workdir: &Path) -> bool {
    fn git(workdir: &Path, args: &[&str]) -> Option<String> {
        let out = Command::new("git")
            .arg("-C")
            .arg(workdir)
            .args(args)
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }

    // Local branch tips. No branches means nothing local to lose -> delete.
    let branches = match git(
        workdir,
        &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
    ) {
        Some(s) => s,
        None => return true,
    };
    let branches: Vec<&str> = branches
        .lines()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .collect();
    if branches.is_empty() {
        return false;
    }

    // The live remote's advertised SHAs (first column of each ls-remote line).
    let remote = match git(workdir, &["ls-remote", "origin"]) {
        Some(s) => s,
        None => return true,
    };
    let remote_shas: std::collections::HashSet<&str> =
        remote.split_whitespace().step_by(2).collect();

    for branch in branches {
        let tip = match git(workdir, &["rev-parse", branch]) {
            Some(s) => s.trim().to_string(),
            None => return true,
        };
        if !remote_shas.contains(tip.as_str()) {
            return true;
        }
    }
    false
}

/// CLAUDE-105: remove gitignored build artifacts from a workdir about to be
/// preserved. `git clean -fdX` deletes only ignored files and directories; a
/// failure is logged and tolerated (the directory is still preserved).
fn prune_ignored_artifacts(workdir: &Path) {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(["clean", "-fdX"])
        .output();
    match out {
        Err(e) => {
            warn!(workdir = %workdir.display(), error = %e, "pruning gitignored artifacts failed (preserving anyway)");
        }
        Ok(o) if !o.status.success() => {
            warn!(
                workdir = %workdir.display(),
                stderr = %String::from_utf8_lossy(&o.stderr),
                "git clean -fdX failed (preserving anyway)"
            );
        }
        Ok(_) => {}
    }
}

/// CLAUDE-102: delete the workdir unless it holds unpushed commits. The
/// preserve branch is the data-loss guard from the CLAUDE-101 incident, where
/// a failed `create_pr` plus an unconditional cleanup destroyed the run's only
/// copy of the committed work.
fn cleanup_or_preserve(workdir: &Path, action: &str, key: &str) {
    if !workdir.exists() {
        return;
    }
    if has_unpushed_commits(workdir) {
        // CLAUDE-105: the guard only protects committed work in `.git`, so drop
        // regenerable gitignored output (`target/`, `node_modules/`, ...) before
        // keeping the directory. `-X` touches only ignored files, leaving source,
        // `.git`, unpushed commits, and uncommitted tracked changes intact.
        prune_ignored_artifacts(workdir);
        warn!(
            action = %action,
            key = %key,
            workdir = %workdir.display(),
            "workdir preserved: it holds unpushed commits and no PR was opened"
        );
    } else {
        cleanup_workdir(workdir);
    }
}

/// CLAUDE-102: move a leftover (preserved) workdir out of the way of a fresh
/// attempt's clone, to the first free `<dir>.kept-<n>` sibling. An empty
/// leftover directory is left in place (the clone tolerates it).
fn preserve_aside(workdir: &Path) {
    let occupied = std::fs::read_dir(workdir)
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if !occupied {
        return;
    }
    for n in 1u32.. {
        let aside = workdir.with_file_name(format!(
            "{}.kept-{n}",
            workdir
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "workdir".to_string())
        ));
        if aside.exists() {
            continue;
        }
        match std::fs::rename(workdir, &aside) {
            Ok(()) => {
                warn!(from = %workdir.display(), to = %aside.display(), "leftover workdir moved aside before fresh clone");
            }
            Err(e) => {
                warn!(workdir = %workdir.display(), error = %e, "moving leftover workdir aside failed; clone may fail");
            }
        }
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::PrStrategy;
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
            timed_out: false,
        }
    }

    fn timed_out_result() -> ClaudeResult {
        ClaudeResult {
            ok: false,
            summary: "partial work before kill".into(),
            cost: 0.3,
            raw: Value::Null,
            error: "claude exited 124, see stream.jsonl".into(),
            transient: false,
            timed_out: true,
        }
    }

    /// CLAUDE-56: a failure with a populated `raw` event leads with the friendly
    /// summary, preserves the first-line error and Cost line, and appends the
    /// pretty-printed event in a collapsed `<details>` block.
    #[test]
    fn failure_comment_friendly_summary_and_collapsed_raw() {
        let r = ClaudeResult {
            ok: false,
            summary: "API Error: 529 Overloaded.".into(),
            cost: 0.01,
            raw: serde_json::json!({"type":"result","is_error":true,"api_error_status":"529"}),
            error: "claude exited 1, see stream.jsonl".into(),
            transient: true,
            timed_out: false,
        };
        let body = failure_comment(&r);
        assert!(body.starts_with("Claude AI agent run FAILED (claude exited 1, see stream.jsonl):"));
        assert!(body.contains("API Error: 529 Overloaded."));
        assert!(body.contains("Cost: $0.01 USD"));
        assert!(body.contains("<details>\n<summary>Raw result event</summary>"));
        assert!(body.contains("```json"));
        assert!(body.contains("\"api_error_status\": \"529\""));
    }

    /// CLAUDE-56: a synthesized failure (empty `raw`) omits the `<details>` block.
    #[test]
    fn failure_comment_omits_details_for_synthesized_raw() {
        let r = ClaudeResult::synthesized("claude exited 1, see stream.jsonl", "pre step failed");
        let body = failure_comment(&r);
        assert!(body.contains("pre step failed"));
        assert!(body.contains("Cost: $0 USD"));
        assert!(!body.contains("<details>"));
    }

    #[test]
    fn build_prompt_default_substitutes_job_fields() {
        // CLAUDE-125 regression: the built-in default renders every placeholder
        // and leaves no `{{...}}` behind, preserving the CLAUDE-24 contract lines.
        let wd = std::path::Path::new("/tmp/wd");
        let p = build_prompt(&src(), &job(), wd);
        assert!(p.starts_with(
            "You are working a YouTrack issue end-to-end. Follow the YouTrack and Git workflows from CLAUDE.md."
        ));
        assert!(p.contains("Issue: MK-16: Do the thing"));
        assert!(p.contains("Repo: https://dev.a8n.run/contoso/claude-run.git"));
        assert!(p.contains("Working directory: /tmp/wd - the repo is already cloned here"));
        assert!(p.contains("yt issue inspect MK-16"));
        assert!(p.contains("references the issue on its own line as '#MK-16'"));
        assert!(p.contains("/tmp/wd/.claude-run-needs-review"));
        assert!(p.contains("/tmp/wd/.claude-run-skip-attempt"));
        assert!(!p.contains("{{"), "unresolved placeholder:\n{p}");
    }

    #[test]
    fn build_prompt_uses_override_when_set() {
        // CLAUDE-125: a set prompt fully replaces the default and still gets its
        // placeholders substituted.
        let mut s = src();
        s.prompt = Some("Do {{key}}: {{summary}} in {{workdir}}".to_string());
        let p = build_prompt(&s, &job(), std::path::Path::new("/tmp/wd"));
        assert_eq!(p, "Do MK-16: Do the thing in /tmp/wd");
        assert!(!p.contains("You are working a YouTrack issue"));
    }

    #[test]
    fn render_substitutes_all_documented_placeholders() {
        let t = "{{key}}|{{summary}}|{{clone_url}}|{{workdir}}|{{needs_review_marker}}|{{skip_attempt_marker}}";
        let p = render_prompt_template(t, &job(), std::path::Path::new("/wd"));
        assert_eq!(
            p,
            "MK-16|Do the thing|https://dev.a8n.run/contoso/claude-run.git|/wd|.claude-run-needs-review|.claude-run-skip-attempt"
        );
    }

    #[test]
    fn render_leaves_unknown_and_unterminated_literal() {
        // CLAUDE-125: an unknown placeholder and an unterminated `{{` survive
        // verbatim; known ones still substitute.
        let p = render_prompt_template(
            "{{key}} {{bogus}} {{oops",
            &job(),
            std::path::Path::new("/wd"),
        );
        assert_eq!(p, "MK-16 {{bogus}} {{oops");
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
                &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
                r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#,
            )
            // ZZ has no enabled VCS -> its issue is dropped.
            .ok(
                "yt",
                &["project", "vcs", "list", "ZZ", "--enabled-only", "--json"],
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
    fn gather_skips_project_whose_vcs_probe_errors() {
        // CLAUDE-124: a probe tool error (e.g. `yt` exit 2) is not the same as
        // "no integration". The issue is skipped this poll, but via the
        // provider-CLI-error path rather than the silent drop path. The good
        // project's issue is unaffected.
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
                &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
                r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#,
            )
            // ZZ's probe errors (mirrors the malformed-argv exit-2 symptom).
            .fail(
                "yt",
                &["project", "vcs", "list", "ZZ", "--enabled-only", "--json"],
                2,
                "unrecognized subcommand",
            );
        let backend = CliBackend::new(Arc::new(runner));
        let jobs = gather(&backend, &src().query).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].key, "MK-16");
    }

    /// CLAUDE-131: a configured assignee is written via `yt issue update
    /// <key> --assignee <login>` during the pre step, after the field / state
    /// writes and before the clone. The clone targets a bogus local path so it
    /// fails fast offline; the pre-clone YouTrack writes are still recorded.
    #[test]
    fn run_pre_sets_assignee_when_configured() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path().join("clone");
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Working"],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "In Progress"], "")
                .ok(
                    "yt",
                    &["issue", "update", "MK-16", "--assignee", "octocat"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let mut s = src();
        s.assignee = Some("octocat".into());
        let mut j = job();
        // Bogus local path: `git clone` fails immediately with no network.
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let _ = run_pre(&backend, &s, &j, &wd);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue update MK-16 --assignee octocat"),
            "{calls:?}"
        );
    }

    /// CLAUDE-131: with no assignee configured (the default), pickup issues no
    /// `yt issue update ... --assignee` write at all.
    #[test]
    fn run_pre_skips_assignee_when_unset() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path().join("clone");
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Working"],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "In Progress"], ""),
        );
        let backend = CliBackend::new(runner.clone());
        let s = src(); // assignee defaults to None
        assert!(s.assignee.is_none());
        let mut j = job();
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let _ = run_pre(&backend, &s, &j, &wd);
        let calls = runner.calls.lock().unwrap();
        assert!(!calls.iter().any(|c| c.contains("--assignee")), "{calls:?}");
    }

    /// CLAUDE-131: an assignee write failure aborts pickup with a failure reason,
    /// consistent with the existing set_field / set_state failure handling.
    #[test]
    fn run_pre_assignee_failure_aborts_pickup() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path().join("clone");
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Working"],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "In Progress"], "")
                .fail(
                    "yt",
                    &["issue", "update", "MK-16", "--assignee", "octocat"],
                    1,
                    "no such user",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let mut s = src();
        s.assignee = Some("octocat".into());
        let mut j = job();
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let err = run_pre(&backend, &s, &j, &wd).unwrap_err();
        assert!(err.starts_with("set Assignee=octocat:"), "{err}");
        // The clone is never attempted: the workdir stays absent.
        assert!(!wd.exists(), "clone must not run after an assignee failure");
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
            PrStrategy::Branch,
            &CancellationToken::new(),
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
    fn finalize_success_branch_strategy_creates_pr_and_sets_code_review() {
        // CLAUDE-103 default: the branch strategy pushes a real server branch and
        // opens the PR via the API; `-aA` is never invoked.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok("git", &["push", "--set-upstream", "origin", "HEAD"], "")
                .ok("git", &["log", "-1", "--format=%s"], "feat: x\n")
                .ok("git", &["log", "-1", "--format=%b"], "body\n\n#MK-16\n")
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "feat: x",
                        "--body-file",
                        ".claude-run-pr-body.md",
                        "--json",
                    ],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
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
                .any(|c| c == "git push --set-upstream origin HEAD"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("-aA")),
            "branch strategy must never invoke -aA: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Code Review"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_success_agit_strategy_creates_pr_and_sets_code_review() {
        // CLAUDE-103 opt-in: the AGit strategy invokes `fj pr create -aA`.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
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
                .any(|c| c == "fj --host dev.a8n.run pr create -aA --json"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "agit must not push a branch: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Code Review"),
            "{calls:?}"
        );
    }

    // ---- CLAUDE-102: workdir preservation -------------------------------

    fn git_in(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .expect("git runs");
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // CLAUDE-113: set a local git identity. `git clone` does not copy the
    // origin's local `user.*`, and CI's container has no global or
    // auto-detectable identity, so every repo that commits must set its own or
    // `git commit` fails with "Author identity unknown".
    fn set_identity(dir: &Path) {
        git_in(dir, &["config", "user.email", "test@test"]);
        git_in(dir, &["config", "user.name", "test"]);
    }

    fn repo_with_commit(dir: &Path) {
        git_in(dir, &["init"]);
        set_identity(dir);
        std::fs::write(dir.join("f.txt"), "x").unwrap();
        git_in(dir, &["add", "."]);
        git_in(dir, &["commit", "-m", "feat: x"]);
    }

    #[test]
    fn workdir_with_unpushed_commits_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        assert!(has_unpushed_commits(dir.path()));
        cleanup_or_preserve(dir.path(), "YouTrack", "MK-16");
        assert!(dir.path().exists(), "unpushed commits must survive cleanup");
    }

    #[test]
    fn clean_clone_is_still_removed() {
        // A clone whose HEAD is covered by remote-tracking refs holds nothing
        // unpushed; cleanup proceeds exactly as before CLAUDE-102.
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.path().join("clone");
        git_in(
            base.path(),
            &["clone", origin.to_str().unwrap(), clone.to_str().unwrap()],
        );
        assert!(!has_unpushed_commits(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16");
        assert!(
            !clone.exists(),
            "a workdir with no unpushed work is removed"
        );
    }

    // ---- CLAUDE-120: post-clone pre-commit hook install -----------------

    fn just_available() -> bool {
        Command::new("just")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[test]
    fn install_hooks_writes_executable_hook_when_recipe_present() {
        if !just_available() {
            eprintln!("skipping: `just` not on PATH");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        git_in(dir.path(), &["init"]);
        // A minimal, portable install-hooks recipe (sh, no nu) mirroring the
        // real one: it rewrites .git/hooks/pre-commit and marks it executable.
        std::fs::write(
            dir.path().join("justfile"),
            "install-hooks:\n    #!/usr/bin/env sh\n    printf '#!/usr/bin/env sh\\nexec just pre-commit\\n' > .git/hooks/pre-commit\n    chmod +x .git/hooks/pre-commit\n",
        )
        .unwrap();

        install_hooks(dir.path());

        let hook = dir.path().join(".git/hooks/pre-commit");
        assert!(hook.exists(), "hook must be written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&hook).unwrap().permissions().mode();
            assert!(mode & 0o111 != 0, "hook must be executable, got {mode:o}");
        }
    }

    #[test]
    fn install_hooks_is_a_noop_without_a_recipe() {
        // A repo with no justfile (so no install-hooks recipe) must not gain a
        // hook and must not fail the bootstrap. Environment-independent: holds
        // whether or not `just` is installed.
        let dir = tempfile::tempdir().unwrap();
        git_in(dir.path(), &["init"]);
        install_hooks(dir.path());
        assert!(
            !dir.path().join(".git/hooks/pre-commit").exists(),
            "no recipe means no hook"
        );
    }

    #[test]
    fn install_hooks_is_idempotent() {
        if !just_available() {
            eprintln!("skipping: `just` not on PATH");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        git_in(dir.path(), &["init"]);
        std::fs::write(
            dir.path().join("justfile"),
            "install-hooks:\n    #!/usr/bin/env sh\n    printf '#!/usr/bin/env sh\\nexec just pre-commit\\n' > .git/hooks/pre-commit\n    chmod +x .git/hooks/pre-commit\n",
        )
        .unwrap();
        install_hooks(dir.path());
        install_hooks(dir.path());
        assert!(dir.path().join(".git/hooks/pre-commit").exists());
    }

    // CLAUDE-105: build an origin + single-branch clone, mimicking the shallow
    // (`--depth` => `--single-branch`) clone the runner makes. Returns the
    // clone path.
    fn origin_and_single_branch_clone(base: &Path) -> std::path::PathBuf {
        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.join("clone");
        git_in(
            base,
            &[
                "clone",
                "--single-branch",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );
        // CLAUDE-113: the clone inherits none of the origin's local `user.*`, so
        // tests that commit in the clone need their own identity.
        set_identity(&clone);
        clone
    }

    #[test]
    fn feature_branch_tip_on_remote_is_deleted() {
        // PMS-174 shape: the agent checked a branch out but left its tip at a
        // commit already on the remote. The old `--not --remotes` heuristic
        // false-positived (only `origin/main` is tracked); the live-remote
        // check sees the tip present and deletes.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/x"]);
        git_in(&clone, &["push", "origin", "feat/x"]);
        assert!(!has_unpushed_commits(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16");
        assert!(!clone.exists(), "tip present on remote must be deleted");
    }

    #[test]
    fn feature_branch_commit_absent_from_remote_is_preserved() {
        // PMS-258 shape: the agent committed work that never reached the remote.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/y"]);
        std::fs::write(clone.join("new.txt"), "work").unwrap();
        git_in(&clone, &["add", "."]);
        git_in(&clone, &["commit", "-m", "feat: y"]);
        assert!(has_unpushed_commits(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16");
        assert!(clone.exists(), "an unpushed commit must survive cleanup");
    }

    #[test]
    fn ls_remote_failure_errs_toward_preserve() {
        // A reachable local branch but an unreachable remote: the live-remote
        // query fails and the guard preserves rather than risk a wrong delete.
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        git_in(
            dir.path(),
            &["remote", "add", "origin", "/nonexistent/repo.git"],
        );
        assert!(has_unpushed_commits(dir.path()));
    }

    #[test]
    fn preserved_workdir_has_gitignored_artifacts_pruned() {
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/z"]);
        std::fs::write(clone.join(".gitignore"), "target/\n").unwrap();
        std::fs::create_dir_all(clone.join("target")).unwrap();
        std::fs::write(clone.join("target/build.o"), "binary").unwrap();
        std::fs::write(clone.join("src.txt"), "source").unwrap();
        git_in(&clone, &["add", ".gitignore", "src.txt"]);
        git_in(&clone, &["commit", "-m", "feat: z"]);

        cleanup_or_preserve(&clone, "YouTrack", "MK-16");

        assert!(clone.exists(), "unpushed work must be preserved");
        assert!(
            !clone.join("target").exists(),
            "gitignored artifacts must be pruned"
        );
        assert!(clone.join("src.txt").exists(), "source must remain");
        assert!(clone.join(".git").exists(), ".git must remain");
    }

    #[test]
    fn preserve_aside_moves_a_leftover_workdir() {
        let base = tempfile::tempdir().unwrap();
        let wd = base.path().join("MK-16");
        std::fs::create_dir_all(&wd).unwrap();
        std::fs::write(wd.join("leftover.txt"), "kept work").unwrap();
        preserve_aside(&wd);
        assert!(!wd.exists(), "original path freed for the fresh clone");
        let aside = base.path().join("MK-16.kept-1");
        assert!(aside.join("leftover.txt").exists(), "content moved aside");

        // An empty leftover is left in place (the clone tolerates it).
        std::fs::create_dir_all(&wd).unwrap();
        preserve_aside(&wd);
        assert!(wd.exists());
    }

    #[test]
    fn finalize_pr_failure_preserves_and_names_the_workdir() {
        // The CLAUDE-101 incident shape: claude succeeded, the selected (branch)
        // strategy fails; finalize must mark Failed, error loudly with the
        // underlying git-push error, and point at the preserved workdir in the
        // comment instead of letting the commits be destroyed. CLAUDE-103: no
        // silent fallback to AGit.
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "git",
                    &["push", "--set-upstream", "origin", "HEAD"],
                    128,
                    "no upstream",
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Failed"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(res, Finalized::Failed);
        assert!(dir.path().exists(), "workdir survives the failed finalize");
        let calls = runner.calls.lock().unwrap();
        let comment = calls
            .iter()
            .find(|c| c.starts_with("yt issue comment MK-16"))
            .expect("failure comment posted");
        assert!(comment.contains("PRESERVED"), "{comment}");
        assert!(
            comment.contains(&dir.path().display().to_string()),
            "comment names the preserved path: {comment}"
        );
        assert!(
            comment.contains("no upstream"),
            "comment carries the underlying push error: {comment}"
        );
        // No silent fallback to AGit.
        assert!(
            !calls.iter().any(|c| c.contains("-aA")),
            "branch failure must not fall back to AGit: {calls:?}"
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
        let res = finalize(
            &tracker,
            &forge,
            &src(),
            &gh_job,
            dir.path(),
            &ok_result(),
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
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
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
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
        let res = finalize(
            &backend,
            &backend,
            &s,
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
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
        // AGit strategy whose `fj pr create -aA` fails finalizes as Failed.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
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
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
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
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &claude,
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
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

    #[test]
    fn finalize_timeout_sets_needs_review_when_still_working() {
        // CLAUDE-52: a timeout (exit 124) with AI Agent still on Working advances
        // the field to Needs Review and posts the timeout comment - never Failed.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Working\n",
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Needs Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &timed_out_result(),
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(res, Finalized::TimedOut);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
        // Never marked Failed, never opened a PR.
        assert!(
            !calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Failed"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.starts_with("fj ")), "{calls:?}");
    }

    #[test]
    fn finalize_timeout_leaves_field_when_already_advanced() {
        // CLAUDE-52: a timeout whose AI Agent has already moved off Working (e.g.
        // a human/agent set Done) leaves the field untouched - only the comment
        // is posted. No set-field write at all.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "get-field", "MK-16", "AI Agent"],
            "Done\n",
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &timed_out_result(),
            PrStrategy::Branch,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(res, Finalized::TimedOut);
        let calls = runner.calls.lock().unwrap();
        // The field is read but never written.
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue get-field MK-16 AI Agent"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("set-field")), "{calls:?}");
        assert!(!calls.iter().any(|c| c.starts_with("fj ")), "{calls:?}");
    }

    // ---- CLAUDE-126: automerge finalize branches ------------------------

    /// A YoutrackSource with automerge enabled and a zero inter-poll interval so
    /// the tests never sleep.
    fn src_automerge(wait_seconds: u64) -> YoutrackSource {
        YoutrackSource {
            automerge: crate::action::Automerge {
                enabled: true,
                wait_seconds,
                check_interval_seconds: 0,
            },
            ..YoutrackSource::default()
        }
    }

    #[test]
    fn finalize_automerge_merges_on_green_and_sets_done() {
        // CLAUDE-126: with automerge enabled, a PR whose checks are green and
        // whose branch is up to date is merged as a merge-commit; the issue is
        // advanced to State=Done + AI Agent=Done and the outcome is Merged.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "status",
                        "contoso/claude-run#7",
                        "--json",
                    ],
                    r#"{"pr":7,"state":"success","mergeable":true,"merged":false}"#,
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "merge",
                        "contoso/claude-run#7",
                        "--method",
                        "merge",
                        "--delete",
                    ],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "Done"], "")
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Done"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src_automerge(600),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::Merged {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into()
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run pr merge contoso/claude-run#7 --method merge --delete"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "yt issue set-state MK-16 Done"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Done"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_automerge_leaves_code_review_on_check_failure() {
        // CLAUDE-126: a CI failure means no merge; the PR stays open and the
        // issue stays at Code Review (the create-time state).
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "status",
                        "contoso/claude-run#7",
                        "--json",
                    ],
                    r#"{"pr":7,"state":"failure","mergeable":false,"merged":false}"#,
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src_automerge(600),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into()
            }
        );
        let calls = runner.calls.lock().unwrap();
        // No merge, and the issue was never advanced to Done.
        assert!(!calls.iter().any(|c| c.contains("pr merge")), "{calls:?}");
        assert!(
            !calls.iter().any(|c| c.contains("AI Agent Done")),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_automerge_leaves_code_review_on_timeout() {
        // CLAUDE-126: checks still pending when the budget is exhausted means no
        // merge; the PR stays open at Code Review. `wait_seconds: 0` exhausts the
        // budget after the first (pending) poll.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "status",
                        "contoso/claude-run#7",
                        "--json",
                    ],
                    r#"{"pr":7,"state":"pending","mergeable":false,"merged":false}"#,
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src_automerge(0),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into()
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(!calls.iter().any(|c| c.contains("pr merge")), "{calls:?}");
    }

    #[test]
    fn finalize_automerge_disabled_leaves_code_review() {
        // CLAUDE-126: the default (disabled) automerge never polls or merges; the
        // success path is exactly the pre-CLAUDE-126 Code Review behavior.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(matches!(res, Finalized::CodeReview { .. }));
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("pr status")),
            "disabled automerge must not poll: {calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("pr merge")), "{calls:?}");
    }
}
