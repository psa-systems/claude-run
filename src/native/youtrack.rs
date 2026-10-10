//! Native YouTrack action handler.
//!
//! Ports the scripted `actions/youtrack.yml` pipeline to typed Rust on the
//! dispatch seam ([`super`]) and the provider abstraction
//! ([`crate::provider`]). The pipeline is:
//!
//! ```text
//! gather   -> IssueTracker::list_queued + resolve_vcs per project (drop no-VCS)
//!             + forge repo-access preflight, once per repo: drop the
//!             repo's issues (leaving them Queued) when the forge token cannot
//!             read it, rather than failing at `pr create` after a paid run
//! eligible -> gather-time pass, BEFORE the KeyGate: live AI Agent
//!             re-read (drop unless still Queued) + dependency gate
//!             (drop unmet-blocker issues, keeping their Queued/comment/backoff
//!             side effects). Ineligible issues are dropped here so they never
//!             hold a project's single per-poll concurrency slot and starve a
//!             workable same-project sibling.
//! filter   -> per-job authoritative re-check AFTER winning the KeyGate: the same
//!             AI Agent re-read + dependency gate guard the cross-run TOCTOU right
//!             before the first mutation (skip unless still Queued / unblocked)
//! pre      -> AI Agent=Working, State=In Progress, git clone --depth 50
//! prompt  -> build the local-only instruction text in Rust
//! claude  -> ClaudeInvoker with the action's claude block + timeout
//! post    -> finalize state machine, runs on EVERY path (success, claude
//!            error, timeout, pre/post failure, cancel):
//!              blocker  (.claude-run-needs-review) -> AI Agent=Needs Review, no PR
//!              no-code  (.claude-run-no-code-change) -> AI Agent=
//!                       states.no_code (default Done), marker text commented,
//!                       no PR
//!              success  -> Forge::create_pr, AI Agent=Code Review
//!                          (PR-create failure -> AI Agent=Failed)
//!              failure  -> AI Agent=Failed, no PR
//! ```
//!
//! State is never set to Done here; it stays In Progress until a human merges
//! the PR. Every external mutation (PR, comment, AI Agent transition) happens
//! in [`finalize`], after claude exits, gated on the outcome - matching the
//! runner-creates-PR contract. The agent itself does local
//! engineering work only (no push, no PR) and the bounded
//! tracker writes [`default_youtrack_prompt`] permits: a descoped piece of the
//! triggering issue goes to the configured `write_projects`, linked `subtask
//! of` the triggering issue; an out-of-scope bug found elsewhere
//! goes to the project whose VCS integration binds the buggy repo (falling
//! back to `write_projects`), linked `relates to` the triggering issue. Either
//! way the agent may update / comment / link only the issues it created this
//! run. The agent may also comment on the triggering issue itself (including
//! an early size note when it expects the issue to be too large for one run).
//! `AI Agent` on any issue, and every other write to the triggering issue
//! (description/summary, tags, assignee, state, fields), stay the runner's.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::db::DbPool;
use tracing::{error, info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::access;
use crate::action::{NativeConfig, PromptPreset, YoutrackSource};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::{raw_event_details, ClaudeInvoker, ClaudeResult};
use crate::concurrency::Executor;
use crate::config::Paths;
use crate::error::{Error, Result};
use crate::job::{self, EffectiveLimits, ProcessedJob, RunnerDefaults};
use crate::provider::{
    self, AuditedTracker, CliBackend, CommandRunner, Forge, IssueTracker, IssueWriteAudit,
    PullRequest,
};
use crate::secrets::{self, JobCredentials};
use crate::state;
use crate::workdir;

/// Workdir marker the agent writes to park a job pending human input.
const NEEDS_REVIEW_MARKER: &str = ".claude-run-needs-review";
/// Workdir marker that suppresses attempt counting for this run.
const SKIP_ATTEMPT_MARKER: &str = ".claude-run-skip-attempt";
/// Workdir marker the agent writes when it deliberately finishes a run that
/// produces tracker output rather than code. Its contents are the
/// human-facing summary, posted verbatim as the issue comment. Never an escape
/// hatch for a code task the agent could not finish: that stays
/// [`NEEDS_REVIEW_MARKER`].
const NO_CODE_MARKER: &str = ".claude-run-no-code-change";

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

/// Built-in YouTrack handler. Routed to by [`super::handler_for`].
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

    // Typed config. Validation guarantees a matching `native` block
    // for `kind: youtrack`, so anything else is a misrouted action.
    let (src, forge, gh) = match &action.native {
        Some(NativeConfig::Youtrack(cfg)) => (&cfg.youtrack, &cfg.forgejo, &cfg.github),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    // Capture the configured PR-creation strategy before the inner
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
    // runner is shared across the per-job tasks, each of which builds
    // its own (optionally credentialed) backend from it.
    let runner = provider::resolve_runner(provider::DEFAULT_PRIORITY)?;

    // Resolve any `token_ref` to a decrypted credential once per run.
    // An all-None set (the shipped, host-auth config) materializes nothing and
    // every backend below falls back to host `yt` / `fj` auth.
    let creds = Arc::new(secrets::resolve_youtrack_credentials(pool, src, forge, gh).await?);
    let cred_base = ctx.paths.runtime_dir.join("credentials").join(&action.name);

    // gather (before run_begin: a gather failure is a no-op run, same as the
    // scripted path). gather only invokes `yt`, so its credentials are yt-only.
    // Stamp the poll once so the input-phase events recorded now
    // (before the run row exists) can be backfilled onto the run after
    // `run_begin`, and so a zero-job poll still associates its events with this
    // timestamp on the action page.
    let poll_ts = state::now_iso();
    // Keep the gather backend alive past `gather` so the gather-time
    // eligibility pass below reuses its yt-only credentials for the AI Agent
    // re-read and dependency-gate reads.
    let (gather_backend, gather_guard) = if creds.is_empty() {
        (CliBackend::new(runner.clone()), None)
    } else {
        let dir = gather_cred_dir(&cred_base);
        let guard = JobCredentials::materialize(&dir, &creds, None)?;
        let backend = credentialed_backend(&runner, &guard);
        (backend, Some(guard))
    };
    // The forge repo-access preflight. It needs its own per-host
    // credential store (the gather backend's is yt-only), so it materializes one
    // per probed repo; `None` when the action references no forge secret.
    let access = forge_access_check(&runner, &creds, &cred_base, gh);
    // Hold the gather marker so the scheduler does not stack another
    // pickup while this one is still gathering.
    let gather_marker = ctx.gather.try_acquire(&action.name);
    let Gathered {
        jobs,
        events: input_events,
        candidate_keys,
    } = gather(
        &gather_backend,
        &src.query,
        access.as_ref().map(|a| a as &dyn RepoAccess),
    )?;
    // Drop dependency-gate rows for issues this query no longer
    // matches. Only a successful query reaches here, so a `yt` failure never
    // prunes. Non-fatal: a bookkeeping failure must not abort the poll, but it is
    // logged, because a prune that stops running silently repopulates the
    // stale-gate banner with issues nobody is waiting on.
    if let Err(e) = state::dependency_gate_retain(pool, &action.name, &candidate_keys).await {
        warn!(action = %action.name, error = %e, "dependency-gate prune failed (non-fatal); the stale-gate banner may list issues that are no longer queued");
    }
    // Persist input-phase diagnostics before `run_begin` so they
    // survive even if no run row is ever created. Non-fatal: a bookkeeping
    // failure must not abort the poll.
    if !input_events.is_empty() {
        // Keep a preflight-passed marker only when it clears an open
        // failure; a query failure here leaves the events untouched rather than
        // dropping diagnostics.
        let input_events = match access::retain_access_recoveries(pool, &action.name, &input_events)
            .await
        {
            Ok(kept) => kept,
            Err(e) => {
                warn!(error = %e, "access recovery filter failed (non-fatal); recording every event");
                input_events.clone()
            }
        };
        if let Err(e) = state::run_events_record(pool, &action.name, &poll_ts, &input_events).await
        {
            warn!(error = %e, "run_events_record failed (non-fatal)");
        }
    }
    info!(action = %action.name, count = jobs.len(), "youtrack jobs gathered");

    // Admission check. Drop gathered jobs whose
    // CONCURRENCY key (project) is already in flight in another (overlapping) run
    // of this action, then, if nothing fresh remains, return WITHOUT `run_begin`
    // so a re-gather that only re-sees already-claimed work does not litter run
    // history with empty run rows. Deduping by concurrency key (not
    // issue id) stops a run from gathering a second same-project issue while that
    // project is already in flight; such a sibling could only park on the KeyGate
    // and defer, so gathering it is pure churn. The live AI-Agent re-read filter,
    // the daemon-wide `KeyGate` (same-project serialization), and this dedup
    // together guarantee two overlapping runs never both work the same project.
    let key_fields = action.concurrency.key.clone();
    let gathered = jobs.len();
    let inflight_ckeys: std::collections::HashSet<String> =
        state::action_inflight_concurrency_keys(pool, &action.name)
            .await?
            .into_iter()
            .collect();
    let jobs: Vec<YoutrackJob> = jobs
        .into_iter()
        .filter(|j| !inflight_ckeys.contains(&concurrency_key(&key_fields, j)))
        .collect();
    if jobs.is_empty() {
        info!(
            action = %action.name,
            gathered,
            "no fresh queued work (all in flight in another run); skipping empty run"
        );
        return Ok(());
    }

    // Gather-time eligibility pass. Evaluate the two read-only checks
    // (live `AI Agent` re-read, dependency gate) BEFORE building
    // executor items and acquiring the KeyGate, and drop every ineligible issue.
    // A blocked or no-longer-Queued issue is a pure read (it never clones,
    // branches, or opens a PR), so it must not hold its project's single per-poll
    // concurrency slot: doing so starved a workable same-project sibling, because
    // the blocked issue won the shared project key each poll and consumed the
    // slot without doing work. Blocked issues keep every dependency-gate side effect
    // (stay Queued, idempotent blocker comment, `blocked_until` backoff, persisted
    // signature); only genuinely-workable issues compete for the project key here.
    // A human-initiated run (daemon `trigger` or a direct `run`, i.e.
    // any origin other than the scheduler's periodic pickup) forces the dependency
    // gate to read links live, ignoring any open backoff window. Both gate call
    // sites in this run receive the same value.
    let bypass_backoff = ctx.triggered_by != "scheduler";
    let eligible_from = jobs.len();
    let jobs = filter_eligible(
        pool,
        &gather_backend,
        src,
        &action.name,
        jobs,
        bypass_backoff,
    )
    .await;
    // The gather backend's credentials are done; scrub them now.
    drop(gather_guard);
    if jobs.is_empty() {
        info!(
            action = %action.name,
            gathered = eligible_from,
            "no workable queued issues after the eligibility pass (all blocked or filtered); skipping empty run"
        );
        return Ok(());
    }

    // The gather marker covers the whole pre-`run_begin` phase (the
    // work a stacked pickup would redo); released here, so overlapping runs past
    // this point stay allowed as intended.
    drop(gather_marker);
    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
    // From here the row MUST be closed on every exit path, including
    // an early `?` out of the bookkeeping below.
    let run_guard = state::RunGuard::new(pool, run_id);
    // Backfill this poll's input-phase events onto the new run row.
    if !input_events.is_empty() {
        if let Err(e) = state::run_events_attach(pool, run_id, &action.name, &poll_ts).await {
            warn!(error = %e, "run_events_attach failed (non-fatal)");
        }
    }

    // Register a cancellation token and ensure it is unregistered on
    // every exit path (including the `?` early returns below).
    let cancel = match ctx.registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard {
        registry: ctx.registry,
        run_id,
    };
    // Run-scoped global-pause gate. Consulted by the executor before
    // each job pickup, by `process_job` before it spends a claude call, and by
    // the automerge CI wait; any trip makes the run finish with `exit::PAUSED`.
    let run_pause = ctx.pause.for_action_run(&action.name);

    // Record planned job keys so the web UI shows what is queued.
    let planned_keys: Vec<String> = jobs.iter().map(|j| j.key.clone()).collect();
    if let Err(e) = state::run_set_job_keys(pool, run_id, &planned_keys).await {
        warn!(error = %e, "run_set_job_keys failed (non-fatal)");
    }

    // The canonical YouTrack issue URL is fully determined by the
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

    // ONE issue-write ledger for the whole run, shared by every
    // per-job backend below, so the ceiling is per run rather than per job and
    // every write this run makes carries `run_id` into the journal.
    let audit = IssueWriteAudit::new(Some(run_id), limits.max_issues_per_run);

    // Keyed, bounded-parallel execution. Distinct repos (the
    // concurrency key) run in parallel; jobs against the same repo serialize so
    // they never collide on the clone dir / branch / PR. `max_parallel: 1`
    // reproduces the original strictly-sequential behavior.
    let max_parallel = action.concurrency.max_parallel.max(1) as usize;
    let loaded_arc = Arc::new(loaded.clone());
    let paths_owned = ctx.paths.clone();
    let pool_owned = pool.clone();
    let src_owned = src.clone();
    let gh_owned = gh.clone();

    let mut items: Vec<(String, crate::concurrency::JobFuture)> = Vec::with_capacity(jobs.len());
    for job in jobs.into_iter() {
        let gate_key = concurrency_key(&key_fields, &job);
        let inflight_ckey = gate_key.clone();
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
        let job_pause = run_pause.clone();
        let job_audit = audit.clone();
        let job_spend = ctx.spend.clone();
        let fut: crate::concurrency::JobFuture = Box::pin(async move {
            let action = &loaded.action;
            // Best-effort cap/budget check before launch; bounded
            // overshoot of at most `max_parallel - 1`.
            if state::daily_count_get(&pool, &action.name).await? >= limits.max_runs_per_day {
                warn!(action = %action.name, "daily cap reached, skipping job");
                return Ok(false);
            }
            if state::daily_cost_get(&pool, &action.name).await? >= limits.daily_budget_usd {
                warn!(action = %action.name, "daily budget reached, skipping job");
                return Ok(false);
            }
            // The run's own ceiling; a run that reached it picks up
            // nothing further.
            if job_spend.exceeded() {
                warn!(action = %action.name, "{}", job_spend.stop_reason());
                return Ok(false);
            }
            // Materialize this job's own credential files (yt config +
            // forge keys.json keyed by the issue's resolved VCS host). The guard
            // scrubs them when the job's provider calls finish; concurrent jobs
            // never share a credential file. No token_ref -> host auth, no files.
            // `run-{run_id}/{job_key}` is already unique per run and
            // job, so this site needs no extra sequence.
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
            if let Err(e) = state::run_inflight_add(&pool, run_id, &job.key, &inflight_ckey).await {
                warn!(error = %e, "run_inflight_add failed (non-fatal)");
            }
            // Pick the Forge by the job's resolved host. A GitHub host
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
            // The job writes through the RUN's ledger, so each
            // comment / field write is journalled with `run_id` and the issue
            // key, and any issue this job creates spends the run's ceiling.
            let tracker = AuditedTracker::new(&job_backend, job_audit);
            let outcome = process_job(
                loaded.as_ref(),
                &paths,
                &pool,
                claude.as_ref(),
                &tracker,
                forge,
                &src,
                &limits,
                &job_cancel,
                &job_pause,
                &job_spend,
                &job,
                pr_strategy,
                bypass_backoff,
                run_id,
            )
            .await;
            if let Err(e) = state::run_inflight_remove(&pool, run_id, &job.key).await {
                warn!(error = %e, "run_inflight_remove failed (non-fatal)");
            }
            // Surface forge-side degradations (an open-PR lookup
            // that failed and fell through to `pr create`) on the action page,
            // not only in the daemon journal. Drained before the `?` below so a
            // failed job still reports them. Non-fatal bookkeeping.
            let notices = forge.take_notices();
            if !notices.is_empty() {
                let events: Vec<state::InputEvent> = notices
                    .into_iter()
                    .map(|message| state::InputEvent {
                        level: "warn".to_string(),
                        context: Some(job.key.clone()),
                        message,
                    })
                    .collect();
                if let Err(e) =
                    state::run_events_record_for_run(&pool, run_id, &action.name, &events).await
                {
                    warn!(error = %e, "run_events_record_for_run failed (non-fatal)");
                }
            }
            let outcome = outcome?;
            record_outcome(loaded.as_ref(), &paths, &pool, run_id, &outcome).await?;
            Ok::<bool, crate::error::Error>(true)
        });
        items.push((gate_key, fut));
    }

    let exec = Executor {
        action: action.name.clone(),
        max_parallel,
        limiter: ctx.limiter.clone(),
        gate: ctx.keygate.clone(),
        cancel: cancel.clone(),
        pause: run_pause.clone(),
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

    // A run stopped by its issue-creation ceiling surfaces on the
    // action page, not only in the daemon journal. The ledger is the run's, so
    // the drain is here rather than per job. Non-fatal bookkeeping.
    let notices = audit.take_notices();
    if !notices.is_empty() {
        let events: Vec<state::InputEvent> = notices
            .into_iter()
            .map(|message| state::InputEvent {
                level: "warn".to_string(),
                context: None,
                message,
            })
            .collect();
        if let Err(e) = state::run_events_record_for_run(pool, run_id, &action.name, &events).await
        {
            warn!(run_id, error = %e, "run_events_record_for_run failed (non-fatal)");
        }
    }

    // An over-budget stop is this run's outcome unless a cancel or a
    // pause already claimed it.
    let exit_code = ctx
        .spend
        .exit_code(run_pause.exit_code(cancel.is_cancelled()));
    if exit_code == crate::exit::OVER_BUDGET {
        warn!(action = %action.name, "{}", ctx.spend.stop_reason());
    }
    run_guard.finish(exit_code, processed).await?;
    // One summary line and one machine-readable record per run.
    ctx.spend.finish(Some(run_id), exit_code);
    if let Some(e) = first_err {
        warn!(action = %action.name, error = %e, "one or more jobs errored during the run");
    }
    info!(action = %action.name, processed, exit_code, "run complete");
    Ok(())
}

/// Resolve a YouTrack job's concurrency key from the configured key
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

/// Build a [`CliBackend`] from `runner`, applying a job's materialized
/// credentials (yt config env, fj store env, and the temp `keys.json` path so
/// `clone_url` reads the same store). An empty credential set leaves host auth.
pub(crate) fn credentialed_backend(
    runner: &Arc<dyn CommandRunner>,
    creds: &JobCredentials,
) -> CliBackend {
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

/// The gather step's credential directory, unique per poll.
///
/// Every [`JobCredentials::materialize`] path must be unique per guard, because
/// the guard's `Drop` removes the whole directory. The three call sites:
///
/// - gather (here): `gather-{pid}-{seq}`, unique per poll via
///   [`secrets::unique_suffix`].
/// - preflight ([`ForgeAccessCheck::check`]): `access-{pid}-{seq}-{host}-{repo}`,
///   unique per probe by the same suffix.
/// - per-job (`run-{run_id}/{job_key}`): already unique per run and job,
///   unchanged.
fn gather_cred_dir(cred_base: &Path) -> PathBuf {
    cred_base.join(format!("gather-{}", secrets::unique_suffix()))
}

/// The gather-time forge repo-access preflight, injected so `gather`
/// can probe the FORGE identity (which needs a per-host credential store) without
/// owning credential materialization.
trait RepoAccess {
    /// `Ok(())` iff the forge identity can read `repo` (`owner/name`) on `host`.
    fn check(&self, host: &str, repo: &str) -> Result<()>;
}

/// The real [`RepoAccess`], which materializes the action's forge
/// token into a per-host credential store and asks the matching backend for
/// [`Forge::repo_access`]. Routes by host exactly as the per-job forge selection
/// does, so a GitHub-hosted repo is probed with the GitHub PAT.
struct ForgeAccessCheck {
    runner: Arc<dyn CommandRunner>,
    creds: Arc<secrets::ResolvedCredentials>,
    cred_base: PathBuf,
    gh_hosts: Vec<String>,
    gh_endpoint: String,
}

impl RepoAccess for ForgeAccessCheck {
    fn check(&self, host: &str, repo: &str) -> Result<()> {
        if provider::is_github_host(host, &self.gh_hosts) {
            // No `github.token_ref`: `gh` would run as the host store, i.e. the
            // wrong identity, so the probe would prove nothing. Skip it.
            if self.creds.github_token.is_none() {
                return Ok(());
            }
            let forge = provider::GithubForge::new(
                self.runner.clone(),
                self.gh_endpoint.clone(),
                self.creds.github_token.clone(),
            );
            return forge.repo_access(host, repo);
        }
        // Same rule for Forgejo: no `forgejo.token_ref` -> `fj` reads the
        // operator store, so the preflight would test the operator, not the bot.
        if self.creds.fj_token.is_none() {
            return Ok(());
        }
        // `{pid}-{seq}`, so two concurrent polls probing the same
        // repo get separate directories (see [`gather_cred_dir`]).
        let dir = self.cred_base.join(format!(
            "access-{}-{}-{}",
            secrets::unique_suffix(),
            sanitize_key(host),
            sanitize_key(repo)
        ));
        let guard = JobCredentials::materialize(&dir, &self.creds, Some(host))?;
        let backend = credentialed_backend(&self.runner, &guard);
        backend.repo_access(host, repo)
    }
}

/// Build the gather-time preflight, or `None` when the action
/// references no forge secret at all. With no `token_ref` the CLIs fall back to
/// host auth (`src/secrets.rs`), so a probe would exercise the operator's
/// identity rather than the bot's and could pass for a repo the bot cannot read.
fn forge_access_check(
    runner: &Arc<dyn CommandRunner>,
    creds: &Arc<secrets::ResolvedCredentials>,
    cred_base: &Path,
    gh: &crate::action::GithubTarget,
) -> Option<ForgeAccessCheck> {
    if creds.fj_token.is_none() && creds.github_token.is_none() {
        return None;
    }
    Some(ForgeAccessCheck {
        runner: runner.clone(),
        creds: creds.clone(),
        cred_base: cred_base.to_path_buf(),
        gh_hosts: gh.hosts.clone(),
        gh_endpoint: gh.mcp_endpoint.clone(),
    })
}

/// gather: list queued issues, resolve each project's VCS once (cached), and
/// drop issues whose project has no usable VCS integration. Mirrors the
/// scripted `input.command`.
///
/// `access` additionally preflights that the forge token can work
/// each resolved repo, once per repo per poll, and drops the repo's issues when
/// it cannot. `AI Agent` is left at `Queued` on that path: the condition is a
/// missing permission grant, not a defect in the issue, so the next poll after
/// the grant picks it up with no manual field edit. The probe covers
/// both capability categories the PR path crosses, a repository read and a
/// pull-request read (`actions/SCHEMA.md`, "Forgejo token scopes"), so a token
/// short of either scope is caught here rather than mid-run.
///
/// Returns the eligible jobs plus the input-phase events (the
/// per-project probe failures and no-VCS drops) that the caller persists to
/// `run_events` so they surface in the web UI rather than only in the journal.
fn gather(
    backend: &dyn IssueTracker,
    query: &str,
    access: Option<&dyn RepoAccess>,
) -> Result<Gathered> {
    let issues = backend.list_queued(query)?;
    // Every key the QUERY matched, before the VCS / access filters
    // below drop any. This is "still a candidate for this action", which is what
    // decides whether a dependency-gate row is still live.
    let candidate_keys: Vec<String> = issues
        .iter()
        .filter_map(|i| i.get("idReadable").and_then(|v| v.as_str()))
        .map(String::from)
        .collect();
    let mut vcs_cache: std::collections::HashMap<String, Option<provider::VcsRepo>> =
        std::collections::HashMap::new();
    // repo -> "the forge token can read it", so N issues in one
    // project (or across projects sharing a repo) cost ONE `repo view` call.
    let mut access_cache: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut jobs = Vec::new();
    let mut events: Vec<state::InputEvent> = Vec::new();
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
                    // Surface the drop so an operator can see why a
                    // project's issues never became jobs.
                    events.push(state::InputEvent {
                        level: "info".into(),
                        context: Some(project.clone()),
                        message: "issues skipped: project has no enabled VCS integration".into(),
                    });
                    None
                }
                // A probe tool error (e.g. `yt` exit 2) is NOT the same as
                // "no integration": the project may well have one and the
                // probe failed. Surface it as a provider-CLI error rather
                // than silently collapsing it into the drop path.
                Err(e) => {
                    warn!(project = %project, error = %e, "VCS probe failed (provider-CLI error); skipping project this poll, not treating as missing integration");
                    // Persist the probe failure so it is visible in
                    // the web UI, not only in the daemon journal.
                    events.push(state::InputEvent {
                        level: "warn".into(),
                        context: Some(project.clone()),
                        message: format!(
                            "VCS probe failed (provider-CLI error); issues skipped this poll: {e}"
                        ),
                    });
                    None
                }
            };
            vcs_cache.insert(project.clone(), resolved);
        }
        // Drop issues whose project has no enabled VCS; they cannot be cloned.
        let Some(repo) = vcs_cache.get(&project).and_then(|r| r.clone()) else {
            continue;
        };
        // Forge repo-access preflight. `git push` authenticates from
        // the host's git credentials, so a repo the forge token cannot see clones
        // and pushes fine and only fails at `fj pr create` with a bare 404, after
        // a whole Claude run is paid for. Probe the forge identity here instead
        // and skip the repo's issues for this poll.
        if let Some(access) = access {
            let host = host_only(&repo.repo_host);
            let cache_key = format!("{host}/{}", repo.repo_path);
            if !access_cache.contains_key(&cache_key) {
                let readable = match access.check(&host, &repo.repo_path) {
                    Ok(()) => {
                        // The recovery marker that clears the
                        // dashboard's access banner. Kept only when this repo was
                        // actually failing (`access::retain_access_recoveries`),
                        // so a healthy poll records nothing.
                        events.push(state::InputEvent {
                            level: "info".into(),
                            context: Some(project.clone()),
                            message: format!(
                                "{} for `{}` on {host}",
                                state::ACCESS_RECOVERY_PREFIX,
                                repo.repo_path
                            ),
                        });
                        true
                    }
                    Err(e) => {
                        warn!(project = %project, host = %host, repo = %repo.repo_path, error = %e, "forge repo-access preflight failed; skipping the repo's issues this poll");
                        events.push(state::InputEvent {
                            level: "warn".into(),
                            context: Some(project.clone()),
                            message: format!(
                                "forge repo-access preflight failed for `{}` on {host}; issues \
                                 skipped this poll and left Queued, so a permission grant is \
                                 picked up on the next poll with no field change: {e}",
                                repo.repo_path
                            ),
                        });
                        false
                    }
                };
                access_cache.insert(cache_key.clone(), readable);
            }
            if access_cache.get(&cache_key) != Some(&true) {
                continue;
            }
        }
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
    Ok(Gathered {
        jobs,
        events,
        candidate_keys,
    })
}

/// One poll's gather result.
struct Gathered {
    /// Workable jobs (query matches that survived the VCS / access filters).
    jobs: Vec<YoutrackJob>,
    /// Input-phase diagnostics the caller persists to `run_events`.
    events: Vec<state::InputEvent>,
    /// Every key the query matched, filters aside. A dependency-gate
    /// row for a key absent from this list holds nothing: the issue left the
    /// action's candidate set (resolved by hand, re-fielded, query changed), so
    /// the row is dead state that would otherwise sit in the stale-gate banner
    /// forever and teach the operator to ignore it.
    candidate_keys: Vec<String>,
}

/// Project key prefix of an issue id (`MK-16` -> `MK`).
fn project_key(idreadable: &str) -> String {
    idreadable
        .split('-')
        .next()
        .unwrap_or(idreadable)
        .to_string()
}

/// A candidate's gather-time verdict. Only [`Eligibility::Eligible`]
/// issues are built into executor items and compete for the KeyGate; a
/// [`Eligibility::Filtered`] (no longer Queued) or [`Eligibility::Blocked`]
/// (unmet dependency) issue is dropped before the gate so it never holds its
/// project's single per-poll concurrency slot.
enum Eligibility {
    Eligible,
    Filtered,
    Blocked,
}

/// Run the two read-only eligibility checks for one gathered
/// candidate BEFORE any executor item is built or the KeyGate acquired: the live
/// `AI Agent` re-read (drop unless still Queued) then the dependency
/// gate (drop an issue with unmet blockers). A blocked issue keeps every
/// dependency-gate side effect; this only moves WHEN the gate runs (at gather, not
/// after winning the concurrency key), so a blocked issue no longer consumes the
/// project's per-poll work slot. `process_job` still re-runs both checks after it
/// wins the key: that post-gate re-read is the authoritative cross-run TOCTOU
/// guard right before the first mutation (`src/concurrency.rs` module docs).
async fn evaluate_eligibility(
    pool: &DbPool,
    backend: &dyn IssueTracker,
    src: &YoutrackSource,
    action: &str,
    job: &YoutrackJob,
    bypass_backoff: bool,
) -> Eligibility {
    let key = &job.key;
    // ---- filter: live AI Agent re-read (drop unless still Queued) ----
    match backend.get_field(key, &src.custom_field) {
        Ok(v) if v.trim() == src.states.queued => {}
        Ok(v) => {
            info!(action = %action, key = %key, ai_agent = %v.trim(), "job filtered at gather (no longer Queued); not competing for the concurrency slot");
            return Eligibility::Filtered;
        }
        Err(e) => {
            // Conservative: a lookup failure drops the candidate this poll so a
            // transient error cannot let an ineligible issue be worked; a
            // still-Queued job returns on the next gather.
            warn!(action = %action, key = %key, error = %e, "AI Agent lookup failed at gather; dropping this poll");
            return Eligibility::Filtered;
        }
    }
    // ---- dependency gate ----
    match dependency_gate(pool, backend, src, action, key, bypass_backoff).await {
        Ok(GateDecision::Proceed) => Eligibility::Eligible,
        Ok(GateDecision::Blocked { blockers }) => {
            info!(
                action = %action,
                key = %key,
                blockers = %blockers.join("; "),
                "job blocked by unmet dependencies at gather; left Queued, no attempt consumed, not competing for the concurrency slot"
            );
            Eligibility::Blocked
        }
        // A dependency-read failure must not strand an otherwise-workable issue:
        // fall back to pickup (the agent self-parks if it truly cannot proceed).
        Err(e) => {
            warn!(action = %action, key = %key, error = %e, "dependency gate read failed at gather; proceeding with pickup");
            Eligibility::Eligible
        }
    }
}

/// Keep only the workable candidates, dropping every issue the
/// gather-time eligibility pass rules out (no-longer-Queued or dependency-
/// blocked). Runs per candidate, before the executor is built, so ineligible
/// issues never compete for a project's concurrency key.
async fn filter_eligible(
    pool: &DbPool,
    backend: &dyn IssueTracker,
    src: &YoutrackSource,
    action: &str,
    jobs: Vec<YoutrackJob>,
    bypass_backoff: bool,
) -> Vec<YoutrackJob> {
    let mut eligible = Vec::with_capacity(jobs.len());
    for job in jobs {
        if let Eligibility::Eligible =
            evaluate_eligibility(pool, backend, src, action, &job, bypass_backoff).await
        {
            eligible.push(job);
        }
    }
    eligible
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
    pool: &DbPool,
    claude: &ClaudeInvoker,
    backend: &dyn IssueTracker,
    forge: &dyn Forge,
    src: &YoutrackSource,
    limits: &EffectiveLimits,
    cancel: &CancellationToken,
    pause: &crate::pause::RunPause,
    // The run's shared ledger and frozen ceiling.
    spend: &crate::usage::RunSpend,
    job: &YoutrackJob,
    pr_strategy: crate::action::PrStrategy,
    bypass_backoff: bool,
    // The run row this job belongs to, for the invocation's usage
    // record; the job itself already threads `run_id` through the calling
    // scheduler loop for `run_inflight_add`/`run_inflight_remove`.
    run_id: i64,
) -> Result<ProcessedJob> {
    let started = Instant::now();
    let action = &loaded.action;
    let key = &job.key;
    let attempts = state::attempts_get(pool, &action.name, key).await?;

    // YouTrack jobs always carry a stable key (idReadable), so the attempt cap
    // always applies.
    // The issue no longer just sits at Queued to be skipped again
    // on the next poll. It gets one comment naming the cap and moves to
    // Needs Review, the same label the needs-review marker and the timeout
    // path use for "parked pending a human decision". A write failure here
    // must not swallow the cap hit: `report_cap_reached` logs each failure at
    // `error` and this still returns `CapHit` so the banner keeps
    // showing the key.
    if attempts >= limits.max_attempts_per_key {
        warn!(
            action = %action.name, key = %key, attempts,
            cap = limits.max_attempts_per_key,
            "attempt cap reached; reporting and moving to Needs Review"
        );
        report_cap_reached(
            backend,
            src,
            &action.name,
            key,
            attempts,
            limits.max_attempts_per_key,
        );
        return Ok(ProcessedJob::CapHit { key: key.clone() });
    }

    info!(action = %action.name, key = %key, attempt = attempts + 1, "processing job");

    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), paths);
    let workdir_path = workdir::job_workdir(&wb_base, &action.name, key);

    // ---- filter: live AI Agent re-read ----
    // The gather-time eligibility pass ([`filter_eligible`]) already
    // dropped issues that were not Queued, so this poll's candidate never should
    // have reached the KeyGate ineligible. This re-read is the authoritative
    // post-gate re-check for the cross-run TOCTOU: an overlapping run may have
    // moved this issue to Working since gather, and it must be caught before the
    // first mutation (`src/concurrency.rs` module docs).
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

    // ---- dependency gate ----
    // The gather-time pass already dropped issues with unmet blockers
    // before the KeyGate, so a blocked issue no longer holds the project's per-poll
    // slot. This post-gate re-check catches the rare case where a dependency became
    // unmet between gather and winning the key; the gate's side effects are
    // idempotent, so re-running it here is safe. AI Agent is confirmed Queued and
    // nothing has been mutated. Skip a queued issue whose blockers are still open:
    // leave it Queued (auto-resumes when the dependency clears), consume no
    // attempt, and never clone or invoke claude. A disabled gate (or a met/absent
    // dependency set) returns `Proceed`.
    match dependency_gate(pool, backend, src, &action.name, key, bypass_backoff).await {
        Ok(GateDecision::Proceed) => {}
        Ok(GateDecision::Blocked { blockers }) => {
            info!(
                action = %action.name,
                key = %key,
                blockers = %blockers.join("; "),
                "job blocked by unmet dependencies; left Queued, no attempt consumed"
            );
            return Ok(ProcessedJob::Blocked {
                key: key.clone(),
                blockers,
            });
        }
        // A dependency-read failure must not strand an otherwise-workable issue:
        // fall back to the pre-gate behavior (pick it up; the agent self-parks if
        // it truly cannot proceed). The gate is an optimization over that correct
        // baseline, not a correctness gate.
        Err(e) => {
            warn!(action = %action.name, key = %key, error = %e, "dependency gate read failed; proceeding with pickup");
        }
    }

    // ---- resume an existing open PR's branch on a re-queue ----
    // A DB lookup (async) plus, when a record is found, a live confirmation
    // against the forge (sync, so wrapped). Any failure here just means a fresh
    // pickup; it never fails the job.
    let resume = match resolve_pr_resume(pool, forge, job, pr_strategy).await {
        Ok(r) => r,
        Err(e) => {
            warn!(action = %action.name, key = %key, error = %e, "PR-resume lookup failed; starting from the default branch");
            None
        }
    };

    // A preserved workdir from a failed finalize (unpushed commits)
    // must not be clobbered by this attempt's clone; move it aside first.
    preserve_aside(&workdir_path);
    std::fs::create_dir_all(&workdir_path)?;

    // ---- pre: mark Working / In Progress, then clone ----
    // `yt` field writes plus a full `git clone` of the target repo,
    // all synchronous; keep them off the async worker thread.
    let resume_branch = resume.as_ref().map(|r| r.branch.as_str());
    let pre_result = crate::blocking::in_place(|| {
        run_pre(backend, forge, src, job, &workdir_path, resume_branch)
    });

    // ---- prompt + claude (skipped on pre failure / cancel / global pause) ----
    // The gate is read once here (not in a match guard, which cannot
    // `await`) and reused below so a paused job's attempt is not counted.
    let paused_before_claude = pre_result.is_ok() && !cancel.is_cancelled() && pause.stop().await;
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
        Ok(()) if paused_before_claude => {
            warn!(action = %action.name, key = %key, "globally paused before claude started (finalize still runs)");
            (
                ClaudeResult::synthesized(
                    "run stopped by global pause",
                    "globally paused before claude started",
                ),
                false,
            )
        }
        Ok(()) => {
            let timeout_minutes = limits.claude_timeout.as_secs() / 60;
            let prompt = build_prompt(src, job, &workdir_path, timeout_minutes, resume.as_ref());
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
                // Cap the child at the run's remaining ceiling.
                Some(spend.remaining_usd()),
            )?;
            // Bill the run before any branch discards `r`.
            spend.record(&r);
            // Persist this invocation's plan usage before any
            // branch discards `r`, mirroring the billing call above.
            crate::usage::record_invocation(
                pool,
                &action.name,
                Some(key),
                Some(run_id),
                &stream_log,
                &r,
            )
            .await;
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
        // The preserve decision runs `git ls-remote` against the
        // remote (network) and may then run `git clean -fdX`; keep it off the
        // async worker thread.
        let token_url = clone_url_for(forge, job);
        crate::blocking::in_place(|| {
            cleanup_or_preserve(&workdir_path, &action.name, key, Some(&token_url))
        });
        return Ok(ProcessedJob::Transient { key: key.clone() });
    }

    if claude_ran {
        state::daily_cost_add(pool, &action.name, claude_result.cost).await?;
    }

    // Computed once, before finalize, so both finalize's
    // compaction check and the `ProcessedJob::Done` field below read the same
    // path; `None` when claude never ran, so there is no log to check.
    let stream_log = claude_ran.then(|| {
        ClaudeInvoker::stream_log_path(&paths.logs_dir(), &action.name, key, attempts + 1)
    });

    // ---- finalize (the native "post"): every external mutation lives here ----
    // Synchronous. Finalize no longer polls CI (that
    // wait is now driven by a webhook event), so it returns promptly.
    let finalize_res = crate::blocking::in_place(|| {
        finalize(
            backend,
            forge,
            src,
            job,
            &workdir_path,
            &claude_result,
            pr_strategy,
            stream_log.as_deref(),
            limits.claude_timeout,
            resume.is_some(),
        )
    });
    // ---- record a newly opened PR against its issue ----
    // Only for a fresh PR (`resume` is `None`): a resumed job's PR is already
    // recorded, and re-inserting it would collide with the store's
    // `UNIQUE(host, repo, pr_number)`. `finalize` cannot make this DB write
    // itself: it is a synchronous state machine whose own unit tests run with
    // no tokio runtime, so the write happens here, right after it returns.
    if resume.is_none() {
        if let Ok(
            Finalized::CodeReview {
                pr_number: Some(pr_number),
                head_branch: Some(head_branch),
                ..
            }
            | Finalized::TimedOut {
                pr_number: Some(pr_number),
                head_branch: Some(head_branch),
            },
        ) = &finalize_res
        {
            record_new_pr(
                pool,
                forge,
                backend,
                &action.name,
                job,
                *pr_number,
                head_branch,
            )
            .await;
        }
    }
    let post_failed = match &finalize_res {
        Ok(Finalized::CodeReview { pr_url, .. }) => {
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

    // ---- skip-attempt marker (parked / blocker), cancel, or pause ----
    let skip_marker = workdir_path.join(SKIP_ATTEMPT_MARKER);
    let cancelled = cancel.is_cancelled();
    let skip_attempt = skip_marker.exists() || cancelled || paused_before_claude;
    if skip_attempt {
        info!(action = %action.name, key = %key, cancelled, paused = paused_before_claude, "not counting attempt");
    }

    let ok = claude_result.ok && !post_failed;
    let stream_log_path = stream_log.as_deref().map(|p| p.display().to_string());

    if !skip_attempt {
        state::attempts_increment(pool, &action.name, key).await?;
    }
    if claude_ran {
        state::daily_count_increment(pool, &action.name).await?;
    }

    // A finalize failure can cost a retry, never the work. Only a
    // successfully opened PR skips the branch-vs-remote check (the AGit push
    // leaves no remote-tracking ref, so that check cannot clear it); every
    // other outcome deletes only when no unpushed commits would be lost. A
    // panic anywhere above preserves trivially: deletion only ever happens
    // here, explicitly.
    // Same as above, the preserve branch shells out to git.
    // A Code Review PR still leaves the branch-vs-remote check
    // skipped, but a dirty tree or stash is work that never made it into the
    // PR at all, so it is checked and preserved just the same.
    crate::blocking::in_place(|| match &finalize_res {
        Ok(Finalized::CodeReview { .. })
        | Ok(Finalized::TimedOut {
            pr_number: Some(_),
            head_branch: Some(_),
        }) => match crate::workdir::uncommitted_or_stashed(&workdir_path) {
            Ok(dirty) if dirty.is_empty() => cleanup_workdir(&workdir_path),
            Ok(dirty) => {
                warn!(
                    action = %action.name,
                    key = %key,
                    workdir = %workdir_path.display(),
                    dirty_paths = dirty.paths.len(),
                    stashes = dirty.stashes.len(),
                    "workdir preserved after PR: uncommitted work not in the PR"
                );
            }
            Err(e) => {
                tracing::error!(
                    action = %action.name,
                    key = %key,
                    workdir = %workdir_path.display(),
                    error = %e,
                    "workdir preserved after PR: uncommitted-work check failed"
                );
            }
        },
        _ => cleanup_or_preserve(
            &workdir_path,
            &action.name,
            key,
            Some(&clone_url_for(forge, job)),
        ),
    });

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

/// An open recorded PR this job continues rather than reopening.
struct PrResume {
    /// The PR's server head branch, cloned instead of the default branch.
    branch: String,
    /// Human-facing PR URL, rendered into the `{{existing_pr_url}}` placeholder.
    pr_url: String,
}

/// Look up whether `job`'s issue already has an open PR recorded
/// against it and, if so, confirm it is still open before resuming its branch.
///
/// Returns `Ok(None)` on every path that should fall back to opening a fresh
/// PR from the default branch: no record, a record already `merged` /
/// `closed`, the AGit strategy (no server branch to continue on), or a failed
/// live confirmation (the record might be stale; a false resume is worse than
/// a redundant PR, so this errs toward a fresh pickup exactly like the rest of
/// this pipeline's read-then-degrade checks).
async fn resolve_pr_resume(
    pool: &DbPool,
    forge: &dyn Forge,
    job: &YoutrackJob,
    pr_strategy: crate::action::PrStrategy,
) -> Result<Option<PrResume>> {
    use crate::ci_failure::store;
    let Some(rec) = store::agent_pr_for_issue(pool, &job.key).await? else {
        return Ok(None);
    };
    if rec.state != "open" {
        return Ok(None);
    }
    if matches!(pr_strategy, crate::action::PrStrategy::Agit) {
        info!(key = %job.key, pr = rec.pr_number, "CLAUDE-275: recorded PR uses the AGit strategy, which has no server branch to resume; opening a new PR");
        return Ok(None);
    }
    let host = host_only(&job.repo_host);
    let repo = job.repo_path.clone();
    let open = match crate::blocking::in_place(|| forge.list_open_prs(&host, &repo)) {
        Ok(prs) => prs,
        Err(e) => {
            warn!(key = %job.key, pr = rec.pr_number, error = %e, "CLAUDE-275: could not confirm the recorded PR is still open; opening a new PR as before");
            return Ok(None);
        }
    };
    if let Some(pr) = open.iter().find(|p| p.number as i64 == rec.pr_number) {
        info!(key = %job.key, pr = rec.pr_number, branch = %rec.head_branch, "CLAUDE-275: resuming the recorded open PR's branch");
        // Prefer the listed PR's own URL, which is correct for any
        // backend (GitHub's `/pull/<n>` vs Forgejo's `/pulls/<n>`); the `format!`
        // is only a fallback for a listing that carries no `html_url`.
        let pr_url = if pr.html_url.is_empty() {
            format!(
                "{}/{}/pulls/{}",
                job.repo_host, job.repo_path, rec.pr_number
            )
        } else {
            pr.html_url.clone()
        };
        return Ok(Some(PrResume {
            branch: rec.head_branch.clone(),
            pr_url,
        }));
    }
    // Confirmed no longer open. Whether it was merged or closed cannot be told
    // apart from an open-PR listing alone; mark it closed so a stale "open"
    // record cannot cause another resume attempt.
    if let Err(e) = store::set_agent_pr_state(pool, rec.id, "closed", &state::now_iso()).await {
        warn!(key = %job.key, pr = rec.pr_number, error = %e, "CLAUDE-275: failed to mark the recorded PR closed (non-fatal)");
    }
    info!(key = %job.key, pr = rec.pr_number, "CLAUDE-275: recorded PR is no longer open; marked closed and starting from the default branch");
    Ok(None)
}

/// Record a freshly opened PR against its issue in the
/// `agent_pull_requests` store, so a later re-queue of the same issue can
/// resume its branch instead of opening a second PR. Best-effort: a write
/// failure is logged at `error`, surfaced as a `run_events` row via
/// [`Forge::note`] (drained by the caller exactly like a degraded lookup), and
/// named in a follow-up issue comment, but never fails the job - the PR
/// already exists and the issue already sits at Code Review.
async fn record_new_pr(
    pool: &DbPool,
    forge: &dyn Forge,
    backend: &dyn IssueTracker,
    action_name: &str,
    job: &YoutrackJob,
    pr_number: u64,
    head_branch: &str,
) {
    use crate::ci_failure::store;
    let host = host_only(&job.repo_host);
    let now = state::now_iso();
    if let Err(e) = store::record_agent_pr(
        pool,
        action_name,
        &job.key,
        &host,
        &job.repo_path,
        pr_number as i64,
        head_branch,
        "open",
        &now,
    )
    .await
    {
        tracing::error!(action = %action_name, key = %job.key, pr = pr_number, error = %e, "CLAUDE-275: failed to record the agent PR in the store");
        let message = format!(
            "CLAUDE-275: recording PR #{pr_number} against `{}` in the agent_pull_requests store failed ({e}); a re-queue of this issue will not resume this PR's branch and may open a second one",
            job.repo_path
        );
        forge.note(message.clone());
        if let Err(e) = backend.comment(&job.key, &message) {
            warn!(key = %job.key, error = %e, "PR-record-failure comment also failed (continuing)");
        }
    }
}

/// One unmet blocker: the linked issue's id and the direction-resolved verb.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Blocker {
    id: String,
    verb: String,
}

impl Blocker {
    /// Human rendering used in the comment, the outcome, and the signature.
    fn display(&self) -> String {
        format!("{} ({})", self.id, self.verb)
    }
}

/// The pre-clone dependency gate's verdict.
enum GateDecision {
    /// No unmet dependency: pickup proceeds exactly as before.
    Proceed,
    /// Blocked by unmet dependencies; the issue stays Queued.
    Blocked { blockers: Vec<String> },
}

/// Pure: the unmet blockers among `links`, per the `block_on` verbs. A link
/// blocks iff its direction-resolved `verb` is listed in `block_on` and its
/// target is unresolved (`resolved` is `None`). Direction is already baked into
/// the verb by YT-40 (`depends on` = Depend INWARD, `parent for` = Subtask
/// OUTWARD, etc.), so matching the verb resolves direction exactly. With
/// `own_project` set (`block_cross_project: false`), a target in any other
/// project never blocks. The result is sorted and de-duplicated so its signature
/// is order-independent.
fn unmet_blockers(
    links: &[provider::IssueLink],
    block_on: &[String],
    own_project: Option<&str>,
) -> Vec<Blocker> {
    let mut out: Vec<Blocker> = links
        .iter()
        .filter(|l| l.resolved.is_none())
        .filter(|l| block_on.iter().any(|v| v == &l.verb))
        .filter(|l| own_project.is_none_or(|p| project_key(&l.target) == p))
        .map(|l| Blocker {
            id: l.target.clone(),
            verb: l.verb.clone(),
        })
        .collect();
    out.sort_by(|a, b| (&a.id, &a.verb).cmp(&(&b.id, &b.verb)));
    out.dedup();
    out
}

/// Canonical, order-independent signature of an (already sorted) blocker set.
/// Equality drives comment idempotency: re-comment only when it changes.
fn blocker_signature(blockers: &[Blocker]) -> String {
    blockers
        .iter()
        .map(Blocker::display)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The blocked-state comment body: lists each unmet blocker by id and
/// relationship. Posted only on entering the blocked state or when the set
/// changes.
fn blocked_comment(blockers: &[Blocker]) -> String {
    let mut s = String::from(
        "Dependency gate: this issue has unmet dependencies, so it was not picked up. \
         It stays Queued and will be re-evaluated automatically; no attempt was consumed.\n\n\
         Unmet blockers:\n",
    );
    for b in blockers {
        s.push_str(&format!("- {} ({})\n", b.id, b.verb));
    }
    s
}

/// The dependency gate. Returns [`GateDecision::Proceed`] when the
/// issue has no unmet dependency (clear, none, or its backoff just re-evaluated
/// clean), else [`GateDecision::Blocked`]. Side effects on the blocked path: post
/// the blocker comment (idempotently), and persist the re-check backoff
/// (`blocked_until`) plus the blocker-set signature. While a backoff window is
/// open the issue is not re-evaluated at all (no `yt` read, no comment); the
/// persisted signature reconstructs the blocker list for the outcome.
///
/// Invoked in two places, and the backoff makes the pair cheap. The
/// gather-time pass ([`filter_eligible`]) runs it BEFORE the KeyGate to drop a
/// blocked issue so it never holds the project's per-poll slot; [`process_job`]
/// re-runs it after winning the key as the authoritative pre-clone re-check. A
/// workable issue persists no state, so the second read is a plain re-read; a
/// blocked issue is dropped at gather and never reaches `process_job`.
///
/// `bypass_backoff` forces the live `issue_links` read even inside an
/// open backoff window. It is set for human-initiated runs (`claude-run trigger`,
/// direct `claude-run run`), where the operator expects an evaluation against the
/// live tracker, not a replay of the cached signature. The scheduler's periodic
/// pickup passes `false`, keeping the throttle above. Every other side effect
/// (signature/`blocked_until` persistence, row clear, changed-set comment) is
/// unchanged; bypass only skips the window short-circuit.
///
/// Every blocked return logs how long the issue has been held, at
/// `warn` once past [`state::STALE_GATE_AFTER_MS`]. A gate that stops resuming is
/// otherwise silent at every layer: the issue stays Queued with no field change,
/// a poll whose candidates are all blocked returns before `run_begin` so no run
/// row is written, and the journal said nothing above `info`.
async fn dependency_gate(
    pool: &DbPool,
    backend: &dyn IssueTracker,
    src: &YoutrackSource,
    action: &str,
    key: &str,
    bypass_backoff: bool,
) -> Result<GateDecision> {
    let deps = &src.dependencies;
    // Disabled: a no-op. No `yt` read, no DB touch; pickup proceeds as before.
    if !deps.enabled {
        return Ok(GateDecision::Proceed);
    }
    let prev = state::dependency_gate_get(pool, action, key).await?;
    let now_ms = state::now_epoch_ms();

    // Backoff: within the window, do not re-evaluate (no `yt` read, no comment).
    // A manual trigger (`bypass_backoff`) skips this short-circuit and reads live.
    if let Some(p) = &prev {
        if !bypass_backoff && now_ms < p.blocked_until_ms {
            let blockers = p
                .signature
                .split("; ")
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            log_held(action, key, &p.signature, p.blocked_since_ms, now_ms);
            return Ok(GateDecision::Blocked { blockers });
        }
    }

    // Re-evaluate: read the live links and compute the unmet blocker set.
    let links = backend.issue_links(key)?;
    let own_project = (!deps.block_cross_project).then(|| project_key(key));
    let blockers = unmet_blockers(&links, &deps.block_on, own_project.as_deref());
    if blockers.is_empty() {
        // Cleared (or never blocked): drop any persisted gate state, proceed.
        if prev.is_some() {
            state::dependency_gate_clear(pool, action, key).await?;
        }
        return Ok(GateDecision::Proceed);
    }

    let signature = blocker_signature(&blockers);
    // Comment only when the unmet-blocker set changed since the last comment.
    let changed = prev
        .as_ref()
        .map(|p| p.signature != signature)
        .unwrap_or(true);
    if changed {
        if let Err(e) = backend.comment(key, &blocked_comment(&blockers)) {
            // Best-effort: a comment failure must not strand the gate.
            warn!(action = %action, key = %key, error = %e, "dependency-gate blocker comment failed (non-fatal)");
        }
    }

    // Persist the re-check backoff window and the current signature. The
    // recheck_timeout is validated at action load, so the parse cannot fail here;
    // the fallback keeps the gate total regardless.
    let recheck = humantime::parse_duration(&deps.recheck_timeout)
        .unwrap_or_else(|_| Duration::from_secs(300));
    let blocked_until = now_ms + recheck.as_millis() as i64;
    state::dependency_gate_upsert(pool, action, key, blocked_until, &signature).await?;

    // An unchanged row keeps its original stamp, so this measures the
    // whole block, not this evaluation.
    let blocked_since = prev.map(|p| p.blocked_since_ms).unwrap_or(now_ms);
    log_held(action, key, &signature, blocked_since, now_ms);

    Ok(GateDecision::Blocked {
        blockers: blockers.iter().map(Blocker::display).collect(),
    })
}

/// Log how long the gate has held this issue, escalating to `warn`
/// past [`state::STALE_GATE_AFTER_MS`]. The gate's own comment promises automatic
/// re-evaluation, so a block that outlives the threshold is either a dependency
/// nobody is working or a re-evaluation that stopped happening; both are worth an
/// operator's attention and neither announces itself anywhere else.
fn log_held(action: &str, key: &str, signature: &str, blocked_since_ms: i64, now_ms: i64) {
    let held_ms = (now_ms - blocked_since_ms).max(0);
    if held_ms >= state::STALE_GATE_AFTER_MS {
        warn!(
            action = %action,
            key = %key,
            blockers = %signature,
            held_secs = held_ms / 1000,
            "dependency gate has held this issue past the staleness threshold; it stays Queued and is not being worked"
        );
    } else {
        info!(action = %action, key = %key, blockers = %signature, held_secs = held_ms / 1000, "dependency gate holding issue");
    }
}

/// Pre steps: mark the issue Working + In Progress, then clone the repo into
/// `workdir`. Returns the failure reason on any step error so the caller can
/// synthesize a failed claude result and still finalize.
fn run_pre(
    backend: &dyn IssueTracker,
    forge: &dyn Forge,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    // The recorded PR's head branch to resume, or `None` for the
    // default-branch clone this always used to be.
    resume_branch: Option<&str>,
) -> std::result::Result<(), String> {
    backend
        .set_field(&job.key, &src.custom_field, &src.states.working)
        .map_err(|e| format!("set {}={}: {e}", src.custom_field, src.states.working))?;
    backend
        .set_state(&job.key, &src.states.in_progress)
        .map_err(|e| format!("set State={}: {e}", src.states.in_progress))?;
    // Optionally assign the issue before cloning, so an assignee
    // failure aborts pickup before any repo work (same contract as the field /
    // state writes above). Unset leaves the Assignee untouched.
    if let Some(login) = &src.assignee {
        backend
            .set_assignee(&job.key, login)
            .map_err(|e| format!("set Assignee={login}: {e}"))?;
    }
    clone_repo(&clone_url_for(forge, job), workdir, resume_branch)
        .map_err(|e| format!("git clone: {e}"))?;
    install_hooks(workdir);
    Ok(())
}

/// Install the repo's git hooks on a fresh clone so a
/// commit or push that fails the repo's local checks is blocked locally instead of only
/// being caught by CI (the gap that produced A8N-69). Detects a `just
/// install-hooks` recipe and, when present, runs it; the recipe is idempotent
/// (it rewrites `.git/hooks/pre-commit` and, under the shared task runner's
/// default `hook_layout`, `.git/hooks/pre-push` too), so running it on every
/// clone is safe.
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

/// The URL to clone this job's repo from.
///
/// Prefer the token-bearing URL the forge builds from its own key
/// store, the same one the comment webhook already uses.
/// `job.clone_url` comes from the YouTrack VCS integration and carries no
/// credential, so on a host with no git credential of its own (the container,
/// deliberately) every clone failed with `could not read Username`.
///
/// Falls back to the plain URL when the forge cannot resolve one, because a host
/// install authenticating through a credential helper or `~/.netrc` must keep
/// working exactly as before. The failure is logged once, naming the host.
fn clone_url_for(forge: &dyn Forge, job: &YoutrackJob) -> String {
    // `Forge::clone_url` looks the host up in the key store and formats the URL
    // itself, so it wants a bare host; `repo_host` carries the scheme.
    let host = job
        .repo_host
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    match forge.clone_url(host, &job.repo_path) {
        Ok(url) => url,
        Err(e) => {
            warn!(
                host = %host,
                repo = %job.repo_path,
                error = %e,
                "no forge credential for this host; cloning with the unauthenticated URL and \
                 whatever git credentials the environment provides"
            );
            job.clone_url.clone()
        }
    }
}

/// Shallow-clone `clone_url` into `workdir` (already created, empty).
///
/// The URL may carry a token, so the shared helper captures and redacts git's
/// output rather than letting it inherit the daemon's stderr; the reason string
/// this produces reaches the action page and can reach a YouTrack comment.
fn clone_repo(clone_url: &str, workdir: &Path, branch: Option<&str>) -> Result<()> {
    provider::shallow_clone(clone_url, branch, workdir, 50)
}

/// The built-in YouTrack agent prompt as a placeholder template.
/// Returns the default instruction text - local-only code work (no push, no PR)
/// plus the bounded tracker writes it permits - with the documented
/// `{{...}}` placeholders that [`render_prompt_template`] substitutes per job.
/// Exposed crate-wide so the web form can prefill the prompt editor with the
/// live default.
///
/// This replaces a blanket YouTrack ban with an explicit permission
/// block, because the `No orphan notes` directive requires a run that discovers
/// a gap to file it as a tracked, linked issue rather than mention it in prose.
/// That permission splits into two cases: a descoped piece of the
/// triggering issue is filed in `write_projects` and linked `subtask of`; a
/// bug found outside the triggering issue's scope is filed in the project
/// whose VCS integration binds the buggy repo (falling back to
/// `write_projects`) and linked `relates to`. The ban that stays absolute is
/// `AI Agent` on ANY issue: setting it queues work, so an agent that could set
/// it could spawn runs.
pub(crate) fn default_youtrack_prompt() -> String {
    [
        "You are working a YouTrack issue end-to-end. Follow the YouTrack and Git workflows from CLAUDE.md.",
        "",
        "Issue: {{key}}: {{summary}}",
        "Repo: {{clone_url}}",
        "Working directory: {{workdir}} - the repo is already cloned here, you are inside it.",
        "",
        "You have {{timeout_minutes}} minutes of wall-clock time; the run is killed at that limit. Budget it: commit your work in checkpoints as you go (see step 4) so a kill still leaves committed progress, and spend the budget on targeted verification rather than repeated full runs (see step 5).",
        "",
        "Do ONLY local engineering work on code: do NOT push and do NOT open a PR. The runner finalizes the issue after you exit: it opens the PR from your local commits and sets the AI Agent field based on the run outcome.",
        "",
        "YouTrack writes. You MAY do exactly the following, and nothing else:",
        "- Read anything the token can see, across every project: 'yt list --query=...', 'yt search', 'yt issue inspect <KEY>', 'yt project list' / 'yt project show' / 'yt project vcs list'.",
        "- Create an issue with 'yt issue create' in exactly two cases:",
        "  1. A gap, deferred item, or descoped piece of {{key}} itself: create it ONLY in these projects: {{write_projects}}, then link it with 'yt issue link <new-issue> {{key}} --type \"subtask of\"'. This is how the CLAUDE.md 'No orphan notes' rule is satisfied: the piece becomes its own tracked issue instead of a sentence in the PR body that nobody owns.",
        "  2. A bug you find that is NOT part of {{key}}, in this repo or in any other repo (for example a defect in a server repo you hit while working on the app that calls it). If it blocks {{key}}'s acceptance criteria and lives in this repo, fix it as part of {{key}}'s change and file nothing. Otherwise do NOT fix it in this run; file it. First run 'yt search' for an open issue that already reports it; if one exists, file nothing and name that issue in your final summary. Create it in the project whose VCS integration binds the repo the bug lives in: find that project with 'yt project list' and 'yt project vcs list <PROJECT>', and if no project or more than one project binds that repo, create it in {{write_projects}}. Its description states what you observed, how to reproduce it, and the file:line or command output that shows it. Then link it with 'yt issue link <new-issue> {{key}} --type \"relates to\"'. If the bug blocks {{key}}'s acceptance criteria from another repo, also take the blocker path below and name the new issue in the reason.",
        "- Update, comment on, and link ONLY the issues you created during THIS run: 'yt issue update', 'yt issue comment', 'yt issue link'.",
        "- On the triggering issue {{key}}, you MAY comment with 'yt issue comment {{key}}' (see the size-note instruction in step 1 below), and you MAY add the links named in the two create cases above, one per issue you created. The runner still posts its own outcome comment on {{key}}; do not duplicate it.",
        "",
        "HARD BANS on YouTrack, no exceptions:",
        "- NEVER set the 'AI Agent' field on ANY issue, including issues you created yourself: no 'yt issue set-field ... \"AI Agent\" ...', and no equivalent through 'yt issue update' or 'yt issue apply'. Setting that field queues an agent run, so an agent that can set it can spawn runs. It belongs to the runner alone.",
        "- No other write to {{key}}: no description or summary edit, no tag, no assignee change, no 'yt issue set-state', no 'yt issue set-field'. The runner owns the outcome comment and the fields on {{key}}, and its State stays In Progress until a human merges the PR.",
        "- No write of any kind to an issue you did not create during this run.",
        "- Never run 'yt issue delete' anywhere.",
        "",
        "Steps:",
        "1. Run 'yt issue inspect {{key}}' to read the full description and acceptance criteria. If you believe {{key}} is too large to finish and verify within the time budget, post one short comment on {{key}} with 'yt issue comment {{key}}' saying why and which layers you expect to be at risk, early, before you start changing code. Do not stop for this and do not use the blocker markers for this: post the comment, then continue with the work regardless. There is no size limit and no refusal; this is a paper trail for a possible timeout, not a decision point.",
        "2. If any acceptance criterion depends on another issue, repo, endpoint, or symbol, verify that dependency exists in the source of truth NOW, before writing code: read the actual file, query the endpoint, or grep for the symbol on the current default branch. Do NOT trust another issue's Done or Resolved state - an issue marked Done may not have shipped its code. If the depended-on artifact is missing or contradicts this issue, take the blocker path below instead of building against it.",
        "3. If {{existing_branch}} is empty, create a feature branch off the default branch (name it for the change, e.g. 'fix/...', 'feat/...', 'chore/...'). If {{existing_branch}} is NOT empty, that branch is already checked out here with its open PR at {{existing_pr_url}}: this issue was re-queued onto its own existing PR, so run 'yt issue inspect {{key}}' and read the newest comments for what needs fixing, ALSO read that PR's own comments (a prior timed-out run may have left one explaining what it salvaged and why, e.g. a draft PR opened from uncommitted work at kill time), then fix it on this SAME branch - do not create another branch or PR. Either way, do ALL work on that branch and leave it checked out as HEAD when you stop. Never run 'git checkout main' / 'git switch main' or otherwise move HEAD off the feature branch: the runner opens the PR from whatever branch is checked out, so switching back to 'main' strands your work on the unpushed branch and produces an empty PR.",
        "4. Make the minimum change required to satisfy the acceptance criteria, in coherent layers (for example schema, model, API, UI, docs). After each layer, make a local commit whose body ends with the bare '#{{key}}' reference, so a kill leaves committed progress instead of none; the final commit still carries the full PR-description format from step 7 and stays the branch tip.",
        "5. Test discipline: while iterating, run only the targeted tests for the code you just changed (the repo's single-test or single-binary form), not the full suite. Run the repo's full check suite (e.g. the repo's 'just check', tests, or build) exactly once, after formatting and lint are clean and the work is complete. Never re-run a long command only to read its output again: capture it to a file outside {{workdir}} the first time and read the file. If a long command fails in a way that needs a fix, re-run only what failed until it is green, then run the full check suite once more. HARD RULE: run every long command in the FOREGROUND and wait for it inline. Do NOT background a build, check, or watch and poll/wait on it across turns. Leave NO background task running when you end your turn: any lingering background process keeps the run's process group alive and will get the whole run timeout-killed (exit 124) even after the functional work is complete - that is what blew the run timeout previously. Kill or wait out every spawned background job before you stop. This is enforced, not just requested: the runner starts every session with background tasks disabled and the 'Monitor' / 'ScheduleWakeup' tools disallowed, so a turn that ends still waiting on either is never finalized as a success.",
        "6. Immediately before your final commit, re-run 'yt issue inspect {{key}}' and compare its description, acceptance criteria, and every comment against what step 1 (or step 3's resume read) showed. If nothing changed, continue to the commit. If anything changed, treat the latest text as authoritative: make the additional changes it requires, re-run the check suite (step 5) to green, and only then commit. If you already made a commit before reaching this step, this re-read still happens before the LAST commit: any further commits made after it are the final ones. If the new text makes the work unsafe, contradictory, or ambiguous, take the blocker path below instead of committing.",
        "7. The runner uses this commit's body as the PR description (if you make more than one commit, the LAST commit carries it, since the runner reads only the branch tip); it joins any hard-wrapped prose lines back together before posting it, but do not rely on that, write it in exactly this format, in order: (a) '## Before this PR is merged' as the FIRST section, but ONLY when the change has a human prerequisite for merging (a secret, a grant, a deploy step, a dependency PR) - name the exact action, identifier and owner; omit the section entirely when there is none, no empty heading and no 'None'. (b) The description of the change under Markdown ATX headings, in order '## Summary', then '## Changes', then '## Verification'; omit a section with nothing to say rather than leaving it empty. (c) Every paragraph and every list item on ONE line, however long - newlines only between paragraphs, between list items, and inside fenced code blocks; never hard-wrap prose. (d) The last block of the body is the literal line 'Closes', then the bare issue reference on the next line alone, with nothing after it on that line and no line after it; a 'Co-Authored-By:' trailer goes above the 'Closes' line, never after the reference. If the commit owns more than one issue, put each '#<KEY>-N' on its own line below the single 'Closes' line. NO 'State ...' command after the reference. Example of the tail:",
        "     ## Verification",
        "",
        "     `just check` passes.",
        "",
        "     Closes",
        "     #{{key}}",
        "   Write the message to a file OUTSIDE {{workdir}} and commit with 'git commit --cleanup=verbatim --file <message-file>', so the '##' headings and the '#{{key}}' line survive verbatim. This links the PR to the issue in YouTrack without transitioning it. Do NOT write 'State Done' - the issue must stay In Progress until a human merges the PR.",
        "8. Verify completion against the acceptance criteria before stopping. Re-read each criterion and state the concrete evidence that it is met: the file:line you changed, the command and its output, or the test that now passes. For any criterion that REMOVES or DELETES something, prove absence with a search (grep for the symbol or file) and confirm zero hits - never assert a removal you have not grep-verified - and add or extend a test or CI guard that fails if it reappears, so the removal is enforced mechanically. A green check suite is necessary but NOT sufficient: green checks do not prove the acceptance criteria are met. If you cannot produce objective evidence for EVERY criterion, you have NOT completed the issue - take the blocker path below instead of stopping as done.",
        "9. Stop. Do not push and do not open a PR; beyond the YouTrack writes permitted above, do not touch the tracker. The runner opens the PR from your commits and transitions the issue.",
        "",
        "If the issue asks for tracker output rather than code - a survey across repos, a report, issues to file - and you therefore finish DELIBERATELY with no commits:",
        "  a. Do the permitted YouTrack writes above: create the issue(s), and link each one as a subtask of {{key}}.",
        "  b. Write the human-facing summary of what you produced, naming every issue you created, to this file (the file contents are posted verbatim as the issue comment by the runner):",
        "       {{workdir}}/{{no_code_marker}}",
        "  c. Do NOT write the blocker markers below; this is a successful outcome, not a parked one. The runner skips the PR entirely, posts your summary, and sets the AI Agent field to its no-code label.",
        "  This path is ONLY for a run whose deliverable is tracker output rather than code. It is NEVER an escape hatch for a code task you could not finish - that is the blocker path below. A run that was supposed to change code and leaves no commits without this marker is failed by the runner, which is intended.",
        "",
        "If you CANNOT safely complete the issue - missing context, ambiguous acceptance criteria, out-of-scope work, dependency on another issue, or you cannot produce objective evidence that every acceptance criterion is met:",
        "  a. Do NOT change any code.",
        "  b. Write the human-facing explanation of exactly what information or decision you need to proceed to this file (the file contents are the reason, posted verbatim as the issue comment by the runner); end it with exactly one explicit question a human can answer directly:",
        "       {{workdir}}/{{needs_review_marker}}",
        "  c. Touch this marker file so the runner does NOT count this attempt toward the per-key cap (the job is parked pending human input, not failed):",
        "       {{workdir}}/{{skip_attempt_marker}}",
        "  d. Do NOT set any YouTrack field yourself; the runner reads the marker and sets AI Agent to 'Needs Review'.",
        "",
        "When you finish, summarize what you did as an acceptance-criteria checklist: one line per criterion with the concrete evidence that proves it is met. Include one more line: 'Re-read {{key}} before the final commit: unchanged', or 'Re-read {{key}} before the final commit: changed (<what changed>, <how it was handled>)'. Do not include a PR URL - the runner creates the PR.",
    ]
    .join("\n")
}

/// The built-in prompt template a preset names.
pub(crate) fn preset_prompt(p: PromptPreset) -> String {
    match p {
        PromptPreset::Default => default_youtrack_prompt(),
        PromptPreset::Audit => super::audit_prompt::default_audit_prompt(),
    }
}

/// The unrendered prompt template for an action: the `native.youtrack.prompt`
/// override when set, otherwise the `prompt_preset` built-in.
pub(crate) fn prompt_template(src: &YoutrackSource) -> String {
    match &src.prompt {
        Some(p) => p.clone(),
        None => preset_prompt(src.prompt_preset),
    }
}

/// Build the agent's instruction text for `job`: [`prompt_template`] (override, else
/// preset) with the `{{...}}` placeholders substituted.
fn build_prompt(
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    timeout_minutes: u64,
    resume: Option<&PrResume>,
) -> String {
    let template = prompt_template(src);
    render_prompt_template(
        &template,
        job,
        workdir,
        timeout_minutes,
        &write_projects(src, job),
        resume,
    )
}

/// The project list `{{write_projects}}` renders to: the configured
/// `native.youtrack.write_projects`, or the triggering issue's own project when
/// that list is empty, so filing a deferred item works with no site-specific
/// configuration and none is baked into the binary.
fn write_projects(src: &YoutrackSource, job: &YoutrackJob) -> String {
    if src.write_projects.is_empty() {
        project_key(&job.key)
    } else {
        src.write_projects.join(", ")
    }
}

/// Substitute the documented `{{...}}` placeholders in `template` with this
/// job's values. An unknown `{{name}}` is left literal and logged at warn
/// (forgiving by design), as is an unterminated `{{`.
///
/// `existing_pr_url` / `existing_branch` render empty when `resume`
/// is `None` (a fresh pickup); when it is `Some`, they carry the recorded PR's
/// URL and head branch, which is already checked out in `workdir`.
fn render_prompt_template(
    template: &str,
    job: &YoutrackJob,
    workdir: &Path,
    timeout_minutes: u64,
    write_projects: &str,
    resume: Option<&PrResume>,
) -> String {
    let workdir = workdir.display().to_string();
    let timeout_minutes = timeout_minutes.to_string();
    let existing_pr_url = resume.map(|r| r.pr_url.as_str()).unwrap_or("");
    let existing_branch = resume.map(|r| r.branch.as_str()).unwrap_or("");
    let vars: [(&str, &str); 11] = [
        ("key", job.key.as_str()),
        ("summary", job.summary.as_str()),
        ("clone_url", job.clone_url.as_str()),
        ("workdir", workdir.as_str()),
        ("timeout_minutes", timeout_minutes.as_str()),
        ("needs_review_marker", NEEDS_REVIEW_MARKER),
        ("skip_attempt_marker", SKIP_ATTEMPT_MARKER),
        ("no_code_marker", NO_CODE_MARKER),
        ("write_projects", write_projects),
        ("existing_pr_url", existing_pr_url),
        ("existing_branch", existing_branch),
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
    /// A successful run that deliberately produced tracker output
    /// rather than code and left `.claude-run-no-code-change`. No PR, the marker
    /// text is commented, AI Agent=`states.no_code` (default the `done` label),
    /// State untouched.
    NoCodeChange,
    /// Success: PR opened; AI Agent=Code Review.
    CodeReview {
        pr_url: String,
        /// The PR's identity, so the caller (`process_job`, which
        /// alone has the pool and can await) records it in the
        /// `agent_pull_requests` store once this synchronous state machine
        /// returns; `finalize`'s own unit tests run with no tokio runtime, so
        /// the store write cannot happen in here. `None` when the strategy or
        /// backend did not report it (e.g. AGit has no server branch).
        pr_number: Option<u64>,
        head_branch: Option<String>,
    },
    /// Hard failure (claude error, pre failure, or PR-create failure);
    /// AI Agent=Failed, no PR (or a failed PR).
    Failed,
    /// Claude hit the wall-clock limit (exit 124). The work may be
    /// complete, partial, or absent, so the issue is NOT marked Failed: a
    /// timeout-specific comment is posted and the AI Agent field is advanced to
    /// Needs Review only when still Working (a human/agent-advanced state is left
    /// untouched). Any local work (uncommitted changes and/or local commits) is
    /// salvaged as a draft PR (CLAUDE-379) before the comment is composed, so
    /// `pr_number`/`head_branch` carry the salvaged PR's identity exactly like
    /// [`Finalized::CodeReview`] does (`None` when there was nothing to salvage
    /// or salvaging failed).
    TimedOut {
        pr_number: Option<u64>,
        head_branch: Option<String>,
    },
}

impl Finalized {
    fn as_str(&self) -> &'static str {
        match self {
            Finalized::NeedsReview => "Needs Review",
            Finalized::NoCodeChange => "No Code Change",
            Finalized::CodeReview { .. } => "Code Review",
            Finalized::Failed => "Failed",
            Finalized::TimedOut { .. } => "Timed Out",
        }
    }
}

/// The finalize state machine (the native "post"). Runs on every path. Mirrors
/// the single finalize `post` step in `actions/youtrack.yml`: the comment is
/// best-effort (a comment failure cannot strand the AI Agent field on Working),
/// and the AI Agent write is the authoritative, last operation. State stays In
/// Progress until a human merges the PR. Finalize itself never
/// merges or waits for CI any more, even with automerge enabled; that happens
/// later, driven by a `success`/`recover` Action-run webhook event calling
/// [`complete_automerge`] on this PR.
#[allow(clippy::too_many_arguments)]
fn finalize(
    backend: &dyn IssueTracker,
    forge: &dyn Forge,
    src: &YoutrackSource,
    job: &YoutrackJob,
    workdir: &Path,
    claude_result: &ClaudeResult,
    pr_strategy: crate::action::PrStrategy,
    stream_log: Option<&Path>,
    claude_timeout: Duration,
    resumed: bool,
) -> Result<Finalized> {
    let needs_review = workdir.join(NEEDS_REVIEW_MARKER);
    let skip_marker = workdir.join(SKIP_ATTEMPT_MARKER);
    let no_code = workdir.join(NO_CODE_MARKER);

    if needs_review.exists() {
        // Blocker: ensure the skip marker is present so this attempt is not
        // counted, surface the reason verbatim, AI Agent=Needs Review, no PR.
        if !skip_marker.exists() {
            std::fs::write(&skip_marker, b"")?;
        }
        let reason = std::fs::read_to_string(&needs_review)?.trim().to_string();
        let reason = append_compaction_note(&reason, stream_log, &job.key);
        if let Err(e) = backend.comment(&job.key, &reason) {
            warn!(key = %job.key, error = %e, "issue comment failed (continuing to tag / AI Agent write)");
        }
        let needs_info_tag = src.tags.needs_info.trim();
        if !needs_info_tag.is_empty() {
            if let Err(e) = backend.add_tag(&job.key, needs_info_tag) {
                warn!(key = %job.key, tag = %needs_info_tag, error = %e, "Needs-Info tag write failed (continuing to AI Agent write)");
            }
        }
        backend.set_field(&job.key, &src.custom_field, &src.states.needs_review)?;
        Ok(Finalized::NeedsReview)
    } else if claude_result.ok && no_code.exists() {
        // The run deliberately produced tracker output rather than
        // code, so there are no commits and nothing to open a PR from. Skip the
        // forge entirely, post the marker text verbatim, and finish on the
        // no-code label. State is left alone, so the issue stays In Progress
        // until a human closes it. Without the marker a zero-commit run keeps
        // the behavior below: the ahead check fails and AI Agent goes
        // to Failed.
        let summary = std::fs::read_to_string(&no_code)?.trim().to_string();
        let label = src.states.no_code_label().to_string();
        comment_then_field(backend, src, &job.key, &summary, &label)?;
        info!(key = %job.key, %label, "no-code-change marker present; skipping PR");
        Ok(Finalized::NoCodeChange)
    } else if claude_result.ok {
        // Success: open the PR from the agent's local commits via the resolved
        // Forge (`fj` agit+autofill for Forgejo hosts, the GitHub
        // backend for GitHub hosts), then report it. Both derive the title/body
        // from the commit messages, which already carry the bare #<KEY>-N
        // reference.
        let host = host_only(&job.repo_host);
        match forge.create_pr(workdir, &host, pr_strategy) {
            Ok(pr) => {
                let url = pr_url(job, &pr);
                // A resumed run (CLAUDE-275) may be adopting a draft PR a
                // prior timed-out run salvaged (CLAUDE-379); now that the run
                // succeeded, replace the salvage's placeholder title/body
                // (CLAUDE-398) with the finished tip's own text, then strip
                // the draft/`WIP: ` status. Both are best-effort: a failure
                // here does not block the success outcome, it just leaves
                // the PR looking like (or titled like) a draft until fixed
                // by hand, and the cause is named in both the error log and
                // the success comment below so it is never silent.
                let mut text_refresh_error = None;
                if resumed {
                    if let Some(pr_number) = pr.number {
                        if let Err(e) =
                            forge.refresh_pr_text(workdir, &host, &job.repo_path, pr_number)
                        {
                            error!(key = %job.key, pr_number, error = %e, "refreshing the resumed PR's title/body failed (continuing)");
                            text_refresh_error = Some(e.to_string());
                        }
                        if let Err(e) = forge.mark_pr_ready(&host, &job.repo_path, pr_number) {
                            error!(key = %job.key, pr_number, error = %e, "clearing draft status on the resumed PR failed (continuing)");
                        }
                    }
                }
                let body = success_comment(claude_result, &url);
                let body = match text_refresh_error {
                    Some(e) => format!("{body}\n\nPR text could not be updated: {e}"),
                    None => body,
                };
                let body = append_compaction_note(&body, stream_log, &job.key);
                comment_then_field(backend, src, &job.key, &body, &src.states.code_review)?;
                // Finalize no longer waits for CI itself. When
                // `automerge.enabled`, the merge happens later, driven by a
                // `success`/`recover` Action-run webhook event on this PR (see
                // `crate::web::webhooks`), not by a poll here. The issue simply
                // stays at Code Review until that event (or a human) advances it.
                Ok(Finalized::CodeReview {
                    pr_url: url,
                    pr_number: pr.number,
                    head_branch: pr.head_branch,
                })
            }
            Err(e) => {
                // The error reaches the journal here (the run loop only records
                // `outcome: Failed`). The comment names where the work actually
                // is (preserved workdir, or the remote branch the push already
                // landed), never "nothing to preserve".
                warn!(key = %job.key, error = %e, "PR creation failed; finalizing as Failed");
                let token_url = clone_url_for(forge, job);
                let body = pr_failure_comment(
                    claude_result,
                    &e,
                    &work_location(workdir, Some(&token_url)),
                );
                comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
                Ok(Finalized::Failed)
            }
        }
    } else if claude_result.timed_out {
        // Timeout (exit 124). Ambiguous outcome: the agent may have
        // finished the work yet a lingering background task held the process
        // open past the limit. Do NOT mark Failed. Salvage any local work
        // (CLAUDE-379) as a draft PR before composing the comment, so a human
        // or a re-queue has something to pick back up from rather than
        // losing it, then advance AI Agent to Needs Review only when it is
        // still Working; if a human/agent already moved it on (Done, Code
        // Review, ...), leave the field untouched so we never stomp an
        // advanced state.
        let salvage = salvage_timeout_work(
            forge,
            job,
            workdir,
            pr_strategy,
            claude_result,
            claude_timeout,
        );
        let body = timeout_comment(claude_result, claude_timeout, &salvage);
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
        Ok(Finalized::TimedOut {
            pr_number: salvage.pr.as_ref().and_then(|p| p.number),
            head_branch: salvage.pr.as_ref().and_then(|p| p.head_branch.clone()),
        })
    } else {
        // Failure: crash, empty prompt, or pre failure.
        let body = failure_comment(claude_result);
        comment_then_field(backend, src, &job.key, &body, &src.states.failed)?;
        Ok(Finalized::Failed)
    }
}

/// A run whose context was compacted mid-run may have rebuilt
/// finding write-ups (or other filed content) from the compacted summary
/// instead of re-reading it, which is what let an earlier audit run file nine issues
/// with citations that did not exist. Appended only to the success and
/// needs-review comments, since those are the two outcomes that can carry a
/// tracker write worth double-checking. A stream-log read failure logs at
/// `error` and leaves `body` unchanged, since the outcome comment must still
/// post; `stream_log` is `None` whenever claude never ran, which also leaves
/// `body` unchanged.
fn append_compaction_note(body: &str, stream_log: Option<&Path>, key: &str) -> String {
    let Some(path) = stream_log else {
        return body.to_string();
    };
    match crate::claude::count_compact_boundaries(path) {
        Ok(0) => body.to_string(),
        Ok(n) => {
            warn!(key = %key, count = n, "context was compacted during this run");
            format!(
                "{body}\n\nContext was compacted {n} time(s) during this run; check the issues it created."
            )
        }
        Err(e) => {
            error!(key = %key, error = %e, "stream log read failed while checking for compaction");
            body.to_string()
        }
    }
}

/// Complete automerge on a PR whose checks a `success`/`recover` Action-run
/// webhook event has already confirmed are green and mergeable. Called from
/// `crate::web::webhooks`'s success-event handler, not from `finalize` (which
/// no longer waits for CI itself). Merges as a merge-commit, then posts a
/// best-effort comment: a comment failure is logged at `error` and never
/// fails the merge result, since the merge already succeeded on its own
/// terms. No `State` or `AI Agent` write here - both stay at Code Review
/// after a runner merge, and the YouTrack workflow
/// (`docs/yt-workflow-resolve-on-pr-merge.js`) resolves the issue once it sees
/// the merge.
pub(crate) fn complete_automerge(
    backend: &dyn IssueTracker,
    forge: &dyn Forge,
    issue_key: &str,
    host: &str,
    repo: &str,
    pr_number: u64,
    pr_url: &str,
) -> Result<()> {
    forge.merge_pr(host, repo, pr_number)?;
    let body = merged_comment(pr_url);
    if let Err(e) = backend.comment(issue_key, &body) {
        error!(key = %issue_key, error = %e, "post-automerge comment failed");
    }
    Ok(())
}

fn merged_comment(pr_url: &str) -> String {
    [
        "The runner merged this PR after its CI checks passed (automerge).".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
    ]
    .join("\n")
}

/// The PR's checks went green but Forgejo reports it as not
/// mergeable (a conflict, still being checked, or a WIP PR) - unrelated to
/// whether it is behind its base, which is decided separately by
/// `behind_base` before this comment is ever reached. Comments and leaves the
/// issue at Code Review for a human to resolve.
pub(crate) fn automerge_not_mergeable_comment(pr_url: &str) -> String {
    [
        "This PR's checks passed, but Forgejo reports it as not mergeable (a conflict, still \
         being checked, or a WIP PR)."
            .to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "A human needs to resolve this; AI Agent stays at Code Review.".to_string(),
    ]
    .join("\n")
}

/// The PR's checks went green and its branch is behind its base
/// (`behind_base: true`). The runner updated it through Forgejo's Update-branch
/// endpoint, which retriggers CI; a later `success` event re-evaluates and
/// merges once that run goes green.
pub(crate) fn automerge_branch_updated_comment(pr_url: &str) -> String {
    [
        "This PR's branch was behind its base; the runner updated it from the base branch."
            .to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "It will be automerged on its next green CI run.".to_string(),
    ]
    .join("\n")
}

/// The branch was behind its base, and updating it against the
/// base failed (for example a 409 conflict). Left at Code Review for a human
/// to resolve; `error` carries Forgejo's own reason when `fj` reports one.
pub(crate) fn automerge_update_failed_comment(pr_url: &str, error: &str) -> String {
    [
        "This PR's branch is behind its base, and the runner could not update it.".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        format!("Error: {error}"),
        String::new(),
        "A human needs to resolve this.".to_string(),
    ]
    .join("\n")
}

/// The branch is behind its base and updating it failed because of a merge
/// conflict; the runner dispatched a `/claude` conflict-resolution run
/// instead of leaving it to a human (CLAUDE-380). Posted in place of
/// [`automerge_update_failed_comment`] on the first attempt per PR head
/// commit; a second failure for the SAME head commit falls back to that
/// comment instead, since the auto-resolve attempt already ran and did not
/// resolve it.
pub(crate) fn automerge_conflict_resolving_comment(pr_url: &str) -> String {
    [
        "This PR's branch is behind its base, and updating it hit a merge conflict.".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "The runner is attempting to resolve the conflict automatically.".to_string(),
    ]
    .join("\n")
}

/// `fj pr status` did not report `behind_base` at all, meaning the
/// runtime's `fj` predates FJ-99. Automerge cannot tell whether the branch is
/// up to date, so it takes no merge action and leaves the issue at Code
/// Review for a human to check.
pub(crate) fn automerge_behind_base_missing_comment(pr_url: &str) -> String {
    [
        "This PR's checks passed, but the runner could not tell whether its branch is behind \
         its base."
            .to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        "`fj pr status` did not report `behind_base`; the runtime's fj must include FJ-99. A \
         human needs to check and merge this PR."
            .to_string(),
    ]
    .join("\n")
}

/// The merge itself was refused by Forgejo (for example a
/// protected base blocking an outdated branch with a 405). Left at Code
/// Review for a human to resolve; `error` carries Forgejo's own refusal
/// reason when `fj` reports one (FJ-99).
pub(crate) fn automerge_merge_failed_comment(pr_url: &str, error: &str) -> String {
    [
        "This PR's checks passed, but the runner could not merge it.".to_string(),
        String::new(),
        format!("PR: {pr_url}"),
        String::new(),
        format!("Error: {error}"),
        String::new(),
        "A human needs to resolve this.".to_string(),
    ]
    .join("\n")
}

/// An issue at the per-key attempt cap leaves the queue instead of
/// being skipped again on every poll. Comments the cap event (best-effort)
/// then sets AI Agent to Needs Review, the same label the needs-review marker
/// and the timeout path use for "parked pending a human". Unlike
/// [`comment_then_field`], a failed write here must not abort the caller (the
/// cap was still hit and `ProcessedJob::CapHit` must still be returned so the
/// stale-attempts banner keeps showing the key), so each write is attempted
/// independently and a failure of either logs at `error` naming the key and
/// the cause.
fn report_cap_reached(
    backend: &dyn IssueTracker,
    src: &YoutrackSource,
    action: &str,
    key: &str,
    attempts: u32,
    cap: u32,
) {
    let body = cap_reached_comment(attempts, cap);
    if let Err(e) = backend.comment(key, &body) {
        error!(action = %action, key = %key, error = %e, "attempt-cap comment failed");
    }
    if let Err(e) = backend.set_field(key, &src.custom_field, &src.states.needs_review) {
        error!(action = %action, key = %key, error = %e, "attempt-cap AI Agent write failed");
    }
}

fn cap_reached_comment(attempts: u32, cap: u32) -> String {
    [
        format!(
            "This issue has reached its attempt cap ({attempts} of {cap}) without finishing successfully."
        ),
        String::new(),
        "Re-queueing it alone (AI Agent = Queued) will not retry it: the attempt counter is not cleared by re-queueing or by a successful run.".to_string(),
        "To retry it, raise the action's `limits.max_attempts_per_key`, or reset this key's counter once CLAUDE-238 lands.".to_string(),
    ]
    .join("\n")
}

/// Comment first (best-effort), then set AI Agent last as the authoritative
/// write. A comment failure is logged and ignored so it cannot prevent the
/// field from advancing off Working; a set_field failure propagates and marks
/// the job's finalize as failed.
fn comment_then_field(
    backend: &dyn IssueTracker,
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
pub(crate) fn host_only(repo_host: &str) -> String {
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

fn pr_failure_comment(r: &ClaudeResult, err: &Error, work: &WorkLocation) -> String {
    // Name where the work actually is. Under the branch PR strategy the push
    // happens before the PR call, so a PR-only failure leaves the work on the
    // remote, not in a preserved workdir.
    let location_line = match work {
        WorkLocation::PreservedWorkdir(p) => format!(
            "The committed branch is PRESERVED on the daemon host at: `{}` (push it manually or re-run after the PR path is fixed).",
            p.display()
        ),
        WorkLocation::UncommittedChanges { workdir, paths } => format!(
            "The work was never committed and is PRESERVED on the daemon host at: `{}` ({} changed path(s), uncommitted). It is not on the remote under any branch; commit and push it manually, or re-run after the PR path is fixed.",
            workdir.display(),
            paths.len()
        ),
        WorkLocation::RemoteBranch { branch, sha } => format!(
            "The work is SAFE on the remote: branch `{branch}` at `{sha}` was pushed before the PR call failed, so nothing was lost. No PR was ever opened here, so nothing was recorded for this issue (CLAUDE-275): re-queuing it (set AI Agent = Queued) clones the default branch again and opens a fresh PR, it does NOT resume this branch by name. Push the same branch again from a fresh clone, or open the PR for it by hand, if this exact work should not be redone. The workdir on the daemon host is not kept, because the forge holds the same commits."
        ),
        WorkLocation::None => "No local commits were found to preserve.".to_string(),
    };
    let mut body = [
        "Claude AI agent run completed but PR creation FAILED:".to_string(),
        String::new(),
        err.to_string(),
        String::new(),
        location_line,
        String::new(),
        r.summary.clone(),
        String::new(),
        format!("Cost: ${} USD", r.cost),
    ]
    .join("\n");
    body.push_str(&raw_event_details(&r.raw));
    body
}

/// Outcome of attempting to salvage a timed-out run's local work as a draft PR
/// (CLAUDE-379), so a human or a re-queue has something to pick back up from
/// rather than losing work a timeout killed mid-flight. Every failure point
/// (reading git state, committing, opening the PR, commenting on it) is
/// recorded separately, never swallowed into one generic "salvage failed", so
/// the issue's timeout comment can name exactly which step a human needs to
/// redo by hand.
#[derive(Default)]
struct SalvageReport {
    /// Nothing local beyond the remote default branch and a clean working
    /// tree: no salvage was attempted.
    nothing_to_salvage: bool,
    /// The working-tree commit step failed (dirty tree present).
    commit_error: Option<String>,
    /// Pushing the branch and/or opening the draft PR failed.
    open_error: Option<String>,
    /// Posting the timeout-explanation comment on the opened PR failed.
    comment_error: Option<String>,
    /// The draft PR opened (or an already-open one adopted), if the open
    /// step succeeded.
    pr: Option<PullRequest>,
    /// `pr_url(job, &pr)` computed at salvage time, for the issue comment.
    pr_url: Option<String>,
}

/// Commit, push, and open a draft PR from a timed-out run's local work
/// (CLAUDE-379). Reuses [`workdir::local_only_work`], the exact query the
/// cleanup/preserve decision runs, so "is there anything to salvage" can
/// never disagree with "should the workdir be preserved". Every step that can
/// fail independently (reading git state, the WIP commit, the push/PR-create,
/// the PR comment) is logged at `error` here and recorded on the returned
/// report rather than swallowed.
fn salvage_timeout_work(
    forge: &dyn Forge,
    job: &YoutrackJob,
    workdir: &Path,
    pr_strategy: crate::action::PrStrategy,
    claude_result: &ClaudeResult,
    timeout: Duration,
) -> SalvageReport {
    let work = match crate::workdir::local_only_work(workdir) {
        Ok(w) => w,
        Err(e) => {
            error!(key = %job.key, error = %e, "timeout salvage: reading the workdir's git state failed; nothing salvaged");
            return SalvageReport {
                open_error: Some(format!("reading the workdir's git state failed: {e}")),
                ..Default::default()
            };
        }
    };

    if work.dirty.paths.is_empty() && work.unpushed_branches.is_empty() {
        return SalvageReport {
            nothing_to_salvage: true,
            ..Default::default()
        };
    }

    let mut report = SalvageReport::default();
    if !work.dirty.paths.is_empty() {
        if let Err(e) = commit_timeout_wip(workdir, &job.key) {
            error!(key = %job.key, error = %e, "timeout salvage: committing the uncommitted working tree failed");
            report.commit_error = Some(e);
        }
    }

    let host = host_only(&job.repo_host);
    let body = draft_pr_body(&job.key);
    match forge.create_pr_with_draft(workdir, &host, pr_strategy, true, Some(&body)) {
        Ok(pr) => {
            report.pr_url = Some(pr_url(job, &pr));
            if let Some(pr_number) = pr.number {
                let body = timeout_salvage_pr_comment(
                    claude_result,
                    timeout,
                    report.commit_error.as_deref(),
                );
                let scratch = std::env::temp_dir()
                    .join(format!("claude-run-timeout-pr-comment-{}.md", job.key));
                match std::fs::write(&scratch, &body) {
                    Ok(()) => {
                        if let Err(e) = forge.pr_comment(&host, &job.repo_path, pr_number, &scratch)
                        {
                            error!(key = %job.key, pr_number, error = %e, "timeout salvage: posting the PR comment failed");
                            report.comment_error = Some(e.to_string());
                        }
                        if let Err(e) = std::fs::remove_file(&scratch) {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                error!(path = %scratch.display(), error = %e, "timeout salvage: removing the PR comment scratch file failed");
                            }
                        }
                    }
                    Err(e) => {
                        error!(key = %job.key, pr_number, error = %e, "timeout salvage: writing the PR comment scratch file failed");
                        report.comment_error =
                            Some(format!("writing the comment body failed: {e}"));
                    }
                }
            } else {
                warn!(key = %job.key, "timeout salvage: opened PR reported no number; skipping the PR comment");
            }
            report.pr = Some(pr);
            report
        }
        Err(e) => {
            error!(key = %job.key, error = %e, "timeout salvage: opening the draft PR failed");
            report.open_error = Some(e.to_string());
            report
        }
    }
}

/// Commit the dirty working tree of a timed-out run as a single `WIP:
/// uncommitted changes at timeout` commit (CLAUDE-379), so it travels with
/// whatever branch a draft PR gets opened from rather than being lost once
/// the workdir is garbage-collected. `.claude-run-*` marker files are
/// excluded by pathspec (operator/runner state, never agent work); gitignored
/// files are never staged by a bare `git add -A` to begin with. The commit
/// body ends with the bare `#<KEY>` reference (CLAUDE-398), the requirement
/// CLAUDE-379 specified but the code never wrote.
fn commit_timeout_wip(workdir: &Path, key: &str) -> std::result::Result<(), String> {
    let add = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(["add", "-A", "--", ".", ":!.claude-run-*"])
        .output()
        .map_err(|e| format!("running `git add` failed: {e}"))?;
    if !add.status.success() {
        return Err(format!(
            "`git add` failed: {}",
            String::from_utf8_lossy(&add.stderr).trim()
        ));
    }
    let message = format!(
        "WIP: uncommitted changes at timeout\n\n\
         The runner committed the working tree a run of {key} left uncommitted when it hit its \
         time limit.\n\n\
         #{key}"
    );
    let commit = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(["commit", "-m", &message])
        .output()
        .map_err(|e| format!("running `git commit` failed: {e}"))?;
    if !commit.status.success() {
        return Err(format!(
            "`git commit` failed: {}",
            String::from_utf8_lossy(&commit.stderr).trim()
        ));
    }
    Ok(())
}

/// The cost line for a timeout comment. A killed run has no terminal
/// `result` event, so `r.cost` defaults to 0.0 whether or not any money was
/// actually spent; printing that default as `Cost: $0 USD` lies to a human
/// reading the comment about a run that may well have cost real money.
/// `r.raw` holding `total_cost_usd` is what distinguishes a real reported
/// cost (the killed-background-task path, which does capture a terminal
/// result before failing) from a run killed before any result landed.
fn timeout_cost_line(r: &ClaudeResult) -> String {
    match r.raw.get("total_cost_usd").and_then(|v| v.as_f64()) {
        Some(cost) => format!("Cost: ${cost} USD"),
        None => "Cost: unknown (the run was killed before it reported one)".to_string(),
    }
}

/// Runner-written PR body for a timeout-salvaged draft PR (CLAUDE-398): the
/// tip commit's own body is either an agent commit whose body is just the
/// bare issue reference or a runner WIP commit with no useful body, so the
/// draft PR body is written here instead of derived from `git log`. A
/// re-queued run that finishes replaces this description (see `finalize`'s
/// resumed-refresh path); until then this is what a human sees on the PR.
fn draft_pr_body(key: &str) -> String {
    [
        "## Summary".to_string(),
        String::new(),
        format!(
            "This draft PR was opened by the runner from a run of {key} that hit its time limit."
        ),
        "The branch holds the work committed so far; it may be incomplete.".to_string(),
        "The first comment on this PR has the timeout limit, the run's cost, and the agent's last summary at kill time.".to_string(),
        "A re-queued run that finishes replaces this description with the agent's own.".to_string(),
        String::new(),
        format!("#{key}"),
    ]
    .join("\n")
}

/// PR comment explaining why a timeout-salvaged draft PR (CLAUDE-379) exists:
/// the timeout limit, the agent's cost and summary at kill time, whether the
/// branch tip is the runner's own WIP commit or the agent's, and a note that
/// re-queuing resumes on this branch rather than opening a second PR.
fn timeout_salvage_pr_comment(
    r: &ClaudeResult,
    timeout: Duration,
    commit_error: Option<&str>,
) -> String {
    let last_commit_note = match commit_error {
        None => "The last commit on this branch is a `WIP: uncommitted changes at timeout` commit the runner made from the working tree at kill time, not the agent's own work.".to_string(),
        Some(e) => format!(
            "Committing the working tree's uncommitted changes failed ({e}); only the agent's own prior commits (if any) made it onto this branch."
        ),
    };
    [
        "Claude AI agent run TIMED OUT; this draft PR salvages the work in progress at kill time.".to_string(),
        String::new(),
        format!("Timeout limit: {}s", timeout.as_secs()),
        timeout_cost_line(r),
        String::new(),
        last_commit_note,
        String::new(),
        "Re-queuing this issue (set AI Agent = Queued) resumes on this branch rather than opening a second PR.".to_string(),
        String::new(),
        r.summary.clone(),
    ]
    .join("\n")
}

/// Timeout-specific issue comment. Surfaces the ambiguity (the work may be
/// complete despite the kill, e.g. a backgrounded command held the process open
/// past the limit), states the runner did NOT mark the issue Failed, names the
/// salvage outcome (CLAUDE-379), and points a human at the branch/PR and the
/// run log (`r.error` carries the log path).
fn timeout_comment(r: &ClaudeResult, timeout: Duration, salvage: &SalvageReport) -> String {
    let mut lines = vec![
        "Claude AI agent run TIMED OUT (the run hit its time limit and was killed):".to_string(),
        String::new(),
        format!("Details: {}", r.error),
        format!("Timeout limit: {}s", timeout.as_secs()),
        String::new(),
        "The agent may have completed all, some, or none of the work before being killed. A backgrounded command can hold the process open past the limit even after the functional work is done, so this is not necessarily a failure. The runner did NOT mark this issue Failed.".to_string(),
        String::new(),
    ];

    if salvage.nothing_to_salvage {
        lines.push("No local work was found to salvage (clean working tree, no commits ahead of the remote default branch).".to_string());
    } else {
        if let Some(e) = &salvage.commit_error {
            lines.push(format!(
                "Committing the uncommitted working tree failed: {e}"
            ));
        }
        match (&salvage.pr_url, &salvage.open_error) {
            (Some(url), _) => {
                lines.push(format!(
                    "Work in progress was salvaged as a draft PR: {url}"
                ));
                if let Some(e) = &salvage.comment_error {
                    lines.push(format!(
                        "Posting the explanation comment on that PR failed: {e}"
                    ));
                }
            }
            (None, Some(e)) => {
                lines.push(format!("Salvaging the work as a draft PR failed: {e}"));
            }
            (None, None) => {}
        }
    }

    lines.push(String::new());
    lines.push("A human should inspect the branch/PR and the run log above, then either resolve the issue manually or re-queue it (set AI Agent = Queued); a re-queue resumes on the salvaged branch rather than starting over.".to_string());
    lines.push(String::new());
    lines.push(r.summary.clone());
    lines.push(String::new());
    lines.push(timeout_cost_line(r));

    let mut body = lines.join("\n");
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
    pool: &DbPool,
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
    crate::workdir::force_remove(workdir);
}

/// True when [`crate::workdir::local_only_work`]
/// finds anything a delete would destroy: a local branch tip the remote does
/// not have under its own name, a dirty tracked/untracked tree, or a stash.
/// Any git or network failure errs toward `true`: preserving costs disk, a
/// wrong delete costs the run. Used by both `cleanup_or_preserve`'s per-job
/// callers and `crate::gc`'s age-based sweep, so the two decisions agree.
pub(crate) fn has_local_only_work(workdir: &Path) -> bool {
    has_local_only_work_with_token(workdir, None)
}

/// Same check as [`has_local_only_work`], authenticating the `ls-remote
/// origin` network call with `token_url` (CLAUDE-395) rather than relying on
/// a credential embedded in `origin`'s URL, which the post-clone scrub now
/// strips. Used by the per-job callers, which have a forge token in scope;
/// `crate::gc`'s context-free age-based sweep keeps calling
/// [`has_local_only_work`] unauthenticated.
pub(crate) fn has_local_only_work_with_token(workdir: &Path, token_url: Option<&str>) -> bool {
    match crate::workdir::local_only_work_with_token(workdir, token_url) {
        Ok(w) => !w.is_clean(),
        Err(_) => true,
    }
}

/// Where a failed-PR run's work actually is, for the
/// comment a human reads. Computed from the same
/// [`crate::workdir::local_only_work`] query that decides preserve-or-delete,
/// so the comment can never contradict [`cleanup_or_preserve`].
enum WorkLocation {
    /// Unpushed commits or a stash exist, so the workdir is kept on the
    /// daemon host.
    PreservedWorkdir(PathBuf),
    /// A dirty working tree (tracked or untracked changes) with no local
    /// commits ahead: still only on the daemon host, but never pushed as a
    /// branch.
    UncommittedChanges {
        workdir: PathBuf,
        paths: Vec<String>,
    },
    /// Every local tip is on the remote under its own name: the push landed
    /// and only the PR call failed, so the work is safe on the forge under
    /// this branch.
    RemoteBranch { branch: String, sha: String },
    /// Nothing local beyond the remote's default branch, clean working tree.
    None,
}

fn work_location(workdir: &Path, token_url: Option<&str>) -> WorkLocation {
    // A failed query errs toward preserve, matching `has_local_only_work`.
    let work = match crate::workdir::local_only_work_with_token(workdir, token_url) {
        Ok(w) => w,
        Err(_) => return WorkLocation::PreservedWorkdir(workdir.to_path_buf()),
    };
    if !work.unpushed_branches.is_empty() || !work.dirty.stashes.is_empty() {
        return WorkLocation::PreservedWorkdir(workdir.to_path_buf());
    }
    if !work.dirty.paths.is_empty() {
        return WorkLocation::UncommittedChanges {
            workdir: workdir.to_path_buf(),
            paths: work.dirty.paths,
        };
    }
    match work.remote_branch {
        Some(t) => WorkLocation::RemoteBranch {
            branch: t.branch,
            sha: t.sha,
        },
        None => WorkLocation::None,
    }
}

/// Delete the workdir unless [`crate::workdir::local_only_work`] finds
/// unpushed commits, a dirty tree, or a stash. The preserve branch is a
/// data-loss guard: an earlier incident had a failed `create_pr` plus an
/// unconditional cleanup destroy a run's only copy of the committed work, and
/// the guard was later extended past committed-but-unpushed branches to the
/// uncommitted and stashed work a branch-only check would miss.
fn cleanup_or_preserve(workdir: &Path, action: &str, key: &str, token_url: Option<&str>) {
    crate::workdir::cleanup_or_preserve_with_token(workdir, action, key, token_url);
}

/// Move a leftover (preserved) workdir out of the way of a fresh
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
    use crate::provider::{CliBackend, IssueLink, LinkDirection};
    use serde_json::Value;
    use std::sync::Arc;

    /// The pre step clones the URL the FORGE resolves from its key
    /// store, not the unauthenticated one the YouTrack VCS integration reports.
    /// A container has no git credential of its own, so the plain URL failed
    /// every job with `could not read Username`.
    #[test]
    fn the_clone_url_comes_from_the_forge_key_store() {
        let dir = tempfile::tempdir().unwrap();
        let keys = dir.path().join("keys.json");
        std::fs::write(
            &keys,
            r#"{"hosts":{"dev.a8n.run":{"name":"bot","token":"tok-123","type":"Application"}}}"#,
        )
        .unwrap();
        let backend = CliBackend::new(Arc::new(StubRunner::new())).with_keys_path(&keys);

        let url = clone_url_for(&backend, &job());
        assert_eq!(
            url, "https://oauth2:tok-123@dev.a8n.run/contoso/claude-run.git",
            "the scheme must be stripped from repo_host before the key lookup"
        );
    }

    /// A host the key store does not know falls back to the plain URL, so a host
    /// install authenticating through a credential helper keeps working.
    #[test]
    fn an_unknown_host_falls_back_to_the_unauthenticated_url() {
        let dir = tempfile::tempdir().unwrap();
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let backend = CliBackend::new(Arc::new(StubRunner::new())).with_keys_path(&keys);

        let j = job();
        assert_eq!(clone_url_for(&backend, &j), j.clone_url);
    }

    /// The token must never reach the agent: `{{clone_url}}` renders the job's
    /// own unauthenticated URL, which the authenticated one does not replace.
    #[test]
    fn the_prompt_never_renders_an_authenticated_url() {
        let j = job();
        let rendered =
            render_prompt_template("Repo: {{clone_url}}", &j, Path::new("/w"), 60, "", None);
        assert!(rendered.contains("https://dev.a8n.run/contoso/claude-run.git"));
        assert!(!rendered.contains("oauth2:"), "{rendered}");
    }

    /// A job whose invocation hits the
    /// authentication failure spends no attempt against
    /// `max_attempts_per_key` and runs no finalize, through the native
    /// YouTrack `process_job` pipeline (not merely the generic scripted one in
    /// `job.rs`). The pre steps (field/state writes, clone) run for real
    /// against a stub `yt` and a local git repo, so the transient outcome is
    /// reached exactly as production does, then claude's stub reports the
    /// structured auth failure.
    #[tokio::test]
    async fn process_job_hitting_auth_failure_is_transient_and_skips_finalize() {
        let tmp = tempfile::tempdir().unwrap();

        // A real local repo to clone, so `run_pre`'s git clone succeeds with no
        // network. `clone_url_for` falls back to the job's own (unauthenticated)
        // URL because `CliBackend::new` reads no key store under test.
        let origin = tmp.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "t@t.example"],
            vec!["config", "user.name", "t"],
        ] {
            assert!(Command::new("git")
                .args(&args)
                .current_dir(&origin)
                .status()
                .unwrap()
                .success());
        }
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        assert!(Command::new("git")
            .args(["add", "."])
            .current_dir(&origin)
            .status()
            .unwrap()
            .success());
        assert!(Command::new("git")
            .args(["commit", "-q", "-m", "init"])
            .current_dir(&origin)
            .status()
            .unwrap()
            .success());

        let stub = tmp.path().join("claude-stub.sh");
        crate::testutil::write_stub(
            &stub,
            "#!/bin/sh\n\
             printf '%s\\n' '{\"type\":\"assistant\",\"error\":\"authentication_failed\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"OAuth session expired\"}]}}'\n\
             printf '%s\\n' '{\"type\":\"result\",\"is_error\":true,\"error\":\"authentication_failed\"}'\n\
             exit 1\n",
        );
        let claude = ClaudeInvoker::new(Some(stub.to_str().unwrap()));

        let runner = Arc::new(
            StubRunner::new()
                .ok("yt", &["issue", "get-field", "MK-16", "AI Agent"], "Queued")
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Working"],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "In Progress"], ""),
        );
        let backend = CliBackend::new(runner.clone());

        let mut j = job();
        j.clone_url = format!("file://{}", origin.display());
        let mut s = src();
        s.dependencies.enabled = false;

        let pool = mem_pool().await;
        let paths = Paths {
            config_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().to_path_buf(),
            state_dir: tmp.path().to_path_buf(),
            runtime_dir: tmp.path().to_path_buf(),
        };
        let loaded =
            crate::action::validate_str("name: yt\nkind: youtrack\nnative: {}\n", "<test>")
                .unwrap();
        let limits = EffectiveLimits {
            max_attempts_per_key: 3,
            max_runs_per_day: 20,
            claude_timeout: Duration::from_secs(30),
            daily_budget_usd: 5.0,
            run_budget_usd: 5.0,
            budget_warn_fraction: 0.8,
            max_issues_per_run: 25,
        };
        let cancel = CancellationToken::new();
        let spend = crate::usage::RunSpend::new("t", limits.run_budget(), None);

        let outcome = process_job(
            &loaded,
            &paths,
            &pool,
            &claude,
            &backend,
            &backend,
            &s,
            &limits,
            &cancel,
            &crate::pause::RunPause::never(),
            &spend,
            &j,
            PrStrategy::Branch,
            false,
            1,
        )
        .await
        .unwrap();

        assert!(
            matches!(outcome, ProcessedJob::Transient { .. }),
            "an authentication failure must finalize as transient, got {outcome:?}"
        );
        assert_eq!(
            state::attempts_get(&pool, "yt", "MK-16").await.unwrap(),
            0,
            "no attempt is spent on a transient auth failure"
        );
        // No `set-field` past `Working` and no comment were registered on the
        // stub, so a call to either would have failed with "no canned
        // response"; reaching this point proves finalize never ran.
    }

    /// A queued issue whose key is at the
    /// cap gets exactly one comment naming the attempt count and the cap, and
    /// its AI Agent field moves to the configured Needs Review label, through
    /// the full `process_job` pipeline (the cap check runs before any other
    /// step, so no workdir or clone is needed).
    #[tokio::test]
    async fn process_job_at_cap_comments_and_sets_needs_review() {
        let tmp = tempfile::tempdir().unwrap();
        let body = cap_reached_comment(3, 3);
        let runner = Arc::new(
            StubRunner::new()
                .ok("yt", &["issue", "comment", "MK-16", &body], "")
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Needs Review"],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());

        let j = job();
        let s = src();
        let pool = mem_pool().await;
        for _ in 0..3 {
            state::attempts_increment(&pool, "yt", &j.key)
                .await
                .unwrap();
        }
        let paths = Paths {
            config_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().to_path_buf(),
            state_dir: tmp.path().to_path_buf(),
            runtime_dir: tmp.path().to_path_buf(),
        };
        let loaded =
            crate::action::validate_str("name: yt\nkind: youtrack\nnative: {}\n", "<test>")
                .unwrap();
        let limits = EffectiveLimits {
            max_attempts_per_key: 3,
            max_runs_per_day: 20,
            claude_timeout: Duration::from_secs(30),
            daily_budget_usd: 5.0,
            run_budget_usd: 5.0,
            budget_warn_fraction: 0.8,
            max_issues_per_run: 25,
        };
        let claude = ClaudeInvoker::new(None);
        let cancel = CancellationToken::new();
        let spend = crate::usage::RunSpend::new("t", limits.run_budget(), None);

        let outcome = process_job(
            &loaded,
            &paths,
            &pool,
            &claude,
            &backend,
            &backend,
            &s,
            &limits,
            &cancel,
            &crate::pause::RunPause::never(),
            &spend,
            &j,
            PrStrategy::Branch,
            false,
            1,
        )
        .await
        .unwrap();

        assert!(
            matches!(&outcome, ProcessedJob::CapHit { key } if key == "MK-16"),
            "{outcome:?}"
        );
        let calls = runner.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("yt issue comment"))
                .count(),
            1,
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
        // The attempt-cap park also lands on Needs Review, but it is a
        // different code path from the agent-signalled blocker and never
        // writes the Needs-Info tag (CLAUDE-373). No `tag add` response is
        // stubbed above, so a call to it would have failed `process_job`'s
        // `unwrap()` before reaching this assertion.
        assert!(!calls.iter().any(|c| c.contains("tag add")), "{calls:?}");
    }

    /// A failed comment or field write at
    /// the cap still returns `CapHit` (the banner must keep showing the key)
    /// rather than propagating the write error out of `process_job`.
    #[tokio::test]
    async fn process_job_at_cap_still_reports_cap_hit_when_writes_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let body = cap_reached_comment(3, 3);
        let runner = Arc::new(
            StubRunner::new()
                .fail("yt", &["issue", "comment", "MK-16", &body], 1, "boom")
                .fail(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Needs Review"],
                    1,
                    "boom",
                ),
        );
        let backend = CliBackend::new(runner.clone());

        let j = job();
        let s = src();
        let pool = mem_pool().await;
        for _ in 0..3 {
            state::attempts_increment(&pool, "yt", &j.key)
                .await
                .unwrap();
        }
        let paths = Paths {
            config_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().to_path_buf(),
            state_dir: tmp.path().to_path_buf(),
            runtime_dir: tmp.path().to_path_buf(),
        };
        let loaded =
            crate::action::validate_str("name: yt\nkind: youtrack\nnative: {}\n", "<test>")
                .unwrap();
        let limits = EffectiveLimits {
            max_attempts_per_key: 3,
            max_runs_per_day: 20,
            claude_timeout: Duration::from_secs(30),
            daily_budget_usd: 5.0,
            run_budget_usd: 5.0,
            budget_warn_fraction: 0.8,
            max_issues_per_run: 25,
        };
        let claude = ClaudeInvoker::new(None);
        let cancel = CancellationToken::new();
        let spend = crate::usage::RunSpend::new("t", limits.run_budget(), None);

        let outcome = process_job(
            &loaded,
            &paths,
            &pool,
            &claude,
            &backend,
            &backend,
            &s,
            &limits,
            &cancel,
            &crate::pause::RunPause::never(),
            &spend,
            &j,
            PrStrategy::Branch,
            false,
            1,
        )
        .await
        .unwrap();

        assert!(
            matches!(&outcome, ProcessedJob::CapHit { key } if key == "MK-16"),
            "a failed comment/field write must not turn the cap hit into an error, got {outcome:?}"
        );
    }

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
            usage: Default::default(),
            raw: Value::Null,
            error: String::new(),
            transient: false,
            timed_out: false,
            login_required: false,
            killed: false,
            exit_code: Some(0),
        }
    }

    fn timed_out_result() -> ClaudeResult {
        ClaudeResult {
            ok: false,
            summary: "partial work before kill".into(),
            cost: 0.3,
            usage: Default::default(),
            raw: Value::Null,
            error: "claude exited 124, see stream.jsonl".into(),
            transient: false,
            timed_out: true,
            login_required: false,
            killed: false,
            exit_code: Some(124),
        }
    }

    /// A failure with a populated `raw` event leads with the friendly
    /// summary, preserves the first-line error and Cost line, and appends the
    /// pretty-printed event in a collapsed `<details>` block.
    #[test]
    fn failure_comment_friendly_summary_and_collapsed_raw() {
        let r = ClaudeResult {
            ok: false,
            summary: "API Error: 529 Overloaded.".into(),
            cost: 0.01,
            usage: Default::default(),
            raw: serde_json::json!({"type":"result","is_error":true,"api_error_status":"529"}),
            error: "claude exited 1, see stream.jsonl".into(),
            transient: true,
            timed_out: false,
            login_required: false,
            killed: false,
            exit_code: Some(1),
        };
        let body = failure_comment(&r);
        assert!(body.starts_with("Claude AI agent run FAILED (claude exited 1, see stream.jsonl):"));
        assert!(body.contains("API Error: 529 Overloaded."));
        assert!(body.contains("Cost: $0.01 USD"));
        assert!(body.contains("<details>\n<summary>Raw result event</summary>"));
        assert!(body.contains("```json"));
        assert!(body.contains("\"api_error_status\": \"529\""));
    }

    /// A killed run has no terminal `result` event, so `r.cost` defaults to
    /// 0.0 whether or not the run actually cost money. The timeout comments
    /// must say "unknown", never claim `$0 USD`.
    #[test]
    fn timeout_comments_report_cost_unknown_when_no_result_event_landed() {
        let r = ClaudeResult::synthesized("claude exited 124, see stream.jsonl", "");
        let salvage = SalvageReport {
            nothing_to_salvage: true,
            ..Default::default()
        };
        let body = timeout_comment(&r, Duration::from_secs(60), &salvage);
        assert!(
            body.contains("Cost: unknown (the run was killed before it reported one)"),
            "{body}"
        );
        assert!(!body.contains("Cost: $0 USD"), "{body}");

        let pr_body = timeout_salvage_pr_comment(&r, Duration::from_secs(60), None);
        assert!(
            pr_body.contains("Cost: unknown (the run was killed before it reported one)"),
            "{pr_body}"
        );
        assert!(!pr_body.contains("Cost: $0 USD"), "{pr_body}");
    }

    /// When the stream log did capture a terminal result with a real cost
    /// (e.g. the killed-background-task path), the timeout comments still
    /// print the tracked dollar figure rather than "unknown".
    #[test]
    fn timeout_comments_report_the_tracked_cost_when_a_result_event_landed() {
        let mut r = ClaudeResult::synthesized("claude exited 124, see stream.jsonl", "");
        r.cost = 1.23;
        r.raw = serde_json::json!({"type": "result", "total_cost_usd": 1.23});
        let salvage = SalvageReport {
            nothing_to_salvage: true,
            ..Default::default()
        };
        let body = timeout_comment(&r, Duration::from_secs(60), &salvage);
        assert!(body.contains("Cost: $1.23 USD"), "{body}");
        assert!(!body.contains("unknown"), "{body}");
    }

    /// A synthesized failure (empty `raw`) omits the `<details>` block.
    #[test]
    fn failure_comment_omits_details_for_synthesized_raw() {
        let r = ClaudeResult::synthesized("claude exited 1, see stream.jsonl", "pre step failed");
        let body = failure_comment(&r);
        assert!(body.contains("pre step failed"));
        assert!(body.contains("Cost: $0 USD"));
        assert!(!body.contains("<details>"));
    }

    /// CLAUDE-372: a killed run's stream tail holds a `thinking` block with a
    /// `signature`. `failure_comment` and `timeout_comment` render whatever
    /// `friendly_failure_summary` produced, so neither body may carry it.
    #[test]
    fn failure_and_timeout_comments_never_carry_a_thinking_signature() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"thinking\",\"thinking\":\"secret reasoning\",\"signature\":\"SIGXYZ\"}]}}\n",
        )
        .unwrap();
        let summary = crate::claude::friendly_failure_summary(&serde_json::Value::Null, &log);
        let mut r = ClaudeResult::synthesized("claude exited 137, see stream.jsonl", "");
        r.summary = summary;

        let failure_body = failure_comment(&r);
        assert!(!failure_body.contains("signature"), "{failure_body}");
        assert!(!failure_body.contains("secret reasoning"), "{failure_body}");

        let salvage = SalvageReport {
            nothing_to_salvage: true,
            ..Default::default()
        };
        let timeout_body = timeout_comment(&r, Duration::from_secs(60), &salvage);
        assert!(!timeout_body.contains("signature"), "{timeout_body}");
        assert!(!timeout_body.contains("secret reasoning"), "{timeout_body}");
    }

    /// A run with no `compact_boundary` event in its stream log
    /// leaves the outcome comment untouched.
    #[test]
    fn append_compaction_note_leaves_body_unchanged_without_a_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(&log, "{\"type\":\"system\",\"subtype\":\"init\"}\n").unwrap();
        let body = append_compaction_note("The PR is ready.", Some(&log), "MK-16");
        assert_eq!(body, "The PR is ready.");
    }

    /// an earlier audit run's shape - a `compact_boundary` event in the run's
    /// stream log - appends the one-line warning to whatever outcome comment
    /// (success or needs-review) it is given.
    #[test]
    fn append_compaction_note_appends_the_line_when_a_boundary_is_present() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"system\",\"subtype\":\"compact_boundary\"}\n",
        )
        .unwrap();
        let body = append_compaction_note("The PR is ready.", Some(&log), "MK-16");
        assert_eq!(
            body,
            "The PR is ready.\n\nContext was compacted 1 time(s) during this run; check the issues it created."
        );
    }

    /// A `None` stream log (claude never ran) leaves the comment
    /// unchanged rather than erroring.
    #[test]
    fn append_compaction_note_leaves_body_unchanged_with_no_stream_log() {
        let body = append_compaction_note("Parked for review.", None, "MK-16");
        assert_eq!(body, "Parked for review.");
    }

    /// An unreadable stream log must not stop the outcome comment
    /// from posting; it just posts without the compaction line.
    #[test]
    fn append_compaction_note_leaves_body_unchanged_on_a_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.jsonl");
        let body = append_compaction_note("The PR is ready.", Some(&missing), "MK-16");
        assert_eq!(body, "The PR is ready.");
    }

    #[test]
    fn build_prompt_default_substitutes_job_fields() {
        // The built-in default renders every placeholder
        // and leaves no `{{...}}` behind, preserving the runner-creates-PR contract lines.
        let wd = std::path::Path::new("/tmp/wd");
        let p = build_prompt(&src(), &job(), wd, 60, None);
        assert!(p.starts_with(
            "You are working a YouTrack issue end-to-end. Follow the YouTrack and Git workflows from CLAUDE.md."
        ));
        assert!(p.contains("Issue: MK-16: Do the thing"));
        assert!(p.contains("Repo: https://dev.a8n.run/contoso/claude-run.git"));
        assert!(p.contains("Working directory: /tmp/wd - the repo is already cloned here"));
        assert!(p.contains("You have 60 minutes of wall-clock time"));
        assert!(p.contains("yt issue inspect MK-16"));
        assert!(p.contains("Closes"));
        assert!(p.contains("#MK-16"));
        assert!(p.contains("/tmp/wd/.claude-run-needs-review"));
        assert!(p.contains("/tmp/wd/.claude-run-skip-attempt"));
        assert!(!p.contains("{{"), "unresolved placeholder:\n{p}");
        // Step 3 is self-contained and no longer delegates the git
        // steps to CLAUDE.md; it instructs staying on the feature branch and
        // never switching to `main` (an earlier leak stranded work that way).
        assert!(
            !p.contains("per the Git workflow in CLAUDE.md"),
            "step 3 must not delegate to the Git workflow in CLAUDE.md:\n{p}"
        );
        assert!(p.contains("leave it checked out as HEAD when you stop"));
        assert!(p.contains("Never run 'git checkout main'"));
    }

    /// CLAUDE-377: the agent must know its wall-clock budget, checkpoint its
    /// work with per-layer commits carrying the bare issue reference, and
    /// follow the four test-discipline rules rather than re-running the full
    /// suite after every change.
    #[test]
    fn build_prompt_default_states_time_budget_checkpoints_and_test_discipline() {
        let p = default_youtrack_prompt();
        assert!(
            p.contains("{{timeout_minutes}} minutes of wall-clock time"),
            "time-budget statement missing: {p}"
        );
        assert!(
            p.contains("After each layer")
                && p.contains("make a local commit whose body ends with the bare '#{{key}}'"),
            "per-layer checkpoint-commit instruction missing: {p}"
        );
        // The four test-discipline rules.
        assert!(
            p.contains("run only the targeted tests for the code you just changed"),
            "targeted-tests rule missing: {p}"
        );
        assert!(
            p.contains("Run the repo's full check suite")
                && p.contains(
                    "exactly once, after formatting and lint are clean and the work is complete"
                ),
            "single full-gate-run rule missing: {p}"
        );
        assert!(
            p.contains("Never re-run a long command only to read its output again")
                && p.contains("capture it to a file outside {{workdir}} the first time"),
            "no-rerun-to-read rule missing: {p}"
        );
        assert!(
            p.contains("re-run only what failed until it is green, then run the full check suite once more"),
            "re-run-only-what-failed rule missing: {p}"
        );
    }

    /// CLAUDE-367: an issue can be edited after pickup, so the default prompt
    /// must re-read it after the check suite goes green and before the final
    /// commit, and the closing summary must report whether anything changed.
    #[test]
    fn default_prompt_re_reads_the_issue_before_the_final_commit() {
        let p = default_youtrack_prompt();
        let check_suite_step = p
            .find("Run the repo's full check suite")
            .expect("check suite step present");
        let reread_step = p
            .find("Immediately before your final commit, re-run 'yt issue inspect {{key}}'")
            .expect("re-read step present");
        let commit_step = p
            .find("The runner uses this commit's body as the PR description")
            .expect("commit step present");
        assert!(
            check_suite_step < reread_step,
            "the re-read must come after the check suite goes green"
        );
        assert!(
            reread_step < commit_step,
            "the re-read must come before the final commit"
        );
        assert!(
            p.contains("Re-read {{key}} before the final commit: unchanged"),
            "the closing summary must require the unchanged report line"
        );
        assert!(
            p.contains("Re-read {{key}} before the final commit: changed (<what changed>, <how it was handled>)"),
            "the closing summary must require the changed report line"
        );
    }

    #[test]
    fn build_prompt_uses_override_when_set() {
        // A set prompt fully replaces the default and still gets its
        // placeholders substituted.
        let mut s = src();
        s.prompt = Some("Do {{key}}: {{summary}} in {{workdir}}".to_string());
        let p = build_prompt(&s, &job(), std::path::Path::new("/tmp/wd"), 60, None);
        assert_eq!(p, "Do MK-16: Do the thing in /tmp/wd");
        assert!(!p.contains("You are working a YouTrack issue"));
    }

    /// Resolution order: a YAML override beats the preset; without one the
    /// preset's built-in text is the template.
    #[test]
    fn build_prompt_override_beats_preset() {
        let mut s = src();
        assert_eq!(prompt_template(&s), default_youtrack_prompt());

        s.prompt_preset = PromptPreset::Audit;
        assert_eq!(
            prompt_template(&s),
            super::super::audit_prompt::default_audit_prompt()
        );
        assert!(prompt_template(&s).starts_with("You are executing a scheduled AUDIT run"));

        s.prompt = Some("Do {{key}}".to_string());
        assert_eq!(prompt_template(&s), "Do {{key}}");
        assert_eq!(
            build_prompt(&s, &job(), std::path::Path::new("/tmp/wd"), 60, None),
            "Do MK-16"
        );
    }

    #[test]
    fn preset_prompt_maps_each_preset() {
        assert_eq!(
            preset_prompt(PromptPreset::Default),
            default_youtrack_prompt()
        );
        assert!(preset_prompt(PromptPreset::Audit)
            .starts_with("You are executing a scheduled AUDIT run"));
    }

    fn audit_src() -> YoutrackSource {
        YoutrackSource {
            prompt_preset: PromptPreset::Audit,
            write_projects: vec!["ALPHA".into(), "GAMMA".into()],
            ..YoutrackSource::default()
        }
    }

    #[test]
    fn audit_preset_template_carries_the_runner_placeholders() {
        let t = preset_prompt(PromptPreset::Audit);
        for placeholder in [
            "{{key}}",
            "{{summary}}",
            "{{clone_url}}",
            "{{workdir}}",
            "{{needs_review_marker}}",
            "{{skip_attempt_marker}}",
            "{{write_projects}}",
            "{{existing_branch}}",
        ] {
            assert!(t.contains(placeholder), "audit preset lacks {placeholder}");
        }
    }

    #[test]
    fn audit_preset_renders_every_placeholder() {
        let resume = PrResume {
            branch: "audit/pms-ui-2026-09-14".into(),
            pr_url: "https://dev.a8n.run/contoso/audits/pulls/9".into(),
        };
        let wd = std::path::Path::new("/tmp/wd");
        for r in [None, Some(&resume)] {
            let p = build_prompt(&audit_src(), &job(), wd, 60, r);
            assert!(!p.contains("{{"), "unresolved placeholder:\n{p}");
            assert!(p.contains("Issue: MK-16: Do the thing"), "{p}");
            assert!(
                p.contains("Repo: https://dev.a8n.run/contoso/claude-run.git"),
                "{p}"
            );
            assert!(p.contains("ONLY in these projects: ALPHA, GAMMA"), "{p}");
            assert!(p.contains("/tmp/wd/.claude-run-needs-review"), "{p}");
            assert!(p.contains("/tmp/wd/.claude-run-skip-attempt"), "{p}");
            assert!(p.contains("#MK-16"), "{p}");
        }
        let resumed = build_prompt(&audit_src(), &job(), wd, 60, Some(&resume));
        assert!(resumed.contains("audit/pms-ui-2026-09-14"), "{resumed}");
        assert!(
            resumed.contains("https://dev.a8n.run/contoso/audits/pulls/9"),
            "{resumed}"
        );
    }

    #[test]
    fn audit_preset_states_its_permits_and_bans() {
        let p = preset_prompt(PromptPreset::Audit);
        assert!(p.is_ascii(), "the audit preset must be ASCII only");
        for needle in [
            "Tracker writes. You may perform EXACTLY these, and nothing else:",
            "`Queued` and `Needs Review` are the only two `AI Agent` values you may set",
            "Every other tracker write is forbidden, in particular:",
            "No `AI Agent` or `State` change on any issue you did not create during this run",
            "No `yt issue set-state` anywhere",
            "No summary edit on any issue.",
            "No assignee change on any issue.",
            "No `yt issue delete`, anywhere.",
            "No pull request URL in any comment, description, or issue body.",
            "No write to the anchor issue from a per-target run.",
            "Never run `git checkout main` or `git switch main`",
            "HARD RULE on long commands: run everything in the FOREGROUND",
        ] {
            assert!(p.contains(needle), "audit preset lacks: {needle}");
        }
    }

    #[test]
    fn render_substitutes_all_documented_placeholders() {
        let t = "{{key}}|{{summary}}|{{clone_url}}|{{workdir}}|{{timeout_minutes}}|{{needs_review_marker}}|{{skip_attempt_marker}}|{{no_code_marker}}|{{write_projects}}|{{existing_pr_url}}|{{existing_branch}}";
        let p = render_prompt_template(t, &job(), std::path::Path::new("/wd"), 60, "MK", None);
        assert_eq!(
            p,
            "MK-16|Do the thing|https://dev.a8n.run/contoso/claude-run.git|/wd|60|.claude-run-needs-review|.claude-run-skip-attempt|.claude-run-no-code-change|MK||"
        );
    }

    #[test]
    fn render_substitutes_resume_placeholders_when_set() {
        // A resumed job's placeholders carry the recorded PR's URL
        // and branch instead of rendering empty.
        let t = "{{existing_pr_url}}|{{existing_branch}}";
        let resume = PrResume {
            branch: "fix/CLAUDE-275".into(),
            pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
        };
        let p = render_prompt_template(
            t,
            &job(),
            std::path::Path::new("/wd"),
            60,
            "MK",
            Some(&resume),
        );
        assert_eq!(
            p,
            "https://dev.a8n.run/contoso/claude-run/pulls/7|fix/CLAUDE-275"
        );
    }

    #[test]
    fn render_leaves_unknown_and_unterminated_literal() {
        // An unknown placeholder and an unterminated `{{` survive
        // verbatim; known ones still substitute.
        let p = render_prompt_template(
            "{{key}} {{bogus}} {{oops",
            &job(),
            std::path::Path::new("/wd"),
            60,
            "MK",
            None,
        );
        assert_eq!(p, "MK-16 {{bogus}} {{oops");
    }

    /// An empty `write_projects` resolves to the triggering issue's
    /// own project, so filing a deferred item works with no site-specific
    /// configuration; a configured list replaces it verbatim.
    #[test]
    fn write_projects_defaults_to_the_issue_project() {
        assert!(src().write_projects.is_empty(), "the default is empty");
        assert_eq!(write_projects(&src(), &job()), "MK");

        let mut s = src();
        s.write_projects = vec!["AUD".into(), "DEV".into()];
        assert_eq!(write_projects(&s, &job()), "AUD, DEV");
    }

    /// The built-in prompt permits the bounded tracker writes the
    /// `No orphan notes` directive needs, and states the bans that make them
    /// safe. The `AI Agent` ban is the load-bearing one: an agent that could set
    /// it could queue further agent runs.
    #[test]
    fn build_prompt_default_permits_bounded_tracker_writes() {
        let wd = std::path::Path::new("/tmp/wd");
        let p = build_prompt(&src(), &job(), wd, 60, None);

        // Permitted: read across projects, create, then update/comment/link what
        // this run created.
        assert!(p.contains("yt issue create"), "{p}");
        assert!(p.contains("yt issue inspect <KEY>"), "{p}");
        assert!(
            p.contains("ONLY the issues you created during THIS run"),
            "{p}"
        );
        assert!(p.contains("No orphan notes"), "{p}");
        // The create scope renders from `{{write_projects}}`, defaulting to the
        // triggering issue's own project.
        assert!(
            p.contains("create it ONLY in these projects: MK,"),
            "create scope must render the project list: {p}"
        );
        // The single permitted link write to the triggering issue.
        assert!(
            p.contains("yt issue link <new-issue> MK-16 --type \"subtask of\""),
            "{p}"
        );
        // The triggering issue may now also be commented on.
        assert!(
            p.contains(
                "you MAY comment with 'yt issue comment {{key}}'"
                    .replace("{{key}}", "MK-16")
                    .as_str()
            ),
            "the comment permission on the triggering issue must be present: {p}"
        );
        assert!(
            p.contains(
                "The runner still posts its own outcome comment on MK-16; do not duplicate it"
            ),
            "{p}"
        );

        // Banned, including on issues the run created.
        assert!(
            p.contains("NEVER set the 'AI Agent' field on ANY issue, including issues you created yourself"),
            "the AI Agent ban must be present verbatim: {p}"
        );
        assert!(p.contains("No other write to MK-16"), "{p}");
        assert!(
            !p.contains("No other write to MK-16: no description or summary edit, no comment,"),
            "comment must no longer be listed among the bans on the triggering issue: {p}"
        );
        assert!(
            p.contains("No write of any kind to an issue you did not create during this run"),
            "{p}"
        );
        assert!(p.contains("Never run 'yt issue delete' anywhere"), "{p}");
        // The code contract is unchanged.
        assert!(p.contains("do NOT push and do NOT open a PR"), "{p}");
        // The blanket ban this replaced must not creep back: it contradicts the
        // `No orphan notes` directive, which is what drove the relaxation.
        assert!(
            !p.contains("do NOT touch YouTrack"),
            "the blanket YouTrack ban must stay removed: {p}"
        );
    }

    /// The prompt tells the agent to post an early size note on the triggering
    /// issue when it expects the issue to be too large, and to keep working
    /// rather than stopping or using the blocker markers.
    #[test]
    fn build_prompt_default_documents_the_size_note() {
        let p = build_prompt(&src(), &job(), std::path::Path::new("/tmp/wd"), 60, None);
        assert!(
            p.contains("too large to finish and verify within the time budget"),
            "{p}"
        );
        assert!(
            p.contains("post one short comment on MK-16 with 'yt issue comment MK-16'"),
            "{p}"
        );
        assert!(
            p.contains("Do not stop for this and do not use the blocker markers for this"),
            "{p}"
        );
        assert!(p.contains("There is no size limit and no refusal"), "{p}");
    }

    /// The no-code path is advertised in the prompt, with the marker
    /// path rendered, and is fenced off from the blocker path.
    #[test]
    fn build_prompt_default_documents_the_no_code_path() {
        let p = build_prompt(&src(), &job(), std::path::Path::new("/tmp/wd"), 60, None);
        assert!(p.contains("/tmp/wd/.claude-run-no-code-change"), "{p}");
        assert!(
            p.contains("NEVER an escape hatch for a code task you could not finish"),
            "{p}"
        );
        assert!(!p.contains("{{"), "unresolved placeholder:\n{p}");
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
            head_branch: Some("fix/x".into()),
        };
        assert_eq!(
            pr_url(&j, &with_num),
            "https://dev.a8n.run/contoso/claude-run/pulls/7"
        );
        let no_num = PullRequest {
            number: None,
            url: "fallback line".into(),
            head_branch: None,
        };
        assert_eq!(pr_url(&j, &no_num), "fallback line");
        // A real HTTP URL (the GitHub backend's html_url) is used
        // verbatim rather than rebuilt against the Forgejo `pulls` path.
        let gh = PullRequest {
            number: Some(11),
            url: "https://github.com/octo/widget/pull/11".into(),
            head_branch: Some("fix/x".into()),
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
        let Gathered { jobs, events, .. } = gather(&backend, &src().query, None).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].key, "MK-16");
        assert_eq!(jobs[0].summary, "a");
        assert_eq!(
            jobs[0].clone_url,
            "https://dev.a8n.run/contoso/claude-run.git"
        );
        // The empty-VCS drop is recorded as an info event so the
        // skipped project is visible in the web UI.
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, "info");
        assert_eq!(events[0].context.as_deref(), Some("ZZ"));
    }

    #[test]
    fn gather_skips_project_whose_vcs_probe_errors() {
        // A probe tool error (e.g. `yt` exit 2) is not the same as
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
        let Gathered {
            jobs,
            events,
            candidate_keys,
        } = gather(&backend, &src().query, None).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].key, "MK-16");
        // The probe failure is recorded as a warn event (not the
        // silent no-integration drop) so it surfaces in the web UI.
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, "warn");
        assert_eq!(events[0].context.as_deref(), Some("ZZ"));
        assert!(events[0].message.contains("VCS probe failed"));
        // The candidate set is what the QUERY matched, so a project
        // skipped this poll keeps its dependency-gate row instead of having it
        // pruned (and re-commented) every time a probe or a grant is flaky.
        assert_eq!(
            candidate_keys,
            vec!["MK-16".to_string(), "ZZ-1".to_string()]
        );
    }

    /// A [`RepoAccess`] double that records every probe and either
    /// passes or denies with a canned message.
    struct StubAccess {
        deny: Option<String>,
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl StubAccess {
        fn allow() -> Self {
            Self {
                deny: None,
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn deny(msg: &str) -> Self {
            Self {
                deny: Some(msg.to_string()),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl RepoAccess for StubAccess {
        fn check(&self, host: &str, repo: &str) -> Result<()> {
            self.calls.lock().unwrap().push(format!("{host}/{repo}"));
            match &self.deny {
                None => Ok(()),
                Some(msg) => Err(Error::Other(anyhow::anyhow!("{msg}"))),
            }
        }
    }

    /// Two queued issues in one project, whose VCS resolves to one
    /// repo. `yt` canned responses are shared, so the same runner can serve
    /// several polls.
    fn access_runner() -> StubRunner {
        StubRunner::new()
            .ok(
                "yt",
                &["list", "--query=-Resolved AI Agent: Queued", "--json"],
                r#"[{"idReadable":"MK-16","summary":"a"},{"idReadable":"MK-17","summary":"b"}]"#,
            )
            .ok(
                "yt",
                &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
                r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#,
            )
    }

    #[test]
    fn gather_keeps_issues_when_forge_can_read_the_repo_and_probes_once() {
        // A readable repo proceeds unchanged, and the probe runs ONCE
        // per repo per poll, not once per issue.
        let backend = CliBackend::new(Arc::new(access_runner()));
        let access = StubAccess::allow();
        let Gathered { jobs, events, .. } = gather(&backend, &src().query, Some(&access)).unwrap();
        assert_eq!(jobs.len(), 2);
        // A pass emits only the recovery marker, once per repo. It is
        // dropped before recording unless it clears an open failure
        // (`access::retain_access_recoveries`), so a healthy poll logs nothing.
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].level, "info");
        assert!(
            events[0].message.starts_with(state::ACCESS_RECOVERY_PREFIX),
            "{}",
            events[0].message
        );
        assert_eq!(
            *access.calls.lock().unwrap(),
            vec!["dev.a8n.run/contoso/claude-run".to_string()]
        );
    }

    #[test]
    fn gather_drops_issues_when_forge_cannot_read_the_repo() {
        // An unreadable repo drops every issue of that repo before any
        // job is built, records one warn event naming host, repo, and cause, and
        // writes NO YouTrack field (the issues stay Queued), so the next poll
        // after the permission grant picks them up with no manual field change.
        let runner = Arc::new(access_runner());
        let backend = CliBackend::new(runner.clone());
        let denied = StubAccess::deny("forge token cannot read it (404)");
        let Gathered { jobs, events, .. } = gather(&backend, &src().query, Some(&denied)).unwrap();
        assert!(jobs.is_empty(), "{jobs:?}");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, "warn");
        assert_eq!(events[0].context.as_deref(), Some("MK"));
        assert!(
            events[0].message.contains("contoso/claude-run"),
            "{}",
            events[0].message
        );
        assert!(
            events[0].message.contains("dev.a8n.run"),
            "{}",
            events[0].message
        );
        assert!(
            events[0]
                .message
                .contains("forge token cannot read it (404)"),
            "{}",
            events[0].message
        );
        // One probe for the repo, not one per issue.
        assert_eq!(denied.calls.lock().unwrap().len(), 1);
        // No `AI Agent` write on the skip path: the issues stay Queued.
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("set-field")),
            "gather must not touch AI Agent: {calls:?}"
        );
        drop(calls);

        // The permission grant lands; the very next poll gathers the same issues
        // with no field change in between.
        let granted = StubAccess::allow();
        let Gathered { jobs, events, .. } = gather(&backend, &src().query, Some(&granted)).unwrap();
        assert_eq!(jobs.len(), 2);
        // The poll that recovers records the marker that clears
        // the dashboard's access banner for this repo.
        assert_eq!(events.len(), 1, "{events:?}");
        assert!(
            events[0].message.starts_with(state::ACCESS_RECOVERY_PREFIX),
            "{}",
            events[0].message
        );
    }

    #[test]
    fn forge_access_check_is_skipped_without_a_forge_token_ref() {
        // With no `token_ref`, `fj` / `gh` read the OPERATOR store, so
        // a probe would test the wrong identity. No checker is built at all.
        let runner: Arc<dyn CommandRunner> = Arc::new(StubRunner::new());
        let gh = crate::action::GithubTarget::default();
        let base = Path::new("/tmp/claude-run-test-creds");
        let host_auth = Arc::new(secrets::ResolvedCredentials::default());
        assert!(forge_access_check(&runner, &host_auth, base, &gh).is_none());
        // A yt-only `token_ref` is still not a FORGE identity.
        let yt_only = Arc::new(secrets::ResolvedCredentials {
            yt: Some(("https://yt.test".into(), "t".into())),
            ..Default::default()
        });
        assert!(forge_access_check(&runner, &yt_only, base, &gh).is_none());
        // A forge `token_ref` builds the checker.
        let fj = Arc::new(secrets::ResolvedCredentials {
            fj_token: Some("t".into()),
            ..Default::default()
        });
        assert!(forge_access_check(&runner, &fj, base, &gh).is_some());
    }

    #[test]
    fn forge_access_check_skips_a_host_it_has_no_token_for() {
        // A run carrying only a GitHub PAT probes GitHub hosts but
        // leaves a Forgejo repo alone (that would test the operator store), and
        // issues no CLI call for it.
        let runner = Arc::new(StubRunner::new());
        let dyn_runner: Arc<dyn CommandRunner> = runner.clone();
        let gh_only = Arc::new(secrets::ResolvedCredentials {
            github_token: Some("pat".into()),
            ..Default::default()
        });
        let check = forge_access_check(
            &dyn_runner,
            &gh_only,
            Path::new("/tmp/claude-run-test-creds"),
            &crate::action::GithubTarget::default(),
        )
        .unwrap();
        check.check("dev.a8n.run", "contoso/claude-run").unwrap();
        assert!(runner.calls.lock().unwrap().is_empty());
    }

    /// Two overlapping polls of one action gather concurrently. Each
    /// gets its own credential directory, so the first guard's `Drop` (which
    /// `remove_dir_all`s the directory) cannot scrub the still-running poll's
    /// `yt` config. With the old `gather-{pid}` name both polls shared a path and
    /// the survivor's `YOUTRACK_CLI_CONFIG` pointed at a deleted file.
    #[test]
    fn concurrent_gather_credentials_do_not_share_a_directory() {
        let base = tempfile::tempdir().unwrap();
        let cred_base = base.path().join("YouTrack");
        let creds = secrets::ResolvedCredentials {
            yt: Some(("https://yt.test".into(), "tok".into())),
            ..Default::default()
        };

        let first_dir = gather_cred_dir(&cred_base);
        let second_dir = gather_cred_dir(&cred_base);
        assert_ne!(first_dir, second_dir, "two polls shared a credential dir");

        let first = JobCredentials::materialize(&first_dir, &creds, None).unwrap();
        let second = JobCredentials::materialize(&second_dir, &creds, None).unwrap();
        let second_cfg = second
            .yt_env
            .iter()
            .find(|(k, _)| k == "YOUTRACK_CLI_CONFIG")
            .map(|(_, v)| PathBuf::from(v))
            .expect("yt env carries YOUTRACK_CLI_CONFIG");

        // The first poll finishes and its guard scrubs its own directory only.
        drop(first);
        assert!(!first_dir.exists());
        assert!(second_dir.join("yt-config.yml").exists());
        assert!(second_cfg.exists(), "{second_cfg:?} was scrubbed");
    }

    /// The preflight carries the same uniqueness. Two
    /// probes of the SAME host and repo (the overlapping-poll case) run against
    /// separate credential stores, visible as distinct `XDG_DATA_HOME` values.
    #[test]
    fn concurrent_preflight_probes_do_not_share_a_directory() {
        let base = tempfile::tempdir().unwrap();
        let repo = "contoso/claude-run";
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "repo", "view", repo, "--json"],
                    r#"{"full_name":"contoso/claude-run"}"#,
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "search",
                        "--repo",
                        repo,
                        "--state",
                        "open",
                        "--json",
                    ],
                    "[]",
                ),
        );
        let dyn_runner: Arc<dyn CommandRunner> = runner.clone();
        let fj_creds = Arc::new(secrets::ResolvedCredentials {
            fj_token: Some("t".into()),
            ..Default::default()
        });
        let check = forge_access_check(
            &dyn_runner,
            &fj_creds,
            base.path(),
            &crate::action::GithubTarget::default(),
        )
        .unwrap();
        check.check("dev.a8n.run", repo).unwrap();
        check.check("dev.a8n.run", repo).unwrap();

        let stores: Vec<String> = runner
            .envs
            .lock()
            .unwrap()
            .iter()
            .filter(|(bin, _)| bin == "fj")
            .filter_map(|(_, env)| {
                env.iter()
                    .find(|(k, _)| k == "XDG_DATA_HOME")
                    .map(|(_, v)| v.clone())
            })
            .collect();
        assert_eq!(stores.len(), 4, "two probes, two fj calls each: {stores:?}");
        assert_ne!(stores[0], stores[2], "probes shared a credential dir");
        // Within one probe both fj calls read the same store.
        assert_eq!(stores[0], stores[1]);
        assert_eq!(stores[2], stores[3]);
    }

    /// A configured assignee is written via `yt issue update
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
        // An empty key store: `clone_url` must resolve nothing here rather than
        // read the developer's real forgejo-cli credentials.
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let backend = CliBackend::new(runner.clone()).with_keys_path(&keys);
        let mut s = src();
        s.assignee = Some("octocat".into());
        let mut j = job();
        // Bogus local path: `git clone` fails immediately with no network.
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let _ = run_pre(&backend, &backend, &s, &j, &wd, None);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue update MK-16 --assignee octocat"),
            "{calls:?}"
        );
    }

    /// With no assignee configured (the default), pickup issues no
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
        // An empty key store: `clone_url` must resolve nothing here rather than
        // read the developer's real forgejo-cli credentials.
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let backend = CliBackend::new(runner.clone()).with_keys_path(&keys);
        let s = src(); // assignee defaults to None
        assert!(s.assignee.is_none());
        let mut j = job();
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let _ = run_pre(&backend, &backend, &s, &j, &wd, None);
        let calls = runner.calls.lock().unwrap();
        assert!(!calls.iter().any(|c| c.contains("--assignee")), "{calls:?}");
    }

    /// An assignee write failure aborts pickup with a failure reason,
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
        // An empty key store: `clone_url` must resolve nothing here rather than
        // read the developer's real forgejo-cli credentials.
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let backend = CliBackend::new(runner.clone()).with_keys_path(&keys);
        let mut s = src();
        s.assignee = Some("octocat".into());
        let mut j = job();
        j.clone_url = "/nonexistent/claude-run-131.git".into();
        let err = run_pre(&backend, &backend, &s, &j, &wd, None).unwrap_err();
        assert!(err.starts_with("set Assignee=octocat:"), "{err}");
        // The clone is never attempted: the workdir stays absent.
        assert!(!wd.exists(), "clone must not run after an assignee failure");
    }

    /// With a resume branch set, `run_pre` clones THAT branch rather
    /// than the repo's default, so the agent lands on the recorded PR's work.
    #[test]
    fn run_pre_clones_the_resume_branch_when_set() {
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_in(&origin, &["init"]);
        set_identity(&origin);
        std::fs::write(origin.join("f.txt"), "main").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-m", "feat: base"]);
        git_in(&origin, &["checkout", "-b", "fix/existing"]);
        std::fs::write(origin.join("f.txt"), "fix").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-m", "fix: existing work"]);

        let wd = base.path().join("clone");
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Working"],
                    "",
                )
                .ok("yt", &["issue", "set-state", "MK-16", "In Progress"], ""),
        );
        let keys = base.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let backend = CliBackend::new(runner.clone()).with_keys_path(&keys);
        let mut j = job();
        j.clone_url = origin.to_str().unwrap().to_string();

        run_pre(&backend, &backend, &src(), &j, &wd, Some("fix/existing")).unwrap();

        let out = Command::new("git")
            .arg("-C")
            .arg(&wd)
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "fix/existing");
        assert_eq!(std::fs::read_to_string(wd.join("f.txt")).unwrap(), "fix");
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
            None,
            Duration::from_secs(1800),
            false,
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

    /// The agent-signalled blocker park (CLAUDE-373) attaches the
    /// Needs-Info tag alongside the marker comment and the `AI Agent` write,
    /// in that order: comment, then tag, then field.
    #[test]
    fn finalize_needs_review_writes_needs_info_tag() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NEEDS_REVIEW_MARKER), "need a decision on X").unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "comment", "MK-16", "need a decision on X"],
                    "",
                )
                .ok(
                    "yt",
                    &["issue", "tag", "add", "MK-16", "Needs-Info", "--create"],
                    "",
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
            wd,
            &ClaudeResult::synthesized("e", "s"),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NeedsReview);
        let calls = runner.calls.lock().unwrap();
        let comment_at = calls
            .iter()
            .position(|c| c.starts_with("yt issue comment"))
            .expect("comment posted");
        let tag_at = calls
            .iter()
            .position(|c| c == "yt issue tag add MK-16 Needs-Info --create")
            .expect("Needs-Info tag attached");
        let field_at = calls
            .iter()
            .position(|c| c == "yt issue set-field MK-16 AI Agent Needs Review")
            .expect("AI Agent write");
        assert!(comment_at < tag_at, "{calls:?}");
        assert!(tag_at < field_at, "{calls:?}");
    }

    /// `tags.needs_info` set to empty disables the tag write entirely; the
    /// `AI Agent` write still happens.
    #[test]
    fn finalize_needs_review_skips_tag_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NEEDS_REVIEW_MARKER), "need a decision on X").unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Needs Review"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let mut s = src();
        s.tags.needs_info = String::new();
        let res = finalize(
            &backend,
            &backend,
            &s,
            &job(),
            wd,
            &ClaudeResult::synthesized("e", "s"),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NeedsReview);
        let calls = runner.calls.lock().unwrap();
        // No stub registered for `tag add`; reaching this point without a
        // "no canned response" error proves add_tag was never called.
        assert!(!calls.iter().any(|c| c.contains("tag add")), "{calls:?}");
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
    }

    /// A failing tag write is logged and does not block the authoritative
    /// `AI Agent` write.
    #[test]
    fn finalize_needs_review_tag_failure_does_not_block_ai_agent_write() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NEEDS_REVIEW_MARKER), "need a decision on X").unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "yt",
                    &["issue", "tag", "add", "MK-16", "Needs-Info", "--create"],
                    1,
                    "no such tag",
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
            wd,
            &ClaudeResult::synthesized("e", "s"),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NeedsReview);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "a failed tag write must not block the AI Agent write: {calls:?}"
        );
    }

    /// A successful run that deliberately produced tracker output
    /// rather than code leaves `.claude-run-no-code-change`. Finalize skips the
    /// PR entirely, posts the marker text verbatim as the issue comment, and
    /// sets AI Agent to the no-code label (defaulting to the `done` label).
    /// `State` is never touched, so the issue stays In Progress for a human.
    #[test]
    fn finalize_no_code_marker_skips_pr_and_sets_no_code_label() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NO_CODE_MARKER), "Filed MK-99 for the gap.\n").unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Done"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            wd,
            &ok_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NoCodeChange);

        let calls = runner.calls.lock().unwrap();
        // No forge call at all, and no git branch resolution ahead of one.
        assert!(
            !calls.iter().any(|c| c.starts_with("fj ")),
            "no PR may be opened for a no-code run: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git ")),
            "the PR path is skipped entirely: {calls:?}"
        );
        // The marker text is the comment, verbatim.
        let comment = calls
            .iter()
            .find(|c| c.starts_with("yt issue comment MK-16"))
            .expect("summary comment posted");
        assert!(comment.contains("Filed MK-99 for the gap."), "{comment}");
        // AI Agent goes to the no-code label; State is left alone.
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Done"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("set-state")),
            "State must stay In Progress: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("AI Agent Failed")),
            "a deliberate no-code run must not be Failed: {calls:?}"
        );
        // A no-code completion never writes the Needs-Info tag (CLAUDE-373).
        assert!(!calls.iter().any(|c| c.contains("tag add")), "{calls:?}");
    }

    /// `states.no_code` is configurable per action and overrides the
    /// `done` default.
    #[test]
    fn finalize_no_code_marker_honours_configured_label() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NO_CODE_MARKER), "report only").unwrap();
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Reported"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let mut s = src();
        s.states.no_code = Some("Reported".into());
        let res = finalize(
            &backend,
            &backend,
            &s,
            &job(),
            wd,
            &ok_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NoCodeChange);
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Reported"),
            "{calls:?}"
        );
    }

    /// The blocker marker still wins over the no-code marker. A run
    /// that parks itself must land on Needs Review, never on the success-shaped
    /// no-code outcome, even if it wrote both files.
    #[test]
    fn finalize_needs_review_wins_over_no_code_marker() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        std::fs::write(wd.join(NEEDS_REVIEW_MARKER), "need a decision on X").unwrap();
        std::fs::write(wd.join(NO_CODE_MARKER), "filed nothing").unwrap();
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
            &ok_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::NeedsReview);
    }

    #[test]
    fn finalize_success_branch_strategy_creates_pr_and_sets_code_review() {
        // By default, the branch strategy pushes a real server branch and
        // opens the PR via the API; `-aA` is never invoked.
        let dir = tempfile::tempdir().unwrap();
        // `find_or_create_pr` resolves the clone's git directory
        // before writing the PR body scratch file there.
        let git_dir = dir.path().join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        // The branch strategy resolves the feature branch by name
        // (origin/HEAD base, for-each-ref listing, rev-list ahead check) before
        // pushing `<branch>:<branch>` and opening the PR with an explicit --head.
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                // Idempotency plumbing before the push. Remote branch
                // absent, no open PR -> unchanged push + create happy path.
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok("git", &["log", "-1", "--format=%s", "feat/x"], "feat: x\n")
                .ok(
                    "git",
                    &["log", "-1", "--format=%b", "feat/x"],
                    "body\n\n#MK-16\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "feat: x",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
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
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/x:feat/x"),
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
        // Opt-in: the AGit strategy invokes `fj pr create -aA`.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_one_branch_ahead(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok("git", &["log", "-1", "--format=%s", "HEAD"], "feat: x\n")
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                // AGit is branchless: no server branch to record.
                head_branch: None,
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

    // ---- workdir preservation -------------------------------

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

    // Set a local git identity. `git clone` does not copy the
    // origin's local `user.*`, and CI's container has no global or
    // auto-detectable identity, so every repo that commits must set its own or
    // `git commit` fails with "Author identity unknown".
    fn set_identity(dir: &Path) {
        git_in(dir, &["config", "user.email", "test@test"]);
        git_in(dir, &["config", "user.name", "test"]);
    }

    /// The ahead check runs for every PR strategy, so a finalize
    /// test that expects PR creation to happen must register the branch
    /// resolution plumbing: `origin/HEAD`, the local branch list, and one
    /// `rev-list <base>..<branch>` per non-base branch. Here `feat/x` is the
    /// single branch ahead of `origin/main`.
    fn with_one_branch_ahead(runner: StubRunner) -> StubRunner {
        runner
            .ok(
                "git",
                &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                "origin/main",
            )
            .ok(
                "git",
                &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                "main\nfeat/x",
            )
            .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
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
        assert!(has_local_only_work(dir.path()));
        cleanup_or_preserve(dir.path(), "YouTrack", "MK-16", None);
        assert!(dir.path().exists(), "unpushed commits must survive cleanup");
    }

    #[test]
    fn clean_clone_is_still_removed() {
        // A clone whose HEAD is covered by remote-tracking refs holds nothing
        // unpushed; cleanup proceeds exactly as before.
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.path().join("clone");
        git_in(
            base.path(),
            &["clone", origin.to_str().unwrap(), clone.to_str().unwrap()],
        );
        assert!(!has_local_only_work(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(
            !clone.exists(),
            "a workdir with no unpushed work is removed"
        );
    }

    #[test]
    fn dirty_tree_with_no_unpushed_branch_counts_as_local_only_work() {
        // A workdir on a branch fully pushed under its own name
        // (so no unpushed commit exists) but with an uncommitted change must
        // still be reported as local-only work, since `crate::gc`'s age-based
        // sweep uses exactly this predicate to decide whether a workdir past
        // its retention window may be deleted.
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.path().join("clone");
        git_in(
            base.path(),
            &["clone", origin.to_str().unwrap(), clone.to_str().unwrap()],
        );
        set_identity(&clone);
        std::fs::write(clone.join("f.txt"), "changed").unwrap();

        assert!(
            has_local_only_work(&clone),
            "a dirty tree with no unpushed branch must still count as local-only work"
        );
        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(clone.exists(), "a dirty tree must survive cleanup");
    }

    #[test]
    fn stash_with_no_unpushed_branch_counts_as_local_only_work() {
        // Same shape as the dirty-tree case above, but for a
        // stash with no corresponding unpushed branch.
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.path().join("clone");
        git_in(
            base.path(),
            &["clone", origin.to_str().unwrap(), clone.to_str().unwrap()],
        );
        set_identity(&clone);
        std::fs::write(clone.join("f.txt"), "changed").unwrap();
        git_in(&clone, &["stash"]);

        assert!(
            has_local_only_work(&clone),
            "a stash with no unpushed branch must still count as local-only work"
        );
        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(clone.exists(), "a stash must survive cleanup");
    }

    // ---- post-clone pre-commit hook install -----------------

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

    // Build an origin + single-branch clone, mimicking the shallow
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
        // The clone inherits none of the origin's local `user.*`, so
        // tests that commit in the clone need their own identity.
        set_identity(&clone);
        clone
    }

    #[test]
    fn feature_branch_tip_on_remote_is_deleted() {
        // The agent checked a branch out but left its tip at a
        // commit already on the remote. The old `--not --remotes` heuristic
        // false-positived (only `origin/main` is tracked); the live-remote
        // check sees the tip present and deletes.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/x"]);
        git_in(&clone, &["push", "origin", "feat/x"]);
        assert!(!has_local_only_work(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(!clone.exists(), "tip present on remote must be deleted");
    }

    #[test]
    fn feature_branch_commit_absent_from_remote_is_preserved() {
        // The agent committed work that never reached the remote.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/y"]);
        std::fs::write(clone.join("new.txt"), "work").unwrap();
        git_in(&clone, &["add", "."]);
        git_in(&clone, &["commit", "-m", "feat: y"]);
        assert!(has_local_only_work(&clone));
        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(clone.exists(), "an unpushed commit must survive cleanup");
    }

    // ---- the PR-failure comment names where the work is --------

    fn pr_error() -> Error {
        Error::Other(anyhow::anyhow!("fj pr create failed: 404 Not Found"))
    }

    #[test]
    fn pr_failure_comment_names_the_pushed_remote_branch() {
        // GAD-1 shape: `push_branch` succeeded and only `fj pr create` failed,
        // so every local tip is on the remote. The comment must point at the
        // branch, never claim there was nothing to preserve.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "fix/redact-env-vars-on-export"]);
        std::fs::write(clone.join("new.txt"), "work").unwrap();
        git_in(&clone, &["add", "."]);
        git_in(&clone, &["commit", "-m", "fix: redact"]);
        git_in(&clone, &["push", "origin", "fix/redact-env-vars-on-export"]);
        let sha = String::from_utf8(
            Command::new("git")
                .arg("-C")
                .arg(&clone)
                .args(["rev-parse", "fix/redact-env-vars-on-export"])
                .output()
                .expect("git runs")
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();

        let body = pr_failure_comment(&ok_result(), &pr_error(), &work_location(&clone, None));

        assert!(
            body.contains("fix/redact-env-vars-on-export"),
            "comment must name the remote branch: {body}"
        );
        assert!(body.contains(&sha), "comment must name the tip SHA: {body}");
        assert!(
            !body.contains("No local commits were found to preserve."),
            "pushed work must never read as unrecoverable: {body}"
        );
        assert!(!body.contains("PRESERVED on the daemon host"));
    }

    #[test]
    fn pr_failure_comment_names_the_preserved_workdir_when_unpushed() {
        // Unchanged path: an unpushed commit keeps the workdir, and
        // the comment is the recovery pointer to it.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/y"]);
        std::fs::write(clone.join("new.txt"), "work").unwrap();
        git_in(&clone, &["add", "."]);
        git_in(&clone, &["commit", "-m", "feat: y"]);

        let body = pr_failure_comment(&ok_result(), &pr_error(), &work_location(&clone, None));

        assert!(
            body.contains("PRESERVED on the daemon host at: `"),
            "unpushed work keeps the preserve line: {body}"
        );
        assert!(body.contains(&clone.display().to_string()));
        assert!(!body.contains("No local commits were found to preserve."));
    }

    #[test]
    fn pr_failure_comment_reports_nothing_for_a_bare_default_branch_clone() {
        // Only the remote's default branch exists locally with no commits ahead:
        // there genuinely is no work to point at.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());

        let body = pr_failure_comment(&ok_result(), &pr_error(), &work_location(&clone, None));

        assert!(body.contains("No local commits were found to preserve."));
    }

    #[test]
    fn pr_failure_comment_names_uncommitted_changes_not_safe_on_remote() {
        // A brand-new branch left at `main`'s tip
        // (no commits of its own) with a dirty working tree. The old SHA-
        // membership check called this branch "on the remote" and the comment
        // claimed the work was SAFE there; it must now be reported as
        // uncommitted and preserved, never "safe on the remote".
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(
            &clone,
            &["checkout", "-b", "feat/CLAUDE-269-login-required-gate"],
        );
        std::fs::write(clone.join("f.txt"), "edited but never committed").unwrap();

        let loc = work_location(&clone, None);
        assert!(
            matches!(loc, WorkLocation::UncommittedChanges { .. }),
            "expected UncommittedChanges"
        );

        let body = pr_failure_comment(&ok_result(), &pr_error(), &loc);
        assert!(
            !body.contains("SAFE on the remote"),
            "an unpushed branch must never read as safe on the remote: {body}"
        );
        assert!(
            body.contains("PRESERVED on the daemon host"),
            "uncommitted work must point at the preserved workdir: {body}"
        );
        assert!(body.contains(&clone.display().to_string()));

        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);
        assert!(clone.exists(), "the dirty tree must survive cleanup");
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
        assert!(has_local_only_work(dir.path()));
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

        cleanup_or_preserve(&clone, "YouTrack", "MK-16", None);

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
        // Claude succeeded, the selected (branch) strategy fails; finalize must
        // mark Failed, error loudly with the underlying git-push error, and
        // point at the preserved workdir in the comment instead of letting the
        // commits be destroyed. No silent fallback to AGit.
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        // Branch resolution succeeds (feat/x is ahead of base); the
        // push of the resolved branch is what fails, and that error must reach
        // the preserved-workdir comment.
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                // Idempotency plumbing before the push. Remote branch
                // absent, so the push is attempted (and fails as before).
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
                .ok("git", &["log", "-1", "--format=%s", "feat/x"], "feat: x\n")
                .fail(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
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
            None,
            Duration::from_secs(1800),
            false,
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
    fn finalize_agit_with_no_commits_fails_instead_of_code_review() {
        // Claude "succeeded" but produced no branch ahead of base.
        // Under the AGit strategy this used to open an empty autofilled PR and
        // move the issue to Code Review. The hoisted ahead check now fails the
        // finalize: AI Agent=Failed, no PR link commented, `fj` never invoked.
        //
        // The no-code path must not relax this: only the explicit
        // `.claude-run-no-code-change` marker buys a zero-commit run a
        // non-failure outcome, and this run writes no marker.
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        assert!(
            !dir.path().join(NO_CODE_MARKER).exists(),
            "this regression covers the no-marker case"
        );
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main",
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
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(res, Finalized::Failed);
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.starts_with("fj ")),
            "no PR may be opened for a run with no commits: {calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c.contains("set-field MK-16 AI Agent Code Review")),
            "must not move to Code Review: {calls:?}"
        );
        let comment = calls
            .iter()
            .find(|c| c.starts_with("yt issue comment MK-16"))
            .expect("failure comment posted");
        assert!(comment.contains("no feature branch"), "{comment}");
        assert!(!comment.contains("/pulls/"), "no PR link: {comment}");
    }

    #[test]
    fn finalize_github_job_opens_pr_via_forge_and_sets_code_review() {
        // A GitHub job finalizes through the GitHub `Forge`. The PR's
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
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://github.com/octo/widget/pull/11".into(),
                pr_number: Some(11),
                head_branch: Some("feat/gh".into()),
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
        let runner = Arc::new(with_one_branch_ahead(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok("git", &["log", "-1", "--format=%s", "HEAD"], "feat: x\n")
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
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &s,
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            None,
            Duration::from_secs(1800),
            false,
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
            None,
            Duration::from_secs(1800),
            false,
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
            None,
            Duration::from_secs(1800),
            false,
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
        // A timeout (exit 124) with AI Agent still on Working advances
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
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: None,
                head_branch: None
            }
        );
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
        // A timeout park also lands on Needs Review but is a different code
        // path from the agent-signalled blocker; it never writes the
        // Needs-Info tag (CLAUDE-373).
        assert!(!calls.iter().any(|c| c.contains("tag add")), "{calls:?}");
    }

    #[test]
    fn finalize_timeout_leaves_field_when_already_advanced() {
        // A timeout whose AI Agent has already moved off Working (e.g.
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
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: None,
                head_branch: None
            }
        );
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

    // ---- CLAUDE-379: salvage timed-out work as a draft PR -----------------

    /// A single-branch clone of `origin` with one tracked file modified in the
    /// working tree but never staged or committed: the real git state
    /// `workdir::local_only_work` reports as a dirty tree (nothing unpushed).
    /// `salvage_timeout_work` reads this directly with real `git`, never
    /// through a `CommandRunner`, so this fixture is real on disk; every
    /// subsequent push/PR-create/comment call below still goes through the
    /// backend's `StubRunner` regardless of what branches this clone actually
    /// has, exactly like the non-timeout `finalize_success_*` tests.
    fn dirty_single_branch_clone(base: &Path) -> PathBuf {
        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_in(&origin, &["init"]);
        set_identity(&origin);
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-m", "feat: x"]);
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
        set_identity(&clone);
        std::fs::write(clone.join("f.txt"), "wip change").unwrap();
        clone
    }

    #[test]
    fn finalize_timeout_salvages_dirty_tree_opens_draft_pr_and_comments() {
        // A dirty tree at kill time is committed as the runner's own WIP
        // commit, pushed, opened as a draft (`WIP: `-prefixed title) PR, and
        // the PR gets a comment explaining the timeout. The issue still lands
        // on Needs Review and the TimedOut outcome carries the PR so
        // `record_new_pr` can store it exactly like a successful run (CLAUDE-275).
        let base = tempfile::tempdir().unwrap();
        let clone = dirty_single_branch_clone(base.path());
        let git_dir = clone.join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let scratch = std::env::temp_dir().join("claude-run-timeout-pr-comment-MK-16.md");
        let _ = std::fs::remove_file(&scratch);

        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Working\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%s", "feat/x"],
                    "feat: agent work\n",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%b", "feat/x"],
                    "body\n\n#MK-16\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "WIP: feat: agent work",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
                        "--json",
                    ],
                    r#"{"number":9,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/9"}"#,
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "comment",
                        "contoso/claude-run#9",
                        "--body-file",
                        scratch.to_string_lossy().as_ref(),
                    ],
                    "",
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
            &clone,
            &timed_out_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: Some(9),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/x:feat/x"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.contains("WIP: feat: agent work")),
            "the draft PR title must carry the WIP: prefix: {calls:?}"
        );
        let expect_comment_call = format!(
            "fj --host dev.a8n.run pr comment contoso/claude-run#9 --body-file {}",
            scratch.display()
        );
        assert!(calls.iter().any(|c| c == &expect_comment_call), "{calls:?}");
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c == "git log -1 --format=%b feat/x"),
            "the runner-written draft body must not be derived from the tip commit's own body: {calls:?}"
        );
        let _ = std::fs::remove_file(&scratch);
    }

    #[test]
    fn finalize_timeout_salvage_draft_body_ignores_a_runner_wip_tip_body_too() {
        // Same shape as the test above, but the tip is the runner's own
        // `WIP: uncommitted changes at timeout` commit rather than a
        // pre-existing agent commit. Either way the draft PR body is the
        // runner-written one (CLAUDE-398), never derived from `git log`.
        let base = tempfile::tempdir().unwrap();
        let clone = dirty_single_branch_clone(base.path());
        let git_dir = clone.join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let scratch = std::env::temp_dir().join("claude-run-timeout-pr-comment-MK-16.md");
        let _ = std::fs::remove_file(&scratch);

        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Working\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%s", "feat/x"],
                    "WIP: uncommitted changes at timeout\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "WIP: uncommitted changes at timeout",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
                        "--json",
                    ],
                    r#"{"number":9,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/9"}"#,
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "comment",
                        "contoso/claude-run#9",
                        "--body-file",
                        scratch.to_string_lossy().as_ref(),
                    ],
                    "",
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
            &clone,
            &timed_out_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: Some(9),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c.contains("WIP: uncommitted changes at timeout")
                    && !c.contains("WIP: WIP:")),
            "a tip already carrying the WIP: prefix must not be double-prefixed: {calls:?}"
        );
        let _ = std::fs::remove_file(&scratch);
    }

    #[test]
    fn finalize_timeout_salvage_push_failure_is_named_in_the_comment() {
        // The push itself fails (e.g. the remote rejects it): salvage records
        // `open_error`, the issue still advances to Needs Review, and no PR
        // is recorded since none was ever opened.
        let base = tempfile::tempdir().unwrap();
        let clone = dirty_single_branch_clone(base.path());

        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Working\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                )
                .fail(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    1,
                    "remote rejected the push",
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
            &clone,
            &timed_out_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: None,
                head_branch: None,
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "a failed push must never reach PR creation: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue set-field MK-16 AI Agent Needs Review"),
            "{calls:?}"
        );
    }

    #[test]
    fn finalize_timeout_salvage_pr_comment_failure_still_reports_the_pr() {
        // The draft PR opens fine but posting the explanation comment on it
        // fails: the PR is still salvaged and returned (so it gets recorded),
        // the comment failure is just named separately.
        let base = tempfile::tempdir().unwrap();
        let clone = dirty_single_branch_clone(base.path());
        let git_dir = clone.join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let scratch = std::env::temp_dir().join("claude-run-timeout-pr-comment-MK-16.md");
        let _ = std::fs::remove_file(&scratch);

        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Working\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%s", "feat/x"],
                    "feat: agent work\n",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%b", "feat/x"],
                    "body\n\n#MK-16\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "WIP: feat: agent work",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
                        "--json",
                    ],
                    r#"{"number":9,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/9"}"#,
                )
                .fail(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "comment",
                        "contoso/claude-run#9",
                        "--body-file",
                        scratch.to_string_lossy().as_ref(),
                    ],
                    1,
                    "comment rejected",
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
            &clone,
            &timed_out_result(),
            PrStrategy::Branch,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        // The PR still counts, even though the comment on it failed.
        assert_eq!(
            res,
            Finalized::TimedOut {
                pr_number: Some(9),
                head_branch: Some("feat/x".into()),
            }
        );
        let _ = std::fs::remove_file(&scratch);
    }

    #[test]
    fn commit_timeout_wip_names_the_git_failure() {
        // A workdir with no `.git` at all (can't happen on the real salvage
        // path, which only calls this after `local_only_work` finds a dirty
        // tracked file, but exercises the commit step's own failure handling
        // in isolation): `git add` fails loudly rather than panicking or
        // silently no-op'ing.
        let dir = tempfile::tempdir().unwrap();
        let err = commit_timeout_wip(dir.path(), "MK-16").unwrap_err();
        assert!(
            err.contains("git add"),
            "the commit-step failure must name `git add`: {err}"
        );
    }

    #[test]
    fn commit_timeout_wip_body_ends_with_the_bare_issue_reference() {
        // CLAUDE-398 / CLAUDE-379: the salvage WIP commit's body must end
        // with the bare `#<KEY>` reference.
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        std::fs::write(dir.path().join("dirty.txt"), "uncommitted\n").unwrap();
        commit_timeout_wip(dir.path(), "MK-16").unwrap();
        let out = Command::new("git")
            .arg("-C")
            .arg(dir.path())
            .args(["log", "-1", "--format=%B"])
            .output()
            .unwrap();
        let message = String::from_utf8_lossy(&out.stdout);
        assert!(
            message.trim_end().ends_with("#MK-16"),
            "the commit body must end with the bare issue reference: {message}"
        );
    }

    #[test]
    fn finalize_resumed_success_strips_the_draft_prefix_via_mark_pr_ready() {
        // CLAUDE-379 criterion 5: a resumed run that finishes successfully
        // must clear the draft/`WIP: ` status the timeout salvage left behind.
        let dir = tempfile::tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
                // `pr search` is called twice with identical args: once by
                // `find_open_pr` (before create, looking for `head_ref ==
                // "feat/x"`) and again by `mark_pr_ready` after the issue
                // advances (looking up PR #7's title to strip its `WIP: `
                // prefix). `StubRunner` keys by exact args, not call order, so
                // one response must satisfy both: `head_ref` left empty so the
                // first lookup finds no match and falls through to create.
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
                    r#"{"pull_requests":[{"number":7,"title":"WIP: feat: x","head_sha":"abc"}]}"#,
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok("git", &["log", "-1", "--format=%s", "feat/x"], "feat: x\n")
                .ok(
                    "git",
                    &["log", "-1", "--format=%b", "feat/x"],
                    "body\n\n#MK-16\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "feat: x",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
                        "--json",
                    ],
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
                        "edit",
                        "contoso/claude-run#7",
                        "--title",
                        "feat: x",
                        "--body-file",
                        std::env::temp_dir()
                            .join("claude-run-pr-refresh-7.md")
                            .to_string_lossy()
                            .as_ref(),
                    ],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "edit",
                        "contoso/claude-run#7",
                        "--title",
                        "feat: x",
                    ],
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
            None,
            Duration::from_secs(1800),
            true,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr edit contoso/claude-run#7 --title feat: x"),
            "a resumed success must strip the WIP: prefix: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with(
                "fj --host dev.a8n.run pr edit contoso/claude-run#7 --title feat: x --body-file"
            )),
            "a resumed success must refresh the salvage placeholder title/body too: {calls:?}"
        );
    }

    #[test]
    fn finalize_resumed_success_surfaces_a_refresh_failure_in_the_success_comment() {
        // CLAUDE-398 criterion 8: `refresh_pr_text` is best-effort (it must
        // never block the success outcome), but its failure is never silent:
        // it is logged AND named in the success comment. Omitting the
        // combined --title/--body-file edit stub makes the refresh fail with
        // a "no canned response" error from the StubRunner.
        let dir = tempfile::tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        let body_path = git_dir
            .join("claude-run")
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "abc\n")
                .ok("git", &["ls-remote", "--heads", "origin", "feat/x"], "")
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
                    r#"{"pull_requests":[{"number":7,"title":"WIP: feat: x","head_sha":"abc"}]}"#,
                )
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                .ok("git", &["log", "-1", "--format=%s", "feat/x"], "feat: x\n")
                .ok(
                    "git",
                    &["log", "-1", "--format=%b", "feat/x"],
                    "body\n\n#MK-16\n",
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "create",
                        "feat: x",
                        "--body-file",
                        body_path.as_str(),
                        "--head",
                        "feat/x",
                        "--json",
                    ],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                )
                // The combined --title/--body-file edit stub is deliberately
                // absent: refresh_pr_text's `fj pr edit` call has no canned
                // response, so it fails. mark_pr_ready's own title-only edit
                // is still stubbed, so it still succeeds independently.
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "edit",
                        "contoso/claude-run#7",
                        "--title",
                        "feat: x",
                    ],
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
            None,
            Duration::from_secs(1800),
            true,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr edit contoso/claude-run#7 --title feat: x"),
            "mark_pr_ready must still succeed even when the refresh failed: {calls:?}"
        );
        let comment = calls
            .iter()
            .find(|c| c.starts_with("yt issue comment MK-16"))
            .unwrap_or_else(|| panic!("no issue comment call recorded: {calls:?}"));
        assert!(
            comment.contains("PR text could not be updated"),
            "a refresh failure must be surfaced in the success comment, not silently swallowed: {comment}"
        );
    }

    #[test]
    fn finalize_resumed_success_on_a_moved_remote_branch_still_strips_the_draft_prefix() {
        // CLAUDE-382 criterion 7: the resumed run's clone predates a commit the
        // runner itself pushed onto the branch while the run was working (e.g.
        // a "branch was behind its base" update), so the remote tip has moved
        // and neither SHA is an ancestor of the other. The runner fetches,
        // merges the remote branch in, pushes the merge (never force), adopts
        // the WIP PR the earlier timeout salvage opened, and still strips the
        // `WIP: ` prefix via `mark_pr_ready` and finalizes as Code Review.
        let dir = tempfile::tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main",
                )
                .ok(
                    "git",
                    &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                    "main\nfeat/x",
                )
                .ok("git", &["rev-list", "origin/main..feat/x"], "abc\n")
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/claude-run.git",
                )
                .ok("git", &["rev-parse", "feat/x"], "localsha\n")
                .ok(
                    "git",
                    &["ls-remote", "--heads", "origin", "feat/x"],
                    "remotesha\trefs/heads/feat/x\n",
                )
                .ok("git", &["log", "-1", "--format=%s", "feat/x"], "feat: x\n")
                .ok("git", &["log", "-1", "--format=%b", "feat/x"], "body\n")
                .ok("git", &["fetch", "origin", "feat/x"], "")
                .fail(
                    "git",
                    &["merge-base", "--is-ancestor", "remotesha", "localsha"],
                    1,
                    "",
                )
                .ok("git", &["merge", "--no-edit", "origin/feat/x"], "")
                .ok(
                    "git",
                    &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                    "",
                )
                // `pr search` is called twice with identical args: once by
                // `find_open_pr` (adopting the WIP PR the timeout salvage left
                // open) and again by `mark_pr_ready` after the issue advances.
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
                    r#"{"pull_requests":[{"number":7,"title":"WIP: feat: x","head_ref":"feat/x","head_sha":"localsha"}]}"#,
                )
                .ok(
                    "git",
                    &["rev-parse", "--absolute-git-dir"],
                    &git_dir.to_string_lossy(),
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
                        "edit",
                        "contoso/claude-run#7",
                        "--title",
                        "feat: x",
                        "--body-file",
                        std::env::temp_dir()
                            .join("claude-run-pr-refresh-7.md")
                            .to_string_lossy()
                            .as_ref(),
                    ],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "edit",
                        "contoso/claude-run#7",
                        "--title",
                        "feat: x",
                    ],
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
            None,
            Duration::from_secs(1800),
            true,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                head_branch: Some("feat/x".into()),
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git merge --no-edit origin/feat/x"),
            "a moved remote branch must be merged in: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with(
                "fj --host dev.a8n.run pr edit contoso/claude-run#7 --title feat: x --body-file"
            )),
            "a resumed success on a moved branch must still refresh the salvage placeholder title/body: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/x:feat/x"),
            "the merge result must still be pushed: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "the adopted PR must not be re-created: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "no force anywhere: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr edit contoso/claude-run#7 --title feat: x"),
            "a resumed success on a moved branch must still strip the WIP: prefix: {calls:?}"
        );
    }

    #[test]
    fn default_prompt_tells_a_resumed_run_to_read_the_prs_comments() {
        // CLAUDE-379 criterion 5: a re-queue's rendered prompt must direct the
        // agent to the PR's own comments, not just the issue's, since a
        // timed-out run's salvage note (what it committed, why) lives there.
        let p = default_youtrack_prompt();
        assert!(
            p.contains("read that PR's own comments"),
            "the default prompt must tell a resumed run to read the PR's comments:\n{p}"
        );
    }

    // ---- automerge no longer polls from finalize -------------

    /// A YoutrackSource with automerge enabled.
    fn src_automerge() -> YoutrackSource {
        YoutrackSource {
            automerge: crate::action::Automerge {
                enabled: true,
                ..crate::action::Automerge::default()
            },
            ..YoutrackSource::default()
        }
    }

    #[test]
    fn finalize_never_polls_or_merges_even_with_automerge_enabled() {
        // Finalize stops at Code Review exactly as when automerge is
        // disabled; the CI wait + merge is now driven entirely by the
        // run-succeeded webhook (see `complete_automerge` and
        // `crate::web::webhooks`), never by finalize itself.
        let dir = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_one_branch_ahead(
            StubRunner::new()
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/contoso/claude-run/pulls/7"}"#,
                )
                .ok("git", &["log", "-1", "--format=%s", "HEAD"], "feat: x\n")
                .ok(
                    "yt",
                    &["issue", "set-field", "MK-16", "AI Agent", "Code Review"],
                    "",
                ),
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(
            &backend,
            &backend,
            &src_automerge(),
            &job(),
            dir.path(),
            &ok_result(),
            PrStrategy::Agit,
            None,
            Duration::from_secs(1800),
            false,
        )
        .unwrap();
        assert_eq!(
            res,
            Finalized::CodeReview {
                pr_url: "https://dev.a8n.run/contoso/claude-run/pulls/7".into(),
                pr_number: Some(7),
                head_branch: None,
            }
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("pr status")),
            "finalize must never poll: {calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("pr merge")), "{calls:?}");
        assert!(
            !calls.iter().any(|c| c.contains("AI Agent Done")),
            "{calls:?}"
        );
    }

    #[test]
    fn complete_automerge_merges_and_comments_with_no_state_or_field_write() {
        // The shared merge sequence the webhook success handler
        // calls once it has confirmed a recorded agent PR's checks are green
        // and mergeable: merge as a merge-commit, then a best-effort comment.
        // No `State` and no `AI Agent` write - the YouTrack workflow resolves
        // the issue once it observes the merge.
        let runner = Arc::new(
            StubRunner::new()
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
                .ok(
                    "yt",
                    &[
                        "issue",
                        "comment",
                        "MK-16",
                        "The runner merged this PR after its CI checks passed (automerge).\n\nPR: https://dev.a8n.run/contoso/claude-run/pulls/7",
                    ],
                    "",
                ),
        );
        let backend = CliBackend::new(runner.clone());
        complete_automerge(
            &backend,
            &backend,
            "MK-16",
            "dev.a8n.run",
            "contoso/claude-run",
            7,
            "https://dev.a8n.run/contoso/claude-run/pulls/7",
        )
        .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run pr merge contoso/claude-run#7 --method merge --delete"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("set-state")), "{calls:?}");
        assert!(!calls.iter().any(|c| c.contains("set-field")), "{calls:?}");
    }

    // ---- pre-clone dependency gate ----

    async fn mem_pool() -> DbPool {
        crate::db::test_pool().await
    }

    fn link(
        direction: LinkDirection,
        verb: &str,
        target: &str,
        resolved: Option<i64>,
    ) -> IssueLink {
        IssueLink {
            direction,
            verb: verb.to_string(),
            target: target.to_string(),
            resolved,
        }
    }

    /// The blocking rule (pure). `depends on` (Depend INWARD) and
    /// `parent for` (Subtask OUTWARD) with an unresolved target block; the reverse
    /// verbs `is required for` / `subtask of` never block; a resolved target is
    /// met. The result is sorted, so its signature is order-independent.
    #[test]
    fn unmet_blockers_honors_direction_and_resolved() {
        let block_on = crate::action::Dependencies::default().block_on;
        let links = vec![
            link(LinkDirection::Inward, "depends on", "YT-40", None), // BLOCK: open blocker
            link(LinkDirection::Outward, "parent for", "CLAUDE-9", None), // BLOCK: open child
            link(LinkDirection::Outward, "is required for", "CLAUDE-1", None), // reverse Depend
            link(LinkDirection::Inward, "subtask of", "CLAUDE-2", None), // reverse Subtask
            link(LinkDirection::Inward, "depends on", "CLAUDE-3", Some(123)), // met (resolved)
        ];
        let blockers = unmet_blockers(&links, &block_on, None);
        let ids: Vec<&str> = blockers.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["CLAUDE-9", "YT-40"],
            "only unmet blocking verbs, sorted"
        );
        assert_eq!(
            blocker_signature(&blockers),
            "CLAUDE-9 (parent for); YT-40 (depends on)"
        );
    }

    /// Scoped to its own project, the gate ignores targets anywhere
    /// else, so an `AUDIT` Epic does not wait on the `GAMMA` findings it filed.
    #[test]
    fn unmet_blockers_skips_other_projects_when_scoped() {
        let block_on = crate::action::Dependencies::default().block_on;
        let links = vec![
            link(LinkDirection::Outward, "parent for", "GAMMA-12", None),
            link(LinkDirection::Outward, "parent for", "AUDIT-7", None),
            link(LinkDirection::Inward, "depends on", "EPSILON-3", None),
        ];
        let ids = |own: Option<&str>| -> Vec<String> {
            unmet_blockers(&links, &block_on, own)
                .into_iter()
                .map(|b| b.id)
                .collect()
        };
        assert_eq!(ids(None), vec!["AUDIT-7", "GAMMA-12", "EPSILON-3"]);
        assert_eq!(ids(Some("AUDIT")), vec!["AUDIT-7"]);
    }

    /// `block_cross_project` reaches the gate. An Epic whose only open
    /// child is in another project is blocked by default and proceeds when false.
    #[tokio::test]
    async fn dependency_gate_scopes_to_own_project_when_configured() {
        let inspect = r#"{"links":[{"direction":"OUTWARD","verb":"parent for","issues":[{"idReadable":"GAMMA-12","summary":"x"}]}]}"#;
        let body = blocked_comment(&[Blocker {
            id: "GAMMA-12".into(),
            verb: "parent for".into(),
        }]);
        let runner = Arc::new(
            StubRunner::new()
                .ok("yt", &["issue", "inspect", "AUDIT-7", "--json"], inspect)
                .ok("yt", &["issue", "comment", "AUDIT-7", &body], ""),
        );
        let backend = CliBackend::new(runner.clone());
        let mut s = src();

        let pool = mem_pool().await;
        let d = dependency_gate(&pool, &backend, &s, "act", "AUDIT-7", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Blocked { .. }), "default blocks");

        s.dependencies.block_cross_project = false;
        let pool = mem_pool().await;
        let d = dependency_gate(&pool, &backend, &s, "act", "AUDIT-7", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Proceed), "scoped gate proceeds");
    }

    /// A blocked issue is left Queued, a blocker comment is posted
    /// once, and a backoff row is persisted. A re-eval inside the backoff window
    /// does no `yt` read and no comment; after the window expires an unchanged
    /// blocker set re-reads but does not re-comment (idempotency).
    #[tokio::test]
    async fn dependency_gate_blocks_comments_then_backs_off() {
        let pool = mem_pool().await;
        let inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"YT-40","summary":"x"}]}]}"#;
        let body = blocked_comment(&[Blocker {
            id: "YT-40".into(),
            verb: "depends on".into(),
        }]);
        let runner = Arc::new(
            StubRunner::new()
                .ok("yt", &["issue", "inspect", "MK-16", "--json"], inspect)
                .ok("yt", &["issue", "comment", "MK-16", &body], ""),
        );
        let backend = CliBackend::new(runner.clone());
        let s = src();

        let count = |needle: &str| {
            runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.starts_with(needle))
                .count()
        };
        let inspects = || count("yt issue inspect MK-16");
        let comments = || count("yt issue comment MK-16");

        // First eval: blocked, comments once, persists backoff.
        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Blocked { .. }));
        assert_eq!(inspects(), 1);
        assert_eq!(comments(), 1);
        let st = state::dependency_gate_get(&pool, "act", "MK-16")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.signature, "YT-40 (depends on)");
        assert!(st.blocked_until_ms > state::now_epoch_ms());

        // Second eval within the backoff window: no read, no comment.
        let d2 = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        match d2 {
            GateDecision::Blocked { blockers } => {
                assert_eq!(blockers, vec!["YT-40 (depends on)".to_string()]);
            }
            _ => panic!("still blocked within backoff"),
        }
        assert_eq!(inspects(), 1, "backoff must suppress the re-read");
        assert_eq!(comments(), 1, "backoff must suppress the re-comment");

        // Expire the window: re-eval reads again, unchanged set -> no re-comment.
        state::dependency_gate_upsert(
            &pool,
            "act",
            "MK-16",
            state::now_epoch_ms() - 1000,
            &st.signature,
        )
        .await
        .unwrap();
        let d3 = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        assert!(matches!(d3, GateDecision::Blocked { .. }));
        assert_eq!(inspects(), 2, "expired backoff re-reads");
        assert_eq!(comments(), 1, "unchanged blocker set must not re-comment");
    }

    /// A re-evaluation that finds the blocker still open re-arms the
    /// window but keeps the original first-blocked stamp, so the age reported to
    /// the operator is the age of the whole block and not of the last poll. This
    /// is what makes a long-held gate surface at all: the gate re-evaluates every
    /// `recheck_timeout`, so an age reset per evaluation would never cross the
    /// threshold.
    #[tokio::test]
    async fn dependency_gate_keeps_the_block_age_across_re_evaluations() {
        let pool = mem_pool().await;
        let inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"AUD-32","summary":"x"}]}]}"#;
        let body = blocked_comment(&[Blocker {
            id: "AUD-32".into(),
            verb: "depends on".into(),
        }]);
        let runner = Arc::new(
            StubRunner::new()
                .ok("yt", &["issue", "inspect", "AUD-31", "--json"], inspect)
                .ok("yt", &["issue", "comment", "AUD-31", &body], ""),
        );
        let backend = CliBackend::new(runner.clone());
        let s = src();

        // First block, then age it by three hours with an already-expired window.
        let d = dependency_gate(&pool, &backend, &s, "act", "AUD-31", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Blocked { .. }));
        let aged = state::now_epoch_ms() - 3 * 3_600_000;
        sqlx::query(&crate::db::q(&pool,
            "UPDATE dependency_gate SET blocked_since = ?, blocked_until = ? WHERE job_key = 'AUD-31'",
        ))
        .bind(aged)
        .bind(state::now_epoch_ms() - 1000)
        .execute(&pool)
        .await
        .unwrap();

        // Re-evaluate: still blocked, window re-armed, stamp untouched.
        let d2 = dependency_gate(&pool, &backend, &s, "act", "AUD-31", false)
            .await
            .unwrap();
        assert!(matches!(d2, GateDecision::Blocked { .. }));
        let st = state::dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.blocked_since_ms, aged, "re-arm must not reset the age");
        assert!(st.blocked_until_ms > state::now_epoch_ms());

        // ...so the operator-facing read surfaces it.
        let stale = state::dependency_gates_stale(
            &pool,
            state::now_epoch_ms(),
            state::STALE_GATE_AFTER_MS,
            10,
        )
        .await
        .unwrap();
        assert_eq!(stale.len(), 1, "{stale:?}");
        assert_eq!(stale[0].job_key, "AUD-31");
        assert_eq!(stale[0].signature, "AUD-32 (depends on)");
        assert!(stale[0].blocked_for_ms >= 3 * 3_600_000);
    }

    /// `enabled: false` makes the gate a no-op: it proceeds without
    /// any `yt` read or DB touch (the stub has no inspect response, so a read
    /// would error).
    #[tokio::test]
    async fn dependency_gate_disabled_is_noop() {
        let pool = mem_pool().await;
        let runner = Arc::new(StubRunner::new());
        let backend = CliBackend::new(runner.clone());
        let mut s = src();
        s.dependencies.enabled = false;
        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Proceed));
        assert!(
            runner.calls.lock().unwrap().is_empty(),
            "disabled gate must not touch yt"
        );
    }

    /// When all dependencies are met (or absent), the gate proceeds
    /// and clears any stale backoff row so a previously-blocked issue resumes.
    #[tokio::test]
    async fn dependency_gate_met_proceeds_and_clears_state() {
        let pool = mem_pool().await;
        let inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"YT-40","resolved":123}]}]}"#;
        let runner =
            Arc::new(StubRunner::new().ok("yt", &["issue", "inspect", "MK-16", "--json"], inspect));
        let backend = CliBackend::new(runner.clone());
        let s = src();
        // Seed a stale (expired) blocked row to prove it is cleared on the met path.
        state::dependency_gate_upsert(
            &pool,
            "act",
            "MK-16",
            state::now_epoch_ms() - 1000,
            "YT-40 (depends on)",
        )
        .await
        .unwrap();
        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        assert!(matches!(d, GateDecision::Proceed));
        assert!(
            state::dependency_gate_get(&pool, "act", "MK-16")
                .await
                .unwrap()
                .is_none(),
            "met dependency must clear the backoff row"
        );
    }

    // ---- manual-trigger backoff bypass ----

    /// A manual trigger (`bypass_backoff = true`) inside an open
    /// backoff window reads links live rather than replaying the cached signature.
    /// With the blocker now resolved, the gate proceeds in the same run and clears
    /// the persisted row, without waiting for the window to expire.
    #[tokio::test]
    async fn dependency_gate_bypass_reads_live_and_proceeds_when_resolved() {
        let pool = mem_pool().await;
        // Live links now show the blocker resolved.
        let inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"YT-40","resolved":123}]}]}"#;
        let runner =
            Arc::new(StubRunner::new().ok("yt", &["issue", "inspect", "MK-16", "--json"], inspect));
        let backend = CliBackend::new(runner.clone());
        let s = src();
        // Seed a still-open backoff window (blocked_until in the future).
        state::dependency_gate_upsert(
            &pool,
            "act",
            "MK-16",
            state::now_epoch_ms() + 60_000,
            "YT-40 (depends on)",
        )
        .await
        .unwrap();

        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", true)
            .await
            .unwrap();
        assert!(
            matches!(d, GateDecision::Proceed),
            "manual trigger re-reads live and proceeds on the resolved blocker"
        );
        assert!(
            runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|c| c == "yt issue inspect MK-16 --json"),
            "bypass forces the live read inside the open window"
        );
        assert!(
            state::dependency_gate_get(&pool, "act", "MK-16")
                .await
                .unwrap()
                .is_none(),
            "resolved blocker clears the persisted backoff row"
        );
    }

    /// A manual trigger inside an open window still reads live, and
    /// when the blocker is still open it stays Blocked and re-arms `blocked_until`.
    /// The unchanged blocker set is not re-commented.
    #[tokio::test]
    async fn dependency_gate_bypass_reads_live_but_still_blocks_when_open() {
        let pool = mem_pool().await;
        let inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"YT-40","summary":"x"}]}]}"#;
        let runner =
            Arc::new(StubRunner::new().ok("yt", &["issue", "inspect", "MK-16", "--json"], inspect));
        let backend = CliBackend::new(runner.clone());
        let s = src();
        let seeded_until = state::now_epoch_ms() + 60_000;
        state::dependency_gate_upsert(&pool, "act", "MK-16", seeded_until, "YT-40 (depends on)")
            .await
            .unwrap();

        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", true)
            .await
            .unwrap();
        match d {
            GateDecision::Blocked { blockers } => {
                assert_eq!(blockers, vec!["YT-40 (depends on)".to_string()]);
            }
            _ => panic!("still blocked on the open dependency"),
        }
        assert_eq!(
            runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.as_str() == "yt issue inspect MK-16 --json")
                .count(),
            1,
            "bypass reads live once inside the window"
        );
        // Unchanged blocker set: no re-comment.
        assert!(
            !runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.starts_with("yt issue comment MK-16")),
            "unchanged blocker set must not re-comment on the bypass path"
        );
        let st = state::dependency_gate_get(&pool, "act", "MK-16")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.signature, "YT-40 (depends on)");
        assert!(
            st.blocked_until_ms > state::now_epoch_ms(),
            "still-blocked issue re-arms the backoff window"
        );
    }

    /// The scheduler (`bypass_backoff = false`) inside an open window
    /// replays the cached signature with no live read, exactly as the gather-time
    /// eligibility pass below. The stub has no inspect response, so any read would error.
    #[tokio::test]
    async fn dependency_gate_scheduler_replays_cached_signature_no_read() {
        let pool = mem_pool().await;
        let runner = Arc::new(StubRunner::new());
        let backend = CliBackend::new(runner.clone());
        let s = src();
        state::dependency_gate_upsert(
            &pool,
            "act",
            "MK-16",
            state::now_epoch_ms() + 60_000,
            "YT-40 (depends on)",
        )
        .await
        .unwrap();

        let d = dependency_gate(&pool, &backend, &s, "act", "MK-16", false)
            .await
            .unwrap();
        match d {
            GateDecision::Blocked { blockers } => {
                assert_eq!(blockers, vec!["YT-40 (depends on)".to_string()]);
            }
            _ => panic!("scheduler replays the cached blocked decision"),
        }
        assert!(
            runner.calls.lock().unwrap().is_empty(),
            "scheduler inside the window must not read the tracker"
        );
    }

    // ---- gather-time eligibility pass ----

    /// The gather-time eligibility pass drops a dependency-blocked
    /// issue so a workable same-project sibling is the sole candidate for the
    /// project's concurrency key. Reproduces the TOOL-33 / TOOL-34 starvation:
    /// TOOL-34 depends on the still-open TOOL-33, TOOL-33 is workable. Gather
    /// order lists the blocked issue first (the trigger that let it win the shared
    /// TOOL key and consume the poll slot before this fix). A single pass keeps
    /// only TOOL-33 and drops TOOL-34, while TOOL-34 keeps its blocked side
    /// effects (idempotent blocker comment + persisted backoff); TOOL-33 persists
    /// no gate state.
    #[tokio::test]
    async fn eligibility_pass_drops_blocked_sibling_keeps_workable() {
        let pool = mem_pool().await;
        let mk_job = |key: &str| YoutrackJob {
            key: key.into(),
            summary: "s".into(),
            repo_host: "https://dev.a8n.run".into(),
            repo_path: "contoso/tools".into(),
            clone_url: "https://dev.a8n.run/contoso/tools.git".into(),
        };
        // TOOL-34 depends on the still-open TOOL-33 -> blocked.
        let blocked_inspect = r#"{"links":[{"direction":"INWARD","verb":"depends on","issues":[{"idReadable":"TOOL-33","summary":"x"}]}]}"#;
        let blocked_body = blocked_comment(&[Blocker {
            id: "TOOL-33".into(),
            verb: "depends on".into(),
        }]);
        // TOOL-33 has no unmet blockers -> workable.
        let workable_inspect = r#"{"links":[]}"#;
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "TOOL-34", "AI Agent"],
                    "Queued",
                )
                .ok(
                    "yt",
                    &["issue", "inspect", "TOOL-34", "--json"],
                    blocked_inspect,
                )
                .ok("yt", &["issue", "comment", "TOOL-34", &blocked_body], "")
                .ok(
                    "yt",
                    &["issue", "get-field", "TOOL-33", "AI Agent"],
                    "Queued",
                )
                .ok(
                    "yt",
                    &["issue", "inspect", "TOOL-33", "--json"],
                    workable_inspect,
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let s = src();

        let jobs = vec![mk_job("TOOL-34"), mk_job("TOOL-33")];
        let eligible = filter_eligible(&pool, &backend, &s, "act", jobs, false).await;

        // Only the workable issue survives to compete for the concurrency key.
        let keys: Vec<&str> = eligible.iter().map(|j| j.key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["TOOL-33"],
            "blocked TOOL-34 dropped before the executor; TOOL-33 is the sole candidate"
        );

        // The blocked issue kept its side effects: comment + backoff.
        {
            let calls = runner.calls.lock().unwrap();
            assert!(
                calls
                    .iter()
                    .any(|c| c == &format!("yt issue comment TOOL-34 {blocked_body}")),
                "blocked issue still gets its blocker comment: {calls:?}"
            );
        }
        let st = state::dependency_gate_get(&pool, "act", "TOOL-34")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st.signature, "TOOL-33 (depends on)");
        assert!(st.blocked_until_ms > state::now_epoch_ms());
        // The workable issue persisted no gate state (nothing to back off).
        assert!(
            state::dependency_gate_get(&pool, "act", "TOOL-33")
                .await
                .unwrap()
                .is_none(),
            "workable issue must not persist gate state"
        );
    }

    /// An issue no longer `Queued` at gather (an overlapping run moved
    /// it to Working) is dropped by the eligibility pass and likewise does not
    /// occupy the project's per-poll slot; a still-Queued sibling survives.
    #[tokio::test]
    async fn eligibility_pass_drops_non_queued_issue() {
        let pool = mem_pool().await;
        let mk_job = |key: &str| YoutrackJob {
            key: key.into(),
            summary: "s".into(),
            repo_host: "https://dev.a8n.run".into(),
            repo_path: "contoso/tools".into(),
            clone_url: "https://dev.a8n.run/contoso/tools.git".into(),
        };
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "TOOL-34", "AI Agent"],
                    "Working",
                )
                .ok(
                    "yt",
                    &["issue", "get-field", "TOOL-33", "AI Agent"],
                    "Queued",
                )
                .ok(
                    "yt",
                    &["issue", "inspect", "TOOL-33", "--json"],
                    r#"{"links":[]}"#,
                ),
        );
        let backend = CliBackend::new(runner.clone());
        let s = src();

        let jobs = vec![mk_job("TOOL-34"), mk_job("TOOL-33")];
        let eligible = filter_eligible(&pool, &backend, &s, "act", jobs, false).await;
        let keys: Vec<&str> = eligible.iter().map(|j| j.key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["TOOL-33"],
            "non-Queued TOOL-34 dropped; TOOL-33 survives"
        );
        // A dropped-at-filter issue is never even read for links.
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c == "yt issue inspect TOOL-34 --json"),
            "filtered issue must not reach the dependency read: {calls:?}"
        );
    }

    // ---- resume a recorded open PR's branch on re-queue ----

    fn open_pr(number: u64, head_ref: &str) -> provider::ForgePullRequest {
        provider::ForgePullRequest {
            number,
            title: String::new(),
            head_sha: String::new(),
            head_ref: head_ref.to_string(),
            base_ref: String::new(),
            author: String::new(),
            html_url: format!("https://dev.a8n.run/contoso/claude-run/pulls/{number}"),
            updated_at: String::new(),
        }
    }

    /// A recorded, still-open PR resolves to a resume target
    /// carrying its branch and URL, so `run_pre` clones that branch and the
    /// prompt's `existing_*` placeholders are populated.
    #[tokio::test]
    async fn resolve_pr_resume_returns_branch_when_recorded_pr_still_open() {
        let pool = mem_pool().await;
        crate::ci_failure::store::record_agent_pr(
            &pool,
            "act",
            "MK-16",
            "dev.a8n.run",
            "contoso/claude-run",
            7,
            "fix/MK-16",
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        let runner = Arc::new(
            StubRunner::new().ok(
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
                &serde_json::json!({
                    "total_count": 1,
                    "pull_requests": [open_pr(7, "fix/MK-16")],
                })
                .to_string(),
            ),
        );
        let forge = CliBackend::new(runner);

        let resume = resolve_pr_resume(&pool, &forge, &job(), PrStrategy::Branch)
            .await
            .unwrap()
            .expect("an open recorded PR resolves to a resume target");
        assert_eq!(resume.branch, "fix/MK-16");
        assert_eq!(
            resume.pr_url,
            "https://dev.a8n.run/contoso/claude-run/pulls/7"
        );
    }

    /// A recorded PR that no longer shows up in the open-PR list is
    /// marked `closed` and the caller starts from the default branch.
    #[tokio::test]
    async fn resolve_pr_resume_marks_closed_when_absent_from_open_list() {
        let pool = mem_pool().await;
        crate::ci_failure::store::record_agent_pr(
            &pool,
            "act",
            "MK-16",
            "dev.a8n.run",
            "contoso/claude-run",
            7,
            "fix/MK-16",
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        let runner = Arc::new(StubRunner::new().ok(
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
            r#"{"total_count":0,"pull_requests":[]}"#,
        ));
        let forge = CliBackend::new(runner);

        let resume = resolve_pr_resume(&pool, &forge, &job(), PrStrategy::Branch)
            .await
            .unwrap();
        assert!(resume.is_none());
        let rec = crate::ci_failure::store::agent_pr_for_issue(&pool, "MK-16")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.state, "closed");
    }

    /// The AGit strategy has no server branch to continue on, so an
    /// open recorded PR is never resumed for it - a new PR is opened as before.
    /// Asserting on a `StubRunner` with no registered `fj` call proves the
    /// open-PR lookup is never even attempted.
    #[tokio::test]
    async fn resolve_pr_resume_skips_agit_strategy() {
        let pool = mem_pool().await;
        crate::ci_failure::store::record_agent_pr(
            &pool,
            "act",
            "MK-16",
            "dev.a8n.run",
            "contoso/claude-run",
            7,
            "fix/MK-16",
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        let runner = Arc::new(StubRunner::new());
        let forge = CliBackend::new(runner.clone());

        let resume = resolve_pr_resume(&pool, &forge, &job(), PrStrategy::Agit)
            .await
            .unwrap();
        assert!(resume.is_none());
        assert!(
            runner.calls.lock().unwrap().is_empty(),
            "AGit must never probe the open-PR list"
        );
    }

    /// A fresh (non-resumed) success records the PR in the store.
    #[tokio::test]
    async fn record_new_pr_writes_one_agent_pull_requests_row() {
        let pool = mem_pool().await;
        let runner = Arc::new(StubRunner::new());
        let forge = CliBackend::new(runner);
        let backend = CliBackend::new(Arc::new(StubRunner::new()));

        record_new_pr(&pool, &forge, &backend, "act", &job(), 9, "feat/x").await;

        let rec = crate::ci_failure::store::agent_pr_for_issue(&pool, "MK-16")
            .await
            .unwrap()
            .expect("record_new_pr inserts a row");
        assert_eq!(rec.pr_number, 9);
        assert_eq!(rec.head_branch, "feat/x");
        assert_eq!(rec.state, "open");
    }

    /// A resumed job's PR is already recorded, so `record_new_pr`
    /// must never run for it - proven end-to-end here by attempting the exact
    /// second insert `process_job` would skip and observing the store's
    /// `UNIQUE(host, repo, pr_number)` reject it, which is why the resume branch
    /// in `process_job` is gated on `resume.is_none()`.
    #[tokio::test]
    async fn record_new_pr_twice_for_the_same_pr_conflicts() {
        let pool = mem_pool().await;
        let runner = Arc::new(StubRunner::new());
        let forge = CliBackend::new(runner);
        let backend = CliBackend::new(Arc::new(StubRunner::new()));

        record_new_pr(&pool, &forge, &backend, "act", &job(), 9, "feat/x").await;
        // A second record for the SAME PR number is exactly what would happen if
        // `process_job` recorded on a resumed run too; `record_new_pr` reports
        // the conflict as a `note` + comment rather than panicking.
        record_new_pr(&pool, &forge, &backend, "act", &job(), 9, "feat/x").await;

        assert_eq!(
            forge.take_notices().len(),
            1,
            "the second, colliding record surfaces as an operator-visible notice"
        );
    }
}
