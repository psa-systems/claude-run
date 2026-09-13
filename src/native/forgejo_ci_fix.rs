//! Native Forgejo CI-fix action handler (CLAUDE-29, supersedes DEV-330).
//!
//! Absorbs the scripted `actions/forgejo-ci-fix.yml` pipeline into typed Rust on
//! the native-action dispatch seam ([`super`]) and the provider abstraction
//! ([`crate::provider`]). It is the second native action after the YouTrack one
//! ([`super::youtrack`]) and follows the same shape. The pipeline is:
//!
//! ```text
//! gather  -> per allow-listed repo: fj pr search (open) + fj pr status (CI);
//!            keep PRs whose combined CI state is `failure` and that were updated
//!            within `window_minutes`. Key = PR head SHA (per-commit attempt cap).
//! filter  -> skip PRs authored by `bot_author` (anti-feedback-loop).
//! pre     -> git clone --branch <head_ref> --depth 50 of the PR branch, using an
//!            authenticated URL from fj's credential store (git stays a subprocess).
//! prompt  -> build the CI-fix instruction text in Rust.
//! claude  -> ClaudeInvoker with the action's claude block + timeout.
//! post    -> decision gate, runs on EVERY path (success, claude error, timeout,
//!            pre failure, cancel):
//!              claude_failed -> comment only, no push
//!              no_changes    -> comment only, no push
//!              too_big       -> comment only, no push (diff > max_files/max_lines)
//!              push          -> commit as the bot identity with the skip marker,
//!                               push the branch, then comment
//!            A PR comment is posted on every outcome path.
//! ```
//!
//! All Forgejo reads (open PRs, head-commit CI status) and the comment write go
//! through the `fj` CLI via the [`provider::Forge`] backend (no raw REST). The
//! provider abstraction admits a future MCP backend behind the same trait.

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{ForgejoCiFixConfig, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::{raw_event_details, ClaudeInvoker, ClaudeResult};
use crate::error::{Error, Result};
use crate::job::{self, EffectiveLimits, ProcessedJob, RunnerDefaults};
use crate::provider::{self, AuditedTracker, Backend, IssueTracker, IssueWriteAudit};
use crate::state;
use crate::workdir;

/// Workdir marker that suppresses attempt counting for this run (parked / cancel).
const SKIP_ATTEMPT_MARKER: &str = ".claude-run-skip-attempt";
/// Workdir file the PR-comment body is written to before `fj pr comment`.
const PR_COMMENT_FILE: &str = ".claude-run-pr-comment.md";
/// Workdir file the commit message is written to before `git commit --file`.
const COMMIT_MSG_FILE: &str = ".claude-run-commit-msg";

/// A typed CI-fix job: one open PR whose head-commit CI is failing.
#[derive(Debug, Clone)]
struct ForgejoCiFixJob {
    /// PR head SHA; also the dedup / attempt key (per-commit attempt cap).
    key: String,
    /// Forgejo host, e.g. `dev.a8n.run`.
    host: String,
    /// Repo path, e.g. `contoso/claude-run`.
    repo: String,
    pr_number: u64,
    /// Head branch name, cloned and pushed.
    branch: String,
    head_sha: String,
    title: String,
    /// PR author login, matched against `bot_author`.
    author: String,
}

/// Built-in Forgejo CI-fix handler (CLAUDE-29). Routed to by [`super::handler_for`].
pub struct ForgejoCiFixHandler;

impl NativeHandler for ForgejoCiFixHandler {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move { run_forgejo_ci_fix(ctx).await })
    }
}

/// Drive a full CI-fix run: caps/budget, gather, then the per-job pipeline,
/// mirroring [`crate::daemon::run_action_inner`]'s run accounting (which native
/// actions bypass) and the structure of [`super::youtrack::run_youtrack`].
async fn run_forgejo_ci_fix(ctx: NativeContext<'_>) -> Result<()> {
    let loaded = ctx.action;
    let action = &loaded.action;
    let pool = ctx.pool;

    // Typed config. Validation guarantees a matching `native` block for
    // `kind: forgejo-ci-fix`, so anything else is a misrouted action.
    let cfg = match &action.native {
        Some(NativeConfig::ForgejoCiFix(cfg)) => (**cfg).clone(),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    // CLAUDE-149: master kill switch (default on). The handler only auto-fixes
    // the bot's own failing PRs; set `enabled: false` to turn it off entirely.
    if !cfg.enabled {
        info!(action = %action.name, "forgejo-ci-fix is disabled; skipping run");
        return Ok(());
    }

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

    // Resolve a provider backend (CLI-first). Errors clearly if yt/fj absent.
    let backend = provider::resolve(provider::DEFAULT_PRIORITY)?;

    // gather (before run_begin: a gather failure is a no-op run).
    // CLAUDE-228: hold the gather marker across the per-repo `fj` scan, which
    // shells out per repo and must not run on the async worker thread.
    let gather_guard = ctx.gather.try_acquire(&action.name);
    let jobs = crate::blocking::in_place(|| gather(backend.as_ref(), &cfg, Utc::now()));
    info!(action = %action.name, count = jobs.len(), "forgejo-ci-fix jobs gathered");

    // CLAUDE-162: admission check. Drop PRs already in flight in another
    // (overlapping) run of this action, then, if nothing fresh remains, return
    // WITHOUT `run_begin` so a re-gather that only re-sees already-claimed work
    // creates no empty run row. The per-PR KeyGate below serializes any PR that a
    // simultaneous re-gather races in before its in-flight marker lands.
    let gathered = jobs.len();
    let inflight: std::collections::HashSet<String> =
        state::action_inflight_keys(pool, &action.name)
            .await?
            .into_iter()
            .collect();
    let jobs: Vec<_> = jobs
        .into_iter()
        .filter(|j| !inflight.contains(&j.key))
        .collect();
    if jobs.is_empty() {
        info!(
            action = %action.name,
            gathered,
            "no fresh queued work (all in flight in another run); skipping empty run"
        );
        return Ok(());
    }
    // CLAUDE-162: this action's per-action cap, enforced daemon-wide.
    let max_parallel = action.concurrency.max_parallel.max(1) as usize;

    // CLAUDE-228: the gather marker covers the whole pre-`run_begin` phase (the
    // work a stacked pickup would redo); released here, so overlapping runs past
    // this point stay allowed exactly as CLAUDE-162 intends.
    drop(gather_guard);
    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
    // CLAUDE-228: from here the row MUST be closed on every exit path, including
    // an early `?` out of the bookkeeping below.
    let run_guard = state::RunGuard::new(pool, run_id);

    // CLAUDE-9: register a cancellation token, unregistered on every exit path.
    let cancel = match ctx.registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard {
        registry: ctx.registry,
        run_id,
    };

    // CLAUDE-234: every tracker write from here on carries this run's id into
    // the journal, and any issue it creates spends the run's ceiling.
    let audit = IssueWriteAudit::new(Some(run_id), limits.max_issues_per_run);
    let tracker = AuditedTracker::new(backend.as_ref(), audit);

    // CLAUDE-1: record planned job keys so the web UI shows what is queued.
    let planned_keys: Vec<String> = jobs.iter().map(|j| j.key.clone()).collect();
    if let Err(e) = state::run_set_job_keys(pool, run_id, &planned_keys).await {
        warn!(error = %e, "run_set_job_keys failed (non-fatal)");
    }

    // CLAUDE-223: run-scoped global-pause gate, checked alongside cancel at both
    // per-job checkpoints so a mid-run pause stops the next PR pickup.
    let run_pause = ctx.pause.for_action_run(&action.name);
    let mut processed: u32 = 0;
    for (idx, job) in jobs.iter().enumerate() {
        if cancel.is_cancelled() {
            info!(action = %action.name, "run cancelled, stopping before next job");
            break;
        }
        if run_pause.stop().await {
            break;
        }
        let current = state::daily_count_get(pool, &action.name).await?;
        if current >= limits.max_runs_per_day {
            warn!(count = current, "daily cap reached mid-run, stopping");
            break;
        }
        let cost = state::daily_cost_get(pool, &action.name).await?;
        if cost >= limits.daily_budget_usd {
            warn!(cost_usd = cost, "daily budget reached mid-run, stopping");
            break;
        }
        // DEV-705: the run's own ceiling, checked before every pickup.
        if ctx.spend.exceeded() {
            warn!(action = %action.name, "{}", ctx.spend.stop_reason());
            break;
        }
        // CLAUDE-162: acquire the daemon-wide caps (global -> per-action) and the
        // per-PR key gate before working the job, so overlapping forgejo-ci-fix
        // runs stay bounded by the global cap and never work the same PR at once.
        // Both permits and the key guard release at the end of this iteration.
        let _permits = ctx.limiter.acquire(&action.name, max_parallel).await;
        let _key_guard = ctx.keygate.acquire(&job.key).await;
        if cancel.is_cancelled() || run_pause.stop().await {
            break;
        }
        // CLAUDE-14: exactly one job shows as "running" at a time.
        if let Err(e) = state::run_set_current_job(pool, run_id, Some(&planned_keys[idx])).await {
            warn!(error = %e, "run_set_current_job failed (non-fatal)");
        }
        // CLAUDE-162: mark this PR in flight so an overlapping run's admission
        // check dedups against it.
        // CLAUDE-163: one job per PR key, so the concurrency key equals the job
        // key here; the sequential `Limiter::acquire` path is otherwise unchanged.
        if let Err(e) =
            state::run_inflight_add(pool, run_id, &planned_keys[idx], &planned_keys[idx]).await
        {
            warn!(error = %e, "run_inflight_add failed (non-fatal)");
        }
        let outcome = process_job(
            &ctx,
            backend.as_ref(),
            &tracker,
            &cfg,
            &limits,
            &cancel,
            &run_pause,
            job,
        )
        .await;
        if let Err(e) = state::run_inflight_remove(pool, run_id, &planned_keys[idx]).await {
            warn!(error = %e, "run_inflight_remove failed (non-fatal)");
        }
        let outcome = outcome?;
        record_outcome(&ctx, run_id, &outcome).await?;
        processed += 1;
    }

    // DEV-705: an over-budget stop is this run's outcome unless a cancel or a
    // pause already claimed it.
    let exit_code = ctx
        .spend
        .exit_code(run_pause.exit_code(cancel.is_cancelled()));
    run_guard.finish(exit_code, processed).await?;
    // DEV-707: one summary line and one machine-readable record per run.
    ctx.spend.finish(Some(run_id), exit_code);
    info!(action = %action.name, processed, exit_code, "run complete");
    Ok(())
}

/// gather: per allow-listed repo, list open PRs and keep those updated within
/// `window_minutes` whose head-commit combined CI state is `failure`. Per-repo
/// and per-PR errors are logged and skipped (one repo's outage cannot abort the
/// scan), mirroring the scripted action's `try { ... } catch { [] }`.
fn gather(
    backend: &dyn Backend,
    cfg: &ForgejoCiFixConfig,
    now: DateTime<Utc>,
) -> Vec<ForgejoCiFixJob> {
    let cutoff = now - chrono::Duration::minutes(cfg.window_minutes as i64);
    let mut jobs = Vec::new();
    for repo in &cfg.repos {
        let prs = match backend.list_open_prs(&cfg.host, repo) {
            Ok(p) => p,
            Err(e) => {
                warn!(repo = %repo, error = %e, "listing open PRs failed; skipping repo");
                continue;
            }
        };
        for pr in prs {
            // Window first (cheap) so stale PRs cost no CI-status call.
            if !within_window(&pr.updated_at, cutoff) {
                continue;
            }
            let ci_state = match backend.pr_ci_state(&cfg.host, repo, pr.number) {
                Ok(s) => s,
                Err(e) => {
                    warn!(repo = %repo, pr = pr.number, error = %e, "reading CI status failed; skipping PR");
                    continue;
                }
            };
            if ci_state != "failure" {
                continue;
            }
            jobs.push(ForgejoCiFixJob {
                key: pr.head_sha.clone(),
                host: cfg.host.clone(),
                repo: repo.clone(),
                pr_number: pr.number,
                branch: pr.head_ref,
                head_sha: pr.head_sha,
                title: pr.title,
                author: pr.author,
            });
        }
    }
    jobs
}

/// True when `updated_at` (RFC 3339) is at or after `cutoff`. A PR whose
/// timestamp fails to parse is excluded (false), matching the scripted action's
/// `try { into datetime } catch { return null }`.
fn within_window(updated_at: &str, cutoff: DateTime<Utc>) -> bool {
    match DateTime::parse_from_rfc3339(updated_at) {
        Ok(dt) => dt.with_timezone(&Utc) >= cutoff,
        Err(_) => false,
    }
}

/// Process a single PR through filter / pre / prompt / claude / finalize,
/// returning the outcome record. Mirrors [`super::youtrack::process_job`]:
/// finalize (the PR comment, and the push when chosen) runs on every path that
/// reached the workdir, transient claude errors skip finalize + attempt
/// counting, the skip marker (or a cancel) suppresses attempt counting, and the
/// daily ledger advances only when claude actually ran.
#[allow(clippy::too_many_arguments)]
async fn process_job(
    ctx: &NativeContext<'_>,
    backend: &dyn Backend,
    tracker: &dyn IssueTracker,
    cfg: &ForgejoCiFixConfig,
    limits: &EffectiveLimits,
    cancel: &CancellationToken,
    pause: &crate::pause::RunPause,
    job: &ForgejoCiFixJob,
) -> Result<ProcessedJob> {
    let started = Instant::now();
    let action = &ctx.action.action;
    let key = &job.key;

    // ---- filter: only the bot's OWN failing PRs are auto-fixed (CLAUDE-149) ----
    // A human's failing PR is left to its author; the bot never pushes a
    // corrective commit onto someone else's branch. Only PRs authored by
    // `bot_author` (the PRs the bot opened working a queued issue) are worked.
    if !is_own_pr(&job.author, &cfg.bot_author) {
        info!(action = %action.name, key = %key, pr = job.pr_number, author = %job.author, "skipping PR not authored by the bot");
        return Ok(ProcessedJob::Filtered { key: key.clone() });
    }

    // ---- per-PR ceiling (CLAUDE-149) ----
    // The bot keeps pushing corrective commits to its own PR until CI passes.
    // The per-commit cap below resets on every new head SHA, so a separate
    // PR-scoped counter bounds the iteration: after `max_pr_attempts` distinct
    // fix pushes on this PR, stop auto-fixing and flag it Needs Review.
    let pr_key = format!("pr:{}#{}", job.repo, job.pr_number);
    let pr_attempts = state::attempts_get(ctx.pool, &action.name, &pr_key).await?;
    if pr_ceiling_reached(pr_attempts, cfg.max_pr_attempts) {
        // Flag Needs Review exactly once per PR. The comment / label bump the
        // PR's updated_at, so it stays in the gather window and is re-picked every
        // tick; a separate marker stops the re-flag from re-commenting each time.
        let review_key = format!("review:{}#{}", job.repo, job.pr_number);
        if state::attempts_get(ctx.pool, &action.name, &review_key).await? == 0 {
            info!(action = %action.name, key = %key, pr = job.pr_number, pr_attempts, "per-PR fix ceiling reached; flagging Needs Review");
            flag_needs_review(ctx, backend, tracker, cfg, job).await;
            state::attempts_increment(ctx.pool, &action.name, &review_key).await?;
        } else {
            info!(action = %action.name, key = %key, pr = job.pr_number, "per-PR ceiling already flagged Needs Review; not re-flagging");
        }
        return Ok(ProcessedJob::NeedsReview { key: key.clone() });
    }

    let attempts = state::attempts_get(ctx.pool, &action.name, key).await?;
    // CI-fix jobs always carry a stable key (the head SHA), so the per-commit
    // attempt cap always applies.
    // CLAUDE-232: `warn`, not `info`: nothing else records the skip, and the
    // failing commit stays a candidate until someone looks.
    if attempts >= limits.max_attempts_per_key {
        warn!(
            action = %action.name, key = %key, attempts,
            cap = limits.max_attempts_per_key,
            "skipping job, attempt cap reached"
        );
        return Ok(ProcessedJob::CapHit { key: key.clone() });
    }

    info!(action = %action.name, key = %key, pr = job.pr_number, attempt = attempts + 1, "processing job");

    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), ctx.paths);
    let workdir_path = workdir::job_workdir(&wb_base, &action.name, key);
    std::fs::create_dir_all(&workdir_path)?;

    // ---- pre: clone the PR branch ----
    // CLAUDE-228: a full `git clone`, synchronous; keep it off the async worker.
    let pre_result = crate::blocking::in_place(|| run_pre(backend, job, &workdir_path));

    // ---- prompt + claude (skipped on pre failure / cancel / global pause) ----
    // CLAUDE-223: the gate is read once here (not in a match guard, which cannot
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
            let prompt = build_prompt(job);
            let stream_log = ClaudeInvoker::stream_log_path(
                &ctx.paths.logs_dir(),
                &action.name,
                key,
                attempts + 1,
            );
            info!(action = %action.name, key = %key, stream_log = %stream_log.display(), "invoking claude");
            let r = ctx.claude.invoke(
                &prompt,
                &action.claude,
                limits.claude_timeout,
                &workdir_path,
                &stream_log,
                cancel,
                // DEV-705: cap the child at the run's remaining ceiling.
                Some(ctx.spend.remaining_usd()),
            )?;
            // DEV-707: bill the run before any branch discards `r`.
            ctx.spend.record(&r);
            if cancel.is_cancelled() {
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
        workdir::cleanup_or_preserve(&workdir_path, &action.name, key);
        return Ok(ProcessedJob::Transient { key: key.clone() });
    }

    if claude_ran {
        state::daily_cost_add(ctx.pool, &action.name, claude_result.cost).await?;
    }

    // ---- finalize (the native "post"): decision gate + guaranteed comment ----
    // CLAUDE-228: git commit / push plus the forge comment, all synchronous.
    let finalize_res =
        crate::blocking::in_place(|| finalize(backend, cfg, job, &workdir_path, &claude_result));
    let post_failed = match &finalize_res {
        Ok(f) => {
            info!(action = %action.name, key = %key, pr = job.pr_number, outcome = %f.as_str(), "finalized");
            f.push_failed
        }
        Err(e) => {
            warn!(action = %action.name, key = %key, error = %e, "finalize failed");
            true
        }
    };

    // CLAUDE-149: a successful fix push counts toward the per-PR ceiling. Only an
    // actual push (within the diff caps, not failed) advances it, so no-change /
    // too-big / claude-failed rounds do not burn the budget.
    let pushed = matches!(
        &finalize_res,
        Ok(f) if matches!(f.decision, Decision::Push { .. }) && !f.push_failed
    );
    if pushed {
        state::attempts_increment(ctx.pool, &action.name, &pr_key).await?;
    }

    // ---- skip-attempt marker (parked), cancel, or global pause (CLAUDE-223) ----
    let skip_marker = workdir_path.join(SKIP_ATTEMPT_MARKER);
    let cancelled = cancel.is_cancelled();
    let skip_attempt = skip_marker.exists() || cancelled || paused_before_claude;
    if skip_attempt {
        info!(action = %action.name, key = %key, cancelled, paused = paused_before_claude, "not counting attempt");
    }

    let ok = claude_result.ok && !post_failed;
    let stream_log_path = if claude_ran {
        Some(
            ClaudeInvoker::stream_log_path(&ctx.paths.logs_dir(), &action.name, key, attempts + 1)
                .display()
                .to_string(),
        )
    } else {
        None
    };

    if !skip_attempt {
        state::attempts_increment(ctx.pool, &action.name, key).await?;
    }
    if claude_ran {
        state::daily_count_increment(ctx.pool, &action.name).await?;
    }

    workdir::cleanup_or_preserve(&workdir_path, &action.name, key);

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

/// Pre step: clone the PR branch into `workdir`, authenticating via fj's stored
/// token (the clone stays a git subprocess). Returns the failure reason on error
/// so the caller can synthesize a failed claude result and still finalize.
fn run_pre(
    backend: &dyn Backend,
    job: &ForgejoCiFixJob,
    workdir: &Path,
) -> std::result::Result<(), String> {
    let url = backend
        .clone_url(&job.host, &job.repo)
        .map_err(|e| format!("resolve clone url: {e}"))?;
    clone_branch(&url, &job.branch, workdir).map_err(|e| format!("git clone: {e}"))?;
    Ok(())
}

/// Shallow-clone `branch` of `clone_url` (token-authenticated) into `workdir`
/// (already created, empty). CLAUDE-252: the shared helper reports git's own
/// reason for a failure with the URL's credential redacted, instead of the bare
/// exit code this used to carry.
fn clone_branch(clone_url: &str, branch: &str, workdir: &Path) -> Result<()> {
    provider::shallow_clone(clone_url, Some(branch), workdir, 50)
}

/// Build the agent's instruction text. Local-only work: investigate and fix the
/// failing CI, make no other changes. The runner owns the commit / push / comment
/// in [`finalize`]. Ported from the scripted `actions/forgejo-ci-fix.yml` prompt.
fn build_prompt(job: &ForgejoCiFixJob) -> String {
    [
        "You are fixing a failing CI build on a pull request.".to_string(),
        String::new(),
        "The repository is checked out in the current working directory.".to_string(),
        "The CI workflow files are in .forgejo/workflows/.".to_string(),
        String::new(),
        format!("PR: #{}: {}", job.pr_number, job.title),
        format!("Branch: {}", job.branch),
        format!("Commit: {}", job.head_sha),
        String::new(),
        "Your job:".to_string(),
        "  1. Read the CI workflow files to understand what checks run".to_string(),
        "  2. Investigate the code to find the cause of the failure".to_string(),
        "  3. Make the MINIMUM change needed to fix it".to_string(),
        "  4. Do NOT weaken tests, add retries to flaky tests, or skip checks".to_string(),
        "  5. If the failure is something you cannot safely fix (flaky test, infra issue, ambiguous requirement), explain why and make no changes".to_string(),
        String::new(),
        "Do ONLY local engineering work. Do NOT commit, push, or open/modify the PR; the runner commits and pushes your changes and comments on the PR after you exit.".to_string(),
        String::new(),
        "When you are done, summarize what you changed and why in plain text.".to_string(),
    ]
    .join("\n")
}

/// The decision gate outcome (the native "post" branch). Computed from the
/// claude result and the working-tree diff size.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// Claude errored / timed out: comment only, no push.
    ClaudeFailed,
    /// No diff, or claude reported it made no changes: comment only, no push.
    NoChanges,
    /// Diff exceeds `max_files` / `max_lines`: comment only, no push.
    TooBig { files: u64, lines: u64 },
    /// Within the caps: commit + push, then comment.
    Push { files: u64, lines: u64 },
}

impl Decision {
    fn as_str(&self) -> &'static str {
        match self {
            Decision::ClaudeFailed => "claude_failed",
            Decision::NoChanges => "no_changes",
            Decision::TooBig { .. } => "too_big",
            Decision::Push { .. } => "push",
        }
    }
}

/// Classify a run into a [`Decision`]. Pure so the gate is unit-testable without
/// a workdir. `claude_ok` false short-circuits to `ClaudeFailed`; otherwise an
/// empty diff or a claude summary saying it made no changes is `no_changes`, a
/// diff over either cap is `too_big`, and anything else is `push`.
fn decide(
    claude_ok: bool,
    summary: &str,
    files: u64,
    lines: u64,
    max_files: u64,
    max_lines: u64,
) -> Decision {
    if !claude_ok {
        return Decision::ClaudeFailed;
    }
    let s = summary.to_lowercase();
    let says_no = s.contains("no changes")
        || s.contains("making no changes")
        || s.contains("changes made: none");
    if files == 0 || says_no {
        Decision::NoChanges
    } else if files > max_files || lines > max_lines {
        Decision::TooBig { files, lines }
    } else {
        Decision::Push { files, lines }
    }
}

/// Sum `git diff --numstat` into `(files, lines_changed)`. Pure parser; a binary
/// file (numstat prints `-\t-`) counts as a changed file with zero lines.
fn parse_numstat(stdout: &str) -> (u64, u64) {
    let mut files = 0u64;
    let mut lines = 0u64;
    for raw in stdout.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        files += 1;
        let mut cols = line.split('\t');
        let added = cols
            .next()
            .and_then(|c| c.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let deleted = cols
            .next()
            .and_then(|c| c.trim().parse::<u64>().ok())
            .unwrap_or(0);
        lines += added + deleted;
    }
    (files, lines)
}

/// Working-tree diff size of `workdir` via `git diff --numstat`. A non-git dir
/// or a git failure yields `(0, 0)` (treated as no changes), matching the
/// scripted action's behavior.
fn diff_stats(workdir: &Path) -> (u64, u64) {
    match Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("diff")
        .arg("--numstat")
        .output()
    {
        Ok(out) if out.status.success() => parse_numstat(&String::from_utf8_lossy(&out.stdout)),
        _ => (0, 0),
    }
}

/// Result of [`finalize`]: the decision taken and whether a chosen push failed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Finalized {
    decision: Decision,
    push_failed: bool,
}

impl Finalized {
    fn as_str(&self) -> &'static str {
        if self.push_failed {
            "push_failed"
        } else {
            self.decision.as_str()
        }
    }
}

/// The finalize state machine (the native "post"). Runs on every path. Computes
/// the decision, performs the commit + push when chosen, then posts a PR comment
/// on every outcome path (best-effort, like the youtrack finalize comment: a
/// comment failure is logged, never strands the run).
fn finalize(
    backend: &dyn Backend,
    cfg: &ForgejoCiFixConfig,
    job: &ForgejoCiFixJob,
    workdir: &Path,
    claude_result: &ClaudeResult,
) -> Result<Finalized> {
    let decision = if !claude_result.ok {
        Decision::ClaudeFailed
    } else {
        let (files, lines) = diff_stats(workdir);
        decide(
            true,
            &claude_result.summary,
            files,
            lines,
            cfg.max_files,
            cfg.max_lines,
        )
    };

    let mut push_err: Option<String> = None;
    if matches!(decision, Decision::Push { .. }) {
        if let Err(e) = push_fix(backend, cfg, job, workdir, claude_result) {
            warn!(key = %job.key, pr = job.pr_number, error = %e, "push failed (commenting failure)");
            push_err = Some(e.to_string());
        }
    }

    // Comment on every path. Best-effort: a comment failure is logged, not fatal.
    let body = comment_body(&decision, claude_result, push_err.as_deref());
    let body_file = workdir.join(PR_COMMENT_FILE);
    match std::fs::write(&body_file, body) {
        Ok(()) => {
            if let Err(e) = backend.pr_comment(&job.host, &job.repo, job.pr_number, &body_file) {
                warn!(key = %job.key, pr = job.pr_number, error = %e, "posting PR comment failed (continuing)");
            }
        }
        Err(e) => {
            warn!(key = %job.key, pr = job.pr_number, error = %e, "writing PR comment body failed (skipping comment)");
        }
    }

    Ok(Finalized {
        decision,
        push_failed: push_err.is_some(),
    })
}

/// Commit the working tree as the bot identity (with the skip marker in the
/// message) and push the PR branch. Auth comes from fj's credential store via
/// [`provider::Forge::clone_url`]; the push stays a git subprocess.
fn push_fix(
    backend: &dyn Backend,
    cfg: &ForgejoCiFixConfig,
    job: &ForgejoCiFixJob,
    workdir: &Path,
    claude_result: &ClaudeResult,
) -> Result<()> {
    git_in(workdir, &["config", "user.name", &cfg.bot_author])?;
    git_in(workdir, &["config", "user.email", &cfg.bot_email])?;
    git_in(workdir, &["add", "-A"])?;

    let msg = commit_message(job, &claude_result.summary, &cfg.skip_marker);
    let msg_file = workdir.join(COMMIT_MSG_FILE);
    std::fs::write(&msg_file, msg)?;
    let msg_arg = msg_file.to_string_lossy();
    git_in(workdir, &["commit", "--file", &msg_arg])?;

    let push_url = backend.clone_url(&job.host, &job.repo)?;
    git_push(workdir, &push_url, &job.branch)
}

/// Run `git -C <workdir> <args...>`, erroring on a non-zero exit. The args carry
/// no secrets (config / add / commit), so the failed-command echo is safe.
fn git_in(workdir: &Path, args: &[&str]) -> Result<()> {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(args)
        .output()?;
    if out.status.success() {
        Ok(())
    } else {
        Err(provider::git_error(&args.join(" "), &out, None))
    }
}

/// Push `branch` to `url` from `workdir`. Kept separate from [`git_in`] so the
/// token-bearing URL never appears in an error message.
fn git_push(workdir: &Path, url: &str, branch: &str) -> Result<()> {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .arg("push")
        .arg(url)
        .arg(branch)
        .output()?;
    if out.status.success() {
        Ok(())
    } else {
        // CLAUDE-252: a rejected push (non-fast-forward, protected branch, a
        // token without write scope) said only `exited 1` before this.
        Err(provider::git_error("push", &out, Some(url)))
    }
}

/// The fix commit message: a subject, the agent's summary, and the skip marker
/// (so the bot's own push does not re-trigger this action).
fn commit_message(job: &ForgejoCiFixJob, summary: &str, skip_marker: &str) -> String {
    [
        format!("fix: Claude auto-fix for PR #{}", job.pr_number),
        String::new(),
        summary.to_string(),
        String::new(),
        skip_marker.to_string(),
    ]
    .join("\n")
}

/// The PR comment body for each outcome. Ported from the scripted action's
/// final `post` step; the push branch also reports a push failure when one
/// occurred so a failed push never reads as a successful fix.
fn comment_body(decision: &Decision, claude: &ClaudeResult, push_err: Option<&str>) -> String {
    match decision {
        Decision::ClaudeFailed => {
            let mut body = [
                format!("**Claude run FAILED ({}).**", claude.error),
                String::new(),
                claude.summary.clone(),
                String::new(),
                "No changes were pushed. See the per-attempt stream log on the runner host for full context.".to_string(),
            ]
            .join("\n");
            body.push_str(&raw_event_details(&claude.raw));
            body
        }
        Decision::NoChanges => [
            "**Claude ran but made no changes.**".to_string(),
            String::new(),
            claude.summary.clone(),
        ]
        .join("\n"),
        Decision::TooBig { files, lines } => [
            "**Claude attempted a fix but the diff is too large for unattended push.**".to_string(),
            String::new(),
            format!("Files: {files}, Lines: {lines}"),
            String::new(),
            "Summary:".to_string(),
            claude.summary.clone(),
            String::new(),
            "This needs human review.".to_string(),
        ]
        .join("\n"),
        Decision::Push { .. } => match push_err {
            Some(err) => [
                "**Claude attempted a fix but the push FAILED.**".to_string(),
                String::new(),
                claude.summary.clone(),
                String::new(),
                "Error:".to_string(),
                err.to_string(),
                String::new(),
                "No changes reached the PR; this needs human review.".to_string(),
            ]
            .join("\n"),
            None => [
                "**Claude pushed a fix.**".to_string(),
                String::new(),
                claude.summary.clone(),
                String::new(),
                "CI will re-run on the new commit. Review the diff before merging.".to_string(),
            ]
            .join("\n"),
        },
    }
}

/// Map a `ProcessedJob::Done` into the JSONL record + `job_outcomes` insert,
/// identical to the scripted and youtrack paths.
async fn record_outcome(
    ctx: &NativeContext<'_>,
    run_id: i64,
    outcome: &ProcessedJob,
) -> Result<()> {
    let loaded = ctx.action;
    let Some(record) = job::done_record(outcome) else {
        return Ok(());
    };
    job::append_output_record(ctx.paths, loaded, &record);
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
            ctx.pool,
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

/// CLAUDE-149: a PR is auto-fixed only when the bot authored it; any other
/// author's PR is left to its author.
fn is_own_pr(author: &str, bot_author: &str) -> bool {
    author == bot_author
}

/// CLAUDE-149: true once the PR has already had `max_pr_attempts` distinct fix
/// pushes, at which point it is flagged Needs Review instead of fixed again.
fn pr_ceiling_reached(pr_attempts: u32, max_pr_attempts: u64) -> bool {
    u64::from(pr_attempts) >= max_pr_attempts
}

/// CLAUDE-149: flag a PR Needs Review once the per-PR fix ceiling is reached.
/// Three best-effort steps (a failure in one is logged and does not block the
/// others): post a PR comment, add the `needs_review_label` Forgejo label, and
/// set the source YouTrack issue (parsed from the PR body) to
/// `needs_review_state`, optionally assigning `review_assignee`. The source-issue
/// step is skipped silently when the PR body references no issue.
async fn flag_needs_review(
    ctx: &NativeContext<'_>,
    backend: &dyn Backend,
    tracker: &dyn IssueTracker,
    cfg: &ForgejoCiFixConfig,
    job: &ForgejoCiFixJob,
) {
    let action = &ctx.action.action;

    // 1. PR comment (best-effort), body written to a short-lived workdir.
    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), ctx.paths);
    let workdir_path = workdir::job_workdir(&wb_base, &action.name, &format!("review-{}", job.key));
    match std::fs::create_dir_all(&workdir_path) {
        Ok(()) => {
            let body = needs_review_comment(cfg.max_pr_attempts, job);
            let body_file = workdir_path.join(PR_COMMENT_FILE);
            match std::fs::write(&body_file, body) {
                Ok(()) => {
                    if let Err(e) =
                        backend.pr_comment(&job.host, &job.repo, job.pr_number, &body_file)
                    {
                        warn!(pr = job.pr_number, error = %e, "needs-review: PR comment failed (continuing)");
                    }
                }
                Err(e) => {
                    warn!(pr = job.pr_number, error = %e, "needs-review: writing comment body failed (skipping comment)")
                }
            }
            workdir::cleanup_or_preserve(&workdir_path, &ctx.action.action.name, &job.key);
        }
        Err(e) => {
            warn!(pr = job.pr_number, error = %e, "needs-review: workdir create failed (skipping comment)")
        }
    }

    // 2. PR label (best-effort).
    if let Err(e) = backend.pr_add_labels(
        &job.host,
        &job.repo,
        job.pr_number,
        std::slice::from_ref(&cfg.needs_review_label),
    ) {
        warn!(pr = job.pr_number, label = %cfg.needs_review_label, error = %e, "needs-review: adding label failed (continuing)");
    }

    // 3. Source issue (best-effort): set State and optionally assign a human.
    match backend.pr_referenced_issues(&job.host, &job.repo, job.pr_number) {
        Ok(keys) => {
            for issue in keys {
                if let Err(e) = tracker.set_state(&issue, &cfg.needs_review_state) {
                    warn!(issue = %issue, error = %e, "needs-review: set_state failed (continuing)");
                }
                if let Some(assignee) = &cfg.review_assignee {
                    if let Err(e) = tracker.set_assignee(&issue, assignee) {
                        warn!(issue = %issue, error = %e, "needs-review: set_assignee failed (continuing)");
                    }
                }
            }
        }
        Err(e) => {
            warn!(pr = job.pr_number, error = %e, "needs-review: reading referenced issues failed (continuing)")
        }
    }
}

/// The PR comment posted when the per-PR fix ceiling is reached (CLAUDE-149).
fn needs_review_comment(max_pr_attempts: u64, job: &ForgejoCiFixJob) -> String {
    [
        format!(
            "**Auto-fix flagged for review.** Claude pushed corrective commits to this PR {max_pr_attempts} time(s) without bringing CI green, so it has stopped auto-fixing and flagged the PR for human review."
        ),
        String::new(),
        format!("PR #{} ({})", job.pr_number, job.branch),
        "A human should take it from here.".to_string(),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;
    use crate::provider::CliBackend;
    use serde_json::Value;
    use std::sync::Arc;

    fn cfg() -> ForgejoCiFixConfig {
        ForgejoCiFixConfig {
            repos: vec!["contoso/claude-run".to_string()],
            ..ForgejoCiFixConfig::default()
        }
    }

    fn job() -> ForgejoCiFixJob {
        ForgejoCiFixJob {
            key: "abc123".into(),
            host: "dev.a8n.run".into(),
            repo: "contoso/claude-run".into(),
            pr_number: 52,
            branch: "feat/x".into(),
            head_sha: "abc123".into(),
            title: "feat: x".into(),
            author: "David".into(),
        }
    }

    fn ok_result(summary: &str) -> ClaudeResult {
        ClaudeResult {
            ok: true,
            summary: summary.into(),
            cost: 0.5,
            usage: Default::default(),
            raw: Value::Null,
            error: String::new(),
            transient: false,
            timed_out: false,
        }
    }

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    // ---- CLAUDE-149: bot-own-PR targeting, ceiling, Needs Review ----------

    #[test]
    fn config_defaults_to_enabled_with_pr_ceiling() {
        // The handler is on by default and only auto-fixes the bot's own PRs.
        let d = ForgejoCiFixConfig::default();
        assert!(d.enabled);
        assert_eq!(d.max_pr_attempts, 3);
        assert_eq!(d.needs_review_label, "needs-review");
        assert_eq!(d.needs_review_state, "Needs Review");
        assert!(d.review_assignee.is_none());
    }

    #[test]
    fn only_the_bots_own_prs_are_fixed() {
        // CLAUDE-149: the filter is inverted vs the old anti-feedback-loop skip.
        // The bot's own PR is worked; a human's PR is left to its author.
        assert!(is_own_pr("claude-run", "claude-run"));
        assert!(!is_own_pr("alice", "claude-run"));
    }

    #[test]
    fn pr_ceiling_triggers_at_max_pr_attempts() {
        // Below the ceiling keep fixing; at or above it, flag Needs Review.
        assert!(!pr_ceiling_reached(0, 3));
        assert!(!pr_ceiling_reached(2, 3));
        assert!(pr_ceiling_reached(3, 3));
        assert!(pr_ceiling_reached(4, 3));
    }

    #[test]
    fn needs_review_comment_names_attempts_and_pr() {
        let body = needs_review_comment(3, &job());
        assert!(body.contains("3 time(s)"), "{body}");
        assert!(body.contains("PR #52"), "{body}");
        assert!(body.to_lowercase().contains("human review"), "{body}");
    }

    // ---- decision gate (AC: decision-gate coverage) -----------------------

    #[test]
    fn decide_claude_failed_short_circuits() {
        // A failed claude run is claude_failed regardless of the diff.
        assert_eq!(
            decide(false, "did stuff", 3, 50, 10, 300),
            Decision::ClaudeFailed
        );
    }

    #[test]
    fn decide_no_changes_on_empty_diff_or_summary() {
        assert_eq!(decide(true, "fixed it", 0, 0, 10, 300), Decision::NoChanges);
        // Summary phrasings the scripted action recognised as "no changes".
        assert_eq!(
            decide(true, "Making no changes here.", 2, 4, 10, 300),
            Decision::NoChanges
        );
        assert_eq!(
            decide(true, "Changes made: none", 2, 4, 10, 300),
            Decision::NoChanges
        );
        assert_eq!(
            decide(true, "There are NO CHANGES to make", 5, 5, 10, 300),
            Decision::NoChanges
        );
    }

    #[test]
    fn decide_too_big_over_either_cap() {
        // Over the file cap.
        assert_eq!(
            decide(true, "big", 11, 50, 10, 300),
            Decision::TooBig {
                files: 11,
                lines: 50
            }
        );
        // Over the line cap.
        assert_eq!(
            decide(true, "big", 3, 301, 10, 300),
            Decision::TooBig {
                files: 3,
                lines: 301
            }
        );
    }

    #[test]
    fn decide_push_within_caps() {
        assert_eq!(
            decide(true, "fixed the lint", 2, 40, 10, 300),
            Decision::Push {
                files: 2,
                lines: 40
            }
        );
        // Exactly at the caps is still a push (caps are exclusive).
        assert_eq!(
            decide(true, "fixed", 10, 300, 10, 300),
            Decision::Push {
                files: 10,
                lines: 300
            }
        );
    }

    #[test]
    fn parse_numstat_sums_files_and_lines() {
        let out = "3\t1\tsrc/a.rs\n0\t5\tsrc/b.rs\n";
        assert_eq!(parse_numstat(out), (2, 9));
        // Binary files print `-\t-`; counted as a file, zero lines.
        assert_eq!(parse_numstat("-\t-\tlogo.png\n"), (1, 0));
        // Blank lines ignored; empty input is no changes.
        assert_eq!(parse_numstat("\n\n"), (0, 0));
        assert_eq!(parse_numstat(""), (0, 0));
    }

    #[test]
    fn within_window_includes_recent_excludes_stale_and_unparsable() {
        let cutoff = at("2026-05-29T11:00:00Z");
        assert!(within_window("2026-05-29T11:10:00Z", cutoff));
        assert!(within_window("2026-05-29T11:00:00Z", cutoff)); // boundary is inclusive
        assert!(!within_window("2026-05-29T10:00:00Z", cutoff));
        assert!(!within_window("not a date", cutoff));
    }

    #[test]
    fn commit_message_carries_subject_summary_and_skip_marker() {
        let msg = commit_message(&job(), "fixed the clippy lint", "[skip claude]");
        assert_eq!(
            msg,
            "fix: Claude auto-fix for PR #52\n\nfixed the clippy lint\n\n[skip claude]"
        );
    }

    #[test]
    fn comment_body_push_failure_does_not_read_as_success() {
        let pushed = comment_body(
            &Decision::Push { files: 1, lines: 2 },
            &ok_result("did it"),
            None,
        );
        assert!(pushed.contains("Claude pushed a fix."));
        let failed = comment_body(
            &Decision::Push { files: 1, lines: 2 },
            &ok_result("did it"),
            Some("git push exited 1"),
        );
        assert!(failed.contains("push FAILED"));
        assert!(failed.contains("git push exited 1"));
        assert!(!failed.contains("Claude pushed a fix."));
    }

    /// CLAUDE-56: the ClaudeFailed arm leads with the friendly summary and
    /// appends the pretty-printed event in a collapsed `<details>` block when
    /// `raw` is populated; a synthesized result omits the block.
    #[test]
    fn comment_body_claude_failed_collapses_raw() {
        let mut failed = ok_result("API Error: 529 Overloaded.");
        failed.ok = false;
        failed.error = "claude exited 1, see stream.jsonl".into();
        failed.raw = serde_json::json!({"type":"result","is_error":true,"api_error_status":"529"});
        let body = comment_body(&Decision::ClaudeFailed, &failed, None);
        assert!(body.contains("**Claude run FAILED (claude exited 1, see stream.jsonl).**"));
        assert!(body.contains("API Error: 529 Overloaded."));
        assert!(!body.contains("Summary / stream log tail"));
        assert!(body.contains("<details>\n<summary>Raw result event</summary>"));
        assert!(body.contains("\"api_error_status\": \"529\""));

        let synth = ClaudeResult::synthesized("claude exited 1", "pre step failed");
        let body = comment_body(&Decision::ClaudeFailed, &synth, None);
        assert!(body.contains("pre step failed"));
        assert!(!body.contains("<details>"));
    }

    // ---- gather -----------------------------------------------------------

    #[test]
    fn gather_keeps_failing_in_window_prs_only() {
        let search = r#"[
            {"number":1,"title":"failing recent","head_sha":"sha1","head_ref":"b1","author":"alice","html_url":"u1","updated_at":"2026-05-29T11:10:00Z"},
            {"number":2,"title":"green recent","head_sha":"sha2","head_ref":"b2","author":"bob","html_url":"u2","updated_at":"2026-05-29T11:15:00Z"},
            {"number":3,"title":"failing but stale","head_sha":"sha3","head_ref":"b3","author":"carol","html_url":"u3","updated_at":"2026-05-29T09:00:00Z"}
        ]"#;
        let runner = StubRunner::new()
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
                search,
            )
            // PR 3 is stale (outside the 45m window from 11:30) -> no status call.
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "pr",
                    "status",
                    "contoso/claude-run#1",
                    "--json",
                ],
                r#"{"pr":1,"state":"failure"}"#,
            )
            .ok(
                "fj",
                &[
                    "--host",
                    "dev.a8n.run",
                    "pr",
                    "status",
                    "contoso/claude-run#2",
                    "--json",
                ],
                r#"{"pr":2,"state":"success"}"#,
            );
        let backend = CliBackend::new(Arc::new(runner));
        let jobs = gather(&backend, &cfg(), at("2026-05-29T11:30:00Z"));
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].pr_number, 1);
        assert_eq!(jobs[0].key, "sha1");
        assert_eq!(jobs[0].branch, "b1");
        assert_eq!(jobs[0].author, "alice");
    }

    #[test]
    fn gather_skips_repo_on_list_failure() {
        // The one repo's PR listing fails -> no jobs, no panic.
        let runner = StubRunner::new().fail(
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
            "boom",
        );
        let backend = CliBackend::new(Arc::new(runner));
        assert!(gather(&backend, &cfg(), at("2026-05-29T11:30:00Z")).is_empty());
    }

    // ---- finalize: comment posted on every path ---------------------------

    /// A failed claude run comments and does not push.
    #[test]
    fn finalize_claude_failed_comments_without_push() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        let body_path = wd.join(PR_COMMENT_FILE);
        let runner = Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "comment",
                "contoso/claude-run#52",
                "--body-file",
                body_path.to_str().unwrap(),
            ],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let claude = ClaudeResult::synthesized("claude exited 1", "error tail");
        let res = finalize(&backend, &cfg(), &job(), wd, &claude).unwrap();
        assert_eq!(res.decision, Decision::ClaudeFailed);
        assert!(!res.push_failed);
        let calls = runner.calls.lock().unwrap();
        // Comment posted; no git/push attempted via fj.
        assert!(
            calls
                .iter()
                .any(|c| c.contains("pr comment contoso/claude-run#52")),
            "{calls:?}"
        );
        // The body file holds the failure comment.
        let body = std::fs::read_to_string(&body_path).unwrap();
        assert!(body.contains("Claude run FAILED"));
    }

    /// A successful run that produced no diff (the workdir is not a git repo, so
    /// `git diff` yields nothing) comments "no changes" and does not push.
    #[test]
    fn finalize_no_changes_comments_without_push() {
        let dir = tempfile::tempdir().unwrap();
        let wd = dir.path();
        let body_path = wd.join(PR_COMMENT_FILE);
        let runner = Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "comment",
                "contoso/claude-run#52",
                "--body-file",
                body_path.to_str().unwrap(),
            ],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let res = finalize(&backend, &cfg(), &job(), wd, &ok_result("had a look")).unwrap();
        assert_eq!(res.decision, Decision::NoChanges);
        let body = std::fs::read_to_string(&body_path).unwrap();
        assert!(body.contains("made no changes"));
    }
}
