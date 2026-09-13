//! Native CI-failure-analysis handler (CLAUDE-189).
//!
//! Enriches every open "CI failure" / "Auto-fix" issue with a deep, log-grounded
//! root-cause comment: the exact failing workflow step, the root-cause diagnostic
//! (or, when the log carries none, whether the job stopped emitting output and
//! was killed or the run ended with no diagnostic), and concrete remediation. It
//! is the automatic, idempotent equivalent of the manual CLAUDE-185 pass.
//!
//! Pipeline (typed Rust, mirroring [`crate::native::forgejo_ci_fix`]):
//!
//! - gather: `IssueTracker::list_queued(query)` -> keep issues whose summary
//!   starts with a configured prefix. Selection is by summary prefix +
//!   analyzed-marker ONLY; it never selects by `AI Agent: Queued` (that field
//!   belongs to the code-fix runner, `actions/youtrack.yml`).
//! - resolve coordinates: parse repo / index / run URL / commit / workflow from
//!   the issue's `## Failure` block. The per-repo run index is all the fetch
//!   needs; `fj` resolves it to the run internally.
//! - fetch: reuse [`crate::forge_log::fetch_run_log`] to pull the complete
//!   redacted run log through `fj actions logs`, plus the failing workflow YAML,
//!   into the workdir. A log miss never crashes. A log that can still arrive
//!   posts a short "unavailable" note (once) and leaves the issue un-marked so a
//!   later run retries; a log that is gone for good (CLAUDE-240) is marked
//!   analyzed with a comment routing the issue to a human, so it stops being
//!   re-fetched. An auth denial is fixable and always stays retryable.
//! - analyze: one agentic `claude` session (tools `Read,Grep,Bash`) over the
//!   redacted full-log file plus the workflow YAML.
//! - finalize: post the analysis as a comment (carrying the hidden analyzed
//!   marker) so the issue is never analyzed twice.
//!
//! Both forge reads stay inside `forge_log` (Rust) and shell out to `fj`: the
//! log through `fj actions logs`, the workflow YAML through `fj repo file`
//! (CLAUDE-205). Never a raw REST call, and never one from a nu snippet, per the
//! tooling-gap rule. Redaction is preserved end to end by reusing
//! `forge_log::redact`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{CiFailureAnalysisConfig, LogTokenRef, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::ClaudeInvoker;
use crate::error::{Error, Result};
use crate::forge_log::{self, ForgeReads, LogFetchConfig, LogOutcome, LogTarget};
use crate::job::{self, EffectiveLimits, ProcessedJob, RunnerDefaults};
use crate::provider::{self, AuditedTracker, IssueInspect, IssueTracker, IssueWriteAudit};
use crate::secrets::{self, SecretsKey};
use crate::state;
use crate::workdir;

/// Hidden token embedded in the analysis comment. Its presence in the issue's
/// description or any comment means the issue is analyzed (idempotency guard).
const ANALYZED_MARKER: &str = "crcianalyzed";

/// Hidden token embedded in the "log unavailable" note, so the note is posted at
/// most once while the issue stays un-marked and retryable.
const UNAVAILABLE_MARKER: &str = "crcianalysisunavailable";

/// CLAUDE-240: how long after a run stops a log that still cannot be fetched is
/// treated as gone for good rather than not yet flushed. A log flushes within
/// seconds of a run finishing, so this is orders of magnitude past the window it
/// must not cut into, and past any transient outage of the forge's log storage.
const PERMANENT_AFTER: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

pub struct CiFailureAnalysisHandler;

impl NativeHandler for CiFailureAnalysisHandler {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move { run_ci_failure_analysis(ctx).await })
    }
}

async fn run_ci_failure_analysis(ctx: NativeContext<'_>) -> Result<()> {
    let loaded = ctx.action;
    let action = &loaded.action;
    let pool = ctx.pool;

    let cfg = match &action.native {
        Some(NativeConfig::CiFailureAnalysis(cfg)) => (**cfg).clone(),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    if !cfg.enabled {
        info!(action = %action.name, "ci-failure-analysis is disabled; skipping run");
        return Ok(());
    }

    // A PAT authorizes the `fj actions logs` read on a private repo. CLAUDE-200:
    // one mapping, resolved per issue against the repo it names.
    let tokens = super::effective_log_tokens(pool, ctx.paths, cfg.log_token_ref.as_ref()).await;

    let defaults = RunnerDefaults::default();
    let limits = job::effective_limits(&defaults, loaded);

    // Pre-run daily caps.
    let daily = state::daily_count_get(pool, &action.name).await?;
    if daily >= limits.max_runs_per_day {
        warn!(action = %action.name, daily, "daily run cap reached; skipping run");
        return Ok(());
    }
    let cost = state::daily_cost_get(pool, &action.name).await?;
    if cost >= limits.daily_budget_usd {
        warn!(action = %action.name, cost, "daily budget reached; skipping run");
        return Ok(());
    }

    let backend = provider::resolve(provider::DEFAULT_PRIORITY)?;

    // gather BEFORE run_begin so a gather failure is a no-op run (no empty row).
    // CLAUDE-228: hold the gather marker so the scheduler does not stack another
    // pickup while this one is still gathering.
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
            if matches_prefix(summary, &cfg.summary_prefixes) {
                Some(key.to_string())
            } else {
                None
            }
        })
        .collect();

    let inflight: std::collections::HashSet<String> =
        state::action_inflight_keys(pool, &action.name)
            .await?
            .into_iter()
            .collect();
    let keys: Vec<String> = keys.into_iter().filter(|k| !inflight.contains(k)).collect();
    if keys.is_empty() {
        info!(action = %action.name, "no CI-failure issues to analyze");
        return Ok(());
    }
    let max_parallel = action.concurrency.max_parallel.max(1) as usize;

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

    if let Err(e) = state::run_set_job_keys(pool, run_id, &keys).await {
        warn!(run_id, error = %e, "recording planned keys failed (continuing)");
    }

    // CLAUDE-234: every tracker write from here on carries this run's id into
    // the journal, and any issue it creates spends the run's ceiling.
    let audit = IssueWriteAudit::new(Some(run_id), limits.max_issues_per_run);
    let tracker = AuditedTracker::new(backend.as_ref(), audit);

    // CLAUDE-205: ONE forge client: `fj actions logs` for the run log and
    // `fj repo file` for the workflow YAML.
    let fj = crate::provider::FjForgeReads::new(std::sync::Arc::new(
        crate::provider::ProcessRunner::new(),
    ));
    // CLAUDE-223: run-scoped global-pause gate, checked alongside cancel at the
    // per-job pickup so a mid-run pause bounds the operator's restart window.
    let run_pause = ctx.pause.for_action_run(&action.name);
    let mut processed: u32 = 0;
    for key in &keys {
        if cancel.is_cancelled() {
            break;
        }
        if run_pause.stop().await {
            break;
        }
        // Mid-run daily caps.
        if state::daily_count_get(pool, &action.name).await? >= limits.max_runs_per_day {
            break;
        }
        if state::daily_cost_get(pool, &action.name).await? >= limits.daily_budget_usd {
            break;
        }
        // DEV-705: the run's own ceiling, checked before every pickup.
        if ctx.spend.exceeded() {
            warn!(action = %action.name, "{}", ctx.spend.stop_reason());
            break;
        }

        let _permits = ctx.limiter.acquire(&action.name, max_parallel).await;
        let _key_guard = ctx.keygate.acquire(key).await;

        if let Err(e) = state::run_set_current_job(pool, run_id, Some(key)).await {
            warn!(run_id, error = %e, "run_set_current_job failed (continuing)");
        }
        if let Err(e) = state::run_inflight_add(pool, run_id, key, key).await {
            warn!(run_id, error = %e, "run_inflight_add failed (continuing)");
        }
        let outcome = process_job(&ctx, &tracker, &cfg, &limits, &fj, &tokens, key).await;
        if let Err(e) = state::run_inflight_remove(pool, run_id, key).await {
            warn!(run_id, error = %e, "run_inflight_remove failed (continuing)");
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
    Ok(())
}

/// Analyze one issue. Idempotent: an already-analyzed issue is skipped; a log
/// miss posts an "unavailable" note (once) and leaves the issue un-marked.
#[allow(clippy::too_many_arguments)]
async fn process_job(
    ctx: &NativeContext<'_>,
    backend: &dyn IssueTracker,
    cfg: &CiFailureAnalysisConfig,
    limits: &EffectiveLimits,
    fj: &dyn ForgeReads,
    tokens: &LogTokenRef,
    key: &str,
) -> Result<ProcessedJob> {
    let action = &ctx.action.action;
    let started = Instant::now();

    let attempts = state::attempts_get(ctx.pool, &action.name, key).await?;
    // CLAUDE-232: `warn`, not `info`: the analysis never runs again for this key
    // and no outcome or event row records the skip.
    if attempts >= limits.max_attempts_per_key {
        warn!(
            action = %action.name, key, attempts,
            cap = limits.max_attempts_per_key,
            "attempt cap reached; skipping analysis"
        );
        return Ok(ProcessedJob::CapHit {
            key: key.to_string(),
        });
    }

    let inspect = backend.inspect_issue(key)?;
    if is_analyzed(&inspect, ANALYZED_MARKER) {
        info!(action = %action.name, key, "already analyzed; skipping");
        return Ok(ProcessedJob::Filtered {
            key: key.to_string(),
        });
    }
    let Some(coords) = parse_failure_block(&inspect.description) else {
        warn!(action = %action.name, key, "could not parse a `## Failure` block; skipping");
        return Ok(ProcessedJob::Filtered {
            key: key.to_string(),
        });
    };

    // CLAUDE-200: the token for THIS issue's repo. A repo the mapping does not
    // cover is skipped here, before any fetch, so no "log unavailable" note is
    // posted on an issue this deployment holds no token for.
    let Some(token_ref) = tokens.resolve(&coords.repo) else {
        warn!(action = %action.name, key, repo = %coords.repo, "no log token maps to this repo; skipping (no note posted)");
        return Ok(ProcessedJob::Filtered {
            key: key.to_string(),
        });
    };
    let token = match resolve_token(ctx.pool, token_ref).await {
        Ok(t) => t,
        Err(e) => {
            warn!(action = %action.name, key, repo = %coords.repo, token_ref, error = %e, "resolving the log token failed; skipping");
            return Ok(ProcessedJob::Filtered {
                key: key.to_string(),
            });
        }
    };

    // Per-job workdir (the log + workflow YAML are written here for claude).
    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), ctx.paths);
    let workdir_path = workdir::job_workdir(&wb_base, &action.name, key);
    if let Err(e) = std::fs::remove_dir_all(&workdir_path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(key, error = %e, "clearing the analysis workdir before reuse failed (continuing)");
        }
    }
    if let Err(e) = std::fs::create_dir_all(&workdir_path) {
        warn!(key, error = %e, "creating the analysis workdir failed; skipping");
        return Ok(ProcessedJob::Filtered {
            key: key.to_string(),
        });
    }

    // Fetch the complete redacted log (+ workflow YAML).
    // CLAUDE-228: shells out to `fj` and sleeps between retries (it is handed
    // `std::thread::sleep`), so it must not run on the async worker thread.
    let fetched =
        crate::blocking::in_place(|| fetch_analysis_inputs(fj, &coords, &token, &workdir_path));
    let inputs = match fetched {
        FetchResult::Ready(inputs) => inputs,
        FetchResult::Unavailable(miss) => {
            // CLAUDE-240: a log that can still arrive is retried as before; one
            // that is gone for good is marked so it leaves the queue.
            // CLAUDE-228: the age fallback shells out to `fj`.
            let decided = crate::blocking::in_place(|| {
                decide_miss(fj, &coords, &token, &inspect, &miss, key)
            });
            match decided {
                MissAction::Retry { note } => {
                    if let Some(body) = note {
                        if let Err(e) = backend.comment(key, &body) {
                            warn!(key, error = %e, "posting the log-unavailable note failed (continuing)");
                        }
                    }
                    info!(action = %action.name, key, reason = %miss.reason, "log unavailable; left un-marked for retry");
                }
                MissAction::GiveUp { comment } => {
                    // The marker rides in the comment, so a failed post leaves
                    // the issue un-marked and it is retried: loud, and never a
                    // silently dropped decision.
                    match backend.comment(key, &comment) {
                        Ok(()) => warn!(
                            action = %action.name, key, reason = %miss.reason,
                            "run log permanently unavailable; marked analyzed and routed to a human"
                        ),
                        Err(e) => warn!(
                            key, error = %e,
                            "posting the permanent-miss comment failed; the issue stays un-marked and is retried"
                        ),
                    }
                }
            }
            return Ok(ProcessedJob::Filtered {
                key: key.to_string(),
            });
        }
    };

    // Agentic analysis.
    let prompt = build_prompt(&inputs, &coords);
    let stream_log =
        ClaudeInvoker::stream_log_path(&ctx.paths.logs_dir(), &action.name, key, attempts + 1);
    info!(action = %action.name, key, stream_log = %stream_log.display(), "invoking claude for analysis");
    let r = ctx.claude.invoke(
        &prompt,
        &action.claude,
        limits.claude_timeout,
        &workdir_path,
        &stream_log,
        &CancellationToken::new(),
        // DEV-705: cap the child at the run's remaining ceiling.
        Some(ctx.spend.remaining_usd()),
    )?;
    // DEV-707: bill the run before any branch returns early.
    ctx.spend.record(&r);

    if r.transient {
        info!(action = %action.name, key, "transient claude error; will retry next run");
        return Ok(ProcessedJob::Transient {
            key: key.to_string(),
        });
    }

    // Ledger: claude ran, so advance daily count + cost; count the attempt.
    state::daily_cost_add(ctx.pool, &action.name, r.cost).await?;
    state::daily_count_increment(ctx.pool, &action.name).await?;
    let attempt = state::attempts_increment(ctx.pool, &action.name, key).await?;

    if r.ok {
        let body = build_analysis_comment(
            &cap_comment(&r.summary, cfg.max_analysis_bytes),
            ANALYZED_MARKER,
        );
        if let Err(e) = backend.comment(key, &body) {
            warn!(key, error = %e, "posting the analysis comment failed");
            return Err(e);
        }
        info!(action = %action.name, key, cost = r.cost, "analysis posted; issue marked analyzed");
    } else {
        warn!(action = %action.name, key, error = %r.error, "analysis did not complete; leaving un-marked");
    }

    Ok(ProcessedJob::Done {
        key: key.to_string(),
        attempt,
        ok: r.ok,
        claude_ran: true,
        claude_ok: r.ok,
        claude_cost: r.cost,
        attempt_counted: true,
        summary: r.summary.clone(),
        stream_log_path: Some(stream_log.display().to_string()),
        duration_ms: started.elapsed().as_millis() as i64,
        timed_out: r.timed_out,
    })
}

/// The redacted-log + workflow-YAML inputs claude reads.
struct AnalysisInputs {
    /// Path to the complete redacted run log written under the workdir.
    log_path: PathBuf,
    /// Path to the failing workflow YAML, or `None` if it could not be fetched.
    workflow_path: Option<PathBuf>,
    /// CLAUDE-236: the run's forge-reported start / stop times, or `None` when
    /// they could not be read (the prompt then says the timing is unavailable).
    timing: Option<forge_log::RunTiming>,
}

/// Fetch result: inputs ready, or the log is unavailable (with a reason).
enum FetchResult {
    Ready(AnalysisInputs),
    Unavailable(Miss),
}

/// CLAUDE-240: a log that could not be read, and what the fetch itself knows
/// about whether asking again could ever help.
struct Miss {
    /// Operator-facing cause, as the note and the log line carry it.
    reason: String,
    verdict: MissVerdict,
}

/// CLAUDE-240: what the fetch knows about the permanence of a miss. Advisory in
/// one direction only: it may only ever mark a log gone, never resurrect one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissVerdict {
    /// `fj` reported the log gone for good (the FJ-79 signal). Decided here,
    /// with no age check.
    Gone,
    /// Retryable whatever the run's age: a credential problem is fixable, so a
    /// rejected token must never terminate an issue.
    Retryable,
    /// Nothing known about permanence; the run's age decides.
    Unknown,
}

impl Miss {
    /// A miss `fj` reported as permanent (FJ-79).
    fn gone(reason: String) -> Self {
        Self {
            reason,
            verdict: MissVerdict::Gone,
        }
    }
    /// A miss that stays retryable however old the run is.
    fn retryable(reason: String) -> Self {
        Self {
            reason,
            verdict: MissVerdict::Retryable,
        }
    }
    /// A miss whose permanence the run's age decides.
    fn unknown(reason: String) -> Self {
        Self {
            reason,
            verdict: MissVerdict::Unknown,
        }
    }
}

/// CLAUDE-240: what to do with an issue whose run log could not be fetched.
enum MissAction {
    /// Leave the issue un-marked so a later run retries. `note` is the one-time
    /// "log unavailable" comment to post, or `None` when it is already there.
    Retry { note: Option<String> },
    /// The log is gone for good: post `comment`, which carries
    /// [`ANALYZED_MARKER`], so the existing analyzed guard never picks this
    /// issue up again and a human triages it instead.
    GiveUp { comment: String },
}

/// CLAUDE-240: decide whether a log miss is permanent, and what to post.
///
/// `fj`'s own verdict wins where it has one (FJ-79); an auth denial is fixable
/// and stays retryable whatever the run's age; anything else falls back to the
/// run's age. A permanence check that cannot be made leaves the miss retryable,
/// so nothing here can terminate an issue that would otherwise have succeeded.
fn decide_miss(
    fj: &dyn ForgeReads,
    coords: &RunCoordinates,
    token: &str,
    inspect: &IssueInspect,
    miss: &Miss,
    key: &str,
) -> MissAction {
    let permanent = match miss.verdict {
        MissVerdict::Gone => true,
        MissVerdict::Retryable => false,
        MissVerdict::Unknown => run_stopped_long_ago(fj, coords, token, key),
    };
    if permanent {
        return MissAction::GiveUp {
            comment: build_permanent_comment(&miss.reason, coords, ANALYZED_MARKER),
        };
    }
    MissAction::Retry {
        note: should_post_unavailable(inspect, UNAVAILABLE_MARKER)
            .then(|| build_unavailable_comment(&miss.reason, UNAVAILABLE_MARKER)),
    }
}

/// CLAUDE-240: true when the run stopped more than [`PERMANENT_AFTER`] ago, so a
/// log that still cannot be read is gone rather than un-flushed.
///
/// TODO(FJ-79): drop this age fallback once the deployed `fj` reports a missing
/// log directly; its `log_state` then decides every case on its own.
///
/// A stop time that cannot be read or parsed, and a run that reports stopping in
/// the future, are logged at `warn` and reported as NOT permanent: a failed
/// permanence check must never terminate an issue by accident.
fn run_stopped_long_ago(
    fj: &dyn ForgeReads,
    coords: &RunCoordinates,
    token: &str,
    key: &str,
) -> bool {
    let Some(host) = forge_log::host_from_html_url(&coords.html_url) else {
        warn!(key, url = %coords.html_url, "could not derive the forge host from the run URL; treating the log miss as retryable");
        return false;
    };
    let stopped = match fj.run_timing(&host, &coords.repo, coords.index_in_repo, Some(token)) {
        Ok(t) => t.stopped,
        Err(reason) => {
            warn!(key, repo = %coords.repo, run = coords.index_in_repo, %reason, "could not read the run's stop time; treating the log miss as retryable");
            return false;
        }
    };
    let stopped_at = match chrono::DateTime::parse_from_rfc3339(&stopped) {
        Ok(t) => t.with_timezone(&chrono::Utc),
        Err(e) => {
            warn!(key, stopped = %stopped, error = %e, "the run's stop time is unparseable; treating the log miss as retryable");
            return false;
        }
    };
    match chrono::Utc::now()
        .signed_duration_since(stopped_at)
        .to_std()
    {
        Ok(age) => age >= PERMANENT_AFTER,
        Err(_) => {
            warn!(key, stopped = %stopped, "the run reports stopping in the future; treating the log miss as retryable");
            false
        }
    }
}

/// Fetch the complete redacted run log through `fj actions logs` and, best-effort,
/// the failing workflow YAML into `out_dir`. Never errors: any miss becomes
/// [`FetchResult::Unavailable`].
fn fetch_analysis_inputs(
    fj: &dyn ForgeReads,
    coords: &RunCoordinates,
    token: &str,
    out_dir: &Path,
) -> FetchResult {
    // CLAUDE-204: the issue names a run that finished long ago, so one call decides.
    let fetch_cfg = LogFetchConfig::no_retry();

    // 1. Fetch the complete redacted log. CLAUDE-204: `fj` takes the per-repo run
    // number the `## Failure` block already carries, so there is no id to resolve.
    let Some(log_target) = LogTarget::for_run(
        &coords.html_url,
        &coords.repo,
        coords.index_in_repo,
        Some(token.to_string()),
    ) else {
        return FetchResult::Unavailable(Miss::retryable(
            "could not derive the forge host from the run URL".to_string(),
        ));
    };
    let log_path = match forge_log::fetch_run_log(
        fj,
        &fetch_cfg,
        &log_target,
        out_dir,
        &[],
        std::thread::sleep,
    ) {
        Ok(LogOutcome::Fetched(log)) => {
            // CLAUDE-240: post-FJ-79 `fj` ANSWERS for a run whose log blobs are
            // gone, handing back every job with an empty log. There is nothing
            // to analyze and re-asking can never help, so that is a permanent
            // miss, not an input.
            match forge_log::logs_permanently_missing(&log.jobs) {
                Some(reason) => return FetchResult::Unavailable(Miss::gone(reason)),
                None => log.artifact,
            }
        }
        Ok(LogOutcome::AuthDenied { reason }) => {
            // A rejected or under-scoped token is fixable, so this stays
            // retryable however old the run is.
            return FetchResult::Unavailable(Miss::retryable(format!(
                "run-logs auth denied ({reason})"
            )));
        }
        Ok(LogOutcome::TransientMiss { reason }) => {
            return FetchResult::Unavailable(Miss::unknown(format!(
                "run logs not available yet or pruned: {reason}"
            )))
        }
        Ok(LogOutcome::Failed { reason }) => {
            return FetchResult::Unavailable(Miss::unknown(format!(
                "fetching the run logs failed: {reason}"
            )))
        }
        Err(e) => {
            return FetchResult::Unavailable(Miss::unknown(format!(
                "fetching the run logs errored: {e}"
            )))
        }
    };

    // 2. Best-effort workflow YAML at the failing commit, through `fj repo file`
    // (CLAUDE-205). Redacted by `read_workflow_yaml` before it is written.
    let workflow_path = forge_log::read_workflow_yaml(
        fj,
        &log_target.host,
        &coords.repo,
        &coords.commit_sha,
        &coords.workflow_id,
        Some(token),
        &[],
    )
    .and_then(|yaml| {
        // A write failure is best-effort (the analysis proceeds over the log
        // alone) but never silent: without the log line the prompt's "could not
        // be fetched" would misreport a local I/O fault as a forge miss.
        let path = out_dir.join("failing-workflow.yml");
        match std::fs::write(&path, yaml) {
            Ok(()) => Some(path),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "writing the failing workflow YAML failed; the analysis proceeds over the log alone");
                None
            }
        }
    });

    // 3. CLAUDE-236: the run's start / stop times, so the analysis can tell a job
    // that stopped emitting output and was killed from a capture that lost data.
    // Best effort: a failed or run-less read is logged and the prompt says the
    // timing is unavailable, rather than the agent inventing a reason.
    let timing = match fj.run_timing(
        &log_target.host,
        &coords.repo,
        coords.index_in_repo,
        Some(token),
    ) {
        Ok(t) => Some(t),
        Err(reason) => {
            warn!(
                repo = %coords.repo,
                run = coords.index_in_repo,
                reason = %reason,
                "reading the run timing failed; the analysis states the timing is unavailable"
            );
            None
        }
    };

    FetchResult::Ready(AnalysisInputs {
        log_path,
        workflow_path,
        timing,
    })
}

/// Resolve the decrypted, trimmed Forgejo PAT from the secrets store.
async fn resolve_token(pool: &crate::db::DbPool, name: &str) -> Result<String> {
    let key = SecretsKey::load()?;
    let token = secrets::resolve_secret(pool, &key, name).await?;
    Ok(token.trim().to_string())
}

/// CLAUDE-189: coordinates parsed from an issue's `## Failure` block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RunCoordinates {
    repo: String,
    index_in_repo: i64,
    commit_sha: String,
    workflow_id: String,
    html_url: String,
}

/// True when `summary` starts with any of `prefixes` (both trimmed).
fn matches_prefix(summary: &str, prefixes: &[String]) -> bool {
    let s = summary.trim_start();
    prefixes.iter().any(|p| s.starts_with(p.trim()))
}

/// True when the issue is already analyzed: the analyzed marker appears in the
/// description or any comment.
fn is_analyzed(inspect: &IssueInspect, marker: &str) -> bool {
    inspect.description.contains(marker) || inspect.comments.iter().any(|c| c.contains(marker))
}

/// True when the "log unavailable" note has NOT yet been posted, so it should be
/// posted now (post-once, while the issue stays un-marked and retryable).
fn should_post_unavailable(inspect: &IssueInspect, marker: &str) -> bool {
    !(inspect.description.contains(marker) || inspect.comments.iter().any(|c| c.contains(marker)))
}

/// Parse the `## Failure` block (as [`crate::mint`] emits it) into run
/// coordinates. Returns `None` when the required Repo / Run lines are absent.
fn parse_failure_block(description: &str) -> Option<RunCoordinates> {
    let mut repo = None;
    let mut commit_sha = String::new();
    let mut workflow_id = String::new();
    let mut html_url = None;
    for line in description.lines() {
        let t = line.trim();
        if let Some(v) = t.strip_prefix("- Repo:") {
            repo = Some(unbacktick(v));
        } else if let Some(v) = t.strip_prefix("- Workflow:") {
            workflow_id = unbacktick(v);
        } else if let Some(v) = t.strip_prefix("- Commit:") {
            commit_sha = unbacktick(v);
        } else if let Some(v) = t.strip_prefix("- Run:") {
            html_url = Some(v.trim().to_string());
        }
    }
    let repo = repo?;
    let html_url = html_url?;
    let index_in_repo = run_index_from_url(&html_url)?;
    Some(RunCoordinates {
        repo,
        index_in_repo,
        commit_sha,
        workflow_id,
        html_url,
    })
}

/// Strip surrounding whitespace and backticks from a `- Key: `value`` value.
fn unbacktick(v: &str) -> String {
    v.trim().trim_matches('`').trim().to_string()
}

/// The trailing `/actions/runs/<N>` index in a run URL.
fn run_index_from_url(url: &str) -> Option<i64> {
    let after = url.split("/actions/runs/").nth(1)?;
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().ok()
}

/// Cap `text` to `max_bytes`, keeping the head and appending a truncation note.
/// UTF-8 safe.
fn cap_comment(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n\n_[analysis truncated to {} bytes]_",
        &text[..end],
        max_bytes
    )
}

/// The agentic-analysis prompt. Directs claude to identify the failing step by
/// mapping log position to workflow step order, state the root-cause diagnostic
/// or, when the log carries none, classify on the run's timing whether the job
/// stopped emitting output and was killed or the run ended with no diagnostic,
/// and give remediation. CLAUDE-236: the stored log is never truncated by this
/// tool, so the prompt never offers truncation as an explanation.
fn build_prompt(inputs: &AnalysisInputs, coords: &RunCoordinates) -> String {
    let log_name = inputs
        .log_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "run.log".to_string());
    let workflow_line = match &inputs.workflow_path {
        Some(p) => format!(
            "The failing workflow YAML (fetched at the failing commit) is at `./{}`.",
            p.file_name().unwrap().to_string_lossy()
        ),
        None => "The failing workflow YAML could not be fetched; analyze the log alone and say so."
            .to_string(),
    };
    // CLAUDE-236: the run's own start / stop times are the evidence that tells a
    // job killed after going silent from a capture that lost data.
    let timing_line = match &inputs.timing {
        Some(t) => format!(
            "The forge reports the run started at `{}` and stopped at `{}`; when the log stops mid-step, compare that stop time against the last timestamp in the log.",
            t.started, t.stopped
        ),
        None => "The run's start / stop times could not be read from the forge; say so where they would matter rather than assuming why the log stops.".to_string(),
    };
    [
        "You are analyzing a failed CI run to produce a deep, log-grounded root-cause report.".to_string(),
        format!(
            "The complete redacted run log is at `./{log_name}` (repo `{}`, workflow `{}`, commit `{}`, run #{}).",
            coords.repo, coords.workflow_id, coords.commit_sha, coords.index_in_repo
        ),
        "That file is the whole log exactly as the forge stores it: this tool applies no size cap and never shortens it, so where the log stops is where the run stopped writing.".to_string(),
        format!("Run URL: {}", coords.html_url),
        timing_line,
        workflow_line,
        String::new(),
        "Use Read/Grep/Bash to read both files. Then report, in Markdown:".to_string(),
        "1. **Failing step**: the exact workflow step that failed, identified by mapping the point where the log stops against the workflow's step order.".to_string(),
        "2. **Root cause**: the concrete diagnostic (the compiler/clippy/test error line). If the log stops mid-step with no diagnostic, classify which of these holds and justify it from the evidence: (a) the job stopped emitting output and was killed or timed out, shown by the gap between the last log timestamp and the run's stop time, or (b) the run ended and the failing step emitted no diagnostic at all. Then give the leading hypothesis (e.g. an OOM if the workflow caps build parallelism).".to_string(),
        "3. **Remediation**: concrete next steps to fix or to capture a diagnostic on the next run.".to_string(),
        String::new(),
        "Ground every claim in the actual log/workflow contents. Be concise. Do not modify any files, push, open a PR, or touch YouTrack: output only the analysis text, which is posted verbatim as a comment.".to_string(),
    ]
    .join("\n")
}

/// The analysis comment: the agent's report plus the hidden analyzed marker.
fn build_analysis_comment(analysis: &str, marker: &str) -> String {
    [
        "## CI-failure analysis".to_string(),
        String::new(),
        analysis.trim().to_string(),
        String::new(),
        "<!-- claude-run ci-failure-analysis; do not edit -->".to_string(),
        format!("Ci-Analysis: {marker}"),
    ]
    .join("\n")
}

/// The "log unavailable" note: a short reason plus the hidden unavailable marker
/// (so it is not re-posted while the issue stays un-marked and retryable).
fn build_unavailable_comment(reason: &str, marker: &str) -> String {
    [
        "## CI-failure analysis: log unavailable".to_string(),
        String::new(),
        format!(
            "The run log could not be fetched, so no analysis was produced: {reason}. This issue is left un-marked; a later run retries once the log is available."
        ),
        String::new(),
        "<!-- claude-run ci-failure-analysis; do not edit -->".to_string(),
        format!("Ci-Analysis-Unavailable: {marker}"),
    ]
    .join("\n")
}

/// CLAUDE-240: the give-up comment for a log that is gone for good. It carries
/// [`ANALYZED_MARKER`], so the existing idempotency guard skips the issue from
/// here on with no new state, and it says plainly that no analysis exists and a
/// human has to take it from here.
fn build_permanent_comment(reason: &str, coords: &RunCoordinates, marker: &str) -> String {
    [
        "## CI-failure analysis: log permanently unavailable".to_string(),
        String::new(),
        format!(
            "The log for run #{} of `{}` cannot be fetched and never will be: {reason}.",
            coords.index_in_repo, coords.repo
        ),
        format!("Run: {}", coords.html_url),
        String::new(),
        "No analysis could be produced, so this issue needs human triage. It is marked analyzed to stop the retries; nothing will pick it up for analysis again.".to_string(),
        String::new(),
        "<!-- claude-run ci-failure-analysis; do not edit -->".to_string(),
        format!("Ci-Analysis: {marker}"),
    ]
    .join("\n")
}

/// Bridge a [`ProcessedJob`] to the JSONL output record + persisted job outcome,
/// byte-identical to the scripted / forgejo-ci-fix path.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forge_log::{ActionLogClient, JobLog, RunLogsOutcome};
    use std::sync::Mutex;

    fn inspect(description: &str, comments: &[&str]) -> IssueInspect {
        IssueInspect {
            description: description.to_string(),
            comments: comments.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// The ONE forge client the analysis holds (CLAUDE-205): a canned
    /// `fj actions logs` outcome, canned `fj repo file` contents keyed by path,
    /// and a canned `fj actions runs --run-number` timing, asserting the token
    /// every read is scoped with. The runs LIST is not used by this action.
    struct StubFj {
        outcome: Mutex<Option<RunLogsOutcome>>,
        files: std::collections::HashMap<String, String>,
        timing: std::result::Result<forge_log::RunTiming, String>,
    }
    impl StubFj {
        fn new(outcome: RunLogsOutcome) -> Self {
            Self {
                outcome: Mutex::new(Some(outcome)),
                files: std::collections::HashMap::new(),
                timing: Ok(forge_log::RunTiming {
                    started: "2026-08-20T20:03:13+02:00".to_string(),
                    stopped: "2026-08-20T20:27:31+02:00".to_string(),
                }),
            }
        }
        fn with_file(mut self, path: &str, content: &str) -> Self {
            self.files.insert(path.to_string(), content.to_string());
            self
        }
        /// The timing read fails (an unreachable forge, a run the page misses).
        fn with_timing_error(mut self, reason: &str) -> Self {
            self.timing = Err(reason.to_string());
            self
        }
        /// CLAUDE-240: the forge reports the run stopping at `stopped`, which is
        /// what the age fallback measures against.
        fn with_stopped(mut self, stopped: &str) -> Self {
            self.timing = Ok(forge_log::RunTiming {
                started: "2026-08-20T20:03:13+02:00".to_string(),
                stopped: stopped.to_string(),
            });
            self
        }
    }
    impl ActionLogClient for StubFj {
        fn run_logs(
            &self,
            host: &str,
            repo: &str,
            run_number: i64,
            token: Option<&str>,
        ) -> RunLogsOutcome {
            assert_eq!(host, "dev.a8n.run");
            assert_eq!(repo, "initech/chat");
            assert_eq!(run_number, 4034);
            assert_eq!(token, Some("test-pat"), "the log token scopes the fetch");
            self.outcome
                .lock()
                .unwrap()
                .take()
                .unwrap_or(RunLogsOutcome::NotFound("no action run #4034".to_string()))
        }
    }
    impl crate::forge_log::ActionRunsClient for StubFj {
        fn list_runs(
            &self,
            _host: &str,
            _repo: &str,
            _max_pages: u32,
            _token: Option<&str>,
        ) -> crate::forge_log::RunsListOutcome {
            unreachable!("the analysis action never lists runs")
        }
        fn run_timing(
            &self,
            host: &str,
            repo: &str,
            run_number: i64,
            token: Option<&str>,
        ) -> std::result::Result<forge_log::RunTiming, String> {
            assert_eq!(host, "dev.a8n.run");
            assert_eq!(repo, "initech/chat");
            assert_eq!(run_number, 4034);
            assert_eq!(token, Some("test-pat"), "the log token scopes the read");
            self.timing.clone()
        }
    }
    impl crate::forge_log::RepoFileClient for StubFj {
        fn read_file(
            &self,
            host: &str,
            repo: &str,
            path: &str,
            git_ref: &str,
            token: Option<&str>,
        ) -> std::result::Result<String, String> {
            assert_eq!(host, "dev.a8n.run");
            assert_eq!(repo, "initech/chat");
            assert_eq!(
                git_ref, "a456a957",
                "the YAML is read at the failing commit"
            );
            assert_eq!(token, Some("test-pat"), "the log token scopes the read");
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| format!("cannot read `{path}`"))
        }
    }

    /// AC (redaction): a log fixture containing a token is redacted before it is
    /// written to the file the analysis reads, so no token can reach the comment.
    #[test]
    fn fetched_log_is_redacted_before_the_analysis_reads_it() {
        let coords = RunCoordinates {
            repo: "initech/chat".to_string(),
            index_in_repo: 4034,
            commit_sha: "a456a957".to_string(),
            workflow_id: "check.yml".to_string(),
            html_url: "https://dev.a8n.run/initech/chat/actions/runs/4034".to_string(),
        };
        let log = "step ok\nAuthorization: token ghp_abcdefghij0123456789ABCDEFG\nboom";
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: log.to_string(),
            ..Default::default()
        }]));
        // Every workflow-YAML candidate misses, so no yaml is written.
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Ready(inputs) =
            fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path())
        else {
            panic!("expected Ready");
        };
        let on_disk = std::fs::read_to_string(&inputs.log_path).unwrap();
        assert!(
            !on_disk.contains("ghp_abcdefghij0123456789ABCDEFG"),
            "token redacted from the analyzed log file"
        );
        assert!(on_disk.contains("[REDACTED]"));
        assert!(on_disk.contains("boom"));
        assert!(
            inputs.workflow_path.is_none(),
            "workflow YAML missing -> None"
        );
    }

    /// CLAUDE-205 AC: the workflow YAML `fj repo file` returns is redacted before
    /// it is written to the file the analysis reads.
    #[test]
    fn workflow_yaml_is_read_at_the_commit_and_redacted_before_it_is_written() {
        let coords = RunCoordinates {
            repo: "initech/chat".to_string(),
            index_in_repo: 4034,
            commit_sha: "a456a957".to_string(),
            workflow_id: "check.yml".to_string(),
            html_url: "https://dev.a8n.run/initech/chat/actions/runs/4034".to_string(),
        };
        let yaml =
            "jobs:\n  check:\n    steps:\n      - run: echo ghp_abcdefghij0123456789ABCDEFG\n";
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: "boom".to_string(),
            ..Default::default()
        }]))
        .with_file(".forgejo/workflows/check.yml", yaml);
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Ready(inputs) =
            fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path())
        else {
            panic!("expected Ready");
        };
        let path = inputs.workflow_path.expect("workflow YAML written");
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert!(on_disk.contains("jobs:"));
        assert!(
            !on_disk.contains("ghp_abcdefghij0123456789ABCDEFG"),
            "token redacted from the analyzed workflow YAML"
        );
        assert!(on_disk.contains("[REDACTED]"));
    }

    /// AC (graceful log miss): a run `fj` cannot find yields Unavailable (never a
    /// panic), so the caller posts a note and leaves the issue un-marked.
    #[test]
    fn fetch_reports_unavailable_on_a_log_miss() {
        let coords = RunCoordinates {
            repo: "initech/chat".to_string(),
            index_in_repo: 4034,
            commit_sha: "a456a957".to_string(),
            workflow_id: "check.yml".to_string(),
            html_url: "https://dev.a8n.run/initech/chat/actions/runs/4034".to_string(),
        };
        let fj = StubFj::new(RunLogsOutcome::NotFound("no action run #4034".to_string()));
        let tmp = tempfile::tempdir().unwrap();
        match fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path()) {
            FetchResult::Unavailable(miss) => {
                assert!(miss.reason.contains("no action run"), "{}", miss.reason);
                // Nothing known about permanence: the run's age decides.
                assert_eq!(miss.verdict, MissVerdict::Unknown);
            }
            FetchResult::Ready(_) => panic!("a missing run must not be Ready"),
        }
    }

    /// A rejected token is reported as an auth denial, not a transient miss.
    #[test]
    fn fetch_reports_unavailable_on_auth_denial() {
        let coords = RunCoordinates {
            repo: "initech/chat".to_string(),
            index_in_repo: 4034,
            commit_sha: "a456a957".to_string(),
            workflow_id: "check.yml".to_string(),
            html_url: "https://dev.a8n.run/initech/chat/actions/runs/4034".to_string(),
        };
        let fj = StubFj::new(RunLogsOutcome::AuthDenied("HTTP 403: ...".to_string()));
        let tmp = tempfile::tempdir().unwrap();
        match fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path()) {
            FetchResult::Unavailable(miss) => {
                assert!(miss.reason.contains("auth denied"), "{}", miss.reason);
                // CLAUDE-240: a credential problem is fixable, so it is never a
                // candidate for the permanence check.
                assert_eq!(miss.verdict, MissVerdict::Retryable);
            }
            FetchResult::Ready(_) => panic!("an auth denial must not be Ready"),
        }
    }

    // ---- selection filter: prefix + marker, never AI Agent ----------------

    #[test]
    fn selection_matches_only_the_configured_prefixes() {
        let prefixes = vec!["CI failure:".to_string(), "Auto-fix:".to_string()];
        assert!(matches_prefix(
            "CI failure: check.yml on initech/chat (run #4034, a456a957)",
            &prefixes
        ));
        assert!(matches_prefix(
            "Auto-fix: cargo fmt on x (run #1, abc)",
            &prefixes
        ));
        // A non-matching summary (e.g. an ordinary feature issue) is excluded even
        // if it happens to be Queued for the code-fix runner: selection never
        // keys off AI Agent, only the summary prefix.
        assert!(!matches_prefix("Implement the events page", &prefixes));
        assert!(!matches_prefix("CI failed yesterday", &prefixes));
    }

    #[test]
    fn analyzed_issue_is_detected_across_description_and_comments() {
        // Marker in the description.
        assert!(is_analyzed(
            &inspect(&format!("body\nCi-Analysis: {ANALYZED_MARKER}"), &[]),
            ANALYZED_MARKER
        ));
        // Marker in a comment (the usual case: the analysis is a comment).
        assert!(is_analyzed(
            &inspect(
                "body",
                &["## CI-failure analysis\n...\nCi-Analysis: crcianalyzed"]
            ),
            ANALYZED_MARKER
        ));
        // Neither: not analyzed.
        assert!(!is_analyzed(
            &inspect("body", &["a normal comment"]),
            ANALYZED_MARKER
        ));
    }

    #[test]
    fn analysis_comment_round_trips_to_analyzed_detection() {
        // The posted comment embeds the marker, so the next inspect detects it:
        // this is the idempotency guarantee (no double comment).
        let body = build_analysis_comment("The Clippy step OOM'd.", ANALYZED_MARKER);
        assert!(body.contains("The Clippy step OOM'd."));
        let after = inspect("original description", &[&body]);
        assert!(is_analyzed(&after, ANALYZED_MARKER));
    }

    // ---- log-miss handling: post note once, never mark analyzed -----------

    #[test]
    fn unavailable_note_posts_once_then_is_suppressed() {
        // First encounter: no marker yet -> post.
        let fresh = inspect("body", &[]);
        assert!(should_post_unavailable(&fresh, UNAVAILABLE_MARKER));
        // After the note is posted (marker present): do not re-post.
        let noted = inspect(
            "body",
            &[&build_unavailable_comment("pruned", UNAVAILABLE_MARKER)],
        );
        assert!(!should_post_unavailable(&noted, UNAVAILABLE_MARKER));
        // The unavailable note must NOT count as analyzed (retry stays possible).
        assert!(!is_analyzed(&noted, ANALYZED_MARKER));
    }

    // ---- CLAUDE-240: giving up on a log that is gone for good --------------

    /// An RFC 3339 stamp `hours` before now, for the age fallback.
    fn hours_ago(hours: i64) -> String {
        (chrono::Utc::now() - chrono::Duration::hours(hours)).to_rfc3339()
    }

    /// A job whose log `fj` reports as permanently gone (the FJ-79 signal).
    fn missing_job() -> JobLog {
        JobLog {
            name: "build".to_string(),
            status: "failure".to_string(),
            log: String::new(),
            log_state: forge_log::LOG_STATE_MISSING.to_string(),
            log_reason: Some("open logs for task 43177: no such file or directory".to_string()),
        }
    }

    /// AC: a log `fj` reports as missing is permanent immediately, with no age
    /// check. The stub's timing read ERRORS, so a decision that reached the age
    /// fallback could not have come out permanent.
    #[test]
    fn a_log_fj_reports_as_missing_is_permanent_with_no_age_check() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![missing_job()]))
            .with_timing_error("the age fallback must not be consulted");
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Unavailable(miss) =
            fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path())
        else {
            panic!("a run whose every job log is gone has nothing to analyze");
        };
        assert_eq!(miss.verdict, MissVerdict::Gone);
        assert!(miss.reason.contains("task 43177"), "{}", miss.reason);

        let fresh = inspect(CLAUDE_185_BODY, &[]);
        let MissAction::GiveUp { comment } =
            decide_miss(&fj, &coords, "test-pat", &fresh, &miss, "GAMMA-970")
        else {
            panic!("a log fj reports gone must not be retried");
        };
        // AC: the comment names the run, the server's reason, and the triage.
        assert!(comment.contains("run #4034"), "{comment}");
        assert!(comment.contains("initech/chat"), "{comment}");
        assert!(comment.contains("task 43177"), "{comment}");
        assert!(comment.contains("needs human triage"), "{comment}");
    }

    /// AC: after the give-up comment, the existing `is_analyzed` guard skips the
    /// issue. Two passes over the same issue: the second sees the comment the
    /// first produced, and `process_job`'s FIRST act is that guard, so it never
    /// reaches a fetch again.
    #[test]
    fn the_give_up_comment_makes_the_next_pass_take_the_analyzed_path() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![missing_job()]));
        let miss = Miss::gone("the log blob is gone".to_string());

        // Pass 1: nothing analyzed yet, so the decision is taken and posted.
        let first = inspect(CLAUDE_185_BODY, &[]);
        assert!(!is_analyzed(&first, ANALYZED_MARKER));
        let MissAction::GiveUp { comment } =
            decide_miss(&fj, &coords, "test-pat", &first, &miss, "GAMMA-970")
        else {
            panic!("expected GiveUp");
        };

        // Pass 2: the issue now carries that comment.
        let second = inspect(CLAUDE_185_BODY, &[&comment]);
        assert!(
            is_analyzed(&second, ANALYZED_MARKER),
            "the analyzed guard skips the issue from here on: {comment}"
        );
        // And it is not mistaken for the retryable note, which would re-arm it.
        assert!(!should_post_unavailable(&second, ANALYZED_MARKER));
    }

    /// AC: an auth denial is never permanent, however old the run is. The run
    /// here stopped a year ago, which the age fallback would call permanent.
    #[test]
    fn an_auth_denial_stays_retryable_however_old_the_run_is() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::AuthDenied("HTTP 403".to_string()))
            .with_stopped(&hours_ago(24 * 365));
        let miss = Miss::retryable("run-logs auth denied (HTTP 403)".to_string());
        let MissAction::Retry { note } = decide_miss(
            &fj,
            &coords,
            "test-pat",
            &inspect(CLAUDE_185_BODY, &[]),
            &miss,
            "GAMMA-970",
        ) else {
            panic!("a fixable credential problem must never terminate an issue");
        };
        let note = note.expect("the one-time note is posted");
        assert!(note.contains(UNAVAILABLE_MARKER));
        assert!(!note.contains(ANALYZED_MARKER), "the issue stays un-marked");
    }

    /// AC: a fetch failure on a run that stopped within `PERMANENT_AFTER` keeps
    /// today's behaviour: the one-time note is posted, the issue stays un-marked,
    /// and no attempt is consumed (the miss path returns `Filtered`, the outcome
    /// kind that records no job outcome and therefore no attempt).
    #[test]
    fn a_recent_run_still_posts_the_note_once_and_stays_unmarked() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::NotFound("no action run #4034".to_string()))
            .with_stopped(&hours_ago(2));
        let miss = Miss::unknown("run logs not available yet or pruned".to_string());

        let fresh = inspect(CLAUDE_185_BODY, &[]);
        let MissAction::Retry { note } =
            decide_miss(&fj, &coords, "test-pat", &fresh, &miss, "GAMMA-970")
        else {
            panic!("a run that stopped 2h ago may still be flushing its log");
        };
        let note = note.expect("the first miss posts the note");
        assert!(note.contains("left un-marked"), "{note}");
        let noted = inspect(CLAUDE_185_BODY, &[&note]);
        assert!(!is_analyzed(&noted, ANALYZED_MARKER), "still un-marked");

        // The second miss on the same issue re-posts nothing.
        let MissAction::Retry { note } =
            decide_miss(&fj, &coords, "test-pat", &noted, &miss, "GAMMA-970")
        else {
            panic!("still retryable");
        };
        assert!(note.is_none(), "the note is posted at most once");
        assert!(
            job::done_record(&ProcessedJob::Filtered {
                key: "GAMMA-970".to_string()
            })
            .is_none(),
            "a filtered job records no outcome, so no attempt is consumed"
        );
    }

    /// AC: with no `fj` signal, a run that stopped more than `PERMANENT_AFTER`
    /// ago whose log still fails to fetch is permanent.
    #[test]
    fn an_old_run_whose_log_still_fails_is_permanent() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let miss = Miss::unknown(
            "fetching the run logs failed: fj actions logs exited 1: unexpected status code 500"
                .to_string(),
        );
        let fresh = inspect(CLAUDE_185_BODY, &[]);

        // One hour short of the threshold: still retried.
        let recent =
            StubFj::new(RunLogsOutcome::NotFound("500".to_string())).with_stopped(&hours_ago(23));
        assert!(matches!(
            decide_miss(&recent, &coords, "test-pat", &fresh, &miss, "GAMMA-970"),
            MissAction::Retry { .. }
        ));

        // One hour past it: given up on, with the server's reason in the comment.
        let old =
            StubFj::new(RunLogsOutcome::NotFound("500".to_string())).with_stopped(&hours_ago(25));
        let MissAction::GiveUp { comment } =
            decide_miss(&old, &coords, "test-pat", &fresh, &miss, "GAMMA-970")
        else {
            panic!("a log unreadable a day after the run stopped is not coming back");
        };
        assert!(comment.contains("unexpected status code 500"), "{comment}");
        assert!(comment.contains(ANALYZED_MARKER), "{comment}");
    }

    /// AC: a permanence decision that cannot be made is treated as NOT permanent,
    /// so a failed lookup can never terminate an issue by accident. Each case is
    /// logged at `warn` by `run_stopped_long_ago`.
    #[test]
    fn an_undecidable_permanence_check_leaves_the_issue_retryable() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let miss = Miss::unknown("fetching the run logs failed".to_string());
        let fresh = inspect(CLAUDE_185_BODY, &[]);
        for fj in [
            // The runs lookup itself failed.
            StubFj::new(RunLogsOutcome::NotFound("500".to_string()))
                .with_timing_error("fj actions runs exited 1"),
            // The forge reported a stop time nothing can parse.
            StubFj::new(RunLogsOutcome::NotFound("500".to_string())).with_stopped("not a date"),
            // The forge reported the run stopping in the future (clock skew).
            StubFj::new(RunLogsOutcome::NotFound("500".to_string())).with_stopped(&hours_ago(-2)),
        ] {
            assert!(
                matches!(
                    decide_miss(&fj, &coords, "test-pat", &fresh, &miss, "GAMMA-970"),
                    MissAction::Retry { .. }
                ),
                "an undecidable check must leave the issue retryable"
            );
        }
    }

    // ---- `## Failure` coordinate parsing (CLAUDE-185 shape) ---------------

    /// The exact `## Failure` block `mint::build_description` emits for CLAUDE-185.
    const CLAUDE_185_BODY: &str = "\
## Triage
Bucket: `human_action` (classified by Llm).

Log truncates during compilation ...

## Failure
- Repo: `initech/chat`
- Workflow: `check.yml`
- Commit: `a456a957`
- Run: https://dev.a8n.run/initech/chat/actions/runs/4034

The redacted failing-job log is attached to this issue.

<!-- claude-run dedupe markers; do not edit -->
Dedupe-Key: crdedupeaaaa crdedupebbbb";

    #[test]
    fn parses_the_claude_185_failure_block() {
        let coords = parse_failure_block(CLAUDE_185_BODY).expect("parsed");
        assert_eq!(coords.repo, "initech/chat");
        assert_eq!(coords.index_in_repo, 4034);
        assert_eq!(coords.workflow_id, "check.yml");
        assert_eq!(coords.commit_sha, "a456a957");
        assert_eq!(
            coords.html_url,
            "https://dev.a8n.run/initech/chat/actions/runs/4034"
        );
    }

    #[test]
    fn parse_returns_none_without_a_failure_block() {
        assert!(parse_failure_block("just some prose, no failure block").is_none());
    }

    #[test]
    fn run_index_is_read_from_the_run_url() {
        assert_eq!(
            run_index_from_url("https://dev.a8n.run/o/r/actions/runs/4034"),
            Some(4034)
        );
        assert_eq!(run_index_from_url("https://x/o/r/no-runs-here"), None);
    }

    // ---- prompt directs the required analysis -----------------------------

    /// The timing every prompt fixture is built with (run #4017's real values).
    fn test_timing() -> forge_log::RunTiming {
        forge_log::RunTiming {
            started: "2026-08-20T20:03:13+02:00".to_string(),
            stopped: "2026-08-20T20:27:31+02:00".to_string(),
        }
    }

    #[test]
    fn prompt_directs_step_root_cause_and_remediation() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/initech_chat-run4034-all-jobs.log"),
            workflow_path: Some(PathBuf::from("/wd/failing-workflow.yml")),
            timing: Some(test_timing()),
        };
        let p = build_prompt(&inputs, &coords);
        // References both files by name.
        assert!(p.contains("initech_chat-run4034-all-jobs.log"));
        assert!(p.contains("failing-workflow.yml"));
        // Directs the three required outputs.
        assert!(p.contains("Failing step"));
        assert!(p.contains("Root cause"));
        assert!(p.contains("Remediation"));
        // CLAUDE-236: the log this tool stores is never truncated, so the prompt
        // never offers truncation as an explanation; it asks for the two-way
        // classification instead.
        assert!(
            !p.to_lowercase().contains("truncat"),
            "the prompt must not call the stored log truncated: {p}"
        );
        assert!(p.contains("never shortens it"));
        assert!(p.contains("stopped emitting output and was killed or timed out"));
        assert!(p.contains("the failing step emitted no diagnostic at all"));
        // Read-only contract.
        assert!(p.contains("Do not modify any files"));
    }

    /// CLAUDE-236 AC: the run's `started` / `stopped` reach the prompt, with the
    /// instruction to compare the stop time against where the log stops.
    #[test]
    fn prompt_carries_the_run_timing_when_it_is_available() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/run.log"),
            workflow_path: None,
            timing: Some(test_timing()),
        };
        let p = build_prompt(&inputs, &coords);
        assert!(p.contains("started at `2026-08-20T20:03:13+02:00`"), "{p}");
        assert!(p.contains("stopped at `2026-08-20T20:27:31+02:00`"), "{p}");
        // The comparison is spelled out where the timing is given and again in
        // the root-cause instruction.
        assert!(p.contains("compare that stop time against the last timestamp in the log"));
        assert!(p.contains("the gap between the last log timestamp and the run's stop time"));
    }

    /// CLAUDE-236 AC: an unreadable timing is stated as unavailable, so the agent
    /// reports the limitation instead of inventing a reason for the log's end.
    #[test]
    fn prompt_states_the_timing_is_unavailable_when_it_could_not_be_read() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/run.log"),
            workflow_path: None,
            timing: None,
        };
        let p = build_prompt(&inputs, &coords);
        assert!(
            p.contains("could not be read from the forge"),
            "the prompt states the timing is unavailable: {p}"
        );
        assert!(!p.contains("started at `"), "{p}");
    }

    /// CLAUDE-236: a timing read that fails does not fail the fetch; the inputs
    /// are Ready with no timing (the reason is logged at `warn` by the fetch).
    #[test]
    fn a_failed_timing_read_still_yields_ready_inputs() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: "boom".to_string(),
            ..Default::default()
        }]))
        .with_timing_error("the page carries no run #4034");
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Ready(inputs) =
            fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path())
        else {
            panic!("a failed timing read must not make the log unavailable");
        };
        assert!(inputs.timing.is_none());
        let p = build_prompt(&inputs, &coords);
        assert!(p.contains("could not be read from the forge"));
    }

    /// CLAUDE-236: the timing the forge returns reaches the prompt end to end.
    #[test]
    fn a_successful_timing_read_reaches_the_prompt() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let fj = StubFj::new(RunLogsOutcome::Fetched(vec![JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: "boom".to_string(),
            ..Default::default()
        }]));
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Ready(inputs) =
            fetch_analysis_inputs(&fj, &coords, "test-pat", tmp.path())
        else {
            panic!("expected Ready");
        };
        assert_eq!(inputs.timing, Some(test_timing()));
        assert!(build_prompt(&inputs, &coords).contains("stopped at `2026-08-20T20:27:31+02:00`"));
    }

    #[test]
    fn prompt_notes_a_missing_workflow_yaml() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/run.log"),
            workflow_path: None,
            timing: Some(test_timing()),
        };
        let p = build_prompt(&inputs, &coords);
        assert!(p.contains("could not be fetched"));
    }

    // ---- CLAUDE-200: per-repo log token -----------------------------------

    /// An issue on a repo the mapping does not cover resolves to no token, and
    /// `process_job` returns Filtered at that point (before the workdir is created
    /// and before `fetch_analysis_inputs`, the only path that posts the
    /// "unavailable" note), so no note lands on another org's issue.
    #[test]
    fn an_unmapped_repo_resolves_to_no_token_so_the_issue_is_skipped() {
        use crate::action::LogTokenEntry;
        let tokens = LogTokenRef::PerRepo(vec![LogTokenEntry {
            repos: vec!["contoso/*".to_string()],
            token_ref: "forgejo-logs-contoso".to_string(),
        }]);
        // The fixture issue is on initech, which this deployment's mapping
        // does not cover.
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        assert_eq!(coords.repo, "initech/chat");
        assert!(tokens.resolve(&coords.repo).is_none());
        // The same mapping still serves its own org's issues.
        assert_eq!(
            tokens.resolve("contoso/claude-run"),
            Some("forgejo-logs-contoso")
        );
    }

    /// One mapping serves issues from two different orgs, each with its own PAT:
    /// one deployed copy of the action covers both.
    #[test]
    fn one_mapping_serves_issues_from_two_orgs() {
        use crate::action::LogTokenEntry;
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
        let psa = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let contoso = parse_failure_block(
            &CLAUDE_185_BODY.replace("initech/chat", "contoso/claude-run"),
        )
        .unwrap();
        assert_eq!(
            tokens.resolve(&psa.repo),
            Some("forgejo-logs-initech"),
            "initech issue"
        );
        assert_eq!(
            tokens.resolve(&contoso.repo),
            Some("forgejo-logs-contoso"),
            "contoso issue"
        );
    }

    // ---- comment cap -------------------------------------------------------

    #[test]
    fn cap_comment_bounds_a_huge_analysis() {
        let big = "x".repeat(100_000);
        let capped = cap_comment(&big, 1024);
        assert!(capped.len() < big.len());
        assert!(capped.contains("analysis truncated"));
        // Small analyses pass through untouched.
        assert_eq!(cap_comment("short", 1024), "short");
    }
}
