//! Native CI-failure-analysis handler (CLAUDE-189).
//!
//! Enriches every open "CI failure" / "Auto-fix" issue with a deep, log-grounded
//! root-cause comment: the exact failing workflow step, the root-cause diagnostic
//! (or an explicit statement that the stored log is truncated / diagnostic-free),
//! and concrete remediation. It is the automatic, idempotent equivalent of the
//! manual CLAUDE-185 pass.
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
//!   into the workdir. A log miss (`AuthDenied` / `TransientMiss` / `Failed`)
//!   posts a short "unavailable" note (once) and leaves the issue un-marked so a
//!   later run retries; it never crashes or marks analyzed.
//! - analyze: one agentic `claude` session (tools `Read,Grep,Bash`) over the
//!   redacted full-log file plus the workflow YAML.
//! - finalize: post the analysis as a comment (carrying the hidden analyzed
//!   marker) so the issue is never analyzed twice.
//!
//! Log fetching stays inside `forge_log` (Rust) and shells out to `fj`, never a
//! raw REST call from a nu snippet, per the tooling-gap rule. Redaction is
//! preserved end to end by reusing `forge_log::redact`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{CiFailureAnalysisConfig, LogTokenRef, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::claude::ClaudeInvoker;
use crate::error::{Error, Result};
use crate::forge_log::{
    self, ActionLogClient, HttpLogTransport, LogFetchConfig, LogOutcome, LogTarget, LogTransport,
};
use crate::job::{self, EffectiveLimits, ProcessedJob, RunnerDefaults};
use crate::provider::{self, Backend, IssueInspect};
use crate::secrets::{self, SecretsKey};
use crate::state;
use crate::workdir;

/// Hidden token embedded in the analysis comment. Its presence in the issue's
/// description or any comment means the issue is analyzed (idempotency guard).
const ANALYZED_MARKER: &str = "crcianalyzed";

/// Hidden token embedded in the "log unavailable" note, so the note is posted at
/// most once while the issue stays un-marked and retryable.
const UNAVAILABLE_MARKER: &str = "crcianalysisunavailable";

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

    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
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

    // CLAUDE-204: `fj actions logs` for the run log; the HTTP transport is only
    // for the workflow-YAML read `fj` has no command for (TODO(FJ-67)).
    let logs = crate::provider::FjActionLogs::new(std::sync::Arc::new(
        crate::provider::ProcessRunner::new(),
    ));
    let transport = HttpLogTransport::new();
    let mut processed: u32 = 0;
    for key in &keys {
        if cancel.is_cancelled() {
            break;
        }
        // Mid-run daily caps.
        if state::daily_count_get(pool, &action.name).await? >= limits.max_runs_per_day {
            break;
        }
        if state::daily_cost_get(pool, &action.name).await? >= limits.daily_budget_usd {
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
        let outcome = process_job(
            &ctx,
            backend.as_ref(),
            &cfg,
            &limits,
            &logs,
            &transport,
            &tokens,
            key,
        )
        .await;
        if let Err(e) = state::run_inflight_remove(pool, run_id, key).await {
            warn!(run_id, error = %e, "run_inflight_remove failed (continuing)");
        }
        let outcome = outcome?;
        record_outcome(&ctx, run_id, &outcome).await?;
        processed += 1;
    }

    let exit_code = if cancel.is_cancelled() {
        crate::exit::CANCELLED
    } else {
        crate::exit::OK
    };
    state::run_finish(pool, run_id, exit_code, processed).await?;
    Ok(())
}

/// Analyze one issue. Idempotent: an already-analyzed issue is skipped; a log
/// miss posts an "unavailable" note (once) and leaves the issue un-marked.
#[allow(clippy::too_many_arguments)]
async fn process_job(
    ctx: &NativeContext<'_>,
    backend: &dyn Backend,
    cfg: &CiFailureAnalysisConfig,
    limits: &EffectiveLimits,
    logs: &dyn ActionLogClient,
    transport: &dyn LogTransport,
    tokens: &LogTokenRef,
    key: &str,
) -> Result<ProcessedJob> {
    let action = &ctx.action.action;
    let started = Instant::now();

    let attempts = state::attempts_get(ctx.pool, &action.name, key).await?;
    if attempts >= limits.max_attempts_per_key {
        info!(action = %action.name, key, "attempt cap reached; skipping analysis");
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
    let _ = std::fs::remove_dir_all(&workdir_path);
    if let Err(e) = std::fs::create_dir_all(&workdir_path) {
        warn!(key, error = %e, "creating the analysis workdir failed; skipping");
        return Ok(ProcessedJob::Filtered {
            key: key.to_string(),
        });
    }

    // Fetch the complete redacted log (+ workflow YAML).
    let fetched = fetch_analysis_inputs(logs, transport, &coords, &token, &workdir_path);
    let inputs = match fetched {
        FetchResult::Ready(inputs) => inputs,
        FetchResult::Unavailable(reason) => {
            // Post the note once; leave the issue un-marked so a later run (once
            // logs flush / a token is fixed) retries. No attempt is consumed.
            if should_post_unavailable(&inspect, UNAVAILABLE_MARKER) {
                let body = build_unavailable_comment(&reason, UNAVAILABLE_MARKER);
                if let Err(e) = backend.comment(key, &body) {
                    warn!(key, error = %e, "posting the log-unavailable note failed (continuing)");
                }
            }
            info!(action = %action.name, key, reason = %reason, "log unavailable; left un-marked for retry");
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
    )?;

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
}

/// Fetch result: inputs ready, or the log is unavailable (with a reason).
enum FetchResult {
    Ready(AnalysisInputs),
    Unavailable(String),
}

/// Fetch the complete redacted run log through `fj actions logs` and, best-effort,
/// the failing workflow YAML into `out_dir`. Never errors: any miss becomes
/// [`FetchResult::Unavailable`].
fn fetch_analysis_inputs(
    logs: &dyn ActionLogClient,
    transport: &dyn LogTransport,
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
        return FetchResult::Unavailable(
            "could not derive the forge host from the run URL".to_string(),
        );
    };
    let log_path = match forge_log::fetch_run_log(
        logs,
        &fetch_cfg,
        &log_target,
        out_dir,
        &[],
        std::thread::sleep,
    ) {
        Ok(LogOutcome::Fetched(log)) => log.artifact,
        Ok(LogOutcome::AuthDenied { reason }) => {
            return FetchResult::Unavailable(format!("run-logs auth denied ({reason})"))
        }
        Ok(LogOutcome::TransientMiss { reason }) => {
            return FetchResult::Unavailable(format!(
                "run logs not available yet or pruned: {reason}"
            ))
        }
        Ok(LogOutcome::Failed { reason }) => {
            return FetchResult::Unavailable(format!("fetching the run logs failed: {reason}"))
        }
        Err(e) => return FetchResult::Unavailable(format!("fetching the run logs errored: {e}")),
    };

    // 2. Best-effort workflow YAML at the failing commit. TODO(FJ-67): still a
    // REST read because `fj` has no raw-file command.
    let Some(base_url) = forge_log::base_from_html_url(&coords.html_url, &coords.repo) else {
        return FetchResult::Ready(AnalysisInputs {
            log_path,
            workflow_path: None,
        });
    };
    let workflow_path = forge_log::fetch_workflow_yaml(
        transport,
        &base_url,
        &coords.repo,
        &coords.commit_sha,
        &coords.workflow_id,
        token,
        &[],
    )
    .and_then(|yaml| {
        let path = out_dir.join("failing-workflow.yml");
        std::fs::write(&path, yaml).ok().map(|()| path)
    });

    FetchResult::Ready(AnalysisInputs {
        log_path,
        workflow_path,
    })
}

/// Resolve the decrypted, trimmed Forgejo PAT from the secrets store.
async fn resolve_token(pool: &sqlx::SqlitePool, name: &str) -> Result<String> {
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
/// or that the log is truncated / diagnostic-free, and give remediation.
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
    [
        "You are analyzing a failed CI run to produce a deep, log-grounded root-cause report.".to_string(),
        format!(
            "The complete redacted run log is at `./{log_name}` (repo `{}`, workflow `{}`, commit `{}`, run #{}).",
            coords.repo, coords.workflow_id, coords.commit_sha, coords.index_in_repo
        ),
        format!("Run URL: {}", coords.html_url),
        workflow_line,
        String::new(),
        "Use Read/Grep/Bash to read both files. Then report, in Markdown:".to_string(),
        "1. **Failing step**: the exact workflow step that failed, identified by mapping the log's end/truncation point against the workflow's step order.".to_string(),
        "2. **Root cause**: the concrete diagnostic (the compiler/clippy/test error line). If the stored log is truncated or carries no diagnostic (it ends mid-step with no error captured), say so explicitly and give the leading hypothesis (e.g. an OOM if the workflow caps build parallelism).".to_string(),
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
    use crate::forge_log::{JobLog, RunLogsOutcome};
    use std::sync::Mutex;

    fn inspect(description: &str, comments: &[&str]) -> IssueInspect {
        IssueInspect {
            description: description.to_string(),
            comments: comments.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// A bytes transport that returns a queued response per call, for the
    /// workflow-YAML read; a miss returns 404.
    struct StubTransport {
        responses: Mutex<std::collections::VecDeque<(u16, Vec<u8>)>>,
    }
    impl StubTransport {
        fn new(responses: Vec<(u16, Vec<u8>)>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }
    impl LogTransport for StubTransport {
        fn get_bytes(
            &self,
            _url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<(u16, Vec<u8>), String> {
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or((404, Vec::new())))
        }
    }

    /// A canned `fj actions logs` client returning one fixed outcome and
    /// asserting the token the fetch was scoped with.
    struct StubLogs {
        outcome: Mutex<Option<RunLogsOutcome>>,
    }
    impl StubLogs {
        fn new(outcome: RunLogsOutcome) -> Self {
            Self {
                outcome: Mutex::new(Some(outcome)),
            }
        }
    }
    impl ActionLogClient for StubLogs {
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
        let logs = StubLogs::new(RunLogsOutcome::Fetched(vec![JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: log.to_string(),
        }]));
        // Workflow-YAML candidates all 404 (the stub's default), so no yaml is written.
        let transport = StubTransport::new(vec![]);
        let tmp = tempfile::tempdir().unwrap();
        let FetchResult::Ready(inputs) =
            fetch_analysis_inputs(&logs, &transport, &coords, "test-pat", tmp.path())
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
        let logs = StubLogs::new(RunLogsOutcome::NotFound("no action run #4034".to_string()));
        let transport = StubTransport::new(vec![]);
        let tmp = tempfile::tempdir().unwrap();
        match fetch_analysis_inputs(&logs, &transport, &coords, "test-pat", tmp.path()) {
            FetchResult::Unavailable(reason) => {
                assert!(reason.contains("no action run"), "{reason}")
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
        let logs = StubLogs::new(RunLogsOutcome::AuthDenied("HTTP 403: ...".to_string()));
        let transport = StubTransport::new(vec![]);
        let tmp = tempfile::tempdir().unwrap();
        match fetch_analysis_inputs(&logs, &transport, &coords, "test-pat", tmp.path()) {
            FetchResult::Unavailable(reason) => assert!(reason.contains("auth denied"), "{reason}"),
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

    #[test]
    fn prompt_directs_step_root_cause_and_remediation() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/initech_chat-run4034-all-jobs.log"),
            workflow_path: Some(PathBuf::from("/wd/failing-workflow.yml")),
        };
        let p = build_prompt(&inputs, &coords);
        // References both files by name.
        assert!(p.contains("initech_chat-run4034-all-jobs.log"));
        assert!(p.contains("failing-workflow.yml"));
        // Directs the three required outputs.
        assert!(p.contains("Failing step"));
        assert!(p.contains("Root cause"));
        assert!(p.contains("Remediation"));
        // Names the truncated / diagnostic-free case explicitly.
        assert!(p.to_lowercase().contains("truncated"));
        // Read-only contract.
        assert!(p.contains("Do not modify any files"));
    }

    #[test]
    fn prompt_notes_a_missing_workflow_yaml() {
        let coords = parse_failure_block(CLAUDE_185_BODY).unwrap();
        let inputs = AnalysisInputs {
            log_path: PathBuf::from("/wd/run.log"),
            workflow_path: None,
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
