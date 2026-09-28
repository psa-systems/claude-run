//! Failure triage engine.
//!
//! A verified `action_run_failure` event plus its redacted job log
//! ([`crate::forge_log`]) is sorted into one of three buckets:
//!
//! 1. [`TriageBucket::CodeFix`]  - a PR check failed and a code change on the PR
//!    branch will fix it (the CI-fix handler can act).
//! 2. [`TriageBucket::HumanAction`] - no code change on a PR fixes it: a push to
//!    a protected/main branch, a tag / release / build-publish failure with no PR
//!    to fix, a missing secret / config, or an infra change only a human can make.
//! 3. [`TriageBucket::Transient`] - environmental / infra / flaky: runner offline,
//!    registry 401, the docker fast-fail, a not-ready / pruned log, a network blip.
//!
//! The engine is **hybrid**: a cheap deterministic [`triage_rules`] pass runs
//! first and short-circuits clear transient/infra failures to bucket 3 with *no*
//! LLM call. Only the residue it cannot label is handed to a [`FailureClassifier`]
//! (the LLM pass), which separates bucket 1 from bucket 2 and writes the analysis
//! string. The classifier is a trait so tests inject a stub and never shell out;
//! the production [`AnalysisClassifier`] reuses the existing
//! [`ClaudeInvoker`] (`claude --print`) the runner already uses, running one
//! tool-using session (`Read,Grep,Bash`) over the COMPLETE log plus the failing
//! workflow YAML and ending with a fenced JSON verdict
//! ([`parse_analysis_verdict`]). This replaces the old two-pass split (a
//! tool-less classifier at ingest bounded by [`bound_log`], a separate scheduled
//! deep analysis later): one session now produces both the routing decision and
//! the analysis text the minted issue carries.
//!
//! Wiring the result into the issue-minting step is
//! [`crate::web::webhooks::consume_events`], which runs each event's triage +
//! mint under a worker pool bounded by `config.webhook.analysis_concurrency`.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::action::ClaudeBlock;
use crate::autofix::{self, AutoFix};
use crate::cancel::CancellationToken;
use crate::ci_failure::analysis::{self as analysis, AnalysisInputs, RunCoordinates};
use crate::claude::ClaudeInvoker;
use crate::config::AutoFixConfig;
use crate::db::DbPool;
use crate::error::Result;
use crate::forge_log::ForgeReads;

/// Default cap (bytes) for the log text inlined into the classifier prompt.
pub const DEFAULT_MAX_LOG_BYTES: usize = 48 * 1024;

/// The three triage buckets. Lowercase `snake_case` is the stable wire form
/// (used in the classifier reply and any persistence).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriageBucket {
    /// Bucket 1: a PR check failed; a code change on the PR branch fixes it.
    CodeFix,
    /// Bucket 2: no PR code change fixes it; a human must act.
    HumanAction,
    /// Bucket 3: transient / infra / flaky; a retry may simply succeed.
    Transient,
    /// A mechanically-fixable verification failure (cargo fmt, a
    /// failing checks workflow). Routes like [`TriageBucket::CodeFix`] (mint a
    /// queued issue so the runner opens a corrective PR). Applies
    /// only when the failing branch has no open PR (the canonical case is a push
    /// to a protected/main branch); a failing check on an open, unmerged PR
    /// falls through to the classifier instead. Set only by the deterministic
    /// [`autofix`] detectors, never the LLM.
    AutoFix,
}

impl TriageBucket {
    /// Stable lowercase string form.
    pub fn as_str(&self) -> &'static str {
        match self {
            TriageBucket::CodeFix => "code_fix",
            TriageBucket::HumanAction => "human_action",
            TriageBucket::Transient => "transient",
            TriageBucket::AutoFix => "auto_fix",
        }
    }
}

impl fmt::Display for TriageBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TriageBucket {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "code_fix" => Ok(TriageBucket::CodeFix),
            "human_action" => Ok(TriageBucket::HumanAction),
            "transient" => Ok(TriageBucket::Transient),
            "auto_fix" => Ok(TriageBucket::AutoFix),
            other => Err(format!("unknown triage bucket: {other}")),
        }
    }
}

/// Which pass produced a [`Triage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassifiedBy {
    /// The deterministic [`triage_rules`] pass.
    Rules,
    /// The [`FailureClassifier`] (LLM) pass.
    Llm,
}

/// The typed triage result consumed by the issue-minting subtask.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Triage {
    /// The bucket this failure was sorted into.
    pub bucket: TriageBucket,
    /// Human-readable root-cause / next-step analysis.
    pub analysis: String,
    /// Which pass produced the verdict.
    pub classified_by: ClassifiedBy,
    /// The auto-fix detail when `bucket == AutoFix`, carried through
    /// to the mint so the issue summary / description name the concrete problem
    /// and fix command. `None` for every other bucket.
    #[serde(default)]
    pub auto_fix: Option<AutoFix>,
}

impl Triage {
    fn rules(bucket: TriageBucket, analysis: impl Into<String>) -> Self {
        Self {
            bucket,
            analysis: analysis.into(),
            classified_by: ClassifiedBy::Rules,
            auto_fix: None,
        }
    }

    fn llm(bucket: TriageBucket, analysis: impl Into<String>) -> Self {
        Self {
            bucket,
            analysis: analysis.into(),
            classified_by: ClassifiedBy::Llm,
            auto_fix: None,
        }
    }

    /// A deterministic auto-fix verdict carrying its detector detail.
    /// The analysis mirrors the detector's problem statement so existing
    /// analysis renderers stay coherent.
    fn auto_fix(detail: AutoFix) -> Self {
        Self {
            bucket: TriageBucket::AutoFix,
            analysis: detail.problem.clone(),
            classified_by: ClassifiedBy::Rules,
            auto_fix: Some(detail),
        }
    }
}

/// Everything the engine needs about one verified failure: the metadata from the
/// `action_run_failure` event plus its redacted log.
#[derive(Debug, Clone)]
pub struct TriageInput {
    /// `owner/repo`.
    pub repo: String,
    /// Workflow filename, e.g. `ci.yml`.
    pub workflow_id: String,
    /// Failing commit SHA.
    pub commit_sha: String,
    /// Run URL, for the analysis / human context.
    pub html_url: String,
    /// Redacted log text (empty when unavailable; see `log_available`).
    pub log: String,
    /// False when the log could not be fetched. A real failure with
    /// no obtainable log routes to a HUMAN (bucket 2) so it can never be
    /// silently dropped; `log_unavailable_reason` says why in the analysis.
    pub log_available: bool,
    /// Why the log is missing (unauthorized private repo, exhausted
    /// flush retries, fetch error). `None` when `log_available` is true.
    pub log_unavailable_reason: Option<String>,
    /// Whether the failing branch has an open PR. Strong bucket-1-vs-2 signal
    /// passed to the classifier (bucket 1 needs a PR to fix).
    pub has_open_pr: bool,
    /// The resolved `log_token_ref` secret that authorized the log
    /// fetch, carried through so [`AnalysisClassifier`] can fetch the failing
    /// workflow YAML with the same credential. `None` reads it unauthenticated
    /// (a public repo).
    pub log_token: Option<String>,
}

/// Infra / transient signatures (lowercased substrings) that mark a bucket-3
/// failure unambiguously enough to skip the LLM. Curated to stay clear of
/// ordinary test-failure output.
const TRANSIENT_SIGNATURES: &[&str] = &[
    // Runner availability.
    "no runner",
    "waiting for a runner",
    "runner is offline",
    "no matching runner",
    // Registry / image pull.
    "401 unauthorized",
    "unauthorized: authentication required",
    "denied: requested access to the resource is denied",
    "toomanyrequests",
    "received unexpected http status",
    "manifest unknown",
    // Network / DNS / upstream.
    "connection reset",
    "connection refused",
    "i/o timeout",
    "tls handshake timeout",
    "temporary failure in name resolution",
    "503 service unavailable",
    "502 bad gateway",
    "504 gateway",
    "dial tcp",
];

/// Deterministic first pass. Returns `Some` only for a clear transient/infra
/// failure (bucket 3), so the caller skips the LLM; `None` for everything else,
/// which the caller hands to the [`FailureClassifier`].
pub fn triage_rules(input: &TriageInput) -> Option<Triage> {
    // A real failure whose log could not be obtained routes to a
    // HUMAN, never to the silent transient bucket. The original rule treated
    // every miss as "not flushed yet, a retry will get it", which quietly
    // swallowed failures on private repos (whose logs are unreadable on this
    // Forgejo version) and any other permanently-unfetchable log. Non-failure
    // statuses never reach triage at all, so everything here IS a
    // failure someone should see.
    if !input.log_available {
        let why = input
            .log_unavailable_reason
            .as_deref()
            .unwrap_or("log not available (no reason recorded)");
        return Some(Triage::rules(
            TriageBucket::HumanAction,
            format!("failing job log unavailable: {why}; routed to a human with no log attached"),
        ));
    }

    let haystack = input.log.to_lowercase();

    // A job the runner skipped (workflow `if` guard evaluated false) never ran:
    // Forgejo still fires `action_run_failure` for the run, but there is nothing
    // to fix and nothing for a human to act on. The runner emits
    // `Skipping job '<name>' due to '<expr>'` only for the job it is skipping,
    // and a job log carries only that job's output, so an executed-and-failed
    // job cannot match. Dedicated branch (not a TRANSIENT_SIGNATURES entry)
    // because the match needs both substrings and the analysis should name the
    // skip semantics, not a generic infra signature.
    if haystack.contains("skipping job '") && haystack.contains("' due to '") {
        return Some(Triage::rules(
            TriageBucket::Transient,
            "job was skipped by its workflow condition ('Skipping job ... due to ...'); nothing ran, no issue needed",
        ));
    }

    // A runner that cannot reach its network is an operator concern
    // no corrective PR can fix. It mints one grouped, branch-agnostic issue
    // (keyed on the infra kind) for a human instead of vanishing as transient.
    // This runs BEFORE the transient scan so the detector owns its phrases.
    if let Some(infra) = crate::infra_failure::detect(&input.log) {
        if infra.kind == "runner-unavailable" {
            return Some(Triage::rules(TriageBucket::HumanAction, infra.problem));
        }
    }

    if let Some(sig) = TRANSIENT_SIGNATURES
        .iter()
        .copied()
        .find(|sig| haystack.contains(*sig))
    {
        return Some(Triage::rules(
            TriageBucket::Transient,
            format!("log matched transient/infra signature {sig:?}"),
        ));
    }

    None
}

/// The LLM pass, behind a trait so tests inject a stub and production shells out
/// to `claude`. Implementations classify the residue into bucket 1 vs 2 and write
/// the analysis; they must never panic and should degrade rather than propagate.
pub trait FailureClassifier: Send + Sync {
    /// Classify a non-transient failure (the rules abstained). Returning `Err`
    /// is allowed; [`triage`] degrades it to [`TriageBucket::HumanAction`].
    ///
    /// `spend` is the caller's own per-delivery ledger: an
    /// implementation that shells out to `claude` records its cost onto it
    /// (and caps the child at `spend.remaining_usd()`) rather than owning a
    /// ledger of its own, since a classifier is a daemon-lifetime `Arc`
    /// shared across concurrent deliveries and a ledger held on it would race.
    /// The caller reads the ledger back after `classify` returns and finishes
    /// it exactly once, so an implementation must never call
    /// [`crate::usage::RunSpend::finish`] itself.
    fn classify(&self, input: &TriageInput, spend: &crate::usage::RunSpend) -> Result<Triage>;
}

/// Hybrid triage: deterministic rules first, then the deterministic auto-fix
/// detectors, and only then the `classifier` for the residue. A classifier error
/// degrades to [`TriageBucket::HumanAction`] (a human looks) rather than failing
/// the pipeline.
///
/// The [`autofix`] detectors run AFTER the transient/no-log rules
/// (so a flaky run of a checks workflow is still classed transient, never
/// auto-fixed) but BEFORE the LLM. They run only when the failing
/// branch has no open PR (the canonical case is a push to a protected/main
/// branch); a failing check on an open, unmerged PR skips auto-fix and falls
/// through to the classifier, so the runner never opens a *new* corrective PR
/// against an unmerged PR. When the detectors do run, a match short-circuits to
/// [`TriageBucket::AutoFix`], so a mechanically-fixable push-to-main failure
/// never reaches the classifier prompt that would label it `human_action`.
pub fn triage(
    input: &TriageInput,
    classifier: &dyn FailureClassifier,
    autofix_cfg: &AutoFixConfig,
    spend: &crate::usage::RunSpend,
) -> Triage {
    if let Some(verdict) = triage_rules(input) {
        return verdict;
    }
    // Auto-fix only failures on a branch with no open PR. A failing
    // check on an open, unmerged PR is that PR's own concern; minting a queued
    // issue here would open a new corrective PR against the unmerged PR, which
    // causes more confusion than it fixes.
    if !input.has_open_pr {
        if let Some(detail) = autofix::detect(input, autofix_cfg) {
            return Triage::auto_fix(detail);
        }
    }
    match classifier.classify(input, spend) {
        Ok(verdict) => verdict,
        Err(e) => Triage::llm(
            TriageBucket::HumanAction,
            format!("classification failed ({e}); routing to a human"),
        ),
    }
}

/// Bound `log` to at most `max_bytes`, keeping a head and a (larger) tail joined
/// by a truncation marker - the root cause usually sits at the end of a CI log,
/// but the head carries the workflow/setup context. UTF-8 safe.
pub fn bound_log(log: &str, max_bytes: usize) -> String {
    if log.len() <= max_bytes {
        return log.to_string();
    }
    let head_budget = max_bytes / 4;
    let tail_budget = max_bytes - head_budget;
    let head = take_prefix(log, head_budget);
    let tail = take_suffix(log, tail_budget);
    let dropped = log.len() - head.len() - tail.len();
    format!("{head}\n...[{dropped} bytes truncated]...\n{tail}")
}

/// Largest char-boundary prefix of `s` no longer than `max` bytes.
fn take_prefix(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Largest char-boundary suffix of `s` no longer than `max` bytes.
fn take_suffix(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut start = s.len() - max;
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

/// Find and parse the LAST balanced `{...}` JSON object in `s`, scanning
/// backward from the final `}` and matching braces so a Markdown report
/// (which may itself contain `{`/`}`, e.g. in a code block) before the fenced
/// verdict does not corrupt the span. `None` when `s` has no `}` or the
/// balanced span is not valid JSON.
fn extract_last_json_object(s: &str) -> Option<Value> {
    let bytes = s.as_bytes();
    let end = s.rfind('}')?;
    let mut depth = 0i32;
    let mut start = None;
    for (i, &b) in bytes[..=end].iter().enumerate().rev() {
        match b {
            b'}' => depth += 1,
            b'{' => {
                depth -= 1;
                if depth == 0 {
                    start = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let start = start?;
    serde_json::from_str(&s[start..=end]).ok()
}

/// The four verdicts [`AnalysisClassifier`]'s prompt asks for, and
/// how each routes.
const VERDICT_CODE_DEFECT: &str = "code_defect";
const VERDICT_NEEDS_HUMAN: &str = "needs_human";
const VERDICT_FLAKY: &str = "flaky";
const VERDICT_INFRA: &str = "infra";

/// Parse an [`AnalysisClassifier`] reply into a [`Triage`]. Reads the LAST
/// JSON object in the reply (the fenced verdict [`analysis::build_prompt`]
/// asks for at the end): `{"verdict": "code_defect"|"needs_human"|"flaky"|
/// "infra", "failing_tests": [...], "summary": "..."}`. `code_defect` maps to
/// [`TriageBucket::CodeFix`], `needs_human` to [`TriageBucket::HumanAction`],
/// and `flaky` / `infra` both to [`TriageBucket::Transient`] (neither
/// warrants a fix PR, and the existing dedupe/reopen machinery
/// already handles a flake resurfacing). An unparseable or missing verdict
/// degrades to [`TriageBucket::HumanAction`] and logs at `warn` with a reply
/// excerpt - it must NEVER silently become `code_fix`. The full Markdown
/// report is kept as the analysis text (falling back to the parsed "summary"
/// only when the whole reply is empty), so the issue carries the deep
/// analysis, not just the one-line verdict summary.
pub fn parse_analysis_verdict(reply: &str) -> Triage {
    if let Some(obj) = extract_last_json_object(reply) {
        if let Some(bucket) = obj
            .get("verdict")
            .and_then(Value::as_str)
            .and_then(|v| match v {
                VERDICT_CODE_DEFECT => Some(TriageBucket::CodeFix),
                VERDICT_NEEDS_HUMAN => Some(TriageBucket::HumanAction),
                VERDICT_FLAKY | VERDICT_INFRA => Some(TriageBucket::Transient),
                _ => None,
            })
        {
            let analysis_text = if reply.trim().is_empty() {
                obj.get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            } else {
                reply.trim().to_string()
            };
            return Triage::llm(bucket, analysis_text);
        }
    }
    warn!(
        reply_excerpt = %reply.chars().take(500).collect::<String>(),
        "analysis reply carried no parseable verdict; routing to a human"
    );
    Triage::llm(
        TriageBucket::HumanAction,
        "the analysis reply carried no parseable verdict; routing to a human review",
    )
}

/// Production [`FailureClassifier`]: one tool-using `claude`
/// session per failure, over the COMPLETE log (not the bounded copy the old
/// [`bound_log`]-based classifier used) plus the failing workflow YAML, ending
/// with a fenced JSON verdict ([`parse_analysis_verdict`]) that routes the
/// failure. Replaces the old two-pass split (a tool-less classifier at ingest,
/// a separate scheduled deep-analysis action over the full log later): the
/// analysis and the routing decision now come from
/// the SAME session, so the
/// deep, log-grounded report is available before the issue is even minted.
/// Any non-completion reply degrades to human review, same as the old
/// classifier.
pub struct AnalysisClassifier {
    invoker: Arc<ClaudeInvoker>,
    claude_cfg: ClaudeBlock,
    timeout: Duration,
    /// `webhook-logs/analysis/`; each classification gets its own
    /// `<owner>_<repo>-run<N>/` subdirectory, removed when the classification
    /// returns.
    analysis_base_dir: PathBuf,
    logs_dir: PathBuf,
    fj: Arc<dyn ForgeReads>,
    /// Where this classification's `claude_invocations` row is
    /// written. `classify` is synchronous and always run inside
    /// `tokio::task::spawn_blocking` (never `block_in_place`), so bridging into
    /// the async write with `Handle::current().block_on` blocks only that
    /// dedicated blocking-pool thread, not an async worker.
    pool: DbPool,
}

impl AnalysisClassifier {
    /// Build a classifier. `analysis_base_dir` is `webhook-logs/analysis/`
    /// (each call gets its own subdirectory under it); `logs_dir` is where the
    /// per-classification NDJSON stream log lands (see
    /// [`ClaudeInvoker::stream_log_path`]).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        invoker: Arc<ClaudeInvoker>,
        claude_cfg: ClaudeBlock,
        timeout: Duration,
        analysis_base_dir: PathBuf,
        logs_dir: PathBuf,
        fj: Arc<dyn ForgeReads>,
        pool: DbPool,
    ) -> Self {
        Self {
            invoker,
            claude_cfg,
            timeout,
            analysis_base_dir,
            logs_dir,
            fj,
            pool,
        }
    }

    /// This classification's dedicated working directory,
    /// `<owner>_<repo>-run<N>/` under `analysis_base_dir`.
    fn event_dir(&self, coords: &RunCoordinates) -> PathBuf {
        let slug = coords.repo.replace('/', "_");
        self.analysis_base_dir
            .join(format!("{slug}-run{}", coords.index_in_repo))
    }
}

impl FailureClassifier for AnalysisClassifier {
    fn classify(&self, input: &TriageInput, spend: &crate::usage::RunSpend) -> Result<Triage> {
        let Some(index_in_repo) = analysis::run_index_from_url(&input.html_url) else {
            warn!(html_url = %input.html_url, "could not derive the run number from the run URL; routing to a human");
            return Ok(Triage::llm(
                TriageBucket::HumanAction,
                "could not derive the run number from the run URL; routing to a human",
            ));
        };
        let coords = RunCoordinates {
            repo: input.repo.clone(),
            index_in_repo,
            commit_sha: input.commit_sha.clone(),
            workflow_id: input.workflow_id.clone(),
            html_url: input.html_url.clone(),
        };
        let cwd = self.event_dir(&coords);
        // The directory is removed when the classification
        // returns, on every exit path (an early `?` included), so a failure
        // never leaks a live event's workdir. The redacted log artifact under
        // `webhook-logs/` (a sibling of `analysis/`, written by the caller
        // before triage ever runs) is untouched by this guard.
        struct CleanupGuard(PathBuf);
        impl Drop for CleanupGuard {
            fn drop(&mut self) {
                if let Err(e) = std::fs::remove_dir_all(&self.0) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(dir = %self.0.display(), error = %e, "removing the per-event analysis directory failed");
                    }
                }
            }
        }
        if let Err(e) = std::fs::create_dir_all(&cwd) {
            warn!(dir = %cwd.display(), error = %e, "creating the analysis workdir failed; routing to a human");
            return Ok(Triage::llm(
                TriageBucket::HumanAction,
                format!("could not create the analysis workdir ({e}); routing to a human"),
            ));
        }
        let _cleanup = CleanupGuard(cwd.clone());

        let log_path = cwd.join("run.log");
        if let Err(e) = std::fs::write(&log_path, &input.log) {
            warn!(path = %log_path.display(), error = %e, "writing the run log for analysis failed; routing to a human");
            return Ok(Triage::llm(
                TriageBucket::HumanAction,
                format!("could not write the run log for analysis ({e}); routing to a human"),
            ));
        }

        // Best-effort workflow YAML + timing, same as the (now-retired) scheduled
        // analysis: neither failure blocks the analysis, which proceeds over the
        // log alone and says so.
        let workflow_path = crate::forge_log::host_from_html_url(&input.html_url).and_then(|host| {
            crate::forge_log::read_workflow_yaml(
                self.fj.as_ref(),
                &host,
                &input.repo,
                &input.commit_sha,
                &input.workflow_id,
                input.log_token.as_deref(),
                &[],
            )
            .and_then(|yaml| {
                let path = cwd.join("failing-workflow.yml");
                match std::fs::write(&path, yaml) {
                    Ok(()) => Some(path),
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "writing the failing workflow YAML failed; the analysis proceeds over the log alone");
                        None
                    }
                }
            })
        });
        let timing = crate::forge_log::host_from_html_url(&input.html_url).and_then(|host| {
            match self
                .fj
                .run_timing(&host, &input.repo, coords.index_in_repo, input.log_token.as_deref())
            {
                Ok(t) => Some(t),
                Err(reason) => {
                    warn!(repo = %input.repo, run = coords.index_in_repo, %reason, "reading the run timing failed; the analysis states the timing is unavailable");
                    None
                }
            }
        });
        let inputs = AnalysisInputs {
            log_path,
            workflow_path,
            timing,
        };
        let prompt = analysis::build_prompt(&inputs, &coords);

        let stream_log = ClaudeInvoker::stream_log_path(
            &self.logs_dir,
            "webhook-triage-analysis",
            &format!("{}#{}", coords.repo, coords.index_in_repo),
            1,
        );
        // Classification is not cancellable on its own; a fresh token never trips.
        let cancel = CancellationToken::new();
        let result = self.invoker.invoke(
            &prompt,
            &self.claude_cfg,
            self.timeout,
            &cwd,
            &stream_log,
            &cancel,
            // Cap the child at the caller's remaining per-delivery ceiling.
            Some(spend.remaining_usd()),
        )?;
        // Fold this call's tokens and cost into the caller's own
        // ledger; the caller (the webhook mint consumer) owns the ledger and
        // finishes it exactly once, after `classify` returns, with the run's
        // real `run_id`.
        spend.record(&result);
        // This classification has neither a scheduled job key nor
        // a run row, so both are `None`. `classify` runs inside
        // `spawn_blocking` (see the `pool` field doc), so blocking this thread
        // on the async write is safe.
        tokio::runtime::Handle::current().block_on(crate::usage::record_invocation(
            &self.pool,
            crate::web::webhooks::WEBHOOK_MINT_ACTION,
            None,
            None,
            &stream_log,
            &result,
        ));
        if !result.ok {
            // Surface the reason claude actually reported
            // (`result.summary`, built by `friendly_failure_summary`) instead
            // of the opaque `claude exited N`, so the minted issue names the
            // real cause. Fall back to `result.error` only when the summary is
            // empty. Flag a transient claude error (rate / usage limit) so the
            // reader knows a re-run may classify cleanly rather than treating it
            // as a CI bug.
            let reason = if result.summary.trim().is_empty() {
                result.error.trim().to_string()
            } else {
                result.summary.trim().to_string()
            };
            let transient_note = if result.transient {
                " This looks like a transient claude error (e.g. rate / usage limit), not a CI problem; re-running may classify it cleanly."
            } else {
                ""
            };
            return Ok(Triage::llm(
                TriageBucket::HumanAction,
                format!(
                    "Automated analysis did not complete ({reason}); routing to a human.{transient_note}"
                ),
            ));
        }
        Ok(parse_analysis_verdict(&result.summary))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Canned classifier that records how many times it was consulted, so a test
    /// can prove the rules short-circuited before any LLM call.
    struct StubClassifier {
        reply: Triage,
        calls: AtomicUsize,
    }

    impl StubClassifier {
        fn new(reply: Triage) -> Self {
            Self {
                reply,
                calls: AtomicUsize::new(0),
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl FailureClassifier for StubClassifier {
        fn classify(
            &self,
            _input: &TriageInput,
            _spend: &crate::usage::RunSpend,
        ) -> Result<Triage> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.reply.clone())
        }
    }

    /// A fresh, unbounded per-test ledger; no assertion in this module reads
    /// it, so its budget never matters.
    fn test_spend() -> crate::usage::RunSpend {
        crate::usage::RunSpend::new("test", crate::usage::RunBudget::new(1000.0, 0.8), None)
    }

    fn input_with_log(log: &str) -> TriageInput {
        TriageInput {
            repo: "contoso/claude-run".to_string(),
            workflow_id: "ci.yml".to_string(),
            commit_sha: "abc123".to_string(),
            log_unavailable_reason: None,
            html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
            log: log.to_string(),
            log_available: true,
            has_open_pr: true,
            log_token: None,
        }
    }

    /// Default auto-fix config for the hybrid-triage tests. The cargo-fmt
    /// detector is signature-driven and the checks-workflow list defaults to
    /// `["check.yml"]`; `input_with_log` uses `ci.yml`, so the existing
    /// non-auto-fix cases fall through to the classifier unchanged.
    fn autofix_cfg() -> AutoFixConfig {
        AutoFixConfig::default()
    }

    // ---- bucket string round-trip ----------------------------------------

    #[test]
    fn bucket_roundtrips_through_lowercase_form() {
        for (bucket, s) in [
            (TriageBucket::CodeFix, "code_fix"),
            (TriageBucket::HumanAction, "human_action"),
            (TriageBucket::Transient, "transient"),
            (TriageBucket::AutoFix, "auto_fix"),
        ] {
            assert_eq!(bucket.to_string(), s);
            assert_eq!(s.parse::<TriageBucket>().unwrap(), bucket);
            assert_eq!(serde_json::to_string(&bucket).unwrap(), format!("\"{s}\""));
        }
        assert!("nonsense".parse::<TriageBucket>().is_err());
    }

    // ---- AC 1: rules short-circuit transient/infra to bucket 3, no LLM ----

    #[test]
    fn rules_classify_each_transient_signature() {
        let cases = [
            "Error: No runner is available for this job",
            "denied: requested access to the resource is denied",
            "Get \"https://registry/v2/\": 401 Unauthorized",
            "dial tcp 10.0.0.1:443: connect: connection refused",
            "received unexpected HTTP status: 503 Service Unavailable",
        ];
        for log in cases {
            let verdict = triage_rules(&input_with_log(log))
                .unwrap_or_else(|| panic!("expected a rules verdict for {log:?}"));
            assert_eq!(verdict.bucket, TriageBucket::Transient, "{log:?}");
            assert_eq!(verdict.classified_by, ClassifiedBy::Rules, "{log:?}");
        }
    }

    #[test]
    fn rules_route_runner_unavailable_to_a_human() {
        // The detector's network conditions leave the transient
        // bucket; a reset on a line naming a registry counts, a bare one does not.
        for log in [
            "could not resolve host: registry.example.com",
            "error: failed to fetch from registry: connection reset by peer",
        ] {
            let verdict = triage_rules(&input_with_log(log))
                .unwrap_or_else(|| panic!("expected a rules verdict for {log:?}"));
            assert_eq!(verdict.bucket, TriageBucket::HumanAction, "{log:?}");
            assert_eq!(verdict.classified_by, ClassifiedBy::Rules, "{log:?}");
        }
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "unused"));
        let verdict = triage(
            &input_with_log("could not resolve host: index.crates.io"),
            &stub,
            &autofix_cfg(),
            &test_spend(),
        );
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert_eq!(stub.calls(), 0, "the classifier must NOT be consulted");
    }

    #[test]
    fn rules_route_unavailable_log_to_a_human() {
        // A real failure with no obtainable log must surface to a
        // human (bucket 2), never silently drop as transient; the analysis
        // names the recorded reason.
        let mut input = input_with_log("");
        input.log_available = false;
        input.log_unavailable_reason = Some(
            "private repo: unauthorized (Forgejo's internal logs route has no token auth)"
                .to_string(),
        );
        let verdict = triage_rules(&input).unwrap();
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert!(
            verdict.analysis.contains("private repo: unauthorized"),
            "analysis names the reason: {}",
            verdict.analysis
        );

        // No recorded reason still routes to a human with a generic analysis.
        input.log_unavailable_reason = None;
        let verdict = triage_rules(&input).unwrap();
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert!(verdict.analysis.contains("no reason recorded"));
    }

    #[test]
    fn rules_abstain_on_an_ordinary_test_failure() {
        let log =
            "running 12 tests\ntest auth::login ... FAILED\nassertion failed: left == right\n";
        assert!(triage_rules(&input_with_log(log)).is_none());
    }

    #[test]
    fn rules_classify_a_condition_skipped_job_as_transient() {
        // The job never ran; the
        // runner skipped it because the workflow `if` guard evaluated false.
        let log = "2026-06-07T03:21:19Z host-a(version:v12.6.4) received task 10721 of job create-release, be triggered by event: pull_request\n\
            workflow prepared\n\
            evaluating expression 'github.event.pull_request.merged == true'\n\
            expression 'github.event.pull_request.merged == true' evaluated to 'false'\n\
            Skipping job 'Create release from merged PR' due to 'github.event.pull_request.merged == true'\n";
        let verdict = triage_rules(&input_with_log(log)).expect("skip log gets a rules verdict");
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert!(
            verdict
                .analysis
                .contains("skipped by its workflow condition"),
            "analysis names the skip: {}",
            verdict.analysis
        );
    }

    #[test]
    fn rules_abstain_when_a_failure_log_merely_mentions_skipped() {
        // "skipped" alone (e.g. test harness output) must not match; the rule
        // needs the runner's full `Skipping job '<name>' due to '<expr>'` shape.
        let log = "running 12 tests\ntest auth::login ... FAILED\n3 tests skipped\n\
            note: job skipping is configured elsewhere\n";
        assert!(triage_rules(&input_with_log(log)).is_none());
    }

    #[test]
    fn hybrid_short_circuits_a_skipped_job_without_calling_classifier() {
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "unused"));
        let log = "Skipping job 'release' due to 'startsWith(github.ref, refs/tags)'";
        let verdict = triage(&input_with_log(log), &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(stub.calls(), 0, "classifier untouched");
    }

    // ---- AC 2/4: hybrid falls through to the (mockable) LLM ---------------

    #[test]
    fn hybrid_falls_through_to_classifier_for_non_transient() {
        let stub = StubClassifier::new(Triage::llm(
            TriageBucket::CodeFix,
            "the unit test on the PR branch is failing; fix the assertion",
        ));
        let verdict = triage(
            &input_with_log("test foo ... FAILED"),
            &stub,
            &autofix_cfg(),
            &test_spend(),
        );
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        assert_eq!(verdict.classified_by, ClassifiedBy::Llm);
        assert!(!verdict.analysis.is_empty());
        assert_eq!(stub.calls(), 1, "the classifier must be consulted");
    }

    #[test]
    fn hybrid_short_circuits_rules_without_calling_classifier() {
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "unused"));
        let verdict = triage(
            &input_with_log("fatal: connection reset by peer"),
            &stub,
            &autofix_cfg(),
            &test_spend(),
        );
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert_eq!(stub.calls(), 0, "the classifier must NOT be consulted");
    }

    // ---- Deterministic auto-fix routing -----------------------

    #[test]
    fn cargo_fmt_failure_on_push_to_main_routes_to_auto_fix_not_human() {
        // AC 1 + AC 3: a check.yml run, NO open PR (push to main), failing on
        // `cargo fmt --check` is routed deterministically to the auto-fix bucket
        // with no LLM call - it never reaches the classifier that would label a
        // push-to-main failure human_action.
        let stub = StubClassifier::new(Triage::llm(TriageBucket::HumanAction, "would be wrong"));
        let mut input = input_with_log(
            "Run cargo fmt --check\nDiff in /build/src/main.rs at line 9:\n-let a=1;\n+let a = 1;\n",
        );
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = false;
        let verdict = triage(&input, &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::AutoFix);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert_eq!(stub.calls(), 0, "auto-fix short-circuits before the LLM");
        let detail = verdict.auto_fix.expect("auto-fix detail carried");
        assert_eq!(detail.kind, "cargo-fmt");
        assert_eq!(detail.fix_commands, vec!["cargo fmt --all".to_string()]);
    }

    #[test]
    fn check_yml_failure_routes_to_auto_fix_only_without_an_open_pr() {
        // AC 3: the checks-workflow detector fires for a non-fmt
        // check.yml failure, but ONLY when the failing branch has no open PR.
        // With an open PR the failure is that PR's own concern, so triage skips
        // auto-fix and hands it to the classifier instead of opening a new
        // corrective PR against the unmerged PR.
        let log = "##[group]Clippy\nerror: unused variable\n##[error]the step has failed\n";

        // No open PR (push to main): routed to auto-fix, no LLM call.
        let stub = StubClassifier::new(Triage::llm(TriageBucket::HumanAction, "unused"));
        let mut input = input_with_log(log);
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = false;
        let verdict = triage(&input, &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::AutoFix);
        assert_eq!(verdict.auto_fix.unwrap().kind, "checks-workflow");
        assert_eq!(stub.calls(), 0, "auto-fix short-circuits before the LLM");

        // Open PR: auto-fix is skipped and the classifier decides instead.
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "fix the PR branch"));
        let mut input = input_with_log(log);
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = true;
        let verdict = triage(&input, &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        assert!(verdict.auto_fix.is_none());
        assert_eq!(
            stub.calls(),
            1,
            "the classifier is consulted for an open-PR failure"
        );
    }

    #[test]
    fn cargo_fmt_failure_on_an_open_pr_skips_auto_fix() {
        // Even the signature-driven cargo-fmt detector is gated on
        // has_open_pr. A fmt failure on an open, unmerged PR is left to that PR
        // (the classifier routes it), never auto-fixed via a new corrective PR.
        let stub = StubClassifier::new(Triage::llm(
            TriageBucket::CodeFix,
            "run cargo fmt on the PR",
        ));
        let mut input = input_with_log("Run cargo fmt --check\nDiff in src/main.rs at line 1:\n");
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = true;
        let verdict = triage(&input, &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        assert!(verdict.auto_fix.is_none());
        assert_eq!(stub.calls(), 1);
    }

    #[test]
    fn transient_check_yml_failure_stays_transient_not_auto_fix() {
        // A flaky check.yml run (runner offline) must stay transient: the
        // transient rules run before the auto-fix detectors, so the checks-
        // workflow detector never claims an infra blip.
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "unused"));
        let mut input = input_with_log("Error: No runner is available for this job");
        input.workflow_id = "check.yml".to_string();
        let verdict = triage(&input, &stub, &autofix_cfg(), &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert!(verdict.auto_fix.is_none());
        assert_eq!(stub.calls(), 0);
    }

    #[test]
    fn disabled_auto_fix_falls_through_to_classifier() {
        // With the framework off, a cargo-fmt failure is handed to the LLM like
        // any other non-transient failure (today's behavior).
        let disabled = AutoFixConfig {
            enabled: false,
            ..AutoFixConfig::default()
        };
        let stub = StubClassifier::new(Triage::llm(TriageBucket::HumanAction, "human"));
        let mut input = input_with_log("Run cargo fmt --check\nDiff in src/main.rs at line 1:\n");
        input.workflow_id = "check.yml".to_string();
        let verdict = triage(&input, &stub, &disabled, &test_spend());
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert_eq!(stub.calls(), 1, "classifier consulted when auto-fix is off");
    }

    // ---- AC 3: log bounding before the LLM call --------------------------

    #[test]
    fn bound_log_leaves_small_logs_untouched() {
        let log = "short log line\n";
        assert_eq!(bound_log(log, 1024), log);
    }

    #[test]
    fn bound_log_truncates_large_logs_keeping_head_and_tail() {
        let log = format!("HEAD-MARKER\n{}\nTAIL-MARKER", "x".repeat(10_000));
        let bounded = bound_log(&log, 1024);
        assert!(bounded.len() < log.len());
        assert!(bounded.contains("HEAD-MARKER"));
        assert!(bounded.contains("TAIL-MARKER"));
        assert!(bounded.contains("bytes truncated"));
    }

    #[test]
    fn bound_log_is_utf8_boundary_safe() {
        // Multi-byte chars across the cut points must not panic or split.
        let log = "héllo wörld ".repeat(1000);
        let bounded = bound_log(&log, 64);
        assert!(bounded.len() <= log.len());
        // Round-trips as valid UTF-8 (a split codepoint would not).
        assert!(bounded.chars().count() > 0);
    }

    // ---- AC 2: analysis verdict mapping, all four values plus an
    // unparseable reply -----------------------------------------------------

    #[test]
    fn analysis_verdict_maps_code_defect_to_code_fix() {
        let reply = "## Root cause\nthe assertion is wrong.\n\n```json\n{\"verdict\": \"code_defect\", \"failing_tests\": [\"t\"], \"summary\": \"fix the assertion\"}\n```";
        let verdict = parse_analysis_verdict(reply);
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        assert_eq!(verdict.classified_by, ClassifiedBy::Llm);
        assert!(verdict.analysis.contains("Root cause"));
    }

    #[test]
    fn analysis_verdict_maps_needs_human_to_human_action() {
        let reply =
            "analysis\n```json\n{\"verdict\": \"needs_human\", \"summary\": \"rotate the token\"}\n```";
        let verdict = parse_analysis_verdict(reply);
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert_eq!(verdict.classified_by, ClassifiedBy::Llm);
    }

    #[test]
    fn analysis_verdict_maps_flaky_and_infra_to_transient() {
        for kind in ["flaky", "infra"] {
            let reply =
                format!("analysis\n```json\n{{\"verdict\": \"{kind}\", \"summary\": \"s\"}}\n```");
            let verdict = parse_analysis_verdict(&reply);
            assert_eq!(verdict.bucket, TriageBucket::Transient, "{kind}");
            assert_eq!(verdict.classified_by, ClassifiedBy::Llm, "{kind}");
        }
    }

    /// AC 2: an unparseable / missing verdict routes `human_action`, never
    /// `code_fix`, and (separately, informally) logs at `warn`.
    #[test]
    fn analysis_verdict_degrades_to_human_action_on_an_unparseable_reply() {
        for reply in [
            "not json at all {",
            "```json\n{\"verdict\": \"banana\", \"summary\": \"s\"}\n```",
            "",
            "some prose with a stray brace } but no object",
        ] {
            let verdict = parse_analysis_verdict(reply);
            assert_eq!(verdict.bucket, TriageBucket::HumanAction, "{reply:?}");
            assert_eq!(verdict.classified_by, ClassifiedBy::Llm, "{reply:?}");
        }
    }

    /// The verdict object is found even when the Markdown report before it
    /// contains braces of its own (e.g. a code block), by scanning from the
    /// end and matching braces rather than taking the first `{` in the reply.
    #[test]
    fn analysis_verdict_ignores_braces_in_the_report_body() {
        let reply = "```rust\nfn f() { 1 }\n```\nsee above.\n```json\n{\"verdict\": \"code_defect\", \"summary\": \"s\"}\n```";
        let verdict = parse_analysis_verdict(reply);
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
    }

    // ---- AC 1: AnalysisClassifier session shape -----------------

    use crate::forge_log::{ActionLogClient, ActionRunsClient, RepoFileClient, RunLogsOutcome};

    /// A [`ForgeReads`] stub with no workflow files and a timing error, so the
    /// analysis proceeds over the log alone; the point of this test is the
    /// `claude` invocation shape, not the forge reads.
    struct NoForgeReads;
    impl ActionLogClient for NoForgeReads {
        fn run_logs(
            &self,
            _host: &str,
            _repo: &str,
            _run_number: i64,
            _token: Option<&str>,
        ) -> RunLogsOutcome {
            unreachable!("the analysis classifier never re-fetches the log")
        }
    }
    impl ActionRunsClient for NoForgeReads {
        fn list_runs(
            &self,
            _host: &str,
            _repo: &str,
            _max_pages: u32,
            _token: Option<&str>,
        ) -> crate::forge_log::RunsListOutcome {
            unreachable!("the analysis classifier never lists runs")
        }
        fn run_timing(
            &self,
            _host: &str,
            _repo: &str,
            _run_number: i64,
            _token: Option<&str>,
        ) -> std::result::Result<crate::forge_log::RunTiming, String> {
            Err("no timing in this test".to_string())
        }
    }
    impl RepoFileClient for NoForgeReads {
        fn read_file(
            &self,
            _host: &str,
            _repo: &str,
            path: &str,
            _git_ref: &str,
            _token: Option<&str>,
        ) -> std::result::Result<String, String> {
            Err(format!("no such file: {path}"))
        }
    }

    /// AC 1: a failure with no store match runs exactly one analysis session
    /// with the configured allowed tools, timeout, and a prompt naming the log
    /// and workflow files. Asserted against a stub `claude` binary recording
    /// its own argv and the prompt it was invoked with.
    #[tokio::test]
    async fn analysis_classifier_invokes_claude_with_the_configured_session_shape() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        let argv_file = dir.path().join("argv.txt");
        let result_text =
            "analysis done\n```json\n{\"verdict\": \"code_defect\", \"summary\": \"s\"}\n```";
        let result_line = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "result": result_text,
            "total_cost_usd": 0.1,
        })
        .to_string();
        crate::testutil::write_stub(
            &stub,
            &format!(
                "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done > '{}'\nprintf '%s\\n' '{result_line}'\nexit 0\n",
                argv_file.display()
            ),
        );
        let invoker = Arc::new(ClaudeInvoker::new(Some(stub.to_str().unwrap())));
        let classifier = AnalysisClassifier::new(
            invoker,
            ClaudeBlock {
                allowed_tools: Some("Read,Grep,Bash".to_string()),
                ..ClaudeBlock::default()
            },
            Duration::from_secs(600),
            dir.path().join("analysis"),
            dir.path().join("logs"),
            Arc::new(NoForgeReads),
            crate::db::test_pool().await,
        );
        // The configured timeout, asserted on the struct directly: the actual
        // wall-clock bound is enforced by the `timeout(1)` wrapper `invoke`
        // shells out to, which is outside what this stub's own argv can see.
        assert_eq!(classifier.timeout, Duration::from_secs(600));
        let input = input_with_log("test foo ... FAILED");
        let spend =
            crate::usage::RunSpend::new("test", crate::usage::RunBudget::new(1.0, 0.8), None);
        let spend_for_classify = spend.clone();
        // `classify` is synchronous and calls `Handle::current().block_on` to
        // persist the usage record, mirroring the production call through
        // `spawn_blocking` rather than calling it inline on the test's own
        // async task (which would deadlock).
        let verdict =
            tokio::task::spawn_blocking(move || classifier.classify(&input, &spend_for_classify))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        // The classifier records its cost onto the caller's own
        // ledger instead of owning one, so the caller can attribute it to the
        // right run.
        assert_eq!(spend.cost_usd(), 0.1);
        assert!(spend.claude_ran());
        assert!(spend.claude_ok());

        let argv = std::fs::read_to_string(&argv_file).unwrap();
        assert!(
            argv.lines().any(|l| l == "--allowed-tools=Read,Grep,Bash"),
            "allowed tools passed through, got:\n{argv}"
        );
        // The prompt is the trailing positional argument and itself spans many
        // lines, so it is checked against the whole recorded argv text rather
        // than a single line of it.
        assert!(argv.contains("run.log"), "prompt names the log file");
        assert!(
            argv.contains("workflow YAML could not be fetched"),
            "prompt says the workflow YAML is unavailable when the fetch misses"
        );

        // AC 6: the per-event analysis directory is gone once classification
        // returns.
        assert!(
            !dir.path().join("analysis").exists()
                || std::fs::read_dir(dir.path().join("analysis"))
                    .unwrap()
                    .next()
                    .is_none(),
            "the per-event analysis directory is removed after classification"
        );
    }
}
