//! Failure triage engine (CLAUDE-45, part of the webhook fast-path epic
//! CLAUDE-36).
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
//! LLM call. Only the residue it cannot label is handed to an [`FailureClassifier`]
//! (the LLM pass), which separates bucket 1 from bucket 2 and writes the analysis
//! string. The classifier is a trait so tests inject a stub and never shell out;
//! the production [`ClaudeClassifier`] reuses the existing [`ClaudeInvoker`]
//! (`claude --print`) the runner already uses. Logs are bounded by [`bound_log`]
//! before the call so a huge job log cannot blow the prompt budget.
//!
//! This subtask delivers the engine and its types; wiring the result into the
//! issue-minting step (and into [`crate::web::webhooks::consume_events`]) is a
//! sibling subtask, so nothing here is called from the webhook consumer yet.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::action::ClaudeBlock;
use crate::autofix::{self, AutoFix};
use crate::cancel::CancellationToken;
use crate::claude::ClaudeInvoker;
use crate::config::AutoFixConfig;
use crate::error::Result;

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
    /// CLAUDE-104: a mechanically-fixable verification failure (cargo fmt, a
    /// failing checks workflow). Routes like [`TriageBucket::CodeFix`] (mint a
    /// queued issue so the runner opens a corrective PR). CLAUDE-115: applies
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
    /// CLAUDE-104: the auto-fix detail when `bucket == AutoFix`, carried through
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

    /// CLAUDE-104: a deterministic auto-fix verdict carrying its detector detail.
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
    /// False when the log could not be fetched. CLAUDE-92: a real failure with
    /// no obtainable log routes to a HUMAN (bucket 2) so it can never be
    /// silently dropped; `log_unavailable_reason` says why in the analysis.
    pub log_available: bool,
    /// CLAUDE-92: why the log is missing (unauthorized private repo, exhausted
    /// flush retries, fetch error). `None` when `log_available` is true.
    pub log_unavailable_reason: Option<String>,
    /// Whether the failing branch has an open PR. Strong bucket-1-vs-2 signal
    /// passed to the classifier (bucket 1 needs a PR to fix).
    pub has_open_pr: bool,
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
    "could not resolve host",
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
    // CLAUDE-92: a real failure whose log could not be obtained routes to a
    // HUMAN, never to the silent transient bucket. The original rule treated
    // every miss as "not flushed yet, a retry will get it", which quietly
    // swallowed failures on private repos (whose logs are unreadable on this
    // Forgejo version) and any other permanently-unfetchable log. Non-failure
    // statuses never reach triage at all (CLAUDE-85), so everything here IS a
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
    fn classify(&self, input: &TriageInput) -> Result<Triage>;
}

/// Hybrid triage: deterministic rules first, then the deterministic auto-fix
/// detectors, and only then the `classifier` for the residue. A classifier error
/// degrades to [`TriageBucket::HumanAction`] (a human looks) rather than failing
/// the pipeline.
///
/// CLAUDE-104: the [`autofix`] detectors run AFTER the transient/no-log rules
/// (so a flaky run of a checks workflow is still classed transient, never
/// auto-fixed) but BEFORE the LLM. CLAUDE-115: they run only when the failing
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
) -> Triage {
    if let Some(verdict) = triage_rules(input) {
        return verdict;
    }
    // CLAUDE-115: auto-fix only failures on a branch with no open PR. A failing
    // check on an open, unmerged PR is that PR's own concern; minting a queued
    // issue here would open a new corrective PR against the unmerged PR, which
    // causes more confusion than it fixes.
    if !input.has_open_pr {
        if let Some(detail) = autofix::detect(input, autofix_cfg) {
            return Triage::auto_fix(detail);
        }
    }
    match classifier.classify(input) {
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

/// Parse a classifier reply into a [`Triage`]. Reads the first JSON object in the
/// reply (`{"bucket": "code_fix"|"human_action", "analysis": "..."}`) and degrades
/// to [`TriageBucket::HumanAction`] on any malformed / unexpected reply, so a bad
/// model response routes to a human instead of crashing.
pub fn parse_classification(reply: &str) -> Triage {
    if let Some(obj) = extract_json_object(reply) {
        let analysis = obj
            .get("analysis")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match obj.get("bucket").and_then(Value::as_str) {
            Some("code_fix") => return Triage::llm(TriageBucket::CodeFix, analysis),
            Some("human_action") => return Triage::llm(TriageBucket::HumanAction, analysis),
            _ => {}
        }
    }
    Triage::llm(
        TriageBucket::HumanAction,
        "could not parse classifier reply; defaulting to human review",
    )
}

/// Find and parse the first `{...}` JSON object in `s`.
fn extract_json_object(s: &str) -> Option<Value> {
    let start = s.find('{')?;
    let end = s.rfind('}')?;
    if end < start {
        return None;
    }
    serde_json::from_str(&s[start..=end]).ok()
}

/// Production [`FailureClassifier`]: shells out to `claude --print` via the same
/// [`ClaudeInvoker`] the runner uses, with a strict-JSON prompt over the bounded
/// log. Any non-completion or unparseable reply degrades to human review.
pub struct ClaudeClassifier {
    invoker: Arc<ClaudeInvoker>,
    claude_cfg: ClaudeBlock,
    timeout: Duration,
    cwd: PathBuf,
    stream_log: PathBuf,
    max_log_bytes: usize,
}

impl ClaudeClassifier {
    /// Build a classifier. `stream_log` is the per-classification NDJSON log path
    /// (see [`ClaudeInvoker::stream_log_path`]); `cwd` is the working directory
    /// the classification runs in (no repo checkout is needed to classify).
    pub fn new(
        invoker: Arc<ClaudeInvoker>,
        claude_cfg: ClaudeBlock,
        timeout: Duration,
        cwd: PathBuf,
        stream_log: PathBuf,
    ) -> Self {
        Self {
            invoker,
            claude_cfg,
            timeout,
            cwd,
            stream_log,
            max_log_bytes: DEFAULT_MAX_LOG_BYTES,
        }
    }

    fn build_prompt(&self, input: &TriageInput) -> String {
        let log = bound_log(&input.log, self.max_log_bytes);
        [
            "You are triaging a failed CI run for an automation system.".to_string(),
            "Deterministic rules already ruled out transient/infra failures, so this is NOT transient.".to_string(),
            "Classify it into exactly one of two buckets:".to_string(),
            "  - \"code_fix\": a pull-request check failed and a code change on the PR branch will fix it.".to_string(),
            "  - \"human_action\": no PR code change fixes it (a push to a protected/main branch, a tag/release/build-publish failure with no PR, a missing secret or config, or an infra change a human must make).".to_string(),
            String::new(),
            format!("repo: {}", input.repo),
            format!("workflow: {}", input.workflow_id),
            format!("commit: {}", input.commit_sha),
            format!("run: {}", input.html_url),
            format!("open PR for this branch: {}", input.has_open_pr),
            String::new(),
            "Failing job log (redacted, possibly truncated):".to_string(),
            "---".to_string(),
            log,
            "---".to_string(),
            String::new(),
            "Reply with ONLY a JSON object: {\"bucket\": \"code_fix\" | \"human_action\", \"analysis\": \"<2-4 sentences: root cause and the suggested next step>\"}.".to_string(),
        ]
        .join("\n")
    }
}

impl FailureClassifier for ClaudeClassifier {
    fn classify(&self, input: &TriageInput) -> Result<Triage> {
        let prompt = self.build_prompt(input);
        // Classification is not cancellable on its own; a fresh token never trips.
        let cancel = CancellationToken::new();
        let result = self.invoker.invoke(
            &prompt,
            &self.claude_cfg,
            self.timeout,
            &self.cwd,
            &self.stream_log,
            &cancel,
        )?;
        if !result.ok {
            return Ok(Triage::llm(
                TriageBucket::HumanAction,
                format!(
                    "claude classification did not complete ({}); routing to a human",
                    result.error
                ),
            ));
        }
        Ok(parse_classification(&result.summary))
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
        fn classify(&self, _input: &TriageInput) -> Result<Triage> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.reply.clone())
        }
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
            "could not resolve host: registry.example.com",
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
    fn rules_route_unavailable_log_to_a_human() {
        // CLAUDE-92: a real failure with no obtainable log must surface to a
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
        // CLAUDE-78: the live CLAUDE-73 mis-mint shape; the job never ran, the
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
        let verdict = triage(&input_with_log(log), &stub, &autofix_cfg());
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
        );
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert_eq!(stub.calls(), 0, "the classifier must NOT be consulted");
    }

    // ---- CLAUDE-104: deterministic auto-fix routing -----------------------

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
        let verdict = triage(&input, &stub, &autofix_cfg());
        assert_eq!(verdict.bucket, TriageBucket::AutoFix);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert_eq!(stub.calls(), 0, "auto-fix short-circuits before the LLM");
        let detail = verdict.auto_fix.expect("auto-fix detail carried");
        assert_eq!(detail.kind, "cargo-fmt");
        assert_eq!(detail.fix_commands, vec!["cargo fmt --all".to_string()]);
    }

    #[test]
    fn check_yml_failure_routes_to_auto_fix_only_without_an_open_pr() {
        // AC 3 + CLAUDE-115: the checks-workflow detector fires for a non-fmt
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
        let verdict = triage(&input, &stub, &autofix_cfg());
        assert_eq!(verdict.bucket, TriageBucket::AutoFix);
        assert_eq!(verdict.auto_fix.unwrap().kind, "checks-workflow");
        assert_eq!(stub.calls(), 0, "auto-fix short-circuits before the LLM");

        // Open PR: auto-fix is skipped and the classifier decides instead.
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "fix the PR branch"));
        let mut input = input_with_log(log);
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = true;
        let verdict = triage(&input, &stub, &autofix_cfg());
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
        // CLAUDE-115: even the signature-driven cargo-fmt detector is gated on
        // has_open_pr. A fmt failure on an open, unmerged PR is left to that PR
        // (the classifier routes it), never auto-fixed via a new corrective PR.
        let stub = StubClassifier::new(Triage::llm(
            TriageBucket::CodeFix,
            "run cargo fmt on the PR",
        ));
        let mut input = input_with_log("Run cargo fmt --check\nDiff in src/main.rs at line 1:\n");
        input.workflow_id = "check.yml".to_string();
        input.has_open_pr = true;
        let verdict = triage(&input, &stub, &autofix_cfg());
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
        let verdict = triage(&input, &stub, &autofix_cfg());
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
        let verdict = triage(&input, &stub, &disabled);
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

    // ---- AC 2/4: parsing helper degrades on a bad reply ------------------

    #[test]
    fn parse_classification_reads_a_good_reply() {
        let reply = r#"Here is my verdict:
            {"bucket": "human_action", "analysis": "the release tag job failed; rotate the publish token"}"#;
        let verdict = parse_classification(reply);
        assert_eq!(verdict.bucket, TriageBucket::HumanAction);
        assert_eq!(verdict.classified_by, ClassifiedBy::Llm);
        assert!(verdict.analysis.contains("publish token"));
    }

    #[test]
    fn parse_classification_degrades_to_human_on_malformed_reply() {
        for reply in ["not json at all {", "{\"bucket\": \"banana\"}", ""] {
            let verdict = parse_classification(reply);
            assert_eq!(verdict.bucket, TriageBucket::HumanAction, "{reply:?}");
            assert_eq!(verdict.classified_by, ClassifiedBy::Llm, "{reply:?}");
        }
    }
}
