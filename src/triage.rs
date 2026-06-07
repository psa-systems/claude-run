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
use crate::cancel::CancellationToken;
use crate::claude::ClaudeInvoker;
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
}

impl TriageBucket {
    /// Stable lowercase string form.
    pub fn as_str(&self) -> &'static str {
        match self {
            TriageBucket::CodeFix => "code_fix",
            TriageBucket::HumanAction => "human_action",
            TriageBucket::Transient => "transient",
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
}

impl Triage {
    fn rules(bucket: TriageBucket, analysis: impl Into<String>) -> Self {
        Self {
            bucket,
            analysis: analysis.into(),
            classified_by: ClassifiedBy::Rules,
        }
    }

    fn llm(bucket: TriageBucket, analysis: impl Into<String>) -> Self {
        Self {
            bucket,
            analysis: analysis.into(),
            classified_by: ClassifiedBy::Llm,
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
    /// False when the log could not be fetched (a `TransientMiss`): itself a
    /// bucket-3 signal handled by [`triage_rules`].
    pub log_available: bool,
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
    // A log we could not fetch (not-ready / pruned) is itself transient.
    if !input.log_available {
        return Some(Triage::rules(
            TriageBucket::Transient,
            "failing job log not available (not ready or pruned); transient infra miss",
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

/// Hybrid triage: deterministic rules first, the `classifier` only for the
/// residue. A classifier error degrades to [`TriageBucket::HumanAction`] (a human
/// looks) rather than failing the pipeline.
pub fn triage(input: &TriageInput, classifier: &dyn FailureClassifier) -> Triage {
    if let Some(verdict) = triage_rules(input) {
        return verdict;
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
            html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
            log: log.to_string(),
            log_available: true,
            has_open_pr: true,
        }
    }

    // ---- bucket string round-trip ----------------------------------------

    #[test]
    fn bucket_roundtrips_through_lowercase_form() {
        for (bucket, s) in [
            (TriageBucket::CodeFix, "code_fix"),
            (TriageBucket::HumanAction, "human_action"),
            (TriageBucket::Transient, "transient"),
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
    fn rules_classify_unavailable_log_as_transient() {
        let mut input = input_with_log("");
        input.log_available = false;
        let verdict = triage_rules(&input).unwrap();
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
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
        let verdict = triage(&input_with_log(log), &stub);
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
        let verdict = triage(&input_with_log("test foo ... FAILED"), &stub);
        assert_eq!(verdict.bucket, TriageBucket::CodeFix);
        assert_eq!(verdict.classified_by, ClassifiedBy::Llm);
        assert!(!verdict.analysis.is_empty());
        assert_eq!(stub.calls(), 1, "the classifier must be consulted");
    }

    #[test]
    fn hybrid_short_circuits_rules_without_calling_classifier() {
        let stub = StubClassifier::new(Triage::llm(TriageBucket::CodeFix, "unused"));
        let verdict = triage(&input_with_log("fatal: connection reset by peer"), &stub);
        assert_eq!(verdict.bucket, TriageBucket::Transient);
        assert_eq!(verdict.classified_by, ClassifiedBy::Rules);
        assert_eq!(stub.calls(), 0, "the classifier must NOT be consulted");
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
