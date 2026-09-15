//! CLAUDE-274: the reusable pieces of a deep, log-grounded CI-failure
//! analysis, used by the at-ingest [`crate::triage::AnalysisClassifier`]:
//! fetching the run log + workflow YAML, building the agentic prompt, and
//! capping/formatting the analysis text for a comment. Originally shared with
//! a scheduled analysis native handler, retired in CLAUDE-279.

use std::path::PathBuf;

use crate::forge_log;

/// Cap (bytes) on an analysis comment/description, so a runaway agent reply
/// cannot blow the comment. Used by [`crate::mint::build_description`]
/// (CLAUDE-274) to cap the [`crate::triage::AnalysisClassifier`]'s report.
pub(crate) const DEFAULT_MAX_ANALYSIS_BYTES: usize = 24 * 1024;

/// The redacted-log + workflow-YAML inputs claude reads.
pub(crate) struct AnalysisInputs {
    /// Path to the complete redacted run log written under the workdir.
    pub(crate) log_path: PathBuf,
    /// Path to the failing workflow YAML, or `None` if it could not be fetched.
    pub(crate) workflow_path: Option<PathBuf>,
    /// CLAUDE-236: the run's forge-reported start / stop times, or `None` when
    /// they could not be read (the prompt then says the timing is unavailable).
    pub(crate) timing: Option<forge_log::RunTiming>,
}

/// CLAUDE-189: coordinates identifying one failed run, whether parsed from an
/// issue's `## Failure` block or derived directly from a [`crate::triage::TriageInput`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunCoordinates {
    pub(crate) repo: String,
    pub(crate) index_in_repo: i64,
    pub(crate) commit_sha: String,
    pub(crate) workflow_id: String,
    pub(crate) html_url: String,
}

/// The trailing `/actions/runs/<N>` index in a run URL.
pub(crate) fn run_index_from_url(url: &str) -> Option<i64> {
    let after = url.split("/actions/runs/").nth(1)?;
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().ok()
}

/// Cap `text` to `max_bytes`, keeping the head and appending a truncation note.
/// UTF-8 safe.
pub(crate) fn cap_comment(text: &str, max_bytes: usize) -> String {
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
/// tool, so the prompt never offers truncation as an explanation. CLAUDE-274:
/// ends with a fenced JSON verdict so the at-ingest classifier can route the
/// failure from the same session; the scheduled action ignores the trailing
/// JSON and posts the whole reply as the comment.
pub(crate) fn build_prompt(inputs: &AnalysisInputs, coords: &RunCoordinates) -> String {
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
        "Ground every claim in the actual log/workflow contents. Be concise. Do not modify any files, push, open a PR, or touch YouTrack.".to_string(),
        String::new(),
        "End your reply with exactly one fenced JSON object naming your verdict, on its own, after the Markdown report:".to_string(),
        "```json".to_string(),
        "{\"verdict\": \"code_defect\" | \"needs_human\" | \"flaky\" | \"infra\", \"failing_tests\": [\"...\"], \"summary\": \"<2-4 sentences: root cause and the suggested next step>\"}".to_string(),
        "```".to_string(),
        "\"code_defect\": a code change fixes it. \"needs_human\": no code change fixes it (missing secret/config, an infra change only a human can make, a push to a protected branch with no PR). \"flaky\": the failure looks like a non-deterministic/timing flake. \"infra\": a runner/registry/network problem unrelated to the code.".to_string(),
    ]
    .join("\n")
}
