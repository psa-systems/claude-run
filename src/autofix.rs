//! Deterministic auto-fix detector layer.
//!
//! Implements SPEC-3.5.1's auto-fix detector step.
//!
//! A CI failure with a mechanical signature (today `cargo fmt --check`) is
//! mechanically fixable: the fix is a known command with no judgement. A failed
//! workflow with no such signature is never an auto-fix (SPEC-4.4.6); it goes to
//! the analysis pass. These detectors run inside the hybrid triage as a deterministic
//! short-circuit BEFORE the LLM classifier (see [`crate::triage::triage`]), so
//! such a failure never reaches the prompt that would otherwise label a
//! push-to-protected-branch failure `human_action`.
//!
//! Each detector takes the [`crate::triage::TriageInput`] (repo, `workflow_id`,
//! `commit_sha`, log, `has_open_pr`) and returns `Some(AutoFix)` describing a
//! stable kind id, a human-readable problem statement, the concrete fix
//! command(s) the runner runs, and an optional step/file scope. The detectors

use serde::{Deserialize, Serialize};

use crate::config::AutoFixConfig;
use crate::triage::TriageInput;

/// SPEC-3.5.1: a mechanically-fixable CI failure produced by a detector. Flows through the
/// [`crate::triage::Triage`] verdict into the mint so the issue summary /
/// description name the concrete problem and fix instead of the generic
/// "CI failure" template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoFix {
    /// Stable detector id, e.g. `cargo-fmt` or `checks-workflow`. Used verbatim
    /// in the corrective branch name (`chore/<kind>-<short-sha>`).
    pub kind: String,
    /// Human-readable statement of what failed and why it is auto-fixable.
    pub problem: String,
    /// Concrete fix command(s) the runner executes, in order. Empty when the
    /// fix is "reproduce the failing workflow and fix what it reports" (the
    /// generic checks-workflow case, where the exact command depends on the
    /// failing step).
    pub fix_commands: Vec<String>,
    /// Optional failing step / file scope lifted from the log, surfaced to the
    /// runner. `None` when not determinable.
    pub scope: Option<String>,
}

/// SPEC-3.5.1: run the detectors against a failure, most-specific first, only
/// when the framework is enabled. Returns `None` when disabled or no detector
/// matches (the caller then falls through to the LLM classifier).
pub fn detect(input: &TriageInput, cfg: &AutoFixConfig) -> Option<AutoFix> {
    if !cfg.enabled {
        return None;
    }
    detect_cargo_fmt(&input.log)
}

/// `cargo-fmt` detector: matches the rustfmt `cargo fmt --check` failure
/// signature - the `Diff in <path> at line N:` lines rustfmt prints, plus a
/// `cargo fmt` / `rustfmt` marker from the failing step. Both are required so an
/// ordinary diff in unrelated log output cannot trip it. Fix: `cargo fmt --all`.
fn detect_cargo_fmt(log: &str) -> Option<AutoFix> {
    let haystack = log.to_lowercase();
    let has_diff = haystack.contains("diff in ");
    let has_fmt_marker = haystack.contains("cargo fmt") || haystack.contains("rustfmt");
    if has_diff && has_fmt_marker {
        Some(AutoFix {
            kind: "cargo-fmt".to_string(),
            problem: "rustfmt found unformatted code: `cargo fmt --check` reported diffs."
                .to_string(),
            fix_commands: vec!["cargo fmt --all".to_string()],
            scope: None,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(workflow_id: &str, log: &str) -> TriageInput {
        TriageInput {
            repo: "contoso/claude-run".to_string(),
            workflow_id: workflow_id.to_string(),
            commit_sha: "abc123def456".to_string(),
            html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
            log: log.to_string(),
            log_available: true,
            log_unavailable_reason: None,
            // The canonical case: a push to main with NO open PR. The detectors
            // ignore this field, which is exactly the point - they route the
            // same regardless of `has_open_pr`.
            has_open_pr: false,
            branch: "main".to_string(),
            log_token: None,
        }
    }

    fn cfg() -> AutoFixConfig {
        AutoFixConfig::default()
    }

    // ---- AC 2: cargo-fmt signature detection ------------------------------

    #[test]
    fn cargo_fmt_signature_is_detected() {
        let log = "Run cargo fmt --check\n\
            Diff in /build/src/triage.rs at line 42:\n\
            -    let x=1;\n\
            +    let x = 1;\n\
            ##[error]Process completed with exit code 1\n";
        let af = detect(&input("check.yml", log), &cfg()).expect("cargo-fmt detected");
        assert_eq!(af.kind, "cargo-fmt");
        assert_eq!(af.fix_commands, vec!["cargo fmt --all".to_string()]);
    }

    #[test]
    fn cargo_fmt_detected_independent_of_workflow_id() {
        // The signature, not the workflow id, drives the cargo-fmt detector, so
        // a fmt failure in any workflow is still auto-fixable.
        let log = "rustfmt check\nDiff in src/lib.rs at line 1:\n";
        let af = detect(&input("ci.yml", log), &cfg()).expect("cargo-fmt detected");
        assert_eq!(af.kind, "cargo-fmt");
    }

    #[test]
    fn cargo_fmt_requires_both_diff_and_fmt_marker() {
        // A diff with no fmt marker (e.g. a `git diff` in test output) must not
        // trip the detector.
        let log = "Diff in some patch output\nassertion failed: left == right\n";
        assert!(detect(&input("ci.yml", log), &cfg()).is_none());
        // A fmt marker with no diff (fmt passed, something else failed) likewise.
        let log = "Run cargo fmt --check\nclippy error: unused variable\n";
        assert!(detect(&input("ci.yml", log), &cfg()).is_none());
    }

    // ---- SPEC-4.4.6: a failed workflow with no signature is not an auto-fix ----

    #[test]
    fn a_checks_workflow_failure_without_a_signature_is_not_an_auto_fix() {
        let log = "##[group]Clippy\n\
            error: unused variable `x`\n\
            ##[error]the step has failed\n";
        assert!(detect(&input("check.yml", log), &cfg()).is_none());
    }

    #[test]
    fn cargo_fmt_wins_for_a_fmt_failure_of_check_yml() {
        let log = "Run cargo fmt --check\nDiff in src/main.rs at line 3:\n";
        let af = detect(&input("check.yml", log), &cfg()).expect("detected");
        assert_eq!(af.kind, "cargo-fmt");
    }

    // ---- master toggle ----------------------------------------------------

    #[test]
    fn disabled_framework_detects_nothing() {
        let disabled = AutoFixConfig {
            enabled: false,
            ..AutoFixConfig::default()
        };
        let log = "Run cargo fmt --check\nDiff in src/main.rs at line 3:\n";
        assert!(detect(&input("check.yml", log), &disabled).is_none());
    }
}
