//! Deterministic infrastructure-failure detector.
//!
//! Implements the infrastructure-detection half of SPEC-3.5.1 and the
//! branch-agnostic `infra:<kind>` fingerprint/dedupe key it feeds into
//! SPEC-3.5.2.
//!
//! A CI failure caused by the RUNNER rather than the commit (a full disk, an
//! OOM kill, an unreachable package registry) is identical on every branch and
//! is not fixable on any of them. The root-cause dedupe key contains
//! `branch`, so one such host condition mints a fresh issue per PR branch and
//! the duplicates never collapse. [`detect`] gives the mint a branch-agnostic
//! `kind` id to key on instead (see [`crate::mint`]).
//!
//! Modelled on [`crate::autofix`]: pure string work over the FULL redacted log,
//! no LLM and no I/O, so it sees the whole log, same as the classifier's own
//! session ([`crate::triage::AnalysisClassifier`] writes the complete redacted
//! log to the per-event workdir and reads it with no truncation).
//!
//! Nothing about triage buckets or routing changes: an infra failure is still
//! classified and routed as it was, because reclaiming disk is a human action.
//!
//! The detector set is deliberately conservative. An over-broad pattern would
//! merge two unrelated CODE failures across branches into one issue, which is
//! worse than leaving a host condition ungrouped, so each detector requires an
//! unambiguous phrase and an undetected condition is the preferred failure mode.

/// SPEC-3.5.1 / SPEC-3.12 item 8: every `kind` [`detect`] can return. Each is a
/// runner condition rather than a defect in the commit, so a CI failure that
/// carries one spends no `webhook.agent_pr_fix_attempts` attempt and starts no
/// remediation run ([`InfraFailure::spends_no_attempt`]).
pub const KINDS: &[&str] = &[
    "disk-full",
    "out-of-memory",
    "runner-unavailable",
    "job-timeout",
    "container-not-started",
    "runner-deadline",
    "no-diagnostic",
];

/// SPEC-3.5.1 / SPEC-3.5.2: an infrastructure / runner condition recognized in
/// a job log. Same shape as
/// [`crate::autofix::AutoFix`]: a stable `kind` id (what the dedupe key is built
/// from, so the same condition keys identically however the message is worded)
/// plus a human-readable statement of the problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraFailure {
    /// Stable detector id: one of [`KINDS`]. This, not the matched line, is what [`crate::mint`] hashes.
    pub kind: String,
    /// Human-readable statement of the host condition, for the log line and the
    /// operator reading it.
    pub problem: String,
}

impl InfraFailure {
    /// SPEC-3.12 item 8: an infrastructure failure never counts toward
    /// `webhook.agent_pr_fix_attempts` and starts no remediation run. True for
    /// every kind in [`KINDS`]; a string outside the closed set is not an
    /// infrastructure kind and still spends an attempt.
    pub fn spends_no_attempt(kind: &str) -> bool {
        KINDS.contains(&kind)
    }
}

/// SPEC-3.5.1: run the infrastructure detectors against a full redacted job
/// log, most specific first. `None` when no detector matches, which is the
/// common case: a code failure must never match here.
///
/// Cargo test-result lines (`test <name> ... ok` / `FAILED` / `ignored[, ...]`)
/// and the bare `<target>::<test>` paths cargo lists under `failures:` are
/// dropped before any detector runs, so a test whose NAME happens to contain a
/// detector phrase (`enospc`, `out of memory`, ...) never matches here, whether
/// that test passed or failed: only text the runner or toolchain emitted as a
/// diagnostic counts.
pub fn detect(log: &str) -> Option<InfraFailure> {
    let filtered = strip_test_name_lines(log);
    let lower = filtered.to_ascii_lowercase();
    detect_disk_full(&lower)
        .or_else(|| detect_out_of_memory(&lower))
        .or_else(|| detect_runner_unavailable(&lower))
        .or_else(|| detect_job_timeout(&lower))
        .or_else(|| detect_container_not_started(&lower))
        .or_else(|| detect_runner_deadline(&lower))
        .or_else(|| detect_no_diagnostic(log, &filtered))
}

/// Drop lines that carry only a test name: a `test ... ... ok|FAILED|ignored`
/// result line, or a bare `<target>::<test>` path from cargo's `failures:`
/// list. Every other line, including its original casing, passes through
/// unchanged so the detectors below still see it verbatim.
fn strip_test_name_lines(log: &str) -> String {
    log.lines()
        .filter(|line| !is_test_result_line(line) && !is_test_path_line(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Trim a line and drop a single leading timestamp token, as the forge log
/// format writes it (e.g. `2024-01-01T12:00:00.123456789Z `).
fn strip_leading_timestamp(line: &str) -> &str {
    let trimmed = line.trim();
    match trimmed.split_once(' ') {
        Some((first, rest)) if first.ends_with('Z') => rest.trim_start(),
        _ => trimmed,
    }
}

/// `test <name> ... ok`, `test <name> ... FAILED`, `test <name> ... ignored`
/// or `test <name> ... ignored, <reason>`, with or without a leading
/// timestamp.
fn is_test_result_line(line: &str) -> bool {
    let Some(rest) = strip_leading_timestamp(line).strip_prefix("test ") else {
        return false;
    };
    let Some((_, status)) = rest.split_once(" ... ") else {
        return false;
    };
    let status = status.trim();
    status == "ok" || status == "FAILED" || status == "ignored" || status.starts_with("ignored,")
}

/// A bare `<target>::<test>` path, the indented list cargo prints under
/// `failures:`, with or without a leading timestamp: no whitespace, at least
/// one `::`, and nothing but identifier characters and `::`.
fn is_test_path_line(line: &str) -> bool {
    let s = strip_leading_timestamp(line);
    !s.is_empty()
        && s.contains("::")
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == ':')
}

/// `disk-full`: the kernel's own ENOSPC wording, or a workflow capacity-guard
/// step that aborted naming insufficient free space (the
/// `initech/billing-service` `integration.yml` case, whose guard prints a
/// capacity abort that no compiler-diagnostic pattern matches).
fn detect_disk_full(lower: &str) -> Option<InfraFailure> {
    const PHRASES: &[&str] = &[
        "no space left on device",
        "enospc",
        "insufficient disk space",
        "insufficient free space",
        "not enough disk space",
        "not enough free space",
    ];
    PHRASES.iter().any(|p| lower.contains(p)).then(|| InfraFailure {
        kind: "disk-full".to_string(),
        problem: "the runner's filesystem is out of space: the job log reports no space left on device or a capacity guard aborted naming insufficient free space. This is a runner-capacity problem, not a problem with the commit.".to_string(),
    })
}

/// `out-of-memory`: an allocator failure or an OOM kill. `killed (signal 9)` is
/// included because the kernel OOM killer's own line is often absent from a
/// container's job log while the SIGKILL is not.
fn detect_out_of_memory(lower: &str) -> Option<InfraFailure> {
    const PHRASES: &[&str] = &[
        "out of memory",
        "oom-kill",
        "cannot allocate memory",
        "killed (signal 9)",
    ];
    PHRASES.iter().any(|p| lower.contains(p)).then(|| InfraFailure {
        kind: "out-of-memory".to_string(),
        problem: "the runner ran out of memory: the job log reports an allocation failure or an OOM kill. This is a runner-capacity problem, not a problem with the commit.".to_string(),
    })
}

/// `runner-unavailable`: the runner could not reach the network it depends on.
/// `could not resolve host` is unambiguous on its own; `connection reset by
/// peer` is not (a test asserting on a socket error prints it too), so it counts
/// only on a line that also names a package or container registry.
///
/// [`crate::triage::triage_rules`] routes this kind to `HumanAction` before its
/// transient-signature scan, so it reaches the mint and collapses onto one
/// grouped issue. A bare `connection reset` with no registry marker is not
/// matched here and stays transient.
fn detect_runner_unavailable(lower: &str) -> Option<InfraFailure> {
    const REGISTRY_MARKERS: &[&str] = &[
        "registry",
        "crates.io",
        "docker.io",
        "ghcr.io",
        "quay.io",
        "npmjs",
        "pypi",
        "apt.",
        "deb.",
        "alpinelinux",
    ];
    let matched = lower.contains("could not resolve host")
        || lower.lines().any(|l| {
            l.contains("connection reset by peer") && REGISTRY_MARKERS.iter().any(|m| l.contains(m))
        });
    matched.then(|| InfraFailure {
        kind: "runner-unavailable".to_string(),
        problem: "the runner could not reach the network it depends on: the job log reports a DNS failure or a reset connection to a package or container registry. This is a runner-connectivity problem, not a problem with the commit.".to_string(),
    })
}

/// `job-timeout`: the job was killed by its own `timeout-minutes` (SPEC-3.12
/// item 8). The runner prints the exceeded-limit wording, not the test suite.
fn detect_job_timeout(lower: &str) -> Option<InfraFailure> {
    const PHRASES: &[&str] = &[
        "exceeded the maximum execution time",
        "job execution time exceeded",
        "job timed out after",
        "exceeded timeout-minutes",
    ];
    PHRASES.iter().any(|p| lower.contains(p)).then(|| InfraFailure {
        kind: "job-timeout".to_string(),
        problem: "the job was killed by its own timeout-minutes limit: the job log reports the maximum execution time was exceeded. This is a runner-capacity or workflow-limit problem, not a defect the agent can fix in the commit.".to_string(),
    })
}

/// `container-not-started`: the job container never came up, so no step of the
/// workflow ran (SPEC-3.12 item 8).
fn detect_container_not_started(lower: &str) -> Option<InfraFailure> {
    const PHRASES: &[&str] = &[
        "failed to start container",
        "failed to create container",
        "cannot start container",
        "failed to create task for container",
        "container failed to start",
    ];
    PHRASES.iter().any(|p| lower.contains(p)).then(|| InfraFailure {
        kind: "container-not-started".to_string(),
        problem: "the job container never started: the job log reports the container runtime could not create or start it. This is a runner problem, not a problem with the commit.".to_string(),
    })
}

/// `runner-deadline`: the runner's own API call to the forge or the container
/// runtime timed out (SPEC-3.12 item 8).
fn detect_runner_deadline(lower: &str) -> Option<InfraFailure> {
    lower
        .contains("context deadline exceeded")
        .then(|| InfraFailure {
            kind: "runner-deadline".to_string(),
            problem: "a runner API call timed out: the job log reports context deadline exceeded. This is a runner-connectivity or load problem, not a problem with the commit.".to_string(),
        })
}

/// `no-diagnostic` (SPEC-3.12 item 8): a non-empty log with no parsed failing
/// test id and no compiler, test or lint diagnostic of any kind. Nothing in it
/// points at the commit, so a fix attempt has nothing to act on. The runner's
/// own `Process completed with exit code` line is not a diagnostic.
/// Deliberately conservative: any line naming an error, a failure, a panic, a
/// diff or a traceback counts as a diagnostic, so a log that merely looks
/// unusual stays with the classifier.
fn detect_no_diagnostic(raw: &str, filtered: &str) -> Option<InfraFailure> {
    // Markers are read from the whole log, test-result lines included: a
    // `FAILED` test line is a diagnostic even though detection ignores its name.
    let lower = raw.to_ascii_lowercase();
    if filtered.trim().is_empty() || !crate::ci_failure::fingerprint::failing_tests(raw).is_empty()
    {
        return None;
    }
    const MARKERS: &[&str] = &[
        "error",
        "fail",
        "panick",
        "assert",
        "traceback",
        "exception",
        "diff in ",
        "warning",
    ];
    let has_diagnostic = lower.lines().any(|l| {
        let l = l.trim();
        !l.contains("process completed with exit code") && MARKERS.iter().any(|m| l.contains(m))
    });
    (!has_diagnostic).then(|| InfraFailure {
        kind: "no-diagnostic".to_string(),
        problem: "the job failed with no parsed failing test and no compiler, test or lint diagnostic in its log. Nothing points at the commit, so this is treated as a runner problem.".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind_of(log: &str) -> Option<String> {
        detect(log).map(|f| f.kind)
    }

    #[test]
    fn disk_full_is_detected_from_enospc_and_from_a_capacity_guard() {
        // The kernel wording.
        assert_eq!(
            kind_of(
                "Compiling foo\nerror: failed to write: No space left on device (os error 28)\n"
            )
            .as_deref(),
            Some("disk-full")
        );
        assert_eq!(
            kind_of("write failed: ENOSPC\n").as_deref(),
            Some("disk-full")
        );
        // The billing-service case: a workflow guard step aborts before the suite
        // runs, printing a capacity message no compiler-diagnostic pattern sees.
        assert_eq!(
            kind_of(
                "::group::Check workspace capacity\n\
                 workspace filesystem: 875G size, 2.9G free\n\
                 Insufficient free space: the suite needs ~25G to link test binaries.\n\
                 ##[error]Process completed with exit code 1\n"
            )
            .as_deref(),
            Some("disk-full")
        );
    }

    #[test]
    fn out_of_memory_is_detected() {
        assert_eq!(
            kind_of("rustc: fatal runtime error: out of memory\n").as_deref(),
            Some("out-of-memory")
        );
        assert_eq!(
            kind_of("Memory cgroup out of memory: OOM-killed process 1234 (rustc)\n").as_deref(),
            Some("out-of-memory")
        );
        assert_eq!(
            kind_of("mmap: Cannot allocate memory\n").as_deref(),
            Some("out-of-memory")
        );
        assert_eq!(
            kind_of("cc: Killed (signal 9)\n").as_deref(),
            Some("out-of-memory")
        );
    }

    #[test]
    fn runner_unavailable_is_detected() {
        assert_eq!(
            kind_of("curl: (6) Could not resolve host: dev.a8n.run\n").as_deref(),
            Some("runner-unavailable")
        );
        assert_eq!(
            kind_of(
                "Downloading crates ...\n\
                 error: failed to fetch from registry: connection reset by peer\n"
            )
            .as_deref(),
            Some("runner-unavailable")
        );
    }

    #[test]
    fn a_reset_connection_alone_is_not_enough() {
        // Conservative on purpose: a reset socket with no registry on the line
        // is as likely to be a test asserting on one, and a false positive
        // merges two unrelated code failures across branches.
        assert!(
            detect("thread 'net::tests::reset' panicked at connection reset by peer\n").is_none()
        );
    }

    #[test]
    fn an_ordinary_compile_or_test_failure_does_not_match() {
        // The negative case that bounds the whole feature: nothing a normal
        // code failure prints may take the branch-agnostic key.
        assert!(detect(
            "   Compiling claude-run v0.1.0\n\
             error[E0308]: mismatched types\n  --> src/mint.rs:412:9\n\
             error: could not compile `claude-run` due to 1 previous error\n"
        )
        .is_none());
        assert!(detect(
            "running 42 tests\n\
             thread 'mint::tests::collapse' panicked at src/mint.rs:1550:9:\n\
             assertion failed: left == right\n\
             test result: FAILED. 41 passed; 1 failed\n"
        )
        .is_none());
        assert!(detect("").is_none());
    }

    #[test]
    fn detection_is_case_insensitive_and_kind_is_stable_across_wordings() {
        // The kind id, not the matched line, is what the dedupe key hashes, so
        // two differently-worded reports of one condition must agree.
        let a = detect("No Space Left On Device\n").unwrap();
        let b = detect("df reports 0 bytes: no space left on device (os error 28)\n").unwrap();
        assert_eq!(a.kind, b.kind);
        assert_eq!(a, b, "same kind and same problem sentence");
    }

    #[test]
    fn a_passing_test_named_after_a_detector_phrase_does_not_match() {
        // CLAUDE-459: this repo's own `disk_full_is_detected_from_enospc...`
        // test prints exactly this line on every passing run.
        assert!(detect(
            "test infra_failure::tests::disk_full_is_detected_from_enospc_and_from_a_capacity_guard ... ok\n"
        )
        .is_none());
        assert!(detect("test oom::tests::out_of_memory_is_detected ... ok\n").is_none());
        assert!(
            detect("test net::tests::runner_unavailable_when_host_unresolvable ... ok\n").is_none()
        );
        // Same, with the forge log's leading timestamp token.
        assert!(detect(
            "2024-01-01T12:00:00.123456789Z test infra_failure::tests::disk_full_is_detected_from_enospc_and_from_a_capacity_guard ... ok\n"
        )
        .is_none());
    }

    #[test]
    fn a_failing_test_named_after_a_detector_phrase_does_not_match() {
        assert!(detect("test disk_full_enospc_regression ... FAILED\n").is_none());
        assert!(detect("test out_of_memory_regression ... FAILED\n").is_none());
        assert!(detect("test runner_unavailable_regression ... FAILED\n").is_none());
        assert!(detect(
            "2024-01-01T12:00:00.000000000Z test disk_full_enospc_regression ... FAILED\n"
        )
        .is_none());
    }

    #[test]
    fn a_bare_test_path_under_failures_does_not_match() {
        assert!(detect(
            "failures:\n    infra_failure::tests::disk_full_is_detected_from_enospc_and_from_a_capacity_guard\n\n\
             test result: FAILED. 0 passed; 1 failed\n"
        )
        .is_none());
    }

    #[test]
    fn a_real_diagnostic_still_matches_alongside_a_coincidentally_named_passing_test() {
        assert_eq!(
            kind_of(
                "test infra_failure::tests::disk_full_is_detected_from_enospc_and_from_a_capacity_guard ... ok\n\
                 error: failed to write: No space left on device (os error 28)\n"
            )
            .as_deref(),
            Some("disk-full")
        );
        assert_eq!(
            kind_of(
                "test oom::tests::out_of_memory_is_detected ... ok\n\
                 Memory cgroup out of memory: OOM-killed process 1234 (rustc)\n"
            )
            .as_deref(),
            Some("out-of-memory")
        );
        assert_eq!(
            kind_of(
                "test net::tests::runner_unavailable_is_detected ... ok\n\
                 curl: (6) Could not resolve host: dev.a8n.run\n"
            )
            .as_deref(),
            Some("runner-unavailable")
        );
    }

    #[test]
    fn disk_full_wins_over_a_coincident_later_condition() {
        // Ordering is fixed so one log carrying two phrases keys the same way
        // every time; a full disk is the more specific, actionable cause.
        assert_eq!(
            kind_of("no space left on device\ncould not resolve host: proxy\n").as_deref(),
            Some("disk-full")
        );
    }

    #[test]
    fn job_timeout_is_detected() {
        assert_eq!(
            kind_of(
                "Running tests\n##[error]The job running on runner r1 has exceeded the maximum execution time of 60 minutes.\n"
            )
            .as_deref(),
            Some("job-timeout")
        );
    }

    #[test]
    fn container_not_started_is_detected() {
        assert_eq!(
            kind_of(
                "Error response from daemon: failed to create task for container: OCI runtime create failed\n"
            )
            .as_deref(),
            Some("container-not-started")
        );
    }

    #[test]
    fn runner_deadline_is_detected() {
        assert_eq!(
            kind_of("Post \"https://forge.example.org/api/actions\": context deadline exceeded\n")
                .as_deref(),
            Some("runner-deadline")
        );
    }

    #[test]
    fn no_diagnostic_is_detected_for_a_log_with_nothing_to_act_on() {
        assert_eq!(
            kind_of(
                "Set up job\nCheckout\nRun step\n##[error]Process completed with exit code 1\n"
            )
            .as_deref(),
            Some("no-diagnostic")
        );
    }

    #[test]
    fn no_diagnostic_does_not_match_a_log_with_a_diagnostic_or_no_log() {
        assert!(detect("Set up job\nerror: expected `;`\n").is_none());
        assert!(detect("Set up job\nDiff in src/main.rs:1:\n").is_none());
        assert!(detect("   \n").is_none());
    }

    #[test]
    fn the_new_phrases_inside_a_test_name_do_not_match() {
        assert!(detect(
            "test runner::tests::context_deadline_exceeded_is_detected ... ok\n\
             test job::tests::failed_to_start_container_is_detected ... ok\n\
             test job::tests::exceeded_the_maximum_execution_time ... FAILED\n"
        )
        .is_none());
    }

    #[test]
    fn every_kind_spends_no_attempt_and_an_unknown_kind_does_not() {
        for k in KINDS {
            assert!(InfraFailure::spends_no_attempt(k), "{k}");
        }
        assert!(!InfraFailure::spends_no_attempt("compile-error"));
    }
}
