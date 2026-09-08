//! CLAUDE-264: deterministic infrastructure-failure detector.
//!
//! A CI failure caused by the RUNNER rather than the commit (a full disk, an
//! OOM kill, an unreachable package registry) is identical on every branch and
//! is not fixable on any of them. The CLAUDE-190 root-cause dedupe key contains
//! `branch`, so one such host condition mints a fresh issue per PR branch and
//! the duplicates never collapse. [`detect`] gives the mint a branch-agnostic
//! `kind` id to key on instead (see [`crate::mint`]).
//!
//! Modelled on [`crate::autofix`]: pure string work over the FULL redacted log,
//! no LLM and no I/O, so it sees the whole log rather than the bounded copy the
//! classifier prompt gets ([`crate::triage`] bounds that to 48 KB, which for a
//! post-steps capacity abort is exactly the evidence that gets cut).
//!
//! Nothing about triage buckets or routing changes: an infra failure is still
//! classified and routed as it was, because reclaiming disk is a human action.
//!
//! The detector set is deliberately conservative. An over-broad pattern would
//! merge two unrelated CODE failures across branches into one issue, which is
//! worse than leaving a host condition ungrouped, so each detector requires an
//! unambiguous phrase and an undetected condition is the preferred failure mode.

/// An infrastructure / runner condition recognized in a job log. Same shape as
/// [`crate::autofix::AutoFix`]: a stable `kind` id (what the dedupe key is built
/// from, so the same condition keys identically however the message is worded)
/// plus a human-readable statement of the problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraFailure {
    /// Stable detector id: `disk-full`, `out-of-memory`, `runner-unavailable`.
    /// This, not the matched line, is what [`crate::mint`] hashes.
    pub kind: String,
    /// Human-readable statement of the host condition, for the log line and the
    /// operator reading it.
    pub problem: String,
}

/// Run the infrastructure detectors against a full redacted job log, most
/// specific first. `None` when no detector matches, which is the common case: a
/// code failure must never match here.
pub fn detect(log: &str) -> Option<InfraFailure> {
    let lower = log.to_ascii_lowercase();
    detect_disk_full(&lower)
        .or_else(|| detect_out_of_memory(&lower))
        .or_else(|| detect_runner_unavailable(&lower))
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
/// Note the overlap with [`crate::triage`]'s `TRANSIENT_SIGNATURES`, which
/// already carries `could not resolve host` and `connection reset`: such a
/// failure is routed to bucket 3 and mints nothing, so this kind reaches the
/// mint only if that list changes. It is implemented because the condition is
/// real and the classification of it belongs here, not because it fires today.
/// See CLAUDE-266.
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
    fn disk_full_wins_over_a_coincident_later_condition() {
        // Ordering is fixed so one log carrying two phrases keys the same way
        // every time; a full disk is the more specific, actionable cause.
        assert_eq!(
            kind_of("no space left on device\ncould not resolve host: proxy\n").as_deref(),
            Some("disk-full")
        );
    }
}
