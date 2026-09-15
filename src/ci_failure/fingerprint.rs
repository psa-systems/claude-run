//! Turn a redacted job log into the ids the CI-failure store matches on.
//!
//! Assumes cargo test output is the only format that needs per-test parsing.
//! A repository whose CI runs something else falls back to
//! [`crate::mint::failure_signature`], the same normalized first-diagnostic
//! line the mint used before this store existed; revise this when a
//! non-cargo repository needs finer matching.

use std::collections::BTreeSet;

use crate::infra_failure;
use crate::mint;

/// A line naming the target binary under test, e.g.
/// `     Running unittests src/lib.rs (target/debug/deps/billing_service-...)`
/// or `     Running tests/invitations.rs (target/debug/deps/invitations-...)`.
const RUNNING_PREFIX: &str = "Running ";

/// A line reporting one failed test, e.g.
/// `test create_list_revoke_roundtrip ... FAILED`.
const TEST_PREFIX: &str = "test ";
const FAILED_SUFFIX: &str = "FAILED";

/// A line naming a target that failed to link or run at all, e.g.
/// ``    `--test invitation_team_id` ``, under a preceding
/// `error: N targets failed:` line.
const TARGETS_FAILED_MARKER: &str = "target failed:";
const TARGETS_FAILED_MARKER_PLURAL: &str = "targets failed:";
const TARGET_LINE_MARKER: &str = "--test ";

/// Parse cargo test output into sorted, deduplicated `<target>::<test>` ids.
///
/// The target comes from the nearest preceding `Running <path>.rs (...)` line;
/// `Running unittests src/lib.rs (...)` gives `lib`. The test comes from
/// `test <name> ... FAILED` lines, attributed to whichever target line
/// preceded it. A target named under an `error: N targets failed:` block that
/// never produced a parsed `FAILED` line (it failed to build or run, not to
/// pass) contributes a bare `<target>` with no `::`.
pub fn failing_tests(log: &str) -> Vec<String> {
    let mut ids: BTreeSet<String> = BTreeSet::new();
    let mut targets_with_tests: BTreeSet<String> = BTreeSet::new();
    let mut current_target: Option<String> = None;
    let mut in_targets_failed = false;

    for line in log.lines() {
        let trimmed = line.trim();

        if let Some(rest) = trimmed.strip_prefix(RUNNING_PREFIX) {
            if let Some(target) = parse_running_target(rest) {
                current_target = Some(target);
            }
            in_targets_failed = false;
            continue;
        }

        if let Some(name) = parse_failed_test(trimmed) {
            if let Some(target) = &current_target {
                ids.insert(format!("{target}::{name}"));
                targets_with_tests.insert(target.clone());
            }
            in_targets_failed = false;
            continue;
        }

        if trimmed.starts_with("error:")
            && (trimmed.contains(TARGETS_FAILED_MARKER_PLURAL)
                || trimmed.contains(TARGETS_FAILED_MARKER))
        {
            in_targets_failed = true;
            continue;
        }

        if in_targets_failed {
            if let Some(target) = parse_target_line(trimmed) {
                if !targets_with_tests.contains(&target) {
                    ids.insert(target);
                }
            }
        }
    }

    ids.into_iter().collect()
}

/// The full fingerprint of a job log: `["infra:<kind>"]` when
/// [`infra_failure::detect`] recognises a host condition, else
/// [`failing_tests`], falling back to the normalized
/// [`mint::failure_signature`] as a single id when neither parses anything.
pub fn fingerprint(log: &str) -> Vec<String> {
    if let Some(infra) = infra_failure::detect(log) {
        return vec![format!("infra:{}", infra.kind)];
    }
    let tests = failing_tests(log);
    if !tests.is_empty() {
        return tests;
    }
    match mint::failure_signature(log) {
        Some(sig) => vec![sig],
        None => Vec::new(),
    }
}

/// `unittests src/lib.rs (target/debug/deps/...)` -> `lib`,
/// `tests/invitations.rs (target/debug/deps/...)` -> `invitations`.
fn parse_running_target(rest: &str) -> Option<String> {
    let without_binary = match rest.find('(') {
        Some(idx) => &rest[..idx],
        None => rest,
    };
    let path = without_binary.split_whitespace().last()?;
    let basename = path.rsplit('/').next().unwrap_or(path);
    basename.strip_suffix(".rs").map(str::to_string)
}

/// `test create_list_revoke_roundtrip ... FAILED` -> `create_list_revoke_roundtrip`.
fn parse_failed_test(trimmed: &str) -> Option<String> {
    let rest = trimmed.strip_prefix(TEST_PREFIX)?;
    if !rest.trim_end().ends_with(FAILED_SUFFIX) {
        return None;
    }
    rest.split_whitespace().next().map(str::to_string)
}

/// ``    `--test invitation_team_id` `` -> `invitation_team_id`.
fn parse_target_line(trimmed: &str) -> Option<String> {
    let inner = trimmed.trim_matches('`');
    let rest = inner.strip_prefix(TARGET_LINE_MARKER)?;
    let name = rest.trim();
    (!name.is_empty()).then(|| name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CLAUDE-272 acceptance criterion: these lines from billing-service run
    /// 5874 (timestamps stripped) must yield exactly these three ids, sorted.
    #[test]
    fn failing_tests_parses_cargo_test_output() {
        let log = "\
     Running unittests src/lib.rs (target/debug/deps/billing_service-1c58e9b2630780df)
     Running tests/invitation_team_id.rs (target/debug/deps/invitation_team_id-f77f8308d8921a15)
test create_refuses_a_foreign_tenant_team ... FAILED
     Running tests/invitations.rs (target/debug/deps/invitations-920c3d78ee72d1de)
test create_list_revoke_roundtrip ... FAILED
test reinvite_same_email_upserts ... FAILED
error: 2 targets failed:
    `--test invitation_team_id`
    `--test invitations`
";
        assert_eq!(
            failing_tests(log),
            vec![
                "invitation_team_id::create_refuses_a_foreign_tenant_team".to_string(),
                "invitations::create_list_revoke_roundtrip".to_string(),
                "invitations::reinvite_same_email_upserts".to_string(),
            ]
        );
    }

    #[test]
    fn a_target_that_never_produced_a_failed_test_contributes_a_bare_id() {
        let log = "\
     Running tests/link_error.rs (target/debug/deps/link_error-abc)
error: 1 target failed:
    `--test link_error`
";
        assert_eq!(failing_tests(log), vec!["link_error".to_string()]);
    }

    #[test]
    fn no_recognizable_output_yields_no_tests() {
        assert_eq!(
            failing_tests("nothing interesting here\n"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn fingerprint_prefers_infra_over_failing_tests() {
        let log = "no space left on device\ntest foo ... FAILED\n";
        assert_eq!(fingerprint(log), vec!["infra:disk-full".to_string()]);
    }

    #[test]
    fn fingerprint_falls_back_to_the_normalized_signature() {
        let log = "error: could not compile `billing-service` (lib) due to 1 previous error\n";
        let fp = fingerprint(log);
        assert_eq!(fp.len(), 1);
        assert_eq!(fp, vec![mint::failure_signature(log).unwrap()]);
    }

    #[test]
    fn fingerprint_is_empty_when_nothing_parses_at_all() {
        assert_eq!(
            fingerprint("nothing interesting here\n"),
            Vec::<String>::new()
        );
    }
}
