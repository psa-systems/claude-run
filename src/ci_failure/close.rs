//! CLAUDE-277: the pure decision the merge-close webhook path makes once it
//! has a merged PR's head commit and the runs list for its repo - which run
//! (if any) is the failing workflow's run at that commit, and was it green.
//!
//! Kept as a free function over [`crate::forge_log::CommitRun`] rather than
//! inline in `src/web/webhooks.rs` so it is unit-testable with no HTTP, DB, or
//! subprocess in the loop.

use crate::forge_log::CommitRun;

/// The latest (highest `global_id`) run of `workflow_id` at `head_sha` among
/// `runs`, or `None` when no run matches both. "Latest" matters because a
/// re-run of the same commit's workflow can leave more than one run at the
/// same commit; the most recent one is authoritative.
pub fn latest_run_at_commit<'a>(
    runs: &'a [CommitRun],
    workflow_id: &str,
    head_sha: &str,
) -> Option<&'a CommitRun> {
    runs.iter()
        .filter(|r| r.workflow_id == workflow_id && r.commit_sha == head_sha)
        .max_by_key(|r| r.global_id)
}

/// True for a run status that denotes the workflow is green.
pub fn is_green(status: &str) -> bool {
    status.eq_ignore_ascii_case("success")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(global_id: i64, workflow_id: &str, commit_sha: &str, status: &str) -> CommitRun {
        CommitRun {
            global_id,
            index: global_id,
            workflow_id: workflow_id.to_string(),
            commit_sha: commit_sha.to_string(),
            status: status.to_string(),
            html_url: format!("https://dev.a8n.run/acme/widgets/actions/runs/{global_id}"),
        }
    }

    #[test]
    fn matches_workflow_and_commit() {
        let runs = vec![
            run(1, "check.yml", "aaa", "success"),
            run(2, "other.yml", "bbb", "success"),
        ];
        let m = latest_run_at_commit(&runs, "check.yml", "aaa").unwrap();
        assert_eq!(m.global_id, 1);
    }

    #[test]
    fn picks_the_latest_of_several_runs_at_the_same_commit() {
        let runs = vec![
            run(1, "check.yml", "aaa", "failure"),
            run(5, "check.yml", "aaa", "success"),
            run(3, "check.yml", "aaa", "failure"),
        ];
        let m = latest_run_at_commit(&runs, "check.yml", "aaa").unwrap();
        assert_eq!(m.global_id, 5);
    }

    #[test]
    fn no_match_when_commit_absent() {
        let runs = vec![run(1, "check.yml", "aaa", "success")];
        assert!(latest_run_at_commit(&runs, "check.yml", "zzz").is_none());
    }

    #[test]
    fn no_match_when_workflow_differs() {
        let runs = vec![run(1, "check.yml", "aaa", "success")];
        assert!(latest_run_at_commit(&runs, "other.yml", "aaa").is_none());
    }

    #[test]
    fn green_is_case_insensitive_success() {
        assert!(is_green("success"));
        assert!(is_green("Success"));
        assert!(!is_green("failure"));
        assert!(!is_green(""));
    }
}
