//! CLAUDE-535: bound the runner's own CI load on one repo.
//!
//! Two gates share one question, "how many runner PRs on this repo have CI
//! pending right now":
//!
//! - the youtrack pickup defers a job while the repo is at its cap
//!   (`concurrency.max_ci_in_flight_per_repo`), and
//! - the automerge behind-base update waits while the repo is at its cap or
//!   while an earlier branch update on the repo is still in CI.
//!
//! Both read live CI state from the forge; only the update holder is kept in
//! memory, because a restart losing it costs at most one extra update.

use std::collections::HashMap;
use std::sync::Mutex;

use tracing::warn;

use crate::ci_failure::store::AgentPullRequest;
use crate::provider::Forge;

/// The PR whose branch update is currently in CI, per `(host, repo)`.
#[derive(Default)]
pub struct UpdateHolders {
    holders: Mutex<HashMap<(String, String), i64>>,
}

impl UpdateHolders {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), i64>> {
        self.holders
            .lock()
            .expect("ci_gate update-holder mutex poisoned")
    }

    /// Record `pr_number` as the repo's update holder, replacing any earlier one.
    pub fn set(&self, host: &str, repo: &str, pr_number: i64) {
        self.lock()
            .insert((host.to_string(), repo.to_string()), pr_number);
    }

    pub fn get(&self, host: &str, repo: &str) -> Option<i64> {
        self.lock()
            .get(&(host.to_string(), repo.to_string()))
            .copied()
    }

    /// Free the repo's slot, but only if `pr_number` still holds it.
    pub fn clear(&self, host: &str, repo: &str, pr_number: i64) {
        let mut holders = self.lock();
        let key = (host.to_string(), repo.to_string());
        if holders.get(&key) == Some(&pr_number) {
            holders.remove(&key);
        }
    }
}

/// Count the open agent PRs on `(host, repo)` whose CI is `pending`, stopping
/// once `cap` is reached. `exclude_issue` skips the PR recorded for that issue
/// (the job being picked up), `exclude_pr` skips that PR number (the PR whose
/// update is being decided). A failed status lookup is logged at `warn` and not
/// counted: the gate then errs toward working, never toward stalling the repo.
pub fn pending_ci(
    forge: &dyn Forge,
    rows: &[AgentPullRequest],
    host: &str,
    repo: &str,
    exclude_issue: Option<&str>,
    exclude_pr: Option<i64>,
    cap: u32,
) -> u32 {
    let mut pending = 0u32;
    for row in rows {
        if pending >= cap {
            break;
        }
        if row.host != host || row.repo != repo {
            continue;
        }
        if exclude_issue == Some(row.issue_key.as_str()) || exclude_pr == Some(row.pr_number) {
            continue;
        }
        match forge.pr_status(host, repo, row.pr_number as u64) {
            Ok(status) if status.state.trim() == "pending" => pending += 1,
            Ok(_) => {}
            Err(e) => warn!(
                repo = %repo,
                pr = row.pr_number,
                issue = %row.issue_key,
                error = %e,
                "CI in-flight count: pr_status failed; PR not counted toward the cap"
            ),
        }
    }
    pending
}

/// Why the behind-base update of `pr_number` must wait, or `None` to go ahead.
/// `cap == 0` disables the gate. The repo's update holder blocks every other PR
/// while its CI is pending (a finished or unreadable holder frees the slot);
/// otherwise the update waits while the other open agent PRs on the repo are at
/// the cap.
pub fn branch_update_wait(
    forge: &dyn Forge,
    holders: &UpdateHolders,
    rows: &[AgentPullRequest],
    host: &str,
    repo: &str,
    pr_number: i64,
    cap: u32,
) -> Option<String> {
    if cap == 0 {
        return None;
    }
    let other_holder = holders
        .get(host, repo)
        .filter(|holder| *holder != pr_number);
    if let Some(holder) = other_holder {
        match forge.pr_status(host, repo, holder as u64) {
            Ok(status) if status.state.trim() == "pending" => {
                return Some(format!(
                    "the branch update of PR #{holder} is still in CI on {repo}"
                ));
            }
            Ok(_) => holders.clear(host, repo, holder),
            Err(e) => {
                warn!(
                    repo = %repo,
                    pr = holder,
                    error = %e,
                    "branch-update holder: pr_status failed; freeing the slot"
                );
                holders.clear(host, repo, holder);
            }
        }
    }
    let pending = pending_ci(forge, rows, host, repo, None, Some(pr_number), cap);
    if pending >= cap {
        return Some(format!(
            "{pending} other runner PR(s) on {repo} have CI pending (cap {cap})"
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;
    use crate::provider::cli::CliBackend;
    use std::sync::Arc;
    use std::time::Duration;

    const HOST: &str = "dev.example.org";
    const REPO: &str = "acme/widgets";

    fn row(issue: &str, pr: i64) -> AgentPullRequest {
        AgentPullRequest {
            id: pr,
            action_name: "act".to_string(),
            issue_key: issue.to_string(),
            host: HOST.to_string(),
            repo: REPO.to_string(),
            pr_number: pr,
            head_branch: format!("fix/{issue}"),
            fix_attempts: 0,
            state: "open".to_string(),
            created_at: String::new(),
            updated_at: String::new(),
            salvaged: false,
            finished: false,
            conflict_resolve_attempts: 0,
            conflict_resolve_started_at: None,
            conflict_files: Vec::new(),
        }
    }

    fn status_args(pr: i64) -> Vec<String> {
        vec![
            "--host".to_string(),
            HOST.to_string(),
            "pr".to_string(),
            "status".to_string(),
            format!("{REPO}#{pr}"),
            "--json".to_string(),
        ]
    }

    /// A forge whose PRs report the given CI states; a PR absent from `states`
    /// has no canned response, so its `pr_status` fails.
    fn forge(states: &[(i64, &str)]) -> CliBackend {
        let mut runner = StubRunner::new();
        for (pr, state) in states {
            let args = status_args(*pr);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let json = format!(r#"{{"pr":{pr},"state":"{state}","mergeable":true}}"#);
            runner = runner.ok("fj", &args, &json);
        }
        CliBackend::new(Arc::new(runner)).with_fj_read_retry_waits([Duration::ZERO; 3])
    }

    #[test]
    fn pending_ci_counts_only_pending_prs_and_stops_at_the_cap() {
        let rows = [row("A-1", 1), row("A-2", 2), row("A-3", 3), row("A-4", 4)];
        let f = forge(&[
            (1, "pending"),
            (2, "success"),
            (3, "pending"),
            (4, "pending"),
        ]);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, None, None, 10), 3);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, None, None, 2), 2);
    }

    #[test]
    fn pending_ci_skips_the_jobs_own_pr_and_other_repos() {
        let mut other = row("B-1", 9);
        other.repo = "acme/gadgets".to_string();
        let rows = [row("A-1", 1), row("A-2", 2), other];
        let f = forge(&[(1, "pending"), (2, "pending"), (9, "pending")]);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, Some("A-1"), None, 10), 1);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, None, Some(2), 10), 1);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, None, None, 10), 2);
    }

    #[test]
    fn a_failed_status_lookup_is_not_counted() {
        let rows = [row("A-1", 1), row("A-2", 2), row("A-3", 3)];
        // PR 2 has no canned response, so its lookup fails.
        let f = forge(&[(1, "pending"), (3, "pending")]);
        assert_eq!(pending_ci(&f, &rows, HOST, REPO, None, None, 10), 2);
    }

    #[test]
    fn update_waits_at_the_cap_and_goes_below_it() {
        let holders = UpdateHolders::new();
        let rows = [row("A-1", 1), row("A-2", 2), row("A-3", 3), row("A-4", 4)];
        let f = forge(&[
            (1, "pending"),
            (2, "pending"),
            (3, "pending"),
            (4, "success"),
        ]);
        let wait = branch_update_wait(&f, &holders, &rows, HOST, REPO, 4, 3);
        assert!(
            wait.is_some_and(|w| w.contains("cap 3")),
            "at the cap waits"
        );
        assert_eq!(
            branch_update_wait(&f, &holders, &rows, HOST, REPO, 4, 4),
            None,
            "below the cap goes ahead"
        );
    }

    #[test]
    fn update_cap_zero_disables_the_gate() {
        let holders = UpdateHolders::new();
        holders.set(HOST, REPO, 1);
        let rows = [row("A-1", 1), row("A-2", 2)];
        let f = forge(&[(1, "pending"), (2, "pending")]);
        assert_eq!(
            branch_update_wait(&f, &holders, &rows, HOST, REPO, 2, 0),
            None
        );
    }

    #[test]
    fn second_update_waits_while_the_holders_ci_is_pending_then_goes() {
        let holders = UpdateHolders::new();
        holders.set(HOST, REPO, 1);
        let rows = [row("A-1", 1), row("A-2", 2)];

        let pending = forge(&[(1, "pending"), (2, "success")]);
        let wait = branch_update_wait(&pending, &holders, &rows, HOST, REPO, 2, 3);
        assert!(
            wait.is_some_and(|w| w.contains("PR #1")),
            "holder pending waits"
        );
        assert_eq!(holders.get(HOST, REPO), Some(1), "slot stays held");

        let finished = forge(&[(1, "success"), (2, "success")]);
        assert_eq!(
            branch_update_wait(&finished, &holders, &rows, HOST, REPO, 2, 3),
            None,
            "a finished holder frees the slot"
        );
        assert_eq!(holders.get(HOST, REPO), None);
    }

    #[test]
    fn the_holder_itself_is_not_blocked_by_its_own_slot() {
        let holders = UpdateHolders::new();
        holders.set(HOST, REPO, 2);
        let rows = [row("A-2", 2)];
        let f = forge(&[(2, "pending")]);
        assert_eq!(
            branch_update_wait(&f, &holders, &rows, HOST, REPO, 2, 3),
            None
        );
    }

    #[test]
    fn an_unreadable_holder_frees_the_slot() {
        let holders = UpdateHolders::new();
        holders.set(HOST, REPO, 1);
        let rows = [row("A-2", 2)];
        // PR 1 has no canned response.
        let f = forge(&[(2, "success")]);
        assert_eq!(
            branch_update_wait(&f, &holders, &rows, HOST, REPO, 2, 3),
            None
        );
        assert_eq!(holders.get(HOST, REPO), None);
    }

    #[test]
    fn clear_only_frees_the_slot_for_its_own_holder() {
        let holders = UpdateHolders::new();
        holders.set(HOST, REPO, 5);
        holders.clear(HOST, REPO, 4);
        assert_eq!(holders.get(HOST, REPO), Some(5));
        holders.clear(HOST, REPO, 5);
        assert_eq!(holders.get(HOST, REPO), None);
    }
}
