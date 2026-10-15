//! Async functions over [`DbPool`] for the CI-failure store
//! (`ci_failures`, `ci_failure_tests`, `ci_failure_runs`,
//! `agent_pull_requests`; `migrations/sqlite/0021_ci_failures.sql` and
//! `migrations/postgres/0002_ci_failures.sql`). Every statement goes through
//! [`db::q`], the dialect chokepoint every other module in the tree uses, so
//! this runs unchanged on either backend.

use sqlx::Row;

use crate::db::{self, DbPool};
use crate::error::Result;

/// A `ci_failures` row a fingerprint matched, returned by [`find_match`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureMatch {
    pub id: i64,
    pub issue_key: Option<String>,
    pub state: String,
}

/// A `ci_failures` row's own fields, without the derived match ranking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub id: i64,
    pub repo: String,
    pub workflow_id: String,
    pub job_name: String,
    pub infra_kind: Option<String>,
    pub issue_key: Option<String>,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
}

/// An `agent_pull_requests` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPullRequest {
    pub id: i64,
    pub action_name: String,
    pub issue_key: String,
    pub host: String,
    pub repo: String,
    pub pr_number: i64,
    pub head_branch: String,
    pub fix_attempts: i64,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    /// True when this PR was opened by the timeout salvage path (CLAUDE-379)
    /// and not yet cleared by a resumed run's successful finish (CLAUDE-429).
    /// Blocks automerge regardless of draft status. See `crate::db::flag`.
    pub salvaged: bool,
}

fn failure_from_row(row: &sqlx::any::AnyRow) -> Result<Failure> {
    Ok(Failure {
        id: row.try_get("id")?,
        repo: row.try_get("repo")?,
        workflow_id: row.try_get("workflow_id")?,
        job_name: row.try_get("job_name")?,
        infra_kind: row.try_get("infra_kind")?,
        issue_key: row.try_get("issue_key")?,
        state: row.try_get("state")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn agent_pr_from_row(row: &sqlx::any::AnyRow) -> Result<AgentPullRequest> {
    Ok(AgentPullRequest {
        id: row.try_get("id")?,
        action_name: row.try_get("action_name")?,
        issue_key: row.try_get("issue_key")?,
        host: row.try_get("host")?,
        repo: row.try_get("repo")?,
        pr_number: row.try_get("pr_number")?,
        head_branch: row.try_get("head_branch")?,
        fix_attempts: row.try_get("fix_attempts")?,
        state: row.try_get("state")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        salvaged: db::flag(row, "salvaged")?,
    })
}

/// Numeric priority a match's `state` sorts by: `open` first, then
/// `resolved`, then `pr_only`. Spelled as a `CASE` in SQL rather than fetched
/// and sorted in Rust, so the "ties go to the newest" tiebreak is one ORDER BY
/// rather than a second pass.
const STATE_PRIORITY_CASE: &str =
    "CASE state WHEN 'open' THEN 0 WHEN 'resolved' THEN 1 WHEN 'pr_only' THEN 2 ELSE 3 END";

/// Find the best `ci_failures` row that shares at least one test id with
/// `test_ids`, within the same `(repo, workflow_id, job_name)`. An `open`
/// match wins over `resolved`, which wins over `pr_only`; ties go to the
/// newest (`updated_at`).
pub async fn find_match(
    pool: &DbPool,
    repo: &str,
    workflow_id: &str,
    job_name: &str,
    test_ids: &[String],
) -> Result<Option<FailureMatch>> {
    if test_ids.is_empty() {
        return Ok(None);
    }
    let placeholders = vec!["?"; test_ids.len()].join(", ");
    let sql = format!(
        "SELECT f.id AS id, f.issue_key AS issue_key, f.state AS state \
         FROM ci_failures f \
         JOIN ci_failure_tests t ON t.failure_id = f.id \
         WHERE f.repo = ? AND f.workflow_id = ? AND f.job_name = ? AND t.test_id IN ({placeholders}) \
         ORDER BY {STATE_PRIORITY_CASE} ASC, f.updated_at DESC \
         LIMIT 1"
    );
    let sql = db::q(pool, &sql).into_owned();
    let mut query = sqlx::query(&sql)
        .bind(repo)
        .bind(workflow_id)
        .bind(job_name);
    for test_id in test_ids {
        query = query.bind(test_id);
    }
    let row = query.fetch_optional(pool).await?;
    Ok(match row {
        Some(row) => Some(FailureMatch {
            id: row.try_get("id")?,
            issue_key: row.try_get("issue_key")?,
            state: row.try_get("state")?,
        }),
        None => None,
    })
}

/// Fetch one `ci_failures` row by id.
pub async fn get_failure(pool: &DbPool, id: i64) -> Result<Option<Failure>> {
    let row = sqlx::query(&db::q(
        pool,
        "SELECT id, repo, workflow_id, job_name, infra_kind, issue_key, state, created_at, updated_at \
         FROM ci_failures WHERE id = ?",
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(failure_from_row).transpose()
}

/// Insert a new `ci_failures` row. Returns its id.
#[allow(clippy::too_many_arguments)]
pub async fn record_failure(
    pool: &DbPool,
    repo: &str,
    workflow_id: &str,
    job_name: &str,
    infra_kind: Option<&str>,
    issue_key: Option<&str>,
    state: &str,
    now: &str,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(&db::q(
        pool,
        "INSERT INTO ci_failures (repo, workflow_id, job_name, infra_kind, issue_key, state, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    ))
    .bind(repo)
    .bind(workflow_id)
    .bind(job_name)
    .bind(infra_kind)
    .bind(issue_key)
    .bind(state)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Add every id in `test_ids` to `failure_id`'s fingerprint. Idempotent: an id
/// already recorded on a repeat sighting is skipped rather than erroring.
pub async fn add_tests(pool: &DbPool, failure_id: i64, test_ids: &[String]) -> Result<()> {
    for test_id in test_ids {
        sqlx::query(&db::q(
            pool,
            "INSERT INTO ci_failure_tests (failure_id, test_id) VALUES (?, ?) \
             ON CONFLICT DO NOTHING",
        ))
        .bind(failure_id)
        .bind(test_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Record one sighting of `failure_id`. Returns the new `ci_failure_runs` id.
pub async fn attach_run(
    pool: &DbPool,
    failure_id: i64,
    run_index: i64,
    commit_sha: &str,
    branch: &str,
    pr_number: Option<i64>,
    ts: &str,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(&db::q(
        pool,
        "INSERT INTO ci_failure_runs (failure_id, run_index, commit_sha, branch, pr_number, ts) \
         VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
    ))
    .bind(failure_id)
    .bind(run_index)
    .bind(commit_sha)
    .bind(branch)
    .bind(pr_number)
    .bind(ts)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Set `run_id`'s own failing-id fingerprint, newline-joined (fingerprint ids
/// never contain a newline; `find_match`'s test-id overlap match is unrelated
/// and never queries this column). CLAUDE-371: only the agent-PR fix-attempt
/// flow needs one sighting's own fingerprint (to compare it against the
/// immediately previous sighting on the same PR), so this and
/// [`previous_run_test_ids`] are its only callers; every other `attach_run`
/// caller leaves the column null.
pub async fn set_run_test_ids(pool: &DbPool, run_id: i64, test_ids: &[String]) -> Result<()> {
    let joined = if test_ids.is_empty() {
        None
    } else {
        Some(test_ids.join("\n"))
    };
    sqlx::query(&db::q(
        pool,
        "UPDATE ci_failure_runs SET test_ids = ? WHERE id = ?",
    ))
    .bind(joined)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `failure_id`'s previous sighting on `pr_number` (the newest
/// `ci_failure_runs` row strictly older than `before_run_id`) whose recorded
/// fingerprint is non-empty, as `(run_index, test_ids)`. `None` covers both
/// "no earlier sighting on this PR" and "the earlier sighting's fingerprint
/// was empty or never recorded": CLAUDE-371 requires the latter to fall back
/// to the plain attempt-cap flow rather than comparing against a fingerprint
/// that isn't there.
pub async fn previous_run_test_ids(
    pool: &DbPool,
    failure_id: i64,
    pr_number: i64,
    before_run_id: i64,
) -> Result<Option<(i64, Vec<String>)>> {
    let row: Option<(i64, Option<String>)> = sqlx::query_as(&db::q(
        pool,
        "SELECT run_index, test_ids FROM ci_failure_runs \
         WHERE failure_id = ? AND pr_number = ? AND id < ? \
         ORDER BY id DESC LIMIT 1",
    ))
    .bind(failure_id)
    .bind(pr_number)
    .bind(before_run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(run_index, ids)| {
        ids.filter(|s| !s.is_empty())
            .map(|s| (run_index, s.split('\n').map(str::to_string).collect()))
    }))
}

/// Point `failure_id` at `issue_key` (or clear it with `None`).
pub async fn set_issue(
    pool: &DbPool,
    failure_id: i64,
    issue_key: Option<&str>,
    now: &str,
) -> Result<()> {
    sqlx::query(&db::q(
        pool,
        "UPDATE ci_failures SET issue_key = ?, updated_at = ? WHERE id = ?",
    ))
    .bind(issue_key)
    .bind(now)
    .bind(failure_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Transition `failure_id` to `state` (`open`, `resolved`, `pr_only`).
pub async fn set_failure_state(
    pool: &DbPool,
    failure_id: i64,
    state: &str,
    now: &str,
) -> Result<()> {
    sqlx::query(&db::q(
        pool,
        "UPDATE ci_failures SET state = ?, updated_at = ? WHERE id = ?",
    ))
    .bind(state)
    .bind(now)
    .bind(failure_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every distinct branch `failure_id` has been seen on, sorted.
pub async fn distinct_branches(pool: &DbPool, failure_id: i64) -> Result<Vec<String>> {
    let branches: Vec<String> = sqlx::query_scalar(&db::q(
        pool,
        "SELECT DISTINCT branch FROM ci_failure_runs WHERE failure_id = ? ORDER BY branch",
    ))
    .bind(failure_id)
    .fetch_all(pool)
    .await?;
    Ok(branches)
}

/// The `ci_failures` row currently claimed by `issue_key`, if any (CLAUDE-276:
/// used to find or lazily create the store row backing an agent's own PR, whose
/// `ci_failures` row is addressed by issue key rather than by test overlap).
pub async fn failure_for_issue(pool: &DbPool, issue_key: &str) -> Result<Option<Failure>> {
    let row = sqlx::query(&db::q(
        pool,
        "SELECT id, repo, workflow_id, job_name, infra_kind, issue_key, state, created_at, updated_at \
         FROM ci_failures WHERE issue_key = ?",
    ))
    .bind(issue_key)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(failure_from_row).transpose()
}

/// Every distinct non-null PR number `failure_id` has been seen sighted on,
/// sorted. CLAUDE-276: used to comment the same issue link onto every open
/// PR a `pr_only` failure's two distinct branches turned out to be on.
pub async fn pr_numbers_for_failure(pool: &DbPool, failure_id: i64) -> Result<Vec<i64>> {
    let numbers: Vec<i64> = sqlx::query_scalar(&db::q(
        pool,
        "SELECT DISTINCT pr_number FROM ci_failure_runs \
         WHERE failure_id = ? AND pr_number IS NOT NULL ORDER BY pr_number",
    ))
    .bind(failure_id)
    .fetch_all(pool)
    .await?;
    Ok(numbers)
}

/// Every run index `failure_id` was sighted at on PR `pr_number`, sorted.
/// CLAUDE-276: used to list every fix attempt's run when an agent's own PR
/// hits the `agent_pr_fix_attempts` ceiling and is flagged `Needs Review`.
pub async fn run_indexes_for_failure_pr(
    pool: &DbPool,
    failure_id: i64,
    pr_number: i64,
) -> Result<Vec<i64>> {
    let indexes: Vec<i64> = sqlx::query_scalar(&db::q(
        pool,
        "SELECT run_index FROM ci_failure_runs \
         WHERE failure_id = ? AND pr_number = ? ORDER BY run_index",
    ))
    .bind(failure_id)
    .bind(pr_number)
    .fetch_all(pool)
    .await?;
    Ok(indexes)
}

/// Record a PR the agent opened. `salvaged` is true for a timeout-salvage PR
/// (`Finalized::TimedOut`), false otherwise (CLAUDE-429). Returns its
/// `agent_pull_requests` id.
#[allow(clippy::too_many_arguments)]
pub async fn record_agent_pr(
    pool: &DbPool,
    action_name: &str,
    issue_key: &str,
    host: &str,
    repo: &str,
    pr_number: i64,
    head_branch: &str,
    state: &str,
    salvaged: bool,
    now: &str,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(&db::q(
        pool,
        "INSERT INTO agent_pull_requests \
         (action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, salvaged, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?) RETURNING id",
    ))
    .bind(action_name)
    .bind(issue_key)
    .bind(host)
    .bind(repo)
    .bind(pr_number)
    .bind(head_branch)
    .bind(state)
    .bind(salvaged as i64)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// Clear `id`'s `salvaged` flag: called only from the resumed-success path
/// (CLAUDE-429), once a resumed run on a salvaged PR finishes successfully.
pub async fn clear_salvaged(pool: &DbPool, id: i64, now: &str) -> Result<()> {
    sqlx::query(&db::q(
        pool,
        "UPDATE agent_pull_requests SET salvaged = 0, updated_at = ? WHERE id = ?",
    ))
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The agent's PR at `(host, repo, pr_number)`, the unique key a PR is
/// addressed by regardless of which issue currently claims it.
pub async fn agent_pr(
    pool: &DbPool,
    host: &str,
    repo: &str,
    pr_number: i64,
) -> Result<Option<AgentPullRequest>> {
    let row = sqlx::query(&db::q(
        pool,
        "SELECT id, action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, salvaged, created_at, updated_at \
         FROM agent_pull_requests WHERE host = ? AND repo = ? AND pr_number = ?",
    ))
    .bind(host)
    .bind(repo)
    .bind(pr_number)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(agent_pr_from_row).transpose()
}

/// The most recently updated agent PR filed against `issue_key`, so a
/// re-queued issue continues the same PR's branch rather than opening a new
/// one.
pub async fn agent_pr_for_issue(
    pool: &DbPool,
    issue_key: &str,
) -> Result<Option<AgentPullRequest>> {
    let row = sqlx::query(&db::q(
        pool,
        "SELECT id, action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, salvaged, created_at, updated_at \
         FROM agent_pull_requests WHERE issue_key = ? ORDER BY updated_at DESC LIMIT 1",
    ))
    .bind(issue_key)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(agent_pr_from_row).transpose()
}

/// Every `agent_pull_requests` row still `open`, for the CLAUDE-328
/// reconciler: a webhook is the low-latency trigger for automerge, but a
/// missed delivery must not leave a green PR stuck at Code Review forever, so
/// the reconciler walks this list on its own tick independent of any webhook.
pub async fn open_agent_prs(pool: &DbPool) -> Result<Vec<AgentPullRequest>> {
    let rows = sqlx::query(&db::q(
        pool,
        "SELECT id, action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, salvaged, created_at, updated_at \
         FROM agent_pull_requests WHERE state = 'open' ORDER BY id",
    ))
    .fetch_all(pool)
    .await?;
    rows.iter().map(agent_pr_from_row).collect()
}

/// Increment `id`'s `fix_attempts` by one.
pub async fn bump_fix_attempts(pool: &DbPool, id: i64, now: &str) -> Result<()> {
    sqlx::query(&db::q(
        pool,
        "UPDATE agent_pull_requests SET fix_attempts = fix_attempts + 1, updated_at = ? WHERE id = ?",
    ))
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Transition `id` to `state` (`open`, `merged`, `closed`).
pub async fn set_agent_pr_state(pool: &DbPool, id: i64, state: &str, now: &str) -> Result<()> {
    sqlx::query(&db::q(
        pool,
        "UPDATE agent_pull_requests SET state = ?, updated_at = ? WHERE id = ?",
    ))
    .bind(state)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_pool;

    async fn seed_failure(
        pool: &DbPool,
        repo: &str,
        workflow_id: &str,
        job_name: &str,
        test_ids: &[&str],
        state: &str,
        updated_at: &str,
    ) -> i64 {
        let id = record_failure(
            pool,
            repo,
            workflow_id,
            job_name,
            None,
            None,
            state,
            updated_at,
        )
        .await
        .unwrap();
        let ids: Vec<String> = test_ids.iter().map(|s| s.to_string()).collect();
        add_tests(pool, id, &ids).await.unwrap();
        // record_failure always stamps `now` for both timestamps; tests that
        // need a specific updated_at (for the tie-break case) set it directly.
        sqlx::query(&db::q(
            pool,
            "UPDATE ci_failures SET updated_at = ? WHERE id = ?",
        ))
        .bind(updated_at)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn one_shared_test_id_matches() {
        let pool = test_pool().await;
        let id = seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a", "lib::b"],
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await;

        let found = find_match(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::b".to_string(), "lib::c".to_string()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(found.id, id);
        assert_eq!(found.state, "open");
    }

    #[tokio::test]
    async fn no_shared_test_id_does_not_match() {
        let pool = test_pool().await;
        seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await;

        let found = find_match(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::z".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn the_same_tests_under_a_different_job_do_not_match() {
        let pool = test_pool().await;
        seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await;

        let found = find_match(
            &pool,
            "acme/widgets",
            "checks.yml",
            "lint",
            &["lib::a".to_string()],
        )
        .await
        .unwrap();
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn open_is_preferred_over_resolved() {
        let pool = test_pool().await;
        let resolved_id = seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "resolved",
            "2026-09-05T00:00:00Z",
        )
        .await;
        let open_id = seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await;
        assert_ne!(resolved_id, open_id);

        let found = find_match(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a".to_string()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(found.id, open_id);
        assert_eq!(found.state, "open");
    }

    #[tokio::test]
    async fn ties_go_to_the_newest() {
        let pool = test_pool().await;
        let older = seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await;
        let newer = seed_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a"],
            "open",
            "2026-09-05T00:00:00Z",
        )
        .await;
        assert_ne!(older, newer);

        let found = find_match(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            &["lib::a".to_string()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(found.id, newer);
    }

    #[tokio::test]
    async fn attach_run_and_distinct_branches_roundtrip() {
        let pool = test_pool().await;
        let id = record_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            None,
            None,
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        attach_run(&pool, id, 1, "abc123", "main", None, "2026-09-01T00:00:00Z")
            .await
            .unwrap();
        attach_run(
            &pool,
            id,
            2,
            "def456",
            "feature/x",
            Some(42),
            "2026-09-02T00:00:00Z",
        )
        .await
        .unwrap();
        attach_run(&pool, id, 3, "ghi789", "main", None, "2026-09-03T00:00:00Z")
            .await
            .unwrap();

        let branches = distinct_branches(&pool, id).await.unwrap();
        assert_eq!(branches, vec!["feature/x".to_string(), "main".to_string()]);
    }

    #[tokio::test]
    async fn previous_run_test_ids_reads_back_the_immediately_prior_sighting_on_the_pr() {
        let pool = test_pool().await;
        let id = record_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            None,
            None,
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        let run_1 = attach_run(
            &pool,
            id,
            1,
            "abc123",
            "fix/x",
            Some(42),
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        set_run_test_ids(&pool, run_1, &["mod_a::t1".to_string()])
            .await
            .unwrap();
        let run_2 = attach_run(
            &pool,
            id,
            2,
            "def456",
            "fix/x",
            Some(42),
            "2026-09-02T00:00:00Z",
        )
        .await
        .unwrap();

        let (prev_index, prev_ids) = previous_run_test_ids(&pool, id, 42, run_2)
            .await
            .unwrap()
            .expect("run 1's fingerprint");
        assert_eq!(prev_index, 1);
        assert_eq!(prev_ids, vec!["mod_a::t1".to_string()]);

        // No sighting older than run 1 on this PR.
        assert_eq!(
            previous_run_test_ids(&pool, id, 42, run_1).await.unwrap(),
            None
        );

        // A never-recorded (null) fingerprint on the prior sighting falls
        // back to "no previous fingerprint" rather than looking like a match.
        let run_3 = attach_run(
            &pool,
            id,
            3,
            "ghi789",
            "fix/y",
            Some(43),
            "2026-09-03T00:00:00Z",
        )
        .await
        .unwrap();
        let run_4 = attach_run(
            &pool,
            id,
            4,
            "jkl012",
            "fix/y",
            Some(43),
            "2026-09-04T00:00:00Z",
        )
        .await
        .unwrap();
        let _ = run_3;
        assert_eq!(
            previous_run_test_ids(&pool, id, 43, run_4).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn set_issue_and_set_failure_state_roundtrip() {
        let pool = test_pool().await;
        let id = record_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            None,
            None,
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();

        set_issue(&pool, id, Some("CLAUDE-999"), "2026-09-02T00:00:00Z")
            .await
            .unwrap();
        set_failure_state(&pool, id, "resolved", "2026-09-03T00:00:00Z")
            .await
            .unwrap();

        let failure = get_failure(&pool, id).await.unwrap().unwrap();
        assert_eq!(failure.issue_key, Some("CLAUDE-999".to_string()));
        assert_eq!(failure.state, "resolved");
        assert_eq!(failure.updated_at, "2026-09-03T00:00:00Z");
    }

    #[tokio::test]
    async fn agent_pr_roundtrip_and_bump_and_state() {
        let pool = test_pool().await;
        let id = record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-42",
            "dev.a8n.run",
            "contoso/claude-run",
            17,
            "fix/CLAUDE-42",
            "open",
            false,
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();

        let by_pr = agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 17)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_pr.id, id);
        assert_eq!(by_pr.fix_attempts, 0);
        assert!(!by_pr.salvaged);

        let by_issue = agent_pr_for_issue(&pool, "CLAUDE-42")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_issue.id, id);

        bump_fix_attempts(&pool, id, "2026-09-02T00:00:00Z")
            .await
            .unwrap();
        set_agent_pr_state(&pool, id, "merged", "2026-09-03T00:00:00Z")
            .await
            .unwrap();

        let after = agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 17)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.fix_attempts, 1);
        assert_eq!(after.state, "merged");
        assert_eq!(after.updated_at, "2026-09-03T00:00:00Z");
    }

    #[tokio::test]
    async fn salvaged_flag_roundtrips_and_clears() {
        let pool = test_pool().await;
        let id = record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-43",
            "dev.a8n.run",
            "contoso/claude-run",
            18,
            "fix/CLAUDE-43",
            "open",
            true,
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();

        let salvaged = agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 18)
            .await
            .unwrap()
            .unwrap();
        assert!(salvaged.salvaged);

        clear_salvaged(&pool, id, "2026-09-02T00:00:00Z")
            .await
            .unwrap();

        let cleared = agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 18)
            .await
            .unwrap()
            .unwrap();
        assert!(!cleared.salvaged);
        assert_eq!(cleared.updated_at, "2026-09-02T00:00:00Z");
    }

    /// `clear_salvaged` is the only mutator that ever flips `salvaged` to
    /// false (CLAUDE-429): a state change from any other path, such as a
    /// human clearing a draft's `WIP: ` prefix (which surfaces here only as
    /// a PR state transition, never a direct column write), leaves it set.
    #[tokio::test]
    async fn set_agent_pr_state_does_not_clear_salvaged() {
        let pool = test_pool().await;
        let id = record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-43",
            "dev.a8n.run",
            "contoso/claude-run",
            19,
            "fix/CLAUDE-43",
            "open",
            true,
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();

        set_agent_pr_state(&pool, id, "closed", "2026-09-02T00:00:00Z")
            .await
            .unwrap();

        let rec = agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 19)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.state, "closed");
        assert!(
            rec.salvaged,
            "a state transition must never clear the salvaged flag"
        );
    }

    #[tokio::test]
    async fn failure_for_issue_finds_the_claiming_row() {
        let pool = test_pool().await;
        let id = record_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            None,
            Some("CLAUDE-42"),
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();

        let found = failure_for_issue(&pool, "CLAUDE-42")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, id);
        assert_eq!(failure_for_issue(&pool, "CLAUDE-999").await.unwrap(), None);
    }

    #[tokio::test]
    async fn pr_numbers_and_run_indexes_roundtrip() {
        let pool = test_pool().await;
        let id = record_failure(
            &pool,
            "acme/widgets",
            "checks.yml",
            "test",
            None,
            Some("CLAUDE-42"),
            "open",
            "2026-09-01T00:00:00Z",
        )
        .await
        .unwrap();
        attach_run(&pool, id, 1, "abc123", "main", None, "2026-09-01T00:00:00Z")
            .await
            .unwrap();
        attach_run(
            &pool,
            id,
            2,
            "def456",
            "fix/thing",
            Some(17),
            "2026-09-02T00:00:00Z",
        )
        .await
        .unwrap();
        attach_run(
            &pool,
            id,
            3,
            "ghi789",
            "fix/thing",
            Some(17),
            "2026-09-03T00:00:00Z",
        )
        .await
        .unwrap();
        attach_run(
            &pool,
            id,
            4,
            "jkl012",
            "main",
            Some(23),
            "2026-09-04T00:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            pr_numbers_for_failure(&pool, id).await.unwrap(),
            vec![17, 23]
        );
        assert_eq!(
            run_indexes_for_failure_pr(&pool, id, 17).await.unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            run_indexes_for_failure_pr(&pool, id, 23).await.unwrap(),
            vec![4]
        );
    }

    #[tokio::test]
    async fn an_empty_test_id_set_matches_nothing() {
        let pool = test_pool().await;
        let found = find_match(&pool, "acme/widgets", "checks.yml", "test", &[])
            .await
            .unwrap();
        assert_eq!(found, None);
    }
}
