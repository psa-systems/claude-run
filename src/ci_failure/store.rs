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

/// Record a PR the agent opened. Returns its `agent_pull_requests` id.
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
    now: &str,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(&db::q(
        pool,
        "INSERT INTO agent_pull_requests \
         (action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, created_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?) RETURNING id",
    ))
    .bind(action_name)
    .bind(issue_key)
    .bind(host)
    .bind(repo)
    .bind(pr_number)
    .bind(head_branch)
    .bind(state)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await?;
    Ok(id)
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
        "SELECT id, action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, created_at, updated_at \
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
        "SELECT id, action_name, issue_key, host, repo, pr_number, head_branch, fix_attempts, state, created_at, updated_at \
         FROM agent_pull_requests WHERE issue_key = ? ORDER BY updated_at DESC LIMIT 1",
    ))
    .bind(issue_key)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(agent_pr_from_row).transpose()
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
    async fn an_empty_test_id_set_matches_nothing() {
        let pool = test_pool().await;
        let found = find_match(&pool, "acme/widgets", "checks.yml", "test", &[])
            .await
            .unwrap();
        assert_eq!(found, None);
    }
}
