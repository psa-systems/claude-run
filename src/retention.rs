//! Age-based retention prune for the unbounded history tables.
//!
//! `job_outcomes` (and `run_events`) grow without bound: every
//! job outcome and every poll-phase diagnostic is appended and nothing ever
//! deletes them. This module is the one shared mechanism that bounds them, so
//! the two prunes are a single consistent subsystem rather than two divergent
//! implementations. The daemon runs [`sweep`] on serve start and on a periodic
//! tick (see `src/daemon.rs`), reading the live [`DbRetentionConfig`] so a
//! Settings save applies on the next sweep, mirroring the workdir GC shape
//! ([`crate::gc`]).
//!
//! Every table here must have a single-column index on its `ts` column so the
//! `ts < cutoff` predicate is an index range seek, not a full table scan (see
//! `migrations/sqlite/0017_job_outcomes_ts_index.sql` and
//! `migrations/sqlite/0018_run_events_ts_index.sql`, and both in
//! `migrations/postgres/0001_init.sql`); the `prune_uses_ts_index` tests
//! assert the query plan uses those indexes.

use crate::db::DbPool;
use chrono::{DateTime, Duration, Utc};

use crate::config::DbRetentionConfig;
use crate::error::Result;

/// One history table pruned by the retention sweep: its name and the RFC3339
/// `ts` column the cutoff compares against. Both are compile-time `&'static str`
/// constants drawn only from [`RETENTION_TABLES`] (never user input), so
/// interpolating them into the DELETE statement carries no injection risk.
#[derive(Debug, Clone, Copy)]
pub struct RetentionTable {
    /// SQL table name.
    pub table: &'static str,
    /// Name of the RFC3339-timestamp column the cutoff is compared against.
    pub ts_col: &'static str,
}

/// The tables the daemon retention sweep prunes. `job_outcomes` is the driver;
/// `run_events` joins the same list, each with its
/// own single-column `ts` index (`idx_job_outcomes_ts` from migration 0017,
/// `idx_run_events_ts` from migration 0018) so both share this exact helper,
/// config, and loop.
/// `ci_failure_runs` joins the same list: a run sighting is a
/// per-occurrence history row, pruned the same way. The other three
/// CI-failure tables (`ci_failures`, `ci_failure_tests`,
/// `agent_pull_requests`) are deliberately absent: a resolved failure or a
/// merged PR must still match a regression months later, so only the
/// per-sighting history is bounded.
/// `claude_invocations` joins the same list for the same reason:
/// one row per `claude` invocation is per-occurrence history, not a lookup
/// table another feature needs to match against indefinitely.
pub const RETENTION_TABLES: &[RetentionTable] = &[
    RetentionTable {
        table: "job_outcomes",
        ts_col: "ts",
    },
    RetentionTable {
        table: "run_events",
        ts_col: "ts",
    },
    RetentionTable {
        table: "ci_failure_runs",
        ts_col: "ts",
    },
    RetentionTable {
        table: "claude_invocations",
        ts_col: "ts",
    },
];

/// Tally of one [`sweep`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionStats {
    /// Rows deleted across every pruned table this pass.
    pub deleted: u64,
}

/// Delete every row of `t` whose `ts` is strictly older than `cutoff` (an
/// RFC3339 string). Returns the number of rows deleted. The predicate is a range
/// on the indexed `ts` column, so it seeks rather than scans.
pub async fn prune_older_than(pool: &DbPool, t: &RetentionTable, cutoff: &str) -> Result<u64> {
    // `t.table` / `t.ts_col` are `&'static str` constants from RETENTION_TABLES,
    // not user input, so this format! cannot inject. The cutoff is bound.
    let sql = format!("DELETE FROM {} WHERE {} < ?", t.table, t.ts_col);
    let res = sqlx::query(&crate::db::q(pool, &sql))
        .bind(cutoff)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

/// Prune every table in [`RETENTION_TABLES`] to the retention window, using
/// `now` to compute the cutoff (`now - retention_days`). A disabled config is a
/// no-op. `now` is injected so a test can age rows by passing a future instant.
pub async fn sweep(
    pool: &DbPool,
    cfg: &DbRetentionConfig,
    now: DateTime<Utc>,
) -> Result<RetentionStats> {
    let mut stats = RetentionStats::default();
    if !cfg.enabled {
        return Ok(stats);
    }
    let cutoff = (now - Duration::days(cfg.retention_days as i64)).to_rfc3339();
    for t in RETENTION_TABLES {
        let deleted = prune_older_than(pool, t, &cutoff).await?;
        if deleted > 0 {
            tracing::info!(
                table = t.table,
                deleted,
                cutoff = %cutoff,
                retention_days = cfg.retention_days,
                "db retention: pruned rows older than the window"
            );
        }
        stats.deleted += deleted;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> DbPool {
        crate::db::test_pool().await
    }

    /// Seed one `runs` row so `job_outcomes.run_id`'s foreign key is satisfied.
    /// Returns its id.
    async fn seed_run(pool: &DbPool) -> i64 {
        sqlx::query_scalar(&crate::db::q(pool,
            "INSERT INTO runs (action_name, started_at) VALUES ('act', '2020-01-01T00:00:00Z') RETURNING id",
        ))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Insert a `job_outcomes` row with an explicit `ts`.
    async fn insert_outcome(pool: &DbPool, job_key: &str, ts: &str) {
        sqlx::query(&crate::db::q(
            pool,
            r#"
            INSERT INTO job_outcomes (
                run_id, action_name, job_key, attempt, ts, ok,
                claude_ran, claude_ok, claude_cost_usd, attempt_counted,
                summary, stream_log_path, duration_ms, timed_out
            ) VALUES (1, 'act', ?, 1, ?, 1, 1, 1, 0.0, 1, NULL, NULL, 0, 0)
            "#,
        ))
        .bind(job_key)
        .bind(ts)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn count(pool: &DbPool) -> i64 {
        sqlx::query_scalar(&crate::db::q(pool, "SELECT COUNT(*) FROM job_outcomes"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Insert a `run_events` row with an explicit `ts`. `run_id` is left NULL
    /// (events are recorded keyed by action_name + ts before any run exists).
    async fn insert_event(pool: &DbPool, message: &str, ts: &str) {
        sqlx::query(&crate::db::q(
            pool,
            "INSERT INTO run_events (action_name, ts, level, message) VALUES ('act', ?, 'warn', ?)",
        ))
        .bind(ts)
        .bind(message)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn count_events(pool: &DbPool) -> i64 {
        sqlx::query_scalar(&crate::db::q(pool, "SELECT COUNT(*) FROM run_events"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn cfg(enabled: bool, days: u64) -> DbRetentionConfig {
        DbRetentionConfig {
            enabled,
            retention_days: days,
        }
    }

    #[tokio::test]
    async fn prunes_rows_past_the_window_keeps_fresh() {
        let pool = pool().await;
        seed_run(&pool).await;
        let now = Utc::now();
        // 200 days old: past the 180-day default window.
        let old = (now - Duration::days(200)).to_rfc3339();
        // 10 days old: inside the window.
        let fresh = (now - Duration::days(10)).to_rfc3339();
        insert_outcome(&pool, "OLD-1", &old).await;
        insert_outcome(&pool, "FRESH-1", &fresh).await;
        assert_eq!(count(&pool).await, 2);

        let stats = sweep(&pool, &cfg(true, 180), now).await.unwrap();
        assert_eq!(stats.deleted, 1, "only the aged row is pruned");
        assert_eq!(count(&pool).await, 1);
        let survivor: String =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT job_key FROM job_outcomes"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(survivor, "FRESH-1", "the in-window row is kept");
    }

    #[tokio::test]
    async fn disabled_sweep_deletes_nothing() {
        let pool = pool().await;
        seed_run(&pool).await;
        let now = Utc::now();
        let old = (now - Duration::days(400)).to_rfc3339();
        insert_outcome(&pool, "OLD-1", &old).await;
        let stats = sweep(&pool, &cfg(false, 180), now).await.unwrap();
        assert_eq!(stats, RetentionStats::default());
        assert_eq!(count(&pool).await, 1, "a disabled sweep keeps every row");
    }

    /// AC: the prune predicate must use the `ts` index, never table-scan an
    /// unbounded `job_outcomes`. Assert the query plan seeks the single-column
    /// index added in migration 0017.
    #[tokio::test]
    async fn prune_uses_ts_index() {
        // SQLite's own plan output, so this one pins the SQLite backend rather
        // than following CLAUDE_RUN_TEST_DATABASE_URL. Postgres has the same
        // index (migrations/postgres/0001_init.sql) but phrases plans its own
        // way, and an empty table there would be seq-scanned whatever we index.
        let pool = crate::db::test_sqlite_pool().await;
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(&crate::db::q(
            &pool,
            "EXPLAIN QUERY PLAN DELETE FROM job_outcomes WHERE ts < ?",
        ))
        .bind(Utc::now().to_rfc3339())
        .fetch_all(&pool)
        .await
        .unwrap();
        let detail = plan
            .iter()
            .map(|(_, _, _, d)| d.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        // SQLite phrases the plan as "SEARCH ... USING INDEX" or "USING COVERING
        // INDEX"; either is an index seek. A missing index would read
        // "SCAN job_outcomes".
        assert!(
            detail.contains("idx_job_outcomes_ts") && detail.contains("SEARCH"),
            "prune must seek idx_job_outcomes_ts, got plan: {detail}"
        );
        assert!(
            !detail.contains("SCAN job_outcomes"),
            "prune must not full-scan job_outcomes, got plan: {detail}"
        );
    }

    /// The shared sweep prunes aged `run_events` rows and keeps the
    /// in-window ones, in the same pass as `job_outcomes`.
    #[tokio::test]
    async fn prunes_run_events_past_the_window_keeps_fresh() {
        let pool = pool().await;
        let now = Utc::now();
        let old = (now - Duration::days(200)).to_rfc3339();
        let fresh = (now - Duration::days(10)).to_rfc3339();
        insert_event(&pool, "OLD", &old).await;
        insert_event(&pool, "FRESH", &fresh).await;
        assert_eq!(count_events(&pool).await, 2);

        let stats = sweep(&pool, &cfg(true, 180), now).await.unwrap();
        assert_eq!(stats.deleted, 1, "only the aged run_events row is pruned");
        assert_eq!(count_events(&pool).await, 1);
        let survivor: String =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT message FROM run_events"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(survivor, "FRESH", "the in-window run_events row is kept");
    }

    /// A disabled sweep must not prune `run_events` either.
    #[tokio::test]
    async fn disabled_sweep_keeps_run_events() {
        let pool = pool().await;
        let now = Utc::now();
        insert_event(&pool, "OLD", &(now - Duration::days(400)).to_rfc3339()).await;
        let stats = sweep(&pool, &cfg(false, 180), now).await.unwrap();
        assert_eq!(stats, RetentionStats::default());
        assert_eq!(count_events(&pool).await, 1);
    }

    /// AC: the `run_events` prune predicate must use the single-column `ts`
    /// index from migration 0018, never table-scan an unbounded `run_events`
    /// (idx_run_events_action_ts leads with action_name and cannot serve the
    /// bare cutoff predicate).
    #[tokio::test]
    async fn prune_uses_ts_index_run_events() {
        // SQLite's own plan output, so this one pins the SQLite backend rather
        // than following CLAUDE_RUN_TEST_DATABASE_URL. Postgres has the same
        // index (migrations/postgres/0001_init.sql) but phrases plans its own
        // way, and an empty table there would be seq-scanned whatever we index.
        let pool = crate::db::test_sqlite_pool().await;
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(&crate::db::q(
            &pool,
            "EXPLAIN QUERY PLAN DELETE FROM run_events WHERE ts < ?",
        ))
        .bind(Utc::now().to_rfc3339())
        .fetch_all(&pool)
        .await
        .unwrap();
        let detail = plan
            .iter()
            .map(|(_, _, _, d)| d.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        assert!(
            detail.contains("idx_run_events_ts") && detail.contains("SEARCH"),
            "prune must seek idx_run_events_ts, got plan: {detail}"
        );
        assert!(
            !detail.contains("SCAN run_events"),
            "prune must not full-scan run_events, got plan: {detail}"
        );
    }

    /// Guard that `run_events` is actually wired into the shared
    /// retention list, so appending it cannot silently regress.
    #[test]
    fn run_events_is_in_retention_tables() {
        assert!(
            RETENTION_TABLES.iter().any(|t| t.table == "run_events"),
            "run_events must be pruned by the shared retention sweep"
        );
    }

    /// AC: the default retention window must not silently truncate the bounded
    /// reporting ranges the web UI exposes (30d / 90d).
    #[tokio::test]
    async fn default_window_covers_longest_report_range() {
        let default = DbRetentionConfig::default();
        assert!(
            default.retention_days >= 90,
            "default retention ({}) must cover the 90-day report range",
            default.retention_days
        );
    }

    /// `ci_failure_runs` rows older than the window are pruned,
    /// like `job_outcomes` and `run_events`; the other three CI-failure tables
    /// (`ci_failures`, `ci_failure_tests`, `agent_pull_requests`) are never
    /// swept, because a resolved failure or a merged PR must still be
    /// matchable against a regression months later.
    #[tokio::test]
    async fn prunes_ci_failure_runs_past_the_window_keeps_the_other_tables() {
        let pool = pool().await;
        let now = Utc::now();
        let old = (now - Duration::days(200)).to_rfc3339();
        let fresh = (now - Duration::days(10)).to_rfc3339();

        let failure_id: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "INSERT INTO ci_failures (repo, workflow_id, job_name, state, created_at, updated_at) \
             VALUES ('acme/widgets', 'checks.yml', 'test', 'open', ?, ?) RETURNING id",
        ))
        .bind(&old)
        .bind(&old)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "INSERT INTO ci_failure_tests (failure_id, test_id) VALUES (?, ?)",
        ))
        .bind(failure_id)
        .bind("lib::a")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "INSERT INTO ci_failure_runs (failure_id, run_index, commit_sha, branch, ts) \
             VALUES (?, 1, 'abc', 'main', ?)",
        ))
        .bind(failure_id)
        .bind(&old)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "INSERT INTO ci_failure_runs (failure_id, run_index, commit_sha, branch, ts) \
             VALUES (?, 2, 'def', 'main', ?)",
        ))
        .bind(failure_id)
        .bind(&fresh)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "INSERT INTO agent_pull_requests \
             (action_name, issue_key, host, repo, pr_number, head_branch, state, created_at, updated_at) \
             VALUES ('youtrack', 'CLAUDE-1', 'dev.a8n.run', 'acme/widgets', 1, 'fix/x', 'open', ?, ?)",
        ))
        .bind(&old)
        .bind(&old)
        .execute(&pool)
        .await
        .unwrap();

        let stats = sweep(&pool, &cfg(true, 180), now).await.unwrap();
        assert_eq!(
            stats.deleted, 1,
            "only the aged ci_failure_runs row is pruned"
        );

        let runs: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM ci_failure_runs WHERE failure_id = ?",
        ))
        .bind(failure_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(runs, 1, "the in-window run survives");

        let failures: i64 =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT COUNT(*) FROM ci_failures"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(failures, 1, "ci_failures is never pruned by age");

        let tests: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM ci_failure_tests",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(tests, 1, "ci_failure_tests is never pruned by age");

        let prs: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM agent_pull_requests",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(prs, 1, "agent_pull_requests is never pruned by age");
    }
}
