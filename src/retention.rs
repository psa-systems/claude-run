//! CLAUDE-175: age-based retention prune for the unbounded history tables.
//!
//! `job_outcomes` (and, per CLAUDE-172, `run_events`) grow without bound: every
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
//! `migrations/0017_job_outcomes_ts_index.sql`); the `prune_uses_ts_index` test
//! asserts the query plan uses that index.

use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;

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

/// The tables the daemon retention sweep prunes. `job_outcomes` is the driver
/// (CLAUDE-175). CLAUDE-172 appends `run_events` here (plus its own
/// single-column `ts` index) so it shares this exact helper, config, and loop.
pub const RETENTION_TABLES: &[RetentionTable] = &[RetentionTable {
    table: "job_outcomes",
    ts_col: "ts",
}];

/// Tally of one [`sweep`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionStats {
    /// Rows deleted across every pruned table this pass.
    pub deleted: u64,
}

/// Delete every row of `t` whose `ts` is strictly older than `cutoff` (an
/// RFC3339 string). Returns the number of rows deleted. The predicate is a range
/// on the indexed `ts` column, so it seeks rather than scans.
pub async fn prune_older_than(pool: &SqlitePool, t: &RetentionTable, cutoff: &str) -> Result<u64> {
    // `t.table` / `t.ts_col` are `&'static str` constants from RETENTION_TABLES,
    // not user input, so this format! cannot inject. The cutoff is bound.
    let sql = format!("DELETE FROM {} WHERE {} < ?", t.table, t.ts_col);
    let res = sqlx::query(&sql).bind(cutoff).execute(pool).await?;
    Ok(res.rows_affected())
}

/// Prune every table in [`RETENTION_TABLES`] to the retention window, using
/// `now` to compute the cutoff (`now - retention_days`). A disabled config is a
/// no-op. `now` is injected so a test can age rows by passing a future instant.
pub async fn sweep(
    pool: &SqlitePool,
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
    use crate::db::MIGRATOR;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    /// Seed one `runs` row so `job_outcomes.run_id`'s foreign key is satisfied.
    /// Returns its id.
    async fn seed_run(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO runs (action_name, started_at) VALUES ('act', '2020-01-01T00:00:00Z') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Insert a `job_outcomes` row with an explicit `ts`.
    async fn insert_outcome(pool: &SqlitePool, job_key: &str, ts: &str) {
        sqlx::query(
            r#"
            INSERT INTO job_outcomes (
                run_id, action_name, job_key, attempt, ts, ok,
                claude_ran, claude_ok, claude_cost_usd, attempt_counted,
                summary, stream_log_path, duration_ms, timed_out
            ) VALUES (1, 'act', ?, 1, ?, 1, 1, 1, 0.0, 1, NULL, NULL, 0, 0)
            "#,
        )
        .bind(job_key)
        .bind(ts)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn count(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM job_outcomes")
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
        let survivor: String = sqlx::query_scalar("SELECT job_key FROM job_outcomes")
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
        let pool = pool().await;
        let plan: Vec<(i64, i64, i64, String)> =
            sqlx::query_as("EXPLAIN QUERY PLAN DELETE FROM job_outcomes WHERE ts < ?")
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
}
