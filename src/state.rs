//! State operations against the SQLite DB.

use chrono::{Local, NaiveDate, Utc};
use sqlx::SqlitePool;

use crate::error::Result;

pub fn today_local() -> NaiveDate {
    Local::now().date_naive()
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub async fn attempts_get(pool: &SqlitePool, action: &str, key: &str) -> Result<u32> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT count FROM attempt_counters WHERE action_name = ? AND job_key = ?")
            .bind(action)
            .bind(key)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(c,)| c as u32).unwrap_or(0))
}

pub async fn attempts_increment(pool: &SqlitePool, action: &str, key: &str) -> Result<u32> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO attempt_counters (action_name, job_key, count, last_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(action_name, job_key) DO UPDATE SET
            count = count + 1,
            last_at = excluded.last_at
        "#,
    )
    .bind(action)
    .bind(key)
    .bind(&now)
    .execute(pool)
    .await?;
    attempts_get(pool, action, key).await
}

pub async fn daily_count_get(pool: &SqlitePool, action: &str) -> Result<u32> {
    let today = today_local().to_string();
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT run_count FROM daily_ledger WHERE action_name = ? AND date = ?")
            .bind(action)
            .bind(&today)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(c,)| c as u32).unwrap_or(0))
}

pub async fn daily_count_increment(pool: &SqlitePool, action: &str) -> Result<()> {
    let today = today_local().to_string();
    sqlx::query(
        r#"
        INSERT INTO daily_ledger (action_name, date, run_count, cost_usd)
        VALUES (?, ?, 1, 0.0)
        ON CONFLICT(action_name, date) DO UPDATE SET
            run_count = run_count + 1
        "#,
    )
    .bind(action)
    .bind(&today)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn daily_cost_get(pool: &SqlitePool, action: &str) -> Result<f64> {
    let today = today_local().to_string();
    let row: Option<(f64,)> =
        sqlx::query_as("SELECT cost_usd FROM daily_ledger WHERE action_name = ? AND date = ?")
            .bind(action)
            .bind(&today)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(c,)| c).unwrap_or(0.0))
}

pub async fn daily_cost_add(pool: &SqlitePool, action: &str, amount: f64) -> Result<()> {
    let today = today_local().to_string();
    sqlx::query(
        r#"
        INSERT INTO daily_ledger (action_name, date, run_count, cost_usd)
        VALUES (?, ?, 0, ?)
        ON CONFLICT(action_name, date) DO UPDATE SET
            cost_usd = cost_usd + excluded.cost_usd
        "#,
    )
    .bind(action)
    .bind(&today)
    .bind(amount)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn run_begin(pool: &SqlitePool, action: &str, triggered_by: &str) -> Result<i64> {
    let now = now_iso();
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO runs (action_name, started_at, triggered_by)
        VALUES (?, ?, ?)
        RETURNING id
        "#,
    )
    .bind(action)
    .bind(&now)
    .bind(triggered_by)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

/// CLAUDE-1: record the planned job keys for a run as soon as input.command
/// has produced them, so the web UI can show what's queued / in-flight
/// before any outcome record exists. Stored as a JSON array of strings on
/// the `runs.job_keys` column.
pub async fn run_set_job_keys(pool: &SqlitePool, run_id: i64, keys: &[String]) -> Result<()> {
    let payload = serde_json::to_string(keys)?;
    sqlx::query("UPDATE runs SET job_keys = ? WHERE id = ?")
        .bind(payload)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-14: mark which planned job a run is currently processing, so the web
/// UI can show exactly the one in-flight job as "running" instead of inferring
/// it from gaps in `job_outcomes`. Pass `Some(key)` before processing a job;
/// `run_finish` clears it back to NULL when the run ends.
pub async fn run_set_current_job(pool: &SqlitePool, run_id: i64, key: Option<&str>) -> Result<()> {
    sqlx::query("UPDATE runs SET current_job_key = ? WHERE id = ?")
        .bind(key)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-28: mark a job key as in flight for a run. Replaces the single
/// `run_set_current_job` marker so a run with several jobs running in parallel
/// badges all of them. Idempotent (`INSERT OR IGNORE`).
pub async fn run_inflight_add(pool: &SqlitePool, run_id: i64, key: &str) -> Result<()> {
    sqlx::query("INSERT OR IGNORE INTO run_inflight_jobs (run_id, job_key) VALUES (?, ?)")
        .bind(run_id)
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-28: clear one job's in-flight marker when it finishes.
pub async fn run_inflight_remove(pool: &SqlitePool, run_id: i64, key: &str) -> Result<()> {
    sqlx::query("DELETE FROM run_inflight_jobs WHERE run_id = ? AND job_key = ?")
        .bind(run_id)
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-28: the set of job keys currently in flight for a run.
pub async fn run_inflight_keys(pool: &SqlitePool, run_id: i64) -> Result<Vec<String>> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT job_key FROM run_inflight_jobs WHERE run_id = ?")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(k,)| k).collect())
}

pub async fn run_job_keys(pool: &SqlitePool, run_id: i64) -> Result<Vec<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as("SELECT job_keys FROM runs WHERE id = ?")
        .bind(run_id)
        .fetch_optional(pool)
        .await?;
    let json = row.and_then(|(j,)| j);
    if let Some(j) = json {
        Ok(serde_json::from_str(&j).unwrap_or_default())
    } else {
        Ok(vec![])
    }
}

pub async fn run_finish(
    pool: &SqlitePool,
    run_id: i64,
    exit_code: i32,
    jobs_processed: u32,
) -> Result<()> {
    let now = now_iso();
    // CLAUDE-14: clear current_job_key atomically with the end state. A finished
    // run has no job in flight, so every break/finish path that reaches here
    // also drops the stale "running" marker.
    sqlx::query(
        r#"
        UPDATE runs
        SET ended_at = ?, exit_code = ?, jobs_processed = ?, current_job_key = NULL
        WHERE id = ?
        "#,
    )
    .bind(&now)
    .bind(exit_code as i64)
    .bind(jobs_processed as i64)
    .bind(run_id)
    .execute(pool)
    .await?;
    // CLAUDE-28: a finished run has nothing in flight; drop any stale in-flight
    // job markers alongside clearing the legacy single-job marker above.
    sqlx::query("DELETE FROM run_inflight_jobs WHERE run_id = ?")
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-10: reconcile orphaned runs on daemon startup. A freshly started
/// daemon cannot have any run still executing, so every row with no end state
/// is a leftover from a dead process. Mark each cancelled with `ended_at` set
/// to now and `exit_code = exit::CANCELLED`. Returns the number reconciled.
/// `job_outcomes` is left untouched: a job either has an outcome or it does
/// not, and no synthetic outcomes are invented. This is a pure DB reconcile,
/// distinct from the live-cancel path (CLAUDE-9): the previous process is
/// gone, so there is no child to terminate or `post` phase to run.
pub async fn cancel_orphaned_runs(pool: &SqlitePool) -> Result<u64> {
    let now = now_iso();
    let result = sqlx::query("UPDATE runs SET ended_at = ?, exit_code = ? WHERE ended_at IS NULL")
        .bind(&now)
        .bind(crate::exit::CANCELLED as i64)
        .execute(pool)
        .await?;
    // CLAUDE-28: at startup nothing is in flight, so any in-flight job markers
    // are leftovers from the dead process. Clear them all.
    sqlx::query("DELETE FROM run_inflight_jobs")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

#[derive(Debug)]
pub struct JobOutcome<'a> {
    pub run_id: i64,
    pub action_name: &'a str,
    pub job_key: &'a str,
    pub attempt: u32,
    pub ok: bool,
    pub claude_ran: bool,
    pub claude_ok: bool,
    pub claude_cost_usd: f64,
    pub attempt_counted: bool,
    pub summary: Option<&'a str>,
    pub stream_log_path: Option<&'a str>,
    /// CLAUDE-20: full per-job pipeline elapsed in milliseconds.
    pub duration_ms: i64,
}

pub async fn job_outcome_insert(pool: &SqlitePool, r: &JobOutcome<'_>) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO job_outcomes (
            run_id, action_name, job_key, attempt, ts, ok,
            claude_ran, claude_ok, claude_cost_usd, attempt_counted,
            summary, stream_log_path, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(r.run_id)
    .bind(r.action_name)
    .bind(r.job_key)
    .bind(r.attempt as i64)
    .bind(&now)
    .bind(r.ok as i64)
    .bind(r.claude_ran as i64)
    .bind(r.claude_ok as i64)
    .bind(r.claude_cost_usd)
    .bind(r.attempt_counted as i64)
    .bind(r.summary)
    .bind(r.stream_log_path)
    .bind(r.duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}

// ---- Phase 2: pause + action index --------------------------------------

pub async fn global_pause_status(pool: &SqlitePool) -> Result<Option<(String, Option<String>)>> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT paused_at, reason FROM global_pause WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(row)
}

pub async fn global_pause_set(pool: &SqlitePool, reason: Option<&str>) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO global_pause (id, paused_at, reason) VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET paused_at = excluded.paused_at, reason = excluded.reason
        "#,
    )
    .bind(&now)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn global_pause_clear(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM global_pause WHERE id = 1")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn action_pause_status(
    pool: &SqlitePool,
    action: &str,
) -> Result<Option<(String, Option<String>)>> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT paused_at, reason FROM paused_actions WHERE action_name = ?")
            .bind(action)
            .fetch_optional(pool)
            .await?;
    Ok(row)
}

pub async fn action_pause_set(pool: &SqlitePool, action: &str, reason: Option<&str>) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO paused_actions (action_name, paused_at, reason) VALUES (?, ?, ?)
        ON CONFLICT(action_name) DO UPDATE SET paused_at = excluded.paused_at, reason = excluded.reason
        "#,
    )
    .bind(action)
    .bind(&now)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn action_pause_clear(pool: &SqlitePool, action: &str) -> Result<()> {
    sqlx::query("DELETE FROM paused_actions WHERE action_name = ?")
        .bind(action)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct ActionIndexRow {
    pub name: String,
    pub file_path: String,
    pub yaml_sha256: String,
    pub enabled: bool,
    pub schedule_kind: Option<String>,
    pub schedule_spec: Option<String>,
    pub last_seen_at: String,
    pub parse_error: Option<String>,
    /// CLAUDE-1: optional URL template for rendering job keys as links.
    pub link_template: Option<String>,
}

pub async fn action_index_upsert(pool: &SqlitePool, row: &ActionIndexRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO actions (
            name, file_path, yaml_sha256, enabled,
            schedule_kind, schedule_spec, last_seen_at, parse_error, link_template
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            file_path = excluded.file_path,
            yaml_sha256 = excluded.yaml_sha256,
            enabled = excluded.enabled,
            schedule_kind = excluded.schedule_kind,
            schedule_spec = excluded.schedule_spec,
            last_seen_at = excluded.last_seen_at,
            parse_error = excluded.parse_error,
            link_template = excluded.link_template
        "#,
    )
    .bind(&row.name)
    .bind(&row.file_path)
    .bind(&row.yaml_sha256)
    .bind(row.enabled as i64)
    .bind(&row.schedule_kind)
    .bind(&row.schedule_spec)
    .bind(&row.last_seen_at)
    .bind(&row.parse_error)
    .bind(&row.link_template)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn action_index_list(pool: &SqlitePool) -> Result<Vec<ActionIndexRow>> {
    let rows = sqlx::query_as::<_, ActionIndexRow>(
        "SELECT name, file_path, yaml_sha256, enabled, schedule_kind, schedule_spec, last_seen_at, parse_error, link_template FROM actions ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn action_last_run_started(pool: &SqlitePool, action: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT started_at FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 1",
    )
    .bind(action)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(s,)| s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MIGRATOR;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn mem_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    async fn exit_code_of(pool: &SqlitePool, run_id: i64) -> Option<i64> {
        sqlx::query_scalar::<_, Option<i64>>("SELECT exit_code FROM runs WHERE id = ?")
            .bind(run_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn ended_at_of(pool: &SqlitePool, run_id: i64) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT ended_at FROM runs WHERE id = ?")
            .bind(run_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cancel_orphaned_runs_marks_only_unfinished() {
        let pool = mem_pool().await;

        // One finished run, two orphaned (no end state).
        let finished = run_begin(&pool, "act", "cli").await.unwrap();
        run_finish(&pool, finished, crate::exit::OK, 3)
            .await
            .unwrap();
        let orphan_a = run_begin(&pool, "act", "cli").await.unwrap();
        let orphan_b = run_begin(&pool, "act", "schedule").await.unwrap();

        let cancelled = cancel_orphaned_runs(&pool).await.unwrap();
        assert_eq!(cancelled, 2, "both orphaned runs reconciled");

        // Orphans now ended with the CANCELLED sentinel.
        for orphan in [orphan_a, orphan_b] {
            assert_eq!(
                exit_code_of(&pool, orphan).await,
                Some(crate::exit::CANCELLED as i64)
            );
            assert!(ended_at_of(&pool, orphan).await.is_some());
        }

        // The completed run is untouched.
        assert_eq!(
            exit_code_of(&pool, finished).await,
            Some(crate::exit::OK as i64)
        );
    }

    #[tokio::test]
    async fn cancel_orphaned_runs_is_noop_when_none() {
        let pool = mem_pool().await;
        let finished = run_begin(&pool, "act", "cli").await.unwrap();
        run_finish(&pool, finished, crate::exit::OK, 0)
            .await
            .unwrap();

        assert_eq!(cancel_orphaned_runs(&pool).await.unwrap(), 0);
        assert_eq!(
            exit_code_of(&pool, finished).await,
            Some(crate::exit::OK as i64)
        );
    }
}
