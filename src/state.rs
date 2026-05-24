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
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT count FROM attempt_counters WHERE action_name = ? AND row_key = ?",
    )
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
        INSERT INTO attempt_counters (action_name, row_key, count, last_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(action_name, row_key) DO UPDATE SET
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
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT run_count FROM daily_ledger WHERE action_name = ? AND date = ?",
    )
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
    let row: Option<(f64,)> = sqlx::query_as(
        "SELECT cost_usd FROM daily_ledger WHERE action_name = ? AND date = ?",
    )
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

pub async fn run_begin(
    pool: &SqlitePool,
    action: &str,
    triggered_by: &str,
) -> Result<i64> {
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

pub async fn run_finish(
    pool: &SqlitePool,
    run_id: i64,
    exit_code: i32,
    rows_processed: u32,
) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        UPDATE runs
        SET ended_at = ?, exit_code = ?, rows_processed = ?
        WHERE id = ?
        "#,
    )
    .bind(&now)
    .bind(exit_code as i64)
    .bind(rows_processed as i64)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug)]
pub struct RowOutcome<'a> {
    pub run_id: i64,
    pub action_name: &'a str,
    pub row_key: &'a str,
    pub attempt: u32,
    pub ok: bool,
    pub claude_ran: bool,
    pub claude_ok: bool,
    pub claude_cost_usd: f64,
    pub attempt_counted: bool,
    pub summary: Option<&'a str>,
    pub stream_log_path: Option<&'a str>,
}

pub async fn row_outcome_insert(pool: &SqlitePool, r: &RowOutcome<'_>) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO row_outcomes (
            run_id, action_name, row_key, attempt, ts, ok,
            claude_ran, claude_ok, claude_cost_usd, attempt_counted,
            summary, stream_log_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(r.run_id)
    .bind(r.action_name)
    .bind(r.row_key)
    .bind(r.attempt as i64)
    .bind(&now)
    .bind(r.ok as i64)
    .bind(r.claude_ran as i64)
    .bind(r.claude_ok as i64)
    .bind(r.claude_cost_usd)
    .bind(r.attempt_counted as i64)
    .bind(r.summary)
    .bind(r.stream_log_path)
    .execute(pool)
    .await?;
    Ok(())
}
