//! CLAUDE-117: shared reporting aggregations over historical state.
//!
//! Both the JSON API (`/api/report/*`, [`crate::api`]) and the `/reports` HTML
//! page ([`crate::web`]) render these series, so the SQL lives here once rather
//! than inlined in two handlers.
//!
//! Sources (see the issue background):
//! - `daily_ledger(action_name, date, run_count, cost_usd)` is the canonical,
//!   already-**local**-dated rollup. It backs the daily and weekly spend / run
//!   counts and the summary totals, but has no sub-day resolution.
//! - `job_outcomes(ts, claude_cost_usd, ...)` carries UTC RFC3339 timestamps and
//!   per-job cost; it backs the time-of-day and day-of-week **spend** buckets.
//! - `runs(started_at, ended_at, ...)` carries UTC RFC3339 wall-clock; it backs
//!   run counts + run minutes for the time-resolved panels.
//!
//! All weekday / hour bucketing converts the stored UTC timestamp to the
//! daemon's **local** timezone *before* extracting the weekday / hour, so "time
//! of day" and "all Sundays" read correctly for a non-UTC operator. The pure
//! conversion is [`weekday_hour_in`]; production passes [`chrono::Local`].

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Timelike, Utc};
use serde::Serialize;
use sqlx::SqlitePool;

use crate::error::Result;

/// Selectable reporting window. Bounds every query by date / `ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    Last30Days,
    Last90Days,
    All,
}

impl Range {
    /// Parse the `range` query param. Anything but `30d` / `90d` (including an
    /// absent value) defaults to all-time, per the issue decision.
    pub fn from_param(s: &str) -> Range {
        match s {
            "30d" => Range::Last30Days,
            "90d" => Range::Last90Days,
            _ => Range::All,
        }
    }

    /// The canonical query-param spelling, for round-tripping into links / forms.
    pub fn as_param(self) -> &'static str {
        match self {
            Range::Last30Days => "30d",
            Range::Last90Days => "90d",
            Range::All => "all",
        }
    }

    /// Window length in days, or `None` for all-time (no lower bound).
    fn days(self) -> Option<i64> {
        match self {
            Range::Last30Days => Some(30),
            Range::Last90Days => Some(90),
            Range::All => None,
        }
    }
}

/// Lower bound for `daily_ledger.date` (a local `YYYY-MM-DD`), or `None` for
/// all-time. A 30-day window includes today and the preceding 29 local days.
fn date_cutoff(range: Range) -> Option<String> {
    range
        .days()
        .map(|d| (Local::now().date_naive() - Duration::days(d - 1)).to_string())
}

/// Lower bound for the UTC RFC3339 `ts` / `started_at` columns, or `None` for
/// all-time. Computed as `now_utc - days`.
fn ts_cutoff(range: Range) -> Option<String> {
    range
        .days()
        .map(|d| (Utc::now() - Duration::days(d)).to_rfc3339())
}

/// Convert a stored UTC RFC3339 timestamp into a `(weekday, hour)` pair in the
/// supplied timezone, where `weekday` is `0 = Sunday .. 6 = Saturday` and `hour`
/// is `0..=23`. Returns `None` if the timestamp fails to parse. Generic over the
/// timezone so tests can force a known offset; production passes
/// [`chrono::Local`].
pub fn weekday_hour_in<Tz: TimeZone>(ts: &str, tz: &Tz) -> Option<(u32, u32)> {
    let dt = DateTime::parse_from_rfc3339(ts).ok()?;
    let local = dt.with_timezone(tz);
    Some((local.weekday().num_days_from_sunday(), local.hour()))
}

/// Wall-clock minutes between two UTC RFC3339 timestamps, clamped at zero, or
/// `None` if either fails to parse. Used for run minutes (`ended - started`).
fn span_minutes(started_at: &str, ended_at: &str) -> Option<f64> {
    let s = DateTime::parse_from_rfc3339(started_at).ok()?;
    let e = DateTime::parse_from_rfc3339(ended_at).ok()?;
    let ms = (e.timestamp_millis() - s.timestamp_millis()).max(0);
    Some(ms as f64 / 60_000.0)
}

/// Short weekday label for bucket index `0 = Sun .. 6 = Sat`.
fn weekday_label(idx: u32) -> &'static str {
    ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        .get(idx as usize)
        .copied()
        .unwrap_or("?")
}

// ---- output series --------------------------------------------------------

/// Top-line totals over the window.
#[derive(Debug, Default, Serialize)]
pub struct Summary {
    pub range: String,
    pub action: Option<String>,
    /// Total spend from `daily_ledger` (consistent with the daily / weekly
    /// series).
    pub total_cost_usd: f64,
    /// Total run count from `daily_ledger`.
    pub total_runs: i64,
    /// Total wall-clock run minutes from `runs` (NULL `ended_at` excluded).
    pub total_run_minutes: f64,
}

#[derive(Debug, Serialize)]
pub struct DailyPoint {
    pub date: String,
    pub cost_usd: f64,
    pub run_count: i64,
}

#[derive(Debug, Serialize)]
pub struct WeeklyPoint {
    /// ISO week, formatted `YYYY-Www` (e.g. `2026-W24`).
    pub week: String,
    pub cost_usd: f64,
    pub run_count: i64,
}

#[derive(Debug, Serialize)]
pub struct WeekdayBucket {
    /// `0 = Sunday .. 6 = Saturday`.
    pub weekday: u32,
    pub label: String,
    pub cost_usd: f64,
    pub run_count: i64,
    pub run_minutes: f64,
}

#[derive(Debug, Serialize)]
pub struct HourBucket {
    /// `0..=23`, local hour.
    pub hour: u32,
    pub cost_usd: f64,
    /// Job outcomes recorded in this hour (spend activity).
    pub job_count: i64,
    /// Runs started in this hour (run activity).
    pub run_starts: i64,
}

// ---- aggregations ---------------------------------------------------------

/// Daily spend + run-count series from `daily_ledger`, oldest to newest.
pub async fn daily(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<DailyPoint>> {
    let cutoff = date_cutoff(range);
    let mut sql = String::from(
        "SELECT date, IFNULL(SUM(cost_usd), 0.0), IFNULL(SUM(run_count), 0) FROM daily_ledger WHERE 1 = 1",
    );
    if cutoff.is_some() {
        sql.push_str(" AND date >= ?");
    }
    if action.is_some() {
        sql.push_str(" AND action_name = ?");
    }
    sql.push_str(" GROUP BY date ORDER BY date");

    let mut q = sqlx::query_as::<_, (String, f64, i64)>(&sql);
    if let Some(c) = &cutoff {
        q = q.bind(c);
    }
    if let Some(a) = action {
        q = q.bind(a);
    }
    let rows = q.fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|(date, cost_usd, run_count)| DailyPoint {
            date,
            cost_usd,
            run_count,
        })
        .collect())
}

/// Weekly (ISO-week) rollup of the daily series. Aggregated in Rust off
/// [`daily`] rather than via SQLite date functions, which lack ISO-week support.
pub async fn weekly(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<WeeklyPoint>> {
    let daily = daily(pool, range, action).await?;
    let mut weeks: BTreeMap<String, (f64, i64)> = BTreeMap::new();
    for d in daily {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(&d.date, "%Y-%m-%d") {
            let iso = date.iso_week();
            let key = format!("{}-W{:02}", iso.year(), iso.week());
            let e = weeks.entry(key).or_default();
            e.0 += d.cost_usd;
            e.1 += d.run_count;
        }
    }
    Ok(weeks
        .into_iter()
        .map(|(week, (cost_usd, run_count))| WeeklyPoint {
            week,
            cost_usd,
            run_count,
        })
        .collect())
}

/// Spend (from `job_outcomes.ts`) and `(ts, claude_cost_usd)` rows over the
/// window, optionally filtered to one action.
async fn outcome_cost_rows(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<(String, f64)>> {
    let cutoff = ts_cutoff(range);
    let mut sql = String::from("SELECT ts, claude_cost_usd FROM job_outcomes WHERE 1 = 1");
    if cutoff.is_some() {
        sql.push_str(" AND ts >= ?");
    }
    if action.is_some() {
        sql.push_str(" AND action_name = ?");
    }
    let mut q = sqlx::query_as::<_, (String, f64)>(&sql);
    if let Some(c) = &cutoff {
        q = q.bind(c);
    }
    if let Some(a) = action {
        q = q.bind(a);
    }
    Ok(q.fetch_all(pool).await?)
}

/// `(started_at, ended_at)` rows over the window, optionally filtered to one
/// action. `ended_at` is `None` for in-flight / orphaned runs.
async fn run_span_rows(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<(String, Option<String>)>> {
    let cutoff = ts_cutoff(range);
    let mut sql = String::from("SELECT started_at, ended_at FROM runs WHERE 1 = 1");
    if cutoff.is_some() {
        sql.push_str(" AND started_at >= ?");
    }
    if action.is_some() {
        sql.push_str(" AND action_name = ?");
    }
    let mut q = sqlx::query_as::<_, (String, Option<String>)>(&sql);
    if let Some(c) = &cutoff {
        q = q.bind(c);
    }
    if let Some(a) = action {
        q = q.bind(a);
    }
    Ok(q.fetch_all(pool).await?)
}

/// Day-of-week aggregation (the sum of all Sundays, all Mondays, ...): spend
/// from `job_outcomes`, run count + run minutes from `runs`, all bucketed by
/// local weekday. Always returns 7 buckets, Sunday first, even when empty.
pub async fn by_weekday(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<WeekdayBucket>> {
    let mut cost = [0.0_f64; 7];
    let mut runs = [0_i64; 7];
    let mut minutes = [0.0_f64; 7];

    for (ts, c) in outcome_cost_rows(pool, range, action).await? {
        if let Some((wd, _)) = weekday_hour_in(&ts, &Local) {
            cost[wd as usize] += c;
        }
    }
    for (started, ended) in run_span_rows(pool, range, action).await? {
        if let Some((wd, _)) = weekday_hour_in(&started, &Local) {
            runs[wd as usize] += 1;
            if let Some(end) = ended {
                if let Some(m) = span_minutes(&started, &end) {
                    minutes[wd as usize] += m;
                }
            }
        }
    }

    Ok((0..7)
        .map(|wd| WeekdayBucket {
            weekday: wd,
            label: weekday_label(wd).to_string(),
            cost_usd: cost[wd as usize],
            run_count: runs[wd as usize],
            run_minutes: minutes[wd as usize],
        })
        .collect())
}

/// Time-of-day aggregation: spend + job activity from `job_outcomes` and run
/// starts from `runs`, bucketed by local hour. Always returns 24 buckets,
/// `0..=23`, even when empty.
pub async fn by_hour(
    pool: &SqlitePool,
    range: Range,
    action: Option<&str>,
) -> Result<Vec<HourBucket>> {
    let mut cost = [0.0_f64; 24];
    let mut jobs = [0_i64; 24];
    let mut starts = [0_i64; 24];

    for (ts, c) in outcome_cost_rows(pool, range, action).await? {
        if let Some((_, hour)) = weekday_hour_in(&ts, &Local) {
            cost[hour as usize] += c;
            jobs[hour as usize] += 1;
        }
    }
    for (started, _) in run_span_rows(pool, range, action).await? {
        if let Some((_, hour)) = weekday_hour_in(&started, &Local) {
            starts[hour as usize] += 1;
        }
    }

    Ok((0..24)
        .map(|hour| HourBucket {
            hour,
            cost_usd: cost[hour as usize],
            job_count: jobs[hour as usize],
            run_starts: starts[hour as usize],
        })
        .collect())
}

/// Top-line totals: spend + run count from `daily_ledger`, run minutes from
/// `runs` (the only source with both endpoints; NULL `ended_at` excluded).
pub async fn summary(pool: &SqlitePool, range: Range, action: Option<&str>) -> Result<Summary> {
    let cutoff_date = date_cutoff(range);
    let mut sql = String::from(
        "SELECT IFNULL(SUM(cost_usd), 0.0), IFNULL(SUM(run_count), 0) FROM daily_ledger WHERE 1 = 1",
    );
    if cutoff_date.is_some() {
        sql.push_str(" AND date >= ?");
    }
    if action.is_some() {
        sql.push_str(" AND action_name = ?");
    }
    let mut q = sqlx::query_as::<_, (f64, i64)>(&sql);
    if let Some(c) = &cutoff_date {
        q = q.bind(c);
    }
    if let Some(a) = action {
        q = q.bind(a);
    }
    let (total_cost_usd, total_runs) = q.fetch_one(pool).await?;

    let total_run_minutes = run_span_rows(pool, range, action)
        .await?
        .into_iter()
        .filter_map(|(started, ended)| ended.and_then(|e| span_minutes(&started, &e)))
        .sum();

    Ok(Summary {
        range: range.as_param().to_string(),
        action: action.map(str::to_string),
        total_cost_usd,
        total_runs,
        total_run_minutes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MIGRATOR;
    use crate::state::{self, JobOutcome};
    use chrono::FixedOffset;
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

    /// CLAUDE-117: the core AC. A timestamp at 02:30 UTC on a Sunday lands on
    /// Saturday 21:30 at UTC-5 and Sunday 11:30 at UTC+9, so the weekday/hour it
    /// buckets into depends on the timezone the conversion uses, proving the
    /// UTC->local conversion happens before extraction.
    #[test]
    fn weekday_hour_converts_to_local_before_extracting() {
        // 2026-06-14 is a Sunday. 02:30 UTC.
        let ts = "2026-06-14T02:30:00Z";

        // UTC: Sunday (0), hour 2.
        let utc = FixedOffset::east_opt(0).unwrap();
        assert_eq!(weekday_hour_in(ts, &utc), Some((0, 2)));

        // UTC-5: rolls back to Saturday (6), hour 21.
        let west5 = FixedOffset::west_opt(5 * 3600).unwrap();
        assert_eq!(weekday_hour_in(ts, &west5), Some((6, 21)));

        // UTC+9: stays Sunday (0) but hour 11.
        let east9 = FixedOffset::east_opt(9 * 3600).unwrap();
        assert_eq!(weekday_hour_in(ts, &east9), Some((0, 11)));

        // Unparseable input yields None, never a panic.
        assert_eq!(weekday_hour_in("not-a-timestamp", &utc), None);
    }

    #[test]
    fn range_parses_and_round_trips() {
        assert_eq!(Range::from_param("30d"), Range::Last30Days);
        assert_eq!(Range::from_param("90d"), Range::Last90Days);
        assert_eq!(Range::from_param("all"), Range::All);
        // Unknown / absent defaults to all-time.
        assert_eq!(Range::from_param(""), Range::All);
        assert_eq!(Range::from_param("bogus"), Range::All);
        assert_eq!(Range::All.as_param(), "all");
        assert_eq!(Range::Last30Days.as_param(), "30d");
    }

    /// Empty data renders every series without panicking, and the fixed-size
    /// buckets are still fully populated (7 weekdays, 24 hours, all zero).
    #[tokio::test]
    async fn empty_window_yields_zeroed_fixed_buckets() {
        let pool = mem_pool().await;
        let s = summary(&pool, Range::All, None).await.unwrap();
        assert_eq!(s.total_cost_usd, 0.0);
        assert_eq!(s.total_runs, 0);
        assert_eq!(s.total_run_minutes, 0.0);
        assert!(daily(&pool, Range::All, None).await.unwrap().is_empty());
        assert!(weekly(&pool, Range::All, None).await.unwrap().is_empty());
        assert_eq!(by_weekday(&pool, Range::All, None).await.unwrap().len(), 7);
        assert_eq!(by_hour(&pool, Range::All, None).await.unwrap().len(), 24);
    }

    /// End-to-end over seeded rows: spend + counts roll up daily/weekly, run
    /// minutes ignore a NULL `ended_at`, and the action filter narrows totals.
    #[tokio::test]
    async fn aggregates_over_seeded_rows() {
        let pool = mem_pool().await;

        // Two actions, two days of ledger spend / runs.
        state::daily_cost_add(&pool, "alpha", 1.5).await.unwrap();
        state::daily_count_increment(&pool, "alpha").await.unwrap();
        state::daily_cost_add(&pool, "beta", 0.5).await.unwrap();
        state::daily_count_increment(&pool, "beta").await.unwrap();

        // A finished run (10 min) and an in-flight run (NULL ended_at) for alpha.
        sqlx::query("INSERT INTO runs (action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by) VALUES (?, ?, ?, 0, 1, 'cli')")
            .bind("alpha")
            .bind("2026-06-14T10:00:00Z")
            .bind("2026-06-14T10:10:00Z")
            .execute(&pool)
            .await
            .unwrap();
        let inflight = state::run_begin(&pool, "alpha", "cli").await.unwrap();
        let _ = inflight;

        // Global summary sees both actions' spend / runs and the 10 finished min.
        let g = summary(&pool, Range::All, None).await.unwrap();
        assert!((g.total_cost_usd - 2.0).abs() < 1e-9);
        assert_eq!(g.total_runs, 2);
        assert!((g.total_run_minutes - 10.0).abs() < 1e-9, "{g:?}");

        // Action filter narrows spend / runs to alpha only.
        let a = summary(&pool, Range::All, Some("alpha")).await.unwrap();
        assert!((a.total_cost_usd - 1.5).abs() < 1e-9);
        assert_eq!(a.total_runs, 1);

        // Daily rolls into one calendar day; weekly into one ISO week.
        let d = daily(&pool, Range::All, None).await.unwrap();
        assert_eq!(d.len(), 1);
        assert!((d[0].cost_usd - 2.0).abs() < 1e-9);
        let w = weekly(&pool, Range::All, None).await.unwrap();
        assert_eq!(w.len(), 1);
        assert!((w[0].cost_usd - 2.0).abs() < 1e-9);

        // A job outcome contributes spend to its hour/weekday bucket.
        state::job_outcome_insert(
            &pool,
            &JobOutcome {
                run_id: 1,
                action_name: "alpha",
                job_key: "K-1",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.25,
                attempt_counted: true,
                summary: None,
                stream_log_path: None,
                duration_ms: 1000,
                timed_out: false,
            },
        )
        .await
        .unwrap();
        let hours = by_hour(&pool, Range::All, None).await.unwrap();
        assert_eq!(hours.len(), 24);
        let hour_cost: f64 = hours.iter().map(|h| h.cost_usd).sum();
        assert!((hour_cost - 0.25).abs() < 1e-9);
        let job_count: i64 = hours.iter().map(|h| h.job_count).sum();
        assert_eq!(job_count, 1);
        // Run starts: two runs began, bucketed somewhere across the 24 hours.
        let run_starts: i64 = hours.iter().map(|h| h.run_starts).sum();
        assert_eq!(run_starts, 2);

        let weekdays = by_weekday(&pool, Range::All, None).await.unwrap();
        assert_eq!(weekdays.len(), 7);
        let wd_cost: f64 = weekdays.iter().map(|d| d.cost_usd).sum();
        assert!((wd_cost - 0.25).abs() < 1e-9);
        // The finished run's 10 minutes land in exactly one weekday bucket.
        let wd_minutes: f64 = weekdays.iter().map(|d| d.run_minutes).sum();
        assert!((wd_minutes - 10.0).abs() < 1e-9);
    }
}
