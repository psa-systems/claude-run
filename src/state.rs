//! State operations against the SQLite DB.

use crate::db::DbPool;
use chrono::{Local, NaiveDate, Utc};

use tracing::{error, warn};

use crate::error::Result;

pub fn today_local() -> NaiveDate {
    Local::now().date_naive()
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Current time as epoch milliseconds. Matches the unit `yt issue inspect --json`
/// reports linked-issue `resolved` timestamps in, so the dependency gate's
/// `blocked_until` backoff (CLAUDE-161) is directly comparable.
pub fn now_epoch_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Persisted dependency-gate backoff state for one (action, issue) (CLAUDE-161).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyGateState {
    /// Epoch ms before which the gate does not re-evaluate the issue.
    pub blocked_until_ms: i64,
    /// CLAUDE-225: epoch ms at which this block STARTED (the row's insert), kept
    /// across every re-arm so the age of a block is readable. The row is deleted
    /// once the blockers clear, so it measures one continuous block.
    pub blocked_since_ms: i64,
    /// Canonical signature of the unmet-blocker set at the last evaluation;
    /// drives comment idempotency (re-comment only when it changes).
    pub signature: String,
}

/// Read the dependency-gate backoff row for `(action, key)`, if any.
pub async fn dependency_gate_get(
    pool: &DbPool,
    action: &str,
    key: &str,
) -> Result<Option<DependencyGateState>> {
    let row: Option<(i64, i64, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT blocked_until, blocked_since, signature FROM dependency_gate \
         WHERE action_name = ? AND job_key = ?",
    ))
    .bind(action)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(blocked_until_ms, blocked_since_ms, signature)| DependencyGateState {
            blocked_until_ms,
            blocked_since_ms,
            signature,
        },
    ))
}

/// Record (or refresh) the dependency-gate backoff for `(action, key)`.
///
/// CLAUDE-225: `blocked_since` is stamped with now on insert and deliberately
/// NOT touched by the conflict branch, so re-arming the backoff (or commenting a
/// changed blocker set) never resets the age of an ongoing block.
pub async fn dependency_gate_upsert(
    pool: &DbPool,
    action: &str,
    key: &str,
    blocked_until_ms: i64,
    signature: &str,
) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO dependency_gate
            (action_name, job_key, blocked_until, blocked_since, signature, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(action_name, job_key) DO UPDATE SET
            blocked_until = excluded.blocked_until,
            signature = excluded.signature,
            updated_at = excluded.updated_at
        "#,
    ))
    .bind(action)
    .bind(key)
    .bind(blocked_until_ms)
    .bind(now_epoch_ms())
    .bind(signature)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-225: keep only the gate rows whose issue is still a candidate for
/// `action`, dropping the rest. `keys` is the key set the action's query matched
/// this poll (filters aside), so a row outside it holds nothing: the issue left
/// the candidate set and its row is dead state that would otherwise sit in the
/// stale-gate banner forever. An empty `keys` means the query matched nothing,
/// which clears the action's rows; only a SUCCESSFUL query calls this, so a `yt`
/// failure never prunes.
pub async fn dependency_gate_retain(pool: &DbPool, action: &str, keys: &[String]) -> Result<()> {
    let placeholders = std::iter::repeat_n("?", keys.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = if keys.is_empty() {
        "DELETE FROM dependency_gate WHERE action_name = ?".to_string()
    } else {
        format!(
            "DELETE FROM dependency_gate WHERE action_name = ? AND job_key NOT IN ({placeholders})"
        )
    };
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut q = sqlx::query(&sql).bind(action);
    for k in keys {
        q = q.bind(k);
    }
    q.execute(pool).await?;
    Ok(())
}

/// CLAUDE-225: how long a dependency gate may hold an issue before the daemon
/// says so out loud. The gate promises the issue "will be re-evaluated
/// automatically", and that promise failing is silent: the issue simply stays
/// Queued forever with nothing on the dashboard, in the run history, or in the
/// journal. One hour is the assumed threshold (an order of magnitude above the
/// 5m default `recheck_timeout`, so a normally-cycling gate never trips it);
/// revise once the normal poll cadence is confirmed.
pub const STALE_GATE_AFTER_MS: i64 = 60 * 60 * 1000;

/// CLAUDE-225: one dependency gate that has held its issue longer than the
/// staleness threshold, as the dashboard banner and `/api/status` report it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
pub struct StaleGate {
    pub action_name: String,
    /// The held issue (its `idReadable`).
    pub job_key: String,
    /// Canonical unmet-blocker set at the last evaluation.
    pub signature: String,
    /// How long the issue has been continuously blocked.
    pub blocked_for_ms: i64,
    /// How long ago the backoff window expired, i.e. how overdue the promised
    /// re-evaluation is. Negative while the window is still open.
    pub recheck_overdue_ms: i64,
    /// RFC 3339 timestamp of the last evaluation.
    pub updated_at: String,
}

/// CLAUDE-225: gates blocked for at least `threshold_ms`, longest-held first.
/// Reads the live gate table rather than an event trail, so it needs no
/// acknowledgement and clears itself: the gate deletes the row the moment the
/// blockers resolve.
pub async fn dependency_gates_stale(
    pool: &DbPool,
    now_ms: i64,
    threshold_ms: i64,
    limit: i64,
) -> Result<Vec<StaleGate>> {
    let rows = sqlx::query_as::<_, StaleGate>(&crate::db::q(
        pool,
        r#"
        SELECT action_name,
               job_key,
               signature,
               ?1 - blocked_since AS blocked_for_ms,
               ?1 - blocked_until AS recheck_overdue_ms,
               updated_at
        FROM dependency_gate
        WHERE ?1 - blocked_since >= ?2
        ORDER BY blocked_for_ms DESC
        LIMIT ?3
        "#,
    ))
    .bind(now_ms)
    .bind(threshold_ms)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Drop the dependency-gate row for `(action, key)` (its dependencies cleared).
pub async fn dependency_gate_clear(pool: &DbPool, action: &str, key: &str) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM dependency_gate WHERE action_name = ? AND job_key = ?",
    ))
    .bind(action)
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-232: how many rows the parked-at-cap read returns, matching the
/// stale-gate banner's window.
pub const ATTEMPT_CAP_HITS_LIMIT: usize = 20;

/// CLAUDE-232: one job parked at its action's per-key attempt cap, as the
/// dashboard banner and `/api/status` report it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CappedKey {
    pub action_name: String,
    /// The parked job (a YouTrack `idReadable`, a head SHA, whatever the
    /// action's `dedup_key` resolves to).
    pub job_key: String,
    /// Attempts recorded against the key.
    pub count: i64,
    /// The action's effective `max_attempts_per_key` it has reached.
    pub cap: i64,
    /// RFC 3339 timestamp of the last counted attempt. It does NOT move once the
    /// cap is hit: a skipped job counts no attempt.
    pub last_at: String,
}

/// CLAUDE-232: keys whose attempt counter has reached their action's cap, most
/// attempts first. Every one of them is skipped on every poll with no
/// `job_outcomes` row and no `run_events` row, so this live read off
/// `attempt_counters` is the only thing that can report them.
///
/// The cap is per action (`job::EffectiveLimits::max_attempts_per_key`), so
/// `caps` carries the resolved cap per action name and `default_cap` covers the
/// actions it does not list (the synthetic webhook actions, which mint jobs
/// against `RunnerDefaults`). SQL narrows to rows at or over the SMALLEST cap in
/// play and the per-action comparison happens here, so one query serves a mixed
/// set of caps.
///
/// Unlike a dependency gate, nothing deletes the row when the operator resolves
/// the parked issue: the counter is the durable record of the attempts. A key
/// stays listed until its action's control state is cleared.
pub async fn attempt_cap_hits(
    pool: &DbPool,
    caps: &std::collections::BTreeMap<String, u32>,
    default_cap: u32,
    limit: usize,
) -> Result<Vec<CappedKey>> {
    // A cap of 0 would match every row including count 0; the counters start at
    // 1, so clamp the SQL floor to 1 and let the per-action compare below decide.
    let min_cap = caps.values().copied().fold(default_cap, u32::min).max(1);
    let rows: Vec<(String, String, i64, String)> = sqlx::query_as(&crate::db::q(
        pool,
        r#"
        SELECT action_name, job_key, count, last_at
        FROM attempt_counters
        WHERE count >= ?1
        ORDER BY count DESC, last_at DESC
        "#,
    ))
    .bind(min_cap as i64)
    .fetch_all(pool)
    .await?;
    let mut out: Vec<CappedKey> = rows
        .into_iter()
        .filter_map(|(action_name, job_key, count, last_at)| {
            let cap = caps.get(&action_name).copied().unwrap_or(default_cap) as i64;
            (count >= cap).then_some(CappedKey {
                action_name,
                job_key,
                count,
                cap,
                last_at,
            })
        })
        .collect();
    out.truncate(limit);
    Ok(out)
}

pub async fn attempts_get(pool: &DbPool, action: &str, key: &str) -> Result<u32> {
    let row: Option<(i64,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT count FROM attempt_counters WHERE action_name = ? AND job_key = ?",
    ))
    .bind(action)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(c,)| c as u32).unwrap_or(0))
}

pub async fn attempts_increment(pool: &DbPool, action: &str, key: &str) -> Result<u32> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO attempt_counters (action_name, job_key, count, last_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(action_name, job_key) DO UPDATE SET
            count = attempt_counters.count + 1,
            last_at = excluded.last_at
        "#,
    ))
    .bind(action)
    .bind(key)
    .bind(&now)
    .execute(pool)
    .await?;
    attempts_get(pool, action, key).await
}

/// CLAUDE-238: clear one (action, key) counter so a job parked at the cap can
/// be retried under the same key. Returns whether a row was removed, so the
/// caller (the API handler) can 404 an unknown counter instead of a silent
/// no-op on a mistyped key.
pub async fn attempts_clear(pool: &DbPool, action: &str, key: &str) -> Result<bool> {
    let result = sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM attempt_counters WHERE action_name = ? AND job_key = ?",
    ))
    .bind(action)
    .bind(key)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn daily_count_get(pool: &DbPool, action: &str) -> Result<u32> {
    let today = today_local().to_string();
    let row: Option<(i64,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT run_count FROM daily_ledger WHERE action_name = ? AND date = ?",
    ))
    .bind(action)
    .bind(&today)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(c,)| c as u32).unwrap_or(0))
}

pub async fn daily_count_increment(pool: &DbPool, action: &str) -> Result<()> {
    let today = today_local().to_string();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO daily_ledger (action_name, date, run_count, cost_usd)
        VALUES (?, ?, 1, 0.0)
        ON CONFLICT(action_name, date) DO UPDATE SET
            run_count = daily_ledger.run_count + 1
        "#,
    ))
    .bind(action)
    .bind(&today)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn daily_cost_get(pool: &DbPool, action: &str) -> Result<f64> {
    let today = today_local().to_string();
    let row: Option<(f64,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT cost_usd FROM daily_ledger WHERE action_name = ? AND date = ?",
    ))
    .bind(action)
    .bind(&today)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(c,)| c).unwrap_or(0.0))
}

pub async fn daily_cost_add(pool: &DbPool, action: &str, amount: f64) -> Result<()> {
    let today = today_local().to_string();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO daily_ledger (action_name, date, run_count, cost_usd)
        VALUES (?, ?, 0, ?)
        ON CONFLICT(action_name, date) DO UPDATE SET
            cost_usd = daily_ledger.cost_usd + excluded.cost_usd
        "#,
    ))
    .bind(action)
    .bind(&today)
    .bind(amount)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn run_begin(pool: &DbPool, action: &str, triggered_by: &str) -> Result<i64> {
    let now = now_iso();
    let id = sqlx::query_scalar::<_, i64>(&crate::db::q(
        pool,
        r#"
        INSERT INTO runs (action_name, started_at, triggered_by)
        VALUES (?, ?, ?)
        RETURNING id
        "#,
    ))
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
pub async fn run_set_job_keys(pool: &DbPool, run_id: i64, keys: &[String]) -> Result<()> {
    let payload = serde_json::to_string(keys)?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE runs SET job_keys = ? WHERE id = ?",
    ))
    .bind(payload)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-93: record per-job outbound links for a run as a JSON object mapping
/// job key to URL (`runs.job_links`). The webhook stores the failure
/// event's `html_url` so the run-detail tables can link the job key straight
/// to the Forgejo Actions run.
pub async fn run_set_job_links(
    pool: &DbPool,
    run_id: i64,
    links: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let payload = serde_json::to_string(links)?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE runs SET job_links = ? WHERE id = ?",
    ))
    .bind(payload)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-93: the per-job link map for a run. Empty for legacy rows (NULL
/// column) and for runs that link via the action's `link_template` instead.
pub async fn run_job_links(
    pool: &DbPool,
    run_id: i64,
) -> Result<std::collections::BTreeMap<String, String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT job_links FROM runs WHERE id = ?",
    ))
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    let json = row.and_then(|(j,)| j);
    if let Some(j) = json {
        Ok(serde_json::from_str(&j).unwrap_or_default())
    } else {
        Ok(Default::default())
    }
}

/// CLAUDE-14: mark which planned job a run is currently processing, so the web
/// UI can show exactly the one in-flight job as "running" instead of inferring
/// it from gaps in `job_outcomes`. Pass `Some(key)` before processing a job;
/// `run_finish` clears it back to NULL when the run ends.
pub async fn run_set_current_job(pool: &DbPool, run_id: i64, key: Option<&str>) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE runs SET current_job_key = ? WHERE id = ?",
    ))
    .bind(key)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-80: set (or clear, with `None`) the live pipeline stage of a run, so
/// the run-detail page can show what an in-flight run is doing (the webhook
/// fast-path advances it through download / triage / classify / mint).
/// `run_finish` clears it together with `current_job_key`.
pub async fn run_stage_set(pool: &DbPool, run_id: i64, stage: Option<&str>) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE runs SET stage = ? WHERE id = ?",
    ))
    .bind(stage)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-28: mark a job key as in flight for a run. Replaces the single
/// `run_set_current_job` marker so a run with several jobs running in parallel
/// badges all of them. Idempotent (`ON CONFLICT DO NOTHING`).
///
/// CLAUDE-163: also records the job's `concurrency_key` so the admission dedup
/// can drop gathered jobs whose concurrency key (project / repo) is already in
/// flight, not just those whose issue id is. For a path with one job per key
/// (webhook) the concurrency key equals `key`.
pub async fn run_inflight_add(
    pool: &DbPool,
    run_id: i64,
    key: &str,
    concurrency_key: &str,
) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "INSERT INTO run_inflight_jobs (run_id, job_key, concurrency_key) VALUES (?, ?, ?) \
         ON CONFLICT DO NOTHING",
    ))
    .bind(run_id)
    .bind(key)
    .bind(concurrency_key)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-28: clear one job's in-flight marker when it finishes.
pub async fn run_inflight_remove(pool: &DbPool, run_id: i64, key: &str) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM run_inflight_jobs WHERE run_id = ? AND job_key = ?",
    ))
    .bind(run_id)
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-28: the set of job keys currently in flight for a run.
pub async fn run_inflight_keys(pool: &DbPool, run_id: i64) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT job_key FROM run_inflight_jobs WHERE run_id = ?",
    ))
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(k,)| k).collect())
}

pub async fn run_job_keys(pool: &DbPool, run_id: i64) -> Result<Vec<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT job_keys FROM runs WHERE id = ?",
    ))
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

/// CLAUDE-147: an input/poll-phase diagnostic emitted by `gather()` before any
/// run row exists (e.g. a VCS probe failure, or a "no VCS integration" drop).
/// Collected during gather and persisted to `run_events`, then surfaced in the
/// web UI so the skipped issues and their reason are visible without reading the
/// daemon journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputEvent {
    /// `warn` / `error` / `info`. Errors are rendered visually distinct.
    pub level: String,
    /// Free-form context, typically the project key whose issues were skipped.
    pub context: Option<String>,
    pub message: String,
}

/// CLAUDE-147: one persisted `run_events` row, as read back for the web UI.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct RunEventRecord {
    /// The run this event was attached to, or `None` for a poll whose events
    /// were never backfilled (e.g. a zero-job poll with no run row).
    pub run_id: Option<i64>,
    pub ts: String,
    pub level: String,
    pub context: Option<String>,
    pub message: String,
}

/// CLAUDE-171: one persisted `run_events` row as read back for the error-log
/// page. The sibling of [`RunEventRecord`] that also carries `action_name`, so
/// the log's Action column can render once the filter is widened past the action
/// the page was entered from.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct RunEventLogRecord {
    pub run_id: Option<i64>,
    pub ts: String,
    pub level: String,
    pub context: Option<String>,
    pub message: String,
    pub action_name: String,
}

/// CLAUDE-235: one row of the Logs page's grouped 24h summary: a `context` or an
/// `action_name` and how many `warn`/`error` events carry it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
pub struct RunEventCount {
    /// The grouped value. Empty for the rows whose `context` is NULL or empty,
    /// which the page renders unlinked (no filter can express them).
    pub value: String,
    pub count: i64,
}

/// CLAUDE-147: persist this poll's input-phase events keyed by `action_name` +
/// `poll_ts`, with a NULL `run_id` (the run row does not exist yet, since
/// `gather()` runs before `run_begin`). [`run_events_attach`] backfills the run
/// id once it is created. Called before `run_begin` so the events survive even
/// if the run row is never created.
pub async fn run_events_record(
    pool: &DbPool,
    action: &str,
    poll_ts: &str,
    events: &[InputEvent],
) -> Result<()> {
    for e in events {
        sqlx::query(&crate::db::q(
            pool,
            r#"
            INSERT INTO run_events (run_id, action_name, ts, level, context, message)
            VALUES (NULL, ?, ?, ?, ?, ?)
            "#,
        ))
        .bind(action)
        .bind(poll_ts)
        .bind(&e.level)
        .bind(&e.context)
        .bind(&e.message)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// CLAUDE-206: persist run-phase events that already know their run row (the
/// forge notices a job accumulates, e.g. a degraded open-PR lookup). Unlike
/// [`run_events_record`] these need no [`run_events_attach`] backfill, so a
/// concurrent poll's NULL-`run_id` rows can never be attached by mistake.
pub async fn run_events_record_for_run(
    pool: &DbPool,
    run_id: i64,
    action: &str,
    events: &[InputEvent],
) -> Result<()> {
    let ts = now_iso();
    for e in events {
        sqlx::query(&crate::db::q(
            pool,
            r#"
            INSERT INTO run_events (run_id, action_name, ts, level, context, message)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        ))
        .bind(run_id)
        .bind(action)
        .bind(&ts)
        .bind(&e.level)
        .bind(&e.context)
        .bind(&e.message)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// CLAUDE-147: backfill `run_id` onto this poll's still-unattached events once
/// `run_begin` has created the run row, so the run-detail / action pages can tie
/// them to the run. Matches on `action_name` + the poll `ts` used at record
/// time; a zero-job poll that never reaches here keeps its events with a NULL
/// `run_id`, and they still surface on the action page under that timestamp.
pub async fn run_events_attach(
    pool: &DbPool,
    run_id: i64,
    action: &str,
    poll_ts: &str,
) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE run_events SET run_id = ? WHERE action_name = ? AND ts = ? AND run_id IS NULL",
    ))
    .bind(run_id)
    .bind(action)
    .bind(poll_ts)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-147: the most recent input-phase events for an action, newest first,
/// for the action-detail "Input errors / warnings" region. Queried by
/// `action_name` independent of any run row, so a zero-job poll's events (NULL
/// `run_id`) are returned too. Bounds display with `limit`.
pub async fn run_events_for_action(
    pool: &DbPool,
    action: &str,
    limit: i64,
) -> Result<Vec<RunEventRecord>> {
    let rows = sqlx::query_as::<_, RunEventRecord>(&crate::db::q(
        pool,
        r#"
        SELECT run_id, ts, level, context, message
        FROM run_events
        WHERE action_name = ?
        ORDER BY ts DESC, id DESC
        LIMIT ?
        "#,
    ))
    .bind(action)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-333: the newest message an action recorded that contains `needle`,
/// so a caller can tell whether a fault it is about to report repeats the one
/// it reported last time.
pub async fn run_events_last_message_containing(
    pool: &DbPool,
    action: &str,
    needle: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT message FROM run_events WHERE action_name = ? AND strpos(message, ?) > 0 ORDER BY ts DESC, id DESC LIMIT 1",
    ))
    .bind(action)
    .bind(needle)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// CLAUDE-235: how many input-phase diagnostics an action has recorded since
/// `started_at`, for the action-detail one-line summary. The rows themselves live
/// on the Logs page; this is only the signal that there are some.
///
/// `started_at` is the current serve's stamp ([`crate::settings::serve_started_at`]).
/// `None` means "no lower bound": every `ts` is a non-empty ISO string, so the
/// empty-string bind matches all rows. That is the degraded path for a DB no
/// serve has stamped yet, not the daemon's normal path.
pub async fn run_events_count_for_action(
    pool: &DbPool,
    action: &str,
    started_at: Option<&str>,
) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(&crate::db::q(
        pool,
        "SELECT COUNT(*) FROM run_events WHERE action_name = ? AND ts >= ?",
    ))
    .bind(action)
    .bind(started_at.unwrap_or(""))
    .fetch_one(pool)
    .await?;
    Ok(count)
}

/// CLAUDE-235: levels the Logs summary and the dashboard banner count. `info`
/// rows (the CLAUDE-216 access-recovery markers among them) are diagnostics of
/// record, not conditions to act on, so they stay out of the counts.
const DIAGNOSTIC_LEVELS: &str = "('warn', 'error')";

/// CLAUDE-235: total `warn`/`error` events recorded at or after `since`, for the
/// dashboard banner's headline count.
pub async fn run_events_count_since(pool: &DbPool, since: &str) -> Result<i64> {
    let sql =
        format!("SELECT COUNT(*) FROM run_events WHERE ts >= ? AND level IN {DIAGNOSTIC_LEVELS}");
    let count = sqlx::query_scalar::<_, i64>(&crate::db::q(pool, &sql))
        .bind(since)
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// CLAUDE-235: `warn`/`error` events at or after `since` grouped by `context`,
/// busiest first, for the Logs page's summary card. A NULL or empty `context`
/// groups under the empty string rather than being dropped, so the rows account
/// for every counted event.
pub async fn run_events_grouped_by_context(
    pool: &DbPool,
    since: &str,
    limit: i64,
) -> Result<Vec<RunEventCount>> {
    run_events_grouped_by(pool, "COALESCE(context, '')", since, limit).await
}

/// CLAUDE-235: `warn`/`error` events at or after `since` grouped by
/// `action_name`, busiest first, for the Logs page's summary card.
pub async fn run_events_grouped_by_action(
    pool: &DbPool,
    since: &str,
    limit: i64,
) -> Result<Vec<RunEventCount>> {
    run_events_grouped_by(pool, "action_name", since, limit).await
}

/// Shared body of the two grouped reads. `column` is a fixed SQL fragment chosen
/// by the caller, never user input; `since` and `limit` are bound parameters.
async fn run_events_grouped_by(
    pool: &DbPool,
    column: &str,
    since: &str,
    limit: i64,
) -> Result<Vec<RunEventCount>> {
    let sql = format!(
        r#"
        SELECT {column} AS value, COUNT(*) AS count
        FROM run_events
        WHERE ts >= ? AND level IN {DIAGNOSTIC_LEVELS}
        GROUP BY value
        ORDER BY count DESC, value ASC
        LIMIT ?
        "#
    );
    let rows = sqlx::query_as::<_, RunEventCount>(&crate::db::q(pool, &sql))
        .bind(since)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// CLAUDE-216: message prefix of the poll-time forge repo-access preflight
/// failure (`src/native/youtrack.rs`). The dashboard banner and the recovery
/// filter both key off it, so the two agree on what counts as an access event.
pub const ACCESS_FAILURE_PREFIX: &str = "forge repo-access preflight failed";
/// CLAUDE-216: message prefix of the recovery marker recorded when a repo that
/// was failing passes the preflight again. It clears the banner.
pub const ACCESS_RECOVERY_PREFIX: &str = "forge repo-access preflight passed";

/// CLAUDE-216: one still-open access failure, as the dashboard banner shows it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
pub struct AccessFailure {
    pub action_name: String,
    /// The project key whose issues were skipped.
    pub context: Option<String>,
    pub message: String,
    pub last_ts: String,
    /// Occurrences in the window (the preflight re-fires every poll).
    pub count: i64,
}

/// CLAUDE-216: poll-time access failures recorded at or after `since` that no
/// later successful preflight has superseded, newest first. A repo whose grant
/// lands mid-window records a recovery marker, which drops its failures here, so
/// the banner clears itself without an operator acknowledging anything.
/// `limit` is `None` for "every match": the dashboard banner counts an outage
/// rather than rendering a bounded table, and a silently truncated count would
/// understate it. The clause is dropped rather than given a negative bound,
/// which only SQLite reads as unlimited.
pub async fn access_failures_recent(
    pool: &DbPool,
    since: &str,
    limit: Option<i64>,
) -> Result<Vec<AccessFailure>> {
    let sql = format!(
        r#"
        SELECT action_name, context, message,
               MAX(ts) AS last_ts,
               COUNT(*) AS count
        FROM run_events e
        WHERE e.ts >= ?1
          AND e.message LIKE ?2 || '%'
          AND NOT EXISTS (
            SELECT 1 FROM run_events r
            WHERE r.action_name = e.action_name
              AND COALESCE(r.context, '') = COALESCE(e.context, '')
              AND r.message LIKE ?3 || '%'
              AND r.ts > e.ts
          )
        GROUP BY action_name, context, message
        ORDER BY last_ts DESC
        {}
        "#,
        match limit {
            Some(_) => "LIMIT ?4",
            None => "",
        }
    );
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut query = sqlx::query_as::<_, AccessFailure>(&sql)
        .bind(since)
        .bind(ACCESS_FAILURE_PREFIX)
        .bind(ACCESS_RECOVERY_PREFIX);
    if let Some(limit) = limit {
        query = query.bind(limit);
    }
    let rows = query.fetch_all(pool).await?;
    Ok(rows)
}

/// CLAUDE-216: true when the newest access event recorded for `(action,
/// context)` is a failure, i.e. a passing preflight now is a recovery worth
/// recording. Keeps the success marker off the event log in the normal case,
/// where every poll passes and nothing changed.
pub async fn access_failure_is_open(
    pool: &DbPool,
    action: &str,
    context: Option<&str>,
) -> Result<bool> {
    let newest: Option<String> = sqlx::query_scalar(&crate::db::q(
        pool,
        r#"
        SELECT message
        FROM run_events
        WHERE action_name = ?1
          AND COALESCE(context, '') = COALESCE(?2, '')
          AND (message LIKE ?3 || '%' OR message LIKE ?4 || '%')
        ORDER BY ts DESC, id DESC
        LIMIT 1
        "#,
    ))
    .bind(action)
    .bind(context)
    .bind(ACCESS_FAILURE_PREFIX)
    .bind(ACCESS_RECOVERY_PREFIX)
    .fetch_optional(pool)
    .await?;
    Ok(newest
        .map(|m| m.starts_with(ACCESS_FAILURE_PREFIX))
        .unwrap_or(false))
}

pub async fn run_finish(
    pool: &DbPool,
    run_id: i64,
    exit_code: i32,
    jobs_processed: u32,
) -> Result<()> {
    let now = now_iso();
    // CLAUDE-14: clear current_job_key atomically with the end state. A finished
    // run has no job in flight, so every break/finish path that reaches here
    // also drops the stale "running" marker. CLAUDE-80: the live stage is
    // cleared the same way so it never outlives its run.
    sqlx::query(&crate::db::q(
        pool,
        r#"
        UPDATE runs
        SET ended_at = ?, exit_code = ?, jobs_processed = ?, current_job_key = NULL, stage = NULL
        WHERE id = ?
        "#,
    ))
    .bind(&now)
    .bind(exit_code as i64)
    .bind(jobs_processed as i64)
    .bind(run_id)
    .execute(pool)
    .await?;
    // CLAUDE-28: a finished run has nothing in flight; drop any stale in-flight
    // job markers alongside clearing the legacy single-job marker above.
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM run_inflight_jobs WHERE run_id = ?",
    ))
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-228: closes a `run_begin` row on EVERY exit path.
///
/// Each runner used to open the row, then apply `?` to a dozen bookkeeping
/// awaits before reaching [`run_finish`]. One transient failure (in the incident
/// it was `pool timed out while waiting for an open connection`) returned from
/// the handler with the row still open, and nothing closed it while the daemon
/// lived: a scheduled CI-failure action accumulated 19 rows over three days,
/// every one of them rendered as a running job in the web UI.
///
/// The happy path calls [`RunGuard::finish`]. Anything else (an early `?`, an
/// error return, a panic unwinding through the handler) hits `Drop`, which
/// closes the row as [`crate::exit::UNEXPECTED`]. [`reap_orphaned_runs`] (at
/// serve start) and [`cancel_orphaned_runs`] (at shutdown) are the backstop for
/// a row this cannot reach, but a long-lived daemon reaches neither, which is
/// why the guard exists.
pub struct RunGuard {
    pool: DbPool,
    run_id: i64,
    finished: bool,
}

impl RunGuard {
    pub fn new(pool: &DbPool, run_id: i64) -> Self {
        Self {
            pool: pool.clone(),
            run_id,
            finished: false,
        }
    }

    /// The normal end of a run. Only a successful write disarms the guard, so a
    /// failing `run_finish` still leaves `Drop` to retry the close.
    pub async fn finish(mut self, exit_code: i32, jobs_processed: u32) -> Result<()> {
        run_finish(&self.pool, self.run_id, exit_code, jobs_processed).await?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let pool = self.pool.clone();
        let run_id = self.run_id;
        // `Drop` cannot await, so the close is spawned. `jobs_processed` stays 0:
        // the count lives in the handler frame that is unwinding, and inventing
        // one would be worse than under-reporting a run that already failed.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                warn!(
                    run_id,
                    "run ended without finalizing its row; closing it as unexpected"
                );
                handle.spawn(async move {
                    if let Err(e) =
                        run_finish(&pool, run_id, crate::exit::UNEXPECTED, 0).await
                    {
                        error!(run_id, error = %e, "closing the unfinalized run row failed; it stays open until the next daemon start");
                    }
                });
            }
            Err(_) => {
                error!(
                    run_id,
                    "run ended without finalizing its row and no runtime is available to close it; it stays open until the next daemon start"
                );
            }
        }
    }
}

/// CLAUDE-10: reconcile runs left open at daemon SHUTDOWN, after the cancel
/// grace elapsed. Those runs were stopped by this process, so they close as
/// `exit::CANCELLED`, the same sentinel the live-cancel path (CLAUDE-9) uses.
/// The startup sweep is [`reap_orphaned_runs`], which closes with a different
/// code because nothing cancelled those rows. Returns the number reconciled.
pub async fn cancel_orphaned_runs(pool: &DbPool) -> Result<u64> {
    close_open_runs(pool, crate::exit::CANCELLED).await
}

/// CLAUDE-228: the startup reap. A freshly started daemon holds the
/// single-instance lock, so no row still open can belong to a live run: each is
/// a leftover from a process that died without finalizing, or a row stranded
/// between `run_begin` and `run_finish` (the failure [`RunGuard`] now prevents).
/// Neither is a cancel, so they close as `exit::UNEXPECTED` and stay
/// distinguishable from an operator's `claude-run stop`. Returns the number
/// reaped; the caller logs a non-zero count at `warn`.
pub async fn reap_orphaned_runs(pool: &DbPool) -> Result<u64> {
    close_open_runs(pool, crate::exit::UNEXPECTED).await
}

/// Close every run row with no end state, stamping `exit_code`. `job_outcomes`
/// is left untouched: a job either has an outcome or it does not, and no
/// synthetic outcomes are invented. This is a pure DB reconcile: the run's
/// process is already gone, so there is no child to terminate or `post` phase to
/// run.
async fn close_open_runs(pool: &DbPool, exit_code: i32) -> Result<u64> {
    let now = now_iso();
    // CLAUDE-80: a dead process reports no stage; clear it with the end state.
    let result = sqlx::query(&crate::db::q(
        pool,
        "UPDATE runs SET ended_at = ?, exit_code = ?, stage = NULL WHERE ended_at IS NULL",
    ))
    .bind(&now)
    .bind(exit_code as i64)
    .execute(pool)
    .await?;
    // CLAUDE-28: nothing of the dead process is in flight, so any in-flight job
    // markers it left are leftovers. Clear them all.
    sqlx::query(&crate::db::q(pool, "DELETE FROM run_inflight_jobs"))
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
    /// CLAUDE-52: the job's claude run hit the wall-clock limit (exit 124) and
    /// was SIGKILLed. Distinct from a hard failure (`ok = 0`, `timed_out = 0`):
    /// the UI badges it separately and the failed-job count excludes it.
    pub timed_out: bool,
}

pub async fn job_outcome_insert(pool: &DbPool, r: &JobOutcome<'_>) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO job_outcomes (
            run_id, action_name, job_key, attempt, ts, ok,
            claude_ran, claude_ok, claude_cost_usd, attempt_counted,
            summary, stream_log_path, duration_ms, timed_out
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    ))
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
    .bind(r.timed_out as i64)
    .execute(pool)
    .await?;
    Ok(())
}

// ---- Phase 2: pause + action index --------------------------------------

pub async fn global_pause_status(pool: &DbPool) -> Result<Option<(String, Option<String>)>> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT paused_at, reason FROM global_pause WHERE id = 1",
    ))
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn global_pause_set(pool: &DbPool, reason: Option<&str>) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO global_pause (id, paused_at, reason) VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET paused_at = excluded.paused_at, reason = excluded.reason
        "#,
    ))
    .bind(&now)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn global_pause_clear(pool: &DbPool) -> Result<()> {
    sqlx::query(&crate::db::q(pool, "DELETE FROM global_pause WHERE id = 1"))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn action_pause_status(
    pool: &DbPool,
    action: &str,
) -> Result<Option<(String, Option<String>)>> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT paused_at, reason FROM paused_actions WHERE action_name = ?",
    ))
    .bind(action)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn action_pause_set(pool: &DbPool, action: &str, reason: Option<&str>) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(pool,
        r#"
        INSERT INTO paused_actions (action_name, paused_at, reason) VALUES (?, ?, ?)
        ON CONFLICT(action_name) DO UPDATE SET paused_at = excluded.paused_at, reason = excluded.reason
        "#,
    ))
    .bind(action)
    .bind(&now)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn action_pause_clear(pool: &DbPool, action: &str) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM paused_actions WHERE action_name = ?",
    ))
    .bind(action)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
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
    /// CLAUDE-162: the action's configured `concurrency.max_parallel`, cached in
    /// the index so the scheduler can resolve the `max_parallel_global` auto
    /// default without re-parsing every action's YAML each tick. Defaults to 1
    /// in the schema; the one query that reads this row always selects it.
    pub max_parallel: i64,
}

// CLAUDE-250: by hand rather than derived, because `enabled` is an integer 0/1
// column on both backends. See `crate::db::flag`.
impl sqlx::FromRow<'_, sqlx::any::AnyRow> for ActionIndexRow {
    fn from_row(row: &sqlx::any::AnyRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            name: row.try_get("name")?,
            file_path: row.try_get("file_path")?,
            yaml_sha256: row.try_get("yaml_sha256")?,
            enabled: crate::db::flag(row, "enabled")?,
            schedule_kind: row.try_get("schedule_kind")?,
            schedule_spec: row.try_get("schedule_spec")?,
            last_seen_at: row.try_get("last_seen_at")?,
            parse_error: row.try_get("parse_error")?,
            link_template: row.try_get("link_template")?,
            max_parallel: row.try_get("max_parallel")?,
        })
    }
}

pub async fn action_index_upsert(pool: &DbPool, row: &ActionIndexRow) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        r#"
        INSERT INTO actions (
            name, file_path, yaml_sha256, enabled,
            schedule_kind, schedule_spec, last_seen_at, parse_error, link_template, max_parallel
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            file_path = excluded.file_path,
            yaml_sha256 = excluded.yaml_sha256,
            enabled = excluded.enabled,
            schedule_kind = excluded.schedule_kind,
            schedule_spec = excluded.schedule_spec,
            last_seen_at = excluded.last_seen_at,
            parse_error = excluded.parse_error,
            link_template = excluded.link_template,
            max_parallel = excluded.max_parallel
        "#,
    ))
    .bind(&row.name)
    .bind(&row.file_path)
    .bind(&row.yaml_sha256)
    .bind(row.enabled as i64)
    .bind(&row.schedule_kind)
    .bind(&row.schedule_spec)
    .bind(&row.last_seen_at)
    .bind(&row.parse_error)
    .bind(&row.link_template)
    .bind(row.max_parallel)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn action_index_list(pool: &DbPool) -> Result<Vec<ActionIndexRow>> {
    let rows = sqlx::query_as::<_, ActionIndexRow>(&crate::db::q(pool,
        "SELECT name, file_path, yaml_sha256, enabled, schedule_kind, schedule_spec, last_seen_at, parse_error, link_template, max_parallel FROM actions ORDER BY name",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn action_last_run_started(pool: &DbPool, action: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT started_at FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 1",
    ))
    .bind(action)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(s,)| s))
}

/// CLAUDE-40: delete the `actions` index row for `name`. Returns `false` when
/// no row matched, mirroring [`secret_delete`]. Only touches the derived index;
/// the caller is responsible for the on-disk YAML and the runtime purge.
pub async fn action_index_delete(pool: &DbPool, name: &str) -> Result<bool> {
    let res = sqlx::query(&crate::db::q(pool, "DELETE FROM actions WHERE name = ?"))
        .bind(name)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// CLAUDE-40: purge an action's control state on delete: the `attempt_counters`
/// and `daily_ledger` rows keyed on `action_name`. The `paused_actions` row is
/// cleared separately via [`action_pause_clear`]. Run history (`runs`,
/// `job_outcomes`) and on-disk stream logs are intentionally left intact so a
/// re-created same-name action re-associates its audit trail.
pub async fn action_runtime_purge(pool: &DbPool, name: &str) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM attempt_counters WHERE action_name = ?",
    ))
    .bind(name)
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM daily_ledger WHERE action_name = ?",
    ))
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-40: purge an action's derived index + control state in one call:
/// the `actions` index row, the `paused_actions` row, and the
/// `attempt_counters` / `daily_ledger` rows. Returns the index row's prior
/// existence (`true` = a row was deleted), so callers can distinguish a real
/// delete from an unknown action. History (`runs`, `job_outcomes`) is kept.
pub async fn action_purge_state(pool: &DbPool, name: &str) -> Result<bool> {
    let existed = action_index_delete(pool, name).await?;
    action_pause_clear(pool, name).await?;
    action_runtime_purge(pool, name).await?;
    Ok(existed)
}

/// CLAUDE-40: true when a run for `action` is still in flight in the DB (a
/// `runs` row with no `ended_at`). The delete flow refuses with a conflict when
/// this holds. The daemon additionally consults its in-memory in-flight set for
/// a job dispatched but not yet recorded as a run.
pub async fn action_has_active_run(pool: &DbPool, action: &str) -> Result<bool> {
    let row: Option<(i64,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT id FROM runs WHERE action_name = ? AND ended_at IS NULL LIMIT 1",
    ))
    .bind(action)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// CLAUDE-223: one still-open run, for the About page's quiet indicator and the
/// `/api/status` activity block.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ActiveRun {
    pub id: i64,
    pub action_name: String,
    pub started_at: String,
}

/// CLAUDE-223: every run row with no `ended_at`, oldest first. Trustworthy as a
/// liveness signal because [`reap_orphaned_runs`] closes every leftover row at
/// daemon startup and [`RunGuard`] closes one whose handler errors mid-run, so
/// an open row means a run really is executing. Returns the
/// rows (not a count) so the busy indicator can list and link each one.
///
/// This is only HALF the quiet signal: it misses a run dispatched but not yet
/// past `run_begin`. The daemon's in-flight map covers that, and the two are
/// combined in `crate::api::activity`.
pub async fn active_runs(pool: &DbPool) -> Result<Vec<ActiveRun>> {
    let rows: Vec<(i64, String, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT id, action_name, started_at FROM runs WHERE ended_at IS NULL ORDER BY started_at",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, action_name, started_at)| ActiveRun {
            id,
            action_name,
            started_at,
        })
        .collect())
}

/// CLAUDE-162: the set of job keys currently in flight across every still-open
/// (not-yet-finished) run of `action`. A re-gather started while a prior run is
/// working an issue dedups its gathered jobs against this set, so an overlapping
/// pickup run never re-claims work another open run already holds. Joins
/// `run_inflight_jobs` (per-job markers, migration 0007) to the open `runs` rows.
pub async fn action_inflight_keys(pool: &DbPool, action: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        r#"
        SELECT rij.job_key
        FROM run_inflight_jobs rij
        JOIN runs r ON r.id = rij.run_id
        WHERE r.action_name = ? AND r.ended_at IS NULL
        "#,
    ))
    .bind(action)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(k,)| k).collect())
}

/// CLAUDE-163: the set of CONCURRENCY keys currently in flight across every
/// still-open run of `action`. A re-gather dedups its gathered jobs against this
/// set (by each job's concurrency key), so an overlapping pickup run never
/// gathers a second same-key job while that key is already in flight and could
/// only park on the KeyGate. NULL `concurrency_key` values (rows written before
/// migration 0016) are ignored. Joins `run_inflight_jobs` to the open `runs`.
pub async fn action_inflight_concurrency_keys(pool: &DbPool, action: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        r#"
        SELECT DISTINCT rij.concurrency_key
        FROM run_inflight_jobs rij
        JOIN runs r ON r.id = rij.run_id
        WHERE r.action_name = ? AND r.ended_at IS NULL
          AND rij.concurrency_key IS NOT NULL
        "#,
    ))
    .bind(action)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(k,)| k).collect())
}

// ---- CLAUDE-26: encrypted secrets ---------------------------------------

/// Metadata for one stored secret. The ciphertext is never carried in this
/// view, so the listing the web UI renders can never leak a value.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct SecretMeta {
    pub name: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

/// List stored secrets (names + timestamps only), alphabetical by name.
pub async fn secret_list(pool: &DbPool) -> Result<Vec<SecretMeta>> {
    let rows = sqlx::query_as::<_, SecretMeta>(&crate::db::q(
        pool,
        "SELECT name, created_at, last_used_at FROM secrets ORDER BY name",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// True when a secret with `name` already exists.
pub async fn secret_exists(pool: &DbPool, name: &str) -> Result<bool> {
    let row: Option<(i64,)> =
        sqlx::query_as(&crate::db::q(pool, "SELECT id FROM secrets WHERE name = ?"))
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// Create a new secret. Errors via the UNIQUE constraint when `name` is taken;
/// callers should check [`secret_exists`] first for a friendly message.
pub async fn secret_create(pool: &DbPool, name: &str, ciphertext: &[u8]) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        "INSERT INTO secrets (name, ciphertext, created_at) VALUES (?, ?, ?)",
    ))
    .bind(name)
    .bind(ciphertext)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Rotate an existing secret's value in place (keeps `created_at`). Returns
/// `false` when no secret with `name` exists.
pub async fn secret_rotate(pool: &DbPool, name: &str, ciphertext: &[u8]) -> Result<bool> {
    let res = sqlx::query(&crate::db::q(
        pool,
        "UPDATE secrets SET ciphertext = ? WHERE name = ?",
    ))
    .bind(ciphertext)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Delete a secret. Returns `false` when no secret with `name` exists.
pub async fn secret_delete(pool: &DbPool, name: &str) -> Result<bool> {
    let res = sqlx::query(&crate::db::q(pool, "DELETE FROM secrets WHERE name = ?"))
        .bind(name)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Fetch a secret's raw ciphertext for decryption at run time.
pub async fn secret_get_ciphertext(pool: &DbPool, name: &str) -> Result<Option<Vec<u8>>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT ciphertext FROM secrets WHERE name = ?",
    ))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(c,)| c))
}

/// Bump a secret's `last_used_at` to now (best-effort run-time audit).
pub async fn secret_touch(pool: &DbPool, name: &str) -> Result<()> {
    let now = now_iso();
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE secrets SET last_used_at = ? WHERE name = ?",
    ))
    .bind(&now)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem_pool() -> DbPool {
        crate::db::test_pool().await
    }

    async fn stage_of(pool: &DbPool, run_id: i64) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(&crate::db::q(
            pool,
            "SELECT stage FROM runs WHERE id = ?",
        ))
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn run_events_probe_failure_persists_and_web_read_returns_it() {
        // CLAUDE-147: a VCS probe failure recorded by gather() is persisted via
        // run_events_record and returned by the web read path
        // (run_events_for_action), proving the error is no longer log-only.
        let pool = mem_pool().await;
        let poll_ts = now_iso();
        let events = vec![
            InputEvent {
                level: "warn".into(),
                context: Some("VS".into()),
                message: "VCS probe failed (provider-CLI error); issues skipped this poll: project 'VS' not found".into(),
            },
            InputEvent {
                level: "info".into(),
                context: Some("ZZ".into()),
                message: "issues skipped: project has no enabled VCS integration".into(),
            },
        ];
        run_events_record(&pool, "youtrack", &poll_ts, &events)
            .await
            .unwrap();

        // The web read path returns both events, newest-first, with a NULL
        // run_id until backfilled (the zero-job / pre-run_begin state).
        let read = run_events_for_action(&pool, "youtrack", 50).await.unwrap();
        assert_eq!(read.len(), 2);
        let warn = read
            .iter()
            .find(|e| e.level == "warn")
            .expect("probe-failure warn event present");
        assert_eq!(warn.context.as_deref(), Some("VS"));
        assert!(warn.message.contains("VCS probe failed"));
        assert!(warn.run_id.is_none());

        // A different action does not see these events.
        assert!(run_events_for_action(&pool, "other", 50)
            .await
            .unwrap()
            .is_empty());

        // After run_begin, the poll's events backfill onto the run row.
        let run = run_begin(&pool, "youtrack", "cli").await.unwrap();
        run_events_attach(&pool, run, "youtrack", &poll_ts)
            .await
            .unwrap();
        let read = run_events_for_action(&pool, "youtrack", 50).await.unwrap();
        assert!(read.iter().all(|e| e.run_id == Some(run)));
    }

    #[tokio::test]
    async fn run_phase_event_attaches_to_its_run_and_reads_on_the_action_page() {
        // CLAUDE-206: a degraded open-PR lookup is recorded mid-run, so it knows
        // its run row already and needs no attach backfill. It reads back on the
        // action page carrying that run id, and a concurrent poll's
        // NULL-run_id event is left alone.
        let pool = mem_pool().await;
        run_events_record(
            &pool,
            "youtrack",
            &now_iso(),
            &[InputEvent {
                level: "info".into(),
                context: Some("ZZ".into()),
                message: "unrelated poll event".into(),
            }],
        )
        .await
        .unwrap();
        let run = run_begin(&pool, "youtrack", "cli").await.unwrap();
        run_events_record_for_run(
            &pool,
            run,
            "youtrack",
            &[InputEvent {
                level: "warn".into(),
                context: Some("CLAUDE-206".into()),
                message: "open-PR lookup for `contoso/claude-run` failed (403); created anyway"
                    .into(),
            }],
        )
        .await
        .unwrap();

        let read = run_events_for_action(&pool, "youtrack", 50).await.unwrap();
        let degraded = read
            .iter()
            .find(|e| e.message.contains("open-PR lookup"))
            .expect("degraded-lookup event present on the action page");
        assert_eq!(degraded.run_id, Some(run));
        assert_eq!(degraded.context.as_deref(), Some("CLAUDE-206"));
        assert_eq!(degraded.level, "warn");
        // The poll event recorded before the run row stays unattached.
        let poll_event = read
            .iter()
            .find(|e| e.message == "unrelated poll event")
            .unwrap();
        assert!(poll_event.run_id.is_none());
    }

    #[tokio::test]
    async fn run_job_links_roundtrip_and_legacy_null() {
        // CLAUDE-93: set + read the per-job link map; a legacy row (no write)
        // reads back as an empty map, not an error.
        let pool = mem_pool().await;
        let run = run_begin(&pool, "act", "webhook").await.unwrap();
        assert!(run_job_links(&pool, run).await.unwrap().is_empty());

        let links = std::collections::BTreeMap::from([(
            "owner/repo#7".to_string(),
            "https://dev.a8n.run/owner/repo/actions/runs/7".to_string(),
        )]);
        run_set_job_links(&pool, run, &links).await.unwrap();
        assert_eq!(run_job_links(&pool, run).await.unwrap(), links);

        let legacy = run_begin(&pool, "act", "cli").await.unwrap();
        assert!(run_job_links(&pool, legacy).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_stage_roundtrips_and_clears_on_finish() {
        // CLAUDE-80: set, overwrite, explicit clear, and the run_finish clear.
        let pool = mem_pool().await;
        let run = run_begin(&pool, "act", "webhook").await.unwrap();
        assert_eq!(stage_of(&pool, run).await, None);

        run_stage_set(&pool, run, Some("downloading forgejo log"))
            .await
            .unwrap();
        assert_eq!(
            stage_of(&pool, run).await.as_deref(),
            Some("downloading forgejo log")
        );

        run_stage_set(&pool, run, Some("minting issue"))
            .await
            .unwrap();
        assert_eq!(stage_of(&pool, run).await.as_deref(), Some("minting issue"));

        run_stage_set(&pool, run, None).await.unwrap();
        assert_eq!(stage_of(&pool, run).await, None);

        run_stage_set(&pool, run, Some("triaging")).await.unwrap();
        run_finish(&pool, run, 0, 1).await.unwrap();
        assert_eq!(stage_of(&pool, run).await, None, "finish clears the stage");
    }

    async fn exit_code_of(pool: &DbPool, run_id: i64) -> Option<i64> {
        sqlx::query_scalar::<_, Option<i64>>(&crate::db::q(
            pool,
            "SELECT exit_code FROM runs WHERE id = ?",
        ))
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn ended_at_of(pool: &DbPool, run_id: i64) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(&crate::db::q(
            pool,
            "SELECT ended_at FROM runs WHERE id = ?",
        ))
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// CLAUDE-228: the happy path closes the row with the run's own exit code.
    #[tokio::test]
    async fn run_guard_finish_closes_the_row() {
        let pool = mem_pool().await;
        let run = run_begin(&pool, "act", "scheduler").await.unwrap();

        RunGuard::new(&pool, run)
            .finish(crate::exit::OK, 4)
            .await
            .unwrap();

        assert_eq!(exit_code_of(&pool, run).await, Some(crate::exit::OK as i64));
        assert!(ended_at_of(&pool, run).await.is_some());
        let processed: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT jobs_processed FROM runs WHERE id = ?",
        ))
        .bind(run)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(processed, 4);
    }

    /// CLAUDE-228: the regression. A handler that returns early (here, the
    /// `pool timed out` shape: an error between `run_begin` and `run_finish`)
    /// used to leave the row open forever, and the web UI rendered it as a
    /// running job for days. The guard closes it as `UNEXPECTED` on drop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_guard_closes_the_row_when_the_handler_errors() {
        let pool = mem_pool().await;
        let run = run_begin(&pool, "act", "scheduler").await.unwrap();

        // A handler shaped like the real ones: open the row, then fail an await.
        async fn handler(pool: &DbPool, run: i64) -> Result<()> {
            let _guard = RunGuard::new(pool, run);
            Err(crate::error::Error::Other(anyhow::anyhow!(
                "pool timed out while waiting for an open connection"
            )))
        }
        assert!(handler(&pool, run).await.is_err());

        // `Drop` cannot await, so the close is spawned; wait for it to land.
        let mut ended = None;
        for _ in 0..100 {
            ended = ended_at_of(&pool, run).await;
            if ended.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            ended.is_some(),
            "an errored handler must not leave its run row open"
        );
        assert_eq!(
            exit_code_of(&pool, run).await,
            Some(crate::exit::UNEXPECTED as i64),
            "an unfinalized run is distinguishable from a clean finish and from a cancel"
        );
    }

    /// A guard that finished must not be re-closed by its own drop, or a clean
    /// run would be relabelled `UNEXPECTED` right after reporting success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_guard_does_not_reclose_a_finished_run() {
        let pool = mem_pool().await;
        let run = run_begin(&pool, "act", "scheduler").await.unwrap();
        RunGuard::new(&pool, run)
            .finish(crate::exit::PAUSED, 1)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            exit_code_of(&pool, run).await,
            Some(crate::exit::PAUSED as i64),
            "the finished exit code survives the guard's drop"
        );
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

    /// CLAUDE-228: the startup reap. A row still open when the daemon starts was
    /// stranded by a process that died without finalizing, not cancelled by an
    /// operator, so it closes as `UNEXPECTED` and stays distinguishable from a
    /// `claude-run stop`. This is what cleared the 19 stranded rows a scheduled
    /// CI-failure action had left, rendered as running jobs for three days.
    #[tokio::test]
    async fn reap_orphaned_runs_closes_open_rows_as_unexpected() {
        let pool = mem_pool().await;

        let finished = run_begin(&pool, "act", "cli").await.unwrap();
        run_finish(&pool, finished, crate::exit::OK, 2)
            .await
            .unwrap();
        let orphan = run_begin(&pool, "act", "scheduler").await.unwrap();
        assert!(ended_at_of(&pool, orphan).await.is_none(), "seeded open");

        assert_eq!(reap_orphaned_runs(&pool).await.unwrap(), 1);

        assert!(
            ended_at_of(&pool, orphan).await.is_some(),
            "the orphaned row is closed"
        );
        assert_eq!(
            exit_code_of(&pool, orphan).await,
            Some(crate::exit::UNEXPECTED as i64),
            "a startup orphan is not an operator cancel"
        );
        // A completed run keeps its own outcome, and a second reap is a no-op.
        assert_eq!(
            exit_code_of(&pool, finished).await,
            Some(crate::exit::OK as i64)
        );
        assert_eq!(reap_orphaned_runs(&pool).await.unwrap(), 0);
    }

    /// CLAUDE-228: every runner that opens a run row pairs it with a
    /// [`RunGuard`], which is what closes the row on an error return (proved by
    /// `run_guard_closes_the_row_when_the_handler_errors` above). Enumerated
    /// mechanically over the runner sources so a NEW runner, or a `run_begin`
    /// added to an existing one, fails here instead of silently reintroducing
    /// the leak. Test modules are excluded: they open rows deliberately.
    #[test]
    fn every_runner_pairs_run_begin_with_a_run_guard() {
        let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let runners = [
            "native/youtrack.rs",
            "native/closed_issue_summary.rs",
            "daemon.rs",
        ];
        for file in runners {
            let text = std::fs::read_to_string(src_dir.join(file)).unwrap();
            let code = text.split("\n#[cfg(test)]").next().unwrap();
            let begins = code.matches("run_begin(").count();
            let guards = code.matches("RunGuard::new(").count();
            assert!(
                begins > 0,
                "{file} no longer opens a run row; update this list"
            );
            assert_eq!(
                begins, guards,
                "{file}: {begins} run_begin call(s) but {guards} RunGuard(s); an unguarded row stays open forever when the handler errors"
            );
        }
    }

    /// CLAUDE-315: every `claude` invocation must leave a `claude_invocations`
    /// row, so plan usage is queryable history rather than only a log line. A
    /// call site that reaches `ClaudeInvoker::invoke` (or the trait method of
    /// the same name) without a matching `record_invocation` call in the same
    /// file loses that history silently. Enumerated mechanically over the
    /// known call sites, matching `every_runner_pairs_run_begin_with_a_run_guard`
    /// above, so a new `.invoke(` call site added without its
    /// `record_invocation` companion fails here instead of shipping a gap in
    /// the usage record. Test modules are excluded: a mock invoker's `.invoke(`
    /// in a test fixture records nothing.
    #[test]
    fn every_claude_invoke_call_site_pairs_with_a_record_invocation_call() {
        let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let call_sites = [
            "job.rs",
            "native/youtrack.rs",
            "triage.rs",
            "web/comment_webhooks.rs",
        ];
        for file in call_sites {
            let text = std::fs::read_to_string(src_dir.join(file)).unwrap();
            let code = text.split("\n#[cfg(test)]").next().unwrap();
            let invokes = code.matches(".invoke(").count();
            let records = code.matches("record_invocation(").count();
            assert!(
                invokes > 0,
                "{file} no longer calls claude.invoke(); update this list"
            );
            assert_eq!(
                invokes, records,
                "{file}: {invokes} .invoke() call(s) but {records} record_invocation() call(s); a claude invocation must always leave a claude_invocations row"
            );
        }
    }

    #[tokio::test]
    async fn secrets_crud_round_trips() {
        let pool = mem_pool().await;
        assert!(secret_list(&pool).await.unwrap().is_empty());
        assert!(!secret_exists(&pool, "yt").await.unwrap());

        secret_create(&pool, "yt", b"ciphertext-v1").await.unwrap();
        assert!(secret_exists(&pool, "yt").await.unwrap());
        // Duplicate create is rejected by the UNIQUE constraint.
        assert!(secret_create(&pool, "yt", b"dup").await.is_err());

        let list = secret_list(&pool).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "yt");
        assert!(list[0].last_used_at.is_none());

        assert_eq!(
            secret_get_ciphertext(&pool, "yt").await.unwrap().unwrap(),
            b"ciphertext-v1"
        );

        // Rotate replaces the ciphertext; touch records use.
        assert!(secret_rotate(&pool, "yt", b"ciphertext-v2").await.unwrap());
        assert_eq!(
            secret_get_ciphertext(&pool, "yt").await.unwrap().unwrap(),
            b"ciphertext-v2"
        );
        secret_touch(&pool, "yt").await.unwrap();
        assert!(secret_list(&pool).await.unwrap()[0].last_used_at.is_some());

        // Rotating / deleting an unknown name reports "did not exist".
        assert!(!secret_rotate(&pool, "ghost", b"x").await.unwrap());
        assert!(!secret_delete(&pool, "ghost").await.unwrap());

        assert!(secret_delete(&pool, "yt").await.unwrap());
        assert!(secret_list(&pool).await.unwrap().is_empty());
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

    #[tokio::test]
    async fn timed_out_outcome_persists_and_is_excluded_from_failed_count() {
        // CLAUDE-52: a timed-out outcome round-trips `timed_out = 1` (still
        // `ok = 0`) and is NOT counted as a failed job, while a plain failure is.
        let pool = mem_pool().await;
        let run_id = run_begin(&pool, "act", "cli").await.unwrap();

        let base = JobOutcome {
            run_id,
            action_name: "act",
            job_key: "",
            attempt: 1,
            ok: false,
            claude_ran: true,
            claude_ok: false,
            claude_cost_usd: 0.1,
            attempt_counted: true,
            summary: None,
            stream_log_path: None,
            duration_ms: 10,
            timed_out: false,
        };
        // A hard failure and a timeout in the same run.
        job_outcome_insert(
            &pool,
            &JobOutcome {
                job_key: "FAIL-1",
                ..base
            },
        )
        .await
        .unwrap();
        job_outcome_insert(
            &pool,
            &JobOutcome {
                job_key: "TIMEOUT-1",
                timed_out: true,
                ..base
            },
        )
        .await
        .unwrap();

        // The timed_out column persisted exactly for the timeout job.
        let flag: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT timed_out FROM job_outcomes WHERE job_key = 'TIMEOUT-1'",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(flag, 1);

        // The web failed-job count (ok = 0 AND timed_out = 0) sees only the
        // hard failure, not the timeout.
        let failed: i64 = sqlx::query_scalar(&crate::db::q(&pool,
            "SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = ? AND o.ok = 0 AND o.timed_out = 0",
        ))
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(failed, 1, "timed-out job excluded from failed count");
    }

    // ---- CLAUDE-40: action delete ----------------------------------------

    fn index_row(name: &str, path: &str) -> ActionIndexRow {
        ActionIndexRow {
            name: name.to_string(),
            file_path: path.to_string(),
            yaml_sha256: "0".repeat(64),
            enabled: true,
            schedule_kind: None,
            schedule_spec: None,
            last_seen_at: now_iso(),
            parse_error: None,
            link_template: None,
            max_parallel: 1,
        }
    }

    async fn runs_count(pool: &DbPool, action: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(&crate::db::q(
            pool,
            "SELECT COUNT(*) FROM runs WHERE action_name = ?",
        ))
        .bind(action)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn action_index_delete_reports_existence() {
        let pool = mem_pool().await;
        action_index_upsert(&pool, &index_row("act", "/tmp/act.yml"))
            .await
            .unwrap();
        // First delete removes the row and reports it existed; second is false.
        assert!(action_index_delete(&pool, "act").await.unwrap());
        assert!(!action_index_delete(&pool, "act").await.unwrap());
        assert!(!action_index_delete(&pool, "ghost").await.unwrap());
    }

    #[tokio::test]
    async fn attempts_clear_removes_one_key_and_reports_existence() {
        let pool = mem_pool().await;
        attempts_increment(&pool, "act", "job-1").await.unwrap();
        attempts_increment(&pool, "act", "job-2").await.unwrap();

        // Clearing a key that has no row is a reported no-op.
        assert!(!attempts_clear(&pool, "act", "ghost").await.unwrap());

        // Clearing the seeded key removes only that key's row.
        assert!(attempts_clear(&pool, "act", "job-1").await.unwrap());
        assert_eq!(attempts_get(&pool, "act", "job-1").await.unwrap(), 0);
        assert_eq!(attempts_get(&pool, "act", "job-2").await.unwrap(), 1);

        // Clearing it again is now a no-op.
        assert!(!attempts_clear(&pool, "act", "job-1").await.unwrap());
    }

    #[tokio::test]
    async fn action_purge_state_clears_control_but_keeps_history() {
        let pool = mem_pool().await;
        // Seed index + control state + a finished run (history).
        action_index_upsert(&pool, &index_row("act", "/tmp/act.yml"))
            .await
            .unwrap();
        action_pause_set(&pool, "act", Some("hold")).await.unwrap();
        attempts_increment(&pool, "act", "job-1").await.unwrap();
        daily_count_increment(&pool, "act").await.unwrap();
        let run = run_begin(&pool, "act", "cli").await.unwrap();
        run_finish(&pool, run, crate::exit::OK, 1).await.unwrap();

        let existed = action_purge_state(&pool, "act").await.unwrap();
        assert!(existed, "purge reports the index row existed");

        // Control state is gone.
        assert!(action_index_list(&pool).await.unwrap().is_empty());
        assert!(action_pause_status(&pool, "act").await.unwrap().is_none());
        assert_eq!(attempts_get(&pool, "act", "job-1").await.unwrap(), 0);
        assert_eq!(daily_count_get(&pool, "act").await.unwrap(), 0);

        // History is kept: the runs row survives.
        assert_eq!(runs_count(&pool, "act").await, 1);

        // Purging an unknown action reports it did not exist.
        assert!(!action_purge_state(&pool, "ghost").await.unwrap());
    }

    #[tokio::test]
    async fn action_has_active_run_tracks_in_flight() {
        let pool = mem_pool().await;
        assert!(!action_has_active_run(&pool, "act").await.unwrap());

        let run = run_begin(&pool, "act", "cli").await.unwrap();
        assert!(
            action_has_active_run(&pool, "act").await.unwrap(),
            "an unfinished run counts as in flight"
        );

        run_finish(&pool, run, crate::exit::OK, 0).await.unwrap();
        assert!(
            !action_has_active_run(&pool, "act").await.unwrap(),
            "a finished run is no longer in flight"
        );
    }

    /// CLAUDE-223: `active_runs` lists every open run (id, action, start) and
    /// empties as each finishes, so the quiet indicator can both decide and list.
    #[tokio::test]
    async fn active_runs_lists_open_runs_only() {
        let pool = mem_pool().await;
        assert!(active_runs(&pool).await.unwrap().is_empty());

        let a = run_begin(&pool, "act-a", "cli").await.unwrap();
        let b = run_begin(&pool, "act-b", "scheduler").await.unwrap();
        let open = active_runs(&pool).await.unwrap();
        assert_eq!(open.len(), 2);
        assert_eq!(open[0].id, a);
        assert_eq!(open[0].action_name, "act-a");
        assert!(!open[0].started_at.is_empty());

        run_finish(&pool, a, crate::exit::OK, 0).await.unwrap();
        let open = active_runs(&pool).await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, b);

        run_finish(&pool, b, crate::exit::PAUSED, 0).await.unwrap();
        assert!(active_runs(&pool).await.unwrap().is_empty());
    }

    // CLAUDE-162: with two OVERLAPPING open runs for one action,
    // `action_has_active_run` stays true until BOTH finish, and
    // `action_last_run_started` returns the most recently started of the two, so
    // the delete-conflict guard and the schedule bookkeeping both behave with
    // multiple concurrent runs.
    #[tokio::test]
    async fn multiple_open_runs_are_tracked_correctly() {
        let pool = mem_pool().await;
        let r1 = run_begin(&pool, "act", "scheduler").await.unwrap();
        let r2 = run_begin(&pool, "act", "trigger").await.unwrap();

        assert!(action_has_active_run(&pool, "act").await.unwrap());
        // The later run started at or after the earlier; the query returns the
        // newest by started_at (ties broken by row order, still an open run).
        let last = action_last_run_started(&pool, "act").await.unwrap();
        assert!(last.is_some(), "an active run reports a start time");

        // Finishing one of two open runs leaves the action still in flight.
        run_finish(&pool, r1, crate::exit::OK, 0).await.unwrap();
        assert!(
            action_has_active_run(&pool, "act").await.unwrap(),
            "still in flight while the second run is open"
        );

        run_finish(&pool, r2, crate::exit::OK, 0).await.unwrap();
        assert!(!action_has_active_run(&pool, "act").await.unwrap());
    }

    // CLAUDE-162: `action_inflight_keys` returns the union of in-flight job keys
    // across every OPEN run of the action, and excludes keys from finished runs,
    // so the overlapping-run admission check dedups against exactly the work
    // another live run currently holds.
    #[tokio::test]
    async fn action_inflight_keys_unions_open_runs() {
        let pool = mem_pool().await;
        let r1 = run_begin(&pool, "act", "scheduler").await.unwrap();
        let r2 = run_begin(&pool, "act", "trigger").await.unwrap();
        run_inflight_add(&pool, r1, "ISSUE-1", "PROJ-A")
            .await
            .unwrap();
        run_inflight_add(&pool, r2, "ISSUE-2", "PROJ-B")
            .await
            .unwrap();
        // A different action's in-flight key must not leak in.
        let other = run_begin(&pool, "other", "scheduler").await.unwrap();
        run_inflight_add(&pool, other, "ISSUE-9", "PROJ-C")
            .await
            .unwrap();

        let mut keys = action_inflight_keys(&pool, "act").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["ISSUE-1".to_string(), "ISSUE-2".to_string()]);

        // Finishing r1 drops its in-flight markers, so only r2's key remains.
        run_finish(&pool, r1, crate::exit::OK, 1).await.unwrap();
        let keys = action_inflight_keys(&pool, "act").await.unwrap();
        assert_eq!(keys, vec!["ISSUE-2".to_string()]);
    }

    // CLAUDE-163: `action_inflight_concurrency_keys` returns the union of
    // in-flight CONCURRENCY keys across open runs of the action (distinct,
    // excluding other actions and finished runs), so the admission dedup drops a
    // gathered job whose project/repo is already in flight even if its issue id
    // is not.
    #[tokio::test]
    async fn action_inflight_concurrency_keys_unions_open_runs() {
        let pool = mem_pool().await;
        let r1 = run_begin(&pool, "act", "scheduler").await.unwrap();
        let r2 = run_begin(&pool, "act", "trigger").await.unwrap();
        // Two different issues sharing one concurrency key (same project).
        run_inflight_add(&pool, r1, "BUNYIP-1", "BUNYIP")
            .await
            .unwrap();
        run_inflight_add(&pool, r2, "MAPPS-3", "MAPPS")
            .await
            .unwrap();
        // A different action's in-flight concurrency key must not leak in.
        let other = run_begin(&pool, "other", "scheduler").await.unwrap();
        run_inflight_add(&pool, other, "PMS-9", "PMS")
            .await
            .unwrap();

        let mut keys = action_inflight_concurrency_keys(&pool, "act")
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["BUNYIP".to_string(), "MAPPS".to_string()]);

        // Finishing r1 drops BUNYIP; only MAPPS remains in flight.
        run_finish(&pool, r1, crate::exit::OK, 1).await.unwrap();
        let keys = action_inflight_concurrency_keys(&pool, "act")
            .await
            .unwrap();
        assert_eq!(keys, vec!["MAPPS".to_string()]);
    }

    /// CLAUDE-216: the dashboard banner shows a poll-time access failure and
    /// stops showing it once a later poll for the same repo passes.
    #[tokio::test]
    async fn access_failures_surface_until_a_later_pass_supersedes_them() {
        let pool = mem_pool().await;
        let fail = InputEvent {
            level: "warn".into(),
            context: Some("CLAUDE".into()),
            message: format!(
                "{ACCESS_FAILURE_PREFIX} for `contoso/claude-run` on dev.a8n.run; issues skipped"
            ),
        };
        run_events_record(
            &pool,
            "youtrack",
            "2026-08-01T00:00:00Z",
            std::slice::from_ref(&fail),
        )
        .await
        .unwrap();
        run_events_record(&pool, "youtrack", "2026-08-01T00:00:05Z", &[fail])
            .await
            .unwrap();

        let open = access_failures_recent(&pool, "2026-07-31T00:00:00Z", Some(10))
            .await
            .unwrap();
        assert_eq!(open.len(), 1, "{open:?}");
        assert_eq!(open[0].action_name, "youtrack");
        assert_eq!(open[0].context.as_deref(), Some("CLAUDE"));
        assert_eq!(open[0].count, 2);
        assert!(access_failure_is_open(&pool, "youtrack", Some("CLAUDE"))
            .await
            .unwrap());

        // The recovery marker recorded on the next passing poll clears it.
        run_events_record(
            &pool,
            "youtrack",
            "2026-08-01T00:00:10Z",
            &[InputEvent {
                level: "info".into(),
                context: Some("CLAUDE".into()),
                message: format!(
                    "{ACCESS_RECOVERY_PREFIX} for `contoso/claude-run` on dev.a8n.run"
                ),
            }],
        )
        .await
        .unwrap();
        assert!(
            access_failures_recent(&pool, "2026-07-31T00:00:00Z", Some(10))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!access_failure_is_open(&pool, "youtrack", Some("CLAUDE"))
            .await
            .unwrap());

        // Outside the window it does not surface at all.
        assert!(
            access_failures_recent(&pool, "2026-09-01T00:00:00Z", Some(10))
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// CLAUDE-235: the Logs summary groups the window's `warn`/`error` events by
    /// context and by action, busiest first, and leaves `info` out of both.
    #[tokio::test]
    async fn run_event_summaries_group_diagnostics_and_skip_info() {
        let pool = mem_pool().await;
        let ev = |level: &str, context: Option<&str>, message: &str| InputEvent {
            level: level.into(),
            context: context.map(str::to_string),
            message: message.into(),
        };
        run_events_record(
            &pool,
            "youtrack",
            "2026-08-01T00:00:00Z",
            &[
                ev("warn", Some("CLAUDE"), "probe failed"),
                ev("error", Some("CLAUDE"), "clone failed"),
                ev("warn", Some("PMS"), "probe failed"),
                // Neither counted nor grouped: `info` is a marker, not a signal.
                ev("info", Some("CLAUDE"), "preflight passed"),
                // A NULL context still accounts for its event.
                ev("warn", None, "no VCS integration"),
            ],
        )
        .await
        .unwrap();
        run_events_record(
            &pool,
            "other",
            "2026-08-01T00:00:05Z",
            &[ev("error", Some("PMS"), "duplicate source")],
        )
        .await
        .unwrap();
        // Before the window: counted by neither read.
        run_events_record(
            &pool,
            "youtrack",
            "2000-01-01T00:00:00Z",
            &[ev("error", Some("CLAUDE"), "ancient failure")],
        )
        .await
        .unwrap();

        let since = "2026-07-31T00:00:00Z";
        assert_eq!(run_events_count_since(&pool, since).await.unwrap(), 5);

        let by_context = run_events_grouped_by_context(&pool, since, 10)
            .await
            .unwrap();
        assert_eq!(
            by_context,
            vec![
                RunEventCount {
                    value: "CLAUDE".into(),
                    count: 2
                },
                RunEventCount {
                    value: "PMS".into(),
                    count: 2
                },
                RunEventCount {
                    value: String::new(),
                    count: 1
                },
            ]
        );

        let by_action = run_events_grouped_by_action(&pool, since, 10)
            .await
            .unwrap();
        assert_eq!(
            by_action,
            vec![
                RunEventCount {
                    value: "youtrack".into(),
                    count: 4
                },
                RunEventCount {
                    value: "other".into(),
                    count: 1
                },
            ]
        );

        // The action-detail count is per action and window-bounded by the serve
        // stamp, and counts every level (the region is "input errors / warnings",
        // but the page it links to lists them all).
        assert_eq!(
            run_events_count_for_action(&pool, "youtrack", Some("2026-07-31T00:00:00Z"))
                .await
                .unwrap(),
            5
        );
        assert_eq!(
            run_events_count_for_action(&pool, "youtrack", None)
                .await
                .unwrap(),
            6
        );
    }

    /// CLAUDE-225: re-arming the backoff (and re-commenting a changed blocker
    /// set) must not reset `blocked_since`, or a gate re-evaluated every 5m would
    /// read as 5m old forever and never surface. Clearing the row and blocking
    /// again does start a new block.
    #[tokio::test]
    async fn dependency_gate_upsert_preserves_the_first_blocked_stamp() {
        let pool = mem_pool().await;
        dependency_gate_upsert(
            &pool,
            "act",
            "AUD-31",
            now_epoch_ms() + 300_000,
            "AUD-32 (depends on)",
        )
        .await
        .unwrap();
        let first = dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .unwrap();

        // Age the row, then re-arm with a CHANGED blocker set (a blocker cleared).
        sqlx::query(&crate::db::q(
            &pool,
            "UPDATE dependency_gate SET blocked_since = ? WHERE job_key = 'AUD-31'",
        ))
        .bind(first.blocked_since_ms - 7_200_000)
        .execute(&pool)
        .await
        .unwrap();
        dependency_gate_upsert(
            &pool,
            "act",
            "AUD-31",
            now_epoch_ms() + 300_000,
            "AUD-33 (depends on)",
        )
        .await
        .unwrap();
        let again = dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            again.blocked_since_ms,
            first.blocked_since_ms - 7_200_000,
            "the first-blocked stamp survives a re-arm"
        );
        assert_eq!(again.signature, "AUD-33 (depends on)");

        // A cleared gate starts the clock over on the next block.
        dependency_gate_clear(&pool, "act", "AUD-31").await.unwrap();
        dependency_gate_upsert(
            &pool,
            "act",
            "AUD-31",
            now_epoch_ms() + 300_000,
            "AUD-34 (depends on)",
        )
        .await
        .unwrap();
        let fresh = dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .unwrap();
        assert!(
            fresh.blocked_since_ms > first.blocked_since_ms - 7_200_000,
            "a new block after a clear is a new block"
        );
    }

    /// CLAUDE-225: the poll-time prune keeps the gate rows of issues the query
    /// still matches and drops the rest, so an issue that left the candidate set
    /// while blocked (resolved by hand, re-fielded) cannot sit in the stale-gate
    /// banner forever. Other actions' rows are untouched.
    #[tokio::test]
    async fn dependency_gate_retain_drops_rows_outside_the_candidate_set() {
        let pool = mem_pool().await;
        for (action, key) in [
            ("act", "AUD-31"),
            ("act", "TR-8"),
            ("act", "GONE-1"),
            ("other", "MK-16"),
        ] {
            dependency_gate_upsert(
                &pool,
                action,
                key,
                now_epoch_ms() + 300_000,
                "X-1 (depends on)",
            )
            .await
            .unwrap();
        }

        dependency_gate_retain(&pool, "act", &["AUD-31".to_string(), "TR-8".to_string()])
            .await
            .unwrap();
        assert!(dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .is_some());
        assert!(dependency_gate_get(&pool, "act", "TR-8")
            .await
            .unwrap()
            .is_some());
        assert!(
            dependency_gate_get(&pool, "act", "GONE-1")
                .await
                .unwrap()
                .is_none(),
            "a key the query no longer matches is pruned"
        );
        assert!(
            dependency_gate_get(&pool, "other", "MK-16")
                .await
                .unwrap()
                .is_some(),
            "another action's rows are not touched"
        );

        // A query that matched nothing clears the action's rows.
        dependency_gate_retain(&pool, "act", &[]).await.unwrap();
        assert!(dependency_gate_get(&pool, "act", "AUD-31")
            .await
            .unwrap()
            .is_none());
        assert!(dependency_gate_get(&pool, "other", "MK-16")
            .await
            .unwrap()
            .is_some());
    }

    /// CLAUDE-225: the dashboard/API read returns only gates held past the
    /// threshold, longest-held first, and reports how overdue each gate's own
    /// re-evaluation is. A gate whose blockers clear deletes its row, so the
    /// signal disappears without anyone acknowledging it.
    #[tokio::test]
    async fn dependency_gates_stale_reports_only_long_blocks_and_overdue_rechecks() {
        let pool = mem_pool().await;
        let now = now_epoch_ms();
        // Fresh block, still inside its backoff window: not stale.
        dependency_gate_upsert(&pool, "act", "MK-16", now + 300_000, "MK-1 (depends on)")
            .await
            .unwrap();
        // Blocked 2h with a re-check due 90m ago: stale AND overdue (the AUD-31
        // shape, where the promised re-evaluation stopped happening).
        dependency_gate_upsert(
            &pool,
            "act",
            "AUD-31",
            now - 5_400_000,
            "AUD-32 (depends on)",
        )
        .await
        .unwrap();
        // Blocked 26h but re-evaluated 1m ago: stale, re-check on schedule.
        dependency_gate_upsert(&pool, "other", "TR-8", now + 240_000, "TR-7 (depends on)")
            .await
            .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "UPDATE dependency_gate SET blocked_since = ? WHERE job_key = ?",
        ))
        .bind(now - 7_200_000)
        .bind("AUD-31")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "UPDATE dependency_gate SET blocked_since = ? WHERE job_key = ?",
        ))
        .bind(now - 93_600_000)
        .bind("TR-8")
        .execute(&pool)
        .await
        .unwrap();

        let stale = dependency_gates_stale(&pool, now, STALE_GATE_AFTER_MS, 20)
            .await
            .unwrap();
        let keys: Vec<&str> = stale.iter().map(|g| g.job_key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["TR-8", "AUD-31"],
            "only blocks past the threshold, longest-held first: {stale:?}"
        );
        assert_eq!(stale[0].action_name, "other");
        assert_eq!(stale[1].blocked_for_ms, 7_200_000);
        assert_eq!(
            stale[1].recheck_overdue_ms, 5_400_000,
            "AUD-31's re-check is 90m overdue"
        );
        assert!(
            stale[0].recheck_overdue_ms < 0,
            "TR-8 is long-blocked but its re-check is not due yet"
        );

        // The gate deletes the row when the blockers clear; the signal goes with it.
        dependency_gate_clear(&pool, "other", "TR-8").await.unwrap();
        dependency_gate_clear(&pool, "act", "AUD-31").await.unwrap();
        assert!(dependency_gates_stale(&pool, now, STALE_GATE_AFTER_MS, 20)
            .await
            .unwrap()
            .is_empty());
    }

    /// CLAUDE-232: the parked-at-cap read compares each key against ITS action's
    /// cap, not one global number, so an action that lowers `max_attempts_per_key`
    /// parks earlier and an action that raises it releases the key again.
    #[tokio::test]
    async fn attempt_cap_hits_reports_each_key_against_its_own_action_cap() {
        let pool = mem_pool().await;
        let caps = |pairs: &[(&str, u32)]| -> std::collections::BTreeMap<String, u32> {
            pairs.iter().map(|(a, c)| (a.to_string(), *c)).collect()
        };
        // `audit` caps at 2, `youtrack` takes the default 3, `webhook-forgejo`
        // has no YAML at all and so is absent from the map.
        for (action, key, n) in [
            ("audit", "AUD-31", 2),
            ("youtrack", "CLAUDE-9", 3),
            ("youtrack", "CLAUDE-8", 2),
            ("webhook-forgejo", "deadbeef", 4),
        ] {
            for _ in 0..n {
                attempts_increment(&pool, action, key).await.unwrap();
            }
        }

        let hits = attempt_cap_hits(&pool, &caps(&[("audit", 2)]), 3, ATTEMPT_CAP_HITS_LIMIT)
            .await
            .unwrap();
        let keys: Vec<&str> = hits.iter().map(|h| h.job_key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["deadbeef", "CLAUDE-9", "AUD-31"],
            "capped keys only, most attempts first: {hits:?}"
        );
        assert_eq!(hits[2].action_name, "audit");
        assert_eq!((hits[2].count, hits[2].cap), (2, 2), "the action's own cap");
        assert_eq!(
            (hits[0].count, hits[0].cap),
            (4, 3),
            "an action absent from the map is measured against the default cap"
        );
        assert!(!hits[0].last_at.is_empty(), "the last attempt is dated");
        assert!(
            !keys.contains(&"CLAUDE-8"),
            "one attempt left is not parked: {hits:?}"
        );

        // Raising the cap releases the key: the same counter is under the limit.
        let hits = attempt_cap_hits(
            &pool,
            &caps(&[("audit", 5), ("youtrack", 5)]),
            5,
            ATTEMPT_CAP_HITS_LIMIT,
        )
        .await
        .unwrap();
        assert!(hits.is_empty(), "nothing is at its cap any more: {hits:?}");

        // The limit bounds the banner, not the comparison.
        let hits = attempt_cap_hits(&pool, &caps(&[("audit", 2)]), 3, 1)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].job_key, "deadbeef");
    }
}
