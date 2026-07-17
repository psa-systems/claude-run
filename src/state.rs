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
    /// Canonical signature of the unmet-blocker set at the last evaluation;
    /// drives comment idempotency (re-comment only when it changes).
    pub signature: String,
}

/// Read the dependency-gate backoff row for `(action, key)`, if any.
pub async fn dependency_gate_get(
    pool: &SqlitePool,
    action: &str,
    key: &str,
) -> Result<Option<DependencyGateState>> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT blocked_until, signature FROM dependency_gate \
         WHERE action_name = ? AND job_key = ?",
    )
    .bind(action)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(blocked_until_ms, signature)| DependencyGateState {
            blocked_until_ms,
            signature,
        }),
    )
}

/// Record (or refresh) the dependency-gate backoff for `(action, key)`.
pub async fn dependency_gate_upsert(
    pool: &SqlitePool,
    action: &str,
    key: &str,
    blocked_until_ms: i64,
    signature: &str,
) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO dependency_gate (action_name, job_key, blocked_until, signature, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(action_name, job_key) DO UPDATE SET
            blocked_until = excluded.blocked_until,
            signature = excluded.signature,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(action)
    .bind(key)
    .bind(blocked_until_ms)
    .bind(signature)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop the dependency-gate row for `(action, key)` (its dependencies cleared).
pub async fn dependency_gate_clear(pool: &SqlitePool, action: &str, key: &str) -> Result<()> {
    sqlx::query("DELETE FROM dependency_gate WHERE action_name = ? AND job_key = ?")
        .bind(action)
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
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

/// CLAUDE-93: record per-job outbound links for a run as a JSON object mapping
/// job key to URL (`runs.job_links`). The webhook stores the failure
/// event's `html_url` so the run-detail tables can link the job key straight
/// to the Forgejo Actions run.
pub async fn run_set_job_links(
    pool: &SqlitePool,
    run_id: i64,
    links: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let payload = serde_json::to_string(links)?;
    sqlx::query("UPDATE runs SET job_links = ? WHERE id = ?")
        .bind(payload)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-93: the per-job link map for a run. Empty for legacy rows (NULL
/// column) and for runs that link via the action's `link_template` instead.
pub async fn run_job_links(
    pool: &SqlitePool,
    run_id: i64,
) -> Result<std::collections::BTreeMap<String, String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as("SELECT job_links FROM runs WHERE id = ?")
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
pub async fn run_set_current_job(pool: &SqlitePool, run_id: i64, key: Option<&str>) -> Result<()> {
    sqlx::query("UPDATE runs SET current_job_key = ? WHERE id = ?")
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
pub async fn run_stage_set(pool: &SqlitePool, run_id: i64, stage: Option<&str>) -> Result<()> {
    sqlx::query("UPDATE runs SET stage = ? WHERE id = ?")
        .bind(stage)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// CLAUDE-28: mark a job key as in flight for a run. Replaces the single
/// `run_set_current_job` marker so a run with several jobs running in parallel
/// badges all of them. Idempotent (`INSERT OR IGNORE`).
///
/// CLAUDE-163: also records the job's `concurrency_key` so the admission dedup
/// can drop gathered jobs whose concurrency key (project / repo) is already in
/// flight, not just those whose issue id is. For a path with one job per key
/// (webhook, forgejo-ci-fix) the concurrency key equals `key`.
pub async fn run_inflight_add(
    pool: &SqlitePool,
    run_id: i64,
    key: &str,
    concurrency_key: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO run_inflight_jobs (run_id, job_key, concurrency_key) VALUES (?, ?, ?)",
    )
    .bind(run_id)
    .bind(key)
    .bind(concurrency_key)
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

/// CLAUDE-171: a group of byte-identical `run_events` rows collapsed into one
/// summary row for the action-detail page.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct RunEventGroup {
    pub level: String,
    pub context: Option<String>,
    pub message: String,
    /// Occurrences of this exact `(level, context, message)` in the window.
    pub count: i64,
    pub first_ts: String,
    pub last_ts: String,
    /// Representative run: the newest attached run id in the group, or `None`
    /// when every occurrence was a zero-job poll with no run row.
    pub run_id: Option<i64>,
}

/// CLAUDE-147: persist this poll's input-phase events keyed by `action_name` +
/// `poll_ts`, with a NULL `run_id` (the run row does not exist yet, since
/// `gather()` runs before `run_begin`). [`run_events_attach`] backfills the run
/// id once it is created. Called before `run_begin` so the events survive even
/// if the run row is never created.
pub async fn run_events_record(
    pool: &SqlitePool,
    action: &str,
    poll_ts: &str,
    events: &[InputEvent],
) -> Result<()> {
    for e in events {
        sqlx::query(
            r#"
            INSERT INTO run_events (run_id, action_name, ts, level, context, message)
            VALUES (NULL, ?, ?, ?, ?, ?)
            "#,
        )
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

/// CLAUDE-147: backfill `run_id` onto this poll's still-unattached events once
/// `run_begin` has created the run row, so the run-detail / action pages can tie
/// them to the run. Matches on `action_name` + the poll `ts` used at record
/// time; a zero-job poll that never reaches here keeps its events with a NULL
/// `run_id`, and they still surface on the action page under that timestamp.
pub async fn run_events_attach(
    pool: &SqlitePool,
    run_id: i64,
    action: &str,
    poll_ts: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE run_events SET run_id = ? WHERE action_name = ? AND ts = ? AND run_id IS NULL",
    )
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
    pool: &SqlitePool,
    action: &str,
    limit: i64,
) -> Result<Vec<RunEventRecord>> {
    let rows = sqlx::query_as::<_, RunEventRecord>(
        r#"
        SELECT run_id, ts, level, context, message
        FROM run_events
        WHERE action_name = ?
        ORDER BY ts DESC, id DESC
        LIMIT ?
        "#,
    )
    .bind(action)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-171: the first `limit` DISTINCT input-phase diagnostics for an action
/// since `started_at`, for the action-detail "Input errors / warnings" summary.
///
/// Rows with an identical `(level, context, message)` collapse into one group
/// carrying its occurrence `count` and first/last-seen stamps, so a diagnostic
/// that re-fires every poll (the CLAUDE-164 config-collision notice, recorded
/// once per 5s poll) takes one row instead of burying every other diagnostic.
/// Groups are ordered by first occurrence ascending: the earliest signal in this
/// daemon lifetime is what the operator wants first, and it is stable as the
/// flood keeps growing.
///
/// `started_at` is the current serve's stamp ([`crate::settings::serve_started_at`]).
/// `None` means "no lower bound": every `ts` is a non-empty ISO string, so the
/// empty-string bind matches all rows. That is the degraded path for a DB no
/// serve has stamped yet, not the daemon's normal path.
pub async fn run_events_summary_for_action(
    pool: &SqlitePool,
    action: &str,
    started_at: Option<&str>,
    limit: i64,
) -> Result<Vec<RunEventGroup>> {
    let rows = sqlx::query_as::<_, RunEventGroup>(
        r#"
        SELECT level, context, message,
               COUNT(*) AS count,
               MIN(ts) AS first_ts,
               MAX(ts) AS last_ts,
               MAX(run_id) AS run_id
        FROM run_events
        WHERE action_name = ? AND ts >= ?
        GROUP BY level, context, message
        ORDER BY first_ts ASC
        LIMIT ?
        "#,
    )
    .bind(action)
    .bind(started_at.unwrap_or(""))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
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
    // also drops the stale "running" marker. CLAUDE-80: the live stage is
    // cleared the same way so it never outlives its run.
    sqlx::query(
        r#"
        UPDATE runs
        SET ended_at = ?, exit_code = ?, jobs_processed = ?, current_job_key = NULL, stage = NULL
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
    // CLAUDE-80: a dead process reports no stage; clear it with the end state.
    let result = sqlx::query(
        "UPDATE runs SET ended_at = ?, exit_code = ?, stage = NULL WHERE ended_at IS NULL",
    )
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
    /// CLAUDE-52: the job's claude run hit the wall-clock limit (exit 124) and
    /// was SIGKILLed. Distinct from a hard failure (`ok = 0`, `timed_out = 0`):
    /// the UI badges it separately and the failed-job count excludes it.
    pub timed_out: bool,
}

pub async fn job_outcome_insert(pool: &SqlitePool, r: &JobOutcome<'_>) -> Result<()> {
    let now = now_iso();
    sqlx::query(
        r#"
        INSERT INTO job_outcomes (
            run_id, action_name, job_key, attempt, ts, ok,
            claude_ran, claude_ok, claude_cost_usd, attempt_counted,
            summary, stream_log_path, duration_ms, timed_out
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    .bind(r.timed_out as i64)
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
    /// CLAUDE-162: the action's configured `concurrency.max_parallel`, cached in
    /// the index so the scheduler can resolve the `max_parallel_global` auto
    /// default without re-parsing every action's YAML each tick. Defaults to 1.
    #[sqlx(default)]
    pub max_parallel: i64,
}

pub async fn action_index_upsert(pool: &SqlitePool, row: &ActionIndexRow) -> Result<()> {
    sqlx::query(
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
    .bind(row.max_parallel)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn action_index_list(pool: &SqlitePool) -> Result<Vec<ActionIndexRow>> {
    let rows = sqlx::query_as::<_, ActionIndexRow>(
        "SELECT name, file_path, yaml_sha256, enabled, schedule_kind, schedule_spec, last_seen_at, parse_error, link_template, max_parallel FROM actions ORDER BY name",
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

/// CLAUDE-40: delete the `actions` index row for `name`. Returns `false` when
/// no row matched, mirroring [`secret_delete`]. Only touches the derived index;
/// the caller is responsible for the on-disk YAML and the runtime purge.
pub async fn action_index_delete(pool: &SqlitePool, name: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM actions WHERE name = ?")
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
pub async fn action_runtime_purge(pool: &SqlitePool, name: &str) -> Result<()> {
    sqlx::query("DELETE FROM attempt_counters WHERE action_name = ?")
        .bind(name)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM daily_ledger WHERE action_name = ?")
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
pub async fn action_purge_state(pool: &SqlitePool, name: &str) -> Result<bool> {
    let existed = action_index_delete(pool, name).await?;
    action_pause_clear(pool, name).await?;
    action_runtime_purge(pool, name).await?;
    Ok(existed)
}

/// CLAUDE-40: true when a run for `action` is still in flight in the DB (a
/// `runs` row with no `ended_at`). The delete flow refuses with a conflict when
/// this holds. The daemon additionally consults its in-memory in-flight set for
/// a job dispatched but not yet recorded as a run.
pub async fn action_has_active_run(pool: &SqlitePool, action: &str) -> Result<bool> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT id FROM runs WHERE action_name = ? AND ended_at IS NULL LIMIT 1")
            .bind(action)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// CLAUDE-162: the set of job keys currently in flight across every still-open
/// (not-yet-finished) run of `action`. A re-gather started while a prior run is
/// working an issue dedups its gathered jobs against this set, so an overlapping
/// pickup run never re-claims work another open run already holds. Joins
/// `run_inflight_jobs` (per-job markers, migration 0007) to the open `runs` rows.
pub async fn action_inflight_keys(pool: &SqlitePool, action: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT rij.job_key
        FROM run_inflight_jobs rij
        JOIN runs r ON r.id = rij.run_id
        WHERE r.action_name = ? AND r.ended_at IS NULL
        "#,
    )
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
pub async fn action_inflight_concurrency_keys(
    pool: &SqlitePool,
    action: &str,
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT rij.concurrency_key
        FROM run_inflight_jobs rij
        JOIN runs r ON r.id = rij.run_id
        WHERE r.action_name = ? AND r.ended_at IS NULL
          AND rij.concurrency_key IS NOT NULL
        "#,
    )
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
pub async fn secret_list(pool: &SqlitePool) -> Result<Vec<SecretMeta>> {
    let rows = sqlx::query_as::<_, SecretMeta>(
        "SELECT name, created_at, last_used_at FROM secrets ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// True when a secret with `name` already exists.
pub async fn secret_exists(pool: &SqlitePool, name: &str) -> Result<bool> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM secrets WHERE name = ?")
        .bind(name)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Create a new secret. Errors via the UNIQUE constraint when `name` is taken;
/// callers should check [`secret_exists`] first for a friendly message.
pub async fn secret_create(pool: &SqlitePool, name: &str, ciphertext: &[u8]) -> Result<()> {
    let now = now_iso();
    sqlx::query("INSERT INTO secrets (name, ciphertext, created_at) VALUES (?, ?, ?)")
        .bind(name)
        .bind(ciphertext)
        .bind(&now)
        .execute(pool)
        .await?;
    Ok(())
}

/// Rotate an existing secret's value in place (keeps `created_at`). Returns
/// `false` when no secret with `name` exists.
pub async fn secret_rotate(pool: &SqlitePool, name: &str, ciphertext: &[u8]) -> Result<bool> {
    let res = sqlx::query("UPDATE secrets SET ciphertext = ? WHERE name = ?")
        .bind(ciphertext)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Delete a secret. Returns `false` when no secret with `name` exists.
pub async fn secret_delete(pool: &SqlitePool, name: &str) -> Result<bool> {
    let res = sqlx::query("DELETE FROM secrets WHERE name = ?")
        .bind(name)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Fetch a secret's raw ciphertext for decryption at run time.
pub async fn secret_get_ciphertext(pool: &SqlitePool, name: &str) -> Result<Option<Vec<u8>>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT ciphertext FROM secrets WHERE name = ?")
        .bind(name)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(c,)| c))
}

/// Bump a secret's `last_used_at` to now (best-effort run-time audit).
pub async fn secret_touch(pool: &SqlitePool, name: &str) -> Result<()> {
    let now = now_iso();
    sqlx::query("UPDATE secrets SET last_used_at = ? WHERE name = ?")
        .bind(&now)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
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

    async fn stage_of(pool: &SqlitePool, run_id: i64) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT stage FROM runs WHERE id = ?")
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
        let flag: i64 =
            sqlx::query_scalar("SELECT timed_out FROM job_outcomes WHERE job_key = 'TIMEOUT-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(flag, 1);

        // The web failed-job count (ok = 0 AND timed_out = 0) sees only the
        // hard failure, not the timeout.
        let failed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = ? AND o.ok = 0 AND o.timed_out = 0",
        )
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

    async fn runs_count(pool: &SqlitePool, action: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs WHERE action_name = ?")
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
}
