//! Web UI served on the TCP listener. Phase 3.
//!
//! - Templates: Askama (compile-time checked).
//! - Interactivity: HTMX (vendored under `assets/htmx.min.js`, embedded into
//!   the binary via `include_bytes!` so the binary stays self-contained).
//! - Live log streaming: Server-Sent Events.
//! - Auth: localhost-only for now. Phase 5 layers a token check for
//!   non-loopback binds.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use askama::Template;
use axum::body::Body;
use axum::extract::{Form, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::api::AppState;
use crate::claude::ClaudeInvoker;
use crate::config::Paths;
use crate::state;

const HTMX_JS: &[u8] = include_bytes!("../../assets/htmx.min.js");

#[derive(Clone)]
pub struct WebState {
    pub api: Arc<AppState>,
    pub paths: Paths,
}

pub fn router(api: Arc<AppState>, paths: Paths) -> Router {
    let st = WebState { api, paths };
    Router::new()
        .route("/", get(dashboard))
        .route("/actions/new", get(action_new_page))
        .route("/actions", post(action_create))
        .route("/actions/:name", get(action_detail))
        .route("/actions/:name/durations", get(action_durations))
        .route(
            "/actions/:name/edit",
            get(action_edit_page).post(action_edit_save),
        )
        .route("/actions/:name/export", get(action_export))
        .route("/runs/:id", get(run_detail))
        .route("/runs/:id/rows/:key/attempt/:attempt", get(row_stream_page))
        .route(
            "/sse/runs/:id/rows/:key/attempt/:attempt",
            get(row_stream_sse),
        )
        .route("/static/htmx.min.js", get(htmx_handler))
        .with_state(st)
}

// ---- helpers --------------------------------------------------------------

#[derive(Serialize)]
struct PauseInfo {
    paused_at: String,
    reason: Option<String>,
}

#[derive(Serialize)]
struct ActionView {
    name: String,
    file_path: String,
    yaml_sha256: String,
    enabled: bool,
    schedule_kind: Option<String>,
    schedule_spec: Option<String>,
    last_seen_at: String,
    parse_error: Option<String>,
    last_run_started: Option<String>,
    paused: Option<PauseInfo>,
}

#[derive(Serialize, sqlx::FromRow)]
struct RunRow {
    id: i64,
    action_name: String,
    started_at: String,
    ended_at: Option<String>,
    exit_code: Option<i64>,
    rows_processed: i64,
    triggered_by: String,
    /// CLAUDE-14: key of the row currently being processed, or NULL when no row
    /// is in flight (run finished, or a legacy run predating the column).
    current_row_key: Option<String>,
    /// CLAUDE-22: count of distinct rows whose LATEST attempt failed (`ok = 0`),
    /// matching the per-row `fail` badges on run-detail. A row that failed an
    /// earlier attempt but passed its latest does not count.
    failed_rows: i64,
}

impl RunRow {
    /// CLAUDE-20: human-readable run duration. A finished or cancelled run shows
    /// the final span (`ended_at - started_at`); an in-flight run (NULL
    /// `ended_at`) shows elapsed-so-far against now, which advances on each poll
    /// refresh. Returns "-" if a timestamp fails to parse.
    fn duration(&self) -> String {
        let Ok(start) = chrono::DateTime::parse_from_rfc3339(&self.started_at) else {
            return "-".to_string();
        };
        let end_ms = match self.ended_at.as_deref() {
            Some(e) => match chrono::DateTime::parse_from_rfc3339(e) {
                Ok(end) => end.timestamp_millis(),
                Err(_) => return "-".to_string(),
            },
            None => chrono::Utc::now().timestamp_millis(),
        };
        format_duration_ms(end_ms - start.timestamp_millis())
    }

    /// CLAUDE-21: completed-run span in milliseconds for the duration chart, or
    /// `None` when the run is in flight (NULL `ended_at`) or either timestamp
    /// fails to parse. Unlike [`RunRow::duration`] this never charts a moving
    /// value: an unfinished run is excluded rather than measured against now.
    fn duration_ms(&self) -> Option<i64> {
        let start = chrono::DateTime::parse_from_rfc3339(&self.started_at).ok()?;
        let end = chrono::DateTime::parse_from_rfc3339(self.ended_at.as_deref()?).ok()?;
        Some((end.timestamp_millis() - start.timestamp_millis()).max(0))
    }
}

#[derive(Serialize, sqlx::FromRow)]
struct OutcomeRow {
    row_key: String,
    attempt: i64,
    ts: String,
    ok: bool,
    claude_ran: bool,
    claude_ok: bool,
    claude_cost_usd: f64,
    attempt_counted: bool,
    /// CLAUDE-20: full per-row pipeline elapsed; NULL for rows predating the
    /// column (migration 0005).
    duration_ms: Option<i64>,
}

impl OutcomeRow {
    /// CLAUDE-20: human-readable per-row duration, or "-" for rows predating the
    /// `duration_ms` column.
    fn duration(&self) -> String {
        match self.duration_ms {
            Some(ms) => format_duration_ms(ms),
            None => "-".to_string(),
        }
    }
}

/// CLAUDE-20: render a millisecond span human-readably (`820ms`, `4s`,
/// `1m 17s`, `2h 3m`) rather than as raw milliseconds. Shared by the run-span
/// and per-row duration columns. Negative inputs clamp to zero.
fn format_duration_ms(ms: i64) -> String {
    let ms = ms.max(0);
    if ms < 1000 {
        return format!("{ms}ms");
    }
    let total_secs = ms / 1000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{hours}h {mins}m")
    } else if mins > 0 {
        format!("{mins}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

// ---- duration chart (CLAUDE-21) -------------------------------------------

/// SVG canvas and plot-area geometry for the per-action duration chart. The
/// plot rectangle is inset by the padding constants to leave room for axis
/// labels.
const CHART_W: i64 = 720;
const CHART_H: i64 = 240;
const CHART_PAD_L: i64 = 64;
const CHART_PAD_R: i64 = 24;
const CHART_PAD_T: i64 = 16;
const CHART_PAD_B: i64 = 32;

/// One plotted run: pixel position plus a `<title>` tooltip string.
#[derive(Serialize)]
struct ChartPoint {
    x: i64,
    y: i64,
    label: String,
}

/// A y-axis tick: a horizontal gridline at `y` labelled with a duration.
#[derive(Serialize)]
struct YTick {
    y: i64,
    label: String,
}

/// Precomputed SVG geometry for the duration chart. Everything the template
/// needs is resolved to integer pixel coordinates here so the `<svg>` markup
/// stays declarative. Built by [`build_duration_chart`] from the completed-run
/// series (oldest to newest); never constructed for an empty series (the
/// handler renders a placeholder instead).
#[derive(Serialize)]
struct DurationChart {
    width: i64,
    height: i64,
    plot_l: i64,
    plot_r: i64,
    plot_t: i64,
    plot_b: i64,
    /// `<polyline>` points, e.g. `"64,208 200,120 ..."`, oldest to newest.
    polyline: String,
    points: Vec<ChartPoint>,
    y_ticks: Vec<YTick>,
    /// Start timestamp of the oldest / newest charted run, for the x-axis ends.
    x_start: String,
    x_end: String,
    /// Number of completed runs plotted.
    count: usize,
}

/// Compute the duration-chart geometry from a `(started_at, duration_ms)`
/// series ordered oldest to newest. Pure function of its input so it is
/// unit-testable without a database. Caller guarantees `series` is non-empty.
fn build_duration_chart(series: &[(String, i64)]) -> DurationChart {
    let count = series.len();
    let plot_l = CHART_PAD_L;
    let plot_r = CHART_W - CHART_PAD_R;
    let plot_t = CHART_PAD_T;
    let plot_b = CHART_H - CHART_PAD_B;
    let plot_w = plot_r - plot_l;
    let plot_h = plot_b - plot_t;

    // y axis tops out at the longest run, floored to 1ms so the scale never
    // divides by zero (a series of all-zero durations still renders flat).
    let max_ms = series.iter().map(|(_, ms)| *ms).max().unwrap_or(0).max(1);

    let x_at = |i: usize| -> i64 {
        if count <= 1 {
            plot_l + plot_w / 2
        } else {
            plot_l + (i as i64) * plot_w / (count as i64 - 1)
        }
    };
    let y_at = |ms: i64| -> i64 { plot_b - (ms * plot_h) / max_ms };

    let mut points = Vec::with_capacity(count);
    let mut coords = Vec::with_capacity(count);
    for (i, (started, ms)) in series.iter().enumerate() {
        let x = x_at(i);
        let y = y_at(*ms);
        coords.push(format!("{x},{y}"));
        points.push(ChartPoint {
            x,
            y,
            label: format!("{} at {started}", format_duration_ms(*ms)),
        });
    }

    let y_ticks = vec![
        YTick {
            y: y_at(0),
            label: format_duration_ms(0),
        },
        YTick {
            y: y_at(max_ms / 2),
            label: format_duration_ms(max_ms / 2),
        },
        YTick {
            y: y_at(max_ms),
            label: format_duration_ms(max_ms),
        },
    ];

    DurationChart {
        width: CHART_W,
        height: CHART_H,
        plot_l,
        plot_r,
        plot_t,
        plot_b,
        polyline: coords.join(" "),
        points,
        y_ticks,
        x_start: series.first().map(|(s, _)| s.clone()).unwrap_or_default(),
        x_end: series.last().map(|(s, _)| s.clone()).unwrap_or_default(),
        count,
    }
}

async fn enriched_actions(pool: &SqlitePool) -> Result<Vec<ActionView>, WebError> {
    let rows = state::action_index_list(pool).await?;
    let mut out = Vec::with_capacity(rows.len());
    for a in rows {
        let paused = state::action_pause_status(pool, &a.name)
            .await?
            .map(|(at, r)| PauseInfo {
                paused_at: at,
                reason: r,
            });
        let last = state::action_last_run_started(pool, &a.name).await?;
        out.push(ActionView {
            name: a.name,
            file_path: a.file_path,
            yaml_sha256: a.yaml_sha256,
            enabled: a.enabled,
            schedule_kind: a.schedule_kind,
            schedule_spec: a.schedule_spec,
            last_seen_at: a.last_seen_at,
            parse_error: a.parse_error,
            last_run_started: last,
            paused,
        });
    }
    Ok(out)
}

async fn fetch_recent_runs(pool: &SqlitePool, limit: i64) -> Result<Vec<RunRow>, WebError> {
    let rows = sqlx::query_as::<_, RunRow>(
        "SELECT id, action_name, started_at, ended_at, exit_code, rows_processed, triggered_by, current_row_key, (SELECT COUNT(*) FROM row_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM row_outcomes o2 WHERE o2.run_id = o.run_id AND o2.row_key = o.row_key)) AS failed_rows FROM runs ORDER BY started_at DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---- templates ------------------------------------------------------------

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTpl {
    version: &'static str,
    global_pause: Option<PauseInfo>,
    today_runs: u32,
    today_actions: usize,
    today_cost: f64,
    actions: Vec<ActionView>,
    recent_runs: Vec<RunRow>,
}

async fn dashboard(State(s): State<WebState>) -> Result<Html<String>, WebError> {
    let pool = &s.api.pool;
    let global_pause = state::global_pause_status(pool)
        .await?
        .map(|(a, r)| PauseInfo {
            paused_at: a,
            reason: r,
        });
    let actions = enriched_actions(pool).await?;
    let recent_runs = fetch_recent_runs(pool, 20).await?;

    // Today totals: sum across all actions
    let today = state::today_local().to_string();
    let totals: Option<(i64, f64)> = sqlx::query_as(
        "SELECT IFNULL(SUM(run_count), 0), IFNULL(SUM(cost_usd), 0.0) FROM daily_ledger WHERE date = ?",
    )
    .bind(&today)
    .fetch_optional(pool)
    .await?;
    let today_actions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM daily_ledger WHERE date = ? AND run_count > 0")
            .bind(&today)
            .fetch_one(pool)
            .await
            .unwrap_or(0);

    let (today_runs, today_cost) = totals.unwrap_or((0, 0.0));
    let tpl = DashboardTpl {
        version: s.api.version,
        global_pause,
        today_runs: today_runs as u32,
        today_actions: today_actions as usize,
        today_cost,
        actions,
        recent_runs,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

#[derive(Template)]
#[template(path = "action_detail.html")]
struct ActionDetailTpl {
    version: &'static str,
    action: ActionView,
    paused: Option<PauseInfo>,
    runs: Vec<RunRow>,
}

async fn action_detail(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    let actions = enriched_actions(pool).await?;
    let Some(action) = actions.into_iter().find(|a| a.name == name) else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let paused = state::action_pause_status(pool, &name)
        .await?
        .map(|(at, r)| PauseInfo {
            paused_at: at,
            reason: r,
        });
    let runs = sqlx::query_as::<_, RunRow>(
        "SELECT id, action_name, started_at, ended_at, exit_code, rows_processed, triggered_by, current_row_key, (SELECT COUNT(*) FROM row_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM row_outcomes o2 WHERE o2.run_id = o.run_id AND o2.row_key = o.row_key)) AS failed_rows FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 50",
    )
    .bind(&name)
    .fetch_all(pool)
    .await?;
    let tpl = ActionDetailTpl {
        version: s.api.version,
        action,
        paused,
        runs,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

// ---- duration chart view (CLAUDE-21) --------------------------------------

#[derive(Template)]
#[template(path = "action_chart.html")]
struct ActionChartTpl {
    version: &'static str,
    action_name: String,
    /// `None` renders the empty-state placeholder (no completed runs).
    chart: Option<DurationChart>,
}

async fn action_durations(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    // 404 unknown actions cleanly; only names the daemon has indexed resolve.
    if indexed_action_path(pool, &name).await?.is_none() {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    }
    // Same recent window as the action-detail table (last 50 by start). The
    // query returns newest-first; the chart wants oldest-first, so reverse.
    let runs = sqlx::query_as::<_, RunRow>(
        "SELECT id, action_name, started_at, ended_at, exit_code, rows_processed, triggered_by, current_row_key, (SELECT COUNT(*) FROM row_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM row_outcomes o2 WHERE o2.run_id = o.run_id AND o2.row_key = o.row_key)) AS failed_rows FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 50",
    )
    .bind(&name)
    .fetch_all(pool)
    .await?;
    // Completed runs only: in-flight runs (NULL ended_at) are dropped here so
    // the chart never plots a moving value.
    let series: Vec<(String, i64)> = runs
        .iter()
        .rev()
        .filter_map(|r| r.duration_ms().map(|ms| (r.started_at.clone(), ms)))
        .collect();
    let chart = (!series.is_empty()).then(|| build_duration_chart(&series));
    let tpl = ActionChartTpl {
        version: s.api.version,
        action_name: name,
        chart,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

// ---- action editor / import / export (CLAUDE-7) ---------------------------

#[derive(Template)]
#[template(path = "action_editor.html")]
struct ActionEditorTpl {
    version: &'static str,
    heading: String,
    /// URL the form POSTs to.
    post_url: String,
    /// YAML to prefill the textarea (current file, submitted draft, or skeleton).
    yaml: String,
    /// Inline validation error, rendered above the editor when set.
    error: Option<String>,
    /// Import (create) mode vs editing an existing file.
    is_new: bool,
    /// Existing action name; drives the Cancel / Export links.
    name: Option<String>,
}

#[derive(Deserialize)]
struct ActionForm {
    yaml: String,
}

/// Skeleton prefilled into the new-action editor. Validates as-is (name /
/// input.command / prompt are all non-empty) and ships disabled so an import
/// never starts running before the author reviews it.
const NEW_ACTION_TEMPLATE: &str = "name: my-action\n\
input:\n\
\x20\x20command: |\n\
\x20\x20\x20\x20# nu: print a JSON array of rows to stdout\n\
\x20\x20\x20\x20print '[]'\n\
prompt: |\n\
\x20\x20# nu: print the Claude prompt for the current row to stdout\n\
\x20\x20print \"Describe the task for this row.\"\n\
enabled: false\n";

/// Resolve an indexed action's on-disk path by exact name. Only names the
/// daemon has scanned (always under `actions_dir`) resolve, so a hostile
/// `:name` such as `../../etc/passwd` can never reach the filesystem here.
async fn indexed_action_path(pool: &SqlitePool, name: &str) -> Result<Option<String>, WebError> {
    let rows = state::action_index_list(pool).await?;
    Ok(rows
        .into_iter()
        .find(|r| r.name == name)
        .map(|r| r.file_path))
}

/// Constrain an action name to a single safe path component for use as a
/// `<stem>.yml` filename. Rejects empty, `.`/`..`, path separators, and any
/// character outside `[A-Za-z0-9._-]`.
fn safe_action_stem(name: &str) -> Option<String> {
    let n = name.trim();
    if n.is_empty() || n == "." || n == ".." {
        return None;
    }
    if !n
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return None;
    }
    Some(n.to_string())
}

/// Re-render the editor with an inline error (HTTP 422), preserving the draft.
fn editor_error(
    version: &'static str,
    is_new: bool,
    name: Option<String>,
    yaml: String,
    message: String,
) -> Result<Response, WebError> {
    let (heading, post_url) = match &name {
        Some(n) if !is_new => (format!("Edit {n}"), format!("/actions/{n}/edit")),
        _ => ("New action".to_string(), "/actions".to_string()),
    };
    let tpl = ActionEditorTpl {
        version,
        heading,
        post_url,
        yaml,
        error: Some(message),
        is_new,
        name,
    };
    Ok((
        StatusCode::UNPROCESSABLE_ENTITY,
        Html(tpl.render().map_err(WebError::from)?),
    )
        .into_response())
}

async fn action_edit_page(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let yaml = std::fs::read_to_string(&path).map_err(WebError::from)?;
    let tpl = ActionEditorTpl {
        version: s.api.version,
        heading: format!("Edit {name}"),
        post_url: format!("/actions/{name}/edit"),
        yaml,
        error: None,
        is_new: false,
        name: Some(name),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

async fn action_edit_save(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
    Form(form): Form<ActionForm>,
) -> Result<Response, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    // Validate through the same checks the loader/scanner uses. A broken
    // candidate is rejected inline and the on-disk file is left untouched.
    if let Err(e) = crate::action::validate_str(&form.yaml, &path) {
        return editor_error(s.api.version, false, Some(name), form.yaml, e.to_string());
    }
    std::fs::write(&path, form.yaml.as_bytes()).map_err(WebError::from)?;
    // The daemon's 5s directory re-scan picks up the new sha256; no restart.
    Ok(Redirect::to(&format!("/actions/{name}")).into_response())
}

async fn action_new_page(State(s): State<WebState>) -> Result<Html<String>, WebError> {
    let tpl = ActionEditorTpl {
        version: s.api.version,
        heading: "New action".into(),
        post_url: "/actions".into(),
        yaml: NEW_ACTION_TEMPLATE.into(),
        error: None,
        is_new: true,
        name: None,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

async fn action_create(
    State(s): State<WebState>,
    Form(form): Form<ActionForm>,
) -> Result<Response, WebError> {
    let loaded = match crate::action::validate_str(&form.yaml, "<import>") {
        Ok(l) => l,
        Err(e) => return editor_error(s.api.version, true, None, form.yaml, e.to_string()),
    };
    let Some(stem) = safe_action_stem(&loaded.action.name) else {
        let msg = format!(
            "action name `{}` is not a safe filename (allowed: letters, digits, '-', '_', '.')",
            loaded.action.name
        );
        return editor_error(s.api.version, true, None, form.yaml, msg);
    };
    let actions_dir = s.paths.actions_dir();
    let dest = actions_dir.join(format!("{stem}.yml"));
    if dest.exists() {
        let msg = format!("`{stem}.yml` already exists; edit it instead of importing a duplicate");
        return editor_error(s.api.version, true, None, form.yaml, msg);
    }
    std::fs::create_dir_all(&actions_dir).map_err(WebError::from)?;
    std::fs::write(&dest, form.yaml.as_bytes()).map_err(WebError::from)?;
    Ok(Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response())
}

async fn action_export(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let body = std::fs::read_to_string(&path).map_err(WebError::from)?;
    let mut resp = Response::new(Body::from(body));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-yaml"),
    );
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}.yml\"")) {
        resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(resp)
}

#[derive(Serialize)]
pub struct PlannedRow {
    pub key: String,
    pub href: Option<String>,
    /// One of:
    /// - "ok" / "fail": an outcome record landed.
    /// - "running": no outcome, run still active, and this is the one row the
    ///   run is processing (`runs.current_row_key`). Carries `watch_attempt`.
    /// - "queued": no outcome, run still active, not the in-flight row.
    /// - "skipped": no outcome and the run has ended (planned but never
    ///   completed). Replaces the stale "running" badge.
    pub status: String,
    /// Attempt number for the "watch live" link, set only for the "running"
    /// row. Resolved from the highest `<key>-attempt-<n>.jsonl` stream log on
    /// disk (`None` until claude opens the log for the row's first attempt).
    pub watch_attempt: Option<u32>,
}

/// Resolve the highest attempt number with a stream log on disk for `key`.
/// Stream logs are written live by `ClaudeInvoker::invoke` to
/// `<logs>/<action>/streams/<key>-attempt-<n>.jsonl`, so the presence of one
/// without a matching `row_outcomes` record means that row is in flight. This
/// is the on-disk fallback for the CLAUDE-9 per-run in-flight registry.
fn latest_attempt_on_disk(logs_dir: &std::path::Path, action: &str, key: &str) -> Option<u32> {
    let dir = logs_dir.join(action).join("streams");
    let prefix = format!("{key}-attempt-");
    let mut best: Option<u32> = None;
    for entry in std::fs::read_dir(&dir).ok()?.flatten() {
        let fname = entry.file_name();
        let Some(fname) = fname.to_str() else {
            continue;
        };
        let Some(rest) = fname.strip_prefix(&prefix) else {
            continue;
        };
        let Some(num) = rest.strip_suffix(".jsonl") else {
            continue;
        };
        let Ok(n) = num.parse::<u32>() else {
            continue;
        };
        best = Some(best.map_or(n, |b| b.max(n)));
    }
    best
}

#[derive(Template)]
#[template(path = "run_detail.html")]
struct RunDetailTpl {
    version: &'static str,
    run: RunRow,
    outcomes: Vec<OutcomeRow>,
    /// CLAUDE-1: rows planned at run-start; populated as soon as
    /// `input.command` returns. Pre-existing rows have NULL `run.row_keys`
    /// and this stays empty.
    planned_rows: Vec<PlannedRow>,
    /// Per-row href builder for the outcomes table. Empty string when the
    /// action declares no `link_template`.
    link_template: Option<String>,
}

async fn run_detail(
    State(s): State<WebState>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    let Some(run): Option<RunRow> = sqlx::query_as::<_, RunRow>(
        "SELECT id, action_name, started_at, ended_at, exit_code, rows_processed, triggered_by, current_row_key, (SELECT COUNT(*) FROM row_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM row_outcomes o2 WHERE o2.run_id = o.run_id AND o2.row_key = o.row_key)) AS failed_rows FROM runs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await? else {
        return Ok((StatusCode::NOT_FOUND, format!("no run #{id}")).into_response());
    };
    let outcomes = sqlx::query_as::<_, OutcomeRow>(
        "SELECT row_key, attempt, ts, ok, claude_ran, claude_ok, claude_cost_usd, attempt_counted, duration_ms FROM row_outcomes WHERE run_id = ? ORDER BY id ASC",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;

    // Look up the action's link_template once and reuse for both planned
    // rows and the outcomes table.
    let link_template: Option<String> =
        sqlx::query_scalar("SELECT link_template FROM actions WHERE name = ?")
            .bind(&run.action_name)
            .fetch_optional(pool)
            .await?
            .flatten();

    let planned_keys = state::run_row_keys(pool, run.id).await.unwrap_or_default();
    // Build a quick lookup: key -> overall ok flag from row_outcomes (latest
    // attempt wins, since outcomes are inserted in order).
    let mut outcome_by_key: std::collections::HashMap<&str, bool> =
        std::collections::HashMap::new();
    for o in &outcomes {
        outcome_by_key.insert(o.row_key.as_str(), o.ok);
    }
    // CLAUDE-14: a run with a NULL `ended_at` is still active; only then can a
    // row be in flight, and exactly the one matching `runs.current_row_key` is.
    // Rows are processed strictly sequentially, so every other no-outcome row is
    // queued, not running. Once the run ends, `current_row_key` is NULL and a
    // no-outcome row never finished (filtered / capped / skipped / not reached),
    // so it must not keep the "running" badge.
    let run_active = run.ended_at.is_none();
    let current_row = run.current_row_key.as_deref();
    let logs_dir = s.paths.logs_dir();
    let planned_rows: Vec<PlannedRow> = planned_keys
        .into_iter()
        .map(|k| {
            let (status, watch_attempt) = match outcome_by_key.get(k.as_str()) {
                Some(true) => ("ok".to_string(), None),
                Some(false) => ("fail".to_string(), None),
                None if run_active && current_row == Some(k.as_str()) => {
                    // The single in-flight row. Resolve its live-stream attempt
                    // from disk for the "watch live" link.
                    let attempt = latest_attempt_on_disk(&logs_dir, &run.action_name, &k);
                    ("running".to_string(), attempt)
                }
                None if run_active => ("queued".to_string(), None),
                None => ("skipped".to_string(), None),
            };
            let href = link_template
                .as_ref()
                .map(|t| crate::action::render_link(t, &k));
            PlannedRow {
                key: k,
                href,
                status,
                watch_attempt,
            }
        })
        .collect();

    let tpl = RunDetailTpl {
        version: s.api.version,
        run,
        outcomes,
        planned_rows,
        link_template,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

#[derive(Template)]
#[template(path = "row_stream.html")]
struct RowStreamTpl {
    version: &'static str,
    run_id: i64,
    /// Parent action, resolved alongside the stream-log path. Feeds the
    /// breadcrumb's link back to the action (CLAUDE-19).
    action_name: String,
    row_key: String,
    attempt: u32,
    stream_path: String,
}

async fn row_stream_page(
    State(s): State<WebState>,
    AxumPath((id, key, attempt)): AxumPath<(i64, String, u32)>,
) -> Result<Html<String>, WebError> {
    // Resolve the run's action so we can compute the stream-log path.
    let action_name: Option<String> =
        sqlx::query_scalar("SELECT action_name FROM runs WHERE id = ?")
            .bind(id)
            .fetch_optional(&s.api.pool)
            .await?;
    let action_name = action_name.ok_or_else(|| WebError::not_found(format!("no run #{id}")))?;
    let stream = ClaudeInvoker::stream_log_path(&s.paths.logs_dir(), &action_name, &key, attempt);
    let tpl = RowStreamTpl {
        version: s.api.version,
        run_id: id,
        action_name,
        row_key: key,
        attempt,
        stream_path: stream.display().to_string(),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

// ---- SSE ------------------------------------------------------------------

async fn row_stream_sse(
    State(s): State<WebState>,
    AxumPath((id, key, attempt)): AxumPath<(i64, String, u32)>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, WebError> {
    let action_name: Option<String> =
        sqlx::query_scalar("SELECT action_name FROM runs WHERE id = ?")
            .bind(id)
            .fetch_optional(&s.api.pool)
            .await?;
    let action_name = action_name.ok_or_else(|| WebError::not_found(format!("no run #{id}")))?;
    let path = ClaudeInvoker::stream_log_path(&s.paths.logs_dir(), &action_name, &key, attempt);

    // Resume offset for reconnects. Each `msg` event is tagged with its 0-based
    // line index as the SSE `id`. On a native browser reconnect the index comes
    // back in the `Last-Event-ID` header; a manual reconnect (the client gave up
    // because `readyState` reached CLOSED) carries it as a `lastEventId` query
    // param instead, since a freshly created `EventSource` cannot set headers.
    // We resume from the line *after* the last one the client saw (N + 1).
    let resume_from: u64 = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| params.get("lastEventId").cloned())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|n| n + 1)
        .unwrap_or(0);

    // Stream the file line-by-line. If the run is still writing, keep
    // polling until either we see a `"type":"result"` line (which closes
    // the stream cleanly with a `done` SSE event) or the row's outcome
    // record lands in `row_outcomes` (run has moved on past this attempt).
    let pool = s.api.pool.clone();
    let stream = async_stream::stream! {
        // Wait briefly for the file to exist (claude may be spinning up).
        let mut waited = 0u32;
        while !path.exists() && waited < 20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            waited += 1;
        }
        if !path.exists() {
            yield Ok(Event::default().event("msg").data(
                serde_json::json!({"type": "system", "subtype": "no-stream-yet"}).to_string()
            ));
            yield Ok(Event::default().event("done").data(""));
            return;
        }

        let file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                yield Ok(Event::default().event("msg").data(
                    serde_json::json!({"type": "system", "error": format!("open failed: {e}")}).to_string()
                ));
                yield Ok(Event::default().event("done").data(""));
                return;
            }
        };
        let mut reader = BufReader::new(file).lines();

        // 0-based index of every line read from the log. Becomes the SSE event
        // `id` so a reconnect can resume past lines it already rendered. Counted
        // across the whole file (not just the lines we emit) so the id stays a
        // stable line number even when we skip the already-sent prefix.
        let mut idx: u64 = 0;
        loop {
            match reader.next_line().await {
                Ok(Some(line)) => {
                    let is_terminal = line.contains("\"type\":\"result\"");
                    if idx >= resume_from {
                        yield Ok(Event::default().event("msg").id(idx.to_string()).data(line));
                    }
                    idx += 1;
                    if is_terminal {
                        yield Ok(Event::default().event("done").data(""));
                        return;
                    }
                }
                Ok(None) => {
                    // EOF: either the run is still writing more, or it's done.
                    // Check the DB to decide whether to keep waiting.
                    let done: Option<i64> = sqlx::query_scalar(
                        "SELECT id FROM row_outcomes WHERE run_id = ? AND row_key = ? AND attempt = ?",
                    )
                    .bind(id)
                    .bind(&key)
                    .bind(attempt as i64)
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
                    if done.is_some() {
                        yield Ok(Event::default().event("done").data(""));
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    yield Ok(Event::default().event("msg").data(
                        serde_json::json!({"type": "system", "error": format!("read failed: {e}")}).to_string()
                    ));
                    yield Ok(Event::default().event("done").data(""));
                    return;
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

// ---- static htmx ----------------------------------------------------------

async fn htmx_handler() -> Response {
    let mut resp = Response::new(Body::from(HTMX_JS));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/javascript"),
    );
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    resp
}

// ---- error ----------------------------------------------------------------

pub struct WebError {
    status: StatusCode,
    message: String,
}

impl WebError {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<crate::error::Error> for WebError {
    fn from(e: crate::error::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl From<sqlx::Error> for WebError {
    fn from(e: sqlx::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl From<std::io::Error> for WebError {
    fn from(e: std::io::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl From<askama::Error> for WebError {
    fn from(e: askama::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{build_duration_chart, format_duration_ms, latest_attempt_on_disk};

    #[test]
    fn format_duration_ms_buckets() {
        assert_eq!(format_duration_ms(-5), "0ms");
        assert_eq!(format_duration_ms(0), "0ms");
        assert_eq!(format_duration_ms(820), "820ms");
        assert_eq!(format_duration_ms(999), "999ms");
        assert_eq!(format_duration_ms(1_000), "1s");
        assert_eq!(format_duration_ms(4_500), "4s");
        assert_eq!(format_duration_ms(77_000), "1m 17s");
        assert_eq!(format_duration_ms(3_600_000), "1h 0m");
        assert_eq!(format_duration_ms(7_380_000), "2h 3m");
    }

    fn touch(path: &std::path::Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"").unwrap();
    }

    #[test]
    fn latest_attempt_picks_highest_and_ignores_other_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path();
        let streams = logs.join("act").join("streams");
        touch(&streams.join("CLAUDE-9-attempt-1.jsonl"));
        touch(&streams.join("CLAUDE-9-attempt-2.jsonl"));
        // A longer key that shares the prefix must not leak in.
        touch(&streams.join("CLAUDE-90-attempt-5.jsonl"));
        // Unrelated files are skipped.
        touch(&streams.join("CLAUDE-9-attempt-bogus.jsonl"));
        touch(&streams.join("CLAUDE-9.txt"));

        assert_eq!(latest_attempt_on_disk(logs, "act", "CLAUDE-9"), Some(2));
        assert_eq!(latest_attempt_on_disk(logs, "act", "CLAUDE-90"), Some(5));
    }

    #[test]
    fn chart_scales_to_plot_area_and_orders_points() {
        let series = vec![
            ("2026-05-26T10:00:00+00:00".to_string(), 0),
            ("2026-05-26T11:00:00+00:00".to_string(), 5_000),
            ("2026-05-26T12:00:00+00:00".to_string(), 10_000),
        ];
        let c = build_duration_chart(&series);
        assert_eq!(c.count, 3);
        // Three points, evenly spaced across the plot width. First sits on the
        // left edge, last on the right edge.
        assert_eq!(c.points.len(), 3);
        assert_eq!(c.points[0].x, c.plot_l);
        assert_eq!(c.points[2].x, c.plot_r);
        // y inverts: the longest run (max) sits at the top, a zero run on the
        // baseline, the midpoint halfway between.
        assert_eq!(c.points[0].y, c.plot_b);
        assert_eq!(c.points[2].y, c.plot_t);
        assert_eq!(c.points[1].y, (c.plot_t + c.plot_b) / 2);
        // Tooltip carries a human duration and the run start.
        assert_eq!(c.points[2].label, "10s at 2026-05-26T12:00:00+00:00");
        // Polyline is the space-joined "x,y" coordinate list.
        assert_eq!(
            c.polyline,
            format!(
                "{},{} {},{} {},{}",
                c.points[0].x,
                c.points[0].y,
                c.points[1].x,
                c.points[1].y,
                c.points[2].x,
                c.points[2].y
            )
        );
        // Axis ends label the oldest and newest run start.
        assert_eq!(c.x_start, "2026-05-26T10:00:00+00:00");
        assert_eq!(c.x_end, "2026-05-26T12:00:00+00:00");
    }

    #[test]
    fn chart_single_point_centers_and_flat_series_does_not_divide_by_zero() {
        // One run: centered horizontally, no panic.
        let one = vec![("2026-05-26T10:00:00+00:00".to_string(), 1_234)];
        let c = build_duration_chart(&one);
        assert_eq!(c.points.len(), 1);
        assert_eq!(c.points[0].x, (c.plot_l + c.plot_r) / 2);

        // All-zero durations: max_ms floors to 1 so the scale never divides by
        // zero; every point lands on the baseline.
        let flat = vec![
            ("2026-05-26T10:00:00+00:00".to_string(), 0),
            ("2026-05-26T11:00:00+00:00".to_string(), 0),
        ];
        let c = build_duration_chart(&flat);
        assert!(c.points.iter().all(|p| p.y == c.plot_b));
    }

    #[test]
    fn latest_attempt_none_when_no_stream_dir_or_no_match() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path();
        // No streams dir at all.
        assert_eq!(latest_attempt_on_disk(logs, "act", "CLAUDE-9"), None);
        // Dir exists but holds no matching file.
        touch(
            &logs
                .join("act")
                .join("streams")
                .join("other-attempt-1.jsonl"),
        );
        assert_eq!(latest_attempt_on_disk(logs, "act", "CLAUDE-9"), None);
    }
}
