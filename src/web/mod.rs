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

use crate::action::{
    Action, ClaudeBlock, ForgejoTarget, Limits, NativeConfig, YoutrackConfig, YoutrackSource,
};
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
        .route("/actions/new/youtrack", get(youtrack_new_page))
        .route("/actions", post(action_create))
        .route("/actions/youtrack", post(youtrack_create))
        .route("/actions/:name", get(action_detail))
        .route("/actions/:name/durations", get(action_durations))
        .route(
            "/actions/:name/edit",
            get(action_edit_page).post(action_edit_save),
        )
        .route(
            "/actions/:name/form",
            get(youtrack_edit_page).post(youtrack_edit_save),
        )
        .route("/actions/:name/export", get(action_export))
        .route("/secrets", get(secrets_page).post(secret_create))
        .route("/secrets/:name/rotate", post(secret_rotate))
        .route("/secrets/:name/delete", post(secret_delete))
        .route("/runs/:id", get(run_detail))
        .route("/runs/:id/jobs/:key/attempt/:attempt", get(job_stream_page))
        .route(
            "/sse/runs/:id/jobs/:key/attempt/:attempt",
            get(job_stream_sse),
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
    jobs_processed: i64,
    triggered_by: String,
    /// CLAUDE-14: key of the job currently being processed, or NULL when no job
    /// is in flight (run finished, or a legacy run predating the column).
    current_job_key: Option<String>,
    /// CLAUDE-22: count of distinct jobs whose LATEST attempt failed (`ok = 0`),
    /// matching the per-job `fail` badges on run-detail. A job that failed an
    /// earlier attempt but passed its latest does not count.
    failed_jobs: i64,
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
struct JobOutcomeRow {
    job_key: String,
    attempt: i64,
    ts: String,
    ok: bool,
    claude_ran: bool,
    claude_ok: bool,
    claude_cost_usd: f64,
    attempt_counted: bool,
    /// CLAUDE-20: full per-job pipeline elapsed; NULL for jobs predating the
    /// column (migration 0005).
    duration_ms: Option<i64>,
}

impl JobOutcomeRow {
    /// CLAUDE-20: human-readable per-job duration, or "-" for jobs predating the
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
/// and per-job duration columns. Negative inputs clamp to zero.
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

/// CLAUDE-35: when `show_empty` is false, finished runs that processed zero jobs
/// are hidden. In-progress runs (`exit_code IS NULL`) are always kept so a
/// just-started run never momentarily vanishes (it can legitimately sit at
/// `jobs_processed = 0` before its first job lands). The `LIMIT` applies after
/// the filter, so up to `limit` matching runs are returned, not `limit` minus
/// the hidden rows.
async fn fetch_recent_runs(
    pool: &SqlitePool,
    limit: i64,
    show_empty: bool,
) -> Result<Vec<RunRow>, WebError> {
    let where_clause = if show_empty {
        ""
    } else {
        " WHERE jobs_processed > 0 OR exit_code IS NULL"
    };
    let sql = format!(
        "SELECT id, action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by, current_job_key, (SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM job_outcomes o2 WHERE o2.run_id = o.run_id AND o2.job_key = o.job_key)) AS failed_jobs FROM runs{where_clause} ORDER BY started_at DESC LIMIT ?"
    );
    let rows = sqlx::query_as::<_, RunRow>(&sql)
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
    /// CLAUDE-35: current state of the "Show empty runs" toggle. Threaded into
    /// the poll-region `hx-get` so the choice survives the 5s self-refresh.
    show_empty: bool,
}

/// CLAUDE-35: query params for the dashboard. `show_empty` defaults to `false`
/// when the param is absent, so the page loads with 0-job finished runs hidden.
#[derive(Deserialize, Default)]
struct DashboardQuery {
    #[serde(default)]
    show_empty: bool,
}

async fn dashboard(
    State(s): State<WebState>,
    Query(q): Query<DashboardQuery>,
) -> Result<Html<String>, WebError> {
    let pool = &s.api.pool;
    let global_pause = state::global_pause_status(pool)
        .await?
        .map(|(a, r)| PauseInfo {
            paused_at: a,
            reason: r,
        });
    let actions = enriched_actions(pool).await?;
    let recent_runs = fetch_recent_runs(pool, 20, q.show_empty).await?;

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
        show_empty: q.show_empty,
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
        "SELECT id, action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by, current_job_key, (SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM job_outcomes o2 WHERE o2.run_id = o.run_id AND o2.job_key = o.job_key)) AS failed_jobs FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 50",
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
        "SELECT id, action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by, current_job_key, (SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM job_outcomes o2 WHERE o2.run_id = o.run_id AND o2.job_key = o.job_key)) AS failed_jobs FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 50",
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
    /// Link to the structured editor, shown when one applies (a `kind: youtrack`
    /// action being edited, or the new-action page offering a YouTrack form).
    form_url: Option<String>,
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
\x20\x20\x20\x20# nu: print a JSON array of jobs to stdout\n\
\x20\x20\x20\x20print '[]'\n\
prompt: |\n\
\x20\x20# nu: print the Claude prompt for the current job to stdout\n\
\x20\x20print \"Describe the task for this job.\"\n\
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
        form_url: None,
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
    // Offer the structured editor only for a parseable `kind: youtrack` action.
    let form_url = match crate::action::validate_str(&yaml, &path) {
        Ok(loaded) if matches!(loaded.action.native, Some(NativeConfig::Youtrack(_))) => {
            Some(format!("/actions/{name}/form"))
        }
        _ => None,
    };
    let tpl = ActionEditorTpl {
        version: s.api.version,
        heading: format!("Edit {name}"),
        post_url: format!("/actions/{name}/edit"),
        yaml,
        error: None,
        is_new: false,
        name: Some(name),
        form_url,
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
        form_url: Some("/actions/new/youtrack".into()),
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

// ---- structured YouTrack action editor (CLAUDE-27) ------------------------
//
// A field-based form for the native `kind: youtrack` action, an alternative to
// the raw-YAML textarea above. The form owns the native config (youtrack source
// + forgejo target) plus the generic Action blocks it surfaces (workdir_base,
// link_template, schedule, claude, limits, enabled); every other key the file
// carries (concurrency, output, dedup_key, ...) is left untouched on save.
// `token_ref` fields are dropdowns of stored secret *names* - a value is never
// rendered (CLAUDE-26). Submissions round-trip through the same
// `crate::action::validate_str` the raw editor uses, so a bad config is rejected
// inline with the entered values preserved.

#[derive(Template)]
#[template(path = "youtrack_editor.html")]
struct YoutrackEditorTpl {
    version: &'static str,
    heading: String,
    /// URL the form POSTs to.
    post_url: String,
    /// URL of the raw-YAML editor for the same action (advanced / fallback).
    raw_url: String,
    /// Prefilled (or submitted-and-rejected) field values.
    form: YoutrackEditForm,
    /// Stored secret names offered in the `token_ref` dropdowns. Names only -
    /// values are never loaded here.
    secrets: Vec<String>,
    /// Inline validation error, rendered above the form when set.
    error: Option<String>,
    /// Existing action name (`None` in create mode); drives the heading,
    /// breadcrumb, and Cancel link.
    name: Option<String>,
}

/// Flat form backing the structured editor. Every input posts a string; the
/// mapping into the typed config (and number / enum parsing) happens in
/// [`youtrack_form_to_yaml`]. `#[serde(default)]` lets an unchecked checkbox or
/// an omitted field deserialize cleanly.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct YoutrackEditForm {
    name: String,
    yt_base_url: String,
    yt_token_ref: String,
    yt_query: String,
    yt_custom_field: String,
    state_queued: String,
    state_working: String,
    state_code_review: String,
    state_needs_review: String,
    state_failed: String,
    state_in_progress: String,
    fj_base_url: String,
    fj_token_ref: String,
    workdir_base: String,
    link_template: String,
    /// "", "interval", "cron", or "manual".
    schedule_kind: String,
    schedule_spec: String,
    claude_cwd: String,
    claude_allowed_tools: String,
    claude_permission_mode: String,
    limit_max_attempts_per_key: String,
    limit_max_runs_per_day: String,
    limit_claude_timeout_seconds: String,
    limit_daily_budget_usd: String,
    /// Present ("on") when the enabled checkbox is checked, absent otherwise.
    enabled: Option<String>,
}

impl YoutrackEditForm {
    /// Whether the enabled checkbox was ticked (the browser omits an unchecked
    /// box, so absence means false).
    fn is_enabled(&self) -> bool {
        self.enabled.is_some()
    }
}

/// Field defaults for a brand-new YouTrack action: blank identity, but the
/// query / custom field / state labels seeded from [`YoutrackConfig::default`]
/// so the form opens with the same values a bare `native: {}` resolves to.
fn youtrack_form_defaults() -> YoutrackEditForm {
    let c = YoutrackConfig::default();
    YoutrackEditForm {
        yt_query: c.youtrack.query,
        yt_custom_field: c.youtrack.custom_field,
        state_queued: c.youtrack.states.queued,
        state_working: c.youtrack.states.working,
        state_code_review: c.youtrack.states.code_review,
        state_needs_review: c.youtrack.states.needs_review,
        state_failed: c.youtrack.states.failed,
        state_in_progress: c.youtrack.states.in_progress,
        ..Default::default()
    }
}

/// Prefill the form from a loaded action. Returns `None` when the action is not
/// a native `kind: youtrack` action (the structured editor only handles that
/// kind; callers redirect to the raw editor instead).
fn action_to_youtrack_form(a: &Action) -> Option<YoutrackEditForm> {
    let NativeConfig::Youtrack(yt) = a.native.as_ref()? else {
        return None;
    };
    let s = |o: &Option<String>| o.clone().unwrap_or_default();
    let n = |o: Option<u32>| o.map(|v| v.to_string()).unwrap_or_default();
    Some(YoutrackEditForm {
        name: a.name.clone(),
        yt_base_url: s(&yt.youtrack.base_url),
        yt_token_ref: s(&yt.youtrack.token_ref),
        yt_query: yt.youtrack.query.clone(),
        yt_custom_field: yt.youtrack.custom_field.clone(),
        state_queued: yt.youtrack.states.queued.clone(),
        state_working: yt.youtrack.states.working.clone(),
        state_code_review: yt.youtrack.states.code_review.clone(),
        state_needs_review: yt.youtrack.states.needs_review.clone(),
        state_failed: yt.youtrack.states.failed.clone(),
        state_in_progress: yt.youtrack.states.in_progress.clone(),
        fj_base_url: s(&yt.forgejo.base_url),
        fj_token_ref: s(&yt.forgejo.token_ref),
        workdir_base: s(&a.workdir_base),
        link_template: s(&a.link_template),
        schedule_kind: a
            .schedule
            .as_ref()
            .map(|sc| {
                match sc.kind {
                    crate::action::ScheduleKind::Interval => "interval",
                    crate::action::ScheduleKind::Cron => "cron",
                    crate::action::ScheduleKind::Manual => "manual",
                }
                .to_string()
            })
            .unwrap_or_default(),
        schedule_spec: a
            .schedule
            .as_ref()
            .and_then(|sc| sc.spec.clone())
            .unwrap_or_default(),
        claude_cwd: s(&a.claude.cwd),
        claude_allowed_tools: s(&a.claude.allowed_tools),
        claude_permission_mode: s(&a.claude.permission_mode),
        limit_max_attempts_per_key: n(a.limits.max_attempts_per_key),
        limit_max_runs_per_day: n(a.limits.max_runs_per_day),
        limit_claude_timeout_seconds: n(a.limits.claude_timeout_seconds),
        limit_daily_budget_usd: a
            .limits
            .daily_budget_usd
            .map(|v| v.to_string())
            .unwrap_or_default(),
        enabled: a.enabled.then(|| "on".to_string()),
    })
}

/// Recursively drop `null` values and empty maps / sequences so the emitted
/// YAML carries only the fields the user actually set (a `None` `token_ref`
/// disappears rather than serializing as `token_ref: null`). Returns `None`
/// when the whole value prunes away to nothing.
fn prune_yaml(v: serde_yaml::Value) -> Option<serde_yaml::Value> {
    use serde_yaml::Value;
    match v {
        Value::Null => None,
        Value::Mapping(m) => {
            let mut out = serde_yaml::Mapping::new();
            for (k, val) in m {
                if let Some(p) = prune_yaml(val) {
                    out.insert(k, p);
                }
            }
            (!out.is_empty()).then_some(Value::Mapping(out))
        }
        Value::Sequence(seq) => {
            let pruned: Vec<Value> = seq.into_iter().filter_map(prune_yaml).collect();
            (!pruned.is_empty()).then_some(Value::Sequence(pruned))
        }
        scalar => Some(scalar),
    }
}

/// Map the structured form into a full action YAML string, overlaid onto `base`
/// (the parsed existing file for an edit, or an empty mapping for a create).
/// The form owns the `name`, `kind`, `native`, `claude`, `limits`, `schedule`,
/// `workdir_base`, `link_template`, and `enabled` keys; every other key in
/// `base` is preserved untouched. Returns `Err` with a human-readable message
/// when a numeric limit or the schedule kind fails to parse.
fn youtrack_form_to_yaml(
    mut base: serde_yaml::Value,
    form: &YoutrackEditForm,
) -> Result<String, String> {
    use serde_yaml::Value;

    let map = base
        .as_mapping_mut()
        .ok_or_else(|| "the action file is not a YAML mapping".to_string())?;

    // Empty text input -> unset (None); trimmed non-empty -> Some.
    let opt = |raw: &str| -> Option<String> {
        let t = raw.trim();
        (!t.is_empty()).then(|| t.to_string())
    };
    // Identity.
    map.insert(Value::from("name"), Value::from(form.name.trim()));
    map.insert(Value::from("kind"), Value::from("youtrack"));

    // Native YouTrack config. Required string fields fall back to their
    // defaults when left blank so a cleared field never becomes an empty label.
    let mut youtrack = YoutrackSource {
        base_url: opt(&form.yt_base_url),
        token_ref: opt(&form.yt_token_ref),
        ..YoutrackSource::default()
    };
    if let Some(q) = opt(&form.yt_query) {
        youtrack.query = q;
    }
    if let Some(c) = opt(&form.yt_custom_field) {
        youtrack.custom_field = c;
    }
    let states = &mut youtrack.states;
    if let Some(v) = opt(&form.state_queued) {
        states.queued = v;
    }
    if let Some(v) = opt(&form.state_working) {
        states.working = v;
    }
    if let Some(v) = opt(&form.state_code_review) {
        states.code_review = v;
    }
    if let Some(v) = opt(&form.state_needs_review) {
        states.needs_review = v;
    }
    if let Some(v) = opt(&form.state_failed) {
        states.failed = v;
    }
    if let Some(v) = opt(&form.state_in_progress) {
        states.in_progress = v;
    }
    let native = YoutrackConfig {
        youtrack,
        forgejo: ForgejoTarget {
            base_url: opt(&form.fj_base_url),
            token_ref: opt(&form.fj_token_ref),
        },
    };
    let native_val = prune_yaml(serde_yaml::to_value(&native).map_err(|e| e.to_string())?)
        .unwrap_or_else(|| Value::Mapping(serde_yaml::Mapping::new()));
    map.insert(Value::from("native"), native_val);

    // Limits. Blank -> unset; non-numeric -> inline error.
    let pu32 = |label: &str, raw: &str| -> Result<Option<u32>, String> {
        match raw.trim() {
            "" => Ok(None),
            t => t
                .parse::<u32>()
                .map(Some)
                .map_err(|_| format!("{label} must be a whole number (got `{t}`)")),
        }
    };
    let limits = Limits {
        max_attempts_per_key: pu32("Max attempts per key", &form.limit_max_attempts_per_key)?,
        max_runs_per_day: pu32("Max runs per day", &form.limit_max_runs_per_day)?,
        claude_timeout_seconds: pu32(
            "Claude timeout (seconds)",
            &form.limit_claude_timeout_seconds,
        )?,
        daily_budget_usd: match form.limit_daily_budget_usd.trim() {
            "" => None,
            t => Some(
                t.parse::<f64>()
                    .map_err(|_| format!("Daily budget (USD) must be a number (got `{t}`)"))?,
            ),
        },
    };
    set_or_remove(
        map,
        "limits",
        prune_yaml(serde_yaml::to_value(&limits).map_err(|e| e.to_string())?),
    );

    // Claude block.
    let claude = ClaudeBlock {
        cwd: opt(&form.claude_cwd),
        allowed_tools: opt(&form.claude_allowed_tools),
        permission_mode: opt(&form.claude_permission_mode),
    };
    set_or_remove(
        map,
        "claude",
        prune_yaml(serde_yaml::to_value(&claude).map_err(|e| e.to_string())?),
    );

    // Schedule.
    match form.schedule_kind.trim() {
        "" | "none" => {
            map.remove("schedule");
        }
        kind @ ("interval" | "cron" | "manual") => {
            let mut sm = serde_yaml::Mapping::new();
            sm.insert(Value::from("kind"), Value::from(kind));
            if let Some(spec) = opt(&form.schedule_spec) {
                sm.insert(Value::from("spec"), Value::from(spec));
            }
            map.insert(Value::from("schedule"), Value::Mapping(sm));
        }
        other => return Err(format!("unknown schedule kind `{other}`")),
    }

    // Scalars that drop out of the file when cleared.
    set_scalar_or_remove(map, "workdir_base", opt(&form.workdir_base));
    set_scalar_or_remove(map, "link_template", opt(&form.link_template));
    map.insert(Value::from("enabled"), Value::from(form.is_enabled()));

    serde_yaml::to_string(&base).map_err(|e| e.to_string())
}

/// Insert `val` under `key`, or remove the key when `val` is `None`.
fn set_or_remove(map: &mut serde_yaml::Mapping, key: &str, val: Option<serde_yaml::Value>) {
    match val {
        Some(v) => {
            map.insert(serde_yaml::Value::from(key), v);
        }
        None => {
            map.remove(key);
        }
    }
}

/// Insert a scalar string under `key`, or remove the key when `val` is `None`.
fn set_scalar_or_remove(map: &mut serde_yaml::Mapping, key: &str, val: Option<String>) {
    set_or_remove(map, key, val.map(serde_yaml::Value::from));
}

/// Stored secret names, with any `token_ref` the action already references that
/// no longer exists folded in so an orphaned reference still shows as selected.
async fn token_ref_options(s: &WebState, form: &YoutrackEditForm) -> Result<Vec<String>, WebError> {
    let mut names: Vec<String> = state::secret_list(&s.api.pool)
        .await?
        .into_iter()
        .map(|m| m.name)
        .collect();
    for r in [&form.yt_token_ref, &form.fj_token_ref] {
        let r = r.trim();
        if !r.is_empty() && !names.iter().any(|n| n == r) {
            names.push(r.to_string());
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Render the structured editor at `status`, preserving the supplied `form`.
async fn render_youtrack_editor(
    s: &WebState,
    status: StatusCode,
    name: Option<String>,
    form: YoutrackEditForm,
    error: Option<String>,
) -> Result<Response, WebError> {
    let secrets = token_ref_options(s, &form).await?;
    let (heading, post_url, raw_url) = match &name {
        Some(n) => (
            format!("Edit {n}"),
            format!("/actions/{n}/form"),
            format!("/actions/{n}/edit"),
        ),
        None => (
            "New YouTrack action".to_string(),
            "/actions/youtrack".to_string(),
            "/actions/new".to_string(),
        ),
    };
    let tpl = YoutrackEditorTpl {
        version: s.api.version,
        heading,
        post_url,
        raw_url,
        form,
        secrets,
        error,
        name,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

async fn youtrack_new_page(State(s): State<WebState>) -> Result<Response, WebError> {
    render_youtrack_editor(&s, StatusCode::OK, None, youtrack_form_defaults(), None).await
}

async fn youtrack_edit_page(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let yaml = std::fs::read_to_string(&path).map_err(WebError::from)?;
    // A file that no longer parses, or is not a youtrack action, has no
    // structured form; send the user to the raw editor instead.
    let form = match crate::action::validate_str(&yaml, &path) {
        Ok(loaded) => action_to_youtrack_form(&loaded.action),
        Err(_) => None,
    };
    let Some(form) = form else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    render_youtrack_editor(&s, StatusCode::OK, Some(name), form, None).await
}

async fn youtrack_edit_save(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
    Form(form): Form<YoutrackEditForm>,
) -> Result<Response, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let existing = std::fs::read_to_string(&path).map_err(WebError::from)?;
    let base: serde_yaml::Value = serde_yaml::from_str(&existing)
        .unwrap_or_else(|_| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    let yaml = match youtrack_form_to_yaml(base, &form) {
        Ok(y) => y,
        Err(msg) => {
            return render_youtrack_editor(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(msg),
            )
            .await
        }
    };
    let loaded = match crate::action::validate_str(&yaml, &path) {
        Ok(l) => l,
        Err(e) => {
            return render_youtrack_editor(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(e.to_string()),
            )
            .await
        }
    };
    std::fs::write(&path, yaml.as_bytes()).map_err(WebError::from)?;
    // Redirect to the action's own `name`, which may differ from the URL path
    // when the form renames it (the daemon re-indexes by the `name:` field).
    Ok(Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response())
}

async fn youtrack_create(
    State(s): State<WebState>,
    Form(form): Form<YoutrackEditForm>,
) -> Result<Response, WebError> {
    let base = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let yaml = match youtrack_form_to_yaml(base, &form) {
        Ok(y) => y,
        Err(msg) => {
            return render_youtrack_editor(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                form,
                Some(msg),
            )
            .await
        }
    };
    let loaded = match crate::action::validate_str(&yaml, "<import>") {
        Ok(l) => l,
        Err(e) => {
            return render_youtrack_editor(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                form,
                Some(e.to_string()),
            )
            .await
        }
    };
    let Some(stem) = safe_action_stem(&loaded.action.name) else {
        let msg = format!(
            "action name `{}` is not a safe filename (allowed: letters, digits, '-', '_', '.')",
            loaded.action.name
        );
        return render_youtrack_editor(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg))
            .await;
    };
    let actions_dir = s.paths.actions_dir();
    let dest = actions_dir.join(format!("{stem}.yml"));
    if dest.exists() {
        let msg = format!("`{stem}.yml` already exists; edit it instead of creating a duplicate");
        return render_youtrack_editor(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg))
            .await;
    }
    std::fs::create_dir_all(&actions_dir).map_err(WebError::from)?;
    std::fs::write(&dest, yaml.as_bytes()).map_err(WebError::from)?;
    Ok(Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response())
}

// ---- secrets management (CLAUDE-26) ---------------------------------------
//
// Values are write-only: the create / rotate forms accept a value, but the
// listing only ever shows a secret's name + timestamps. A stored value is never
// re-rendered or sent back to the browser; it leaves the DB only as ciphertext,
// decrypted in-process at run time. Encryption needs the `age` key file named by
// `CLAUDE_RUN_SECRETS_KEY_FILE`; when it is missing, create / rotate fail closed
// with an inline error and the listing shows a banner.

#[derive(Template)]
#[template(path = "secrets.html")]
struct SecretsTpl {
    version: &'static str,
    secrets: Vec<state::SecretMeta>,
    /// Inline error from a rejected create / rotate / delete, rendered up top.
    error: Option<String>,
    /// Whether the secrets key file loads; gates the create / rotate forms.
    key_ready: bool,
    /// Why the key file did not load (shown when `key_ready` is false).
    key_error: Option<String>,
}

#[derive(Deserialize)]
struct SecretCreateForm {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct SecretValueForm {
    value: String,
}

/// Constrain a secret name to a safe handle: non-empty, `[A-Za-z0-9._-]` only.
fn valid_secret_name(name: &str) -> bool {
    let n = name.trim();
    !n.is_empty()
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// Render the secrets page (HTTP `status`), loading the listing and the key
/// status. Never renders any secret value.
async fn render_secrets(
    s: &WebState,
    status: StatusCode,
    error: Option<String>,
) -> Result<Response, WebError> {
    let secrets = state::secret_list(&s.api.pool).await?;
    let (key_ready, key_error) = match crate::secrets::SecretsKey::load() {
        Ok(_) => (true, None),
        Err(e) => (false, Some(e.to_string())),
    };
    let tpl = SecretsTpl {
        version: s.api.version,
        secrets,
        error,
        key_ready,
        key_error,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

async fn secrets_page(State(s): State<WebState>) -> Result<Response, WebError> {
    render_secrets(&s, StatusCode::OK, None).await
}

async fn secret_create(
    State(s): State<WebState>,
    Form(form): Form<SecretCreateForm>,
) -> Result<Response, WebError> {
    let name = form.name.trim().to_string();
    if !valid_secret_name(&name) {
        return render_secrets(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            Some(format!(
                "secret name `{name}` is invalid (allowed: letters, digits, '-', '_', '.')"
            )),
        )
        .await;
    }
    if form.value.is_empty() {
        return render_secrets(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("a secret value is required".to_string()),
        )
        .await;
    }
    let key = match crate::secrets::SecretsKey::load() {
        Ok(k) => k,
        Err(e) => {
            return render_secrets(&s, StatusCode::UNPROCESSABLE_ENTITY, Some(e.to_string())).await
        }
    };
    if state::secret_exists(&s.api.pool, &name).await? {
        return render_secrets(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            Some(format!(
                "a secret named `{name}` already exists; rotate it instead"
            )),
        )
        .await;
    }
    let ciphertext = match key.encrypt(&form.value) {
        Ok(c) => c,
        Err(e) => {
            return render_secrets(&s, StatusCode::UNPROCESSABLE_ENTITY, Some(e.to_string())).await
        }
    };
    state::secret_create(&s.api.pool, &name, &ciphertext).await?;
    Ok(Redirect::to("/secrets").into_response())
}

async fn secret_rotate(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
    Form(form): Form<SecretValueForm>,
) -> Result<Response, WebError> {
    if form.value.is_empty() {
        return render_secrets(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("a new secret value is required to rotate".to_string()),
        )
        .await;
    }
    let key = match crate::secrets::SecretsKey::load() {
        Ok(k) => k,
        Err(e) => {
            return render_secrets(&s, StatusCode::UNPROCESSABLE_ENTITY, Some(e.to_string())).await
        }
    };
    let ciphertext = match key.encrypt(&form.value) {
        Ok(c) => c,
        Err(e) => {
            return render_secrets(&s, StatusCode::UNPROCESSABLE_ENTITY, Some(e.to_string())).await
        }
    };
    if !state::secret_rotate(&s.api.pool, &name, &ciphertext).await? {
        return render_secrets(
            &s,
            StatusCode::NOT_FOUND,
            Some(format!("no secret named `{name}`")),
        )
        .await;
    }
    Ok(Redirect::to("/secrets").into_response())
}

async fn secret_delete(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    if !state::secret_delete(&s.api.pool, &name).await? {
        return render_secrets(
            &s,
            StatusCode::NOT_FOUND,
            Some(format!("no secret named `{name}`")),
        )
        .await;
    }
    Ok(Redirect::to("/secrets").into_response())
}

#[derive(Serialize)]
pub struct PlannedJob {
    pub key: String,
    pub href: Option<String>,
    /// One of:
    /// - "ok" / "fail": an outcome record landed.
    /// - "running": no outcome, run still active, and this is the one job the
    ///   run is processing (`runs.current_job_key`). Carries `watch_attempt`.
    /// - "queued": no outcome, run still active, not the in-flight job.
    /// - "skipped": no outcome and the run has ended (planned but never
    ///   completed). Replaces the stale "running" badge.
    pub status: String,
    /// Attempt number for the "watch live" link, set only for the "running"
    /// job. Resolved from the highest `<key>-attempt-<n>.jsonl` stream log on
    /// disk (`None` until claude opens the log for the job's first attempt).
    pub watch_attempt: Option<u32>,
}

/// Resolve the highest attempt number with a stream log on disk for `key`.
/// Stream logs are written live by `ClaudeInvoker::invoke` to
/// `<logs>/<action>/streams/<key>-attempt-<n>.jsonl`, so the presence of one
/// without a matching `job_outcomes` record means that job is in flight. This
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
    outcomes: Vec<JobOutcomeRow>,
    /// CLAUDE-1: jobs planned at run-start; populated as soon as
    /// `input.command` returns. Pre-existing runs have NULL `run.job_keys`
    /// and this stays empty.
    planned_jobs: Vec<PlannedJob>,
    /// Per-job href builder for the outcomes table. Empty string when the
    /// action declares no `link_template`.
    link_template: Option<String>,
}

async fn run_detail(
    State(s): State<WebState>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    let Some(run): Option<RunRow> = sqlx::query_as::<_, RunRow>(
        "SELECT id, action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by, current_job_key, (SELECT COUNT(*) FROM job_outcomes o WHERE o.run_id = runs.id AND o.ok = 0 AND o.attempt = (SELECT MAX(o2.attempt) FROM job_outcomes o2 WHERE o2.run_id = o.run_id AND o2.job_key = o.job_key)) AS failed_jobs FROM runs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await? else {
        return Ok((StatusCode::NOT_FOUND, format!("no run #{id}")).into_response());
    };
    let outcomes = sqlx::query_as::<_, JobOutcomeRow>(
        "SELECT job_key, attempt, ts, ok, claude_ran, claude_ok, claude_cost_usd, attempt_counted, duration_ms FROM job_outcomes WHERE run_id = ? ORDER BY id ASC",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;

    // Look up the action's link_template once and reuse for both planned
    // jobs and the outcomes table.
    let link_template: Option<String> =
        sqlx::query_scalar("SELECT link_template FROM actions WHERE name = ?")
            .bind(&run.action_name)
            .fetch_optional(pool)
            .await?
            .flatten();

    let planned_keys = state::run_job_keys(pool, run.id).await.unwrap_or_default();
    // Build a quick lookup: key -> overall ok flag from job_outcomes (latest
    // attempt wins, since outcomes are inserted in order).
    let mut outcome_by_key: std::collections::HashMap<&str, bool> =
        std::collections::HashMap::new();
    for o in &outcomes {
        outcome_by_key.insert(o.job_key.as_str(), o.ok);
    }
    // CLAUDE-14 / CLAUDE-28: a run with a NULL `ended_at` is still active; only
    // then can jobs be in flight. CLAUDE-28 runs distinct-key jobs in parallel,
    // so several jobs can be in flight at once: `run_inflight_jobs` holds that
    // set, and any planned job whose key is in it is "running". Every other
    // no-outcome job is queued. Once the run ends, the in-flight set is empty
    // and a no-outcome job never finished (filtered / capped / skipped / not
    // reached), so it must not keep the "running" badge.
    let run_active = run.ended_at.is_none();
    let inflight: std::collections::HashSet<String> = state::run_inflight_keys(pool, run.id)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();
    let logs_dir = s.paths.logs_dir();
    let planned_jobs: Vec<PlannedJob> = planned_keys
        .into_iter()
        .map(|k| {
            let (status, watch_attempt) = match outcome_by_key.get(k.as_str()) {
                Some(true) => ("ok".to_string(), None),
                Some(false) => ("fail".to_string(), None),
                None if run_active && inflight.contains(k.as_str()) => {
                    // An in-flight job. Resolve its live-stream attempt from disk
                    // for the "watch live" link.
                    let attempt = latest_attempt_on_disk(&logs_dir, &run.action_name, &k);
                    ("running".to_string(), attempt)
                }
                None if run_active => ("queued".to_string(), None),
                None => ("skipped".to_string(), None),
            };
            let href = link_template
                .as_ref()
                .map(|t| crate::action::render_link(t, &k));
            PlannedJob {
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
        planned_jobs,
        link_template,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

#[derive(Template)]
#[template(path = "job_stream.html")]
struct JobStreamTpl {
    version: &'static str,
    run_id: i64,
    /// Parent action, resolved alongside the stream-log path. Feeds the
    /// breadcrumb's link back to the action (CLAUDE-19).
    action_name: String,
    job_key: String,
    attempt: u32,
    stream_path: String,
}

async fn job_stream_page(
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
    let tpl = JobStreamTpl {
        version: s.api.version,
        run_id: id,
        action_name,
        job_key: key,
        attempt,
        stream_path: stream.display().to_string(),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

// ---- SSE ------------------------------------------------------------------

async fn job_stream_sse(
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
    // the stream cleanly with a `done` SSE event) or the job's outcome
    // record lands in `job_outcomes` (run has moved on past this attempt).
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
                        "SELECT id FROM job_outcomes WHERE run_id = ? AND job_key = ? AND attempt = ?",
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

#[derive(Debug)]
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

    // ---- action editor handlers (CLAUDE-34) -------------------------------
    //
    // The editor is kind-agnostic: `action_edit_save` / `action_create` route
    // every submission through the kind-aware `crate::action::validate_str`, so
    // a native youtrack action is accepted, validated against the native schema,
    // and rejected inline on bad config exactly as a scripted action is. These
    // tests exercise that path end-to-end against an in-memory DB and a tempdir
    // actions directory.
    mod editor {
        use super::super::{
            action_create, action_edit_save, action_export, action_to_youtrack_form,
            youtrack_create, youtrack_edit_page, youtrack_edit_save, youtrack_form_to_yaml,
            youtrack_new_page, ActionForm, WebState, YoutrackEditForm,
        };
        use crate::api::AppState;
        use crate::cancel::RunRegistry;
        use crate::config::Paths;
        use crate::db::MIGRATOR;
        use crate::state;
        use axum::extract::{Form, Path as AxumPath, State};
        use axum::http::StatusCode;
        use axum::response::{IntoResponse, Response};
        use sqlx::sqlite::SqlitePoolOptions;
        use sqlx::SqlitePool;
        use std::sync::Arc;

        /// A valid native youtrack action. `native: {}` resolves every field to
        /// its default.
        const NATIVE_MINIMAL: &str = "name: youtrack\nkind: youtrack\nnative: {}\n";

        /// Build a `WebState` backed by an in-memory DB and a tempdir-rooted
        /// `Paths`. The trigger receiver is dropped; none of the editor handlers
        /// send on it.
        async fn web_state(tmp: &std::path::Path) -> WebState {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            MIGRATOR.run(&pool).await.unwrap();
            let (trigger_tx, _rx) = tokio::sync::mpsc::channel(8);
            let api = Arc::new(AppState {
                pool,
                trigger_tx,
                version: "test",
                runs: Arc::new(RunRegistry::new()),
            });
            let paths = Paths {
                config_dir: tmp.join("config"),
                data_dir: tmp.join("data"),
                state_dir: tmp.join("state"),
                runtime_dir: tmp.join("runtime"),
            };
            WebState { api, paths }
        }

        /// Register an action in the index so `indexed_action_path` resolves it
        /// to `file_path` (the only names the edit/export handlers will touch).
        async fn index_action(pool: &SqlitePool, name: &str, file_path: &str) {
            state::action_index_upsert(
                pool,
                &state::ActionIndexRow {
                    name: name.to_string(),
                    file_path: file_path.to_string(),
                    yaml_sha256: "0".repeat(64),
                    enabled: false,
                    schedule_kind: None,
                    schedule_spec: None,
                    last_seen_at: "2026-01-01T00:00:00+00:00".to_string(),
                    parse_error: None,
                    link_template: None,
                },
            )
            .await
            .unwrap();
        }

        async fn body_string(resp: Response) -> String {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        #[tokio::test]
        async fn edit_save_accepts_and_writes_native_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;

            // A native action with an overridden query validates against the
            // native schema and is written to disk.
            let new_yaml = "name: youtrack\nkind: youtrack\nnative:\n  youtrack:\n    query: \"-Resolved AI Agent: Ready\"\n";
            let resp = action_edit_save(
                State(s.clone()),
                AxumPath("youtrack".to_string()),
                Form(ActionForm {
                    yaml: new_yaml.to_string(),
                }),
            )
            .await
            .unwrap()
            .into_response();

            // Redirect::to is 303 See Other.
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert_eq!(std::fs::read_to_string(&file).unwrap(), new_yaml);
        }

        #[tokio::test]
        async fn edit_save_rejects_invalid_native_config_inline() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;

            // An unknown key under the native config is rejected by the native
            // schema (`deny_unknown_fields`).
            let bad =
                "name: youtrack\nkind: youtrack\nnative:\n  youtrack:\n    bogus_field: nope\n";
            let resp = action_edit_save(
                State(s.clone()),
                AxumPath("youtrack".to_string()),
                Form(ActionForm {
                    yaml: bad.to_string(),
                }),
            )
            .await
            .unwrap()
            .into_response();

            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            // The on-disk file is left untouched.
            assert_eq!(std::fs::read_to_string(&file).unwrap(), NATIVE_MINIMAL);
            // The submitted draft is preserved in the re-rendered editor.
            let body = body_string(resp).await;
            assert!(body.contains("bogus_field"));
        }

        #[tokio::test]
        async fn create_accepts_native_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let yaml = "name: yt-native\nkind: youtrack\nnative: {}\n";
            let resp = action_create(
                State(s.clone()),
                Form(ActionForm {
                    yaml: yaml.to_string(),
                }),
            )
            .await
            .unwrap()
            .into_response();

            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            let dest = s.paths.actions_dir().join("yt-native.yml");
            assert_eq!(std::fs::read_to_string(&dest).unwrap(), yaml);
        }

        #[tokio::test]
        async fn create_rejects_invalid_native_config_inline() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let bad = "name: yt-native\nkind: youtrack\nnative:\n  forgejo:\n    bogus: 1\n";
            let resp = action_create(
                State(s.clone()),
                Form(ActionForm {
                    yaml: bad.to_string(),
                }),
            )
            .await
            .unwrap()
            .into_response();

            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            // Nothing is written for a rejected draft.
            assert!(!s.paths.actions_dir().join("yt-native.yml").exists());
            let body = body_string(resp).await;
            assert!(body.contains("bogus"));
        }

        #[tokio::test]
        async fn export_returns_native_config_unchanged() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            // Comments and field ordering must survive export byte-for-byte.
            let content = "# native youtrack action\nname: youtrack\nkind: youtrack\nnative:\n  youtrack:\n    query: \"-Resolved AI Agent: Queued\"\n";
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, content).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;

            let resp = action_export(State(s.clone()), AxumPath("youtrack".to_string()))
                .await
                .unwrap()
                .into_response();

            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(body_string(resp).await, content);
        }

        // ---- structured YouTrack editor (CLAUDE-27) -----------------------

        /// A populated form, covering native config + the generic blocks.
        fn filled_form() -> YoutrackEditForm {
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.yt_base_url = "https://youtrack.example.com".to_string();
            f.yt_token_ref = "yt-prod".to_string();
            f.yt_query = "-Resolved AI Agent: Ready".to_string();
            f.fj_token_ref = "fj-prod".to_string();
            f.workdir_base = "~/work".to_string();
            f.link_template = "https://youtrack.example.com/issue/{key}".to_string();
            f.schedule_kind = "interval".to_string();
            f.schedule_spec = "5m".to_string();
            f.claude_permission_mode = "acceptEdits".to_string();
            f.limit_max_runs_per_day = "24".to_string();
            f.limit_daily_budget_usd = "1.50".to_string();
            f.enabled = Some("on".to_string());
            f
        }

        fn empty_base() -> serde_yaml::Value {
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
        }

        #[test]
        fn form_maps_to_valid_native_config() {
            let yaml = youtrack_form_to_yaml(empty_base(), &filled_form()).unwrap();
            // The generated YAML validates through the same schema the loader uses.
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let a = loaded.action;
            assert_eq!(a.name, "yt");
            assert_eq!(a.kind, crate::action::ActionKind::Youtrack);
            assert!(a.enabled);
            assert_eq!(a.workdir_base.as_deref(), Some("~/work"));
            assert_eq!(
                a.link_template.as_deref(),
                Some("https://youtrack.example.com/issue/{key}")
            );
            assert_eq!(a.limits.max_runs_per_day, Some(24));
            assert_eq!(a.limits.daily_budget_usd, Some(1.5));
            assert_eq!(a.claude.permission_mode.as_deref(), Some("acceptEdits"));
            let sched = a.schedule.unwrap();
            assert_eq!(sched.kind, crate::action::ScheduleKind::Interval);
            assert_eq!(sched.spec.as_deref(), Some("5m"));
            let crate::action::NativeConfig::Youtrack(yt) = a.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(
                yt.youtrack.base_url.as_deref(),
                Some("https://youtrack.example.com")
            );
            assert_eq!(yt.youtrack.token_ref.as_deref(), Some("yt-prod"));
            assert_eq!(yt.youtrack.query, "-Resolved AI Agent: Ready");
            // A blank required field keeps the type default rather than blanking.
            assert_eq!(yt.youtrack.custom_field, "AI Agent");
            assert_eq!(yt.forgejo.token_ref.as_deref(), Some("fj-prod"));
            assert!(yt.forgejo.base_url.is_none());
        }

        #[test]
        fn form_output_omits_unset_fields() {
            // A near-empty form should not emit `null`s for the optional fields.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(!yaml.contains("null"), "unexpected null in:\n{yaml}");
            assert!(!yaml.contains("token_ref"), "blank token leaked:\n{yaml}");
            assert!(!yaml.contains("schedule"), "no schedule expected:\n{yaml}");
        }

        #[test]
        fn form_rejects_non_numeric_limit() {
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.limit_max_runs_per_day = "lots".to_string();
            let err = youtrack_form_to_yaml(empty_base(), &f).unwrap_err();
            assert!(err.contains("Max runs per day"), "{err}");
        }

        #[test]
        fn form_preserves_untouched_keys() {
            // A key the form does not own (here `concurrency`) survives the
            // round-trip untouched.
            let base: serde_yaml::Value = serde_yaml::from_str(
                "name: yt\nkind: youtrack\nnative: {}\nconcurrency:\n  max_parallel: 4\n",
            )
            .unwrap();
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(base, &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            assert_eq!(loaded.action.concurrency.max_parallel, 4);
        }

        #[test]
        fn prefill_round_trips() {
            // Map an action to form fields and back; the config is unchanged.
            let yaml = "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    query: \"q\"\n    token_ref: yt-prod\n  forgejo:\n    base_url: https://dev.a8n.run\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            let out = youtrack_form_to_yaml(empty_base(), &form).unwrap();
            let reloaded = crate::action::validate_str(&out, "<test>").unwrap();
            assert_eq!(reloaded.action.native, loaded.action.native);
        }

        #[test]
        fn prefill_rejects_non_youtrack_action() {
            // A scripted action has no structured form.
            let yaml = "name: s\ninput:\n  command: echo '[]'\nprompt: hi\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            assert!(action_to_youtrack_form(&loaded.action).is_none());
        }

        #[tokio::test]
        async fn youtrack_edit_save_writes_clean_yaml() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("yt.yml");
            std::fs::write(&file, "name: yt\nkind: youtrack\nnative: {}\n").unwrap();
            index_action(&s.api.pool, "yt", file.to_str().unwrap()).await;

            let resp = youtrack_edit_save(
                State(s.clone()),
                AxumPath("yt".to_string()),
                Form(filled_form()),
            )
            .await
            .unwrap()
            .into_response();

            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            let written = std::fs::read_to_string(&file).unwrap();
            // Persisted file re-validates and carries the edited query.
            let loaded = crate::action::validate_str(&written, "<test>").unwrap();
            assert_eq!(loaded.action.name, "yt");
            assert!(written.contains("-Resolved AI Agent: Ready"));
        }

        #[tokio::test]
        async fn youtrack_edit_save_redirects_to_renamed_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("yt.yml");
            std::fs::write(&file, "name: yt\nkind: youtrack\nnative: {}\n").unwrap();
            index_action(&s.api.pool, "yt", file.to_str().unwrap()).await;

            // The form renames the action; the redirect must target the new name
            // (the daemon re-indexes by the `name:` field), not the URL path.
            let mut f = filled_form();
            f.name = "yt-renamed".to_string();
            let resp = youtrack_edit_save(State(s.clone()), AxumPath("yt".to_string()), Form(f))
                .await
                .unwrap()
                .into_response();

            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                resp.headers().get("location").unwrap(),
                "/actions/yt-renamed"
            );
        }

        #[tokio::test]
        async fn youtrack_create_writes_new_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let resp = youtrack_create(State(s.clone()), Form(filled_form()))
                .await
                .unwrap()
                .into_response();

            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            let dest = s.paths.actions_dir().join("yt.yml");
            let written = std::fs::read_to_string(&dest).unwrap();
            crate::action::validate_str(&written, "<test>").unwrap();
        }

        #[tokio::test]
        async fn youtrack_create_rejects_bad_number_inline() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let mut f = filled_form();
            f.limit_claude_timeout_seconds = "soon".to_string();

            let resp = youtrack_create(State(s.clone()), Form(f))
                .await
                .unwrap()
                .into_response();

            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert!(!s.paths.actions_dir().join("yt.yml").exists());
            // The submitted values are preserved in the re-rendered form.
            let body = body_string(resp).await;
            assert!(body.contains("Claude timeout"));
        }

        #[tokio::test]
        async fn youtrack_new_page_renders_form() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let resp = youtrack_new_page(State(s.clone()))
                .await
                .unwrap()
                .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            // Default state labels and section headings are present.
            assert!(body.contains("New YouTrack action"));
            assert!(body.contains("AI Agent state labels"));
            assert!(body.contains("Code Review"));
            assert!(body.contains("/actions/youtrack"));
        }

        #[tokio::test]
        async fn youtrack_edit_page_renders_prefilled_form() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("yt.yml");
            std::fs::write(
                &file,
                "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    query: \"my-query\"\nschedule:\n  kind: interval\n  spec: 5m\n",
            )
            .unwrap();
            index_action(&s.api.pool, "yt", file.to_str().unwrap()).await;

            let resp = youtrack_edit_page(State(s.clone()), AxumPath("yt".to_string()))
                .await
                .unwrap()
                .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            assert!(body.contains("value=\"my-query\""));
            // The interval schedule option is preselected.
            assert!(body.contains("value=\"interval\" selected"));
        }

        #[tokio::test]
        async fn youtrack_edit_page_redirects_for_scripted_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("scripted.yml");
            std::fs::write(
                &file,
                "name: scripted\ninput:\n  command: echo '[]'\nprompt: hi\n",
            )
            .unwrap();
            index_action(&s.api.pool, "scripted", file.to_str().unwrap()).await;

            let resp = youtrack_edit_page(State(s.clone()), AxumPath("scripted".to_string()))
                .await
                .unwrap()
                .into_response();

            // Not a youtrack action -> redirect to the raw editor.
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                resp.headers().get("location").unwrap(),
                "/actions/scripted/edit"
            );
        }
    }

    // ---- secrets UI (CLAUDE-26) -------------------------------------------
    //
    // One test owns the `CLAUDE_RUN_SECRETS_KEY_FILE` env (no other test reads
    // it), so create -> list -> rotate -> delete runs against a stable key
    // without an env race. It proves the listing is write-only: the page shows
    // a secret's name but never its value, and the stored bytes are ciphertext.
    mod secrets_ui {
        use super::super::{
            secret_create, secret_delete, secret_rotate, secrets_page, SecretCreateForm,
            SecretValueForm, WebState,
        };
        use crate::api::AppState;
        use crate::cancel::RunRegistry;
        use crate::config::Paths;
        use crate::db::MIGRATOR;
        use crate::state;
        use age::secrecy::ExposeSecret;
        use axum::extract::{Form, Path as AxumPath, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use sqlx::sqlite::SqlitePoolOptions;
        use std::sync::Arc;

        async fn web_state(tmp: &std::path::Path) -> WebState {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            MIGRATOR.run(&pool).await.unwrap();
            let (trigger_tx, _rx) = tokio::sync::mpsc::channel(8);
            let api = Arc::new(AppState {
                pool,
                trigger_tx,
                version: "test",
                runs: Arc::new(RunRegistry::new()),
            });
            let paths = Paths {
                config_dir: tmp.join("config"),
                data_dir: tmp.join("data"),
                state_dir: tmp.join("state"),
                runtime_dir: tmp.join("runtime"),
            };
            WebState { api, paths }
        }

        async fn body_string(resp: axum::response::Response) -> String {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        #[tokio::test]
        async fn create_list_rotate_delete_is_write_only() {
            let tmp = tempfile::tempdir().unwrap();
            // Generate an age key and point the env at it for this test only.
            let key_path = tmp.path().join("key.txt");
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
            std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

            let s = web_state(tmp.path()).await;
            let value = "perm:super-secret-token-value";

            // Create.
            let resp = secret_create(
                State(s.clone()),
                Form(SecretCreateForm {
                    name: "yt-prod".into(),
                    value: value.into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);

            // Listing shows the name but never the value (write-only).
            let page = body_string(
                secrets_page(State(s.clone()))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            assert!(page.contains("yt-prod"));
            assert!(
                !page.contains(value),
                "secret value leaked into the listing"
            );

            // Stored at rest as ciphertext, not plaintext.
            let ct = state::secret_get_ciphertext(&s.api.pool, "yt-prod")
                .await
                .unwrap()
                .unwrap();
            assert_ne!(ct, value.as_bytes());

            // Duplicate create is rejected; nothing changes.
            let dup = secret_create(
                State(s.clone()),
                Form(SecretCreateForm {
                    name: "yt-prod".into(),
                    value: "other".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(dup.status(), StatusCode::UNPROCESSABLE_ENTITY);

            // Rotate replaces the value in place.
            let rotated = secret_rotate(
                State(s.clone()),
                AxumPath("yt-prod".to_string()),
                Form(SecretValueForm {
                    value: "perm:rotated".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(rotated.status(), StatusCode::SEE_OTHER);

            // Delete removes it.
            let deleted = secret_delete(State(s.clone()), AxumPath("yt-prod".to_string()))
                .await
                .unwrap()
                .into_response();
            assert_eq!(deleted.status(), StatusCode::SEE_OTHER);
            assert!(!state::secret_exists(&s.api.pool, "yt-prod").await.unwrap());

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }
    }

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
