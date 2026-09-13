//! Web UI served on the TCP listener. Phase 3.
//!
//! - Templates: Askama (compile-time checked).
//! - Interactivity: HTMX (vendored under `assets/htmx.min.js`, embedded into
//!   the binary via `include_bytes!` so the binary stays self-contained).
//! - Live log streaming: Server-Sent Events.
//! - Auth: loopback binds are unauthenticated by default; a non-loopback bind
//!   gates the web UI and `/api/*` behind a bearer token (CLAUDE-48, Phase 5).
//!   The token layer is applied to the TCP router in `daemon.rs` via
//!   `crate::auth::protect`; the `/webhooks/` prefix stays exempt.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crate::db::DbPool;
use askama::Template;
use axum::body::Body;
use axum::extract::{Form, Path as AxumPath, Query, RawForm, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::action::{
    Action, ActionKind, Automerge, ClaudeBlock, Concurrency, ForgejoTarget, GithubTarget, Limits,
    NativeConfig, YoutrackConfig, YoutrackSource,
};
use crate::api::AppState;
use crate::claude::ClaudeInvoker;
use crate::config::Paths;
use crate::state;

pub mod comment_webhooks;
pub mod native_forms;
pub mod webhooks;

/// CLAUDE-154: custom Askama filters. `{{ ts | datetime }}` renders a stored
/// RFC3339 UTC timestamp string through the live display preferences (timezone +
/// strftime format). Askama filters are free functions, so the preferences come
/// from the process-global snapshot kept current by `LiveSettings`. Malformed
/// input and sentinels (`running`, `never`, empty) pass through unchanged.
mod filters {
    pub fn datetime(value: &str) -> ::askama::Result<String> {
        Ok(crate::settings::format_datetime(
            value,
            &crate::settings::current_preferences(),
        ))
    }
}

const HTMX_JS: &[u8] = include_bytes!("../../assets/htmx.min.js");
// CLAUDE-139: idiomorph + its htmx extension, vendored so the live poll can
// morph the DOM in place instead of replacing it (preserves selection/scroll).
const IDIOMORPH_JS: &[u8] = include_bytes!("../../assets/idiomorph-ext.min.js");

#[derive(Clone)]
pub struct WebState {
    pub api: Arc<AppState>,
    pub paths: Paths,
    /// CLAUDE-61: whether the Forgejo webhook is configured
    /// (`[webhook].secret_ref` set). When true the web layer injects a synthetic
    /// read-only `webhook-forgejo` action so its runs are visible; when false
    /// the entry is absent (historical runs stay queryable on the dashboard).
    pub webhook_enabled: bool,
    /// CLAUDE-220: whether the `/claude` comment listener has at least one
    /// configured source. When true the web layer injects the synthetic
    /// read-only `webhook-forgejo-comment` action alongside `webhook-forgejo`.
    pub comment_webhook_enabled: bool,
}

pub fn router(
    api: Arc<AppState>,
    paths: Paths,
    webhook_enabled: bool,
    comment_webhook_enabled: bool,
) -> Router {
    let st = WebState {
        api,
        paths,
        webhook_enabled,
        comment_webhook_enabled,
    };
    Router::new()
        .route("/", get(dashboard))
        // CLAUDE-211: the new-action page is a kind picker; raw YAML is the
        // explicit escape hatch at `/actions/new/raw` rather than the default.
        .route("/actions/new", get(action_new_page))
        .route("/actions/new/raw", get(action_new_raw_page))
        // GET renders the structured new-action form; POST creates it. Kept under
        // `/actions/new/` so it never shadows the `/actions/:name` detail route
        // (a bare `/actions/youtrack` would mask the action named `youtrack`).
        .route(
            "/actions/new/youtrack",
            get(youtrack_new_page).post(youtrack_create),
        )
        .route(
            "/actions/new/forgejo-ci-fix",
            get(native_forms::forgejo_ci_fix_new_page).post(native_forms::forgejo_ci_fix_create),
        )
        .route(
            "/actions/new/ci-failure-analysis",
            get(native_forms::ci_failure_analysis_new_page)
                .post(native_forms::ci_failure_analysis_create),
        )
        .route(
            "/actions/new/ci-failure-reconcile",
            get(native_forms::ci_failure_reconcile_new_page)
                .post(native_forms::ci_failure_reconcile_create),
        )
        .route(
            "/actions/new/closed-issue-summary",
            get(native_forms::closed_issue_summary_new_page)
                .post(native_forms::closed_issue_summary_create),
        )
        .route("/actions", post(action_create))
        .route("/actions/:name", get(action_detail))
        .route("/actions/:name/durations", get(action_durations))
        // CLAUDE-235: the log page is global and lives at `/logs`. This route is
        // kept as a permanent redirect that carries the action into the Action
        // filter, so existing links and bookmarks keep working.
        .route("/actions/:name/events", get(action_events_redirect))
        .route(
            "/actions/:name/edit",
            get(action_edit_page).post(action_edit_save),
        )
        // CLAUDE-211: dispatches to the structured editor for the action's own
        // native kind (raw editor for `scripted`).
        .route(
            "/actions/:name/form",
            get(action_form_page).post(action_form_save),
        )
        .route("/actions/:name/export", get(action_export))
        .route("/actions/:name/delete", post(action_delete))
        // CLAUDE-235: the one diagnostics screen. Every `run_events` row, for
        // every action, undeduplicated and filterable; `?action=` narrows it.
        .route("/logs", get(logs))
        .route("/secrets", get(secrets_page).post(secret_create))
        .route("/secrets/:name/rotate", post(secret_rotate))
        .route("/secrets/:name/delete", post(secret_delete))
        // CLAUDE-216: on-demand access check. GET renders the page (no probing);
        // POST runs the check and swaps the result table in. The per-secret
        // Check button on /secrets lands on the same page with that credential's
        // identity probe already run.
        .route("/access", get(access_page))
        .route("/access/check", post(access_check))
        .route("/secrets/:name/check", post(secret_check))
        // CLAUDE-101: runtime settings page. GET renders all sections; POST saves
        // one section (re-rendering inline serde errors on rejection). Behind the
        // CLAUDE-48 auth gate like every other TCP route.
        .route("/settings", get(settings_page))
        .route("/settings/auth/claude", post(auth_save_claude))
        .route("/settings/auth/claude/login", post(auth_login_start))
        .route("/settings/auth/claude/code", post(auth_login_code))
        .route("/settings/auth/claude/cancel", post(auth_login_cancel))
        // CLAUDE-269: the shared not-ready banner fragment, polled from every
        // page's base template.
        .route("/settings/auth/claude/banner", get(auth_claude_banner))
        .route("/settings/auth/youtrack", post(auth_save_youtrack))
        .route("/settings/auth/forgejo", post(auth_save_forgejo))
        .route("/settings/:section", post(settings_save))
        // CLAUDE-117: historical reporting page (spend / runs / time-of-day /
        // day-of-week). Global by default; `action` + `range` query params narrow
        // it. The shared aggregations live in `crate::report`.
        .route("/reports", get(reports_page))
        // CLAUDE-129: About page (version + build info) with on-demand update
        // check / apply. The POST actions ride the existing CLAUDE-48 auth gate
        // like every other TCP route; they never restart the daemon.
        .route("/about", get(about_page))
        .route("/about/check", post(about_check))
        .route("/about/update", post(about_update))
        // CLAUDE-223: the Activity card's live quiet indicator polls this
        // fragment every 2s. Its own route (rather than an `hx-select` off
        // `/about`) so the busy / idle / unknown states are rendered by one
        // handler and an activity-query failure cannot be mistaken for idle.
        .route("/about/activity", get(about_activity))
        .route("/runs/:id", get(run_detail))
        .route("/runs/:id/jobs/:key/attempt/:attempt", get(job_stream_page))
        .route(
            "/sse/runs/:id/jobs/:key/attempt/:attempt",
            get(job_stream_sse),
        )
        .route("/static/htmx.min.js", get(htmx_handler))
        .route("/static/idiomorph-ext.min.js", get(idiomorph_handler))
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
    /// CLAUDE-61: a synthetic, read-only, event-driven action (the
    /// `webhook-forgejo` entry). The templates label it event-driven and hide
    /// the edit / delete / trigger controls, which do not apply. CLAUDE-153: the
    /// Pause / Resume controls DO apply and are shown (a paused webhook accepts
    /// and drops deliveries).
    synthetic: bool,
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
    /// CLAUDE-80: live pipeline stage of an in-flight run (webhook:
    /// download / triage / classify / mint), or NULL when the run records no
    /// stage info (scheduled runs, finished runs).
    stage: Option<String>,
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

#[derive(Serialize)]
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
    /// CLAUDE-52: the job's claude run hit the wall-clock limit (exit 124).
    /// Rendered with a distinct "timed out" badge instead of "fail", and
    /// excluded from the run's failed-job count.
    timed_out: bool,
    /// CLAUDE-61: free-text outcome summary. Webhook fast-path runs have no
    /// stream log, so this carries the triage bucket + mint result + redacted
    /// log artifact reference; rendered on the run-detail outcomes table.
    summary: Option<String>,
}

// CLAUDE-250: by hand rather than derived, because `ok` / `claude_ran` /
// `claude_ok` / `attempt_counted` / `timed_out` are integer 0/1 columns on both
// backends. See `crate::db::flag`.
impl sqlx::FromRow<'_, sqlx::any::AnyRow> for JobOutcomeRow {
    fn from_row(row: &sqlx::any::AnyRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            job_key: row.try_get("job_key")?,
            attempt: row.try_get("attempt")?,
            ts: row.try_get("ts")?,
            ok: crate::db::flag(row, "ok")?,
            claude_ran: crate::db::flag(row, "claude_ran")?,
            claude_ok: crate::db::flag(row, "claude_ok")?,
            claude_cost_usd: row.try_get("claude_cost_usd")?,
            attempt_counted: crate::db::flag(row, "attempt_counted")?,
            duration_ms: row.try_get("duration_ms")?,
            timed_out: crate::db::flag(row, "timed_out")?,
            summary: row.try_get("summary")?,
        })
    }
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

    /// CLAUDE-89: full summary rendered from Markdown to HTML for the
    /// view-details dialog on the run-detail page. The template injects this
    /// with `|safe`, so the output must be trusted; see
    /// [`render_summary_markdown`] for the raw-HTML escaping that makes it so.
    fn summary_html(&self) -> Option<String> {
        self.summary.as_deref().map(render_summary_markdown)
    }
}

/// CLAUDE-89: convert a job-outcome summary from Markdown to HTML. Raw HTML
/// events in the source are demoted to text (which `push_html` escapes), never
/// passed through: summaries are model output and must not inject markup into
/// the page.
fn render_summary_markdown(md: &str) -> String {
    use pulldown_cmark::{html, Event, Parser};
    let parser = Parser::new(md).map(|ev| match ev {
        Event::Html(s) | Event::InlineHtml(s) => Event::Text(s),
        other => other,
    });
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
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

async fn enriched_actions(
    pool: &DbPool,
    webhook_enabled: bool,
    comment_webhook_enabled: bool,
) -> Result<Vec<ActionView>, WebError> {
    let rows = state::action_index_list(pool).await?;
    let mut out = Vec::with_capacity(rows.len() + 1);
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
            synthetic: false,
        });
    }
    // CLAUDE-61: surface the event-driven webhook as a synthetic
    // read-only action when it is configured, so its runs get the same
    // dashboard / action-detail surfaces other actions have. It has no backing
    // YAML and is never indexed in the `actions` table; it lives only here.
    if webhook_enabled {
        out.push(synthetic_action_view(pool, webhooks::WEBHOOK_MINT_ACTION).await?);
    }
    // CLAUDE-220: the same treatment for the `/claude` comment listener, so both
    // event-driven listeners surface (and pause) like any other action.
    if comment_webhook_enabled {
        out.push(synthetic_action_view(pool, comment_webhooks::WEBHOOK_COMMENT_ACTION).await?);
    }
    Ok(out)
}

/// CLAUDE-61: build the synthetic `webhook-forgejo` action view. Read-only and
/// event-driven: no schedule, no file, always "enabled" (it fires on webhook
/// deliveries, not a timer). Its `last_run_started` comes from the recorded
/// webhook runs so the dashboard shows recency like any other action.
///
/// CLAUDE-153: unlike edit/export/delete (which stay rejected), this action
/// participates in Pause. Its `paused` state is read from the shared
/// `paused_actions` row like every other action, so the dashboard shows a
/// Paused badge and the Pause/Resume controls act on it; a paused delivery is
/// accepted-and-dropped by the inbound endpoint.
async fn synthetic_action_view(pool: &DbPool, name: &str) -> Result<ActionView, WebError> {
    let name = name.to_string();
    let last = state::action_last_run_started(pool, &name).await?;
    let paused = state::action_pause_status(pool, &name)
        .await?
        .map(|(at, r)| PauseInfo {
            paused_at: at,
            reason: r,
        });
    Ok(ActionView {
        name,
        file_path: "(synthetic, event-driven; no file)".to_string(),
        yaml_sha256: String::new(),
        enabled: true,
        schedule_kind: None,
        schedule_spec: None,
        last_seen_at: String::new(),
        parse_error: None,
        last_run_started: last,
        paused,
        synthetic: true,
    })
}

/// CLAUDE-61: true for the reserved synthetic action name. Mutating endpoints
/// (edit / export / delete) reject it: it has no backing YAML to operate on.
pub fn is_synthetic_action(name: &str) -> bool {
    SYNTHETIC_ACTIONS.contains(&name)
}

/// CLAUDE-61 / CLAUDE-220: every reserved synthetic action name, in ONE place.
/// The daemon's scan guard, the API's trigger / delete rejections, and the web
/// layer's edit / export / delete rejections all read this set, so a new
/// event-driven listener is reserved everywhere by adding one entry here.
pub const SYNTHETIC_ACTIONS: &[&str] = &[
    webhooks::WEBHOOK_MINT_ACTION,
    comment_webhooks::WEBHOOK_COMMENT_ACTION,
];

/// CLAUDE-220: true when `name` is a synthetic action whose listener is actually
/// configured this run, so the action-detail / durations pages render it instead
/// of 404ing. An unconfigured listener keeps its historical runs queryable but
/// has no live entry.
fn synthetic_enabled(s: &WebState, name: &str) -> bool {
    match name {
        n if n == webhooks::WEBHOOK_MINT_ACTION => s.webhook_enabled,
        n if n == comment_webhooks::WEBHOOK_COMMENT_ACTION => s.comment_webhook_enabled,
        _ => false,
    }
}

/// CLAUDE-61: a 403 response rejecting a mutating operation on the synthetic
/// action, or `None` when `name` is a normal action.
fn reject_synthetic(name: &str) -> Option<Response> {
    is_synthetic_action(name).then(|| {
        (
            StatusCode::FORBIDDEN,
            format!(
                "`{name}` is a synthetic event-driven action and cannot be edited, exported, or deleted"
            ),
        )
            .into_response()
    })
}

/// CLAUDE-230: the one definition of "the latest attempt of each job in this
/// run", parameterised by an outcome predicate on the `o` alias. Every
/// `failed_jobs` count and the Jobs-column outcome filter are built from it, so
/// the rows the filter selects and the `(N failed)` marker a row renders can
/// never drift apart. `pred` is always one of the fixed constants below, never
/// user input.
fn latest_job_attempts(pred: &str) -> String {
    format!(
        "FROM job_outcomes o WHERE o.run_id = runs.id AND {pred} AND o.attempt = (SELECT MAX(o2.attempt) FROM job_outcomes o2 WHERE o2.run_id = o.run_id AND o2.job_key = o.job_key)"
    )
}

/// A hard-failed job. Excludes timeouts, which CLAUDE-52 badges separately and
/// which the displayed failed count has always left out.
const JOB_FAILED: &str = "o.ok = 0 AND o.timed_out = 0";
/// A job whose `claude` hit the wall-clock limit and was SIGKILLed.
const JOB_TIMED_OUT: &str = "o.timed_out = 1";

/// Column list for every `RunRow` listing, including the derived `failed_jobs`
/// count that `templates/dashboard.html` renders as `(N failed)`.
fn run_row_columns() -> String {
    format!(
        "id, action_name, started_at, ended_at, exit_code, jobs_processed, triggered_by, current_job_key, stage, (SELECT COUNT(*) {}) AS failed_jobs",
        latest_job_attempts(JOB_FAILED)
    )
}

/// CLAUDE-230: `EXISTS` predicate for a run holding at least one job matching
/// `pred` on its latest attempt, for the Jobs-column outcome filter.
fn has_job_matching(pred: &str) -> String {
    format!("EXISTS (SELECT 1 {})", latest_job_attempts(pred))
}

/// CLAUDE-35: when `show_empty` is false, finished runs that processed zero jobs
/// are hidden. In-progress runs (`exit_code IS NULL`) are always kept so a
/// just-started run never momentarily vanishes (it can legitimately sit at
/// `jobs_processed = 0` before its first job lands). The filter applies before
/// the `LIMIT`/`OFFSET`, so up to `limit` matching runs are returned per page,
/// not `limit` minus the hidden rows.
///
/// CLAUDE-142: `limit`/`offset` drive `?page=` pagination on the dashboard.
///
/// CLAUDE-141: `view` carries the whitelisted sort + filter selection. Its
/// `ORDER BY` is built from a fixed column allow-list (no raw input reaches the
/// clause) and its filter values are passed as bound parameters.
async fn fetch_recent_runs(
    pool: &DbPool,
    limit: i64,
    offset: i64,
    show_empty: bool,
    view: &RunsView,
) -> Result<Vec<RunRow>, WebError> {
    let (where_clause, binds) = view.where_clause(show_empty);
    let order_by = view.order_by();
    let sql = format!(
        "SELECT {} FROM runs{where_clause} {order_by} LIMIT ? OFFSET ?",
        run_row_columns()
    );
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut q = sqlx::query_as::<_, RunRow>(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    let rows = q.bind(limit).bind(offset).fetch_all(pool).await?;
    Ok(rows)
}

/// CLAUDE-142 / CLAUDE-141: total number of dashboard runs under the active
/// `show_empty` toggle and sort/filter `view`, used to render the "X-Y of N"
/// indicator and the last-page boundary. Uses the same `WHERE` (and the same
/// bound filter values) as the listing so the count matches the paged rows.
async fn count_recent_runs(
    pool: &DbPool,
    show_empty: bool,
    view: &RunsView,
) -> Result<i64, WebError> {
    let (where_clause, binds) = view.where_clause(show_empty);
    let sql = format!("SELECT COUNT(*) FROM runs{where_clause}");
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut q = sqlx::query_scalar::<_, i64>(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    let total = q.fetch_one(pool).await?;
    Ok(total)
}

/// CLAUDE-141: distinct `action_name` values that appear in the `runs` table,
/// alphabetically. Drives the dashboard Action filter dropdown so it only offers
/// actions that actually have runs (not every indexed action), and serves as the
/// allow-list that whitelists the incoming `?action=` value.
async fn distinct_run_actions(pool: &DbPool) -> Result<Vec<String>, WebError> {
    let rows = sqlx::query_scalar::<_, String>(&crate::db::q(
        pool,
        "SELECT DISTINCT action_name FROM runs ORDER BY action_name",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-141: distinct `triggered_by` values present in the `runs` table,
/// alphabetically. Drives the dashboard Triggered filter dropdown and whitelists
/// the incoming `?triggered=` value.
async fn distinct_triggered_by(pool: &DbPool) -> Result<Vec<String>, WebError> {
    let rows = sqlx::query_scalar::<_, String>(&crate::db::q(
        pool,
        "SELECT DISTINCT triggered_by FROM runs ORDER BY triggered_by",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ---- run search (CLAUDE-143) ----------------------------------------------
//
// The Runs screen search finds jobs by content. The `summary` is always
// searched (cheap, indexed-free LIKE/instr over `job_outcomes`). When the
// opt-in `in_stream` toggle is set, the per-attempt stream `.jsonl` logs are
// also grepped server-side, so a failure whose only evidence is in the stream
// (the motivating "No space left on device" case) is findable. Matching is a
// case-insensitive ASCII substring for both, per the issue assumptions.

/// CLAUDE-143: cap on how many summary matches the search returns.
const SEARCH_SUMMARY_LIMIT: i64 = 200;
/// CLAUDE-143: max stream `.jsonl` files grepped when stream search is enabled.
/// Bounds the opt-in scan: only the most recent failed-job attempts (which carry
/// a `stream_log_path`) are searched, newest first, up to this many files.
const SEARCH_STREAM_MAX_FILES: usize = 200;
/// CLAUDE-143: max bytes read from a single stream file before giving up, so one
/// pathologically large log cannot stall the whole search.
const SEARCH_STREAM_MAX_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
struct JobSearchRow {
    run_id: i64,
    action_name: String,
    job_key: String,
    attempt: i64,
    ts: String,
    ok: bool,
    timed_out: bool,
    summary: Option<String>,
    stream_log_path: Option<String>,
}

// CLAUDE-250: by hand rather than derived; `ok` and `timed_out` are integer 0/1
// columns on both backends. See `crate::db::flag`.
impl sqlx::FromRow<'_, sqlx::any::AnyRow> for JobSearchRow {
    fn from_row(row: &sqlx::any::AnyRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(Self {
            run_id: row.try_get("run_id")?,
            action_name: row.try_get("action_name")?,
            job_key: row.try_get("job_key")?,
            attempt: row.try_get("attempt")?,
            ts: row.try_get("ts")?,
            ok: crate::db::flag(row, "ok")?,
            timed_out: crate::db::flag(row, "timed_out")?,
            summary: row.try_get("summary")?,
            stream_log_path: row.try_get("stream_log_path")?,
        })
    }
}

/// CLAUDE-143: one matching job attempt in the search results. Keyed in the
/// builder by `(run_id, job_key, attempt)` so a job matched in both its summary
/// and its stream collapses to a single row carrying both badges.
#[derive(Serialize)]
struct JobSearchHit {
    run_id: i64,
    action_name: String,
    job_key: String,
    attempt: i64,
    ts: String,
    ok: bool,
    timed_out: bool,
    matched_summary: bool,
    matched_stream: bool,
    /// A short, single-line context window around the first match.
    snippet: String,
}

/// CLAUDE-143: rendered search state threaded into the dashboard template.
#[derive(Serialize)]
struct SearchView {
    q: String,
    in_stream: bool,
    hits: Vec<JobSearchHit>,
    /// Stream search hit the file cap; older streams were not searched. Surfaced
    /// in the UI so results are never silently truncated.
    capped: bool,
    /// Number of stream files actually opened and grepped end to end (missing
    /// and unreadable files are not counted).
    files_searched: usize,
    /// CLAUDE-246: stream logs that opened but failed mid-read, so the search
    /// could not answer for those runs. Rendered separately: an unreadable log
    /// is not a clean "no match".
    unreadable: Vec<UnreadableStream>,
    /// The file cap (`SEARCH_STREAM_MAX_FILES`), surfaced for the capped note.
    max_files: usize,
}

/// CLAUDE-246: one stream log the search opened and then failed to read.
#[derive(Serialize)]
struct UnreadableStream {
    run_id: i64,
    job_key: String,
    attempt: i64,
    path: String,
    /// The underlying I/O error, shown so the operator can act on the cause.
    error: String,
}

/// CLAUDE-143: outcome of grepping a single stream log file.
#[derive(Debug)]
enum StreamGrep {
    /// File absent or could not be opened (path recorded but rotated/deleted) -
    /// skipped, not counted toward `files_searched`.
    Missing,
    /// CLAUDE-246: the file opened but a read failed partway through (truncated,
    /// corrupt, or an I/O error). Distinct from `Searched(None)`: only part of
    /// the file was scanned, so "no match" would be an answer the search never
    /// established.
    Unreadable(String),
    /// File was opened and scanned to EOF (or the byte cap); `Some(snippet)` on
    /// the first matching line.
    Searched(Option<String>),
}

/// CLAUDE-143: read a stream `.jsonl` log line-by-line (as `job_stream_sse`
/// does) looking for the first line containing `needle` (already ASCII
/// lowercased). Reads at most `max_bytes` before giving up so a huge log cannot
/// stall the search. A file that cannot be opened returns
/// [`StreamGrep::Missing`]; one that fails mid-read returns
/// [`StreamGrep::Unreadable`] (CLAUDE-246).
async fn stream_match_snippet(path: &std::path::Path, needle: &str, max_bytes: u64) -> StreamGrep {
    let Ok(file) = tokio::fs::File::open(path).await else {
        return StreamGrep::Missing;
    };
    let mut reader = BufReader::new(file).lines();
    let mut read: u64 = 0;
    loop {
        match reader.next_line().await {
            Ok(Some(line)) => {
                if line.to_ascii_lowercase().contains(needle) {
                    return StreamGrep::Searched(Some(match_snippet(&line, needle)));
                }
                read += line.len() as u64 + 1;
                if read >= max_bytes {
                    return StreamGrep::Searched(None);
                }
            }
            Ok(None) => return StreamGrep::Searched(None),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "stream log search: read failed; this run is reported unreadable, not unmatched"
                );
                return StreamGrep::Unreadable(e.to_string());
            }
        }
    }
}

/// CLAUDE-143: a short, single-line context window around the first
/// case-insensitive (ASCII) match of `needle` in `haystack`. `needle` is already
/// ASCII-lowercased. Control characters are flattened to spaces so a snippet
/// from a JSON log line stays on one row. Falls back to the leading chars when
/// no match is found (e.g. the SQL matched a field this view does not show).
fn match_snippet(haystack: &str, needle: &str) -> String {
    const BEFORE: usize = 40;
    const AFTER: usize = 100;
    let flat: String = haystack
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    // ASCII-lowercasing is length-preserving, so byte offsets in `lower` map
    // 1:1 onto `flat`.
    let lower = flat.to_ascii_lowercase();
    let Some(pos) = lower.find(needle) else {
        return truncate_chars(flat.trim(), BEFORE + AFTER);
    };
    let mut start = pos.saturating_sub(BEFORE);
    while start > 0 && !flat.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (pos + needle.len() + AFTER).min(flat.len());
    while end < flat.len() && !flat.is_char_boundary(end) {
        end += 1;
    }
    let mut out = String::new();
    if start > 0 {
        out.push('\u{2026}');
    }
    out.push_str(flat[start..end].trim());
    if end < flat.len() {
        out.push('\u{2026}');
    }
    out
}

/// CLAUDE-143: first `max` chars of `s`, with a trailing ellipsis when truncated.
fn truncate_chars(s: &str, max: usize) -> String {
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('\u{2026}');
    }
    out
}

/// CLAUDE-143: find jobs whose `summary` (always) - and, when `in_stream`, whose
/// per-attempt stream `.jsonl` log - contains `q` (case-insensitive ASCII
/// substring). Stream search is bounded to the most recent failed-job attempts
/// (`SEARCH_STREAM_MAX_FILES`, reading at most `SEARCH_STREAM_MAX_BYTES` each);
/// missing/rotated files are skipped without error, a file that fails mid-read
/// is reported in `unreadable` rather than counted as a non-match (CLAUDE-246),
/// and `capped` reports when the file cap was reached so the UI never silently
/// truncates.
async fn search_jobs(
    pool: &DbPool,
    paths: &Paths,
    q: &str,
    in_stream: bool,
) -> Result<SearchView, WebError> {
    let needle = q.to_ascii_lowercase();

    // Summary search (always on). `strpos(lower(...), needle)` is a plain
    // substring test, so query text containing LIKE metacharacters (% _) needs
    // no escaping. `lower()` is ASCII-only on SQLite, matching the ASCII needle.
    let summary_rows = sqlx::query_as::<_, JobSearchRow>(&crate::db::q(pool,
        "SELECT run_id, action_name, job_key, attempt, ts, ok, timed_out, summary, stream_log_path \
         FROM job_outcomes \
         WHERE summary IS NOT NULL AND strpos(lower(summary), ?) > 0 \
         ORDER BY id DESC LIMIT ?",
    ))
    .bind(&needle)
    .bind(SEARCH_SUMMARY_LIMIT)
    .fetch_all(pool)
    .await?;

    // Keyed by (run_id, job_key, attempt) so a job matched in both summary and
    // stream collapses to one row carrying both badges.
    let mut hits: std::collections::BTreeMap<(i64, String, i64), JobSearchHit> =
        std::collections::BTreeMap::new();

    for r in summary_rows {
        let snippet = r
            .summary
            .as_deref()
            .map(|s| match_snippet(s, &needle))
            .unwrap_or_default();
        let key = (r.run_id, r.job_key.clone(), r.attempt);
        hits.insert(
            key,
            JobSearchHit {
                run_id: r.run_id,
                action_name: r.action_name,
                job_key: r.job_key,
                attempt: r.attempt,
                ts: r.ts,
                ok: r.ok,
                timed_out: r.timed_out,
                matched_summary: true,
                matched_stream: false,
                snippet,
            },
        );
    }

    let mut files_searched = 0usize;
    let mut unreadable: Vec<UnreadableStream> = Vec::new();
    let mut capped = false;

    if in_stream {
        // Scope: the most recent failed-job attempts that recorded a stream log
        // path. Fetch one past the cap to detect truncation.
        let mut candidates = sqlx::query_as::<_, JobSearchRow>(&crate::db::q(pool,
            "SELECT run_id, action_name, job_key, attempt, ts, ok, timed_out, summary, stream_log_path \
             FROM job_outcomes \
             WHERE ok = 0 AND stream_log_path IS NOT NULL \
             ORDER BY id DESC LIMIT ?",
        ))
        .bind(SEARCH_STREAM_MAX_FILES as i64 + 1)
        .fetch_all(pool)
        .await?;

        if candidates.len() > SEARCH_STREAM_MAX_FILES {
            capped = true;
            candidates.truncate(SEARCH_STREAM_MAX_FILES);
        }

        for r in candidates {
            let Some(path) = r.stream_log_path.as_deref() else {
                continue;
            };
            // CLAUDE-253: the stored path is absolute and was recorded under
            // whatever layout the daemon ran with when the row was written. A
            // deployment that moved its state dir (the container's flat /state)
            // would otherwise search nothing for every pre-move row, silently
            // reporting no matches. Recompute from the CURRENT logs dir when the
            // recorded path is gone; the stream page already resolves this way.
            let recorded = std::path::PathBuf::from(path);
            let resolved = if recorded.exists() {
                recorded
            } else {
                ClaudeInvoker::stream_log_path(
                    &paths.logs_dir(),
                    &r.action_name,
                    &r.job_key,
                    r.attempt as u32,
                )
            };
            match stream_match_snippet(&resolved, &needle, SEARCH_STREAM_MAX_BYTES).await {
                StreamGrep::Missing => continue,
                StreamGrep::Unreadable(error) => {
                    // The resolved path, not the recorded one: on a moved
                    // deployment those differ and only the resolved file was
                    // actually opened.
                    unreadable.push(UnreadableStream {
                        run_id: r.run_id,
                        job_key: r.job_key.clone(),
                        attempt: r.attempt,
                        path: resolved.display().to_string(),
                        error,
                    });
                }
                StreamGrep::Searched(found) => {
                    files_searched += 1;
                    let Some(snippet) = found else { continue };
                    let key = (r.run_id, r.job_key.clone(), r.attempt);
                    hits.entry(key)
                        .and_modify(|h| h.matched_stream = true)
                        .or_insert_with(move || JobSearchHit {
                            run_id: r.run_id,
                            action_name: r.action_name,
                            job_key: r.job_key,
                            attempt: r.attempt,
                            ts: r.ts,
                            ok: r.ok,
                            timed_out: r.timed_out,
                            matched_summary: false,
                            matched_stream: true,
                            snippet,
                        });
                }
            }
        }
    }

    // Newest first by timestamp (ties broken by run id) for a stable order.
    let mut hits: Vec<JobSearchHit> = hits.into_values().collect();
    hits.sort_by(|a, b| b.ts.cmp(&a.ts).then(b.run_id.cmp(&a.run_id)));

    Ok(SearchView {
        q: q.to_string(),
        in_stream,
        hits,
        capped,
        files_searched,
        unreadable,
        max_files: SEARCH_STREAM_MAX_FILES,
    })
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
    /// CLAUDE-143: current search text, echoed into the search box so it
    /// survives the morph poll. Empty when no search is active.
    search_q: String,
    /// CLAUDE-143: current state of the "grep stream logs" toggle, echoed back
    /// into the checkbox.
    in_stream: bool,
    /// CLAUDE-143: `Some` when a search is active; the template then renders the
    /// matching jobs in place of the Recent runs table.
    search: Option<SearchView>,
    /// CLAUDE-142: pagination state for the Recent runs table (page number,
    /// bounds, and the "X-Y of N" indicator). Computed even while a search is
    /// active so the hidden `page` input keeps the morph poll on the right page.
    page: PageNav,
    /// CLAUDE-235: `warn`/`error` events in the last 24h. Zero renders no
    /// banner; the rows themselves live on `/logs`.
    events_24h: i64,
    /// CLAUDE-216 / CLAUDE-235: the repo-access subset of `events_24h` that no
    /// later successful preflight superseded, named separately in the banner
    /// because its remedy is the access check. Zero once a recovery lands.
    access_failure_events: i64,
    /// Distinct `(action, context, message)` access failures behind
    /// `access_failure_events`.
    access_failure_groups: usize,
    /// CLAUDE-225: dependency gates that have held an issue longer than
    /// [`state::STALE_GATE_AFTER_MS`]. Empty renders no banner.
    stale_gates: Vec<StaleGateRow>,
    /// CLAUDE-232: keys parked at their action's per-key attempt cap. Empty
    /// renders no banner.
    attempt_cap_hits: Vec<state::CappedKey>,
    /// CLAUDE-141: whitelisted sort + filter selection, echoed into the header
    /// sort links, filter dropdowns, poll URL, and pagination links so the
    /// chosen view survives the morph poll.
    view: RunsView,
    /// CLAUDE-141: distinct `action_name` values present in `runs`, for the
    /// Action filter dropdown options.
    action_options: Vec<String>,
    /// CLAUDE-141: distinct `triggered_by` values present in `runs`, for the
    /// Triggered filter dropdown options.
    triggered_options: Vec<String>,
}

/// CLAUDE-225: one stale dependency gate, with its ages pre-rendered for the
/// dashboard banner.
struct StaleGateRow {
    gate: state::StaleGate,
}

impl StaleGateRow {
    fn action_name(&self) -> &str {
        &self.gate.action_name
    }

    fn job_key(&self) -> &str {
        &self.gate.job_key
    }

    fn signature(&self) -> &str {
        &self.gate.signature
    }

    fn updated_at(&self) -> &str {
        &self.gate.updated_at
    }

    /// How long the issue has been continuously blocked.
    fn blocked_for(&self) -> String {
        format_duration_ms(self.gate.blocked_for_ms)
    }

    /// True once the gate's own re-check deadline has passed by more than the
    /// staleness threshold: the promised re-evaluation is not happening, which is
    /// a different fault from a dependency that is simply still open.
    fn recheck_overdue(&self) -> bool {
        self.gate.recheck_overdue_ms >= state::STALE_GATE_AFTER_MS
    }

    /// How long the re-check has been overdue (rendered only when it is).
    fn recheck_overdue_for(&self) -> String {
        format_duration_ms(self.gate.recheck_overdue_ms)
    }
}

/// CLAUDE-142: rendered pagination state for the dashboard Recent runs table.
struct PageNav {
    /// Current 1-based page (already clamped to `[1, total_pages]`).
    page: i64,
    /// Total number of pages (at least 1, even when there are no runs).
    total_pages: i64,
    /// Total matching runs under the active filter.
    total: i64,
    /// 1-based index of the first row on this page (0 when there are no rows).
    start: i64,
    /// 1-based index of the last row on this page (0 when there are no rows).
    end: i64,
    /// Whether a previous / next page exists (drives the disabled controls).
    has_prev: bool,
    has_next: bool,
}

/// CLAUDE-141: whitelisted sort + filter selection for the dashboard recent-runs
/// table. Every field is validated in [`dashboard`] before it lands here:
/// `sort`/`dir`/`exit`/`outcome` against fixed allow-lists, `action`/`triggered` against
/// the actual distinct DB values. So nothing user-controlled reaches the SQL
/// `ORDER BY`, the filter values are bound as parameters, and the strings echoed
/// back into query strings and the page are all known-safe.
#[derive(Default, Clone)]
struct RunsView {
    /// Whitelisted sort column key (`run`/`action`/`started`/`ended`/
    /// `duration`/`jobs`/`failed`/`exit`), or empty for the default
    /// `started_at DESC`.
    sort: String,
    /// `asc` or `desc`; empty (the default) sorts descending.
    dir: String,
    /// Exact `action_name` to filter to, or empty for all actions.
    action: String,
    /// Exact `triggered_by` to filter to, or empty for all.
    triggered: String,
    /// Derived exit/outcome filter: `success`/`failed`/`cancelled`/`running`,
    /// or empty for all. This is the RUN's own exit code.
    exit: String,
    /// CLAUDE-230: job-level outcome filter, independent of `exit`:
    /// `failed`/`timedout`, or empty for all. Named `outcome` and not `jobs`
    /// because the sort key `jobs` already means `jobs_processed`.
    outcome: String,
}

impl RunsView {
    /// SQL `ORDER BY` built from the whitelisted `sort`/`dir`. Defaults to
    /// `started_at DESC` (the pre-CLAUDE-141 order) when no column is selected.
    /// A trailing `id DESC` tiebreaker keeps paging stable when the primary key
    /// has ties. The column is chosen from a fixed `match`, so no user input
    /// ever reaches the clause text.
    fn order_by(&self) -> String {
        let col = match self.sort.as_str() {
            "run" => "id",
            "action" => "action_name",
            "ended" => "ended_at",
            // No duration column; derive it from the timestamps.
            "duration" => "elapsed_seconds(started_at, ended_at)",
            "jobs" => "jobs_processed",
            // CLAUDE-230: the derived count aliased by `run_row_columns`.
            "failed" => "failed_jobs",
            "exit" => "exit_code",
            // "started" and any unknown/empty value fall back to the default.
            _ => "started_at",
        };
        let dir = if self.dir == "asc" { "ASC" } else { "DESC" };
        if col == "id" {
            format!("ORDER BY {col} {dir}")
        } else {
            format!("ORDER BY {col} {dir}, id DESC")
        }
    }

    /// SQL `WHERE` clause (empty or leading ` WHERE ...`) shared by the listing
    /// and its `COUNT(*)`, with `?` placeholders for the value filters, plus the
    /// ordered bind values. `show_empty=false` keeps the CLAUDE-35 hide-empty
    /// rule and composes with the new filters via `AND`.
    fn where_clause(&self, show_empty: bool) -> (String, Vec<String>) {
        let mut conds: Vec<String> = Vec::new();
        let mut binds: Vec<String> = Vec::new();
        if !show_empty {
            conds.push("(jobs_processed > 0 OR exit_code IS NULL)".to_string());
        }
        if !self.action.is_empty() {
            conds.push("action_name = ?".to_string());
            binds.push(self.action.clone());
        }
        if !self.triggered.is_empty() {
            conds.push("triggered_by = ?".to_string());
            binds.push(self.triggered.clone());
        }
        // Derived outcome states, mirroring the Exit-column badges: 0 = success,
        // 1 = stopped by the global pause (CLAUDE-223), 130 = cancelled, any
        // other non-NULL = failed, NULL = still running. A paused run is
        // excluded from "failed": it did not fail, it was told to stop.
        match self.exit.as_str() {
            "success" => conds.push("exit_code = 0".to_string()),
            "cancelled" => conds.push("exit_code = 130".to_string()),
            "paused" => conds.push("exit_code = 1".to_string()),
            "failed" => {
                conds.push("exit_code IS NOT NULL AND exit_code NOT IN (0, 1, 130)".to_string())
            }
            "running" => conds.push("exit_code IS NULL".to_string()),
            _ => {}
        }
        // CLAUDE-230: job-level outcome, a different axis from the run's exit
        // code (a run whose every job failed still finishes 0). Built from the
        // same fragment as the displayed `(N failed)` count so the two agree.
        match self.outcome.as_str() {
            "failed" => conds.push(has_job_matching(JOB_FAILED)),
            "timedout" => conds.push(has_job_matching(JOB_TIMED_OUT)),
            _ => {}
        }
        let where_clause = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        (where_clause, binds)
    }

    /// Direction the `col` header should request next: flip to `desc` when that
    /// column is already the ascending sort, otherwise `asc`.
    fn dir_for(&self, col: &str) -> &'static str {
        if self.sort == col && self.dir == "asc" {
            "desc"
        } else {
            "asc"
        }
    }

    /// Sort-direction glyph for the `col` header, empty when not sorted by it.
    fn arrow(&self, col: &str) -> &'static str {
        arrow(&self.sort, &self.dir, col)
    }

    /// Full sort/filter query string for the live poll and pagination links, so
    /// each morph refresh and Prev/Next re-requests the current view. The
    /// search/show-empty params are carried separately via `hx-include` of the
    /// search form, and `page` via `hx-vals`; this carries only the CLAUDE-141
    /// params. Values are pre-whitelisted; askama escapes the `&` joiners.
    fn poll_query(&self) -> String {
        self.query_with(&self.sort, &self.dir)
    }

    /// Poll query with the sort set to `col` and its toggled direction: the
    /// target a header sort link points at.
    fn sort_query(&self, col: &str) -> String {
        self.query_with(col, self.dir_for(col))
    }

    fn query_with(&self, sort: &str, dir: &str) -> String {
        format!(
            "sort={sort}&dir={dir}&action={}&triggered={}&exit={}&outcome={}",
            self.action, self.triggered, self.exit, self.outcome,
        )
    }

    /// Every param except `omit`, for a filter `<select name="omit">` whose own
    /// value supplies the omitted param. Keeping it out of the URL avoids a
    /// duplicate query key (which axum's `Query` would reject).
    fn query_without(&self, omit: &str) -> String {
        [
            ("sort", self.sort.as_str()),
            ("dir", self.dir.as_str()),
            ("action", self.action.as_str()),
            ("triggered", self.triggered.as_str()),
            ("exit", self.exit.as_str()),
            ("outcome", self.outcome.as_str()),
        ]
        .iter()
        .filter(|(k, _)| *k != omit)
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
    }
}

/// CLAUDE-35: query params for the dashboard. `show_empty` defaults to `false`
/// when the param is absent, so the page loads with 0-job finished runs hidden.
#[derive(Deserialize, Default)]
struct DashboardQuery {
    #[serde(default)]
    show_empty: bool,
    /// CLAUDE-141: whitelisted sort column key for the Recent runs table.
    #[serde(default)]
    sort: String,
    /// CLAUDE-141: sort direction (`asc`/`desc`).
    #[serde(default)]
    dir: String,
    /// CLAUDE-141: Action filter (exact `action_name`), empty for all.
    #[serde(default)]
    action: String,
    /// CLAUDE-141: Triggered filter (exact `triggered_by`), empty for all.
    #[serde(default)]
    triggered: String,
    /// CLAUDE-141: derived Exit/outcome filter, empty for all.
    #[serde(default)]
    exit: String,
    /// CLAUDE-230: job-level outcome filter (`failed`/`timedout`), empty for
    /// all.
    #[serde(default)]
    outcome: String,
    /// CLAUDE-143: search text. Empty/absent leaves the normal runs view.
    #[serde(default)]
    q: String,
    /// CLAUDE-143: also grep the per-attempt stream logs (opt-in checkbox).
    #[serde(default)]
    in_stream: bool,
    /// CLAUDE-142: 1-based page for the Recent runs table. Absent/invalid falls
    /// back to page 1, preserving the pre-pagination first-page behavior. The
    /// value is clamped to the valid range in the handler.
    #[serde(default)]
    page: Option<i64>,
}

/// CLAUDE-142: rows per page on the dashboard Recent runs table. Matches the
/// previous hardcoded `LIMIT 20` so page 1 shows exactly the rows it did before.
const DASHBOARD_PAGE_SIZE: i64 = 20;

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
    let actions = enriched_actions(pool, s.webhook_enabled, s.comment_webhook_enabled).await?;

    // CLAUDE-141: whitelist the sort/filter selections before they reach SQL or
    // the page. `sort`/`dir`/`exit`/`outcome` are checked against fixed allow-lists; the
    // Action/Triggered values are kept only when they match an actual distinct
    // value in `runs` (the same lists that populate the dropdowns), so a stale
    // or hand-edited param drops to "all" instead of echoing arbitrary input.
    let action_options = distinct_run_actions(pool).await?;
    let triggered_options = distinct_triggered_by(pool).await?;
    let view = RunsView {
        sort: whitelisted(
            &q.sort,
            &[
                "run", "action", "started", "ended", "duration", "jobs", "failed", "exit",
            ],
        ),
        dir: if q.dir == "asc" { "asc" } else { "desc" }.to_string(),
        action: whitelist_member(&q.action, &action_options),
        triggered: whitelist_member(&q.triggered, &triggered_options),
        exit: whitelisted(
            &q.exit,
            &["success", "failed", "cancelled", "paused", "running"],
        ),
        outcome: whitelisted(&q.outcome, &["failed", "timedout"]),
    };

    // CLAUDE-142: page the Recent runs table. `total` is a COUNT over the same
    // `WHERE` (CLAUDE-141 sort/filter included) as the listing, so the page
    // bounds and the "X-Y of N" indicator line up with the rows actually
    // returned. The page is clamped to the valid range so a stale/oversized
    // `?page=` lands on the last page rather than an empty one.
    let total = count_recent_runs(pool, q.show_empty, &view).await?;
    let total_pages = ((total + DASHBOARD_PAGE_SIZE - 1) / DASHBOARD_PAGE_SIZE).max(1);
    let page = q.page.unwrap_or(1).clamp(1, total_pages);
    let offset = (page - 1) * DASHBOARD_PAGE_SIZE;
    let recent_runs =
        fetch_recent_runs(pool, DASHBOARD_PAGE_SIZE, offset, q.show_empty, &view).await?;
    let page_nav = PageNav {
        page,
        total_pages,
        total,
        start: if recent_runs.is_empty() {
            0
        } else {
            offset + 1
        },
        end: offset + recent_runs.len() as i64,
        has_prev: page > 1,
        has_next: page < total_pages,
    };

    // CLAUDE-143: run the content search when the box is non-empty. The result
    // region morph-swaps in place (poll-region) so the search composes with the
    // live refresh and the "Show empty runs" toggle without a refresh-wipe.
    let search_q = q.q.trim().to_string();
    let search = if search_q.is_empty() {
        None
    } else {
        Some(search_jobs(pool, &s.paths, &search_q, q.in_stream).await?)
    };

    // Today totals: sum across all actions
    let today = state::today_local().to_string();
    let totals: Option<(i64, f64)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT CAST(COALESCE(SUM(run_count), 0) AS BIGINT), COALESCE(SUM(cost_usd), 0.0) \
         FROM daily_ledger WHERE date = ?",
    ))
    .bind(&today)
    .fetch_optional(pool)
    .await?;
    let today_actions: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "SELECT COUNT(*) FROM daily_ledger WHERE date = ? AND run_count > 0",
    ))
    .bind(&today)
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let (today_runs, today_cost) = totals.unwrap_or((0, 0.0));

    // CLAUDE-216: the poll-time repo-access preflight leaves its issues Queued
    // with no field change, so without this banner the only symptom is issues
    // that never move. 24h window; a later passing preflight for the same repo
    // clears it (`state::access_failures_recent`).
    // CLAUDE-235: a count, not a table. The messages are on `/logs`; the
    // dashboard says how many there are and which of them the access check fixes.
    let since = events_window_start();
    let access_failures = state::access_failures_recent(pool, &since, None).await?;
    let events_24h = state::run_events_count_since(pool, &since).await?;

    // CLAUDE-225: a dependency-blocked issue stays Queued with no field change,
    // and a poll whose candidates are all blocked returns before `run_begin`, so
    // nothing reaches the run history either. Read the live gate table so a block
    // that outlives its promised re-evaluation is on screen instead of waiting
    // for someone to notice a stale issue by hand.
    let stale_gates: Vec<StaleGateRow> =
        state::dependency_gates_stale(pool, state::now_epoch_ms(), state::STALE_GATE_AFTER_MS, 20)
            .await?
            .into_iter()
            .map(|gate| StaleGateRow { gate })
            .collect();

    // CLAUDE-232: the sibling silent no-dispatch condition. A key at its
    // action's `max_attempts_per_key` is skipped on every poll with no outcome
    // row, no run event, and no field change, so the counter table is the only
    // live record that the queue has stopped moving. The cap is per action, so
    // resolve it from each indexed action's YAML (disk, hence `in_place`).
    let index_rows = state::action_index_list(pool).await?;
    let defaults = crate::job::RunnerDefaults::default();
    let caps = crate::blocking::in_place(|| crate::job::attempt_caps(&index_rows, &defaults));
    let attempt_cap_hits = state::attempt_cap_hits(
        pool,
        &caps,
        defaults.max_attempts_per_key,
        state::ATTEMPT_CAP_HITS_LIMIT,
    )
    .await?;

    let tpl = DashboardTpl {
        version: s.api.version,
        global_pause,
        today_runs: today_runs as u32,
        today_actions: today_actions as usize,
        today_cost,
        actions,
        recent_runs,
        show_empty: q.show_empty,
        search_q,
        in_stream: q.in_stream,
        search,
        page: page_nav,
        events_24h,
        access_failure_events: access_failures.iter().map(|f| f.count).sum(),
        access_failure_groups: access_failures.len(),
        stale_gates,
        attempt_cap_hits,
        view,
        action_options,
        triggered_options,
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
    /// CLAUDE-147: input/poll-phase diagnostics for this action (probe failures,
    /// no-VCS drops), which skip a project's issues before any job exists and so
    /// never reach the per-run outcomes.
    /// CLAUDE-235: a count, not a dump: how many were recorded since this serve
    /// started. Zero renders no region; the rows are on `/logs?action=<name>`.
    event_count: i64,
    /// CLAUDE-42: current state of the "Show empty runs" toggle. Threaded into
    /// the poll-region `hx-get` so the choice survives the 5s self-refresh.
    show_empty: bool,
}

/// CLAUDE-42: query params for the action detail page. Mirrors `DashboardQuery`:
/// `show_empty` defaults to `false` when the param is absent, so the Recent runs
/// table loads with 0-job finished runs hidden.
#[derive(Deserialize, Default)]
struct ActionDetailQuery {
    #[serde(default)]
    show_empty: bool,
}

async fn action_detail(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
    Query(q): Query<ActionDetailQuery>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    let actions = enriched_actions(pool, s.webhook_enabled, s.comment_webhook_enabled).await?;
    let Some(action) = actions.into_iter().find(|a| a.name == name) else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let paused = state::action_pause_status(pool, &name)
        .await?
        .map(|(at, r)| PauseInfo {
            paused_at: at,
            reason: r,
        });
    // CLAUDE-42: mirror the dashboard's "Show empty runs" toggle. When
    // `show_empty` is false, finished runs that processed zero jobs are hidden;
    // in-progress runs (`exit_code IS NULL`) stay visible. The filter applies
    // before the `LIMIT`, exactly as in `fetch_recent_runs`.
    let empty_filter = if q.show_empty {
        ""
    } else {
        " AND (jobs_processed > 0 OR exit_code IS NULL)"
    };
    let sql = format!(
        "SELECT {} FROM runs WHERE action_name = ?{empty_filter} ORDER BY started_at DESC LIMIT 50",
        run_row_columns()
    );
    let runs = sqlx::query_as::<_, RunRow>(&crate::db::q(pool, &sql))
        .bind(&name)
        .fetch_all(pool)
        .await?;
    // CLAUDE-147: input/poll-phase diagnostics (VCS probe failures, no-VCS
    // drops) recorded by `gather()`. Queried by action name independent of any
    // run row, so a zero-job poll's events are surfaced too.
    // CLAUDE-235: counted, not dumped. A diagnostic that re-fires every poll used
    // to fill the whole region with byte-identical rows; the page now states how
    // many there are and links to the one log screen for the rows themselves.
    let started_at = crate::settings::serve_started_at(pool).await?;
    let event_count =
        state::run_events_count_for_action(pool, &name, started_at.as_deref()).await?;
    let tpl = ActionDetailTpl {
        version: s.api.version,
        action,
        paused,
        runs,
        event_count,
        show_empty: q.show_empty,
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
    // CLAUDE-61: the synthetic webhook action is not indexed but charts its
    // recorded runs like any action, so accept it when webhook is configured.
    let known_synthetic = synthetic_enabled(&s, &name);
    if !known_synthetic && indexed_action_path(pool, &name).await?.is_none() {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    }
    // Same recent window as the action-detail table (last 50 by start). The
    // query returns newest-first; the chart wants oldest-first, so reverse.
    let runs = sqlx::query_as::<_, RunRow>(&crate::db::q(
        pool,
        &format!(
            "SELECT {} FROM runs WHERE action_name = ? ORDER BY started_at DESC LIMIT 50",
            run_row_columns()
        ),
    ))
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

// ---- logs page (CLAUDE-171, made global in CLAUDE-235) ---------------------
//
// The one diagnostics screen. It lists every `run_events` row with no dedup,
// filtered by whitelisted Level / Action / Context selections, a parameterized
// message substring search, and a since-this-serve / last-24h / all-time window,
// sorted by time and paginated. The dashboard and the action page only count;
// this page is the record. The filter/whitelist/HTMX shape is the dashboard's
// (`RunsView`, CLAUDE-141/142): every user-supplied value is validated
// server-side before it reaches SQL, and every control morph-swaps the table
// region in place.

/// CLAUDE-171: rows per page on the log table.
const EVENTS_PAGE_SIZE: i64 = 50;

/// CLAUDE-235: rows per grouped summary card on the logs page. A card is a
/// signal, not a second log; the table below carries the rest.
const EVENTS_SUMMARY_LIMIT: i64 = 10;

/// CLAUDE-216 / CLAUDE-235: start of the 24h diagnostics window shared by the
/// dashboard banner, the logs summary cards, and the logs page's `since=24h`
/// selection, so the three never disagree about what "last 24h" means.
fn events_window_start() -> String {
    (chrono::Utc::now() - chrono::Duration::hours(24))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// CLAUDE-171: distinct `level` values present in `run_events`, alphabetically.
/// Drives the Level dropdown and whitelists the incoming `?level=`.
async fn distinct_event_levels(pool: &DbPool) -> Result<Vec<String>, WebError> {
    let rows = sqlx::query_scalar::<_, String>(&crate::db::q(
        pool,
        "SELECT DISTINCT level FROM run_events ORDER BY level",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-171: distinct `action_name` values present in `run_events`,
/// alphabetically. Drives the Action dropdown and whitelists `?action=`. Note
/// this is the events table's own action column, not `runs`': an action can have
/// diagnostics without ever having produced a run row.
async fn distinct_event_actions(pool: &DbPool) -> Result<Vec<String>, WebError> {
    let rows = sqlx::query_scalar::<_, String>(&crate::db::q(
        pool,
        "SELECT DISTINCT action_name FROM run_events ORDER BY action_name",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-171: distinct non-null `context` values present in `run_events`,
/// alphabetically. Drives the Context dropdown and whitelists `?context=`.
/// `context` is free-form (a project key, or `config-collision`) and nullable;
/// NULL/empty is not a selectable value, so it is excluded from the list.
async fn distinct_event_contexts(pool: &DbPool) -> Result<Vec<String>, WebError> {
    let rows = sqlx::query_scalar::<_, String>(&crate::db::q(pool,
        "SELECT DISTINCT context FROM run_events WHERE context IS NOT NULL AND context <> '' ORDER BY context",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// CLAUDE-171: whitelisted filter + sort selection for the error-log table. The
/// `RunsView` sibling: every field is validated in [`logs`] before it
/// lands here (`dir`/`since` against fixed allow-lists, `level`/`action`/
/// `context` against the actual distinct DB values), so nothing user-controlled
/// reaches the SQL text and every string echoed back into the page or a query
/// string is known-safe. `q` is the one free-form field: it never reaches the
/// clause text either, only a bound `strpos()` parameter.
#[derive(Default, Clone)]
struct EventsView {
    /// `asc` or `desc`; empty (the default) sorts descending (newest first).
    dir: String,
    /// Exact `level` to filter to, or empty for all.
    level: String,
    /// Exact `action_name` to filter to, or empty for all actions.
    action: String,
    /// Exact `context` to filter to, or empty for all.
    context: String,
    /// `all` for all-time, `24h` for the last 24 hours (the window the summary
    /// cards and the dashboard banner count); empty (the default) bounds to the
    /// current serve.
    since: String,
    /// Free-text message substring (bound as a parameter, never interpolated).
    q: String,
}

impl EventsView {
    /// Whether the all-time window is selected (the since toggle's non-default).
    fn all_time(&self) -> bool {
        self.since == "all"
    }

    /// CLAUDE-235: whether the last-24h window is selected. It is what the
    /// summary cards and the dashboard banner link to, so a count and the rows
    /// behind it cover the same window.
    fn last_24h(&self) -> bool {
        self.since == "24h"
    }

    /// SQL `ORDER BY` for the When column. The only sortable column, so just the
    /// whitelisted direction; `id` breaks ties (several events share one poll
    /// `ts`) and keeps paging stable.
    fn order_by(&self) -> String {
        let dir = if self.dir == "asc" { "ASC" } else { "DESC" };
        format!("ORDER BY ts {dir}, id {dir}")
    }

    /// SQL `WHERE` clause (empty or leading ` WHERE ...`) shared by the listing
    /// and its `COUNT(*)`, with `?` placeholders for every value, plus the
    /// ordered binds. `started_at` bounds the default since-this-serve window;
    /// `None` (no serve has stamped the DB) leaves the window unbounded rather
    /// than hiding every row.
    fn where_clause(&self, started_at: Option<&str>) -> (String, Vec<String>) {
        let mut conds: Vec<String> = Vec::new();
        let mut binds: Vec<String> = Vec::new();
        if !self.level.is_empty() {
            conds.push("level = ?".to_string());
            binds.push(self.level.clone());
        }
        if !self.action.is_empty() {
            conds.push("action_name = ?".to_string());
            binds.push(self.action.clone());
        }
        if !self.context.is_empty() {
            conds.push("context = ?".to_string());
            binds.push(self.context.clone());
        }
        if !self.q.is_empty() {
            // Same plain substring test as the CLAUDE-143 run search:
            // `strpos` over a lowercased needle, so LIKE metacharacters (% _)
            // in the query text need no escaping. `lower()` is ASCII-only on
            // SQLite, matching the ASCII needle.
            conds.push("strpos(lower(message), ?) > 0".to_string());
            binds.push(self.q.to_ascii_lowercase());
        }
        // CLAUDE-235: three windows, one bound. `all` leaves it off, `24h` uses
        // the shared 24h start, and the default follows the current serve (an
        // unstamped DB leaves it unbounded rather than hiding every row).
        if self.last_24h() {
            conds.push("ts >= ?".to_string());
            binds.push(events_window_start());
        } else if !self.all_time() {
            if let Some(start) = started_at {
                conds.push("ts >= ?".to_string());
                binds.push(start.to_string());
            }
        }
        let where_clause = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        (where_clause, binds)
    }

    /// Direction the When header should request next: flip to `desc` when it is
    /// already the ascending sort, otherwise `asc`.
    fn dir_for_when(&self) -> &'static str {
        if self.dir == "asc" {
            "desc"
        } else {
            "asc"
        }
    }

    /// Sort-direction glyph for the When header. The table is always sorted by
    /// time, so the arrow is always shown.
    fn arrow_when(&self) -> &'static str {
        arrow("when", &self.dir, "when")
    }

    /// Full filter/sort query string for pagination and the poll refresh, so
    /// each request re-states the active view. `q` rides `hx-include` of the
    /// filter form and `page` rides `hx-vals`, exactly as on the dashboard.
    /// Values are pre-whitelisted; askama escapes the `&` joiners.
    fn filter_query(&self) -> String {
        self.query_without("")
    }

    /// The When header's target: the same view with the direction toggled.
    fn sort_query(&self) -> String {
        format!(
            "dir={}&level={}&action={}&context={}&since={}",
            self.dir_for_when(),
            self.level,
            self.action,
            self.context,
            self.since,
        )
    }

    /// Every param except `omit`, for a filter control whose own value supplies
    /// the omitted param. Keeping it out of the URL avoids a duplicate query key
    /// (which axum's `Query` would reject).
    fn query_without(&self, omit: &str) -> String {
        [
            ("dir", self.dir.as_str()),
            ("level", self.level.as_str()),
            ("action", self.action.as_str()),
            ("context", self.context.as_str()),
            ("since", self.since.as_str()),
        ]
        .iter()
        .filter(|(k, _)| *k != omit)
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
    }
}

/// CLAUDE-171: total `run_events` rows matching the active filter, for the page
/// bounds and the "X-Y of N" indicator. Counts over the same `WHERE` as
/// [`fetch_run_events`], so the two never disagree.
async fn count_run_events(
    pool: &DbPool,
    view: &EventsView,
    started_at: Option<&str>,
) -> Result<i64, WebError> {
    let (where_clause, binds) = view.where_clause(started_at);
    let sql = format!("SELECT COUNT(*) FROM run_events{where_clause}");
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut q = sqlx::query_scalar::<_, i64>(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    Ok(q.fetch_one(pool).await?)
}

/// CLAUDE-171: one page of `run_events` rows under the active filter. Every row
/// is returned as recorded: no dedup here, unlike the action-detail summary.
async fn fetch_run_events(
    pool: &DbPool,
    view: &EventsView,
    started_at: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<state::RunEventLogRecord>, WebError> {
    let (where_clause, binds) = view.where_clause(started_at);
    let order_by = view.order_by();
    let sql = format!(
        "SELECT run_id, ts, level, context, message, action_name FROM run_events{where_clause} {order_by} LIMIT ? OFFSET ?"
    );
    let sql = crate::db::q(pool, &sql).into_owned();
    let mut q = sqlx::query_as::<_, state::RunEventLogRecord>(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    Ok(q.bind(limit).bind(offset).fetch_all(pool).await?)
}

#[derive(Template)]
#[template(path = "logs.html")]
struct LogsTpl {
    version: &'static str,
    events: Vec<state::RunEventLogRecord>,
    page: PageNav,
    view: EventsView,
    level_options: Vec<String>,
    action_options: Vec<String>,
    context_options: Vec<String>,
    /// The current serve's start, for the since-toggle label. `None` renders the
    /// all-time-only degraded state (no serve has stamped this DB).
    started_at: Option<String>,
    /// CLAUDE-235: last-24h `warn`/`error` counts grouped by `context`, busiest
    /// first. Independent of the active filter: it is the way in to a filter,
    /// not a summary of one.
    by_context: Vec<state::RunEventCount>,
    /// CLAUDE-235: the same 24h counts grouped by `action_name`.
    by_action: Vec<state::RunEventCount>,
}

/// CLAUDE-171: query params for the logs page. CLAUDE-235: `action` stays
/// `Option` for symmetry with the other filters, but an absent param now means
/// every action (the page is global); an explicit `?action=<name>` narrows it,
/// which is what the redirect from `/actions/<name>/events` and the links from
/// the action page and the summary cards send.
#[derive(Deserialize, Default)]
struct LogsQuery {
    #[serde(default)]
    dir: String,
    #[serde(default)]
    level: String,
    action: Option<String>,
    #[serde(default)]
    context: String,
    #[serde(default)]
    since: String,
    #[serde(default)]
    q: String,
    #[serde(default)]
    page: Option<i64>,
}

/// CLAUDE-235: the log page moved from `/actions/:name/events` to the global
/// `/logs`. The old route stays as a permanent redirect that carries the action
/// into the Action filter, so links, bookmarks and old READMEs still land on the
/// same rows.
async fn action_events_redirect(AxumPath(name): AxumPath<String>) -> Redirect {
    Redirect::permanent(&format!("/logs?action={name}"))
}

async fn logs(State(s): State<WebState>, Query(q): Query<LogsQuery>) -> Result<Response, WebError> {
    let pool = &s.api.pool;

    // Whitelist every selection before it reaches SQL or the page. `dir`/`since`
    // check against fixed allow-lists; Level/Action/Context are kept only when
    // they match an actual distinct value in `run_events` (the same lists that
    // populate the dropdowns), so a stale or hand-edited param drops to "all"
    // instead of echoing arbitrary input.
    let level_options = distinct_event_levels(pool).await?;
    let context_options = distinct_event_contexts(pool).await?;
    let mut action_options = distinct_event_actions(pool).await?;
    // CLAUDE-235: an action the daemon has indexed (or the synthetic webhook
    // action) is a legitimate filter even with no events yet, since the action
    // page links here unconditionally. Anything else drops to "all" rather than
    // being echoed back into the page.
    if let Some(requested) = q.action.as_deref().filter(|a| !a.is_empty()) {
        if !action_options.iter().any(|a| a == requested)
            && (synthetic_enabled(&s, requested)
                || indexed_action_path(pool, requested).await?.is_some())
        {
            action_options.push(requested.to_string());
            action_options.sort();
        }
    }
    let view = EventsView {
        dir: if q.dir == "asc" { "asc" } else { "desc" }.to_string(),
        level: whitelist_member(&q.level, &level_options),
        // Absent or unknown => every action.
        action: whitelist_member(q.action.as_deref().unwrap_or(""), &action_options),
        context: whitelist_member(&q.context, &context_options),
        since: whitelisted(&q.since, &["all", "24h"]),
        q: q.q.clone(),
    };

    // CLAUDE-235: the summary cards, over a fixed 24h window at warn/error. They
    // are the entry point the dashboard banner sends the operator to, so they do
    // not narrow with the table's filter.
    let window = events_window_start();
    let by_context =
        state::run_events_grouped_by_context(pool, &window, EVENTS_SUMMARY_LIMIT).await?;
    let by_action =
        state::run_events_grouped_by_action(pool, &window, EVENTS_SUMMARY_LIMIT).await?;

    let started_at = crate::settings::serve_started_at(pool).await?;
    let total = count_run_events(pool, &view, started_at.as_deref()).await?;
    let total_pages = ((total + EVENTS_PAGE_SIZE - 1) / EVENTS_PAGE_SIZE).max(1);
    let page = q.page.unwrap_or(1).clamp(1, total_pages);
    let offset = (page - 1) * EVENTS_PAGE_SIZE;
    let events =
        fetch_run_events(pool, &view, started_at.as_deref(), EVENTS_PAGE_SIZE, offset).await?;
    let page_nav = PageNav {
        page,
        total_pages,
        total,
        start: if total == 0 { 0 } else { offset + 1 },
        end: offset + events.len() as i64,
        has_prev: page > 1,
        has_next: page < total_pages,
    };
    let tpl = LogsTpl {
        version: s.api.version,
        events,
        page: page_nav,
        view,
        level_options,
        action_options,
        context_options,
        started_at,
        by_context,
        by_action,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

// ---- reports page (CLAUDE-117) --------------------------------------------
//
// Historical spend / runs / time-of-day / day-of-week, rendered off the shared
// `crate::report` aggregations. Spend is the primary metric and renders first.
// Bars are CSS-width divs (no charting library, no divide-by-zero on empty
// windows); the percentage per row is precomputed against the series max here.

/// One daily spend row with a precomputed bar width (0-100, relative to the
/// window's busiest day).
#[derive(Serialize)]
struct DailyBar {
    date: String,
    cost: f64,
    runs: i64,
    pct: u32,
}

#[derive(Serialize)]
struct WeeklyBar {
    week: String,
    cost: f64,
    runs: i64,
    pct: u32,
}

#[derive(Serialize)]
struct HourBar {
    /// Zero-padded local hour, `"00".."23"`.
    hour: String,
    cost: f64,
    jobs: i64,
    runs: i64,
    pct: u32,
}

#[derive(Serialize)]
struct WeekdayBar {
    day: String,
    cost: f64,
    runs: i64,
    minutes: f64,
    pct: u32,
}

#[derive(Template)]
#[template(path = "reports.html")]
struct ReportsTpl {
    version: &'static str,
    /// Selected range param (`all` / `30d` / `90d`), for the selector state.
    range: String,
    /// Selected action filter, or `None` for global totals.
    action: Option<String>,
    /// Action names offered in the filter dropdown.
    actions: Vec<String>,
    summary: crate::report::Summary,
    daily: Vec<DailyBar>,
    weekly: Vec<WeeklyBar>,
    /// Always 24 buckets, `00..23`.
    hours: Vec<HourBar>,
    /// Always 7 buckets, Sunday first.
    weekdays: Vec<WeekdayBar>,
}

/// Query params for `/reports`: `range` (default all-time) and an optional
/// `action` filter (empty string = all actions).
#[derive(Deserialize, Default)]
struct ReportsQuery {
    #[serde(default)]
    range: Option<String>,
    #[serde(default)]
    action: Option<String>,
}

/// Bar width as a percentage of the series max, guarded against an all-zero
/// series so an empty window renders flat rather than dividing by zero.
fn bar_pct(value: f64, max: f64) -> u32 {
    if max <= 0.0 {
        0
    } else {
        ((value / max) * 100.0).round() as u32
    }
}

async fn reports_page(
    State(s): State<WebState>,
    Query(q): Query<ReportsQuery>,
) -> Result<Html<String>, WebError> {
    let pool = &s.api.pool;
    let range = crate::report::Range::from_param(q.range.as_deref().unwrap_or("all"));
    let action_filter = q.action.as_deref().filter(|a| !a.is_empty());

    let summary = crate::report::summary(pool, range, action_filter).await?;
    let daily_pts = crate::report::daily(pool, range, action_filter).await?;
    let weekly_pts = crate::report::weekly(pool, range, action_filter).await?;
    let hour_pts = crate::report::by_hour(pool, range, action_filter).await?;
    let weekday_pts = crate::report::by_weekday(pool, range, action_filter).await?;

    let daily_max = daily_pts.iter().map(|d| d.cost_usd).fold(0.0, f64::max);
    let daily = daily_pts
        .iter()
        .map(|d| DailyBar {
            date: d.date.clone(),
            cost: d.cost_usd,
            runs: d.run_count,
            pct: bar_pct(d.cost_usd, daily_max),
        })
        .collect();

    let weekly_max = weekly_pts.iter().map(|w| w.cost_usd).fold(0.0, f64::max);
    let weekly = weekly_pts
        .iter()
        .map(|w| WeeklyBar {
            week: w.week.clone(),
            cost: w.cost_usd,
            runs: w.run_count,
            pct: bar_pct(w.cost_usd, weekly_max),
        })
        .collect();

    let hour_max = hour_pts.iter().map(|h| h.cost_usd).fold(0.0, f64::max);
    let hours = hour_pts
        .iter()
        .map(|h| HourBar {
            hour: format!("{:02}", h.hour),
            cost: h.cost_usd,
            jobs: h.job_count,
            runs: h.run_starts,
            pct: bar_pct(h.cost_usd, hour_max),
        })
        .collect();

    let wd_max = weekday_pts.iter().map(|d| d.cost_usd).fold(0.0, f64::max);
    let weekdays = weekday_pts
        .iter()
        .map(|d| WeekdayBar {
            day: d.label.clone(),
            cost: d.cost_usd,
            runs: d.run_count,
            minutes: d.run_minutes,
            pct: bar_pct(d.cost_usd, wd_max),
        })
        .collect();

    let actions = state::action_index_list(pool)
        .await?
        .into_iter()
        .map(|a| a.name)
        .collect();

    let tpl = ReportsTpl {
        version: s.api.version,
        range: range.as_param().to_string(),
        action: action_filter.map(str::to_string),
        actions,
        summary,
        daily,
        weekly,
        hours,
        weekdays,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

// ---- action editor / import / export (CLAUDE-7) ---------------------------

/// CLAUDE-211: new-action kind picker.
#[derive(Template)]
#[template(path = "action_new.html")]
struct ActionNewTpl {
    version: &'static str,
    kinds: Vec<KindCard>,
}

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
async fn indexed_action_path(pool: &DbPool, name: &str) -> Result<Option<String>, WebError> {
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
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    let yaml = std::fs::read_to_string(&path).map_err(WebError::from)?;
    // CLAUDE-211: offer the structured editor for any parseable native kind (the
    // form route dispatches to that kind's editor). `scripted` has none, so it
    // stays on raw YAML.
    let form_url = match crate::action::validate_str(&yaml, &path) {
        Ok(loaded) if loaded.action.native.is_some() => Some(format!("/actions/{name}/form")),
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
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
    let Some(path) = indexed_action_path(&s.api.pool, &name).await? else {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    };
    // Validate through the same checks the loader/scanner uses. A broken
    // candidate is rejected inline and the on-disk file is left untouched.
    if let Err(e) = crate::action::validate_str(&form.yaml, &path) {
        return editor_error(s.api.version, false, Some(name), form.yaml, e.to_string());
    }
    std::fs::write(&path, form.yaml.as_bytes()).map_err(WebError::from)?;
    // Re-index synchronously so the redirect reflects the edit immediately,
    // instead of waiting for the daemon's next periodic scan (CLAUDE-39).
    crate::daemon::scan_actions(&s.api.pool, &s.paths.actions_dir())
        .await
        .map_err(WebError::from)?;
    Ok(Redirect::to(&format!("/actions/{name}")).into_response())
}

/// CLAUDE-211: the new-action page lists every [`ActionKind`] with a one-line
/// description and a link to its editor. Raw-YAML paste is an explicit
/// alternative (`/actions/new/raw`), not the default.
async fn action_new_page(State(s): State<WebState>) -> Result<Html<String>, WebError> {
    let tpl = ActionNewTpl {
        version: s.api.version,
        kinds: ActionKind::ALL.into_iter().map(kind_card).collect(),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?))
}

/// One row of the new-action kind picker.
struct KindCard {
    kind: &'static str,
    description: &'static str,
    url: &'static str,
    /// Label of the row's button: which editor the link opens.
    action_label: &'static str,
}

/// Describe a kind for the picker. The match is exhaustive, so a new
/// [`ActionKind`] cannot ship without deciding which editor creates it.
fn kind_card(kind: ActionKind) -> KindCard {
    let (description, url, action_label) = match kind {
        ActionKind::Youtrack => (
            "Work tracker issues end to end: pick up a queued YouTrack issue, run the agent on a \
             clone, open the PR, and drive the AI Agent state field.",
            "/actions/new/youtrack",
            "Structured form",
        ),
        ActionKind::ForgejoCiFix => (
            "Fix failing CI on the bot's own Forgejo PRs: clone the PR branch, run the agent, and \
             push a corrective commit when the diff stays inside the size gate.",
            "/actions/new/forgejo-ci-fix",
            "Structured form",
        ),
        ActionKind::CiFailureAnalysis => (
            "Comment a log-grounded root-cause analysis on each open CI-failure issue, from the \
             complete run log plus the failing workflow YAML.",
            "/actions/new/ci-failure-analysis",
            "Structured form",
        ),
        ActionKind::CiFailureReconcile => (
            "Close each open CI-failure issue whose failing job is green again on its latest run, \
             and collapse issues that share a root-cause key.",
            "/actions/new/ci-failure-reconcile",
            "Structured form",
        ),
        ActionKind::ClosedIssueSummary => (
            "Report rather than work: count and list every issue closed in a rolling window across \
             a set of projects, and file the summary as a subtask of a nominated parent.",
            "/actions/new/closed-issue-summary",
            "Structured form",
        ),
        ActionKind::Scripted => (
            "Deprecated generic pipeline (input.command -> filter -> pre -> prompt -> post). Its \
             fields are nu snippets, which a form cannot structure, so it is raw YAML only.",
            "/actions/new/raw",
            "Raw YAML",
        ),
    };
    KindCard {
        kind: kind.as_str(),
        description,
        url,
        action_label,
    }
}

/// The raw-YAML editor for a new action, reached from the kind picker.
async fn action_new_raw_page(State(s): State<WebState>) -> Result<Html<String>, WebError> {
    let tpl = ActionEditorTpl {
        version: s.api.version,
        heading: "New action".into(),
        post_url: "/actions".into(),
        yaml: NEW_ACTION_TEMPLATE.into(),
        error: None,
        is_new: true,
        name: None,
        form_url: Some("/actions/new".into()),
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
    // Index the new action synchronously so the redirect target resolves now,
    // rather than 404ing until the daemon's next periodic scan (CLAUDE-39).
    crate::daemon::scan_actions(&s.api.pool, &actions_dir)
        .await
        .map_err(WebError::from)?;
    Ok(Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response())
}

async fn action_export(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
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

/// CLAUDE-40: delete an action from the web UI. Mirrors `secret_delete`: 404s
/// an unknown action, renders a 409 when a run for it is in flight, otherwise
/// removes the YAML file + purges the derived index/control state (keeping run
/// history) and redirects to the dashboard, which no longer lists it.
async fn action_delete(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
    if indexed_action_path(&s.api.pool, &name).await?.is_none() {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    }
    let dispatched = s
        .api
        .in_flight
        .lock()
        .await
        .get(&name)
        .is_some_and(|&n| n > 0);
    if dispatched || state::action_has_active_run(&s.api.pool, &name).await? {
        return Ok((
            StatusCode::CONFLICT,
            format!("action `{name}` has a run in flight; cancel or wait before deleting"),
        )
            .into_response());
    }
    crate::api::delete_indexed_action(&s.api.pool, &name).await?;
    Ok(Redirect::to("/").into_response())
}

// ---- structured YouTrack action editor (CLAUDE-27) ------------------------
//
// A field-based form for the native `kind: youtrack` action, an alternative to
// the raw-YAML textarea above. The form owns the native config (youtrack source
// + forgejo target) plus the generic Action blocks it surfaces (workdir_base,
// link_template, schedule, claude, limits, concurrency, enabled); every other
// key the file carries (output, dedup_key, ...) is left untouched on save.
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
    /// Optional Assignee set on the issue at pickup (CLAUDE-131). Empty string
    /// maps to `None` (Assignee left untouched). A login, `me`, or `unassigned`.
    yt_assignee: String,
    /// Full-replacement agent prompt (CLAUDE-125). Prefilled with the saved
    /// override or, when none, the built-in default template (placeholders
    /// literal). Cleared or left equal to the default -> no `prompt` key saved.
    yt_prompt: String,
    state_queued: String,
    state_working: String,
    state_code_review: String,
    state_needs_review: String,
    state_failed: String,
    state_in_progress: String,
    /// AI Agent `done` label written after a successful automerge (CLAUDE-126).
    state_done: String,
    /// Standard `State` field value written after automerge (CLAUDE-126).
    state_state_done: String,
    /// Present ("on") when the automerge checkbox is checked (CLAUDE-126).
    automerge_enabled: Option<String>,
    /// Max total seconds to wait for the PR's checks (number-as-text). Blank ->
    /// the `Automerge` default (1800).
    automerge_wait_seconds: String,
    /// Seconds between CI status polls (number-as-text). Blank -> the `Automerge`
    /// default (30).
    automerge_check_interval_seconds: String,
    fj_base_url: String,
    fj_token_ref: String,
    /// PR-creation strategy (CLAUDE-103): `"branch"` (default) or `"agit"`.
    fj_pr_strategy: String,
    /// GitHub PAT secret name (CLAUDE-54). The form exposes only `token_ref`;
    /// `github.hosts` / `github.mcp_endpoint` stay raw-YAML-only and are
    /// preserved on save.
    github_token_ref: String,
    workdir_base: String,
    link_template: String,
    /// "", "interval", "cron", or "manual".
    schedule_kind: String,
    schedule_spec: String,
    claude_cwd: String,
    claude_allowed_tools: String,
    claude_permission_mode: String,
    /// CLAUDE-239: per-action `--model`; blank inherits the global
    /// `config.claude.model`.
    claude_model: String,
    /// CLAUDE-259: per-action `--effort`; blank inherits the global
    /// `config.claude.effort`.
    claude_effort: String,
    /// DEV-706: per-action `--autocompact`; blank inherits the global
    /// `config.claude.autocompact`, then the runner's bounded default.
    claude_autocompact: String,
    limit_max_attempts_per_key: String,
    limit_max_runs_per_day: String,
    limit_claude_timeout_seconds: String,
    limit_daily_budget_usd: String,
    /// DEV-705: dollars ONE run may spend; blank inherits the daily budget.
    limit_run_budget_usd: String,
    /// DEV-705: fraction of the per-run budget at which the run warns once.
    limit_budget_warn_fraction: String,
    /// CLAUDE-234: issues one run may create in the tracker.
    limit_max_issues_per_run: String,
    /// Per-run parallelism cap (number-as-text, like the `limit_*` fields).
    concurrency_max_parallel: String,
    /// Comma-separated job-field names composing the concurrency key.
    concurrency_key: String,
    /// Present ("on") when the enabled checkbox is checked, absent otherwise.
    enabled: Option<String>,
}

impl YoutrackEditForm {
    /// Whether the enabled checkbox was ticked (the browser omits an unchecked
    /// box, so absence means false).
    fn is_enabled(&self) -> bool {
        self.enabled.is_some()
    }

    /// Whether the automerge checkbox was ticked (CLAUDE-126). Like
    /// [`Self::is_enabled`], absence means the box was unchecked.
    fn is_automerge_enabled(&self) -> bool {
        self.automerge_enabled.is_some()
    }
}

/// Field defaults for a brand-new YouTrack action: blank identity, but the
/// query / custom field / state labels seeded from [`YoutrackConfig::default`]
/// so the form opens with the same values a bare `native: {}` resolves to.
fn youtrack_form_defaults() -> YoutrackEditForm {
    let c = YoutrackConfig::default();
    let conc = Concurrency::default();
    YoutrackEditForm {
        yt_query: c.youtrack.query,
        yt_custom_field: c.youtrack.custom_field,
        yt_prompt: crate::native::youtrack::default_youtrack_prompt(),
        state_queued: c.youtrack.states.queued,
        state_working: c.youtrack.states.working,
        state_code_review: c.youtrack.states.code_review,
        state_needs_review: c.youtrack.states.needs_review,
        state_failed: c.youtrack.states.failed,
        state_in_progress: c.youtrack.states.in_progress,
        state_done: c.youtrack.states.done,
        state_state_done: c.youtrack.states.state_done,
        automerge_wait_seconds: c.youtrack.automerge.wait_seconds.to_string(),
        automerge_check_interval_seconds: c.youtrack.automerge.check_interval_seconds.to_string(),
        concurrency_max_parallel: conc.max_parallel.to_string(),
        concurrency_key: conc.key.join(", "),
        fj_pr_strategy: c.forgejo.pr_strategy.as_str().to_string(),
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
    let f = |o: Option<f64>| o.map(|v| v.to_string()).unwrap_or_default();
    Some(YoutrackEditForm {
        name: a.name.clone(),
        yt_base_url: s(&yt.youtrack.base_url),
        yt_token_ref: s(&yt.youtrack.token_ref),
        yt_query: yt.youtrack.query.clone(),
        yt_custom_field: yt.youtrack.custom_field.clone(),
        yt_assignee: s(&yt.youtrack.assignee),
        yt_prompt: yt
            .youtrack
            .prompt
            .clone()
            .unwrap_or_else(crate::native::youtrack::default_youtrack_prompt),
        state_queued: yt.youtrack.states.queued.clone(),
        state_working: yt.youtrack.states.working.clone(),
        state_code_review: yt.youtrack.states.code_review.clone(),
        state_needs_review: yt.youtrack.states.needs_review.clone(),
        state_failed: yt.youtrack.states.failed.clone(),
        state_in_progress: yt.youtrack.states.in_progress.clone(),
        state_done: yt.youtrack.states.done.clone(),
        state_state_done: yt.youtrack.states.state_done.clone(),
        automerge_enabled: yt.youtrack.automerge.enabled.then(|| "on".to_string()),
        automerge_wait_seconds: yt.youtrack.automerge.wait_seconds.to_string(),
        automerge_check_interval_seconds: yt.youtrack.automerge.check_interval_seconds.to_string(),
        fj_base_url: s(&yt.forgejo.base_url),
        fj_token_ref: s(&yt.forgejo.token_ref),
        fj_pr_strategy: yt.forgejo.pr_strategy.as_str().to_string(),
        github_token_ref: s(&yt.github.token_ref),
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
        claude_model: s(&a.claude.model),
        claude_effort: s(&a.claude.effort),
        claude_autocompact: s(&a.claude.autocompact),
        limit_max_attempts_per_key: n(a.limits.max_attempts_per_key),
        limit_max_runs_per_day: n(a.limits.max_runs_per_day),
        limit_claude_timeout_seconds: n(a.limits.claude_timeout_seconds),
        limit_daily_budget_usd: a
            .limits
            .daily_budget_usd
            .map(|v| v.to_string())
            .unwrap_or_default(),
        limit_run_budget_usd: f(a.limits.run_budget_usd),
        limit_budget_warn_fraction: f(a.limits.budget_warn_fraction),
        limit_max_issues_per_run: n(a.limits.max_issues_per_run),
        concurrency_max_parallel: a.concurrency.max_parallel.to_string(),
        concurrency_key: a.concurrency.key.join(", "),
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
    // CLAUDE-131: blank Assignee box -> no `assignee` key (Assignee untouched);
    // any value is persisted as a literal `yt` assignee argument.
    youtrack.assignee = opt(&form.yt_assignee);
    // CLAUDE-125: store the prompt only as a real override. Normalize browser
    // CRLF to LF, trim, and compare to the built-in default: an empty box or one
    // left equal to the default writes no `prompt` key (stay on the built-in so
    // future default improvements flow through); anything else is persisted
    // verbatim as a full-replacement override.
    let prompt_norm = form.yt_prompt.replace("\r\n", "\n");
    let prompt_trim = prompt_norm.trim();
    youtrack.prompt = (!prompt_trim.is_empty()
        && prompt_trim != crate::native::youtrack::default_youtrack_prompt().trim())
    .then(|| prompt_trim.to_string());
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
    // CLAUDE-133: the form now owns the `done` / `state_done` labels and the
    // automerge block; a blank label falls back to its default.
    if let Some(v) = opt(&form.state_done) {
        states.done = v;
    }
    if let Some(v) = opt(&form.state_state_done) {
        states.state_done = v;
    }
    // CLAUDE-133: automerge comes from the form. A blank timing falls back to the
    // `Automerge` default; prune_yaml still drops an all-default block so an
    // untouched action writes no `automerge`.
    let am_default = Automerge::default();
    let pu64 = |label: &str, raw: &str, default: u64| -> Result<u64, String> {
        match raw.trim() {
            "" => Ok(default),
            t => t
                .parse::<u64>()
                .map_err(|_| format!("{label} must be a whole number (got `{t}`)")),
        }
    };
    youtrack.automerge = Automerge {
        enabled: form.is_automerge_enabled(),
        wait_seconds: pu64(
            "Automerge wait (seconds)",
            &form.automerge_wait_seconds,
            am_default.wait_seconds,
        )?,
        check_interval_seconds: pu64(
            "Automerge check interval (seconds)",
            &form.automerge_check_interval_seconds,
            am_default.check_interval_seconds,
        )?,
    };
    // CLAUDE-161: preserve a hand-written `dependencies` block. The structured
    // form has no dependency-gate UI, so read the existing
    // `native.youtrack.dependencies` sub-block off the parsed on-disk action
    // (before we overwrite the `native` key below) and carry it forward, so
    // editing any other field in the form does not silently reset the gate.
    // Missing / unparsable -> the default (enabled 5m gate); `prune_yaml` still
    // drops an all-default block so an untouched action writes no `dependencies`.
    youtrack.dependencies = map
        .get("native")
        .and_then(|n| n.get("youtrack"))
        .and_then(|y| y.get("dependencies"))
        .cloned()
        .and_then(|d| serde_yaml::from_value::<crate::action::Dependencies>(d).ok())
        .unwrap_or_default();
    // CLAUDE-214: same carry-forward for `write_projects` and `states.no_code`,
    // neither of which the structured form exposes. Without this, saving any
    // other field would silently narrow the agent's create scope back to the
    // triggering issue's project and reset the no-code label.
    youtrack.write_projects = map
        .get("native")
        .and_then(|n| n.get("youtrack"))
        .and_then(|y| y.get("write_projects"))
        .cloned()
        .and_then(|w| serde_yaml::from_value::<Vec<String>>(w).ok())
        .unwrap_or_default();
    youtrack.states.no_code = map
        .get("native")
        .and_then(|n| n.get("youtrack"))
        .and_then(|y| y.get("states"))
        .and_then(|s| s.get("no_code"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    // CLAUDE-54: preserve a hand-written `github` block. The form exposes only
    // `token_ref`, so read the existing `native.github` sub-block off the parsed
    // on-disk action (before we overwrite the `native` key below) to carry its
    // `hosts` / `mcp_endpoint` forward, then overlay `token_ref` from the form.
    // Missing or unparsable block -> defaults; `prune_yaml` still drops an
    // all-default github block so Forgejo-only actions write no `github`.
    let mut github = map
        .get("native")
        .and_then(|n| n.get("github"))
        .cloned()
        .and_then(|g| serde_yaml::from_value::<GithubTarget>(g).ok())
        .unwrap_or_default();
    github.token_ref = opt(&form.github_token_ref);
    let native = YoutrackConfig {
        youtrack,
        forgejo: ForgejoTarget {
            base_url: opt(&form.fj_base_url),
            token_ref: opt(&form.fj_token_ref),
            pr_strategy: crate::action::PrStrategy::from_form_value(&form.fj_pr_strategy),
        },
        github,
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
    let pf64 = |label: &str, raw: &str| -> Result<Option<f64>, String> {
        match raw.trim() {
            "" => Ok(None),
            t => t
                .parse::<f64>()
                .map(Some)
                .map_err(|_| format!("{label} must be a number (got `{t}`)")),
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
        run_budget_usd: pf64("Per-run budget (USD)", &form.limit_run_budget_usd)?,
        budget_warn_fraction: pf64("Budget warning fraction", &form.limit_budget_warn_fraction)?,
        max_issues_per_run: pu32("Max issues per run", &form.limit_max_issues_per_run)?,
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
        model: opt(&form.claude_model),
        effort: opt(&form.claude_effort),
        autocompact: opt(&form.claude_autocompact),
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

    // Concurrency. Blank max_parallel -> the default (1); non-numeric -> inline
    // error like the limit fields. The key splits on commas, trims, and drops
    // empties; a blank key falls back to the default tuple. When the result
    // equals `Concurrency::default()` the whole block is removed so freshly
    // created files stay minimal, matching how limits / claude / schedule omit
    // defaulted values.
    let max_parallel = match form.concurrency_max_parallel.trim() {
        "" => Concurrency::default().max_parallel,
        t => t
            .parse::<u32>()
            .map_err(|_| format!("Max parallel must be a whole number (got `{t}`)"))?,
    };
    let key: Vec<String> = {
        let parsed: Vec<String> = form
            .concurrency_key
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if parsed.is_empty() {
            Concurrency::default().key
        } else {
            parsed
        }
    };
    let concurrency = Concurrency { max_parallel, key };
    if concurrency == Concurrency::default() {
        map.remove("concurrency");
    } else {
        // Read-modify-write: overlay onto any existing `concurrency` mapping so
        // an unrelated future sub-field is preserved rather than blown away.
        let serialized = serde_yaml::to_value(&concurrency).map_err(|e| e.to_string())?;
        let merged = match (map.remove("concurrency"), serialized) {
            (Some(Value::Mapping(mut existing)), Value::Mapping(new)) => {
                for (k, v) in new {
                    existing.insert(k, v);
                }
                Value::Mapping(existing)
            }
            (_, v) => v,
        };
        if let Some(p) = prune_yaml(merged) {
            map.insert(Value::from("concurrency"), p);
        }
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
    for r in [
        &form.yt_token_ref,
        &form.fj_token_ref,
        &form.github_token_ref,
    ] {
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
            "/actions/new/youtrack".to_string(),
            "/actions/new/raw".to_string(),
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

/// CLAUDE-211: `/actions/:name/form` for any kind. Renders the structured
/// editor matching the action's own native config; an action with no structured
/// editor (`scripted`, or a file that no longer parses) falls back to the raw
/// editor.
async fn action_form_page(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
    if indexed_action_path(&s.api.pool, &name).await?.is_none() {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    }
    let Some((_, action)) = native_forms::indexed_action(&s, &name).await? else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    match action.native {
        Some(NativeConfig::Youtrack(_)) => youtrack_edit_page(State(s), AxumPath(name)).await,
        _ => native_forms::edit_page_for_kind(&s, name, &action).await,
    }
}

/// POST counterpart of [`action_form_page`]: decodes the body with the form type
/// of the action's own kind. The body is taken raw because the per-repo
/// `log_token_ref` rows post repeated keys, which `Form<T>` collapses.
async fn action_form_save(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
    RawForm(body): RawForm,
) -> Result<Response, WebError> {
    if let Some(r) = reject_synthetic(&name) {
        return Ok(r);
    }
    if indexed_action_path(&s.api.pool, &name).await?.is_none() {
        return Ok((StatusCode::NOT_FOUND, format!("unknown action `{name}`")).into_response());
    }
    let Some((path, action)) = native_forms::indexed_action(&s, &name).await? else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    match action.native {
        Some(NativeConfig::Youtrack(_)) => {
            let form: YoutrackEditForm = match serde_urlencoded::from_bytes(&body) {
                Ok(f) => f,
                Err(e) => {
                    return Ok((
                        StatusCode::BAD_REQUEST,
                        format!("malformed form submission: {e}"),
                    )
                        .into_response())
                }
            };
            youtrack_edit_save(State(s), AxumPath(name), Form(form)).await
        }
        _ => native_forms::edit_save_for_kind(&s, name, &path, &action, &body).await,
    }
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
    // Re-index synchronously so the redirect reflects the edit immediately,
    // instead of waiting for the daemon's next periodic scan (CLAUDE-39).
    crate::daemon::scan_actions(&s.api.pool, &s.paths.actions_dir())
        .await
        .map_err(WebError::from)?;
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
    // Index the new action synchronously so the redirect target resolves now,
    // rather than 404ing until the daemon's next periodic scan (CLAUDE-39).
    crate::daemon::scan_actions(&s.api.pool, &actions_dir)
        .await
        .map_err(WebError::from)?;
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

// ---- CLAUDE-216: access check --------------------------------------------
//
// One service (`crate::access`) backs this page, `POST /api/access/check`, and
// `claude-run access check`. A row names the secret, the probe, and the failure
// text; a secret VALUE is never read back here, exactly as on `/secrets`.

#[derive(Template)]
#[template(path = "access.html")]
struct AccessTpl {
    version: &'static str,
    /// The form's current values, echoed back so a check keeps its inputs.
    form: crate::access::AccessRequest,
    /// `None` until a check has been run (the bare GET).
    report: Option<crate::access::AccessReport>,
}

/// The result table alone, for the HTMX swap.
#[derive(Template)]
#[template(path = "access_rows.html")]
struct AccessRowsTpl {
    report: Option<crate::access::AccessReport>,
}

/// Run the check, rendering a setup failure as a report row rather than a 500:
/// "the daemon cannot invoke `yt` / `fj`" is itself the answer.
async fn run_access_check(
    s: &WebState,
    req: &crate::access::AccessRequest,
) -> crate::access::AccessReport {
    let base = crate::api::access_cred_base(&s.paths);
    match crate::access::check(&s.api.pool, &base, req).await {
        Ok(report) => report,
        Err(e) => crate::access::AccessReport::from_error(&e),
    }
}

async fn access_page(
    State(s): State<WebState>,
    Query(q): Query<crate::access::AccessRequest>,
) -> Result<Response, WebError> {
    let tpl = AccessTpl {
        version: s.api.version,
        form: q.normalized(),
        report: None,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

async fn access_check(
    State(s): State<WebState>,
    Form(form): Form<crate::access::AccessRequest>,
) -> Result<Response, WebError> {
    let report = run_access_check(&s, &form.normalized()).await;
    let tpl = AccessRowsTpl {
        report: Some(report),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

/// The per-secret Check button on `/secrets`: run the identity probe of every
/// action that references this secret, and land on `/access` with the result.
async fn secret_check(
    State(s): State<WebState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Response, WebError> {
    let req = crate::access::AccessRequest {
        secret: Some(name),
        ..Default::default()
    };
    let report = run_access_check(&s, &req).await;
    let tpl = AccessTpl {
        version: s.api.version,
        form: req,
        report: Some(report),
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

// ---- CLAUDE-101: Settings page -------------------------------------------

#[derive(Template)]
#[template(path = "settings.html")]
struct SettingsTpl {
    version: &'static str,
    sections: Vec<SettingsSectionView>,
    saved: Option<String>,
    auth: AuthCardView,
}

/// What the Auth card shows. Derived from the `config.auth` document alone, so
/// rendering the page never decrypts a secret; "configured" means a credential
/// is named and its non-secret half is present.
struct AuthCardView {
    anthropic_key: bool,
    youtrack_base_url: Option<String>,
    forgejo_host: Option<String>,
    error: Option<String>,
    /// Whether a Claude credential exists at all (an OAuth login in the agent
    /// config dir, a stored API key, or the env). This is what gates the
    /// scheduler, so the card says so plainly.
    claude_ready: bool,
    /// CLAUDE-269: a credential exists but the CLI itself reported an
    /// authentication failure (an expired/revoked login), distinct from never
    /// having signed in at all. Takes the badge over `claude_ready`.
    claude_login_expired: bool,
    /// The CLI's own message when `claude_login_expired` is set.
    claude_login_message: Option<String>,
    /// An interactive sign-in waiting for its code.
    login_url: Option<String>,
    /// The CLI's own message after a refused code, shown above the field.
    login_error: Option<String>,
}

struct SettingsSectionView {
    path: &'static str,
    title: &'static str,
    apply_badge: &'static str,
    json: String,
    error: Option<String>,
    fields: Vec<FieldDoc>,
}

struct FieldDoc {
    name: &'static str,
    badge: &'static str,
    note: &'static str,
}

#[derive(Deserialize)]
struct SettingsForm {
    json: String,
}

/// Static per-field apply-semantics reference shown on each section card.
fn section_fields(section: crate::settings::Section) -> Vec<FieldDoc> {
    use crate::settings::Section::*;
    match section {
        // Rendered by the Auth card at the top of the page, not the generic JSON
        // editor: the values are typed into password fields and never echoed.
        Auth => vec![],
        Mint => vec![
            FieldDoc {
                name: "repo_project_map",
                badge: "next event",
                note: "owner/repo -> YouTrack project override.",
            },
            FieldDoc {
                name: "fallback_project",
                badge: "next event",
                note: "project for unclaimed repos.",
            },
            FieldDoc {
                name: "bot_logins",
                badge: "next event",
                note: "trigger logins that never mint (loop safety).",
            },
            FieldDoc {
                name: "human_assignee",
                badge: "next event",
                note: "bucket-2 assignee login; null leaves unassigned.",
            },
            FieldDoc {
                name: "target_state",
                badge: "next event",
                note: "State set on a minted issue.",
            },
            FieldDoc {
                name: "ai_agent_field",
                badge: "next event",
                note: "AI Agent custom-field name.",
            },
            FieldDoc {
                name: "queued_value",
                badge: "next event",
                note: "AI Agent value for a code-fix issue.",
            },
        ],
        Update => vec![
            FieldDoc {
                name: "train",
                badge: "next invocation",
                note: "release | latest.",
            },
            FieldDoc {
                name: "token_ref",
                badge: "next invocation",
                note: "secret name for the registry token; null = anonymous-first.",
            },
        ],
        Webhook => vec![
            FieldDoc {
                name: "replay_window_secs",
                badge: "restart",
                note: "shared replay window; the replay store is built once at start.",
            },
            FieldDoc {
                name: "sources",
                badge: "live",
                note: "per-source secret_ref, allowed_repos, rate_limit_per_min, log_token_ref.",
            },
        ],
        WebhookComment => vec![
            FieldDoc {
                name: "trigger",
                badge: "live",
                note: "command prefix the first line of a PR comment must open with (default /claude).",
            },
            FieldDoc {
                name: "max_instruction_chars",
                badge: "live",
                note: "longer instructions are refused with a reply naming the limit.",
            },
            FieldDoc {
                name: "sources",
                badge: "live",
                note: "per-source secret_ref, allowed_repos, rate_limit_per_min, write_token_ref (WRITE scope), allowed_permissions, max_files, max_lines.",
            },
        ],
        LogFilter => vec![FieldDoc {
            name: "filter",
            badge: "live",
            note: "tracing EnvFilter directive; CLAUDE_RUN_LOG still wins this run.",
        }],
        WorkdirGc => vec![
            FieldDoc {
                name: "enabled",
                badge: "next sweep",
                note: "master toggle; false parks the sweeper without losing the windows.",
            },
            FieldDoc {
                name: "retention_days",
                badge: "next sweep",
                note: "age past which a preserved / aside workdir is deleted.",
            },
            FieldDoc {
                name: "unpushed_hard_cap_days",
                badge: "next sweep",
                note: "longer age past which even unpushed-commit workdirs are deleted.",
            },
        ],
        StreamLogGc => vec![
            FieldDoc {
                name: "enabled",
                badge: "next sweep",
                note: "master toggle; false parks the stream-log sweeper without losing the window.",
            },
            FieldDoc {
                name: "retention_days",
                badge: "next sweep",
                note: "age past which logs/<action>/streams/<key>-attempt-<n>.jsonl is deleted; a job still in flight keeps its log at any age.",
            },
            FieldDoc {
                name: "daemon_log_max_bytes",
                badge: "next sweep",
                note: "size ceiling for logs/daemon.log; past it the file rolls to daemon.log.1 and keeps its newest half. 0 = unbounded.",
            },
            FieldDoc {
                name: "output_file_max_bytes",
                badge: "next sweep",
                note: "size ceiling for each logs/<action>/<output.file>; bounded by size, never deleted by age, because it is an operator-configured sink. 0 = unbounded.",
            },
            FieldDoc {
                name: "usage_log_max_bytes",
                badge: "next sweep",
                note: "size ceiling for logs/usage.jsonl, the per-run token and cost record; past it the file rolls to usage.jsonl.1 and keeps its newest whole records. 0 = unbounded.",
            },
            FieldDoc {
                name: "snippet_retention_days",
                badge: "next sweep",
                note: "age past which snippets/<action>/<job_key>/ is deleted; a job still in flight keeps its snippet directory at any age.",
            },
            FieldDoc {
                name: "webhook_log_retention_days",
                badge: "next sweep",
                note: "age past which a file directly under webhook-logs/ is deleted; the reused webhook-logs/triage/ working directory is never swept.",
            },
        ],
        Preferences => vec![
            FieldDoc {
                name: "locale",
                badge: "live",
                note:
                    "BCP-47 locale (e.g. fr-FR); localizes month/weekday/AM-PM names (%A %a %B %b %p).",
            },
            FieldDoc {
                name: "timezone",
                badge: "live",
                note: "IANA name (e.g. America/New_York); all displayed dates convert into it.",
            },
            FieldDoc {
                name: "datetime_format",
                badge: "live",
                note: "chrono strftime pattern; use a preset button below or edit freely.",
            },
        ],
        Concurrency => vec![
            FieldDoc {
                name: "max_parallel_global",
                badge: "next tick",
                note: "global cap on concurrent jobs across all actions; 0 = auto (max enabled max_parallel).",
            },
            FieldDoc {
                name: "regather_interval_secs",
                badge: "next tick",
                note: "cadence (seconds) the scheduler starts overlapping pickup runs; >= 1.",
            },
        ],
        Claude => vec![
            FieldDoc {
                name: "model",
                badge: "next invocation",
                note: "default --model for every action that pins no claude.model; null = the claude CLI's own default. Passed through unvalidated (sonnet, opus, claude-sonnet-4-5-20250929, ...).",
            },
            FieldDoc {
                name: "effort",
                badge: "next invocation",
                note: "default --effort for every action that pins no claude.effort; null = the claude CLI's own default. low, medium, high, xhigh, max; another value is warned about and still passed through.",
            },
        ],
        DbRetention => vec![
            FieldDoc {
                name: "enabled",
                badge: "next sweep",
                note: "master toggle; false parks the history-table prune without losing the window.",
            },
            FieldDoc {
                name: "retention_days",
                badge: "next sweep",
                note: "age past which job_outcomes / run_events / ci_failure_runs rows are pruned; must be >= 90 (the longest report range).",
            },
        ],
    }
}

/// Build the full settings view. `flash` carries a just-saved section name or a
/// just-rejected section's submitted-but-invalid JSON + serde error.
async fn render_settings(
    s: &WebState,
    status: StatusCode,
    saved: Option<String>,
    rejected: Option<(crate::settings::Section, String, String)>,
) -> Result<Response, WebError> {
    render_settings_with(s, status, saved, rejected, None).await
}

async fn render_settings_with(
    s: &WebState,
    status: StatusCode,
    saved: Option<String>,
    rejected: Option<(crate::settings::Section, String, String)>,
    auth_error: Option<String>,
) -> Result<Response, WebError> {
    render_settings_full(s, status, saved, rejected, auth_error, None).await
}

async fn render_settings_full(
    s: &WebState,
    status: StatusCode,
    saved: Option<String>,
    rejected: Option<(crate::settings::Section, String, String)>,
    auth_error: Option<String>,
    login_error: Option<String>,
) -> Result<Response, WebError> {
    let mut sections = Vec::with_capacity(crate::settings::Section::ALL.len());
    for section in crate::settings::Section::ALL {
        // The Auth section has its own card with real password inputs; a JSON
        // textarea would invite pasting a token into a stored document.
        if section == crate::settings::Section::Auth {
            continue;
        }
        let (json, error) = match &rejected {
            Some((sec, submitted, msg)) if *sec == section => {
                (submitted.clone(), Some(msg.clone()))
            }
            _ => (s.api.settings.section_json(section).await, None),
        };
        sections.push(SettingsSectionView {
            path: section.as_path(),
            title: section.title(),
            apply_badge: section.apply_badge(),
            json,
            error,
            fields: section_fields(section),
        });
    }
    let auth_cfg = s.api.settings.auth_config().await;
    let readiness_check = s.api.readiness.as_ref().map(|g| g.check());
    let tpl = SettingsTpl {
        version: s.api.version,
        sections,
        saved,
        auth: AuthCardView {
            claude_ready: readiness_check.as_ref().map(|r| r.ready).unwrap_or(true),
            // CLAUDE-269: distinct from "never signed in" so the badge and the
            // card above the sign-in control read "login expired" (with the
            // CLI's own message) rather than the plain "required" state.
            claude_login_expired: readiness_check
                .as_ref()
                .and_then(|r| r.kind)
                .map(|k| k == crate::readiness::NotReadyKind::LoginRequired)
                .unwrap_or(false),
            claude_login_message: readiness_check
                .as_ref()
                .and_then(|r| r.login_message.clone()),
            login_url: s
                .api
                .claude_login
                .as_ref()
                .and_then(|b| b.view())
                .map(|v| v.url),
            login_error,
            anthropic_key: auth_cfg.anthropic_key_ref.is_some(),
            youtrack_base_url: auth_cfg
                .youtrack_token_ref
                .is_some()
                .then_some(auth_cfg.youtrack_base_url)
                .flatten(),
            forgejo_host: auth_cfg
                .forgejo_token_ref
                .is_some()
                .then_some(auth_cfg.forgejo_host)
                .flatten(),
            error: auth_error,
        },
    };
    Ok((status, Html(tpl.render()?)).into_response())
}

/// `?saved=<what>` renders the confirmation banner after an Auth-card save
/// redirects back here.
#[derive(Deserialize)]
struct SettingsQuery {
    saved: Option<String>,
}

async fn settings_page(
    State(s): State<WebState>,
    Query(q): Query<SettingsQuery>,
) -> Result<Response, WebError> {
    render_settings(&s, StatusCode::OK, q.saved, None).await
}

async fn settings_save(
    State(s): State<WebState>,
    AxumPath(section): AxumPath<String>,
    Form(form): Form<SettingsForm>,
) -> Result<Response, WebError> {
    let Some(sec) = crate::settings::Section::from_path(&section) else {
        return Err(WebError::not_found(format!(
            "unknown settings section `{section}`"
        )));
    };
    match s.api.settings.save_section(sec, &form.json).await {
        Ok(()) => Ok(Redirect::to("/settings").into_response()),
        Err(crate::settings::SaveError::Invalid(msg)) => {
            render_settings(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                Some((sec, form.json, msg)),
            )
            .await
        }
        Err(crate::settings::SaveError::Db(e)) => Err(e.into()),
    }
}

// ---- Settings: service auth ------------------------------------------------
//
// The daemon's own logins to Claude, YouTrack and Forgejo, typed into the
// Settings page instead of a CLI. Each handler stores the secret encrypted under
// a fixed name, records the non-secret half in the `config.auth` document, and
// re-materializes the credential files the CLIs read (see
// `crate::service_auth`). A value is never rendered back, only the fact that it
// is set.

#[derive(Deserialize)]
struct AuthClaudeForm {
    api_key: String,
}

#[derive(Deserialize)]
struct AuthYoutrackForm {
    base_url: String,
    token: String,
}

#[derive(Deserialize)]
struct AuthForgejoForm {
    host: String,
    token: String,
}

/// Store `value` under `name`, creating or rotating as needed. The caller has
/// already checked the value is non-empty.
async fn put_secret(s: &WebState, name: &str, value: &str) -> std::result::Result<(), String> {
    let key = crate::secrets::SecretsKey::load().map_err(|e| e.to_string())?;
    let ciphertext = key.encrypt(value).map_err(|e| e.to_string())?;
    let exists = state::secret_exists(&s.api.pool, name)
        .await
        .map_err(|e| e.to_string())?;
    if exists {
        state::secret_rotate(&s.api.pool, name, &ciphertext)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        state::secret_create(&s.api.pool, name, &ciphertext)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Persist `cfg` as the `config.auth` document and re-materialize the credential
/// files from it, so the next `yt` / `fj` / `claude` child sees the new login
/// with no restart.
async fn save_auth_config(
    s: &WebState,
    cfg: &crate::config::AuthConfig,
) -> std::result::Result<(), String> {
    let json = serde_json::to_string(cfg).map_err(|e| e.to_string())?;
    s.api
        .settings
        .save_section(crate::settings::Section::Auth, &json)
        .await
        .map_err(|e| match e {
            crate::settings::SaveError::Invalid(m) => m,
            crate::settings::SaveError::Db(e) => e.to_string(),
        })?;
    let (_, api_key) =
        crate::service_auth::materialize(&s.api.pool, &s.api.paths, cfg, &s.api.fj_data_home)
            .await
            .map_err(|e| e.to_string())?;
    if let Some(handle) = &s.api.anthropic_key {
        match handle.write() {
            Ok(mut slot) => *slot = api_key,
            // A poisoned lock means a panic already happened elsewhere; say so
            // rather than let the key silently fail to apply.
            Err(e) => return Err(format!("applying the Anthropic key: {e}")),
        }
    }
    Ok(())
}

async fn auth_save_claude(
    State(s): State<WebState>,
    Form(form): Form<AuthClaudeForm>,
) -> Result<Response, WebError> {
    let key = form.api_key.trim().to_string();
    if key.is_empty() {
        return render_settings_with(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            None,
            None,
            Some("an Anthropic API key is required".to_string()),
        )
        .await;
    }
    let name = crate::config::ANTHROPIC_KEY_SECRET;
    if let Err(e) = put_secret(&s, name, &key).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    let mut cfg = s.api.settings.auth_config().await;
    cfg.anthropic_key_ref = Some(name.to_string());
    if let Err(e) = save_auth_config(&s, &cfg).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    // CLAUDE-269: an API key save is one of the ways login-required clears
    // with no restart, alongside a brokered sign-in and the status probe.
    if let Some(gate) = &s.api.readiness {
        gate.clear_login_required();
    }
    Ok(Redirect::to("/settings?saved=Claude+auth").into_response())
}

async fn auth_save_youtrack(
    State(s): State<WebState>,
    Form(form): Form<AuthYoutrackForm>,
) -> Result<Response, WebError> {
    let base_url = form.base_url.trim().trim_end_matches('/').to_string();
    let token = form.token.trim().to_string();
    if base_url.is_empty() || token.is_empty() {
        return render_settings_with(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            None,
            None,
            Some("a YouTrack base URL and token are both required".to_string()),
        )
        .await;
    }
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return render_settings_with(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            None,
            None,
            Some(format!(
                "the YouTrack base URL must include a scheme, e.g. https://{base_url}"
            )),
        )
        .await;
    }
    let name = crate::config::YOUTRACK_TOKEN_SECRET;
    if let Err(e) = put_secret(&s, name, &token).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    let mut cfg = s.api.settings.auth_config().await;
    cfg.youtrack_base_url = Some(base_url);
    cfg.youtrack_token_ref = Some(name.to_string());
    if let Err(e) = save_auth_config(&s, &cfg).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    Ok(Redirect::to("/settings?saved=YouTrack+auth").into_response())
}

async fn auth_save_forgejo(
    State(s): State<WebState>,
    Form(form): Form<AuthForgejoForm>,
) -> Result<Response, WebError> {
    // `fj` keys are keyed by bare host, so strip a pasted scheme rather than
    // storing a key nothing will ever match.
    let host = form
        .host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let token = form.token.trim().to_string();
    if host.is_empty() || token.is_empty() {
        return render_settings_with(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            None,
            None,
            Some("a Forgejo host and token are both required".to_string()),
        )
        .await;
    }
    let name = crate::config::FORGEJO_TOKEN_SECRET;
    if let Err(e) = put_secret(&s, name, &token).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    let mut cfg = s.api.settings.auth_config().await;
    cfg.forgejo_host = Some(host);
    cfg.forgejo_token_ref = Some(name.to_string());
    if let Err(e) = save_auth_config(&s, &cfg).await {
        return render_settings_with(&s, StatusCode::UNPROCESSABLE_ENTITY, None, None, Some(e))
            .await;
    }
    Ok(Redirect::to("/settings?saved=Forgejo+auth").into_response())
}

#[derive(Deserialize)]
struct AuthCodeForm {
    code: String,
}

/// POST `/settings/auth/claude/login`: start `claude auth login` and show the
/// authorize URL. The child stays alive holding its PKCE verifier until the code
/// arrives (see `crate::claude_login`).
async fn auth_login_start(State(s): State<WebState>) -> Result<Response, WebError> {
    let Some(broker) = s.api.claude_login.clone() else {
        return render_settings_with(
            &s,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            None,
            Some("interactive sign-in is not available in this process".to_string()),
        )
        .await;
    };
    let binary = s.api.claude_binary.clone();
    let dir = s.api.agent_config_dir.clone().unwrap_or_default();
    // Spawning and reading the URL blocks; keep it off the async worker
    // (CLAUDE-228).
    let started = crate::blocking::in_place(move || broker.start(&binary, &dir));
    match started {
        Ok(_) => Ok(Redirect::to("/settings#section-auth").into_response()),
        Err(e) => {
            render_settings_with(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                None,
                Some(e.to_string()),
            )
            .await
        }
    }
}

/// POST `/settings/auth/claude/code`: hand the pasted code to the waiting child.
async fn auth_login_code(
    State(s): State<WebState>,
    Form(form): Form<AuthCodeForm>,
) -> Result<Response, WebError> {
    let Some(broker) = s.api.claude_login.clone() else {
        return render_settings_with(
            &s,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            None,
            Some("interactive sign-in is not available in this process".to_string()),
        )
        .await;
    };
    let code = form.code.trim().to_string();
    if code.is_empty() {
        return render_settings_full(
            &s,
            StatusCode::UNPROCESSABLE_ENTITY,
            None,
            None,
            None,
            Some("paste the code from the authorization page".to_string()),
        )
        .await;
    }
    let outcome = crate::blocking::in_place(move || broker.submit_code(&code));
    match outcome {
        Ok(crate::claude_login::CodeOutcome::LoggedIn) => {
            tracing::info!("claude sign-in completed from the web UI");
            // CLAUDE-269: clear login-required immediately, rather than
            // waiting for the next status probe tick, so the banner and the
            // Auth card badge drop right away and the next scheduler tick
            // dispatches normally.
            if let Some(gate) = &s.api.readiness {
                gate.clear_login_required();
            }
            Ok(Redirect::to("/settings?saved=Claude+sign-in").into_response())
        }
        // The CLI refused the code and is still waiting: show its wording and
        // leave the session open for another paste.
        Ok(crate::claude_login::CodeOutcome::Retry(msg)) => {
            render_settings_full(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                None,
                None,
                Some(msg),
            )
            .await
        }
        Err(e) => {
            render_settings_full(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                None,
                None,
                Some(e.to_string()),
            )
            .await
        }
    }
}

/// POST `/settings/auth/claude/cancel`: abandon an in-flight sign-in, killing
/// the child rather than leaving it holding a pipe and a verifier.
async fn auth_login_cancel(State(s): State<WebState>) -> Result<Response, WebError> {
    if let Some(broker) = &s.api.claude_login {
        broker.cancel();
    }
    Ok(Redirect::to("/settings#section-auth").into_response())
}

/// CLAUDE-269: the shared not-ready banner fragment, polled from every page via
/// `templates/base.html`. Behind the same CLAUDE-48 auth gate as the rest of
/// `/settings` (mounted on the same router); `templates/login.html` and
/// `templates/setup.html` do not extend `base.html`, so the poll never runs
/// unauthenticated.
#[derive(Template)]
#[template(path = "claude_readiness_banner.html")]
struct ClaudeReadinessBannerTpl {
    reason: Option<String>,
    login_required: bool,
}

async fn auth_claude_banner(State(s): State<WebState>) -> Result<Html<String>, WebError> {
    let check = s.api.readiness.as_ref().map(|g| g.check());
    let tpl = ClaudeReadinessBannerTpl {
        reason: check.as_ref().and_then(|r| r.reason()),
        login_required: check
            .as_ref()
            .and_then(|r| r.kind)
            .map(|k| k == crate::readiness::NotReadyKind::LoginRequired)
            .unwrap_or(false),
    };
    Ok(Html(tpl.render()?))
}

// ---- About page (CLAUDE-129) ----------------------------------------------
//
// Version / build info plus an on-demand "Check for updates" and "Update now"
// driven through the shared non-interactive cores in `crate::update`. The apply
// downloads, verifies, and atomically swaps the binary but never restarts the
// daemon: after a successful apply the page tells the operator to restart the
// service themselves (the runner does not manage the service lifecycle).

/// View of a completed registry check for the template.
#[derive(Serialize)]
struct CheckView {
    /// Configured train label (`release` / `latest`).
    train: String,
    /// Running identity: `vX.Y.Z` (release) or installed SHA-256 (latest).
    current: String,
    /// Newest registry identity, or a "(none)" placeholder when nothing is
    /// published.
    available: String,
    update_available: bool,
    /// Human-readable status line.
    message: String,
    /// Whether `current` / `available` are digests (latest train), so the
    /// template can label them "installed" / "published" rather than
    /// "current" / "available".
    digest: bool,
}

impl CheckView {
    fn from_outcome(o: &crate::update::CheckOutcome) -> Self {
        use crate::update::CheckState;
        let digest = matches!(o.train, crate::update::UpdateTrain::Latest);
        let message = match o.state {
            CheckState::UpdateAvailable => "Update available.".to_string(),
            CheckState::UpToDate => "Up to date.".to_string(),
            CheckState::Ahead => {
                "Running a newer build than the registry; nothing to do.".to_string()
            }
            CheckState::NoneFound if digest => {
                "No `latest` build is published in the registry.".to_string()
            }
            CheckState::NoneFound => "No vX.Y.Z releases found in the registry.".to_string(),
        };
        Self {
            train: match o.train {
                crate::update::UpdateTrain::Release => "release",
                crate::update::UpdateTrain::Latest => "latest",
            }
            .to_string(),
            current: o.current.clone(),
            available: o.available.clone().unwrap_or_else(|| "(none)".to_string()),
            update_available: o.update_available(),
            message,
            digest,
        }
    }
}

/// CLAUDE-223: the Activity card's live indicator, in exactly one of three
/// states. `Unknown` exists because the alternative (rendering an empty list as
/// idle) would tell an operator it is safe to reboot BECAUSE the query failed.
#[derive(Serialize)]
enum ActivityView {
    /// Nothing is live: no dispatched run, no open `runs` row. Safe to restart.
    Idle,
    /// Live work, listed so the operator can follow each run.
    Busy {
        runs: Vec<ActiveRunView>,
        /// Actions dispatched but with no `runs` row yet (the map-only signal).
        dispatched_only: Vec<String>,
    },
    /// The activity query itself failed. NEVER rendered as idle; the underlying
    /// error is logged at `error` where it is produced.
    Unknown { error: String },
}

impl ActivityView {
    /// The state label the template branches on. `busy` and `idle` are named
    /// explicitly and anything else renders as unknown, so a variant added
    /// later can never fall through to "safe to restart".
    fn state(&self) -> &'static str {
        match self {
            ActivityView::Idle => "idle",
            ActivityView::Busy { .. } => "busy",
            ActivityView::Unknown { .. } => "unknown",
        }
    }
    fn runs(&self) -> &[ActiveRunView] {
        match self {
            ActivityView::Busy { runs, .. } => runs,
            _ => &[],
        }
    }
    fn dispatched_only(&self) -> &[String] {
        match self {
            ActivityView::Busy {
                dispatched_only, ..
            } => dispatched_only,
            _ => &[],
        }
    }
    fn error(&self) -> &str {
        match self {
            ActivityView::Unknown { error } => error,
            _ => "",
        }
    }
}

#[derive(Serialize)]
struct ActiveRunView {
    id: i64,
    action_name: String,
    started_at: String,
}

/// CLAUDE-223: resolve the current activity for the Activity card. A failed
/// query is logged at `error` here and converted to [`ActivityView::Unknown`],
/// so the failure reaches both the log and the operator's screen instead of
/// being collapsed into an empty (idle-looking) list.
async fn activity_view(s: &WebState) -> ActivityView {
    match crate::api::activity(&s.api.pool, &s.api.in_flight).await {
        Ok(a) if a.quiet => ActivityView::Idle,
        Ok(a) => {
            let listed: std::collections::HashSet<&str> = a
                .active_runs
                .iter()
                .map(|r| r.action_name.as_str())
                .collect();
            let dispatched_only = a
                .dispatched_actions
                .iter()
                .filter(|name| !listed.contains(name.as_str()))
                .cloned()
                .collect();
            ActivityView::Busy {
                runs: a
                    .active_runs
                    .into_iter()
                    .map(|r| ActiveRunView {
                        id: r.id,
                        action_name: r.action_name,
                        started_at: r.started_at,
                    })
                    .collect(),
                dispatched_only,
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "activity query failed; the About page reports UNKNOWN, not idle");
            ActivityView::Unknown {
                error: e.to_string(),
            }
        }
    }
}

/// CLAUDE-223: the polled Activity fragment (`GET /about/activity`). Rendered
/// standalone every 2s and included in the full About page, so both surfaces
/// agree by construction.
#[derive(Template)]
#[template(path = "activity.html")]
struct ActivityTpl {
    activity: ActivityView,
}

async fn about_activity(State(s): State<WebState>) -> Result<Response, WebError> {
    let tpl = ActivityTpl {
        activity: activity_view(&s).await,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

#[derive(Template)]
#[template(path = "about.html")]
struct AboutTpl {
    version: &'static str,
    /// CLAUDE-223: the global pause, so the Activity card offers Pause when
    /// running and Resume (with since / reason) when paused.
    global_pause: Option<PauseInfo>,
    /// CLAUDE-223: the first render of the live indicator; the 2s poll then
    /// replaces it from `/about/activity`.
    activity: ActivityView,
    /// Configured update train (`release` / `latest`), changed on Settings.
    train: String,
    /// Whether a registry `token_ref` is configured (informational note).
    token_ref_set: bool,
    /// Outcome of a just-run check (only on `POST /about/check`).
    check: Option<CheckView>,
    /// Applied tag after a successful `POST /about/update`.
    applied: Option<String>,
    /// Non-fatal error from a failed check or apply (registry unreachable,
    /// checksum mismatch, unwritable binary path, ...). The running binary is
    /// left untouched on an apply error.
    error: Option<String>,
}

/// Build a lazy resolver for the registry token named by `[update].token_ref`
/// (CLAUDE-55). `None` when no `token_ref` is set (the anonymous-first path).
/// Mirrors the CLI's `configured_update_token` (CLAUDE-158): nothing is decrypted
/// here; the secret is resolved only on registry escalation.
fn configured_update_token(
    pool: &DbPool,
    cfg: &crate::config::UpdateConfig,
) -> Option<crate::update::ConfiguredToken> {
    cfg.token_ref
        .clone()
        .map(|name| crate::update::ConfiguredToken::new(pool.clone(), name))
}

/// Render the About page (HTTP 200). `check` / `applied` / `error` carry the
/// result of a just-run action; all `None` on the plain GET.
async fn render_about(
    s: &WebState,
    check: Option<CheckView>,
    applied: Option<String>,
    error: Option<String>,
) -> Result<Response, WebError> {
    let cfg = s.api.settings.update_config().await;
    let global_pause = state::global_pause_status(&s.api.pool)
        .await?
        .map(|(a, r)| PauseInfo {
            paused_at: a,
            reason: r,
        });
    let tpl = AboutTpl {
        version: s.api.version,
        global_pause,
        activity: activity_view(s).await,
        train: cfg.train,
        token_ref_set: cfg.token_ref.is_some(),
        check,
        applied,
        error,
    };
    Ok(Html(tpl.render().map_err(WebError::from)?).into_response())
}

async fn about_page(State(s): State<WebState>) -> Result<Response, WebError> {
    render_about(&s, None, None, None).await
}

async fn about_check(State(s): State<WebState>) -> Result<Response, WebError> {
    let cfg = s.api.settings.update_config().await;
    let train = match crate::update::UpdateTrain::from_config_str(&cfg.train) {
        Ok(t) => t,
        Err(e) => return render_about(&s, None, None, Some(e.to_string())).await,
    };
    let configured = configured_update_token(&s.api.pool, &cfg);
    match crate::update::web_check(train, configured).await {
        Ok(outcome) => render_about(&s, Some(CheckView::from_outcome(&outcome)), None, None).await,
        Err(e) => {
            render_about(
                &s,
                None,
                None,
                Some(format!("Could not reach the registry: {e}")),
            )
            .await
        }
    }
}

async fn about_update(State(s): State<WebState>) -> Result<Response, WebError> {
    let cfg = s.api.settings.update_config().await;
    let train = match crate::update::UpdateTrain::from_config_str(&cfg.train) {
        Ok(t) => t,
        Err(e) => return render_about(&s, None, None, Some(e.to_string())).await,
    };
    let configured = configured_update_token(&s.api.pool, &cfg);
    match crate::update::web_update(train, configured).await {
        Ok(tag) => render_about(&s, None, Some(tag), None).await,
        Err(e) => render_about(&s, None, None, Some(e.to_string())).await,
    }
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
    /// CLAUDE-93: per-job outbound links recorded on the run (webhook
    /// fast-path stores the event's `html_url` here). Wins over
    /// `link_template`; empty for legacy rows and scheduled actions.
    job_links: std::collections::BTreeMap<String, String>,
    /// CLAUDE-140: sanitized sort/filter selections, echoed back into the
    /// poll URL and the header controls so the chosen view survives the 2s
    /// morph poll. Each field is already whitelisted in the handler, so the
    /// template can interpolate them into query strings verbatim.
    sort: RunDetailSort,
    /// CLAUDE-140: whether the run has any planned jobs / outcomes at all,
    /// captured before the filters run. Lets an over-tight filter render an
    /// empty table whose dropdowns stay reachable, instead of hiding the
    /// whole section (which would strip the controls needed to reset it).
    has_planned: bool,
    has_outcomes: bool,
}

/// CLAUDE-140: whitelisted sort/filter state for the two run-detail tables.
/// Every value is validated against a fixed allow-list in [`run_detail`]
/// before it lands here, so nothing user-controlled reaches a SQL `ORDER BY`
/// (the sort/filter run in-memory) or gets echoed unescaped into the page.
#[derive(Default)]
struct RunDetailSort {
    jobs_sort: String,
    jobs_dir: String,
    jobs_state: String,
    out_sort: String,
    out_dir: String,
    out_ok: String,
    out_claude: String,
    out_counted: String,
}

impl RunDetailSort {
    /// Direction the Jobs-in-batch `col` header should request next: flip to
    /// `desc` when that column is already the ascending sort, otherwise `asc`.
    fn jobs_dir_for(&self, col: &str) -> &'static str {
        if self.jobs_sort == col && self.jobs_dir == "asc" {
            "desc"
        } else {
            "asc"
        }
    }

    /// Sort-direction glyph for a Jobs-in-batch header, empty when the table
    /// is not currently sorted by `col`.
    fn jobs_arrow(&self, col: &str) -> &'static str {
        arrow(&self.jobs_sort, &self.jobs_dir, col)
    }

    /// Direction the Job-outcomes `col` header should request next.
    fn out_dir_for(&self, col: &str) -> &'static str {
        if self.out_sort == col && self.out_dir == "asc" {
            "desc"
        } else {
            "asc"
        }
    }

    /// Sort-direction glyph for a Job-outcomes header.
    fn out_arrow(&self, col: &str) -> &'static str {
        arrow(&self.out_sort, &self.out_dir, col)
    }

    /// Full eight-param query string for the live poll, so each 2s morph
    /// refresh re-requests the current view. Values are pre-whitelisted; the
    /// `&` joiners are HTML-escaped by askama to `&amp;` and decoded back by
    /// the browser, so the request carries the literal `&`.
    fn poll_query(&self) -> String {
        self.query_with(
            &self.jobs_sort,
            &self.jobs_dir,
            &self.out_sort,
            &self.out_dir,
        )
    }

    /// Poll query with the Jobs-in-batch sort set to `col` and its toggled
    /// direction: the target a Jobs header link points at.
    fn jobs_sort_query(&self, col: &str) -> String {
        self.query_with(col, self.jobs_dir_for(col), &self.out_sort, &self.out_dir)
    }

    /// Poll query with the Job-outcomes sort set to `col` and its toggled
    /// direction.
    fn out_sort_query(&self, col: &str) -> String {
        self.query_with(&self.jobs_sort, &self.jobs_dir, col, self.out_dir_for(col))
    }

    fn query_with(&self, js: &str, jd: &str, os: &str, od: &str) -> String {
        format!(
            "jobs_sort={js}&jobs_dir={jd}&jobs_state={}&out_sort={os}&out_dir={od}&out_ok={}&out_claude={}&out_counted={}",
            self.jobs_state, self.out_ok, self.out_claude, self.out_counted,
        )
    }

    /// Every param except `omit`, for a filter `<select name="omit">` whose own
    /// value supplies the omitted param. Keeping it out of the URL avoids a
    /// duplicate query key (which axum's `Query` would reject).
    fn query_without(&self, omit: &str) -> String {
        [
            ("jobs_sort", self.jobs_sort.as_str()),
            ("jobs_dir", self.jobs_dir.as_str()),
            ("jobs_state", self.jobs_state.as_str()),
            ("out_sort", self.out_sort.as_str()),
            ("out_dir", self.out_dir.as_str()),
            ("out_ok", self.out_ok.as_str()),
            ("out_claude", self.out_claude.as_str()),
            ("out_counted", self.out_counted.as_str()),
        ]
        .iter()
        .filter(|(k, _)| *k != omit)
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
    }
}

/// Up/down triangle for the column `col` given the active `sort`/`dir`, or the
/// empty string when `col` is not the sorted column (CLAUDE-140).
fn arrow(sort: &str, dir: &str, col: &str) -> &'static str {
    if sort != col {
        ""
    } else if dir == "desc" {
        " \u{25BC}"
    } else {
        " \u{25B2}"
    }
}

/// CLAUDE-140: keep `v` only when it is in `allowed`, else return the empty
/// string (the "no selection" sentinel). This is the whitelist that stops raw
/// query input from reaching the in-memory `ORDER BY`/`WHERE` equivalents.
fn whitelisted(v: &str, allowed: &[&str]) -> String {
    if allowed.contains(&v) {
        v.to_string()
    } else {
        String::new()
    }
}

/// CLAUDE-141: keep `v` only when it appears in `allowed` (an owned allow-list
/// built from actual DB values, e.g. the distinct action / triggered_by sets),
/// else the empty "no selection" sentinel. The `&[String]` sibling of
/// [`whitelisted`] for filters whose legal values come from the data.
fn whitelist_member(v: &str, allowed: &[String]) -> String {
    if allowed.iter().any(|a| a == v) {
        v.to_string()
    } else {
        String::new()
    }
}

/// CLAUDE-140: claude-column rank used to sort the Job-outcomes "Claude"
/// column: did-not-run < ran-and-failed < ran-and-ok.
fn claude_rank(o: &JobOutcomeRow) -> u8 {
    if !o.claude_ran {
        0
    } else if o.claude_ok {
        2
    } else {
        1
    }
}

/// CLAUDE-140: compare two outcomes by a whitelisted column. Unknown columns
/// compare equal (the caller only sorts when the column passed the whitelist).
fn out_cmp(a: &JobOutcomeRow, b: &JobOutcomeRow, col: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match col {
        "key" => a.job_key.cmp(&b.job_key),
        "attempt" => a.attempt.cmp(&b.attempt),
        "ok" => a.ok.cmp(&b.ok),
        "claude" => claude_rank(a).cmp(&claude_rank(b)),
        "cost" => a
            .claude_cost_usd
            .partial_cmp(&b.claude_cost_usd)
            .unwrap_or(Ordering::Equal),
        "counted" => a.attempt_counted.cmp(&b.attempt_counted),
        "when" => a.ts.cmp(&b.ts),
        "duration" => a
            .duration_ms
            .unwrap_or(-1)
            .cmp(&b.duration_ms.unwrap_or(-1)),
        _ => Ordering::Equal,
    }
}

/// CLAUDE-140: query params for the run-detail page. The two tables are
/// namespaced (`jobs_*` vs `out_*`) so they sort/filter independently. Every
/// field defaults to empty (the page's pre-CLAUDE-140 behavior) and is
/// whitelisted in the handler before use.
#[derive(Deserialize, Default)]
struct RunDetailQuery {
    #[serde(default)]
    jobs_sort: String,
    #[serde(default)]
    jobs_dir: String,
    #[serde(default)]
    jobs_state: String,
    #[serde(default)]
    out_sort: String,
    #[serde(default)]
    out_dir: String,
    #[serde(default)]
    out_ok: String,
    #[serde(default)]
    out_claude: String,
    #[serde(default)]
    out_counted: String,
}

async fn run_detail(
    State(s): State<WebState>,
    AxumPath(id): AxumPath<i64>,
    Query(q): Query<RunDetailQuery>,
) -> Result<Response, WebError> {
    let pool = &s.api.pool;
    let Some(run): Option<RunRow> = sqlx::query_as::<_, RunRow>(&crate::db::q(
        pool,
        &format!("SELECT {} FROM runs WHERE id = ?", run_row_columns()),
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok((StatusCode::NOT_FOUND, format!("no run #{id}")).into_response());
    };
    let outcomes = sqlx::query_as::<_, JobOutcomeRow>(&crate::db::q(pool,
        "SELECT job_key, attempt, ts, ok, claude_ran, claude_ok, claude_cost_usd, attempt_counted, duration_ms, timed_out, summary FROM job_outcomes WHERE run_id = ? ORDER BY id ASC",
    ))
    .bind(id)
    .fetch_all(pool)
    .await?;

    // Look up the action's link_template once and reuse for both planned
    // jobs and the outcomes table.
    let link_template: Option<String> = sqlx::query_scalar(&crate::db::q(
        pool,
        "SELECT link_template FROM actions WHERE name = ?",
    ))
    .bind(&run.action_name)
    .fetch_optional(pool)
    .await?
    .flatten();

    let planned_keys = state::run_job_keys(pool, run.id).await.unwrap_or_default();
    // CLAUDE-93: per-job links recorded on the run (the webhook
    // stores the event's `html_url`). A map entry beats the action's
    // link_template; scheduled actions have no entries and render as before.
    let job_links = state::run_job_links(pool, run.id).await.unwrap_or_default();
    // Build a quick lookup: key -> overall ok flag from job_outcomes (latest
    // attempt wins, since outcomes are inserted in order).
    let mut outcome_by_key: std::collections::HashMap<&str, bool> =
        std::collections::HashMap::new();
    // CLAUDE-52: keys whose latest outcome timed out, badged distinctly from a
    // plain failure in the planned-jobs table.
    let mut timed_out_keys: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for o in &outcomes {
        outcome_by_key.insert(o.job_key.as_str(), o.ok);
        if o.timed_out {
            timed_out_keys.insert(o.job_key.as_str());
        } else {
            timed_out_keys.remove(o.job_key.as_str());
        }
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
    let mut planned_jobs: Vec<PlannedJob> = planned_keys
        .into_iter()
        .map(|k| {
            let (status, watch_attempt) = match outcome_by_key.get(k.as_str()) {
                Some(true) => ("ok".to_string(), None),
                Some(false) if timed_out_keys.contains(k.as_str()) => {
                    ("timed_out".to_string(), None)
                }
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
            let href = job_links.get(&k).cloned().or_else(|| {
                link_template
                    .as_ref()
                    .map(|t| crate::action::render_link(t, &k))
            });
            PlannedJob {
                key: k,
                href,
                status,
                watch_attempt,
            }
        })
        .collect();

    // CLAUDE-140: whitelist the sort/filter selections, then apply them
    // in-memory. The two tables share this handler but are namespaced
    // (`jobs_*` vs `out_*`) so they sort/filter independently. Sorting and
    // filtering happen on the already-loaded Vecs, never as interpolated SQL,
    // so no raw query input can reach an `ORDER BY`/`WHERE`. `planned_jobs` is
    // built from the full outcome set above, so trimming it here does not
    // disturb the per-key status derivation.
    let sort = RunDetailSort {
        jobs_sort: whitelisted(&q.jobs_sort, &["job", "state"]),
        jobs_dir: if q.jobs_dir == "desc" { "desc" } else { "asc" }.to_string(),
        jobs_state: whitelisted(
            &q.jobs_state,
            &["ok", "timed_out", "fail", "running", "queued", "skipped"],
        ),
        out_sort: whitelisted(
            &q.out_sort,
            &[
                "key", "attempt", "ok", "claude", "cost", "counted", "when", "duration",
            ],
        ),
        out_dir: if q.out_dir == "desc" { "desc" } else { "asc" }.to_string(),
        out_ok: whitelisted(&q.out_ok, &["true", "false"]),
        out_claude: whitelisted(&q.out_claude, &["ran_ok", "ran_err", "skip"]),
        out_counted: whitelisted(&q.out_counted, &["true", "false"]),
    };

    // Jobs-in-batch: filter by the derived status, then sort by the chosen
    // column. Default (no `jobs_sort`) keeps the planned order. `has_planned`
    // is captured before the filter so an over-tight State filter still shows
    // the section (and its dropdown) rather than hiding it.
    let has_planned = !planned_jobs.is_empty();
    if !sort.jobs_state.is_empty() {
        planned_jobs.retain(|p| p.status == sort.jobs_state);
    }
    match sort.jobs_sort.as_str() {
        "job" => planned_jobs.sort_by(|a, b| a.key.cmp(&b.key)),
        "state" => planned_jobs.sort_by(|a, b| a.status.cmp(&b.status)),
        _ => {}
    }
    if sort.jobs_dir == "desc" && !sort.jobs_sort.is_empty() {
        planned_jobs.reverse();
    }

    // Job outcomes: filter on the OK / Claude / Counted dropdowns, then sort by
    // the chosen column. Default (no `out_sort`) keeps the `id ASC` order from
    // the query above.
    let mut outcomes = outcomes;
    let has_outcomes = !outcomes.is_empty();
    if !sort.out_ok.is_empty() {
        let want = sort.out_ok == "true";
        outcomes.retain(|o| o.ok == want);
    }
    match sort.out_claude.as_str() {
        "ran_ok" => outcomes.retain(|o| o.claude_ran && o.claude_ok),
        "ran_err" => outcomes.retain(|o| o.claude_ran && !o.claude_ok),
        "skip" => outcomes.retain(|o| !o.claude_ran),
        _ => {}
    }
    if !sort.out_counted.is_empty() {
        let want = sort.out_counted == "true";
        outcomes.retain(|o| o.attempt_counted == want);
    }
    if !sort.out_sort.is_empty() {
        outcomes.sort_by(|a, b| out_cmp(a, b, &sort.out_sort));
        if sort.out_dir == "desc" {
            outcomes.reverse();
        }
    }

    let tpl = RunDetailTpl {
        version: s.api.version,
        run,
        outcomes,
        planned_jobs,
        link_template,
        job_links,
        sort,
        has_planned,
        has_outcomes,
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
    let action_name: Option<String> = sqlx::query_scalar(&crate::db::q(
        &s.api.pool,
        "SELECT action_name FROM runs WHERE id = ?",
    ))
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

/// CLAUDE-154: add a preference-formatted `ts_fmt` alongside a stream event's
/// raw `ts` so the live log renders the same timezone + format as every other
/// date, while the client keeps the raw `ts` for its hover tooltip. Lines that
/// are not JSON objects, or carry no string `ts`, pass through unchanged.
fn augment_stream_line_ts(line: &str, prefs: &crate::config::PreferencesConfig) -> String {
    use serde_json::Value;
    let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(line) else {
        return line.to_string();
    };
    let ts = match map.get("ts") {
        Some(Value::String(s)) => s.clone(),
        _ => return line.to_string(),
    };
    let fmt = crate::settings::format_datetime(&ts, prefs);
    map.insert("ts_fmt".to_string(), Value::String(fmt));
    Value::Object(map).to_string()
}

async fn job_stream_sse(
    State(s): State<WebState>,
    AxumPath((id, key, attempt)): AxumPath<(i64, String, u32)>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, WebError> {
    let run: Option<(String, Option<String>)> = sqlx::query_as(&crate::db::q(
        &s.api.pool,
        "SELECT action_name, ended_at FROM runs WHERE id = ?",
    ))
    .bind(id)
    .fetch_optional(&s.api.pool)
    .await?;
    let (action_name, ended_at) =
        run.ok_or_else(|| WebError::not_found(format!("no run #{id}")))?;
    // CLAUDE-245: a finished run's log will never appear, so an absent file is a
    // permanent answer (swept by the retention sweep, or never written), not the
    // "the agent has not started writing" one the wait below reports.
    let run_ended = ended_at.is_some();
    let path = ClaudeInvoker::stream_log_path(&s.paths.logs_dir(), &action_name, &key, attempt);

    // CLAUDE-154: snapshot the display preferences for this connection so each
    // event's `ts` is formatted server-side (chrono strftime can't run in JS),
    // keeping the live log on the same timezone + format as every other date.
    let prefs = crate::settings::current_preferences();

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
        // Wait briefly for the file to exist (claude may be spinning up). A run
        // that has already ended is never going to write it, so skip the wait.
        let mut waited = 0u32;
        while !run_ended && !path.exists() && waited < 20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            waited += 1;
        }
        if !path.exists() {
            // CLAUDE-245: the two absences are different facts and the operator
            // must be able to tell them apart. "Not written yet" is a live run
            // spinning up; a finished run's missing log is gone for good, most
            // often deleted by the stream-log retention sweep.
            let subtype = if run_ended { "stream-log-gone" } else { "no-stream-yet" };
            yield Ok(Event::default().event("msg").data(
                serde_json::json!({"type": "system", "subtype": subtype}).to_string()
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
                        let data = augment_stream_line_ts(&line, &prefs);
                        yield Ok(Event::default().event("msg").id(idx.to_string()).data(data));
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
                    let done: Option<i64> = sqlx::query_scalar(&crate::db::q(&pool,
                        "SELECT id FROM job_outcomes WHERE run_id = ? AND job_key = ? AND attempt = ?",
                    ))
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

async fn idiomorph_handler() -> Response {
    let mut resp = Response::new(Body::from(IDIOMORPH_JS));
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
    use super::{
        build_duration_chart, format_duration_ms, latest_attempt_on_disk, render_summary_markdown,
    };

    // ---- summary markdown rendering (CLAUDE-89) ----------------------------

    /// Markdown structure in a summary renders as HTML elements.
    #[test]
    fn summary_markdown_renders_formatting() {
        let html =
            render_summary_markdown("## Summary\n\nDid **the thing** with `code`.\n\n- one\n- two");
        assert!(html.contains("<h2>Summary</h2>"), "heading missing: {html}");
        assert!(
            html.contains("<strong>the thing</strong>"),
            "bold missing: {html}"
        );
        assert!(
            html.contains("<code>code</code>"),
            "inline code missing: {html}"
        );
        assert!(html.contains("<li>one</li>"), "list missing: {html}");
    }

    /// Raw HTML in a summary is escaped to text, never emitted as markup. The
    /// template injects the result with `|safe`, so this is the trust boundary
    /// against model output scripting the page.
    #[test]
    fn summary_markdown_escapes_raw_html() {
        let html =
            render_summary_markdown("hi <script>alert(1)</script>\n\n<img src=x onerror=alert(2)>");
        assert!(
            !html.contains("<script>"),
            "script tag passed through: {html}"
        );
        assert!(!html.contains("<img"), "img tag passed through: {html}");
        assert!(
            html.contains("&lt;script&gt;"),
            "script tag not escaped: {html}"
        );
    }

    // ---- run-detail modal heading link (CLAUDE-108) -----------------------

    /// The summary/details modal heading links the job key when the run has a
    /// populated `job_links` map, with `- attempt N` left outside the anchor and
    /// opening in a new tab. Covers both YouTrack (which now populates
    /// `job_links`) and Forgejo runs.
    #[test]
    fn modal_heading_links_job_key_from_job_links() {
        use super::{JobOutcomeRow, RunDetailTpl, RunRow};
        use askama::Template;

        let run = RunRow {
            id: 1,
            action_name: "YouTrack".to_string(),
            started_at: "2026-06-13T00:00:00Z".to_string(),
            ended_at: Some("2026-06-13T00:01:00Z".to_string()),
            exit_code: Some(0),
            jobs_processed: 1,
            triggered_by: "schedule".to_string(),
            current_job_key: None,
            stage: None,
            failed_jobs: 0,
        };
        let outcome = JobOutcomeRow {
            job_key: "CLAUDE-108".to_string(),
            attempt: 2,
            ts: "2026-06-13T00:00:30Z".to_string(),
            ok: true,
            claude_ran: true,
            claude_ok: true,
            claude_cost_usd: 0.0,
            attempt_counted: true,
            duration_ms: Some(1000),
            timed_out: false,
            summary: Some("did the thing".to_string()),
        };
        let job_links = std::collections::BTreeMap::from([(
            "CLAUDE-108".to_string(),
            "https://example.myjetbrains.com/issue/CLAUDE-108".to_string(),
        )]);
        let tpl = RunDetailTpl {
            version: "test",
            run,
            outcomes: vec![outcome],
            planned_jobs: vec![],
            link_template: None,
            job_links,
            sort: super::RunDetailSort::default(),
            has_planned: false,
            has_outcomes: true,
        };
        let html = tpl.render().expect("render run_detail");
        assert!(
            html.contains(
                "<a href=\"https://example.myjetbrains.com/issue/CLAUDE-108\" target=\"_blank\" rel=\"noopener\">CLAUDE-108</a> - attempt 2"
            ),
            "modal heading should link the key with attempt outside the anchor: {html}"
        );
    }

    // ---- run-detail link_template links (CLAUDE-222) ----------------------

    /// A CI-failure run links every key from the shipped action's
    /// `link_template`: the planned-jobs table, the outcomes table, and the
    /// summary-modal heading. The template comes from the shipped YAML, so a
    /// run screen and the config it ships with are checked together.
    #[test]
    fn ci_failure_run_links_every_key_from_the_shipped_link_template() {
        use super::{JobOutcomeRow, PlannedJob, RunDetailTpl, RunRow};
        use askama::Template;

        const SHIPPED: &str = include_str!("../../actions/ci-failure-analysis.yml");
        let template = crate::action::validate_str(SHIPPED, "<test>")
            .unwrap()
            .action
            .link_template
            .expect("shipped link_template");
        let href = crate::action::render_link(&template, "CLAUDE-222");
        assert_eq!(href, "https://example.myjetbrains.com/issue/CLAUDE-222");

        let run = RunRow {
            id: 3,
            action_name: "ci-failure-analysis".to_string(),
            started_at: "2026-08-08T00:00:00Z".to_string(),
            ended_at: Some("2026-08-08T00:01:00Z".to_string()),
            exit_code: Some(0),
            jobs_processed: 1,
            triggered_by: "schedule".to_string(),
            current_job_key: None,
            stage: None,
            failed_jobs: 0,
        };
        let outcome = JobOutcomeRow {
            job_key: "CLAUDE-222".to_string(),
            attempt: 1,
            ts: "2026-08-08T00:00:30Z".to_string(),
            ok: true,
            claude_ran: true,
            claude_ok: true,
            claude_cost_usd: 0.0,
            attempt_counted: true,
            duration_ms: Some(1000),
            timed_out: false,
            summary: Some("analyzed".to_string()),
        };
        let planned = PlannedJob {
            key: "CLAUDE-222".to_string(),
            href: Some(href.clone()),
            status: "ok".to_string(),
            watch_attempt: None,
        };
        let tpl = RunDetailTpl {
            version: "test",
            run,
            outcomes: vec![outcome],
            planned_jobs: vec![planned],
            link_template: Some(template),
            // No per-run links: the action's template is the only source.
            job_links: std::collections::BTreeMap::new(),
            sort: super::RunDetailSort::default(),
            has_planned: true,
            has_outcomes: true,
        };
        let html = tpl.render().expect("render run_detail");
        let anchor =
            format!("<a href=\"{href}\" target=\"_blank\" rel=\"noopener\">CLAUDE-222</a>");
        // Planned-jobs table, outcomes table, and the modal heading.
        assert_eq!(
            html.matches(anchor.as_str()).count(),
            3,
            "every key should link out: {html}"
        );
    }

    // ---- run-detail sort/filter (CLAUDE-140) ------------------------------

    /// Unknown sort columns and filter values are rejected by the whitelist,
    /// so nothing user-controlled reaches the in-memory `ORDER BY`/`WHERE`.
    #[test]
    fn whitelist_rejects_unknown_values() {
        use super::whitelisted;
        assert_eq!(whitelisted("state", &["job", "state"]), "state");
        assert_eq!(whitelisted("job; DROP TABLE", &["job", "state"]), "");
        assert_eq!(whitelisted("", &["true", "false"]), "");
    }

    /// The Claude column ranks did-not-run < ran-and-failed < ran-and-ok, and
    /// `out_cmp` orders outcomes by the chosen whitelisted column.
    #[test]
    fn out_cmp_orders_by_column() {
        use super::{out_cmp, JobOutcomeRow};
        use std::cmp::Ordering;
        let row = |attempt: i64, ran: bool, ok: bool| JobOutcomeRow {
            job_key: "K".to_string(),
            attempt,
            ts: "t".to_string(),
            ok: true,
            claude_ran: ran,
            claude_ok: ok,
            claude_cost_usd: 0.0,
            attempt_counted: true,
            duration_ms: Some(1),
            timed_out: false,
            summary: None,
        };
        let skip = row(1, false, false);
        let ran_err = row(2, true, false);
        let ran_ok = row(3, true, true);
        assert_eq!(out_cmp(&skip, &ran_err, "claude"), Ordering::Less);
        assert_eq!(out_cmp(&ran_ok, &ran_err, "claude"), Ordering::Greater);
        assert_eq!(out_cmp(&skip, &ran_ok, "attempt"), Ordering::Less);
        assert_eq!(out_cmp(&skip, &ran_ok, "bogus"), Ordering::Equal);
    }

    /// The sort/filter selection round-trips into the poll URL (so the 2s morph
    /// poll keeps the view) and renders the header sort links + filter
    /// dropdowns with the active option marked selected.
    #[test]
    fn run_detail_renders_sort_filter_controls() {
        use super::{RunDetailSort, RunDetailTpl, RunRow};
        use askama::Template;

        let run = RunRow {
            id: 7,
            action_name: "YouTrack".to_string(),
            started_at: "2026-06-27T00:00:00Z".to_string(),
            ended_at: None,
            exit_code: None,
            jobs_processed: 0,
            triggered_by: "schedule".to_string(),
            current_job_key: None,
            stage: None,
            failed_jobs: 0,
        };
        let sort = RunDetailSort {
            out_sort: "cost".to_string(),
            out_dir: "desc".to_string(),
            out_ok: "true".to_string(),
            ..RunDetailSort::default()
        };
        let tpl = RunDetailTpl {
            version: "test",
            run,
            outcomes: vec![],
            planned_jobs: vec![],
            link_template: None,
            job_links: std::collections::BTreeMap::new(),
            sort,
            has_planned: false,
            has_outcomes: true,
        };
        let html = tpl.render().expect("render run_detail");
        // Poll URL carries the active selection (askama escapes `&` to `&amp;`).
        assert!(
            html.contains("out_sort=cost&amp;out_dir=desc"),
            "poll URL should carry the active sort: {html}"
        );
        // Header sort links exist and the sorted column shows a direction glyph.
        assert!(
            html.contains("class=\"sort-link\""),
            "sort links missing: {html}"
        );
        assert!(
            html.contains("$ \u{25BC}"),
            "descending arrow on the cost column missing: {html}"
        );
        // The OK filter dropdown marks the active value selected.
        assert!(
            html.contains("name=\"out_ok\""),
            "OK filter dropdown missing: {html}"
        );
        assert!(
            html.contains("value=\"true\" selected"),
            "active OK filter option not selected: {html}"
        );
    }

    // ---- dashboard sort/filter (CLAUDE-141) -------------------------------

    /// `order_by` maps each whitelisted column to its SQL expression, defaults
    /// unknown/empty `sort` to `started_at DESC`, and never interpolates raw
    /// input (the column comes from a fixed `match`).
    #[test]
    fn runs_view_order_by_whitelists_column() {
        use super::RunsView;
        let v = |sort: &str, dir: &str| RunsView {
            sort: sort.to_string(),
            dir: dir.to_string(),
            ..RunsView::default()
        };
        assert_eq!(v("run", "asc").order_by(), "ORDER BY id ASC");
        assert_eq!(
            v("action", "desc").order_by(),
            "ORDER BY action_name DESC, id DESC"
        );
        assert_eq!(
            v("duration", "asc").order_by(),
            "ORDER BY elapsed_seconds(started_at, ended_at) ASC, id DESC"
        );
        // Empty / unknown sort falls back to the pre-CLAUDE-141 default.
        assert_eq!(v("", "").order_by(), "ORDER BY started_at DESC, id DESC");
        assert_eq!(
            v("bogus; DROP", "asc").order_by(),
            "ORDER BY started_at ASC, id DESC"
        );
        // CLAUDE-230: the derived failed-job count is sortable worst-first and
        // keeps the `id DESC` tiebreaker.
        assert_eq!(
            v("failed", "desc").order_by(),
            "ORDER BY failed_jobs DESC, id DESC"
        );
        assert_eq!(
            v("failed", "asc").order_by(),
            "ORDER BY failed_jobs ASC, id DESC"
        );
    }

    /// `where_clause` composes the CLAUDE-35 hide-empty rule with the Action /
    /// Triggered / Exit filters, emitting `?` placeholders and ordered binds for
    /// the value filters (so nothing user-controlled is interpolated).
    #[test]
    fn runs_view_where_clause_composes_filters() {
        use super::RunsView;
        // No filters, show_empty true -> no WHERE at all.
        let empty = RunsView::default();
        let (w, b) = empty.where_clause(true);
        assert_eq!(w, "");
        assert!(b.is_empty());

        // Hide-empty off plus all three filters, AND-composed, binds in order.
        let v = RunsView {
            action: "YouTrack".to_string(),
            triggered: "schedule".to_string(),
            exit: "failed".to_string(),
            ..RunsView::default()
        };
        let (w, b) = v.where_clause(false);
        assert_eq!(
            w,
            " WHERE (jobs_processed > 0 OR exit_code IS NULL) AND action_name = ? AND triggered_by = ? AND exit_code IS NOT NULL AND exit_code NOT IN (0, 1, 130)"
        );
        assert_eq!(b, vec!["YouTrack".to_string(), "schedule".to_string()]);

        // The derived exit states map to the matching predicates.
        let success = RunsView {
            exit: "success".to_string(),
            ..RunsView::default()
        };
        assert_eq!(success.where_clause(true).0, " WHERE exit_code = 0");
        let running = RunsView {
            exit: "running".to_string(),
            ..RunsView::default()
        };
        assert_eq!(running.where_clause(true).0, " WHERE exit_code IS NULL");
    }

    /// CLAUDE-230: the job-level `outcome` filter is its own axis. It maps to an
    /// `EXISTS` over the SHARED latest-attempt fragment (so it can never drift
    /// from the displayed `(N failed)` count), composes with the other filters
    /// via `AND` without adding binds, and ignores an unrecognised value.
    #[test]
    fn runs_view_where_clause_filters_job_outcome() {
        use super::{
            has_job_matching, latest_job_attempts, run_row_columns, RunsView, JOB_FAILED,
            JOB_TIMED_OUT,
        };
        let with = |outcome: &str| RunsView {
            outcome: outcome.to_string(),
            ..RunsView::default()
        };
        assert_eq!(
            with("failed").where_clause(true).0,
            format!(" WHERE {}", has_job_matching(JOB_FAILED))
        );
        assert_eq!(
            with("timedout").where_clause(true).0,
            format!(" WHERE {}", has_job_matching(JOB_TIMED_OUT))
        );
        // Both the filter predicate and the displayed count are built from the
        // one `latest_job_attempts` fragment, so they cannot drift apart. This
        // is asserted against the fragment itself, never a copy of its text.
        let shared = latest_job_attempts(JOB_FAILED);
        assert!(with("failed").where_clause(true).0.contains(&shared));
        assert!(run_row_columns().contains(&shared));
        // An unrecognised (or empty) value adds no condition at all.
        assert_eq!(with("bogus; DROP").where_clause(true).0, "");
        assert_eq!(with("").where_clause(true).0, "");

        // Composes with action / triggered / exit via AND, binds unchanged and
        // in order (the outcome filter interpolates no user input).
        let v = RunsView {
            action: "YouTrack".to_string(),
            triggered: "schedule".to_string(),
            exit: "success".to_string(),
            outcome: "failed".to_string(),
            ..RunsView::default()
        };
        let (w, b) = v.where_clause(false);
        assert_eq!(
            w,
            format!(
                " WHERE (jobs_processed > 0 OR exit_code IS NULL) AND action_name = ? AND triggered_by = ? AND exit_code = 0 AND {}",
                has_job_matching(JOB_FAILED)
            )
        );
        assert_eq!(b, vec!["YouTrack".to_string(), "schedule".to_string()]);
    }

    /// CLAUDE-230: `outcome` rides in the poll/pagination query string and in
    /// `query_without` for every other dropdown, so it survives the 2s refresh,
    /// a page change, and a change to any sibling filter.
    #[test]
    fn runs_view_query_carries_outcome() {
        use super::RunsView;
        let v = RunsView {
            sort: "failed".to_string(),
            dir: "desc".to_string(),
            action: "YouTrack".to_string(),
            triggered: "schedule".to_string(),
            exit: "success".to_string(),
            outcome: "timedout".to_string(),
        };
        assert_eq!(
            v.poll_query(),
            "sort=failed&dir=desc&action=YouTrack&triggered=schedule&exit=success&outcome=timedout"
        );
        // The outcome select supplies its own value, so its URL omits it; every
        // other dropdown's URL carries it.
        assert_eq!(
            v.query_without("outcome"),
            "sort=failed&dir=desc&action=YouTrack&triggered=schedule&exit=success"
        );
        assert!(v.query_without("exit").contains("outcome=timedout"));
        assert!(v.query_without("action").contains("outcome=timedout"));
        assert!(v.query_without("triggered").contains("outcome=timedout"));
    }

    /// `whitelist_member` keeps only values present in the data-derived list.
    #[test]
    fn whitelist_member_rejects_unknown_values() {
        use super::whitelist_member;
        let allowed = vec!["YouTrack".to_string(), "forgejo".to_string()];
        assert_eq!(whitelist_member("YouTrack", &allowed), "YouTrack");
        assert_eq!(whitelist_member("nope", &allowed), "");
        assert_eq!(whitelist_member("", &allowed), "");
    }

    /// The dashboard sort/filter selection round-trips into the poll URL,
    /// pagination links, header sort links, and filter dropdowns (with the
    /// active option marked selected and data-derived options rendered).
    #[test]
    fn dashboard_renders_sort_filter_controls() {
        use super::{DashboardTpl, PageNav, RunRow, RunsView};
        use askama::Template;

        let run = RunRow {
            id: 7,
            action_name: "YouTrack".to_string(),
            started_at: "2026-06-27T00:00:00Z".to_string(),
            ended_at: None,
            exit_code: None,
            jobs_processed: 0,
            triggered_by: "schedule".to_string(),
            current_job_key: None,
            stage: None,
            failed_jobs: 0,
        };
        let view = RunsView {
            sort: "duration".to_string(),
            dir: "desc".to_string(),
            action: "YouTrack".to_string(),
            exit: "running".to_string(),
            outcome: "timedout".to_string(),
            ..RunsView::default()
        };
        let tpl = DashboardTpl {
            version: "test",
            global_pause: None,
            today_runs: 0,
            today_actions: 0,
            today_cost: 0.0,
            actions: vec![],
            recent_runs: vec![run],
            show_empty: false,
            search_q: String::new(),
            in_stream: false,
            search: None,
            page: PageNav {
                page: 1,
                total_pages: 1,
                total: 1,
                start: 1,
                end: 1,
                has_prev: false,
                has_next: false,
            },
            events_24h: 0,
            access_failure_events: 0,
            access_failure_groups: 0,
            stale_gates: vec![],
            attempt_cap_hits: vec![],
            view,
            action_options: vec!["YouTrack".to_string(), "forgejo".to_string()],
            triggered_options: vec!["schedule".to_string(), "webhook".to_string()],
        };
        let html = tpl.render().expect("render dashboard");
        // Poll URL carries the active sort/filter (askama escapes `&` to `&amp;`).
        assert!(
            html.contains("sort=duration&amp;dir=desc"),
            "poll URL should carry the active sort: {html}"
        );
        // Header sort links exist and the sorted column shows a direction glyph.
        assert!(
            html.contains("class=\"sort-link\""),
            "sort links missing: {html}"
        );
        assert!(
            html.contains("Duration \u{25BC}"),
            "descending arrow on the duration column missing: {html}"
        );
        // Filter dropdowns render with data-derived options and active selection.
        assert!(
            html.contains("name=\"action\""),
            "Action filter dropdown missing: {html}"
        );
        assert!(
            html.contains("value=\"YouTrack\" selected"),
            "active Action filter option not selected: {html}"
        );
        assert!(
            html.contains("name=\"triggered\""),
            "Triggered filter dropdown missing: {html}"
        );
        assert!(
            html.contains("value=\"webhook\""),
            "data-derived Triggered option missing: {html}"
        );
        assert!(
            html.contains("name=\"exit\""),
            "Exit filter dropdown missing: {html}"
        );
        assert!(
            html.contains("value=\"running\" selected"),
            "active Exit filter option not selected: {html}"
        );
        // CLAUDE-230: the job-outcome dropdown under the Jobs header, with its
        // active option selected, and the failed-count sort link beside it.
        assert!(
            html.contains("name=\"outcome\""),
            "job outcome filter dropdown missing: {html}"
        );
        assert!(
            html.contains("value=\"timedout\" selected"),
            "active outcome filter option not selected: {html}"
        );
        assert!(
            html.contains("value=\"failed\""),
            "failed outcome option missing: {html}"
        );
        assert!(
            html.contains("sort=failed"),
            "failed-count sort link missing: {html}"
        );
        // The outcome select's own URL omits `outcome`; every other control
        // carries it, so the filter survives the poll and sibling changes.
        assert!(
            html.contains("exit=running&amp;outcome=timedout"),
            "outcome not carried in the poll/sort URLs: {html}"
        );
    }

    /// CLAUDE-225: a gate held past the threshold renders the dashboard banner
    /// with its issue, age, blockers, and whether its own re-check is overdue.
    /// With no stale gate the banner is absent entirely.
    #[test]
    fn dashboard_banners_stale_dependency_gates() {
        use super::{DashboardTpl, PageNav, RunsView, StaleGateRow};
        use crate::state;
        use askama::Template;

        let tpl = |stale_gates: Vec<StaleGateRow>| DashboardTpl {
            version: "test",
            global_pause: None,
            today_runs: 0,
            today_actions: 0,
            today_cost: 0.0,
            actions: vec![],
            recent_runs: vec![],
            show_empty: false,
            search_q: String::new(),
            in_stream: false,
            search: None,
            page: PageNav {
                page: 1,
                total_pages: 1,
                total: 0,
                start: 0,
                end: 0,
                has_prev: false,
                has_next: false,
            },
            events_24h: 0,
            access_failure_events: 0,
            access_failure_groups: 0,
            stale_gates,
            attempt_cap_hits: vec![],
            view: RunsView::default(),
            action_options: vec![],
            triggered_options: vec![],
        };

        let quiet = tpl(vec![]).render().expect("render dashboard");
        assert!(
            !quiet.contains("Dependency gates blocked"),
            "no stale gate must render no banner: {quiet}"
        );

        let html = tpl(vec![StaleGateRow {
            gate: state::StaleGate {
                action_name: "youtrack".to_string(),
                job_key: "AUD-31".to_string(),
                signature: "AUD-32 (depends on)".to_string(),
                blocked_for_ms: 21 * 3_600_000,
                recheck_overdue_ms: 20 * 3_600_000,
                updated_at: "2026-08-11T07:30:00Z".to_string(),
            },
        }])
        .render()
        .expect("render dashboard");
        assert!(
            html.contains("Dependency gates blocked over an hour"),
            "banner heading missing: {html}"
        );
        assert!(html.contains("AUD-31"), "held issue missing: {html}");
        assert!(
            html.contains("AUD-32 (depends on)"),
            "blocker set missing: {html}"
        );
        assert!(html.contains("21h 0m"), "block age missing: {html}");
        assert!(
            html.contains("overdue 20h 0m"),
            "overdue re-check not flagged: {html}"
        );
    }

    /// CLAUDE-232: a key at its action's attempt cap renders the dashboard
    /// banner with its action, key, count against the cap, and last attempt,
    /// and the banner is absent entirely when nothing is parked.
    #[test]
    fn dashboard_banners_keys_parked_at_the_attempt_cap() {
        use super::{DashboardTpl, PageNav, RunsView};
        use crate::state;
        use askama::Template;

        let tpl = |attempt_cap_hits: Vec<state::CappedKey>| DashboardTpl {
            version: "test",
            global_pause: None,
            today_runs: 0,
            today_actions: 0,
            today_cost: 0.0,
            actions: vec![],
            recent_runs: vec![],
            show_empty: false,
            search_q: String::new(),
            in_stream: false,
            search: None,
            page: PageNav {
                page: 1,
                total_pages: 1,
                total: 0,
                start: 0,
                end: 0,
                has_prev: false,
                has_next: false,
            },
            events_24h: 0,
            access_failure_events: 0,
            access_failure_groups: 0,
            stale_gates: vec![],
            attempt_cap_hits,
            view: RunsView::default(),
            action_options: vec![],
            triggered_options: vec![],
        };

        let quiet = tpl(vec![]).render().expect("render dashboard");
        assert!(
            !quiet.contains("parked at the attempt cap"),
            "nothing parked must render no banner: {quiet}"
        );

        let html = tpl(vec![state::CappedKey {
            action_name: "youtrack".to_string(),
            job_key: "CLAUDE-9".to_string(),
            count: 3,
            cap: 3,
            last_at: "2026-08-11T07:30:00Z".to_string(),
        }])
        .render()
        .expect("render dashboard");
        assert!(
            html.contains("Jobs parked at the attempt cap"),
            "banner heading missing: {html}"
        );
        assert!(html.contains("CLAUDE-9"), "parked key missing: {html}");
        assert!(
            html.contains("/actions/youtrack"),
            "link to the owning action missing: {html}"
        );
        assert!(
            html.contains("3 / 3"),
            "attempt count against the cap missing: {html}"
        );
        assert!(
            html.contains("max_attempts_per_key"),
            "banner must name the limit that parked the job: {html}"
        );
        assert!(
            html.contains("AI Agent"),
            "banner must say what releases the job: {html}"
        );
    }

    // ---- job-outcome filter, DB-backed (CLAUDE-230) -----------------------
    mod outcome_filter {
        use super::super::{count_recent_runs, fetch_recent_runs, RunsView};
        use crate::db::DbPool;
        use crate::state;

        async fn pool() -> DbPool {
            crate::db::test_pool().await
        }

        /// Seed one finished (exit 0) run and its job attempts, given as
        /// `(job_key, attempt, ok, timed_out)`. Returns the run id.
        async fn seed_run(pool: &DbPool, attempts: &[(&str, u32, bool, bool)]) -> i64 {
            let id = state::run_begin(pool, "youtrack", "schedule")
                .await
                .unwrap();
            for (job_key, attempt, ok, timed_out) in attempts {
                state::job_outcome_insert(
                    pool,
                    &state::JobOutcome {
                        run_id: id,
                        action_name: "youtrack",
                        job_key,
                        attempt: *attempt,
                        ok: *ok,
                        claude_ran: true,
                        claude_ok: *ok,
                        claude_cost_usd: 0.0,
                        attempt_counted: true,
                        summary: None,
                        stream_log_path: None,
                        duration_ms: 1,
                        timed_out: *timed_out,
                    },
                )
                .await
                .unwrap();
            }
            state::run_finish(pool, id, 0, 1).await.unwrap();
            id
        }

        async fn matching(pool: &DbPool, outcome: &str) -> Vec<i64> {
            let view = RunsView {
                outcome: outcome.to_string(),
                ..RunsView::default()
            };
            let rows = fetch_recent_runs(pool, 50, 0, true, &view).await.unwrap();
            // The COUNT drives pagination and must agree with the listing.
            let total = count_recent_runs(pool, true, &view).await.unwrap();
            assert_eq!(total, rows.len() as i64, "count disagrees with the listing");
            let mut ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
            ids.sort_unstable();
            ids
        }

        /// The motivating case: every one of these runs finishes `exit_code = 0`
        /// (the runners only ever write 0 / 1 / 130), so the Exit filter cannot
        /// tell them apart. `outcome` selects on the JOB result instead, using
        /// the latest attempt per job key: the retried run is a success and is
        /// excluded, and a timeout is not a failure.
        #[tokio::test]
        async fn outcome_filter_selects_runs_by_latest_job_attempt() {
            let pool = pool().await;
            let failed = seed_run(&pool, &[("A-1", 1, false, false)]).await;
            let retried =
                seed_run(&pool, &[("A-2", 1, false, false), ("A-2", 2, true, false)]).await;
            let timed_out = seed_run(&pool, &[("A-3", 1, false, true)]).await;
            let clean = seed_run(&pool, &[("A-4", 1, true, false)]).await;

            // No filter: every run, as before.
            let mut all = vec![failed, retried, timed_out, clean];
            all.sort_unstable();
            assert_eq!(matching(&pool, "").await, all);
            // An unrecognised value is ignored, not treated as a filter.
            assert_eq!(matching(&pool, "bogus").await, all);

            assert_eq!(matching(&pool, "failed").await, vec![failed]);
            assert_eq!(matching(&pool, "timedout").await, vec![timed_out]);

            // The displayed `(N failed)` count is derived from the same
            // fragment, so it agrees row for row with what the filter selected.
            let view = RunsView::default();
            let rows = fetch_recent_runs(&pool, 50, 0, true, &view).await.unwrap();
            for r in &rows {
                let expected = i64::from(r.id == failed);
                assert_eq!(
                    r.failed_jobs, expected,
                    "run #{} failed_jobs disagrees with the filter",
                    r.id
                );
            }
        }

        /// `sort=failed` orders by the derived count, worst first.
        #[tokio::test]
        async fn failed_sort_orders_by_derived_count() {
            let pool = pool().await;
            let clean = seed_run(&pool, &[("B-1", 1, true, false)]).await;
            let two = seed_run(&pool, &[("B-2", 1, false, false), ("B-3", 1, false, false)]).await;
            let one = seed_run(&pool, &[("B-4", 1, false, false)]).await;

            let view = RunsView {
                sort: "failed".to_string(),
                dir: "desc".to_string(),
                ..RunsView::default()
            };
            let rows = fetch_recent_runs(&pool, 50, 0, true, &view).await.unwrap();
            assert_eq!(
                rows.iter().map(|r| r.id).collect::<Vec<_>>(),
                vec![two, one, clean]
            );
            assert_eq!(
                rows.iter().map(|r| r.failed_jobs).collect::<Vec<_>>(),
                vec![2, 1, 0]
            );
        }
    }

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
            action_create, action_delete, action_detail, action_edit_page, action_edit_save,
            action_events_redirect, action_export, action_to_youtrack_form, auth_claude_banner,
            auth_login_code, auth_login_start, auth_save_claude, dashboard, logs, settings_page,
            settings_save, youtrack_create, youtrack_edit_page, youtrack_edit_save,
            youtrack_form_to_yaml, youtrack_new_page, ActionDetailQuery, ActionForm,
            AuthClaudeForm, AuthCodeForm, DashboardQuery, EventsView, LogsQuery, SettingsForm,
            WebState, YoutrackEditForm,
        };
        use crate::api::AppState;
        use crate::cancel::RunRegistry;
        use crate::config::Paths;
        use crate::db::DbPool;
        use crate::state;
        use axum::extract::Query;
        use axum::extract::{Form, Path as AxumPath, State};
        use axum::http::StatusCode;
        use axum::response::{IntoResponse, Response};
        use std::sync::Arc;

        /// A valid native youtrack action. `native: {}` resolves every field to
        /// its default.
        const NATIVE_MINIMAL: &str = "name: youtrack\nkind: youtrack\nnative: {}\n";

        /// Build a `WebState` backed by an in-memory DB and a tempdir-rooted
        /// `Paths`. The trigger receiver is dropped; none of the editor handlers
        /// send on it.
        async fn web_state(tmp: &std::path::Path) -> WebState {
            let pool = crate::db::test_pool().await;
            let (trigger_tx, _rx) = tokio::sync::mpsc::channel(8);
            let api = Arc::new(AppState {
                pool: pool.clone(),
                trigger_tx,
                version: "test",
                runs: Arc::new(RunRegistry::new()),
                in_flight: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
                anthropic_key: None,
                readiness: None,
                claude_login: None,
                fj_data_home: tmp.join("fj-data"),
                claude_binary: "claude".to_string(),
                agent_config_dir: None,
                settings: Arc::new(crate::settings::LiveSettings::detached(pool)),
                // Tmp, not `Paths::resolve()`: handlers that write credential
                // files must never land in the developer's real config dir.
                paths: Paths {
                    config_dir: tmp.join("config"),
                    data_dir: tmp.join("data"),
                    state_dir: tmp.join("state"),
                    runtime_dir: tmp.join("runtime"),
                },
            });
            let paths = crate::config::Paths {
                config_dir: tmp.join("config"),
                data_dir: tmp.join("data"),
                state_dir: tmp.join("state"),
                runtime_dir: tmp.join("runtime"),
            };
            WebState {
                api,
                paths,
                webhook_enabled: false,
                comment_webhook_enabled: false,
            }
        }

        /// CLAUDE-269: [`web_state`] with a real readiness gate attached, so a
        /// test can trip / clear login-required and observe it through the
        /// banner fragment, the settings page, and the auth-save handlers.
        async fn web_state_with_readiness(
            tmp: &std::path::Path,
            gate: crate::readiness::ReadinessGate,
        ) -> WebState {
            let s = web_state(tmp).await;
            let api = AppState {
                readiness: Some(gate),
                ..(*s.api).clone()
            };
            WebState {
                api: Arc::new(api),
                ..s
            }
        }

        /// Register an action in the index so `indexed_action_path` resolves it
        /// to `file_path` (the only names the edit/export handlers will touch).
        async fn index_action(pool: &DbPool, name: &str, file_path: &str) {
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
                    max_parallel: 1,
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
        async fn action_detail_renders_input_events_region() {
            // CLAUDE-147: a recorded probe-failure event is surfaced on the
            // action-detail page, so the poll/input-phase error that has no
            // per-job outcome row is not invisible. Covers the zero-job-poll
            // case: the event has a NULL run_id and still counts.
            // CLAUDE-235: the region is one counted line linking to the Logs
            // screen; the message text itself is no longer rendered here.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;

            let poll_ts = state::now_iso();
            state::run_events_record(
                &s.api.pool,
                "youtrack",
                &poll_ts,
                &[state::InputEvent {
                    level: "warn".into(),
                    context: Some("VS".into()),
                    message: "VCS probe failed (provider-CLI error); issues skipped this poll: project 'VS' not found".into(),
                }],
            )
            .await
            .unwrap();

            let resp = action_detail(
                State(s.clone()),
                AxumPath("youtrack".to_string()),
                Query(ActionDetailQuery::default()),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            assert!(body.contains("Input errors / warnings"));
            assert!(body.contains("1 poll/input-phase diagnostics"));
            assert!(body.contains(r#"href="/logs?action=youtrack""#));
            // The wall of message text is what CLAUDE-235 removed: the rows live
            // on the Logs screen, which the line above links to.
            assert!(!body.contains("VCS probe failed"));
        }

        /// Record `n` byte-identical input events for `action`, one per poll
        /// second apart, mimicking a diagnostic that re-fires every poll.
        async fn record_repeats(
            pool: &DbPool,
            action: &str,
            level: &str,
            context: &str,
            message: &str,
            stamps: &[&str],
        ) {
            for ts in stamps {
                state::run_events_record(
                    pool,
                    action,
                    ts,
                    &[state::InputEvent {
                        level: level.to_string(),
                        context: Some(context.to_string()),
                        message: message.to_string(),
                    }],
                )
                .await
                .unwrap();
            }
        }

        #[tokio::test]
        async fn action_detail_counts_repeated_events_instead_of_listing_them() {
            // CLAUDE-171 / CLAUDE-235: the motivating case. The CLAUDE-164
            // config-collision warn is recorded on every 5s poll; the region used
            // to render one byte-identical row per poll, burying every other
            // diagnostic. It is now a single counted line, and no message text
            // reaches the page at all.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();

            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "config-collision",
                "duplicate source: youtrack and youtrack-2 poll the same query",
                &[
                    "2999-01-01T00:00:00Z",
                    "2999-01-01T00:00:05Z",
                    "2999-01-01T00:00:10Z",
                    "2999-01-01T00:00:15Z",
                ],
            )
            .await;
            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "VS",
                "VCS probe failed; issues skipped this poll",
                &["2999-01-01T00:00:20Z"],
            )
            .await;

            let resp = action_detail(
                State(s.clone()),
                AxumPath("youtrack".to_string()),
                Query(ActionDetailQuery::default()),
            )
            .await
            .unwrap()
            .into_response();
            let body = body_string(resp).await;
            // One line stating five diagnostics, and not one message body.
            assert!(body.contains("5 poll/input-phase diagnostics"));
            assert!(!body.contains("duplicate source"));
            assert!(!body.contains("VCS probe failed"));
            assert!(body.contains(r#"href="/logs?action=youtrack""#));
        }

        #[tokio::test]
        async fn action_detail_count_is_bounded_to_this_serve() {
            // CLAUDE-171 / CLAUDE-235: events from a previous daemon lifetime
            // (ts < started_at) are out of the window, so the count is what this
            // serve has recorded and not the whole retained history.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();

            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "OLD",
                "from a previous serve",
                &["2000-01-01T00:00:00Z"],
            )
            .await;
            for i in 0..7 {
                record_repeats(
                    &s.api.pool,
                    "youtrack",
                    "warn",
                    "VS",
                    &format!("probe failure {i}"),
                    &[&format!("2999-01-01T00:00:0{i}Z")],
                )
                .await;
            }

            let resp = action_detail(
                State(s.clone()),
                AxumPath("youtrack".to_string()),
                Query(ActionDetailQuery::default()),
            )
            .await
            .unwrap()
            .into_response();
            let body = body_string(resp).await;
            assert!(body.contains("7 poll/input-phase diagnostics"), "{body}");
        }

        #[tokio::test]
        async fn action_events_page_lists_every_event_undeduplicated() {
            // CLAUDE-171: the log page is the counters' opposite: the same four
            // identical rows the action page states as a count must all appear
            // here, with their owning action rendered in the Action column.
            // CLAUDE-235: served by `/logs`, with no action in the path.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();

            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "config-collision",
                "duplicate source detected",
                &[
                    "2999-01-01T00:00:00Z",
                    "2999-01-01T00:00:05Z",
                    "2999-01-01T00:00:10Z",
                    "2999-01-01T00:00:15Z",
                ],
            )
            .await;

            let resp = logs(State(s.clone()), Query(LogsQuery::default()))
                .await
                .unwrap()
                .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            // No dedup: one row per recorded event, and the count says four.
            assert_eq!(body.matches("duplicate source detected").count(), 4);
            assert!(body.contains("1-4 of 4"));
            // The Action column renders `run_events.action_name`.
            assert!(body.contains(r#"<td><a href="/actions/youtrack">youtrack</a></td>"#));
        }

        #[tokio::test]
        async fn action_events_route_permanently_redirects_to_logs() {
            // CLAUDE-235: the old per-action route keeps working, landing on the
            // global page with the action already selected in the Action filter.
            // Driven through the router, so the registration is proven too.
            use tower::ServiceExt;
            let resp = action_events_redirect(AxumPath("youtrack".to_string()))
                .await
                .into_response();
            assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
            assert_eq!(
                resp.headers().get("location").unwrap(),
                "/logs?action=youtrack"
            );

            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let app = super::super::router(
                s.api.clone(),
                s.paths.clone(),
                s.webhook_enabled,
                s.comment_webhook_enabled,
            );
            let resp = app
                .oneshot(
                    axum::http::Request::builder()
                        .method("GET")
                        .uri("/actions/youtrack/events")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
            assert_eq!(
                resp.headers().get("location").unwrap(),
                "/logs?action=youtrack"
            );
        }

        #[tokio::test]
        async fn logs_route_serves_the_page() {
            // CLAUDE-235: `/logs` is registered and needs no action in the path.
            use tower::ServiceExt;
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let app = super::super::router(
                s.api.clone(),
                s.paths.clone(),
                s.webhook_enabled,
                s.comment_webhook_enabled,
            );
            let resp = app
                .oneshot(
                    axum::http::Request::builder()
                        .method("GET")
                        .uri("/logs")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            // The nav entry and the page's own heading / filters.
            assert!(body.contains(r#"<a href="/logs">Logs</a>"#));
            assert!(body.contains("<h2>Logs</h2>"));
            assert!(body.contains(r#"name="level""#));
            assert!(body.contains(r#"name="action""#));
            assert!(body.contains(r#"name="context""#));
            assert!(body.contains(r#"name="since""#));
        }

        #[tokio::test]
        async fn logs_page_unknown_action_param_widens_to_all_actions() {
            // CLAUDE-235: the page is global, so an action that is not a distinct
            // value in `run_events` drops to "all" (the whitelist) instead of the
            // 404 the per-action route used to return.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();
            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "VS",
                "VCS probe failed",
                &["2999-01-01T00:00:00Z"],
            )
            .await;

            let resp = logs(
                State(s.clone()),
                Query(LogsQuery {
                    action: Some("nope".to_string()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(body_string(resp).await.contains("VCS probe failed"));
        }

        #[tokio::test]
        async fn logs_page_summarises_the_last_24h_by_context_and_action() {
            // CLAUDE-235: the two summary cards count `warn`/`error` events in
            // the last 24h and link each row back to the same page with that
            // filter applied. `info` rows are markers, not signals, and stay out.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();
            let now = state::now_iso();
            state::run_events_record(
                &s.api.pool,
                "youtrack",
                &now,
                &[
                    state::InputEvent {
                        level: "warn".into(),
                        context: Some("CLAUDE".into()),
                        message: "probe failed".into(),
                    },
                    state::InputEvent {
                        level: "error".into(),
                        context: Some("CLAUDE".into()),
                        message: "clone failed".into(),
                    },
                    state::InputEvent {
                        level: "info".into(),
                        context: Some("QUIET".into()),
                        message: "preflight passed".into(),
                    },
                ],
            )
            .await
            .unwrap();

            let resp = logs(State(s.clone()), Query(LogsQuery::default()))
                .await
                .unwrap()
                .into_response();
            let body = body_string(resp).await;
            assert!(body.contains("Last 24h by context"));
            assert!(body.contains("Last 24h by action"));
            // Each row is a link that applies its own filter over the same 24h
            // window. Askama escapes the `&` joiner.
            assert!(
                body.contains(r#"href="/logs?since=24h&amp;context=CLAUDE""#),
                "{body}"
            );
            assert!(
                body.contains(r#"href="/logs?since=24h&amp;action=youtrack""#),
                "{body}"
            );
            // The `info` marker is counted by neither card (it is still listed in
            // the table below and offered in the Context filter).
            assert!(!body.contains(r#"href="/logs?since=24h&amp;context=QUIET""#));
        }

        #[tokio::test]
        async fn action_events_filters_are_whitelisted_and_applied() {
            // CLAUDE-171: every filter narrows the list, and a value that is not
            // an actual distinct DB value drops to "all" instead of reaching SQL
            // or being echoed back into the page. CLAUDE-235: the default is now
            // every action, and `since=24h` is a third window.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            for name in ["youtrack", "other"] {
                let file = actions.join(format!("{name}.yml"));
                std::fs::write(&file, NATIVE_MINIMAL).unwrap();
                index_action(&s.api.pool, name, file.to_str().unwrap()).await;
            }
            crate::settings::record_serve_start(&s.api.pool)
                .await
                .unwrap();

            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "VS",
                "VCS probe failed",
                &["2999-01-01T00:00:00Z"],
            )
            .await;
            record_repeats(
                &s.api.pool,
                "youtrack",
                "info",
                "VS",
                "no enabled VCS integration",
                &["2999-01-01T00:00:01Z"],
            )
            .await;
            record_repeats(
                &s.api.pool,
                "other",
                "warn",
                "config-collision",
                "duplicate source detected",
                &["2999-01-01T00:00:02Z"],
            )
            .await;
            // A previous lifetime's event: only the all-time window shows it.
            record_repeats(
                &s.api.pool,
                "youtrack",
                "warn",
                "VS",
                "ancient probe failure",
                &["2000-01-01T00:00:00Z"],
            )
            .await;

            async fn render(s: &WebState, q: LogsQuery) -> String {
                let resp = logs(State(s.clone()), Query(q))
                    .await
                    .unwrap()
                    .into_response();
                body_string(resp).await
            }

            // Default: every action, current serve. Only the previous lifetime's
            // event is out.
            let body = render(&s, LogsQuery::default()).await;
            assert!(body.contains("VCS probe failed"));
            assert!(body.contains("no enabled VCS integration"));
            assert!(body.contains("duplicate source detected"));
            assert!(!body.contains("ancient probe failure"));

            // Action filter: the link from the action page and the redirect.
            let body = render(
                &s,
                LogsQuery {
                    action: Some("youtrack".to_string()),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("VCS probe failed"));
            assert!(!body.contains("duplicate source detected"));

            // An indexed action with no events at all keeps its selection rather
            // than silently widening to every action: the action page links here
            // whether or not the action has recorded anything.
            let quiet = actions.join("quiet.yml");
            std::fs::write(&quiet, NATIVE_MINIMAL).unwrap();
            index_action(&s.api.pool, "quiet", quiet.to_str().unwrap()).await;
            let body = render(
                &s,
                LogsQuery {
                    action: Some("quiet".to_string()),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("No input events match this filter"), "{body}");

            // Level filter.
            let body = render(
                &s,
                LogsQuery {
                    level: "info".to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("no enabled VCS integration"));
            assert!(!body.contains("VCS probe failed"));

            // Context filter.
            let body = render(
                &s,
                LogsQuery {
                    context: "config-collision".to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("duplicate source detected"));
            assert!(!body.contains("VCS probe failed"));

            // Message substring search, case-insensitive.
            let body = render(
                &s,
                LogsQuery {
                    q: "PROBE".to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("VCS probe failed"));
            assert!(!body.contains("no enabled VCS integration"));

            // Since toggle: all-time reaches the previous lifetime's event, and
            // the 24h window the summary cards link to does not.
            let body = render(
                &s,
                LogsQuery {
                    since: "all".to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("ancient probe failure"));
            let body = render(
                &s,
                LogsQuery {
                    since: "24h".to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(body.contains("Showing events from the last 24 hours"));
            assert!(body.contains("VCS probe failed"));
            assert!(!body.contains("ancient probe failure"));

            // A hand-edited value that is not a distinct DB value drops to "all"
            // rather than filtering or being echoed into the page.
            let injected = "' OR 1=1 --";
            let body = render(
                &s,
                LogsQuery {
                    level: injected.to_string(),
                    context: injected.to_string(),
                    action: Some(injected.to_string()),
                    dir: injected.to_string(),
                    since: injected.to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert!(!body.contains("OR 1=1"));
            // Dropping to "all" on action/level/context, and to the default
            // (current serve) window on `since`.
            assert!(body.contains("duplicate source detected"));
            assert!(!body.contains("ancient probe failure"));
        }

        #[tokio::test]
        async fn dashboard_banner_counts_events_and_names_the_access_subset() {
            // CLAUDE-235: the dashboard states how many diagnostics there are and
            // links to the Logs screen; it renders none of the message text that
            // used to fill the first screen. CLAUDE-216: the repo-access subset
            // is named separately (its remedy is the access check) and clears
            // itself once a later poll records the recovery marker.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let failure = format!(
                "{} for `contoso/claude-run` on dev.a8n.run; issues skipped this poll",
                state::ACCESS_FAILURE_PREFIX
            );
            // Stamps inside the 24h window and strictly ordered: the recovery
            // marker only supersedes a failure recorded before it.
            state::run_events_record(
                &s.api.pool,
                "youtrack",
                "2999-01-01T00:00:00Z",
                &[
                    state::InputEvent {
                        level: "warn".into(),
                        context: Some("CLAUDE".into()),
                        message: failure.clone(),
                    },
                    state::InputEvent {
                        level: "warn".into(),
                        context: Some("CLAUDE".into()),
                        message: "duplicate source detected".into(),
                    },
                ],
            )
            .await
            .unwrap();

            let body = body_string(
                dashboard(State(s.clone()), Query(DashboardQuery::default()))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            assert!(
                body.contains("2 errors / warnings in the last 24h"),
                "{body}"
            );
            assert!(body.contains("1 of them are still-open repo-access failures"));
            assert!(body.contains(r#"href="/logs?since=24h""#));
            // The messages themselves stay on the Logs screen.
            assert!(!body.contains("duplicate source detected"));
            assert!(!body.contains(state::ACCESS_FAILURE_PREFIX));

            // A later passing preflight clears the access subset; the headline
            // count keeps the other diagnostic.
            state::run_events_record(
                &s.api.pool,
                "youtrack",
                "2999-01-01T00:00:10Z",
                &[state::InputEvent {
                    level: "info".into(),
                    context: Some("CLAUDE".into()),
                    message: format!(
                        "{} for `contoso/claude-run` on dev.a8n.run",
                        state::ACCESS_RECOVERY_PREFIX
                    ),
                }],
            )
            .await
            .unwrap();
            let body = body_string(
                dashboard(State(s.clone()), Query(DashboardQuery::default()))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            assert!(
                body.contains("2 errors / warnings in the last 24h"),
                "{body}"
            );
            assert!(!body.contains("still-open repo-access failures"));
        }

        #[test]
        fn events_view_binds_every_filter_value_as_a_parameter() {
            // CLAUDE-171: the clause text is built from fixed fragments only;
            // every value travels as a `?` bind, in clause order.
            let view = EventsView {
                dir: "asc".to_string(),
                level: "warn".to_string(),
                action: "youtrack".to_string(),
                context: "VS".to_string(),
                since: String::new(),
                q: "Probe".to_string(),
            };
            let (clause, binds) = view.where_clause(Some("2999-01-01T00:00:00Z"));
            assert_eq!(
                clause,
                " WHERE level = ? AND action_name = ? AND context = ? AND strpos(lower(message), ?) > 0 AND ts >= ?"
            );
            assert_eq!(
                binds,
                vec![
                    "warn",
                    "youtrack",
                    "VS",
                    // Lowercased for the ASCII-only `lower()` comparison.
                    "probe",
                    "2999-01-01T00:00:00Z",
                ]
            );
            assert_eq!(view.order_by(), "ORDER BY ts ASC, id ASC");

            // All-time drops the window bound; an unstamped DB does too, rather
            // than hiding every row.
            let all_time = EventsView {
                since: "all".to_string(),
                ..view.clone()
            };
            let (clause, binds) = all_time.where_clause(Some("2999-01-01T00:00:00Z"));
            assert!(!clause.contains("ts >= ?"));
            assert_eq!(binds.len(), 4);
            let (clause, _) = view.where_clause(None);
            assert!(!clause.contains("ts >= ?"));

            // No selection at all: no clause, no binds.
            let (clause, binds) = EventsView::default().where_clause(None);
            assert_eq!(clause, "");
            assert!(binds.is_empty());
            assert_eq!(
                EventsView::default().order_by(),
                "ORDER BY ts DESC, id DESC"
            );
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

        // ---- delete (CLAUDE-40) -------------------------------------------

        #[tokio::test]
        async fn action_delete_404s_unknown_409s_in_flight_then_removes() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("act.yml");
            std::fs::write(&file, "name: act\nkind: youtrack\nnative: {}\n").unwrap();
            index_action(&s.api.pool, "act", file.to_str().unwrap()).await;
            // Seed control state that the delete must purge.
            state::action_pause_set(&s.api.pool, "act", None)
                .await
                .unwrap();
            state::attempts_increment(&s.api.pool, "act", "job-1")
                .await
                .unwrap();

            // Unknown action -> 404.
            let resp = action_delete(State(s.clone()), AxumPath("ghost".to_string()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);

            // A run in flight -> 409, and the file is left untouched.
            let run = state::run_begin(&s.api.pool, "act", "cli").await.unwrap();
            let resp = action_delete(State(s.clone()), AxumPath("act".to_string()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            assert!(file.exists(), "a refused delete leaves the file on disk");
            assert!(
                !state::action_index_list(&s.api.pool)
                    .await
                    .unwrap()
                    .is_empty(),
                "a refused delete leaves the index row"
            );
            state::run_finish(&s.api.pool, run, crate::exit::OK, 0)
                .await
                .unwrap();

            // Now the delete succeeds: redirect, file gone, index + control purged.
            let resp = action_delete(State(s.clone()), AxumPath("act".to_string()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert!(!file.exists(), "the YAML file is removed");
            assert!(state::action_index_list(&s.api.pool)
                .await
                .unwrap()
                .is_empty());
            assert!(state::action_pause_status(&s.api.pool, "act")
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                state::attempts_get(&s.api.pool, "act", "job-1")
                    .await
                    .unwrap(),
                0
            );
            // History is kept.
            let runs: i64 = sqlx::query_scalar(&crate::db::q(
                &s.api.pool,
                "SELECT COUNT(*) FROM runs WHERE action_name = ?",
            ))
            .bind("act")
            .fetch_one(&s.api.pool)
            .await
            .unwrap();
            assert_eq!(runs, 1, "run history survives the delete");
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
            f.claude_model = "opus".to_string();
            f.claude_effort = "xhigh".to_string();
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
            // CLAUDE-239: the editor carries the per-action model, so saving an
            // action through the form does not drop a pinned `model:`.
            assert_eq!(a.claude.model.as_deref(), Some("opus"));
            // CLAUDE-259: same for a pinned `effort:`.
            assert_eq!(a.claude.effort.as_deref(), Some("xhigh"));
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
        fn form_assignee_round_trips_and_clears() {
            // CLAUDE-131: a filled Assignee box serializes to `assignee` and
            // loads back; a blank box emits no `assignee` key (field cleared).
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.yt_assignee = "octocat".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(yaml.contains("assignee: octocat"), "{yaml}");
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(yt.youtrack.assignee.as_deref(), Some("octocat"));

            // Clearing the box drops the key entirely (no `assignee: null`).
            f.yt_assignee = String::new();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(!yaml.contains("assignee"), "assignee leaked:\n{yaml}");
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert!(yt.youtrack.assignee.is_none());
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
        fn form_now_owns_concurrency() {
            // The form owns `concurrency`: a default form overwrites a
            // non-default block in the base rather than leaving it untouched
            // (and, being the default, the key is dropped entirely).
            let base: serde_yaml::Value = serde_yaml::from_str(
                "name: yt\nkind: youtrack\nnative: {}\nconcurrency:\n  max_parallel: 4\n",
            )
            .unwrap();
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(base, &f).unwrap();
            assert!(
                !yaml.contains("concurrency"),
                "default not omitted:\n{yaml}"
            );
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            assert_eq!(loaded.action.concurrency.max_parallel, 1);
        }

        #[test]
        fn form_writes_non_default_concurrency() {
            // Non-default max_parallel + key serialize and round-trip back to
            // the same Concurrency the form showed.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.concurrency_max_parallel = "4".to_string();
            f.concurrency_key = " project , repo ".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            assert_eq!(loaded.action.concurrency.max_parallel, 4);
            assert_eq!(
                loaded.action.concurrency.key,
                vec!["project".to_string(), "repo".to_string()]
            );
        }

        #[test]
        fn form_omits_default_concurrency() {
            // A default-valued concurrency (max_parallel 1, blank key -> the
            // default tuple) produces no `concurrency` key in the file.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.concurrency_max_parallel = "1".to_string();
            f.concurrency_key = "  ".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(
                !yaml.contains("concurrency"),
                "default concurrency leaked:\n{yaml}"
            );
        }

        #[test]
        fn form_rejects_non_numeric_concurrency() {
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.concurrency_max_parallel = "lots".to_string();
            let err = youtrack_form_to_yaml(empty_base(), &f).unwrap_err();
            assert!(err.contains("Max parallel"), "{err}");
        }

        #[test]
        fn concurrency_prefill_round_trips() {
            // An action with a non-default concurrency maps to the form and back
            // unchanged.
            let yaml = "name: yt\nkind: youtrack\nnative: {}\nconcurrency:\n  max_parallel: 3\n  key:\n    - project\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(form.concurrency_max_parallel, "3");
            assert_eq!(form.concurrency_key, "project");
            let out = youtrack_form_to_yaml(empty_base(), &form).unwrap();
            let reloaded = crate::action::validate_str(&out, "<test>").unwrap();
            assert_eq!(reloaded.action.concurrency, loaded.action.concurrency);
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
        fn github_token_ref_prefills() {
            // CLAUDE-54: a loaded action with native.github.token_ref populates
            // the form field.
            let yaml = "name: yt\nkind: youtrack\nnative:\n  github:\n    token_ref: gh-prod\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(form.github_token_ref, "gh-prod");
        }

        #[test]
        fn github_token_ref_set_and_clear_round_trip() {
            // CLAUDE-54: setting github_token_ref writes native.github.token_ref;
            // clearing it drops the key and prunes an otherwise-default github
            // block.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.github_token_ref = "gh-prod".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(yt.github.token_ref.as_deref(), Some("gh-prod"));

            // Clearing the field drops the key; the all-default github block prunes
            // away entirely.
            f.github_token_ref = String::new();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(!yaml.contains("github"), "github block leaked:\n{yaml}");
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert!(yt.github.token_ref.is_none());
        }

        #[test]
        fn github_block_preserved_on_save() {
            // CLAUDE-54 no-wipe regression: an action whose YAML carries a full
            // github block survives a structured-form save with hosts and
            // mcp_endpoint intact; only token_ref is touched by the form.
            let base: serde_yaml::Value = serde_yaml::from_str(
                "name: yt\nkind: youtrack\nnative:\n  github:\n    token_ref: gh-old\n    hosts:\n      - ghe.example.com\n    mcp_endpoint: https://ghe.example.com/mcp/\n",
            )
            .unwrap();
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.github_token_ref = "gh-new".to_string();
            let yaml = youtrack_form_to_yaml(base, &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(yt.github.token_ref.as_deref(), Some("gh-new"));
            assert_eq!(yt.github.hosts, vec!["ghe.example.com".to_string()]);
            assert_eq!(yt.github.mcp_endpoint, "https://ghe.example.com/mcp/");
        }

        #[test]
        fn dependencies_block_preserved_on_save() {
            // CLAUDE-161 no-wipe regression: an action whose YAML carries a
            // hand-written `dependencies` block survives a structured-form save
            // intact, since the form has no dependency-gate UI to reconstruct it.
            let base: serde_yaml::Value = serde_yaml::from_str(
                "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    dependencies:\n      enabled: false\n      recheck_timeout: 30s\n      block_on:\n        - depends on\n",
            )
            .unwrap();
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(base, &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            let deps = &yt.youtrack.dependencies;
            assert!(!deps.enabled, "hand-written enabled:false must survive");
            assert_eq!(deps.recheck_timeout, "30s");
            assert_eq!(deps.block_on, vec!["depends on".to_string()]);
        }

        #[test]
        fn write_projects_and_no_code_label_preserved_on_save() {
            // CLAUDE-214 no-wipe regression: `write_projects` and
            // `states.no_code` have no structured-form UI, so a save that edits
            // any other field must carry them forward rather than silently
            // narrowing the agent's create scope back to the issue's own project.
            let base: serde_yaml::Value = serde_yaml::from_str(
                "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    write_projects:\n      - AUD\n      - DEV\n    states:\n      no_code: Reported\n",
            )
            .unwrap();
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(base, &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(
                yt.youtrack.write_projects,
                vec!["AUD".to_string(), "DEV".to_string()]
            );
            assert_eq!(yt.youtrack.states.no_code_label(), "Reported");
        }

        #[test]
        fn write_projects_and_no_code_default_omit_keys() {
            // The untouched defaults write neither key, so existing action YAML
            // is byte-unchanged by this feature.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(
                !yaml.contains("write_projects"),
                "write_projects leaked:\n{yaml}"
            );
            assert!(!yaml.contains("no_code"), "no_code leaked:\n{yaml}");
        }

        #[test]
        fn automerge_round_trips_from_form() {
            // CLAUDE-133: enabling automerge and tuning the timings serializes to
            // `native.youtrack.automerge` and reloads into the same form.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.automerge_enabled = Some("on".to_string());
            f.automerge_wait_seconds = "600".to_string();
            f.automerge_check_interval_seconds = "15".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.as_ref().unwrap()
            else {
                panic!("expected youtrack native config");
            };
            assert!(yt.youtrack.automerge.enabled);
            assert_eq!(yt.youtrack.automerge.wait_seconds, 600);
            assert_eq!(yt.youtrack.automerge.check_interval_seconds, 15);

            // And the loaded action prefills the form fields it came from.
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(form.automerge_enabled.as_deref(), Some("on"));
            assert_eq!(form.automerge_wait_seconds, "600");
            assert_eq!(form.automerge_check_interval_seconds, "15");
        }

        #[test]
        fn automerge_default_omits_block() {
            // CLAUDE-133: a form left at automerge defaults (disabled, default
            // timings) writes no `automerge` block, so untouched actions are
            // unchanged.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(!yaml.contains("automerge"), "automerge leaked:\n{yaml}");
        }

        #[test]
        fn automerge_blank_timings_fall_back_to_defaults() {
            // CLAUDE-133: blank timing inputs fall back to the Automerge defaults
            // (1800 / 30) rather than erroring or blanking.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.automerge_enabled = Some("on".to_string());
            f.automerge_wait_seconds = String::new();
            f.automerge_check_interval_seconds = "  ".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert!(yt.youtrack.automerge.enabled);
            assert_eq!(yt.youtrack.automerge.wait_seconds, 1800);
            assert_eq!(yt.youtrack.automerge.check_interval_seconds, 30);
        }

        #[test]
        fn automerge_rejects_non_numeric_timing() {
            // CLAUDE-133: a non-numeric (non-blank) timing is an inline error,
            // matching the limits / concurrency fields.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.automerge_wait_seconds = "soon".to_string();
            let err = youtrack_form_to_yaml(empty_base(), &f).unwrap_err();
            assert!(err.contains("Automerge wait"), "{err}");
        }

        #[test]
        fn done_labels_round_trip_from_form() {
            // CLAUDE-133: the done / state_done labels are owned by the form;
            // non-default values serialize and reload.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.state_done = "Merged".to_string();
            f.state_state_done = "Resolved".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.as_ref().unwrap()
            else {
                panic!("expected youtrack native config");
            };
            assert_eq!(yt.youtrack.states.done, "Merged");
            assert_eq!(yt.youtrack.states.state_done, "Resolved");

            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(form.state_done, "Merged");
            assert_eq!(form.state_state_done, "Resolved");
        }

        #[test]
        fn prompt_default_writes_no_override() {
            // CLAUDE-125: a prompt left equal to the built-in default (the value
            // the form prefills) stores no `prompt` key, so the action keeps
            // tracking the built-in default.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            assert!(!yaml.contains("prompt:"), "prompt key leaked:\n{yaml}");
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert!(yt.youtrack.prompt.is_none());
        }

        #[test]
        fn prompt_override_persists_verbatim() {
            // CLAUDE-125: an edited prompt is stored as a full-replacement
            // override; browser CRLF is normalized and surrounding whitespace
            // trimmed.
            let mut f = super::super::youtrack_form_defaults();
            f.name = "yt".to_string();
            f.yt_prompt = "  Work {{key}}: {{summary}}\r\nin {{workdir}}.  ".to_string();
            let yaml = youtrack_form_to_yaml(empty_base(), &f).unwrap();
            let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
            let crate::action::NativeConfig::Youtrack(yt) = loaded.action.native.unwrap() else {
                panic!("expected youtrack native config");
            };
            assert_eq!(
                yt.youtrack.prompt.as_deref(),
                Some("Work {{key}}: {{summary}}\nin {{workdir}}.")
            );
        }

        #[test]
        fn prompt_prefills_from_saved_override() {
            // CLAUDE-125: a loaded action with a prompt override prefills the form
            // textarea with that override, not the default.
            let yaml =
                "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    prompt: custom {{key}}\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(form.yt_prompt, "custom {{key}}");
        }

        #[test]
        fn prompt_prefills_default_when_unset() {
            // CLAUDE-125: with no override, the form shows the built-in default so
            // the operator edits the real prompt rather than a blank box.
            let yaml = "name: yt\nkind: youtrack\nnative: {}\n";
            let loaded = crate::action::validate_str(yaml, "<test>").unwrap();
            let form = action_to_youtrack_form(&loaded.action).unwrap();
            assert_eq!(
                form.yt_prompt,
                crate::native::youtrack::default_youtrack_prompt()
            );
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
        async fn youtrack_create_indexes_action_immediately() {
            // CLAUDE-39: create re-indexes synchronously, so the action the
            // redirect points at is in the index the instant create returns -
            // no transient `unknown action` 404 while the daemon scan catches up.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let resp = youtrack_create(State(s.clone()), Form(filled_form()))
                .await
                .unwrap()
                .into_response();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);

            let rows = state::action_index_list(&s.api.pool).await.unwrap();
            assert!(
                rows.iter().any(|r| r.name == "yt"),
                "index missing: {rows:?}"
            );
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
            // The form posts to the create route, which lives under /actions/new/
            // so it never shadows the /actions/:name detail route.
            assert!(body.contains("/actions/new/youtrack"));
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

        /// CLAUDE-259: the Effort select preselects the stored level, and a level
        /// outside the documented set renders as its own selected option instead
        /// of falling back to blank, so saving the form cannot silently drop or
        /// rewrite a value written by hand or through the API.
        #[tokio::test]
        async fn youtrack_edit_page_preselects_the_stored_effort() {
            for (stored, expected) in [("xhigh", "xhigh"), ("colossal", "colossal")] {
                let tmp = tempfile::tempdir().unwrap();
                let s = web_state(tmp.path()).await;
                let actions = s.paths.actions_dir();
                std::fs::create_dir_all(&actions).unwrap();
                let file = actions.join("yt.yml");
                std::fs::write(
                    &file,
                    format!("name: yt\nkind: youtrack\nnative:\n  youtrack:\n    query: \"q\"\nclaude:\n  effort: {stored}\n"),
                )
                .unwrap();
                index_action(&s.api.pool, "yt", file.to_str().unwrap()).await;

                let resp = youtrack_edit_page(State(s.clone()), AxumPath("yt".to_string()))
                    .await
                    .unwrap()
                    .into_response();
                assert_eq!(resp.status(), StatusCode::OK);
                let body = body_string(resp).await;
                assert!(
                    body.contains(&format!("value=\"{expected}\" selected")),
                    "`{stored}` must render as the selected option"
                );
                // The blank "inherit the global default" option is not selected
                // while a value is stored.
                assert!(
                    !body.contains("<option value=\"\" selected>(the config.claude default)"),
                    "`{stored}` must not fall back to the blank option"
                );
            }
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

        // ---- synthetic webhook action (CLAUDE-61) -------------------------

        #[tokio::test]
        async fn action_list_includes_synthetic_only_when_webhook_enabled() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let synthetic = crate::web::webhooks::WEBHOOK_MINT_ACTION;

            // Webhook unconfigured: the synthetic entry is absent.
            let without = super::super::enriched_actions(&s.api.pool, false, false)
                .await
                .unwrap();
            assert!(
                !without.iter().any(|a| a.name == synthetic),
                "synthetic action present without webhook configured"
            );

            // Webhook configured: a single read-only synthetic entry appears.
            let with = super::super::enriched_actions(&s.api.pool, true, false)
                .await
                .unwrap();
            let entry = with
                .iter()
                .find(|a| a.name == synthetic)
                .expect("synthetic action present when webhook enabled");
            assert!(entry.synthetic, "entry flagged synthetic");
            assert!(entry.enabled, "synthetic action shows as enabled");
            assert!(entry.schedule_kind.is_none(), "event-driven: no schedule");
        }

        #[tokio::test]
        async fn synthetic_action_rejects_mutations() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            // CLAUDE-220: export, edit, and delete all 403 EVERY reserved
            // synthetic name (no YAML backs any of them).
            for synthetic in crate::web::SYNTHETIC_ACTIONS {
                let export = action_export(State(s.clone()), AxumPath(synthetic.to_string()))
                    .await
                    .unwrap();
                assert_eq!(export.status(), StatusCode::FORBIDDEN, "{synthetic}");
                let edit = action_edit_page(State(s.clone()), AxumPath(synthetic.to_string()))
                    .await
                    .unwrap();
                assert_eq!(edit.status(), StatusCode::FORBIDDEN, "{synthetic}");
                let del = action_delete(State(s.clone()), AxumPath(synthetic.to_string()))
                    .await
                    .unwrap();
                assert_eq!(del.status(), StatusCode::FORBIDDEN, "{synthetic}");
            }
        }

        /// CLAUDE-257: the two log size ceilings render on `/settings` in the
        /// `Stream log GC` card and a POST to `/settings/stream_log_gc` applies
        /// them live on the handle the daemon's log GC pass reads.
        #[tokio::test]
        async fn settings_page_renders_and_saves_the_log_size_ceilings() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let page = settings_page(
                State(s.clone()),
                Query(crate::web::SettingsQuery { saved: None }),
            )
            .await
            .unwrap();
            assert_eq!(page.status(), StatusCode::OK);
            let body = axum::body::to_bytes(page.into_body(), usize::MAX)
                .await
                .unwrap();
            let html = String::from_utf8_lossy(&body);
            assert!(html.contains("Stream log GC"), "the section card renders");
            assert!(html.contains("/settings/stream_log_gc"), "with its form");
            assert!(
                html.contains("daemon_log_max_bytes"),
                "the daemon.log ceiling has a field reference"
            );
            assert!(
                html.contains("output_file_max_bytes"),
                "the output.file ceiling has a field reference"
            );

            let saved = settings_save(
                State(s.clone()),
                AxumPath("stream_log_gc".to_string()),
                Form(SettingsForm {
                    json: r#"{"enabled":true,"retention_days":30,"daemon_log_max_bytes":4096,"output_file_max_bytes":8192}"#
                        .to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(saved.status(), StatusCode::SEE_OTHER);
            let applied = s.api.settings.stream_log_gc_handle().read().await.clone();
            assert_eq!(applied.daemon_log_max_bytes, 4096);
            assert_eq!(applied.output_file_max_bytes, 8192);
        }

        /// CLAUDE-262: the two new retention windows (`snippets/` and
        /// `webhook-logs/`) render on `/settings` in the same `Stream log GC`
        /// card and round-trip through a save on the handle the daemon's sweep
        /// loop reads.
        #[tokio::test]
        async fn settings_page_renders_and_saves_the_snippet_and_webhook_log_windows() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let page = settings_page(
                State(s.clone()),
                Query(crate::web::SettingsQuery { saved: None }),
            )
            .await
            .unwrap();
            let body = axum::body::to_bytes(page.into_body(), usize::MAX)
                .await
                .unwrap();
            let html = String::from_utf8_lossy(&body);
            assert!(
                html.contains("snippet_retention_days"),
                "the snippets/ window has a field reference"
            );
            assert!(
                html.contains("webhook_log_retention_days"),
                "the webhook-logs/ window has a field reference"
            );

            let saved = settings_save(
                State(s.clone()),
                AxumPath("stream_log_gc".to_string()),
                Form(SettingsForm {
                    json: r#"{"enabled":true,"retention_days":30,"daemon_log_max_bytes":0,"output_file_max_bytes":0,"snippet_retention_days":7,"webhook_log_retention_days":14}"#
                        .to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(saved.status(), StatusCode::SEE_OTHER);
            let applied = s.api.settings.stream_log_gc_handle().read().await.clone();
            assert_eq!(applied.snippet_retention_days, 7);
            assert_eq!(applied.webhook_log_retention_days, 14);
        }

        /// CLAUDE-220: the `Comment webhook` section renders on `/settings` and a
        /// POST to `/settings/webhook_comment` saves it, so the section is editable
        /// in the app (the JSON API round-trip is covered in `api.rs`).
        #[tokio::test]
        async fn settings_page_renders_and_saves_the_comment_webhook_section() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let page = settings_page(
                State(s.clone()),
                Query(crate::web::SettingsQuery { saved: None }),
            )
            .await
            .unwrap();
            assert_eq!(page.status(), StatusCode::OK);
            let body = axum::body::to_bytes(page.into_body(), usize::MAX)
                .await
                .unwrap();
            let html = String::from_utf8_lossy(&body);
            assert!(
                html.contains("Comment webhook"),
                "the section card is rendered"
            );
            assert!(html.contains("/settings/webhook_comment"), "with its form");
            assert!(html.contains("write_token_ref"), "and its field reference");

            // Saving persists through the same live-settings path every section
            // uses, so the listener routes the new source without a restart.
            let form = Form(SettingsForm {
                json: r#"{"trigger":"/claude","max_instruction_chars":4000,
                    "sources":{"ng":{"secret_ref":"hmac","allowed_repos":["Contoso/*"],
                    "write_token_ref":"write-pat"}}}"#
                    .to_string(),
            });
            let saved = settings_save(
                State(s.clone()),
                AxumPath("webhook_comment".to_string()),
                form,
            )
            .await
            .unwrap();
            assert_eq!(saved.status(), StatusCode::SEE_OTHER);
            assert!(s
                .api
                .settings
                .webhook_comment_handle()
                .read()
                .await
                .sources
                .contains_key("ng"));

            // An invalid payload is rejected inline and persists nothing.
            let bad = settings_save(
                State(s.clone()),
                AxumPath("webhook_comment".to_string()),
                Form(SettingsForm {
                    json: r#"{"trigger":""}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert!(s
                .api
                .settings
                .webhook_comment_handle()
                .read()
                .await
                .sources
                .contains_key("ng"));
        }

        /// CLAUDE-239: the `Claude invocation` section renders on `/settings`, and
        /// a POST to `/settings/claude` sets and clears the global default model
        /// through the same live handle `ClaudeInvoker` reads per invocation.
        #[tokio::test]
        async fn settings_page_renders_and_saves_the_claude_section() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;

            let page = settings_page(
                State(s.clone()),
                Query(crate::web::SettingsQuery { saved: None }),
            )
            .await
            .unwrap();
            assert_eq!(page.status(), StatusCode::OK);
            let body = axum::body::to_bytes(page.into_body(), usize::MAX)
                .await
                .unwrap();
            let html = String::from_utf8_lossy(&body);
            assert!(
                html.contains("Claude invocation"),
                "the section card is rendered"
            );
            assert!(html.contains("/settings/claude"), "with its form");
            assert!(html.contains("model"), "and its field reference");
            // CLAUDE-259: the effort field reference plus the level picker that
            // rewrites the `effort` key in the JSON editor, clear button included.
            assert!(html.contains("effort"), "the effort field reference");
            for level in crate::claude::EFFORT_LEVELS {
                assert!(
                    html.contains(&format!("data-effort=\"{level}\"")),
                    "no picker button for `{level}`"
                );
            }
            assert!(
                html.contains("data-effort=\"\""),
                "no way to clear the effort back to null"
            );

            let handle = s.api.settings.claude_handle();
            assert_eq!(crate::settings::read_claude(&handle).model, None);

            // Setting the default is live on the handle the invoker reads.
            let saved = settings_save(
                State(s.clone()),
                AxumPath("claude".to_string()),
                Form(SettingsForm {
                    json: r#"{"model":"opus"}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(saved.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                crate::settings::read_claude(&handle).model.as_deref(),
                Some("opus")
            );

            // CLAUDE-259: the effort level saves and clears through the same
            // document, on the same handle.
            assert_eq!(crate::settings::read_claude(&handle).effort, None);
            let saved = settings_save(
                State(s.clone()),
                AxumPath("claude".to_string()),
                Form(SettingsForm {
                    json: r#"{"model":"opus","effort":"xhigh"}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(saved.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                crate::settings::read_claude(&handle).effort.as_deref(),
                Some("xhigh")
            );
            let cleared = settings_save(
                State(s.clone()),
                AxumPath("claude".to_string()),
                Form(SettingsForm {
                    json: r#"{"model":"opus","effort":null}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(cleared.status(), StatusCode::SEE_OTHER);
            assert_eq!(crate::settings::read_claude(&handle).effort, None);

            // Clearing it returns to "no --model flag".
            let cleared = settings_save(
                State(s.clone()),
                AxumPath("claude".to_string()),
                Form(SettingsForm {
                    json: r#"{"model":null}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(cleared.status(), StatusCode::SEE_OTHER);
            assert_eq!(crate::settings::read_claude(&handle).model, None);

            // An invalid payload is rejected inline and persists nothing.
            let bad = settings_save(
                State(s.clone()),
                AxumPath("claude".to_string()),
                Form(SettingsForm {
                    json: r#"{"bogus":"x"}"#.to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(crate::settings::read_claude(&handle).model, None);
        }

        /// CLAUDE-269: while login-required, the Auth card badge reads "login
        /// expired" (not "authenticated" or "required") and shows the CLI's own
        /// message above the sign-in control.
        #[tokio::test]
        async fn settings_page_shows_login_expired_while_login_required() {
            let tmp = tempfile::tempdir().unwrap();
            let gate = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            gate.trip_login_required("OAuth session expired", "invocation");
            let s = web_state_with_readiness(tmp.path(), gate).await;

            let page = settings_page(
                State(s.clone()),
                Query(crate::web::SettingsQuery { saved: None }),
            )
            .await
            .unwrap();
            let html = body_string(page).await;
            assert!(html.contains("login expired"), "{html}");
            assert!(html.contains("OAuth session expired"), "{html}");
            assert!(
                !html.contains(">required<"),
                "must not ALSO show the plain no-credential badge"
            );
        }

        /// CLAUDE-269: an Anthropic API key save clears login-required with no
        /// restart, alongside a brokered sign-in and the status probe.
        #[tokio::test]
        async fn saving_an_api_key_clears_login_required() {
            use age::secrecy::ExposeSecret;
            // The save encrypts through the process-global key env; without a
            // key of its own this fails with 422 wherever no operator key exists.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let key_path = tmp.path().join("key.txt");
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
            std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);
            let gate = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            gate.trip_login_required("OAuth session expired", "invocation");
            assert!(gate.is_login_required());
            let s = web_state_with_readiness(tmp.path(), gate.clone()).await;

            let resp = auth_save_claude(
                State(s.clone()),
                Form(AuthClaudeForm {
                    api_key: "sk-ant-test-key".to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert!(
                !gate.is_login_required(),
                "saving an API key must clear login-required"
            );

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }

        /// CLAUDE-269: a completed brokered sign-in clears login-required with
        /// no restart, the same as an API-key save.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_completed_sign_in_clears_login_required() {
            let tmp = tempfile::tempdir().unwrap();
            let gate = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            gate.trip_login_required("OAuth session expired", "invocation");
            let base = web_state_with_readiness(tmp.path(), gate.clone()).await;

            let stub = tmp.path().join("claude-stub");
            crate::testutil::write_stub(
                &stub,
                "#!/bin/sh\n\
                 echo \"If the browser didn't open, visit: https://claude.com/cai/oauth/authorize?code=true&state=s\"\n\
                 printf 'Paste code here if prompted > '\n\
                 read code\n\
                 if [ \"$code\" = \"good-code\" ]; then exit 0; fi\n\
                 echo 'Invalid code.' >&2\n\
                 sleep 30\n",
            );
            let s = WebState {
                api: Arc::new(AppState {
                    claude_login: Some(Arc::new(crate::claude_login::LoginBroker::new())),
                    claude_binary: stub.to_str().unwrap().to_string(),
                    agent_config_dir: Some(tmp.path().to_path_buf()),
                    ..(*base.api).clone()
                }),
                ..base
            };

            let started = auth_login_start(State(s.clone())).await.unwrap();
            assert_eq!(started.status(), StatusCode::SEE_OTHER);

            let code_resp = auth_login_code(
                State(s.clone()),
                Form(AuthCodeForm {
                    code: "good-code".to_string(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(code_resp.status(), StatusCode::SEE_OTHER);
            assert!(
                !gate.is_login_required(),
                "a completed sign-in must clear login-required"
            );
        }

        /// CLAUDE-269: the shared banner fragment renders nothing when ready,
        /// the setup-incomplete wording with no credential, and the
        /// login-required wording (with the CLI's message) when tripped.
        #[tokio::test]
        async fn claude_banner_fragment_covers_both_not_ready_kinds_and_ready() {
            let tmp = tempfile::tempdir().unwrap();

            // No readiness gate at all (`None`, e.g. a test/CLI-direct context):
            // never blocks, renders nothing.
            let s = web_state(tmp.path()).await;
            let html =
                body_string(auth_claude_banner(State(s)).await.unwrap().into_response()).await;
            assert!(html.trim().is_empty(), "{html}");

            // No credential: the setup-incomplete wording, no "login required".
            let no_cred_gate = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            let s = web_state_with_readiness(tmp.path(), no_cred_gate).await;
            let html =
                body_string(auth_claude_banner(State(s)).await.unwrap().into_response()).await;
            assert!(html.contains("Setup incomplete"), "{html}");
            assert!(!html.contains("Claude login required"), "{html}");

            // Login required: the distinct wording plus the CLI's message.
            let login_gate = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            login_gate.trip_login_required("OAuth session expired", "invocation");
            let s = web_state_with_readiness(tmp.path(), login_gate).await;
            let html =
                body_string(auth_claude_banner(State(s)).await.unwrap().into_response()).await;
            assert!(html.contains("Claude login required"), "{html}");
            assert!(html.contains("OAuth session expired"), "{html}");

            // Ready: nothing rendered.
            let ready_dir = tempfile::tempdir().unwrap();
            std::fs::write(ready_dir.path().join(".credentials.json"), "{}").unwrap();
            let ready_gate =
                crate::readiness::ReadinessGate::new(ready_dir.path().to_path_buf(), None);
            let s = web_state_with_readiness(tmp.path(), ready_gate).await;
            let html =
                body_string(auth_claude_banner(State(s)).await.unwrap().into_response()).await;
            assert!(html.trim().is_empty(), "{html}");
        }

        /// CLAUDE-269: the dashboard no longer renders its own setup banner;
        /// the shared `base.html` poll region is what every page (dashboard
        /// included) carries instead.
        #[tokio::test]
        async fn dashboard_carries_the_shared_banner_poll_and_not_its_own() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let page = dashboard(State(s), Query(DashboardQuery::default()))
                .await
                .unwrap();
            let html = body_string(page.into_response()).await;
            assert!(
                html.contains("/settings/auth/claude/banner"),
                "the shared poll region must be present: {html}"
            );
            assert!(
                !html.contains("Setup incomplete: actions are not running"),
                "the dashboard's own separate banner must be gone: {html}"
            );
        }

        /// CLAUDE-220: the comment listener gets its own synthetic entry, injected
        /// on the same rule (at least one configured source) and independent of
        /// the CI-failure listener's.
        #[tokio::test]
        async fn comment_listener_has_its_own_synthetic_entry() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let comment = crate::web::comment_webhooks::WEBHOOK_COMMENT_ACTION;
            let failure = crate::web::webhooks::WEBHOOK_MINT_ACTION;

            // Neither configured: no synthetic entries at all.
            let none = super::super::enriched_actions(&s.api.pool, false, false)
                .await
                .unwrap();
            assert!(!none.iter().any(|a| a.name == comment));

            // Only the comment listener configured: only its entry appears.
            let only_comment = super::super::enriched_actions(&s.api.pool, false, true)
                .await
                .unwrap();
            assert!(
                !only_comment.iter().any(|a| a.name == failure),
                "the CI-failure entry must not ride along"
            );
            let entry = only_comment
                .iter()
                .find(|a| a.name == comment)
                .expect("comment synthetic action present");
            assert!(entry.synthetic);
            assert!(entry.enabled, "event-driven actions show as enabled");
            assert!(entry.schedule_kind.is_none(), "event-driven: no schedule");

            // Both configured: both entries appear.
            let both = super::super::enriched_actions(&s.api.pool, true, true)
                .await
                .unwrap();
            assert!(both.iter().any(|a| a.name == comment));
            assert!(both.iter().any(|a| a.name == failure));
        }

        /// CLAUDE-220: Pause / Resume works on the comment listener exactly like
        /// on the CI-failure one (the shared `paused_actions` row drives both the
        /// badge here and the accept-and-drop in the endpoint).
        #[tokio::test]
        async fn comment_synthetic_action_reflects_pause_state() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let comment = crate::web::comment_webhooks::WEBHOOK_COMMENT_ACTION;

            state::action_pause_set(&s.api.pool, comment, Some("maintenance"))
                .await
                .unwrap();
            let paused = super::super::enriched_actions(&s.api.pool, false, true)
                .await
                .unwrap();
            let entry = paused.iter().find(|a| a.name == comment).unwrap();
            let info = entry.paused.as_ref().expect("paused badge shown");
            assert_eq!(info.reason.as_deref(), Some("maintenance"));

            state::action_pause_clear(&s.api.pool, comment)
                .await
                .unwrap();
            let resumed = super::super::enriched_actions(&s.api.pool, false, true)
                .await
                .unwrap();
            assert!(resumed
                .iter()
                .find(|a| a.name == comment)
                .unwrap()
                .paused
                .is_none());
        }

        #[tokio::test]
        async fn synthetic_action_reflects_pause_state() {
            // CLAUDE-153: the synthetic webhook action participates in Pause. Its
            // `paused` field is read from the shared `paused_actions` row, so a
            // pause set on its name shows up on the dashboard (and Resume clears).
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let synthetic = crate::web::webhooks::WEBHOOK_MINT_ACTION;

            // Unpaused: the synthetic entry reports no pause.
            let before = super::super::enriched_actions(&s.api.pool, true, false)
                .await
                .unwrap();
            let entry = before.iter().find(|a| a.name == synthetic).unwrap();
            assert!(entry.paused.is_none(), "starts unpaused");

            // Pause it, then the entry reports the pause (with the reason).
            state::action_pause_set(&s.api.pool, synthetic, Some("maintenance"))
                .await
                .unwrap();
            let paused = super::super::enriched_actions(&s.api.pool, true, false)
                .await
                .unwrap();
            let entry = paused.iter().find(|a| a.name == synthetic).unwrap();
            let info = entry.paused.as_ref().expect("synthetic action is paused");
            assert_eq!(info.reason.as_deref(), Some("maintenance"));

            // Resume clears it.
            state::action_pause_clear(&s.api.pool, synthetic)
                .await
                .unwrap();
            let after = super::super::enriched_actions(&s.api.pool, true, false)
                .await
                .unwrap();
            let entry = after.iter().find(|a| a.name == synthetic).unwrap();
            assert!(entry.paused.is_none(), "resume clears the pause");
        }

        #[tokio::test]
        async fn detail_route_not_shadowed_by_youtrack_create() {
            use tower::ServiceExt;
            // An action named exactly `youtrack` must still resolve at
            // GET /actions/youtrack. The structured-create route lives under
            // /actions/new/youtrack so it does not shadow /actions/:name; a
            // bare POST /actions/youtrack would make this GET return 405.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join("youtrack.yml");
            std::fs::write(&file, "name: youtrack\nkind: youtrack\nnative: {}\n").unwrap();
            index_action(&s.api.pool, "youtrack", file.to_str().unwrap()).await;

            let app = super::super::router(
                s.api.clone(),
                s.paths.clone(),
                s.webhook_enabled,
                s.comment_webhook_enabled,
            );
            let resp = app
                .oneshot(
                    axum::http::Request::builder()
                        .method("GET")
                        .uri("/actions/youtrack")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // ---- structured editors for every native kind (CLAUDE-211) --------
        //
        // `/actions/new` is a kind picker, every native kind has a form at
        // `/actions/new/<kind>`, and `/actions/:name/form` dispatches to the
        // editor matching the action's own kind. Saves run through the same
        // `validate_str` + write path as the raw editor.

        use super::super::native_forms;
        use super::super::{
            action_form_page, action_form_save, action_new_page, action_new_raw_page,
        };
        use crate::action::NativeConfig;
        use axum::extract::RawForm;

        fn raw_form(body: &str) -> RawForm {
            RawForm(axum::body::Bytes::copy_from_slice(body.as_bytes()))
        }

        /// Write and index an action file, returning its path.
        async fn seed_action(s: &WebState, name: &str, yaml: &str) -> std::path::PathBuf {
            let actions = s.paths.actions_dir();
            std::fs::create_dir_all(&actions).unwrap();
            let file = actions.join(format!("{name}.yml"));
            std::fs::write(&file, yaml).unwrap();
            index_action(&s.api.pool, name, file.to_str().unwrap()).await;
            file
        }

        #[tokio::test]
        async fn action_new_page_lists_every_kind() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let body = body_string(
                action_new_page(State(s.clone()))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            for kind in crate::action::ActionKind::ALL {
                assert!(
                    body.contains(kind.as_str()),
                    "{} missing: {body}",
                    kind.as_str()
                );
            }
            // Every native kind links to its structured form; scripted and the
            // paste-YAML escape hatch both point at the raw editor.
            assert!(body.contains("/actions/new/youtrack"));
            assert!(body.contains("/actions/new/forgejo-ci-fix"));
            assert!(body.contains("/actions/new/ci-failure-analysis"));
            assert!(body.contains("/actions/new/ci-failure-reconcile"));
            assert!(body.contains("/actions/new/closed-issue-summary"));
            assert!(body.contains("/actions/new/raw"));
            // The raw textarea is not the default any more.
            assert!(!body.contains("<textarea"), "{body}");
        }

        #[tokio::test]
        async fn action_new_raw_page_still_serves_the_textarea() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let body = body_string(
                action_new_raw_page(State(s.clone()))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            assert!(body.contains("<textarea"));
            // and offers the picker as the guided alternative.
            assert!(body.contains("/actions/new\""), "{body}");
        }

        /// Every input a kind's form must expose: each field of its native
        /// config struct plus the shared blocks. A field dropped from the
        /// template fails here even though the Rust round trip would still pass.
        const COMMON_INPUTS: [&str; 14] = [
            "name",
            "enabled",
            "workdir_base",
            "schedule_kind",
            "schedule_spec",
            "output_file",
            "limit_max_attempts_per_key",
            "limit_max_runs_per_day",
            "limit_claude_timeout_seconds",
            "limit_daily_budget_usd",
            "limit_run_budget_usd",
            "limit_budget_warn_fraction",
            "native_enabled",
            "log_token_mode",
        ];

        #[tokio::test]
        async fn native_new_pages_render_every_field() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let claude_inputs = [
                "claude_cwd",
                "claude_allowed_tools",
                "claude_permission_mode",
                "claude_model",
                "claude_effort",
                "claude_autocompact",
            ];
            let log_token_inputs = ["log_token_global", "log_token_repos", "log_token_ref"];
            let cases: [(Response, &str, &str, Vec<&str>); 4] = [
                (
                    native_forms::forgejo_ci_fix_new_page(State(s.clone()))
                        .await
                        .unwrap(),
                    "forgejo-ci-fix",
                    "/actions/new/forgejo-ci-fix",
                    [
                        "host",
                        "repos",
                        "window_minutes",
                        "max_files",
                        "max_lines",
                        "bot_author",
                        "bot_email",
                        "skip_marker",
                        "max_pr_attempts",
                        "needs_review_label",
                        "needs_review_state",
                        "review_assignee",
                        "token",
                        "token_clear",
                    ]
                    .into_iter()
                    .chain(claude_inputs)
                    .collect(),
                ),
                (
                    native_forms::ci_failure_analysis_new_page(State(s.clone()))
                        .await
                        .unwrap(),
                    "ci-failure-analysis",
                    "/actions/new/ci-failure-analysis",
                    ["query", "summary_prefixes", "max_analysis_bytes"]
                        .into_iter()
                        .chain(claude_inputs)
                        .chain(log_token_inputs)
                        .collect(),
                ),
                (
                    native_forms::ci_failure_reconcile_new_page(State(s.clone()))
                        .await
                        .unwrap(),
                    "ci-failure-reconcile",
                    "/actions/new/ci-failure-reconcile",
                    [
                        "query",
                        "summary_prefixes",
                        "repos",
                        "resolved_state",
                        "close_comment",
                        "max_branch_resolves",
                        "close_duplicates",
                        "duplicate_comment",
                    ]
                    .into_iter()
                    .chain(log_token_inputs)
                    .collect(),
                ),
                (
                    native_forms::closed_issue_summary_new_page(State(s.clone()))
                        .await
                        .unwrap(),
                    "closed-issue-summary",
                    "/actions/new/closed-issue-summary",
                    [
                        "parents",
                        "query",
                        "custom_field",
                        "done_label",
                        "failed_label",
                        "projects",
                        "window_days",
                        "report_project",
                        "link_type",
                        "summary_template",
                        "max_listed_per_project",
                    ]
                    .into_iter()
                    .collect(),
                ),
            ];
            for (resp, kind, post_url, fields) in cases {
                assert_eq!(resp.status(), StatusCode::OK, "{kind}");
                let body = body_string(resp).await;
                // `ci-failure-reconcile` and `closed-issue-summary` run no agent
                // session, so they expose no claude block; every other input is
                // required of every kind. `closed-issue-summary` reads no forge
                // logs either, so it has no log-token block.
                let expected = fields.into_iter().chain(
                    COMMON_INPUTS
                        .into_iter()
                        .filter(|f| *f != "log_token_mode" || kind != "forgejo-ci-fix")
                        .filter(|f| *f != "log_token_mode" || kind != "closed-issue-summary"),
                );
                for field in expected {
                    assert!(
                        body.contains(&format!("name=\"{field}\"")),
                        "{kind}: no input for `{field}`"
                    );
                }
                assert!(body.contains(post_url), "{post_url} missing");
                // Every form offers the raw editor as the escape hatch.
                assert!(body.contains("/actions/new/raw"), "{kind}");
            }
        }

        #[tokio::test]
        async fn native_create_writes_each_kind() {
            for (kind, body) in [
                ("forgejo-ci-fix", "name=fix&enabled=on&native_enabled=on"),
                (
                    "ci-failure-analysis",
                    "name=analysis&enabled=on&native_enabled=on",
                ),
                (
                    "ci-failure-reconcile",
                    "name=reconcile&enabled=on&native_enabled=on",
                ),
                (
                    "closed-issue-summary",
                    "name=report&enabled=on&native_enabled=on&window_days=7&projects=CLAUDE",
                ),
            ] {
                let tmp = tempfile::tempdir().unwrap();
                let s = web_state(tmp.path()).await;
                let name = body.split('&').next().unwrap().trim_start_matches("name=");
                let resp = match kind {
                    "forgejo-ci-fix" => {
                        native_forms::forgejo_ci_fix_create(State(s.clone()), raw_form(body)).await
                    }
                    "ci-failure-analysis" => {
                        native_forms::ci_failure_analysis_create(State(s.clone()), raw_form(body))
                            .await
                    }
                    "ci-failure-reconcile" => {
                        native_forms::ci_failure_reconcile_create(State(s.clone()), raw_form(body))
                            .await
                    }
                    _ => {
                        native_forms::closed_issue_summary_create(State(s.clone()), raw_form(body))
                            .await
                    }
                }
                .unwrap();
                assert_eq!(resp.status(), StatusCode::SEE_OTHER, "{kind}");
                let file = s.paths.actions_dir().join(format!("{name}.yml"));
                let written = std::fs::read_to_string(&file).unwrap();
                let loaded = crate::action::validate_str(&written, file.to_str().unwrap()).unwrap();
                assert_eq!(loaded.action.kind.as_str(), kind);
                // Indexed synchronously, so the redirect target resolves now.
                assert!(super::super::indexed_action_path(&s.api.pool, name)
                    .await
                    .unwrap()
                    .is_some());
            }
        }

        #[tokio::test]
        async fn native_create_rejects_an_invalid_form_inline() {
            // The name is required by `validate_str`; the form must surface that
            // rejection inline and write nothing.
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let resp =
                native_forms::ci_failure_analysis_create(State(s.clone()), raw_form("enabled=on"))
                    .await
                    .unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            assert!(!s.paths.actions_dir().join(".yml").exists());
            let body = body_string(resp).await;
            assert!(body.contains("validation failed"), "{body}");
        }

        #[tokio::test]
        async fn action_form_page_dispatches_on_the_actions_kind() {
            let cases = [
                (
                    "yt",
                    "name: yt\nkind: youtrack\nnative: {}\n",
                    "name=\"yt_query\"",
                ),
                (
                    "fix",
                    "name: fix\nkind: forgejo-ci-fix\nnative:\n  host: dev.a8n.run\n",
                    "name=\"bot_author\"",
                ),
                (
                    "analysis",
                    "name: analysis\nkind: ci-failure-analysis\nnative:\n  max_analysis_bytes: 1024\n",
                    "name=\"max_analysis_bytes\"",
                ),
                (
                    "reconcile",
                    "name: reconcile\nkind: ci-failure-reconcile\nnative:\n  resolved_state: Done\n",
                    "name=\"max_branch_resolves\"",
                ),
            ];
            for (name, yaml, marker) in cases {
                let tmp = tempfile::tempdir().unwrap();
                let s = web_state(tmp.path()).await;
                seed_action(&s, name, yaml).await;
                let resp = action_form_page(State(s.clone()), AxumPath(name.to_string()))
                    .await
                    .unwrap();
                assert_eq!(resp.status(), StatusCode::OK, "{name}");
                let body = body_string(resp).await;
                assert!(body.contains(marker), "{name}: {marker} missing: {body}");
            }
        }

        #[tokio::test]
        async fn action_form_page_falls_back_to_raw_for_scripted() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            seed_action(
                &s,
                "scripted",
                "name: scripted\ninput:\n  command: echo '[]'\nprompt: hi\n",
            )
            .await;
            let resp = action_form_page(State(s.clone()), AxumPath("scripted".to_string()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                resp.headers().get("location").unwrap(),
                "/actions/scripted/edit"
            );
        }

        #[tokio::test]
        async fn action_form_page_404s_an_unknown_action() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let resp = action_form_page(State(s.clone()), AxumPath("nope".to_string()))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn action_form_save_dispatches_on_the_actions_kind() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let file = seed_action(
                &s,
                "reconcile",
                "name: reconcile\nkind: ci-failure-reconcile\nnative:\n  resolved_state: Done\n",
            )
            .await;
            let resp = action_form_save(
                State(s.clone()),
                AxumPath("reconcile".to_string()),
                raw_form(
                    "name=reconcile&enabled=on&native_enabled=on&resolved_state=Fixed\
                     &log_token_mode=per_repo&log_token_repos=contoso%2F*&log_token_ref=logs",
                ),
            )
            .await
            .unwrap();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            let written = std::fs::read_to_string(&file).unwrap();
            let loaded = crate::action::validate_str(&written, file.to_str().unwrap()).unwrap();
            let Some(NativeConfig::CiFailureReconcile(cfg)) = loaded.action.native else {
                panic!("expected a reconcile config");
            };
            assert_eq!(cfg.resolved_state, "Fixed");
            // The repeated per-repo rows survive the raw-body decode.
            assert_eq!(
                cfg.log_token_ref,
                Some(crate::action::LogTokenRef::PerRepo(vec![
                    crate::action::LogTokenEntry {
                        repos: vec!["contoso/*".to_string()],
                        token_ref: "logs".to_string(),
                    }
                ]))
            );
        }

        #[tokio::test]
        async fn action_form_save_still_serves_youtrack() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            let file = seed_action(&s, "yt", "name: yt\nkind: youtrack\nnative: {}\n").await;
            let resp = action_form_save(
                State(s.clone()),
                AxumPath("yt".to_string()),
                raw_form("name=yt&enabled=on&yt_query=mine"),
            )
            .await
            .unwrap();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);
            let written = std::fs::read_to_string(&file).unwrap();
            let loaded = crate::action::validate_str(&written, file.to_str().unwrap()).unwrap();
            let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
                panic!("expected a youtrack config");
            };
            assert_eq!(cfg.youtrack.query, "mine");
        }

        #[tokio::test]
        async fn edit_page_offers_the_form_for_every_native_kind() {
            for (name, yaml) in [
                ("yt", "name: yt\nkind: youtrack\nnative: {}\n"),
                (
                    "fix",
                    "name: fix\nkind: forgejo-ci-fix\nnative:\n  host: dev.a8n.run\n",
                ),
                (
                    "analysis",
                    "name: analysis\nkind: ci-failure-analysis\nnative:\n  max_analysis_bytes: 1024\n",
                ),
                (
                    "reconcile",
                    "name: reconcile\nkind: ci-failure-reconcile\nnative:\n  resolved_state: Done\n",
                ),
                (
                    "report",
                    "name: report\nkind: closed-issue-summary\nnative:\n  window_days: 7\n  projects: [CLAUDE]\n",
                ),
            ] {
                let tmp = tempfile::tempdir().unwrap();
                let s = web_state(tmp.path()).await;
                seed_action(&s, name, yaml).await;
                let body = body_string(
                    super::super::action_edit_page(State(s.clone()), AxumPath(name.to_string()))
                        .await
                        .unwrap(),
                )
                .await;
                assert!(
                    body.contains(&format!("/actions/{name}/form")),
                    "{name}: no structured-editor link: {body}"
                );
            }
        }

        #[tokio::test]
        async fn edit_page_offers_no_form_for_scripted() {
            let tmp = tempfile::tempdir().unwrap();
            let s = web_state(tmp.path()).await;
            seed_action(
                &s,
                "scripted",
                "name: scripted\ninput:\n  command: echo '[]'\nprompt: hi\n",
            )
            .await;
            let body = body_string(
                super::super::action_edit_page(State(s.clone()), AxumPath("scripted".to_string()))
                    .await
                    .unwrap(),
            )
            .await;
            assert!(!body.contains("/actions/scripted/form"), "{body}");
        }
    }

    // ---- access check UI (CLAUDE-216) -------------------------------------

    mod access_ui {
        use super::super::{access_page, AccessRowsTpl, AccessTpl};
        use crate::access::{AccessReport, AccessRequest, CheckKind, CheckRow, Outcome};
        use askama::Template;
        use axum::extract::{Query, State};

        fn report() -> AccessReport {
            AccessReport {
                rows: vec![
                    CheckRow {
                        target: "youtrack".into(),
                        kind: CheckKind::Identity,
                        credential: "secret `yt-prod`".into(),
                        probe: "yt auth verify".into(),
                        outcome: Outcome::Pass,
                        detail: "authenticated as `claude-run`".into(),
                        remediation: None,
                    },
                    CheckRow {
                        target: "dev.a8n.run/contoso/claude-run".into(),
                        kind: CheckKind::Repo,
                        credential: "secret `fj-prod`".into(),
                        probe: "fj repo view + pr search (Forge::repo_access)".into(),
                        outcome: Outcome::Fail,
                        detail: "HTTP 403: add `read:issue` to the token".into(),
                        remediation: Some("grant read:issue".into()),
                    },
                    CheckRow {
                        target: "dev.a8n.run/contoso/claude-run".into(),
                        kind: CheckKind::Write,
                        credential: "secret `fj-prod`".into(),
                        probe: "not probed (read-only check)".into(),
                        outcome: Outcome::Unverifiable,
                        detail: "opening a PR is a write".into(),
                        remediation: None,
                    },
                ],
            }
        }

        /// The result table renders each outcome distinctly, shows the
        /// `probe_denied` text verbatim, and names the secret without its value.
        #[test]
        fn rows_render_every_outcome_and_no_secret_value() {
            let html = AccessRowsTpl {
                report: Some(report()),
            }
            .render()
            .unwrap();
            assert!(html.contains("1 of 3 probes failed"), "{html}");
            assert!(html.contains("badge ok"), "{html}");
            assert!(html.contains("badge bad"), "{html}");
            assert!(html.contains("badge warn"), "{html}");
            assert!(html.contains("read:issue"), "{html}");
            assert!(html.contains("secret `yt-prod`"), "{html}");
            assert!(html.contains("unverifiable"), "{html}");
        }

        /// The bare page has the form and no results yet.
        #[tokio::test]
        async fn page_renders_the_form_before_any_check() {
            let tmp = tempfile::tempdir().unwrap();
            let s = super::secrets_ui::web_state(tmp.path()).await;
            let resp = access_page(State(s), Query(AccessRequest::default()))
                .await
                .unwrap();
            let body = super::secrets_ui::body_string(resp).await;
            assert!(body.contains("Check all"), "{body}");
            assert!(body.contains("No check run yet."), "{body}");
            // The targeted form takes a repo, a host, and a project key.
            for field in ["name=\"repo\"", "name=\"host\"", "name=\"project\""] {
                assert!(body.contains(field), "{field} missing: {body}");
            }
            // And the page is reachable from the nav on every page.
            assert!(body.contains("<a href=\"/access\">Access</a>"), "{body}");
        }

        /// CLAUDE-216: `/secrets` offers a per-secret identity probe.
        #[tokio::test]
        async fn secrets_page_offers_a_per_secret_check() {
            let tmp = tempfile::tempdir().unwrap();
            let s = super::secrets_ui::web_state(tmp.path()).await;
            crate::state::secret_create(&s.api.pool, "yt-prod", b"ciphertext")
                .await
                .unwrap();
            let resp = super::super::secrets_page(State(s)).await.unwrap();
            let body = super::secrets_ui::body_string(resp).await;
            assert!(body.contains("/secrets/yt-prod/check"), "{body}");
        }

        /// A report on the full page renders through the same fragment.
        #[test]
        fn page_renders_the_report_inline() {
            let html = AccessTpl {
                version: "test",
                form: AccessRequest {
                    project: Some("CLAUDE".into()),
                    ..Default::default()
                },
                report: Some(report()),
            }
            .render()
            .unwrap();
            assert!(html.contains("value=\"CLAUDE\""), "{html}");
            assert!(html.contains("1 of 3 probes failed"), "{html}");
        }
    }

    // ---- secrets UI (CLAUDE-26) -------------------------------------------
    //
    // One test owns the `CLAUDE_RUN_SECRETS_KEY_FILE` env (no other test reads
    // it), so create -> list -> rotate -> delete runs against a stable key
    // without an env race. It proves the listing is write-only: the page shows
    // a secret's name but never its value, and the stored bytes are ciphertext.
    /// Settings -> Auth: the daemon's own service logins, typed into the web UI
    /// instead of a CLI (the container has no CLI operator).
    mod service_auth_ui {
        use super::super::{
            auth_save_forgejo, auth_save_youtrack, settings_page, AuthForgejoForm,
            AuthYoutrackForm, SettingsQuery,
        };
        use super::secrets_ui::{body_string, web_state};
        use age::secrecy::ExposeSecret;
        use axum::extract::{Form, Query, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        /// Point the process-global key env at a throwaway key for one test.
        fn use_temp_key(tmp: &std::path::Path) {
            let key_path = tmp.join("key.txt");
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
            std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);
        }

        /// Saving a YouTrack login stores the token encrypted, records only the
        /// URL and the secret name in `config.auth`, and writes the single-file
        /// `yt` config the daemon exports `YOUTRACK_CLI_CONFIG` for.
        #[tokio::test]
        async fn youtrack_login_is_stored_and_materialized() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            use_temp_key(tmp.path());
            let s = web_state(tmp.path()).await;

            let resp = auth_save_youtrack(
                State(s.clone()),
                Form(AuthYoutrackForm {
                    base_url: "https://yt.example.com/".into(),
                    token: "perm:the-token".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);

            // Stored under the fixed name, and the document holds no token.
            let cfg = s.api.settings.auth_config().await;
            assert_eq!(
                cfg.youtrack_base_url.as_deref(),
                Some("https://yt.example.com"),
                "the trailing slash is trimmed"
            );
            assert_eq!(
                cfg.youtrack_token_ref.as_deref(),
                Some(crate::config::YOUTRACK_TOKEN_SECRET)
            );
            let json = serde_json::to_string(&cfg).unwrap();
            assert!(!json.contains("perm:the-token"), "no token in the document");

            // Materialized where `yt` reads it, owner-only.
            let yt_path = crate::service_auth::yt_config_path(&s.api.paths);
            let body = std::fs::read_to_string(&yt_path).unwrap();
            assert_eq!(body, "url: https://yt.example.com\ntoken: perm:the-token\n");
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&yt_path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);

            // The page reports it as configured, and never echoes the token.
            let page = body_string(
                settings_page(State(s.clone()), Query(SettingsQuery { saved: None }))
                    .await
                    .unwrap()
                    .into_response(),
            )
            .await;
            assert!(page.contains("https://yt.example.com"));
            assert!(!page.contains("perm:the-token"), "the token is write-only");

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }

        /// A pasted scheme is stripped: `fj` keys are keyed by bare host, so
        /// storing `https://host` would write a key nothing ever matches.
        #[tokio::test]
        async fn forgejo_host_is_normalized_and_keys_written() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            use_temp_key(tmp.path());
            let s = web_state(tmp.path()).await;

            let resp = auth_save_forgejo(
                State(s.clone()),
                Form(AuthForgejoForm {
                    host: "https://dev.example.com/".into(),
                    token: "fj-token".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::SEE_OTHER);

            let cfg = s.api.settings.auth_config().await;
            assert_eq!(cfg.forgejo_host.as_deref(), Some("dev.example.com"));
            assert_eq!(
                cfg.forgejo_token_ref.as_deref(),
                Some(crate::config::FORGEJO_TOKEN_SECRET)
            );

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }

        /// Half a credential is refused with the reason shown, and nothing is
        /// written: a stored URL with no token would look configured and fail on
        /// every poll.
        #[tokio::test]
        async fn a_missing_token_is_refused_and_stores_nothing() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            use_temp_key(tmp.path());
            let s = web_state(tmp.path()).await;

            let resp = auth_save_youtrack(
                State(s.clone()),
                Form(AuthYoutrackForm {
                    base_url: "https://yt.example.com".into(),
                    token: "   ".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let html = body_string(resp).await;
            assert!(html.contains("both required"), "{html}");
            assert!(s
                .api
                .settings
                .auth_config()
                .await
                .youtrack_token_ref
                .is_none());
            assert!(!crate::service_auth::yt_config_path(&s.api.paths).exists());

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }

        /// A base URL with no scheme is refused: `yt` would fail on every call
        /// with a less obvious error.
        #[tokio::test]
        async fn a_schemeless_base_url_is_refused() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            use_temp_key(tmp.path());
            let s = web_state(tmp.path()).await;

            let resp = auth_save_youtrack(
                State(s.clone()),
                Form(AuthYoutrackForm {
                    base_url: "yt.example.com".into(),
                    token: "perm:x".into(),
                }),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let html = body_string(resp).await;
            assert!(html.contains("must include a scheme"), "{html}");

            std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        }
    }

    mod secrets_ui {
        use super::super::{
            secret_create, secret_delete, secret_rotate, secrets_page, SecretCreateForm,
            SecretValueForm, WebState,
        };
        use crate::api::AppState;
        use crate::cancel::RunRegistry;
        use crate::config::Paths;
        use crate::state;
        use age::secrecy::ExposeSecret;
        use axum::extract::{Form, Path as AxumPath, State};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use std::sync::Arc;

        pub(super) async fn web_state(tmp: &std::path::Path) -> WebState {
            let pool = crate::db::test_pool().await;
            let (trigger_tx, _rx) = tokio::sync::mpsc::channel(8);
            let api = Arc::new(AppState {
                pool: pool.clone(),
                trigger_tx,
                version: "test",
                runs: Arc::new(RunRegistry::new()),
                in_flight: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
                anthropic_key: None,
                readiness: None,
                claude_login: None,
                fj_data_home: tmp.join("fj-data"),
                claude_binary: "claude".to_string(),
                agent_config_dir: None,
                settings: Arc::new(crate::settings::LiveSettings::detached(pool)),
                // Tmp, not `Paths::resolve()`: handlers that write credential
                // files must never land in the developer's real config dir.
                paths: Paths {
                    config_dir: tmp.join("config"),
                    data_dir: tmp.join("data"),
                    state_dir: tmp.join("state"),
                    runtime_dir: tmp.join("runtime"),
                },
            });
            let paths = crate::config::Paths {
                config_dir: tmp.join("config"),
                data_dir: tmp.join("data"),
                state_dir: tmp.join("state"),
                runtime_dir: tmp.join("runtime"),
            };
            WebState {
                api,
                paths,
                webhook_enabled: false,
                comment_webhook_enabled: false,
            }
        }

        pub(super) async fn body_string(resp: axum::response::Response) -> String {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        #[tokio::test]
        async fn create_list_rotate_delete_is_write_only() {
            // Serialize against the webhook auth-log tests, which also set the
            // process-global key-file env (CLAUDE-64).
            let _env = crate::secrets::key_file_env_lock().await;
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

    // ---- About page handlers (CLAUDE-129) ---------------------------------
    //
    // The GET renders version + configured train; the check / apply POSTs route
    // through `crate::update`'s non-interactive cores. Network paths are covered
    // by `update::tests`; here we cover the page render and the non-network
    // error path (an invalid configured train surfaces as a rendered message,
    // not a 500), confirming the handlers never panic the request.
    mod about {
        use super::super::{about_check, about_page, WebState};
        use crate::api::AppState;
        use crate::cancel::RunRegistry;
        use crate::config::Paths;
        use axum::extract::State;
        use axum::http::StatusCode;
        use axum::response::Response;
        use std::sync::Arc;

        async fn web_state() -> WebState {
            let pool = crate::db::test_pool().await;
            let (trigger_tx, _rx) = tokio::sync::mpsc::channel(8);
            let api = Arc::new(AppState {
                pool: pool.clone(),
                trigger_tx,
                version: "9.9.9 (testsha, 2026-06-25)",
                runs: Arc::new(RunRegistry::new()),
                in_flight: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
                anthropic_key: None,
                readiness: None,
                claude_login: None,
                fj_data_home: std::path::PathBuf::from("/nonexistent/test-fj-data"),
                claude_binary: "claude".to_string(),
                agent_config_dir: None,
                settings: Arc::new(crate::settings::LiveSettings::detached(pool)),
                paths: Paths::resolve().unwrap(),
            });
            WebState {
                api,
                paths: Paths {
                    config_dir: "/tmp/x/config".into(),
                    data_dir: "/tmp/x/data".into(),
                    state_dir: "/tmp/x/state".into(),
                    runtime_dir: "/tmp/x/runtime".into(),
                },
                webhook_enabled: false,
                comment_webhook_enabled: false,
            }
        }

        async fn body_string(resp: Response) -> String {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        /// The GET renders the build version and the configured (default
        /// `release`) train, with both update buttons present.
        #[tokio::test]
        async fn get_renders_version_and_train() {
            let s = web_state().await;
            let resp = about_page(State(s)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            assert!(body.contains("9.9.9 (testsha, 2026-06-25)"), "{body}");
            assert!(body.contains("release"), "{body}");
            assert!(body.contains("/about/check"), "{body}");
            assert!(body.contains("/about/update"), "{body}");
        }

        /// CLAUDE-223: the Activity card carries the global Pause control and
        /// the live indicator, and flips to Resume (with since / reason) once
        /// paused.
        #[tokio::test]
        async fn activity_card_offers_pause_then_resume() {
            let s = web_state().await;
            let body = body_string(about_page(State(s.clone())).await.unwrap()).await;
            assert!(body.contains("Pause all activity"), "{body}");
            assert!(body.contains(r#"hx-post="/api/pause""#), "{body}");
            assert!(body.contains(r#"hx-get="/about/activity""#), "{body}");
            assert!(!body.contains("Resume all activity"), "{body}");

            crate::state::global_pause_set(&s.api.pool, Some("rebooting"))
                .await
                .unwrap();
            let body = body_string(about_page(State(s)).await.unwrap()).await;
            assert!(body.contains("Resume all activity"), "{body}");
            assert!(body.contains(r#"hx-post="/api/resume""#), "{body}");
            assert!(body.contains("rebooting"), "{body}");
            assert!(!body.contains("Pause all activity"), "{body}");
        }

        /// CLAUDE-223: the indicator's three states. Idle says it is safe to
        /// restart; an open run row lists the run and links it; and a failed
        /// activity query renders UNKNOWN, never idle.
        #[tokio::test]
        async fn activity_indicator_renders_idle_busy_and_unknown() {
            let s = web_state().await;
            let body = body_string(
                super::super::about_activity(State(s.clone()))
                    .await
                    .unwrap(),
            )
            .await;
            assert!(body.contains("idle"), "{body}");
            assert!(body.contains("Safe to restart"), "{body}");

            let run = crate::state::run_begin(&s.api.pool, "act", "cli")
                .await
                .unwrap();
            let body = body_string(
                super::super::about_activity(State(s.clone()))
                    .await
                    .unwrap(),
            )
            .await;
            assert!(body.contains("busy"), "{body}");
            assert!(body.contains(&format!("/runs/{run}")), "{body}");
            assert!(body.contains("act"), "{body}");
            assert!(!body.contains("Safe to restart"), "{body}");

            // A failed activity query (closed pool) must render UNKNOWN, not
            // idle: "safe to restart" because the query broke is the bug this
            // state exists to prevent.
            s.api.pool.close().await;
            let body = body_string(super::super::about_activity(State(s)).await.unwrap()).await;
            assert!(body.contains("unknown"), "{body}");
            assert!(body.contains("Do not treat this as idle"), "{body}");
            assert!(!body.contains("Safe to restart"), "{body}");
        }

        /// An invalid configured train is surfaced as a rendered, non-fatal
        /// error (HTTP 200), not a 500 - the check never reaches the registry.
        #[tokio::test]
        async fn check_with_invalid_train_renders_error() {
            let s = web_state().await;
            s.api
                .settings
                .save_section(
                    crate::settings::Section::Update,
                    "{\"train\":\"nightly\",\"token_ref\":null}",
                )
                .await
                .unwrap();
            let resp = about_check(State(s)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = body_string(resp).await;
            assert!(body.contains("invalid"), "expected train error: {body}");
            assert!(body.contains("nightly"), "{body}");
        }
    }

    // ---- runs search (CLAUDE-143) -----------------------------------------

    /// `search_jobs` finds jobs by summary (always) and, opt-in, by grepping the
    /// per-attempt stream `.jsonl` logs. The motivating case is a failure whose
    /// only evidence ("No space left on device") lives in the stream, not the
    /// summary; with stream search on it must surface, and a missing/rotated
    /// stream file must not error.
    mod search {
        use super::super::{match_snippet, search_jobs, stream_match_snippet, StreamGrep};
        use crate::db::DbPool;

        async fn pool() -> DbPool {
            crate::db::test_pool().await
        }

        async fn seed_run(pool: &DbPool, id: i64) {
            sqlx::query(&crate::db::q(
                pool,
                "INSERT INTO runs (id, action_name, started_at, triggered_by) \
                 VALUES (?, 'act', '2026-06-26T00:00:00+00:00', 'schedule')",
            ))
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
        }

        #[allow(clippy::too_many_arguments)]
        async fn seed_job(
            pool: &DbPool,
            run_id: i64,
            job_key: &str,
            attempt: i64,
            ok: bool,
            ts: &str,
            summary: Option<&str>,
            stream_log_path: Option<&str>,
        ) {
            sqlx::query(&crate::db::q(pool,
                "INSERT INTO job_outcomes \
                 (run_id, action_name, job_key, attempt, ts, ok, claude_ran, claude_ok, summary, stream_log_path) \
                 VALUES (?, 'act', ?, ?, ?, ?, 1, ?, ?, ?)",
            ))
            .bind(run_id)
            .bind(job_key)
            .bind(attempt)
            .bind(ts)
            .bind(ok as i64)
            .bind(ok as i64)
            .bind(summary)
            .bind(stream_log_path)
            .execute(pool)
            .await
            .unwrap();
        }

        /// Summary search is case-insensitive substring and always on; stream
        /// search stays off here.
        #[tokio::test]
        async fn summary_match_is_case_insensitive_substring() {
            let pool = pool().await;
            let tmp = tempfile::tempdir().unwrap();
            let paths = crate::config::Paths {
                config_dir: tmp.path().join("config"),
                data_dir: tmp.path().join("data"),
                state_dir: tmp.path().join("state"),
                runtime_dir: tmp.path().join("run"),
            };
            seed_run(&pool, 1).await;
            seed_job(
                &pool,
                1,
                "CLAUDE-1",
                1,
                true,
                "2026-06-26T00:01:00+00:00",
                Some("Implemented the WIDGET refactor"),
                None,
            )
            .await;
            seed_job(
                &pool,
                1,
                "CLAUDE-2",
                1,
                true,
                "2026-06-26T00:02:00+00:00",
                Some("Unrelated work"),
                None,
            )
            .await;

            let view = search_jobs(&pool, &paths, "widget", false).await.unwrap();
            assert_eq!(view.hits.len(), 1);
            assert_eq!(view.hits[0].job_key, "CLAUDE-1");
            assert!(view.hits[0].matched_summary);
            assert!(!view.hits[0].matched_stream);
        }

        /// The disk-space scenario: a failed job whose summary does NOT mention
        /// the cause but whose stream log does. Summary-only search misses it;
        /// enabling stream search surfaces it with a snippet and the stream
        /// badge.
        #[tokio::test]
        async fn stream_search_finds_disk_space_failure() {
            let tmp = tempfile::tempdir().unwrap();
            let paths = crate::config::Paths {
                config_dir: tmp.path().join("config"),
                data_dir: tmp.path().join("data"),
                state_dir: tmp.path().join("state"),
                runtime_dir: tmp.path().join("run"),
            };
            let log = tmp.path().join("CLAUDE-9-attempt-1.jsonl");
            std::fs::write(
                &log,
                "{\"type\":\"system\",\"subtype\":\"init\"}\n\
                 {\"type\":\"result\",\"result\":\"write failed: No space left on device\"}\n",
            )
            .unwrap();

            let pool = pool().await;
            seed_run(&pool, 1).await;
            seed_job(
                &pool,
                1,
                "CLAUDE-9",
                1,
                false,
                "2026-06-26T00:01:00+00:00",
                Some("Run failed"),
                Some(log.to_str().unwrap()),
            )
            .await;

            // Summary-only: the cause is invisible.
            let summary_only = search_jobs(&pool, &paths, "No space left on device", false)
                .await
                .unwrap();
            assert!(summary_only.hits.is_empty());

            // With stream search: found, with a snippet and the stream badge.
            let view = search_jobs(&pool, &paths, "No space left on device", true)
                .await
                .unwrap();
            assert_eq!(view.hits.len(), 1);
            assert_eq!(view.hits[0].job_key, "CLAUDE-9");
            assert!(view.hits[0].matched_stream);
            assert!(view.hits[0].snippet.contains("No space left on device"));
            assert_eq!(view.files_searched, 1);
            assert!(!view.capped);
        }

        /// A stream search whose summary AND stream both match collapses to one
        /// hit carrying both badges, and a candidate whose recorded stream file
        /// is missing (rotated/deleted) is skipped without error.
        #[tokio::test]
        async fn missing_stream_file_is_skipped_and_badges_merge() {
            let tmp = tempfile::tempdir().unwrap();
            let paths = crate::config::Paths {
                config_dir: tmp.path().join("config"),
                data_dir: tmp.path().join("data"),
                state_dir: tmp.path().join("state"),
                runtime_dir: tmp.path().join("run"),
            };
            let log = tmp.path().join("CLAUDE-3-attempt-1.jsonl");
            std::fs::write(&log, "{\"result\":\"boom token here\"}\n").unwrap();
            let gone = tmp.path().join("CLAUDE-4-attempt-1.jsonl");

            let pool = pool().await;
            seed_run(&pool, 1).await;
            // Summary and stream both mention "token".
            seed_job(
                &pool,
                1,
                "CLAUDE-3",
                1,
                false,
                "2026-06-26T00:02:00+00:00",
                Some("failed: token mismatch"),
                Some(log.to_str().unwrap()),
            )
            .await;
            // Failed job whose recorded stream file does not exist on disk.
            seed_job(
                &pool,
                1,
                "CLAUDE-4",
                1,
                false,
                "2026-06-26T00:01:00+00:00",
                Some("no match here"),
                Some(gone.to_str().unwrap()),
            )
            .await;

            let view = search_jobs(&pool, &paths, "token", true).await.unwrap();
            assert_eq!(view.hits.len(), 1);
            let hit = &view.hits[0];
            assert_eq!(hit.job_key, "CLAUDE-3");
            assert!(hit.matched_summary && hit.matched_stream);
            // The missing file was not opened, so it is not counted.
            assert_eq!(view.files_searched, 1);
            // Both files were readable; nothing to report as skipped.
            assert!(view.unreadable.is_empty());
        }

        /// CLAUDE-246: a log that opens and then fails to read (invalid UTF-8 is
        /// the cheapest real read error) is `Unreadable`, never the `Searched`
        /// no-match value a whole-file scan would produce.
        #[tokio::test]
        async fn read_failure_is_not_a_clean_no_match() {
            let tmp = tempfile::tempdir().unwrap();
            let log = tmp.path().join("CLAUDE-5-attempt-1.jsonl");
            // Valid line first, then a line the UTF-8 decode in `lines()` rejects.
            let mut bytes = b"{\"result\":\"fine\"}\n".to_vec();
            bytes.extend_from_slice(&[0xff, 0xfe, b'\n']);
            std::fs::write(&log, &bytes).unwrap();

            let grep = stream_match_snippet(&log, "token", 1 << 20).await;
            match grep {
                StreamGrep::Unreadable(e) => assert!(!e.is_empty()),
                other => panic!("expected Unreadable, got {other:?}"),
            }
        }

        /// The same failure at the search surface: the run is reported in
        /// `unreadable` (with its path and cause) rather than counted as a
        /// searched file that did not match, and it is distinct from the missing
        /// file, which is skipped silently.
        #[tokio::test]
        async fn unreadable_stream_is_reported_not_counted_as_no_match() {
            let tmp = tempfile::tempdir().unwrap();
            let paths = crate::config::Paths {
                config_dir: tmp.path().join("config"),
                data_dir: tmp.path().join("data"),
                state_dir: tmp.path().join("state"),
                runtime_dir: tmp.path().join("run"),
            };
            let bad = tmp.path().join("CLAUDE-5-attempt-1.jsonl");
            std::fs::write(&bad, [0xff, 0xfe, b'\n']).unwrap();
            let good = tmp.path().join("CLAUDE-6-attempt-1.jsonl");
            std::fs::write(&good, "{\"result\":\"nothing of note\"}\n").unwrap();
            let gone = tmp.path().join("CLAUDE-7-attempt-1.jsonl");

            let pool = pool().await;
            seed_run(&pool, 1).await;
            for (key, path) in [("CLAUDE-5", &bad), ("CLAUDE-6", &good), ("CLAUDE-7", &gone)] {
                seed_job(
                    &pool,
                    1,
                    key,
                    1,
                    false,
                    "2026-06-26T00:01:00+00:00",
                    Some("Run failed"),
                    Some(path.to_str().unwrap()),
                )
                .await;
            }

            let view = search_jobs(&pool, &paths, "token", true).await.unwrap();
            assert!(view.hits.is_empty());
            // Only the readable log was scanned end to end.
            assert_eq!(view.files_searched, 1);
            // The unreadable one is named; the missing one stays silent.
            assert_eq!(view.unreadable.len(), 1);
            let u = &view.unreadable[0];
            assert_eq!(u.job_key, "CLAUDE-5");
            assert_eq!(u.attempt, 1);
            assert_eq!(u.path, bad.to_str().unwrap());
            assert!(!u.error.is_empty());
        }

        /// The snippet centers a single-line window on the match and flattens
        /// control characters, falling back to the leading text when the match
        /// is not in the shown haystack.
        #[test]
        fn snippet_windows_around_match() {
            let line = "{\"type\":\"result\",\"result\":\"oops: No space left on device\"}";
            let s = match_snippet(line, "no space left on device");
            assert!(s.contains("No space left on device"));
            assert!(!s.contains('\n'));
        }
    }
}
