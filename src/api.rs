//! HTTP API exposed by the daemon. Phase 2 serves it over a unix socket at
//! `$RUNTIME_DIR/daemon.sock`. Phase 3 binds the same router to TCP for the
//! web UI.
//!
//! Endpoints:
//!
//! | Method | Path | Purpose |
//! |---|---|---|
//! | GET | `/status` | daemon status: actions, pause state, last runs |
//! | GET | `/actions` | list known actions (DB-indexed) |
//! | POST | `/actions/<name>/trigger` | queue an immediate run |
//! | POST | `/actions/<name>/pause` | pause a single action |
//! | POST | `/actions/<name>/resume` | resume a single action |
//! | POST | `/actions/<name>/delete` | delete an action (file + index + control state) |
//! | POST | `/runs/<id>/cancel` | cancel an in-flight run |
//! | POST | `/pause` | global pause |
//! | POST | `/resume` | global resume |
//! | GET | `/report/{summary,daily,weekly,by-weekday,by-hour}` | historical aggregations (CLAUDE-117) |
//! | POST | `/access/check` | on-demand credential / repo / project access check (CLAUDE-216) |
//!
//! `POST /actions/<name>/delete` removes the YAML file, the `actions` index
//! row, and the action's control state (`paused_actions`, `attempt_counters`,
//! `daily_ledger`). It KEEPS run history (`runs`, `job_outcomes`, on-disk
//! stream logs) for audit. It returns 404 for an unknown action and 409 when a
//! run for that action is in flight (the in-memory in-flight set or a `runs`
//! row with no `ended_at`).

use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::cancel::RunRegistry;
use crate::state;

/// CLAUDE-40: the daemon's in-memory set of action names with a dispatched run.
/// Shared between the dispatcher (which bumps/drops a per-action refcount as runs
/// start and finish) and the API delete handler (which consults it for the
/// in-flight guard, authoritative for a job dispatched before its `runs` row is
/// written). CLAUDE-162: a refcount rather than a set, since overlapping runs
/// mean several runs for one action can be dispatched-but-not-recorded at once; a
/// positive count means "in flight".
pub type InFlight = Arc<tokio::sync::Mutex<std::collections::HashMap<String, usize>>>;

/// Shared state injected into every handler. `Clone` is trivial because the
/// fields are already cheap-clone (`SqlitePool` is `Arc`-shaped, `Sender`
/// is `Clone`, version is `&'static`).
#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    /// One-shot trigger channel: dropping a name into here asks the scheduler
    /// to queue an out-of-cycle run for that action.
    pub trigger_tx: mpsc::Sender<String>,
    pub version: &'static str,
    /// CLAUDE-9: per-run cancellation tokens. The cancel handler trips the
    /// token for a given run id; the dispatcher registers/unregisters them.
    pub runs: Arc<RunRegistry>,
    /// CLAUDE-40: shared with the dispatcher so the delete handler can refuse a
    /// delete for an action whose run is dispatched but not yet in `runs`.
    pub in_flight: InFlight,
    /// CLAUDE-101: live runtime settings (mint/update/webhook/log_filter) backing
    /// the Settings page and the `/api/settings/<section>` endpoints.
    pub settings: Arc<crate::settings::LiveSettings>,
    /// CLAUDE-216: resolved on-disk locations, so the access check can write its
    /// throwaway credential files under the runtime dir the daemon already owns.
    pub paths: crate::config::Paths,
}

/// JSON API router. Phase 2 mounted it at root over the unix socket;
/// Phase 3 mounts it under `/api/` over TCP (alongside the web UI). The
/// unix socket continues to serve it at root for cheaper CLI calls.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/status", get(get_status))
        .route("/actions", get(list_actions))
        .route("/actions/:name/trigger", post(trigger_action))
        .route("/actions/:name/pause", post(pause_action))
        .route("/actions/:name/resume", post(resume_action))
        .route("/actions/:name/delete", post(delete_action))
        .route("/runs/:id/cancel", post(cancel_run))
        .route("/pause", post(global_pause_handler))
        .route("/resume", post(global_resume_handler))
        // CLAUDE-101: JSON settings round-trip, one section per request. Mounted
        // under `/api` over TCP (behind the CLAUDE-48 auth gate) and at root over
        // the unix socket. `<section>` is mint|update|webhook|log_filter.
        .route(
            "/settings/:section",
            get(get_settings_section).put(put_settings_section),
        )
        // CLAUDE-117: reporting aggregations. Each accepts `range` (30d|90d|all,
        // default all) and an optional `action` filter, returning the series the
        // `/reports` page also renders. Mounted under `/api` over TCP.
        .route("/report/summary", get(report_summary))
        .route("/report/daily", get(report_daily))
        .route("/report/weekly", get(report_weekly))
        .route("/report/by-weekday", get(report_by_weekday))
        .route("/report/by-hour", get(report_by_hour))
        // CLAUDE-216: on-demand access check. Same typed report the `/access`
        // page renders and `claude-run access check` prints, so the three
        // surfaces cannot drift.
        .route("/access/check", post(access_check))
        .with_state(Arc::new(state))
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    version: String,
    global_paused: Option<PauseInfo>,
    actions: Vec<ActionStatus>,
}

#[derive(Debug, Serialize)]
struct PauseInfo {
    paused_at: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActionStatus {
    name: String,
    enabled: bool,
    schedule_kind: Option<String>,
    schedule_spec: Option<String>,
    paused: Option<PauseInfo>,
    last_run_started: Option<String>,
    parse_error: Option<String>,
}

async fn get_status(State(s): State<Arc<AppState>>) -> Result<Json<StatusResponse>, AppError> {
    let global = state::global_pause_status(&s.pool)
        .await?
        .map(|(a, r)| PauseInfo {
            paused_at: a,
            reason: r,
        });
    let actions = state::action_index_list(&s.pool).await?;
    let mut out = Vec::with_capacity(actions.len());
    for a in actions {
        let paused = state::action_pause_status(&s.pool, &a.name)
            .await?
            .map(|(at, r)| PauseInfo {
                paused_at: at,
                reason: r,
            });
        let last = state::action_last_run_started(&s.pool, &a.name).await?;
        out.push(ActionStatus {
            name: a.name,
            enabled: a.enabled,
            schedule_kind: a.schedule_kind,
            schedule_spec: a.schedule_spec,
            paused,
            last_run_started: last,
            parse_error: a.parse_error,
        });
    }
    Ok(Json(StatusResponse {
        version: s.version.to_string(),
        global_paused: global,
        actions: out,
    }))
}

async fn list_actions(
    State(s): State<Arc<AppState>>,
) -> Result<Json<Vec<state::ActionIndexRow>>, AppError> {
    Ok(Json(state::action_index_list(&s.pool).await?))
}

#[derive(Debug, Serialize)]
struct AckResponse {
    ok: bool,
    message: String,
}

async fn trigger_action(
    State(s): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AckResponse>, AppError> {
    // CLAUDE-61: the synthetic `webhook-forgejo` action is event-driven; it has
    // no input/prompt and the scheduler never dispatches it, so a manual trigger
    // is meaningless. Reject it explicitly rather than 404 as "unknown".
    if name == crate::web::webhooks::WEBHOOK_MINT_ACTION {
        return Err(AppError {
            status: StatusCode::BAD_REQUEST,
            message: format!(
                "`{name}` is an event-driven synthetic action and cannot be triggered"
            ),
        });
    }
    // Confirm the action exists before queueing.
    let rows = state::action_index_list(&s.pool).await?;
    if !rows.iter().any(|r| r.name == name) {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: format!("unknown action `{name}`"),
        });
    }
    s.trigger_tx
        .send(name.clone())
        .await
        .map_err(|e| AppError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: format!("trigger queue full or daemon shutting down: {e}"),
        })?;
    Ok(Json(AckResponse {
        ok: true,
        message: format!("queued trigger for `{name}`"),
    }))
}

#[derive(Debug, Deserialize, Default)]
struct PauseRequest {
    #[serde(default)]
    reason: Option<String>,
}

async fn pause_action(
    State(s): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
    body: Option<Json<PauseRequest>>,
) -> Result<Json<AckResponse>, AppError> {
    let reason = body.and_then(|b| b.0.reason);
    state::action_pause_set(&s.pool, &name, reason.as_deref()).await?;
    Ok(Json(AckResponse {
        ok: true,
        message: format!("paused `{name}`"),
    }))
}

async fn resume_action(
    State(s): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AckResponse>, AppError> {
    state::action_pause_clear(&s.pool, &name).await?;
    Ok(Json(AckResponse {
        ok: true,
        message: format!("resumed `{name}`"),
    }))
}

/// CLAUDE-40: delete an action from every surface. Refuses (409) when a run is
/// in flight, 404s an unknown action, otherwise removes the YAML file, the
/// `actions` index row, and the control state (`paused_actions`,
/// `attempt_counters`, `daily_ledger`) while keeping run history.
async fn delete_action(
    State(s): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<AckResponse>, AppError> {
    // CLAUDE-61: the synthetic `webhook-forgejo` action has no backing YAML to
    // remove; refuse deletion explicitly.
    if name == crate::web::webhooks::WEBHOOK_MINT_ACTION {
        return Err(AppError {
            status: StatusCode::FORBIDDEN,
            message: format!("`{name}` is a synthetic event-driven action and cannot be deleted"),
        });
    }
    // 404 before any side effect: only indexed actions are deletable.
    let rows = state::action_index_list(&s.pool).await?;
    if !rows.iter().any(|r| r.name == name) {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: format!("unknown action `{name}`"),
        });
    }
    // 409 if a run is in flight: the in-memory set catches a job dispatched
    // before its `runs` row exists; the DB query catches a recorded live run.
    let dispatched = s.in_flight.lock().await.get(&name).is_some_and(|&n| n > 0);
    if dispatched || state::action_has_active_run(&s.pool, &name).await? {
        return Err(AppError {
            status: StatusCode::CONFLICT,
            message: format!("action `{name}` has a run in flight; cancel or wait before deleting"),
        });
    }
    delete_indexed_action(&s.pool, &name).await?;
    Ok(Json(AckResponse {
        ok: true,
        message: format!("deleted `{name}`"),
    }))
}

/// CLAUDE-40: remove an indexed action's YAML file (tolerating an already-gone
/// file) and purge its derived index + control state. Run history is kept.
/// Returns `false` when `name` is not in the index (unknown action). Shared by
/// the API, web, and CLI-direct delete paths; the in-flight guard is each
/// caller's responsibility (the API additionally consults its in-flight set).
pub async fn delete_indexed_action(
    pool: &SqlitePool,
    name: &str,
) -> Result<bool, crate::error::Error> {
    let rows = state::action_index_list(pool).await?;
    let Some(row) = rows.into_iter().find(|r| r.name == name) else {
        return Ok(false);
    };
    match std::fs::remove_file(&row.file_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    state::action_purge_state(pool, name).await?;
    Ok(true)
}

/// CLAUDE-9: cancel an in-flight run. Tripping the token tears the run down:
/// the claude child is terminated, the in-flight job runs its `post` phase
/// with a synthesized cancelled result, and the run finalizes with
/// `exit::CANCELLED`. Cancelling a run that is not in flight (already finished,
/// never existed, or owned by a different process) is an idempotent no-op that
/// still returns `ok: true` with an explanatory message.
async fn cancel_run(
    State(s): State<Arc<AppState>>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<AckResponse>, AppError> {
    if s.runs.cancel(id) {
        Ok(Json(AckResponse {
            ok: true,
            message: format!("cancelling run #{id}"),
        }))
    } else {
        Ok(Json(AckResponse {
            ok: true,
            message: format!("run #{id} is not in flight; nothing to cancel"),
        }))
    }
}

async fn global_pause_handler(
    State(s): State<Arc<AppState>>,
    body: Option<Json<PauseRequest>>,
) -> Result<Json<AckResponse>, AppError> {
    let reason = body.and_then(|b| b.0.reason);
    state::global_pause_set(&s.pool, reason.as_deref()).await?;
    Ok(Json(AckResponse {
        ok: true,
        message: "globally paused".into(),
    }))
}

async fn global_resume_handler(
    State(s): State<Arc<AppState>>,
) -> Result<Json<AckResponse>, AppError> {
    state::global_pause_clear(&s.pool).await?;
    Ok(Json(AckResponse {
        ok: true,
        message: "global pause cleared".into(),
    }))
}

/// CLAUDE-101: resolve the URL path segment into a settings section or 404.
fn parse_section(section: &str) -> Result<crate::settings::Section, AppError> {
    crate::settings::Section::from_path(section).ok_or_else(|| AppError {
        status: StatusCode::NOT_FOUND,
        message: format!("unknown settings section `{section}`"),
    })
}

/// `GET /api/settings/<section>` returns the section's current JSON document.
/// Secrets appear as their `*_ref` names only.
async fn get_settings_section(
    State(s): State<Arc<AppState>>,
    AxumPath(section): AxumPath<String>,
) -> Result<axum::response::Response, AppError> {
    let section = parse_section(&section)?;
    let json = s.settings.section_json(section).await;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        json,
    )
        .into_response())
}

/// `PUT /api/settings/<section>` validates the JSON body against the section's
/// config struct, persists it, and applies it live. A serde error 400s with the
/// message; nothing is persisted on rejection.
async fn put_settings_section(
    State(s): State<Arc<AppState>>,
    AxumPath(section): AxumPath<String>,
    body: String,
) -> Result<Json<AckResponse>, AppError> {
    let section = parse_section(&section)?;
    match s.settings.save_section(section, &body).await {
        Ok(()) => Ok(Json(AckResponse {
            ok: true,
            message: format!("saved `{}` settings", section.as_path()),
        })),
        Err(crate::settings::SaveError::Invalid(m)) => Err(AppError {
            status: StatusCode::BAD_REQUEST,
            message: m,
        }),
        Err(crate::settings::SaveError::Db(e)) => Err(AppError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }),
    }
}

// ---- CLAUDE-117: reporting endpoints --------------------------------------

/// Query params shared by every `/api/report/*` endpoint. Both fields are
/// optional: `range` defaults to all-time, an empty / absent `action` means
/// global totals across all actions.
#[derive(Debug, Deserialize, Default)]
struct ReportQuery {
    #[serde(default)]
    range: Option<String>,
    #[serde(default)]
    action: Option<String>,
}

impl ReportQuery {
    fn range(&self) -> crate::report::Range {
        crate::report::Range::from_param(self.range.as_deref().unwrap_or("all"))
    }

    /// The action filter, treating an empty string (the "All actions" form
    /// option) as no filter.
    fn action(&self) -> Option<&str> {
        self.action.as_deref().filter(|a| !a.is_empty())
    }
}

async fn report_summary(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<crate::report::Summary>, AppError> {
    Ok(Json(
        crate::report::summary(&s.pool, q.range(), q.action()).await?,
    ))
}

async fn report_daily(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<Vec<crate::report::DailyPoint>>, AppError> {
    Ok(Json(
        crate::report::daily(&s.pool, q.range(), q.action()).await?,
    ))
}

async fn report_weekly(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<Vec<crate::report::WeeklyPoint>>, AppError> {
    Ok(Json(
        crate::report::weekly(&s.pool, q.range(), q.action()).await?,
    ))
}

async fn report_by_weekday(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<Vec<crate::report::WeekdayBucket>>, AppError> {
    Ok(Json(
        crate::report::by_weekday(&s.pool, q.range(), q.action()).await?,
    ))
}

async fn report_by_hour(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ReportQuery>,
) -> Result<Json<Vec<crate::report::HourBucket>>, AppError> {
    Ok(Json(
        crate::report::by_hour(&s.pool, q.range(), q.action()).await?,
    ))
}

// ---- CLAUDE-216: access check ---------------------------------------------

/// The credential base directory every access check materializes into: the same
/// `credentials` tree the per-job path uses, under its own `access` namespace.
/// Each probe still gets a unique subdirectory and scrubs it on drop.
pub fn access_cred_base(paths: &crate::config::Paths) -> std::path::PathBuf {
    paths.runtime_dir.join("credentials").join("access")
}

/// `POST /access/check`: run the on-demand access check and return the typed
/// report. An absent / empty body checks everything. No secret value is ever
/// part of the response (see `crate::access`).
async fn access_check(
    State(s): State<Arc<AppState>>,
    body: String,
) -> Result<Json<crate::access::AccessReport>, AppError> {
    let req: crate::access::AccessRequest = if body.trim().is_empty() {
        crate::access::AccessRequest::default()
    } else {
        serde_json::from_str(&body).map_err(|e| AppError {
            status: StatusCode::BAD_REQUEST,
            message: format!("invalid access check request: {e}"),
        })?
    };
    let report =
        crate::access::check(&s.pool, &access_cred_base(&s.paths), &req.normalized()).await?;
    Ok(Json(report))
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl From<crate::error::Error> for AppError {
    fn from(e: crate::error::Error) -> Self {
        // CLAUDE-40: an in-flight refusal maps to 409, not a 500.
        let status = match &e {
            crate::error::Error::ActionInFlight { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.message})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MIGRATOR;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn app_state() -> Arc<AppState> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let (trigger_tx, _rx) = mpsc::channel(8);
        Arc::new(AppState {
            pool: pool.clone(),
            trigger_tx,
            version: "test",
            runs: Arc::new(RunRegistry::new()),
            in_flight: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            settings: Arc::new(crate::settings::LiveSettings::detached(pool)),
            paths: crate::config::Paths::resolve().unwrap(),
        })
    }

    /// CLAUDE-216: `POST /access/check` returns the typed report as JSON. With
    /// no indexed action there is nothing to probe, and the endpoint says so as
    /// a failing row rather than an empty 200 that reads as "all good".
    #[tokio::test]
    async fn access_check_returns_the_typed_report() {
        let s = app_state().await;
        let Json(report) = access_check(State(s.clone()), String::new()).await.unwrap();
        assert!(!report.ok(), "{report:?}");
        assert_eq!(report.rows.len(), 1);
        assert_eq!(report.rows[0].kind, crate::access::CheckKind::Setup);
        assert!(report.rows[0]
            .detail
            .contains("no enabled `youtrack` action"));

        // A malformed body is a 400, not a 500.
        let err = access_check(State(s), "{ not json".to_string())
            .await
            .expect_err("malformed body must be rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// CLAUDE-101: an unknown section path 404s; a valid one round-trips JSON
    /// through the same `LiveSettings` save path the web page uses.
    #[tokio::test]
    async fn settings_section_get_and_put_round_trip() {
        let s = app_state().await;
        // Unknown section -> 404.
        let err = get_settings_section(State(s.clone()), AxumPath("bogus".into()))
            .await
            .expect_err("unknown section must 404");
        assert_eq!(err.status, StatusCode::NOT_FOUND);

        // Valid PUT persists; subsequent GET reflects it.
        let _ = put_settings_section(
            State(s.clone()),
            AxumPath("update".into()),
            r#"{"train":"latest"}"#.to_string(),
        )
        .await
        .expect("valid put");
        let resp = get_settings_section(State(s.clone()), AxumPath("update".into()))
            .await
            .expect("get");
        assert_eq!(resp.status(), StatusCode::OK);

        // Invalid PUT (unknown field) -> 400, persists nothing.
        let err = put_settings_section(
            State(s.clone()),
            AxumPath("update".into()),
            r#"{"train":"latest","nope":true}"#.to_string(),
        )
        .await
        .expect_err("invalid put must 400");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    /// CLAUDE-61: the event-driven synthetic action cannot be triggered.
    #[tokio::test]
    async fn trigger_rejects_synthetic_action() {
        let s = app_state().await;
        let name = crate::web::webhooks::WEBHOOK_MINT_ACTION.to_string();
        let err = trigger_action(State(s.clone()), AxumPath(name))
            .await
            .expect_err("synthetic trigger must error");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("event-driven"), "{}", err.message);
    }

    /// CLAUDE-61: the synthetic action cannot be deleted (no backing YAML).
    #[tokio::test]
    async fn delete_rejects_synthetic_action() {
        let s = app_state().await;
        let name = crate::web::webhooks::WEBHOOK_MINT_ACTION.to_string();
        let err = delete_action(State(s.clone()), AxumPath(name))
            .await
            .expect_err("synthetic delete must error");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
    }
}
