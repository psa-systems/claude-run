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
//! | POST | `/pause` | global pause |
//! | POST | `/resume` | global resume |

use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::state;

/// Shared state injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    /// One-shot trigger channel: dropping a name into here asks the scheduler
    /// to queue an out-of-cycle run for that action.
    pub trigger_tx: mpsc::Sender<String>,
    pub version: &'static str,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/status", get(get_status))
        .route("/actions", get(list_actions))
        .route("/actions/:name/trigger", post(trigger_action))
        .route("/actions/:name/pause", post(pause_action))
        .route("/actions/:name/resume", post(resume_action))
        .route("/pause", post(global_pause_handler))
        .route("/resume", post(global_resume_handler))
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

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl From<crate::error::Error> for AppError {
    fn from(e: crate::error::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
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
