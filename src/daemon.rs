//! Daemon: scans the actions dir, runs a 1s scheduler tick, dispatches due
//! actions to a Tokio task pool. Serves the HTTP API over a unix socket so
//! the CLI subcommands can talk to it.
//!
//! Process model: single daemon per machine, enforced by a pidfile lock.
//! `--detach` forks via the `daemonize` crate before binding the listener.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tokio::net::UnixListener;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::action::{self, LoadedAction};
use crate::api::{self, AppState};
use crate::claude::ClaudeInvoker;
use crate::config::{OnDiskConfig, Paths};
use crate::error::Result;
use crate::lock::ActionLock;
use crate::row::{self, RunnerDefaults};
use crate::scheduler::{self, ParsedSchedule};
use crate::snippet::SnippetRunner;
use crate::state::{self, ActionIndexRow};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const TICK_SECS: u64 = 5;

pub struct Daemon {
    pub paths: Paths,
    pub on_disk: OnDiskConfig,
}

impl Daemon {
    pub async fn run(self) -> Result<()> {
        std::fs::create_dir_all(&self.paths.runtime_dir)?;
        std::fs::create_dir_all(&self.paths.data_dir)?;

        // Hold the pidfile lock for the lifetime of this daemon. Second
        // daemon attempt on the same host fails fast.
        let pidfile = self.paths.runtime_dir.join("daemon.pid");
        let _pid_lock = pidfile_lock(&pidfile)?;

        let pool = crate::db::open(&self.paths.state_db()).await?;
        scan_actions(&pool, &self.paths.actions_dir()).await?;

        let (trigger_tx, trigger_rx) = mpsc::channel::<String>(64);

        // Bind the unix socket before any other work. CLI clients can connect
        // the moment the daemon writes the pidfile; failing the bind here
        // means the daemon never claims it is up.
        let sock_path = self.paths.runtime_dir.join("daemon.sock");
        if sock_path.exists() {
            std::fs::remove_file(&sock_path)?;
        }
        let listener = UnixListener::bind(&sock_path)?;
        info!(socket = %sock_path.display(), "daemon listening");

        let snippet = SnippetRunner::new(self.on_disk.daemon.nu_binary.as_deref());
        let claude = ClaudeInvoker::new(self.on_disk.daemon.claude_binary.as_deref());

        let state = AppState {
            pool: pool.clone(),
            trigger_tx: trigger_tx.clone(),
            version: VERSION,
        };
        let app = api::router(state);

        // Tasks: HTTP server on unix socket, scheduler tick, trigger consumer.
        let server = tokio::spawn(serve_unix(listener, app));

        let dispatch = Arc::new(Dispatcher {
            paths: self.paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(snippet),
            claude: Arc::new(claude),
            in_flight: Mutex::new(std::collections::HashSet::new()),
        });

        let scheduler_task = tokio::spawn(scheduler_loop(dispatch.clone(), trigger_tx.clone()));
        let trigger_task = tokio::spawn(trigger_consumer(dispatch.clone(), trigger_rx));

        // Graceful shutdown on SIGINT/SIGTERM.
        let shutdown = tokio::signal::ctrl_c();
        tokio::select! {
            res = server => {
                warn!(?res, "unix-socket server exited");
            }
            _ = shutdown => {
                info!("ctrl-c received, shutting down");
            }
        }
        scheduler_task.abort();
        trigger_task.abort();
        // pidfile lock released on drop
        let _ = std::fs::remove_file(&sock_path);
        Ok(())
    }
}

fn pidfile_lock(pidfile: &Path) -> Result<std::fs::File> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(pidfile)?;
    fs2::FileExt::try_lock_exclusive(&f).map_err(|e| {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::anyhow!(
                "another claude-run daemon already holds {}",
                pidfile.display()
            )
            .into()
        } else {
            crate::error::Error::Io(e)
        }
    })?;
    f.set_len(0)?;
    writeln!(f, "{}", std::process::id())?;
    Ok(f)
}

async fn serve_unix(listener: UnixListener, app: axum::Router) -> Result<()> {
    // axum::serve doesn't take a UnixListener directly; loop accept + handle.
    loop {
        let (stream, _addr) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper_util::service::TowerToHyperService::new(app);
            if let Err(e) =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, service)
                    .await
            {
                tracing::warn!(error = %e, "unix client connection error");
            }
        });
    }
}

pub async fn scan_actions(pool: &SqlitePool, actions_dir: &Path) -> Result<()> {
    if !actions_dir.is_dir() {
        info!(dir = %actions_dir.display(), "actions dir does not exist; nothing to index");
        return Ok(());
    }
    let now = state::now_iso();
    let read = match std::fs::read_dir(actions_dir) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "cannot read actions dir");
            return Ok(());
        }
    };
    for entry in read.flatten() {
        let path = entry.path();
        let ext = path.extension().and_then(|s| s.to_str());
        if !matches!(ext, Some("yml") | Some("yaml")) {
            continue;
        }
        match action::load_from_path(&path) {
            Ok(la) => {
                let row = ActionIndexRow {
                    name: la.action.name.clone(),
                    file_path: la.path.display().to_string(),
                    yaml_sha256: la.yaml_sha256.clone(),
                    enabled: la.action.enabled,
                    schedule_kind: la
                        .action
                        .schedule
                        .as_ref()
                        .map(|s| schedule_kind_str(&s.kind).to_string()),
                    schedule_spec: la.action.schedule.as_ref().and_then(|s| s.spec.clone()),
                    last_seen_at: now.clone(),
                    parse_error: None,
                };
                state::action_index_upsert(pool, &row).await?;
            }
            Err(e) => {
                // Index the broken file too so the operator can see it.
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("invalid");
                let row = ActionIndexRow {
                    name: stem.to_string(),
                    file_path: path.display().to_string(),
                    yaml_sha256: String::new(),
                    enabled: false,
                    schedule_kind: None,
                    schedule_spec: None,
                    last_seen_at: now.clone(),
                    parse_error: Some(e.to_string()),
                };
                state::action_index_upsert(pool, &row).await?;
            }
        }
    }
    Ok(())
}

fn schedule_kind_str(k: &crate::action::ScheduleKind) -> &'static str {
    match k {
        crate::action::ScheduleKind::Interval => "interval",
        crate::action::ScheduleKind::Cron => "cron",
        crate::action::ScheduleKind::Manual => "manual",
    }
}

struct Dispatcher {
    paths: Paths,
    pool: SqlitePool,
    snippet: Arc<SnippetRunner>,
    claude: Arc<ClaudeInvoker>,
    in_flight: Mutex<std::collections::HashSet<String>>,
}

impl Dispatcher {
    async fn dispatch(self: Arc<Self>, action_name: String, trigger_source: &'static str) {
        {
            let mut g = self.in_flight.lock().await;
            if !g.insert(action_name.clone()) {
                info!(action = %action_name, "already in flight, skipping");
                return;
            }
        }
        let me = self.clone();
        let name_for_log = action_name.clone();
        tokio::spawn(async move {
            if let Err(e) = me.execute(&action_name, trigger_source).await {
                warn!(action = %name_for_log, error = %e, "action execution errored");
            }
            me.in_flight.lock().await.remove(&name_for_log);
        });
    }

    async fn execute(&self, action_name: &str, trigger_source: &'static str) -> Result<()> {
        // Pause checks
        if state::global_pause_status(&self.pool).await?.is_some() {
            info!(action = %action_name, "globally paused, skipping");
            return Ok(());
        }
        if state::action_pause_status(&self.pool, action_name)
            .await?
            .is_some()
        {
            info!(action = %action_name, "action paused, skipping");
            return Ok(());
        }

        // Re-load YAML each run so on-disk edits take effect immediately.
        let actions_dir = self.paths.actions_dir();
        let loaded = match action::load(action_name, &actions_dir) {
            Ok(la) => la,
            Err(e) => {
                warn!(action = %action_name, error = %e, "action failed to load");
                return Ok(());
            }
        };

        // Cross-process per-action lock. Returns None if a CLI process or
        // another worker is already running this action.
        let lock_dir = self.paths.runtime_dir.join("locks");
        let _lock = match ActionLock::try_acquire(&lock_dir, action_name)? {
            Some(l) => l,
            None => {
                info!(action = %action_name, "lock held elsewhere, skipping");
                return Ok(());
            }
        };

        run_action_inner(
            &loaded,
            &self.paths,
            &self.pool,
            &self.snippet,
            &self.claude,
            trigger_source,
        )
        .await
    }
}

/// Shared by the daemon and the standalone CLI `run` subcommand.
pub async fn run_action_inner(
    loaded: &LoadedAction,
    paths: &Paths,
    pool: &SqlitePool,
    snippet: &SnippetRunner,
    claude: &ClaudeInvoker,
    triggered_by: &str,
) -> Result<()> {
    let defaults = RunnerDefaults::default();
    let limits = row::effective_limits(&defaults, loaded);

    let daily = state::daily_count_get(pool, &loaded.action.name).await?;
    if daily >= limits.max_runs_per_day {
        warn!(count = daily, "daily cap reached");
        return Ok(());
    }
    let cost = state::daily_cost_get(pool, &loaded.action.name).await?;
    if cost >= limits.daily_budget_usd {
        warn!(cost_usd = cost, "daily budget reached");
        return Ok(());
    }

    let rows = row::gather_rows(loaded, snippet, paths).await?;
    info!(action = %loaded.action.name, count = rows.len(), "input rows gathered");

    let run_id = state::run_begin(pool, &loaded.action.name, triggered_by).await?;
    let mut processed: u32 = 0;
    let ctx = row::RowContext {
        action: loaded,
        limits,
        paths,
        snippet,
        claude,
        pool,
        run_id,
    };

    for (idx, r) in rows.iter().enumerate() {
        let current = state::daily_count_get(pool, &loaded.action.name).await?;
        if current >= limits.max_runs_per_day {
            warn!(count = current, "daily cap reached mid-run, stopping");
            break;
        }
        let cost = state::daily_cost_get(pool, &loaded.action.name).await?;
        if cost >= limits.daily_budget_usd {
            warn!(cost_usd = cost, "daily budget reached mid-run, stopping");
            break;
        }
        let outcome = row::process_row(&ctx, r, idx).await?;
        if let Some(record) = row::done_record(&outcome) {
            row::append_output_record(paths, loaded, &record);
            if let row::ProcessedRow::Done {
                key,
                attempt,
                ok,
                claude_ran,
                claude_ok,
                claude_cost,
                attempt_counted,
                summary,
                stream_log_path,
            } = &outcome
            {
                state::row_outcome_insert(
                    pool,
                    &state::RowOutcome {
                        run_id,
                        action_name: &loaded.action.name,
                        row_key: key,
                        attempt: *attempt,
                        ok: *ok,
                        claude_ran: *claude_ran,
                        claude_ok: *claude_ok,
                        claude_cost_usd: *claude_cost,
                        attempt_counted: *attempt_counted,
                        summary: Some(summary.as_str()),
                        stream_log_path: stream_log_path.as_deref(),
                    },
                )
                .await?;
            }
        }
        processed += 1;
    }
    state::run_finish(pool, run_id, 0, processed).await?;
    info!(action = %loaded.action.name, processed, "run complete");
    Ok(())
}

async fn scheduler_loop(
    dispatch: Arc<Dispatcher>,
    _trigger_tx: mpsc::Sender<String>,
) -> Result<()> {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TICK_SECS));
    loop {
        ticker.tick().await;
        if let Err(e) = scheduler_tick(&dispatch).await {
            warn!(error = %e, "scheduler tick errored");
        }
    }
}

async fn scheduler_tick(dispatch: &Arc<Dispatcher>) -> Result<()> {
    // Refresh on-disk action index so newly-added files become schedulable
    // without a daemon restart.
    scan_actions(&dispatch.pool, &dispatch.paths.actions_dir()).await?;

    if state::global_pause_status(&dispatch.pool).await?.is_some() {
        return Ok(());
    }
    let now = Utc::now();
    let rows = state::action_index_list(&dispatch.pool).await?;
    for r in rows {
        if !r.enabled || r.parse_error.is_some() {
            continue;
        }
        if state::action_pause_status(&dispatch.pool, &r.name)
            .await?
            .is_some()
        {
            continue;
        }
        let Some(kind) = r.schedule_kind.as_deref() else {
            continue;
        };
        let schedule = match kind {
            "manual" => continue,
            "interval" => crate::action::Schedule {
                kind: crate::action::ScheduleKind::Interval,
                spec: r.schedule_spec.clone(),
            },
            "cron" => crate::action::Schedule {
                kind: crate::action::ScheduleKind::Cron,
                spec: r.schedule_spec.clone(),
            },
            _ => continue,
        };
        let parsed = match scheduler::parse_schedule(&schedule) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if matches!(parsed, ParsedSchedule::Manual) {
            continue;
        }
        let last_started: Option<DateTime<Utc>> =
            state::action_last_run_started(&dispatch.pool, &r.name)
                .await?
                .and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                });
        if scheduler::next_fire_due(&parsed, last_started, now).is_some() {
            info!(action = %r.name, "scheduler firing");
            dispatch.clone().dispatch(r.name.clone(), "scheduler").await;
        }
    }
    Ok(())
}

async fn trigger_consumer(dispatch: Arc<Dispatcher>, mut rx: mpsc::Receiver<String>) -> Result<()> {
    while let Some(name) = rx.recv().await {
        info!(action = %name, "trigger received");
        dispatch.clone().dispatch(name, "trigger").await;
    }
    Ok(())
}

/// Helpers for the CLI side to discover the daemon socket.
pub fn daemon_socket_path(paths: &Paths) -> PathBuf {
    paths.runtime_dir.join("daemon.sock")
}

pub fn daemon_pidfile_path(paths: &Paths) -> PathBuf {
    paths.runtime_dir.join("daemon.pid")
}
