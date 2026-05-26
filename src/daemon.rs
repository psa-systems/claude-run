//! Daemon: scans the actions dir, runs a 5s scheduler tick, dispatches due
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
use crate::cancel::{CancellationToken, RegistryGuard, RunRegistry};
use crate::claude::ClaudeInvoker;
use crate::config::{OnDiskConfig, Paths};
use crate::error::Result;
use crate::lock::ActionLock;
use crate::row::{self, RunnerDefaults};
use crate::scheduler::{self, ParsedSchedule};
use crate::snippet::SnippetRunner;
use crate::state::{self, ActionIndexRow};

const VERSION: &str = crate::VERSION;
const TICK_SECS: u64 = 5;
/// CLAUDE-11: how long shutdown waits for cancelled in-flight runs to finalize
/// before force-finalizing their DB rows and exiting. Bounds a wedged claude
/// child so it cannot hang the daemon forever. Assumed 30s; revise after
/// testing per the issue.
const SHUTDOWN_GRACE_SECS: u64 = 30;

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

        // CLAUDE-10: a freshly started daemon cannot have any run still
        // executing, so any run with no end state is orphaned from a dead
        // process. Reconcile it to cancelled before scheduling begins.
        let cancelled = state::cancel_orphaned_runs(&pool).await?;
        if cancelled > 0 {
            info!(count = cancelled, "cancelled orphaned runs on startup");
        }

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

        // CLAUDE-9: one registry shared between the dispatcher (which registers
        // a token per run) and the API state (whose /runs/:id/cancel handler
        // trips it).
        let runs = Arc::new(RunRegistry::new());

        let api_state = Arc::new(AppState {
            pool: pool.clone(),
            trigger_tx: trigger_tx.clone(),
            version: VERSION,
            runs: runs.clone(),
        });

        // Unix-socket router: bare paths (`/status`, `/actions`, ...). CLI
        // continues to call these paths; no /api prefix to avoid breaking
        // existing client code.
        let unix_app = api::router((*api_state).clone());

        // TCP router: web UI at root, JSON API under `/api/`. Same handlers
        // as the unix-socket flavor, just nested + sibling-mounted.
        let api_for_tcp = api::router((*api_state).clone());
        let web_router = crate::web::router(api_state.clone(), self.paths.clone());
        let tcp_app = axum::Router::new()
            .merge(web_router)
            .nest("/api", api_for_tcp);

        // Tasks: unix-socket server, TCP server, scheduler tick, trigger consumer.
        let server_unix = tokio::spawn(serve_unix(listener, unix_app));

        let tcp_bind = format!("{}:{}", self.on_disk.web.bind, self.on_disk.web.port);
        let tcp_listener = match tokio::net::TcpListener::bind(&tcp_bind).await {
            Ok(l) => {
                info!(address = %tcp_bind, "web UI listening");
                Some(l)
            }
            Err(e) => {
                // Don't fail the whole daemon just because the TCP port is
                // taken - the unix-socket API still works.
                warn!(address = %tcp_bind, error = %e, "TCP bind failed; web UI disabled this run");
                None
            }
        };
        let server_tcp = tcp_listener.map(|l| tokio::spawn(serve_tcp(l, tcp_app)));

        let dispatch = Arc::new(Dispatcher {
            paths: self.paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(snippet),
            claude: Arc::new(claude),
            in_flight: Mutex::new(std::collections::HashSet::new()),
            runs,
        });

        let scheduler_task = tokio::spawn(scheduler_loop(dispatch.clone(), trigger_tx.clone()));
        let trigger_task = tokio::spawn(trigger_consumer(dispatch.clone(), trigger_rx));

        // Graceful shutdown on SIGINT (Ctrl-C), SIGTERM (service managers,
        // `claude-run stop`), or SIGQUIT. All fall through to the same cleanup
        // below.
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigquit = signal(SignalKind::quit())?;
        tokio::select! {
            res = server_unix => {
                warn!(?res, "unix-socket server exited");
            }
            _ = sigint.recv() => {
                info!("SIGINT received, shutting down");
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down");
            }
            _ = sigquit.recv() => {
                info!("SIGQUIT received, shutting down");
            }
        }

        // Stop scheduling new work first so nothing dispatches while we drain
        // the runs that are already in flight.
        scheduler_task.abort();
        trigger_task.abort();

        // CLAUDE-11: cancel in-flight runs through the CLAUDE-9 path (child
        // terminated, `post` run with a synthesized cancelled result,
        // `run_finish` with exit::CANCELLED) instead of abandoning them. Wait
        // for them to finalize, bounded by a grace period so a wedged child
        // cannot hang the daemon forever.
        let cancelled = dispatch.runs.cancel_all();
        if !cancelled.is_empty() {
            info!(
                count = cancelled.len(),
                "cancelling in-flight runs for shutdown"
            );
            let grace = std::time::Duration::from_secs(SHUTDOWN_GRACE_SECS);
            if tokio::time::timeout(grace, wait_for_runs_drain(&dispatch.runs))
                .await
                .is_err()
            {
                warn!(
                    grace_secs = SHUTDOWN_GRACE_SECS,
                    "shutdown grace elapsed; force-finalizing remaining runs"
                );
            }
            // Sweep any run row left without an end state (e.g. its task was
            // still mid-`post` when the grace elapsed). Reuses the CLAUDE-10
            // reconcile, here at shutdown rather than startup, so no orphaned
            // "running" row survives this shutdown regardless of timing. The
            // cancelled child is already SIGKILLed by the per-run escalation
            // the token tripped above.
            match state::cancel_orphaned_runs(&pool).await {
                Ok(n) if n > 0 => info!(count = n, "force-finalized runs after grace"),
                Ok(_) => {}
                Err(e) => warn!(error = %e, "force-finalize sweep failed"),
            }
        }

        if let Some(t) = server_tcp {
            t.abort();
        }
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

async fn serve_tcp(listener: tokio::net::TcpListener, app: axum::Router) -> Result<()> {
    axum::serve(listener, app).await?;
    Ok(())
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
                    link_template: la.action.link_template.clone(),
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
                    link_template: None,
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
    /// CLAUDE-9: per-run cancellation tokens, registered by `run_action_inner`.
    runs: Arc<RunRegistry>,
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
            Some(&self.runs),
        )
        .await
    }
}

/// Shared by the daemon and the standalone CLI `run` subcommand.
///
/// CLAUDE-9: `registry` is `Some` only on the daemon path, where a run can be
/// cancelled through the socket API. The standalone `claude-run run` path
/// passes `None` (nothing can reach it to cancel) and gets an inert token.
pub async fn run_action_inner(
    loaded: &LoadedAction,
    paths: &Paths,
    pool: &SqlitePool,
    snippet: &SnippetRunner,
    claude: &ClaudeInvoker,
    triggered_by: &str,
    registry: Option<&RunRegistry>,
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

    // CLAUDE-9: register a cancellation token for this run and ensure it is
    // unregistered on every exit path (including the `?` early returns below).
    let cancel = match registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard { registry, run_id };

    // CLAUDE-1: record the planned row keys so the web UI shows what's
    // queued / in-flight before any outcome record lands.
    let planned_keys: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| row::row_key(loaded, r, i))
        .collect();
    if let Err(e) = state::run_set_row_keys(pool, run_id, &planned_keys).await {
        warn!(error = %e, "run_set_row_keys failed (non-fatal)");
    }

    let mut processed: u32 = 0;
    let ctx = row::RowContext {
        action: loaded,
        limits,
        paths,
        snippet,
        claude,
        pool,
        run_id,
        cancel: cancel.clone(),
    };

    for (idx, r) in rows.iter().enumerate() {
        if cancel.is_cancelled() {
            info!(action = %loaded.action.name, "run cancelled, stopping before next row");
            break;
        }
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
        // CLAUDE-14: mark this row in flight so the web UI shows exactly one
        // "running" row. Set after the cap/budget checks (a capped run never
        // reaches this row) and before process_row; `run_finish` clears it on
        // every finish/break path below.
        if let Err(e) = state::run_set_current_row(pool, run_id, Some(&planned_keys[idx])).await {
            warn!(error = %e, "run_set_current_row failed (non-fatal)");
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
                duration_ms,
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
                        duration_ms: *duration_ms,
                    },
                )
                .await?;
            }
        }
        processed += 1;
    }
    let exit_code = if cancel.is_cancelled() {
        crate::exit::CANCELLED
    } else {
        crate::exit::OK
    };
    state::run_finish(pool, run_id, exit_code, processed).await?;
    info!(action = %loaded.action.name, processed, exit_code, "run complete");
    Ok(())
}

/// CLAUDE-11: resolve once every in-flight run has unregistered, i.e. all the
/// cancellations issued at shutdown have torn down and finalized. Polls the
/// registry rather than holding task handles, since runs dispatch on detached
/// tasks. The caller wraps this in a `timeout` to bound the wait.
async fn wait_for_runs_drain(runs: &RunRegistry) {
    while !runs.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
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

/// Read the PID recorded in the daemon pidfile, if it exists and parses.
pub fn read_pidfile(paths: &Paths) -> Option<i32> {
    let contents = std::fs::read_to_string(daemon_pidfile_path(paths)).ok()?;
    contents.trim().parse::<i32>().ok()
}

/// True if a process with this PID currently exists, via a `kill(pid, 0)`
/// probe. `EPERM` (exists but not signalable by us) still counts as alive;
/// only `ESRCH` means gone.
pub fn process_alive(pid: i32) -> bool {
    let rc = unsafe { libc::kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Send SIGTERM to a PID. The daemon handles it gracefully (see `Daemon::run`).
pub fn send_sigterm(pid: i32) -> std::io::Result<()> {
    if unsafe { libc::kill(pid, libc::SIGTERM) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Paths;

    fn paths_with_runtime(dir: &std::path::Path) -> Paths {
        Paths {
            config_dir: dir.to_path_buf(),
            data_dir: dir.to_path_buf(),
            state_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
        }
    }

    #[test]
    fn current_process_is_alive() {
        assert!(process_alive(std::process::id() as i32));
    }

    #[test]
    fn absurd_pid_is_not_alive() {
        // A PID near the 32-bit max is never allocated on these hosts.
        assert!(!process_alive(0x7fff_fff0));
    }

    #[test]
    fn read_pidfile_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        assert_eq!(read_pidfile(&paths), None);
        std::fs::write(daemon_pidfile_path(&paths), "12345\n").unwrap();
        assert_eq!(read_pidfile(&paths), Some(12345));
        std::fs::write(daemon_pidfile_path(&paths), "not-a-pid").unwrap();
        assert_eq!(read_pidfile(&paths), None);
    }
}
