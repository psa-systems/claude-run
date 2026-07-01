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
use crate::concurrency::{Executor, KeyGate};
use crate::config::{OnDiskConfig, Paths};
use crate::error::Result;
use crate::job::{self, RunnerDefaults};
use crate::lock::ActionLock;
use crate::scheduler::{self, ParsedSchedule};
use crate::snippet::SnippetRunner;
use crate::state::{self, ActionIndexRow};

const VERSION: &str = crate::VERSION;
const TICK_SECS: u64 = 5;
/// CLAUDE-106: how often the workdir GC sweeper runs after the startup sweep.
/// Hourly is far finer than the day-scale retention windows it enforces while
/// staying cheap (one stat per workdir, a git check only for past-window dirs).
const GC_TICK_SECS: u64 = 3600;
/// CLAUDE-11: how long shutdown waits for cancelled in-flight runs to finalize
/// before force-finalizing their DB rows and exiting. Bounds a wedged claude
/// child so it cannot hang the daemon forever. Assumed 30s; revise after
/// testing per the issue.
const SHUTDOWN_GRACE_SECS: u64 = 30;
/// CLAUDE-57: fixed retry interval for a web-UI bind that fails with
/// `AddrNotAvailable` (the configured IP is not assigned yet, e.g. a boot race).
/// Retried indefinitely; the unix-socket API stays up throughout.
const WEB_BIND_RETRY_SECS: u64 = 5;
/// Log a throttled progress line every Nth `AddrNotAvailable` retry (~once per
/// minute at the 5s interval) so a long boot race does not spam the journal.
const WEB_BIND_RETRY_LOG_EVERY: u64 = 12;

pub struct Daemon {
    pub paths: Paths,
    pub on_disk: OnDiskConfig,
    /// CLAUDE-101: live tracing log-filter reload hook, wired from the CLI's
    /// reload handle. `None` when `CLAUDE_RUN_LOG` is set (env wins) or no handle
    /// is available, in which case a saved filter persists but is not applied.
    pub log_apply: Option<crate::settings::LogFilterApply>,
}

impl Daemon {
    pub async fn run(mut self) -> Result<()> {
        std::fs::create_dir_all(&self.paths.runtime_dir)?;
        std::fs::create_dir_all(&self.paths.data_dir)?;

        // Hold the pidfile lock for the lifetime of this daemon. Second
        // daemon attempt on the same host fails fast.
        let pidfile = self.paths.runtime_dir.join("daemon.pid");
        let _pid_lock = pidfile_lock(&pidfile)?;

        let pool = crate::db::open(&self.paths.state_db()).await?;

        // CLAUDE-153: migration 0013 renamed the webhook action's persisted DB
        // rows; the on-disk stream-log directory lives outside SQLite, so rename
        // it here (best-effort) so past webhook run logs stay under the new name.
        rename_webhook_log_dir(&self.paths);

        // CLAUDE-101: import any runtime config section still living in
        // config.toml into the DB (once), or refuse to start if a section is
        // present in BOTH the file and the DB. Then resolve the effective
        // runtime config (DB wins, file is the bootstrap fallback) and hold it
        // behind shared RwLocks so Settings-page saves apply live.
        crate::settings::migrate_config_file(&pool, &self.paths).await?;
        let effective = crate::settings::effective(&pool, &self.on_disk).await?;
        let webhook_enabled = !effective.webhook.sources.is_empty();
        let replay_window_secs = effective.webhook.replay_window_secs;
        let settings = Arc::new(crate::settings::LiveSettings::new(
            pool.clone(),
            effective,
            self.log_apply.take(),
        ));

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
        // `Arc` so the same invoker backs both the dispatcher and the
        // webhook triage classifier (CLAUDE-47).
        let claude = Arc::new(ClaudeInvoker::new(
            self.on_disk.daemon.claude_binary.as_deref(),
        ));

        // CLAUDE-9: one registry shared between the dispatcher (which registers
        // a token per run) and the API state (whose /runs/:id/cancel handler
        // trips it).
        let runs = Arc::new(RunRegistry::new());

        // CLAUDE-40: one in-flight set shared between the dispatcher (which
        // tracks dispatched runs) and the API (whose delete handler refuses a
        // delete while a run is dispatched but not yet recorded in `runs`).
        let in_flight: api::InFlight = Arc::new(Mutex::new(std::collections::HashSet::new()));

        let api_state = Arc::new(AppState {
            pool: pool.clone(),
            trigger_tx: trigger_tx.clone(),
            version: VERSION,
            runs: runs.clone(),
            in_flight: in_flight.clone(),
            settings: settings.clone(),
        });

        // Unix-socket router: bare paths (`/status`, `/actions`, ...). CLI
        // continues to call these paths; no /api prefix to avoid breaking
        // existing client code.
        let unix_app = api::router((*api_state).clone());

        // TCP router: web UI at root, JSON API under `/api/`. Same handlers
        // as the unix-socket flavor, just nested + sibling-mounted.
        let api_for_tcp = api::router((*api_state).clone());
        // CLAUDE-61: the web layer injects the synthetic `webhook-forgejo`
        // action only when the Forgejo webhook is configured. CLAUDE-87: that
        // is now any configured source (an empty `sources` map disables it).
        // CLAUDE-101: `webhook_enabled` reflects the effective (DB-resolved)
        // config at start.
        let web_router = crate::web::router(api_state.clone(), self.paths.clone(), webhook_enabled);

        // CLAUDE-43: Forgejo webhook listener. Verified `action_run_failure`
        // events are emitted onto a channel drained by `consume_events`, which
        // (CLAUDE-44/45/46/47) fetches the log, triages it, and mints/declines a
        // YouTrack issue.
        let (webhook_tx, webhook_rx) =
            mpsc::channel::<crate::web::webhooks::VerifiedFailureEvent>(64);
        // CLAUDE-101: the listener routes against the live config + limiter map
        // shared with LiveSettings, so a Settings save is routable immediately.
        let webhook_state = crate::web::webhooks::WebhookState::from_shared(
            pool.clone(),
            settings.webhook_handle(),
            settings.webhook_rate_handle(),
            replay_window_secs,
            webhook_tx,
        );
        let webhook_router = crate::web::webhooks::router(webhook_state);

        let tcp_app = axum::Router::new()
            .merge(web_router)
            .merge(webhook_router)
            .nest("/api", api_for_tcp);

        // CLAUDE-48 (Phase 5): gate the TCP surface (web UI + /api/*) behind a
        // bearer token whenever the bind is non-loopback. The /webhooks/ prefix
        // keeps its own HMAC and is exempt. When auth is required but the token
        // cannot be resolved, fail closed: drop the TCP listener entirely rather
        // than expose an unauthenticated surface. The unix-socket API is
        // unaffected (loopback-only by construction).
        let tcp_app = match crate::auth::resolve_gate(&self.on_disk.web, &pool).await {
            Ok(None) => Some(tcp_app),
            Ok(Some(gate)) => {
                info!("token auth enabled for the TCP web UI and /api/*");
                Some(crate::auth::protect(tcp_app, gate))
            }
            Err(e) => {
                warn!(error = %e, "token auth required but unavailable; TCP web UI disabled this run");
                None
            }
        };

        // Tasks: unix-socket server, TCP server, scheduler tick, trigger consumer.
        let server_unix = tokio::spawn(serve_unix(listener, unix_app));

        // CLAUDE-57: bind + serve the web UI in its own task. The bind retries
        // on the boot-race `AddrNotAvailable` (configured IP not assigned yet)
        // instead of disabling the web UI for the whole run, and never blocks
        // the scheduler or the unix-socket API while it waits. Other bind error
        // kinds keep the fail-once-and-disable behavior. The config path travels
        // along so every bind log line correlates to its source.
        let server_tcp = tcp_app.map(|tcp_app| {
            let tcp_bind = format!("{}:{}", self.on_disk.web.bind, self.on_disk.web.port);
            let config_path = self.paths.config_file();
            tokio::spawn(bind_and_serve_tcp(tcp_bind, config_path, tcp_app))
        });

        // CLAUDE-43: drain verified webhook failure events for the lifetime of
        // the daemon. CLAUDE-44: each event triggers an internal-route log fetch
        // with retry + secret redaction; the redacted artifacts land under
        // `state_dir/webhook-logs/`. CLAUDE-47: each event is then triaged
        // (CLAUDE-45) and minted / declined (CLAUDE-46) when a provider backend
        // is usable.
        let webhook_log_dir = self.paths.state_dir.join("webhook-logs");
        let webhook_transport: Arc<dyn crate::forge_log::LogTransport> =
            Arc::new(crate::forge_log::HttpLogTransport::new());
        let mint_ctx = build_mint_context(&pool, settings.mint_handle(), &claude, &webhook_log_dir);
        let webhook_consumer = tokio::spawn(crate::web::webhooks::consume_events(
            webhook_rx,
            pool.clone(),
            webhook_transport,
            webhook_log_dir,
            mint_ctx,
            // CLAUDE-96: the consumer resolves each event source's
            // `log_token_ref` for private-repo log fetches. CLAUDE-101: the live
            // handle so a Settings save is picked up by the next event.
            settings.webhook_handle(),
        ));

        let dispatch = Arc::new(Dispatcher {
            paths: self.paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(snippet),
            claude: claude.clone(),
            in_flight,
            // CLAUDE-28: one daemon-wide key gate, shared across every dispatch,
            // so two executions sharing a concurrency key never overlap even
            // across a scheduled run and a manual trigger.
            keygate: KeyGate::new(),
            runs,
        });

        let scheduler_task = tokio::spawn(scheduler_loop(dispatch.clone(), trigger_tx.clone()));
        let trigger_task = tokio::spawn(trigger_consumer(dispatch.clone(), trigger_rx));

        // CLAUDE-106: sweep preserved / aside workdirs older than the configured
        // retention window. The first tick fires immediately, so a sweep runs on
        // serve start; subsequent ticks fire hourly. Reads the live GC config off
        // the shared handle so a Settings save applies on the next sweep.
        let gc_task = tokio::spawn(gc_loop(self.paths.clone(), settings.workdir_gc_handle()));

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
        gc_task.abort();

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
        webhook_consumer.abort();
        // pidfile lock released on drop
        let _ = std::fs::remove_file(&sock_path);
        Ok(())
    }
}

/// Build the webhook [`crate::web::webhooks::MintContext`] (CLAUDE-47):
/// resolve a provider backend (CLI-first), wrap the shared `claude` invoker in a
/// triage [`crate::triage::ClaudeClassifier`], and bundle the on-disk mint
/// config + caps. Returns `None` (mint disabled, the consumer runs log-only)
/// when no `yt`/`fj` backend is usable or the triage workdir cannot be created,
/// so a daemon on a host without the forge CLIs still ingests and logs failures.
fn build_mint_context(
    pool: &SqlitePool,
    mint_cfg: Arc<tokio::sync::RwLock<crate::config::MintConfig>>,
    claude: &Arc<ClaudeInvoker>,
    webhook_log_dir: &Path,
) -> Option<crate::web::webhooks::MintContext> {
    use crate::action::ClaudeBlock;
    use crate::triage::{ClaudeClassifier, FailureClassifier};

    let backend: Arc<dyn crate::provider::Backend> =
        match crate::provider::resolve(crate::provider::DEFAULT_PRIORITY) {
            Ok(b) => Arc::from(b),
            Err(e) => {
                warn!(error = %e, "no usable provider backend; webhook runs log-only (no mint)");
                return None;
            }
        };

    // Classification is a single bounded-log JSON reply: no repo checkout, no
    // edit tools. Run claude read-only in a stable cwd under the webhook log dir.
    let cwd = webhook_log_dir.join("triage");
    if let Err(e) = std::fs::create_dir_all(&cwd) {
        warn!(error = %e, "creating triage workdir failed; webhook runs log-only (no mint)");
        return None;
    }
    let claude_cfg = ClaudeBlock {
        cwd: None,
        allowed_tools: Some(String::new()),
        permission_mode: Some("default".to_string()),
    };
    let stream_log = cwd.join("triage-stream.jsonl");
    let classifier: Arc<dyn FailureClassifier> = Arc::new(ClaudeClassifier::new(
        claude.clone(),
        claude_cfg,
        std::time::Duration::from_secs(crate::web::webhooks::TRIAGE_TIMEOUT_SECS),
        cwd,
        stream_log,
    ));

    info!(backend = backend.name(), "webhook mint enabled");
    Some(crate::web::webhooks::MintContext {
        pool: pool.clone(),
        classifier,
        backend,
        mint_cfg,
        defaults: RunnerDefaults::default(),
        route_cache: Arc::new(crate::mint::VcsRouteCache::new(
            crate::mint::DEFAULT_VCS_MAP_TTL,
        )),
    })
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

/// CLAUDE-153: rename the webhook action's on-disk stream-log directory from the
/// old `logs/webhook-fast-path/` to `logs/webhook-forgejo/` at serve start, so
/// past webhook run logs stay reachable under the renamed action (the DB rows
/// are migrated by migration `0013`; the log dir lives outside SQLite).
///
/// Best-effort and idempotent: it acts only when the old path exists and the new
/// one does not, so a second start (new dir already present) is a no-op, and it
/// never clobbers a directory. Any rename failure is logged and swallowed rather
/// than blocking startup: the logs are diagnostic history, not operational state.
fn rename_webhook_log_dir(paths: &Paths) {
    let logs = paths.logs_dir();
    let old = logs.join("webhook-fast-path");
    let new = logs.join("webhook-forgejo");
    if old.exists() && !new.exists() {
        match std::fs::rename(&old, &new) {
            Ok(()) => info!(
                from = %old.display(),
                to = %new.display(),
                "renamed webhook log directory to the webhook-forgejo name (CLAUDE-153)"
            ),
            Err(e) => warn!(
                from = %old.display(),
                to = %new.display(),
                error = %e,
                "renaming webhook log directory failed; old run logs stay under the former name"
            ),
        }
    }
}

/// CLAUDE-57: classified web-UI bind failure. Carries a stable `kind` tag, a
/// one-line operator hint, and whether the daemon should retry. Only the
/// boot-race `AddrNotAvailable` retries; every other kind disables the web UI
/// for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindFailure {
    pub kind: &'static str,
    pub hint: &'static str,
    pub retry: bool,
}

/// Map a bind [`std::io::ErrorKind`] to its classified [`BindFailure`]. Pure (no
/// logging, no I/O) so the classification is unit-testable without a socket.
pub fn classify_bind_error(kind: std::io::ErrorKind) -> BindFailure {
    use std::io::ErrorKind;
    match kind {
        ErrorKind::AddrNotAvailable => BindFailure {
            kind: "AddrNotAvailable",
            hint: "address is not assigned to any interface; the interface may not be up yet (boot race) or [web].bind is wrong",
            retry: true,
        },
        ErrorKind::AddrInUse => BindFailure {
            kind: "AddrInUse",
            hint: "another process is listening on this address:port",
            retry: false,
        },
        ErrorKind::PermissionDenied => BindFailure {
            kind: "PermissionDenied",
            hint: "binding this address/port requires elevated privileges",
            retry: false,
        },
        _ => BindFailure {
            kind: "other",
            hint: "see the underlying OS error for the cause",
            retry: false,
        },
    }
}

/// CLAUDE-57: bind `tcp_bind`, retrying indefinitely on the boot-race
/// `AddrNotAvailable` and giving up (web UI disabled this run) on every other
/// classified kind. Generic over the bind closure so the retry loop is testable
/// with an injected result sequence. Returns the bound listener, or `None` when
/// a non-retryable error disabled the web UI. Every outcome logs the full
/// attempted `address`, the OS error, and the classified hint so the operator
/// can correlate against the configured bind.
async fn retry_bind<F, Fut>(
    tcp_bind: &str,
    config_path: &Path,
    retry_interval: std::time::Duration,
    mut bind: F,
) -> Option<tokio::net::TcpListener>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::io::Result<tokio::net::TcpListener>>,
{
    let mut attempt: u64 = 0;
    loop {
        match bind().await {
            Ok(listener) => return Some(listener),
            Err(e) => {
                let failure = classify_bind_error(e.kind());
                if !failure.retry {
                    warn!(
                        address = %tcp_bind,
                        config = %config_path.display(),
                        error = %e,
                        kind = failure.kind,
                        hint = failure.hint,
                        "TCP bind failed; web UI disabled this run"
                    );
                    return None;
                }
                if attempt == 0 {
                    warn!(
                        address = %tcp_bind,
                        config = %config_path.display(),
                        error = %e,
                        kind = failure.kind,
                        hint = failure.hint,
                        retry_secs = WEB_BIND_RETRY_SECS,
                        "TCP bind failed; retrying until the address is available"
                    );
                } else if attempt.is_multiple_of(WEB_BIND_RETRY_LOG_EVERY) {
                    info!(
                        address = %tcp_bind,
                        config = %config_path.display(),
                        attempts = attempt,
                        "TCP bind still failing (address not available); retrying"
                    );
                }
                attempt += 1;
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

/// CLAUDE-57: bind the web UI (with boot-race retry) and then serve it. Logs the
/// effective bind before attempting it so a successful default bind is traceable
/// to its source, then `web UI listening` once the bind succeeds. Returns early
/// (web UI disabled this run) when a non-retryable bind error was classified.
async fn bind_and_serve_tcp(tcp_bind: String, config_path: PathBuf, app: axum::Router) {
    info!(address = %tcp_bind, config = %config_path.display(), "binding web UI");
    let Some(listener) = retry_bind(
        &tcp_bind,
        &config_path,
        std::time::Duration::from_secs(WEB_BIND_RETRY_SECS),
        || tokio::net::TcpListener::bind(&tcp_bind),
    )
    .await
    else {
        return;
    };
    info!(address = %tcp_bind, "web UI listening");
    if let Err(e) = serve_tcp(listener, app).await {
        warn!(address = %tcp_bind, error = %e, "web UI server exited with error");
    }
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
                // CLAUDE-61: `webhook-forgejo` is a reserved synthetic action
                // name the web layer injects for the event-driven webhook runs.
                // An on-disk YAML claiming it would be editable / schedulable and
                // would collide with the synthetic entry, so refuse to index it.
                if la.action.name == crate::web::webhooks::WEBHOOK_MINT_ACTION {
                    warn!(
                        path = %path.display(),
                        name = %la.action.name,
                        "refusing to index action: `{}` is a reserved synthetic name",
                        crate::web::webhooks::WEBHOOK_MINT_ACTION
                    );
                    continue;
                }
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
                // CLAUDE-61: same reservation as the parse-ok branch - never let a
                // file (even an unparseable one) occupy the synthetic name's slot.
                if stem == crate::web::webhooks::WEBHOOK_MINT_ACTION {
                    warn!(
                        path = %path.display(),
                        "refusing to index file: `{}` is a reserved synthetic name",
                        crate::web::webhooks::WEBHOOK_MINT_ACTION
                    );
                    continue;
                }
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
    // CLAUDE-40: self-heal the index. Prune rows whose backing YAML no longer
    // exists on disk, so an out-of-band `rm` (or a delete whose row removal
    // ever diverged from the file removal) cannot strand a stale dashboard row.
    // Pruning removes only the index row; retained `runs`/`job_outcomes` history
    // is left intact.
    for row in state::action_index_list(pool).await? {
        if !Path::new(&row.file_path).exists() {
            state::action_index_delete(pool, &row.name).await?;
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
    in_flight: api::InFlight,
    /// CLAUDE-28: daemon-wide per-key gate, threaded into every run so jobs
    /// sharing a concurrency key serialize across runs and triggers.
    keygate: KeyGate,
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
            self.snippet.clone(),
            self.claude.clone(),
            trigger_source,
            Some(&self.runs),
            &self.keygate,
        )
        .await
    }
}

/// Shared by the daemon and the standalone CLI `run` subcommand.
///
/// CLAUDE-9: `registry` is `Some` only on the daemon path, where a run can be
/// cancelled through the socket API. The standalone `claude-run run` path
/// passes `None` (nothing can reach it to cancel) and gets an inert token.
#[allow(clippy::too_many_arguments)]
pub async fn run_action_inner(
    loaded: &LoadedAction,
    paths: &Paths,
    pool: &SqlitePool,
    snippet: Arc<SnippetRunner>,
    claude: Arc<ClaudeInvoker>,
    triggered_by: &str,
    registry: Option<&RunRegistry>,
    keygate: &KeyGate,
) -> Result<()> {
    // CLAUDE-30: native kinds route to a built-in Rust handler. Scripted
    // actions fall through to the generic nu-snippet pipeline below, unchanged.
    if loaded.action.kind.is_native() {
        return crate::native::dispatch(crate::native::NativeContext {
            action: loaded,
            paths,
            pool,
            snippet,
            claude,
            triggered_by,
            registry,
            keygate: keygate.clone(),
        })
        .await;
    }

    let defaults = RunnerDefaults::default();
    let limits = job::effective_limits(&defaults, loaded);

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

    let jobs = job::gather_jobs(loaded, snippet.as_ref(), paths).await?;
    info!(action = %loaded.action.name, count = jobs.len(), "input jobs gathered");

    let run_id = state::run_begin(pool, &loaded.action.name, triggered_by).await?;

    // CLAUDE-9: register a cancellation token for this run and ensure it is
    // unregistered on every exit path (including the `?` early returns below).
    let cancel = match registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard { registry, run_id };

    // CLAUDE-1: record the planned job keys so the web UI shows what's
    // queued / in-flight before any outcome record lands.
    let planned_keys: Vec<String> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| job::job_key(loaded, j, i))
        .collect();
    if let Err(e) = state::run_set_job_keys(pool, run_id, &planned_keys).await {
        warn!(error = %e, "run_set_job_keys failed (non-fatal)");
    }

    // CLAUDE-28: replace the strictly-sequential per-job loop with a keyed,
    // bounded-parallel executor. Each job becomes one task that acquires its
    // concurrency-key gate (same key serializes) and a parallelism permit
    // (`max_parallel` distinct keys at once). `max_parallel: 1` reproduces the
    // original sequential behavior. Shared `loaded`/`paths`/`pool` are cloned
    // into each task so the spawned futures own their data.
    let max_parallel = loaded.action.concurrency.max_parallel.max(1) as usize;
    let loaded_arc = Arc::new(loaded.clone());
    let paths_owned = paths.clone();
    let pool_owned = pool.clone();

    let mut items: Vec<(String, crate::concurrency::JobFuture)> = Vec::with_capacity(jobs.len());
    for (idx, job) in jobs.iter().enumerate() {
        let gate_key = job::concurrency_key(loaded, job, idx);
        let job_key = planned_keys[idx].clone();
        let job = job.clone();
        let loaded = loaded_arc.clone();
        let paths = paths_owned.clone();
        let pool = pool_owned.clone();
        let snippet = snippet.clone();
        let claude = claude.clone();
        let job_cancel = cancel.clone();
        let fut: crate::concurrency::JobFuture = Box::pin(async move {
            // CLAUDE-28: best-effort cap/budget check before launch. Up to
            // `max_parallel` tasks can pass this before any of them increments
            // the ledger, so the overshoot is bounded by `max_parallel - 1`.
            if state::daily_count_get(&pool, &loaded.action.name).await? >= limits.max_runs_per_day
            {
                warn!(action = %loaded.action.name, "daily cap reached, skipping job");
                return Ok(false);
            }
            if state::daily_cost_get(&pool, &loaded.action.name).await? >= limits.daily_budget_usd {
                warn!(action = %loaded.action.name, "daily budget reached, skipping job");
                return Ok(false);
            }
            // CLAUDE-28: mark this job in flight (cleared below and by
            // `run_finish`) so the web UI can badge every running job.
            if let Err(e) = state::run_inflight_add(&pool, run_id, &job_key).await {
                warn!(error = %e, "run_inflight_add failed (non-fatal)");
            }
            let ctx = job::JobContext {
                action: loaded.as_ref(),
                limits,
                paths: &paths,
                snippet: snippet.as_ref(),
                claude: claude.as_ref(),
                pool: &pool,
                run_id,
                cancel: job_cancel,
            };
            let outcome = job::process_job(&ctx, &job, idx).await;
            if let Err(e) = state::run_inflight_remove(&pool, run_id, &job_key).await {
                warn!(error = %e, "run_inflight_remove failed (non-fatal)");
            }
            let outcome = outcome?;
            if let Some(record) = job::done_record(&outcome) {
                job::append_output_record(&paths, loaded.as_ref(), &record);
                if let job::ProcessedJob::Done {
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
                    timed_out,
                } = &outcome
                {
                    state::job_outcome_insert(
                        &pool,
                        &state::JobOutcome {
                            run_id,
                            action_name: &loaded.action.name,
                            job_key: key,
                            attempt: *attempt,
                            ok: *ok,
                            claude_ran: *claude_ran,
                            claude_ok: *claude_ok,
                            claude_cost_usd: *claude_cost,
                            attempt_counted: *attempt_counted,
                            summary: Some(summary.as_str()),
                            stream_log_path: stream_log_path.as_deref(),
                            duration_ms: *duration_ms,
                            timed_out: *timed_out,
                        },
                    )
                    .await?;
                }
            }
            Ok::<bool, crate::error::Error>(true)
        });
        items.push((gate_key, fut));
    }

    let exec = Executor {
        max_parallel,
        gate: keygate.clone(),
        cancel: cancel.clone(),
    };
    let results = exec.run(items).await;
    let mut processed: u32 = 0;
    let mut first_err: Option<crate::error::Error> = None;
    for r in results {
        match r {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    let exit_code = if cancel.is_cancelled() {
        crate::exit::CANCELLED
    } else {
        crate::exit::OK
    };
    state::run_finish(pool, run_id, exit_code, processed).await?;
    if let Some(e) = first_err {
        warn!(action = %loaded.action.name, error = %e, "one or more jobs errored during the run");
    }
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

/// CLAUDE-106: periodic workdir GC. The `tokio::time::interval` fires its first
/// tick immediately, so this both performs the serve-start sweep and the hourly
/// sweeps thereafter. The sweep is blocking filesystem work (a recursive walk
/// plus `remove_dir_all`), so it runs on a blocking thread; the live config is
/// snapshotted per tick off the shared handle.
async fn gc_loop(paths: Paths, cfg: Arc<tokio::sync::RwLock<crate::config::WorkdirGcConfig>>) {
    let base = paths.workdirs_dir();
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(GC_TICK_SECS));
    loop {
        ticker.tick().await;
        let snapshot = cfg.read().await.clone();
        if !snapshot.enabled {
            continue;
        }
        let base = base.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::gc::sweep(
                &base,
                &snapshot,
                std::time::SystemTime::now(),
                crate::native::youtrack::has_unpushed_commits,
            )
        })
        .await;
        match result {
            Ok(stats) if stats.deleted > 0 => info!(
                deleted = stats.deleted,
                kept = stats.kept,
                bytes_reclaimed = stats.bytes_reclaimed,
                "workdir GC sweep complete"
            ),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "workdir GC sweep task failed"),
        }
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
        // CLAUDE-156: consult the in-flight set before treating the action as
        // due. Without this, the window between `dispatch` and `run_begin`
        // writing the fresh `runs.started_at` row (after the gather phase on the
        // native path) leaves `action_last_run_started` reporting a stale/None
        // timestamp, so `next_fire_due` returns "due" on every 5s tick and the
        // log fills with a "scheduler firing" storm, each call short-circuited
        // by the same in-flight guard inside `dispatch`. Skipping here suppresses
        // both the redundant log and the wasted dispatch call. The lock is taken
        // and released before `dispatch`, so there is no lock-ordering hazard.
        let in_flight = dispatch.in_flight.lock().await.contains(&r.name);
        if scheduler::should_fire(&parsed, last_started, now, in_flight) {
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

    // CLAUDE-57: each bind error kind maps to a stable tag, a hint, and the
    // right retry decision (only AddrNotAvailable retries).
    #[test]
    fn classify_bind_error_maps_each_kind() {
        use std::io::ErrorKind;
        let a = classify_bind_error(ErrorKind::AddrNotAvailable);
        assert_eq!(a.kind, "AddrNotAvailable");
        assert!(a.retry, "the boot-race kind must retry");
        let b = classify_bind_error(ErrorKind::AddrInUse);
        assert_eq!(b.kind, "AddrInUse");
        assert!(!b.retry);
        let c = classify_bind_error(ErrorKind::PermissionDenied);
        assert_eq!(c.kind, "PermissionDenied");
        assert!(!c.retry);
        let d = classify_bind_error(ErrorKind::Unsupported);
        assert_eq!(d.kind, "other");
        assert!(!d.retry);
    }

    // CLAUDE-57: a real bind to an unassigned TEST-NET-1 address (RFC 5737,
    // never on a local NIC) surfaces AddrNotAvailable, the boot-race case the
    // retry loop is built for.
    #[tokio::test]
    async fn bind_unassigned_address_classifies_addr_not_available() {
        let err = tokio::net::TcpListener::bind("192.0.2.1:7321")
            .await
            .expect_err("binding an unassigned address must fail");
        let f = classify_bind_error(err.kind());
        assert_eq!(f.kind, "AddrNotAvailable", "got OS kind {:?}", err.kind());
        assert!(f.retry);
    }

    // CLAUDE-57: a second bind on a port already held surfaces AddrInUse, which
    // is non-retryable (web UI disabled this run).
    #[tokio::test]
    async fn double_bind_classifies_addr_in_use() {
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = first.local_addr().unwrap();
        let err = tokio::net::TcpListener::bind(addr)
            .await
            .expect_err("second bind on the same port must fail");
        let f = classify_bind_error(err.kind());
        assert_eq!(f.kind, "AddrInUse", "got OS kind {:?}", err.kind());
        assert!(!f.retry);
    }

    // CLAUDE-57: the retry loop tolerates an AddrNotAvailable, sleeps the (here
    // near-zero) interval, retries, and returns the listener once the bind
    // succeeds.
    #[tokio::test]
    async fn retry_bind_recovers_after_addr_not_available() {
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let bind = || {
            let n = calls.get();
            calls.set(n + 1);
            async move {
                if n == 0 {
                    Err(std::io::Error::from(std::io::ErrorKind::AddrNotAvailable))
                } else {
                    tokio::net::TcpListener::bind("127.0.0.1:0").await
                }
            }
        };
        let path = std::path::PathBuf::from("/tmp/claude-run/config.toml");
        let listener = retry_bind(
            "127.0.0.1:0",
            &path,
            std::time::Duration::from_millis(1),
            bind,
        )
        .await;
        assert!(
            listener.is_some(),
            "listener should come up after the retry"
        );
        assert_eq!(calls.get(), 2, "expected one failure then one success");
    }

    // CLAUDE-57: a non-retryable kind short-circuits the loop to None (web UI
    // disabled this run) on the first attempt, without sleeping.
    #[tokio::test]
    async fn retry_bind_gives_up_on_non_retryable_kind() {
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let bind = || {
            calls.set(calls.get() + 1);
            async move { Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)) }
        };
        let path = std::path::PathBuf::from("/tmp/claude-run/config.toml");
        let listener = retry_bind(
            "127.0.0.1:1",
            &path,
            std::time::Duration::from_millis(1),
            bind,
        )
        .await;
        assert!(listener.is_none(), "non-retryable kind disables the web UI");
        assert_eq!(calls.get(), 1, "must not retry a non-retryable kind");
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

    // CLAUDE-30/CLAUDE-32: a native action routes to the native-handler seam
    // (`crate::native::dispatch`) instead of the scripted gather + per-job loop.
    // With `max_runs_per_day: 0` the youtrack handler trips its own daily cap
    // and returns before touching the provider backend, so this exercises the
    // native branch deterministically (no `yt`/`fj`, no network). A scripted
    // action would instead fail at gather on the missing `input.command`.
    #[tokio::test]
    async fn run_action_inner_dispatches_native_to_youtrack_handler() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open(&paths.state_db()).await.unwrap();
        let snippet = SnippetRunner::new(None);
        let claude = ClaudeInvoker::new(None);

        let raw = "name: yt\nkind: youtrack\nnative: {}\nlimits:\n  max_runs_per_day: 0\n";
        let loaded = crate::action::validate_str(raw, "<test>").unwrap();

        run_action_inner(
            &loaded,
            &paths,
            &pool,
            Arc::new(snippet),
            Arc::new(claude),
            "test",
            None,
            &KeyGate::new(),
        )
        .await
        .expect("native handler caps out cleanly");
    }

    // CLAUDE-40: an out-of-band `rm` of an action file must be reconciled by the
    // next scan, which prunes the stale index row while leaving run history.
    #[tokio::test]
    async fn scan_actions_prunes_vanished_files() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open(&paths.state_db()).await.unwrap();
        let actions_dir = tmp.path().join("actions");
        std::fs::create_dir_all(&actions_dir).unwrap();
        let file = actions_dir.join("doomed.yml");
        std::fs::write(
            &file,
            "name: doomed\ninput:\n  command: print '[]'\nprompt: print 'hi'\nenabled: false\n",
        )
        .unwrap();

        // First scan indexes the file.
        scan_actions(&pool, &actions_dir).await.unwrap();
        assert!(state::action_index_list(&pool)
            .await
            .unwrap()
            .iter()
            .any(|r| r.name == "doomed"));

        // Seed a finished run so we can assert pruning keeps history.
        let run = state::run_begin(&pool, "doomed", "cli").await.unwrap();
        state::run_finish(&pool, run, crate::exit::OK, 0)
            .await
            .unwrap();

        // Out-of-band removal; the next scan must prune the stale index row.
        std::fs::remove_file(&file).unwrap();
        scan_actions(&pool, &actions_dir).await.unwrap();
        assert!(
            !state::action_index_list(&pool)
                .await
                .unwrap()
                .iter()
                .any(|r| r.name == "doomed"),
            "vanished file's index row is pruned"
        );

        // History survives the prune.
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE action_name = ?")
            .bind("doomed")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(runs, 1, "run history is retained across a prune");
    }

    // CLAUDE-61: an on-disk YAML claiming the reserved synthetic name
    // `webhook-forgejo` must not be indexed (it would collide with the
    // synthetic action the web layer injects, and become editable/schedulable).
    #[tokio::test]
    async fn scan_actions_refuses_reserved_synthetic_name() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open(&paths.state_db()).await.unwrap();
        let actions_dir = tmp.path().join("actions");
        std::fs::create_dir_all(&actions_dir).unwrap();
        // A perfectly valid action that simply claims the reserved name.
        let file = actions_dir.join("webhook-forgejo.yml");
        std::fs::write(
            &file,
            "name: webhook-forgejo\ninput:\n  command: print '[]'\nprompt: print 'hi'\nenabled: false\n",
        )
        .unwrap();

        scan_actions(&pool, &actions_dir).await.unwrap();
        assert!(
            !state::action_index_list(&pool)
                .await
                .unwrap()
                .iter()
                .any(|r| r.name == crate::web::webhooks::WEBHOOK_MINT_ACTION),
            "a YAML using the reserved synthetic name must not be indexed"
        );
    }

    // CLAUDE-153: the webhook log dir is renamed to the new action name at start.
    #[test]
    fn rename_webhook_log_dir_moves_old_dir_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let old = paths.logs_dir().join("webhook-fast-path");
        let new = paths.logs_dir().join("webhook-forgejo");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::write(old.join("marker.jsonl"), b"{}").unwrap();

        rename_webhook_log_dir(&paths);
        assert!(!old.exists(), "old dir is gone after the rename");
        assert!(
            new.join("marker.jsonl").exists(),
            "logs moved to the new dir"
        );

        // Second start: new dir already present, old absent -> no-op, no panic.
        rename_webhook_log_dir(&paths);
        assert!(new.join("marker.jsonl").exists());
    }

    // CLAUDE-153: an existing new dir is never clobbered by a stray old dir.
    #[test]
    fn rename_webhook_log_dir_does_not_clobber_existing_new_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let old = paths.logs_dir().join("webhook-fast-path");
        let new = paths.logs_dir().join("webhook-forgejo");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::create_dir_all(&new).unwrap();
        std::fs::write(new.join("keep.jsonl"), b"new").unwrap();

        rename_webhook_log_dir(&paths);
        // Both remain: the guard skips the rename when the new dir exists.
        assert!(old.exists(), "old dir left intact when new already exists");
        assert!(new.join("keep.jsonl").exists(), "new dir untouched");
    }
}
