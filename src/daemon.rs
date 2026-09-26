//! Daemon: scans the actions dir, runs a 5s scheduler tick, dispatches due
//! actions to a Tokio task pool. Serves the HTTP API over a unix socket so
//! the CLI subcommands can talk to it.
//!
//! Process model: single daemon per machine, enforced by a pidfile lock.
//! `--detach` forks via the `daemonize` crate before binding the listener.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::collections::HashMap;

use crate::db::DbPool;
use chrono::{DateTime, Utc};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{info, warn};

use crate::action::{self, LoadedAction};
use crate::api::{self, AppState};
use crate::cancel::{CancellationToken, RegistryGuard, RunRegistry};
use crate::claude::ClaudeInvoker;
use crate::concurrency::{Executor, GatherGate, KeyGate, Limiter};
use crate::config::{ConcurrencyConfig, OnDiskConfig, Paths};
use crate::error::Result;
use crate::job::{self, RunnerDefaults};
use crate::scheduler;
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
    /// Whether to bind the unix-socket control API at
    /// `$CLAUDE_RUN_RUNTIME_DIR/daemon.sock`. It exists for the CLI on a host
    /// install; `serve --no-socket` turns it off for deployments (the container)
    /// where the web UI is the whole interface and an unauthenticated second
    /// copy of the control plane is a liability rather than a convenience.
    pub unix_socket: bool,
    /// CLAUDE-101: live tracing log-filter reload hook, wired from the CLI's
    /// reload handle. `None` when `CLAUDE_RUN_LOG` is set (env wins) or no handle
    /// is available, in which case a saved filter persists but is not applied.
    pub log_apply: Option<crate::settings::LogFilterApply>,
}

impl Daemon {
    pub async fn run(mut self) -> Result<()> {
        let unix_socket = self.unix_socket;
        std::fs::create_dir_all(&self.paths.runtime_dir)?;
        std::fs::create_dir_all(&self.paths.data_dir)?;

        // Hold the pidfile lock for the lifetime of this daemon. Second
        // daemon attempt on the same host fails fast.
        let pidfile = self.paths.runtime_dir.join("daemon.pid");
        let _pid_lock = pidfile_lock(&pidfile)?;

        let target = crate::config::resolve_database(&self.paths, &self.on_disk.database)?;
        let pool = crate::db::open(&target).await?;

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
        let replay_window_secs = effective.webhook.replay_window_secs;
        let settings = Arc::new(crate::settings::LiveSettings::new(
            pool.clone(),
            effective,
            self.log_apply.take(),
        ));

        // CLAUDE-10 / CLAUDE-228: a freshly started daemon holds the
        // single-instance lock, so any run with no end state is orphaned from a
        // process that died without finalizing. Reap it before scheduling
        // begins, as `UNEXPECTED` rather than cancelled (nothing cancelled it),
        // and say so at `warn`: a non-zero count means the last daemon died
        // dirty or a row leaked, which the web UI was rendering as a live run.
        let reaped = state::reap_orphaned_runs(&pool).await?;
        if reaped > 0 {
            warn!(
                count = reaped,
                "reaped orphaned run rows on startup; closed as unexpected"
            );
        }

        // CLAUDE-223: the global pause is DB-persisted, so it survives a restart
        // and the daemon can come up already paused. Say so at `info`, otherwise
        // a daemon that schedules nothing looks broken rather than paused. The
        // dashboard banner and the About Activity card are the visual reminder.
        if let Some((paused_at, reason)) = state::global_pause_status(&pool).await? {
            info!(
                paused_at = %paused_at,
                reason = reason.as_deref().unwrap_or("(none)"),
                "starting up GLOBALLY PAUSED; no action will be dispatched until Resume"
            );
        }

        // CLAUDE-171: stamp this serve's start. The action-detail input-error
        // summary and the error-log page's default window are bounded to it, so
        // each serve gets a fresh diagnostics window instead of replaying every
        // event ever recorded.
        let started_at = crate::settings::record_serve_start(&pool).await?;
        info!(started_at = %started_at, "recorded serve start");

        scan_actions(&pool, &self.paths.actions_dir()).await?;

        let (trigger_tx, trigger_rx) = mpsc::channel::<String>(64);

        // Bind the unix socket before any other work. CLI clients can connect
        // the moment the daemon writes the pidfile; failing the bind here
        // means the daemon never claims it is up. A stale socket is removed
        // either way, so `--no-socket` cannot leave a dead path a CLI would
        // still try to connect to.
        let sock_path = self.paths.runtime_dir.join("daemon.sock");
        if sock_path.exists() {
            std::fs::remove_file(&sock_path)?;
        }
        let listener = if unix_socket {
            let listener = UnixListener::bind(&sock_path)?;
            info!(socket = %sock_path.display(), "daemon listening");
            Some(listener)
        } else {
            info!(
                "unix-socket API disabled (serve --no-socket); the TCP web UI and its /api routes \
                 are the only control plane this run"
            );
            None
        };

        let snippet = SnippetRunner::new(self.on_disk.daemon.nu_binary.as_deref());
        // CLAUDE-165: seed the agent's dedicated CLAUDE_CONFIG_DIR (slim bot
        // CLAUDE.md + the host login when there is one) before spawning any
        // agent, so the child never loads the operator's global
        // `~/.claude/CLAUDE.md`. CLAUDE-247: a missing host login is a warn, not
        // an abort, so the container comes up for the operator to log into.
        let agent_config_dir = crate::agent_home::resolve_config_dir(
            &self.paths,
            self.on_disk.daemon.claude_config_dir.as_deref(),
        );
        crate::agent_home::ensure(&agent_config_dir)?;
        info!(dir = %agent_config_dir.display(), "seeded claude agent config dir");
        // Materialize the daemon's own service logins (Settings -> Auth) before
        // anything polls: the `yt` single-file config, the `fj` keys.json, and
        // the Anthropic API key handed to the invoker below. `YOUTRACK_CLI_CONFIG`
        // is exported first and unconditionally, so a credential saved later in
        // the web UI needs no restart to reach the next `yt` child.
        // The runner owns its age key: generate one when the store is empty, move
        // an older one out of the data dir, and refuse to invent a replacement
        // when ciphertext already exists (see `secrets::ensure_key`).
        match crate::secrets::ensure_key(&pool).await {
            Ok(path) => info!(path = %path.display(), "secrets key ready"),
            // Not fatal: the daemon still serves, and every credential path
            // reports the same error with its own context. Failing to boot would
            // take the web UI down, which is where the fix is applied.
            Err(e) => {
                warn!(error = %e, "secrets key unavailable; stored credentials cannot be read")
            }
        }
        crate::service_auth::export_yt_config_env(&self.paths);
        crate::service_auth::export_gh_config_env(&self.paths);
        let gh_binary = "gh";
        let auth_cfg = settings.auth_config().await;
        let anthropic_key: crate::service_auth::ApiKeyHandle = {
            let (status, key) = crate::service_auth::materialize(
                &pool,
                &self.paths,
                &auth_cfg,
                &crate::service_auth::default_fj_data_home(),
                gh_binary,
            )
            .await?;
            info!(
                anthropic_key = status.anthropic_key,
                youtrack = status.youtrack_base_url.as_deref().unwrap_or("<unset>"),
                forgejo = status.forgejo_host.as_deref().unwrap_or("<unset>"),
                github = status.github_host.as_deref().unwrap_or("<unset>"),
                "service auth materialized"
            );
            Arc::new(std::sync::RwLock::new(key))
        };

        // Broker for the web-UI-driven `claude auth login` (Settings -> Auth).
        let claude_login = Arc::new(crate::claude_login::LoginBroker::new());

        // The scheduler asks this before every pickup and every dispatch.
        let readiness = crate::readiness::ReadinessGate::new(
            agent_config_dir.clone(),
            Some(anthropic_key.clone()),
        );
        if !readiness.check().ready {
            warn!("{}", crate::readiness::NOT_READY_REASON);
        }

        // `Arc` so the same invoker backs both the dispatcher and the
        // webhook triage classifier (CLAUDE-47).
        // CLAUDE-239: the invoker also takes the live `config.claude` handle, so
        // an action with no `claude.model` inherits the global default and a
        // Settings save applies to the next invocation with no restart.
        // CLAUDE-269: the invoker trips `readiness` on a structured
        // `authentication_failed`, so every caller sharing this `Arc` (the
        // dispatcher, the CI-triage classifier, the comment-webhook route)
        // detects an expired/revoked login from its own invocations.
        let claude = Arc::new(
            ClaudeInvoker::new(self.on_disk.daemon.claude_binary.as_deref())
                .with_config_dir(Some(agent_config_dir.clone()))
                .with_default_claude(settings.claude_handle())
                .with_api_key(Some(anthropic_key.clone()))
                .with_readiness(readiness.clone()),
        );

        // CLAUDE-9: one registry shared between the dispatcher (which registers
        // a token per run) and the API state (whose /runs/:id/cancel handler
        // trips it).
        let runs = Arc::new(RunRegistry::new());

        // CLAUDE-40: one in-flight set shared between the dispatcher (which
        // tracks dispatched runs) and the API (whose delete handler refuses a
        // delete while a run is dispatched but not yet recorded in `runs`).
        // CLAUDE-162: a per-action refcount (not a set), since overlapping runs
        // mean several runs for one action can be dispatched-but-not-recorded at
        // once; the delete handler treats a positive count as "in flight".
        let in_flight: api::InFlight = Arc::new(Mutex::new(HashMap::new()));

        let api_state = Arc::new(AppState {
            pool: pool.clone(),
            trigger_tx: trigger_tx.clone(),
            version: VERSION,
            runs: runs.clone(),
            in_flight: in_flight.clone(),
            anthropic_key: Some(anthropic_key.clone()),
            readiness: Some(readiness.clone()),
            claude_login: Some(claude_login.clone()),
            fj_data_home: crate::service_auth::default_fj_data_home(),
            claude_binary: self
                .on_disk
                .daemon
                .claude_binary
                .clone()
                .unwrap_or_else(|| "claude".to_string()),
            gh_binary: gh_binary.to_string(),
            agent_config_dir: Some(agent_config_dir.clone()),
            settings: settings.clone(),
            paths: self.paths.clone(),
        });

        // Unix-socket router: bare paths (`/status`, `/actions`, ...). CLI
        // continues to call these paths; no /api prefix to avoid breaking
        // existing client code. Built only when the socket is enabled; the TCP
        // flavor below is the same router, so nothing is lost without it.
        let unix_app = unix_socket.then(|| api::router((*api_state).clone()));

        // TCP router: web UI at root, JSON API under `/api/`. Same handlers
        // as the unix-socket flavor, just nested + sibling-mounted.
        let api_for_tcp = api::router((*api_state).clone());
        let web_router = crate::web::router(api_state.clone(), self.paths.clone());

        // CLAUDE-43: Forgejo webhook listener. Verified `action_run_failure`
        // events are emitted onto a channel drained by `consume_events`, which
        // (CLAUDE-44/45/46/47) fetches the log, triages it, and mints/declines a
        // YouTrack issue.
        let (webhook_tx, webhook_rx) =
            mpsc::channel::<crate::web::webhooks::VerifiedFailureEvent>(64);
        // CLAUDE-277: verified `pull_request` deliveries are emitted onto their
        // own channel, drained by `consume_pr_events`, which closes (or leaves
        // open) the issue a merged fix PR references.
        let (pr_tx, pr_rx) = mpsc::channel::<crate::web::webhooks::VerifiedPullRequestEvent>(64);
        // CLAUDE-278: verified `success`/`recover` Action-run deliveries are
        // emitted onto their own channel, drained by
        // `consume_action_success_events`, which merges a recorded agent PR
        // whose action has automerge enabled once its checks go green.
        let (success_tx, success_rx) =
            mpsc::channel::<crate::web::webhooks::VerifiedActionSuccessEvent>(64);
        // CLAUDE-101: the listener routes against the live config + limiter map
        // shared with LiveSettings, so a Settings save is routable immediately.
        let webhook_state = crate::web::webhooks::WebhookState::from_shared(
            pool.clone(),
            settings.webhook_handle(),
            settings.webhook_rate_handle(),
            replay_window_secs,
            webhook_tx,
        )
        .with_readiness(readiness.clone())
        .with_pr_events(pr_tx)
        .with_success_events(success_tx);
        // CLAUDE-220: the `/claude` comment listener is a SECOND endpoint with
        // its own config section, write-scoped token, and Pause switch. It shares
        // the CI-failure listener's replay-store INSTANCE (so one delivery id
        // fanned out to both endpoints is still caught) and reuses the mint
        // config's loop-safety bot logins.
        let (comment_tx, comment_rx) =
            mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(64);
        let comment_state = crate::web::comment_webhooks::CommentWebhookState::from_shared(
            pool.clone(),
            settings.webhook_comment_handle(),
            settings.mint_handle(),
            settings.webhook_comment_rate_handle(),
            webhook_state.seen_handle(),
            comment_tx,
        );
        let webhook_router = crate::web::webhooks::router(webhook_state);
        let comment_router = crate::web::comment_webhooks::router(comment_state);

        let tcp_app = axum::Router::new()
            .merge(web_router)
            .merge(webhook_router)
            .merge(comment_router)
            .nest("/api", api_for_tcp);

        // CLAUDE-48 (Phase 5): gate the TCP surface (web UI + /api/*) whenever
        // the bind is non-loopback. The /webhooks/ prefix keeps its own HMAC and
        // is exempt. An instance with no credential at all serves only its
        // first-run setup page (see `auth::resolve_gate`), so the surface is
        // never unauthenticated but the operator can still reach it to create
        // an administrator; a missing `auth_token_ref` secret no longer takes
        // the whole web UI down. The unix-socket API, when enabled, is
        // unaffected (loopback-only by construction).
        let tcp_app = match crate::auth::resolve_gate(&self.on_disk.web, &pool).await {
            Ok(None) => Some(tcp_app),
            Ok(Some(gate)) => {
                info!("auth enabled for the TCP web UI and /api/*");
                Some(crate::auth::protect(tcp_app, gate))
            }
            Err(e) => {
                warn!(error = %e, "could not read the web credentials; TCP web UI disabled this run");
                None
            }
        };

        // Tasks: unix-socket server, TCP server, scheduler tick, trigger consumer.
        let server_unix = match (listener, unix_app) {
            (Some(listener), Some(app)) => Some(tokio::spawn(serve_unix(listener, app))),
            _ => None,
        };

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
        // the daemon. CLAUDE-44/204: each event triggers an `fj actions logs`
        // fetch with retry + secret redaction; the redacted artifacts land under
        // `state_dir/webhook-logs/`. CLAUDE-47: each event is then triaged
        // (CLAUDE-45) and minted / declined (CLAUDE-46) when a provider backend
        // is usable.
        // CLAUDE-28: one daemon-wide key gate, shared across every dispatch, so
        // two executions sharing a concurrency key never overlap even across a
        // scheduled run and a manual trigger. CLAUDE-220: the comment consumer
        // takes the same gate on its per-(repo, PR) key. CLAUDE-274: the
        // webhook event consumer takes it too, on a (repo, workflow, job) key,
        // so two concurrent failures of the same job never both run an
        // analysis.
        let keygate = KeyGate::new();

        let webhook_log_dir = self.paths.webhook_logs_dir();
        let webhook_logs: Arc<dyn crate::forge_log::ActionLogClient> = Arc::new(
            crate::provider::FjForgeReads::new(Arc::new(crate::provider::ProcessRunner::new())),
        );
        // CLAUDE-274: `analysis_timeout_secs` is read once here: the
        // classifier is built once and shared for the daemon's lifetime, so a
        // later Settings save to it needs a restart (unlike the rest of
        // `[webhook]`, which the consumer re-reads live per event).
        let analysis_timeout_secs = settings.webhook_handle().read().await.analysis_timeout_secs;
        let mint_ctx = build_mint_context(
            &pool,
            settings.mint_handle(),
            analysis_timeout_secs,
            &claude,
            &webhook_log_dir,
            &self.paths,
        );
        // CLAUDE-277/278: the pull-request and success-event consumers share
        // the same MintContext (backend, mint config, commit-runs client) as
        // the failure consumer; built once, cloned for each extra task.
        let pr_consumer = tokio::spawn(crate::web::webhooks::consume_pr_events(
            pr_rx,
            pool.clone(),
            mint_ctx.clone(),
            settings.webhook_handle(),
            keygate.clone(),
        ));
        let success_consumer = tokio::spawn(crate::web::webhooks::consume_action_success_events(
            success_rx,
            pool.clone(),
            mint_ctx.clone(),
            keygate.clone(),
        ));
        // CLAUDE-328: the reconciler backstop for a missed/dropped/too-early
        // success webhook. Independent of the consumers above: it ticks on its
        // own schedule and walks every recorded open agent PR through the same
        // decision function `success_consumer` calls per event.
        let automerge_reconciler_task = tokio::spawn(automerge_reconciler_loop(
            pool.clone(),
            mint_ctx.clone(),
            keygate.clone(),
        ));
        let webhook_consumer = tokio::spawn(crate::web::webhooks::consume_events(
            webhook_rx,
            pool.clone(),
            webhook_logs,
            webhook_log_dir,
            mint_ctx,
            // CLAUDE-96: the consumer resolves each event source's
            // `log_token_ref` to authorize the log fetch. CLAUDE-101: the live
            // handle so a Settings save is picked up by the next event.
            settings.webhook_handle(),
            keygate.clone(),
        ));

        // CLAUDE-220: serve accepted `/claude` comment commands off the request
        // path for the daemon's lifetime. Each command authorizes against a live
        // permission read, then takes its route: a PR comment clones / runs /
        // pushes, a plain-issue comment mints a Queued YouTrack issue
        // (CLAUDE-221). The shared key gate keeps at most one job per thread.
        let comment_ctx = Arc::new(crate::web::comment_webhooks::CommentContext {
            pool: pool.clone(),
            paths: self.paths.clone(),
            claude: claude.clone(),
            config: settings.webhook_comment_handle(),
            mint_cfg: settings.mint_handle(),
            route_cache: Arc::new(crate::mint::VcsRouteCache::new(
                crate::mint::DEFAULT_VCS_MAP_TTL,
            )),
            defaults: crate::job::RunnerDefaults::default(),
            keygate: keygate.clone(),
            denials: Arc::new(crate::web::comment_webhooks::DenialReplyLimiter::new()),
            readiness: Some(readiness.clone()),
        });
        let comment_consumer = tokio::spawn(crate::web::comment_webhooks::consume_commands(
            comment_rx,
            comment_ctx,
        ));

        // CLAUDE-162: one daemon-wide concurrency limiter (global + per-action
        // semaphores), shared across every run so caps hold across overlapping
        // runs. The initial global cap is resolved from the concurrency setting
        // and the indexed actions (auto = the max enabled-action max_parallel);
        // `scheduler_tick` recomputes it every tick so a Settings save applies.
        let concurrency = settings.concurrency_handle();
        let initial_global = {
            let cfg = concurrency.read().await.clone();
            let actions = state::action_index_list(&pool).await.unwrap_or_default();
            resolve_global_cap(&cfg, &actions)
        };
        let limiter = Limiter::new(initial_global);

        let dispatch = Arc::new(Dispatcher {
            paths: self.paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(snippet),
            claude: claude.clone(),
            in_flight,
            keygate,
            gather: GatherGate::new(),
            runs,
            // CLAUDE-162: daemon-wide caps + continuous-pickup state.
            limiter,
            concurrency,
            // CLAUDE-269: attached so every `RunPause` this gate mints also
            // stops a run when the Claude login needs renewing.
            pause: crate::pause::PauseGate::new(pool.clone()).with_readiness(readiness.clone()),
            readiness: readiness.clone(),
            last_fire: Mutex::new(HashMap::new()),
        });

        let scheduler_task = tokio::spawn(scheduler_loop(dispatch.clone(), trigger_tx.clone()));
        let trigger_task = tokio::spawn(trigger_consumer(dispatch.clone(), trigger_rx));

        // CLAUDE-106: sweep preserved / aside workdirs older than the configured
        // retention window. The first tick fires immediately, so a sweep runs on
        // serve start; subsequent ticks fire hourly. Reads the live GC config off
        // the shared handle so a Settings save applies on the next sweep.
        let gc_task = tokio::spawn(gc_loop(self.paths.clone(), settings.workdir_gc_handle()));

        // CLAUDE-175: Phase 6 retention sweep. Prunes the unbounded history
        // tables (job_outcomes; run_events joins via the same helper under
        // CLAUDE-172) past the configured window on the same hourly cadence as
        // the workdir GC. The first tick fires immediately, so a prune runs on
        // serve start; reads the live config off the shared handle so a Settings
        // save applies on the next sweep.
        let retention_task =
            tokio::spawn(retention_loop(pool.clone(), settings.db_retention_handle()));

        // CLAUDE-245: sweep per-attempt stream logs older than the configured
        // window. Nothing ever deleted them, and the full log partition that
        // caused the truncations diagnosed in CLAUDE-233 is what this bounds.
        // Same cadence and live-config shape as the two sweeps above.
        let stream_log_gc_task = tokio::spawn(stream_log_gc_loop(
            self.paths.clone(),
            pool.clone(),
            settings.stream_log_gc_handle(),
        ));

        // CLAUDE-269: the status probe. Detects an expired/revoked login even
        // between invocations (a quiet action, or an invalid API key, which
        // never trips the invocation-side detection). Skipped entirely on the
        // `always()` gate (no daemon holds it) and while unready for the
        // no-credential reason (nothing to probe yet).
        let login_probe_task = tokio::spawn(login_probe_loop(
            readiness.clone(),
            self.on_disk
                .daemon
                .claude_binary
                .clone()
                .unwrap_or_else(|| "claude".to_string()),
            agent_config_dir.clone(),
            anthropic_key.clone(),
        ));

        // Graceful shutdown on SIGINT (Ctrl-C), SIGTERM (service managers,
        // `claude-run stop`), or SIGQUIT. All fall through to the same cleanup
        // below.
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigquit = signal(SignalKind::quit())?;
        // With `--no-socket` there is no server task to watch, so the arm waits
        // forever and shutdown comes from a signal exactly as it does when the
        // socket server stays healthy.
        let unix_exit = async move {
            match server_unix {
                Some(handle) => handle.await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(unix_exit);
        tokio::select! {
            res = &mut unix_exit => {
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
        retention_task.abort();
        stream_log_gc_task.abort();
        login_probe_task.abort();

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
            // still mid-`post` when the grace elapsed), so no orphaned "running"
            // row survives this shutdown regardless of timing. `CANCELLED`, not
            // the startup reap's `UNEXPECTED`: this shutdown is what stopped
            // them, and the cancelled child is already SIGKILLed by the per-run
            // escalation the token tripped above.
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
        pr_consumer.abort();
        success_consumer.abort();
        automerge_reconciler_task.abort();
        comment_consumer.abort();
        // pidfile lock released on drop
        if unix_socket {
            let _ = std::fs::remove_file(&sock_path);
        }
        Ok(())
    }
}

/// Build the webhook [`crate::web::webhooks::MintContext`] (CLAUDE-47):
/// resolve a provider backend (CLI-first), wrap the shared `claude` invoker in
/// an at-ingest [`crate::triage::AnalysisClassifier`] (CLAUDE-274), and bundle
/// the on-disk mint config + caps. Returns `None` (mint disabled, the consumer
/// runs log-only) when no `yt`/`fj` backend is usable or the analysis base
/// directory cannot be created, so a daemon on a host without the forge CLIs
/// still ingests and logs failures.
fn build_mint_context(
    pool: &DbPool,
    mint_cfg: Arc<tokio::sync::RwLock<crate::config::MintConfig>>,
    analysis_timeout_secs: u64,
    claude: &Arc<ClaudeInvoker>,
    webhook_log_dir: &Path,
    paths: &Paths,
) -> Option<crate::web::webhooks::MintContext> {
    use crate::action::ClaudeBlock;
    use crate::triage::{AnalysisClassifier, FailureClassifier};

    let backend: Arc<dyn crate::provider::Backend> =
        match crate::provider::resolve(crate::provider::DEFAULT_PRIORITY) {
            Ok(b) => Arc::from(b),
            Err(e) => {
                warn!(error = %e, "no usable provider backend; webhook runs log-only (no mint)");
                return None;
            }
        };

    // CLAUDE-274: each analysis gets its own `<owner>_<repo>-run<N>/`
    // subdirectory under here, holding the full log and the failing workflow
    // YAML; removed when the classification returns.
    let analysis_base_dir = webhook_log_dir.join("analysis");
    if let Err(e) = std::fs::create_dir_all(&analysis_base_dir) {
        warn!(error = %e, "creating the analysis base directory failed; webhook runs log-only (no mint)");
        return None;
    }
    let claude_cfg = ClaudeBlock {
        cwd: None,
        allowed_tools: Some("Read,Grep,Bash".to_string()),
        permission_mode: Some("default".to_string()),
        // CLAUDE-239: the analysis pins no model, so it follows the global
        // `config.claude.model` (or the CLI default when that is unset).
        model: None,
        // CLAUDE-259: same for the effort level.
        effort: None,
        // DEV-706: same for the compaction window.
        autocompact: None,
    };
    let fj: Arc<dyn crate::forge_log::ForgeReads> = Arc::new(crate::provider::FjForgeReads::new(
        Arc::new(crate::provider::ProcessRunner::new()),
    ));
    let classifier: Arc<dyn FailureClassifier> = Arc::new(AnalysisClassifier::new(
        claude.clone(),
        claude_cfg,
        std::time::Duration::from_secs(analysis_timeout_secs),
        analysis_base_dir,
        paths.logs_dir(),
        fj,
        // DEV-705: a classification bills against the runner's default budget,
        // frozen here; there is no per-action YAML for the webhook mint path.
        {
            let defaults = RunnerDefaults::default();
            crate::usage::RunBudget::new(defaults.daily_budget_usd, defaults.budget_warn_fraction)
        },
        Some(paths.usage_log()),
        pool.clone(),
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
        paths: paths.clone(),
        pr_comment_runner: Arc::new(crate::provider::ProcessRunner::new()),
        commit_runs: Arc::new(crate::provider::FjForgeReads::new(Arc::new(
            crate::provider::ProcessRunner::new(),
        ))),
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

/// CLAUDE-164: one enabled, successfully-parsed action's identity, collected
/// during a scan so the guardrail can spot two actions doing identical work.
struct ScannedAction {
    /// The action `name:` as written (case preserved).
    name: String,
    /// On-disk YAML path, for the operator-facing notice.
    file_path: String,
    /// Effective native source key (`kind` + source query + `base_url`, or
    /// `kind` + host + repos), or `None` for a scripted action, whose source is
    /// an opaque nu snippet we cannot compare.
    source_key: Option<String>,
}

/// CLAUDE-164: a detected redundant-configuration collision between two or more
/// enabled actions. `members` are the action names whose pages carry the
/// notice; `message` is the operator-facing warning naming every colliding file.
struct Collision {
    members: Vec<String>,
    message: String,
}

/// CLAUDE-164: the effective native source key for same-source detection.
/// `None` for a scripted action (its source is an opaque nu snippet). For a
/// native kind the key folds in the kind plus the fields that determine which
/// jobs it gathers, so two actions with equal keys provably poll the same
/// source. `\u{1f}` (unit separator) joins the parts so no field value can forge
/// a boundary.
fn native_source_key(action: &crate::action::Action) -> Option<String> {
    use crate::action::NativeConfig;
    match action.native.as_ref()? {
        NativeConfig::Youtrack(cfg) => {
            let base = cfg.youtrack.base_url.as_deref().unwrap_or("");
            Some(format!("youtrack\u{1f}{base}\u{1f}{}", cfg.youtrack.query))
        }
        NativeConfig::ClosedIssueSummary(cfg) => {
            // Two reporters produce the same report iff they select the same
            // parents (nominated ids plus query) over the same project set. Both
            // lists are sets, so they are sorted before joining.
            let mut parents = cfg.parents.clone();
            parents.sort();
            let mut projects = cfg.projects.clone();
            projects.sort();
            Some(format!(
                "closed-issue-summary\u{1f}{}\u{1f}{}\u{1f}{}",
                cfg.query,
                parents.join(","),
                projects.join(",")
            ))
        }
    }
}

/// CLAUDE-164: detect enabled actions that do redundant work. Two conditions,
/// each a distinct acceptance criterion: (1) names equal ignoring case
/// (`youtrack` vs `YouTrack`), almost always the same intended action split by a
/// typo, both scheduled; (2) the same native kind + source key, so they gather
/// the same jobs.
///
/// A group of one never fires; scripted actions have no source key and so are
/// exempt from (2). The two checks are independent, so the production case
/// (case-colliding names that also share a query) yields both notices.
fn detect_collisions(scanned: &[ScannedAction]) -> Vec<Collision> {
    use std::collections::BTreeMap;
    let mut collisions = Vec::new();

    let describe = |group: &[&ScannedAction]| {
        group
            .iter()
            .map(|a| format!("{} ({})", a.name, a.file_path))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // 1. Case-insensitive action-name collision.
    let mut by_lower: BTreeMap<String, Vec<&ScannedAction>> = BTreeMap::new();
    for a in scanned {
        by_lower.entry(a.name.to_lowercase()).or_default().push(a);
    }
    for (lower, group) in &by_lower {
        if group.len() < 2 {
            continue;
        }
        collisions.push(Collision {
            members: group.iter().map(|a| a.name.clone()).collect(),
            message: format!(
                "redundant config: {} enabled actions share the case-insensitive name \"{}\": {}. Both are scheduled and silently compete on the shared concurrency caps; rename or disable one (CLAUDE-164).",
                group.len(),
                lower,
                describe(group),
            ),
        });
    }

    // 2. Same native source (kind + query/base_url, or kind + host + repos).
    let mut by_source: BTreeMap<&str, Vec<&ScannedAction>> = BTreeMap::new();
    for a in scanned {
        if let Some(key) = a.source_key.as_deref() {
            by_source.entry(key).or_default().push(a);
        }
    }
    for group in by_source.values() {
        if group.len() < 2 {
            continue;
        }
        collisions.push(Collision {
            members: group.iter().map(|a| a.name.clone()).collect(),
            message: format!(
                "redundant config: {} enabled actions poll the same source: {}. They gather the same jobs and silently compete on the shared concurrency caps; consolidate or disable one (CLAUDE-164).",
                group.len(),
                describe(group),
            ),
        });
    }

    collisions
}

pub async fn scan_actions(pool: &DbPool, actions_dir: &Path) -> Result<()> {
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
    // CLAUDE-164: identities of the enabled, successfully-parsed actions, used
    // after the loop to flag redundant-work collisions.
    let mut scanned: Vec<ScannedAction> = Vec::new();
    for entry in read.flatten() {
        let path = entry.path();
        let ext = path.extension().and_then(|s| s.to_str());
        if !matches!(ext, Some("yml") | Some("yaml")) {
            continue;
        }
        match action::load_from_path(&path) {
            Ok(la) => {
                // CLAUDE-61 / CLAUDE-220: `webhook-forgejo` and
                // `webhook-forgejo-comment` are reserved synthetic action names
                // the web layer injects for the event-driven listeners. An
                // on-disk YAML claiming one would be editable / schedulable and
                // would collide with the synthetic entry, so refuse to index it.
                // The reserved set is the one shared predicate.
                if crate::web::is_synthetic_action(&la.action.name) {
                    warn!(
                        path = %path.display(),
                        name = %la.action.name,
                        "refusing to index action: the name is reserved for a synthetic event-driven action"
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
                    // CLAUDE-162: cache the per-action cap so the scheduler can
                    // resolve the global auto default without re-parsing YAML.
                    max_parallel: la.action.concurrency.max_parallel.max(1) as i64,
                };
                state::action_index_upsert(pool, &row).await?;
                // CLAUDE-164: only enabled actions are scheduled, so only they
                // can compete; disabled and parse-errored files are excluded.
                if la.action.enabled {
                    scanned.push(ScannedAction {
                        name: la.action.name.clone(),
                        file_path: la.path.display().to_string(),
                        source_key: native_source_key(&la.action),
                    });
                }
            }
            Err(e) => {
                // Index the broken file too so the operator can see it.
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("invalid");
                // CLAUDE-61 / CLAUDE-220: same reservation as the parse-ok
                // branch - never let a file (even an unparseable one) occupy a
                // synthetic name's slot.
                if crate::web::is_synthetic_action(stem) {
                    warn!(
                        path = %path.display(),
                        name = stem,
                        "refusing to index file: the name is reserved for a synthetic event-driven action"
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
                    max_parallel: 1,
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

    // CLAUDE-164: guardrail. Two enabled actions that poll the same source
    // (case-colliding names, or the same native kind + source query) are both
    // scheduled and silently double-gather while contending on the shared
    // daemon-wide concurrency caps. Detection is operator-visible only: a WARN
    // log plus a persisted `run_events` notice on each colliding action. Never
    // auto-disable - that risks silencing the wrong one and dropping work.
    for collision in detect_collisions(&scanned) {
        warn!(message = %collision.message, "duplicate-source action guardrail");
        let events = [state::InputEvent {
            level: "warn".to_string(),
            context: Some("config-collision".to_string()),
            message: collision.message.clone(),
        }];
        for member in &collision.members {
            state::run_events_record(pool, member, &now, &events).await?;
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
    pool: DbPool,
    snippet: Arc<SnippetRunner>,
    claude: Arc<ClaudeInvoker>,
    in_flight: api::InFlight,
    /// CLAUDE-28: daemon-wide per-key gate, threaded into every run so jobs
    /// sharing a concurrency key serialize across runs and triggers.
    keygate: KeyGate,
    /// CLAUDE-9: per-run cancellation tokens, registered by `run_action_inner`.
    runs: Arc<RunRegistry>,
    /// CLAUDE-228: daemon-wide per-action gather marker. The tick reads it to
    /// skip a pickup whose previous gather is still running.
    gather: GatherGate,
    /// CLAUDE-162: daemon-wide concurrency limiter (global + per-action
    /// semaphores), acquired per job so caps hold across overlapping runs.
    limiter: Limiter,
    /// CLAUDE-162: live handle to the concurrency settings (global cap + regather
    /// cadence), read each scheduler tick so a Settings save applies live.
    concurrency: Arc<RwLock<ConcurrencyConfig>>,
    /// CLAUDE-223: daemon-wide global-pause reader. Refreshed every scheduler
    /// tick (which has to read the flag anyway) and handed to every run so the
    /// per-job gates share one TTL-cached read instead of querying per job.
    pause: crate::pause::PauseGate,
    /// Whether an agent can be spawned at all (a Claude credential exists). A
    /// pause is an operator decision; this is an instance that was never
    /// finished, and it stops the scheduler for the same reason: dispatching a
    /// run that can only fail at the `claude` call burns an attempt and buries
    /// real failures in noise.
    readiness: crate::readiness::ReadinessGate,
    /// CLAUDE-162: in-memory per-action timestamp of the last pickup fire. Drives
    /// the `regather_interval` cadence decoupled from run rows, so an action whose
    /// re-gather finds no fresh work (and thus creates no run row) is still not
    /// re-fired every 5s tick.
    last_fire: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl Dispatcher {
    /// CLAUDE-162: start an (overlapping) run for `action_name`. Unlike the
    /// pre-CLAUDE-162 dispatch, a run is NOT rejected because another run for the
    /// same action is in flight; overlap is the point. The in-flight refcount is
    /// bumped for the delete-conflict guard and dropped when the run finishes.
    async fn dispatch(self: Arc<Self>, action_name: String, trigger_source: &'static str) {
        {
            let mut g = self.in_flight.lock().await;
            *g.entry(action_name.clone()).or_insert(0) += 1;
        }
        let me = self.clone();
        let name_for_log = action_name.clone();
        tokio::spawn(async move {
            if let Err(e) = me.execute(&action_name, trigger_source).await {
                warn!(action = %name_for_log, error = %e, "action execution errored");
            }
            let mut g = me.in_flight.lock().await;
            if let Some(n) = g.get_mut(&name_for_log) {
                *n -= 1;
                if *n == 0 {
                    g.remove(&name_for_log);
                }
            }
        });
    }

    async fn execute(&self, action_name: &str, trigger_source: &'static str) -> Result<()> {
        // Readiness first: an unconfigured instance cannot spawn an agent, and a
        // run dispatched anyway would fail at the `claude` call and consume an
        // attempt against `max_attempts_per_key`.
        if !self.readiness.ready() {
            info!(
                action = %action_name,
                "{}", crate::readiness::NOT_READY_REASON
            );
            return Ok(());
        }
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

        // CLAUDE-162: the whole-run per-action `flock` (`ActionLock`) is gone on
        // the daemon path: it forced a full drain (only one run per action at a
        // time), which is exactly the run barrier this issue removes. In-process
        // correctness now comes from the daemon-wide `Limiter` (per-action +
        // global caps) plus the `KeyGate` (same-key serialization). The
        // standalone `claude-run run` path still takes `ActionLock` for its
        // cross-process guard; running standalone `run` for an action the daemon
        // also schedules is unsupported concurrent use (documented in the README).

        run_action_inner(
            &loaded,
            &self.paths,
            &self.pool,
            self.snippet.clone(),
            self.claude.clone(),
            trigger_source,
            Some(&self.runs),
            &self.keygate,
            &self.limiter,
            &self.pause,
            &self.gather,
        )
        .await
        .map(|_| ())
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
    pool: &DbPool,
    snippet: Arc<SnippetRunner>,
    claude: Arc<ClaudeInvoker>,
    triggered_by: &str,
    registry: Option<&RunRegistry>,
    keygate: &KeyGate,
    limiter: &Limiter,
    pause: &crate::pause::PauseGate,
    // CLAUDE-228: per-action gather marker, held across the gather step only.
    gather: &GatherGate,
) -> Result<i32> {
    let defaults = RunnerDefaults::default();
    let limits = job::effective_limits(&defaults, loaded);
    // DEV-705: the run's spend ceiling, resolved from configuration BEFORE any
    // work starts and shared by every job of this run. Nothing hands back a way
    // to raise it, which is what makes it a ceiling rather than a prompt.
    let spend = crate::usage::RunSpend::new(
        loaded.action.name.clone(),
        limits.run_budget(),
        Some(paths.usage_log()),
    );

    // Pre-run daily caps, checked for EVERY kind. DEV-705: the documented exit
    // codes 4 and 6 are what a refused run reports; native handlers re-check the
    // same two conditions themselves, so this only moves WHERE the refusal is
    // observed, never whether the run is refused.
    let daily = state::daily_count_get(pool, &loaded.action.name).await?;
    if daily >= limits.max_runs_per_day {
        warn!(count = daily, "daily cap reached");
        return Ok(crate::exit::DAILY_CAP);
    }
    let cost = state::daily_cost_get(pool, &loaded.action.name).await?;
    if cost >= limits.daily_budget_usd {
        warn!(cost_usd = cost, "daily budget reached");
        return Ok(crate::exit::OVER_BUDGET);
    }

    // CLAUDE-30: native kinds route to a built-in Rust handler. Scripted
    // actions fall through to the generic nu-snippet pipeline below, unchanged.
    if loaded.action.kind.is_native() {
        crate::native::dispatch(crate::native::NativeContext {
            action: loaded,
            paths,
            pool,
            snippet,
            claude,
            triggered_by,
            registry,
            keygate: keygate.clone(),
            limiter: limiter.clone(),
            pause: pause.clone(),
            gather: gather.clone(),
            spend: spend.clone(),
        })
        .await?;
        // DEV-705: the handler owns its own run row and closed it with the
        // right code; this is the PROCESS exit code, so a standalone
        // `claude-run run` of a native action that hit its ceiling exits
        // non-zero too.
        return Ok(spend.exit_code(crate::exit::OK));
    }

    // CLAUDE-228: mark the action as gathering so the scheduler does not stack
    // another pickup on top of a slow `input.command`. Dropped right after the
    // gather; overlapping runs past this point stay allowed (CLAUDE-162).
    let jobs = {
        let _gather_guard = gather.try_acquire(&loaded.action.name);
        job::gather_jobs(loaded, snippet.as_ref(), paths).await?
    };
    info!(action = %loaded.action.name, count = jobs.len(), "input jobs gathered");

    // CLAUDE-162 / CLAUDE-163: admission check. Compute each gathered job's issue
    // key and concurrency key, and drop jobs whose CONCURRENCY key is already in
    // flight in another (overlapping) run of this action. If nothing fresh
    // remains, return WITHOUT `run_begin` so a re-gather that only re-sees
    // already-claimed work does not litter run history with empty, redundant run
    // rows. CLAUDE-163: deduping by concurrency key (not issue id) stops a run
    // from gathering a second same-key job while that key is in flight; such a
    // sibling could only park on the KeyGate and defer. Same-key serialization +
    // this dedup keep two overlapping runs from ever working the same key.
    let all_keys: Vec<String> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| job::job_key(loaded, j, i))
        .collect();
    let all_ckeys: Vec<String> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| job::concurrency_key(loaded, j, i))
        .collect();
    let inflight_ckeys: std::collections::HashSet<String> =
        state::action_inflight_concurrency_keys(pool, &loaded.action.name)
            .await?
            .into_iter()
            .collect();
    let fresh_idx: Vec<usize> = (0..jobs.len())
        .filter(|&i| !inflight_ckeys.contains(&all_ckeys[i]))
        .collect();
    if fresh_idx.is_empty() {
        info!(
            action = %loaded.action.name,
            gathered = jobs.len(),
            "no fresh queued work (all in flight in another run); skipping empty run"
        );
        return Ok(crate::exit::OK);
    }

    let run_id = state::run_begin(pool, &loaded.action.name, triggered_by).await?;
    // CLAUDE-228: from here the row MUST be closed on every exit path, including
    // an early `?` out of the bookkeeping below.
    let run_guard = state::RunGuard::new(pool, run_id);

    // CLAUDE-9: register a cancellation token for this run and ensure it is
    // unregistered on every exit path (including the `?` early returns below).
    let cancel = match registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard { registry, run_id };
    // CLAUDE-223: run-scoped view of the global-pause gate. Any job it stops
    // makes the run finish with `exit::PAUSED`.
    let run_pause = pause.for_action_run(&loaded.action.name);

    // CLAUDE-1: record the planned job keys so the web UI shows what's
    // queued / in-flight before any outcome record lands (fresh subset only).
    let planned_keys: Vec<String> = fresh_idx.iter().map(|&i| all_keys[i].clone()).collect();
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

    // CLAUDE-162: build one task per FRESH job (the dedup dropped in-flight ones).
    let mut items: Vec<(String, crate::concurrency::JobFuture)> =
        Vec::with_capacity(fresh_idx.len());
    for (slot, &idx) in fresh_idx.iter().enumerate() {
        let job = &jobs[idx];
        let gate_key = job::concurrency_key(loaded, job, idx);
        let inflight_ckey = gate_key.clone();
        let job_key = planned_keys[slot].clone();
        let job = job.clone();
        let loaded = loaded_arc.clone();
        let paths = paths_owned.clone();
        let pool = pool_owned.clone();
        let snippet = snippet.clone();
        let claude = claude.clone();
        let job_cancel = cancel.clone();
        let job_pause = run_pause.clone();
        let job_spend = spend.clone();
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
            // DEV-705: the run's own ceiling. Checked before every pickup, so a
            // run that reached it starts no further job.
            if job_spend.exceeded() {
                warn!(action = %loaded.action.name, "{}", job_spend.stop_reason());
                return Ok(false);
            }
            // CLAUDE-28: mark this job in flight (cleared below and by
            // `run_finish`) so the web UI can badge every running job.
            if let Err(e) = state::run_inflight_add(&pool, run_id, &job_key, &inflight_ckey).await {
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
                pause: job_pause,
                spend: job_spend,
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
        action: loaded.action.name.clone(),
        max_parallel,
        limiter: limiter.clone(),
        gate: keygate.clone(),
        cancel: cancel.clone(),
        pause: run_pause.clone(),
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

    // DEV-705: an over-budget stop is the run's outcome unless a cancel or a
    // pause already claimed it.
    let exit_code = spend.exit_code(run_pause.exit_code(cancel.is_cancelled()));
    if exit_code == crate::exit::OVER_BUDGET {
        warn!(action = %loaded.action.name, "{}", spend.stop_reason());
    }
    run_guard.finish(exit_code, processed).await?;
    // DEV-707: one summary line and one machine-readable record per run.
    spend.finish(Some(run_id), exit_code);
    if let Some(e) = first_err {
        warn!(action = %loaded.action.name, error = %e, "one or more jobs errored during the run");
    }
    info!(action = %loaded.action.name, processed, exit_code, "run complete");
    Ok(exit_code)
}

/// CLAUDE-162: resolve the effective global concurrency cap. An explicit
/// `max_parallel_global > 0` wins; `0` (auto) resolves to the maximum
/// `max_parallel` among enabled actions, so a single action still reaches its
/// full per-action cap out of the box. Floored to 1 (an empty/all-disabled index
/// yields at least one permit so triggers still run). Pure, so the auto default
/// is unit-testable without a daemon.
pub fn resolve_global_cap(cfg: &ConcurrencyConfig, actions: &[ActionIndexRow]) -> usize {
    if cfg.max_parallel_global > 0 {
        return cfg.max_parallel_global as usize;
    }
    actions
        .iter()
        .filter(|a| a.enabled && a.parse_error.is_none())
        .map(|a| a.max_parallel.max(1) as usize)
        .max()
        .unwrap_or(1)
        .max(1)
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
                crate::native::youtrack::has_local_only_work,
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

/// CLAUDE-175: periodic DB retention sweep. Prunes the unbounded history tables
/// past the configured window on the [`GC_TICK_SECS`] cadence, reading the live
/// config off the shared handle so a Settings save applies on the next tick. The
/// interval's first tick fires immediately, so a prune runs on serve start.
async fn retention_loop(
    pool: crate::db::DbPool,
    cfg: Arc<tokio::sync::RwLock<crate::config::DbRetentionConfig>>,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(GC_TICK_SECS));
    loop {
        ticker.tick().await;
        let snapshot = cfg.read().await.clone();
        if !snapshot.enabled {
            continue;
        }
        match crate::retention::sweep(&pool, &snapshot, chrono::Utc::now()).await {
            Ok(stats) if stats.deleted > 0 => {
                info!(deleted = stats.deleted, "db retention sweep complete")
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "db retention sweep failed"),
        }
    }
}

/// CLAUDE-245: the `(action_name, job_key)` pairs whose job is running right
/// now, from `run_inflight_jobs` joined to the runs that have not ended. This is
/// the evidence the stream-log sweep needs that a log is finished with: the
/// sweep runs inside the daemon that owns those runs, and every runner marks a
/// job in flight for exactly the span it is being worked
/// (`state::run_inflight_add` / `run_inflight_remove`, cleared wholesale by
/// `run_finish`, with `reap_orphaned_runs` closing anything a crash stranded).
async fn inflight_stream_log_jobs(
    pool: &DbPool,
) -> Result<std::collections::HashSet<(String, String)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT r.action_name, j.job_key
         FROM run_inflight_jobs j
         JOIN runs r ON r.id = j.run_id
         WHERE r.ended_at IS NULL",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// CLAUDE-245: periodic log GC, on the [`GC_TICK_SECS`] cadence the other two
/// sweeps use. The interval's first tick fires immediately, so a sweep runs on
/// serve start; the config is read off the shared handle per tick so a Settings
/// save applies on the next pass. The walks, the unlinks and the truncations are
/// blocking filesystem work, so they run on a blocking thread.
///
/// Four passes on the one tick, because this section of config now bounds four
/// kinds of writer. Per-attempt stream logs (`logs/<action>/streams/`) and
/// CLAUDE-262's per-job snippet directories (`snippets/<action>/<job_key>/`)
/// are both per-run artifacts and are swept by AGE, both guarded by the same
/// in-flight read: a failed read SKIPS both passes rather than sweeping with an
/// empty set, because without that table neither sweep can tell a finished
/// job's artifacts from a running one's, and the wrong guess deletes a live
/// run's only transcript or its snippet directory. CLAUDE-257's `daemon.log`
/// and each `logs/<action>/<output.file>` are single append-forever files
/// instead, so they are bounded by SIZE ([`crate::gc::sweep_oversized_logs`])
/// and that pass runs regardless of the in-flight read, which it does not
/// depend on. CLAUDE-262's `webhook-logs/` files are per-triaged-run artifacts
/// like the stream logs, but a webhook run is not tracked in
/// `run_inflight_jobs` (it goes through the mint pipeline, not a scheduled
/// job), so [`crate::gc::sweep_webhook_logs`] is age-only and also runs
/// regardless of the in-flight read.
async fn stream_log_gc_loop(
    paths: Paths,
    pool: DbPool,
    cfg: Arc<tokio::sync::RwLock<crate::config::StreamLogGcConfig>>,
) {
    let logs_base = paths.logs_dir();
    let snippets_base = paths.snippets_dir();
    let webhook_logs_base = paths.webhook_logs_dir();
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(GC_TICK_SECS));
    loop {
        ticker.tick().await;
        let snapshot = cfg.read().await.clone();
        if !snapshot.enabled {
            continue;
        }
        match inflight_stream_log_jobs(&pool).await {
            Ok(in_flight) => {
                let base = logs_base.clone();
                let cfg = snapshot.clone();
                let flight = in_flight.clone();
                let result = tokio::task::spawn_blocking(move || {
                    crate::gc::sweep_stream_logs(&base, &cfg, std::time::SystemTime::now(), &flight)
                })
                .await;
                match result {
                    Ok(stats) => info!(
                        deleted = stats.deleted,
                        kept = stats.kept,
                        bytes_reclaimed = stats.bytes_reclaimed,
                        "stream-log GC sweep complete"
                    ),
                    Err(e) => warn!(error = %e, "stream-log GC sweep task failed"),
                }

                let base = snippets_base.clone();
                let cfg = snapshot.clone();
                let result = tokio::task::spawn_blocking(move || {
                    crate::gc::sweep_snippets(&base, &cfg, std::time::SystemTime::now(), &in_flight)
                })
                .await;
                match result {
                    Ok(stats) => info!(
                        deleted = stats.deleted,
                        kept = stats.kept,
                        bytes_reclaimed = stats.bytes_reclaimed,
                        "snippet GC sweep complete"
                    ),
                    Err(e) => warn!(error = %e, "snippet GC sweep task failed"),
                }
            }
            Err(e) => {
                warn!(error = %e, "stream-log/snippet GC: cannot read in-flight jobs; skipping both sweeps rather than risk deleting a live run's artifacts")
            }
        }

        let base = logs_base.clone();
        let cfg = snapshot.clone();
        let result =
            tokio::task::spawn_blocking(move || crate::gc::sweep_oversized_logs(&base, &cfg)).await;
        match result {
            Ok(stats) => info!(
                bounded = stats.bounded,
                kept = stats.kept,
                bytes_reclaimed = stats.bytes_reclaimed,
                "log size GC pass complete"
            ),
            Err(e) => warn!(error = %e, "log size GC task failed"),
        }

        let base = webhook_logs_base.clone();
        let cfg = snapshot.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::gc::sweep_webhook_logs(&base, &cfg, std::time::SystemTime::now())
        })
        .await;
        match result {
            Ok(stats) => info!(
                deleted = stats.deleted,
                kept = stats.kept,
                bytes_reclaimed = stats.bytes_reclaimed,
                "webhook-log GC sweep complete"
            ),
            Err(e) => warn!(error = %e, "webhook-log GC sweep task failed"),
        }
    }
}

/// CLAUDE-328: the automerge reconciler's own tick. Independent of both the
/// scheduler's [`TICK_SECS`] (too chatty: every open agent PR would take a
/// forge `pr_status` call five times a minute) and the hourly [`GC_TICK_SECS`]
/// (too slow a backstop for a PR that only needed a missed webhook re-tried),
/// this is a fixed middle ground chosen for a first cut; revisit if it proves
/// too chatty against the forge or too slow as a backstop in practice.
const AUTOMERGE_RECONCILE_TICK_SECS: u64 = 300;

/// CLAUDE-328: the reconciler backstop for a missed, dropped, or too-early
/// Action Run success webhook. Ticks independently of any webhook delivery,
/// on [`AUTOMERGE_RECONCILE_TICK_SECS`] (first tick immediate, so a sweep also
/// runs on serve start), and calls
/// [`crate::web::webhooks::reconcile_automerge`], which walks every currently
/// `open` `agent_pull_requests` row through the exact same decision function
/// the webhook path uses. A `None` mint context (no usable provider backend)
/// skips the tick entirely, matching how the webhook consumers themselves
/// degrade with no backend to call.
async fn automerge_reconciler_loop(
    pool: DbPool,
    mint: Option<crate::web::webhooks::MintContext>,
    keygate: KeyGate,
) {
    let Some(mint) = mint else {
        warn!("automerge reconciler disabled: no usable provider backend (mint disabled)");
        return;
    };
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
        AUTOMERGE_RECONCILE_TICK_SECS,
    ));
    loop {
        ticker.tick().await;
        crate::web::webhooks::reconcile_automerge(&pool, &mint, &keygate).await;
    }
}

/// CLAUDE-269: how often the `claude auth status --json` probe runs while the
/// daemon reads as ready.
const LOGIN_PROBE_INTERVAL_READY_SECS: u64 = 600;
/// How often it runs while login-required, so the daemon notices a fresh
/// sign-in (from outside the web UI's own clear-on-success path, e.g. an
/// operator running `claude auth login` by hand inside the container) sooner
/// than the ready-state cadence.
const LOGIN_PROBE_INTERVAL_LOGIN_REQUIRED_SECS: u64 = 60;

/// CLAUDE-269: periodically probe whether the agent's own Claude login is
/// still usable, independent of whether any action happens to invoke `claude`
/// right now. Runs once immediately at serve start, then on a cadence that
/// tightens while login-required so a fresh sign-in is noticed quickly.
///
/// Skipped on the `always()` gate (empty `agent_config_dir`: nothing holds
/// this state, by construction) and while unready for the no-credential
/// reason (there is no login to probe yet; probing then would only ever
/// report `loggedIn: false` for a reason readiness already reports).
async fn login_probe_loop(
    readiness: crate::readiness::ReadinessGate,
    claude_binary: String,
    agent_config_dir: PathBuf,
    api_key: crate::service_auth::ApiKeyHandle,
) {
    if agent_config_dir.as_os_str().is_empty() {
        return;
    }
    loop {
        login_probe_tick(&readiness, &claude_binary, &agent_config_dir, &api_key).await;
        let interval = if readiness.is_login_required() {
            LOGIN_PROBE_INTERVAL_LOGIN_REQUIRED_SECS
        } else {
            LOGIN_PROBE_INTERVAL_READY_SECS
        };
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
    }
}

/// One pass of [`login_probe_loop`], factored out so it is unit-testable
/// without driving an infinite loop. Skips the probe (and touches nothing)
/// while the gate is unready for the no-credential reason: there is no login
/// to probe yet, and probing then would only ever report `loggedIn: false` for
/// a reason readiness already reports.
async fn login_probe_tick(
    readiness: &crate::readiness::ReadinessGate,
    claude_binary: &str,
    agent_config_dir: &Path,
    api_key: &crate::service_auth::ApiKeyHandle,
) {
    let check = readiness.check();
    if check.kind == Some(crate::readiness::NotReadyKind::NoCredential) {
        return;
    }
    let key = match api_key.read() {
        Ok(slot) => slot.clone(),
        Err(e) => {
            warn!(error = %e, "Anthropic key handle is poisoned; the probe runs without ANTHROPIC_API_KEY");
            None
        }
    };
    let binary = claude_binary.to_string();
    let dir = agent_config_dir.to_path_buf();
    let outcome = crate::blocking::in_place(move || {
        crate::service_auth::probe_claude_login(&binary, &dir, key.as_deref())
    });
    match outcome {
        crate::service_auth::LoginProbeOutcome::LoggedOut => {
            readiness.trip_login_required(
                "the status probe reports the Claude CLI is not logged in",
                "probe",
            );
        }
        crate::service_auth::LoginProbeOutcome::LoggedIn => {
            readiness.clear_login_required();
        }
        crate::service_auth::LoginProbeOutcome::Indeterminate(reason) => {
            // Neither logged in nor logged out: change nothing. A spawn
            // failure, a timeout, or unparseable output must never be
            // collapsed into either answer.
            tracing::error!(reason = %reason, "Claude login status probe inconclusive; leaving readiness unchanged");
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

/// CLAUDE-228: what one action's pause read means for this tick's fire decision.
/// An unreadable pause state skips the action: it cannot prove the action is NOT
/// paused, and the next tick retries 5s later. Pure so the error branch is
/// testable; the caller logs the cause before asking.
fn pause_read_skips_pickup<T>(status: &Result<Option<T>>) -> bool {
    match status {
        Ok(None) => false,
        Ok(Some(_)) => true,
        Err(_) => true,
    }
}

async fn scheduler_tick(dispatch: &Arc<Dispatcher>) -> Result<()> {
    // Refresh on-disk action index so newly-added files become schedulable
    // without a daemon restart.
    scan_actions(&dispatch.pool, &dispatch.paths.actions_dir()).await?;

    // CLAUDE-223: refresh through the shared gate rather than reading the flag
    // directly, so the value the blocking per-job checkpoints read is never more
    // than one tick stale. A read failure is logged at `error` inside the gate
    // and reported as not paused, so a transient DB error cannot silently stop
    // the daemon.
    if dispatch.pause.refresh().await {
        return Ok(());
    }
    // No Claude credential means no run can succeed, so poll nothing: the source
    // queries (and their auth errors) are pure noise until setup finishes.
    if !dispatch.readiness.ready() {
        return Ok(());
    }
    let now = Utc::now();
    let rows = state::action_index_list(&dispatch.pool).await?;

    // CLAUDE-162: resolve + apply the effective global cap every tick, so a
    // Settings save (or an action's max_parallel edit) takes effect on the next
    // tick. `set_global_max` is a no-op when the value is unchanged.
    let cfg = dispatch.concurrency.read().await.clone();
    dispatch
        .limiter
        .set_global_max(resolve_global_cap(&cfg, &rows));
    let interval = std::time::Duration::from_secs(cfg.regather_interval_secs.max(1));

    for r in rows {
        if !r.enabled || r.parse_error.is_some() {
            continue;
        }
        // CLAUDE-228: a failed pause read skips THIS action only. It used to be
        // a `?`, so one transient pool timeout aborted the whole tick and no
        // action fired at all.
        let pause_status = state::action_pause_status(&dispatch.pool, &r.name).await;
        if let Err(e) = &pause_status {
            warn!(action = %r.name, error = %e, "pause status read failed; skipping this action's pickup for this tick");
        }
        if pause_read_skips_pickup(&pause_status) {
            continue;
        }
        // Manual actions never auto-fire; only `trigger` dispatches them.
        let Some(kind) = r.schedule_kind.as_deref() else {
            continue;
        };
        if kind == "manual" {
            continue;
        }
        // CLAUDE-162: the pickup fire decision is governed by `regather_interval`,
        // decoupled from the action's `schedule.spec`, using the in-memory
        // last-fire stamp (not run rows) so an empty re-gather does not re-fire
        // every tick. Overlapping runs are allowed, so there is NO in-flight
        // suppression here; a saturated cap is instead handled by `can_admit`,
        // which back-pressures without advancing the last-fire stamp so the next
        // tick retries the moment a permit frees.
        let last_fire = dispatch.last_fire.lock().await.get(&r.name).copied();
        if !scheduler::regather_due(last_fire, now, interval) {
            continue;
        }
        // CLAUDE-228: a gather still running from the previous fire means this
        // pickup would re-run the same query against the same source. Skip it
        // WITHOUT advancing `last_fire` (same contract as `can_admit` below), so
        // the next tick fires the moment the gather finishes.
        if dispatch.gather.is_gathering(&r.name) {
            info!(action = %r.name, "pickup deferred: gather still in flight");
            continue;
        }
        let max_parallel = r.max_parallel.max(1) as usize;
        if !dispatch.limiter.can_admit(&r.name, max_parallel) {
            info!(action = %r.name, "pickup deferred: concurrency cap saturated");
            continue;
        }
        info!(action = %r.name, "scheduler firing (pickup)");
        dispatch.last_fire.lock().await.insert(r.name.clone(), now);
        dispatch.clone().dispatch(r.name.clone(), "scheduler").await;
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

    /// CLAUDE-269: a `loggedIn: false` probe trips the gate; a subsequent
    /// `loggedIn: true` clears it.
    #[tokio::test]
    async fn login_probe_tick_trips_and_clears_the_gate() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let readiness = crate::readiness::ReadinessGate::new(tmp.path().to_path_buf(), None);
        assert!(readiness.check().ready);

        let stub = tmp.path().join("claude-stub.sh");
        crate::testutil::write_stub(&stub, "#!/bin/sh\nprintf '%s' '{\"loggedIn\":false}'\n");
        let key: crate::service_auth::ApiKeyHandle = Arc::new(std::sync::RwLock::new(None));
        login_probe_tick(&readiness, stub.to_str().unwrap(), tmp.path(), &key).await;
        assert!(!readiness.check().ready);
        assert_eq!(
            readiness.check().kind,
            Some(crate::readiness::NotReadyKind::LoginRequired)
        );

        crate::testutil::write_stub(&stub, "#!/bin/sh\nprintf '%s' '{\"loggedIn\":true}'\n");
        login_probe_tick(&readiness, stub.to_str().unwrap(), tmp.path(), &key).await;
        assert!(readiness.check().ready);
    }

    /// The probe is skipped entirely while unready for the no-credential
    /// reason: there is no login to probe, and the stub would fail loudly if
    /// it were ever invoked (no canned behavior needed to prove the skip).
    #[tokio::test]
    async fn login_probe_tick_skips_the_probe_with_no_credential() {
        let tmp = tempfile::tempdir().unwrap();
        // No `.credentials.json`: the gate reads not-ready for `NoCredential`.
        let readiness = crate::readiness::ReadinessGate::new(tmp.path().to_path_buf(), None);
        assert_eq!(
            readiness.check().kind,
            Some(crate::readiness::NotReadyKind::NoCredential)
        );

        let key: crate::service_auth::ApiKeyHandle = Arc::new(std::sync::RwLock::new(None));
        // A binary that does not exist: if the tick tried to probe, it would
        // still resolve to `Indeterminate` (a spawn failure) rather than
        // panicking, so this alone would not prove the skip. The real proof is
        // the kind staying `NoCredential` (a probe outcome never sets that
        // kind) with no `login_required` state ever appearing.
        login_probe_tick(&readiness, "/nonexistent/claude-binary", tmp.path(), &key).await;
        assert!(!readiness.is_login_required());
        assert_eq!(
            readiness.check().kind,
            Some(crate::readiness::NotReadyKind::NoCredential)
        );
    }

    /// An indeterminate probe outcome (unparseable output) changes nothing.
    #[tokio::test]
    async fn login_probe_tick_leaves_state_unchanged_on_an_indeterminate_outcome() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let readiness = crate::readiness::ReadinessGate::new(tmp.path().to_path_buf(), None);
        let stub = tmp.path().join("claude-stub.sh");
        crate::testutil::write_stub(&stub, "#!/bin/sh\nprintf 'garbage'\nexit 1\n");
        let key: crate::service_auth::ApiKeyHandle = Arc::new(std::sync::RwLock::new(None));

        login_probe_tick(&readiness, stub.to_str().unwrap(), tmp.path(), &key).await;
        assert!(readiness.check().ready, "unchanged: still ready");
        assert!(!readiness.is_login_required());
    }

    #[test]
    fn current_process_is_alive() {
        assert!(process_alive(std::process::id() as i32));
    }

    /// CLAUDE-245: the set the stream-log sweep is protected by. A job marked
    /// in flight on a run that has not ended is named; the same job once its
    /// run finishes is not, and a stale marker on an already-ended run does not
    /// pin its log forever. The end-to-end assertion is the last one: an aged
    /// log for the live job survives a sweep that deletes its neighbour.
    #[tokio::test]
    async fn inflight_jobs_protect_only_the_running_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::test_pool().await;

        let live = state::run_begin(&pool, "youtrack", "cli").await.unwrap();
        state::run_inflight_add(&pool, live, "CLAUDE-1", "CLAUDE")
            .await
            .unwrap();
        let done = state::run_begin(&pool, "youtrack", "cli").await.unwrap();
        state::run_inflight_add(&pool, done, "CLAUDE-2", "CLAUDE")
            .await
            .unwrap();
        state::run_finish(&pool, done, 0, 1).await.unwrap();

        let set = inflight_stream_log_jobs(&pool).await.unwrap();
        assert!(set.contains(&("youtrack".to_string(), "CLAUDE-1".to_string())));
        assert!(
            !set.contains(&("youtrack".to_string(), "CLAUDE-2".to_string())),
            "a finished run's job is not in flight"
        );

        let logs = paths.logs_dir();
        let streams = logs.join("youtrack").join("streams");
        std::fs::create_dir_all(&streams).unwrap();
        let running = streams.join("CLAUDE-1-attempt-1.jsonl");
        let finished = streams.join("CLAUDE-2-attempt-1.jsonl");
        std::fs::write(&running, b"{}\n").unwrap();
        std::fs::write(&finished, b"{}\n").unwrap();

        let stats = crate::gc::sweep_stream_logs(
            &logs,
            &crate::config::StreamLogGcConfig::default(),
            std::time::SystemTime::now() + std::time::Duration::from_secs(365 * 86_400),
            &set,
        );
        assert!(running.exists(), "the in-flight job's log must survive");
        assert!(!finished.exists(), "the finished job's aged log is swept");
        assert_eq!((stats.kept, stats.deleted), (1, 1));
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

    /// CLAUDE-228: an action whose previous gather is still running does not get
    /// another pickup, and the deferral does NOT advance `last_fire`, so the next
    /// tick fires the moment the gather finishes (same contract as a saturated
    /// concurrency cap).
    #[tokio::test]
    async fn a_gather_in_flight_defers_that_actions_pickup() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        std::fs::create_dir_all(paths.actions_dir()).unwrap();
        std::fs::write(
            paths.actions_dir().join("act.yml"),
            "name: act\nkind: youtrack\nnative: {}\nschedule:\n  kind: interval\n  spec: 5m\n",
        )
        .unwrap();
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        let dispatch = test_dispatcher(&paths, &pool);

        // Marker held: the tick must not fire, and must not stamp last_fire.
        let marker = dispatch.gather.try_acquire("act").expect("free at first");
        scheduler_tick(&dispatch).await.unwrap();
        assert!(
            dispatch.last_fire.lock().await.get("act").is_none(),
            "a deferred pickup must not advance last_fire"
        );
        assert!(
            state::active_runs(&pool).await.unwrap().is_empty(),
            "no run row is opened for a deferred pickup"
        );

        // Gather done: the very next tick fires, with no cadence penalty.
        drop(marker);
        scheduler_tick(&dispatch).await.unwrap();
        assert!(
            dispatch.last_fire.lock().await.get("act").is_some(),
            "the next tick after the gather finishes fires"
        );
    }

    /// CLAUDE-228: the three pause-read outcomes. The `Err` arm is the fix: it
    /// used to be a `?` that aborted the whole tick, so one transient pool
    /// timeout stopped EVERY action from firing, not just this one.
    #[test]
    fn a_failed_pause_read_skips_only_that_action() {
        let readable_unpaused: Result<Option<(String, Option<String>)>> = Ok(None);
        let readable_paused: Result<Option<(String, Option<String>)>> = Ok(Some((
            "2026-08-17T18:00:00Z".to_string(),
            Some("rebooting".to_string()),
        )));
        let unreadable: Result<Option<(String, Option<String>)>> = Err(crate::error::Error::Other(
            anyhow::anyhow!("pool timed out"),
        ));

        assert!(
            !pause_read_skips_pickup(&readable_unpaused),
            "unpaused fires"
        );
        assert!(
            pause_read_skips_pickup(&readable_paused),
            "paused never fires"
        );
        assert!(
            pause_read_skips_pickup(&unreadable),
            "an unreadable pause state skips this action instead of firing it"
        );
    }

    /// An instance with no Claude credential dispatches nothing. The runner
    /// exists to spawn `claude`, so a run would fail at that call, burn an
    /// attempt against `max_attempts_per_key`, and bury the real cause under a
    /// per-tick source-query error from every action.
    #[tokio::test]
    async fn an_unready_daemon_dispatches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_home = tmp.path().join("agent-home");
        std::fs::create_dir_all(&agent_home).unwrap();
        let gate = crate::readiness::ReadinessGate::new(agent_home.clone(), None);
        assert!(!gate.check().ready, "no credential file yet");

        let paths = paths_with_runtime(tmp.path());
        std::fs::create_dir_all(paths.actions_dir()).unwrap();
        std::fs::write(
            paths.actions_dir().join("act.yml"),
            "name: act\ninput:\n  command: \"echo '[]'\"\nprompt: \"x\"\n",
        )
        .unwrap();
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        scan_actions(&pool, &paths.actions_dir()).await.unwrap();

        let mut dispatch = test_dispatcher(&paths, &pool);
        Arc::get_mut(&mut dispatch).unwrap().readiness = gate.clone();
        dispatch.execute("act", "test").await.unwrap();

        let runs: i64 = sqlx::query_scalar(&crate::db::q(&pool, "SELECT COUNT(*) FROM runs"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(runs, 0, "no run row may be opened while unready");

        // The credential appearing is all it takes; nothing else is consulted.
        std::fs::write(agent_home.join(".credentials.json"), "{}").unwrap();
        assert!(gate.check().ready);
    }

    fn test_dispatcher(paths: &Paths, pool: &crate::db::DbPool) -> Arc<Dispatcher> {
        Arc::new(Dispatcher {
            paths: paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(SnippetRunner::new(None)),
            claude: Arc::new(ClaudeInvoker::new(None)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            keygate: KeyGate::new(),
            gather: GatherGate::new(),
            runs: Arc::new(RunRegistry::new()),
            limiter: Limiter::new(1),
            concurrency: Arc::new(RwLock::new(ConcurrencyConfig::default())),
            pause: crate::pause::PauseGate::new(pool.clone()),
            readiness: crate::readiness::ReadinessGate::always(),
            last_fire: Mutex::new(HashMap::new()),
        })
    }

    /// CLAUDE-223: a globally paused daemon does no scheduler work at all. The
    /// tick returns at the pause gate, BEFORE it recomputes the global
    /// concurrency cap, so an unchanged cap is the observable proof that
    /// nothing downstream (including dispatch) ran. Resuming lets the same tick
    /// through; the action is `manual`, so it is still never dispatched and the
    /// test spawns no run.
    #[tokio::test]
    async fn a_global_pause_blocks_the_scheduler_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        std::fs::create_dir_all(paths.actions_dir()).unwrap();
        std::fs::write(
            paths.actions_dir().join("act.yml"),
            "name: act\nkind: youtrack\nnative: {}\nschedule:\n  kind: manual\nconcurrency:\n  max_parallel: 7\n",
        )
        .unwrap();
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        state::global_pause_set(&pool, Some("rebooting"))
            .await
            .unwrap();

        let dispatch = Arc::new(Dispatcher {
            paths: paths.clone(),
            pool: pool.clone(),
            snippet: Arc::new(SnippetRunner::new(None)),
            claude: Arc::new(ClaudeInvoker::new(None)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            keygate: KeyGate::new(),
            gather: GatherGate::new(),
            runs: Arc::new(RunRegistry::new()),
            limiter: Limiter::new(1),
            concurrency: Arc::new(RwLock::new(ConcurrencyConfig::default())),
            pause: crate::pause::PauseGate::new(pool.clone()),
            readiness: crate::readiness::ReadinessGate::always(),
            last_fire: Mutex::new(HashMap::new()),
        });

        scheduler_tick(&dispatch).await.unwrap();
        assert_eq!(
            dispatch.limiter.global_max(),
            1,
            "a paused tick must return before it touches the limiter, let alone dispatch"
        );
        assert!(state::active_runs(&pool).await.unwrap().is_empty());

        state::global_pause_clear(&pool).await.unwrap();
        scheduler_tick(&dispatch).await.unwrap();
        assert_eq!(
            dispatch.limiter.global_max(),
            7,
            "the resumed tick runs the whole body again"
        );
        assert!(
            state::active_runs(&pool).await.unwrap().is_empty(),
            "a manual action is still never auto-dispatched"
        );
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
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
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
            &Limiter::new(1),
            &crate::pause::PauseGate::never(),
            &GatherGate::new(),
        )
        .await
        .expect("native handler caps out cleanly");
    }

    /// DEV-705: `run_action_inner` reports the RUN's exit code instead of a
    /// blanket `OK`, so a standalone `claude-run run` that was refused exits
    /// non-zero with the code `actions/SCHEMA.md` documents. Both refusals are
    /// checked for every kind, native included.
    #[tokio::test]
    async fn run_action_inner_reports_a_refused_run_as_non_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();

        let run = |raw: &'static str| {
            let paths = paths.clone();
            let pool = pool.clone();
            async move {
                let loaded = crate::action::validate_str(raw, "<test>").unwrap();
                run_action_inner(
                    &loaded,
                    &paths,
                    &pool,
                    Arc::new(SnippetRunner::new(None)),
                    Arc::new(ClaudeInvoker::new(None)),
                    "test",
                    None,
                    &KeyGate::new(),
                    &Limiter::new(1),
                    &crate::pause::PauseGate::never(),
                    &GatherGate::new(),
                )
                .await
                .unwrap()
            }
        };

        let capped =
            run("name: yt\nkind: youtrack\nnative: {}\nlimits:\n  max_runs_per_day: 0\n").await;
        assert_eq!(capped, crate::exit::DAILY_CAP, "exit code 4 is documented");

        let broke = run(
            "name: yt\nkind: youtrack\nnative: {}\nlimits:\n  daily_budget_usd: 0.0\n  run_budget_usd: 0.0\n",
        )
        .await;
        assert_eq!(
            broke,
            crate::exit::OVER_BUDGET,
            "a run with nothing left to spend exits non-zero"
        );
    }

    /// CLAUDE-269 acceptance criterion 3: a job whose invocation hits the
    /// authentication failure spends no attempt against
    /// `max_attempts_per_key` and runs no `post`, through the scripted (job.rs)
    /// transient path.
    #[tokio::test]
    async fn a_scripted_job_hitting_auth_failure_spends_no_attempt_and_skips_post() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        let snippet = SnippetRunner::new(None);

        let stub = tmp.path().join("claude-stub.sh");
        crate::testutil::write_stub(
            &stub,
            "#!/bin/sh\n\
             printf '%s\\n' '{\"type\":\"assistant\",\"error\":\"authentication_failed\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"OAuth session expired\"}]}}'\n\
             printf '%s\\n' '{\"type\":\"result\",\"is_error\":true,\"error\":\"authentication_failed\"}'\n\
             exit 1\n",
        );
        // The post step writes a marker file only if it ran, so its ABSENCE
        // proves post was skipped (the transient path, not merely a passing
        // assertion on the return value).
        let post_marker = tmp.path().join("post-ran");
        // `sh`, not `nu`: the `just pre-commit` image has no `nu` (see snippet.rs tests).
        let raw = format!(
            "name: t\ninput:\n  command:\n    run: printf '[{{}}]'\n    shell: sh\nprompt:\n  run: echo hi\n  shell: sh\npost:\n  - run: touch '{}'\n    shell: sh\n",
            post_marker.display()
        );
        let loaded = crate::action::validate_str(&raw, "<test>").unwrap();
        let claude = ClaudeInvoker::new(Some(stub.to_str().unwrap()));

        let exit_code = run_action_inner(
            &loaded,
            &paths,
            &pool,
            Arc::new(snippet),
            Arc::new(claude),
            "test",
            None,
            &KeyGate::new(),
            &Limiter::new(1),
            &crate::pause::PauseGate::never(),
            &GatherGate::new(),
        )
        .await
        .unwrap();

        assert_eq!(exit_code, crate::exit::OK, "the run itself is not refused");
        assert_eq!(
            state::attempts_get(&pool, "t", "job-0").await.unwrap(),
            0,
            "an authentication failure must not spend an attempt"
        );
        assert!(
            !post_marker.exists(),
            "post must not run on the transient auth-failure path"
        );
    }

    // CLAUDE-40: an out-of-band `rm` of an action file must be reconciled by the
    // next scan, which prunes the stale index row while leaving run history.
    #[tokio::test]
    async fn scan_actions_prunes_vanished_files() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
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
        let runs: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM runs WHERE action_name = ?",
        ))
        .bind("doomed")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(runs, 1, "run history is retained across a prune");
    }

    // CLAUDE-61 / CLAUDE-220: an on-disk YAML claiming EITHER reserved synthetic
    // name must not be indexed (it would collide with the synthetic action the
    // web layer injects, and become editable/schedulable). Both names come from
    // the one shared predicate, so this loops over the whole reserved set.
    #[tokio::test]
    async fn scan_actions_refuses_every_reserved_synthetic_name() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        let actions_dir = tmp.path().join("actions");
        std::fs::create_dir_all(&actions_dir).unwrap();
        assert!(
            crate::web::SYNTHETIC_ACTIONS.contains(&"webhook-forgejo")
                && crate::web::SYNTHETIC_ACTIONS.contains(&"webhook-forgejo-comment"),
            "both listeners are reserved"
        );
        for name in crate::web::SYNTHETIC_ACTIONS {
            // A perfectly valid action that simply claims the reserved name.
            let file = actions_dir.join(format!("{name}.yml"));
            std::fs::write(
                &file,
                format!("name: {name}\ninput:\n  command: print '[]'\nprompt: print 'hi'\nenabled: false\n"),
            )
            .unwrap();
            // And an unparseable file whose STEM claims it, covering the other
            // branch of the scan.
            let broken = actions_dir.join(format!("{name}.yaml"));
            std::fs::write(&broken, ":\n  not: [valid").unwrap();

            scan_actions(&pool, &actions_dir).await.unwrap();
            assert!(
                !state::action_index_list(&pool)
                    .await
                    .unwrap()
                    .iter()
                    .any(|r| &r.name == name),
                "a YAML using the reserved synthetic name `{name}` must not be indexed"
            );
            std::fs::remove_file(&file).unwrap();
            std::fs::remove_file(&broken).unwrap();
        }
    }

    // CLAUDE-164: unit-level collision detection over synthetic identities.
    fn scanned(name: &str, source: Option<&str>) -> ScannedAction {
        ScannedAction {
            name: name.to_string(),
            file_path: format!("/actions/{name}.yml"),
            source_key: source.map(str::to_string),
        }
    }

    #[test]
    fn detect_collisions_flags_case_insensitive_name_collision() {
        // Two enabled actions whose names differ only by case, even with
        // unrelated sources, are flagged and both are named as members.
        let cols = detect_collisions(&[
            scanned("youtrack", Some("youtrack\u{1f}\u{1f}q-a")),
            scanned("YouTrack", Some("youtrack\u{1f}\u{1f}q-b")),
        ]);
        assert_eq!(cols.len(), 1, "one name-case collision");
        let mut members = cols[0].members.clone();
        members.sort();
        assert_eq!(members, vec!["YouTrack", "youtrack"]);
        assert!(
            cols[0].message.contains("youtrack.yml") && cols[0].message.contains("YouTrack.yml"),
            "message names both files: {}",
            cols[0].message
        );
    }

    #[test]
    fn detect_collisions_flags_same_native_source_with_unrelated_names() {
        // Same kind + source key, names unrelated: still redundant work.
        let cols = detect_collisions(&[
            scanned(
                "primary",
                Some("youtrack\u{1f}\u{1f}-Resolved AI Agent: Queued"),
            ),
            scanned(
                "mirror",
                Some("youtrack\u{1f}\u{1f}-Resolved AI Agent: Queued"),
            ),
        ]);
        assert_eq!(cols.len(), 1, "one same-source collision");
        let mut members = cols[0].members.clone();
        members.sort();
        assert_eq!(members, vec!["mirror", "primary"]);
        assert!(cols[0].message.contains("poll the same source"));
    }

    #[test]
    fn detect_collisions_silent_for_single_distinct_and_scripted() {
        // A lone action never collides.
        assert!(detect_collisions(&[scanned("solo", Some("youtrack\u{1f}\u{1f}q"))]).is_empty());
        // Two genuinely different queries do not collide.
        assert!(detect_collisions(&[
            scanned("a", Some("youtrack\u{1f}\u{1f}q-a")),
            scanned("b", Some("youtrack\u{1f}\u{1f}q-b")),
        ])
        .is_empty());
        // Two scripted actions (no source key) with distinct names do not
        // collide: their nu-snippet sources are opaque and uncomparable.
        assert!(detect_collisions(&[scanned("x", None), scanned("y", None)]).is_empty());
    }

    // CLAUDE-164: a full scan of two case-colliding, same-query youtrack actions
    // must WARN via a persisted run_events notice on BOTH actions, and must NOT
    // auto-disable either (both index rows survive, still enabled).
    #[tokio::test]
    async fn scan_actions_warns_on_duplicate_source_and_keeps_both() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        let actions_dir = tmp.path().join("actions");
        std::fs::create_dir_all(&actions_dir).unwrap();
        let body = |name: &str| {
            format!(
                "name: {name}\nkind: youtrack\nnative:\n  youtrack:\n    query: \"-Resolved AI Agent: Queued\"\n"
            )
        };
        std::fs::write(actions_dir.join("YouTrack.yml"), body("YouTrack")).unwrap();
        std::fs::write(actions_dir.join("youtrack.yml"), body("youtrack")).unwrap();

        scan_actions(&pool, &actions_dir).await.unwrap();

        // Both actions carry a warn-level config-collision notice.
        for name in ["YouTrack", "youtrack"] {
            let events = state::run_events_for_action(&pool, name, 50).await.unwrap();
            assert!(
                events
                    .iter()
                    .any(|e| e.level == "warn" && e.context.as_deref() == Some("config-collision")),
                "action {name} has a config-collision warning: {events:?}"
            );
        }

        // No auto-disable: both index rows survive and stay enabled.
        let rows = state::action_index_list(&pool).await.unwrap();
        for name in ["YouTrack", "youtrack"] {
            let row = rows.iter().find(|r| r.name == name).expect("row present");
            assert!(row.enabled, "action {name} is NOT auto-disabled");
        }
    }

    // CLAUDE-164: a single youtrack action, and a disabled colliding twin, must
    // NOT produce any config-collision notice.
    #[tokio::test]
    async fn scan_actions_silent_for_single_and_disabled_twin() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_with_runtime(tmp.path());
        let pool = crate::db::open_sqlite(&paths.state_db()).await.unwrap();
        let actions_dir = tmp.path().join("actions");
        std::fs::create_dir_all(&actions_dir).unwrap();
        // Enabled action plus a DISABLED case-colliding twin sharing its query.
        std::fs::write(
            actions_dir.join("YouTrack.yml"),
            "name: YouTrack\nkind: youtrack\nnative:\n  youtrack:\n    query: \"-Resolved AI Agent: Queued\"\n",
        )
        .unwrap();
        std::fs::write(
            actions_dir.join("youtrack.yml"),
            "name: youtrack\nkind: youtrack\nnative:\n  youtrack:\n    query: \"-Resolved AI Agent: Queued\"\nenabled: false\n",
        )
        .unwrap();

        scan_actions(&pool, &actions_dir).await.unwrap();

        for name in ["YouTrack", "youtrack"] {
            let events = state::run_events_for_action(&pool, name, 50).await.unwrap();
            assert!(
                !events
                    .iter()
                    .any(|e| e.context.as_deref() == Some("config-collision")),
                "no collision notice when the twin is disabled: {name} {events:?}"
            );
        }
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

    // CLAUDE-162: the global-cap resolver. An explicit `max_parallel_global > 0`
    // wins verbatim; `0` (auto) resolves to the maximum enabled-action
    // `max_parallel`; disabled / parse-error actions are excluded; an empty set
    // floors to 1 so triggers still run.
    #[test]
    fn resolve_global_cap_explicit_and_auto() {
        fn row(name: &str, enabled: bool, parse_ok: bool, mp: i64) -> ActionIndexRow {
            ActionIndexRow {
                name: name.to_string(),
                file_path: format!("/x/{name}.yml"),
                yaml_sha256: String::new(),
                enabled,
                schedule_kind: Some("interval".to_string()),
                schedule_spec: Some("60s".to_string()),
                last_seen_at: "2026-01-01T00:00:00+00:00".to_string(),
                parse_error: (!parse_ok).then(|| "bad".to_string()),
                link_template: None,
                max_parallel: mp,
            }
        }
        let rows = vec![
            row("a", true, true, 3),
            row("b", true, true, 5),
            // Disabled and parse-error rows do not raise the auto cap.
            row("c", false, true, 9),
            row("d", true, false, 9),
        ];

        // Explicit value wins regardless of the actions.
        let explicit = ConcurrencyConfig {
            max_parallel_global: 2,
            regather_interval_secs: 60,
        };
        assert_eq!(resolve_global_cap(&explicit, &rows), 2);

        // Auto (0) -> max enabled+parseable max_parallel (5, not 9).
        let auto = ConcurrencyConfig {
            max_parallel_global: 0,
            regather_interval_secs: 60,
        };
        assert_eq!(resolve_global_cap(&auto, &rows), 5);

        // Auto with no eligible actions floors to 1.
        assert_eq!(resolve_global_cap(&auto, &[]), 1);
        assert_eq!(
            resolve_global_cap(&auto, &[row("c", false, true, 9)]),
            1,
            "a lone disabled action still yields at least one permit"
        );
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
