//! `claude-run` CLI entry point.
//!
//! Phase 1 subcommands: `run`, `migrate`.
//! Phase 2 subcommands: `serve`, `stop`, `restart`, `trigger`, `status`,
//! `pause`, `resume`, `action {list,show}`. CLI commands that mutate runtime state (trigger,
//! pause, resume) delegate to the daemon when its socket is reachable, else
//! operate on the DB directly so the binary is useful both standalone and as
//! a daemon control plane.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::json;
use tracing_subscriber::{fmt, prelude::*, reload, EnvFilter, Registry};

use claude_run::{
    action, api, claude as claude_mod, client::DaemonClient, config as cfg, daemon, db, exit,
    jsonfmt, lock, migrate, paint, secrets, snippet, state, update,
};

/// `--color` choices. Maps to both `anstream` (our own output) and clap's
/// help/error rendering so they color together (CLAUDE-60).
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum ColorWhen {
    /// Color only when stdout/stderr is a TTY; honors `NO_COLOR`.
    Auto,
    /// Always color, even through a pipe.
    Always,
    /// Never color (script-safe).
    Never,
}

/// clap help/usage palette: headers bold yellow, literals (subcommand and flag
/// names) bold green, placeholders cyan. Shades may be tuned in review; the
/// structure stands (CLAUDE-60).
fn help_styles() -> clap::builder::Styles {
    use clap::builder::styling::AnsiColor;
    clap::builder::Styles::styled()
        .header(AnsiColor::Yellow.on_default().bold())
        .usage(AnsiColor::Yellow.on_default().bold())
        .literal(AnsiColor::Green.on_default().bold())
        .placeholder(AnsiColor::Cyan.on_default())
}

#[derive(Parser)]
#[command(
    name = "claude-run",
    version = claude_run::VERSION,
    about = "YAML-driven Claude Code runner",
    long_about = "Run actions defined in $CONFIG_DIR/actions/*.yml. Each action's input.command \
                  emits a JSON array; one Claude invocation per job. See actions/SCHEMA.md.",
    styles = help_styles()
)]
struct Cli {
    /// Override the actions directory (default: $CONFIG_DIR/actions/).
    #[arg(long, env = "CLAUDE_RUN_ACTIONS_DIR", global = true)]
    actions_dir: Option<String>,

    /// When to color output: auto (TTY-detect, honors NO_COLOR), always, never.
    #[arg(long, value_enum, default_value_t = ColorWhen::Auto, global = true)]
    color: ColorWhen,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run an action once. Holds the per-action lock; conflicts with a
    /// daemon-driven run.
    Run {
        /// Action name (resolved against the actions dir) or path to a YAML file.
        action: String,
    },
    /// Start the daemon. Owns the scheduler and the unix-socket API.
    Serve {
        /// Detach (fork twice, write pidfile, redirect stdio to daemon.log).
        #[arg(long)]
        detach: bool,
    },
    /// Stop a running daemon (SIGTERM, graceful). Idempotent: a no-op exit 0
    /// when no daemon is running. Never force-kills; if the daemon does not
    /// exit within the grace window, reports and exits non-zero.
    Stop,
    /// Restart the daemon: stop the running one, then `serve` again.
    Restart {
        /// Detach the restarted daemon (mirrors `serve --detach`).
        #[arg(long)]
        detach: bool,
    },
    /// Ask the daemon to run an action immediately (out of cycle).
    Trigger { action: String },
    /// Cancel an in-flight run by id. Requires a running daemon (the run lives
    /// inside it). Idempotent: a clear no-op when the run is not in flight.
    Cancel { run_id: i64 },
    /// Pause global / per-action scheduling. With no `action`, pauses globally.
    Pause {
        action: Option<String>,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Resume global / per-action scheduling.
    Resume { action: Option<String> },
    /// Print daemon status as JSON.
    Status,
    /// Action management subcommands.
    #[command(subcommand)]
    Action(ActionCmd),
    /// Manage encrypted secrets (the web auth token, action `token_ref`s).
    /// Operates directly on the local state DB and the age key file
    /// (`CLAUDE_RUN_SECRETS_KEY_FILE`); no daemon required.
    #[command(subcommand)]
    Secret(SecretCmd),
    /// Import legacy `~/.local/share/claude-run/state/*.json` files into SQLite.
    Migrate {
        #[arg(long)]
        from: Option<String>,
    },
    /// Show the running version, check the registry for a newer build, or
    /// self-update in place. With no subcommand, behaves like `show`.
    Version {
        #[command(subcommand)]
        cmd: Option<VersionCmd>,
    },
}

#[derive(Subcommand)]
enum VersionCmd {
    /// Print the build-stamped version banner (identical to `--version`).
    Show,
    /// Report the current version, the newest available `vX.Y.Z` in the
    /// registry, and whether an update is available. Exits non-zero only on a
    /// registry/transport error.
    Check {
        /// Update channel: `release` (newest semver) or `latest` (moving main
        /// build). Defaults to `[update].train` in config.toml (`release`).
        #[arg(long, value_enum)]
        train: Option<update::UpdateTrain>,
        /// On a 401/403, use the `fj` token for the registry host without an
        /// interactive prompt.
        #[arg(long)]
        use_fj_token: bool,
    },
    /// Download the newest build (or `--version <tag>`), verify its SHA-256
    /// against the published sidecar, and atomically replace this binary.
    Update {
        /// Pin a specific release tag (e.g. v0.2.0) instead of the newest.
        /// Independent of `--train`.
        #[arg(long = "version")]
        version: Option<String>,
        /// Update channel: `release` (newest semver) or `latest` (moving main
        /// build). Defaults to `[update].train` in config.toml (`release`).
        #[arg(long, value_enum)]
        train: Option<update::UpdateTrain>,
        /// On a 401/403, use the `fj` token for the registry host without an
        /// interactive prompt.
        #[arg(long)]
        use_fj_token: bool,
        /// Skip the confirmation prompt before swapping the binary.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum ActionCmd {
    /// List actions known to the daemon (or scanned directly if no daemon).
    List,
    /// Show one action's metadata.
    Show { name: String },
    /// Delete an action: remove its YAML file, index row, and control state
    /// (paused/counters/ledger). Run history is kept. Refuses with
    /// `exit::CONFLICT` when a run for the action is in flight.
    Delete { name: String },
}

#[derive(Subcommand)]
enum SecretCmd {
    /// Create or replace a secret. The plaintext value never lands in argv (shell
    /// history, `ps`). When run interactively the command prompts twice with input
    /// hidden (re-enter to confirm). When stdin is piped or redirected it reads the
    /// value from stdin and strips a single trailing newline, e.g.
    /// `printf %s "$TOKEN" | claude-run secret set web-ui-token`.
    Set { name: String },
    /// List secret names and timestamps as JSON. Values are never printed.
    List,
    /// Delete a secret. Exits `CONFIG_ERROR` when no secret has that name.
    Delete { name: String },
}

fn main() -> ExitCode {
    let log_reload = init_tracing();

    // Resolve the color choice BEFORE clap renders any help/error: clap
    // colorizes its own output during parse, before we could read the parsed
    // `--color` flag, so pre-scan argv for it (honoring NO_COLOR via anstream's
    // auto detection). The flag is still defined on `Cli` for help/validation;
    // this pre-scan only feeds clap's renderer and our global anstream choice.
    let raw_args: Vec<String> = std::env::args().collect();
    let color = resolve_color_choice(&raw_args);
    anstream_choice(color).write_global();

    let cli = parse_cli(color);

    // daemonize MUST happen before any tokio runtime exists. forking after
    // the runtime is constructed produces a child with corrupt scheduler
    // state. Resolve paths and do the fork in this sync context, then build
    // the runtime in the child.
    let maybe_detach = matches!(&cli.cmd, Cmd::Serve { detach: true });
    if maybe_detach {
        if let Err(e) = detach_now() {
            paint::error(&format!("detach failed: {e}"));
            return ExitCode::from(exit::UNEXPECTED as u8);
        }
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            paint::error(&format!("tokio runtime build failed: {e}"));
            return ExitCode::from(exit::UNEXPECTED as u8);
        }
    };
    rt.block_on(async move {
        match real_main(cli, log_reload).await {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                tracing::error!(error = %e, "fatal");
                ExitCode::from(exit::UNEXPECTED as u8)
            }
        }
    })
}

/// Parse the CLI with clap's help/error coloring forced to match `color`.
/// Mirrors `Cli::parse()` but injects `.color(...)` (the derive can only set a
/// static color), so `--color always|never` also governs help and usage errors.
fn parse_cli(color: ColorWhen) -> Cli {
    use clap::{CommandFactory, FromArgMatches};
    let mut cmd = Cli::command().color(clap_choice(color));
    let matches = cmd
        .try_get_matches_from_mut(std::env::args_os())
        .unwrap_or_else(|e| e.exit());
    Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit())
}

/// Best-effort scan of argv for `--color <v>` / `--color=<v>`, used only to seed
/// the renderer before full parse. Invalid values fall back to `Auto` here and
/// surface as a proper clap error during [`parse_cli`].
fn resolve_color_choice(args: &[String]) -> ColorWhen {
    let mut choice = ColorWhen::Auto;
    let mut iter = args.iter().skip(1); // skip the program name
    while let Some(arg) = iter.next() {
        let value = if let Some(v) = arg.strip_prefix("--color=") {
            Some(v.to_string())
        } else if arg == "--color" {
            iter.next().cloned()
        } else {
            None
        };
        if let Some(v) = value {
            if let Ok(c) = <ColorWhen as ValueEnum>::from_str(&v, true) {
                choice = c;
            }
        }
    }
    choice
}

/// Map to anstream's global choice for our own output. `Auto` honors `NO_COLOR`,
/// `CLICOLOR_FORCE`, and TTY detection.
fn anstream_choice(color: ColorWhen) -> anstream::ColorChoice {
    match color {
        ColorWhen::Auto => anstream::ColorChoice::Auto,
        ColorWhen::Always => anstream::ColorChoice::Always,
        ColorWhen::Never => anstream::ColorChoice::Never,
    }
}

/// Map to clap's color choice for help/usage/error rendering.
fn clap_choice(color: ColorWhen) -> clap::ColorChoice {
    match color {
        ColorWhen::Auto => clap::ColorChoice::Auto,
        ColorWhen::Always => clap::ColorChoice::Always,
        ColorWhen::Never => clap::ColorChoice::Never,
    }
}

fn detach_now() -> claude_run::error::Result<()> {
    let paths = cfg::Paths::resolve()?;
    let log = paths.state_dir.join("logs").join("daemon.log");
    if let Some(parent) = log.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let stdout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)?;
    let stderr = stdout.try_clone()?;
    // Don't let daemonize manage the pidfile - it acquires its own exclusive
    // lock on it, which would race with our explicit `pidfile_lock()` in
    // `Daemon::run`. The runtime pidfile lock is the single source of truth.
    daemonize::Daemonize::new()
        .working_directory("/")
        .stdout(stdout)
        .stderr(stderr)
        .start()
        .map_err(|e| anyhow::anyhow!("daemonize: {e}"))?;
    Ok(())
}

/// Build the tracing subscriber and return a reload handle for its `EnvFilter`.
/// The initial filter is unchanged from before CLAUDE-62 (`CLAUDE_RUN_LOG` if
/// set, else the built-in default), so every line emitted before config load
/// logs exactly as it did. Wrapping the filter in a `reload::Layer` lets
/// `apply_config_log_filter` swap in `[daemon].log_filter` once the config is
/// loaded, without a second (illegal) `set_global_default`.
fn init_tracing() -> reload::Handle<EnvFilter, Registry> {
    let filter = EnvFilter::try_from_env("CLAUDE_RUN_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,claude_run=info,sqlx=warn"));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt::layer().with_target(false).json())
        .init();
    reload_handle
}

/// CLAUDE-62: apply `[daemon].log_filter` after config load, honoring the
/// `CLAUDE_RUN_LOG` > file > default precedence (see [`cfg::log_filter_choice`]).
/// An unparseable directive logs a warning and keeps the active filter rather
/// than aborting the daemon.
fn apply_config_log_filter(
    handle: &reload::Handle<EnvFilter, Registry>,
    file_filter: Option<&str>,
) {
    let env_set = std::env::var_os("CLAUDE_RUN_LOG").is_some();
    match cfg::log_filter_choice(env_set, file_filter) {
        cfg::LogFilterChoice::EnvWins => {
            tracing::debug!(
                "CLAUDE_RUN_LOG set; [daemon].log_filter ignored (env takes precedence)"
            );
        }
        cfg::LogFilterChoice::KeepDefault => {}
        cfg::LogFilterChoice::ApplyFile(f) => match EnvFilter::try_new(&f) {
            Ok(filter) => match handle.reload(filter) {
                Ok(()) => tracing::info!(filter = %f, "log filter applied from config.toml"),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to swap log filter; keeping current filter")
                }
            },
            Err(e) => tracing::warn!(
                value = %f,
                error = %e,
                "invalid [daemon].log_filter; keeping current filter"
            ),
        },
    }
}

async fn real_main(
    cli: Cli,
    log_reload: reload::Handle<EnvFilter, Registry>,
) -> claude_run::error::Result<i32> {
    let paths = cfg::Paths::resolve()?;
    let (on_disk, config_source) = cfg::OnDiskConfig::load(&paths)?;
    apply_config_log_filter(&log_reload, on_disk.daemon.log_filter.as_deref());

    let actions_dir = cli
        .actions_dir
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.actions_dir());

    match cli.cmd {
        Cmd::Run { action } => run_action(&action, &actions_dir, &paths, &on_disk).await,
        Cmd::Serve { detach } => serve(detach, paths, on_disk, config_source).await,
        Cmd::Stop => stop(&paths).await,
        Cmd::Restart { detach } => restart(detach, cli.actions_dir.as_deref(), &paths).await,
        Cmd::Trigger { action } => trigger(&action, &paths).await,
        Cmd::Cancel { run_id } => cancel_run(run_id, &paths).await,
        Cmd::Pause { action, reason } => pause(action, reason, &paths).await,
        Cmd::Resume { action } => resume(action, &paths).await,
        Cmd::Status => status(&paths).await,
        Cmd::Action(c) => action_cmd(c, &paths, &actions_dir).await,
        Cmd::Secret(c) => secret_cmd(c, &paths).await,
        Cmd::Migrate { from } => {
            let pool = db::open(&paths.state_db()).await?;
            let legacy = from.map(PathBuf::from).unwrap_or_else(|| {
                dirs::home_dir()
                    .unwrap()
                    .join(".local/share/claude-run/state")
            });
            let report = migrate::import_legacy_state(&pool, &legacy).await?;
            tracing::info!(
                actions = report.actions,
                attempts = report.attempts,
                daily_rows = report.daily_rows,
                "legacy state imported"
            );
            Ok(exit::OK)
        }
        Cmd::Version { cmd } => version_cmd(cmd, &paths, &on_disk).await,
    }
}

async fn version_cmd(
    cmd: Option<VersionCmd>,
    paths: &cfg::Paths,
    on_disk: &cfg::OnDiskConfig,
) -> claude_run::error::Result<i32> {
    match cmd.unwrap_or(VersionCmd::Show) {
        VersionCmd::Show => {
            println!("{}", claude_run::VERSION);
            Ok(exit::OK)
        }
        VersionCmd::Check {
            train,
            use_fj_token,
        } => {
            let train = resolve_train(train, &on_disk.update.train)?;
            let token = resolve_update_token(paths, on_disk).await?;
            update::check(train, use_fj_token, token).await
        }
        VersionCmd::Update {
            version,
            train,
            use_fj_token,
            yes,
        } => {
            let train = resolve_train(train, &on_disk.update.train)?;
            let token = resolve_update_token(paths, on_disk).await?;
            update::update(train, version, use_fj_token, token, yes).await
        }
    }
}

/// CLI `--train` flag overrides the `[update].train` config default.
fn resolve_train(
    flag: Option<update::UpdateTrain>,
    config_train: &str,
) -> claude_run::error::Result<update::UpdateTrain> {
    match flag {
        Some(t) => Ok(t),
        None => update::UpdateTrain::from_config_str(config_train),
    }
}

/// Resolve the registry token from `[update].token_ref` (CLAUDE-55). `None` when
/// no `token_ref` is configured (the anonymous-first path). When set, the secret
/// is decrypted from the store; a missing secret or key file surfaces an error.
async fn resolve_update_token(
    paths: &cfg::Paths,
    on_disk: &cfg::OnDiskConfig,
) -> claude_run::error::Result<Option<String>> {
    let Some(name) = on_disk.update.token_ref.as_deref() else {
        return Ok(None);
    };
    let pool = db::open(&paths.state_db()).await?;
    let key = secrets::SecretsKey::load()?;
    let token = secrets::resolve_secret(&pool, &key, name).await?;
    Ok(Some(token))
}

async fn run_action(
    arg: &str,
    actions_dir: &std::path::Path,
    paths: &cfg::Paths,
    on_disk: &cfg::OnDiskConfig,
) -> claude_run::error::Result<i32> {
    let loaded = action::load(arg, actions_dir)?;
    tracing::info!(action = %loaded.action.name, path = %loaded.path.display(), "starting run");

    let pool = db::open(&paths.state_db()).await?;

    // Pause checks (Phase 2 contract)
    if state::global_pause_status(&pool).await?.is_some() {
        tracing::info!("globally paused");
        return Ok(exit::PAUSED);
    }
    if state::action_pause_status(&pool, &loaded.action.name)
        .await?
        .is_some()
    {
        tracing::info!(action = %loaded.action.name, "action paused");
        return Ok(exit::PAUSED);
    }

    // Cross-process action lock (Phase 2)
    let lock_dir = paths.runtime_dir.join("locks");
    let _lock = match lock::ActionLock::try_acquire(&lock_dir, &loaded.action.name)? {
        Some(l) => l,
        None => {
            tracing::info!(action = %loaded.action.name, "another runner holds the lock");
            return Ok(exit::LOCKED);
        }
    };

    let snippet = Arc::new(snippet::SnippetRunner::new(
        on_disk.daemon.nu_binary.as_deref(),
    ));
    let claude = Arc::new(claude_mod::ClaudeInvoker::new(
        on_disk.daemon.claude_binary.as_deref(),
    ));
    // CLAUDE-28: a standalone `run` is a single process with no daemon, so it
    // gets its own ephemeral key gate; the per-action lock already prevents it
    // from overlapping a daemon-driven run of the same action.
    let keygate = claude_run::concurrency::KeyGate::new();
    daemon::run_action_inner(
        &loaded, paths, &pool, snippet, claude, "cli", None, &keygate,
    )
    .await?;
    Ok(exit::OK)
}

async fn serve(
    _detach: bool, // already actioned in main() before the tokio runtime started
    paths: cfg::Paths,
    on_disk: cfg::OnDiskConfig,
    config_source: cfg::ConfigSource,
) -> claude_run::error::Result<i32> {
    // CLAUDE-57: one startup line recording which config file backed this run
    // (or that defaults silently won), so `journalctl` alone answers why the
    // web UI bound where it did.
    config_source.log();
    // CLAUDE-87: flag any webhook source name that needs URL escaping (likely a
    // typo) before the listener binds, so the warning sits next to the config line.
    on_disk.webhook.warn_unusual_source_names();
    let d = daemon::Daemon { paths, on_disk };
    d.run().await?;
    Ok(exit::OK)
}

/// Grace window the daemon gets to exit after SIGTERM before `stop` gives up.
const STOP_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

enum StopOutcome {
    NotRunning,
    Stopped,
    /// Still alive after the grace window; carries the PID for the message.
    Timeout(i32),
}

/// Remove a leftover socket if the daemon exited without cleaning up (e.g. an
/// older build that was killed before SIGTERM was handled gracefully).
fn cleanup_stale_socket(paths: &cfg::Paths) {
    let sock = daemon::daemon_socket_path(paths);
    if sock.exists() {
        let _ = std::fs::remove_file(&sock);
    }
}

/// Send SIGTERM to the daemon (if any) and poll for it to exit. Never escalates
/// to SIGKILL - per the repo safety rule, a stuck daemon is left for the
/// operator to deal with.
async fn stop_daemon(paths: &cfg::Paths) -> claude_run::error::Result<StopOutcome> {
    let pid = match daemon::read_pidfile(paths) {
        Some(p) if daemon::process_alive(p) => p,
        _ => {
            cleanup_stale_socket(paths);
            return Ok(StopOutcome::NotRunning);
        }
    };

    daemon::send_sigterm(pid)?;
    tracing::info!(pid, "sent SIGTERM to daemon");

    let deadline = std::time::Instant::now() + STOP_GRACE;
    while std::time::Instant::now() < deadline {
        if !daemon::process_alive(pid) {
            cleanup_stale_socket(paths);
            return Ok(StopOutcome::Stopped);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    if daemon::process_alive(pid) {
        Ok(StopOutcome::Timeout(pid))
    } else {
        cleanup_stale_socket(paths);
        Ok(StopOutcome::Stopped)
    }
}

async fn stop(paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    match stop_daemon(paths).await? {
        StopOutcome::NotRunning => {
            paint::notice("daemon not running");
            Ok(exit::OK)
        }
        StopOutcome::Stopped => {
            paint::success("daemon stopped");
            Ok(exit::OK)
        }
        StopOutcome::Timeout(pid) => {
            paint::error(&format!(
                "daemon (pid {pid}) did not exit within {}s; not force-killing - \
                 stop it manually if it is truly stuck",
                STOP_GRACE.as_secs()
            ));
            Ok(exit::UNEXPECTED)
        }
    }
}

async fn restart(
    detach: bool,
    actions_dir_arg: Option<&str>,
    paths: &cfg::Paths,
) -> claude_run::error::Result<i32> {
    match stop_daemon(paths).await? {
        StopOutcome::Timeout(pid) => {
            paint::error(&format!(
                "daemon (pid {pid}) did not exit within the grace window; not restarting"
            ));
            return Ok(exit::UNEXPECTED);
        }
        StopOutcome::NotRunning => tracing::info!("no daemon running; starting fresh"),
        StopOutcome::Stopped => tracing::info!("daemon stopped; starting again"),
    }

    // Re-exec `claude-run serve [--detach]`. exec replaces this process image,
    // so the detach-must-happen-before-the-tokio-runtime ordering enforced in
    // main() is handled cleanly by the freshly-started child.
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(exe);
    if let Some(dir) = actions_dir_arg {
        cmd.arg("--actions-dir").arg(dir);
    }
    cmd.arg("serve");
    if detach {
        cmd.arg("--detach");
    }
    let err = cmd.exec();
    Err(anyhow::anyhow!("failed to exec `serve` on restart: {err}").into())
}

async fn trigger(action: &str, paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if !DaemonClient::is_reachable(&sock) {
        return Err(anyhow::anyhow!(
            "daemon not reachable at {}; start it with `claude-run serve --detach`",
            sock.display()
        )
        .into());
    }
    let c = DaemonClient::new(&sock);
    let v: serde_json::Value = c
        .post(&format!("/actions/{}/trigger", action), None)
        .await?;
    jsonfmt::print_json(&v)?;
    Ok(exit::OK)
}

async fn cancel_run(run_id: i64, paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if !DaemonClient::is_reachable(&sock) {
        return Err(anyhow::anyhow!(
            "daemon not reachable at {}; an in-flight run only exists inside the daemon, \
             so cancellation requires it to be running",
            sock.display()
        )
        .into());
    }
    let c = DaemonClient::new(&sock);
    let v: serde_json::Value = c.post(&format!("/runs/{}/cancel", run_id), None).await?;
    jsonfmt::print_json(&v)?;
    Ok(exit::OK)
}

async fn pause(
    action: Option<String>,
    reason: Option<String>,
    paths: &cfg::Paths,
) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    let body = reason.as_ref().map(|r| json!({"reason": r}));

    if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        let path = match &action {
            Some(name) => format!("/actions/{}/pause", name),
            None => "/pause".to_string(),
        };
        let v: serde_json::Value = c.post(&path, body.as_ref()).await?;
        jsonfmt::print_json(&v)?;
    } else {
        let pool = db::open(&paths.state_db()).await?;
        match action {
            Some(name) => state::action_pause_set(&pool, &name, reason.as_deref()).await?,
            None => state::global_pause_set(&pool, reason.as_deref()).await?,
        }
        paint::success("ok");
    }
    Ok(exit::OK)
}

async fn resume(action: Option<String>, paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        let path = match &action {
            Some(name) => format!("/actions/{}/resume", name),
            None => "/resume".to_string(),
        };
        let v: serde_json::Value = c.post(&path, None).await?;
        jsonfmt::print_json(&v)?;
    } else {
        let pool = db::open(&paths.state_db()).await?;
        match action {
            Some(name) => state::action_pause_clear(&pool, &name).await?,
            None => state::global_pause_clear(&pool).await?,
        }
        paint::success("ok");
    }
    Ok(exit::OK)
}

async fn status(paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        let v: serde_json::Value = c.get("/status").await?;
        jsonfmt::print_json(&v)?;
        return Ok(exit::OK);
    }
    let pool = db::open(&paths.state_db()).await?;
    let global = state::global_pause_status(&pool).await?;
    let actions = state::action_index_list(&pool).await?;
    let report = json!({
        "daemon": "not running",
        "socket_expected_at": sock.display().to_string(),
        "global_paused": global.map(|(a, r)| json!({"paused_at": a, "reason": r})),
        "actions": actions,
    });
    jsonfmt::print_json(&report)?;
    Ok(exit::OK)
}

async fn action_cmd(
    cmd: ActionCmd,
    paths: &cfg::Paths,
    actions_dir: &std::path::Path,
) -> claude_run::error::Result<i32> {
    let pool = db::open(&paths.state_db()).await?;
    let sock = daemon::daemon_socket_path(paths);
    let reachable = DaemonClient::is_reachable(&sock);
    if !reachable {
        // Refresh the index ourselves; without a daemon, nobody else does.
        daemon::scan_actions(&pool, actions_dir).await?;
    }
    match cmd {
        ActionCmd::List => {
            let rows = state::action_index_list(&pool).await?;
            jsonfmt::print_json(&rows)?;
        }
        ActionCmd::Show { name } => {
            let rows = state::action_index_list(&pool).await?;
            match rows.into_iter().find(|r| r.name == name) {
                Some(r) => jsonfmt::print_json(&r)?,
                None => {
                    paint::error(&format!("no action `{name}` in index"));
                    return Ok(exit::CONFIG_ERROR);
                }
            }
        }
        ActionCmd::Delete { name } => {
            // CLAUDE-40: delegate to the daemon when it is up (it owns the
            // authoritative in-flight set); otherwise delete directly against
            // the DB + filesystem after the same active-run guard.
            if reachable {
                let c = DaemonClient::new(&sock);
                let (status, body) = c
                    .post_status(&format!("/actions/{name}/delete"), None)
                    .await?;
                return match status {
                    200 => {
                        paint::success(&format!("deleted `{name}`"));
                        Ok(exit::OK)
                    }
                    404 => {
                        paint::error(&format!("no action `{name}` in index"));
                        Ok(exit::CONFIG_ERROR)
                    }
                    409 => {
                        paint::error(&format!(
                            "action `{name}` has a run in flight; cancel or wait before deleting"
                        ));
                        Ok(exit::CONFLICT)
                    }
                    _ => {
                        paint::error(&format!("daemon returned {status}: {body}"));
                        Ok(exit::UNEXPECTED)
                    }
                };
            }
            if state::action_has_active_run(&pool, &name).await? {
                paint::error(&format!(
                    "action `{name}` has a run in flight; cancel or wait before deleting"
                ));
                return Ok(exit::CONFLICT);
            }
            if !api::delete_indexed_action(&pool, &name).await? {
                paint::error(&format!("no action `{name}` in index"));
                return Ok(exit::CONFIG_ERROR);
            }
            paint::success(&format!("deleted `{name}`"));
        }
    }
    Ok(exit::OK)
}

async fn secret_cmd(cmd: SecretCmd, paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let pool = db::open(&paths.state_db()).await?;
    match cmd {
        SecretCmd::Set { name } => {
            // The value never appears in argv (shell history, `ps`). Interactive
            // terminals get a masked double-entry prompt; piped/redirected stdin is
            // read as before. Detect the difference with IsTerminal, the same
            // pattern src/update.rs uses for its confirmation gate.
            use std::io::IsTerminal;
            let owned;
            let value: &str = if std::io::stdin().is_terminal() {
                // TTY path: prompt twice with echo disabled. Double entry catches
                // typos that masked input would otherwise hide (passwd convention).
                paint::notice("input is hidden; you will be asked to re-enter to confirm");
                let first =
                    rpassword::prompt_password(format!("Enter secret value for `{name}`: "))?;
                let second = rpassword::prompt_password("Re-enter to confirm: ")?;
                if first != second {
                    paint::error("entries do not match; nothing stored");
                    return Ok(exit::CONFIG_ERROR);
                }
                owned = first;
                &owned
            } else {
                // Piped path: announce the read on stderr (stdout stays clean for
                // scripts), then read to EOF. Strip a single trailing newline (echo
                // / heredoc add one); multi-line secrets pass through otherwise.
                use std::io::Read;
                paint::notice("reading secret value from stdin");
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                owned = buf
                    .strip_suffix('\n')
                    .map(|s| s.strip_suffix('\r').unwrap_or(s))
                    .unwrap_or(&buf)
                    .to_string();
                &owned
            };
            if value.is_empty() {
                paint::error(&format!("refusing to store an empty secret `{name}`"));
                return Ok(exit::CONFIG_ERROR);
            }
            // Encrypt under the age key (CLAUDE_RUN_SECRETS_KEY_FILE). A missing
            // key file surfaces the same clear error the daemon emits.
            let key = secrets::SecretsKey::load()?;
            let ciphertext = key.encrypt(value)?;
            if state::secret_exists(&pool, &name).await? {
                state::secret_rotate(&pool, &name, &ciphertext).await?;
                paint::success(&format!("rotated secret `{name}`"));
            } else {
                state::secret_create(&pool, &name, &ciphertext).await?;
                paint::success(&format!("created secret `{name}`"));
            }
            // The web-auth gate resolves its token once at daemon startup, so a
            // change to that secret needs a restart to take effect.
            paint::notice(
                "note: if this is the web UI auth token, restart the daemon to apply it \
                 (the auth gate resolves it at startup)",
            );
            Ok(exit::OK)
        }
        SecretCmd::List => {
            let rows = state::secret_list(&pool).await?;
            jsonfmt::print_json(&rows)?;
            Ok(exit::OK)
        }
        SecretCmd::Delete { name } => {
            if state::secret_delete(&pool, &name).await? {
                paint::success(&format!("deleted secret `{name}`"));
                Ok(exit::OK)
            } else {
                paint::error(&format!("no secret named `{name}`"));
                Ok(exit::CONFIG_ERROR)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn color_flag_defaults_to_auto() {
        let cli = Cli::try_parse_from(["claude-run", "status"]).unwrap();
        assert_eq!(cli.color, ColorWhen::Auto);
    }

    #[test]
    fn color_flag_parses_all_three_values() {
        for (arg, expected) in [
            ("auto", ColorWhen::Auto),
            ("always", ColorWhen::Always),
            ("never", ColorWhen::Never),
        ] {
            let cli = Cli::try_parse_from(["claude-run", "--color", arg, "status"]).unwrap();
            assert_eq!(cli.color, expected, "--color {arg}");
            let cli_eq = Cli::try_parse_from(["claude-run", "status", &format!("--color={arg}")])
                .expect("global flag accepted after the subcommand too");
            assert_eq!(cli_eq.color, expected, "--color={arg}");
        }
    }

    #[test]
    fn color_flag_rejects_unknown_value() {
        assert!(Cli::try_parse_from(["claude-run", "--color", "rainbow", "status"]).is_err());
    }

    #[test]
    fn pre_scan_matches_parsed_value() {
        // The argv pre-scan that seeds clap's renderer must agree with the
        // value clap ultimately parses, for all three forms.
        for (args, expected) in [
            (vec!["claude-run", "status"], ColorWhen::Auto),
            (
                vec!["claude-run", "--color", "always", "status"],
                ColorWhen::Always,
            ),
            (
                vec!["claude-run", "status", "--color=never"],
                ColorWhen::Never,
            ),
        ] {
            let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
            assert_eq!(resolve_color_choice(&owned), expected, "{args:?}");
        }
    }
}
