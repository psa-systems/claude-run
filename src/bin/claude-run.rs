//! `claude-run` CLI entry point.
//!
//! Phase 1 subcommands: `run`, `migrate`.
//! Phase 2 subcommands: `serve`, `trigger`, `status`, `pause`, `resume`,
//! `action {list,show}`. CLI commands that mutate runtime state (trigger,
//! pause, resume) delegate to the daemon when its socket is reachable, else
//! operate on the DB directly so the binary is useful both standalone and as
//! a daemon control plane.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde_json::json;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use claude_run::{
    action, claude as claude_mod, client::DaemonClient, config as cfg, daemon, db, exit, lock,
    migrate, snippet, state,
};

#[derive(Parser)]
#[command(
    name = "claude-run",
    version = claude_run::VERSION,
    about = "YAML-driven Claude Code runner",
    long_about = "Run actions defined in $CONFIG_DIR/actions/*.yml. Each action's input.command \
                  emits a JSON array; one Claude invocation per row. See actions/SCHEMA.md."
)]
struct Cli {
    /// Override the actions directory (default: $CONFIG_DIR/actions/).
    #[arg(long, env = "CLAUDE_RUN_ACTIONS_DIR", global = true)]
    actions_dir: Option<String>,

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
    /// Ask the daemon to run an action immediately (out of cycle).
    Trigger { action: String },
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
    /// Import legacy `~/.local/share/claude-run/state/*.json` files into SQLite.
    Migrate {
        #[arg(long)]
        from: Option<String>,
    },
}

#[derive(Subcommand)]
enum ActionCmd {
    /// List actions known to the daemon (or scanned directly if no daemon).
    List,
    /// Show one action's metadata.
    Show { name: String },
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    // daemonize MUST happen before any tokio runtime exists. forking after
    // the runtime is constructed produces a child with corrupt scheduler
    // state. Resolve paths and do the fork in this sync context, then build
    // the runtime in the child.
    let maybe_detach = matches!(&cli.cmd, Cmd::Serve { detach: true });
    if maybe_detach {
        if let Err(e) = detach_now() {
            eprintln!("detach failed: {e}");
            return ExitCode::from(exit::UNEXPECTED as u8);
        }
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio runtime build failed: {e}");
            return ExitCode::from(exit::UNEXPECTED as u8);
        }
    };
    rt.block_on(async move {
        match real_main(cli).await {
            Ok(code) => ExitCode::from(code as u8),
            Err(e) => {
                tracing::error!(error = %e, "fatal");
                ExitCode::from(exit::UNEXPECTED as u8)
            }
        }
    })
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

fn init_tracing() {
    let filter = EnvFilter::try_from_env("CLAUDE_RUN_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,claude_run=info,sqlx=warn"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).json())
        .init();
}

async fn real_main(cli: Cli) -> claude_run::error::Result<i32> {
    let paths = cfg::Paths::resolve()?;
    let on_disk = cfg::OnDiskConfig::load(&paths)?;

    let actions_dir = cli
        .actions_dir
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.actions_dir());

    match cli.cmd {
        Cmd::Run { action } => run_action(&action, &actions_dir, &paths, &on_disk).await,
        Cmd::Serve { detach } => serve(detach, paths, on_disk).await,
        Cmd::Trigger { action } => trigger(&action, &paths).await,
        Cmd::Pause { action, reason } => pause(action, reason, &paths).await,
        Cmd::Resume { action } => resume(action, &paths).await,
        Cmd::Status => status(&paths).await,
        Cmd::Action(c) => action_cmd(c, &paths, &actions_dir).await,
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
    }
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

    let snippet = snippet::SnippetRunner::new(on_disk.daemon.nu_binary.as_deref());
    let claude = claude_mod::ClaudeInvoker::new(on_disk.daemon.claude_binary.as_deref());
    daemon::run_action_inner(&loaded, paths, &pool, &snippet, &claude, "cli").await?;
    Ok(exit::OK)
}

async fn serve(
    _detach: bool, // already actioned in main() before the tokio runtime started
    paths: cfg::Paths,
    on_disk: cfg::OnDiskConfig,
) -> claude_run::error::Result<i32> {
    let d = daemon::Daemon { paths, on_disk };
    d.run().await?;
    Ok(exit::OK)
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
    println!("{}", serde_json::to_string_pretty(&v)?);
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
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        let pool = db::open(&paths.state_db()).await?;
        match action {
            Some(name) => state::action_pause_set(&pool, &name, reason.as_deref()).await?,
            None => state::global_pause_set(&pool, reason.as_deref()).await?,
        }
        println!("ok");
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
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        let pool = db::open(&paths.state_db()).await?;
        match action {
            Some(name) => state::action_pause_clear(&pool, &name).await?,
            None => state::global_pause_clear(&pool).await?,
        }
        println!("ok");
    }
    Ok(exit::OK)
}

async fn status(paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        let v: serde_json::Value = c.get("/status").await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
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
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(exit::OK)
}

async fn action_cmd(
    cmd: ActionCmd,
    paths: &cfg::Paths,
    actions_dir: &std::path::Path,
) -> claude_run::error::Result<i32> {
    let pool = db::open(&paths.state_db()).await?;
    let sock = daemon::daemon_socket_path(paths);
    if !DaemonClient::is_reachable(&sock) {
        // Refresh the index ourselves; without a daemon, nobody else does.
        daemon::scan_actions(&pool, actions_dir).await?;
    }
    match cmd {
        ActionCmd::List => {
            let rows = state::action_index_list(&pool).await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        ActionCmd::Show { name } => {
            let rows = state::action_index_list(&pool).await?;
            match rows.into_iter().find(|r| r.name == name) {
                Some(r) => println!("{}", serde_json::to_string_pretty(&r)?),
                None => {
                    eprintln!("no action `{name}` in index");
                    return Ok(exit::CONFIG_ERROR);
                }
            }
        }
    }
    Ok(exit::OK)
}
