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
    access, action, api, claude as claude_mod, client::DaemonClient, config as cfg, daemon, db,
    exit, import, jsonfmt, lock, mcp, migrate, paint, secrets, settings, snippet, state,
    test_postgres,
};

/// `--color` choices. Maps to both `anstream` (our own output) and clap's
/// help/error rendering so they color together.
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
/// structure stands.
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
    /// Start the daemon. Owns the scheduler and, unless `--no-socket`, the
    /// unix-socket API.
    Serve {
        /// Detach (fork twice, write pidfile, redirect stdio to daemon.log).
        #[arg(long)]
        detach: bool,
        /// Do not bind the unix-socket control API. The CLI subcommands that
        /// need a daemon (`trigger`, `cancel`) then fail, and the TCP web UI
        /// and its `/api` routes are the only control plane. Intended for the
        /// container image, where no CLI operator exists and the socket would
        /// only be an unauthenticated second copy of that control plane.
        #[arg(long)]
        no_socket: bool,
    },
    /// Stop a running daemon (SIGTERM, graceful). Idempotent: a no-op exit 0
    /// when no daemon is running. Never force-kills; if the daemon does not
    /// exit within the grace window, reports and exits non-zero.
    Stop,
    /// Restart the daemon: stop the running one, then `serve` again.
    Restart {
        /// Restart without the unix-socket API (mirrors `serve --no-socket`).
        #[arg(long)]
        no_socket: bool,
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
    /// Check the runner's access: every credential's identity, and whether a
    /// named repo / YouTrack project is workable by the credential that would
    /// actually be used for it. Read-only.
    #[command(subcommand)]
    Access(AccessCmd),
    /// Manage encrypted secrets (the web auth token, action `token_ref`s).
    /// Operates directly on the local state DB and the age key file
    /// (`CLAUDE_RUN_SECRETS_KEY_FILE`); no daemon required.
    #[command(subcommand)]
    Secret(SecretCmd),
    /// Run `<command>` against a fresh, private, non-durable Postgres in the
    /// Docker-in-Docker sidecar every agent session has, then stop the
    /// container, however the command ends. Sweeps any earlier orphaned
    /// container (owning pid no longer alive) first.
    TestPostgres {
        /// Postgres image to run. Default matches CI's own image; override to
        /// match a repo that pins a different tag (e.g. app-server).
        #[arg(long)]
        image: Option<String>,
        /// SQL file applied (with ON_ERROR_STOP) after durability is
        /// disabled, in the order given. Repeatable.
        #[arg(long = "init-sql")]
        init_sql: Vec<PathBuf>,
        /// The command to run, with `DATABASE_URL` and
        /// `CLAUDE_RUN_TEST_DATABASE_URL` pointed at the throwaway Postgres.
        /// Everything after `--`.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Read-only troubleshooting MCP server subcommands (CLAUDE-437).
    #[command(subcommand)]
    Mcp(McpCmd),
    /// Import legacy `~/.local/share/claude-run/state/*.json` files into SQLite.
    Migrate {
        #[arg(long)]
        from: Option<String>,
    },
    /// Copy an existing SQLite state store into the configured database.
    /// The target is whatever `[database] url` /
    /// `CLAUDE_RUN_DATABASE_URL` resolves to, so the import and the daemon
    /// cannot disagree about where the rows went; the source is opened
    /// read-only and is never modified.
    Import {
        /// SQLite file to read. Defaults to `<CLAUDE_RUN_DATA_DIR>/state.db`,
        /// the store this install uses with no database URL set.
        #[arg(long)]
        from: Option<String>,
        /// Copy into a target that already holds rows. Off by default: the
        /// expected move is into a fresh database, and a second import into a
        /// populated one duplicates history.
        #[arg(long)]
        allow_nonempty: bool,
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
    /// Clear one key's attempt counter, so a job parked at the action's
    /// per-key cap (`limits.max_attempts_per_key`) can be retried. Exits
    /// `exit::CONFIG_ERROR` when the key has no counter row.
    ResetAttempts { name: String, key: String },
}

#[derive(Subcommand)]
enum AccessCmd {
    /// Run the access check and print the typed report as JSON. With no target
    /// it checks every enabled `youtrack` action's credentials, the projects its
    /// query reaches, and the repos those projects bind to. Exits non-zero when
    /// any probe fails, so it works as a deployment smoke test.
    Check {
        /// Repo to probe, as `owner/name`.
        #[arg(long)]
        repo: Option<String>,
        /// Forge host for `--repo` (e.g. dev.a8n.run). Defaults to the host the
        /// project's VCS binding resolves to.
        #[arg(long)]
        host: Option<String>,
        /// YouTrack project short name to probe (e.g. CLAUDE).
        #[arg(long)]
        project: Option<String>,
        /// Restrict to actions referencing this secret name.
        #[arg(long)]
        secret: Option<String>,
    },
    /// Clear a web account's TOTP enrollment and every recovery code, forcing
    /// re-enrollment at the next login. Rotates the session key too, signing
    /// out any session already open. DB-direct: works with no daemon running,
    /// which is what makes it useful for lockout recovery.
    #[command(name = "reset-2fa")]
    ResetTotp {
        /// The account's username.
        username: String,
    },
}

#[derive(Subcommand)]
enum McpCmd {
    /// Serve the read-only troubleshooting MCP tools over stdio
    /// (JSON-RPC in on stdin, JSON-RPC out on stdout; logs go to stderr).
    /// DB-direct: works with no daemon running, same as every other
    /// DB-direct subcommand. No tool ever writes.
    Stdio,
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
    // Pre-scan argv before building the tracing subscriber: `mcp stdio`'s
    // stdout must carry only MCP frames, so its logs are routed to stderr
    // from the first line on (see `init_tracing`). This has to happen before
    // clap parses `Cli::cmd`, since the subscriber is process-global and can
    // only be installed once.
    let raw_args: Vec<String> = std::env::args().collect();
    let log_reload = init_tracing(argv_is_mcp_stdio(&raw_args));

    // Resolve the color choice BEFORE clap renders any help/error: clap
    // colorizes its own output during parse, before we could read the parsed
    // `--color` flag, so pre-scan argv for it (honoring NO_COLOR via anstream's
    // auto detection). The flag is still defined on `Cli` for help/validation;
    // this pre-scan only feeds clap's renderer and our global anstream choice.
    let color = resolve_color_choice(&raw_args);
    anstream_choice(color).write_global();

    let cli = parse_cli(color);

    // daemonize MUST happen before any tokio runtime exists. forking after
    // the runtime is constructed produces a child with corrupt scheduler
    // state. Resolve paths and do the fork in this sync context, then build
    // the runtime in the child.
    let maybe_detach = matches!(&cli.cmd, Cmd::Serve { detach: true, .. });
    if maybe_detach {
        if let Err(e) = detach_now() {
            paint::error(&format!("detach failed: {e}"));
            return ExitCode::from(exit::UNEXPECTED as u8);
        }
    }

    if matches!(&cli.cmd, Cmd::Serve { .. }) {
        log_server_started();
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

/// Fork into the background with `logs/daemon.log` as the daemon's stdout and
/// stderr. Nothing rotates that file here; the daemon's own log GC
/// pass (`crate::gc::sweep_oversized_logs`, first tick at serve start) keeps it
/// under `config.stream_log_gc.daemon_log_max_bytes` by truncating it in place,
/// which the `O_APPEND` handle opened below follows to the new end of file.
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

/// First log line of a `serve` process, emitted before config load, the pool and
/// the migrations. `recorded serve start` (`daemon.rs`) is a different line: it
/// stamps the DB-backed diagnostics window and must stay after the pool opens.
fn log_server_started() {
    tracing::info!(
        version = claude_run::VERSION,
        pid = std::process::id(),
        "Server started"
    );
}

/// Build the tracing subscriber and return a reload handle for its `EnvFilter`.
/// The initial filter is `CLAUDE_RUN_LOG` if set, else the built-in default,
/// so every line emitted before config load
/// logs exactly as it did. Wrapping the filter in a `reload::Layer` lets
/// `apply_config_log_filter` swap in `[daemon].log_filter` once the config is
/// loaded, without a second (illegal) `set_global_default`.
///
/// `to_stderr` is true only for `mcp stdio` (CLAUDE-437): that transport's
/// stdout must carry nothing but MCP JSON-RPC frames, so every other
/// subcommand keeps logging to stdout as before and only this one is routed
/// to stderr. Decided by [`argv_is_mcp_stdio`] because this runs before clap
/// parses `Cli::cmd`.
fn init_tracing(to_stderr: bool) -> reload::Handle<EnvFilter, Registry> {
    let filter = EnvFilter::try_from_env("CLAUDE_RUN_LOG")
        .unwrap_or_else(|_| EnvFilter::new("info,claude_run=info,sqlx=warn"));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    if to_stderr {
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(
                fmt::layer()
                    .with_target(false)
                    .json()
                    .with_writer(std::io::stderr),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt::layer().with_target(false).json())
            .init();
    }
    reload_handle
}

/// Best-effort scan of argv for the `mcp stdio` subcommand, used only to pick
/// `init_tracing`'s writer before clap has parsed `Cli::cmd`. Ignores any
/// token starting with `-` (global flags and their values) and looks at the
/// remaining positional tokens, mirroring [`resolve_color_choice`]'s
/// best-effort style; a genuinely ambiguous argv still gets a proper clap
/// error later in [`parse_cli`].
fn argv_is_mcp_stdio(args: &[String]) -> bool {
    let mut positional = args.iter().skip(1).filter(|a| !a.starts_with('-'));
    positional.next().map(String::as_str) == Some("mcp")
        && positional.next().map(String::as_str) == Some("stdio")
}

/// Apply `[daemon].log_filter` after config load, honoring the
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
    // Which state store to open, resolved once so every subcommand
    // and the daemon agree on the backend.
    let db_target = cfg::resolve_database(&paths, &on_disk.database)?;

    let actions_dir = cli
        .actions_dir
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.actions_dir());

    match cli.cmd {
        Cmd::Run { action } => {
            run_action(&action, &actions_dir, &paths, &on_disk, &db_target).await
        }
        Cmd::Serve { detach, no_socket } => {
            serve(
                detach,
                !no_socket,
                paths,
                on_disk,
                config_source,
                log_reload,
            )
            .await
        }
        Cmd::Stop => stop(&paths).await,
        Cmd::Restart { detach, no_socket } => {
            restart(detach, no_socket, cli.actions_dir.as_deref(), &paths).await
        }
        Cmd::Trigger { action } => trigger(&action, &paths).await,
        Cmd::Cancel { run_id } => cancel_run(run_id, &paths).await,
        Cmd::Pause { action, reason } => pause(action, reason, &paths, &db_target).await,
        Cmd::Resume { action } => resume(action, &paths, &db_target).await,
        Cmd::Status => status(&paths, &db_target).await,
        Cmd::Action(c) => action_cmd(c, &paths, &actions_dir, &db_target).await,
        Cmd::Access(c) => access_cmd(c, &paths, &db_target).await,
        Cmd::Secret(c) => secret_cmd(c, &db_target).await,
        Cmd::Mcp(McpCmd::Stdio) => {
            mcp::serve_stdio(&db_target, paths).await?;
            Ok(exit::OK)
        }
        Cmd::TestPostgres {
            image,
            init_sql,
            command,
        } => {
            let opts = test_postgres::Opts { image, init_sql };
            claude_run::blocking::in_place(|| test_postgres::run(&opts, &command))
        }
        Cmd::Migrate { from } => {
            let pool = db::open(&db_target).await?;
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
        Cmd::Import {
            from,
            allow_nonempty,
        } => {
            let source = from
                .map(|f| PathBuf::from(cfg::expand_path(&f)))
                .unwrap_or_else(|| paths.state_db());
            import::refuse_same_target(&source, &db_target)?;
            let target = db::open(&db_target).await?;
            let report =
                import::import_sqlite(&source, &target, import::ImportOptions { allow_nonempty })
                    .await?;
            print!("{}", report.render());
            tracing::info!(
                source = %source.display(),
                tables = report.tables.len(),
                rows = report.total_copied(),
                "sqlite state store imported"
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
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
    let loaded = action::load(arg, actions_dir)?;
    tracing::info!(action = %loaded.action.name, path = %loaded.path.display(), "starting run");

    let pool = db::open(db_target).await?;

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
    // Like the daemon, a standalone `run` seeds and routes the agent
    // to its dedicated CLAUDE_CONFIG_DIR so it loads the slim bot CLAUDE.md, not
    // the operator's global `~/.claude/CLAUDE.md`.
    let agent_config_dir = claude_run::agent_home::resolve_config_dir(
        paths,
        on_disk.daemon.claude_config_dir.as_deref(),
    );
    claude_run::agent_home::ensure(&agent_config_dir)?;
    // No daemon here, so there is no `LiveSettings` to share a
    // handle with; read the stored global `config.claude` once and hand the
    // invoker a handle over that snapshot.
    let claude_defaults = Arc::new(std::sync::RwLock::new(
        settings::effective_claude(&pool).await?,
    ));
    // A fresh, run-scoped gate. It has nothing to persist across
    // invocations of the CLI (there is no daemon), but it still stops a LATER
    // job in the SAME multi-job run once an earlier invocation trips it, via
    // the same `PauseGate`/`RunPause` wiring the daemon uses.
    let readiness = claude_run::readiness::ReadinessGate::new(agent_config_dir.clone(), None);
    let claude = Arc::new(
        claude_mod::ClaudeInvoker::new(on_disk.daemon.claude_binary.as_deref())
            .with_config_dir(Some(agent_config_dir))
            .with_default_claude(claude_defaults)
            .with_readiness(readiness.clone()),
    );
    // A standalone `run` is a single process with no daemon, so it
    // gets its own ephemeral key gate; the per-action lock already prevents it
    // from overlapping a daemon-driven run of the same action.
    // It also gets its own ephemeral limiter sized to the action's
    // own `max_parallel`, so the global cap never over-restricts a single
    // standalone run (there are no other actions to bound it against here).
    let keygate = claude_run::concurrency::KeyGate::new();
    let limiter = claude_run::concurrency::Limiter::new(
        loaded.action.concurrency.max_parallel.max(1) as usize,
    );
    // The standalone path takes the same live global-pause gate as
    // the daemon, so a pause taken while it works stops it picking up new jobs
    // (the up-front check above only covers the moment the run started).
    let pause = claude_run::pause::PauseGate::new(pool.clone()).with_readiness(readiness);
    // The standalone path has no scheduler to defer, so its gather
    // marker is a fresh one nothing else reads.
    let gather = claude_run::concurrency::GatherGate::new();
    // The run's own exit code, so a standalone run that hit its spend
    // ceiling exits `exit::OVER_BUDGET` rather than reporting success.
    daemon::run_action_inner(
        &loaded, paths, &pool, snippet, claude, "cli", None, &keygate, &limiter, &pause, &gather,
    )
    .await
}

async fn serve(
    _detach: bool, // already actioned in main() before the tokio runtime started
    unix_socket: bool,
    paths: cfg::Paths,
    on_disk: cfg::OnDiskConfig,
    config_source: cfg::ConfigSource,
    log_reload: reload::Handle<EnvFilter, Registry>,
) -> claude_run::error::Result<i32> {
    // One startup line recording which config file backed this run
    // (or that defaults silently won), so `journalctl` alone answers why the
    // web UI bound where it did.
    config_source.log();
    // Flag any webhook source name that needs URL escaping (likely a
    // typo) before the listener binds, so the warning sits next to the config line.
    on_disk.webhook.warn_unusual_source_names();
    // Hand the daemon a live log-filter reload hook so a Settings
    // save applies immediately. When `CLAUDE_RUN_LOG` is set the env var wins,
    // so the hook is `None`: a saved filter persists but is not
    // applied this run.
    let log_apply: Option<settings::LogFilterApply> =
        if std::env::var_os("CLAUDE_RUN_LOG").is_some() {
            None
        } else {
            let handle = log_reload;
            Some(Arc::new(move |directive: &str| {
                let filter = EnvFilter::try_new(directive).map_err(|e| e.to_string())?;
                handle.reload(filter).map_err(|e| e.to_string())
            }) as settings::LogFilterApply)
        };
    let d = daemon::Daemon {
        paths,
        on_disk,
        log_apply,
        unix_socket,
    };
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
    no_socket: bool,
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
    if no_socket {
        cmd.arg("--no-socket");
    }
    let err = cmd.exec();
    Err(anyhow::anyhow!("failed to exec `serve` on restart: {err}").into())
}

/// The error for "the daemon socket is not answering", told apart from "no
/// daemon is running". With `serve --no-socket` the daemon is healthy and the
/// socket is absent by design, and reporting that as "not running" sends the
/// operator to restart a daemon that is working fine.
fn daemon_unreachable(
    paths: &cfg::Paths,
    sock: &std::path::Path,
    consequence: &str,
) -> claude_run::error::Error {
    match daemon::read_pidfile(paths) {
        Some(pid) if daemon::process_alive(pid) => anyhow::anyhow!(
            "daemon (pid {pid}) is running with its unix-socket API disabled (`serve --no-socket`), \
             so {consequence}. Use the web UI or its `/api` routes over TCP, or restart the daemon \
             without --no-socket."
        )
        .into(),
        _ => anyhow::anyhow!(
            "daemon not reachable at {}; start it with `claude-run serve --detach`",
            sock.display()
        )
        .into(),
    }
}

async fn trigger(action: &str, paths: &cfg::Paths) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if !DaemonClient::is_reachable(&sock) {
        return Err(daemon_unreachable(
            paths,
            &sock,
            "a trigger cannot reach its scheduler",
        ));
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
        return Err(daemon_unreachable(
            paths,
            &sock,
            "an in-flight run exists only inside it and cannot be cancelled from here",
        ));
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
    db_target: &cfg::DatabaseTarget,
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
        let pool = db::open(db_target).await?;
        match action {
            Some(name) => state::action_pause_set(&pool, &name, reason.as_deref()).await?,
            None => state::global_pause_set(&pool, reason.as_deref()).await?,
        }
        paint::success("ok");
    }
    Ok(exit::OK)
}

async fn resume(
    action: Option<String>,
    paths: &cfg::Paths,
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
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
        let pool = db::open(db_target).await?;
        match action {
            Some(name) => state::action_pause_clear(&pool, &name).await?,
            None => state::global_pause_clear(&pool).await?,
        }
        paint::success("ok");
    }
    Ok(exit::OK)
}

async fn status(
    paths: &cfg::Paths,
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
    let sock = daemon::daemon_socket_path(paths);
    if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        let v: serde_json::Value = c.get("/status").await?;
        jsonfmt::print_json(&v)?;
        return Ok(exit::OK);
    }
    let pool = db::open(db_target).await?;
    jsonfmt::print_json(&daemonless_status(&pool, paths, &sock).await?)?;
    Ok(exit::OK)
}

/// The report `status` prints with no daemon running, or running with the
/// unix socket disabled. Split out of [`status`] so the blocks it must carry
/// are testable without a live daemon or captured stdout.
async fn daemonless_status(
    pool: &crate::db::DbPool,
    paths: &cfg::Paths,
    sock: &std::path::Path,
) -> claude_run::error::Result<serde_json::Value> {
    // The same blocks `/status` carries, assembled once in the library
    // (`state::status_blocks`) so the daemon's `/status` route and this
    // daemonless path can never drift apart (CLAUDE-437 also builds
    // `get_status`'s MCP tool output from this function).
    let b = state::status_blocks(pool).await?;
    // Never guess "not running" from the missing socket alone: it is the
    // pidfile lock's liveness and the `serve --no-socket` marker that decide
    // it (CLAUDE-438).
    let presence = daemon::daemon_presence(paths);
    let (daemon_field, socket_expected_at) = match &presence {
        daemon::DaemonPresence::RunningNoSocket => (
            json!("running (unix-socket API disabled by `serve --no-socket`)"),
            None,
        ),
        daemon::DaemonPresence::NotRunning => {
            (json!("not running"), Some(sock.display().to_string()))
        }
        daemon::DaemonPresence::Undetermined(reason) => (
            json!({"state": "undetermined", "reason": reason}),
            Some(sock.display().to_string()),
        ),
    };
    let mut report = json!({
        "daemon": daemon_field,
        "global_paused": b.global_paused.map(|(a, r)| json!({"paused_at": a, "reason": r})),
        "activity": {
            "quiet": b.active_runs.is_empty(),
            "active_runs": b.active_runs,
            "dispatched_actions": Vec::<String>::new(),
        },
        "stale_gates": b.stale_gates,
        "attempt_cap_hits": b.attempt_cap_hits,
        "actions": b.actions,
    });
    if let Some(expected_at) = socket_expected_at {
        report["socket_expected_at"] = json!(expected_at);
    }
    Ok(report)
}

/// `action show` output: the index row plus prompt provenance read from the YAML. Both prompt
/// fields are null for a kind with no agent prompt, or when the YAML does not parse.
#[derive(serde::Serialize)]
struct ActionShow {
    #[serde(flatten)]
    row: state::ActionIndexRow,
    prompt_source: Option<String>,
    prompt_preset: Option<&'static str>,
}

impl ActionShow {
    fn new(row: state::ActionIndexRow, a: &action::Action) -> Self {
        let preset = match &a.native {
            Some(action::NativeConfig::Youtrack(yt)) => Some(yt.youtrack.prompt_preset.as_str()),
            _ => None,
        };
        Self {
            row,
            prompt_source: a
                .native
                .as_ref()
                .and_then(|n| n.prompt_source())
                .map(|s| s.to_string()),
            prompt_preset: preset,
        }
    }

    fn without_prompt(row: state::ActionIndexRow) -> Self {
        Self {
            row,
            prompt_source: None,
            prompt_preset: None,
        }
    }
}

async fn action_cmd(
    cmd: ActionCmd,
    paths: &cfg::Paths,
    actions_dir: &std::path::Path,
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
    let pool = db::open(db_target).await?;
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
            let Some(row) = rows.into_iter().find(|r| r.name == name) else {
                paint::error(&format!("no action `{name}` in index"));
                return Ok(exit::CONFIG_ERROR);
            };
            // Prompt provenance is read from the YAML on disk; the index has no column for it.
            if row.parse_error.is_some() {
                jsonfmt::print_json(&ActionShow::without_prompt(row))?;
            } else {
                match action::load_from_path(std::path::Path::new(&row.file_path)) {
                    Ok(loaded) => jsonfmt::print_json(&ActionShow::new(row, &loaded.action))?,
                    Err(e) => {
                        let path = row.file_path.clone();
                        jsonfmt::print_json(&ActionShow::without_prompt(row))?;
                        paint::error(&format!(
                            "action `{name}` is indexed but failed to load from {path}: {e}"
                        ));
                        return Ok(exit::CONFIG_ERROR);
                    }
                }
            }
        }
        ActionCmd::Delete { name } => {
            // Delegate to the daemon when it is up (it owns the
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
        ActionCmd::ResetAttempts { name, key } => {
            // Delegate to the daemon when it is up (so the reset's
            // `run_events` row is journaled through the same pool the daemon
            // itself writes through); otherwise clear directly against the DB,
            // matching the pause / resume / delete split above.
            if reachable {
                let c = DaemonClient::new(&sock);
                let (status, body) = c
                    .post_status(&format!("/actions/{name}/attempts/{key}/reset"), None)
                    .await?;
                return match status {
                    200 => {
                        paint::success(&format!("cleared attempt counter for `{name}`/`{key}`"));
                        Ok(exit::OK)
                    }
                    404 => {
                        paint::error(&format!("no attempt counter for `{name}`/`{key}`"));
                        Ok(exit::CONFIG_ERROR)
                    }
                    _ => {
                        paint::error(&format!("daemon returned {status}: {body}"));
                        Ok(exit::UNEXPECTED)
                    }
                };
            }
            let count = state::attempts_get(&pool, &name, &key).await?;
            if count == 0 {
                paint::error(&format!("no attempt counter for `{name}`/`{key}`"));
                return Ok(exit::CONFIG_ERROR);
            }
            state::attempts_clear(&pool, &name, &key).await?;
            state::run_events_record(
                &pool,
                &name,
                &state::now_iso(),
                &[state::InputEvent {
                    level: "warn".into(),
                    context: Some(key.clone()),
                    message: format!(
                        "attempt counter reset for `{key}`: {count} attempt(s) discarded"
                    ),
                }],
            )
            .await?;
            paint::success(&format!("cleared {count} attempt(s) for `{name}`/`{key}`"));
        }
    }
    Ok(exit::OK)
}

/// `claude-run access check`. Delegates to the daemon over the unix
/// socket when it is up (so the check runs as the daemon's identity, with its
/// PATH and env) and runs the same service directly against the DB when it is
/// not. Prints the typed report as JSON either way, and exits `CONFIG_ERROR`
/// when any probe failed so it is usable as a deployment smoke test.
async fn access_cmd(
    cmd: AccessCmd,
    paths: &cfg::Paths,
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
    let (repo, host, project, secret) = match cmd {
        AccessCmd::ResetTotp { username } => {
            let pool = db::open(db_target).await?;
            let account = match claude_run::web_admin::load_by_username(&pool, &username).await {
                Ok(Some(a)) => a,
                Ok(None) => {
                    paint::error(&format!("no account named `{username}`"));
                    return Ok(exit::CONFIG_ERROR);
                }
                Err(e) => {
                    paint::error(&format!("could not look up `{username}`: {e}"));
                    return Ok(exit::CONFIG_ERROR);
                }
            };
            return match claude_run::web_admin::reset_totp(&pool, account.id).await {
                Ok(_) => {
                    paint::success(
                        "TOTP enrollment cleared; the next login will be sent to enrollment",
                    );
                    Ok(exit::OK)
                }
                Err(e) => {
                    paint::error(&format!("could not reset 2FA: {e}"));
                    Ok(exit::CONFIG_ERROR)
                }
            };
        }
        AccessCmd::Check {
            repo,
            host,
            project,
            secret,
        } => (repo, host, project, secret),
    };
    let req = access::AccessRequest {
        secret,
        host,
        repo,
        project,
    }
    .normalized();

    let sock = daemon::daemon_socket_path(paths);
    let report: access::AccessReport = if DaemonClient::is_reachable(&sock) {
        let c = DaemonClient::new(&sock);
        c.post("/access/check", Some(&serde_json::to_value(&req)?))
            .await?
    } else {
        let pool = db::open(db_target).await?;
        match access::check(&pool, &api::access_cred_base(paths), &req).await {
            Ok(r) => r,
            Err(e) => access::AccessReport::from_error(&e),
        }
    };

    jsonfmt::print_json(&report)?;
    if report.ok() {
        Ok(exit::OK)
    } else {
        paint::error(&format!("{} access probe(s) failed", report.failures()));
        Ok(exit::CONFIG_ERROR)
    }
}

async fn secret_cmd(
    cmd: SecretCmd,
    db_target: &cfg::DatabaseTarget,
) -> claude_run::error::Result<i32> {
    let pool = db::open(db_target).await?;
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
            // Encrypt under the age key. `ensure_key` generates one on a fresh
            // install so `secret set` works with nothing provisioned, and refuses
            // to mint a replacement when secrets are already stored under a key
            // that has gone missing.
            secrets::ensure_key(&pool).await?;
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

    #[derive(Clone)]
    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// `serve` logs `Server started` with `version` and `pid`, in the JSON shape
    /// `init_tracing` writes, so operators see which build started and when.
    #[test]
    fn log_server_started_emits_message_version_and_pid() {
        let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sub = fmt()
            .json()
            .with_target(false)
            .with_writer(CaptureWriter(buf.clone()))
            .finish();
        tracing::subscriber::with_default(sub, log_server_started);

        let raw = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        let mut lines = raw.lines();
        let line: serde_json::Value = serde_json::from_str(lines.next().expect("one log line"))
            .unwrap_or_else(|e| panic!("not JSON ({e}): {raw}"));
        assert!(lines.next().is_none(), "exactly one line expected: {raw}");
        assert_eq!(line["level"], "INFO", "{raw}");
        assert_eq!(line["fields"]["message"], "Server started", "{raw}");
        assert_eq!(line["fields"]["version"], claude_run::VERSION, "{raw}");
        assert_eq!(line["fields"]["pid"], std::process::id(), "{raw}");
    }

    /// `action show` adds the prompt provenance read from each shipped action's YAML.
    #[test]
    fn action_show_reports_prompt_provenance() {
        let row = |name: &str| state::ActionIndexRow {
            name: name.to_string(),
            file_path: format!("actions/{name}.yml"),
            yaml_sha256: String::new(),
            enabled: true,
            schedule_kind: None,
            schedule_spec: None,
            last_seen_at: String::new(),
            parse_error: None,
            link_template: None,
            max_parallel: 1,
        };
        for (name, raw, source, preset) in [
            (
                "audit-run",
                include_str!("../../actions/audit-run.yml"),
                Some("builtin:audit"),
                Some("audit"),
            ),
            (
                "youtrack",
                include_str!("../../actions/youtrack.yml"),
                Some("builtin:default"),
                Some("default"),
            ),
            (
                "closed-issue-summary",
                include_str!("../../actions/closed-issue-summary.yml"),
                None,
                None,
            ),
        ] {
            let loaded = action::validate_str(raw, name).unwrap();
            let v = serde_json::to_value(ActionShow::new(row(name), &loaded.action)).unwrap();
            assert_eq!(v["name"], name, "the index row is flattened in");
            assert_eq!(v["prompt_source"].as_str(), source, "{name}");
            assert_eq!(v["prompt_preset"].as_str(), preset, "{name}");
        }

        let v = serde_json::to_value(ActionShow::without_prompt(row("broken"))).unwrap();
        assert!(v["prompt_source"].is_null() && v["prompt_preset"].is_null());
        assert!(v.get("parse_error").is_some());
    }

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

    /// `access check` parses its targets, and each flag is optional
    /// (no flags at all is the aggregate check).
    #[test]
    fn access_check_parses_its_targets() {
        let cli = Cli::try_parse_from([
            "claude-run",
            "access",
            "check",
            "--repo",
            "contoso/claude-run",
            "--host",
            "dev.a8n.run",
            "--project",
            "CLAUDE",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Access(AccessCmd::Check {
                repo,
                host,
                project,
                secret,
            }) => {
                assert_eq!(repo.as_deref(), Some("contoso/claude-run"));
                assert_eq!(host.as_deref(), Some("dev.a8n.run"));
                assert_eq!(project.as_deref(), Some("CLAUDE"));
                assert_eq!(secret, None);
            }
            _ => panic!("expected access check"),
        }
        assert!(Cli::try_parse_from(["claude-run", "access", "check"]).is_ok());
    }

    /// `access reset-2fa` takes the target account's username; it is the
    /// DB-direct lockout recovery path.
    #[test]
    fn access_reset_2fa_parses() {
        let cli = Cli::try_parse_from(["claude-run", "access", "reset-2fa", "admin"]).unwrap();
        assert!(matches!(
            cli.cmd,
            Cmd::Access(AccessCmd::ResetTotp { username }) if username == "admin"
        ));
    }

    /// `test-postgres` takes `--image`, repeatable `--init-sql`, and collects
    /// everything after `--` as the command, hyphens and all.
    #[test]
    fn test_postgres_parses_image_init_sql_and_trailing_command() {
        let cli = Cli::try_parse_from([
            "claude-run",
            "test-postgres",
            "--image",
            "postgres:18.2-alpine3.23",
            "--init-sql",
            "a.sql",
            "--init-sql",
            "b.sql",
            "--",
            "cargo",
            "test",
            "--",
            "--nocapture",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::TestPostgres {
                image,
                init_sql,
                command,
            } => {
                assert_eq!(image.as_deref(), Some("postgres:18.2-alpine3.23"));
                assert_eq!(
                    init_sql,
                    vec![PathBuf::from("a.sql"), PathBuf::from("b.sql")]
                );
                assert_eq!(
                    command,
                    vec!["cargo", "test", "--", "--nocapture"]
                        .into_iter()
                        .map(String::from)
                        .collect::<Vec<_>>()
                );
            }
            _ => panic!("expected test-postgres"),
        }
        assert!(Cli::try_parse_from(["claude-run", "test-postgres"]).is_err());
    }

    /// `import` takes its source from `--from` (defaulting to the
    /// install's own `state.db`) and copies into a populated target only when
    /// told to.
    #[test]
    fn import_parses_its_source_and_defaults_to_refusing_a_populated_target() {
        let cli = Cli::try_parse_from(["claude-run", "import"]).unwrap();
        assert!(
            matches!(
                cli.cmd,
                Cmd::Import {
                    from: None,
                    allow_nonempty: false
                }
            ),
            "no source means <CLAUDE_RUN_DATA_DIR>/state.db, and an empty target"
        );

        let cli = Cli::try_parse_from([
            "claude-run",
            "import",
            "--from",
            "/srv/old/state.db",
            "--allow-nonempty",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Import {
                from,
                allow_nonempty,
            } => {
                assert_eq!(from.as_deref(), Some("/srv/old/state.db"));
                assert!(allow_nonempty);
            }
            _ => panic!("expected import"),
        }
    }

    /// The container image runs `serve --no-socket`, so the flag must parse on
    /// both `serve` and `restart`, default to off, and not disturb the
    /// detach pre-scan that decides whether to fork before the tokio runtime.
    #[test]
    fn no_socket_flag_parses_on_serve_and_restart() {
        let cli = Cli::try_parse_from(["claude-run", "serve"]).unwrap();
        assert!(
            matches!(
                cli.cmd,
                Cmd::Serve {
                    detach: false,
                    no_socket: false
                }
            ),
            "the socket stays on by default, so a host install is unchanged"
        );

        let cli = Cli::try_parse_from(["claude-run", "serve", "--no-socket"]).unwrap();
        assert!(matches!(
            cli.cmd,
            Cmd::Serve {
                detach: false,
                no_socket: true
            }
        ));

        let cli = Cli::try_parse_from(["claude-run", "serve", "--detach", "--no-socket"]).unwrap();
        assert!(matches!(
            cli.cmd,
            Cmd::Serve {
                detach: true,
                no_socket: true
            }
        ));
        assert!(
            matches!(&cli.cmd, Cmd::Serve { detach: true, .. }),
            "the detach pre-scan must still fire when --no-socket follows it"
        );

        let cli = Cli::try_parse_from(["claude-run", "restart", "--no-socket"]).unwrap();
        assert!(matches!(
            cli.cmd,
            Cmd::Restart {
                detach: false,
                no_socket: true
            }
        ));
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

    /// `claude-run status` with no daemon carries the parked-at-cap
    /// block, like the stale-gate block beside it. The counters are
    /// plain DB state, so omitting them here would read as "nothing is stuck"
    /// exactly when the daemon is down and nobody can check the web UI.
    #[tokio::test]
    async fn daemonless_status_reports_keys_parked_at_the_attempt_cap() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::open_sqlite(&dir.path().join("state.db")).await.unwrap();
        let paths = test_paths(dir.path());
        let sock = dir.path().join("daemon.sock");

        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        assert_eq!(report["attempt_cap_hits"].as_array().unwrap().len(), 0);

        // No action index row, so the key is measured against the default cap.
        for _ in 0..3 {
            state::attempts_increment(&pool, "youtrack", "CLAUDE-9")
                .await
                .unwrap();
        }
        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        let hits = report["attempt_cap_hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1, "{report:#}");
        assert_eq!(hits[0]["job_key"], "CLAUDE-9");
        assert_eq!(hits[0]["action_name"], "youtrack");
        assert_eq!(hits[0]["count"], 3);
        assert_eq!(hits[0]["cap"], 3);
    }

    fn test_paths(dir: &std::path::Path) -> cfg::Paths {
        cfg::Paths {
            config_dir: dir.to_path_buf(),
            data_dir: dir.to_path_buf(),
            state_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
        }
    }

    /// With no daemon running at all, `status` reports it as such and omits
    /// no `socket_expected_at`: a socket would be expected on the next
    /// `serve` unless that start also disables it.
    #[tokio::test]
    async fn daemonless_status_reports_not_running_with_no_pidfile() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::open_sqlite(&dir.path().join("state.db")).await.unwrap();
        let paths = test_paths(dir.path());
        let sock = dir.path().join("daemon.sock");

        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        assert_eq!(report["daemon"], "not running");
        assert_eq!(report["socket_expected_at"], sock.display().to_string());
    }

    /// A live pidfile pid plus the `--no-socket` marker reports the daemon as
    /// running with the socket disabled, not as "not running", and omits
    /// `socket_expected_at` since this deployment deliberately has none.
    #[tokio::test]
    async fn daemonless_status_reports_running_with_socket_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::open_sqlite(&dir.path().join("state.db")).await.unwrap();
        let paths = test_paths(dir.path());
        let sock = dir.path().join("daemon.sock");
        std::fs::write(
            daemon::daemon_pidfile_path(&paths),
            (std::process::id() as i32).to_string(),
        )
        .unwrap();
        std::fs::write(daemon::daemon_no_socket_marker_path(&paths), b"").unwrap();

        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        assert_eq!(
            report["daemon"],
            "running (unix-socket API disabled by `serve --no-socket`)"
        );
        assert!(report.get("socket_expected_at").is_none(), "{report:#}");
    }

    /// A stale pidfile left by a crashed daemon (dead pid) still reports "not
    /// running", even with a leftover `--no-socket` marker beside it.
    #[tokio::test]
    async fn daemonless_status_reports_not_running_with_a_stale_pidfile() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::open_sqlite(&dir.path().join("state.db")).await.unwrap();
        let paths = test_paths(dir.path());
        let sock = dir.path().join("daemon.sock");
        std::fs::write(
            daemon::daemon_pidfile_path(&paths),
            (0x7fff_fff0_i32).to_string(),
        )
        .unwrap();
        std::fs::write(daemon::daemon_no_socket_marker_path(&paths), b"").unwrap();

        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        assert_eq!(report["daemon"], "not running");
        assert_eq!(report["socket_expected_at"], sock.display().to_string());
    }

    /// A live pidfile pid with no `--no-socket` marker (socket expected but
    /// unreachable for some other reason) is reported as undetermined with a
    /// reason, not asserted as either running or not running.
    #[tokio::test]
    async fn daemonless_status_reports_undetermined_with_a_live_pid_and_no_marker() {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::open_sqlite(&dir.path().join("state.db")).await.unwrap();
        let paths = test_paths(dir.path());
        let sock = dir.path().join("daemon.sock");
        std::fs::write(
            daemon::daemon_pidfile_path(&paths),
            (std::process::id() as i32).to_string(),
        )
        .unwrap();

        let report = daemonless_status(&pool, &paths, &sock).await.unwrap();
        assert_eq!(report["daemon"]["state"], "undetermined");
        assert!(
            report["daemon"]["reason"]
                .as_str()
                .unwrap()
                .contains("unreachable"),
            "{report:#}"
        );
        assert_eq!(report["socket_expected_at"], sock.display().to_string());
    }
}
