//! `claude-run` CLI entry point. Phase 1 supports the two subcommands needed
//! for parity with the old nu script: `run` and `migrate`. Phase 2 adds
//! `serve`, `action`, `config`, `pause`, etc.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use claude_run::{
    action, claude as claude_mod, config as cfg, db, exit, migrate, row, snippet, state,
};

#[derive(Parser)]
#[command(
    name = "claude-run",
    version,
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
    /// Run an action once. Behaves like the legacy `scripts/claude-run.nu`.
    Run {
        /// Action name (resolved against the actions dir) or path to a YAML file.
        action: String,
    },
    /// Import legacy `~/.local/share/claude-run/state/*.json` files into SQLite.
    Migrate {
        /// Override the legacy state directory.
        #[arg(long)]
        from: Option<String>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match real_main(cli).await {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            tracing::error!(error = %e, "fatal");
            ExitCode::from(exit::UNEXPECTED as u8)
        }
    }
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
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| paths.actions_dir());

    match cli.cmd {
        Cmd::Run { action } => run_action(&action, &actions_dir, &paths, &on_disk).await,
        Cmd::Migrate { from } => {
            let pool = db::open(&paths.state_db()).await?;
            let legacy = from
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| dirs::home_dir().unwrap().join(".local/share/claude-run/state"));
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
    tracing::info!(
        action = %loaded.action.name,
        path = %loaded.path.display(),
        "starting run"
    );

    let pool = db::open(&paths.state_db()).await?;
    let snippet = snippet::SnippetRunner::new(on_disk.daemon.nu_binary.as_deref());
    let claude = claude_mod::ClaudeInvoker::new(on_disk.daemon.claude_binary.as_deref());

    let defaults = row::RunnerDefaults::default();
    let limits = row::effective_limits(&defaults, &loaded);

    // ---- daily cap + budget pre-check (parity with old exit codes) ----
    let daily = state::daily_count_get(&pool, &loaded.action.name).await?;
    if daily >= limits.max_runs_per_day {
        tracing::warn!(count = daily, cap = limits.max_runs_per_day, "daily cap reached");
        return Ok(exit::DAILY_CAP);
    }
    let cost_today = state::daily_cost_get(&pool, &loaded.action.name).await?;
    if cost_today >= limits.daily_budget_usd {
        tracing::warn!(cost_usd = cost_today, budget_usd = limits.daily_budget_usd, "daily budget reached");
        return Ok(exit::OVER_BUDGET);
    }

    // ---- gather input rows ----
    let rows = row::gather_rows(&loaded, &snippet, paths).await?;
    tracing::info!(count = rows.len(), "input rows gathered");

    // ---- mark run, iterate ----
    let run_id = state::run_begin(&pool, &loaded.action.name, "cli").await?;
    let mut processed: u32 = 0;
    let ctx = row::RowContext {
        action: &loaded,
        limits,
        paths,
        snippet: &snippet,
        claude: &claude,
        pool: &pool,
        run_id,
    };

    for (idx, r) in rows.iter().enumerate() {
        // Re-check caps mid-loop the way the old runner did.
        let current = state::daily_count_get(&pool, &loaded.action.name).await?;
        if current >= limits.max_runs_per_day {
            tracing::warn!(count = current, "daily cap reached mid-run, stopping");
            break;
        }
        let cost = state::daily_cost_get(&pool, &loaded.action.name).await?;
        if cost >= limits.daily_budget_usd {
            tracing::warn!(cost_usd = cost, "daily budget reached mid-run, stopping");
            break;
        }

        let outcome = row::process_row(&ctx, r, idx).await?;
        if let Some(record) = row::done_record(&outcome) {
            row::append_output_record(paths, &loaded, &record);
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
                    &pool,
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

    state::run_finish(&pool, run_id, exit::OK, processed).await?;
    let final_count = state::daily_count_get(&pool, &loaded.action.name).await?;
    let final_cost = state::daily_cost_get(&pool, &loaded.action.name).await?;
    tracing::info!(
        final_daily_count = final_count,
        final_daily_cost_usd = final_cost,
        "run complete"
    );
    Ok(exit::OK)
}
