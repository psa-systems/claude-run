//! SQLite connection + migrations.
//!
//! Single shared pool, configured for WAL and busy-timeout so multiple
//! processes (one daemon + occasional CLI invocations) cooperate cleanly.

use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;

use crate::error::Result;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn open(db_path: &Path) -> Result<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let url = format!("sqlite://{}?mode=rwc", db_path.display());
    let opts = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        // CLAUDE-228: 8 left no headroom once the daemon runs several overlapping
        // runs plus the web UI's polling.
        .max_connections(16)
        // CLAUDE-228: sqlx's default, stated so it is a decision and not an
        // inherited value. An acquire that waits this long is a real fault.
        .acquire_timeout(std::time::Duration::from_secs(30))
        // CLAUDE-228: raised from the 2s default. With blocking work off the
        // async runtime an acquire is sub-millisecond, so 5s flags a genuine
        // regression instead of logging routine contention.
        .acquire_slow_threshold(std::time::Duration::from_secs(5))
        .connect_with(opts)
        .await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
