//! claude-run: YAML-driven Claude Code runner.
//!
//! Library entry point used by the `claude-run` binary. Modules are organized
//! around the concerns documented in `actions/SCHEMA.md`:
//!
//! - [`config`]: runtime + on-disk settings, XDG paths.
//! - [`action`]: YAML action loader.
//! - [`workdir`]: per-row workdir path resolution (env-var expansion only).
//! - [`snippet`]: nu snippet execution (shell out to `nu`).
//! - [`claude`]: invoke the `claude` CLI with stream-json output.
//! - [`row`]: per-row pipeline (filter / pre / prompt / claude / post).
//! - [`state`]: SQLite-backed attempt counters and daily ledger.
//! - [`migrate`]: one-shot import of legacy state.json files.
//!
//! See `actions/SCHEMA.md` for the user-facing schema, `README.md` for the
//! runtime contract.

pub mod action;
pub mod api;
pub mod claude;
pub mod client;
pub mod config;
pub mod daemon;
pub mod db;
pub mod error;
pub mod lock;
pub mod migrate;
pub mod row;
pub mod scheduler;
pub mod snippet;
pub mod state;
pub mod web;
pub mod workdir;

/// Build-stamped version string: semver + short git hash + build date, baked
/// at compile time by `build.rs`. Example: `0.1.0 (1b66909c2a3f, 2026-05-24)`.
/// Single source for `--version`, the `/api/status` `version` field, and the
/// web footer (CLAUDE-2).
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("CLAUDE_RUN_GIT_SHA"),
    ", ",
    env!("CLAUDE_RUN_BUILD_DATE"),
    ")"
);

/// Exit codes match the previous nu implementation so existing cron and
/// systemd units behave the same after the port.
pub mod exit {
    pub const OK: i32 = 0;
    pub const PAUSED: i32 = 1;
    pub const LOCKED: i32 = 2;
    pub const CONFIG_ERROR: i32 = 3;
    pub const DAILY_CAP: i32 = 4;
    pub const UNEXPECTED: i32 = 5;
    pub const OVER_BUDGET: i32 = 6;
}
