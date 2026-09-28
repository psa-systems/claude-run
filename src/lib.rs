//! claude-run: YAML-driven Claude Code runner.
//!
//! Library entry point used by the `claude-run` binary. Modules are organized
//! around the concerns documented in `actions/SCHEMA.md`:
//!
//! - [`config`]: runtime + on-disk settings, XDG paths.
//! - [`action`]: YAML action loader.
//! - [`workdir`]: per-job workdir path resolution (env-var expansion only).
//! - [`snippet`]: nu snippet execution (shell out to `nu`).
//! - [`claude`]: invoke the `claude` CLI with stream-json output.
//! - [`job`]: per-job pipeline (filter / pre / prompt / claude / post).
//! - [`state`]: SQLite-backed attempt counters and daily ledger.
//! - [`usage`]: per-run token / cost accounting and the per-run spend ceiling.
//! - [`migrate`]: one-shot import of legacy state.json files.
//! - [`import`]: copy an existing SQLite state store into the configured
//!   backend.
//!
//! See `actions/SCHEMA.md` for the user-facing schema, `README.md` for the
//! runtime contract.

pub mod access;
pub mod action;
pub mod agent_home;
pub mod api;
pub mod auth;
pub mod autofix;
pub mod blocking;
pub mod cancel;
pub mod ci_failure;
pub mod claude;
pub mod claude_login;
pub mod client;
pub mod concurrency;
pub mod config;
pub mod daemon;
pub mod db;
pub mod error;
pub mod forge_log;
pub mod gc;
pub mod import;
pub mod infra_failure;
pub mod job;
pub mod jsonfmt;
pub mod lock;
pub mod migrate;
pub mod mint;
pub mod native;
pub mod paint;
pub mod pause;
pub mod provider;
pub mod readiness;
pub mod report;
pub mod retention;
pub mod scheduler;
pub mod secrets;
pub mod service_auth;
pub mod settings;
pub mod snippet;
pub mod state;
/// Test-only helpers shared across the unit tests.
#[cfg(test)]
pub mod testutil;
pub mod triage;
pub mod update;
pub mod usage;
pub mod web;
pub mod web_admin;
pub mod workdir;

/// Build-stamped version string: semver + short git hash + build date, baked
/// at compile time by `build.rs`. Example: `0.1.0 (1b66909c2a3f, 2026-05-24)`.
/// Single source for `--version`, the `/api/status` `version` field, and the
/// web footer.
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
    /// Work was refused or stopped because of a pause. Used both by the
    /// standalone `claude-run run` (refused up front) and as
    /// the `runs.exit_code` of a run that stopped picking up new jobs when the
    /// operator paused globally mid-run. Distinct from [`OK`] and [`CANCELLED`]
    /// so the run list can badge it separately.
    pub const PAUSED: i32 = 1;
    pub const LOCKED: i32 = 2;
    pub const CONFIG_ERROR: i32 = 3;
    pub const DAILY_CAP: i32 = 4;
    /// Also the `runs.exit_code` of a run whose row was closed
    /// without its handler finalizing it, by [`crate::state::RunGuard`] on drop
    /// or by the startup reap in [`crate::state::reap_orphaned_runs`].
    pub const UNEXPECTED: i32 = 5;
    pub const OVER_BUDGET: i32 = 6;
    /// A mutating command was refused because it conflicts with live
    /// state, e.g. `action delete` while a run for that action is in flight.
    /// The web UI / API map the same condition to HTTP 409.
    pub const CONFLICT: i32 = 7;
    /// A run stopped picking up new jobs because the Claude login
    /// needs renewing (the CLI reported `authentication_failed`, or the status
    /// probe found `loggedIn: false`), distinct from [`PAUSED`] (an operator
    /// decision) and from [`OK`]. An operator cancel wins over both, and a
    /// global pause wins over this when both hold; see
    /// [`crate::pause::RunPause::exit_code`].
    pub const LOGIN_REQUIRED: i32 = 8;
    /// Sentinel exit_code for a run that was cancelled rather than finished,
    /// following the SIGINT convention (128 + SIGINT). Used both for live
    /// cancel on demand and for the shutdown sweep of runs the
    /// daemon stopped; a row found open at STARTUP is
    /// [`UNEXPECTED`] instead. 130 does not collide with the codes above or
    /// claude's own exits, so the web UI and CLI can map it to a distinct
    /// "cancelled" badge without a schema migration.
    pub const CANCELLED: i32 = 130;
}
