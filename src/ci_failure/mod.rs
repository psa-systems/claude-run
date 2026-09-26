//! CLAUDE-272: the CI-failure store.
//!
//! The CI-failure redesign (CLAUDE-270) dedupes failures by failing-test
//! overlap and recognises the agent's own PRs from the runner's own records,
//! rather than a YouTrack full-text search for a `crdedupe` marker (an
//! unindexed issue is invisible to that search, and a failure every PR
//! inherits from `main` mints one issue per PR). This module is the store:
//! [`fingerprint`] turns a job log into the ids that decide overlap, and
//! [`store`] holds the async functions over [`crate::db::DbPool`] that record
//! failures, their runs, and the agent's own pull requests, and answer "which
//! issue does this failure belong to".
//!
//! Nothing in the webhook or the `youtrack` action calls this yet: the routing
//! issues under CLAUDE-270 wire it in. This module is public so that
//! not-yet-called API is not dead code.

pub mod fingerprint;
pub mod store;

/// CLAUDE-274: reusable pieces of a deep, log-grounded CI-failure analysis,
/// used by the at-ingest classifier. Crate-internal: nothing here is a stable
/// API surface.
pub(crate) mod analysis;
