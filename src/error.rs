use std::time::Duration;

use thiserror::Error;

/// The one error type every fallible path in the binary returns. SPEC-5.5.
#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A dedicated name for `sqlx::Error::PoolTimedOut`, SPEC-10.1.4: a pool
    /// acquire that exceeds `acquire_timeout` must name the pool, how long it
    /// waited, and the pool's size and idle count, so a slow-disk stall is
    /// distinguishable from a leaked connection instead of surfacing as a
    /// bare `Sqlx(PoolTimedOut)`.
    #[error(
        "pool `{pool}` acquire timed out after {waited:?} (configured timeout {timeout:?}); \
         size {size}, idle {idle}"
    )]
    PoolAcquireTimeout {
        pool: String,
        waited: Duration,
        timeout: Duration,
        size: u32,
        idle: u32,
    },

    #[error("sqlx migrate: {0}")]
    SqlxMigrate(#[from] sqlx::migrate::MigrateError),

    #[error("action {name}: missing required field `{field}` ({path})")]
    ActionMissingField {
        name: String,
        field: String,
        path: String,
    },

    #[error("action `{name}` not found in {searched}")]
    ActionNotFound { name: String, searched: String },

    #[error("action `{name}` has a run in flight; cancel or wait before deleting")]
    ActionInFlight { name: String },

    #[error("action {name}: invalid `{field}` ({path}): {reason}")]
    ActionInvalidField {
        name: String,
        field: String,
        reason: String,
        path: String,
    },

    #[error("action {name}: kind `{kind}` does not match native config `{native}` ({path})")]
    ActionKindMismatch {
        name: String,
        kind: String,
        native: String,
        path: String,
    },

    #[error("native action kind `{kind}` is not implemented yet")]
    NativeUnimplemented { kind: String },

    #[error("action kind `{kind}` was retired and is no longer supported ({path})")]
    ActionRetiredKind { kind: String, path: String },

    #[error("snippet {phase} failed (exit {exit_code}): {stderr}")]
    SnippetFailed {
        phase: String,
        exit_code: i32,
        stderr: String,
    },

    #[error("claude binary not found in PATH")]
    ClaudeNotFound,

    #[error("test-postgres {stage}: {message}")]
    TestPostgresFailed { stage: String, message: String },

    #[error("input.command stdout was not a JSON array: {0}")]
    InputNotJsonArray(String),

    #[error("provider CLI `{binary}` not found in PATH")]
    CliNotFound { binary: String },

    #[error("provider CLI `{binary} {args}` failed (exit {exit_code}): {stderr}")]
    CliFailed {
        binary: String,
        args: String,
        exit_code: i32,
        stderr: String,
    },

    #[error("provider CLI `{binary}` output parse failed ({context}): {source}")]
    CliParse {
        binary: String,
        context: String,
        source: serde_json::Error,
    },

    #[error("no enabled VCS integration for project `{0}`")]
    NoVcsIntegration(String),

    #[error("no usable provider backend (tried: {0})")]
    NoBackend(String),

    /// The run already created `cap` issues, so the provider refused
    /// to create another. `run` is the run id, or `none` outside a run.
    #[error(
        "run {run} reached its per-run issue-creation cap of {cap}; refusing to create another \
         issue in `{project}`. Raise `limits.max_issues_per_run` if this run legitimately needs \
         more."
    )]
    IssueCapReached {
        run: String,
        cap: u32,
        project: String,
    },

    #[error("secret: {0}")]
    Secret(String),

    /// CLAUDE-549: a bounded forge read (`Forge::pr_status_within`) did not
    /// answer within its timeout. The caller falls back rather than waiting.
    #[error("forge `{op}` did not answer within {timeout:?}")]
    ForgeTimeout { op: String, timeout: Duration },

    /// SPEC-3.12 item 4: the final push found `branch` diverged, and the
    /// configured bounded conflict-resolution pass either failed or left the
    /// merge unresolved, so the merge was aborted exactly as it would have
    /// been with no resolver configured. Distinct from the generic push/
    /// PR-create failure so `finalize` can route it to `Needs Review` naming
    /// the conflicting files and both SHAs, per the SPEC-3.12 outcome table.
    #[error(
        "remote branch `{branch}` has diverged from the local work (local {local_sha}, remote \
         {remote_sha}) and the bounded conflict-resolution pass could not resolve it ({detail}); \
         the merge was aborted. Refusing to force-push over it. Conflicting files: {files}."
    )]
    ConflictResolutionFailed {
        branch: String,
        local_sha: String,
        remote_sha: String,
        files: String,
        detail: String,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The `Result` alias every fallible path returns. SPEC-5.5.
pub type Result<T> = std::result::Result<T, Error>;
