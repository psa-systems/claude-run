use thiserror::Error;

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

    #[error("action {name}: kind `{kind}` does not match native config `{native}` ({path})")]
    ActionKindMismatch {
        name: String,
        kind: String,
        native: String,
        path: String,
    },

    #[error("native action kind `{kind}` is not implemented yet")]
    NativeUnimplemented { kind: String },

    #[error("snippet {phase} failed (exit {exit_code}): {stderr}")]
    SnippetFailed {
        phase: String,
        exit_code: i32,
        stderr: String,
    },

    #[error("claude binary not found in PATH")]
    ClaudeNotFound,

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

    /// CLAUDE-102: both PR-creation strategies failed; carries both errors so
    /// the finalize comment and journal name the AGit attempt and the
    /// git-push fallback.
    #[error("PR creation failed: AGit attempt: {agit}; git-push fallback: {fallback}")]
    PrCreateFailed { agit: String, fallback: String },

    #[error("no enabled VCS integration for project `{0}`")]
    NoVcsIntegration(String),

    #[error("no usable provider backend (tried: {0})")]
    NoBackend(String),

    #[error("secret: {0}")]
    Secret(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
