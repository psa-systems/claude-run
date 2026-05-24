//! Action YAML loader.
//!
//! Schema mirrors `actions/SCHEMA.md`. Snippets stay as opaque strings; the
//! runner shells out to `nu` to execute them (see `crate::snippet`).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub name: String,

    #[serde(default)]
    pub workdir_base: Option<String>,

    pub input: InputBlock,

    #[serde(default)]
    pub dedup_key: Option<String>,

    #[serde(default)]
    pub filter: Option<String>,

    #[serde(default)]
    pub pre: Vec<String>,

    pub prompt: String,

    #[serde(default)]
    pub claude: ClaudeBlock,

    #[serde(default)]
    pub post: Vec<String>,

    #[serde(default)]
    pub output: OutputBlock,

    #[serde(default)]
    pub limits: Limits,

    #[serde(default)]
    pub schedule: Option<Schedule>,

    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputBlock {
    pub command: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClaudeBlock {
    pub cwd: Option<String>,
    pub allowed_tools: Option<String>,
    pub permission_mode: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutputBlock {
    pub file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_attempts_per_key: Option<u32>,
    pub max_runs_per_day: Option<u32>,
    pub claude_timeout_seconds: Option<u32>,
    pub daily_budget_usd: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Schedule {
    pub kind: ScheduleKind,
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Interval,
    Cron,
    Manual,
}

/// Action + provenance: original file path and SHA-256 of the on-disk YAML.
/// The DB indexes the hash so the daemon can detect on-disk edits.
#[derive(Debug, Clone)]
pub struct LoadedAction {
    pub action: Action,
    pub path: PathBuf,
    pub yaml_sha256: String,
}

/// Load a YAML file as an [`Action`]. Path resolution rules:
///
/// - If `arg` exists as a file, use it.
/// - Else, look for `<actions_dir>/<arg>.yml` and `.yaml`.
pub fn resolve_action_path(arg: &str, actions_dir: &Path) -> Option<PathBuf> {
    let direct = PathBuf::from(arg);
    if direct.is_file() {
        return Some(direct);
    }
    for ext in ["yml", "yaml"] {
        let p = actions_dir.join(format!("{}.{}", arg, ext));
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

pub fn load_from_path(path: &Path) -> Result<LoadedAction> {
    let raw = std::fs::read_to_string(path)?;
    let action: Action = serde_yaml::from_str(&raw)?;

    // Required-field validation that goes beyond what serde catches with
    // `#[serde(deny_unknown_fields)]` (which already catches typos).
    if action.name.trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: "(unnamed)".into(),
            field: "name".into(),
            path: path.display().to_string(),
        });
    }
    if action.input.command.trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: action.name.clone(),
            field: "input.command".into(),
            path: path.display().to_string(),
        });
    }
    if action.prompt.trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: action.name.clone(),
            field: "prompt".into(),
            path: path.display().to_string(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let yaml_sha256 = hex::encode(hasher.finalize());

    Ok(LoadedAction {
        action,
        path: path.to_path_buf(),
        yaml_sha256,
    })
}

/// Resolve `arg` to a path and load. Convenience for the CLI.
pub fn load(arg: &str, actions_dir: &Path) -> Result<LoadedAction> {
    let path = resolve_action_path(arg, actions_dir).ok_or_else(|| Error::ActionNotFound {
        name: arg.to_string(),
        searched: actions_dir.display().to_string(),
    })?;
    load_from_path(&path)
}
