//! Action YAML loader.
//!
//! Schema mirrors `actions/SCHEMA.md`. Each script-bearing field holds opaque
//! code plus an optional interpreter; the runner shells out to that interpreter
//! (default `nu`) to execute it (see `crate::snippet`).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// A script-bearing field (`input.command`, `filter`, each `pre`, `prompt`,
/// each `post`). Accepted in two forms:
///
/// - a plain YAML string (implicit `shell: nu`) for back-compat, or
/// - a `{run: <code>, shell: <interpreter>}` mapping, following Docker
///   Compose's `run` / `shell` convention.
///
/// `shell` defaults to `nu` when omitted. The interpreter is a bare binary
/// name resolved on `PATH` (no arguments).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Script {
    /// Plain string form: code with the default `nu` interpreter.
    Plain(String),
    /// Explicit mapping form: code plus an optional interpreter.
    Mapping {
        run: String,
        #[serde(default)]
        shell: Option<String>,
    },
}

impl Script {
    /// The code to execute.
    pub fn code(&self) -> &str {
        match self {
            Script::Plain(s) => s,
            Script::Mapping { run, .. } => run,
        }
    }

    /// The declared interpreter, or `None` to use the runner's default (`nu`).
    pub fn shell(&self) -> Option<&str> {
        match self {
            Script::Plain(_) => None,
            Script::Mapping { shell, .. } => shell.as_deref(),
        }
    }
}

impl From<String> for Script {
    fn from(s: String) -> Self {
        Script::Plain(s)
    }
}

impl From<&str> for Script {
    fn from(s: &str) -> Self {
        Script::Plain(s.to_string())
    }
}

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
    pub filter: Option<Script>,

    #[serde(default)]
    pub pre: Vec<Script>,

    pub prompt: Script,

    #[serde(default)]
    pub claude: ClaudeBlock,

    #[serde(default)]
    pub post: Vec<Script>,

    #[serde(default)]
    pub output: OutputBlock,

    #[serde(default)]
    pub limits: Limits,

    #[serde(default)]
    pub schedule: Option<Schedule>,

    /// Optional URL template for rendering job keys as clickable links in the
    /// web UI. `{key}` is substituted with the job key. Example:
    /// `https://example.myjetbrains.com/issue/{key}`.
    #[serde(default)]
    pub link_template: Option<String>,

    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

/// Substitute `{key}` in a template string. Other `{...}` tokens pass through
/// unchanged so templates can include literal braces if they need to.
pub fn render_link(template: &str, key: &str) -> String {
    template.replace("{key}", key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_link_substitutes_key() {
        assert_eq!(
            render_link("https://example.myjetbrains.com/issue/{key}", "MK-16"),
            "https://example.myjetbrains.com/issue/MK-16"
        );
    }

    #[test]
    fn render_link_leaves_unknown_tokens_alone() {
        assert_eq!(render_link("/{repo}/{key}", "PR-42"), "/{repo}/PR-42");
    }

    #[test]
    fn script_field_accepts_string_and_mapping_forms() {
        let yaml = r#"
name: t
input:
  command: "echo []"
filter:
  run: "exit 0"
  shell: bash
pre:
  - "print pre"
prompt:
  run: "print hi"
post:
  - run: "echo done"
    shell: sh
"#;
        let action: Action = serde_yaml::from_str(yaml).unwrap();

        // Plain string -> default interpreter (nu).
        assert_eq!(action.input.command.code(), "echo []");
        assert_eq!(action.input.command.shell(), None);

        // Mapping with explicit shell.
        let filter = action.filter.as_ref().unwrap();
        assert_eq!(filter.code(), "exit 0");
        assert_eq!(filter.shell(), Some("bash"));

        // List of plain strings.
        assert_eq!(action.pre[0].code(), "print pre");
        assert_eq!(action.pre[0].shell(), None);

        // Mapping with omitted shell -> default (nu).
        assert_eq!(action.prompt.code(), "print hi");
        assert_eq!(action.prompt.shell(), None);

        // List of mappings.
        assert_eq!(action.post[0].code(), "echo done");
        assert_eq!(action.post[0].shell(), Some("sh"));
    }

    const MINIMAL: &str = "name: demo\ninput:\n  command: echo '[]'\nprompt: do the thing\n";

    #[test]
    fn validate_str_accepts_minimal_action() {
        let loaded = validate_str(MINIMAL, "<candidate>").expect("valid");
        assert_eq!(loaded.action.name, "demo");
        assert_eq!(loaded.yaml_sha256.len(), 64);
    }

    #[test]
    fn validate_str_rejects_missing_prompt() {
        let raw = "name: demo\ninput:\n  command: echo '[]'\nprompt: \"\"\n";
        let err = validate_str(raw, "<candidate>").unwrap_err();
        assert!(matches!(err, Error::ActionMissingField { field, .. } if field == "prompt"));
    }

    #[test]
    fn validate_str_rejects_unknown_field() {
        let raw = format!("{MINIMAL}bogus: nope\n");
        assert!(matches!(
            validate_str(&raw, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputBlock {
    pub command: Script,
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
    let mut loaded = validate_str(&raw, &path.display().to_string())?;
    loaded.path = path.to_path_buf();
    Ok(loaded)
}

/// Validate raw YAML as an [`Action`] without touching disk, computing the
/// same sha256 provenance [`load_from_path`] does. `label` names the candidate
/// source in error messages (e.g. the would-be file path); the returned
/// `path` is set to it but callers that persist should overwrite it with the
/// real destination. Used by the web UI to reject broken YAML before it ever
/// reaches `actions/<name>.yml`.
pub fn validate_str(raw: &str, label: &str) -> Result<LoadedAction> {
    let action: Action = serde_yaml::from_str(raw)?;

    // Required-field validation that goes beyond what serde catches with
    // `#[serde(deny_unknown_fields)]` (which already catches typos).
    if action.name.trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: "(unnamed)".into(),
            field: "name".into(),
            path: label.to_string(),
        });
    }
    if action.input.command.code().trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: action.name.clone(),
            field: "input.command".into(),
            path: label.to_string(),
        });
    }
    if action.prompt.code().trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: action.name.clone(),
            field: "prompt".into(),
            path: label.to_string(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let yaml_sha256 = hex::encode(hasher.finalize());

    Ok(LoadedAction {
        action,
        path: PathBuf::from(label),
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
