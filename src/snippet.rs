//! Run a nu snippet with bindings (`$row`, `$workdir`, `$claude_result`, ...)
//! injected via a prelude.
//!
//! Each binding is serialized to JSON and written to a tempfile next to the
//! snippet. The prelude reads those files and `let`-binds the values - this
//! avoids any quoting issues with embedding values inside nu source.
//!
//! Matches the semantics of the old `snippet-run` / `snippet-write` defs in
//! `scripts/claude-run.nu`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;

use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct Bindings {
    /// Ordered for reproducibility of the generated prelude. Each value is
    /// serialized to a tempfile and surfaced as a nu `let`.
    pub values: BTreeMap<String, Value>,
}

impl Bindings {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert<S: Into<String>>(&mut self, name: S, value: Value) -> &mut Self {
        self.values.insert(name.into(), value);
        self
    }
}

#[derive(Debug)]
pub struct SnippetOutcome {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub struct SnippetRunner {
    nu_binary: String,
}

impl SnippetRunner {
    pub fn new(nu_binary: Option<&str>) -> Self {
        Self {
            nu_binary: nu_binary.unwrap_or("nu").to_string(),
        }
    }

    /// Write the snippet (with a binding-loading prelude) to `<snippet_dir>/<phase>.nu`
    /// and execute it. Bindings are stored in sibling JSON files.
    pub fn run(
        &self,
        body: &str,
        bindings: &Bindings,
        snippet_dir: &Path,
        phase: &str,
    ) -> Result<SnippetOutcome> {
        std::fs::create_dir_all(snippet_dir)?;

        let mut prelude = String::new();
        for (name, value) in &bindings.values {
            let val_path = snippet_dir.join(format!("binding-{name}.json"));
            let json = serde_json::to_string(value)?;
            std::fs::write(&val_path, json)?;
            // Each binding becomes: let <name> = (open --raw '<path>' | from json)
            prelude.push_str(&format!(
                "let {name} = (open --raw '{path}' | from json)\n",
                name = name,
                path = val_path.display(),
            ));
        }

        let script_path = snippet_dir.join(format!("{phase}.nu"));
        let script = format!("{prelude}\n{body}");
        std::fs::write(&script_path, script)?;

        let output = Command::new(&self.nu_binary)
            .arg(&script_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        Ok(SnippetOutcome {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

/// Compute the per-row snippet directory:
/// `<state_dir>/snippets/<action_name>/<row_key>/`.
pub fn snippet_dir_for(state_dir: &Path, action_name: &str, row_key: &str) -> PathBuf {
    state_dir.join("snippets").join(action_name).join(row_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn snippet_sees_row_binding() {
        let dir = tempdir().unwrap();
        let runner = SnippetRunner::new(None);
        let mut b = Bindings::new();
        b.insert("row", json!({"id": "hello"}));
        let out = runner
            .run(r#"print $row.id"#, &b, dir.path(), "test")
            .unwrap();
        assert!(out.ok, "stderr: {}", out.stderr);
        assert!(out.stdout.trim() == "hello", "got {:?}", out.stdout);
    }

    #[test]
    fn snippet_exit_code_propagates() {
        let dir = tempdir().unwrap();
        let runner = SnippetRunner::new(None);
        let out = runner
            .run(r#"exit 7"#, &Bindings::new(), dir.path(), "fail")
            .unwrap();
        assert!(!out.ok);
        assert_eq!(out.exit_code, 7);
    }
}
