//! Run a nu snippet with bindings (`$job`, `$workdir`, `$claude_result`, ...)
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

use crate::action::Action;
use crate::db::DbPool;
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
    /// Environment from the action's `env` and `secrets` blocks (CLAUDE-478).
    /// Set on the snippet process only, so the `claude` child never sees it.
    env: Vec<(String, String)>,
    /// Values of the action's `secrets`, scrubbed from snippet output.
    secret_values: Vec<String>,
}

impl SnippetRunner {
    pub fn new(nu_binary: Option<&str>) -> Self {
        Self {
            nu_binary: nu_binary.unwrap_or("nu").to_string(),
            env: Vec::new(),
            secret_values: Vec::new(),
        }
    }

    /// A runner for `action`'s snippets, carrying its `env` and decrypted
    /// `secrets`, or `None` when it declares neither. An unknown secret fails
    /// here, before any job is gathered or attempt counted.
    pub async fn for_action(&self, action: &Action, pool: &DbPool) -> Result<Option<Self>> {
        if action.env.is_empty() && action.secrets.is_empty() {
            return Ok(None);
        }
        let mut env: Vec<(String, String)> = action
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut secret_values = Vec::new();
        if !action.secrets.is_empty() {
            let key = crate::secrets::SecretsKey::load()?;
            for (var, secret) in &action.secrets {
                let value = crate::secrets::resolve_secret(pool, &key, secret).await?;
                secret_values.push(value.clone());
                env.push((var.clone(), value));
            }
        }
        Ok(Some(Self {
            nu_binary: self.nu_binary.clone(),
            env,
            secret_values,
        }))
    }

    /// Replace every secret value in `text`, so a script that prints one
    /// cannot put it in a log, an error or a prompt.
    fn scrub(&self, text: String) -> String {
        self.secret_values
            .iter()
            .filter(|s| !s.is_empty())
            .fold(text, |t, s| t.replace(s.as_str(), "[REDACTED]"))
    }

    /// Write the snippet to `<snippet_dir>/<phase>.<ext>` and execute it with
    /// the declared interpreter. `shell` is the per-field interpreter from the
    /// action's `{run:, shell:}` mapping; `None` (or `"nu"`) selects the
    /// runner's configured nu binary.
    ///
    /// For `nu`, bindings are written to sibling JSON files and loaded by a
    /// generated prelude (`let <name> = (open --raw '<path>' | from json)`).
    /// For any other interpreter the prelude does not apply, so each binding is
    /// exposed instead as an environment variable named after the binding,
    /// holding the compact-JSON value (e.g. `$job`, `$workdir` in a bash
    /// snippet, decode with `jq`).
    pub fn run(
        &self,
        body: &str,
        bindings: &Bindings,
        snippet_dir: &Path,
        phase: &str,
        shell: Option<&str>,
    ) -> Result<SnippetOutcome> {
        std::fs::create_dir_all(snippet_dir)?;

        // None or "nu" -> configured nu binary; anything else is the literal
        // interpreter resolved on PATH.
        let is_nu = matches!(shell, None | Some("nu"));
        let interpreter: &str = if is_nu {
            self.nu_binary.as_str()
        } else {
            shell.unwrap()
        };

        let mut prelude = String::new();
        let mut env_bindings: Vec<(String, String)> = Vec::new();
        for (name, value) in &bindings.values {
            let json = serde_json::to_string(value)?;
            if is_nu {
                let val_path = snippet_dir.join(format!("binding-{name}.json"));
                std::fs::write(&val_path, &json)?;
                // Each binding becomes: let <name> = (open --raw '<path>' | from json)
                prelude.push_str(&format!(
                    "let {name} = (open --raw '{path}' | from json)\n",
                    name = name,
                    path = val_path.display(),
                ));
            } else {
                env_bindings.push((name.clone(), json));
            }
        }

        let ext = if is_nu { "nu" } else { "script" };
        let script_path = snippet_dir.join(format!("{phase}.{ext}"));
        let script = if is_nu {
            format!("{prelude}\n{body}")
        } else {
            body.to_string()
        };
        std::fs::write(&script_path, script)?;

        // A scripted action's nu snippet is arbitrary operator code
        // with no time bound; keep it off the async worker thread.
        let output = crate::blocking::in_place(|| {
            Command::new(interpreter)
                .arg(&script_path)
                .envs(self.env.iter().map(|(k, v)| (k, v)))
                .envs(env_bindings)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
        })?;

        Ok(SnippetOutcome {
            ok: output.status.success(),
            stdout: self.scrub(String::from_utf8_lossy(&output.stdout).into_owned()),
            stderr: self.scrub(String::from_utf8_lossy(&output.stderr).into_owned()),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

/// Compute the per-job snippet directory:
/// `<state_dir>/snippets/<action_name>/<job_key>/`.
///
/// This directory is bounded by a periodic AGE sweep
/// ([`crate::gc::sweep_snippets`]), not deleted at the end of `process_job`
/// beside the workdir: unlike the workdir, a snippet directory is small (one
/// rendered script plus a JSON binding file per step) and is the only place a
/// failed `filter` / `pre` / `prompt` / `post` step's rendered nu source and
/// inputs survive for debugging after the job ends, so deleting it immediately
/// would remove diagnostic value the age window instead preserves for a while.
pub fn snippet_dir_for(state_dir: &Path, action_name: &str, job_key: &str) -> PathBuf {
    state_dir.join("snippets").join(action_name).join(job_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn snippet_sees_job_binding() {
        // Use `sh` (guaranteed present, including in the minimal alpine/musl dev
        // image used by `just pre-commit`) instead of `nu`, which is a host
        // runtime dependency not installed in that image. For non-nu shells each
        // binding arrives as an env var holding the compact-JSON value, so the
        // snippet echoes `$job` and we assert on the serialized form.
        let dir = tempdir().unwrap();
        let runner = SnippetRunner::new(None);
        let mut b = Bindings::new();
        b.insert("job", json!({"id": "hello"}));
        let out = runner
            .run(r#"printf '%s' "$job""#, &b, dir.path(), "test", Some("sh"))
            .unwrap();
        assert!(out.ok, "stderr: {}", out.stderr);
        assert_eq!(
            out.stdout.trim(),
            r#"{"id":"hello"}"#,
            "got {:?}",
            out.stdout
        );
    }

    #[test]
    fn snippet_exit_code_propagates() {
        // `sh` rather than `nu`: see `snippet_sees_job_binding` for why. Exit-code
        // propagation is shell-agnostic, so this also covers the nu path.
        let dir = tempdir().unwrap();
        let runner = SnippetRunner::new(None);
        let out = runner
            .run(
                r#"exit 7"#,
                &Bindings::new(),
                dir.path(),
                "fail",
                Some("sh"),
            )
            .unwrap();
        assert!(!out.ok);
        assert_eq!(out.exit_code, 7);
    }

    #[test]
    fn snippet_dispatches_to_declared_shell() {
        let dir = tempdir().unwrap();
        let runner = SnippetRunner::new(None);
        // Non-nu interpreter: bindings arrive as env vars holding compact JSON.
        let mut b = Bindings::new();
        b.insert("job", json!({"id": "hello"}));
        let out = runner
            .run(r#"echo "$job""#, &b, dir.path(), "test", Some("bash"))
            .unwrap();
        assert!(out.ok, "stderr: {}", out.stderr);
        assert_eq!(out.stdout.trim(), r#"{"id":"hello"}"#);
    }

    /// CLAUDE-478: the snippet sees the action's env and secrets, a printed
    /// secret comes back redacted on both streams, and it is on no file.
    #[test]
    fn action_env_reaches_the_snippet_and_secrets_are_scrubbed() {
        let dir = tempdir().unwrap();
        let runner = SnippetRunner {
            nu_binary: "nu".into(),
            env: vec![
                ("SITE".into(), "news.example.com".into()),
                ("TOKEN".into(), "tok_supersecret".into()),
            ],
            secret_values: vec!["tok_supersecret".into()],
        };
        let out = runner
            .run(
                r#"echo "$SITE $TOKEN"; echo "err $TOKEN" >&2"#,
                &Bindings::new(),
                dir.path(),
                "test",
                Some("bash"),
            )
            .unwrap();
        assert!(out.ok, "stderr: {}", out.stderr);
        assert_eq!(out.stdout.trim(), "news.example.com [REDACTED]");
        assert_eq!(out.stderr.trim(), "err [REDACTED]");
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let body = std::fs::read_to_string(entry.unwrap().path()).unwrap();
            assert!(!body.contains("tok_supersecret"), "{body}");
        }
    }
}
