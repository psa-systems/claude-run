//! XDG-aware path resolution + on-disk config.
//!
//! Layout (defaults; each can be overridden by the corresponding env var):
//!
//! | Purpose | Default | Env override |
//! |---|---|---|
//! | Config (actions, config.toml) | `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR` |
//! | Data (state.db, workdirs) | `$XDG_DATA_HOME/claude-run/` | `CLAUDE_RUN_DATA_DIR` |
//! | State (logs) | `$XDG_STATE_HOME/claude-run/` | `CLAUDE_RUN_STATE_DIR` |
//! | Runtime (pid, socket) | `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` |
//!
//! `~`, `$VAR`, and `${VAR}` are expanded in all values (the env override and
//! the `workdir_base` field on actions both go through `expand_path`).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Resolved on-disk locations. Computed once at startup.
#[derive(Debug, Clone)]
pub struct Paths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
}

impl Paths {
    pub fn resolve() -> Result<Self> {
        Ok(Self {
            config_dir: resolve_dir("CLAUDE_RUN_CONFIG_DIR", dirs::config_dir(), "claude-run")?,
            data_dir: resolve_dir("CLAUDE_RUN_DATA_DIR", dirs::data_dir(), "claude-run")?,
            state_dir: resolve_dir("CLAUDE_RUN_STATE_DIR", dirs::state_dir(), "claude-run")?,
            runtime_dir: resolve_dir(
                "CLAUDE_RUN_RUNTIME_DIR",
                dirs::runtime_dir()
                    .or_else(|| Some(std::env::temp_dir().join("claude-run-runtime"))),
                "claude-run",
            )?,
        })
    }

    pub fn actions_dir(&self) -> PathBuf {
        self.config_dir.join("actions")
    }

    pub fn config_file(&self) -> PathBuf {
        self.config_dir.join("config.toml")
    }

    pub fn state_db(&self) -> PathBuf {
        self.data_dir.join("state.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.state_dir.join("logs")
    }

    pub fn workdirs_dir(&self) -> PathBuf {
        self.data_dir.join("workdirs")
    }
}

fn resolve_dir(env_var: &str, xdg_default: Option<PathBuf>, subdir: &str) -> Result<PathBuf> {
    if let Ok(raw) = std::env::var(env_var) {
        return Ok(PathBuf::from(expand_path(&raw)));
    }
    let base =
        xdg_default.ok_or_else(|| anyhow::anyhow!("could not resolve XDG dir for {}", env_var))?;
    Ok(base.join(subdir))
}

/// Expand `~`, `$VAR`, and `${VAR}` in a path string. No nu evaluation.
/// Used for the `workdir_base` action field and the env-override values.
pub fn expand_path(raw: &str) -> String {
    shellexpand::full(raw)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| raw.to_string())
}

/// `config.toml` schema. All fields optional; absence means "use built-in default".
/// Runtime-tunable settings live in the DB (`settings` table); this file is
/// for things that have to be known before the DB is opened.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnDiskConfig {
    pub web: WebConfig,
    pub daemon: DaemonConfig,
    /// CLAUDE-43: Forgejo webhook listener settings (HMAC secret reference,
    /// repo allow-list, replay window, rate limit).
    pub webhook: WebhookConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub bind: String,
    pub port: u16,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 7321,
        }
    }
}

/// CLAUDE-43: Forgejo webhook listener config. Lives in `config.toml` because
/// the daemon needs it before it binds the TCP listener. The HMAC secret itself
/// is never inlined here; `secret_ref` names a credential in the CLAUDE-26
/// `secrets` table, resolved (and decrypted) at request time.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
    /// Name of the secret (in the `secrets` table) holding the Forgejo webhook
    /// HMAC secret. When unset, the `/webhooks/forgejo` endpoint fails closed
    /// and rejects every request with 401.
    pub secret_ref: Option<String>,
    /// Allow-list of `owner/repo` full names. Only `action_run_failure` events
    /// from a repo on this list produce a verified-failure event; every other
    /// authenticated request is acked (2xx) and dropped so Forgejo does not retry.
    pub allowed_repos: Vec<String>,
    /// Replay-protection window in seconds. A delivery whose `X-Forgejo-Delivery`
    /// id was already seen inside this window is rejected with 409.
    pub replay_window_secs: u64,
    /// Fixed-window rate limit: max accepted requests per minute across the
    /// endpoint. Excess requests get 429 before any crypto work.
    pub rate_limit_per_min: u32,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            secret_ref: None,
            allowed_repos: Vec::new(),
            replay_window_secs: 300,
            rate_limit_per_min: 60,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// Optional override for the nu binary. Default: "nu" (looked up on PATH).
    pub nu_binary: Option<String>,
    /// Optional override for the claude binary. Default: "claude".
    pub claude_binary: Option<String>,
}

impl OnDiskConfig {
    pub fn load(paths: &Paths) -> Result<Self> {
        let f = paths.config_file();
        if !f.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(&f)?;
        let cfg: Self =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {}", f.display(), e))?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_handles_tilde_and_env() {
        std::env::set_var("CLAUDE_RUN_TEST_FOO", "bar");
        let out = expand_path("$CLAUDE_RUN_TEST_FOO/x");
        assert_eq!(out, "bar/x");
        let out = expand_path("${CLAUDE_RUN_TEST_FOO}/x");
        assert_eq!(out, "bar/x");
        let out = expand_path("~/x");
        assert!(out.starts_with('/'));
    }
}
