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

use std::collections::BTreeMap;
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
    /// CLAUDE-46: triage-driven issue-mint settings (repo-to-project map,
    /// fallback project, loop-safety bot logins, bucket-2 assignee).
    pub mint: MintConfig,
    /// CLAUDE-55: `version` self-update settings (release/latest train default
    /// and the name of the secret holding the registry token).
    pub update: UpdateConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub bind: String,
    pub port: u16,
    /// Phase 5 (CLAUDE-48): name of the stored secret (CLAUDE-26) holding the
    /// bearer token that gates the web UI and `/api/*` when the bind is
    /// non-loopback. Unset is fine for a loopback bind; required (else the TCP
    /// listener fails closed) for any exposed bind.
    pub auth_token_ref: Option<String>,
    /// Phase 5 (CLAUDE-48): require the bearer token even on a loopback bind.
    /// Off by default so local development stays unauthenticated; set true to
    /// exercise the gate locally.
    pub require_auth_on_loopback: bool,
    /// Username for the browser login form. When set (and auth is required), an
    /// unauthenticated browser request is redirected to `/login`, where this
    /// username plus the `auth_token_ref` secret value (as the password) mint a
    /// session cookie. Lets a password manager (e.g. Bitwarden) sign in without
    /// crafting an `Authorization: Bearer` header. Unset disables the form;
    /// `Authorization: Bearer <token>` still works for API clients.
    pub auth_username: Option<String>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 7321,
            auth_token_ref: None,
            require_auth_on_loopback: false,
            auth_username: None,
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

/// CLAUDE-46: settings for minting (or updating) a YouTrack issue from a triaged
/// failure. Lives in `config.toml` because the daemon's webhook consumer needs it
/// at startup. The repo-to-project routing decision (2026-05-31) sends an issue to
/// the repo's own YouTrack project, with `fallback_project` (CLAUDE) for any repo
/// not in `repo_project_map`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct MintConfig {
    /// Map of `owner/repo` full name to the YouTrack project short name the
    /// minted issue should land in, e.g. `fabrikam/forgejo-cli` -> `FJ`.
    /// A repo absent from this map falls back to [`MintConfig::fallback_project`].
    pub repo_project_map: BTreeMap<String, String>,
    /// Project short name used for any repo not in `repo_project_map`.
    pub fallback_project: String,
    /// Loop-safety allow-list of trigger-user logins whose failing runs never
    /// mint a task. The CI-fix bot pushes fixes as itself, so a failing fix-PR
    /// run is triggered by one of these logins; minting from it would recurse.
    pub bot_logins: Vec<String>,
    /// Bucket-2 (human-action) assignee login. `None` leaves the minted issue
    /// unassigned: the project is watched, so a human is still notified.
    pub human_assignee: Option<String>,
    /// `State` value set on a freshly minted issue. Default `To do`.
    pub target_state: String,
    /// `AI Agent` custom-field name driven on the minted issue.
    pub ai_agent_field: String,
    /// `AI Agent` value set on a bucket-1 (code-fix) issue so the existing
    /// forgejo-ci-fix pickup logic (CLAUDE-29) opens the fix PR. Default `Queued`.
    pub queued_value: String,
}

impl Default for MintConfig {
    fn default() -> Self {
        Self {
            repo_project_map: BTreeMap::new(),
            fallback_project: "CLAUDE".to_string(),
            bot_logins: vec!["claude-run".to_string()],
            human_assignee: None,
            target_state: "To do".to_string(),
            ai_agent_field: "AI Agent".to_string(),
            queued_value: "Queued".to_string(),
        }
    }
}

/// CLAUDE-55: settings for the `version` self-update command. Lives in
/// `config.toml` so an operator can pin a default channel (and reference a
/// registry token) without passing flags every time. The token itself is never
/// inlined here; `token_ref` names a credential in the CLAUDE-26 `secrets`
/// table, resolved (and decrypted) at run time and used for the registry list /
/// asset requests when private packages require auth. The `train` string is
/// validated into [`crate::update::UpdateTrain`] when the command runs.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct UpdateConfig {
    /// Default update channel: `release` (newest semver `vX.Y.Z`) or `latest`
    /// (the moving `latest` main build). Overridable per invocation by
    /// `version {check,update} --train <release|latest>`.
    pub train: String,
    /// Name of the secret (in the `secrets` table) holding the registry token.
    /// When unset, the updater stays anonymous-first and falls back to the `fj`
    /// token only on a 401/403 with `--use-fj-token`.
    pub token_ref: Option<String>,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            train: "release".to_string(),
            token_ref: None,
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

    #[test]
    fn update_config_defaults_to_release_train_no_token() {
        let cfg = UpdateConfig::default();
        assert_eq!(cfg.train, "release");
        assert!(cfg.token_ref.is_none());
        // An empty config still yields the release default.
        let parsed: OnDiskConfig = toml::from_str("").unwrap();
        assert_eq!(parsed.update, UpdateConfig::default());
    }

    #[test]
    fn update_block_parses_train_and_token_ref() {
        let parsed: OnDiskConfig = toml::from_str(
            r#"
            [update]
            train = "latest"
            token_ref = "registry-token"
            "#,
        )
        .unwrap();
        assert_eq!(parsed.update.train, "latest");
        assert_eq!(parsed.update.token_ref.as_deref(), Some("registry-token"));
    }
}
