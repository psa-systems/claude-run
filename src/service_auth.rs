//! The daemon's own logins to Claude, YouTrack and Forgejo, entered on the
//! Settings page and materialized into the files and env the CLIs read.
//!
//! Before this the daemon had exactly one way to reach YouTrack or Forgejo for
//! its own scheduler work: whatever the host operator had logged in with, i.e.
//! `~/.config/youtrack-cli` and `~/.local/share/forgejo-cli/keys.json`. A
//! container has neither, so every poll failed with `no instance configured.
//! Run 'yt auth login --base-url <url>'`, and the fix was a CLI the operator was
//! not supposed to need. Now the operator types the credentials into the web UI
//! once and this module writes them where the CLIs look:
//!
//! - **YouTrack**: a single-file `yt` config at `<config_dir>/auth/youtrack.yml`
//!   (`url:` + `token:`, 0600), pointed at by `YOUTRACK_CLI_CONFIG` which the
//!   daemon exports at startup. Every `yt` child inherits it, so no call site
//!   changes. A per-job credential (an action's `youtrack.token_ref`) sets the
//!   same variable in the child's own env and still wins.
//! - **Forgejo**: `keys.json` in the daemon's XDG data dir, the exact file
//!   `fj auth add-key` writes, through the same writer the per-job path uses.
//! - **Claude**: the API key is handed to [`crate::claude::ClaudeInvoker`] as
//!   `ANTHROPIC_API_KEY` for the child. It is an alternative to an interactive
//!   `claude auth login` inside the container, not a replacement: an OAuth login
//!   in the agent's config dir keeps working and is not touched here.
//!
//! Only the non-secret parts (base URL, host, secret NAMES) live in the
//! `config.auth` settings document. The tokens live encrypted in the secrets
//! store and are read back only here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use sqlx::SqlitePool;

use crate::config::{AuthConfig, Paths};
use crate::error::Result;
use crate::secrets::{resolve_secret, SecretsKey};

/// Env var naming the single-file `yt` config. Exported by the daemon so every
/// `yt` child inherits the configured login.
pub const YT_CONFIG_ENV: &str = "YOUTRACK_CLI_CONFIG";

/// Live Anthropic API key, shared with the `claude` invoker. A `std::sync`
/// lock because the reader (`ClaudeInvoker::invoke`) is synchronous.
pub type ApiKeyHandle = Arc<RwLock<Option<String>>>;

/// Where the materialized `yt` config lives. Fixed, so `YOUTRACK_CLI_CONFIG` can
/// be exported once at startup and stay correct as the credential changes.
pub fn yt_config_path(paths: &Paths) -> PathBuf {
    paths.config_dir.join("auth").join("youtrack.yml")
}

/// Export [`YT_CONFIG_ENV`] for this process and its children.
///
/// Called once at serve start, before any child is spawned. Exported even when
/// nothing is configured yet: `yt` treats a missing config file as "no instance
/// configured" (verified against yt 0.13.0), which is the same state the daemon
/// would be in anyway, and it means a credential saved later needs no restart.
///
/// # Safety
///
/// `std::env::set_var` is not thread-safe against concurrent `getenv`. This runs
/// on the serve path before the daemon spawns anything, which is the same
/// discipline the rest of the startup sequence follows.
pub fn export_yt_config_env(paths: &Paths) {
    std::env::set_var(YT_CONFIG_ENV, yt_config_path(paths));
}

/// What is configured, for the Settings page. Never carries a secret value.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct AuthStatus {
    /// An Anthropic API key is stored.
    pub anthropic_key: bool,
    /// The configured YouTrack base URL, when a token is stored for it.
    pub youtrack_base_url: Option<String>,
    /// The configured Forgejo host, when a token is stored for it.
    pub forgejo_host: Option<String>,
}

/// Materialize whatever `config.auth` names into the credential files the CLIs
/// read, and return the Anthropic key for the `claude` invoker.
///
/// A configured credential whose secret cannot be resolved is a `warn` naming
/// the secret, and that one credential is skipped: a broken YouTrack token must
/// not stop the Forgejo login from being written, and neither is worth refusing
/// to start over.
pub async fn materialize(
    pool: &SqlitePool,
    paths: &Paths,
    cfg: &AuthConfig,
) -> Result<(AuthStatus, Option<String>)> {
    let mut status = AuthStatus::default();
    let mut api_key = None;

    // Nothing configured: leave every file alone (an operator may have logged the
    // CLIs in by hand inside the container, and overwriting that would be rude).
    if cfg.anthropic_key_ref.is_none()
        && cfg.youtrack_token_ref.is_none()
        && cfg.forgejo_token_ref.is_none()
    {
        return Ok((status, None));
    }

    let key = SecretsKey::load()?;

    if let Some(name) = &cfg.anthropic_key_ref {
        match resolve_secret(pool, &key, name).await {
            Ok(v) if !v.is_empty() => {
                api_key = Some(v);
                status.anthropic_key = true;
            }
            Ok(_) => tracing::warn!(secret = name, "stored Anthropic API key is empty; ignoring"),
            Err(e) => tracing::warn!(secret = name, error = %e, "Anthropic API key not resolvable"),
        }
    }

    if let (Some(name), Some(url)) = (&cfg.youtrack_token_ref, &cfg.youtrack_base_url) {
        match resolve_secret(pool, &key, name).await {
            Ok(token) if !token.is_empty() => {
                write_yt_config(&yt_config_path(paths), url, &token)?;
                status.youtrack_base_url = Some(url.clone());
            }
            Ok(_) => tracing::warn!(secret = name, "stored YouTrack token is empty; ignoring"),
            Err(e) => tracing::warn!(secret = name, error = %e, "YouTrack token not resolvable"),
        }
    }

    if let (Some(name), Some(host)) = (&cfg.forgejo_token_ref, &cfg.forgejo_host) {
        match resolve_secret(pool, &key, name).await {
            Ok(token) if !token.is_empty() => {
                let data_home = fj_data_home();
                crate::secrets::write_fj_keys(&data_home, host, &token)?;
                status.forgejo_host = Some(host.clone());
            }
            Ok(_) => tracing::warn!(secret = name, "stored Forgejo token is empty; ignoring"),
            Err(e) => tracing::warn!(secret = name, error = %e, "Forgejo token not resolvable"),
        }
    }

    Ok((status, api_key))
}

/// The XDG data dir `fj` reads its `keys.json` from.
fn fj_data_home() -> PathBuf {
    dirs::data_dir().unwrap_or_else(|| PathBuf::from("."))
}

/// Write the single-file `yt` config: `url:` + `token:`, 0600, parent created.
/// Same two-line shape [`crate::secrets::JobCredentials::materialize`] writes for
/// a per-job credential, so there is one format in the tree, not two.
fn write_yt_config(path: &Path, base_url: &str, token: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    crate::secrets::write_0600(
        path,
        format!("url: {}\ntoken: {}\n", base_url.trim(), token).as_bytes(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn paths_in(dir: &Path) -> Paths {
        Paths {
            config_dir: dir.join("config"),
            data_dir: dir.join("data"),
            state_dir: dir.join("state"),
            runtime_dir: dir.join("run"),
        }
    }

    #[test]
    fn yt_config_is_written_owner_only_in_the_shape_yt_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let path = yt_config_path(&paths);

        write_yt_config(&path, "https://yt.example.com", "perm:token").unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "url: https://yt.example.com\ntoken: perm:token\n");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "a file holding a token must be owner-only");
    }

    #[test]
    fn the_config_path_is_stable_so_the_env_can_be_exported_once() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        assert_eq!(
            yt_config_path(&paths),
            paths.config_dir.join("auth").join("youtrack.yml"),
            "the path must not depend on the credential, or a later save would \
             need a restart to be picked up"
        );
    }

    #[tokio::test]
    async fn nothing_configured_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::db::MIGRATOR.run(&pool).await.unwrap();

        let (status, key) = materialize(&pool, &paths, &AuthConfig::default())
            .await
            .unwrap();
        assert_eq!(status, AuthStatus::default());
        assert!(key.is_none());
        assert!(
            !yt_config_path(&paths).exists(),
            "an unconfigured instance must not clobber a hand-made login"
        );
    }
}
