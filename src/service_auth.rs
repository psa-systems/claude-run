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

use crate::db::DbPool;

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

/// The XDG data dir `fj` reads its `keys.json` from. Callers pass this in; it is
/// deliberately NOT a `dirs::data_dir()` call inside [`materialize`], because a
/// test that reached for the real one overwrote the developer's own
/// `~/.local/share/forgejo-cli/keys.json` and destroyed two live tokens. Nothing
/// in this module resolves a home directory on its own.
pub fn default_fj_data_home() -> PathBuf {
    dirs::data_dir().unwrap_or_else(|| PathBuf::from("."))
}

/// Materialize whatever `config.auth` names into the credential files the CLIs
/// read, and return the Anthropic key for the `claude` invoker.
///
/// A configured credential whose secret cannot be resolved is a `warn` naming
/// the secret, and that one credential is skipped: a broken YouTrack token must
/// not stop the Forgejo login from being written, and neither is worth refusing
/// to start over.
pub async fn materialize(
    pool: &DbPool,
    paths: &Paths,
    cfg: &AuthConfig,
    fj_data_home: &Path,
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
                crate::secrets::write_fj_keys(fj_data_home, host, &token)?;
                status.forgejo_host = Some(host.clone());
            }
            Ok(_) => tracing::warn!(secret = name, "stored Forgejo token is empty; ignoring"),
            Err(e) => tracing::warn!(secret = name, error = %e, "Forgejo token not resolvable"),
        }
    }

    Ok((status, api_key))
}

/// CLAUDE-269: outcome of one `claude auth status --json` probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginProbeOutcome {
    /// `{"loggedIn": true, ...}`.
    LoggedIn,
    /// `{"loggedIn": false, ...}`.
    LoggedOut,
    /// The probe could not spawn, timed out, or printed something that did not
    /// parse as the expected JSON shape. Neither logged in nor logged out: the
    /// caller must change nothing on this outcome rather than guess.
    Indeterminate(String),
}

/// Run `claude auth status --json` under the agent's own config dir (and its
/// `ANTHROPIC_API_KEY`, when one is stored), with a 30s wall-clock timeout via
/// the same coreutils `timeout(1)` wrapper [`crate::claude::ClaudeInvoker`]
/// uses. Blocking (a real subprocess spawn and wait): callers run this off the
/// tokio worker (`crate::blocking::in_place` or `spawn_blocking`), per
/// CLAUDE-228.
pub fn probe_claude_login(
    claude_binary: &str,
    config_dir: &Path,
    api_key: Option<&str>,
) -> LoginProbeOutcome {
    let mut cmd = std::process::Command::new("timeout");
    cmd.arg("30s")
        .arg(claude_binary)
        .arg("auth")
        .arg("status")
        .arg("--json")
        .env("CLAUDE_CONFIG_DIR", config_dir)
        .stdin(std::process::Stdio::null());
    if let Some(key) = api_key {
        cmd.env("ANTHROPIC_API_KEY", key);
    }
    let out = match cmd.output() {
        Ok(out) => out,
        Err(e) => return LoginProbeOutcome::Indeterminate(format!("spawn failed: {e}")),
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    match serde_json::from_str::<serde_json::Value>(stdout.trim()) {
        Ok(v) => match v.get("loggedIn").and_then(|b| b.as_bool()) {
            Some(true) => LoginProbeOutcome::LoggedIn,
            Some(false) => LoginProbeOutcome::LoggedOut,
            None => LoginProbeOutcome::Indeterminate(format!("no `loggedIn` field in: {stdout}")),
        },
        Err(e) => LoginProbeOutcome::Indeterminate(format!("unparseable output ({e}): {stdout}")),
    }
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

    /// Every path this module writes is derived from an argument, never from
    /// `dirs::data_dir()` or `$HOME`. A test that reached for the real Forgejo
    /// data dir once overwrote the developer's own `keys.json` and destroyed two
    /// live tokens; this asserts the only home-resolving helper is the explicit
    /// one callers opt into.
    #[test]
    fn no_write_path_resolves_a_home_directory_on_its_own() {
        let src = include_str!("service_auth.rs");
        let mut in_resolver = false;
        for (n, line) in src.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            if code.contains("fn default_fj_data_home") {
                in_resolver = true;
            } else if in_resolver && line == "}" {
                in_resolver = false;
            }
            if code.contains("dirs::data_dir") || code.contains("dirs::home_dir") {
                assert!(
                    in_resolver,
                    "line {}: home resolution outside default_fj_data_home: {line}",
                    n + 1
                );
            }
        }
        // The one permitted resolver lives in its own function, so a caller that
        // wants the real location has to say so.
        assert!(src.contains("pub fn default_fj_data_home"));
    }

    /// CLAUDE-269: `loggedIn: true` / `loggedIn: false` are the two decisive
    /// outcomes; anything else (a spawn failure, a timeout, unparseable
    /// output) is `Indeterminate` and must never be collapsed into either.
    #[test]
    fn probe_reads_logged_in_and_logged_out() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        crate::testutil::write_stub(
            &stub,
            "#!/bin/sh\nprintf '%s' '{\"loggedIn\":true,\"authMethod\":\"oauth\"}'\n",
        );
        assert_eq!(
            probe_claude_login(stub.to_str().unwrap(), dir.path(), None),
            LoginProbeOutcome::LoggedIn
        );

        crate::testutil::write_stub(&stub, "#!/bin/sh\nprintf '%s' '{\"loggedIn\":false}'\n");
        assert_eq!(
            probe_claude_login(stub.to_str().unwrap(), dir.path(), None),
            LoginProbeOutcome::LoggedOut
        );
    }

    /// A spawn failure (binary absent) is indeterminate, not logged-out.
    #[test]
    fn probe_reports_a_missing_binary_as_indeterminate() {
        let dir = tempfile::tempdir().unwrap();
        let outcome = probe_claude_login(
            dir.path().join("no-such-claude-binary").to_str().unwrap(),
            dir.path(),
            None,
        );
        assert!(
            matches!(outcome, LoginProbeOutcome::Indeterminate(_)),
            "{outcome:?}"
        );
    }

    /// Unparseable output (a crash, a non-JSON error message) is indeterminate,
    /// never guessed as either logged-in or logged-out state.
    #[test]
    fn probe_reports_unparseable_output_as_indeterminate() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        crate::testutil::write_stub(&stub, "#!/bin/sh\nprintf 'not json'\nexit 1\n");
        let outcome = probe_claude_login(stub.to_str().unwrap(), dir.path(), None);
        assert!(
            matches!(outcome, LoginProbeOutcome::Indeterminate(_)),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn nothing_configured_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let pool = crate::db::test_pool().await;

        let (status, key) = materialize(
            &pool,
            &paths,
            &AuthConfig::default(),
            &tmp.path().join("fj-data"),
        )
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
