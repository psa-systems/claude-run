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
//! - **GitHub**: `gh`'s own `hosts.yml`, written by `gh auth login --with-token`
//!   itself (the tool's own writer, not a format this module reproduces) under
//!   `<config_dir>/auth/gh`, pointed at by `GH_CONFIG_DIR` which the daemon
//!   exports at startup next to `YOUTRACK_CLI_CONFIG`.
//! - **Claude**: the API key is handed to [`crate::claude::ClaudeInvoker`] as
//!   `ANTHROPIC_API_KEY` for the child. It is an alternative to an interactive
//!   `claude auth login` inside the container, not a replacement: an OAuth login
//!   in the agent's config dir keeps working and is not touched here.
//!
//! Only the non-secret parts (base URL, host, secret NAMES) live in the
//! `config.auth` settings document. The tokens live encrypted in the secrets
//! store and are read back only here.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::db::DbPool;

use crate::config::{AuthConfig, Paths, DEFAULT_GIT_AUTHOR_EMAIL, DEFAULT_GIT_AUTHOR_NAME};
use crate::error::Result;
use crate::secrets::{resolve_secret, SecretsKey};

/// Env var naming the single-file `yt` config. Exported by the daemon so every
/// `yt` child inherits the configured login.
pub const YT_CONFIG_ENV: &str = "YOUTRACK_CLI_CONFIG";

/// Env var naming `gh`'s own config dir. Exported by the daemon so every `gh`
/// child (the runner's own `GithubForge` calls and agent sessions) inherits
/// the configured login.
pub const GH_CONFIG_ENV: &str = "GH_CONFIG_DIR";

/// Live Anthropic API key, shared with the `claude` invoker. A `std::sync`
/// lock because the reader (`ClaudeInvoker::invoke`) is synchronous.
pub type ApiKeyHandle = Arc<RwLock<Option<String>>>;

/// Where the materialized `yt` config lives. Fixed, so `YOUTRACK_CLI_CONFIG` can
/// be exported once at startup and stay correct as the credential changes.
// SPEC-7.3: a fixed path so the exported env var stays correct across saves.
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
// SPEC-7.3: exported once at serve start, before any child is spawned.
pub fn export_yt_config_env(paths: &Paths) {
    std::env::set_var(YT_CONFIG_ENV, yt_config_path(paths));
}

/// Where the materialized `gh` config lives. Fixed, so [`GH_CONFIG_ENV`] can be
/// exported once at startup and stay correct as the credential changes.
// SPEC-7.3: a fixed path so the exported env var stays correct across saves.
pub fn gh_config_dir(paths: &Paths) -> PathBuf {
    paths.config_dir.join("auth").join("gh")
}

/// Export [`GH_CONFIG_ENV`] for this process and its children. See
/// [`export_yt_config_env`] for why this runs unconditionally at serve start,
/// before any child is spawned, and the same `set_var` safety note applies.
///
/// # Safety
///
/// `std::env::set_var` is not thread-safe against concurrent `getenv`. This
/// runs on the serve path before the daemon spawns anything.
// SPEC-7.3: exported once at serve start, before any child is spawned.
pub fn export_gh_config_env(paths: &Paths) {
    std::env::set_var(GH_CONFIG_ENV, gh_config_dir(paths));
}

/// Where the materialized git identity config lives: the path
/// `GIT_CONFIG_GLOBAL` names in the container image
/// (`oci-build/Dockerfile`: `/config/git/config`, i.e.
/// `<config_dir>/git/config`), so the env var can be exported once at startup
/// and stay correct as the credential changes. A host install leaves
/// `GIT_CONFIG_GLOBAL` unset and resolves git config the normal way; this path
/// only matters once [`export_git_config_env`] points the var at it.
// SPEC-7.3: a fixed path so the exported env var stays correct across saves.
pub fn git_config_path(paths: &Paths) -> PathBuf {
    paths.config_dir.join("git").join("config")
}

/// Export `GIT_CONFIG_GLOBAL` for this process and its children, pointing at
/// [`git_config_path`]. See [`export_yt_config_env`] for why this runs
/// unconditionally at serve start, before any child is spawned, and the same
/// `set_var` safety note applies.
///
/// # Safety
///
/// `std::env::set_var` is not thread-safe against concurrent `getenv`. This
/// runs on the serve path before the daemon spawns anything.
// SPEC-7.3: exported once at serve start, before any child is spawned.
pub fn export_git_config_env(paths: &Paths) {
    std::env::set_var("GIT_CONFIG_GLOBAL", git_config_path(paths));
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
    /// The configured GitHub host, when a token is stored for it.
    pub github_host: Option<String>,
    /// The git commit author name actually in effect: the configured value, or
    /// the `"claude-run"` default when unset. Always present, since a git
    /// identity is always materialized.
    pub git_author_name: String,
    /// The git commit author email actually in effect. See
    /// [`AuthStatus::git_author_name`].
    pub git_author_email: String,
}

/// The XDG data dir `fj` reads its `keys.json` from. Callers pass this in; it is
/// deliberately NOT a `dirs::data_dir()` call inside [`materialize`], because a
/// test that reached for the real one overwrote the developer's own
/// `~/.local/share/forgejo-cli/keys.json` and destroyed two live tokens. Nothing
/// in this module resolves a home directory on its own.
// SPEC-7.3: the XDG data dir `fj` reads, passed in rather than resolved by
// `materialize` itself; see the module-level doc and the no-write-path test
// below for why.
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
// SPEC-7.3: each of the four logins materializes independently; a broken one
// warns and is skipped rather than aborting the rest. The git identity (below)
// is the one credential always written, defaulted when unset.
pub async fn materialize(
    pool: &DbPool,
    paths: &Paths,
    cfg: &AuthConfig,
    fj_data_home: &Path,
    gh_binary: &str,
) -> Result<(AuthStatus, Option<String>)> {
    let mut status = AuthStatus::default();
    let mut api_key = None;

    // Git identity is written unconditionally, defaulted rather
    // than gated on "something else is configured", so a fresh deployment
    // with nothing set in Settings -> Auth still produces a working commit
    // identity with no exec step.
    let git_author_name = cfg
        .git_author_name
        .clone()
        .unwrap_or_else(|| DEFAULT_GIT_AUTHOR_NAME.to_string());
    let git_author_email = cfg
        .git_author_email
        .clone()
        .unwrap_or_else(|| DEFAULT_GIT_AUTHOR_EMAIL.to_string());
    write_git_config(
        &git_config_path(paths),
        &git_author_name,
        &git_author_email,
        cfg.forgejo_host.as_deref(),
    )?;
    status.git_author_name = git_author_name;
    status.git_author_email = git_author_email;

    // Nothing configured: leave every other file alone (an operator may have
    // logged the CLIs in by hand inside the container, and overwriting that
    // would be rude).
    if cfg.anthropic_key_ref.is_none()
        && cfg.youtrack_token_ref.is_none()
        && cfg.forgejo_token_ref.is_none()
        && cfg.github_token_ref.is_none()
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

    if let (Some(name), Some(host)) = (&cfg.github_token_ref, &cfg.github_host) {
        match resolve_secret(pool, &key, name).await {
            Ok(token) if !token.is_empty() => {
                match gh_login(gh_binary, &gh_config_dir(paths), host, &token) {
                    Ok(()) => status.github_host = Some(host.clone()),
                    Err(e) => tracing::warn!(host, error = %e, "gh auth login failed"),
                }
            }
            Ok(_) => tracing::warn!(secret = name, "stored GitHub token is empty; ignoring"),
            Err(e) => tracing::warn!(secret = name, error = %e, "GitHub token not resolvable"),
        }
    }

    Ok((status, api_key))
}

/// Outcome of one `claude auth status --json` probe.
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
/// tokio worker (`crate::blocking::in_place` or `spawn_blocking`).
// SPEC-7.4: loggedIn true/false are decisive; everything else is
// Indeterminate and must never be collapsed into either.
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

/// Single-quote `s` for interpolation into a POSIX shell command line.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Run `gh auth login --hostname <host> --with-token --git-protocol https
/// --insecure-storage`, delivering `token` on `gh`'s stdin via an explicit
/// shell redirection from a throwaway 0600 file (rather than a pipe this
/// process writes to directly): a `timeout(1)` that backgrounds its child
/// (some test doubles do, and a real `timeout` never does) substitutes
/// `/dev/null` for a backgrounded command's own inherited stdin per POSIX,
/// which would silently hand `gh` an empty token; an explicit `<` redirection
/// written on the command itself is unaffected because it is applied fresh
/// when that command runs, regardless of what its parent's stdin was. `GH_CONFIG_DIR`
/// is set to `config_dir` so the login lands there rather than in whatever
/// config `gh` would otherwise resolve. `--insecure-storage` because the
/// container has no keyring. Blocking: callers run this off the tokio worker
/// (`crate::blocking::in_place`).
///
/// A non-zero exit is an error carrying the exit code and `gh`'s stderr, run
/// through [`crate::forge_log::redact`] with the token as a known secret so it
/// never reaches a log line or a rendered page.
// SPEC-7.3: the token travels on stdin via an explicit redirection, never argv
// or a bare pipe; a failure redacts the token from the reported error.
pub fn gh_login(gh_binary: &str, config_dir: &Path, host: &str, token: &str) -> Result<()> {
    std::fs::create_dir_all(config_dir)?;
    std::fs::set_permissions(config_dir, std::fs::Permissions::from_mode(0o700))?;

    let token_path = config_dir.join(format!(
        ".gh-login-token-{}",
        crate::secrets::unique_suffix()
    ));
    crate::secrets::write_0600(&token_path, token.as_bytes())?;
    let cleanup = || {
        let _ = std::fs::remove_file(&token_path);
    };

    let script = format!(
        "exec {} auth login --hostname {} --with-token --git-protocol https --insecure-storage < {}",
        shell_quote(gh_binary),
        shell_quote(host),
        shell_quote(token_path.to_string_lossy().as_ref()),
    );

    let out = std::process::Command::new("timeout")
        .arg("30s")
        .arg("sh")
        .arg("-c")
        .arg(&script)
        .env(GH_CONFIG_ENV, config_dir)
        .stdin(std::process::Stdio::null())
        .output();
    cleanup();
    let out = out.map_err(|e| {
        crate::error::Error::Other(anyhow::anyhow!("spawning {gh_binary} auth login: {e}"))
    })?;

    if out.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let redacted = crate::forge_log::redact(&stderr, &[token.to_string()]);
        Err(crate::error::Error::CliFailed {
            binary: gh_binary.to_string(),
            args: "auth login".to_string(),
            exit_code: out.status.code().unwrap_or(-1),
            stderr: redacted.trim().to_string(),
        })
    }
}

/// Write the single-file `yt` config: `url:` + `token:`, 0600, parent created.
/// Same two-line shape [`crate::secrets::JobCredentials::materialize`] writes for
/// a per-job credential, so there is one format in the tree, not two.
// SPEC-7.3: same two-line 0600 shape the per-job path writes (SPEC-7.2), so
// there is one `yt` config format in the tree.
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

/// Write the `[user]` git config block `GIT_CONFIG_GLOBAL` names, 0600 like
/// [`write_yt_config`]'s output. With a Forgejo host, also scope `fj auth
/// git-credential` to that host so the agent's plain `git` authenticates from
/// `keys.json` (CLAUDE-411). The file never holds a token.
// SPEC-7.3: the Forgejo credential stanza is scoped to one host, and a host
// value that could break out of the config quoting is refused.
fn write_git_config(
    path: &Path,
    name: &str,
    email: &str,
    forgejo_host: Option<&str>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut body = format!("[user]\n\tname = {name}\n\temail = {email}\n");
    if let Some(host) = forgejo_host {
        let host = host.trim();
        // The host lands inside a quoted config subsection header: reject
        // anything that could close it or start a new line.
        if host.is_empty()
            || host
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '"' | '\\' | '[' | ']'))
        {
            return Err(crate::error::Error::Other(anyhow::anyhow!(
                "forgejo_host {host:?} is not a bare host; refusing to write it into the git config"
            )));
        }
        // The empty `helper =` clears any inherited helper list for this host.
        body.push_str(&format!(
            "[credential \"https://{host}\"]\n\thelper =\n\thelper = !fj auth git-credential\n"
        ));
    }
    crate::secrets::write_0600(path, body.as_bytes())?;
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

    /// `loggedIn: true` / `loggedIn: false` are the two decisive
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
    async fn nothing_configured_writes_nothing_but_the_defaulted_git_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let pool = crate::db::test_pool().await;

        let (status, key) = materialize(
            &pool,
            &paths,
            &AuthConfig::default(),
            &tmp.path().join("fj-data"),
            "gh",
        )
        .await
        .unwrap();
        assert_eq!(
            status,
            AuthStatus {
                git_author_name: "claude-run".to_string(),
                git_author_email: "claude-run@a8n.run".to_string(),
                ..AuthStatus::default()
            },
            "git identity is the one login that always defaults; everything \
             else stays unset with nothing configured"
        );
        assert!(key.is_none());
        assert!(
            !yt_config_path(&paths).exists(),
            "an unconfigured instance must not clobber a hand-made login"
        );
        let body = std::fs::read_to_string(git_config_path(&paths)).unwrap();
        assert_eq!(
            body,
            "[user]\n\tname = claude-run\n\temail = claude-run@a8n.run\n"
        );
    }

    /// Both fields configured are used verbatim; only one
    /// configured leaves the other defaulted; the materialized file is 0600.
    #[tokio::test]
    async fn git_identity_configured_fields_are_used_verbatim_and_unset_fields_default() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let pool = crate::db::test_pool().await;

        let cfg = AuthConfig {
            git_author_name: Some("Ops Bot".to_string()),
            git_author_email: None,
            ..Default::default()
        };
        let (status, _key) = materialize(&pool, &paths, &cfg, &tmp.path().join("fj-data"), "gh")
            .await
            .unwrap();
        assert_eq!(status.git_author_name, "Ops Bot");
        assert_eq!(status.git_author_email, "claude-run@a8n.run");
        let body = std::fs::read_to_string(git_config_path(&paths)).unwrap();
        assert_eq!(
            body,
            "[user]\n\tname = Ops Bot\n\temail = claude-run@a8n.run\n"
        );
        let mode = std::fs::metadata(git_config_path(&paths))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        let cfg = AuthConfig {
            git_author_name: Some("Ops Bot".to_string()),
            git_author_email: Some("ops@example.com".to_string()),
            ..Default::default()
        };
        let (status, _key) = materialize(&pool, &paths, &cfg, &tmp.path().join("fj-data"), "gh")
            .await
            .unwrap();
        assert_eq!(status.git_author_name, "Ops Bot");
        assert_eq!(status.git_author_email, "ops@example.com");
        let body = std::fs::read_to_string(git_config_path(&paths)).unwrap();
        assert_eq!(
            body,
            "[user]\n\tname = Ops Bot\n\temail = ops@example.com\n"
        );
    }

    fn forgejo_cfg(host: &str) -> AuthConfig {
        AuthConfig {
            forgejo_host: Some(host.to_string()),
            ..Default::default()
        }
    }

    /// With a Forgejo host the stanza follows the `[user]` block, the file stays
    /// 0600, and a host change rewrites it on the next materialize (CLAUDE-411).
    #[tokio::test]
    async fn forgejo_host_writes_the_credential_stanza_and_a_new_host_rewrites_it() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let pool = crate::db::test_pool().await;
        let fj_data = tmp.path().join("fj-data");

        materialize(
            &pool,
            &paths,
            &forgejo_cfg("forge.example.com"),
            &fj_data,
            "gh",
        )
        .await
        .unwrap();
        let body = std::fs::read_to_string(git_config_path(&paths)).unwrap();
        assert_eq!(
            body,
            "[user]\n\tname = claude-run\n\temail = claude-run@a8n.run\n\
             [credential \"https://forge.example.com\"]\n\thelper =\n\
             \thelper = !fj auth git-credential\n"
        );
        let mode = std::fs::metadata(git_config_path(&paths))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        materialize(
            &pool,
            &paths,
            &forgejo_cfg("other.example.org"),
            &fj_data,
            "gh",
        )
        .await
        .unwrap();
        let body = std::fs::read_to_string(git_config_path(&paths)).unwrap();
        assert!(body.contains("[credential \"https://other.example.org\"]"));
        assert!(!body.contains("forge.example.com"));
    }

    /// A host that could close the quoted subsection header or start a new
    /// config line fails loudly instead of landing in the file.
    #[test]
    fn write_git_config_rejects_a_host_that_is_not_bare() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("git").join("config");
        for bad in [
            "",
            "a\"]\n[core]\n\tpager = x",
            "host name",
            "host]",
            "back\\slash",
        ] {
            let err = write_git_config(&path, "n", "e", Some(bad)).unwrap_err();
            assert!(
                err.to_string().contains("not a bare host"),
                "{bad:?}: {err}"
            );
            assert!(!path.exists(), "{bad:?} must not leave a partial file");
        }
    }

    /// Real `git credential fill` through the written config: the stub `fj`
    /// answers for the configured host, and git never asks it about another.
    #[test]
    fn git_credential_fill_uses_the_fj_helper_for_the_configured_host_only() {
        use std::io::Write;
        use std::process::{Command, Stdio};

        // Reads the process-wide `PATH` below to build the child's env; held
        // for the whole body per `testutil::path_env_lock`'s contract so no
        // other test's concurrent `PATH` mutation can hand this test a torn
        // or wrong value (CLAUDE-461).
        let _path_guard = crate::testutil::path_env_lock();

        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        crate::testutil::write_stub(
            &bin.join("fj"),
            "#!/bin/sh\n\
             [ \"$1 $2 $3\" = \"auth git-credential get\" ] || exit 2\n\
             cat >/dev/null\n\
             printf 'username=fixture-user\\npassword=fixture-pass\\n'\n",
        );
        let cfg = tmp.path().join("git-config");
        write_git_config(&cfg, "n", "e", Some("forge.example.com")).unwrap();
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );

        let fill = |host: &str| {
            let mut cmd = Command::new("git");
            cmd.args(["credential", "fill"])
                .env("PATH", &path)
                .env("GIT_CONFIG_GLOBAL", &cfg)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("XDG_CONFIG_HOME", tmp.path().join("xdg"))
                .env("HOME", tmp.path().join("home"))
                .env("GIT_TERMINAL_PROMPT", "0");
            crate::testutil::clear_ambient_git_env(&mut cmd);
            let mut child = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(format!("protocol=https\nhost={host}\n\n").as_bytes())
                .unwrap();
            child.wait_with_output().unwrap()
        };

        let hit = fill("forge.example.com");
        let out = String::from_utf8_lossy(&hit.stdout);
        assert!(
            hit.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&hit.stderr)
        );
        assert!(out.contains("username=fixture-user"), "{out}");
        assert!(out.contains("password=fixture-pass"), "{out}");

        let miss = fill("elsewhere.example.net");
        assert!(
            !miss.status.success(),
            "a host outside the stanza must get no credential: {}",
            String::from_utf8_lossy(&miss.stdout)
        );
        assert!(!String::from_utf8_lossy(&miss.stdout).contains("fixture-pass"));
    }

    /// `gh_login` passes the documented flag set, points `GH_CONFIG_DIR`
    /// at the given dir, and delivers the token on stdin rather than as an argv
    /// value (which would leak it into `ps`).
    #[test]
    fn gh_login_passes_flags_env_and_stdin_to_the_stub() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("gh-stub.sh");
        let record = dir.path().join("record.txt");
        crate::testutil::write_stub(
            &stub,
            &format!(
                "#!/bin/sh\n{{ printf 'args: %s\\n' \"$*\"; printf 'GH_CONFIG_DIR: %s\\n' \"$GH_CONFIG_DIR\"; printf 'stdin: '; cat; printf '\\n'; }} > \"{}\"\n",
                record.display()
            ),
        );
        let config_dir = dir.path().join("gh-config");

        gh_login(
            stub.to_str().unwrap(),
            &config_dir,
            "github.com",
            "ghp_secret",
        )
        .unwrap();

        let body = std::fs::read_to_string(&record).unwrap();
        assert!(
            body.contains("args: auth login --hostname github.com --with-token --git-protocol https --insecure-storage"),
            "{body}"
        );
        assert!(
            body.contains(&format!("GH_CONFIG_DIR: {}", config_dir.display())),
            "{body}"
        );
        assert!(body.contains("stdin: ghp_secret"), "{body}");
    }

    /// A failing `gh` names the exit code and its stderr, with the token
    /// redacted so it never lands in a log line or a rendered page.
    #[test]
    fn gh_login_failure_carries_exit_code_and_redacts_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("gh-stub.sh");
        crate::testutil::write_stub(
            &stub,
            "#!/bin/sh\ncat >/dev/null\necho 'error: bad credentials for ghp_secret' >&2\nexit 1\n",
        );
        let config_dir = dir.path().join("gh-config");

        let err = gh_login(
            stub.to_str().unwrap(),
            &config_dir,
            "github.com",
            "ghp_secret",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("exit 1"), "{err}");
        assert!(!err.contains("ghp_secret"), "{err}");
        assert!(err.contains("[REDACTED]"), "{err}");
    }

    /// A configured GitHub credential calls `gh_login` and reports the host in
    /// `AuthStatus`; nothing configured still writes nothing (the existing
    /// `nothing_configured_writes_nothing` case above).
    #[tokio::test]
    async fn materialize_with_github_credential_calls_gh_login() {
        use age::secrecy::ExposeSecret;

        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let key_path = tmp.path().join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

        let paths = paths_in(tmp.path());
        let pool = crate::db::test_pool().await;
        let key = SecretsKey::load().unwrap();
        let ciphertext = key.encrypt("ghp_secret").unwrap();
        crate::state::secret_create(&pool, "auth-github-token", &ciphertext)
            .await
            .unwrap();

        let stub = tmp.path().join("gh-stub.sh");
        crate::testutil::write_stub(&stub, "#!/bin/sh\ncat >/dev/null\nexit 0\n");

        let cfg = AuthConfig {
            github_host: Some("github.com".to_string()),
            github_token_ref: Some("auth-github-token".to_string()),
            ..Default::default()
        };

        let (status, _key) = materialize(
            &pool,
            &paths,
            &cfg,
            &tmp.path().join("fj-data"),
            stub.to_str().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(status.github_host.as_deref(), Some("github.com"));

        std::env::remove_var(crate::secrets::KEY_FILE_ENV);
    }
}
