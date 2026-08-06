//! CLAUDE-165: route the spawned `claude` agent to a dedicated user-level config
//! dir so it loads the slim bot memory (`deploy/bot-CLAUDE.md`) instead of the
//! operator's interactive `~/.claude/CLAUDE.md`.
//!
//! Mechanism: the daemon (and the standalone `run` subcommand) seed a config dir
//! and spawn `claude` with `CLAUDE_CONFIG_DIR=<dir>`. Per the Claude Code docs,
//! `CLAUDE_CONFIG_DIR` relocates where Claude Code reads the user-level
//! `CLAUDE.md`, `.credentials.json`, and `settings.json`, and (unlike `--bare`)
//! does NOT disable MCP servers, hooks, or skills. `HOME` stays inherited so
//! git/ssh keep working.
//!
//! Seeding writes `<dir>/CLAUDE.md` from the embedded bot file on every start
//! (so a bot-file update ships with the binary, mirroring the container
//! bake-in) and, on Linux, symlinks `<dir>/.credentials.json` to the host login
//! at `$HOME/.claude/.credentials.json` so an OAuth refresh written by the child
//! persists to the shared host file. On macOS credentials live in the Keychain
//! rather than `.credentials.json`, so the symlink step is Linux-only.

use std::path::{Path, PathBuf};

use crate::config::{expand_path, Paths};
use crate::error::Result;

/// The slim bot memory, embedded so the host deployment and the container image
/// (which `COPY`s the same file to `~/.claude/CLAUDE.md`) share one source of
/// truth for the agent's user-level `CLAUDE.md`.
pub const BOT_CLAUDE_MD: &str = include_str!("../deploy/bot-CLAUDE.md");

/// Resolve the agent's `CLAUDE_CONFIG_DIR`. Uses the configured
/// `[daemon].claude_config_dir` (with `~`/`$VAR` expansion) when set, else
/// defaults to `<config_dir>/claude-agent-home`, self-contained under
/// claude-run's own config tree.
pub fn resolve_config_dir(paths: &Paths, configured: Option<&str>) -> PathBuf {
    match configured {
        Some(raw) => PathBuf::from(expand_path(raw)),
        None => paths.config_dir.join("claude-agent-home"),
    }
}

/// Ensure the agent config dir exists and is seeded. Writes `<dir>/CLAUDE.md`
/// from [`BOT_CLAUDE_MD`] (overwritten each start) and, on Linux, links
/// `<dir>/.credentials.json` to the host login. Errors (loudly) if the host
/// credential file is missing so the daemon does not start an agent that cannot
/// authenticate.
pub fn ensure(dir: &Path) -> Result<()> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("HOME is not set; cannot locate the host claude login"))?;
    ensure_seeded(dir, &home)
}

/// Testable core of [`ensure`]: `home` is injected so the credential-link step
/// can be exercised without touching the process environment.
fn ensure_seeded(dir: &Path, home: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(dir.join("CLAUDE.md"), BOT_CLAUDE_MD)?;
    #[cfg(target_os = "linux")]
    link_credentials(dir, home)?;
    // `home` is only consumed by the Linux credential-link step above.
    #[cfg(not(target_os = "linux"))]
    let _ = home;
    Ok(())
}

/// Symlink `<dir>/.credentials.json` to `$HOME/.claude/.credentials.json` so the
/// child and the host share one credential file (a token refresh written by the
/// child persists to the host login; a copy would silently diverge). The link is
/// recreated each start so it always points at the current host file. A missing
/// host credential file is a hard error naming the path and the login step.
#[cfg(target_os = "linux")]
fn link_credentials(dir: &Path, home: &Path) -> Result<()> {
    let src = home.join(".claude").join(".credentials.json");
    if !src.exists() {
        return Err(anyhow::anyhow!(
            "host claude credentials not found at {}; run `claude` once to log in before starting claude-run \
             (the agent authenticates with the shared host login)",
            src.display()
        )
        .into());
    }
    let link = dir.join(".credentials.json");
    match std::fs::symlink_metadata(&link) {
        Ok(_) => std::fs::remove_file(&link)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    std::os::unix::fs::symlink(&src, &link)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_bot_file_carries_the_local_only_contract() {
        // The embedded file is the actual slim bot memory, not the operator's
        // interactive global file: it states the local-only contract and carries
        // no Git workflow section.
        assert!(BOT_CLAUDE_MD.contains("claude-run bot agent memory"));
        assert!(BOT_CLAUDE_MD.contains("You perform NO external mutation"));
        // CLAUDE-214: the bot file must match the relaxed prompt, so it carries
        // the `No orphan notes` obligation and the `AI Agent` ban that keeps a
        // filing agent from queueing further agent runs.
        assert!(BOT_CLAUDE_MD.contains("No orphan notes"));
        assert!(BOT_CLAUDE_MD.contains("NEVER set the `AI Agent` field on ANY issue"));
        assert!(BOT_CLAUDE_MD.contains(".claude-run-no-code-change"));
    }

    #[test]
    fn resolve_defaults_under_config_dir_when_unset() {
        let paths = Paths {
            config_dir: PathBuf::from("/cfg"),
            data_dir: PathBuf::from("/data"),
            state_dir: PathBuf::from("/state"),
            runtime_dir: PathBuf::from("/run"),
        };
        assert_eq!(
            resolve_config_dir(&paths, None),
            PathBuf::from("/cfg/claude-agent-home")
        );
    }

    #[test]
    fn resolve_uses_configured_value_with_expansion() {
        let paths = Paths {
            config_dir: PathBuf::from("/cfg"),
            data_dir: PathBuf::from("/data"),
            state_dir: PathBuf::from("/state"),
            runtime_dir: PathBuf::from("/run"),
        };
        assert_eq!(
            resolve_config_dir(&paths, Some("/custom/agent-home")),
            PathBuf::from("/custom/agent-home")
        );
    }

    #[test]
    fn seeding_writes_the_bot_claude_md() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        // Provide a host credential file so the Linux link step succeeds.
        let cred = home.path().join(".claude");
        std::fs::create_dir_all(&cred).unwrap();
        std::fs::write(cred.join(".credentials.json"), "{}").unwrap();

        let dir = tmp.path().join("claude-agent-home");
        ensure_seeded(&dir, home.path()).unwrap();

        let written = std::fs::read_to_string(dir.join("CLAUDE.md")).unwrap();
        assert_eq!(written, BOT_CLAUDE_MD);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn seeding_symlinks_credentials_to_host_login() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let cred_dir = home.path().join(".claude");
        std::fs::create_dir_all(&cred_dir).unwrap();
        let host_cred = cred_dir.join(".credentials.json");
        std::fs::write(&host_cred, "{\"token\":1}").unwrap();

        let dir = tmp.path().join("claude-agent-home");
        ensure_seeded(&dir, home.path()).unwrap();

        let link = dir.join(".credentials.json");
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "credentials entry must be a symlink, not a copy"
        );
        assert_eq!(std::fs::read_link(&link).unwrap(), host_cred);
        // Re-seeding is idempotent: the link is recreated, not doubled up.
        ensure_seeded(&dir, home.path()).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), host_cred);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn seeding_errors_when_host_credentials_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap(); // no ~/.claude/.credentials.json
        let dir = tmp.path().join("claude-agent-home");
        let err = ensure_seeded(&dir, home.path()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(".credentials.json") && msg.contains("log in"),
            "error must name the missing credential path and the login step: {msg}"
        );
    }
}
