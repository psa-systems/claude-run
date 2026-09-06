//! Whether the daemon has what it needs to run an agent at all.
//!
//! Setup is not finished when the web administrator exists: the runner's whole
//! job is spawning `claude`, and an instance with no Claude credential cannot do
//! it. Before this gate, a fresh container polled YouTrack every tick, logged an
//! error per action per tick, and would have dispatched a run that could only
//! fail at the `claude` invocation. That is noise that hides real failures, and
//! it burns attempts against `max_attempts_per_key`.
//!
//! So the scheduler, the dispatcher and the trigger endpoint all ask here first,
//! exactly as they ask [`crate::pause`]. The two states are deliberately
//! distinct: a pause is an operator decision to stop, and unready is an instance
//! that was never finished. They read differently in the log and on the page.
//!
//! A credential counts when any of these hold, so a host install with an
//! existing login is unaffected:
//!
//! - `<agent config dir>/.credentials.json` exists (on a host that is the
//!   symlink [`crate::agent_home`] points at the operator's login; in a
//!   container it is the file the `/setup` sign-in wrote).
//! - An Anthropic API key is stored (Settings -> Auth).
//! - `ANTHROPIC_API_KEY` is set in the daemon's environment.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::service_auth::ApiKeyHandle;

/// Why the daemon is not ready, phrased for a log line and a page banner.
pub const NOT_READY_REASON: &str =
    "no Claude credential: sign in at Settings -> Auth (or store an API key) to start running actions";

/// The readiness answer plus how the credential was found, for the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    /// Whether an agent can be spawned at all.
    pub ready: bool,
    /// Where the credential came from: `login`, `api_key`, `env`, or `none`.
    pub source: &'static str,
}

impl Readiness {
    /// The operator-facing reason, or `None` when ready.
    pub fn reason(&self) -> Option<&'static str> {
        (!self.ready).then_some(NOT_READY_REASON)
    }
}

/// Shared readiness check. Cheap enough (one `stat` plus a lock read) to call
/// per tick and per dispatch, so there is no cache to go stale: a sign-in
/// completed in the web UI takes effect on the next tick.
#[derive(Clone)]
pub struct ReadinessGate {
    agent_config_dir: PathBuf,
    api_key: Option<ApiKeyHandle>,
    /// Last state reported, so the transition is logged once instead of every
    /// tick. An unready daemon otherwise writes a line every few seconds
    /// forever, which is the log spam this gate exists to remove.
    last_ready: Arc<AtomicBool>,
}

impl ReadinessGate {
    pub fn new(agent_config_dir: PathBuf, api_key: Option<ApiKeyHandle>) -> Self {
        Self {
            agent_config_dir,
            api_key,
            // Start at "was ready" so the first unready check logs.
            last_ready: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A gate that is always ready. For tests and the daemonless `run` path,
    /// where the operator invoked the runner directly and any credential
    /// problem surfaces immediately at the `claude` call.
    pub fn always() -> Self {
        Self {
            agent_config_dir: PathBuf::new(),
            api_key: None,
            last_ready: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Current readiness, without logging.
    pub fn check(&self) -> Readiness {
        if self.agent_config_dir.as_os_str().is_empty() {
            return Readiness {
                ready: true,
                source: "unchecked",
            };
        }
        if credentials_present(&self.agent_config_dir) {
            return Readiness {
                ready: true,
                source: "login",
            };
        }
        let stored_key = self
            .api_key
            .as_ref()
            .and_then(|h| h.read().ok().map(|slot| slot.is_some()))
            .unwrap_or(false);
        if stored_key {
            return Readiness {
                ready: true,
                source: "api_key",
            };
        }
        if std::env::var_os("ANTHROPIC_API_KEY").is_some_and(|v| !v.is_empty()) {
            return Readiness {
                ready: true,
                source: "env",
            };
        }
        Readiness {
            ready: false,
            source: "none",
        }
    }

    /// Readiness for a scheduling decision, logging each transition once.
    pub fn ready(&self) -> bool {
        let now = self.check();
        let was = self.last_ready.swap(now.ready, Ordering::Relaxed);
        if was != now.ready {
            if now.ready {
                tracing::info!(
                    source = now.source,
                    "Claude credential present; scheduling enabled"
                );
            } else {
                tracing::warn!(
                    agent_config_dir = %self.agent_config_dir.display(),
                    "{NOT_READY_REASON}; scheduling is disabled until then"
                );
            }
        }
        now.ready
    }
}

/// Whether a usable credential file sits in `dir`. `exists()` follows symlinks,
/// so the host install's link to `~/.claude/.credentials.json` counts and a
/// dangling link correctly does not.
fn credentials_present(dir: &Path) -> bool {
    dir.join(".credentials.json").exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;

    #[test]
    fn a_credential_file_makes_the_daemon_ready() {
        let tmp = tempfile::tempdir().unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);
        assert!(!gate.check().ready, "nothing is configured yet");
        assert_eq!(gate.check().source, "none");

        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let r = gate.check();
        assert!(r.ready);
        assert_eq!(r.source, "login");
        assert_eq!(r.reason(), None);
    }

    #[test]
    fn a_dangling_symlink_is_not_a_credential() {
        let tmp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(
            tmp.path().join("nowhere.json"),
            tmp.path().join(".credentials.json"),
        )
        .unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);
        assert!(
            !gate.check().ready,
            "a link to a missing host login must not read as authenticated"
        );
    }

    #[test]
    fn a_stored_api_key_makes_the_daemon_ready() {
        let tmp = tempfile::tempdir().unwrap();
        let handle: ApiKeyHandle = Arc::new(RwLock::new(None));
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), Some(Arc::clone(&handle)));
        assert!(!gate.check().ready);

        *handle.write().unwrap() = Some("sk-ant-test".to_string());
        let r = gate.check();
        assert!(r.ready);
        assert_eq!(r.source, "api_key");
    }

    #[test]
    fn the_unready_reason_names_where_to_fix_it() {
        let tmp = tempfile::tempdir().unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);
        let reason = gate.check().reason().unwrap();
        assert!(reason.contains("Settings -> Auth"), "{reason}");
    }

    #[test]
    fn the_always_gate_never_blocks() {
        let gate = ReadinessGate::always();
        assert!(gate.check().ready);
        assert!(gate.ready());
    }
}
