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
//!
//! CLAUDE-269 adds a second, independent not-ready axis: a login that WAS
//! present but has expired or been revoked. `.credentials.json` stays on disk in
//! that case, so the credential check above keeps answering ready; nothing here
//! reads the file's own expiry (see the CLAUDE-269 "Alternatives considered" for
//! why). Instead, [`ReadinessGate::trip_login_required`] is called from every
//! place that actually talks to the CLI and can observe the CLI say so:
//! [`crate::claude::ClaudeInvoker::invoke`] (structured `authentication_failed`
//! on the stream) and the `claude auth status --json` probe. The two axes are
//! logged independently, each exactly once per transition: the credential axis
//! keeps its existing once-per-direction log, and the login-required axis is
//! logged solely by [`ReadinessGate::trip_login_required`] /
//! [`ReadinessGate::clear_login_required`], guarded so a repeated trip (or a
//! probe re-confirming an already-tripped state) writes nothing further.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::service_auth::ApiKeyHandle;

/// Why the daemon is not ready, phrased for a log line and a page banner.
pub const NOT_READY_REASON: &str =
    "no Claude credential: sign in at Settings -> Auth (or store an API key) to start running actions";

/// Which of the two not-ready axes is active. `no_credential` and
/// `login_required` are also the exact `/api/status` `not_ready_kind` strings
/// ([`NotReadyKind::as_str`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReadyKind {
    NoCredential,
    LoginRequired,
}

impl NotReadyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            NotReadyKind::NoCredential => "no_credential",
            NotReadyKind::LoginRequired => "login_required",
        }
    }
}

/// The login-required state: the CLI's own message, when it was first observed,
/// and where the observation came from. Cloned out of the gate's mutex rather
/// than held under lock, so a reader never blocks a concurrent trip.
#[derive(Debug, Clone)]
struct LoginRequiredState {
    message: String,
    detected_at: DateTime<Utc>,
}

/// The readiness answer plus how the credential was found, for the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Readiness {
    /// Whether an agent can be spawned at all.
    pub ready: bool,
    /// Where the credential came from: `login`, `api_key`, `env`, or `none`.
    pub source: &'static str,
    /// Which not-ready axis this is, or `None` when ready.
    pub kind: Option<NotReadyKind>,
    /// The CLI's own message, set only when `kind` is `LoginRequired`.
    pub login_message: Option<String>,
    /// When the login-required state was first observed, set only when `kind`
    /// is `LoginRequired`.
    pub login_required_since: Option<DateTime<Utc>>,
}

impl Readiness {
    /// The operator-facing reason, or `None` when ready.
    pub fn reason(&self) -> Option<String> {
        match self.kind? {
            NotReadyKind::NoCredential => Some(NOT_READY_REASON.to_string()),
            NotReadyKind::LoginRequired => Some(format!(
                "Claude login required ({}, detected {}): sign in again at Settings -> Auth",
                self.login_message
                    .as_deref()
                    .unwrap_or("authentication failed"),
                self.login_required_since
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_default(),
            )),
        }
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
    /// forever, which is the log spam this gate exists to remove. Tracks the
    /// CREDENTIAL axis only; the login-required axis logs independently (see
    /// [`Self::trip_login_required`]).
    last_ready: Arc<AtomicBool>,
    /// CLAUDE-269: set when the CLI itself reported an authentication failure,
    /// cleared on a successful sign-in / API key save / a probe reporting
    /// `loggedIn: true`. Not persisted: a restart re-derives it from the
    /// startup probe, and a re-trip after a restart costs nothing.
    login_required: Arc<Mutex<Option<LoginRequiredState>>>,
}

impl ReadinessGate {
    pub fn new(agent_config_dir: PathBuf, api_key: Option<ApiKeyHandle>) -> Self {
        Self {
            agent_config_dir,
            api_key,
            // Start at "was ready" so the first unready check logs.
            last_ready: Arc::new(AtomicBool::new(true)),
            login_required: Arc::new(Mutex::new(None)),
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
            login_required: Arc::new(Mutex::new(None)),
        }
    }

    /// Credential-only readiness, ignoring the login-required overlay. This is
    /// what backs the credential axis's once-per-direction log in [`Self::ready`]
    /// so that axis is never disturbed by a login-required trip/clear.
    fn credential_readiness(&self) -> Readiness {
        if self.agent_config_dir.as_os_str().is_empty() {
            return Readiness {
                ready: true,
                source: "unchecked",
                kind: None,
                login_message: None,
                login_required_since: None,
            };
        }
        if credentials_present(&self.agent_config_dir) {
            return Readiness {
                ready: true,
                source: "login",
                kind: None,
                login_message: None,
                login_required_since: None,
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
                kind: None,
                login_message: None,
                login_required_since: None,
            };
        }
        if std::env::var_os("ANTHROPIC_API_KEY").is_some_and(|v| !v.is_empty()) {
            return Readiness {
                ready: true,
                source: "env",
                kind: None,
                login_message: None,
                login_required_since: None,
            };
        }
        Readiness {
            ready: false,
            source: "none",
            kind: Some(NotReadyKind::NoCredential),
            login_message: None,
            login_required_since: None,
        }
    }

    /// Current readiness, without logging. The login-required axis (an
    /// expired/revoked login observed after a credential was once present)
    /// takes priority: `.credentials.json` stays on disk when a login expires,
    /// so the credential check alone would keep answering ready.
    pub fn check(&self) -> Readiness {
        // The "always ready" gate (empty `agent_config_dir`) never blocks,
        // regardless of either axis: it is used where there is no daemon to
        // hold a login-required state in the first place (tests, the
        // daemonless CLI-direct paths), so the overlay never applies to it.
        if self.agent_config_dir.as_os_str().is_empty() {
            return self.credential_readiness();
        }
        let login = self
            .login_required
            .lock()
            .expect("readiness mutex poisoned")
            .clone();
        if let Some(state) = login {
            let cred = self.credential_readiness();
            return Readiness {
                ready: false,
                source: cred.source,
                kind: Some(NotReadyKind::LoginRequired),
                login_message: Some(state.message),
                login_required_since: Some(state.detected_at),
            };
        }
        self.credential_readiness()
    }

    /// Readiness for a scheduling decision, logging each CREDENTIAL-axis
    /// transition once. The login-required axis is logged separately, exactly
    /// once per transition, by [`Self::trip_login_required`] /
    /// [`Self::clear_login_required`] themselves.
    pub fn ready(&self) -> bool {
        let cred = self.credential_readiness();
        let was = self.last_ready.swap(cred.ready, Ordering::Relaxed);
        if was != cred.ready {
            if cred.ready {
                tracing::info!(
                    source = cred.source,
                    "Claude credential present; scheduling enabled"
                );
            } else {
                tracing::warn!(
                    agent_config_dir = %self.agent_config_dir.display(),
                    "{NOT_READY_REASON}; scheduling is disabled until then"
                );
            }
        }
        if self.agent_config_dir.as_os_str().is_empty() {
            return cred.ready;
        }
        cred.ready && !self.is_login_required()
    }

    /// Whether the login-required axis is currently tripped, with no logging.
    pub fn is_login_required(&self) -> bool {
        self.login_required
            .lock()
            .expect("readiness mutex poisoned")
            .is_some()
    }

    /// Record that the CLI itself reported an authentication failure (a
    /// structured `authentication_failed`, never matched on human-readable
    /// text). `source` is `"invocation"` or `"probe"`, carried through to the
    /// UI only for operator context; detection logic never branches on it.
    /// Logs once, at `error`, the first time this trips; a repeated trip (a
    /// second invocation failing, or the probe re-confirming) updates the
    /// message/time silently.
    pub fn trip_login_required(&self, message: impl Into<String>, source: &'static str) {
        let message = message.into();
        let mut guard = self
            .login_required
            .lock()
            .expect("readiness mutex poisoned");
        let already_tripped = guard.is_some();
        let now = Utc::now();
        *guard = Some(LoginRequiredState {
            message: message.clone(),
            detected_at: guard.as_ref().map(|s| s.detected_at).unwrap_or(now),
        });
        drop(guard);
        if !already_tripped {
            tracing::error!(
                message = %message,
                source,
                "Claude login required; scheduling is disabled until a fresh sign-in at Settings -> Auth"
            );
        }
    }

    /// Clear the login-required state (a brokered sign-in completed, an API key
    /// was saved, or the status probe reported `loggedIn: true`). Logs once, at
    /// `info`, only when the state was actually set.
    pub fn clear_login_required(&self) {
        let mut guard = self
            .login_required
            .lock()
            .expect("readiness mutex poisoned");
        if guard.take().is_some() {
            drop(guard);
            tracing::info!("Claude login restored; scheduling enabled");
        }
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

    /// CLAUDE-269: a credential file on disk stays ready from the OLD axis, but
    /// a trip makes the gate answer not-ready with the login-required kind, the
    /// CLI's message, and a detection time. `.credentials.json` is untouched by
    /// this, matching the real CLI behavior an expired login leaves on disk.
    #[test]
    fn tripping_login_required_overrides_a_present_credential() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);
        assert!(gate.check().ready, "credential file alone is ready");

        gate.trip_login_required(
            "OAuth session expired and could not be refreshed",
            "invocation",
        );
        let r = gate.check();
        assert!(!r.ready);
        assert_eq!(r.kind, Some(NotReadyKind::LoginRequired));
        assert!(r
            .login_message
            .as_deref()
            .unwrap()
            .contains("OAuth session expired"));
        assert!(r.login_required_since.is_some());
        let reason = r.reason().unwrap();
        assert!(reason.contains("OAuth session expired"));
        assert!(reason.contains("Settings -> Auth"));
        assert!(!gate.ready());
    }

    /// A repeated trip does not reset the original detection time, and clearing
    /// restores full readiness.
    #[test]
    fn a_repeated_trip_keeps_the_first_detection_time_and_clear_restores_readiness() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);

        gate.trip_login_required("first", "invocation");
        let first_since = gate.check().login_required_since.unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        gate.trip_login_required("second", "probe");
        let r = gate.check();
        assert_eq!(r.login_required_since.unwrap(), first_since);
        assert_eq!(r.login_message.as_deref(), Some("second"));

        gate.clear_login_required();
        let r = gate.check();
        assert!(r.ready);
        assert_eq!(r.kind, None);
    }

    /// The credential axis's once-per-direction log is untouched by the
    /// login-required overlay: `ready()` still reflects the credential check
    /// for its own transition bookkeeping, even while login-required is set (a
    /// removed credential file is still noticed independently).
    #[test]
    fn login_required_does_not_disturb_the_credential_axis() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".credentials.json"), "{}").unwrap();
        let gate = ReadinessGate::new(tmp.path().to_path_buf(), None);
        gate.trip_login_required("expired", "invocation");
        // Not ready overall (login required)...
        assert!(!gate.ready());
        // ...but the credential itself is still considered present underneath.
        assert_eq!(gate.credential_readiness().source, "login");
    }

    /// The always-ready gate is genuinely never blocked, even if something
    /// mistakenly tripped login-required on it (it has no daemon holding it,
    /// so nothing should, but the doc promise is unconditional).
    #[test]
    fn the_always_gate_ignores_a_trip() {
        let gate = ReadinessGate::always();
        gate.trip_login_required("should not matter", "invocation");
        assert!(gate.check().ready);
        assert!(gate.ready());
    }
}
