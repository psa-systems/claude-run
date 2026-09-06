//! Broker for an interactive `claude auth login`, driven from the web UI.
//!
//! `claude auth login` is an OAuth flow, but not one that needs a browser on the
//! machine running it: with pipes on stdin/stdout it prints the authorize URL,
//! then blocks reading a code from stdin, and its redirect target is a
//! paste-the-code callback rather than a `localhost` listener. That is what
//! makes it brokerable from a container:
//!
//! ```text
//! Opening browser to sign in...
//! If the browser didn't open, visit: https://claude.com/cai/oauth/authorize?code=true&...
//! Paste code here if prompted >
//! ```
//!
//! So the daemon spawns the child, hands the operator the URL, takes the code
//! back through a form, and writes it to the child's stdin. The child must stay
//! alive between those two requests because it holds the PKCE verifier the code
//! is redeemed against; that is why this is daemon state and not a pair of
//! independent handlers. A wrong code does not kill it (`Invalid code. Please
//! make sure the full code was copied.` on stderr, prompt re-armed), so a retry
//! reuses the same session.
//!
//! The alternative remains an `ANTHROPIC_API_KEY` typed into the same card,
//! which is the right credential for API-billed deployments.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// How long to wait for the child to print its authorize URL before giving up.
const URL_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for the child to exit after the code is submitted.
const CODE_TIMEOUT: Duration = Duration::from_secs(45);
/// A started login older than this is abandoned: the child holds a PKCE verifier
/// and a pipe, and an operator who wandered off should not pin either forever.
pub const SESSION_MAX_AGE: Duration = Duration::from_secs(15 * 60);

/// What the Auth card renders for an in-flight login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingView {
    /// The authorize URL to open.
    pub url: String,
    /// Seconds since the login was started.
    pub age_secs: u64,
}

/// A live `claude auth login` child plus everything needed to finish it.
struct Pending {
    child: Child,
    stdin: ChildStdin,
    url: String,
    started: Instant,
    /// Everything the child has written to stdout and stderr, appended by the
    /// reader threads. The tail is what the operator is shown when a code is
    /// refused, so the CLI's own wording reaches the page instead of a generic
    /// failure.
    output: Arc<Mutex<String>>,
}

impl Pending {
    /// Kill the child and reap it. Called on cancel, expiry, and every failure
    /// path, so no login session outlives its usefulness.
    fn terminate(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Outcome of submitting a code.
#[derive(Debug, PartialEq, Eq)]
pub enum CodeOutcome {
    /// The child exited 0. The credential now lives in its config dir.
    LoggedIn,
    /// The child rejected the code and is still waiting: the operator can paste
    /// another one into the same session. Carries the CLI's own message.
    Retry(String),
}

/// Daemon-wide holder for the one login that may be in flight.
#[derive(Clone, Default)]
pub struct LoginBroker {
    pending: Arc<Mutex<Option<Pending>>>,
}

impl LoginBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start `claude auth login` under `config_dir` and return the authorize URL.
    ///
    /// Replaces any existing session: the operator pressing the button again
    /// means the old URL is stale, and leaving the old child running would leak
    /// a process. Blocking (spawn plus a bounded read), so callers on an async
    /// task must wrap it per CLAUDE-228.
    pub fn start(&self, binary: &str, config_dir: &PathBuf) -> Result<String> {
        self.cancel();

        let mut child = Command::new(binary)
            .arg("auth")
            .arg("login")
            .env("CLAUDE_CONFIG_DIR", config_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Secret(format!("spawning `{binary} auth login`: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Secret("claude login child has no stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Secret("claude login child has no stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::Secret("claude login child has no stderr".to_string()))?;

        let output = Arc::new(Mutex::new(String::new()));
        let (url_tx, url_rx) = std::sync::mpsc::channel::<String>();

        // stdout reader: forwards the authorize URL as soon as it appears, and
        // keeps draining afterwards so the pipe never fills and blocks the child.
        {
            let output = Arc::clone(&output);
            std::thread::spawn(move || {
                let mut sent = false;
                for line in BufReader::new(stdout)
                    .lines()
                    .map_while(std::result::Result::ok)
                {
                    if !sent {
                        if let Some(url) = extract_url(&line) {
                            sent = url_tx.send(url).is_ok();
                        }
                    }
                    append(&output, &line);
                }
            });
        }
        // stderr reader: this is where `Invalid code` and any real failure land.
        {
            let output = Arc::clone(&output);
            std::thread::spawn(move || {
                for line in BufReader::new(stderr)
                    .lines()
                    .map_while(std::result::Result::ok)
                {
                    append(&output, &line);
                }
            });
        }

        let url = match url_rx.recv_timeout(URL_TIMEOUT) {
            Ok(u) => u,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                let seen = output.lock().map(|o| o.clone()).unwrap_or_default();
                return Err(Error::Secret(format!(
                    "`{binary} auth login` printed no authorize URL within {}s. Output so far: {}",
                    URL_TIMEOUT.as_secs(),
                    tail(&seen)
                )));
            }
        };

        *self.lock()? = Some(Pending {
            child,
            stdin,
            url: url.clone(),
            started: Instant::now(),
            output,
        });
        Ok(url)
    }

    /// Write `code` to the waiting child and report what happened.
    ///
    /// `Ok(LoggedIn)` clears the session. `Ok(Retry)` keeps it, because the CLI
    /// re-arms its prompt after a bad code. Every error path terminates the
    /// child rather than leaving a half-dead session behind.
    pub fn submit_code(&self, code: &str) -> Result<CodeOutcome> {
        let mut slot = self.lock()?;
        let Some(pending) = slot.as_mut() else {
            return Err(Error::Secret(
                "no sign-in is in progress; start one first".to_string(),
            ));
        };

        let mark = pending
            .output
            .lock()
            .map(|o| o.len())
            .map_err(|e| Error::Secret(format!("login output lock poisoned: {e}")))?;

        if let Err(e) = writeln!(pending.stdin, "{}", code.trim()) {
            pending.terminate();
            *slot = None;
            return Err(Error::Secret(format!("writing the code to `claude`: {e}")));
        }
        if let Err(e) = pending.stdin.flush() {
            pending.terminate();
            *slot = None;
            return Err(Error::Secret(format!("flushing the code to `claude`: {e}")));
        }

        // Poll for exit. The child stays alive on a bad code, so a timeout here
        // is the normal "wrong code" path, not a hang.
        let deadline = Instant::now() + CODE_TIMEOUT;
        loop {
            match pending.child.try_wait() {
                Ok(Some(status)) => {
                    let seen = pending.output.lock().map(|o| o.clone()).unwrap_or_default();
                    let _ = pending.child.wait();
                    *slot = None;
                    return if status.success() {
                        Ok(CodeOutcome::LoggedIn)
                    } else {
                        Err(Error::Secret(format!(
                            "`claude auth login` exited {}: {}",
                            status.code().unwrap_or(-1),
                            tail(&seen)
                        )))
                    };
                }
                Ok(None) => {}
                Err(e) => {
                    pending.terminate();
                    *slot = None;
                    return Err(Error::Secret(format!("waiting for `claude`: {e}")));
                }
            }
            let fresh = pending
                .output
                .lock()
                .map(|o| o[mark.min(o.len())..].trim().to_string())
                .unwrap_or_default();
            if !fresh.is_empty() {
                // The CLI said something without exiting: a refused code. Hand
                // its own wording back and keep the session for another try.
                return Ok(CodeOutcome::Retry(tail(&fresh)));
            }
            if Instant::now() >= deadline {
                pending.terminate();
                *slot = None;
                return Err(Error::Secret(format!(
                    "`claude auth login` neither finished nor answered within {}s",
                    CODE_TIMEOUT.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Abandon any in-flight login.
    pub fn cancel(&self) {
        if let Ok(mut slot) = self.pending.lock() {
            if let Some(mut p) = slot.take() {
                p.terminate();
            }
        }
    }

    /// The in-flight login for the page, dropping one that has aged out.
    pub fn view(&self) -> Option<PendingView> {
        let mut slot = self.pending.lock().ok()?;
        let expired = slot
            .as_ref()
            .is_some_and(|p| p.started.elapsed() > SESSION_MAX_AGE);
        if expired {
            if let Some(mut p) = slot.take() {
                p.terminate();
            }
            return None;
        }
        slot.as_ref().map(|p| PendingView {
            url: p.url.clone(),
            age_secs: p.started.elapsed().as_secs(),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Option<Pending>>> {
        self.pending
            .lock()
            .map_err(|e| Error::Secret(format!("login broker lock poisoned: {e}")))
    }
}

fn append(buf: &Arc<Mutex<String>>, line: &str) {
    if let Ok(mut b) = buf.lock() {
        b.push_str(line);
        b.push('\n');
    }
}

/// The last 400 characters, so a page shows the CLI's message without pasting a
/// screenful of banner.
fn tail(s: &str) -> String {
    let t = s.trim();
    match t.char_indices().nth_back(399) {
        Some((i, _)) => format!("...{}", &t[i..]),
        None => t.to_string(),
    }
}

/// Pull the authorize URL out of a line of the CLI's output. Matches on the
/// scheme rather than the surrounding prose, so a reworded prompt still works.
fn extract_url(line: &str) -> Option<String> {
    let start = line.find("https://")?;
    let rest = &line[start..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    let url = rest[..end].trim_end_matches(['.', ',']);
    url.contains("oauth").then(|| url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that write an executable stub and then exec it.
    /// `cargo test` runs them in parallel, and a fork taken while another
    /// thread still holds the stub open for writing makes the exec fail with
    /// `ETXTBSY` (`Text file busy`), which showed up only under the containered
    /// `just check`. Nothing about the broker is racy; this is the classic
    /// write-then-fork/exec hazard in the harness.
    fn stub_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn the_authorize_url_is_recognized_in_the_cli_wording() {
        let line = "If the browser didn't open, visit: https://claude.com/cai/oauth/authorize?code=true&client_id=abc&state=xyz";
        assert_eq!(
            extract_url(line).as_deref(),
            Some("https://claude.com/cai/oauth/authorize?code=true&client_id=abc&state=xyz")
        );
    }

    #[test]
    fn non_oauth_lines_are_not_mistaken_for_the_url() {
        assert_eq!(extract_url("Opening browser to sign in..."), None);
        assert_eq!(
            extract_url("see https://docs.claude.com/help for more"),
            None
        );
        assert_eq!(extract_url("Paste code here if prompted > "), None);
    }

    #[test]
    fn tail_keeps_the_end_and_marks_the_cut() {
        assert_eq!(tail("  short message  "), "short message");
        let long = "x".repeat(1000);
        let t = tail(&long);
        assert!(t.starts_with("..."), "{t}");
        assert_eq!(t.chars().count(), 403);
    }

    /// The broker drives a real child over pipes: a stub that prints the URL,
    /// reads a line, and exits 0 on the expected code. Proves the two-step
    /// handshake without contacting Anthropic.
    #[test]
    fn a_login_round_trips_through_the_broker() {
        let _serial = stub_lock();
        let tmp = tempfile::tempdir().unwrap();
        let stub = tmp.path().join("claude-stub");
        std::fs::write(
            &stub,
            "#!/bin/sh\n\
             echo \"Opening browser to sign in...\"\n\
             echo \"If the browser didn't open, visit: https://claude.com/cai/oauth/authorize?code=true&state=s\"\n\
             printf 'Paste code here if prompted > '\n\
             read code\n\
             if [ \"$code\" = \"good-code\" ]; then exit 0; fi\n\
             echo 'Invalid code. Please make sure the full code was copied.' >&2\n\
             sleep 30\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

        let broker = LoginBroker::new();
        let dir = tmp.path().to_path_buf();
        let url = broker.start(stub.to_str().unwrap(), &dir).unwrap();
        assert!(url.contains("oauth/authorize"), "{url}");
        assert!(broker.view().is_some(), "the session is held for the code");

        // A wrong code keeps the session and returns the CLI's own wording.
        match broker.submit_code("wrong").unwrap() {
            CodeOutcome::Retry(msg) => assert!(msg.contains("Invalid code"), "{msg}"),
            other => panic!("expected a retry, got {other:?}"),
        }
        assert!(broker.view().is_some(), "a refused code keeps the session");

        broker.cancel();
        assert!(broker.view().is_none(), "cancel terminates the session");

        // The happy path clears the session.
        broker.start(stub.to_str().unwrap(), &dir).unwrap();
        assert_eq!(
            broker.submit_code("good-code").unwrap(),
            CodeOutcome::LoggedIn
        );
        assert!(broker.view().is_none(), "a completed login holds nothing");
    }

    /// Submitting with no session in flight is an error naming the fix, not a
    /// silent no-op.
    #[test]
    fn submitting_without_a_session_errors() {
        let broker = LoginBroker::new();
        let err = broker.submit_code("whatever").unwrap_err();
        assert!(err.to_string().contains("start one first"), "{err}");
    }

    /// A child that never prints a URL is killed and reported, not left running.
    #[test]
    fn a_child_that_prints_no_url_is_reaped() {
        let _serial = stub_lock();
        let tmp = tempfile::tempdir().unwrap();
        let stub = tmp.path().join("silent");
        std::fs::write(&stub, "#!/bin/sh\nexit 3\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

        let broker = LoginBroker::new();
        let err = broker
            .start(stub.to_str().unwrap(), &tmp.path().to_path_buf())
            .unwrap_err();
        assert!(err.to_string().contains("no authorize URL"), "{err}");
        assert!(broker.view().is_none());
    }
}
