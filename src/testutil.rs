//! Test-only helpers shared across the crate's unit tests.
//!
//! Compiled under `cfg(test)` only, so nothing here ships in the binary.
//!
//! The one thing it owns today is [`write_stub`], the safe way for a test to
//! create an executable stub it is about to run. `cargo test` runs the suite on
//! many threads: any writable descriptor this process holds on the stub can be
//! inherited by a child another thread forks, and the later `execve` of the
//! stub then fails with `ETXTBSY` ("Text file busy"). That is CLAUDE-224 in
//! `claude.rs` and CLAUDE-255 in `claude_login.rs`, the same defect twice, so
//! the helper lives in one place and every site uses it.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// The one lock every test that mutates the process-wide `PATH` must hold for
/// its whole body. Separate per-module locks let one test restore a stale
/// `PATH` snapshot over another's stub dir, so the stub vanishes mid-test.
///
/// Re-entrant on one thread: a test holds it for its whole body and then calls
/// production code that takes it again under `cfg(test)`, which a plain
/// `std::sync::Mutex` turns into a permanent self-deadlock (CLAUDE-536). Take
/// it BEFORE any other test lock (`git_allow_protocol_test_lock`,
/// `git_trace_test_lock`) so two locks are never acquired in opposite orders.
pub fn path_env_lock() -> PathEnvGuard {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    thread_local! {
        static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
    let outer = DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        depth == 0
    });
    let guard = outer.then(|| LOCK.lock().unwrap_or_else(|e| e.into_inner()));
    PathEnvGuard {
        _guard: guard,
        depth: &DEPTH,
    }
}

/// Guard returned by [`path_env_lock`]. Only the outermost guard on a thread
/// owns the mutex; inner guards just count. Not `Send`, like the
/// `MutexGuard` it wraps.
pub struct PathEnvGuard {
    // `Some` only on the outermost guard of a thread; released after `drop` resets the depth.
    _guard: Option<std::sync::MutexGuard<'static, ()>>,
    depth: &'static std::thread::LocalKey<std::cell::Cell<usize>>,
}

impl Drop for PathEnvGuard {
    fn drop(&mut self) {
        self.depth.with(|d| d.set(d.get() - 1));
    }
}

/// Strip every ambient git-config and credential input from `cmd` before a
/// test execs real `git`.
///
/// A `git push` run with `-c credential.helper=...` and the runner's own
/// `CLAUDE_RUN_GIT_USERNAME` / `CLAUDE_RUN_GIT_PASSWORD` exported passes both
/// to `GIT_CONFIG_PARAMETERS` and the two variables down to any hook it spawns
/// (CLAUDE-426): a hook's own `cargo test` therefore inherits them from the
/// process environment unless a test clears them first. Only a test that
/// invokes git's credential subsystem (`git credential fill`, or a transport
/// that needs to authenticate) is actually reachable through them, but
/// clearing the same fixed list everywhere keeps that judgement out of each
/// call site.
pub fn clear_ambient_git_env(cmd: &mut Command) {
    cmd.env_remove("GIT_CONFIG_PARAMETERS")
        .env_remove("GIT_CONFIG_COUNT")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_ASKPASS")
        .env_remove("SSH_ASKPASS")
        .env_remove("CLAUDE_RUN_GIT_USERNAME")
        .env_remove("CLAUDE_RUN_GIT_PASSWORD");
    if let Ok(count) = std::env::var("GIT_CONFIG_COUNT") {
        if let Ok(n) = count.parse::<u32>() {
            for i in 0..n {
                cmd.env_remove(format!("GIT_CONFIG_KEY_{i}"))
                    .env_remove(format!("GIT_CONFIG_VALUE_{i}"));
            }
        }
    }
}

/// Pin a coreutils-faithful `timeout` (exit 124 on timeout) first on PATH for
/// the test process.
///
/// Production resolves `timeout` via PATH and relies on its exit-124-on-timeout
/// contract (a documented coreutils host dependency). The pre-commit dev image
/// (`rust-builder-musl:*-alpine`) ships only busybox `timeout`, which exits 143
/// (SIGTERM) instead of 124 on timeout, so a test that asserts the 124 contract
/// is non-deterministic across hosts. This installs a tiny POSIX-sh shim that
/// reproduces coreutils' behaviour (TERM after N seconds, exit 124 if it had to
/// kill) and prepends it to PATH once, so the exit-code contract is exercised
/// deterministically regardless of which `timeout` the host happens to ship.
/// For non-timeout runs the shim forwards the child's own exit status, so
/// other tests invoking a `timeout`-wrapped subprocess are unaffected.
pub fn ensure_coreutils_timeout_on_path() {
    use std::sync::OnceLock;

    // Hold the same lock every other PATH reader/mutator in the suite holds
    // (see `path_env_lock`) for the whole read-then-maybe-write below, so this
    // function can never race a concurrent `std::env::set_var("PATH", ...)`
    // from another test thread.
    let _path_guard = path_env_lock();

    static SHIM_DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    let dir = SHIM_DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join("claude-run-test-timeout-shim");
        std::fs::create_dir_all(&dir).unwrap();
        let shim = dir.join("timeout");
        // Parse a leading `<N>s` (or bare `<N>`) duration like coreutils, run
        // the command in the background, and SIGTERM it after the budget. A
        // marker file records that the watchdog fired: exit 124 when it did
        // (the run was killed for exceeding the budget), otherwise forward the
        // child's own exit status.
        // The watcher's own `sleep "$secs"` must be killable
        // directly. Killing only the watcher subshell (the old shape) does
        // NOT kill a child that subshell already forked: `sleep` survives
        // as an orphan, reparented to init, and keeps running for the rest
        // of its duration. An orphaned `sleep` inherits this script's
        // stdout/stderr fds (the pipe `ClaudeInvoker::invoke` reads to
        // EOF), so on every run that finishes before `secs` elapses, the
        // caller's relay thread blocks reading that pipe until the orphan
        // itself exits, i.e. for the full `secs`. The trap lets the normal
        // exit path (`kill "$watcher"`, below) reach the grandchild `sleep`
        // and terminate it immediately instead of leaving it to run out
        // its clock.
        //
        // The trap must name that grandchild through `jobs -p`, never
        // through a `spid=$!` variable (SPEC-10.1): capturing `$!` is a
        // separate statement from the `sleep "$secs" &` that forks it, and
        // under the scheduling delay `SPEC-10.1.1`/`SPEC-10.1.3` require
        // tolerating, `TERM` can land in the gap between the two, between the
        // fork completing and the assignment running. The trap then reads a
        // stale (empty) `$spid` and kills nothing, leaking `sleep` as the
        // orphan described above. `jobs -p` instead reads the subshell's own
        // job table, which the fork updates atomically as part of the same
        // command that creates the job, so it is never stale: empty before
        // the fork (nothing to kill yet, and the fork never runs because
        // `exit 0` in the handler already ended the subshell), accurate after.
        let body = r#"#!/bin/sh
secs=$1; shift
secs=${secs%s}
fired="$(mktemp)"
rm -f "$fired"
"$@" &
child=$!
(
    trap 'kill -TERM $(jobs -p) 2>/dev/null; exit 0' TERM
    sleep "$secs" &
    wait
    : > "$fired"
    kill -TERM "$child" 2>/dev/null
    sleep 1
    kill -KILL "$child" 2>/dev/null
) &
watcher=$!
wait "$child"
code=$?
kill "$watcher" 2>/dev/null
wait "$watcher" 2>/dev/null
if [ -e "$fired" ]; then
    rm -f "$fired"
    exit 124
fi
rm -f "$fired"
exit "$code"
"#;
        write_stub(&shim, body);
        dir
    });
    let current = std::env::var_os("PATH").unwrap_or_default();
    // Idempotent: only prepend if our shim dir is not already leading.
    let already = std::env::split_paths(&current).next().map(|p| p == *dir);
    if already != Some(true) {
        let mut paths = vec![dir.clone()];
        paths.extend(std::env::split_paths(&current));
        let joined = std::env::join_paths(paths).unwrap();
        // Safe under edition 2021; the init is effectively single-shot and
        // only ever prepends the same shim dir.
        std::env::set_var("PATH", &joined);
    }
}

/// Poll `probe` until it returns `Some`, or `budget` elapses, backing off
/// exponentially between attempts (starting at 5ms, capped at 200ms) instead
/// of spinning at a fixed interval.
///
/// CLAUDE-504: a `RunGuard`'s `Drop` cannot await its own close, so it spawns
/// the write and a test has to poll for it to land. A fixed, short interval
/// polled across a fixed, short total window (1s at 10ms) flaked on the
/// Postgres backend under the heavy I/O contention a full `cargo test` run
/// puts on it, where the spawned close can take far longer than the window.
/// Backing off keeps a healthy run's overhead negligible (a handful of
/// sub-10ms probes) while giving a contended run a much wider budget to land
/// in, and without adding to that contention the way fast fixed-interval
/// polling would.
pub async fn poll_until<F, Fut, T>(budget: std::time::Duration, mut probe: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = std::time::Duration::from_millis(5);
    loop {
        if let Some(value) = probe().await {
            return Some(value);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return None;
        }
        tokio::time::sleep(backoff.min(deadline - now)).await;
        backoff = (backoff * 2).min(std::time::Duration::from_millis(200));
    }
}

/// Write an executable test stub WITHOUT this process ever holding a writable
/// descriptor on it.
///
/// An external `sh` owns both the write and the `chmod`, so the descriptor
/// never enters our file-descriptor table and no fork taken by a sibling test
/// thread can carry it into the `execve` window. Every test stub the suite goes
/// on to exec must be written with this: `std::fs::write` plus
/// `set_permissions` is the shape that races.
pub fn write_stub(path: &Path, body: &str) {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("cat > \"$1\" && chmod 0755 \"$1\"")
        .arg("sh")
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "writing stub {} failed", path.display());
}

#[cfg(test)]
mod tests {
    /// CLAUDE-536: the guard is re-entrant on one thread (nested production
    /// takes no longer self-deadlock) and still excludes other threads.
    #[test]
    fn path_env_lock_is_reentrant_and_still_excludes_other_threads() {
        let outer = path_env_lock();
        let inner = path_env_lock();
        let (tx, rx) = std::sync::mpsc::channel();
        let other = std::thread::spawn(move || {
            let _g = path_env_lock();
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(300))
                .is_err(),
            "another thread must not acquire the lock while this one holds it"
        );
        drop(inner);
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(300))
                .is_err(),
            "dropping the inner guard must not release the lock"
        );
        drop(outer);
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .expect("the lock must be released once the outer guard drops");
        other.join().unwrap();
    }

    use super::*;

    /// CLAUDE-224 guard: hammer write-then-exec while sibling threads fork, the
    /// exact shape that failed CI run #1008. An in-process write reproduces
    /// `ETXTBSY` within a few dozen iterations here; with [`write_stub`] the
    /// descriptor never enters our table, so the exec cannot fail that way.
    #[test]
    fn write_stub_survives_concurrent_forks() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        const ETXTBSY: i32 = 26;

        let stop = Arc::new(AtomicBool::new(false));
        let forkers: Vec<_> = (0..4)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        // Best-effort noise: a failed spawn is one fork fewer this
                        // tick, so report it and keep hammering rather than flaking
                        // the assertion on a transient EAGAIN.
                        if let Err(e) = Command::new("sh").arg("-c").arg("exit 0").status() {
                            eprintln!("forker spawn failed: {e}");
                        }
                    }
                })
            })
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        let mut busy = 0usize;
        for _ in 0..80 {
            write_stub(&stub, "#!/bin/sh\nexit 0\n");
            match Command::new(&stub).status() {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(ETXTBSY) => busy += 1,
                Err(e) => panic!("unexpected exec error: {e}"),
            }
        }

        stop.store(true, Ordering::Relaxed);
        for f in forkers {
            f.join().unwrap();
        }
        assert_eq!(
            busy, 0,
            "stub exec hit ETXTBSY: a writable fd leaked to a forked child"
        );
    }

    /// CLAUDE-255 guard: the fix is only durable if a later test cannot quietly
    /// go back to `fs::write` plus an in-process `chmod +x`. Scan the
    /// `#[cfg(test)]` half of every source file for a mode literal carrying an
    /// exec bit and fail on any hit.
    ///
    /// Test code only: production code that writes an executable it never execs
    /// is a different shape, and directory modes such as
    /// `0o700` in `secrets.rs` are not this hazard at all. A source scan rather
    /// than a runtime check because the defect is a write-then-exec shape, not
    /// an observable behaviour: by the time a test flakes, CI has already gone
    /// red intermittently.
    #[test]
    fn no_test_sets_an_exec_bit_from_inside_this_process() {
        // Split so the needles do not match this file's own scanning code.
        let needles = [concat!("from_", "mode(0o"), concat!("set_", "mode(0o")];

        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut sources = Vec::new();
        collect_rust_sources(&root.join("src"), &mut sources);
        assert!(!sources.is_empty(), "the guard scanned no sources");

        let mut offenders = Vec::new();
        for file in &sources {
            let rel = file
                .strip_prefix(root)
                .unwrap_or(file)
                .to_string_lossy()
                .into_owned();
            let src = std::fs::read_to_string(file)
                .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
            let lines: Vec<&str> = src.lines().collect();
            let mut in_tests = false;
            for (i, line) in lines.iter().enumerate() {
                let trimmed = line.trim_start();
                // The test module and everything after it. A bare `#[cfg(test)]`
                // on a helper fn (secrets.rs) is NOT the marker; the module is.
                if trimmed == "#[cfg(test)]"
                    && lines
                        .get(i + 1)
                        .is_some_and(|n| n.trim_start().starts_with("mod "))
                {
                    in_tests = true;
                }
                if !in_tests {
                    continue;
                }
                // Skip comments: this test's own prose names the shape.
                if trimmed.starts_with("//") {
                    continue;
                }
                for needle in needles {
                    let Some(rest) = line.split_once(needle).map(|(_, r)| r) else {
                        continue;
                    };
                    let digits: String = rest.chars().take_while(|c| c.is_digit(8)).collect();
                    let mode = u32::from_str_radix(&digits, 8)
                        .unwrap_or_else(|e| panic!("{rel}: unreadable mode `{digits}`: {e}"));
                    if mode & 0o111 != 0 {
                        offenders.push(format!("{rel}:{}: {trimmed}", i + 1));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "a test setting an exec bit from inside this process re-introduces the ETXTBSY race \
             (CLAUDE-255); write the stub with `crate::testutil::write_stub` instead: {offenders:#?}"
        );
    }

    /// Collect every `.rs` file under `dir`, recursively. A read failure is
    /// fatal: a guard that silently scanned nothing would pass forever.
    fn collect_rust_sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read dir {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("read dir entry").path();
            if path.is_dir() {
                collect_rust_sources(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
}

thread_local! {
    /// The current thread's log capture, when a test has one running.
    static LOG_BUF: std::cell::RefCell<Option<std::sync::Arc<std::sync::Mutex<Vec<u8>>>>> =
        const { std::cell::RefCell::new(None) };
}

/// Writes a formatted log line into the current thread's capture, or drops
/// it when the thread has none.
struct ThreadLogWriter;

impl Write for ThreadLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG_BUF.with(|cell| {
            if let Some(capture) = cell.borrow().as_ref() {
                capture
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Install, once per test binary, the global subscriber every log capture
/// reads through (CLAUDE-569). A subscriber set per test with
/// `set_default`/`with_default` made `tracing` rebuild its per-callsite
/// interest cache each time one was created or dropped; with dozens of test
/// threads doing that at once, a log site could stay cached as disabled
/// while a test's own subscriber was in place, and its event never reached
/// the capture (PR #153, run #835). With one subscriber for the whole run,
/// nothing is created or dropped and that cache never changes.
fn install_test_subscriber() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let sub = tracing_subscriber::fmt()
            .with_writer(|| ThreadLogWriter)
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(sub)
            .expect("the test binary's only global subscriber");
    });
}

/// Captures what the current thread logs while it is alive, for a test that
/// asserts a path logged (or did not log) something. Hold it across `.await`
/// in a current-thread async test; events from other threads are not seen,
/// as with the scoped subscribers it replaces.
pub struct LogCapture {
    buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    previous: Option<std::sync::Arc<std::sync::Mutex<Vec<u8>>>>,
}

impl LogCapture {
    pub fn start() -> Self {
        install_test_subscriber();
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let previous = LOG_BUF.with(|cell| cell.borrow_mut().replace(buf.clone()));
        LogCapture { buf, previous }
    }

    /// Everything captured so far.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock().unwrap_or_else(|p| p.into_inner())).into_owned()
    }
}

impl Drop for LogCapture {
    fn drop(&mut self) {
        let previous = self.previous.take();
        LOG_BUF.with(|cell| *cell.borrow_mut() = previous);
    }
}

/// Run `f` and return what it returned and everything the current thread
/// logged meanwhile.
pub fn capture_logs<T>(f: impl FnOnce() -> T) -> (T, String) {
    let capture = LogCapture::start();
    let out = f();
    let logs = capture.text();
    (out, logs)
}
