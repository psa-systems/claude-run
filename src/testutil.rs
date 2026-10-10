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
