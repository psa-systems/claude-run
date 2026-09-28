//! Per-action cross-process advisory lock.
//!
//! Both the daemon's worker pool and a standalone `claude-run run <action>`
//! invocation acquire this lock before processing jobs, so they serialize even
//! when running in different processes. Held only for the duration of one
//! run; released on drop.
//!
//! `flock(LOCK_EX | LOCK_NB)` semantics: try-lock, fail fast if held.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::error::Result;

pub struct ActionLock {
    _file: File,
    path: PathBuf,
}

impl ActionLock {
    /// Try to acquire an exclusive non-blocking lock on
    /// `<lock_dir>/<action>.lock`. Returns `Ok(None)` if another process holds
    /// it, `Ok(Some(_))` on success.
    pub fn try_acquire(lock_dir: &Path, action: &str) -> Result<Option<Self>> {
        std::fs::create_dir_all(lock_dir)?;
        let path = lock_dir.join(format!("{action}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { _file: file, path })),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn first_acquire_succeeds_second_fails() {
        let dir = tempdir().unwrap();
        let a = ActionLock::try_acquire(dir.path(), "test").unwrap();
        assert!(a.is_some());
        let b = ActionLock::try_acquire(dir.path(), "test").unwrap();
        assert!(b.is_none(), "second acquire should fail while first held");
        drop(a);
        // The lock releases on drop, but another test thread mid
        // `fork`/`exec` can hold an inherited duplicate of the lock fd until its
        // `exec` (the fd is `O_CLOEXEC`, which closes it at `exec`, not at
        // `fork`), transiently keeping the `flock` held. Under the parallel test
        // harness the re-acquire can therefore see a spurious `WouldBlock`. Poll
        // until the lock frees (clears within milliseconds); a genuinely
        // unreleased lock never becomes `Some` and still fails at the timeout.
        let mut c = None;
        for _ in 0..50 {
            c = ActionLock::try_acquire(dir.path(), "test").unwrap();
            if c.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            c.is_some(),
            "third acquire should succeed after first dropped"
        );
    }
}
