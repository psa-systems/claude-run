//! CLAUDE-106: time-based GC sweeper for preserved and aside workdirs.
//!
//! The YouTrack runner intentionally keeps some workdirs rather than deleting
//! them: [`crate::native::youtrack`] preserves a directory that holds unpushed
//! commits, and moves a leftover workdir aside (`<key>.kept-<n>`) out of the way
//! of a fresh attempt. Nothing else ever deletes these, so preserved and aside
//! workdirs accumulate without bound under
//! `$CLAUDE_RUN_DATA_DIR/workdirs/<source>/<key>` (a single preserved Rust
//! workdir can be tens of GB of `target/` output).
//!
//! [`sweep`] walks `<base>/*/*` and deletes any directory older than the
//! configured retention window. A directory that still holds unpushed commits is
//! respected (kept) until a separate, longer hard-cap age, so genuinely un-saved
//! work is not discarded the moment the window passes. Every deletion is logged
//! with action, key, path, age, and bytes reclaimed so a sweep is auditable.
//!
//! The unpushed-commit predicate is injected (production passes
//! [`crate::native::youtrack::has_unpushed_commits`]) so the decision logic is
//! testable without a real git repository.

use std::path::Path;
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use crate::config::WorkdirGcConfig;

const SECS_PER_DAY: u64 = 86_400;

/// Tally of one [`sweep`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Workdirs deleted this pass.
    pub deleted: u64,
    /// Workdirs left in place (fresh, or unpushed under the hard cap).
    pub kept: u64,
    /// Total bytes reclaimed by the deletions.
    pub bytes_reclaimed: u64,
}

/// Walk `<base>/<source>/<key>` and delete every workdir older than the
/// retention window, keeping unpushed-commit workdirs until the longer hard cap.
///
/// `now` is injected (production passes [`SystemTime::now`]) so a test can age a
/// directory by passing a future instant rather than mutating its mtime. A
/// directory whose mtime is unreadable, or is newer than `now`, is treated as
/// age zero (kept). Filesystem errors on individual entries are logged and
/// skipped; the sweep never aborts the whole pass for one bad directory.
pub fn sweep<F>(base: &Path, cfg: &WorkdirGcConfig, now: SystemTime, is_unpushed: F) -> SweepStats
where
    F: Fn(&Path) -> bool,
{
    let mut stats = SweepStats::default();
    if !cfg.enabled {
        return stats;
    }
    let retention = Duration::from_secs(cfg.retention_days.saturating_mul(SECS_PER_DAY));
    let hard_cap = Duration::from_secs(cfg.unpushed_hard_cap_days.saturating_mul(SECS_PER_DAY));

    let sources = match std::fs::read_dir(base) {
        Ok(r) => r,
        // Missing base dir (no workdirs created yet) is a clean no-op.
        Err(_) => return stats,
    };
    for source in sources.flatten() {
        let source_path = source.path();
        if !source_path.is_dir() {
            continue;
        }
        let action = source.file_name().to_string_lossy().into_owned();
        let keys = match std::fs::read_dir(&source_path) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %source_path.display(), error = %e, "workdir GC: cannot read source dir; skipping");
                continue;
            }
        };
        for key in keys.flatten() {
            let path = key.path();
            if !path.is_dir() {
                continue;
            }
            let key_name = key.file_name().to_string_lossy().into_owned();
            let age = dir_age(&path, now);
            if age < retention {
                stats.kept += 1;
                continue;
            }
            let unpushed = is_unpushed(&path);
            if unpushed && age < hard_cap {
                debug!(
                    action = %action,
                    key = %key_name,
                    path = %path.display(),
                    age = %humantime::format_duration(whole_secs(age)),
                    "workdir GC: kept; holds unpushed commits and is under the hard cap"
                );
                stats.kept += 1;
                continue;
            }
            let bytes = dir_size(&path);
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {
                    info!(
                        action = %action,
                        key = %key_name,
                        path = %path.display(),
                        age = %humantime::format_duration(whole_secs(age)),
                        bytes_reclaimed = bytes,
                        unpushed,
                        "workdir GC: deleted workdir older than the retention window"
                    );
                    stats.deleted += 1;
                    stats.bytes_reclaimed += bytes;
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "workdir GC: delete failed (continuing)");
                    stats.kept += 1;
                }
            }
        }
    }
    stats
}

/// Age of `path` relative to `now`, from its last-modified time. Unreadable
/// mtime or an mtime in the future (relative to `now`) yields `Duration::ZERO`.
fn dir_age(path: &Path, now: SystemTime) -> Duration {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| now.duration_since(m).ok())
        .unwrap_or(Duration::ZERO)
}

/// Truncate a duration to whole seconds so the logged age reads cleanly (e.g.
/// `15days` rather than `15days 327ms`).
fn whole_secs(d: Duration) -> Duration {
    Duration::from_secs(d.as_secs())
}

/// Recursively sum the byte size of every regular file under `path`. Symlinks
/// are not followed (their own link size is counted, not the target). Best
/// effort: an unreadable entry contributes zero rather than aborting.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            total += dir_size(&entry.path());
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(enabled: bool) -> WorkdirGcConfig {
        WorkdirGcConfig {
            enabled,
            retention_days: 14,
            unpushed_hard_cap_days: 60,
        }
    }

    /// Create `<base>/<source>/<key>` with one file of `bytes` bytes inside.
    fn make_workdir(base: &Path, source: &str, key: &str, bytes: usize) -> std::path::PathBuf {
        let dir = base.join(source).join(key);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("file.txt"), vec![b'x'; bytes]).unwrap();
        dir
    }

    fn days(n: u64) -> Duration {
        Duration::from_secs(n * SECS_PER_DAY)
    }

    #[test]
    fn stale_workdir_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_workdir(tmp.path(), "youtrack", "CLAUDE-1", 100);
        // 30 days "from now": older than the 14-day window.
        let now = SystemTime::now() + days(30);
        let stats = sweep(tmp.path(), &cfg(true), now, |_| false);
        assert!(!dir.exists(), "a workdir past the window must be deleted");
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.kept, 0);
        assert!(stats.bytes_reclaimed >= 100);
    }

    #[test]
    fn fresh_workdir_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_workdir(tmp.path(), "youtrack", "CLAUDE-2", 10);
        // "now" is real now, so the dir's age is ~0: inside the window.
        let stats = sweep(tmp.path(), &cfg(true), SystemTime::now(), |_| false);
        assert!(dir.exists(), "a fresh workdir must be kept");
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
    }

    #[test]
    fn unpushed_workdir_kept_until_hard_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_workdir(tmp.path(), "youtrack", "CLAUDE-3", 10);

        // 30 days old: past the 14-day window but under the 60-day hard cap.
        // With unpushed commits it is respected (kept).
        let stats = sweep(tmp.path(), &cfg(true), SystemTime::now() + days(30), |_| {
            true
        });
        assert!(
            dir.exists(),
            "unpushed work under the hard cap must be kept"
        );
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted, 0);

        // 90 days old: past the hard cap. Even unpushed work is now deleted.
        let stats = sweep(tmp.path(), &cfg(true), SystemTime::now() + days(90), |_| {
            true
        });
        assert!(
            !dir.exists(),
            "unpushed work past the hard cap must be deleted"
        );
        assert_eq!(stats.deleted, 1);
    }

    #[test]
    fn aside_kept_dir_is_swept_like_any_other() {
        // The `<key>.kept-<n>` aside dirs are siblings of key dirs under the
        // same source dir, so the `<base>/*/*` walk catches them.
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_workdir(tmp.path(), "youtrack", "CLAUDE-4.kept-1", 10);
        let stats = sweep(tmp.path(), &cfg(true), SystemTime::now() + days(30), |_| {
            false
        });
        assert!(!dir.exists(), "a stale aside dir must be swept");
        assert_eq!(stats.deleted, 1);
    }

    #[test]
    fn disabled_sweep_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_workdir(tmp.path(), "youtrack", "CLAUDE-5", 10);
        let stats = sweep(
            tmp.path(),
            &cfg(false),
            SystemTime::now() + days(365),
            |_| false,
        );
        assert!(dir.exists(), "a disabled sweep must not delete");
        assert_eq!(stats, SweepStats::default());
    }

    #[test]
    fn missing_base_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("does-not-exist");
        let stats = sweep(&base, &cfg(true), SystemTime::now(), |_| false);
        assert_eq!(stats, SweepStats::default());
    }
}
