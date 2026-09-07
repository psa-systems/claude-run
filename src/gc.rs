//! Time-based GC sweepers for the two unbounded on-disk trees: preserved and
//! aside workdirs ([`sweep`], CLAUDE-106) and per-attempt stream logs
//! ([`sweep_stream_logs`], CLAUDE-245). Both are scheduled by the daemon on the
//! same tick and read their own DB-backed retention window.
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

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use crate::config::{StreamLogGcConfig, WorkdirGcConfig};

const SECS_PER_DAY: u64 = 86_400;

/// Tally of one [`sweep`] or [`sweep_stream_logs`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Entries deleted this pass (workdirs, or stream logs).
    pub deleted: u64,
    /// Entries left in place (fresh, unpushed under the hard cap, or in flight).
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

/// CLAUDE-245: walk `<logs_dir>/<action>/streams/*.jsonl` and delete every
/// stream log older than the retention window.
///
/// `in_flight` is the set of `(action, job_key)` pairs whose job is still
/// running, read by the caller from `run_inflight_jobs` joined to the runs that
/// have not ended. Age alone would not prove a run is finished: a job with a
/// long `claude_timeout_seconds` can outlive any window while its log is being
/// appended to, and deleting it mid-run destroys the only record of it. The
/// sweep runs in the daemon that owns those runs, so that table is the
/// authority, and anything it names is kept regardless of age. A log path is
/// `<key>-attempt-<n>.jsonl` ([`crate::claude::ClaudeInvoker::stream_log_path`]),
/// so the key is the stem up to the last `-attempt-`; a file that does not match
/// that shape was not written by a run and is swept on age alone.
///
/// `now` is injected the same way [`sweep`] injects it, so a test can age a file
/// by passing a future instant. Per-entry filesystem errors are logged and
/// skipped; the pass never aborts for one bad file.
pub fn sweep_stream_logs(
    logs_dir: &Path,
    cfg: &StreamLogGcConfig,
    now: SystemTime,
    in_flight: &HashSet<(String, String)>,
) -> SweepStats {
    let mut stats = SweepStats::default();
    if !cfg.enabled {
        return stats;
    }
    let retention = Duration::from_secs(cfg.retention_days.saturating_mul(SECS_PER_DAY));

    let actions = match std::fs::read_dir(logs_dir) {
        Ok(r) => r,
        // Missing logs dir (nothing has run yet) is a clean no-op, exactly as
        // `sweep` treats a missing workdir base.
        Err(_) => return stats,
    };
    for action in actions.flatten() {
        let streams_dir = action.path().join("streams");
        if !streams_dir.is_dir() {
            continue;
        }
        let action_name = action.file_name().to_string_lossy().into_owned();
        let entries = match std::fs::read_dir(&streams_dir) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %streams_dir.display(), error = %e, "stream-log GC: cannot read streams dir; skipping");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // The directory holds only stream logs; anything else is left alone.
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = stream_log_key(&name);
            if let Some(k) = &key {
                if in_flight.contains(&(action_name.clone(), k.clone())) {
                    debug!(
                        action = %action_name,
                        key = %k,
                        path = %path.display(),
                        "stream-log GC: kept; the job is still in flight"
                    );
                    stats.kept += 1;
                    continue;
                }
            }
            let age = dir_age(&path, now);
            if age < retention {
                stats.kept += 1;
                continue;
            }
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    debug!(
                        action = %action_name,
                        path = %path.display(),
                        age = %humantime::format_duration(whole_secs(age)),
                        bytes_reclaimed = bytes,
                        "stream-log GC: deleted a stream log older than the retention window"
                    );
                    stats.deleted += 1;
                    stats.bytes_reclaimed += bytes;
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "stream-log GC: delete failed (continuing)");
                    stats.kept += 1;
                }
            }
        }
    }
    stats
}

/// Job key encoded in a stream-log file name (`<key>-attempt-<n>.jsonl`), or
/// `None` when the name does not carry one.
fn stream_log_key(file_name: &str) -> Option<String> {
    let stem = file_name.strip_suffix(".jsonl")?;
    let (key, attempt) = stem.rsplit_once("-attempt-")?;
    // Only a numeric attempt suffix is ours; `foo-attempt-bogus.jsonl` is not.
    if key.is_empty() || attempt.is_empty() || !attempt.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(key.to_string())
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

    // ---- CLAUDE-245: stream-log sweep --------------------------------------

    fn log_cfg(enabled: bool) -> StreamLogGcConfig {
        StreamLogGcConfig {
            enabled,
            retention_days: 30,
        }
    }

    /// Create `<logs>/<action>/streams/<key>-attempt-<n>.jsonl` with `bytes`
    /// bytes of content.
    fn make_stream_log(
        logs: &Path,
        action: &str,
        key: &str,
        attempt: u32,
        bytes: usize,
    ) -> std::path::PathBuf {
        let dir = logs.join(action).join("streams");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{key}-attempt-{attempt}.jsonl"));
        std::fs::write(&path, vec![b'x'; bytes]).unwrap();
        path
    }

    fn in_flight(pairs: &[(&str, &str)]) -> HashSet<(String, String)> {
        pairs
            .iter()
            .map(|(a, k)| (a.to_string(), k.to_string()))
            .collect()
    }

    #[test]
    fn stale_stream_log_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_stream_log(tmp.path(), "youtrack", "CLAUDE-1", 1, 4096);
        // 60 days "from now": older than the 30-day window.
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(60),
            &in_flight(&[]),
        );
        assert!(
            !log.exists(),
            "a stream log past the window must be deleted"
        );
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.kept, 0);
        assert_eq!(stats.bytes_reclaimed, 4096);
    }

    #[test]
    fn fresh_stream_log_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_stream_log(tmp.path(), "youtrack", "CLAUDE-2", 1, 10);
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now(),
            &in_flight(&[]),
        );
        assert!(log.exists(), "a stream log inside the window must be kept");
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
    }

    /// AC: age is not evidence a run has finished. A job still in flight keeps
    /// its log no matter how far past the window the file's mtime sits.
    #[test]
    fn in_flight_stream_log_is_kept_regardless_of_age() {
        let tmp = tempfile::tempdir().unwrap();
        let live = make_stream_log(tmp.path(), "youtrack", "CLAUDE-3", 2, 10);
        let done = make_stream_log(tmp.path(), "youtrack", "CLAUDE-4", 1, 10);
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[("youtrack", "CLAUDE-3")]),
        );
        assert!(
            live.exists(),
            "an in-flight job's log must never be deleted"
        );
        assert!(!done.exists(), "a finished job's aged log is still swept");
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted, 1);
    }

    /// The in-flight guard is keyed on (action, key), so the same job key under
    /// a different action is not protected by proxy.
    #[test]
    fn in_flight_guard_is_scoped_to_its_action() {
        let tmp = tempfile::tempdir().unwrap();
        let other = make_stream_log(tmp.path(), "YouTrack", "CLAUDE-3", 1, 10);
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[("youtrack", "CLAUDE-3")]),
        );
        assert!(!other.exists(), "another action's aged log is swept");
        assert_eq!(stats.deleted, 1);
    }

    #[test]
    fn disabled_stream_log_sweep_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_stream_log(tmp.path(), "youtrack", "CLAUDE-5", 1, 10);
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(false),
            SystemTime::now() + days(365),
            &in_flight(&[]),
        );
        assert!(log.exists(), "a disabled sweep must not delete");
        assert_eq!(stats, SweepStats::default());
    }

    /// AC: a missing `logs/` directory is a clean no-op, matching `sweep`'s
    /// behaviour on a missing base dir.
    #[test]
    fn missing_logs_dir_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("does-not-exist");
        let stats = sweep_stream_logs(
            &logs,
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[]),
        );
        assert_eq!(stats, SweepStats::default());
    }

    /// The daily runner logs (`logs/<action>/YYYY-MM-DD.log`) and any other file
    /// beside `streams/` are not this sweep's business and stay untouched.
    #[test]
    fn only_the_streams_dir_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let daily = tmp.path().join("youtrack").join("2026-01-01.log");
        std::fs::create_dir_all(daily.parent().unwrap()).unwrap();
        std::fs::write(&daily, b"line").unwrap();
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[]),
        );
        assert!(daily.exists(), "a daily runner log is not a stream log");
        assert_eq!(stats, SweepStats::default());
    }

    #[test]
    fn stream_log_key_parses_only_our_shape() {
        assert_eq!(
            stream_log_key("CLAUDE-9-attempt-2.jsonl").as_deref(),
            Some("CLAUDE-9")
        );
        // A key that itself contains the separator: the LAST one wins.
        assert_eq!(
            stream_log_key("odd-attempt-key-attempt-1.jsonl").as_deref(),
            Some("odd-attempt-key")
        );
        assert_eq!(stream_log_key("CLAUDE-9-attempt-bogus.jsonl"), None);
        assert_eq!(stream_log_key("CLAUDE-9.jsonl"), None);
        assert_eq!(stream_log_key("CLAUDE-9-attempt-1.txt"), None);
    }
}
