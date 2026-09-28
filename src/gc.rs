//! GC sweepers for the unbounded on-disk trees: preserved and aside workdirs
//! ([`sweep`]), per-attempt stream logs ([`sweep_stream_logs`]), the two
//! size-bounded files under `logs/` ([`sweep_oversized_logs`]), per-job
//! snippet directories ([`sweep_snippets`]), and redacted webhook CI logs
//! ([`sweep_webhook_logs`]). All are scheduled by the daemon on the same tick
//! and read their own DB-backed retention window.
//!
//! The YouTrack runner intentionally keeps some workdirs rather than deleting
//! them: [`crate::native::youtrack`] preserves a directory that holds
//! local-only work (unpushed commits, a dirty tree, or a stash), and moves a
//! leftover workdir aside (`<key>.kept-<n>`) out of the way of a fresh
//! attempt. Nothing else ever deletes these, so preserved and aside workdirs
//! accumulate without bound under
//! `$CLAUDE_RUN_DATA_DIR/workdirs/<source>/<key>` (a single preserved Rust
//! workdir can be tens of GB of `target/` output).
//!
//! [`sweep`] walks `<base>/*/*` and deletes any directory older than the
//! configured retention window. A directory that still holds local-only work
//! (unpushed commits, a dirty tracked/untracked tree, or a stash) is
//! respected (kept) until a separate, longer hard-cap
//! age, so genuinely un-saved work is not discarded the moment the window
//! passes. Every deletion is logged with action, key, path, age, and bytes
//! reclaimed so a sweep is auditable.
//!
//! The local-only-work predicate is injected (production passes
//! [`crate::native::youtrack::has_local_only_work`]) so the decision logic is
//! testable without a real git repository.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use crate::config::{StreamLogGcConfig, WorkdirGcConfig};

const SECS_PER_DAY: u64 = 86_400;

/// The daemon's own stdout/stderr file, opened by `detach_now` in
/// `src/bin/claude-run.rs` and bounded by [`sweep_oversized_logs`].
const DAEMON_LOG: &str = "daemon.log";

/// Suffix of the single rolled-over predecessor [`bound_file`] leaves behind.
/// The walk skips anything carrying it, so a predecessor is never itself rolled
/// over into a second generation.
const ROTATED_SUFFIX: &str = ".1";

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
/// retention window, keeping workdirs holding local-only work (unpushed
/// commits, a dirty tree, or a stash) until the longer hard cap.
///
/// `now` is injected (production passes [`SystemTime::now`]) so a test can age a
/// directory by passing a future instant rather than mutating its mtime. A
/// directory whose mtime is unreadable, or is newer than `now`, is treated as
/// age zero (kept). Filesystem errors on individual entries are logged and
/// skipped; the sweep never aborts the whole pass for one bad directory.
pub fn sweep<F>(
    base: &Path,
    cfg: &WorkdirGcConfig,
    now: SystemTime,
    has_local_only_work: F,
) -> SweepStats
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
        // Missing base dir (no workdirs created yet) is a clean no-op; any other
        // error means the sweep swept nothing, so it is logged rather than
        // reported as an empty pass.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            warn!(path = %base.display(), error = %e, "workdir GC: cannot read the workdir base; nothing was swept this pass");
            return stats;
        }
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
            let local_only_work = has_local_only_work(&path);
            if local_only_work && age < hard_cap {
                debug!(
                    action = %action,
                    key = %key_name,
                    path = %path.display(),
                    age = %humantime::format_duration(whole_secs(age)),
                    "workdir GC: kept; holds local-only work and is under the hard cap"
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
                        local_only_work,
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

/// Walk `<logs_dir>/<action>/streams/*.jsonl` and delete every stream log
/// older than the retention window.
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
        // `sweep` treats a missing workdir base; any other error is logged.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            warn!(path = %logs_dir.display(), error = %e, "stream-log GC: cannot read the logs dir; nothing was swept this pass");
            return stats;
        }
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

/// Walk `<state_dir>/snippets/<action>/<job_key>/` and delete every snippet
/// directory older than `cfg.snippet_retention_days`, keeping one whose job
/// is still in flight regardless of age.
///
/// The shape is the per-source/per-key walk [`sweep`] already does for
/// workdirs, but with the stream-log sweep's in-flight guard rather than the
/// unpushed-commit one: a snippet directory holds no git history to preserve,
/// only the rendered scripts and JSON bindings of one job's `filter` / `pre` /
/// `prompt` / `post` steps (`binding-job.json` carries the whole job payload,
/// so the per-job cost is not negligible). `in_flight` is the same
/// `(action, job_key)` set [`sweep_stream_logs`] reads from `run_inflight_jobs`
/// joined to runs that have not ended; a failed read of that set must skip this
/// sweep too; sweeping with an empty set would delete a live run's own
/// snippet directory the moment its age crosses the window.
///
/// `now` is injected the same way [`sweep`] and [`sweep_stream_logs`] inject
/// it. Per-entry filesystem errors are logged and skipped; the pass never
/// aborts for one bad directory.
pub fn sweep_snippets(
    base: &Path,
    cfg: &StreamLogGcConfig,
    now: SystemTime,
    in_flight: &HashSet<(String, String)>,
) -> SweepStats {
    let mut stats = SweepStats::default();
    if !cfg.enabled {
        return stats;
    }
    let retention = Duration::from_secs(cfg.snippet_retention_days.saturating_mul(SECS_PER_DAY));

    let actions = match std::fs::read_dir(base) {
        Ok(r) => r,
        // Missing base dir (no scripted step has ever run) is a clean no-op,
        // exactly as `sweep` treats a missing workdir base.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            warn!(path = %base.display(), error = %e, "snippet GC: cannot read the snippets dir; nothing was swept this pass");
            return stats;
        }
    };
    for action in actions.flatten() {
        let action_path = action.path();
        if !action_path.is_dir() {
            continue;
        }
        let action_name = action.file_name().to_string_lossy().into_owned();
        let keys = match std::fs::read_dir(&action_path) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %action_path.display(), error = %e, "snippet GC: cannot read action dir; skipping");
                continue;
            }
        };
        for key in keys.flatten() {
            let path = key.path();
            if !path.is_dir() {
                continue;
            }
            let key_name = key.file_name().to_string_lossy().into_owned();
            if in_flight.contains(&(action_name.clone(), key_name.clone())) {
                debug!(
                    action = %action_name,
                    key = %key_name,
                    path = %path.display(),
                    "snippet GC: kept; the job is still in flight"
                );
                stats.kept += 1;
                continue;
            }
            let age = dir_age(&path, now);
            if age < retention {
                stats.kept += 1;
                continue;
            }
            let bytes = dir_size(&path);
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {
                    debug!(
                        action = %action_name,
                        key = %key_name,
                        path = %path.display(),
                        age = %humantime::format_duration(whole_secs(age)),
                        bytes_reclaimed = bytes,
                        "snippet GC: deleted a snippet directory older than the retention window"
                    );
                    stats.deleted += 1;
                    stats.bytes_reclaimed += bytes;
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "snippet GC: delete failed (continuing)");
                    stats.kept += 1;
                }
            }
        }
    }
    stats
}

/// Walk the files directly under `<state_dir>/webhook-logs/` and delete every
/// one older than `cfg.webhook_log_retention_days`.
///
/// Only files DIRECTLY under `webhook-logs/` are considered: `write_artifact`
/// ([`crate::forge_log`]) is the only writer that puts one there, one per
/// triaged CI run. `webhook-logs/triage/` is a subdirectory, not a file, so the
/// walk skips it: it is the mint classifier's reused working directory
/// ([`crate::daemon::build_mint_context`]), not a per-run artifact, and ageing
/// it out from under an in-progress triage would break that triage rather than
/// reclaim anything meaningful.
///
/// `now` is injected the same way the other sweeps in this module inject it.
/// Per-entry filesystem errors are logged and skipped; the pass never aborts
/// for one bad file.
pub fn sweep_webhook_logs(dir: &Path, cfg: &StreamLogGcConfig, now: SystemTime) -> SweepStats {
    let mut stats = SweepStats::default();
    if !cfg.enabled {
        return stats;
    }
    let retention =
        Duration::from_secs(cfg.webhook_log_retention_days.saturating_mul(SECS_PER_DAY));

    let entries = match std::fs::read_dir(dir) {
        Ok(r) => r,
        // Missing dir (no webhook run has ever been triaged) is a clean no-op.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            warn!(path = %dir.display(), error = %e, "webhook-log GC: cannot read the webhook-logs dir; nothing was swept this pass");
            return stats;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
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
                    path = %path.display(),
                    age = %humantime::format_duration(whole_secs(age)),
                    bytes_reclaimed = bytes,
                    "webhook-log GC: deleted a webhook log older than the retention window"
                );
                stats.deleted += 1;
                stats.bytes_reclaimed += bytes;
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "webhook-log GC: delete failed (continuing)");
                stats.kept += 1;
            }
        }
    }
    stats
}

/// Tally of one [`sweep_oversized_logs`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BoundStats {
    /// Files rolled over and truncated back under their ceiling this pass.
    pub bounded: u64,
    /// Files left alone (already under their ceiling, or ceiling disabled).
    pub kept: u64,
    /// Total bytes reclaimed from the bounded files themselves. The rolled-over
    /// predecessors are not subtracted: they are a second copy on disk until the
    /// next rotation replaces them.
    pub bytes_reclaimed: u64,
}

/// Bound the append-forever files under `<logs_dir>` by SIZE: `daemon.log`,
/// `usage.jsonl`, and every `logs/<action>/<output.file>`.
///
/// None is a per-run artifact, so the age window [`sweep_stream_logs`] uses
/// does not apply: each is one file that a long-lived daemon appends to for the
/// life of the install. Past its ceiling a file is rolled to `<name>.1` and
/// truncated in place back to its newest half (see [`bound_file`]).
///
/// `usage.jsonl` is newline-delimited JSON, and [`bound_file`] already cuts at
/// the first line boundary inside the retained window, so the survivor is
/// still parseable line by line rather than starting mid-record.
///
/// The action-directory walk treats every regular file directly under
/// `logs/<action>/` as an output sink, because `append_output_record`
/// ([`crate::job`]) is the only writer that puts one there; `streams/` is a
/// directory and is skipped, as is any `<name>.1` predecessor. Per-entry
/// filesystem errors are logged and skipped; the pass never aborts for one bad
/// file.
pub fn sweep_oversized_logs(logs_dir: &Path, cfg: &StreamLogGcConfig) -> BoundStats {
    let mut stats = BoundStats::default();
    if !cfg.enabled {
        return stats;
    }
    bound_one(
        &logs_dir.join(DAEMON_LOG),
        cfg.daemon_log_max_bytes,
        "daemon log",
        &mut stats,
    );
    // Sits directly under `logs/`, so the action walk below (which only
    // descends into directories) never reaches it. Bounded explicitly, the
    // same way `daemon.log` is.
    bound_one(
        &logs_dir.join(crate::config::USAGE_LOG),
        cfg.usage_log_max_bytes,
        "usage log",
        &mut stats,
    );

    let actions = match std::fs::read_dir(logs_dir) {
        Ok(r) => r,
        // Missing logs dir (nothing has run yet) is a clean no-op; anything else
        // means the pass silently bounded nothing, so it is logged.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return stats,
        Err(e) => {
            warn!(path = %logs_dir.display(), error = %e, "log size GC: cannot read the logs dir; nothing was bounded this pass");
            return stats;
        }
    };
    for action in actions.flatten() {
        let dir = action.path();
        if !dir.is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %dir.display(), error = %e, "log size GC: cannot read action log dir; skipping");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // `streams/` is a directory, so this also excludes the stream logs:
            // those are swept by age, never truncated.
            if !path.is_file() {
                continue;
            }
            if entry
                .file_name()
                .to_string_lossy()
                .ends_with(ROTATED_SUFFIX)
            {
                continue;
            }
            bound_one(&path, cfg.output_file_max_bytes, "output file", &mut stats);
        }
    }
    stats
}

/// Apply [`bound_file`] to one path, folding the outcome into `stats` and
/// logging what happened. Only a MISSING file is silent (a foreground daemon
/// writes no `daemon.log`, an action may have written no records yet) and it is
/// not counted either way; every other error is a file this pass failed to
/// bound, so it is logged rather than dropped.
fn bound_one(path: &Path, max_bytes: u64, what: &str, stats: &mut BoundStats) {
    match bound_file(path, max_bytes) {
        Ok(Some((was, now))) => {
            info!(
                path = %path.display(),
                kind = what,
                bytes_before = was,
                bytes_after = now,
                bytes_reclaimed = was - now,
                predecessor = %rotated_path(path).display(),
                "log size GC: rolled over a log past its size ceiling and kept its tail"
            );
            stats.bounded += 1;
            stats.bytes_reclaimed += was - now;
        }
        Ok(None) => stats.kept += 1,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(path = %path.display(), kind = what, error = %e, "log size GC: bounding failed (continuing)");
            stats.kept += 1;
        }
    }
}

/// Roll `path` over and truncate it back under `max_bytes`, returning the
/// `(before, after)` byte sizes when it acted and `None` when the file was
/// already inside its ceiling (or the ceiling is disabled with `0`).
///
/// The newest `max_bytes / 2` bytes are kept, starting at the first line
/// boundary inside that window so every retained line is whole (a window
/// carrying no newline at all is kept verbatim rather than emptied). The full
/// previous content goes to `<name>.1`, one generation only.
///
/// The truncation is IN PLACE, not a rename: the daemon holds this file open as
/// its stdout/stderr, so a rename would leave it appending to the predecessor
/// and the live log would stay empty. With `O_APPEND` the daemon's next write
/// lands at the new end of file. A write racing this rewrite can lose a line,
/// which is the accepted cost of bounding a file that is open for append.
fn bound_file(path: &Path, max_bytes: u64) -> std::io::Result<Option<(u64, u64)>> {
    // Stat first even when the ceiling is disabled, so a missing file reports as
    // `NotFound` (silent, uncounted) rather than as a file that was kept.
    let len = std::fs::metadata(path)?.len();
    if max_bytes == 0 {
        return Ok(None);
    }
    if len <= max_bytes {
        return Ok(None);
    }
    let keep = max_bytes / 2;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    file.seek(SeekFrom::Start(len - keep))?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail)?;
    if let Some(nl) = tail.iter().position(|b| *b == b'\n') {
        tail.drain(..=nl);
    }

    // Copy before the rewrite: the predecessor is the only surviving copy of
    // what the truncation drops.
    std::fs::copy(path, rotated_path(path))?;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&tail)?;
    file.set_len(tail.len() as u64)?;
    file.flush()?;
    Ok(Some((len, tail.len() as u64)))
}

/// `<path>.1`, the single rolled-over predecessor of `path`.
fn rotated_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(ROTATED_SUFFIX);
    path.with_file_name(name)
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

    // ---- stream-log sweep ---------------------------------------------------

    fn log_cfg(enabled: bool) -> StreamLogGcConfig {
        StreamLogGcConfig {
            enabled,
            retention_days: 30,
            // The age sweep reads neither ceiling; `0` keeps these tests to the
            // one axis they are about.
            daemon_log_max_bytes: 0,
            output_file_max_bytes: 0,
            usage_log_max_bytes: 0,
            snippet_retention_days: 30,
            webhook_log_retention_days: 30,
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

    /// A file beside `streams/` is the action's `output.file` sink, which
    /// [`sweep_oversized_logs`] bounds by size. It is never age-swept, so this
    /// sweep leaves it untouched no matter how old it is.
    #[test]
    fn only_the_streams_dir_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = tmp.path().join("youtrack").join("outcomes.jsonl");
        std::fs::create_dir_all(sink.parent().unwrap()).unwrap();
        std::fs::write(&sink, b"line").unwrap();
        let stats = sweep_stream_logs(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[]),
        );
        assert!(sink.exists(), "an output.file sink is not a stream log");
        assert_eq!(stats, SweepStats::default());
    }

    // ---- snippet directories --------------------------------------------------

    /// Create `<base>/<action>/<key>/` with one file of `bytes` bytes inside,
    /// mirroring what [`crate::snippet::snippet_dir_for`] plus
    /// [`crate::snippet::SnippetRunner::run`] write for a job.
    fn make_snippet_dir(base: &Path, action: &str, key: &str, bytes: usize) -> std::path::PathBuf {
        let dir = base.join(action).join(key);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("binding-job.json"), vec![b'x'; bytes]).unwrap();
        dir
    }

    #[test]
    fn stale_snippet_dir_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_snippet_dir(tmp.path(), "youtrack", "CLAUDE-1", 100);
        let stats = sweep_snippets(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(60),
            &in_flight(&[]),
        );
        assert!(
            !dir.exists(),
            "a snippet directory past the window must be deleted"
        );
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.kept, 0);
        assert!(stats.bytes_reclaimed >= 100);
    }

    #[test]
    fn fresh_snippet_dir_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_snippet_dir(tmp.path(), "youtrack", "CLAUDE-2", 10);
        let stats = sweep_snippets(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now(),
            &in_flight(&[]),
        );
        assert!(dir.exists(), "a fresh snippet directory must be kept");
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
    }

    /// AC: a job still in flight keeps its snippet directory no matter how far
    /// past the window its mtime sits, the same guarantee
    /// [`in_flight_stream_log_is_kept_regardless_of_age`] proves for stream logs.
    #[test]
    fn in_flight_snippet_dir_is_kept_regardless_of_age() {
        let tmp = tempfile::tempdir().unwrap();
        let live = make_snippet_dir(tmp.path(), "youtrack", "CLAUDE-3", 10);
        let done = make_snippet_dir(tmp.path(), "youtrack", "CLAUDE-4", 10);
        let stats = sweep_snippets(
            tmp.path(),
            &log_cfg(true),
            SystemTime::now() + days(365),
            &in_flight(&[("youtrack", "CLAUDE-3")]),
        );
        assert!(
            live.exists(),
            "an in-flight job's snippet directory must never be deleted"
        );
        assert!(
            !done.exists(),
            "a finished job's aged snippet directory is still swept"
        );
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted, 1);
    }

    #[test]
    fn disabled_snippet_sweep_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_snippet_dir(tmp.path(), "youtrack", "CLAUDE-5", 10);
        let stats = sweep_snippets(
            tmp.path(),
            &log_cfg(false),
            SystemTime::now() + days(365),
            &in_flight(&[]),
        );
        assert!(dir.exists(), "a disabled sweep must not delete");
        assert_eq!(stats, SweepStats::default());
    }

    #[test]
    fn missing_snippets_dir_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("does-not-exist");
        let stats = sweep_snippets(&base, &log_cfg(true), SystemTime::now(), &in_flight(&[]));
        assert_eq!(stats, SweepStats::default());
    }

    // ---- webhook-logs/ --------------------------------------------------------

    fn make_webhook_log(dir: &Path, name: &str, bytes: usize) -> std::path::PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, vec![b'x'; bytes]).unwrap();
        path
    }

    #[test]
    fn stale_webhook_log_is_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_webhook_log(tmp.path(), "org_repo-run1-all-jobs.log", 42);
        let stats = sweep_webhook_logs(tmp.path(), &log_cfg(true), SystemTime::now() + days(60));
        assert!(
            !log.exists(),
            "a webhook log past the window must be deleted"
        );
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.bytes_reclaimed, 42);
    }

    #[test]
    fn fresh_webhook_log_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_webhook_log(tmp.path(), "org_repo-run2-all-jobs.log", 10);
        let stats = sweep_webhook_logs(tmp.path(), &log_cfg(true), SystemTime::now());
        assert!(log.exists(), "a webhook log inside the window must be kept");
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
    }

    /// AC: `webhook-logs/triage/` is the mint classifier's reused working
    /// directory, not a per-run artifact, so this sweep must never descend into
    /// it or delete it.
    #[test]
    fn triage_subdirectory_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let triage = tmp.path().join("triage");
        make_webhook_log(&triage, "triage-stream.jsonl", 10);
        let stats = sweep_webhook_logs(tmp.path(), &log_cfg(true), SystemTime::now() + days(365));
        assert!(
            triage.exists(),
            "the triage working directory must never be swept"
        );
        assert_eq!(stats, SweepStats::default());
    }

    #[test]
    fn disabled_webhook_log_sweep_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_webhook_log(tmp.path(), "org_repo-run3-all-jobs.log", 10);
        let stats = sweep_webhook_logs(tmp.path(), &log_cfg(false), SystemTime::now() + days(365));
        assert!(log.exists(), "a disabled sweep must not delete");
        assert_eq!(stats, SweepStats::default());
    }

    #[test]
    fn missing_webhook_logs_dir_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("does-not-exist");
        let stats = sweep_webhook_logs(&dir, &log_cfg(true), SystemTime::now());
        assert_eq!(stats, SweepStats::default());
    }

    // ---- size ceilings on daemon.log and output.file -------------------------

    /// `bytes` bytes of numbered lines, so a test can tell head from tail.
    fn numbered_lines(bytes: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes + 64);
        let mut n = 0u64;
        while out.len() < bytes {
            out.extend_from_slice(format!("line {n}\n").as_bytes());
            n += 1;
        }
        out
    }

    fn size_cfg(daemon_max: u64, output_max: u64) -> StreamLogGcConfig {
        size_cfg_with_usage(daemon_max, output_max, 0)
    }

    fn size_cfg_with_usage(daemon_max: u64, output_max: u64, usage_max: u64) -> StreamLogGcConfig {
        StreamLogGcConfig {
            enabled: true,
            retention_days: 30,
            daemon_log_max_bytes: daemon_max,
            output_file_max_bytes: output_max,
            usage_log_max_bytes: usage_max,
            snippet_retention_days: 30,
            webhook_log_retention_days: 30,
        }
    }

    /// AC: an oversized `daemon.log` comes back under the ceiling with its tail
    /// preserved, and the dropped head survives in the one predecessor.
    #[test]
    fn oversized_daemon_log_is_bounded_with_its_tail_preserved() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("daemon.log");
        let content = numbered_lines(40_000);
        std::fs::write(&log, &content).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(10_000, 0));
        let after = std::fs::read(&log).unwrap();
        assert!(
            after.len() as u64 <= 10_000,
            "the log must be back under the ceiling, got {}",
            after.len()
        );
        let tail_text = String::from_utf8(after.clone()).unwrap();
        let full_text = String::from_utf8(content.clone()).unwrap();
        assert!(
            full_text.ends_with(&tail_text),
            "what remains must be the tail of what was there"
        );
        assert!(
            tail_text.starts_with("line "),
            "the partial first line is dropped so every kept line is whole"
        );
        assert!(!tail_text.contains("line 0\n"), "the head is gone");

        // One rolled-over predecessor holds everything the truncation dropped.
        let prev = std::fs::read(tmp.path().join("daemon.log.1")).unwrap();
        assert_eq!(prev, content, "the predecessor is the full previous file");

        assert_eq!(stats.bounded, 1);
        assert_eq!(stats.kept, 0);
        assert_eq!(
            stats.bytes_reclaimed,
            (content.len() - after.len()) as u64,
            "the tally is what the truncation actually reclaimed"
        );
    }

    #[test]
    fn daemon_log_inside_its_ceiling_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("daemon.log");
        std::fs::write(&log, numbered_lines(1_000)).unwrap();
        let before = std::fs::read(&log).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(10_000, 0));
        assert_eq!(std::fs::read(&log).unwrap(), before);
        assert!(!tmp.path().join("daemon.log.1").exists());
        assert_eq!(stats.bounded, 0);
        assert_eq!(stats.kept, 1);
    }

    /// AC: the `output.file` sink is bounded by size and never deleted, so its
    /// newest records stay where the operator's consumer expects them.
    #[test]
    fn oversized_output_file_is_bounded_never_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("youtrack");
        std::fs::create_dir_all(&dir).unwrap();
        let sink = dir.join("outcomes.jsonl");
        std::fs::write(&sink, numbered_lines(40_000)).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(0, 10_000));
        assert!(sink.exists(), "the sink is bounded, never deleted");
        let after = std::fs::read(&sink).unwrap();
        assert!(after.len() as u64 <= 10_000);
        assert!(String::from_utf8(after).unwrap().starts_with("line "));
        assert!(dir.join("outcomes.jsonl.1").exists());
        assert_eq!(stats.bounded, 1);
    }

    /// A predecessor is never itself rolled over: it holds a full pre-truncation
    /// copy, so a second generation would grow `logs/` instead of bounding it.
    #[test]
    fn rolled_over_predecessor_is_not_rotated_again() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("youtrack");
        std::fs::create_dir_all(&dir).unwrap();
        let prev = dir.join("outcomes.jsonl.1");
        let content = numbered_lines(40_000);
        std::fs::write(&prev, &content).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(0, 10_000));
        assert_eq!(
            std::fs::read(&prev).unwrap(),
            content,
            "the predecessor is untouched"
        );
        assert!(!dir.join("outcomes.jsonl.1.1").exists());
        assert_eq!(stats, BoundStats::default());
    }

    /// The stream logs are swept by age, never truncated, so the size pass must
    /// not reach into `streams/`.
    #[test]
    fn the_size_pass_never_touches_a_stream_log() {
        let tmp = tempfile::tempdir().unwrap();
        let log = make_stream_log(tmp.path(), "youtrack", "CLAUDE-1", 1, 40_000);
        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(10_000, 10_000));
        assert_eq!(std::fs::metadata(&log).unwrap().len(), 40_000);
        assert_eq!(stats, BoundStats::default());
    }

    /// `0` is the documented opt-out for an operator who rotates the sink
    /// themselves; the master toggle parks the whole pass.
    ///
    /// AC: an oversized `usage.jsonl` comes back under its ceiling and every
    /// surviving line is still a whole JSON record. A cut that landed
    /// mid-object would leave the file unparseable, which is the specific way
    /// this sweep could corrupt the thing it is bounding.
    #[test]
    fn oversized_usage_log_stays_parseable_newline_delimited_json() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("usage.jsonl");
        let mut content = String::new();
        for i in 0..4_000 {
            content.push_str(&format!(
                r#"{{"run_id":"run-{i}","action":"youtrack","exit_code":0,"cost_usd":0.0125,"tool_calls":{i}}}"#
            ));
            content.push('\n');
        }
        std::fs::write(&log, &content).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg_with_usage(0, 0, 10_000));

        let after = std::fs::read_to_string(&log).unwrap();
        assert!(
            after.len() as u64 <= 10_000,
            "the usage log must be back under its ceiling, got {}",
            after.len()
        );
        assert!(!after.is_empty(), "the sweep must keep the newest records");
        assert!(
            content.ends_with(&after),
            "what remains must be the tail of what was there"
        );

        // The point of the test: every surviving line parses on its own.
        let mut parsed = 0;
        for line in after.lines() {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("surviving line is not whole JSON: {e}: {line}"));
            assert!(v.get("run_id").is_some(), "record lost its run_id: {line}");
            parsed += 1;
        }
        assert!(parsed > 0, "expected surviving records to parse");

        // The predecessor keeps everything the truncation dropped.
        let prev = std::fs::read_to_string(tmp.path().join("usage.jsonl.1")).unwrap();
        assert_eq!(prev, content, "the predecessor is the full previous file");

        assert_eq!(stats.bounded, 1);
        assert_eq!(stats.kept, 0);
    }

    /// AC: a usage log already inside its ceiling is left byte-for-byte alone
    /// and grows no predecessor.
    #[test]
    fn usage_log_inside_its_ceiling_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("usage.jsonl");
        let content = "{\"run_id\":\"run-1\",\"cost_usd\":0.01}\n";
        std::fs::write(&log, content).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg_with_usage(0, 0, 10_000));

        assert_eq!(std::fs::read_to_string(&log).unwrap(), content);
        assert!(!tmp.path().join("usage.jsonl.1").exists());
        assert_eq!(stats.bounded, 0);
        assert_eq!(stats.kept, 1);
    }

    /// The usage log sits directly under `logs/`, so the action-directory walk
    /// must not treat it as an `output.file` sink and bound it against the
    /// wrong ceiling.
    #[test]
    fn the_usage_log_is_not_swept_as_an_action_output_file() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("usage.jsonl");
        let content = numbered_lines(40_000);
        std::fs::write(&log, &content).unwrap();

        // A tight output-file ceiling and NO usage ceiling: if the walk reached
        // the usage log it would truncate it here.
        let stats = sweep_oversized_logs(tmp.path(), &size_cfg_with_usage(0, 100, 0));

        assert_eq!(
            std::fs::read(&log).unwrap(),
            content,
            "the usage log answers only to usage_log_max_bytes"
        );
        assert_eq!(stats.bounded, 0);
    }

    #[test]
    fn a_zero_ceiling_and_a_disabled_config_bound_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("daemon.log");
        let content = numbered_lines(40_000);
        std::fs::write(&log, &content).unwrap();

        let stats = sweep_oversized_logs(tmp.path(), &size_cfg(0, 0));
        assert_eq!(std::fs::read(&log).unwrap(), content);
        assert_eq!(stats.bounded, 0);

        let mut off = size_cfg(10_000, 10_000);
        off.enabled = false;
        let stats = sweep_oversized_logs(tmp.path(), &off);
        assert_eq!(std::fs::read(&log).unwrap(), content);
        assert_eq!(stats, BoundStats::default());
    }

    #[test]
    fn missing_logs_dir_bounds_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let stats = sweep_oversized_logs(&tmp.path().join("nope"), &size_cfg(10_000, 10_000));
        assert_eq!(stats, BoundStats::default());
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
