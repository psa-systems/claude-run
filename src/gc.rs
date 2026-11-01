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
use std::process::Command;
use std::time::{Duration, SystemTime};

use tracing::{debug, error, info, warn};

use crate::config::{DockerGcConfig, StreamLogGcConfig, WorkdirGcConfig};

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

// ---- Docker sidecar named-volume sweep (CLAUDE-433) -----------------------

/// Name prefix of a buildx state volume. Never removed: it is buildkit's own
/// cache, not a stack artifact, and recreating it is far more expensive than
/// the disk it costs.
pub const BUILDX_VOLUME_PREFIX: &str = "buildx_buildkit_";

/// One Docker named volume as reported by [`DockerVolumeClient::list_volumes`].
#[derive(Debug, Clone)]
pub struct DockerVolume {
    pub name: String,
    /// True when Docker reports a container currently references this volume.
    /// Age alone never overrides this: a reused volume is kept regardless of
    /// how old it is.
    pub in_use: bool,
    /// Docker's own `CreatedAt`. Docker records no last-used time, so age
    /// means time since creation; a volume reused minutes ago still ages from
    /// when it was first created.
    pub created_at: SystemTime,
    /// Size in bytes, when Docker reports one (a plain `docker volume
    /// inspect` does not compute it, so this is usually `None`).
    pub size_bytes: Option<u64>,
}

/// Why [`DockerVolumeClient::remove_volume`] failed, so the sweep can log a
/// volume Docker refused because it is in use at `info` (routine) and any
/// other failure at `error` (actionable).
#[derive(Debug)]
pub enum VolumeRemoveError {
    /// Docker refused the remove because a container now references the
    /// volume (a race with the `in_use` read above, or a session that started
    /// a container between this sweep's list and remove calls).
    InUse(String),
    Other(String),
}

/// SPEC-3.11 item 1: the sidecar's total Docker storage usage by category, in
/// bytes. Read once before a [`reclaim_docker_storage`] pass decides whether
/// the ceiling is exceeded, and the running total each step's before/after
/// log is computed from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DockerUsage {
    pub containers_bytes: u64,
    pub images_bytes: u64,
    pub volumes_bytes: u64,
    pub build_cache_bytes: u64,
}

impl DockerUsage {
    pub fn total_bytes(&self) -> u64 {
        self.containers_bytes
            .saturating_add(self.images_bytes)
            .saturating_add(self.volumes_bytes)
            .saturating_add(self.build_cache_bytes)
    }
}

/// One stopped (exited, created, or dead) container, as reported by
/// [`DockerVolumeClient::list_stopped_containers`].
#[derive(Debug, Clone)]
pub struct StoppedContainer {
    pub id: String,
    /// Time the container last stopped. Age is measured from this, not from
    /// creation, so a container that ran for weeks before exiting an hour ago
    /// is not removed as "stale" from when it was first created.
    pub finished_at: SystemTime,
    pub size_bytes: Option<u64>,
}

/// One Docker image, as reported by
/// [`DockerVolumeClient::list_dangling_images`] /
/// [`DockerVolumeClient::list_unused_images`].
#[derive(Debug, Clone)]
pub struct DockerImage {
    pub id: String,
    pub created_at: SystemTime,
    pub size_bytes: Option<u64>,
    /// True when a container (running or stopped) still references this
    /// image. Age alone never overrides this, the same rule
    /// [`DockerVolume::in_use`] follows for a named volume.
    pub in_use: bool,
}

/// Talks to the sidecar's Docker daemon on behalf of [`sweep_docker_volumes`]
/// and [`reclaim_docker_storage`]. Injected so the decision logic (age,
/// in-use, buildx-prefix, reclaim order) is testable without a real Docker
/// daemon; production passes [`CliDockerVolumeClient`].
pub trait DockerVolumeClient {
    /// Every named volume the daemon knows about. `Err` signals the daemon
    /// could not be reached (including `DOCKER_HOST` unset); the sweep treats
    /// that as "skip this pass" rather than guessing.
    fn list_volumes(&self) -> Result<Vec<DockerVolume>, String>;
    /// Remove one volume by name. Never passes `--force`: Docker's own refusal
    /// of an in-use volume is exactly the safety net this sweep relies on.
    fn remove_volume(&self, name: &str) -> Result<(), VolumeRemoveError>;

    /// SPEC-3.11 item 1: the sidecar's total usage by category. `Err` means
    /// the daemon could not be reached; [`reclaim_docker_storage`] treats that
    /// as "skip this pass, remove nothing" rather than guessing the total is
    /// zero, which would read as always-under-the-ceiling and never reclaim.
    fn usage(&self) -> Result<DockerUsage, String>;

    /// Every stopped container. Order is the reclaim function's job, not the
    /// client's.
    fn list_stopped_containers(&self) -> Result<Vec<StoppedContainer>, String>;
    /// Remove one container by id. Never `--force`: a container this listed
    /// as stopped may have been restarted between the list and this call, and
    /// Docker's own refusal of a running container is the safety net.
    fn remove_container(&self, id: &str) -> Result<(), String>;

    /// Every dangling (untagged, unreferenced by any tagged image) image.
    fn list_dangling_images(&self) -> Result<Vec<DockerImage>, String>;
    /// Every tagged image no container (running or stopped) references.
    fn list_unused_images(&self) -> Result<Vec<DockerImage>, String>;
    /// Remove one image by id.
    fn remove_image(&self, id: &str) -> Result<(), String>;

    /// SPEC-3.11: build cache cannot be removed by id with the `docker` CLI
    /// (there is no `docker builder rm <id>`), so this is the one exception
    /// to "remove by explicit id from a listing": it shells out to `docker
    /// builder prune`, scoped to `older_than` with a `--filter until=` so it
    /// never touches cache newer than that, and the force flag it needs to
    /// skip its confirmation prompt is safe here because the filter, not the
    /// confirmation, is what bounds its blast radius. Returns bytes
    /// reclaimed.
    fn prune_build_cache(&self, older_than: Duration) -> Result<u64, String>;
}

/// Production [`DockerVolumeClient`], talking to whichever daemon
/// `DOCKER_HOST` points the `docker` CLI at (the sidecar, in the deployed
/// container). Never passes `--all` to a list or prune call, which has no age
/// filter and would remove a volume a session created moments ago.
pub struct CliDockerVolumeClient;

impl DockerVolumeClient for CliDockerVolumeClient {
    fn list_volumes(&self) -> Result<Vec<DockerVolume>, String> {
        if std::env::var_os("DOCKER_HOST").is_none() {
            return Err("DOCKER_HOST is not set".to_string());
        }
        let all = run_docker(&["volume", "ls", "-q"])?;
        let names: Vec<&str> = all
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let dangling_out = run_docker(&["volume", "ls", "-f", "dangling=true", "-q"])?;
        let dangling: HashSet<&str> = dangling_out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();

        let mut args = vec!["volume", "inspect"];
        args.extend(names.iter().copied());
        let inspect_out = run_docker(&args)?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&inspect_out)
            .map_err(|e| format!("cannot parse `docker volume inspect` output: {e}"))?;

        let mut volumes = Vec::with_capacity(parsed.len());
        for v in parsed {
            let name = v
                .get("Name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            let created_at = v
                .get("CreatedAt")
                .and_then(|c| c.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .and_then(|dt| {
                    SystemTime::UNIX_EPOCH
                        .checked_add(Duration::from_secs(dt.timestamp().try_into().unwrap_or(0)))
                })
                // An unparseable/missing CreatedAt is treated as "now" (age
                // zero, kept this pass) rather than guessed as old enough to
                // remove.
                .unwrap_or_else(SystemTime::now);
            let in_use = !dangling.contains(name.as_str());
            volumes.push(DockerVolume {
                name,
                in_use,
                created_at,
                size_bytes: None,
            });
        }
        Ok(volumes)
    }

    fn remove_volume(&self, name: &str) -> Result<(), VolumeRemoveError> {
        let output = Command::new("docker")
            .args(["volume", "rm", name])
            .output()
            .map_err(|e| VolumeRemoveError::Other(format!("failed to spawn docker: {e}")))?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.contains("volume is in use") {
            Err(VolumeRemoveError::InUse(stderr))
        } else {
            Err(VolumeRemoveError::Other(stderr))
        }
    }

    fn usage(&self) -> Result<DockerUsage, String> {
        if std::env::var_os("DOCKER_HOST").is_none() {
            return Err("DOCKER_HOST is not set".to_string());
        }
        let out = run_docker(&["system", "df", "--format", "{{json .}}"])?;
        let mut usage = DockerUsage::default();
        for line in out.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| format!("cannot parse `docker system df` output: {e}"))?;
            let kind = v.get("Type").and_then(|t| t.as_str()).unwrap_or_default();
            let bytes = v
                .get("Size")
                .and_then(|s| s.as_str())
                .map(parse_docker_size)
                .unwrap_or(0);
            match kind {
                "Images" => usage.images_bytes = bytes,
                "Containers" => usage.containers_bytes = bytes,
                "Local Volumes" => usage.volumes_bytes = bytes,
                "Build Cache" => usage.build_cache_bytes = bytes,
                _ => {}
            }
        }
        Ok(usage)
    }

    fn list_stopped_containers(&self) -> Result<Vec<StoppedContainer>, String> {
        if std::env::var_os("DOCKER_HOST").is_none() {
            return Err("DOCKER_HOST is not set".to_string());
        }
        let ids_out = run_docker(&[
            "ps",
            "-a",
            "--filter",
            "status=exited",
            "--filter",
            "status=dead",
            "--filter",
            "status=created",
            "-q",
        ])?;
        let ids: Vec<&str> = ids_out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec!["container", "inspect", "--size"];
        args.extend(ids.iter().copied());
        let inspect_out = run_docker(&args)?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&inspect_out)
            .map_err(|e| format!("cannot parse `docker container inspect` output: {e}"))?;
        Ok(parsed
            .into_iter()
            .map(|c| {
                let id = c
                    .get("Id")
                    .and_then(|i| i.as_str())
                    .unwrap_or_default()
                    .to_string();
                let finished_at = c
                    .get("State")
                    .and_then(|s| s.get("FinishedAt"))
                    .and_then(|f| f.as_str())
                    .and_then(parse_rfc3339)
                    // An unparseable/missing FinishedAt is treated as "now"
                    // (age zero, kept this pass) rather than guessed old.
                    .unwrap_or_else(SystemTime::now);
                let size_bytes = c.get("SizeRootFs").and_then(|s| s.as_u64());
                StoppedContainer {
                    id,
                    finished_at,
                    size_bytes,
                }
            })
            .collect())
    }

    fn remove_container(&self, id: &str) -> Result<(), String> {
        run_docker(&["rm", id]).map(|_| ())
    }

    fn list_dangling_images(&self) -> Result<Vec<DockerImage>, String> {
        self.list_images(true)
    }

    fn list_unused_images(&self) -> Result<Vec<DockerImage>, String> {
        self.list_images(false)
    }

    fn remove_image(&self, id: &str) -> Result<(), String> {
        run_docker(&["rmi", id]).map(|_| ())
    }

    fn prune_build_cache(&self, older_than: Duration) -> Result<u64, String> {
        if std::env::var_os("DOCKER_HOST").is_none() {
            return Err("DOCKER_HOST is not set".to_string());
        }
        let filter = format!("until={}h", older_than.as_secs() / 3600);
        // SPEC-3.11: the one exception to "remove by explicit id from a
        // listing, never a prune command that needs a force flag" - there is
        // no `docker builder rm <id>`, so the engine's own prune call is the
        // only way to remove build cache at all. The `--filter` bounds its
        // blast radius the same way an explicit id list would elsewhere, so
        // the force flag only skips a confirmation prompt, not a safety
        // check.
        let out = run_docker(&["builder", "prune", "--force", "--filter", &filter])?;
        Ok(out
            .lines()
            .find_map(|l| l.trim().strip_prefix("Total reclaimed space:"))
            .map(|s| parse_docker_size(s.trim()))
            .unwrap_or(0))
    }
}

impl CliDockerVolumeClient {
    /// Shared listing for [`DockerVolumeClient::list_dangling_images`] and
    /// [`DockerVolumeClient::list_unused_images`]: `dangling` selects the
    /// Docker filter, and `in_use` is computed separately by cross-
    /// referencing every container's image id, since the dangling filter
    /// alone says nothing about container use.
    fn list_images(&self, dangling: bool) -> Result<Vec<DockerImage>, String> {
        if std::env::var_os("DOCKER_HOST").is_none() {
            return Err("DOCKER_HOST is not set".to_string());
        }
        let filter = format!("dangling={dangling}");
        let ids_out = run_docker(&["images", "-f", &filter, "-q"])?;
        let ids: Vec<&str> = ids_out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let referenced = referenced_image_ids()?;
        let mut args = vec!["image", "inspect"];
        args.extend(ids.iter().copied());
        let inspect_out = run_docker(&args)?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&inspect_out)
            .map_err(|e| format!("cannot parse `docker image inspect` output: {e}"))?;
        Ok(parsed
            .into_iter()
            .map(|img| {
                let id = img
                    .get("Id")
                    .and_then(|i| i.as_str())
                    .unwrap_or_default()
                    .to_string();
                let created_at = img
                    .get("Created")
                    .and_then(|c| c.as_str())
                    .and_then(parse_rfc3339)
                    .unwrap_or_else(SystemTime::now);
                let size_bytes = img.get("Size").and_then(|s| s.as_u64());
                let in_use = referenced.contains(&id);
                DockerImage {
                    id,
                    created_at,
                    size_bytes,
                    in_use,
                }
            })
            .collect())
    }
}

/// Image ids (`docker inspect --format '{{.Image}}'`) referenced by any
/// container, running or stopped, so the image listings above can mark an
/// image `in_use` regardless of its own tag/dangling status.
fn referenced_image_ids() -> Result<HashSet<String>, String> {
    let ids_out = run_docker(&["ps", "-a", "-q"])?;
    let ids: Vec<&str> = ids_out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let mut args = vec!["inspect", "--format", "{{.Image}}"];
    args.extend(ids.iter().copied());
    let out = run_docker(&args)?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// Parse an RFC3339 timestamp (as Docker's JSON fields report it) into a
/// [`SystemTime`]. `None` on anything unparseable, left to the caller's own
/// "treat as now" fallback rather than guessed as old.
fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    chrono::DateTime::parse_from_rfc3339(s).ok().and_then(|dt| {
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(dt.timestamp().try_into().unwrap_or(0)))
    })
}

/// Parse a Docker-formatted size string (e.g. `"7.904GB"`, `"512MB"`,
/// `"0B"`) into bytes. Docker's own `go-units` formats these with decimal
/// (1000-based) multiples. An unparseable or unrecognized unit yields 0
/// rather than guessing, since this only ever feeds a reclaim decision
/// (never a removal target), so undercounting is the safe direction.
fn parse_docker_size(s: &str) -> u64 {
    let s = s.trim();
    let Some(idx) = s.find(|c: char| c.is_ascii_alphabetic()) else {
        return s.parse::<f64>().unwrap_or(0.0) as u64;
    };
    let (num, unit) = (s[..idx].trim(), s[idx..].trim());
    let Ok(num) = num.parse::<f64>() else {
        return 0;
    };
    let multiplier: f64 = match unit.to_ascii_uppercase().as_str() {
        "B" => 1.0,
        "KB" => 1_000.0,
        "MB" => 1_000_000.0,
        "GB" => 1_000_000_000.0,
        "TB" => 1_000_000_000_000.0,
        "PB" => 1_000_000_000_000_000.0,
        _ => return 0,
    };
    (num * multiplier) as u64
}

/// Run `docker <args>` against whatever `DOCKER_HOST` points at, returning
/// stdout on success and a message naming the command on failure (spawn
/// failure or non-zero exit).
fn run_docker(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("failed to spawn `docker {}`: {e}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "`docker {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Remove every named volume in the sidecar's Docker daemon that no container
/// references and that is older than `cfg.volume_retention_days`, keeping a
/// `buildx_buildkit_*` state volume regardless of age.
///
/// `client` is injected ([`CliDockerVolumeClient`] in production) so the
/// decision logic is testable without a real Docker daemon. `now` is injected
/// the same way the other sweeps in this module inject it. When the daemon
/// cannot be reached (including `DOCKER_HOST` unset), the pass is SKIPPED and
/// logged at `warn` naming the cause - never reported as a clean sweep, and
/// nothing is removed. A remove Docker refuses because the volume is now in
/// use is logged at `info` and left in place; any other remove failure is
/// logged at `error` naming the volume, and the pass continues.
pub fn sweep_docker_volumes(
    client: &dyn DockerVolumeClient,
    cfg: &DockerGcConfig,
    now: SystemTime,
) -> SweepStats {
    let mut stats = SweepStats::default();
    if !cfg.enabled {
        return stats;
    }
    let retention = Duration::from_secs(cfg.volume_retention_days.saturating_mul(SECS_PER_DAY));

    let volumes = match client.list_volumes() {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "docker volume GC: cannot reach the sidecar's Docker daemon; nothing was swept this pass");
            return stats;
        }
    };

    for vol in volumes {
        if vol.in_use {
            stats.kept += 1;
            continue;
        }
        if vol.name.starts_with(BUILDX_VOLUME_PREFIX) {
            stats.kept += 1;
            continue;
        }
        let age = now.duration_since(vol.created_at).unwrap_or(Duration::ZERO);
        if age < retention {
            stats.kept += 1;
            continue;
        }
        match client.remove_volume(&vol.name) {
            Ok(()) => {
                info!(
                    name = %vol.name,
                    age = %humantime::format_duration(whole_secs(age)),
                    bytes_reclaimed = ?vol.size_bytes,
                    "docker volume GC: removed unused named volume older than the retention window"
                );
                stats.deleted += 1;
                stats.bytes_reclaimed += vol.size_bytes.unwrap_or(0);
            }
            Err(VolumeRemoveError::InUse(msg)) => {
                info!(name = %vol.name, reason = %msg, "docker volume GC: volume is now in use; kept");
                stats.kept += 1;
            }
            Err(VolumeRemoveError::Other(msg)) => {
                error!(name = %vol.name, error = %msg, "docker volume GC: remove failed");
                stats.kept += 1;
            }
        }
    }
    info!(
        deleted = stats.deleted,
        kept = stats.kept,
        bytes_reclaimed = stats.bytes_reclaimed,
        "docker volume GC sweep complete"
    );
    stats
}

// ---- Docker sidecar storage ceiling (SPEC-3.11, CLAUDE-516) ---------------

/// One SPEC-3.11 reclaim step's result, for the "category, bytes reclaimed,
/// size before and after" line each step logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimStep {
    pub category: &'static str,
    pub bytes_reclaimed: u64,
    pub before_bytes: u64,
    pub after_bytes: u64,
}

/// Result of a [`reclaim_docker_storage`] pass. An empty `steps` list means
/// nothing was reclaimed, which is a CLEAN pass only when `usage_read_failed`
/// is also `false` (disabled, skipped for an in-flight run, or already under
/// the ceiling); a failed usage read is never reported as clean.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReclaimStats {
    pub steps: Vec<ReclaimStep>,
    pub usage_read_failed: bool,
}

fn record_step(
    stats: &mut ReclaimStats,
    total: &mut u64,
    category: &'static str,
    bytes_reclaimed: u64,
) {
    let before = *total;
    let after = before.saturating_sub(bytes_reclaimed);
    info!(
        category,
        bytes_reclaimed,
        before_bytes = before,
        after_bytes = after,
        "docker storage GC: reclaim step complete"
    );
    stats.steps.push(ReclaimStep {
        category,
        bytes_reclaimed,
        before_bytes: before,
        after_bytes: after,
    });
    *total = after;
}

/// SPEC-3.11 step 1: stopped containers older than 1 day. Skipped entirely
/// (logged at `error`, zero reclaimed) when the listing cannot be read.
fn reclaim_stopped_containers(client: &dyn DockerVolumeClient, now: SystemTime) -> u64 {
    let containers = match client.list_stopped_containers() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "docker storage GC: cannot list stopped containers; step reclaimed nothing");
            return 0;
        }
    };
    let retention = Duration::from_secs(SECS_PER_DAY);
    let mut reclaimed = 0u64;
    for c in containers {
        let age = now.duration_since(c.finished_at).unwrap_or(Duration::ZERO);
        if age < retention {
            continue;
        }
        match client.remove_container(&c.id) {
            Ok(()) => {
                info!(
                    id = %c.id,
                    age = %humantime::format_duration(whole_secs(age)),
                    bytes_reclaimed = ?c.size_bytes,
                    "docker storage GC: removed stopped container older than 1 day"
                );
                reclaimed += c.size_bytes.unwrap_or(0);
            }
            Err(e) => error!(id = %c.id, error = %e, "docker storage GC: container remove failed"),
        }
    }
    reclaimed
}

/// SPEC-3.11 step 2: dangling images, no age filter. An image a running or
/// stopped container still references is kept regardless.
fn reclaim_dangling_images(client: &dyn DockerVolumeClient) -> u64 {
    let images = match client.list_dangling_images() {
        Ok(i) => i,
        Err(e) => {
            error!(error = %e, "docker storage GC: cannot list dangling images; step reclaimed nothing");
            return 0;
        }
    };
    reclaim_images(client, images, None, SystemTime::now())
}

/// SPEC-3.11 step 4: tagged images no container references, older than 7
/// days.
fn reclaim_unused_images(client: &dyn DockerVolumeClient, now: SystemTime) -> u64 {
    let images = match client.list_unused_images() {
        Ok(i) => i,
        Err(e) => {
            error!(error = %e, "docker storage GC: cannot list unused images; step reclaimed nothing");
            return 0;
        }
    };
    reclaim_images(
        client,
        images,
        Some(Duration::from_secs(7 * SECS_PER_DAY)),
        now,
    )
}

fn reclaim_images(
    client: &dyn DockerVolumeClient,
    images: Vec<DockerImage>,
    retention: Option<Duration>,
    now: SystemTime,
) -> u64 {
    let mut reclaimed = 0u64;
    for img in images {
        if img.in_use {
            continue;
        }
        if let Some(retention) = retention {
            let age = now.duration_since(img.created_at).unwrap_or(Duration::ZERO);
            if age < retention {
                continue;
            }
        }
        match client.remove_image(&img.id) {
            Ok(()) => {
                info!(
                    id = %img.id,
                    bytes_reclaimed = ?img.size_bytes,
                    "docker storage GC: removed image"
                );
                reclaimed += img.size_bytes.unwrap_or(0);
            }
            Err(e) => error!(id = %img.id, error = %e, "docker storage GC: image remove failed"),
        }
    }
    reclaimed
}

/// SPEC-3.11 step 3: build cache older than 7 days, through the one
/// prune-call exception [`DockerVolumeClient::prune_build_cache`] documents.
fn reclaim_build_cache(client: &dyn DockerVolumeClient) -> u64 {
    match client.prune_build_cache(Duration::from_secs(7 * SECS_PER_DAY)) {
        Ok(bytes) => {
            info!(
                bytes_reclaimed = bytes,
                "docker storage GC: pruned build cache older than 7 days"
            );
            bytes
        }
        Err(e) => {
            error!(error = %e, "docker storage GC: build cache prune failed");
            0
        }
    }
}

/// SPEC-3.11: when the sidecar's total Docker storage exceeds
/// `cfg.max_bytes`, reclaim in order (stopped containers older than 1 day,
/// dangling images, build cache older than 7 days, unused images older than 7
/// days, then the existing unreferenced-volume sweep) and stop as soon as the
/// running total is back under the ceiling. Nothing runs when `cfg.enabled`
/// is `false`, when `run_in_flight` is `true` (any run may be using the
/// sidecar), or when the initial usage read fails - the last case sets
/// [`ReclaimStats::usage_read_failed`] so the pass is never read as clean. A
/// failed category read or removal is logged at `error` and the pass
/// continues with the next category.
pub fn reclaim_docker_storage(
    client: &dyn DockerVolumeClient,
    cfg: &DockerGcConfig,
    now: SystemTime,
    run_in_flight: bool,
) -> ReclaimStats {
    let mut stats = ReclaimStats::default();
    if !cfg.enabled {
        return stats;
    }
    if run_in_flight {
        info!("docker storage GC: skipped this pass, a run is in flight");
        return stats;
    }
    let usage = match client.usage() {
        Ok(u) => u,
        Err(e) => {
            error!(error = %e, "docker storage GC: cannot read sidecar Docker usage; nothing was reclaimed this pass");
            stats.usage_read_failed = true;
            return stats;
        }
    };
    let mut total = usage.total_bytes();
    if total <= cfg.max_bytes {
        info!(
            total_bytes = total,
            max_bytes = cfg.max_bytes,
            "docker storage GC: under the ceiling, nothing reclaimed"
        );
        return stats;
    }

    record_step(
        &mut stats,
        &mut total,
        "stopped containers",
        reclaim_stopped_containers(client, now),
    );
    if total > cfg.max_bytes {
        record_step(
            &mut stats,
            &mut total,
            "dangling images",
            reclaim_dangling_images(client),
        );
    }
    if total > cfg.max_bytes {
        record_step(
            &mut stats,
            &mut total,
            "build cache",
            reclaim_build_cache(client),
        );
    }
    if total > cfg.max_bytes {
        record_step(
            &mut stats,
            &mut total,
            "unused images",
            reclaim_unused_images(client, now),
        );
    }
    if total > cfg.max_bytes {
        let vol_stats = sweep_docker_volumes(client, cfg, now);
        record_step(
            &mut stats,
            &mut total,
            "unreferenced volumes",
            vol_stats.bytes_reclaimed,
        );
    }
    stats
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

    // ---- docker volume sweep --------------------------------------------------

    fn docker_cfg(enabled: bool) -> DockerGcConfig {
        docker_cfg_with_ceiling(enabled, u64::MAX)
    }

    fn docker_cfg_with_ceiling(enabled: bool, max_bytes: u64) -> DockerGcConfig {
        DockerGcConfig {
            enabled,
            volume_retention_days: 3,
            max_bytes,
        }
    }

    fn volume(name: &str, in_use: bool, age_days: u64) -> DockerVolume {
        DockerVolume {
            name: name.to_string(),
            in_use,
            created_at: SystemTime::now() - Duration::from_secs(age_days * SECS_PER_DAY),
            size_bytes: Some(1024),
        }
    }

    /// Fake [`DockerVolumeClient`]: `list` is the fixed set of volumes to
    /// report, `removed` records every name [`remove_volume`] was called
    /// with, and `remove_result` maps a name to the outcome to return (missing
    /// = success). The SPEC-3.11 fields (`usage`, the three listings, the
    /// build-cache result) each default to an empty/zero `Ok`, so a test only
    /// sets the ones its scenario needs.
    struct FakeDockerClient {
        list: std::result::Result<Vec<DockerVolume>, String>,
        removed: std::sync::Mutex<Vec<String>>,
        remove_result: std::collections::HashMap<String, VolumeRemoveErrorKind>,
        usage: std::result::Result<DockerUsage, String>,
        stopped_containers: std::result::Result<Vec<StoppedContainer>, String>,
        dangling_images: std::result::Result<Vec<DockerImage>, String>,
        unused_images: std::result::Result<Vec<DockerImage>, String>,
        build_cache_bytes: std::result::Result<u64, String>,
        removed_containers: std::sync::Mutex<Vec<String>>,
        removed_images: std::sync::Mutex<Vec<String>>,
    }

    #[derive(Clone)]
    enum VolumeRemoveErrorKind {
        InUse,
        Other,
    }

    impl FakeDockerClient {
        fn new(volumes: Vec<DockerVolume>) -> Self {
            Self {
                list: Ok(volumes),
                removed: std::sync::Mutex::new(Vec::new()),
                remove_result: std::collections::HashMap::new(),
                usage: Ok(DockerUsage::default()),
                stopped_containers: Ok(Vec::new()),
                dangling_images: Ok(Vec::new()),
                unused_images: Ok(Vec::new()),
                build_cache_bytes: Ok(0),
                removed_containers: std::sync::Mutex::new(Vec::new()),
                removed_images: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn unreachable(reason: &str) -> Self {
            let mut c = Self::new(Vec::new());
            c.list = Err(reason.to_string());
            c
        }

        fn with_remove_failure(mut self, name: &str, kind: VolumeRemoveErrorKind) -> Self {
            self.remove_result.insert(name.to_string(), kind);
            self
        }

        fn with_usage(mut self, usage: DockerUsage) -> Self {
            self.usage = Ok(usage);
            self
        }

        fn with_usage_err(mut self, reason: &str) -> Self {
            self.usage = Err(reason.to_string());
            self
        }

        fn with_stopped_containers(mut self, containers: Vec<StoppedContainer>) -> Self {
            self.stopped_containers = Ok(containers);
            self
        }

        fn with_dangling_images(mut self, images: Vec<DockerImage>) -> Self {
            self.dangling_images = Ok(images);
            self
        }

        fn with_unused_images(mut self, images: Vec<DockerImage>) -> Self {
            self.unused_images = Ok(images);
            self
        }
    }

    impl DockerVolumeClient for FakeDockerClient {
        fn list_volumes(&self) -> std::result::Result<Vec<DockerVolume>, String> {
            self.list.clone()
        }

        fn remove_volume(&self, name: &str) -> std::result::Result<(), VolumeRemoveError> {
            self.removed.lock().unwrap().push(name.to_string());
            match self.remove_result.get(name) {
                Some(VolumeRemoveErrorKind::InUse) => {
                    Err(VolumeRemoveError::InUse("volume is in use".to_string()))
                }
                Some(VolumeRemoveErrorKind::Other) => {
                    Err(VolumeRemoveError::Other("permission denied".to_string()))
                }
                None => Ok(()),
            }
        }

        fn usage(&self) -> std::result::Result<DockerUsage, String> {
            self.usage.clone()
        }

        fn list_stopped_containers(&self) -> std::result::Result<Vec<StoppedContainer>, String> {
            self.stopped_containers.clone()
        }

        fn remove_container(&self, id: &str) -> std::result::Result<(), String> {
            self.removed_containers.lock().unwrap().push(id.to_string());
            Ok(())
        }

        fn list_dangling_images(&self) -> std::result::Result<Vec<DockerImage>, String> {
            self.dangling_images.clone()
        }

        fn list_unused_images(&self) -> std::result::Result<Vec<DockerImage>, String> {
            self.unused_images.clone()
        }

        fn remove_image(&self, id: &str) -> std::result::Result<(), String> {
            self.removed_images.lock().unwrap().push(id.to_string());
            Ok(())
        }

        fn prune_build_cache(&self, _older_than: Duration) -> std::result::Result<u64, String> {
            self.build_cache_bytes.clone()
        }
    }

    /// AC: an unused volume older than the retention window is removed.
    #[test]
    fn stale_unused_volume_is_removed() {
        let client = FakeDockerClient::new(vec![volume("dev-cargo-registry", false, 10)]);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.kept, 0);
        assert_eq!(stats.bytes_reclaimed, 1024);
        assert_eq!(
            *client.removed.lock().unwrap(),
            vec!["dev-cargo-registry".to_string()]
        );
    }

    /// AC: an unused volume younger than the retention window is kept.
    #[test]
    fn fresh_unused_volume_is_kept() {
        let client = FakeDockerClient::new(vec![volume("dev-cargo-registry", false, 1)]);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
        assert!(client.removed.lock().unwrap().is_empty());
    }

    /// AC: an in-use volume is kept regardless of age.
    #[test]
    fn in_use_volume_is_kept_regardless_of_age() {
        let client = FakeDockerClient::new(vec![volume("dev-postgres-data", true, 30)]);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
        assert!(client.removed.lock().unwrap().is_empty());
    }

    /// AC: a `buildx_buildkit_*` volume is kept regardless of age.
    #[test]
    fn buildx_volume_is_kept_regardless_of_age() {
        let client =
            FakeDockerClient::new(vec![volume("buildx_buildkit_mybuilder0_state", false, 30)]);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
        assert!(client.removed.lock().unwrap().is_empty());
    }

    /// AC: an unreachable daemon removes nothing and is not a clean sweep.
    #[test]
    fn unreachable_daemon_removes_nothing() {
        let client = FakeDockerClient::unreachable("DOCKER_HOST is not set");
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats, SweepStats::default());
    }

    /// AC: a remove error other than "in use" is logged at `error` and the
    /// volume is kept; the pass continues for the rest.
    #[test]
    fn remove_error_other_than_in_use_keeps_the_volume_and_continues() {
        let client = FakeDockerClient::new(vec![
            volume("dev-broken-volume", false, 10),
            volume("dev-ok-volume", false, 10),
        ])
        .with_remove_failure("dev-broken-volume", VolumeRemoveErrorKind::Other);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.kept, 1);
    }

    /// Docker's own refusal because a container now references the volume
    /// (a race between the list and the remove) is kept, not treated as an
    /// error.
    #[test]
    fn remove_refused_as_in_use_is_kept_not_errored() {
        let client = FakeDockerClient::new(vec![volume("dev-raced-volume", false, 10)])
            .with_remove_failure("dev-raced-volume", VolumeRemoveErrorKind::InUse);
        let stats = sweep_docker_volumes(&client, &docker_cfg(true), SystemTime::now());
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.kept, 1);
    }

    #[test]
    fn disabled_docker_sweep_removes_nothing() {
        let client = FakeDockerClient::new(vec![volume("dev-cargo-registry", false, 10)]);
        let stats = sweep_docker_volumes(&client, &docker_cfg(false), SystemTime::now());
        assert_eq!(stats, SweepStats::default());
        assert!(client.removed.lock().unwrap().is_empty());
    }

    // ---- SPEC-3.11 docker storage ceiling reclaim ------------------------

    fn stopped_container(id: &str, age_days: u64, size: u64) -> StoppedContainer {
        StoppedContainer {
            id: id.to_string(),
            finished_at: SystemTime::now() - Duration::from_secs(age_days * SECS_PER_DAY),
            size_bytes: Some(size),
        }
    }

    fn docker_image(id: &str, in_use: bool, age_days: u64, size: u64) -> DockerImage {
        DockerImage {
            id: id.to_string(),
            created_at: SystemTime::now() - Duration::from_secs(age_days * SECS_PER_DAY),
            size_bytes: Some(size),
            in_use,
        }
    }

    /// AC: under the ceiling, the pass reclaims nothing and touches no
    /// category.
    #[test]
    fn under_ceiling_reclaims_nothing() {
        let client = FakeDockerClient::new(Vec::new()).with_usage(DockerUsage {
            containers_bytes: 10,
            images_bytes: 10,
            volumes_bytes: 10,
            build_cache_bytes: 10,
        });
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 1_000),
            SystemTime::now(),
            false,
        );
        assert!(stats.steps.is_empty());
        assert!(!stats.usage_read_failed);
    }

    /// AC: over the ceiling, reclaim runs in the SPEC-3.11 order and stops
    /// as soon as the running total is back under the ceiling.
    #[test]
    fn over_ceiling_reclaims_in_order_and_stops_early() {
        let client = FakeDockerClient::new(vec![volume("dev-untouched", false, 10)])
            .with_usage(DockerUsage {
                containers_bytes: 100,
                images_bytes: 0,
                volumes_bytes: 0,
                build_cache_bytes: 0,
            })
            .with_stopped_containers(vec![stopped_container("c1", 2, 100)])
            .with_dangling_images(vec![docker_image("img-dangling", false, 10, 999)]);
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 50),
            SystemTime::now(),
            false,
        );
        assert_eq!(stats.steps.len(), 1);
        assert_eq!(stats.steps[0].category, "stopped containers");
        assert_eq!(stats.steps[0].bytes_reclaimed, 100);
        assert_eq!(stats.steps[0].before_bytes, 100);
        assert_eq!(stats.steps[0].after_bytes, 0);
        assert_eq!(
            *client.removed_containers.lock().unwrap(),
            vec!["c1".to_string()]
        );
        assert!(
            client.removed_images.lock().unwrap().is_empty(),
            "later steps never run once the pass is back under the ceiling"
        );
        assert!(client.removed.lock().unwrap().is_empty());
    }

    /// AC: a failed usage read reclaims nothing and is flagged, never read
    /// as a clean pass.
    #[test]
    fn unreadable_usage_reclaims_nothing() {
        let client = FakeDockerClient::new(Vec::new()).with_usage_err("DOCKER_HOST is not set");
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 10),
            SystemTime::now(),
            false,
        );
        assert!(stats.steps.is_empty());
        assert!(stats.usage_read_failed);
    }

    /// AC: an image a container still references is kept regardless of age
    /// or category order.
    #[test]
    fn referenced_image_is_kept() {
        let client = FakeDockerClient::new(Vec::new())
            .with_usage(DockerUsage {
                containers_bytes: 0,
                images_bytes: 1_000,
                volumes_bytes: 0,
                build_cache_bytes: 0,
            })
            .with_dangling_images(vec![docker_image("img-in-use", true, 10, 1_000)]);
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 1),
            SystemTime::now(),
            false,
        );
        let dangling = stats
            .steps
            .iter()
            .find(|s| s.category == "dangling images")
            .expect("dangling images step runs since stopped containers alone did not clear the ceiling");
        assert_eq!(dangling.bytes_reclaimed, 0);
        assert!(client.removed_images.lock().unwrap().is_empty());
    }

    /// AC: a run in flight skips the pass entirely, even over the ceiling.
    #[test]
    fn run_in_flight_skips_the_pass() {
        let client = FakeDockerClient::new(Vec::new()).with_usage(DockerUsage {
            containers_bytes: 1_000,
            images_bytes: 0,
            volumes_bytes: 0,
            build_cache_bytes: 0,
        });
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 1),
            SystemTime::now(),
            true,
        );
        assert!(stats.steps.is_empty());
        assert!(!stats.usage_read_failed);
        assert!(client.removed_containers.lock().unwrap().is_empty());
    }

    /// AC: a disabled section reclaims nothing.
    #[test]
    fn disabled_reclaim_removes_nothing() {
        let client = FakeDockerClient::new(Vec::new()).with_usage(DockerUsage {
            containers_bytes: 1_000,
            images_bytes: 0,
            volumes_bytes: 0,
            build_cache_bytes: 0,
        });
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(false, 1),
            SystemTime::now(),
            false,
        );
        assert_eq!(stats, ReclaimStats::default());
    }

    /// AC: an unused image older than 7 days is reclaimed when the earlier
    /// steps alone do not clear the ceiling; a fresh unused image is kept.
    #[test]
    fn unused_images_step_reclaims_stale_and_keeps_fresh() {
        let client = FakeDockerClient::new(Vec::new())
            .with_usage(DockerUsage {
                containers_bytes: 0,
                images_bytes: 2_000,
                volumes_bytes: 0,
                build_cache_bytes: 0,
            })
            .with_unused_images(vec![
                docker_image("img-stale", false, 10, 1_500),
                docker_image("img-fresh", false, 1, 500),
            ]);
        let stats = reclaim_docker_storage(
            &client,
            &docker_cfg_with_ceiling(true, 1),
            SystemTime::now(),
            false,
        );
        let unused = stats
            .steps
            .iter()
            .find(|s| s.category == "unused images")
            .expect("unused images step runs since the earlier steps reclaimed nothing");
        assert_eq!(unused.bytes_reclaimed, 1_500);
        assert_eq!(*client.removed_images.lock().unwrap(), vec!["img-stale"]);
    }

    #[test]
    fn docker_usage_total_bytes_sums_every_category() {
        let usage = DockerUsage {
            containers_bytes: 1,
            images_bytes: 2,
            volumes_bytes: 3,
            build_cache_bytes: 4,
        };
        assert_eq!(usage.total_bytes(), 10);
    }

    #[test]
    fn parse_docker_size_handles_docker_df_units() {
        assert_eq!(parse_docker_size("0B"), 0);
        assert_eq!(parse_docker_size("512MB"), 512_000_000);
        assert_eq!(parse_docker_size("1.2GB"), 1_200_000_000);
        assert_eq!(parse_docker_size("bogus"), 0);
    }
}
