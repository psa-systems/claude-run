//! Per-job workdir path resolution.
//!
//! `workdir_base` is now a plain path that may contain `~`, `$VAR`, and
//! `${VAR}` references. The nu-expression form (a leading `$` triggering
//! `^nu --commands`) is gone - environment-variable expansion is enough and
//! lets us drop nu as a dependency for path resolution.
//!
//! Resolution order for `<workdir_base>`:
//!
//! 1. The action's `workdir_base` YAML field, if set.
//! 2. `$CLAUDE_RUN_WORKDIR_BASE`, if set.
//! 3. `<data_dir>/workdirs`.
//!
//! The final per-job workdir is `<workdir_base>/<action_name>/<job_key>`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use tracing::{error, warn};

use crate::config::{expand_path, Paths};

pub fn resolve_workdir_base(action_workdir_base: Option<&str>, paths: &Paths) -> PathBuf {
    if let Some(raw) = action_workdir_base {
        return PathBuf::from(expand_path(raw));
    }
    if let Ok(raw) = std::env::var("CLAUDE_RUN_WORKDIR_BASE") {
        return PathBuf::from(expand_path(&raw));
    }
    paths.workdirs_dir()
}

pub fn job_workdir(base: &Path, action_name: &str, job_key: &str) -> PathBuf {
    base.join(action_name).join(job_key)
}

// ---- The one local-only-work check every workdir deletion uses --

/// One local branch tip and whether the live remote advertises it under its
/// own name at the same SHA, or the tip equals the remote default branch's
/// tip (no commits of its own, so nothing would be lost).
pub struct BranchTip {
    pub branch: String,
    pub sha: String,
    pub on_remote: bool,
}

/// Uncommitted, unstashed state: `git status --porcelain` entries (ignored
/// files excluded) and `git stash list` entries.
pub struct DirtyState {
    pub paths: Vec<String>,
    pub stashes: Vec<String>,
}

impl DirtyState {
    fn empty() -> Self {
        DirtyState {
            paths: Vec::new(),
            stashes: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty() && self.stashes.is_empty()
    }
}

/// What a workdir holds that a delete would destroy: local branch tips the
/// remote does not have under their own name (and that are not just the
/// default branch's tip), plus any dirty tree / stash. `remote_branch` is the
/// first local branch (other than the default) found pushed under its own
/// name, for callers reporting where the work actually is.
pub struct LocalWork {
    pub unpushed_branches: Vec<BranchTip>,
    pub remote_branch: Option<BranchTip>,
    pub dirty: DirtyState,
}

impl LocalWork {
    pub fn is_clean(&self) -> bool {
        self.unpushed_branches.is_empty() && self.dirty.is_empty()
    }
}

/// A git or network failure querying local-only work. Every caller preserves
/// the workdir on this rather than risk a wrong delete.
#[derive(Debug)]
pub struct WorkCheckError(pub String);

impl std::fmt::Display for WorkCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn git_out(workdir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

fn git_required(workdir: &Path, args: &[&str]) -> Result<String, WorkCheckError> {
    git_out(workdir, args).ok_or_else(|| WorkCheckError(format!("git {} failed", args.join(" "))))
}

/// The dirty-tree / stash half of the check, with no network call: `git
/// status --porcelain=v1 --untracked-files=normal` (ignored files are never
/// listed) plus `git stash list`. A workdir with no `.git` (a plain scratch
/// directory, never a clone) has nothing git-trackable to lose.
pub fn uncommitted_or_stashed(workdir: &Path) -> Result<DirtyState, WorkCheckError> {
    if !workdir.join(".git").exists() {
        return Ok(DirtyState::empty());
    }
    let status = git_required(
        workdir,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
    )?;
    let paths: Vec<String> = status
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();

    let stash = git_required(workdir, &["stash", "list"])?;
    let stashes: Vec<String> = stash
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();

    Ok(DirtyState { paths, stashes })
}

/// The one check every workdir deletion site runs before
/// deleting. "Local-only" means any of: commits on a local branch the remote
/// does not have under that branch name (excluding a branch whose tip equals
/// the remote default branch's tip, which has no commits of its own);
/// tracked or staged modifications; untracked files that are not ignored; or
/// stashes. A git or network failure is an `Err`; callers log it and
/// preserve.
///
/// This replaces the CLAUDE-193 `branch_tips` SHA-membership check (a local
/// tip counted as "on the remote" if its SHA appeared under ANY advertised
/// ref, so a brand-new branch left at `main`'s tip read as pushed) with an
/// exact `refs/heads/<name>` lookup, and adds the dirty-tree / stash half the
/// old check never asked about at all.
pub fn local_only_work(workdir: &Path) -> Result<LocalWork, WorkCheckError> {
    let dirty = uncommitted_or_stashed(workdir)?;
    if !workdir.join(".git").exists() {
        return Ok(LocalWork {
            unpushed_branches: Vec::new(),
            remote_branch: None,
            dirty,
        });
    }

    let branches = git_required(
        workdir,
        &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
    )?;
    let branches: Vec<&str> = branches
        .lines()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .collect();
    if branches.is_empty() {
        return Ok(LocalWork {
            unpushed_branches: Vec::new(),
            remote_branch: None,
            dirty,
        });
    }

    // The live remote's advertised `refname -> sha` map, plus the `ref:
    // refs/heads/<x>\tHEAD` symref line naming the default branch.
    let remote = git_required(workdir, &["ls-remote", "--symref", "origin"])?;
    let mut remote_refs: HashMap<String, String> = HashMap::new();
    let mut default_branch: Option<String> = None;
    for line in remote.lines() {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next(), fields.next()) {
            (Some("ref:"), Some(target), Some("HEAD")) => {
                default_branch = target.strip_prefix("refs/heads/").map(str::to_string);
            }
            (Some(sha), Some(refname), None) => {
                remote_refs.insert(refname.to_string(), sha.to_string());
            }
            _ => {}
        }
    }
    let default_sha = default_branch
        .as_deref()
        .and_then(|b| remote_refs.get(&format!("refs/heads/{b}")).cloned());

    let mut unpushed_branches = Vec::new();
    let mut remote_branch = None;
    for branch in branches {
        let sha = git_required(workdir, &["rev-parse", branch])?
            .trim()
            .to_string();
        let pushed_under_own_name = remote_refs.get(&format!("refs/heads/{branch}")) == Some(&sha);
        let equals_default_tip = default_sha.as_deref() == Some(sha.as_str());
        let on_remote = pushed_under_own_name || equals_default_tip;
        let tip = BranchTip {
            branch: branch.to_string(),
            sha,
            on_remote,
        };
        if !on_remote {
            unpushed_branches.push(tip);
        } else if pushed_under_own_name
            && default_branch.as_deref() != Some(branch)
            && remote_branch.is_none()
        {
            remote_branch = Some(tip);
        }
    }

    Ok(LocalWork {
        unpushed_branches,
        remote_branch,
        dirty,
    })
}

/// Remove gitignored build artifacts from a workdir about to be
/// preserved. `git clean -fdX` deletes only ignored files and directories; a
/// failure is logged and tolerated (the directory is still preserved).
fn prune_ignored_artifacts(workdir: &Path) {
    let out = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(["clean", "-fdX"])
        .output();
    match out {
        Err(e) => {
            warn!(workdir = %workdir.display(), error = %e, "pruning gitignored artifacts failed (preserving anyway)");
        }
        Ok(o) if !o.status.success() => {
            warn!(
                workdir = %workdir.display(),
                stderr = %String::from_utf8_lossy(&o.stderr),
                "git clean -fdX failed (preserving anyway)"
            );
        }
        Ok(_) => {}
    }
}

/// Unconditionally remove `workdir`, tolerating a failed delete (logged and
/// continued, matching every runner's prior behaviour).
pub fn force_remove(workdir: &Path) {
    if workdir.exists() {
        if let Err(e) = std::fs::remove_dir_all(workdir) {
            warn!(workdir = %workdir.display(), error = %e, "workdir cleanup failed (continuing)");
        }
    }
}

/// The one guarded delete every runner uses. Deletes
/// only when [`local_only_work`] proves nothing exists solely in `workdir`;
/// any local-only work, or a failed check, preserves instead (pruning
/// gitignored build artifacts first) and logs what was found.
pub fn cleanup_or_preserve(workdir: &Path, action: &str, key: &str) {
    if !workdir.exists() {
        return;
    }
    match local_only_work(workdir) {
        Ok(work) if work.is_clean() => force_remove(workdir),
        Ok(work) => {
            prune_ignored_artifacts(workdir);
            warn!(
                action = %action,
                key = %key,
                workdir = %workdir.display(),
                unpushed_branches = work.unpushed_branches.len(),
                dirty_paths = work.dirty.paths.len(),
                stashes = work.dirty.stashes.len(),
                "workdir preserved: local-only work found"
            );
        }
        Err(e) => {
            error!(
                action = %action,
                key = %key,
                workdir = %workdir.display(),
                error = %e,
                "workdir preserved: local-only-work check failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_field_beats_env_beats_default() {
        std::env::set_var("CLAUDE_RUN_WORKDIR_BASE", "/from-env");
        let paths = Paths {
            config_dir: "/tmp/cfg".into(),
            data_dir: "/tmp/data".into(),
            state_dir: "/tmp/state".into(),
            runtime_dir: "/tmp/run".into(),
        };

        // Action field wins.
        let p = resolve_workdir_base(Some("/from-action"), &paths);
        assert_eq!(p, PathBuf::from("/from-action"));

        // Env wins over default.
        let p = resolve_workdir_base(None, &paths);
        assert_eq!(p, PathBuf::from("/from-env"));

        // Default falls back to data_dir/workdirs.
        std::env::remove_var("CLAUDE_RUN_WORKDIR_BASE");
        let p = resolve_workdir_base(None, &paths);
        assert_eq!(p, PathBuf::from("/tmp/data/workdirs"));
    }

    #[test]
    fn job_workdir_joins() {
        let p = job_workdir(Path::new("/base"), "youtrack", "MK-16");
        assert_eq!(p, PathBuf::from("/base/youtrack/MK-16"));
    }

    // ---- local_only_work / cleanup_or_preserve fixtures --------

    fn git_in(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .expect("git runs");
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn set_identity(dir: &Path) {
        git_in(dir, &["config", "user.email", "test@test"]);
        git_in(dir, &["config", "user.name", "test"]);
    }

    fn repo_with_commit(dir: &Path) {
        git_in(dir, &["init"]);
        set_identity(dir);
        std::fs::write(dir.join("f.txt"), "x").unwrap();
        git_in(dir, &["add", "."]);
        git_in(dir, &["commit", "-m", "feat: x"]);
    }

    /// A `--single-branch` clone, mimicking the shallow
    /// clone the runner makes. Returns the clone path.
    fn origin_and_single_branch_clone(base: &Path) -> PathBuf {
        let origin = base.join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        repo_with_commit(&origin);
        let clone = base.join("clone");
        git_in(
            base,
            &[
                "clone",
                "--single-branch",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );
        set_identity(&clone);
        clone
    }

    #[test]
    fn modified_tracked_file_on_main_is_preserved() {
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        std::fs::write(clone.join("f.txt"), "changed").unwrap();

        let work = local_only_work(&clone).unwrap();
        assert!(!work.is_clean(), "modified tracked file must count");
        assert_eq!(work.dirty.paths.len(), 1);

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(clone.exists(), "dirty tree must survive cleanup");
    }

    #[test]
    fn untracked_non_ignored_file_is_preserved() {
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        std::fs::write(clone.join("new.txt"), "work").unwrap();

        let work = local_only_work(&clone).unwrap();
        assert!(!work.is_clean(), "untracked file must count");

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(clone.exists(), "untracked work must survive cleanup");
    }

    #[test]
    fn only_ignored_extra_files_is_deleted() {
        // The `.gitignore` ships already committed on `origin`, so the clone's
        // tip matches the remote exactly and the only extra content is the
        // ignored build artifact created after cloning.
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        git_in(&origin, &["init"]);
        set_identity(&origin);
        std::fs::write(origin.join(".gitignore"), "target/\n").unwrap();
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-m", "feat: x"]);
        let clone = base.path().join("clone");
        git_in(
            base.path(),
            &[
                "clone",
                "--single-branch",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );
        set_identity(&clone);
        std::fs::create_dir_all(clone.join("target")).unwrap();
        std::fs::write(clone.join("target/build.o"), "binary").unwrap();

        let work = local_only_work(&clone).unwrap();
        assert!(
            work.is_clean(),
            "an ignored-only tree must read as clean: {:?}",
            work.dirty.paths
        );

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(!clone.exists(), "ignored-only extras must be deleted");
    }

    #[test]
    fn stash_is_preserved() {
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        std::fs::write(clone.join("f.txt"), "changed").unwrap();
        git_in(&clone, &["stash"]);

        let work = local_only_work(&clone).unwrap();
        assert!(!work.is_clean(), "a stash must count");
        assert_eq!(work.dirty.stashes.len(), 1);

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(clone.exists(), "a stash must survive cleanup");
    }

    #[test]
    fn clean_branch_not_on_remote_at_default_tip_is_deleted() {
        // The run-10822 branch shape without the dirty tree: a fresh branch
        // checked out from `main`'s tip, never pushed, holds no commits of
        // its own, so deleting it loses nothing.
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(
            &clone,
            &["checkout", "-b", "feat/CLAUDE-269-login-required"],
        );

        let work = local_only_work(&clone).unwrap();
        assert!(
            work.is_clean(),
            "a branch with no commits of its own must read as clean"
        );

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(!clone.exists(), "a branch at main's tip must be deleted");
    }

    #[test]
    fn branch_pushed_under_its_own_name_is_deleted() {
        let base = tempfile::tempdir().unwrap();
        let clone = origin_and_single_branch_clone(base.path());
        git_in(&clone, &["checkout", "-b", "feat/x"]);
        std::fs::write(clone.join("new.txt"), "work").unwrap();
        git_in(&clone, &["add", "."]);
        git_in(&clone, &["commit", "-m", "feat: x"]);
        git_in(&clone, &["push", "origin", "feat/x"]);

        let work = local_only_work(&clone).unwrap();
        assert!(work.is_clean(), "a pushed branch must read as clean");
        assert!(work.remote_branch.is_some());

        cleanup_or_preserve(&clone, "test", "K-1");
        assert!(
            !clone.exists(),
            "a branch pushed under its own name must be deleted"
        );
    }

    #[test]
    fn unreachable_remote_preserves() {
        let dir = tempfile::tempdir().unwrap();
        repo_with_commit(dir.path());
        git_in(
            dir.path(),
            &["remote", "add", "origin", "/nonexistent/repo.git"],
        );

        assert!(local_only_work(dir.path()).is_err());

        cleanup_or_preserve(dir.path(), "test", "K-1");
        assert!(
            dir.path().exists(),
            "a failed ls-remote must preserve rather than guess"
        );
    }

    #[test]
    fn non_git_scratch_dir_reads_as_clean() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("comment-body.md"), "hi").unwrap();

        let work = local_only_work(dir.path()).unwrap();
        assert!(
            work.is_clean(),
            "a plain scratch dir holds nothing local-only"
        );
    }
}
