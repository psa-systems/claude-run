//! Per-row workdir path resolution.
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
//! The final per-row workdir is `<workdir_base>/<action_name>/<row_key>`.

use std::path::{Path, PathBuf};

use crate::config::{expand_path, Paths};

pub fn resolve_workdir_base(
    action_workdir_base: Option<&str>,
    paths: &Paths,
) -> PathBuf {
    if let Some(raw) = action_workdir_base {
        return PathBuf::from(expand_path(raw));
    }
    if let Ok(raw) = std::env::var("CLAUDE_RUN_WORKDIR_BASE") {
        return PathBuf::from(expand_path(&raw));
    }
    paths.workdirs_dir()
}

pub fn row_workdir(base: &Path, action_name: &str, row_key: &str) -> PathBuf {
    base.join(action_name).join(row_key)
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
    fn row_workdir_joins() {
        let p = row_workdir(Path::new("/base"), "youtrack", "MK-16");
        assert_eq!(p, PathBuf::from("/base/youtrack/MK-16"));
    }
}
