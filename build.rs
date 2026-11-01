//! SPEC-2.1: "`build.rs` embeds the 12-character git SHA and the UTC build
//! date ... or a local probe, else `unknown`." Bake build metadata into the
//! binary so two `0.1.0` artifacts built from different commits are
//! distinguishable (see CLAUDE-2).
//!
//! Emits two compile-time env vars consumed by `claude_run::VERSION`:
//!   - `CLAUDE_RUN_GIT_SHA`   : short (12-char) commit hash
//!   - `CLAUDE_RUN_BUILD_DATE`: UTC build date (YYYY-MM-DD)
//!
//! Resolution order for each: an explicit build env var (the CI path, set as
//! `ENV` in `oci-build/Dockerfile`), else a local probe (`git rev-parse` /
//! `date`), else `"unknown"` so a tarball build with no `.git` still compiles.

use std::process::Command;

// SPEC-2.1: resolves and emits `CLAUDE_RUN_GIT_SHA` / `CLAUDE_RUN_BUILD_DATE`
// as `cargo:rustc-env`, which `claude_run::VERSION` consumes.
fn main() {
    let git_sha = env_or_else("CLAUDE_RUN_GIT_SHA", || {
        probe("git", &["rev-parse", "--short=12", "HEAD"])
    });
    let build_date = env_or_else("CLAUDE_RUN_BUILD_DATE", || {
        probe("date", &["-u", "+%Y-%m-%d"])
    });

    println!("cargo:rustc-env=CLAUDE_RUN_GIT_SHA={git_sha}");
    println!("cargo:rustc-env=CLAUDE_RUN_BUILD_DATE={build_date}");
    // Re-bake when CI passes a new value. The default package-file scan (kept
    // alive because we emit no `rerun-if-changed`) covers local edits.
    println!("cargo:rerun-if-env-changed=CLAUDE_RUN_GIT_SHA");
    println!("cargo:rerun-if-env-changed=CLAUDE_RUN_BUILD_DATE");
}

/// SPEC-2.1: "an explicit build env var ... else a local probe." Use `$var`
/// when set and non-empty, else fall back to `f`.
fn env_or_else(var: &str, f: impl FnOnce() -> String) -> String {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => f(),
    }
}

/// SPEC-2.1: the local-probe fallback (`git rev-parse` / `date`), returning
/// `"unknown"` on any failure so a tarball build with no `.git` still compiles.
fn probe(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
