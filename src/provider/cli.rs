//! CLI backend: implements [`IssueTracker`] + [`Forge`] over the host `yt` /
//! `fj` binaries, parsing their `--json` output and mapping failures to typed
//! errors. No raw REST.
//!
//! Process spawning goes through the [`CommandRunner`] seam so the backend can
//! be unit-tested with a stub that returns canned stdout / exit codes (see
//! [`testutil::StubRunner`]) instead of invoking real binaries.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use serde::Deserialize;
use tracing::{debug, warn};

use super::{Backend, Forge, IssueTracker, PullRequest, VcsMapEntry, VcsRepo};
use crate::error::{Error, Result};

/// CLAUDE-136: derive a PR number from a forge PR URL's trailing
/// `/pulls/<n>` (Forgejo) or `/pull/<n>` (GitHub-style) segment. Used only as a
/// fallback when `fj pr create --json` omits the `number` field.
fn number_from_pr_url(url: &str) -> Option<u64> {
    let rest = url
        .rsplit_once("/pulls/")
        .or_else(|| url.rsplit_once("/pull/"))?
        .1;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<u64>().ok()
}

/// CLAUDE-160: extract only the *owning* `#<KEY>-N` issue references from
/// `text`, returning the keys without the leading `#`, deduped in first-seen
/// order. A key matches `[A-Z][A-Z0-9]*-[0-9]+`.
///
/// An owning reference is a **bare `#<KEY>-N` alone on its own line** (the
/// commit convention's trailer block, per each repo's `CLAUDE.md`): "End of
/// body, last block, one issue per line, BARE id with nothing after it." A
/// commit can legitimately serve several issues, so every such bare line is an
/// owning reference and is returned.
///
/// IDs that appear embedded in prose - a "Related issues" list, "depends on
/// #KEY-N" / "relates to #KEY-N" cross-links, or the `(#NNN)` PR number in a
/// Forgejo "Merge pull request" subject - carry other text on the line and are
/// deliberately IGNORED. They are cross-links, never transition targets. This
/// is what keeps VCS-triggered automation (e.g. the CI-fix `Needs Review` flag)
/// acting only on the change's owning issue rather than fanning its command out
/// to every ID mentioned in the body.
///
/// Only ASCII bytes are matched, so the slice boundaries stay on char
/// boundaries.
fn parse_issue_refs(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in text.lines() {
        if let Some(key) = bare_issue_ref(line.trim()) {
            if !out.contains(&key) {
                out.push(key);
            }
        }
    }
    out
}

/// CLAUDE-160: if `s` is EXACTLY a bare `#<KEY>-N` reference and nothing else,
/// return the key (`<KEY>-N`, no leading `#`); otherwise `None`. Callers pass an
/// already-trimmed line, so any surrounding prose leaves trailing/leading bytes
/// that make the match fail - which is precisely how a cross-link mention is
/// distinguished from the owning-reference trailer.
fn bare_issue_ref(s: &str) -> Option<String> {
    let rest = s.strip_prefix('#')?;
    let bytes = rest.as_bytes();
    let mut j = 0;
    // KEY: an uppercase letter then uppercase letters / digits.
    if j < bytes.len() && bytes[j].is_ascii_uppercase() {
        j += 1;
        while j < bytes.len() && (bytes[j].is_ascii_uppercase() || bytes[j].is_ascii_digit()) {
            j += 1;
        }
        // `-` then one or more digits, consuming the WHOLE string (bare id,
        // nothing after it).
        if j < bytes.len() && bytes[j] == b'-' {
            j += 1;
            let digits_start = j;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > digits_start && j == bytes.len() {
                return Some(rest.to_string());
            }
        }
    }
    None
}

/// Captured result of running an external command.
#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Abstraction over spawning a process. The real impl is [`ProcessRunner`];
/// tests substitute a stub. The methods return [`Error::CliNotFound`] when
/// `binary` is absent from `PATH`, so callers can probe availability.
///
/// [`CommandRunner::run_in_env`] is the one required method (CLAUDE-26: it adds
/// per-child env injection so a job's decrypted credentials reach the `yt` / `fj`
/// child only). `run` / `run_in` are thin wrappers, so existing call sites that
/// need neither a working dir nor env are unchanged.
pub trait CommandRunner: Send + Sync {
    /// Run `binary args...` in optional working dir `dir`, with `envs`
    /// (`(name, value)`) set on the child process only.
    fn run_in_env(
        &self,
        dir: Option<&Path>,
        binary: &str,
        args: &[&str],
        envs: &[(&str, &str)],
    ) -> Result<CmdOutput>;

    /// Run `binary args...` and capture its output.
    fn run(&self, binary: &str, args: &[&str]) -> Result<CmdOutput> {
        self.run_in_env(None, binary, args, &[])
    }

    /// Run `binary args...` with the working directory set to `dir` (needed for
    /// `fj pr create`, which reads the repo from `cwd`).
    fn run_in(&self, dir: &Path, binary: &str, args: &[&str]) -> Result<CmdOutput> {
        self.run_in_env(Some(dir), binary, args, &[])
    }
}

/// Real [`CommandRunner`]: shells out via [`std::process::Command`].
#[derive(Debug, Default, Clone)]
pub struct ProcessRunner;

impl ProcessRunner {
    pub fn new() -> Self {
        Self
    }

    fn exec(
        binary: &str,
        args: &[&str],
        dir: Option<&Path>,
        envs: &[(&str, &str)],
    ) -> Result<CmdOutput> {
        let mut cmd = Command::new(binary);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let output = match cmd.output() {
            Ok(o) => o,
            // A missing binary surfaces as NotFound from the spawn; map it to
            // the typed CliNotFound so the factory can fall through.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::CliNotFound {
                    binary: binary.to_string(),
                });
            }
            Err(e) => return Err(Error::Io(e)),
        };
        Ok(CmdOutput {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

impl CommandRunner for ProcessRunner {
    fn run_in_env(
        &self,
        dir: Option<&Path>,
        binary: &str,
        args: &[&str],
        envs: &[(&str, &str)],
    ) -> Result<CmdOutput> {
        Self::exec(binary, args, dir, envs)
    }
}

/// CLI-backed provider. Holds the configurable binary names and the forge
/// credential store path, plus the [`CommandRunner`] used to invoke them.
pub struct CliBackend {
    runner: Arc<dyn CommandRunner>,
    yt: String,
    fj: String,
    /// Path to the forge CLI's key store (`forgejo-cli/keys.json`), read by
    /// [`Forge::clone_url`]. Overridable for tests and for CLAUDE-26's per-job
    /// temp credential store.
    keys_path: PathBuf,
    /// CLAUDE-26: env (`(name, value)`) injected on the `yt` child only, e.g.
    /// `YOUTRACK_CLI_CONFIG` pointing at a temp single-file config holding the
    /// decrypted token. Empty (the default) leaves the `yt` CLI on host auth.
    yt_env: Vec<(String, String)>,
    /// CLAUDE-26: env injected on the `fj` child only, e.g. an `XDG_DATA_HOME`
    /// override pointing at a temp `forgejo-cli/keys.json`. Empty by default.
    fj_env: Vec<(String, String)>,
}

/// Shape of the forge CLI key store (`~/.local/share/forgejo-cli/keys.json`):
/// `{ "hosts": { "<host>": { "token": "..." } } }`.
#[derive(Debug, Deserialize)]
struct ForgeKeys {
    hosts: std::collections::BTreeMap<String, ForgeHostKey>,
}

#[derive(Debug, Deserialize)]
struct ForgeHostKey {
    token: String,
}

/// One enabled VCS processor as `yt project vcs --json` emits it.
#[derive(Debug, Deserialize)]
struct VcsProcessor {
    #[serde(rename = "serverUrl")]
    server_url: String,
    path: String,
}

/// One project as `yt project list --json` emits it. Only the short name and the
/// archived flag are consumed; unknown fields are ignored.
#[derive(Debug, Deserialize)]
struct ProjectListEntry {
    #[serde(rename = "shortName")]
    short_name: String,
    #[serde(default)]
    archived: bool,
}

/// The `links` array of `yt issue inspect --json` (YT-40 / CLAUDE-161). Only the
/// links are deserialized; the surrounding `issue` / `comments` / etc. objects
/// are ignored.
#[derive(Debug, Deserialize)]
struct InspectLinks {
    #[serde(default)]
    links: Vec<RawLink>,
}

/// One `links[]` entry: the resolved `direction` / `verb` (YT-40) plus the linked
/// issues that entry connects to.
#[derive(Debug, Deserialize)]
struct RawLink {
    #[serde(default)]
    direction: String,
    #[serde(default)]
    verb: String,
    #[serde(default)]
    issues: Vec<RawLinkedIssue>,
}

/// One linked issue inside a `links[]` entry. `resolved` is the nullable epoch-ms
/// resolution timestamp; it is omitted entirely for an unresolved issue, so
/// `#[serde(default)]` maps both absent and null to `None` (an unmet dependency).
#[derive(Debug, Deserialize)]
struct RawLinkedIssue {
    #[serde(rename = "idReadable")]
    id_readable: String,
    #[serde(default)]
    resolved: Option<i64>,
}

impl CliBackend {
    /// Backend with default binary names (`yt`, `fj`) and the default forge key
    /// store under the user's data dir.
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        let keys_path = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("forgejo-cli")
            .join("keys.json");
        Self {
            runner,
            yt: "yt".to_string(),
            fj: "fj".to_string(),
            keys_path,
            yt_env: Vec::new(),
            fj_env: Vec::new(),
        }
    }

    /// Override the binary names (config-driven later).
    pub fn with_binaries(mut self, yt: impl Into<String>, fj: impl Into<String>) -> Self {
        self.yt = yt.into();
        self.fj = fj.into();
        self
    }

    /// Override the forge key store path (tests).
    pub fn with_keys_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.keys_path = path.into();
        self
    }

    /// CLAUDE-26: inject env on the `yt` child only (decrypted-credential
    /// `YOUTRACK_CLI_CONFIG`). Empty leaves host auth.
    pub fn with_yt_env(mut self, env: Vec<(String, String)>) -> Self {
        self.yt_env = env;
        self
    }

    /// CLAUDE-26: inject env on the `fj` child only (an `XDG_DATA_HOME` override
    /// pointing at a temp `keys.json`). Empty leaves host auth.
    pub fn with_fj_env(mut self, env: Vec<(String, String)>) -> Self {
        self.fj_env = env;
        self
    }

    /// Borrow the `yt` child env as the `(&str, &str)` slice the runner takes.
    fn yt_envs(&self) -> Vec<(&str, &str)> {
        self.yt_env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Borrow the `fj` child env as the `(&str, &str)` slice the runner takes.
    fn fj_envs(&self) -> Vec<(&str, &str)> {
        self.fj_env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Parse `fj pr create --json` stdout into a [`PullRequest`]. CLAUDE-136:
    /// the human `text` line carries only the PR URL (`.../pulls/<n>`) and no
    /// `#<n>` token, so the old `#`-splitting heuristic always lost the number
    /// (or split on the body's `#<KEY>-N` reference and read zero digits),
    /// which silently disabled automerge. `--json` returns the number and URL
    /// directly; consume that. Shared by the AGit and branch paths.
    fn created_pr(&self, stdout: &str) -> Result<PullRequest> {
        /// The created PR as `fj pr create --json` reports it. Field names
        /// mirror `fj pr search --json` ([`super::ForgePullRequest`]); `url` is
        /// accepted as an alias so a differing field name still binds. Unknown
        /// fields are ignored.
        #[derive(Deserialize)]
        struct Created {
            #[serde(default)]
            number: Option<u64>,
            #[serde(default, alias = "url")]
            html_url: String,
        }
        let created: Created = Self::parse_json(&self.fj, "fj pr create --json", stdout.trim())?;
        // On the normal success path the JSON carries `number`; if a future fj
        // omits it, fall back to the URL's trailing `/pulls/<n>` (or `/pull/<n>`)
        // segment rather than the discarded `#` heuristic.
        let number = created
            .number
            .or_else(|| number_from_pr_url(&created.html_url));
        Ok(PullRequest {
            number,
            url: created.html_url,
        })
    }

    /// CLAUDE-166: resolve the feature branch by NAME, independent of the
    /// current `HEAD`. The base (default) branch is whatever `origin/HEAD` points
    /// at (e.g. `origin/main`); the feature branch is the single LOCAL branch
    /// (other than the base) that has commits ahead of the base. This is also the
    /// defense-in-depth guardrail (proposed-approach step 6): a branch is a
    /// candidate ONLY if it has a non-empty `rev-list <base>..<branch>`, so the
    /// "no commits ahead" / no-diff case falls into the zero-candidate error
    /// below rather than delegating to `fj`'s opaque "no changes between head and
    /// base". The workdir is named in the errors so the branch stays recoverable
    /// (the failure comment in `src/native/youtrack.rs` preserves it).
    fn resolve_feature_branch(&self, workdir: &Path) -> Result<String> {
        // The base branch is whatever `origin/HEAD` points at, e.g. `origin/main`.
        let base_args = ["rev-parse", "--abbrev-ref", "origin/HEAD"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &base_args, &[])?;
        let base_ref = Self::checked("git", out, &base_args)?.trim().to_string();
        let base_branch = base_ref
            .strip_prefix("origin/")
            .unwrap_or(&base_ref)
            .to_string();

        // Enumerate local branches (never the remote-tracking or `HEAD` refs).
        let list_args = ["for-each-ref", "--format=%(refname:short)", "refs/heads/"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &list_args, &[])?;
        let listed = Self::checked("git", out, &list_args)?;

        let mut candidates: Vec<String> = Vec::new();
        for line in listed.lines() {
            let branch = line.trim();
            if branch.is_empty() || branch == base_branch {
                continue;
            }
            // Ahead of base? A non-empty `rev-list <base>..<branch>` means yes.
            let range = format!("{base_ref}..{branch}");
            let ahead_args = ["rev-list", range.as_str()];
            let out = self
                .runner
                .run_in_env(Some(workdir), "git", &ahead_args, &[])?;
            let ahead = Self::checked("git", out, &ahead_args)?;
            if !ahead.trim().is_empty() {
                candidates.push(branch.to_string());
            }
        }

        match candidates.len() {
            1 => Ok(candidates.pop().unwrap()),
            0 => Err(Error::Other(anyhow::anyhow!(
                "no feature branch with commits ahead of `{base_ref}`; the agent \
                 produced no branch or no commits (base `{base_branch}`). The work, \
                 if any, is preserved in the workdir `{}` for recovery.",
                workdir.display()
            ))),
            _ => Err(Error::Other(anyhow::anyhow!(
                "multiple candidate feature branches ahead of `{base_ref}`: {}; \
                 refusing to guess which to open a PR for. The workdir `{}` is \
                 preserved for recovery.",
                candidates.join(", "),
                workdir.display()
            ))),
        }
    }

    /// Branch-backed PR creation (CLAUDE-103 default, originally the CLAUDE-102
    /// fallback). Push the working branch with the git CLI (same-name remote
    /// branch), then open the PR over the Forgejo API with `fj pr create`,
    /// deriving the title and body from the head commit exactly like AGit
    /// autofill would (the body carries the bare `#<KEY>-N` reference, so
    /// YouTrack still links the PR). The body travels via a file (fj convention
    /// for long bodies), written under the workdir with a RELATIVE path so the
    /// runner key stays deterministic; removed best-effort afterwards. The head
    /// is `refs/heads/<branch>`, never `refs/pull/<N>/head`.
    ///
    /// CLAUDE-166: the feature branch is resolved authoritatively by NAME, never
    /// from the ambient `HEAD`. Everything (push, title/body, `fj --head`) binds
    /// to that resolved branch, so a stray `git checkout main`, a detached
    /// `HEAD`, or an agent bug can no longer strand the work and open a silent
    /// `main`-vs-`main` PR. The push, title, and body all target the resolved
    /// branch; `fj pr create` is invoked with an explicit `--head`, mirroring the
    /// GitHub backend (`github.rs`).
    fn create_pr_branch(&self, workdir: &Path, host: &str) -> Result<PullRequest> {
        let branch = self.resolve_feature_branch(workdir)?;

        // Push the resolved branch BY NAME (`<branch>:<branch>`), not `HEAD`, so
        // the server branch carries the feature name regardless of the current
        // checkout.
        let refspec = format!("{branch}:{branch}");
        let push_args = ["push", "--set-upstream", "origin", refspec.as_str()];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &push_args, &[])?;
        Self::checked("git", out, &push_args)?;

        // Title/body derive from the feature branch tip, not `HEAD`.
        let title_args = ["log", "-1", "--format=%s", branch.as_str()];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &title_args, &[])?;
        let title = Self::checked("git", out, &title_args)?.trim().to_string();

        let body_args = ["log", "-1", "--format=%b", branch.as_str()];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &body_args, &[])?;
        let body = Self::checked("git", out, &body_args)?.trim().to_string();

        const BODY_FILE: &str = ".claude-run-pr-body.md";
        std::fs::write(workdir.join(BODY_FILE), &body)?;
        let create_args = [
            "--host",
            host,
            "pr",
            "create",
            title.as_str(),
            "--body-file",
            BODY_FILE,
            "--head",
            branch.as_str(),
            "--json",
        ];
        let res = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &create_args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &create_args));
        let _ = std::fs::remove_file(workdir.join(BODY_FILE));
        self.created_pr(&res?)
    }

    /// CLAUDE-103 opt-in: AGit + autofill (`fj --host <host> pr create -aA`).
    /// fj pushes the local commits to `refs/for/<base>` and opens a branchless
    /// PR (head at `refs/pull/<N>/head`), deriving the title/body from the
    /// commit messages. No server branch is produced. A failure errors loudly;
    /// there is no silent switch to the branch path.
    fn create_pr_agit(&self, workdir: &Path, host: &str) -> Result<PullRequest> {
        let args = ["--host", host, "pr", "create", "-aA", "--json"];
        let stdout = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &args))?;
        self.created_pr(&stdout)
    }

    /// Probe that both CLIs are invokable (`<bin> --version`). Used by the
    /// factory to decide whether this backend is usable. A missing binary
    /// surfaces as [`Error::CliNotFound`].
    pub fn probe(&self) -> Result<()> {
        self.runner.run(&self.yt, &["--version"])?;
        self.runner.run(&self.fj, &["--version"])?;
        Ok(())
    }

    /// CLAUDE-148: the value of the injected `YOUTRACK_CLI_CONFIG`, if any. The
    /// PATH only is returned, never the file contents (which hold the token).
    /// `None` means host-auth fallback (no injected config).
    fn yt_config_path(&self) -> Option<&str> {
        self.yt_env
            .iter()
            .find(|(k, _)| k == "YOUTRACK_CLI_CONFIG")
            .map(|(_, v)| v.as_str())
    }

    /// CLAUDE-148: the YouTrack base URL the injected config targets, read from
    /// the single-file `yt` config's `url:` line (see
    /// [`crate::secrets::JobCredentials::materialize`], which writes
    /// `url: <url>\ntoken: <token>\n`). The `token:` line is never read or
    /// returned. Returns `None` on host-auth fallback (no injected config) or if
    /// the file is unreadable / has no `url:` line, so the base URL can be logged
    /// in a diagnostic without ever exposing the secret.
    fn yt_base_url(&self) -> Option<String> {
        let path = self.yt_config_path()?;
        let contents = std::fs::read_to_string(path).ok()?;
        contents
            .lines()
            .find_map(|l| l.trim().strip_prefix("url:"))
            .map(|v| v.trim().to_string())
    }

    /// Run `yt args...`, returning trimmed stdout on success or
    /// [`Error::CliFailed`] on a non-zero exit.
    ///
    /// CLAUDE-148: logs the non-secret execution context at `debug` for every
    /// call (resolved binary, injected env KEY names, whether
    /// `YOUTRACK_CLI_CONFIG` is set vs host-auth fallback) and, on a non-zero
    /// exit, a `warn` carrying the config presence and the YouTrack base URL in
    /// effect, so a failure's auth/config context is in the journal. No secret
    /// value (token, decrypted config contents) is ever logged.
    fn yt_ok(&self, args: &[&str]) -> Result<String> {
        let env_keys: Vec<&str> = self.yt_env.iter().map(|(k, _)| k.as_str()).collect();
        let config_present = self.yt_config_path().is_some();
        debug!(
            binary = %self.yt,
            args = %args.join(" "),
            env_keys = ?env_keys,
            youtrack_cli_config_present = config_present,
            "invoking yt"
        );
        let out = self
            .runner
            .run_in_env(None, &self.yt, args, &self.yt_envs())?;
        if !out.ok {
            warn!(
                binary = %self.yt,
                args = %args.join(" "),
                exit_code = out.exit_code,
                youtrack_cli_config_present = config_present,
                base_url = %self.yt_base_url().as_deref().unwrap_or("<host-auth or unknown>"),
                "yt invocation failed"
            );
        }
        Self::checked(&self.yt, out, args)
    }

    /// CLAUDE-148: one-shot identity probe run when a VCS probe (`yt project vcs
    /// list <PROJECT>`) fails. Re-runs `yt` with the SAME injected env as the
    /// failing call to log the authenticated identity (`yt user me`, the
    /// current-user subcommand the installed `yt` exposes) and the project keys
    /// that token can read (`yt project list --json`). Together these separate a
    /// permissions gap (target project absent from the visible list) from a
    /// wrong instance (unexpected base URL) from a host-auth fallback (no
    /// injected config) directly from the journal, without reproducing by hand.
    /// Best-effort: each sub-probe failing is itself logged, never propagated.
    /// No REST (Tooling Gap Discipline) and no secret is ever logged.
    fn log_vcs_probe_identity(&self, project_key: &str) {
        let config_present = self.yt_config_path().is_some();
        let base_url = self.yt_base_url();
        let base = base_url.as_deref().unwrap_or("<host-auth or unknown>");

        match self
            .runner
            .run_in_env(None, &self.yt, &["user", "me"], &self.yt_envs())
        {
            Ok(out) if out.ok => warn!(
                project = %project_key,
                identity = %out.stdout.trim(),
                youtrack_cli_config_present = config_present,
                base_url = %base,
                "VCS probe failed: daemon's yt authenticated identity"
            ),
            Ok(out) => warn!(
                project = %project_key,
                exit_code = out.exit_code,
                stderr = %out.stderr.trim(),
                youtrack_cli_config_present = config_present,
                base_url = %base,
                "VCS probe failed: `yt user me` also failed (cannot determine identity)"
            ),
            Err(e) => warn!(
                project = %project_key,
                error = %e,
                "VCS probe failed: `yt user me` could not run"
            ),
        }

        match self.runner.run_in_env(
            None,
            &self.yt,
            &["project", "list", "--json"],
            &self.yt_envs(),
        ) {
            Ok(out) if out.ok => {
                match Self::parse_json::<Vec<ProjectListEntry>>(
                    &self.yt,
                    "yt project list --json",
                    out.stdout.trim(),
                ) {
                    Ok(projects) => {
                        let keys: Vec<&str> =
                            projects.iter().map(|p| p.short_name.as_str()).collect();
                        let target_visible = keys.contains(&project_key);
                        warn!(
                            project = %project_key,
                            target_visible,
                            visible_projects = ?keys,
                            "VCS probe failed: project keys this yt token can read"
                        );
                    }
                    Err(e) => warn!(
                        project = %project_key,
                        error = %e,
                        "VCS probe failed: `yt project list --json` returned unparseable JSON"
                    ),
                }
            }
            Ok(out) => warn!(
                project = %project_key,
                exit_code = out.exit_code,
                stderr = %out.stderr.trim(),
                "VCS probe failed: `yt project list --json` also failed (cannot list visible projects)"
            ),
            Err(e) => warn!(
                project = %project_key,
                error = %e,
                "VCS probe failed: `yt project list --json` could not run"
            ),
        }
    }

    /// Convert a [`CmdOutput`] into trimmed stdout or a typed [`Error::CliFailed`].
    fn checked(binary: &str, out: CmdOutput, args: &[&str]) -> Result<String> {
        if out.ok {
            Ok(out.stdout)
        } else {
            Err(Error::CliFailed {
                binary: binary.to_string(),
                args: args.join(" "),
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            })
        }
    }

    /// Parse `json` (the stdout of `binary`) into `T`, mapping a parse failure
    /// to the typed [`Error::CliParse`] with `context`.
    fn parse_json<T: serde::de::DeserializeOwned>(
        binary: &str,
        context: &str,
        json: &str,
    ) -> Result<T> {
        serde_json::from_str(json).map_err(|source| Error::CliParse {
            binary: binary.to_string(),
            context: context.to_string(),
            source,
        })
    }
}

impl IssueTracker for CliBackend {
    fn list_queued(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        // `--query=VAL` (not `--query VAL`): the YouTrack query can start with
        // `-Resolved`, which clap would otherwise read as an unknown short flag.
        let query_arg = format!("--query={query}");
        let stdout = self.yt_ok(&["list", &query_arg, "--json"])?;
        Self::parse_json(&self.yt, "yt list --json", &stdout)
    }

    fn get_field(&self, key: &str, field: &str) -> Result<String> {
        let stdout = self.yt_ok(&["issue", "get-field", key, field])?;
        Ok(stdout.trim().to_string())
    }

    fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()> {
        self.yt_ok(&["issue", "set-field", key, field, value])?;
        Ok(())
    }

    fn set_state(&self, key: &str, state: &str) -> Result<()> {
        self.yt_ok(&["issue", "set-state", key, state])?;
        Ok(())
    }

    fn set_assignee(&self, key: &str, login: &str) -> Result<()> {
        // `yt issue update <key> --assignee <login>` is the purpose-built write
        // for the user-type Assignee field (CLAUDE-131); `login` may be a login,
        // `me`, or `unassigned`.
        self.yt_ok(&["issue", "update", key, "--assignee", login])?;
        Ok(())
    }

    fn comment(&self, key: &str, body: &str) -> Result<()> {
        self.yt_ok(&["issue", "comment", key, body])?;
        Ok(())
    }

    fn issue_links(&self, key: &str) -> Result<Vec<super::IssueLink>> {
        let stdout = self.yt_ok(&["issue", "inspect", key, "--json"])?;
        let parsed: InspectLinks = Self::parse_json(&self.yt, "yt issue inspect --json", &stdout)?;
        // Flatten each `links[]` entry into one row per linked target, carrying the
        // entry's direction/verb (YT-40) onto every target it lists.
        let mut out = Vec::new();
        for link in parsed.links {
            for issue in link.issues {
                out.push(super::IssueLink {
                    direction: super::LinkDirection::from_yt(&link.direction),
                    verb: link.verb.clone(),
                    target: issue.id_readable,
                    resolved: issue.resolved,
                });
            }
        }
        Ok(out)
    }

    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo> {
        // `vcs` is a command group; `list` is the subcommand that takes the
        // project short-name positional. Omitting it makes the project land in
        // the `<COMMAND>` slot and `yt` exits 2 (CLAUDE-124).
        // CLAUDE-148: on a probe tool failure (e.g. `project 'VS' not found`,
        // which YouTrack also returns for a project the token cannot READ), run
        // the one-shot identity probe before propagating, so the journal records
        // which yt user/instance the daemon used and whether the project was
        // visible to that token. The error is unchanged; only diagnostics are
        // added.
        let stdout = match self.yt_ok(&[
            "project",
            "vcs",
            "list",
            project_key,
            "--enabled-only",
            "--json",
        ]) {
            Ok(s) => s,
            Err(e) => {
                self.log_vcs_probe_identity(project_key);
                return Err(e);
            }
        };
        let procs: Vec<VcsProcessor> =
            Self::parse_json(&self.yt, "yt project vcs list --json", &stdout)?;
        let first = procs
            .into_iter()
            .next()
            .ok_or_else(|| Error::NoVcsIntegration(project_key.to_string()))?;
        let host = first.server_url.trim_end_matches('/').to_string();
        let clone_url = format!("{host}/{}.git", first.path);
        Ok(VcsRepo {
            repo_host: host,
            repo_path: first.path,
            clone_url,
        })
    }

    fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>> {
        let stdout = self.yt_ok(&["project", "list", "--json"])?;
        let projects: Vec<ProjectListEntry> =
            Self::parse_json(&self.yt, "yt project list --json", &stdout)?;
        let mut entries = Vec::new();
        for project in projects {
            // Archived projects never own live routing; skip without a `vcs` call.
            if project.archived {
                continue;
            }
            // A single project's `vcs` call failing (or emitting malformed JSON)
            // must not disable routing for every other project: log and skip it.
            let raw = match self.yt_ok(&[
                "project",
                "vcs",
                "list",
                &project.short_name,
                "--enabled-only",
                "--json",
            ]) {
                Ok(s) => s,
                Err(e) => {
                    warn!(project = %project.short_name, error = %e, "yt project vcs list failed; skipping project for mint routing");
                    continue;
                }
            };
            let procs: Vec<VcsProcessor> = match Self::parse_json(
                &self.yt,
                "yt project vcs list --json",
                &raw,
            ) {
                Ok(p) => p,
                Err(e) => {
                    warn!(project = %project.short_name, error = %e, "yt project vcs returned unparseable JSON; skipping project for mint routing");
                    continue;
                }
            };
            for proc in procs {
                entries.push(VcsMapEntry {
                    server_url: proc.server_url,
                    path: proc.path,
                    project_key: project.short_name.clone(),
                });
            }
        }
        Ok(entries)
    }

    fn create_issue(
        &self,
        project: &str,
        summary: &str,
        description: &str,
        assignee: Option<&str>,
    ) -> Result<String> {
        // `--description` (not `--description-file`): std::process passes the
        // body as a single argv entry, so embedded newlines / Markdown need no
        // shell escaping and no temp file. `--json` makes the new id parseable.
        let mut args: Vec<&str> = vec![
            "issue",
            "create",
            "--project",
            project,
            "--summary",
            summary,
            "--description",
            description,
        ];
        if let Some(login) = assignee {
            args.push("--assignee");
            args.push(login);
        }
        args.push("--json");
        let stdout = self.yt_ok(&args)?;
        // `yt issue create --json` emits the created issue; pull its idReadable.
        let value: serde_json::Value =
            Self::parse_json(&self.yt, "yt issue create --json", &stdout)?;
        value
            .get("idReadable")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "yt issue create --json returned no idReadable: {value}"
                ))
            })
    }

    fn attach_file(&self, key: &str, file: &Path) -> Result<()> {
        let file_arg = file.to_string_lossy();
        self.yt_ok(&["issue", "attachment", "add", key, &file_arg])?;
        Ok(())
    }
}

impl Forge for CliBackend {
    fn create_pr(
        &self,
        workdir: &Path,
        host: &str,
        strategy: crate::action::PrStrategy,
    ) -> Result<PullRequest> {
        use crate::action::PrStrategy;
        match strategy {
            // CLAUDE-103 default: push a real server branch with the git CLI
            // (proven https-capable, it performed the clone) and open the PR
            // over the API. The resulting head is `refs/heads/<branch>`, so
            // Forgejo can always "Update Branch" and the PR stays mergeable.
            // The selected strategy is authoritative: a failure errors loudly
            // (the finalize step preserves the workdir) rather than silently
            // emitting a branchless AGit PR.
            PrStrategy::Branch => self.create_pr_branch(workdir, host),
            // Opt-in AGit + autofill: fj derives the title/body from the local
            // commit messages, which already carry the bare #<KEY>-N reference.
            // No server branch (head lives only at `refs/pull/<N>/head`).
            PrStrategy::Agit => self.create_pr_agit(workdir, host),
        }
    }

    fn list_open_prs(&self, host: &str, repo: &str) -> Result<Vec<super::ForgePullRequest>> {
        let args = [
            "--host", host, "pr", "search", "--repo", repo, "--state", "open", "--json",
        ];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        Self::parse_json(&self.fj, "fj pr search --json", &stdout)
    }

    fn pr_ci_state(&self, host: &str, repo: &str, pr_number: u64) -> Result<String> {
        // Combined CI status is one field of the full status snapshot.
        Ok(self.pr_status(host, repo, pr_number)?.state)
    }

    fn pr_status(&self, host: &str, repo: &str, pr_number: u64) -> Result<super::PrStatus> {
        // Address the PR as `owner/repo#n` so `fj` resolves the repo without a
        // local checkout (gather runs before any clone).
        let id = format!("{repo}#{pr_number}");
        let args = ["--host", host, "pr", "status", &id, "--json"];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        #[derive(Deserialize)]
        struct Status {
            state: String,
            // A `fj` predating CLAUDE-126's field, or a PR with no verdict yet,
            // is treated as not-mergeable so automerge updates the branch rather
            // than racing a premature merge.
            #[serde(default)]
            mergeable: bool,
        }
        let status: Status = Self::parse_json(&self.fj, "fj pr status --json", &stdout)?;
        Ok(super::PrStatus {
            state: status.state,
            mergeable: status.mergeable,
        })
    }

    fn merge_pr(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        // Merge-commit (`--method merge`), never squash/rebase: every PR from
        // the native action is a single commit carrying the bare `#<KEY>`
        // reference, and only a merge-commit preserves it in history (CLAUDE-126).
        let id = format!("{repo}#{pr_number}");
        // `--delete` removes the PR head branch after merge, matching the
        // Forgejo web UI's "Delete branch after merge" behavior so automerged
        // PRs don't leave stale merged branches behind (CLAUDE-145). It is a
        // no-op when the branch is already gone or is a protected base.
        let args = [
            "--host", host, "pr", "merge", &id, "--method", "merge", "--delete",
        ];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        Self::checked(&self.fj, out, &args)?;
        Ok(())
    }

    fn update_branch(&self, workdir: &Path) -> Result<()> {
        // CLAUDE-126: `fj` exposes no `pr update-branch` subcommand, so perform
        // the Forgejo "Update Branch" with the git CLI (the same binary that
        // cloned and pushed the branch) instead of the REST API: refresh the
        // remote, merge the base branch into the PR head, and push. The push
        // retriggers CI, which the caller then re-polls.
        let fetch = ["fetch", "origin"];
        let out = self.runner.run_in_env(Some(workdir), "git", &fetch, &[])?;
        Self::checked("git", out, &fetch)?;

        // The base branch is whatever `origin/HEAD` points at, e.g. `origin/main`.
        let base_args = ["rev-parse", "--abbrev-ref", "origin/HEAD"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &base_args, &[])?;
        let base = Self::checked("git", out, &base_args)?.trim().to_string();

        let merge_args = ["merge", "--no-edit", base.as_str()];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &merge_args, &[])?;
        Self::checked("git", out, &merge_args)?;

        let push = ["push", "origin", "HEAD"];
        let out = self.runner.run_in_env(Some(workdir), "git", &push, &[])?;
        Self::checked("git", out, &push)?;
        Ok(())
    }

    fn pr_comment(&self, host: &str, repo: &str, pr_number: u64, body_file: &Path) -> Result<()> {
        let id = format!("{repo}#{pr_number}");
        let body = body_file.to_string_lossy();
        let args = ["--host", host, "pr", "comment", &id, "--body-file", &body];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        Self::checked(&self.fj, out, &args)?;
        Ok(())
    }

    fn pr_add_labels(
        &self,
        host: &str,
        repo: &str,
        pr_number: u64,
        labels: &[String],
    ) -> Result<()> {
        // CLAUDE-149: `fj pr edit <repo>#<n> labels --add <label>`. fj's `--add`
        // takes one label, so add each in its own call; addressing the PR as
        // `owner/repo#n` resolves the repo with no local checkout.
        let id = format!("{repo}#{pr_number}");
        for label in labels {
            let args = ["--host", host, "pr", "edit", &id, "labels", "--add", label];
            let out = self
                .runner
                .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
            Self::checked(&self.fj, out, &args)?;
        }
        Ok(())
    }

    fn pr_referenced_issues(&self, host: &str, repo: &str, pr_number: u64) -> Result<Vec<String>> {
        // CLAUDE-160: read the PR body and extract only the owning-reference
        // lines (bare `#<KEY>-N` alone on a line, per the commit convention), so
        // flagging the source issue Needs Review never fans out to related
        // issues cross-linked in prose. Addressed as `owner/repo#n` (no checkout
        // needed).
        let id = format!("{repo}#{pr_number}");
        let args = ["--host", host, "pr", "view", &id, "--json"];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        #[derive(Deserialize)]
        struct View {
            #[serde(default)]
            body: String,
        }
        let view: View = Self::parse_json(&self.fj, "fj pr view --json", &stdout)?;
        Ok(parse_issue_refs(&view.body))
    }

    fn clone_url(&self, host: &str, repo: &str) -> Result<String> {
        // fj exposes no command to emit an authenticated clone URL, so read its
        // own key store (the same store `fj` writes). No token in YAML or env.
        let raw = std::fs::read_to_string(&self.keys_path)?;
        let keys: ForgeKeys = serde_json::from_str(&raw).map_err(|source| Error::CliParse {
            binary: "fj".to_string(),
            context: format!("keys.json at {}", self.keys_path.display()),
            source,
        })?;
        let token = keys
            .hosts
            .get(host)
            .map(|h| h.token.as_str())
            .ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "no forge credentials for host `{host}` in {}",
                    self.keys_path.display()
                ))
            })?;
        Ok(format!("https://oauth2:{token}@{host}/{repo}.git"))
    }
}

impl Backend for CliBackend {
    fn name(&self) -> &'static str {
        "cli"
    }
}

/// Test helper: a [`CommandRunner`] returning canned outcomes keyed by
/// `(binary, args)`, or a missing-binary error for configured binaries.
#[cfg(test)]
pub mod testutil {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// One recorded env injection: `(binary, [(name, value), ...])`.
    pub type EnvCall = (String, Vec<(String, String)>);

    #[derive(Default)]
    pub struct StubRunner {
        /// (binary, joined-args) -> outcome.
        responses: HashMap<String, CmdOutput>,
        /// Binaries that should report as absent from PATH.
        missing: Vec<String>,
        /// Records every (binary, args) invocation for assertions.
        pub calls: Mutex<Vec<String>>,
        /// CLAUDE-26: records the injected child env per invocation for assertions.
        pub envs: Mutex<Vec<EnvCall>>,
    }

    impl StubRunner {
        pub fn new() -> Self {
            Self::default()
        }

        fn key(binary: &str, args: &[&str]) -> String {
            format!("{binary} {}", args.join(" "))
        }

        /// Register a successful (exit 0) response for `binary args...`.
        pub fn ok(mut self, binary: &str, args: &[&str], stdout: &str) -> Self {
            self.responses.insert(
                Self::key(binary, args),
                CmdOutput {
                    ok: true,
                    stdout: stdout.to_string(),
                    stderr: String::new(),
                    exit_code: 0,
                },
            );
            self
        }

        /// Register a failing response (non-zero exit) for `binary args...`.
        pub fn fail(mut self, binary: &str, args: &[&str], exit_code: i32, stderr: &str) -> Self {
            self.responses.insert(
                Self::key(binary, args),
                CmdOutput {
                    ok: false,
                    stdout: String::new(),
                    stderr: stderr.to_string(),
                    exit_code,
                },
            );
            self
        }

        /// Mark `binary` as absent from PATH (every call to it -> CliNotFound).
        pub fn missing(mut self, binary: &str) -> Self {
            self.missing.push(binary.to_string());
            self
        }

        fn lookup(&self, binary: &str, args: &[&str]) -> Result<CmdOutput> {
            self.calls.lock().unwrap().push(Self::key(binary, args));
            if self.missing.iter().any(|b| b == binary) {
                return Err(Error::CliNotFound {
                    binary: binary.to_string(),
                });
            }
            self.responses
                .get(&Self::key(binary, args))
                .cloned()
                .ok_or_else(|| {
                    Error::Other(anyhow::anyhow!(
                        "StubRunner: no canned response for `{}`",
                        Self::key(binary, args)
                    ))
                })
        }
    }

    impl CommandRunner for StubRunner {
        fn run_in_env(
            &self,
            _dir: Option<&Path>,
            binary: &str,
            args: &[&str],
            envs: &[(&str, &str)],
        ) -> Result<CmdOutput> {
            self.envs.lock().unwrap().push((
                binary.to_string(),
                envs.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ));
            self.lookup(binary, args)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::StubRunner;
    use super::*;
    use crate::action::PrStrategy;

    fn backend(runner: StubRunner) -> CliBackend {
        CliBackend::new(Arc::new(runner))
    }

    #[test]
    fn list_queued_parses_json_array() {
        let json = r#"[{"idReadable":"MK-16","summary":"x"},{"idReadable":"MK-17"}]"#;
        let b = backend(StubRunner::new().ok(
            "yt",
            &["list", "--query=AI Agent: Queued", "--json"],
            json,
        ));
        let issues = b.list_queued("AI Agent: Queued").unwrap();
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0]["idReadable"], "MK-16");
    }

    /// CLAUDE-161: `issue_links` flattens the `yt issue inspect --json` `links`
    /// array into one row per linked target, carrying each entry's resolved
    /// `direction` / `verb` (YT-40) and each target's nullable `resolved`. A
    /// resolved target keeps its epoch-ms timestamp; an unresolved one (the key
    /// omitted) maps to `None`. Empty `issues` arrays contribute no rows.
    #[test]
    fn issue_links_parses_direction_verb_resolved() {
        let json = r#"{
          "issue": {"idReadable": "CLAUDE-31"},
          "links": [
            {"direction": "BOTH", "verb": "relates to", "issues": []},
            {"direction": "INWARD", "verb": "depends on",
             "issues": [{"idReadable": "CLAUDE-30", "summary": "x"}]},
            {"direction": "OUTWARD", "verb": "is required for",
             "issues": [{"idReadable": "CLAUDE-32", "resolved": 1780049281523}]},
            {"direction": "OUTWARD", "verb": "parent for",
             "issues": [{"idReadable": "CLAUDE-40"}]}
          ]
        }"#;
        let b =
            backend(StubRunner::new().ok("yt", &["issue", "inspect", "CLAUDE-31", "--json"], json));
        let links = b.issue_links("CLAUDE-31").unwrap();
        assert_eq!(links.len(), 3, "empty issues array contributes no row");

        let dep = links.iter().find(|l| l.target == "CLAUDE-30").unwrap();
        assert_eq!(dep.direction, crate::provider::LinkDirection::Inward);
        assert_eq!(dep.verb, "depends on");
        assert_eq!(dep.resolved, None, "resolved key omitted => unmet");

        let req = links.iter().find(|l| l.target == "CLAUDE-32").unwrap();
        assert_eq!(req.direction, crate::provider::LinkDirection::Outward);
        assert_eq!(req.verb, "is required for");
        assert_eq!(req.resolved, Some(1780049281523));

        let child = links.iter().find(|l| l.target == "CLAUDE-40").unwrap();
        assert_eq!(child.verb, "parent for");
        assert_eq!(child.resolved, None);
    }

    #[test]
    fn list_queued_malformed_json_is_typed_parse_error() {
        let b = backend(StubRunner::new().ok("yt", &["list", "--query=q", "--json"], "not json"));
        match b.list_queued("q").unwrap_err() {
            Error::CliParse {
                binary, context, ..
            } => {
                assert_eq!(binary, "yt");
                assert!(context.contains("yt list"), "got {context}");
            }
            other => panic!("expected CliParse, got {other:?}"),
        }
    }

    #[test]
    fn get_field_trims_stdout() {
        let b = backend(StubRunner::new().ok(
            "yt",
            &["issue", "get-field", "MK-16", "AI Agent"],
            "Queued\n",
        ));
        assert_eq!(b.get_field("MK-16", "AI Agent").unwrap(), "Queued");
    }

    #[test]
    fn nonzero_exit_is_typed_cli_failure() {
        let b = backend(StubRunner::new().fail(
            "yt",
            &["issue", "set-field", "MK-16", "AI Agent", "Working"],
            2,
            "boom",
        ));
        match b.set_field("MK-16", "AI Agent", "Working").unwrap_err() {
            Error::CliFailed {
                binary,
                exit_code,
                stderr,
                ..
            } => {
                assert_eq!(binary, "yt");
                assert_eq!(exit_code, 2);
                assert_eq!(stderr, "boom");
            }
            other => panic!("expected CliFailed, got {other:?}"),
        }
    }

    #[test]
    fn missing_binary_is_typed_not_found() {
        let b = backend(StubRunner::new().missing("yt"));
        match b.get_field("MK-16", "State").unwrap_err() {
            Error::CliNotFound { binary } => assert_eq!(binary, "yt"),
            other => panic!("expected CliNotFound, got {other:?}"),
        }
    }

    #[test]
    fn resolve_vcs_builds_repo() {
        let json = r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#;
        let b = backend(StubRunner::new().ok(
            "yt",
            &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
            json,
        ));
        let repo = b.resolve_vcs("MK").unwrap();
        assert_eq!(repo.repo_host, "https://dev.a8n.run");
        assert_eq!(repo.repo_path, "contoso/claude-run");
        assert_eq!(
            repo.clone_url,
            "https://dev.a8n.run/contoso/claude-run.git"
        );
    }

    #[test]
    fn resolve_vcs_empty_is_no_integration() {
        let b = backend(StubRunner::new().ok(
            "yt",
            &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
            "[]",
        ));
        match b.resolve_vcs("MK").unwrap_err() {
            Error::NoVcsIntegration(key) => assert_eq!(key, "MK"),
            other => panic!("expected NoVcsIntegration, got {other:?}"),
        }
    }

    #[test]
    fn resolve_vcs_failure_runs_identity_diagnostics() {
        // CLAUDE-148: a VCS probe failure (the `project 'VS' not found` shape)
        // triggers the one-shot identity probe: `yt user me` and `yt project
        // list --json`, both with the same injected env, so the journal can
        // separate a permissions gap from a wrong instance. The original
        // CliFailed is still propagated unchanged.
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "yt",
                    &["project", "vcs", "list", "VS", "--enabled-only", "--json"],
                    1,
                    "project 'VS' not found",
                )
                .ok("yt", &["user", "me"], "minted-bot @ https://yt.example.com")
                .ok(
                    "yt",
                    &["project", "list", "--json"],
                    r#"[{"shortName":"CLAUDE","archived":false},{"shortName":"FJ","archived":false}]"#,
                ),
        );
        let b = CliBackend::new(runner.clone());
        match b.resolve_vcs("VS").unwrap_err() {
            Error::CliFailed { binary, stderr, .. } => {
                assert_eq!(binary, "yt");
                assert!(stderr.contains("not found"), "{stderr}");
            }
            other => panic!("expected CliFailed, got {other:?}"),
        }
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c == "yt user me"),
            "identity probe must run `yt user me`: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "yt project list --json"),
            "identity probe must list visible projects: {calls:?}"
        );
    }

    #[test]
    fn resolve_vcs_success_skips_identity_diagnostics() {
        // The identity probe is a failure-only diagnostic: a successful probe
        // must not spend two extra `yt` calls.
        let json = r#"[{"serverUrl":"https://dev.a8n.run/","path":"contoso/claude-run"}]"#;
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["project", "vcs", "list", "MK", "--enabled-only", "--json"],
            json,
        ));
        let b = CliBackend::new(runner.clone());
        b.resolve_vcs("MK").unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c == "yt user me"),
            "no identity probe on success: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c == "yt project list --json"),
            "no identity probe on success: {calls:?}"
        );
    }

    #[test]
    fn identity_probe_reuses_injected_yt_env() {
        // CLAUDE-148: the diagnostic sub-probes must carry the SAME injected env
        // as the failing call, so the logged identity belongs to the daemon's
        // minted token, not host auth.
        let runner = Arc::new(
            StubRunner::new()
                .fail(
                    "yt",
                    &["project", "vcs", "list", "VS", "--enabled-only", "--json"],
                    1,
                    "project 'VS' not found",
                )
                .ok("yt", &["user", "me"], "bot")
                .ok("yt", &["project", "list", "--json"], "[]"),
        );
        let b = CliBackend::new(runner.clone()).with_yt_env(vec![(
            "YOUTRACK_CLI_CONFIG".to_string(),
            "/tmp/creds/yt-config.yml".to_string(),
        )]);
        let _ = b.resolve_vcs("VS").unwrap_err();
        let envs = runner.envs.lock().unwrap();
        // Every recorded yt invocation (probe + both sub-probes) saw the config.
        let yt_calls: Vec<_> = envs.iter().filter(|(bin, _)| bin == "yt").collect();
        assert_eq!(yt_calls.len(), 3, "{yt_calls:?}");
        for (_, env) in yt_calls {
            assert!(
                env.iter()
                    .any(|(k, v)| k == "YOUTRACK_CLI_CONFIG" && v == "/tmp/creds/yt-config.yml"),
                "sub-probe lost the injected env: {env:?}"
            );
        }
    }

    #[test]
    fn yt_base_url_reads_url_never_token() {
        // CLAUDE-148: base URL is extracted from the config's `url:` line only;
        // the `token:` line is never read into the returned value, so the
        // failure-path log can carry the base URL without exposing the secret.
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("yt-config.yml");
        std::fs::write(
            &cfg,
            "url: https://yt.example.com\ntoken: perm:SUPER-SECRET\n",
        )
        .unwrap();
        let b = CliBackend::new(Arc::new(StubRunner::new())).with_yt_env(vec![(
            "YOUTRACK_CLI_CONFIG".to_string(),
            cfg.to_string_lossy().into_owned(),
        )]);
        assert_eq!(b.yt_base_url().as_deref(), Some("https://yt.example.com"));
        assert_eq!(b.yt_config_path(), Some(cfg.to_string_lossy().as_ref()));
        // Host-auth fallback (no injected config) yields no base URL.
        let host_auth = CliBackend::new(Arc::new(StubRunner::new()));
        assert_eq!(host_auth.yt_base_url(), None);
        assert_eq!(host_auth.yt_config_path(), None);
    }

    #[test]
    fn vcs_repo_map_flattens_skips_archived_and_tolerates_a_failing_project() {
        // CLAUDE-86: list projects, skip the archived one, flatten each live
        // project's enabled processors, and degrade a failing per-project `vcs`
        // call to a skip (not a hard error).
        let projects = r#"[
            {"shortName":"CLAUDE","archived":false},
            {"shortName":"OLD","archived":true},
            {"shortName":"FJ","archived":false},
            {"shortName":"BROKEN","archived":false}
        ]"#;
        let runner = StubRunner::new()
            .ok("yt", &["project", "list", "--json"], projects)
            .ok(
                "yt",
                &[
                    "project",
                    "vcs",
                    "list",
                    "CLAUDE",
                    "--enabled-only",
                    "--json",
                ],
                r#"[{"serverUrl":"https://dev.a8n.run","path":"contoso/claude-run"}]"#,
            )
            .ok(
                "yt",
                &["project", "vcs", "list", "FJ", "--enabled-only", "--json"],
                r#"[{"serverUrl":"https://dev.a8n.run","path":"fabrikam/forgejo-cli"}]"#,
            )
            .fail(
                "yt",
                &[
                    "project",
                    "vcs",
                    "list",
                    "BROKEN",
                    "--enabled-only",
                    "--json",
                ],
                1,
                "boom",
            );
        let b = backend(runner);
        let map = b.vcs_repo_map().unwrap();
        // The archived project is never queried; the broken one is skipped.
        assert_eq!(map.len(), 2, "{map:?}");
        assert!(map.iter().any(|e| e.project_key == "CLAUDE"
            && e.path == "contoso/claude-run"
            && e.server_url == "https://dev.a8n.run"));
        assert!(map
            .iter()
            .any(|e| e.project_key == "FJ" && e.path == "fabrikam/forgejo-cli"));
        assert!(!map.iter().any(|e| e.project_key == "BROKEN"));
    }

    #[test]
    fn create_pr_agit_strategy_parses_number() {
        // CLAUDE-103: `pr_strategy: agit` invokes `fj pr create -aA` as before.
        let runner = Arc::new(StubRunner::new().ok(
            "fj",
            &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
            r#"{"number":42,"html_url":"https://dev.a8n.run/n/c/pulls/42"}"#,
        ));
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(Path::new("/tmp/x"), "dev.a8n.run", PrStrategy::Agit)
            .unwrap();
        assert_eq!(pr.number, Some(42));
        assert_eq!(pr.url, "https://dev.a8n.run/n/c/pulls/42");
        // The AGit path issues `-aA --json` and never pushes a branch.
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr create -aA --json"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "agit must not push a branch: {calls:?}"
        );
    }

    /// Register the git plumbing calls CLAUDE-166 resolve-by-name performs
    /// before the push: `origin/HEAD` -> base, `for-each-ref` lists local
    /// branches, and one `rev-list <base>..<branch>` per non-base branch. The
    /// caller passes the branch listing and, per branch, whether it is ahead.
    fn with_branch_resolution(
        runner: StubRunner,
        base_ref: &str,
        branches: &[&str],
        ahead: &[(&str, &str)],
    ) -> StubRunner {
        let base_branch = base_ref.strip_prefix("origin/").unwrap_or(base_ref);
        let listing = branches.join("\n");
        let mut r = runner
            .ok(
                "git",
                &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                base_ref,
            )
            .ok(
                "git",
                &["for-each-ref", "--format=%(refname:short)", "refs/heads/"],
                &listing,
            );
        for b in branches {
            if *b == base_branch {
                continue;
            }
            let range = format!("{base_ref}..{b}");
            let out = ahead
                .iter()
                .find(|(name, _)| name == b)
                .map(|(_, sha)| *sha)
                .unwrap_or("");
            r = r.ok("git", &["rev-list", range.as_str()], out);
        }
        r
    }

    #[test]
    fn create_pr_branch_strategy_pushes_branch_and_creates_via_api() {
        // CLAUDE-103 default: `pr_strategy: branch` pushes a real server branch
        // with the git CLI and opens the PR over the API, deriving title/body
        // from the head commit. `-aA` is never invoked. CLAUDE-166: the branch is
        // resolved by name and pushed as `<branch>:<branch>` with an explicit
        // `--head`.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        )
        .ok(
            "git",
            &["push", "--set-upstream", "origin", "feat/x:feat/x"],
            "",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%s", "feat/x"],
            "feat(x): do the thing\n",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%b", "feat/x"],
            "Body of the commit.\n\n#MK-16\n",
        )
        .ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): do the thing",
                "--body-file",
                ".claude-run-pr-body.md",
                "--head",
                "feat/x",
                "--json",
            ],
            r#"{"number":77,"html_url":"https://dev.a8n.run/n/c/pulls/77"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(77));
        assert_eq!(pr.url, "https://dev.a8n.run/n/c/pulls/77");
        // The body file is cleaned up after the create.
        assert!(!wd.path().join(".claude-run-pr-body.md").exists());
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/x:feat/x"),
            "{calls:?}"
        );
        // Never push the ambient HEAD; always the resolved branch by name.
        assert!(
            !calls
                .iter()
                .any(|c| c == "git push --set-upstream origin HEAD"),
            "must not push HEAD: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("-aA")),
            "branch strategy must never invoke -aA: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_resolves_by_name_when_head_is_on_main() {
        // CLAUDE-166 / FJ-51: the agent committed on a feature branch but left
        // `HEAD` on `main`. Resolution is by NAME (never `HEAD`), so the runner
        // still pushes the feature branch and opens feature -> base, rather than
        // attempting main-vs-main. The StubRunner keys on (binary, args) with no
        // notion of the current `HEAD`; the guarantee is that the runner never
        // issues a `HEAD`-derived push or a `main`-vs-`main` create.
        let wd = tempfile::tempdir().unwrap();
        // `main` (the base) is listed alongside the feature branch and is NOT a
        // candidate (`main` == base, and it has no commits ahead of itself).
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "fix/CLAUDE-166-work"],
            &[("fix/CLAUDE-166-work", "deadbeef\n")],
        )
        .ok(
            "git",
            &[
                "push",
                "--set-upstream",
                "origin",
                "fix/CLAUDE-166-work:fix/CLAUDE-166-work",
            ],
            "",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%s", "fix/CLAUDE-166-work"],
            "fix: harden PR creation\n",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%b", "fix/CLAUDE-166-work"],
            "#CLAUDE-166\n",
        )
        .ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix: harden PR creation",
                "--body-file",
                ".claude-run-pr-body.md",
                "--head",
                "fix/CLAUDE-166-work",
                "--json",
            ],
            r#"{"number":51,"html_url":"https://dev.a8n.run/n/c/pulls/51"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(51));
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c
                    == "git push --set-upstream origin fix/CLAUDE-166-work:fix/CLAUDE-166-work"),
            "must push the feature branch by name: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.contains("--head fix/CLAUDE-166-work")),
            "fj must open feature -> base with explicit --head: {calls:?}"
        );
        // The bug's signature: a HEAD push or a `main`-vs-`main` create.
        assert!(
            !calls
                .iter()
                .any(|c| c == "git push --set-upstream origin HEAD"),
            "must not push HEAD: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--head main")),
            "must not open main-vs-main: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_zero_candidates_is_distinct_actionable_error() {
        // CLAUDE-166: the FJ-51 no-branch / no-commits case is made explicit. The
        // only local branch is the base itself, so there is no candidate; the
        // runner errors loudly (naming base and preserved workdir) instead of
        // delegating to `fj`'s opaque "no changes between head and base".
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main"],
            &[],
        ));
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
        {
            Error::Other(e) => {
                let msg = e.to_string();
                assert!(msg.contains("no feature branch"), "{msg}");
                assert!(msg.contains("origin/main"), "{msg}");
                assert!(msg.contains("preserved"), "{msg}");
            }
            other => panic!("expected Other actionable error, got {other:?}"),
        }
        // Never reached `fj`, so no opaque validation error is surfaced.
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "must not delegate to fj on zero candidates: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_multiple_candidates_names_them_and_refuses() {
        // CLAUDE-166: more than one branch ahead of base -> refuse to guess,
        // naming the candidates.
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/a", "feat/b"],
            &[("feat/a", "aaa\n"), ("feat/b", "bbb\n")],
        ));
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
        {
            Error::Other(e) => {
                let msg = e.to_string();
                assert!(msg.contains("multiple candidate"), "{msg}");
                assert!(msg.contains("feat/a"), "{msg}");
                assert!(msg.contains("feat/b"), "{msg}");
            }
            other => panic!("expected Other actionable error, got {other:?}"),
        }
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "must not delegate to fj on multiple candidates: {calls:?}"
        );
    }

    #[test]
    fn created_pr_falls_back_to_url_when_number_field_absent() {
        // CLAUDE-136: a future fj that omits `number` from the JSON still yields
        // a number, derived from the URL's trailing `/pulls/<n>` segment, so
        // automerge keeps working rather than silently disabling.
        let b = CliBackend::new(Arc::new(StubRunner::new()));
        let pr = b
            .created_pr(r#"{"html_url":"https://dev.a8n.run/fabrikam/gizmo/pulls/26"}"#)
            .unwrap();
        assert_eq!(pr.number, Some(26));
        assert_eq!(pr.url, "https://dev.a8n.run/fabrikam/gizmo/pulls/26");
    }

    #[test]
    fn number_from_pr_url_handles_both_path_shapes() {
        // CLAUDE-136: Forgejo emits `/pulls/<n>`, GitHub-style `/pull/<n>`.
        assert_eq!(
            number_from_pr_url("https://dev.a8n.run/n/c/pulls/52"),
            Some(52)
        );
        assert_eq!(number_from_pr_url("https://github.com/o/r/pull/7"), Some(7));
        assert_eq!(number_from_pr_url("https://dev.a8n.run/n/c/issues/3"), None);
    }

    #[test]
    fn create_pr_branch_strategy_errors_loudly_on_push_failure() {
        // CLAUDE-103: a failure of the selected (branch) strategy surfaces the
        // underlying CLI error directly. There is NO silent switch to AGit that
        // would emit a branchless PR.
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            with_branch_resolution(
                StubRunner::new(),
                "origin/main",
                &["main", "feat/x"],
                &[("feat/x", "abc\n")],
            )
            .fail(
                "git",
                &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                128,
                "could not read Username",
            ),
        );
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
        {
            Error::CliFailed { binary, stderr, .. } => {
                assert_eq!(binary, "git");
                assert!(stderr.contains("could not read Username"), "{stderr}");
            }
            other => panic!("expected CliFailed, got {other:?}"),
        }
        // No fallback to AGit was attempted.
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("-aA")),
            "branch failure must not fall back to AGit: {calls:?}"
        );
    }

    #[test]
    fn clone_url_reads_token_from_keystore() {
        let dir = tempfile::tempdir().unwrap();
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{"dev.a8n.run":{"token":"secret"}}}"#).unwrap();
        let b = CliBackend::new(Arc::new(StubRunner::new())).with_keys_path(&keys);
        let url = b.clone_url("dev.a8n.run", "contoso/claude-run").unwrap();
        assert_eq!(
            url,
            "https://oauth2:secret@dev.a8n.run/contoso/claude-run.git"
        );
    }

    #[test]
    fn list_open_prs_parses_search_json() {
        let json = r#"[{"number":52,"title":"feat: x","state":"open","head_sha":"abc","head_ref":"feat/x","base_ref":"main","author":"David","html_url":"https://dev.a8n.run/n/c/pulls/52","updated_at":"2026-05-29T11:10:28Z","draft":false,"labels":[]}]"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "search",
                "--repo",
                "contoso/claude-run",
                "--state",
                "open",
                "--json",
            ],
            json,
        ));
        let prs = b
            .list_open_prs("dev.a8n.run", "contoso/claude-run")
            .unwrap();
        assert_eq!(prs.len(), 1);
        assert_eq!(prs[0].number, 52);
        assert_eq!(prs[0].head_sha, "abc");
        assert_eq!(prs[0].head_ref, "feat/x");
        assert_eq!(prs[0].author, "David");
        assert_eq!(prs[0].updated_at, "2026-05-29T11:10:28Z");
    }

    #[test]
    fn pr_ci_state_extracts_combined_state() {
        let json = r#"{"pr":52,"head_sha":"abc","state":"failure","statuses":[],"mergeable":true,"merged":false}"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "status",
                "contoso/claude-run#52",
                "--json",
            ],
            json,
        ));
        assert_eq!(
            b.pr_ci_state("dev.a8n.run", "contoso/claude-run", 52)
                .unwrap(),
            "failure"
        );
    }

    #[test]
    fn pr_status_extracts_state_and_mergeable() {
        // CLAUDE-126: pr_status surfaces both the combined CI state and the
        // mergeability verdict from the same `fj pr status --json`.
        let json = r#"{"pr":52,"head_sha":"abc","state":"success","statuses":[],"mergeable":true,"merged":false}"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "status",
                "contoso/claude-run#52",
                "--json",
            ],
            json,
        ));
        let st = b
            .pr_status("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        assert_eq!(st.state, "success");
        assert!(st.mergeable);
    }

    #[test]
    fn merge_pr_invokes_fj_merge_with_merge_method_and_deletes_branch() {
        // CLAUDE-126: merge-commit method, addressed as owner/repo#n.
        // CLAUDE-145: `--delete` removes the PR head branch after merge.
        let runner = std::sync::Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "merge",
                "contoso/claude-run#52",
                "--method",
                "merge",
                "--delete",
            ],
            "",
        ));
        let b = CliBackend::new(runner.clone());
        b.merge_pr("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| {
                c
                == "fj --host dev.a8n.run pr merge contoso/claude-run#52 --method merge --delete"
            }),
            "{calls:?}"
        );
    }

    #[test]
    fn update_branch_merges_base_into_head_and_pushes() {
        // CLAUDE-126: the git-based "Update Branch" fetches, resolves the base
        // from origin/HEAD, merges it into the PR head, and pushes.
        let runner = std::sync::Arc::new(
            StubRunner::new()
                .ok("git", &["fetch", "origin"], "")
                .ok(
                    "git",
                    &["rev-parse", "--abbrev-ref", "origin/HEAD"],
                    "origin/main\n",
                )
                .ok("git", &["merge", "--no-edit", "origin/main"], "")
                .ok("git", &["push", "origin", "HEAD"], ""),
        );
        let b = CliBackend::new(runner.clone());
        b.update_branch(Path::new("/tmp/wd")).unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c == "git merge --no-edit origin/main"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "git push origin HEAD"),
            "{calls:?}"
        );
    }

    #[test]
    fn pr_comment_addresses_pr_by_owner_repo_hash() {
        let runner = std::sync::Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "comment",
                "contoso/claude-run#52",
                "--body-file",
                "/tmp/body.md",
            ],
            "",
        ));
        let b = CliBackend::new(runner.clone());
        b.pr_comment(
            "dev.a8n.run",
            "contoso/claude-run",
            52,
            Path::new("/tmp/body.md"),
        )
        .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run pr comment contoso/claude-run#52 --body-file /tmp/body.md"),
            "{calls:?}"
        );
    }

    #[test]
    fn pr_add_labels_edits_each_label_addressed_by_owner_repo_hash() {
        // CLAUDE-149: `fj pr edit <repo>#<n> labels --add <label>`, one call per
        // label.
        let runner = std::sync::Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "edit",
                        "contoso/claude-run#52",
                        "labels",
                        "--add",
                        "needs-review",
                    ],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "edit",
                        "contoso/claude-run#52",
                        "labels",
                        "--add",
                        "blocked",
                    ],
                    "",
                ),
        );
        let b = CliBackend::new(runner.clone());
        b.pr_add_labels(
            "dev.a8n.run",
            "contoso/claude-run",
            52,
            &["needs-review".to_string(), "blocked".to_string()],
        )
        .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| {
                c
                == "fj --host dev.a8n.run pr edit contoso/claude-run#52 labels --add needs-review"
            }),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run pr edit contoso/claude-run#52 labels --add blocked"),
            "{calls:?}"
        );
    }

    #[test]
    fn pr_referenced_issues_extracts_keys_from_the_pr_body() {
        // CLAUDE-149: the PR body carries the bare `#<KEY>-N` reference; read it
        // back via `fj pr view --json` and return the issue keys.
        let view =
            r#"{"number":52,"body":"Implements the fix.\n\n#CLAUDE-149\n\nCo-Authored-By: x"}"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "view",
                "contoso/claude-run#52",
                "--json",
            ],
            view,
        ));
        let keys = b
            .pr_referenced_issues("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        assert_eq!(keys, vec!["CLAUDE-149".to_string()]);
    }

    #[test]
    fn pr_referenced_issues_empty_when_body_has_no_ref() {
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "view",
                "contoso/claude-run#52",
                "--json",
            ],
            r#"{"number":52,"body":"no references here"}"#,
        ));
        assert!(b
            .pr_referenced_issues("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn pr_referenced_issues_returns_only_the_owning_ref_not_cross_links() {
        // CLAUDE-160: the PR body cross-links two related issues in prose and
        // carries the owning issue as a bare trailer line. Only the owning issue
        // is returned, so flagging Needs Review never touches the related work.
        let view = r#"{"number":52,"body":"Implements the fix.\n\nRelated issues:\n- depends on #YOTUN-72\n- relates to #YOTUN-73\n\n#YOTUN-65\n\nCo-Authored-By: x"}"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "view",
                "contoso/claude-run#52",
                "--json",
            ],
            view,
        ));
        let keys = b
            .pr_referenced_issues("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        assert_eq!(keys, vec!["YOTUN-65".to_string()]);
    }

    #[test]
    fn parse_issue_refs_finds_dedupes_and_ignores_non_refs() {
        // A bare owning reference on its own line is picked up.
        assert_eq!(parse_issue_refs("#CLAUDE-149"), vec!["CLAUDE-149"]);
        // Multiple owning refs (one per line), deduped in first-seen order.
        assert_eq!(
            parse_issue_refs("#DELTA-200\n#YT-1\n#DELTA-200"),
            vec!["DELTA-200".to_string(), "YT-1".to_string()]
        );
        // Surrounding whitespace on the trailer line is tolerated.
        assert_eq!(parse_issue_refs("  #YT-1  "), vec!["YT-1".to_string()]);
        // A bare `#` or a `#word` with no `-N` is not a ref.
        assert!(parse_issue_refs("# heading\n#notakey\n#lower-1").is_empty());
        // A key needs at least one trailing digit.
        assert!(parse_issue_refs("#CLAUDE-").is_empty());
    }

    #[test]
    fn parse_issue_refs_ignores_cross_links_and_keeps_only_owning_line() {
        // CLAUDE-160: a body that cross-links related work in prose but carries
        // the owning reference as a bare trailer line must yield ONLY the owning
        // issue - never the related / depends / relates mentions.
        let body = "Implements the pointer-event drag fix.\n\
             \n\
             Related issues:\n\
             - depends on #YOTUN-72 (future feature)\n\
             - relates to #YOTUN-73 (backend bug)\n\
             \n\
             #YOTUN-65\n\
             \n\
             Co-Authored-By: x";
        assert_eq!(parse_issue_refs(body), vec!["YOTUN-65".to_string()]);
    }

    #[test]
    fn parse_issue_refs_ignores_forgejo_merge_subject_pr_number_and_prose() {
        // CLAUDE-160: a Forgejo "Merge pull request ... (#NNN)" commit embeds the
        // whole PR description. The `(#145)` PR number is not a KEY-N ref, and a
        // second unrelated issue ID mentioned in prose must not be returned;
        // only the bare owning trailer line survives.
        let body = "Merge pull request 'feat: drag' (#145) from feat/x into main\n\
             \n\
             Reviewed-on: https://dev.a8n.run/contoso/x/pulls/145\n\
             Implements the fix. See also #YOTUN-73 for the backend follow-up.\n\
             \n\
             #YOTUN-65";
        assert_eq!(parse_issue_refs(body), vec!["YOTUN-65".to_string()]);
    }

    #[test]
    fn injected_env_reaches_yt_and_fj_children_only() {
        // CLAUDE-26: a credentialed backend injects YOUTRACK_CLI_CONFIG onto the
        // yt child and an XDG_DATA_HOME override onto the fj child, each on its
        // own child only.
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "get-field", "MK-16", "AI Agent"],
                    "Queued\n",
                )
                .ok(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    r#"{"number":7,"html_url":"https://dev.a8n.run/n/c/pulls/7"}"#,
                ),
        );
        let backend = CliBackend::new(runner.clone())
            .with_yt_env(vec![(
                "YOUTRACK_CLI_CONFIG".to_string(),
                "/tmp/creds/yt-config.yml".to_string(),
            )])
            .with_fj_env(vec![(
                "XDG_DATA_HOME".to_string(),
                "/tmp/creds".to_string(),
            )]);

        backend.get_field("MK-16", "AI Agent").unwrap();
        backend
            .create_pr(Path::new("/tmp/x"), "dev.a8n.run", PrStrategy::Agit)
            .unwrap();

        let envs = runner.envs.lock().unwrap();
        let yt = envs.iter().find(|(b, _)| b == "yt").unwrap();
        assert_eq!(
            yt.1,
            vec![(
                "YOUTRACK_CLI_CONFIG".to_string(),
                "/tmp/creds/yt-config.yml".to_string()
            )]
        );
        // The yt child never sees the fj env, and vice versa.
        assert!(!yt.1.iter().any(|(k, _)| k == "XDG_DATA_HOME"));
        let fj = envs.iter().find(|(b, _)| b == "fj").unwrap();
        assert_eq!(
            fj.1,
            vec![("XDG_DATA_HOME".to_string(), "/tmp/creds".to_string())]
        );
        assert!(!fj.1.iter().any(|(k, _)| k == "YOUTRACK_CLI_CONFIG"));
    }

    #[test]
    fn clone_url_unknown_host_errors() {
        let dir = tempfile::tempdir().unwrap();
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let b = CliBackend::new(Arc::new(StubRunner::new())).with_keys_path(&keys);
        assert!(b.clone_url("dev.a8n.run", "x/y").is_err());
    }
}
