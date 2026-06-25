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
use tracing::warn;

use super::{Backend, Forge, IssueTracker, PullRequest, VcsMapEntry, VcsRepo};
use crate::error::{Error, Result};

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

    /// Parse `fj pr create` stdout (a line containing `pull request #<n>`)
    /// into a [`PullRequest`]. Shared by the AGit and fallback paths.
    fn created_pr(stdout: &str) -> PullRequest {
        let trimmed = stdout.trim();
        let number = trimmed.split_once('#').and_then(|(_, rest)| {
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse::<u64>().ok()
        });
        PullRequest {
            number,
            url: trimmed.to_string(),
        }
    }

    /// Branch-backed PR creation (CLAUDE-103 default, originally the CLAUDE-102
    /// fallback). Push the working branch with the git CLI (`--set-upstream
    /// origin HEAD`, same-name remote branch), then open the PR over the Forgejo
    /// API with `fj pr create`, deriving the title and body from the head commit
    /// exactly like AGit autofill would (the body carries the bare `#<KEY>-N`
    /// reference, so YouTrack still links the PR). The body travels via a file
    /// (fj convention for long bodies), written under the workdir with a
    /// RELATIVE path so the runner key stays deterministic; removed best-effort
    /// afterwards. The head is `refs/heads/<branch>`, never `refs/pull/<N>/head`.
    fn create_pr_branch(&self, workdir: &Path, host: &str) -> Result<PullRequest> {
        let push_args = ["push", "--set-upstream", "origin", "HEAD"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &push_args, &[])?;
        Self::checked("git", out, &push_args)?;

        let title_args = ["log", "-1", "--format=%s"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &title_args, &[])?;
        let title = Self::checked("git", out, &title_args)?.trim().to_string();

        let body_args = ["log", "-1", "--format=%b"];
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
        ];
        let res = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &create_args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &create_args));
        let _ = std::fs::remove_file(workdir.join(BODY_FILE));
        Ok(Self::created_pr(&res?))
    }

    /// CLAUDE-103 opt-in: AGit + autofill (`fj --host <host> pr create -aA`).
    /// fj pushes the local commits to `refs/for/<base>` and opens a branchless
    /// PR (head at `refs/pull/<N>/head`), deriving the title/body from the
    /// commit messages. No server branch is produced. A failure errors loudly;
    /// there is no silent switch to the branch path.
    fn create_pr_agit(&self, workdir: &Path, host: &str) -> Result<PullRequest> {
        let args = ["--host", host, "pr", "create", "-aA"];
        let stdout = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &args))?;
        Ok(Self::created_pr(&stdout))
    }

    /// Probe that both CLIs are invokable (`<bin> --version`). Used by the
    /// factory to decide whether this backend is usable. A missing binary
    /// surfaces as [`Error::CliNotFound`].
    pub fn probe(&self) -> Result<()> {
        self.runner.run(&self.yt, &["--version"])?;
        self.runner.run(&self.fj, &["--version"])?;
        Ok(())
    }

    /// Run `yt args...`, returning trimmed stdout on success or
    /// [`Error::CliFailed`] on a non-zero exit.
    fn yt_ok(&self, args: &[&str]) -> Result<String> {
        let out = self
            .runner
            .run_in_env(None, &self.yt, args, &self.yt_envs())?;
        Self::checked(&self.yt, out, args)
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

    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo> {
        // `vcs` is a command group; `list` is the subcommand that takes the
        // project short-name positional. Omitting it makes the project land in
        // the `<COMMAND>` slot and `yt` exits 2 (CLAUDE-124).
        let stdout = self.yt_ok(&[
            "project",
            "vcs",
            "list",
            project_key,
            "--enabled-only",
            "--json",
        ])?;
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
        let args = ["--host", host, "pr", "merge", &id, "--method", "merge"];
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
            &["--host", "dev.a8n.run", "pr", "create", "-aA"],
            "Created pull request #42\n",
        ));
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(Path::new("/tmp/x"), "dev.a8n.run", PrStrategy::Agit)
            .unwrap();
        assert_eq!(pr.number, Some(42));
        assert_eq!(pr.url, "Created pull request #42");
        // The AGit path issues `-aA` and never pushes a branch.
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "fj --host dev.a8n.run pr create -aA"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "agit must not push a branch: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_strategy_pushes_branch_and_creates_via_api() {
        // CLAUDE-103 default: `pr_strategy: branch` pushes a real server branch
        // with the git CLI and opens the PR over the API, deriving title/body
        // from the head commit. `-aA` is never invoked.
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(
            StubRunner::new()
                .ok("git", &["push", "--set-upstream", "origin", "HEAD"], "")
                .ok(
                    "git",
                    &["log", "-1", "--format=%s"],
                    "feat(x): do the thing\n",
                )
                .ok(
                    "git",
                    &["log", "-1", "--format=%b"],
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
                    ],
                    "Created pull request #77\n",
                ),
        );
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(77));
        // The body file is cleaned up after the create.
        assert!(!wd.path().join(".claude-run-pr-body.md").exists());
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin HEAD"),
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("-aA")),
            "branch strategy must never invoke -aA: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_strategy_errors_loudly_on_push_failure() {
        // CLAUDE-103: a failure of the selected (branch) strategy surfaces the
        // underlying CLI error directly. There is NO silent switch to AGit that
        // would emit a branchless PR.
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(StubRunner::new().fail(
            "git",
            &["push", "--set-upstream", "origin", "HEAD"],
            128,
            "could not read Username",
        ));
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
    fn merge_pr_invokes_fj_merge_with_merge_method() {
        // CLAUDE-126: merge-commit method, addressed as owner/repo#n.
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
            ],
            "",
        ));
        let b = CliBackend::new(runner.clone());
        b.merge_pr("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c
                    == "fj --host dev.a8n.run pr merge contoso/claude-run#52 --method merge"),
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
                    &["--host", "dev.a8n.run", "pr", "create", "-aA"],
                    "Created pull request #7",
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
