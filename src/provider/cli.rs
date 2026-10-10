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
use std::time::Duration;

use serde::Deserialize;
use tracing::{debug, error, info, warn};

use super::{Backend, Forge, ForgePullRequest, IssueTracker, PullRequest, VcsMapEntry, VcsRepo};
use crate::error::{Error, Result};

/// The envelope `fj pr search --json` reports as of `fj` 0.17.1
/// (`{"total_count": N, "pull_requests": [...]}`); older `fj` versions emitted
/// the bare array. `total_count` is read by nothing here; the array is.
#[derive(Deserialize)]
struct PrSearchResponse {
    pull_requests: Vec<ForgePullRequest>,
}

/// Wait before attempts 2, 3 and 4 of a retried read-only `fj` call
/// (CLAUDE-366): the first retry runs immediately, then 2s, then 5s, for a
/// worst-case added delay of 7s across four attempts total.
const DEFAULT_FJ_READ_RETRY_WAITS: [Duration; 3] = [
    Duration::from_secs(0),
    Duration::from_secs(2),
    Duration::from_secs(5),
];

/// The known secrets to redact out of a retried git call's logged stderr
/// (CLAUDE-396): `token_url`'s userinfo when it carries a credential, empty
/// when the call ran unauthenticated.
fn token_url_secrets(token_url: Option<&str>) -> Vec<String> {
    token_url
        .map(crate::provider::url_secrets)
        .unwrap_or_default()
}

/// The last non-empty line of `stderr`, for a retry log that names the failure
/// without repeating the CLI's full (often multi-line) error text.
fn stderr_tail(stderr: &str) -> &str {
    stderr
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no stderr")
}

/// Run a read-only `fj` invocation with up to four attempts total, waiting
/// `waits[0]` / `waits[1]` / `waits[2]` before attempts 2, 3 and 4 (CLAUDE-366).
/// A failed attempt is a runner `Ok(out)` with `!out.ok`: any non-zero exit is
/// retried on the exit code alone, never by classifying `stderr`'s wording
/// (forbidden by `tooling-gap-discipline`), because every read-only `fj` call is
/// idempotent and a deterministic failure (404, 401) only costs the wait time. A
/// runner `Err` (a spawn failure, `CliNotFound`) is not retried, since it cannot
/// be transient. Only the last attempt's outcome is returned; the caller
/// converts it to `Error::CliFailed` exactly as it did before this call could
/// retry.
fn run_fj_read_retrying(
    runner: &dyn CommandRunner,
    fj: &str,
    dir: Option<&Path>,
    args: &[&str],
    envs: &[(&str, &str)],
    waits: &[Duration; 3],
) -> Result<CmdOutput> {
    let mut out = runner.run_in_env(dir, fj, args, envs)?;
    for (idx, wait) in waits.iter().enumerate() {
        if out.ok {
            return Ok(out);
        }
        let attempt = idx as u32 + 1;
        warn!(
            binary = %fj,
            args = %args.join(" "),
            attempt,
            stderr = %stderr_tail(&out.stderr),
            "read-only fj call failed; retrying"
        );
        if !wait.is_zero() {
            crate::blocking::in_place(|| std::thread::sleep(*wait));
        }
        out = runner.run_in_env(dir, fj, args, envs)?;
        if out.ok {
            info!(
                binary = %fj,
                args = %args.join(" "),
                attempt = attempt + 1,
                "read-only fj call succeeded after a retry"
            );
        }
    }
    Ok(out)
}

/// Restate a `fj pr create` failure with the repo, the host, and the
/// permission hypothesis. The bare CLI text for a repo the token cannot see is
/// `not found: The target couldn't be found.`, which names none of the three, and
/// the push having succeeded (it authenticates with the host's git credentials,
/// not the forge token) makes a permission cause look impossible. Applied on both
/// create paths (branch and AGit); `head` is the pushed branch where there is one.
fn pr_create_error(fj: &str, repo: &str, host: &str, head: Option<&str>, e: Error) -> Error {
    let head = head
        .map(|b| format!(" (head branch `{b}`)"))
        .unwrap_or_default();
    Error::Other(anyhow::anyhow!(
        "`{fj} pr create` failed for `{repo}` on {host}{head}: {e}. The forge token may lack \
         access to `{repo}`: a private repo the token cannot see answers 404 `not found` here \
         even though the push succeeded, because the push authenticates with the host's git \
         credentials, not the forge token."
    ))
}

/// Does this `fj pr create` failure mean "an open PR for this head
/// already exists"? Forgejo answers the create with 409 and the body message
/// `pull request already exists for these targets [...]`
/// (`ErrPullRequestAlreadyExists`), and `forgejo-api` renders a 409 from
/// `RepoCreatePullRequest` as `api error: <message>`, so that text is what `fj`
/// prints. Matched on the message, not on a status code the CLI never prints.
fn is_pr_already_exists(e: &Error) -> bool {
    matches!(e, Error::CliFailed { stderr, .. } if stderr.to_lowercase().contains("already exists"))
}

/// How a lookup attempt ended, for an error message that has to name
/// both causes. `None` is "the lookup ran and found no open PR".
fn lookup_outcome(e: Option<&Error>) -> String {
    match e {
        Some(e) => format!("failed ({e})"),
        None => "succeeded and found no open PR for the branch".to_string(),
    }
}

/// A `pr create` failure where the open-PR lookup had ALSO failed.
/// [`pr_create_error`] alone reads as "PR creation failed" when the run really
/// could not check whether a PR already existed, so the lookup cause is named
/// alongside it.
fn pr_create_error_with_lookup(
    fj: &str,
    repo: &str,
    host: &str,
    branch: &str,
    create_err: Error,
    lookup_err: Option<Error>,
) -> Error {
    let base = pr_create_error(fj, repo, host, Some(branch), create_err);
    match lookup_err {
        None => base,
        Some(l) => Error::Other(anyhow::anyhow!(
            "{base} The open-PR lookup for `{repo}` had also {}, so the runner could not tell \
             whether a PR already existed before creating one; both calls failed.",
            lookup_outcome(Some(&l))
        )),
    }
}

/// Derive a PR number from a forge PR URL's trailing
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

/// Derive the `owner/repo` path (the form `list_open_prs` expects)
/// from an `origin` remote URL. Handles the HTTPS clone URLs this runner uses
/// (`https://[creds@]host/owner/repo[.git]`) and the SSH form
/// (`git@host:owner/repo[.git]`): strip a trailing `.git`, take the last two
/// `/`-separated segments, and drop any scheme/host/credentials prefix left on
/// the owner segment. Returns `None` if either segment is empty.
fn repo_path_from_remote_url(url: &str) -> Option<String> {
    let url = url.trim();
    let url = url.strip_suffix(".git").unwrap_or(url);
    let mut segments = url.rsplit('/');
    let repo = segments.next()?;
    let owner_seg = segments.next()?;
    // The owner segment may still carry an SSH `host:` or userinfo `@` prefix;
    // keep only the final component after the last `:` or `@`.
    let owner = owner_seg.rsplit([':', '@']).next()?;
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some(format!("{owner}/{repo}"))
}

/// If `s` is EXACTLY a bare `#<KEY>-N` reference and nothing else,
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

/// Does `trimmed` (already `trim_start`'d) open or close a fenced
/// code block?
fn is_fence_line(trimmed: &str) -> bool {
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

/// An ATX heading, `#` to `######` followed by a space.
fn is_atx_heading(line: &str) -> bool {
    let t = line.trim_start();
    let hashes = t.chars().take_while(|&c| c == '#').count();
    (1..=6).contains(&hashes) && t.as_bytes().get(hashes) == Some(&b' ')
}

/// A bullet (`- `, `* `, `+ `) or ordered (`1. ` style) list item,
/// optionally indented.
fn is_list_item(line: &str) -> bool {
    let t = line.trim_start();
    if t.starts_with("- ") || t.starts_with("* ") || t.starts_with("+ ") {
        return true;
    }
    let digits = t.bytes().take_while(u8::is_ascii_digit).count();
    digits > 0 && t[digits..].starts_with(". ")
}

/// A `Co-Authored-By:` / `Signed-off-by:` / `Reviewed-by:` trailer,
/// unindented (a commit trailer never is).
fn is_trailer_line(line: &str) -> bool {
    line.starts_with("Co-Authored-By:")
        || line.starts_with("Signed-off-by:")
        || line.starts_with("Reviewed-by:")
}

/// Does `line` end in an intentional Markdown hard break (two or
/// more trailing spaces, or a trailing backslash)?
fn ends_with_hard_break(line: &str) -> bool {
    line.ends_with('\\') || line.len() - line.trim_end_matches(' ').len() >= 2
}

/// Is `line` a blockquote, optionally indented?
fn is_blockquote(line: &str) -> bool {
    line.trim_start().starts_with('>')
}

/// Does `line` start a new Markdown block, so it is never joined
/// onto the previous line? Covers headings, list items, blockquotes, table
/// rows, fence lines, the `Closes` line, a bare issue reference, and a commit
/// trailer.
fn starts_new_block(line: &str) -> bool {
    let trimmed = line.trim();
    is_atx_heading(line)
        || is_list_item(line)
        || is_blockquote(line)
        || line.trim_start().starts_with('|')
        || is_fence_line(line.trim_start())
        || trimmed == "Closes"
        || bare_issue_ref(trimmed).is_some()
        || is_trailer_line(line)
}

/// Once `line` has been emitted (whether it started a new block or
/// was itself a joined continuation), can a following wrapped line join onto
/// it? A list item or blockquote wraps like prose; a heading, table row,
/// `Closes` line, bare issue reference, or trailer never takes a
/// continuation, since each is a single, complete line by construction.
fn accepts_continuation(line: &str) -> bool {
    if is_atx_heading(line)
        || line.trim_start().starts_with('|')
        || line.trim() == "Closes"
        || bare_issue_ref(line.trim()).is_some()
        || is_trailer_line(line)
    {
        return false;
    }
    !ends_with_hard_break(line)
}

/// Reflow hard-wrapped prose in a PR body so Forgejo renders it at
/// full page width instead of a ragged column at the agent's commit-wrap
/// width. Line by line: a fenced code block is copied untouched (including
/// blank lines inside it); a blank line ends the current paragraph and is
/// kept as is; a line that starts a new block (see [`starts_new_block`]) is
/// never joined onto the previous line; any other non-blank line is a wrapped
/// continuation, joined onto the previous line with a single space; a line
/// ending in a hard break (two or more trailing spaces, or a backslash) is
/// never joined to by the line after it. Infallible, introduces no new error
/// path.
fn unwrap_prose(body: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut in_fence = false;
    let mut prev_continues = false;

    for line in body.lines() {
        if in_fence {
            out.push(line.to_string());
            if is_fence_line(line.trim_start()) {
                in_fence = false;
            }
            prev_continues = false;
            continue;
        }
        if is_fence_line(line.trim_start()) {
            out.push(line.to_string());
            in_fence = true;
            prev_continues = false;
            continue;
        }
        if line.trim().is_empty() {
            out.push(line.to_string());
            prev_continues = false;
            continue;
        }
        if prev_continues && !starts_new_block(line) {
            if let Some(last) = out.last_mut() {
                last.push(' ');
                last.push_str(line.trim());
            }
        } else {
            out.push(line.to_string());
        }
        prev_continues = accepts_continuation(line);
    }

    out.join("\n")
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
/// [`CommandRunner::run_in_env`] is the one required method (it adds
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
        // Every `yt` / `fj` call in the runner lands here, most of
        // them from an async job future. `yt list` has been observed taking
        // minutes under load, so the worker's core goes back to the runtime.
        let output = match crate::blocking::in_place(|| cmd.output()) {
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
    /// [`Forge::clone_url`]. Overridable for tests and for the per-job
    /// temp credential store.
    keys_path: PathBuf,
    /// Env (`(name, value)`) injected on the `yt` child only, e.g.
    /// `YOUTRACK_CLI_CONFIG` pointing at a temp single-file config holding the
    /// decrypted token. Empty (the default) leaves the `yt` CLI on host auth.
    yt_env: Vec<(String, String)>,
    /// Env injected on the `fj` child only, e.g. an `XDG_DATA_HOME`
    /// override pointing at a temp `forgejo-cli/keys.json`. Empty by default.
    fj_env: Vec<(String, String)>,
    /// Operator-visible notices accumulated while serving the job
    /// (currently: an open-PR lookup that failed and was degraded to a create).
    /// Drained by [`Forge::take_notices`] and persisted as run events.
    notices: std::sync::Mutex<Vec<String>>,
    /// Wait schedule for a retried read-only `fj` call (CLAUDE-366). Defaults to
    /// [`DEFAULT_FJ_READ_RETRY_WAITS`]; overridable so tests run with no delay.
    fj_read_retry_waits: [Duration; 3],
    /// Wait schedule for a retried git network call (CLAUDE-396). Defaults to
    /// [`crate::provider::GIT_NETWORK_RETRY_WAITS`]; overridable so tests run
    /// with no delay.
    git_retry_waits: [Duration; 3],
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

/// `yt auth verify --json`, whose `user` object carries the
/// authenticated login. Only the login is consumed; no token is present in the
/// payload.
#[derive(Debug, Deserialize)]
struct AuthVerify {
    user: AuthUser,
}

#[derive(Debug, Deserialize)]
struct AuthUser {
    #[serde(default)]
    login: String,
}

/// `yt project show <KEY> --json`. Only the display name is read.
#[derive(Debug, Deserialize)]
struct ProjectShow {
    #[serde(default)]
    name: String,
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

/// The `links` array of `yt issue inspect --json` (YT-40). Only the
/// links are deserialized; the surrounding `issue` / `comments` / etc. objects
/// are ignored.
#[derive(Debug, Deserialize)]
struct InspectLinks {
    #[serde(default)]
    links: Vec<RawLink>,
}

/// The `issue` + `comments` of `yt issue inspect --json`. Only the
/// description and each comment's text are consumed; `links` and the rest are
/// ignored here (parsed separately by [`InspectLinks`]).
#[derive(Debug, Deserialize)]
struct InspectBody {
    #[serde(default)]
    issue: InspectIssue,
    #[serde(default)]
    comments: Vec<InspectComment>,
}

/// The `issue` object of the inspect JSON. Only the description is read.
#[derive(Debug, Default, Deserialize)]
struct InspectIssue {
    #[serde(default)]
    description: String,
}

/// One `comments[]` entry. Forgejo/YouTrack name the body `text`; a couple of
/// fallbacks keep the parse tolerant across `yt` versions.
#[derive(Debug, Deserialize)]
struct InspectComment {
    #[serde(default, alias = "content", alias = "body")]
    text: String,
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
        #[cfg(not(test))]
        let keys_path = dirs::data_dir()
            .unwrap_or_else(|| {
                warn!("could not resolve the platform data dir; falling back to `.`");
                PathBuf::from(".")
            })
            .join("forgejo-cli")
            .join("keys.json");
        // Tests never read the host's real key store; one that did cloned a live repo.
        #[cfg(test)]
        let keys_path = PathBuf::from("/nonexistent/forgejo-cli/keys.json");
        Self {
            runner,
            yt: "yt".to_string(),
            fj: "fj".to_string(),
            keys_path,
            yt_env: Vec::new(),
            fj_env: Vec::new(),
            notices: std::sync::Mutex::new(Vec::new()),
            fj_read_retry_waits: DEFAULT_FJ_READ_RETRY_WAITS,
            git_retry_waits: crate::provider::GIT_NETWORK_RETRY_WAITS,
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

    /// Inject env on the `yt` child only (decrypted-credential
    /// `YOUTRACK_CLI_CONFIG`). Empty leaves host auth.
    pub fn with_yt_env(mut self, env: Vec<(String, String)>) -> Self {
        self.yt_env = env;
        self
    }

    /// Inject env on the `fj` child only (an `XDG_DATA_HOME` override
    /// pointing at a temp `keys.json`). Empty leaves host auth.
    pub fn with_fj_env(mut self, env: Vec<(String, String)>) -> Self {
        self.fj_env = env;
        self
    }

    /// Override the read-retry wait schedule (tests set this to all-zero so a
    /// retried read runs with no delay).
    pub fn with_fj_read_retry_waits(mut self, waits: [Duration; 3]) -> Self {
        self.fj_read_retry_waits = waits;
        self
    }

    /// Override the git network retry wait schedule (CLAUDE-396; tests set
    /// this to all-zero so a retried git call runs with no delay).
    pub fn with_git_retry_waits(mut self, waits: [Duration; 3]) -> Self {
        self.git_retry_waits = waits;
        self
    }

    /// Run a read-only `fj` invocation, retried up to four attempts total
    /// (CLAUDE-366). Never used for a write: a write must fail on its first and
    /// only attempt.
    fn run_fj_read(
        &self,
        dir: Option<&Path>,
        args: &[&str],
        envs: &[(&str, &str)],
    ) -> Result<CmdOutput> {
        run_fj_read_retrying(
            self.runner.as_ref(),
            &self.fj,
            dir,
            args,
            envs,
            &self.fj_read_retry_waits,
        )
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

    /// Parse `fj pr create --json` stdout into a [`PullRequest`]. The human
    /// `text` line carries only the PR URL (`.../pulls/<n>`) and no
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
            // The caller (branch-backed create, or AGit) fills in
            // `head_branch` when it has one; AGit has none.
            head_branch: None,
        })
    }

    /// Resolve the feature branch by NAME, independent of the
    /// current `HEAD`. The base (default) branch is whatever `origin/HEAD` points
    /// at (e.g. `origin/main`); the feature branch is the single LOCAL branch
    /// (other than the base) that has commits ahead of the base. This is also the
    /// defense-in-depth guardrail (proposed-approach step 6): a branch is a
    /// candidate ONLY if it has a non-empty `rev-list <base>..<branch>`, so the
    /// "no commits ahead" / no-diff case falls into the zero-candidate error
    /// below rather than delegating to `fj`'s opaque "no changes between head and
    /// base". The workdir is named in the errors so the branch stays recoverable
    /// (the failure comment in `src/native/youtrack.rs` preserves it).
    ///
    /// Called from [`Forge::create_pr`] BEFORE the strategy dispatch,
    /// so the ahead check guards AGit too. AGit discards the resolved name (`fj
    /// pr create -aA` pushes `HEAD`), it only needs the zero/multiple-candidate
    /// rejection.
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
                // State the condition, not an outcome
                // the later `cleanup_or_preserve` decision can contradict: it
                // is the one that checks the workdir and decides whether
                // anything is preserved.
                "no feature branch with commits ahead of `{base_ref}`; the agent \
                 produced no branch or no commits (base `{base_branch}`). Any \
                 unpushed work in the workdir `{}` is preserved only if the \
                 cleanup check finds it there; branches already pushed remain on \
                 the remote regardless.",
                workdir.display()
            ))),
            _ => Err(Error::Other(anyhow::anyhow!(
                "multiple candidate feature branches ahead of `{base_ref}`: {}; \
                 refusing to guess which to open a PR for. Any unpushed work in \
                 the workdir `{}` is preserved only if the cleanup check finds it \
                 there; branches already pushed remain on the remote regardless.",
                candidates.join(", "),
                workdir.display()
            ))),
        }
    }

    /// Branch-backed PR creation (the default strategy). Push the working branch
    /// with the git CLI (same-name remote
    /// branch), then open the PR over the Forgejo API with `fj pr create`,
    /// deriving the title and body from the head commit exactly like AGit
    /// autofill would (the body carries the bare `#<KEY>-N` reference, so
    /// YouTrack still links the PR). The body travels via a file (fj convention
    /// for long bodies), written into the clone's git-directory scratch space
    /// rather than the workdir, so it can never be staged or
    /// committed; its removal failure is logged rather than swallowed. The head
    /// is `refs/heads/<branch>`, never `refs/pull/<N>/head`.
    ///
    /// The feature branch is resolved authoritatively by NAME, never
    /// from the ambient `HEAD`. Everything (push, title/body, `fj --head`) binds
    /// to that resolved branch, so a stray `git checkout main`, a detached
    /// `HEAD`, or an agent bug can no longer strand the work and open a silent
    /// `main`-vs-`main` PR. The push, title, and body all target the resolved
    /// branch; `fj pr create` is invoked with an explicit `--head`, mirroring the
    /// GitHub backend (`github.rs`). `branch` is resolved by the
    /// caller ([`Forge::create_pr`]) so both strategies share the ahead check.
    ///
    /// Idempotent against a pre-existing `origin/<branch>`. The
    /// feature-branch name is chosen by the agent and not deterministic per issue
    /// key; a pre-existing remote branch of the same name is legitimate, not just
    /// a collision, because the resume path for a re-queued issue clones its
    /// recorded PR branch by name and continues committing on it. Before pushing,
    /// the remote state of the branch is read (`git ls-remote`) and the remote
    /// branch is fetched (CLAUDE-382: a resumed run's clone predates any commit
    /// the runner pushed onto the branch while the run was working, and without
    /// the fetch `is_ancestor` fails with exit 128 rather than resolving
    /// ancestry), then reconciled WITHOUT force: absent -> push + create; local
    /// fast-forwards or equals remote -> (fast-forward) push then find-or-create
    /// the PR; diverged (the remote gained commits, typically the runner's own
    /// "behind its base" update, while the local branch has the agent's own) ->
    /// merge `origin/<branch>` into the local branch, then push (still a fast-
    /// forward, since the merge now carries the remote tip as an ancestor) and
    /// find-or-create the PR. No `--force` / `--force-with-lease` is ever
    /// issued; a merge conflict aborts the merge and fails loudly naming both
    /// SHAs and the workdir.
    ///
    /// The error text states the retention CONDITION
    /// ("preserved only if the cleanup check finds it there"), never a flat
    /// outcome, because `cleanup_or_preserve` makes the keep-or-delete
    /// decision later, from its own check of the workdir, and can legitimately
    /// delete the directory.
    fn create_pr_branch(
        &self,
        workdir: &Path,
        host: &str,
        branch: &str,
        draft: bool,
    ) -> Result<PullRequest> {
        // Repo path (`owner/repo`) for the open-PR lookup, derived from the
        // workdir's `origin` remote (the form `list_open_prs` expects).
        let repo = self.origin_repo_path(workdir)?;

        // The token-bearing URL for this host/repo, resolved once up front
        // (CLAUDE-395): `origin` itself carries no credential after the
        // post-clone scrub, so every network call below authenticates
        // per invocation through this instead. `None` when no credential
        // resolves; callers fall back to an unauthenticated invocation (any
        // host git credential helper still applies), matching the existing
        // `clone_url` failure behavior.
        let token_url = self.clone_url(host, &repo).ok();
        let token_url = token_url.as_deref();

        // Local tip of the resolved branch, and the remote branch's tip (if the
        // branch already exists on `origin`), read BEFORE any push.
        let local_sha = self.rev_parse(workdir, branch)?;
        let remote_sha = self.remote_branch_sha(workdir, branch, token_url)?;

        match remote_sha {
            // Absent: unchanged happy path (push `<branch>:<branch>`, create the
            // PR with `--head <branch>`).
            None => {
                self.push_branch(workdir, branch, token_url)?;
                self.find_or_create_pr(workdir, host, &repo, branch, draft)
            }
            // Equal to local: nothing to push; find-or-create the PR.
            Some(ref remote_sha) if *remote_sha == local_sha => {
                self.find_or_create_pr(workdir, host, &repo, branch, draft)
            }
            // Present and not equal. Fetch first (CLAUDE-382) so the remote
            // tip's objects exist locally: a resumed run's clone predates any
            // commit the runner pushed onto the branch while the run was
            // working, and without this `is_ancestor` fails with exit 128
            // ("not a valid commit name") rather than resolving ancestry.
            Some(remote_sha) => {
                self.fetch_branch(workdir, branch, token_url)?;
                if self.is_ancestor(workdir, &remote_sha, &local_sha)? {
                    // A fast-forward push is legal when the remote tip is an
                    // ancestor of the local tip.
                    self.push_branch(workdir, branch, token_url)?;
                    self.find_or_create_pr(workdir, host, &repo, branch, draft)
                } else {
                    // Diverged: the remote gained commits (typically the
                    // runner's own "branch was behind its base" update) while
                    // the local branch has the agent's own. Merge the remote
                    // in (never force) so the push below stays a fast-forward,
                    // then find-or-create the PR as usual.
                    self.merge_remote_branch(workdir, branch, &local_sha, &remote_sha)?;
                    self.push_branch(workdir, branch, token_url)?;
                    self.find_or_create_pr(workdir, host, &repo, branch, draft)
                }
            }
        }
    }

    /// The `owner/repo` path of the workdir's `origin` remote, for
    /// the open-PR lookup (`list_open_prs`). Errors loudly if the remote URL is
    /// unparseable so the caller never silently skips the idempotency reconcile.
    fn origin_repo_path(&self, workdir: &Path) -> Result<String> {
        let url_args = ["remote", "get-url", "origin"];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &url_args, &[])?;
        let url = Self::checked("git", out, &url_args)?;
        repo_path_from_remote_url(url.trim()).ok_or_else(|| {
            Error::Other(anyhow::anyhow!(
                "could not parse an `owner/repo` path from the origin remote URL `{}`",
                url.trim()
            ))
        })
    }

    /// The commit SHA a git revision (e.g. a branch name) resolves to
    /// in the workdir.
    fn rev_parse(&self, workdir: &Path, rev: &str) -> Result<String> {
        let args = ["rev-parse", rev];
        let out = self.runner.run_in_env(Some(workdir), "git", &args, &[])?;
        Ok(Self::checked("git", out, &args)?.trim().to_string())
    }

    /// The run's scratch directory INSIDE the clone's own git
    /// directory (`git rev-parse --absolute-git-dir`, never a hard-coded
    /// `.git`, which is a FILE rather than a directory in a worktree or
    /// submodule checkout), joined with `claude-run` and created if missing.
    /// Git never tracks, stages, or commits anything under the git directory,
    /// so a scratch file written here can never reach a commit.
    fn git_dir_scratch(&self, workdir: &Path) -> Result<PathBuf> {
        let args = ["rev-parse", "--absolute-git-dir"];
        let out = self.runner.run_in_env(Some(workdir), "git", &args, &[])?;
        let git_dir = Self::checked("git", out, &args)?.trim().to_string();
        let dir = PathBuf::from(git_dir).join("claude-run");
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    /// The SHA of `origin/<branch>` if the branch exists on the
    /// remote, else `None`. `git ls-remote --heads origin <branch>` prints one
    /// `"<sha>\t<ref>"` line per matching head and nothing (exit 0) when the
    /// branch is absent, so no branch existence is inferred from an exit code.
    ///
    /// Authenticated per invocation via `token_url` (CLAUDE-395): `origin`
    /// itself carries no credential after the post-clone scrub, so this reads
    /// the remote through the inline credential helper rather than relying on
    /// a token baked into the remote URL. `None` runs unauthenticated (any
    /// host git credential helper still applies).
    fn remote_branch_sha(
        &self,
        workdir: &Path,
        branch: &str,
        token_url: Option<&str>,
    ) -> Result<Option<String>> {
        let args = ["ls-remote", "--heads", "origin", branch];
        let secrets = token_url_secrets(token_url);
        let out = crate::provider::retry_git_network(
            "ls-remote",
            &secrets,
            &self.git_retry_waits,
            || {
                crate::provider::run_authed_git(
                    self.runner.as_ref(),
                    Some(workdir),
                    &args,
                    token_url,
                )
            },
        )?;
        let stdout = Self::checked("git", out, &args)?;
        Ok(stdout
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().next())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()))
    }

    /// `true` iff `ancestor` is an ancestor of `descendant` (i.e. a
    /// fast-forward from `ancestor` to `descendant` is possible).
    /// `git merge-base --is-ancestor` signals the answer via exit status (0 =
    /// yes, 1 = no), so a bare `checked` would misread the legitimate "no" as an
    /// error; the exit code is inspected directly and any OTHER code propagates.
    fn is_ancestor(&self, workdir: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
        let args = ["merge-base", "--is-ancestor", ancestor, descendant];
        let out = self.runner.run_in_env(Some(workdir), "git", &args, &[])?;
        match out.exit_code {
            0 => Ok(true),
            1 => Ok(false),
            // Any other exit is a real failure (bad object, not a repo, ...).
            _ => Self::checked("git", out, &args).map(|_| unreachable!()),
        }
    }

    /// Fetch `branch` from `origin` (CLAUDE-382), read-only and
    /// authenticated the same way as every other git subprocess here
    /// (`self.runner.run_in_env`, CLAUDE-252). Called before `is_ancestor`
    /// touches the remote tip, so the remote's objects exist in the local
    /// clone: a resumed run's clone predates any commit the runner pushed
    /// onto the branch while the run was working, and without this fetch
    /// `git merge-base` fails with exit 128 ("not a valid commit name")
    /// rather than resolving ancestry. Authenticated per invocation via
    /// `token_url` (CLAUDE-395), same as [`Self::remote_branch_sha`].
    fn fetch_branch(&self, workdir: &Path, branch: &str, token_url: Option<&str>) -> Result<()> {
        let args = ["fetch", "origin", branch];
        let secrets = token_url_secrets(token_url);
        let out =
            crate::provider::retry_git_network("fetch", &secrets, &self.git_retry_waits, || {
                crate::provider::run_authed_git(
                    self.runner.as_ref(),
                    Some(workdir),
                    &args,
                    token_url,
                )
            })?;
        Self::checked("git", out, &args)?;
        Ok(())
    }

    /// Merge `origin/<branch>` into the local branch (CLAUDE-382),
    /// called only once `is_ancestor` has ruled out a plain fast-forward: the
    /// remote gained commits (typically the runner's own "branch was behind
    /// its base" update) while the local branch carries the agent's own, so
    /// neither is an ancestor of the other. A clean merge leaves the local
    /// branch with the remote tip as an ancestor, so the caller's subsequent
    /// push stays a fast-forward; `--force` is never used anywhere in this
    /// path. A conflicting merge is aborted immediately (never left for the
    /// caller to push over) and reported loudly, naming both SHAs and the
    /// workdir, exactly as this arm did before it attempted a merge at all.
    fn merge_remote_branch(
        &self,
        workdir: &Path,
        branch: &str,
        local_sha: &str,
        remote_sha: &str,
    ) -> Result<()> {
        let remote_ref = format!("origin/{branch}");
        let args = ["merge", "--no-edit", remote_ref.as_str()];
        let out = self.runner.run_in_env(Some(workdir), "git", &args, &[])?;
        if out.ok {
            return Ok(());
        }
        let merge_stderr = out.stderr.trim().to_string();
        error!(
            branch = %branch,
            local_sha = %local_sha,
            remote_sha = %remote_sha,
            workdir = %workdir.display(),
            stderr = %merge_stderr,
            "merging the remote branch into the local work conflicted; aborting the merge"
        );
        let abort_args = ["merge", "--abort"];
        let abort_out = self
            .runner
            .run_in_env(Some(workdir), "git", &abort_args, &[])?;
        Self::checked("git", abort_out, &abort_args)?;
        Err(Error::Other(anyhow::anyhow!(
            "remote branch `{branch}` has diverged from the local work (local {local_sha}, \
             remote {remote_sha}) and merging origin/{branch} into it conflicted ({merge_stderr}); \
             the merge was aborted. Refusing to force-push over it. Any unpushed work in the \
             workdir `{}` is preserved only if the cleanup check finds it there; branches \
             already pushed remain on the remote regardless.",
            workdir.display()
        )))
    }

    /// Push the resolved branch BY NAME (`<branch>:<branch>`), not
    /// `HEAD`, so the server branch carries the feature name regardless of the
    /// current checkout. Never forced: a rejected non-fast-forward push is a
    /// real-divergence signal the caller has already ruled out before calling.
    fn push_branch(&self, workdir: &Path, branch: &str, token_url: Option<&str>) -> Result<()> {
        let refspec = format!("{branch}:{branch}");
        let push_args = ["push", "--set-upstream", "origin", refspec.as_str()];
        let secrets = token_url_secrets(token_url);
        let out =
            crate::provider::retry_git_network("push", &secrets, &self.git_retry_waits, || {
                crate::provider::run_authed_git(
                    self.runner.as_ref(),
                    Some(workdir),
                    &push_args,
                    token_url,
                )
            })?;
        Self::checked("git", out, &push_args)?;
        Ok(())
    }

    /// An already-open PR whose head branch is `branch`, matched by
    /// `head_ref` against `list_open_prs` (`fj pr search --repo <repo> --state
    /// open --json`). Returns the first match (a repo has at most one open PR per
    /// head branch) as an adoptable [`PullRequest`], else `None`.
    fn find_open_pr(&self, host: &str, repo: &str, branch: &str) -> Result<Option<PullRequest>> {
        let prs = self.list_open_prs(host, repo)?;
        Ok(prs
            .into_iter()
            .find(|p| p.head_ref == branch)
            .map(|p| PullRequest {
                number: Some(p.number),
                url: p.html_url,
                head_branch: Some(branch.to_string()),
            }))
    }

    /// Adopt an already-open PR for `branch` if one exists, else
    /// create it. The title/body derivation from the branch tip and the
    /// `fj pr create` call run ONLY on the create path, so adopting an existing
    /// PR issues no `pr create`.
    ///
    /// A FAILED lookup is not fatal here. The lookup is an
    /// optimization (`fj pr search` is an issue-category call, `pr create` a
    /// repository-category one, so they fail independently), and the create
    /// below is what delivers the work; a failure degrades to "unknown, assume
    /// none" with a warn plus an operator-visible notice. The degraded path can
    /// race a PR that already exists, so a create rejected as already-existing
    /// retries the lookup exactly once and adopts what it finds.
    fn find_or_create_pr(
        &self,
        workdir: &Path,
        host: &str,
        repo: &str,
        branch: &str,
        draft: bool,
    ) -> Result<PullRequest> {
        let lookup_err = match self.find_open_pr(host, repo, branch) {
            Ok(Some(pr)) => return Ok(pr),
            Ok(None) => None,
            Err(e) => {
                self.note_degraded_lookup(host, repo, branch, &e);
                Some(e)
            }
        };

        // Title/body derive from the feature branch tip, not `HEAD`. A draft
        // open (CLAUDE-379, timeout salvage) is forced to carry fj's `WIP: `
        // title-prefix convention regardless of the tip commit's own subject,
        // so the draft status holds even when the tip is a pre-existing agent
        // commit rather than the salvage's own `WIP: ...` commit.
        let title_args = ["log", "-1", "--format=%s", branch];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &title_args, &[])?;
        let mut title = Self::checked("git", out, &title_args)?.trim().to_string();
        if draft && !title.starts_with("WIP: ") {
            title = format!("WIP: {title}");
        }

        let body_args = ["log", "-1", "--format=%b", branch];
        let out = self
            .runner
            .run_in_env(Some(workdir), "git", &body_args, &[])?;
        let body = unwrap_prose(Self::checked("git", out, &body_args)?.trim());

        const BODY_FILE: &str = ".claude-run-pr-body.md";
        // Written into the clone's git-directory scratch space,
        // never its work tree, so it can never be staged or committed; an
        // absolute path is passed to `--body-file` since it no longer sits
        // under `workdir`, the `fj` child's own cwd.
        let scratch = self.git_dir_scratch(workdir)?;
        let body_path = scratch.join(BODY_FILE);
        std::fs::write(&body_path, &body)?;
        let body_path_arg = body_path.to_string_lossy().to_string();
        let create_args = [
            "--host",
            host,
            "pr",
            "create",
            title.as_str(),
            "--body-file",
            body_path_arg.as_str(),
            "--head",
            branch,
            "--json",
        ];
        let res = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &create_args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &create_args));
        if let Err(e) = std::fs::remove_file(&body_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::error!(
                    path = %body_path.display(),
                    error = %e,
                    "removing the PR body file failed"
                );
            }
        }
        // The branch-backed create just pushed this exact server
        // branch (`self.push_branch` / the fast-forward arm above), so it is
        // always the resulting PR's head, whatever `fj pr create --json` itself
        // reports.
        let stdout = match res {
            Ok(stdout) => stdout,
            // The forge says a PR for this head already exists (a
            // concurrent run, or one the degraded lookup could not see). Retry
            // the lookup exactly once and adopt what it returns.
            Err(e) if is_pr_already_exists(&e) => {
                let retry = self.find_open_pr(host, repo, branch);
                let retry_err = match retry {
                    Ok(Some(pr)) => return Ok(pr),
                    Ok(None) => None,
                    Err(re) => Some(re),
                };
                return Err(Error::Other(anyhow::anyhow!(
                    "`{} pr create` for `{repo}` on {host} (head branch `{branch}`) reports that \
                     a pull request for that head already exists ({e}), but the open-PR lookup \
                     cannot return it, so there is nothing to adopt: the first lookup {}, the \
                     retry {}. The branch is already pushed; fix the lookup (a token missing \
                     `read:issue` 403s `fj pr search` while `pr create` still works) and re-run.",
                    self.fj,
                    lookup_outcome(lookup_err.as_ref()),
                    lookup_outcome(retry_err.as_ref()),
                )));
            }
            Err(e) => {
                return Err(pr_create_error_with_lookup(
                    &self.fj, repo, host, branch, e, lookup_err,
                ));
            }
        };
        self.created_pr(&stdout).map(|mut pr| {
            pr.head_branch = Some(branch.to_string());
            pr
        })
    }

    /// Record a lookup failure that was degraded to a create, in the
    /// daemon journal and as a notice the run surfaces on the action page.
    fn note_degraded_lookup(&self, host: &str, repo: &str, branch: &str, e: &Error) {
        warn!(
            repo = %repo,
            branch = %branch,
            error = %e,
            "open-PR lookup failed; assuming no open PR and creating one"
        );
        self.notices.lock().unwrap().push(format!(
            "open-PR lookup for `{repo}` on {host} failed ({e}); assumed no open PR exists for \
             `{branch}` and created one. `fj pr search` is an issue-category call while \
             `pr create` is repository-category, so a token missing `read:issue` fails only the \
             lookup (actions/SCHEMA.md, \"Forgejo token scopes\")."
        ));
    }

    /// Opt-in AGit + autofill (`fj --host <host> pr create -aA`).
    /// fj pushes the local commits to `refs/for/<base>` and opens a branchless
    /// PR (head at `refs/pull/<N>/head`), deriving the title/body from the
    /// commit messages. No server branch is produced. A failure errors loudly;
    /// there is no silent switch to the branch path.
    ///
    /// Reached only after [`Forge::create_pr`] resolved exactly one
    /// branch ahead of base. Without that guard a run with no commits still
    /// succeeded here (`-aA` pushes the base tip and autofills from an unrelated
    /// commit), and the issue was moved to `AI Agent = Code Review`.
    fn create_pr_agit(&self, workdir: &Path, host: &str) -> Result<PullRequest> {
        let args = ["--host", host, "pr", "create", "-aA", "--json"];
        let stdout = self
            .runner
            .run_in_env(Some(workdir), &self.fj, &args, &self.fj_envs())
            .and_then(|out| Self::checked(&self.fj, out, &args))
            // Same restatement as the branch path; the repo is read
            // from `origin` only on this failure path.
            .map_err(|e| {
                let repo = self
                    .origin_repo_path(workdir)
                    .unwrap_or_else(|_| "<unparseable origin remote>".to_string());
                pr_create_error(&self.fj, &repo, host, None, e)
            })?;
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

    /// The value of the injected `YOUTRACK_CLI_CONFIG`, if any. The
    /// PATH only is returned, never the file contents (which hold the token).
    /// `None` means host-auth fallback (no injected config).
    fn yt_config_path(&self) -> Option<String> {
        if let Some((_, v)) = self
            .yt_env
            .iter()
            .find(|(k, _)| k == crate::service_auth::YT_CONFIG_ENV)
        {
            return Some(v.clone());
        }
        // No per-job credential: the child still inherits the daemon's own
        // `YOUTRACK_CLI_CONFIG` (Settings -> Auth). Reading it here keeps the
        // failure diagnostics honest; before, a run using the daemon credential
        // logged `youtrack_cli_config_present: false` and `<host-auth or
        // unknown>`, which pointed at the wrong cause.
        std::env::var(crate::service_auth::YT_CONFIG_ENV)
            .ok()
            .filter(|v| !v.is_empty())
    }

    /// The YouTrack base URL the injected config targets, read from
    /// the single-file `yt` config's `url:` line (see
    /// [`crate::secrets::JobCredentials::materialize`], which writes
    /// `url: <url>\ntoken: <token>\n`). The `token:` line is never read or
    /// returned. Returns `None` on host-auth fallback (no injected config) or if
    /// the file is unreadable / has no `url:` line, so the base URL can be logged
    /// in a diagnostic without ever exposing the secret.
    fn yt_base_url(&self) -> Option<String> {
        let path = self.yt_config_path()?;
        let contents = std::fs::read_to_string(&path).ok()?;
        contents
            .lines()
            .find_map(|l| l.trim().strip_prefix("url:"))
            .map(|v| v.trim().to_string())
    }

    /// Run `yt args...`, returning trimmed stdout on success or
    /// [`Error::CliFailed`] on a non-zero exit.
    ///
    /// Logs the non-secret execution context at `debug` for every
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
            // Name the likeliest cause when the configured credential file is
            // simply absent: that is an unconfigured instance, not a broken
            // token, and the fix is one page away.
            let hint = match self.yt_config_path() {
                Some(p) if !std::path::Path::new(&p).exists() => {
                    " (no YouTrack credential yet: Settings -> Auth)"
                }
                _ => "",
            };
            warn!(
                binary = %self.yt,
                args = %args.join(" "),
                exit_code = out.exit_code,
                youtrack_cli_config_present = config_present,
                base_url = %self.yt_base_url().as_deref().unwrap_or("<host-auth or unknown>"),
                "yt invocation failed{hint}"
            );
        }
        Self::checked(&self.yt, out, args)
    }

    /// One-shot identity probe run when a VCS probe (`yt project vcs
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
        // for the user-type Assignee field; `login` may be a login,
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

    fn inspect_issue(&self, key: &str) -> Result<super::IssueInspect> {
        let stdout = self.yt_ok(&["issue", "inspect", key, "--json"])?;
        let parsed: InspectBody = Self::parse_json(&self.yt, "yt issue inspect --json", &stdout)?;
        Ok(super::IssueInspect {
            description: parsed.issue.description,
            comments: parsed.comments.into_iter().map(|c| c.text).collect(),
        })
    }

    fn auth_verify(&self) -> Result<String> {
        let stdout = self.yt_ok(&["auth", "verify", "--json"])?;
        let parsed: AuthVerify = Self::parse_json(&self.yt, "yt auth verify --json", &stdout)?;
        Ok(parsed.user.login)
    }

    fn list_query_top(&self, query: &str, top: u32) -> Result<Vec<serde_json::Value>> {
        let query_arg = format!("--query={query}");
        let top_arg = top.to_string();
        let stdout = self.yt_ok(&["list", &query_arg, "--top", &top_arg, "--json"])?;
        Self::parse_json(&self.yt, "yt list --json", &stdout)
    }

    fn project_show(&self, project_key: &str) -> Result<String> {
        let stdout = self.yt_ok(&["project", "show", project_key, "--json"])?;
        let parsed: ProjectShow = Self::parse_json(&self.yt, "yt project show --json", &stdout)?;
        Ok(parsed.name)
    }

    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo> {
        // `vcs` is a command group; `list` is the subcommand that takes the
        // project short-name positional. Omitting it makes the project land in
        // the `<COMMAND>` slot and `yt` exits 2.
        // On a probe tool failure (e.g. `project 'VS' not found`,
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

    fn link_issue(&self, key: &str, link_type: &str, target: &str) -> Result<()> {
        // `yt issue link <source> <target> --type <link-type>`; the type is a
        // flag with a `relates` default, so it is always passed explicitly.
        self.yt_ok(&["issue", "link", key, target, "--type", link_type])?;
        Ok(())
    }

    fn add_tag(&self, key: &str, tag: &str) -> Result<()> {
        // `--create` so a Needs-Info tag not yet present on the instance is
        // created rather than failing the write.
        self.yt_ok(&["issue", "tag", "add", key, tag, "--create"])?;
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
        // The ahead check runs BEFORE the dispatch, so
        // both strategies reject a run that produced no commits (zero
        // candidates) or an ambiguous workdir (multiple candidates) with the
        // same loud, workdir-naming error, before `fj` is invoked.
        let branch = self.resolve_feature_branch(workdir)?;
        match strategy {
            // Default: push a real server branch with the git CLI
            // (proven https-capable, it performed the clone) and open the PR
            // over the API. The resulting head is `refs/heads/<branch>`, so
            // Forgejo can always "Update Branch" and the PR stays mergeable.
            // The selected strategy is authoritative: a failure errors loudly
            // (the finalize step preserves the workdir) rather than silently
            // emitting a branchless AGit PR.
            PrStrategy::Branch => self.create_pr_branch(workdir, host, &branch, false),
            // Opt-in AGit + autofill: fj derives the title/body from the local
            // commit messages, which already carry the bare #<KEY>-N reference.
            // No server branch (head lives only at `refs/pull/<N>/head`).
            PrStrategy::Agit => self.create_pr_agit(workdir, host),
        }
    }

    /// Draft support (CLAUDE-379) is implemented only for the branch-backed
    /// strategy, which is the one the timeout salvage path uses (AGit produces
    /// no server branch, so it cannot be resumed and is never used there);
    /// `PrStrategy::Agit` falls back to the plain create, unprefixed.
    fn create_pr_with_draft(
        &self,
        workdir: &Path,
        host: &str,
        strategy: crate::action::PrStrategy,
        draft: bool,
    ) -> Result<PullRequest> {
        use crate::action::PrStrategy;
        let branch = self.resolve_feature_branch(workdir)?;
        match strategy {
            PrStrategy::Branch => self.create_pr_branch(workdir, host, &branch, draft),
            PrStrategy::Agit => self.create_pr_agit(workdir, host),
        }
    }

    /// Strip the `WIP: ` title prefix a timeout-salvaged draft PR (CLAUDE-379)
    /// carries, when a re-queued run resumes onto it and now succeeds. A PR
    /// whose title already lacks the prefix (not a salvaged draft, or already
    /// stripped) is left untouched.
    fn mark_pr_ready(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        let prs = self.list_open_prs(host, repo)?;
        let title = prs
            .into_iter()
            .find(|p| p.number == pr_number)
            .map(|p| p.title)
            .ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "PR #{pr_number} in `{repo}` on {host} is not open; cannot read its title \
                     to strip the draft prefix"
                ))
            })?;
        let Some(stripped) = title.strip_prefix("WIP: ") else {
            return Ok(());
        };
        let id = format!("{repo}#{pr_number}");
        let args = ["--host", host, "pr", "edit", &id, "--title", stripped];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        Self::checked(&self.fj, out, &args)?;
        Ok(())
    }

    fn take_notices(&self) -> Vec<String> {
        std::mem::take(&mut *self.notices.lock().unwrap())
    }

    fn note(&self, message: String) {
        self.notices.lock().unwrap().push(message);
    }

    /// Two probes, one per scope category the PR path
    /// crosses (`actions/SCHEMA.md`, "Forgejo token scopes").
    ///
    /// 1. `fj --host <host> repo view <owner/repo> --json` (repository read). A
    ///    readable repo answers with the repo object; one the token cannot see
    ///    answers 404 `not found`, the same way `fj pr create` does, so this probe
    ///    carries the signal `pr search` lacks (an invisible repo and a repo with
    ///    no open PRs both search as an empty array, exit 0).
    /// 2. `fj --host <host> pr search --repo <repo> --state open --json` (issue
    ///    read), the exact call [`Forge::list_open_prs`] makes on the PR path. It
    ///    is what a repository-only token 403s on after the run is paid for, so
    ///    the preflight has to exercise it. An empty result is a pass.
    fn repo_access(&self, host: &str, repo: &str) -> Result<()> {
        let envs = self.fj_envs();
        let args = ["--host", host, "repo", "view", repo, "--json"];
        let out = self.run_fj_read(None, &args, &envs)?;
        if !out.ok {
            return Err(super::probe_denied(
                "forge token",
                &self.fj,
                "repo view",
                "read:repository",
                host,
                repo,
                &out,
            ));
        }
        let args = [
            "--host", host, "pr", "search", "--repo", repo, "--state", "open", "--json",
        ];
        let out = self.run_fj_read(None, &args, &envs)?;
        if !out.ok {
            return Err(super::probe_denied(
                "forge token",
                &self.fj,
                "pr search",
                "read:issue",
                host,
                repo,
                &out,
            ));
        }
        Ok(())
    }

    /// `fj --host <host> whoami`. `fj` wraps each name in Unicode
    /// bidi isolates (U+2066..U+2069), stripped so the row renders as plain text.
    fn identity(&self, host: &str) -> Result<String> {
        let args = ["--host", host, "whoami"];
        let out = self.run_fj_read(None, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        Ok(stdout
            .chars()
            .filter(|c| !matches!(c, '\u{2066}'..='\u{2069}'))
            .collect::<String>()
            .trim()
            .to_string())
    }

    fn list_open_prs(&self, host: &str, repo: &str) -> Result<Vec<super::ForgePullRequest>> {
        let args = [
            "--host", host, "pr", "search", "--repo", repo, "--state", "open", "--json",
        ];
        let out = self.run_fj_read(None, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        // `fj` 0.17.1 wraps the results in `{"total_count": N, "pull_requests":
        // [...]}` rather than emitting the bare array older versions did.
        let response: PrSearchResponse =
            Self::parse_json(&self.fj, "fj pr search --json", &stdout)?;
        Ok(response.pull_requests)
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
        let out = self.run_fj_read(None, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        #[derive(Deserialize)]
        struct Status {
            state: String,
            // Absent or `null` (a merged PR, or no verdict yet) is unknown and
            // never merges: only `Some(true)` does.
            #[serde(default)]
            mergeable: Option<bool>,
            // Absent (an `fj` predating this field) or explicit `null` both parse to
            // `None`: unknown, never collapsed to `false`.
            #[serde(default)]
            behind_base: Option<bool>,
            // Absent (an `fj` predating this field) or explicit `null` (a merged
            // PR) both parse to `None`.
            #[serde(default)]
            head_sha: Option<String>,
        }
        let status: Status = Self::parse_json(&self.fj, "fj pr status --json", &stdout)?;
        Ok(super::PrStatus {
            state: status.state,
            mergeable: status.mergeable,
            behind_base: status.behind_base,
            head_sha: status.head_sha,
        })
    }

    fn merge_pr(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        // Merge-commit (`--method merge`), never squash/rebase: every PR from
        // the native action is a single commit carrying the bare `#<KEY>`
        // reference, and only a merge-commit preserves it in history.
        let id = format!("{repo}#{pr_number}");
        // `--delete` removes the PR head branch after merge, matching the
        // Forgejo web UI's "Delete branch after merge" behavior so automerged
        // PRs don't leave stale merged branches behind. It is a
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

    fn update_pr_branch(&self, host: &str, repo: &str, pr_number: u64) -> Result<()> {
        // Forgejo's Update-branch endpoint. `--update-style
        // merge` matches the merge-commit policy above and never rewrites
        // the agent's branch. Renamed from `--style` in a later Forgejo
        // release, which collided with the global output-styling flag of the
        // same name and caused a clap panic.
        let id = format!("{repo}#{pr_number}");
        let args = [
            "--host",
            host,
            "pr",
            "update",
            &id,
            "--update-style",
            "merge",
        ];
        let out = self
            .runner
            .run_in_env(None, &self.fj, &args, &self.fj_envs())?;
        Self::checked(&self.fj, out, &args)?;
        Ok(())
    }

    fn repo_permission(&self, host: &str, repo: &str, username: &str) -> Result<String> {
        // `fj repo collaborator <owner>/<repo> permission <user>
        // --format json` answers `{"username":..,"permission":..,"role_name":..}`.
        // Only `permission` is consumed; the finer-grained `role_name` is left
        // for a future allow-list without changing this call.
        let args = [
            "--host",
            host,
            "repo",
            "collaborator",
            repo,
            "permission",
            username,
            "--format",
            "json",
        ];
        let out = self.run_fj_read(None, &args, &self.fj_envs())?;
        let stdout = Self::checked(&self.fj, out, &args)?;
        #[derive(Deserialize)]
        struct Collaborator {
            #[serde(default)]
            permission: String,
        }
        let parsed: Collaborator = Self::parse_json(
            &self.fj,
            "fj repo collaborator permission --format json",
            stdout.trim(),
        )?;
        if parsed.permission.trim().is_empty() {
            return Err(Error::Other(anyhow::anyhow!(
                "`{} repo collaborator {repo} permission {username}` returned no `permission` field",
                self.fj
            )));
        }
        Ok(parsed.permission.trim().to_string())
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

    fn issue_comment(
        &self,
        host: &str,
        repo: &str,
        issue_number: u64,
        body_file: &Path,
    ) -> Result<()> {
        // `fj issue comment <repo>#<n> --body-file <path>`, the same
        // `owner/repo#n` addressing (no local checkout) and body-through-a-file
        // handling `pr_comment` uses.
        let id = format!("{repo}#{issue_number}");
        let body = body_file.to_string_lossy();
        let args = [
            "--host",
            host,
            "issue",
            "comment",
            &id,
            "--body-file",
            &body,
        ];
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
        // `fj pr edit <repo>#<n> labels --add <label>`. fj's `--add`
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

/// Every forge read the CI-failure actions make, all
/// through `fj`, so each action holds ONE client:
///
/// - [`ActionLogClient`] -> `fj actions logs`, the ONE path a run's logs are read
///   through for public and private repos alike: `fj` resolves the per-repo run
///   number to the internal run id, enumerates the run's jobs, and returns each
///   job's log, so nothing here guesses a job index.
/// - [`crate::forge_log::ActionRunsClient`] -> `fj actions runs`, paged.
/// - [`crate::forge_log::RepoFileClient`] -> `fj repo file`.
///
/// A resolved `log_token_ref` is injected the same way the per-job forge
/// credentials are: a temp `$XDG_DATA_HOME` holding a single-host
/// `keys.json` that carries that one secret and nothing else, removed when the
/// call returns. The token is never a process argument and never logged.
pub struct FjForgeReads {
    runner: Arc<dyn CommandRunner>,
    fj: String,
    /// Wait schedule for a retried read (CLAUDE-366); see
    /// [`DEFAULT_FJ_READ_RETRY_WAITS`].
    fj_read_retry_waits: [Duration; 3],
}

impl FjForgeReads {
    /// Client using the default `fj` binary name.
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            runner,
            fj: "fj".to_string(),
            fj_read_retry_waits: DEFAULT_FJ_READ_RETRY_WAITS,
        }
    }

    /// Override the binary name (tests point this at a stub on `PATH`).
    pub fn with_binary(mut self, fj: impl Into<String>) -> Self {
        self.fj = fj.into();
        self
    }

    /// Override the read-retry wait schedule (tests set this to all-zero).
    pub fn with_fj_read_retry_waits(mut self, waits: [Duration; 3]) -> Self {
        self.fj_read_retry_waits = waits;
        self
    }

    /// Every call this client makes is a read (CLAUDE-366): actions logs,
    /// actions runs, and repo file fetches never change forge state.
    fn run_fj_read(&self, args: &[&str], envs: &[(&str, &str)]) -> Result<CmdOutput> {
        run_fj_read_retrying(
            self.runner.as_ref(),
            &self.fj,
            None,
            args,
            envs,
            &self.fj_read_retry_waits,
        )
    }
}

/// The temp single-host `keys.json` a `fj` read runs against, plus the env that
/// points the child at it. Held for the whole call (a paged scan reuses one
/// store) and removed on drop.
struct FjCreds {
    /// Held only so the store outlives the call and is removed on drop.
    _keys: Option<crate::secrets::TempFjKeys>,
    xdg: Option<String>,
}

impl FjCreds {
    /// Write the store for `token`, or nothing at all when there is no token (a
    /// public repo reads unauthenticated).
    fn create(host: &str, token: Option<&str>) -> std::result::Result<Self, String> {
        let keys = match token {
            Some(t) => Some(
                crate::secrets::TempFjKeys::create(host, t)
                    .map_err(|e| format!("writing the temp fj key store failed: {e}"))?,
            ),
            None => None,
        };
        let xdg = keys.as_ref().map(|k| k.xdg_data_home());
        Ok(Self { _keys: keys, xdg })
    }

    /// The child env: the key-store override only, or nothing.
    fn envs(&self) -> Vec<(&str, &str)> {
        match &self.xdg {
            Some(v) => vec![("XDG_DATA_HOME", v.as_str())],
            None => Vec::new(),
        }
    }
}

/// Map a failed `fj actions logs` invocation onto an outcome. `fj` reports a run
/// it cannot resolve as `no action run #<n> on <repo>` and everything else as
/// `unexpected status code <code> <reason>`, so the code is the discriminator:
/// 401/403 is a rejected or under-scoped token (terminal), 404 is "not visible"
/// (a private repo read without a token, or a pruned run), and anything else is
/// a fault. Only `fj`'s own stderr is quoted; it carries no token.
fn classify_fj_log_error(stderr: &str, exit_code: i32) -> crate::forge_log::RunLogsOutcome {
    use crate::forge_log::RunLogsOutcome;
    let lower = stderr.to_ascii_lowercase();
    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no stderr")
        .to_string();
    if lower.contains("no action run") {
        return RunLogsOutcome::NotFound(detail);
    }
    if lower.contains("status code 401") {
        return RunLogsOutcome::AuthDenied(
            "HTTP 401: the log token is not a valid Forgejo user PAT (wrong value, expired, or not a token-scheme PAT)"
                .to_string(),
        );
    }
    if lower.contains("status code 403") {
        return RunLogsOutcome::AuthDenied(
            "HTTP 403: the log token is valid but lacks read:repository scope or Actions read access on this repo"
                .to_string(),
        );
    }
    if lower.contains("status code 404") {
        return RunLogsOutcome::NotFound(format!(
            "HTTP 404: the run is not visible (private repo without a log token, or pruned): {detail}"
        ));
    }
    RunLogsOutcome::Failed(format!("fj actions logs exited {exit_code}: {detail}"))
}

impl crate::forge_log::ActionLogClient for FjForgeReads {
    fn run_logs(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> crate::forge_log::RunLogsOutcome {
        use crate::forge_log::RunLogsOutcome;
        let creds = match FjCreds::create(host, token) {
            Ok(c) => c,
            Err(e) => return RunLogsOutcome::Failed(e),
        };
        let run = run_number.to_string();
        let args = [
            "--host", host, "actions", "logs", "--repo", repo, "--json", &run,
        ];
        match self.run_fj_read(&args, &creds.envs()) {
            Ok(out) if out.ok => match crate::forge_log::parse_run_logs(&out.stdout) {
                Ok(jobs) => RunLogsOutcome::Fetched(jobs),
                Err(e) => RunLogsOutcome::Failed(format!("parsing fj actions logs --json: {e}")),
            },
            Ok(out) => classify_fj_log_error(&out.stderr, out.exit_code),
            Err(e) => RunLogsOutcome::Failed(format!("running {} actions logs: {e}", self.fj)),
        }
    }
}

/// Runs requested per `fj actions runs` page. With the reconciler's page ceiling
/// (`RECONCILE_MAX_PAGES`) this scans the `50 * 40 = 2000` most-recent runs,
/// enough to reach a recently-minted "CI failure" issue's run; an older run is
/// simply not in the scan and its issue stays open.
const RUNS_PAGE_LIMIT: u32 = 50;

/// Map a failed `fj actions runs` invocation onto a runs-list outcome, on the
/// same discriminator [`classify_fj_log_error`] uses: `fj` reports an HTTP
/// failure as `unexpected status code <code> <reason>`. Only `fj`'s own stderr
/// is quoted; it carries no token.
fn classify_fj_runs_error(stderr: &str, exit_code: i32) -> crate::forge_log::RunsListOutcome {
    use crate::forge_log::RunsListOutcome;
    let lower = stderr.to_ascii_lowercase();
    let detail = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no stderr")
        .to_string();
    if lower.contains("status code 401") {
        return RunsListOutcome::AuthDenied(401);
    }
    if lower.contains("status code 403") {
        return RunsListOutcome::AuthDenied(403);
    }
    if lower.contains("status code 404") || lower.contains("not found") {
        return RunsListOutcome::NotFound;
    }
    RunsListOutcome::Failed(format!("fj actions runs exited {exit_code}: {detail}"))
}

impl crate::forge_log::ActionRunsClient for FjForgeReads {
    fn list_runs(
        &self,
        host: &str,
        repo: &str,
        max_pages: u32,
        token: Option<&str>,
    ) -> crate::forge_log::RunsListOutcome {
        use crate::forge_log::RunsListOutcome;
        // One store for the whole scan; removed when this call returns.
        let creds = match FjCreds::create(host, token) {
            Ok(c) => c,
            Err(e) => return RunsListOutcome::Failed(e),
        };
        let envs = creds.envs();
        let limit = RUNS_PAGE_LIMIT.to_string();

        // Page until a short page (the last one) or the ceiling. An error or an
        // unparseable page is terminal only before anything was collected; once
        // some entries are in hand, paging stops and returns them (partial
        // coverage beats none for a poll).
        let mut all = Vec::new();
        for page in 1..=max_pages {
            let page = page.to_string();
            let args = [
                "--host", host, "actions", "runs", "--repo", repo, "--json", "--page", &page,
                "--limit", &limit,
            ];
            let out = match self.run_fj_read(&args, &envs) {
                Ok(out) => out,
                Err(e) => {
                    if !all.is_empty() {
                        break;
                    }
                    return RunsListOutcome::Failed(format!(
                        "running {} actions runs: {e}",
                        self.fj
                    ));
                }
            };
            if !out.ok {
                if !all.is_empty() {
                    break;
                }
                return classify_fj_runs_error(&out.stderr, out.exit_code);
            }
            let entries = match crate::forge_log::parse_runs_list_entries(&out.stdout) {
                Ok(e) => e,
                Err(e) => {
                    if !all.is_empty() {
                        break;
                    }
                    return RunsListOutcome::Failed(format!("parsing fj actions runs --json: {e}"));
                }
            };
            let short = entries.len() < RUNS_PAGE_LIMIT as usize;
            all.extend(entries);
            if short {
                break;
            }
        }
        RunsListOutcome::Resolved(all)
    }

    fn run_timing(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> std::result::Result<crate::forge_log::RunTiming, String> {
        let creds = FjCreds::create(host, token)?;
        // `--run-number` returns the ONE run, so this never pages.
        let run = run_number.to_string();
        let args = [
            "--host",
            host,
            "actions",
            "runs",
            "--repo",
            repo,
            "--json",
            "--run-number",
            &run,
        ];
        match self.run_fj_read(&args, &creds.envs()) {
            Ok(out) if out.ok => crate::forge_log::parse_run_timing(&out.stdout, run_number),
            Ok(out) => Err(format!(
                "fj actions runs exited {}: {}",
                out.exit_code,
                out.stderr
                    .lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty())
                    .unwrap_or("no stderr")
            )),
            Err(e) => Err(format!("running {} actions runs: {e}", self.fj)),
        }
    }
}

impl crate::forge_log::RepoFileClient for FjForgeReads {
    fn read_file(
        &self,
        host: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        token: Option<&str>,
    ) -> std::result::Result<String, String> {
        let creds = FjCreds::create(host, token)?;
        // No `--json`: `fj repo file` writes the file's bytes to stdout
        // unchanged, so the YAML arrives as it is in the repo.
        let args = [
            "--host", host, "repo", "file", path, "--ref", git_ref, "--repo", repo,
        ];
        match self.run_fj_read(&args, &creds.envs()) {
            Ok(out) if out.ok => Ok(out.stdout),
            Ok(out) => Err(format!(
                "fj repo file exited {}: {}",
                out.exit_code,
                out.stderr
                    .lines()
                    .map(str::trim)
                    .find(|l| !l.is_empty())
                    .unwrap_or("no stderr")
            )),
            Err(e) => Err(format!("running {} repo file: {e}", self.fj)),
        }
    }
}

/// Test helper: a [`CommandRunner`] returning canned outcomes keyed by
/// `(binary, args)`, or a missing-binary error for configured binaries.
#[cfg(test)]
pub mod testutil {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    /// One recorded env injection: `(binary, [(name, value), ...])`.
    pub type EnvCall = (String, Vec<(String, String)>);

    #[derive(Default)]
    pub struct StubRunner {
        /// (binary, joined-args) -> outcome.
        responses: HashMap<String, CmdOutput>,
        /// (binary, joined-args) -> a queue of outcomes consumed in order, one
        /// per matching call, before falling through to `responses`.
        once: Mutex<HashMap<String, VecDeque<CmdOutput>>>,
        /// Binaries that should report as absent from PATH.
        missing: Vec<String>,
        /// Records every (binary, args) invocation for assertions.
        pub calls: Mutex<Vec<String>>,
        /// Records the injected child env per invocation for assertions.
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

        /// Register a failing response consumed by the FIRST call to
        /// `binary args...` only, so a test can model a call that fails once and
        /// succeeds on the retry (pair it with `ok` for the standing response).
        pub fn fail_once(self, binary: &str, args: &[&str], exit_code: i32, stderr: &str) -> Self {
            self.fail_times(binary, args, exit_code, stderr, 1)
        }

        /// Register a failing response consumed by the first `times` calls to
        /// `binary args...`, so a test can model a call that fails N times before
        /// succeeding on the retry that follows (pair it with `ok` for the
        /// standing response).
        pub fn fail_times(
            self,
            binary: &str,
            args: &[&str],
            exit_code: i32,
            stderr: &str,
            times: usize,
        ) -> Self {
            let out = CmdOutput {
                ok: false,
                stdout: String::new(),
                stderr: stderr.to_string(),
                exit_code,
            };
            self.once
                .lock()
                .unwrap()
                .entry(Self::key(binary, args))
                .or_default()
                .extend(std::iter::repeat_n(out, times));
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
            if let Some(out) = self
                .once
                .lock()
                .unwrap()
                .get_mut(&Self::key(binary, args))
                .and_then(VecDeque::pop_front)
            {
                return Ok(out);
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
    use std::sync::Mutex;

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

    /// `issue_links` flattens the `yt issue inspect --json` `links`
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

    /// The link is source-first and the type is always passed
    /// explicitly, since `yt issue link` defaults `--type` to `relates`.
    #[test]
    fn link_issue_passes_the_type_explicitly() {
        let b = backend(StubRunner::new().ok(
            "yt",
            &[
                "issue",
                "link",
                "CLAUDE-230",
                "CLAUDE-100",
                "--type",
                "subtask of",
            ],
            "",
        ));
        b.link_issue("CLAUDE-230", "subtask of", "CLAUDE-100")
            .unwrap();
    }

    /// `--create` so a tag not yet present on the instance is created rather
    /// than failing the write (CLAUDE-373).
    #[test]
    fn add_tag_creates_a_missing_tag() {
        let b = backend(StubRunner::new().ok(
            "yt",
            &[
                "issue",
                "tag",
                "add",
                "CLAUDE-373",
                "Needs-Info",
                "--create",
            ],
            "",
        ));
        b.add_tag("CLAUDE-373", "Needs-Info").unwrap();
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
        assert_eq!(repo.clone_url, "https://dev.a8n.run/contoso/claude-run.git");
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
        // A VCS probe failure (the `project 'VS' not found` shape)
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
        // The diagnostic sub-probes must carry the SAME injected env
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
        // Base URL is extracted from the config's `url:` line only;
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
        assert_eq!(
            b.yt_config_path().as_deref(),
            Some(cfg.to_string_lossy().as_ref())
        );
        // Host-auth fallback (no injected config) yields no base URL. This
        // reads the AMBIENT `YOUTRACK_CLI_CONFIG` env var (see
        // `yt_config_path`), which a host or container running the daemon's
        // own service login (`service_auth::export_yt_config_env`) may have
        // set for the whole test process, so save and restore it
        // around the assertion rather than trusting the ambient env is clean.
        let prior = std::env::var(crate::service_auth::YT_CONFIG_ENV).ok();
        std::env::remove_var(crate::service_auth::YT_CONFIG_ENV);
        let host_auth = CliBackend::new(Arc::new(StubRunner::new()));
        assert_eq!(host_auth.yt_base_url(), None);
        assert_eq!(host_auth.yt_config_path(), None);
        match prior {
            Some(v) => std::env::set_var(crate::service_auth::YT_CONFIG_ENV, v),
            None => std::env::remove_var(crate::service_auth::YT_CONFIG_ENV),
        }
    }

    #[test]
    fn vcs_repo_map_flattens_skips_archived_and_tolerates_a_failing_project() {
        // List projects, skip the archived one, flatten each live
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
        // `pr_strategy: agit` invokes `fj pr create -aA` as before.
        // With exactly one branch ahead of base the guard passes and
        // the AGit call is unchanged.
        let runner = Arc::new(with_branch_resolution(
            StubRunner::new().ok(
                "fj",
                &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                r#"{"number":42,"html_url":"https://dev.a8n.run/n/c/pulls/42"}"#,
            ),
            "origin/main",
            &["main", "fix/x"],
            &[("fix/x", "aaa\n")],
        ));
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(Path::new("/tmp/x"), "dev.a8n.run", PrStrategy::Agit)
            .unwrap();
        assert_eq!(pr.number, Some(42));
        assert_eq!(pr.url, "https://dev.a8n.run/n/c/pulls/42");
        // The AGit path issues exactly `-aA --json` and never pushes a branch.
        let calls = runner.calls.lock().unwrap();
        let fj_calls: Vec<&String> = calls.iter().filter(|c| c.starts_with("fj ")).collect();
        assert_eq!(
            fj_calls,
            vec!["fj --host dev.a8n.run pr create -aA --json"],
            "{calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "agit must not push a branch: {calls:?}"
        );
    }

    /// Register the git plumbing calls the resolve-by-name step performs
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

    /// Register the pre-push idempotency plumbing `create_pr_branch`
    /// runs after resolving the branch: the origin remote URL (parsed to
    /// `owner/repo`), the local branch SHA, and the remote branch state.
    /// `ls_remote` is the `git ls-remote --heads` stdout to return (empty string
    /// = branch absent on the remote; a `"<sha>\tref"` line = present at `<sha>`).
    fn with_remote_state(
        runner: StubRunner,
        branch: &str,
        local_sha: &str,
        ls_remote: &str,
    ) -> StubRunner {
        runner
            .ok(
                "git",
                &["remote", "get-url", "origin"],
                "https://dev.a8n.run/contoso/claude-run.git",
            )
            .ok("git", &["rev-parse", branch], local_sha)
            .ok(
                "git",
                &["ls-remote", "--heads", "origin", branch],
                ls_remote,
            )
    }

    /// The argv of the open-PR lookup the branch path issues against
    /// `contoso/claude-run` (the repo `with_remote_state`'s origin URL parses
    /// to).
    const OPEN_PR_SEARCH: [&str; 9] = [
        "--host",
        "dev.a8n.run",
        "pr",
        "search",
        "--repo",
        "contoso/claude-run",
        "--state",
        "open",
        "--json",
    ];

    /// The `fj pr search` open-PR lookup the branch path issues.
    /// `prs_json` is the bare JSON array of PRs to return (`"[]"` = no open
    /// PRs); this wraps it in the `{"total_count", "pull_requests"}` envelope
    /// `fj` 0.17.1 actually emits.
    fn with_open_pr_search(runner: StubRunner, prs_json: &str) -> StubRunner {
        let wrapped = format!(r#"{{"total_count":0,"pull_requests":{prs_json}}}"#);
        runner.ok("fj", &OPEN_PR_SEARCH, &wrapped)
    }

    /// The same lookup, failing the way a token without the issue
    /// scope does (`fj pr search` 403s while `pr create` still works).
    fn with_failed_open_pr_search(runner: StubRunner) -> StubRunner {
        runner.fail(
            "fj",
            &OPEN_PR_SEARCH,
            1,
            "Error: unexpected status code 403 Forbidden",
        )
    }

    /// Register the `git rev-parse --absolute-git-dir` lookup
    /// `find_or_create_pr` issues before writing the PR body scratch file, and
    /// return the scratch directory (`<git-dir>/claude-run`) the body file
    /// lands in, so a test can compute the exact `--body-file` argument and
    /// existence check.
    fn with_git_dir_scratch(runner: StubRunner, wd: &std::path::Path) -> (StubRunner, PathBuf) {
        let git_dir = wd.join(".git");
        let r = runner.ok(
            "git",
            &["rev-parse", "--absolute-git-dir"],
            &git_dir.to_string_lossy(),
        );
        (r, git_dir.join("claude-run"))
    }

    #[test]
    fn create_pr_branch_strategy_pushes_branch_and_creates_via_api() {
        // Default strategy: `pr_strategy: branch` pushes a real server branch
        // with the git CLI and opens the PR over the API, deriving title/body
        // from the head commit. `-aA` is never invoked. The branch is
        // resolved by name and pushed as `<branch>:<branch>` with an explicit
        // `--head`.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        );
        // Remote branch absent -> unchanged happy path (push +
        // create). No open PR yet, so find-or-create falls through to create.
        let runner = with_remote_state(runner, "feat/x", "abc123", "");
        let runner = with_open_pr_search(runner, "[]")
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
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path = scratch.join(".claude-run-pr-body.md");
        let body_path_str = body_path.to_string_lossy().to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): do the thing",
                "--body-file",
                body_path_str.as_str(),
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
        assert!(!body_path.exists());
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

    /// Acceptance (CLAUDE-395): when a credential resolves, `ls-remote` /
    /// `fetch` / `push` against `origin` are prefixed with the credential
    /// helper's `-c` arguments and NEVER carry the token itself anywhere in
    /// argv; the token is visible only in each call's own env.
    #[test]
    fn create_pr_branch_authenticates_network_calls_without_token_in_argv() {
        let wd = tempfile::tempdir().unwrap();
        let keys_dir = tempfile::tempdir().unwrap();
        let keys = keys_dir.path().join("keys.json");
        std::fs::write(
            &keys,
            r#"{"hosts":{"dev.a8n.run":{"token":"super-secret-token"}}}"#,
        )
        .unwrap();

        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        );
        let runner = runner
            .ok(
                "git",
                &["remote", "get-url", "origin"],
                "https://dev.a8n.run/contoso/claude-run.git",
            )
            .ok("git", &["rev-parse", "feat/x"], "abc123")
            .ok(
                "git",
                &[
                    "-c",
                    "credential.helper=",
                    "-c",
                    crate::provider::CREDENTIAL_HELPER,
                    "ls-remote",
                    "--heads",
                    "origin",
                    "feat/x",
                ],
                "",
            );
        let runner = with_open_pr_search(runner, "[]")
            .ok(
                "git",
                &[
                    "-c",
                    "credential.helper=",
                    "-c",
                    crate::provider::CREDENTIAL_HELPER,
                    "push",
                    "--set-upstream",
                    "origin",
                    "feat/x:feat/x",
                ],
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
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path = scratch.join(".claude-run-pr-body.md");
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): do the thing",
                "--body-file",
                body_path.to_string_lossy().as_ref(),
                "--head",
                "feat/x",
                "--json",
            ],
            r#"{"number":77,"html_url":"https://dev.a8n.run/n/c/pulls/77"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone()).with_keys_path(&keys);
        b.create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();

        let calls = runner.calls.lock().unwrap();
        for call in calls.iter() {
            assert!(
                !call.contains("super-secret-token"),
                "the token must never appear in argv: {call}"
            );
        }
        assert!(
            calls.iter().any(|c| c.contains("credential.helper=")
                && c.contains("push")
                && c.contains("--set-upstream")),
            "the push must run through the credential helper: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("git -c credential.helper= -c")
                    && c.contains("ls-remote --heads origin feat/x")),
            "ls-remote must run through the credential helper too: {calls:?}"
        );
        drop(calls);

        let envs = runner.envs.lock().unwrap();
        let git_envs_with_cred: Vec<_> = envs
            .iter()
            .filter(|(bin, env)| {
                bin == "git" && env.iter().any(|(k, _)| k == "CLAUDE_RUN_GIT_USERNAME")
            })
            .collect();
        assert!(
            !git_envs_with_cred.is_empty(),
            "the credential must reach at least one git call via env: {envs:?}"
        );
        for (_, env) in git_envs_with_cred {
            assert!(
                env.iter()
                    .any(|(k, v)| k == "CLAUDE_RUN_GIT_PASSWORD" && v == "super-secret-token"),
                "the token must be carried in the child's env, not argv: {env:?}"
            );
        }
    }

    #[test]
    fn create_pr_branch_resolves_by_name_when_head_is_on_main() {
        // The agent committed on a feature branch but left
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
        );
        // Remote branch absent, no open PR -> push + create.
        let runner = with_remote_state(runner, "fix/CLAUDE-166-work", "deadbeef", "");
        let runner = with_open_pr_search(runner, "[]")
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
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix: harden PR creation",
                "--body-file",
                body_path_str.as_str(),
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
        // The no-branch / no-commits case is made explicit. The
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
        // More than one branch ahead of base -> refuse to guess,
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
    fn create_pr_agit_zero_candidates_is_distinct_actionable_error() {
        // The guardrail is strategy-independent. With no branch ahead
        // of base, AGit gets the same loud, workdir-naming error the branch
        // strategy gives, and `fj` is never invoked (so no empty PR is opened
        // from the base tip and the run finalizes as Failed).
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main"],
            &[],
        ));
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Agit)
            .unwrap_err()
        {
            Error::Other(e) => {
                let msg = e.to_string();
                assert!(msg.contains("no feature branch"), "{msg}");
                assert!(msg.contains("origin/main"), "{msg}");
                assert!(msg.contains("preserved"), "{msg}");
                assert!(msg.contains(&wd.path().display().to_string()), "{msg}");
            }
            other => panic!("expected Other actionable error, got {other:?}"),
        }
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.starts_with("fj ")),
            "must not delegate to fj on zero candidates: {calls:?}"
        );
    }

    #[test]
    fn create_pr_agit_multiple_candidates_names_them_and_refuses() {
        // Two branches ahead of base would let `fj pr create -aA`
        // silently pick whatever `HEAD` happens to be; refuse and name them.
        let wd = tempfile::tempdir().unwrap();
        let runner = Arc::new(with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/a", "feat/b"],
            &[("feat/a", "aaa\n"), ("feat/b", "bbb\n")],
        ));
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Agit)
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
            !calls.iter().any(|c| c.starts_with("fj ")),
            "must not delegate to fj on multiple candidates: {calls:?}"
        );
    }

    #[test]
    fn created_pr_falls_back_to_url_when_number_field_absent() {
        // A future fj that omits `number` from the JSON still yields
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
        // Forgejo emits `/pulls/<n>`, GitHub-style `/pull/<n>`.
        assert_eq!(
            number_from_pr_url("https://dev.a8n.run/n/c/pulls/52"),
            Some(52)
        );
        assert_eq!(number_from_pr_url("https://github.com/o/r/pull/7"), Some(7));
        assert_eq!(number_from_pr_url("https://dev.a8n.run/n/c/issues/3"), None);
    }

    #[test]
    fn create_pr_branch_strategy_errors_loudly_on_push_failure() {
        // A failure of the selected (branch) strategy surfaces the
        // underlying CLI error directly. There is NO silent switch to AGit that
        // would emit a branchless PR.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc\n")],
        );
        // Remote branch absent, so the push is attempted (and fails).
        let runner = Arc::new(with_remote_state(runner, "feat/x", "abc", "").fail(
            "git",
            &["push", "--set-upstream", "origin", "feat/x:feat/x"],
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
    fn repo_path_from_remote_url_parses_https_and_ssh_and_creds() {
        // The open-PR lookup derives `owner/repo` from the origin
        // remote. HTTPS (the runner's clone form), token-embedded HTTPS, and SSH
        // all reduce to the last two path segments with the host/creds dropped.
        assert_eq!(
            repo_path_from_remote_url("https://dev.a8n.run/contoso/claude-run.git"),
            Some("contoso/claude-run".to_string())
        );
        assert_eq!(
            repo_path_from_remote_url("https://dev.a8n.run/contoso/claude-run"),
            Some("contoso/claude-run".to_string())
        );
        assert_eq!(
            repo_path_from_remote_url("https://oauth2:tok@dev.a8n.run/contoso/claude-run.git"),
            Some("contoso/claude-run".to_string())
        );
        assert_eq!(
            repo_path_from_remote_url("git@dev.a8n.run:contoso/claude-run.git"),
            Some("contoso/claude-run".to_string())
        );
    }

    #[test]
    fn create_pr_branch_adopts_open_pr_when_remote_equals_local() {
        // The remote branch already exists at the SAME commit as
        // the local tip (a re-run of already-delivered work). No push is needed;
        // the already-open PR for the branch is adopted. No `pr create` is issued.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        );
        let runner = with_remote_state(runner, "feat/x", "abc123", "abc123\trefs/heads/feat/x\n");
        let runner = Arc::new(with_open_pr_search(
            runner,
            r#"[{"number":88,"head_ref":"feat/x","html_url":"https://dev.a8n.run/n/c/pulls/88"}]"#,
        ));
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(88));
        assert_eq!(pr.url, "https://dev.a8n.run/n/c/pulls/88");
        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "equal remote must not push: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "adopting an open PR must not create a new one: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "no force anywhere: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_fast_forwards_then_creates_when_local_ahead() {
        // The remote branch exists and the local tip fast-forwards
        // it (remote is an ancestor of local). The push is an ordinary (never
        // forced) fast-forward; with no open PR yet, the PR is then created.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "newsha\n")],
        );
        let runner = with_remote_state(runner, "feat/x", "newsha", "oldsha\trefs/heads/feat/x\n");
        let runner = with_open_pr_search(runner, "[]")
            // CLAUDE-382: fetched before the ancestry check, so the remote
            // tip's objects (oldsha) are present locally when it runs.
            .ok("git", &["fetch", "origin", "feat/x"], "")
            // remote (oldsha) IS an ancestor of local (newsha) -> fast-forward.
            .ok(
                "git",
                &["merge-base", "--is-ancestor", "oldsha", "newsha"],
                "",
            )
            .ok(
                "git",
                &["push", "--set-upstream", "origin", "feat/x:feat/x"],
                "",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%s", "feat/x"],
                "feat(x): more work\n",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%b", "feat/x"],
                "#CLAUDE-170\n",
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): more work",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "feat/x",
                "--json",
            ],
            r#"{"number":90,"html_url":"https://dev.a8n.run/n/c/pulls/90"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(90));
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/x:feat/x"),
            "fast-forward must push the branch: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "fast-forward must never force: {calls:?}"
        );
        let fetch_idx = calls
            .iter()
            .position(|c| c == "git fetch origin feat/x")
            .expect("fetch must be called before the ancestry check");
        let ancestor_idx = calls
            .iter()
            .position(|c| c.starts_with("git merge-base --is-ancestor"))
            .expect("the ancestry check must be called");
        assert!(
            fetch_idx < ancestor_idx,
            "CLAUDE-382: fetch must happen before `is_ancestor` touches the remote tip, \
             else merge-base fails with exit 128 on a SHA the clone has never seen: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_adopts_open_pr_when_diverged() {
        // The YOTUN-96 signature: the remote branch is present
        // and NOT a fast-forward of the local tip (neither is an ancestor of the
        // other). Force is banned, so the runner fetches, merges `origin/<branch>`
        // into the local work (CLAUDE-382), pushes the resulting fast-forward,
        // and adopts the already-open PR for the branch via the ordinary
        // find-or-create path: it returns the PR (finalize advances to Code
        // Review, NOT Failed) and issues no `pr create` and no force.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/yotun-96"],
            &[("feat/yotun-96", "localsha\n")],
        );
        let runner = with_remote_state(
            runner,
            "feat/yotun-96",
            "localsha",
            "remotesha\trefs/heads/feat/yotun-96\n",
        );
        let runner = with_open_pr_search(
            runner,
            r#"[{"number":165,"head_ref":"feat/yotun-96","html_url":"https://dev.a8n.run/a/y/pulls/165"}]"#,
        )
        .ok("git", &["fetch", "origin", "feat/yotun-96"], "")
        // Neither commit is an ancestor of the other -> diverged (exit 1).
        .fail(
            "git",
            &["merge-base", "--is-ancestor", "remotesha", "localsha"],
            1,
            "",
        )
        .ok("git", &["merge", "--no-edit", "origin/feat/yotun-96"], "")
        .ok(
            "git",
            &[
                "push",
                "--set-upstream",
                "origin",
                "feat/yotun-96:feat/yotun-96",
            ],
            "",
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(165), "adopts the existing open PR");
        assert_eq!(pr.url, "https://dev.a8n.run/a/y/pulls/165");
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git merge --no-edit origin/feat/yotun-96"),
            "diverged must merge the remote branch in: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "git push --set-upstream origin feat/yotun-96:feat/yotun-96"),
            "diverged must push the merge result: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("pr create")),
            "diverged-with-PR must adopt, not create: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "no force anywhere: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_diverged_with_no_pr_is_loud_error() {
        // Remote branch present and diverged, and merging
        // `origin/<branch>` into the local work conflicts. The runner cannot
        // force and has nothing clean to push, so it aborts the merge and
        // returns a distinct, actionable error naming the branch, both SHAs,
        // and the preserved workdir. No push is issued and no force is
        // attempted.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "localsha\n")],
        );
        let runner = with_remote_state(
            runner,
            "feat/x",
            "localsha",
            "remotesha\trefs/heads/feat/x\n",
        );
        let runner = with_open_pr_search(runner, "[]")
            .ok("git", &["fetch", "origin", "feat/x"], "")
            .fail(
                "git",
                &["merge-base", "--is-ancestor", "remotesha", "localsha"],
                1,
                "",
            )
            .fail(
                "git",
                &["merge", "--no-edit", "origin/feat/x"],
                1,
                "CONFLICT (content): Merge conflict in src/lib.rs",
            )
            .ok("git", &["merge", "--abort"], "");
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
        {
            Error::Other(e) => {
                let msg = e.to_string();
                assert!(msg.contains("feat/x"), "names the branch: {msg}");
                assert!(msg.contains("localsha"), "names the local SHA: {msg}");
                assert!(msg.contains("remotesha"), "names the remote SHA: {msg}");
                assert!(msg.contains("preserved"), "names the workdir: {msg}");
                assert!(
                    msg.contains("conflicted"),
                    "names the merge conflict: {msg}"
                );
            }
            other => panic!("expected Other actionable error, got {other:?}"),
        }
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c == "git merge --abort"),
            "a conflicting merge must be aborted: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git push")),
            "a conflicting merge must not push: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "no force anywhere: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_creates_when_open_pr_lookup_fails() {
        // `fj pr search` 403s (an issue-category token gap) on
        // the remote-absent arm. The lookup is an optimization, so the run
        // degrades to "assume no open PR", still pushes and creates the PR, and
        // records the CLI error as an operator-visible notice.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "fix/CLAUDE-206"],
            &[("fix/CLAUDE-206", "abc123\n")],
        );
        let runner = with_remote_state(runner, "fix/CLAUDE-206", "abc123", "");
        let runner = with_failed_open_pr_search(runner)
            .ok(
                "git",
                &[
                    "push",
                    "--set-upstream",
                    "origin",
                    "fix/CLAUDE-206:fix/CLAUDE-206",
                ],
                "",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%s", "fix/CLAUDE-206"],
                "fix(pr): degrade a failed lookup\n",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%b", "fix/CLAUDE-206"],
                "#CLAUDE-206\n",
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix(pr): degrade a failed lookup",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "fix/CLAUDE-206",
                "--json",
            ],
            r#"{"number":206,"html_url":"https://dev.a8n.run/n/c/pulls/206"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(206), "a failed lookup must not abort");
        // The degradation is reported, naming the CLI error and the repo.
        let notices = b.take_notices();
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("403 Forbidden"), "{}", notices[0]);
        assert!(notices[0].contains("contoso/claude-run"), "{}", notices[0]);
        // Drained: a second call reports nothing again.
        assert!(b.take_notices().is_empty());
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c.contains("pr create")),
            "must fall through to create: {calls:?}"
        );
    }

    #[test]
    fn create_pr_branch_retries_lookup_once_when_create_says_already_exists() {
        // The race the degraded path exposes. The first lookup
        // fails, so the create runs and Forgejo rejects it with 409 "pull
        // request already exists". Exactly one lookup retry follows, and it now
        // succeeds, so the existing PR is adopted instead of the run failing.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "fix/CLAUDE-206"],
            &[("fix/CLAUDE-206", "abc123\n")],
        );
        let runner = with_remote_state(runner, "fix/CLAUDE-206", "abc123", "");
        let runner = with_open_pr_search(
            runner,
            r#"[{"number":207,"head_ref":"fix/CLAUDE-206","html_url":"https://dev.a8n.run/n/c/pulls/207"}]"#,
        )
        // ... but the FIRST lookup (before the create) fails.
        .fail_once(
            "fj",
            &OPEN_PR_SEARCH,
            1,
            "Error: unexpected status code 403 Forbidden",
        )
        .ok(
            "git",
            &[
                "push",
                "--set-upstream",
                "origin",
                "fix/CLAUDE-206:fix/CLAUDE-206",
            ],
            "",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%s", "fix/CLAUDE-206"],
            "fix(pr): degrade a failed lookup\n",
        )
        .ok(
            "git",
            &["log", "-1", "--format=%b", "fix/CLAUDE-206"],
            "#CLAUDE-206\n",
        );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.fail(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix(pr): degrade a failed lookup",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "fix/CLAUDE-206",
                "--json",
            ],
            1,
            "Error: api error: pull request already exists for these targets \
             [head_repo_id: 4, base_repo_id: 4, head_branch: fix/CLAUDE-206, base_branch: main]",
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone());
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(207), "the retry adopts the existing PR");
        let calls = runner.calls.lock().unwrap();
        let searches = calls.iter().filter(|c| c.contains("pr search")).count();
        assert_eq!(searches, 2, "exactly one retry: {calls:?}");
    }

    #[test]
    fn create_pr_branch_names_both_causes_when_lookup_and_create_fail() {
        // The create fails for its own reason after the lookup had
        // already failed. The error names BOTH, so "could not check whether a PR
        // existed" is never reported as a bare "PR creation failed".
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "fix/CLAUDE-206"],
            &[("fix/CLAUDE-206", "abc123\n")],
        );
        let runner = with_remote_state(runner, "fix/CLAUDE-206", "abc123", "");
        let runner = with_failed_open_pr_search(runner)
            .ok(
                "git",
                &[
                    "push",
                    "--set-upstream",
                    "origin",
                    "fix/CLAUDE-206:fix/CLAUDE-206",
                ],
                "",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%s", "fix/CLAUDE-206"],
                "fix(pr): degrade a failed lookup\n",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%b", "fix/CLAUDE-206"],
                "#CLAUDE-206\n",
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.fail(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix(pr): degrade a failed lookup",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "fix/CLAUDE-206",
                "--json",
            ],
            1,
            "Error: not found: The target couldn't be found.",
        );
        let b = CliBackend::new(Arc::new(runner)).with_fj_read_retry_waits(NO_WAIT);
        match b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
        {
            Error::Other(e) => {
                let msg = e.to_string();
                assert!(msg.contains("not found"), "names the create cause: {msg}");
                assert!(
                    msg.contains("403 Forbidden"),
                    "names the lookup cause: {msg}"
                );
            }
            other => panic!("expected Other actionable error, got {other:?}"),
        }
    }

    #[test]
    fn create_pr_branch_diverged_distinguishes_failed_lookup_from_no_pr() {
        // CLAUDE-382: once the diverged arm merges and pushes
        // cleanly, it falls through to the ordinary find-or-create path, so a
        // FAILED open-PR search degrades the same way it does for any other
        // arm: assume no open PR, push is already done, and create one (with
        // an operator-visible notice), rather than treating the diverged
        // branch as fatal on a lookup failure.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "fix/CLAUDE-206"],
            &[("fix/CLAUDE-206", "localsha\n")],
        );
        let runner = with_remote_state(
            runner,
            "fix/CLAUDE-206",
            "localsha",
            "remotesha\trefs/heads/fix/CLAUDE-206\n",
        );
        let runner = with_failed_open_pr_search(runner)
            .ok("git", &["fetch", "origin", "fix/CLAUDE-206"], "")
            .fail(
                "git",
                &["merge-base", "--is-ancestor", "remotesha", "localsha"],
                1,
                "",
            )
            .ok("git", &["merge", "--no-edit", "origin/fix/CLAUDE-206"], "")
            .ok(
                "git",
                &[
                    "push",
                    "--set-upstream",
                    "origin",
                    "fix/CLAUDE-206:fix/CLAUDE-206",
                ],
                "",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%s", "fix/CLAUDE-206"],
                "fix(pr): degrade a failed lookup after a merge\n",
            )
            .ok(
                "git",
                &["log", "-1", "--format=%b", "fix/CLAUDE-206"],
                "#CLAUDE-206\n",
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path_str = scratch
            .join(".claude-run-pr-body.md")
            .to_string_lossy()
            .to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "fix(pr): degrade a failed lookup after a merge",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "fix/CLAUDE-206",
                "--json",
            ],
            r#"{"number":91,"html_url":"https://dev.a8n.run/n/c/pulls/91"}"#,
        );
        let runner = Arc::new(runner);
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(91));
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls
                .iter()
                .any(|c| c == "git merge --no-edit origin/fix/CLAUDE-206"),
            "diverged must merge the remote branch in: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("git push")),
            "the merge result must still be pushed: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("--force")),
            "no force anywhere: {calls:?}"
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
        let json = r#"{"total_count":1,"pull_requests":[{"number":52,"title":"feat: x","state":"open","head_sha":"abc","head_ref":"feat/x","base_ref":"main","author":"David","html_url":"https://dev.a8n.run/n/c/pulls/52","updated_at":"2026-05-29T11:10:28Z","draft":false,"labels":[]}]}"#;
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
        assert_eq!(prs[0].base_ref, "main");
        assert_eq!(prs[0].author, "David");
        assert_eq!(prs[0].updated_at, "2026-05-29T11:10:28Z");
    }

    #[test]
    fn default_fj_read_retry_waits_are_0s_2s_5s() {
        assert_eq!(
            DEFAULT_FJ_READ_RETRY_WAITS,
            [
                Duration::from_secs(0),
                Duration::from_secs(2),
                Duration::from_secs(5),
            ]
        );
    }

    const NO_WAIT: [Duration; 3] = [Duration::ZERO, Duration::ZERO, Duration::ZERO];

    #[test]
    fn read_call_retries_once_and_succeeds_on_second_attempt() {
        let args = [
            "--host",
            "dev.a8n.run",
            "pr",
            "search",
            "--repo",
            "contoso/claude-run",
            "--state",
            "open",
            "--json",
        ];
        let runner = Arc::new(
            StubRunner::new()
                .fail_once("fj", &args, 1, "connect timeout")
                .ok("fj", &args, r#"{"total_count":0,"pull_requests":[]}"#),
        );
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let prs = b
            .list_open_prs("dev.a8n.run", "contoso/claude-run")
            .unwrap();
        assert!(prs.is_empty());
        assert_eq!(runner.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn remote_branch_sha_retries_once_and_succeeds_on_second_attempt() {
        let args = ["ls-remote", "--heads", "origin", "feature"];
        let runner = Arc::new(
            StubRunner::new()
                .fail_once("git", &args, 1, "connect timeout")
                .ok("git", &args, "deadbeef\trefs/heads/feature\n"),
        );
        let b = CliBackend::new(runner.clone()).with_git_retry_waits(NO_WAIT);
        let sha = b
            .remote_branch_sha(Path::new("/tmp/wd"), "feature", None)
            .unwrap();
        assert_eq!(sha, Some("deadbeef".to_string()));
        assert_eq!(runner.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn fetch_branch_retries_once_and_succeeds_on_second_attempt() {
        let args = ["fetch", "origin", "feature"];
        let runner = Arc::new(
            StubRunner::new()
                .fail_once("git", &args, 1, "connect timeout")
                .ok("git", &args, ""),
        );
        let b = CliBackend::new(runner.clone()).with_git_retry_waits(NO_WAIT);
        b.fetch_branch(Path::new("/tmp/wd"), "feature", None)
            .unwrap();
        assert_eq!(runner.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn push_branch_retries_once_and_succeeds_on_second_attempt() {
        let args = ["push", "--set-upstream", "origin", "feature:feature"];
        let runner = Arc::new(
            StubRunner::new()
                .fail_once("git", &args, 1, "connect timeout")
                .ok("git", &args, ""),
        );
        let b = CliBackend::new(runner.clone()).with_git_retry_waits(NO_WAIT);
        b.push_branch(Path::new("/tmp/wd"), "feature", None)
            .unwrap();
        assert_eq!(runner.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn push_branch_failing_all_four_attempts_returns_the_last_failure() {
        let args = ["push", "--set-upstream", "origin", "feature:feature"];
        let runner = Arc::new(StubRunner::new().fail("git", &args, 1, "connection reset"));
        let b = CliBackend::new(runner.clone()).with_git_retry_waits(NO_WAIT);
        let err = b
            .push_branch(Path::new("/tmp/wd"), "feature", None)
            .unwrap_err();
        assert!(err.to_string().contains("connection reset"));
        assert_eq!(runner.calls.lock().unwrap().len(), 4);
    }

    /// A rejected non-fast-forward push is a real-divergence signal, never a
    /// transient blip: it must still fail after exhausting all four attempts,
    /// and git's own rejection text must still reach the caller unchanged.
    #[test]
    fn push_branch_rejected_as_non_fast_forward_fails_after_exhausting_attempts() {
        let args = ["push", "--set-upstream", "origin", "feature:feature"];
        let rejection = "! [rejected]        feature -> feature (non-fast-forward)\n\
             error: failed to push some refs to 'origin'";
        let runner = Arc::new(StubRunner::new().fail("git", &args, 1, rejection));
        let b = CliBackend::new(runner.clone()).with_git_retry_waits(NO_WAIT);
        let err = b
            .push_branch(Path::new("/tmp/wd"), "feature", None)
            .unwrap_err();
        assert!(err.to_string().contains("non-fast-forward"), "{err}");
        assert_eq!(runner.calls.lock().unwrap().len(), 4);
    }

    #[test]
    fn read_call_retries_three_times_and_succeeds_on_fourth_attempt() {
        let args = [
            "--host",
            "dev.a8n.run",
            "pr",
            "search",
            "--repo",
            "contoso/claude-run",
            "--state",
            "open",
            "--json",
        ];
        let runner = Arc::new(
            StubRunner::new()
                .fail_times("fj", &args, 1, "connect timeout", 3)
                .ok("fj", &args, r#"{"total_count":0,"pull_requests":[]}"#),
        );
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let prs = b
            .list_open_prs("dev.a8n.run", "contoso/claude-run")
            .unwrap();
        assert!(prs.is_empty());
        assert_eq!(runner.calls.lock().unwrap().len(), 4);
    }

    #[test]
    fn read_call_failing_all_four_attempts_returns_the_last_failure() {
        let args = [
            "--host",
            "dev.a8n.run",
            "pr",
            "search",
            "--repo",
            "contoso/claude-run",
            "--state",
            "open",
            "--json",
        ];
        let runner = Arc::new(StubRunner::new().fail("fj", &args, 1, "connect timeout"));
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let err = b
            .list_open_prs("dev.a8n.run", "contoso/claude-run")
            .unwrap_err();
        match err {
            Error::CliFailed {
                binary,
                exit_code,
                stderr,
                ..
            } => {
                assert_eq!(binary, "fj");
                assert_eq!(exit_code, 1);
                assert_eq!(stderr, "connect timeout");
            }
            other => panic!("expected Error::CliFailed, got {other:?}"),
        }
        assert_eq!(runner.calls.lock().unwrap().len(), 4);
    }

    #[test]
    fn read_call_runner_err_is_not_retried() {
        let runner = Arc::new(StubRunner::new().missing("fj"));
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let err = b
            .list_open_prs("dev.a8n.run", "contoso/claude-run")
            .unwrap_err();
        assert!(matches!(err, Error::CliNotFound { .. }));
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn write_call_that_fails_is_never_retried() {
        let args = [
            "--host",
            "dev.a8n.run",
            "pr",
            "merge",
            "contoso/claude-run#52",
            "--method",
            "merge",
            "--delete",
        ];
        let runner = Arc::new(StubRunner::new().fail("fj", &args, 1, "merge conflict"));
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let err = b
            .merge_pr("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap_err();
        assert!(matches!(err, Error::CliFailed { .. }));
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
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
        // pr_status surfaces both the combined CI state and the
        // mergeability verdict from the same `fj pr status --json`.
        let json = r#"{"pr":52,"head_sha":"abc","state":"success","statuses":[],"mergeable":true,"merged":false,"behind_base":true}"#;
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
        assert_eq!(st.mergeable, Some(true));
        assert_eq!(st.behind_base, Some(true));
    }

    #[test]
    fn pr_status_merged_pr_null_mergeable_parses_to_none() {
        // A merged PR reports `"mergeable": null` and no `behind_base`.
        let json = r#"{"pr":336,"head_sha":null,"state":"merged","statuses":[],"mergeable":null,"merged":true}"#;
        let b = backend(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "status",
                "contoso/claude-run#336",
                "--json",
            ],
            json,
        ));
        let st = b
            .pr_status("dev.a8n.run", "contoso/claude-run", 336)
            .unwrap();
        assert_eq!(st.state, "merged");
        assert_eq!(st.mergeable, None);
        assert_eq!(st.behind_base, None);
    }

    #[test]
    fn pr_status_behind_base_false_when_forge_reports_up_to_date() {
        // `behind_base: false` parses to `Some(false)`.
        let json = r#"{"pr":52,"head_sha":"abc","state":"success","statuses":[],"mergeable":true,"merged":false,"behind_base":false}"#;
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
        assert_eq!(st.behind_base, Some(false));
    }

    #[test]
    fn pr_status_behind_base_null_parses_to_none() {
        // An explicit `null` (Forgejo has not computed merge_base
        // yet, or the PR is merged) must never collapse to `false`.
        let json = r#"{"pr":52,"head_sha":"abc","state":"success","statuses":[],"mergeable":true,"merged":false,"behind_base":null}"#;
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
        assert_eq!(st.behind_base, None);
    }

    #[test]
    fn pr_status_behind_base_absent_parses_to_none() {
        // An `fj` predating this field omits it entirely, which
        // must also read as unknown rather than `false`.
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
        assert_eq!(st.behind_base, None);
    }

    #[test]
    fn merge_pr_invokes_fj_merge_with_merge_method_and_deletes_branch() {
        // Merge-commit method, addressed as owner/repo#n.
        // `--delete` removes the PR head branch after merge.
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
        b.merge_pr("dev.a8n.run", "contoso/claude-run", 52).unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| {
                c == "fj --host dev.a8n.run pr merge contoso/claude-run#52 --method merge --delete"
            }),
            "{calls:?}"
        );
    }

    #[test]
    fn update_pr_branch_invokes_fj_update_with_merge_style() {
        // Forgejo's Update-branch endpoint, addressed as
        // owner/repo#n, always with `--update-style merge` (never rebase,
        // matching the merge-commit policy above). Flag renamed from
        // `--style` in a later Forgejo release.
        let runner = std::sync::Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "update",
                "contoso/claude-run#52",
                "--update-style",
                "merge",
            ],
            "",
        ));
        let b = CliBackend::new(runner.clone());
        b.update_pr_branch("dev.a8n.run", "contoso/claude-run", 52)
            .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| {
                c == "fj --host dev.a8n.run pr update contoso/claude-run#52 --update-style merge"
            }),
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
            calls.iter().any(|c| {
                c
                == "fj --host dev.a8n.run pr comment contoso/claude-run#52 --body-file /tmp/body.md"
            }),
            "{calls:?}"
        );
    }

    /// The plain-issue reply goes through `fj issue comment`, with
    /// the same `owner/repo#n` addressing and body-through-a-file handling as
    /// `pr_comment`. No raw REST anywhere on this path.
    #[test]
    fn issue_comment_addresses_the_issue_by_owner_repo_hash() {
        let runner = std::sync::Arc::new(StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "issue",
                "comment",
                "contoso/claude-run#52",
                "--body-file",
                "/tmp/body.md",
            ],
            "",
        ));
        let b = CliBackend::new(runner.clone());
        b.issue_comment(
            "dev.a8n.run",
            "contoso/claude-run",
            52,
            Path::new("/tmp/body.md"),
        )
        .unwrap();
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run issue comment contoso/claude-run#52 --body-file /tmp/body.md"),
            "{calls:?}"
        );
    }

    /// The comment-command authorization gate reads the commenter's
    /// effective permission through `fj repo collaborator ... --format json` and
    /// nothing else (no raw REST). A failing lookup surfaces as an error so the
    /// caller can deny.
    #[test]
    fn repo_permission_reads_the_collaborator_permission() {
        let args = [
            "--host",
            "dev.a8n.run",
            "repo",
            "collaborator",
            "contoso/claude-run",
            "permission",
            "alice",
            "--format",
            "json",
        ];
        let runner = std::sync::Arc::new(StubRunner::new().ok(
            "fj",
            &args,
            r#"{"username":"alice","permission":"write","role_name":"write"}"#,
        ));
        let b = CliBackend::new(runner.clone());
        assert_eq!(
            b.repo_permission("dev.a8n.run", "contoso/claude-run", "alice")
                .unwrap(),
            "write"
        );
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c
                == "fj --host dev.a8n.run repo collaborator contoso/claude-run permission alice --format json"),
            "{calls:?}"
        );
    }

    #[test]
    fn repo_permission_errors_when_the_lookup_fails() {
        // A non-collaborator / unreadable repo makes `fj` exit non-zero; the gate
        // treats that as a DENY, so the error must propagate rather than default.
        let runner = std::sync::Arc::new(StubRunner::new().fail(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "repo",
                "collaborator",
                "contoso/claude-run",
                "permission",
                "mallory",
                "--format",
                "json",
            ],
            1,
            "not found",
        ));
        let b = CliBackend::new(runner).with_fj_read_retry_waits(NO_WAIT);
        assert!(b
            .repo_permission("dev.a8n.run", "contoso/claude-run", "mallory")
            .is_err());
    }

    #[test]
    fn pr_add_labels_edits_each_label_addressed_by_owner_repo_hash() {
        // `fj pr edit <repo>#<n> labels --add <label>`, one call per
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
                c == "fj --host dev.a8n.run pr edit contoso/claude-run#52 labels --add needs-review"
            }),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c
                    == "fj --host dev.a8n.run pr edit contoso/claude-run#52 labels --add blocked"),
            "{calls:?}"
        );
    }

    #[test]
    fn injected_env_reaches_yt_and_fj_children_only() {
        // A credentialed backend injects YOUTRACK_CLI_CONFIG onto the
        // yt child and an XDG_DATA_HOME override onto the fj child, each on its
        // own child only.
        let runner = Arc::new(with_branch_resolution(
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
            "origin/main",
            &["main", "fix/x"],
            &[("fix/x", "aaa\n")],
        ));
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

    /// The two preflight probes for `repo`, as StubRunner arg lists.
    fn view_args(repo: &str) -> Vec<&str> {
        vec!["--host", "dev.a8n.run", "repo", "view", repo, "--json"]
    }

    fn search_args(repo: &str) -> Vec<&str> {
        vec![
            "--host",
            "dev.a8n.run",
            "pr",
            "search",
            "--repo",
            repo,
            "--state",
            "open",
            "--json",
        ]
    }

    #[test]
    fn repo_access_ok_when_both_probes_pass() {
        // The repository read AND the issue-category
        // open-PR lookup both exit 0, and both inject the fj credential env so
        // they test the BOT identity, not the operator store. An empty search
        // result (a repo with no open PRs) is a pass.
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &view_args("contoso/claude-run"),
                    r#"{"full_name":"Contoso/claude-run","private":true}"#,
                )
                .ok("fj", &search_args("contoso/claude-run"), "[]"),
        );
        let b = CliBackend::new(runner.clone()).with_fj_env(vec![(
            "XDG_DATA_HOME".to_string(),
            "/tmp/creds".to_string(),
        )]);
        b.repo_access("dev.a8n.run", "contoso/claude-run").unwrap();
        assert_eq!(runner.calls.lock().unwrap().len(), 2);
        let envs = runner.envs.lock().unwrap();
        assert_eq!(envs.len(), 2);
        for (bin, env) in envs.iter() {
            assert_eq!(bin, "fj");
            assert_eq!(
                *env,
                vec![("XDG_DATA_HOME".to_string(), "/tmp/creds".to_string())]
            );
        }
    }

    #[test]
    fn repo_access_fails_when_only_the_issue_probe_is_denied() {
        // This is the exact scope-short token case: a
        // repository-scoped token reads the repo fine and 403s on `pr search`,
        // the first issue-category call the PR path makes. The error must name
        // the failing capability, the status, and the scope to add.
        let b = CliBackend::new(Arc::new(
            StubRunner::new()
                .ok(
                    "fj",
                    &view_args("contoso/gadgets"),
                    r#"{"full_name":"Contoso/gadgets"}"#,
                )
                .fail(
                    "fj",
                    &search_args("contoso/gadgets"),
                    1,
                    "unexpected status code 403 Forbidden",
                ),
        ))
        .with_fj_read_retry_waits(NO_WAIT);
        let err = b
            .repo_access("dev.a8n.run", "contoso/gadgets")
            .unwrap_err()
            .to_string();
        assert!(err.contains("pr search"), "got {err}");
        assert!(err.contains("403"), "got {err}");
        assert!(err.contains("read:issue"), "got {err}");
        assert!(err.contains("contoso/gadgets"), "got {err}");
        assert!(err.contains("dev.a8n.run"), "got {err}");
        // The 403 wording points at the scope / unit grant, not at visibility.
        assert!(err.contains("unit permission"), "got {err}");
        assert!(!err.contains("not visible"), "got {err}");
    }

    #[test]
    fn repo_access_404_names_visibility_not_scope() {
        // An invisible private repo answers 404 on the
        // repository probe. The error must name the host, the `owner/repo`, and
        // the permission hypothesis (the bare CLI text names none), and must read
        // differently from the 403 above: adding a scope cannot reveal the repo.
        let runner = Arc::new(StubRunner::new().fail(
            "fj",
            &view_args("contoso/gadgets"),
            1,
            "not found: The target couldn't be found.",
        ));
        let b = CliBackend::new(runner.clone()).with_fj_read_retry_waits(NO_WAIT);
        let err = b
            .repo_access("dev.a8n.run", "contoso/gadgets")
            .unwrap_err()
            .to_string();
        assert!(err.contains("repo view"), "got {err}");
        assert!(err.contains("404"), "got {err}");
        assert!(err.contains("contoso/gadgets"), "got {err}");
        assert!(err.contains("dev.a8n.run"), "got {err}");
        assert!(err.contains("permission"), "got {err}");
        assert!(err.contains("not visible"), "got {err}");
        // The second probe is not attempted once the first one fails, but
        // the first one is itself retried four times (CLAUDE-366) before
        // that failure is surfaced.
        assert_eq!(runner.calls.lock().unwrap().len(), 4);
    }

    #[test]
    fn pr_create_failure_names_repo_host_and_permission_cause() {
        // Permissions can change mid-run, past the gather-time
        // preflight. The `fj pr create` 404 is then restated with the repo, the
        // host, and the possible cause instead of the bare CLI text.
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        );
        let runner = with_remote_state(runner, "feat/x", "abc123", "");
        let runner = with_open_pr_search(runner, "[]")
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
            .ok("git", &["log", "-1", "--format=%b", "feat/x"], "#MK-16\n");
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path = scratch.join(".claude-run-pr-body.md");
        let body_path_str = body_path.to_string_lossy().to_string();
        let runner = runner.fail(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): do the thing",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "feat/x",
                "--json",
            ],
            1,
            "not found: The target couldn't be found.",
        );
        let b = CliBackend::new(Arc::new(runner));
        let err = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap_err()
            .to_string();
        assert!(err.contains("contoso/claude-run"), "got {err}");
        assert!(err.contains("dev.a8n.run"), "got {err}");
        assert!(err.contains("may lack access"), "got {err}");
        // The body file is still cleaned up on the failure path.
        assert!(!body_path.exists());
    }

    #[test]
    fn agit_pr_create_failure_also_names_repo_host_and_cause() {
        // The AGit create path is the sibling of the branch path and
        // gets the same restatement; the repo is read from `origin` on failure.
        let b = CliBackend::new(Arc::new(with_branch_resolution(
            StubRunner::new()
                .fail(
                    "fj",
                    &["--host", "dev.a8n.run", "pr", "create", "-aA", "--json"],
                    1,
                    "not found: The target couldn't be found.",
                )
                .ok(
                    "git",
                    &["remote", "get-url", "origin"],
                    "https://dev.a8n.run/contoso/gadgets.git",
                ),
            "origin/main",
            &["main", "fix/x"],
            &[("fix/x", "aaa\n")],
        )));
        let err = b
            .create_pr(Path::new("/tmp/wd"), "dev.a8n.run", PrStrategy::Agit)
            .unwrap_err()
            .to_string();
        assert!(err.contains("contoso/gadgets"), "got {err}");
        assert!(err.contains("dev.a8n.run"), "got {err}");
        assert!(err.contains("may lack access"), "got {err}");
    }

    #[test]
    fn clone_url_unknown_host_errors() {
        let dir = tempfile::tempdir().unwrap();
        let keys = dir.path().join("keys.json");
        std::fs::write(&keys, r#"{"hosts":{}}"#).unwrap();
        let b = CliBackend::new(Arc::new(StubRunner::new())).with_keys_path(&keys);
        assert!(b.clone_url("dev.a8n.run", "x/y").is_err());
    }

    // ---- `fj actions logs` ------------------------------------

    use crate::forge_log::{ActionLogClient, RunLogsOutcome};

    const LOGS_ARGS: &[&str] = &[
        "--host",
        "dev.a8n.run",
        "actions",
        "logs",
        "--repo",
        "contoso/claude-run",
        "--json",
        "889",
    ];

    /// The document `fj actions logs --json` emits, verified against
    /// `fj v0.13.0` on dev.a8n.run.
    const LOGS_JSON: &str = r#"{"run_number":889,"jobs":[{"name":"fmt + clippy","status":"failure","log":"boom\n"},{"name":"deploy","status":"skipped","log":""}]}"#;

    #[test]
    fn action_logs_parses_the_json_document() {
        let runner = Arc::new(StubRunner::new().ok("fj", LOGS_ARGS, LOGS_JSON));
        let logs = FjForgeReads::new(runner);
        let out = logs.run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
        let RunLogsOutcome::Fetched(jobs) = out else {
            panic!("expected Fetched, got {out:?}");
        };
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].name, "fmt + clippy");
        assert_eq!(jobs[0].status, "failure");
        assert_eq!(jobs[0].log, "boom\n");
        // A never-started job's empty log rides along; it is not a miss.
        assert_eq!(jobs[1].log, "");
    }

    #[test]
    fn action_logs_injects_the_token_through_a_temp_single_host_keys_json() {
        // The resolved `log_token_ref` reaches the `fj` child only,
        // through a temp `keys.json` carrying that one secret and nothing else.
        // It is never a process argument.
        let runner = Arc::new(StubRunner::new().ok("fj", LOGS_ARGS, LOGS_JSON));
        let logs = FjForgeReads::new(runner.clone());
        let out = logs.run_logs("dev.a8n.run", "contoso/claude-run", 889, Some("test-pat"));
        assert!(matches!(out, RunLogsOutcome::Fetched(_)), "{out:?}");

        let envs = runner.envs.lock().unwrap();
        let (_, env) = envs.iter().find(|(b, _)| b == "fj").expect("fj child ran");
        let (_, xdg) = env
            .iter()
            .find(|(k, _)| k == "XDG_DATA_HOME")
            .expect("XDG_DATA_HOME injected");
        assert_eq!(env.len(), 1, "only the key-store override is injected");
        // The store the child would read carries the one host and one token. It
        // is removed when the call returns, so read it back from the recorded
        // path only to prove the path shape; the content check is on the writer.
        assert!(xdg.contains("claude-run-fj-"), "temp store path: {xdg}");
        // No argument carries the token.
        assert!(
            !LOGS_ARGS.iter().any(|a| a.contains("test-pat")),
            "token is never a process argument"
        );
    }

    #[test]
    fn temp_fj_keys_carries_only_that_host_and_token() {
        // The store the log fetch writes has exactly one host entry, and its
        // token is the per-call one (shape: type / name / token).
        let keys = crate::secrets::TempFjKeys::create("dev.a8n.run", "test-pat").unwrap();
        let path = std::path::PathBuf::from(keys.xdg_data_home())
            .join("forgejo-cli")
            .join("keys.json");
        let raw = std::fs::read_to_string(&path).unwrap();
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let hosts = doc.get("hosts").unwrap().as_object().unwrap();
        assert_eq!(hosts.len(), 1, "one host only: {raw}");
        assert_eq!(
            hosts["dev.a8n.run"]["token"].as_str(),
            Some("test-pat"),
            "{raw}"
        );
        drop(keys);
        assert!(!path.exists(), "the temp store is removed on drop");
    }

    #[test]
    fn a_run_fj_cannot_find_is_not_found_so_the_fetch_retries() {
        // `fj` reports an unresolvable run as `no action run #N on <repo>`; that
        // is the retryable "logs have not flushed yet / pruned" case.
        let runner = Arc::new(StubRunner::new().fail(
            "fj",
            LOGS_ARGS,
            1,
            "Error: no action run #889 on contoso/claude-run\n",
        ));
        let out = FjForgeReads::new(runner)
            .with_fj_read_retry_waits(NO_WAIT)
            .run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
        assert!(
            matches!(&out, RunLogsOutcome::NotFound(r) if r.contains("no action run")),
            "{out:?}"
        );
    }

    #[test]
    fn a_rejected_or_unscoped_token_is_auth_denied() {
        for (code, needle) in [(401, "401"), (403, "403")] {
            let runner = Arc::new(StubRunner::new().fail(
                "fj",
                LOGS_ARGS,
                1,
                &format!("Error: unexpected status code {code} Unauthorized\n"),
            ));
            let out = FjForgeReads::new(runner)
                .with_fj_read_retry_waits(NO_WAIT)
                .run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
            assert!(
                matches!(&out, RunLogsOutcome::AuthDenied(r) if r.contains(needle)),
                "{out:?}"
            );
        }
    }

    #[test]
    fn a_404_is_not_found_not_a_fault() {
        // A private repo read without a token 404s; so does a pruned run.
        let runner = Arc::new(StubRunner::new().fail(
            "fj",
            LOGS_ARGS,
            1,
            "Error: unexpected status code 404 Not Found\n",
        ));
        let out = FjForgeReads::new(runner)
            .with_fj_read_retry_waits(NO_WAIT)
            .run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
        assert!(matches!(out, RunLogsOutcome::NotFound(_)), "{out:?}");
    }

    #[test]
    fn a_missing_fj_or_unparseable_output_is_a_fault() {
        let runner = Arc::new(StubRunner::new().missing("fj"));
        let out =
            FjForgeReads::new(runner).run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
        assert!(matches!(out, RunLogsOutcome::Failed(_)), "{out:?}");

        let runner = Arc::new(StubRunner::new().ok("fj", LOGS_ARGS, "not json"));
        let out =
            FjForgeReads::new(runner).run_logs("dev.a8n.run", "contoso/claude-run", 889, None);
        assert!(
            matches!(&out, RunLogsOutcome::Failed(r) if r.contains("parsing")),
            "{out:?}"
        );
    }

    // ---- `fj actions runs` ----------------------------

    use crate::forge_log::{ActionRunsClient, RepoFileClient, RunsListOutcome};

    const RUNS_REPO: &str = "initech/chat";
    const RUNS_MAX_PAGES: u32 = 40;

    /// The argv for one `fj actions runs` page, as the client builds it.
    fn runs_args(page: &str) -> Vec<String> {
        [
            "--host",
            "dev.a8n.run",
            "actions",
            "runs",
            "--repo",
            RUNS_REPO,
            "--json",
            "--page",
            page,
            "--limit",
            "50",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// A `fj actions runs --json` page carrying `count` runs.
    fn runs_page(count: usize, first_id: i64, branch: &str) -> String {
        let items: Vec<String> = (0..count)
            .map(|n| {
                let id = first_id + n as i64;
                format!(
                    r#"{{"id":{id},"run_number":{id},"index_in_repo":{id},"workflow_id":"ci.yml","head_branch":"{branch}","status":"success","conclusion":"success","html_url":"https://dev.a8n.run/{RUNS_REPO}/actions/runs/{id}"}}"#
                )
            })
            .collect();
        format!(
            r#"{{"total_count":{},"runs":[{}]}}"#,
            items.len(),
            items.join(",")
        )
    }

    /// Register a canned page response on the stub runner.
    fn with_runs_page(runner: StubRunner, page: &str, body: &str) -> StubRunner {
        let args = runs_args(page);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        runner.ok("fj", &args, body)
    }

    #[test]
    fn runs_list_pages_until_a_short_page() {
        // A full first page (50 runs) then a short second page ends paging.
        let runner = with_runs_page(StubRunner::new(), "1", &runs_page(50, 90000, "main"));
        let runner = with_runs_page(runner, "2", &runs_page(1, 21077, "feature"));
        let runner = Arc::new(runner);
        let out = FjForgeReads::new(runner.clone()).list_runs(
            "dev.a8n.run",
            RUNS_REPO,
            RUNS_MAX_PAGES,
            Some("test-pat"),
        );
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved, got {out:?}");
        };
        assert_eq!(entries.len(), 51);
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "stopped at the short page: {calls:?}");
        assert!(calls[0].contains("actions runs --repo initech/chat --json --page 1"));
        // The token is never a process argument.
        assert!(!calls.iter().any(|c| c.contains("test-pat")), "{calls:?}");
    }

    #[test]
    fn runs_list_auth_denied_on_the_first_page_is_terminal() {
        for (code, expected) in [(401, 401u16), (403, 403u16)] {
            let args = runs_args("1");
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let runner = Arc::new(StubRunner::new().fail(
                "fj",
                &args,
                1,
                &format!("Error: unexpected status code {code} Unauthorized\n"),
            ));
            let out = FjForgeReads::new(runner.clone())
                .with_fj_read_retry_waits(NO_WAIT)
                .list_runs("dev.a8n.run", RUNS_REPO, RUNS_MAX_PAGES, Some("test-pat"));
            assert_eq!(out, RunsListOutcome::AuthDenied(expected));
            assert_eq!(
                runner.calls.lock().unwrap().len(),
                4,
                "terminal: no second page, but the first page's own call is retried up to four times (CLAUDE-366)"
            );
        }
    }

    #[test]
    fn runs_list_error_after_the_first_page_returns_what_was_collected() {
        // Page 1 full (so paging continues), page 2 fails: keep page-1 entries.
        let runner = with_runs_page(StubRunner::new(), "1", &runs_page(50, 90000, "main"));
        let args = runs_args("2");
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let runner = Arc::new(runner.fail("fj", &args, 1, "Error: connection refused\n"));
        let out = FjForgeReads::new(runner)
            .with_fj_read_retry_waits(NO_WAIT)
            .list_runs("dev.a8n.run", RUNS_REPO, RUNS_MAX_PAGES, Some("test-pat"));
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved with the collected page-1 entries, got {out:?}");
        };
        assert_eq!(entries.len(), 50);
    }

    #[test]
    fn runs_list_injects_the_token_through_a_temp_single_host_keys_json() {
        let runner = Arc::new(with_runs_page(
            StubRunner::new(),
            "1",
            &runs_page(1, 90000, "main"),
        ));
        let out = FjForgeReads::new(runner.clone()).list_runs(
            "dev.a8n.run",
            RUNS_REPO,
            RUNS_MAX_PAGES,
            Some("test-pat"),
        );
        assert!(matches!(out, RunsListOutcome::Resolved(_)), "{out:?}");
        let envs = runner.envs.lock().unwrap();
        let (_, env) = envs.iter().find(|(b, _)| b == "fj").expect("fj child ran");
        assert_eq!(env.len(), 1, "only the key-store override is injected");
        let (_, xdg) = env
            .iter()
            .find(|(k, _)| k == "XDG_DATA_HOME")
            .expect("XDG_DATA_HOME injected");
        assert!(xdg.contains("claude-run-fj-"), "temp store path: {xdg}");
    }

    #[test]
    fn a_public_repo_runs_scan_injects_no_credential() {
        let runner = Arc::new(with_runs_page(
            StubRunner::new(),
            "1",
            &runs_page(1, 90000, "main"),
        ));
        let out = FjForgeReads::new(runner.clone()).list_runs(
            "dev.a8n.run",
            RUNS_REPO,
            RUNS_MAX_PAGES,
            None,
        );
        assert!(matches!(out, RunsListOutcome::Resolved(_)), "{out:?}");
        let envs = runner.envs.lock().unwrap();
        let (_, env) = envs.iter().find(|(b, _)| b == "fj").expect("fj child ran");
        assert!(env.is_empty(), "no env injected without a token: {env:?}");
    }

    // ---- `fj actions runs --run-number` (one run's timing) ----

    /// The argv for the single-run timing read, as the client builds it.
    const TIMING_ARGS: &[&str] = &[
        "--host",
        "dev.a8n.run",
        "actions",
        "runs",
        "--repo",
        RUNS_REPO,
        "--json",
        "--run-number",
        "4034",
    ];

    #[test]
    fn run_timing_reads_one_run_and_never_pages() {
        let body = r#"{"total_count":1,"runs":[{"id":21077,"index_in_repo":4034,"started":"2026-08-20T20:03:13+02:00","stopped":"2026-08-20T20:27:31+02:00","status":"failure"}]}"#;
        let runner = Arc::new(StubRunner::new().ok("fj", TIMING_ARGS, body));
        let t = FjForgeReads::new(runner.clone())
            .run_timing("dev.a8n.run", RUNS_REPO, 4034, Some("test-pat"))
            .expect("timing read");
        assert_eq!(t.started, "2026-08-20T20:03:13+02:00");
        assert_eq!(t.stopped, "2026-08-20T20:27:31+02:00");
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "one call, no paging: {calls:?}");
        // The token is never a process argument.
        assert!(!calls.iter().any(|c| c.contains("test-pat")), "{calls:?}");
    }

    #[test]
    fn run_timing_reports_a_failed_call_as_an_err_with_the_reason() {
        let runner = Arc::new(StubRunner::new().fail(
            "fj",
            TIMING_ARGS,
            1,
            "Error: unexpected status code 404 Not Found\n",
        ));
        let e = FjForgeReads::new(runner)
            .with_fj_read_retry_waits(NO_WAIT)
            .run_timing("dev.a8n.run", RUNS_REPO, 4034, Some("test-pat"))
            .unwrap_err();
        assert!(e.contains("404"), "the reason names the failure: {e}");
    }

    // ---- `fj repo file` -------------------------------

    const FILE_ARGS: &[&str] = &[
        "--host",
        "dev.a8n.run",
        "repo",
        "file",
        ".forgejo/workflows/check.yml",
        "--ref",
        "a456a957",
        "--repo",
        RUNS_REPO,
    ];

    #[test]
    fn repo_file_returns_the_bytes_fj_wrote_to_stdout() {
        let yaml = "name: check\njobs:\n  check:\n    steps: []\n";
        let runner = Arc::new(StubRunner::new().ok("fj", FILE_ARGS, yaml));
        let out = FjForgeReads::new(runner.clone())
            .read_file(
                "dev.a8n.run",
                RUNS_REPO,
                ".forgejo/workflows/check.yml",
                "a456a957",
                Some("test-pat"),
            )
            .expect("the file resolves");
        assert_eq!(out, yaml, "no `--json`, so the bytes arrive unchanged");
        // The token rides in the temp key store, never in argv.
        let calls = runner.calls.lock().unwrap();
        assert!(!calls.iter().any(|c| c.contains("test-pat")), "{calls:?}");
        let envs = runner.envs.lock().unwrap();
        let (_, env) = envs.iter().find(|(b, _)| b == "fj").expect("fj child ran");
        assert!(env.iter().any(|(k, _)| k == "XDG_DATA_HOME"));
    }

    #[test]
    fn a_missing_path_is_an_error_the_caller_can_step_past() {
        // Maps the raw endpoint's 404 to a message naming path and ref.
        let runner = Arc::new(StubRunner::new().fail(
            "fj",
            FILE_ARGS,
            1,
            "Error: cannot read `.forgejo/workflows/check.yml` at ref `a456a957` in initech/chat\n",
        ));
        let err = FjForgeReads::new(runner)
            .with_fj_read_retry_waits(NO_WAIT)
            .read_file(
                "dev.a8n.run",
                RUNS_REPO,
                ".forgejo/workflows/check.yml",
                "a456a957",
                Some("test-pat"),
            )
            .expect_err("a missing path must not read as content");
        assert!(err.contains("check.yml"), "{err}");
    }

    /// Regression (ALPHA-726 shape): a resumed-PR clone pins the
    /// feature branch, which `git clone --branch` turns into
    /// `--single-branch`. That used to leave no `origin/HEAD` in
    /// `provider::shallow_clone`, so `resolve_feature_branch`'s first command
    /// failed outright. With the fix, the pinned clone still carries
    /// `origin/HEAD`, and `resolve_feature_branch` finds the feature branch.
    #[test]
    fn resolve_feature_branch_succeeds_on_a_pinned_resume_clone() {
        let tmp = tempfile::tempdir().unwrap();
        let origin = tmp.path().join("origin");
        std::fs::create_dir_all(&origin).unwrap();
        let git_in = |dir: &Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git_in(&origin, &["init", "-q", "-b", "main"]);
        git_in(&origin, &["config", "user.name", "t"]);
        git_in(&origin, &["config", "user.email", "t@t.example"]);
        std::fs::write(origin.join("f.txt"), "x").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-q", "-m", "init"]);
        git_in(&origin, &["checkout", "-q", "-b", "feat"]);
        std::fs::write(origin.join("g.txt"), "y").unwrap();
        git_in(&origin, &["add", "."]);
        git_in(&origin, &["commit", "-q", "-m", "feat commit"]);
        // The origin repo's own checked-out branch is what a `file://` clone's
        // remote HEAD follows; leave it back on `main`.
        git_in(&origin, &["checkout", "-q", "main"]);

        let url = format!("file://{}", origin.display());
        let clone = tmp.path().join("clone");
        crate::provider::shallow_clone(&url, Some("feat"), &clone, 50).unwrap();

        let b = CliBackend::new(Arc::new(ProcessRunner::new()));
        let branch = b.resolve_feature_branch(&clone).unwrap();
        assert_eq!(branch, "feat");
    }

    // `unwrap_prose` reflows hard-wrapped commit-body prose so the
    // PR description Forgejo renders uses the full page width.

    #[test]
    fn unwrap_prose_joins_pms_1281_body_to_one_paragraph_line() {
        let body = "compose.dev.yml pinned name: dev-billing while every container, volume\n\
                    and network was already ${USER}-suffixed, so the project itself was\n\
                    shared: a docker compose run under one USER could treat another\n\
                    developer's network as an orphan of the same project (or wipe their\n\
                    volumes via just down / just dev-clean). Make the project name\n\
                    dev-billing-${USER} so each developer's stack is its own project, and\n\
                    document the one-time migration step in CLAUDE.md.\n\
                    \n\
                    #GAMMA-1281\n\
                    \n\
                    Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>";
        let expected = "compose.dev.yml pinned name: dev-billing while every container, volume and network was already ${USER}-suffixed, so the project itself was shared: a docker compose run under one USER could treat another developer's network as an orphan of the same project (or wipe their volumes via just down / just dev-clean). Make the project name dev-billing-${USER} so each developer's stack is its own project, and document the one-time migration step in CLAUDE.md.\n\
                        \n\
                        #GAMMA-1281\n\
                        \n\
                        Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>";
        assert_eq!(unwrap_prose(body), expected);
    }

    #[test]
    fn unwrap_prose_leaves_a_fenced_code_block_untouched() {
        let body = "Summary line.\n\
                    \n\
                    ```\n\
                    line one\n\
                    \n\
                    line two, not joined\n\
                    ```\n\
                    \n\
                    #KEY-1";
        assert_eq!(unwrap_prose(body), body);
    }

    #[test]
    fn unwrap_prose_keeps_heading_separate_and_joins_its_paragraph() {
        let body = "## Summary\n\
                    This is a wrapped\n\
                    paragraph that should\n\
                    become one line.";
        let expected = "## Summary\nThis is a wrapped paragraph that should become one line.";
        assert_eq!(unwrap_prose(body), expected);
    }

    #[test]
    fn unwrap_prose_joins_list_item_continuation_but_not_the_next_item() {
        let body = "- first item wraps\n\
                    onto this line\n\
                    - second item stays separate";
        let expected = "- first item wraps onto this line\n- second item stays separate";
        assert_eq!(unwrap_prose(body), expected);
    }

    #[test]
    fn unwrap_prose_keeps_closes_and_issue_ref_on_their_own_lines() {
        let body = "Summary.\n\
                    \n\
                    Closes\n\
                    #KEY-2";
        assert_eq!(unwrap_prose(body), body);
    }

    #[test]
    fn unwrap_prose_respects_a_trailing_two_space_hard_break() {
        let body = "First line.  \nSecond line, kept separate.";
        assert_eq!(unwrap_prose(body), body);
    }

    #[test]
    fn unwrap_prose_leaves_an_already_unwrapped_body_unchanged() {
        let body = "## Summary\n\
                    Already one line per paragraph.\n\
                    \n\
                    Closes\n\
                    #KEY-3\n\
                    \n\
                    Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>";
        assert_eq!(unwrap_prose(body), body);
    }

    /// A `CommandRunner` that forwards every call to `inner`, but
    /// captures the `--body-file` scratch file's content the instant `fj pr
    /// create` runs, before `find_or_create_pr`'s own cleanup deletes it.
    struct BodyCapturingRunner {
        inner: StubRunner,
        body_path: PathBuf,
        captured: Arc<Mutex<Option<String>>>,
    }

    impl CommandRunner for BodyCapturingRunner {
        fn run_in_env(
            &self,
            dir: Option<&Path>,
            binary: &str,
            args: &[&str],
            envs: &[(&str, &str)],
        ) -> Result<CmdOutput> {
            if binary == "fj" && args.contains(&"create") {
                *self.captured.lock().unwrap() =
                    Some(std::fs::read_to_string(&self.body_path).unwrap());
            }
            self.inner.run_in_env(dir, binary, args, envs)
        }
    }

    #[test]
    fn create_pr_branch_writes_the_reflowed_body_to_the_scratch_file() {
        let wd = tempfile::tempdir().unwrap();
        let runner = with_branch_resolution(
            StubRunner::new(),
            "origin/main",
            &["main", "feat/x"],
            &[("feat/x", "abc123\n")],
        );
        let runner = with_remote_state(runner, "feat/x", "abc123", "");
        let runner = with_open_pr_search(runner, "[]")
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
                "Wrapped line one\nwrapped line two.\n\n#MK-16\n",
            );
        let (runner, scratch) = with_git_dir_scratch(runner, wd.path());
        let body_path = scratch.join(".claude-run-pr-body.md");
        let body_path_str = body_path.to_string_lossy().to_string();
        let runner = runner.ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "create",
                "feat(x): do the thing",
                "--body-file",
                body_path_str.as_str(),
                "--head",
                "feat/x",
                "--json",
            ],
            r#"{"number":77,"html_url":"https://dev.a8n.run/n/c/pulls/77"}"#,
        );
        let captured = Arc::new(Mutex::new(None));
        let capturing = BodyCapturingRunner {
            inner: runner,
            body_path,
            captured: captured.clone(),
        };
        let b = CliBackend::new(Arc::new(capturing));
        let pr = b
            .create_pr(wd.path(), "dev.a8n.run", PrStrategy::Branch)
            .unwrap();
        assert_eq!(pr.number, Some(77));
        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some("Wrapped line one wrapped line two.\n\n#MK-16")
        );
    }
}
