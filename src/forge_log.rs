//! Forgejo Action log fetch with retry and secret redaction.
//!
//! A run's logs are read through the `fj` CLI:
//!
//! ```text
//! fj --host <host> actions logs --repo <owner>/<repo> --json <run_number>
//! ```
//!
//! `fj` resolves the per-repo run number to the internal run id, enumerates the
//! run's jobs, and returns each job's plaintext log, so ONE path serves public
//! and private repos alike and nothing guesses a job index or an attempt
//! number. `<run_number>` is the webhook payload's `index_in_repo`.
//!
//! Logs are pruned after a retention window, and a freshly finished run can
//! briefly read as missing before its logs flush, so the fetch retries with
//! exponential backoff and a persistent miss is surfaced as a *transient*
//! outcome ([`LogOutcome::TransientMiss`]), never an error or a panic (triage
//! bucket 3 in the epic). A job `fj` returns with an empty log is NOT a miss; a
//! run `fj` cannot find is. Before any value is returned or written to disk the
//! text is run through [`redact`] so CI tokens (a failed publish step can echo
//! one) do not land in storage or a YouTrack attachment.
//!
//! ## Auth
//!
//! A private repo's read is authorized by the `log_token_ref` mapped to that
//! repo. The resolved PAT reaches the `fj` child through a temp single-host
//! `keys.json` ([`crate::provider::cli::FjForgeReads`]), never a process
//! argument and never a log line. A public repo's run reads without a token, so
//! a deployment that configures no token still analyzes public failures.
//! `read:repository` is the only scope needed: the three endpoints `fj` calls
//! all sit in Forgejo's repository group behind `reqRepoReader(unit.TypeActions)`.
//!
//! ## Every forge read goes through `fj`
//!
//! The runs list and the raw workflow YAML were the last two hand-rolled REST
//! calls; they now shell out to `fj actions runs` and `fj repo file`.
//! The three read seams ([`ActionLogClient`], [`ActionRunsClient`],
//! [`RepoFileClient`]) are collected in [`ForgeReads`] so a caller holds ONE
//! client, and this module builds no HTTP request at all.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::Result;

/// Marker substituted for any redacted secret.
const REDACTED: &str = "[REDACTED]";

/// Token prefixes that unambiguously denote a secret regardless of context.
const SECRET_PREFIXES: &[&str] = &[
    "ghp_",
    "gho_",
    "ghu_",
    "ghs_",
    "ghr_",
    "github_pat_",
    "glpat-",
    "xoxb-",
    "xoxp-",
    "xoxa-",
    "AGE-SECRET-KEY-",
];

/// A known secret value shorter than this is not masked, to avoid redacting a
/// common substring that merely happens to equal a trivial secret.
const MIN_KNOWN_SECRET_LEN: usize = 8;

/// Minimum length of a token that follows an auth keyword (`Bearer` / `token` /
/// `Basic`) before it is treated as a secret value. Combined with a digit check
/// this keeps ordinary prose ("the token implementation") from being redacted.
const MIN_VALUE_LEN: usize = 16;

/// One job of a run as `fj actions logs --json` reports it. The vector order is
/// the run order, so a job's position IS its job index.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct JobLog {
    /// The job's display name.
    #[serde(default)]
    pub name: String,
    /// The job's outcome (`success`, `failure`, ...).
    #[serde(default)]
    pub status: String,
    /// The job's plaintext log. Empty for a job that never started (`fj` maps
    /// that 404 to an empty log); an empty log is NOT a fetch miss.
    #[serde(default)]
    pub log: String,
    /// What `fj` says about this log: `available`, `missing` (gone for
    /// good, so re-asking can never help) or `unavailable` (it could not be read
    /// this time). Empty from an `fj` predating FJ-79, which is why it defaults:
    /// its absence means "nothing known", never "gone".
    #[serde(default)]
    pub log_state: String,
    /// The forge's own reason [`JobLog::log`] is empty, when it is.
    #[serde(default)]
    pub log_reason: Option<String>,
}

/// The [`JobLog::log_state`] value that means the log is gone for good.
pub const LOG_STATE_MISSING: &str = "missing";

/// `Some(reason)` when `fj` reports EVERY job's log as permanently
/// missing (the FJ-79 signal), so the run has no log to analyze and never will.
/// `None` when any job carries a log, when the run has no jobs, or when the `fj`
/// that answered predates FJ-79 and says nothing about the state: this may only
/// ever turn a readable log into a missing one, never the reverse.
pub fn logs_permanently_missing(jobs: &[JobLog]) -> Option<String> {
    if jobs.is_empty() || !jobs.iter().all(|j| j.log_state == LOG_STATE_MISSING) {
        return None;
    }
    let per_job: Vec<String> = jobs
        .iter()
        .map(|j| {
            format!(
                "`{}`: {}",
                j.name,
                j.log_reason.as_deref().unwrap_or("no reason given")
            )
        })
        .collect();
    Some(format!(
        "`fj` reports every job's log as permanently missing ({})",
        per_job.join("; ")
    ))
}

/// The `fj actions logs --json` document: the run number plus its jobs.
#[derive(Debug, Clone, Default, Deserialize)]
struct RunLogsDoc {
    #[serde(default)]
    jobs: Vec<JobLog>,
}

/// Outcome of ONE `fj actions logs` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunLogsOutcome {
    /// The run's jobs in run order, with raw (unredacted) logs.
    Fetched(Vec<JobLog>),
    /// `fj` cannot see the run: its logs have not flushed yet, it was pruned, or
    /// it is not visible to the credential. Retried by [`fetch_run_log`].
    NotFound(String),
    /// The token was rejected (401) or lacks the scope (403). Terminal.
    AuthDenied(String),
    /// Anything else: `fj` absent, unparseable output, an unexpected status.
    Failed(String),
}

/// Seam over `fj actions logs`, implemented by
/// [`crate::provider::cli::FjForgeReads`]. Abstracted so the retry / redaction
/// logic is unit-testable without spawning a process.
pub trait ActionLogClient: Send + Sync {
    /// Read `run_number`'s per-job logs for `repo` (`owner/repo`) on forge
    /// `host`. `token` is the resolved `log_token_ref`; `None` reads
    /// unauthenticated, which is enough for a public repo.
    fn run_logs(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> RunLogsOutcome;
}

/// Seam over `fj actions runs`, implemented by
/// [`crate::provider::cli::FjForgeReads`]. The implementation owns the paging,
/// so a caller asks once for up to `max_pages` pages worth of runs.
pub trait ActionRunsClient: Send + Sync {
    /// List `repo`'s workflow runs on forge `host`, newest-first, over at most
    /// `max_pages` pages. `token` is the resolved `log_token_ref`; `None` reads
    /// unauthenticated, which is enough for a public repo.
    fn list_runs(
        &self,
        host: &str,
        repo: &str,
        max_pages: u32,
        token: Option<&str>,
    ) -> RunsListOutcome;

    /// Read ONE run's forge-reported `started` / `stopped` through
    /// `fj actions runs --json --run-number <n>` (one run, not a paged scan).
    /// `Err` carries a human-readable reason (an unreadable page, a page
    /// carrying no such run, a run reporting no times), so a best-effort caller
    /// logs it and continues without the timing.
    fn run_timing(
        &self,
        host: &str,
        repo: &str,
        run_number: i64,
        token: Option<&str>,
    ) -> std::result::Result<RunTiming, String>;
}

/// Seam over `fj repo file`, implemented by
/// [`crate::provider::cli::FjForgeReads`].
pub trait RepoFileClient: Send + Sync {
    /// Read `path` in `repo` at `git_ref` on forge `host`, returning the file's
    /// text. `Err` carries a human-readable reason (a missing path included), so
    /// a best-effort caller can try the next candidate.
    fn read_file(
        &self,
        host: &str,
        repo: &str,
        path: &str,
        git_ref: &str,
        token: Option<&str>,
    ) -> std::result::Result<String, String>;
}

/// Every forge read the CI-failure actions make, so each holds ONE client
/// instead of one per endpoint.
pub trait ForgeReads: ActionLogClient + ActionRunsClient + RepoFileClient {}

impl<T: ActionLogClient + ActionRunsClient + RepoFileClient> ForgeReads for T {}

/// Parse a `fj actions logs --json` document into its jobs.
pub fn parse_run_logs(stdout: &str) -> std::result::Result<Vec<JobLog>, String> {
    let doc: RunLogsDoc = serde_json::from_str(stdout.trim()).map_err(|e| e.to_string())?;
    Ok(doc.jobs)
}

/// Which run's logs to fetch.
#[derive(Debug, Clone)]
pub struct LogTarget {
    /// Forge host, e.g. `dev.a8n.run` (what `fj --host` takes).
    pub host: String,
    /// `owner/repo`.
    pub repo: String,
    /// Run index within the repo (`index_in_repo` from the webhook), which is
    /// exactly the run number `fj actions logs` takes.
    pub run_number: i64,
    /// The `log_token_ref` resolved for this repo; `None` reads unauthenticated.
    pub token: Option<String>,
}

impl LogTarget {
    /// Build a target for `(repo, run_number)` from the failure event's
    /// `html_url`, deriving the forge host from it. `None` when the host cannot
    /// be derived.
    pub fn for_run(
        html_url: &str,
        repo: &str,
        run_number: i64,
        token: Option<String>,
    ) -> Option<Self> {
        Some(Self {
            host: host_from_html_url(html_url)?,
            repo: repo.to_string(),
            run_number,
            token,
        })
    }
}

/// Tunables for the fetch retry/backoff loop.
#[derive(Debug, Clone)]
pub struct LogFetchConfig {
    /// Number of *retries* after the first attempt. Total GETs = `1 + max_retries`.
    pub max_retries: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Cap the (doubling) backoff at this value.
    pub max_backoff: Duration,
}

impl Default for LogFetchConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl LogFetchConfig {
    /// One call, no backoff. For the polling actions, which read runs that
    /// finished long ago: a run `fj` cannot see there is
    /// pruned, not un-flushed, so waiting only burns the budget per issue. The
    /// webhook consumer keeps the retrying default, since its event arrives the
    /// moment the run finishes.
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }
}

/// A successfully fetched and redacted run log.
#[derive(Debug, Clone)]
pub struct RedactedLog {
    /// Every job's redacted log concatenated under a `===== name =====` header,
    /// usable in a summary.
    pub text: String,
    /// Path to the redacted text written to disk, for a YouTrack attachment (one
    /// run can exceed an inline-comment size limit).
    pub artifact: PathBuf,
    /// The run's jobs in run order, each carrying its REDACTED log. The failing
    /// job is selected from this with [`resolve_failing_job`].
    pub jobs: Vec<JobLog>,
}

/// Outcome of a fetch. A persistent miss is `TransientMiss`, not an error, so
/// the caller treats it as "logs not available yet / pruned" rather than a fault.
#[derive(Debug, Clone)]
pub enum LogOutcome {
    Fetched(RedactedLog),
    TransientMiss {
        reason: String,
    },
    /// The token was rejected or lacks the scope. Terminal after ONE call: the
    /// event arrives once the run finished, so there is nothing to wait for.
    AuthDenied {
        reason: String,
    },
    /// A fault that survived the retries (`fj` absent, unparseable output).
    Failed {
        reason: String,
    },
}

/// The last classified attempt, so an exhausted budget reports the right kind:
/// a run that stayed invisible is a miss, a broken `fj` call is a fault.
enum LastProbe {
    Miss(String),
    Fault(String),
}

/// Fetch every job's log for `target` through `fj actions logs`, retrying while
/// the run is not visible yet, redact each job's log, and write the concatenated
/// redacted text to an artifact file under `out_dir`. Returns
/// [`LogOutcome::Fetched`] on success, [`LogOutcome::AuthDenied`] immediately on
/// a rejected token, and [`LogOutcome::TransientMiss`] / [`LogOutcome::Failed`]
/// after the retries are exhausted. `known_secrets` are masked verbatim in
/// addition to the pattern-based redaction (pass the values the service itself
/// knows leaked).
///
/// `sleep` is injected so tests drive the backoff without waiting; production
/// passes `std::thread::sleep`. The only hard error this returns is an I/O
/// failure writing the artifact; a missing log is a `TransientMiss`, never `Err`.
pub fn fetch_run_log<S: FnMut(Duration), C: ActionLogClient + ?Sized>(
    client: &C,
    cfg: &LogFetchConfig,
    target: &LogTarget,
    out_dir: &Path,
    known_secrets: &[String],
    mut sleep: S,
) -> Result<LogOutcome> {
    let mut backoff = cfg.initial_backoff;
    let mut last = LastProbe::Miss("no attempt made".to_string());

    for attempt_no in 0..=cfg.max_retries {
        match client.run_logs(
            &target.host,
            &target.repo,
            target.run_number,
            target.token.as_deref(),
        ) {
            RunLogsOutcome::Fetched(jobs) => {
                // Redact BEFORE anything is rendered, stored, or handed back.
                let jobs: Vec<JobLog> = jobs
                    .into_iter()
                    .map(|j| {
                        let log = redact(&j.log, known_secrets);
                        JobLog { log, ..j }
                    })
                    .collect();
                let text = render_run_log(&jobs);
                let artifact = write_artifact(out_dir, target, &text)?;
                return Ok(LogOutcome::Fetched(RedactedLog {
                    text,
                    artifact,
                    jobs,
                }));
            }
            RunLogsOutcome::AuthDenied(reason) => return Ok(LogOutcome::AuthDenied { reason }),
            RunLogsOutcome::NotFound(reason) => last = LastProbe::Miss(reason),
            RunLogsOutcome::Failed(reason) => last = LastProbe::Fault(reason),
        }
        if attempt_no < cfg.max_retries {
            sleep(backoff);
            backoff = (backoff * 2).min(cfg.max_backoff);
        }
    }

    Ok(match last {
        LastProbe::Miss(reason) => LogOutcome::TransientMiss { reason },
        LastProbe::Fault(reason) => LogOutcome::Failed { reason },
    })
}

/// Concatenate every job's log under a `===== {name} ({status}) =====` header,
/// in run order.
fn render_run_log(jobs: &[JobLog]) -> String {
    let mut out = String::new();
    for job in jobs {
        out.push_str(&format!("===== {} ({}) =====\n", job.name, job.status));
        out.push_str(&job.log);
        if !out.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// Authority (`host[:port]`) of a forge URL, the bare host `fj --host` takes
/// (e.g. `dev.a8n.run`). `None` when the URL has no host segment.
pub fn host_from_html_url(url: &str) -> Option<String> {
    let after = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = after.split('/').next().unwrap_or("");
    (!host.is_empty()).then(|| host.to_string())
}

/// Write `text` to a per-(repo, run) artifact file under `out_dir` and return
/// its path. `out_dir` is created if absent.
fn write_artifact(out_dir: &Path, target: &LogTarget, text: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stem = target.repo.replace('/', "_");
    let name = format!("{stem}-run{}-all-jobs.log", target.run_number);
    let path = out_dir.join(name);
    std::fs::write(&path, text)?;
    Ok(path)
}

/// One run pulled from the runs-list JSON with the full set of fields
/// the auto-close reconciler needs to group and evaluate runs. The reconciler
/// reduces these to the LATEST run per `(branch, workflow_id)` and reads each
/// latest run's per-job status from the jobs `fj actions logs` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntry {
    /// The run's GLOBAL id (`run.id`); orders runs by recency.
    pub global_id: i64,
    /// The run's per-repo index (`index_in_repo`); the `/actions/runs/<N>` URL.
    pub index: i64,
    /// Workflow filename, e.g. `ci.yml`.
    pub workflow_id: String,
    /// The run's branch from the runs list (`head_branch`), or empty when the API
    /// returns it null (the deployed Forgejo), in which case the reconciler
    /// resolves it from the run's log environment dump.
    pub branch: String,
    /// The run outcome: the `conclusion` when present, else the `status`.
    pub status: String,
    /// The run web URL, used to derive the forge origin and link the close comment.
    pub html_url: String,
    /// When the run stopped, as the runs list reports it: `stopped`,
    /// else `updated`, else `created`. Empty when the page carries none of the
    /// three, so a caller must read an empty value as "unknown", never as "long
    /// ago". The CI-failure age fallback reads the same value for ONE run
    /// through [`ActionRunsClient::run_timing`], which does not page.
    pub stopped: String,
}

/// Parse one `fj actions runs --json` page into [`RunEntry`]s,
/// tolerant of Forgejo's field naming. Runs missing an `id` are skipped. The
/// branch is empty when the page carries no plain branch name (the reconciler
/// then resolves it from the log).
pub fn parse_runs_list_entries(stdout: &str) -> std::result::Result<Vec<RunEntry>, String> {
    let value: serde_json::Value =
        serde_json::from_str(stdout.trim()).map_err(|e| e.to_string())?;
    let arr = runs_array(&value);
    let mut out = Vec::new();
    for run in arr {
        let Some(global_id) = run.get("id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        let index = run_index(&run);
        // The outcome is `conclusion` when present, else `status` (mirrors the
        // jobs-list parse), so a completed-success run reads `success`.
        let status = run
            .get("conclusion")
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| run.get("status").and_then(serde_json::Value::as_str))
            .unwrap_or("")
            .to_string();
        out.push(RunEntry {
            global_id,
            index,
            workflow_id: str_field(&run, &["workflow_id", "workflow_name", "workflow"]),
            branch: branch_name(&str_field(&run, &["head_branch", "head_ref", "branch"])),
            status,
            html_url: str_field(&run, &["html_url", "url"]),
            stopped: str_field(&run, &["stopped", "updated", "created"]),
        });
    }
    Ok(out)
}

/// The runs of a `fj actions runs --json` page, tolerant of Forgejo's field
/// naming (and of a page that is a bare array).
fn runs_array(value: &serde_json::Value) -> Vec<serde_json::Value> {
    ["runs", "workflow_runs"]
        .iter()
        .find_map(|k| value.get(*k).and_then(|v| v.as_array()))
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default()
}

/// The first non-empty string among `keys`, or `""`.
fn str_field(run: &serde_json::Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|k| run.get(*k).and_then(serde_json::Value::as_str))
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_string()
}

/// A run's per-repo index, under any of the names the page may use.
fn run_index(run: &serde_json::Value) -> i64 {
    ["index_in_repo", "index", "run_number", "number"]
        .iter()
        .find_map(|k| run.get(*k).and_then(serde_json::Value::as_i64))
        .unwrap_or(0)
}

/// One run's forge-reported start / stop times. Handed to the
/// CI-failure analysis so it can tell a job that stopped emitting output and was
/// killed (a long gap between the last log line and `stopped`) from a capture
/// that lost data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTiming {
    /// When the run started, as the forge reports it (RFC 3339).
    pub started: String,
    /// When the run stopped, as the forge reports it (RFC 3339).
    pub stopped: String,
}

/// Read `run_number`'s `started` / `stopped` out of a
/// `fj actions runs --json` page. `Err` names the reason (unparseable page, no
/// such run, a run still in flight that reports no stop time), so the caller
/// logs it and analyzes without the timing rather than inventing one.
pub fn parse_run_timing(stdout: &str, run_number: i64) -> std::result::Result<RunTiming, String> {
    let value: serde_json::Value =
        serde_json::from_str(stdout.trim()).map_err(|e| e.to_string())?;
    let Some(run) = runs_array(&value)
        .into_iter()
        .find(|r| run_index(r) == run_number)
    else {
        return Err(format!(
            "the `fj actions runs --json` page carries no run #{run_number}"
        ));
    };
    let started = str_field(&run, &["started", "started_at", "created"]);
    let stopped = str_field(&run, &["stopped", "stopped_at", "completed_at"]);
    if started.is_empty() || stopped.is_empty() {
        return Err(format!(
            "run #{run_number} reports no start / stop time (started `{started}`, stopped `{stopped}`); the run may still be in flight"
        ));
    }
    Ok(RunTiming { started, stopped })
}

/// Keep a plain branch name, drop anything else. `fj`'s
/// `head_branch` is Forgejo's `prettyref`, which is the branch name only for a
/// branch push; verified against `dev.a8n.run`, a PR run reports `#<pr-number>`
/// and a ref-deleted push reports the bare commit sha. A non-branch value is
/// dropped so the reconciler resolves that run's branch from its log env dump,
/// exactly as it does for a null `head_branch`. Rejecting a real branch merely
/// costs one log read, so the test is deliberately strict.
fn branch_name(value: &str) -> String {
    let looks_like_sha = value.len() >= 7 && value.chars().all(|c| c.is_ascii_hexdigit());
    let plain = !value.is_empty()
        && !value.starts_with("refs/")
        && !value.starts_with('#')
        && !value.contains("pull/")
        && !looks_like_sha;
    if plain {
        value.to_string()
    } else {
        String::new()
    }
}

/// Outcome of fetching the runs list. Auth-class / not-found responses
/// on the FIRST page are terminal; a mid-paging error stops paging and returns the
/// entries already collected (partial coverage is better than none for a poll).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunsListOutcome {
    Resolved(Vec<RunEntry>),
    AuthDenied(u16),
    NotFound,
    Failed(String),
}

/// Candidate repo paths a failing workflow file may live under, tried
/// in order. `workflow_id` is the payload's workflow filename (e.g. `check.yml`).
const WORKFLOW_DIRS: &[&str] = &[
    ".forgejo/workflows",
    ".github/workflows",
    ".gitea/workflows",
];

/// Read the failing workflow's YAML from the repo at the failing
/// commit, read-only, so the analysis can map log position to workflow step
/// order. Tries each known workflow directory through `fj repo file`
/// and returns the first that resolves, redacted. `None` when none
/// resolve (the analysis proceeds over the log alone and says the YAML was
/// unavailable). Never errors on a miss.
pub fn read_workflow_yaml<C: RepoFileClient + ?Sized>(
    client: &C,
    host: &str,
    repo: &str,
    commit: &str,
    workflow_id: &str,
    token: Option<&str>,
    known_secrets: &[String],
) -> Option<String> {
    // A workflow_id already carrying a path segment is tried verbatim first.
    let mut candidates: Vec<String> = Vec::new();
    if workflow_id.contains('/') {
        candidates.push(workflow_id.to_string());
    }
    for dir in WORKFLOW_DIRS {
        candidates.push(format!("{dir}/{workflow_id}"));
    }
    for path in candidates {
        if let Ok(text) = client.read_file(host, repo, &path, commit, token) {
            if !text.trim().is_empty() {
                // Redacted before the analysis (or disk) ever sees it.
                return Some(redact(&text, known_secrets));
            }
        }
    }
    None
}

/// The ref-carrying environment fields a job log exposes, in whichever form the
/// runner printed them. Empty values are dropped at collection time, so `Some`
/// always means a usable value.
#[derive(Default)]
struct RefEnv {
    head_ref: Option<String>,
    git_ref: Option<String>,
    ref_name: Option<String>,
}

impl RefEnv {
    /// First non-empty value wins, so the earliest job in a multi-job log sets
    /// the branch and later jobs cannot overwrite it.
    fn set(&mut self, key: &str, value: &str) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        let slot = match key {
            "GITHUB_HEAD_REF" => &mut self.head_ref,
            "GITHUB_REF" => &mut self.git_ref,
            "GITHUB_REF_NAME" => &mut self.ref_name,
            _ => return,
        };
        slot.get_or_insert_with(|| value.to_string());
    }
}

/// Collect the ref fields from both forms a runner log uses: a
/// line-leading `KEY=value` env dump, and the `setupEnv => map[K:V K:V ...]`
/// Go map dump the deployed Forgejo runner prints (one whitespace-separated
/// token per variable, key and value split at the first colon).
fn collect_ref_env(log: &str) -> RefEnv {
    let mut env = RefEnv::default();
    for line in log.lines() {
        if line.contains("setupEnv") {
            if let Some(map) = line
                .split_once("map[")
                .map(|(_, rest)| rest)
                .and_then(|rest| rest.rfind(']').map(|end| &rest[..end]))
            {
                for token in map.split_whitespace() {
                    if let Some((key, value)) = token.split_once(':') {
                        env.set(key, value);
                    }
                }
            }
            continue;
        }
        let line = line.trim();
        if let Some((key, value)) = line.split_once('=') {
            env.set(key, value);
        }
    }
    env
}

/// A branch name from one ref value: a `refs/heads/` ref short-ended, or an
/// already-short name. Any other qualified ref (`refs/pull/N/head` on a PR run,
/// `refs/tags/v1` on a tag build) is not a branch and yields `None`. Some
/// Forgejo runners set `GITHUB_HEAD_REF` to the pull ref rather than the source
/// branch, so every candidate goes through here, not just `GITHUB_REF`.
fn branch_from_ref(value: &str) -> Option<&str> {
    let value = value.trim();
    match value.strip_prefix("refs/heads/") {
        Some(short) if !short.is_empty() => Some(short),
        Some(_) => None,
        None if value.starts_with("refs/") || value.is_empty() => None,
        None => Some(value),
    }
}

/// Parse the failing run's branch from a redacted job log's
/// environment dump. The run-detail API returns `head_branch: null` on the
/// deployed Forgejo, so the log is the reliable source.
///
/// Precedence: `GITHUB_HEAD_REF` (the only field carrying the source branch of
/// a PR run), else `GITHUB_REF`, else `GITHUB_REF_NAME`. A PR run's
/// `GITHUB_REF` is `refs/pull/N/head` and its `GITHUB_REF_NAME` the bare PR
/// number, so the last fallback is skipped when `GITHUB_REF` names a pull or
/// tag ref rather than returning a number or a tag name as a branch. `None`
/// when the log carries no readable ref (a log that stops before the env dump,
/// a diagnostic-free log, a logless failure).
pub fn parse_branch_from_log(log: &str) -> Option<String> {
    let env = collect_ref_env(log);
    for candidate in [env.head_ref.as_deref(), env.git_ref.as_deref()] {
        if let Some(branch) = candidate.and_then(branch_from_ref) {
            return Some(branch.to_string());
        }
    }
    // `GITHUB_REF_NAME` is a bare name, so it cannot be told apart from a PR
    // number or a tag on its own: the qualified ref decides whether to trust it.
    let qualified_non_branch = env
        .git_ref
        .as_deref()
        .is_some_and(|r| r.starts_with("refs/pull/") || r.starts_with("refs/tags/"));
    if qualified_non_branch {
        return None;
    }
    env.ref_name
}

/// Resolve the branch, preferring a non-empty run-detail `head_branch`
/// and falling back to the log environment dump. On the deployed Forgejo run
/// detail returns `head_branch: null`, so in practice this is the log parse; the
/// run-detail arm keeps the resolver correct where the field IS populated.
pub fn resolve_branch(run_detail_branch: Option<&str>, log: &str) -> Option<String> {
    match run_detail_branch {
        Some(b) if !b.trim().is_empty() => Some(b.trim().to_string()),
        _ => parse_branch_from_log(log),
    }
}

/// The failing job of a run, selected from the jobs `fj actions
/// logs` returned: the first job whose status denotes a failure, else the first
/// job. `None` for a run with no jobs. The job's position in the slice is its
/// job index in run order, so nothing has to assume index 0.
pub fn resolve_failing_job(jobs: &[JobLog]) -> Option<&JobLog> {
    jobs.iter()
        .find(|j| is_failure_status(&j.status))
        .or_else(|| jobs.first())
}

/// True for a job status/conclusion that denotes a failure.
fn is_failure_status(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "failure" | "failed" | "error"
    )
}

/// Job statuses that mean the run was stopped rather than broken. A
/// push that supersedes an in-flight run leaves every job `cancelled`, and a
/// server-side skipped job never ran; neither is a defect in the repository.
const STOPPED_STATUSES: &[&str] = &["cancelled", "skipped"];

/// What a run's job list says about whether anything actually
/// failed. Forgejo reports run-level `failure` for a run whose jobs were merely
/// cancelled (a newer push superseded it), so the payload status alone cannot
/// tell a broken build from a stopped one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobsVerdict {
    /// At least one job matched [`is_failure_status`]; carries its name and its
    /// position in run order.
    Failed { name: String, index: usize },
    /// The job list resolved and no job is a failure; carries every observed
    /// status in run order.
    NoFailure { statuses: Vec<String> },
    /// The job list did not resolve (miss, auth denial, fault, panicked task),
    /// so nothing is known about the jobs. Callers fail open on this.
    Unknown { reason: String },
}

impl JobsVerdict {
    /// The verdict for one fetch. `unavailable_reason` is the caller's cause for
    /// a fetch that returned no job list (transient miss, auth denial, fault,
    /// panicked task); when it is set the verdict is [`JobsVerdict::Unknown`]
    /// regardless of `jobs`.
    pub fn resolve(unavailable_reason: Option<&str>, jobs: &[JobLog]) -> Self {
        if let Some(reason) = unavailable_reason {
            return Self::Unknown {
                reason: reason.to_string(),
            };
        }
        match jobs.iter().position(|j| is_failure_status(&j.status)) {
            Some(index) => Self::Failed {
                name: jobs[index].name.clone(),
                index,
            },
            None => Self::NoFailure {
                statuses: jobs.iter().map(|j| j.status.clone()).collect(),
            },
        }
    }

    /// True when the run was stopped rather than broken: it has jobs, none
    /// failed, and every one ended in [`STOPPED_STATUSES`]. False for an empty
    /// job list, so a run that resolved with no jobs at all still mints.
    pub fn stopped(&self) -> bool {
        match self {
            Self::NoFailure { statuses } => {
                !statuses.is_empty()
                    && statuses
                        .iter()
                        .all(|s| STOPPED_STATUSES.contains(&s.to_ascii_lowercase().as_str()))
            }
            _ => false,
        }
    }

    /// The observed statuses as one comma-separated string, for a log line and
    /// the run summary. Empty for a verdict that carries no status list.
    pub fn statuses_display(&self) -> String {
        match self {
            Self::NoFailure { statuses } => statuses.join(", "),
            _ => String::new(),
        }
    }
}

/// Redact secrets from `input`: first mask each known secret value verbatim,
/// then redact token *shapes* (known prefixes, and high-entropy values that
/// follow an auth keyword). Pure; the unit of the AC's redaction test.
pub fn redact(input: &str, known_secrets: &[String]) -> String {
    let mut text = input.to_string();
    for s in known_secrets {
        let s = s.trim();
        if s.len() >= MIN_KNOWN_SECRET_LEN {
            text = text.replace(s, REDACTED);
        }
    }
    redact_token_shapes(&text)
}

/// True for a character that is part of a token (identifier-ish run). Includes
/// `-` so `AGE-SECRET-KEY-...` and `glpat-...` stay one token.
fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// True if `tok` begins with one of the unambiguous secret prefixes.
fn has_secret_prefix(tok: &str) -> bool {
    SECRET_PREFIXES.iter().any(|p| tok.starts_with(p))
}

/// True if `tok` (lowercased) is an auth keyword whose following token is a
/// secret value.
fn is_auth_keyword(tok: &str) -> bool {
    matches!(
        tok.to_ascii_lowercase().as_str(),
        "bearer" | "token" | "basic"
    )
}

/// True if `tok` looks like a secret value: long and containing at least one
/// digit. Used to gate keyword-triggered redaction so ordinary words after the
/// word "token" are left alone.
fn looks_secretish(tok: &str) -> bool {
    tok.len() >= MIN_VALUE_LEN && tok.chars().any(|c| c.is_ascii_digit())
}

/// Redact token shapes: any token with a secret prefix, plus any secret-ish
/// token immediately following an auth keyword on the same line.
fn redact_token_shapes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    // Whether the previous token on this line was an auth keyword.
    let mut prev_keyword = false;
    while i < chars.len() {
        let c = chars[i];
        if is_token_char(c) {
            let start = i;
            while i < chars.len() && is_token_char(chars[i]) {
                i += 1;
            }
            let tok: String = chars[start..i].iter().collect();
            if has_secret_prefix(&tok) || (prev_keyword && looks_secretish(&tok)) {
                out.push_str(REDACTED);
            } else {
                out.push_str(&tok);
            }
            prev_keyword = is_auth_keyword(&tok);
        } else {
            out.push(c);
            if c == '\n' {
                prev_keyword = false;
            }
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    /// One recorded `run_logs` call: `(host, repo, run_number, token)`.
    type LoggedCall = (String, String, i64, Option<String>);

    fn cfg() -> LogFetchConfig {
        LogFetchConfig {
            max_retries: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(8),
        }
    }

    // ---- `fj actions logs` fetch ------------------------------------------

    /// Scripted [`ActionLogClient`]: pops queued outcomes in order, defaulting
    /// to a not-found once drained so an "always missing" run is easy to set up.
    /// Records the arguments so the call shape is asserted.
    struct StubLogClient {
        outcomes: Mutex<VecDeque<RunLogsOutcome>>,
        calls: Mutex<Vec<LoggedCall>>,
    }

    impl StubLogClient {
        fn new(outcomes: Vec<RunLogsOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }
        fn calls(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl ActionLogClient for StubLogClient {
        fn run_logs(
            &self,
            host: &str,
            repo: &str,
            run_number: i64,
            token: Option<&str>,
        ) -> RunLogsOutcome {
            self.calls.lock().unwrap().push((
                host.to_string(),
                repo.to_string(),
                run_number,
                token.map(str::to_string),
            ));
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| RunLogsOutcome::NotFound("no action run #7".to_string()))
        }
    }

    fn job(name: &str, status: &str, log: &str) -> JobLog {
        JobLog {
            name: name.to_string(),
            status: status.to_string(),
            log: log.to_string(),
            ..Default::default()
        }
    }

    /// A job whose log `fj` reports as permanently gone.
    fn missing_job(name: &str, reason: &str) -> JobLog {
        JobLog {
            name: name.to_string(),
            status: "failure".to_string(),
            log: String::new(),
            log_state: LOG_STATE_MISSING.to_string(),
            log_reason: Some(reason.to_string()),
        }
    }

    fn target() -> LogTarget {
        LogTarget {
            host: "dev.a8n.run".to_string(),
            repo: "contoso/claude-run".to_string(),
            run_number: 7,
            token: Some("test-pat".to_string()),
        }
    }

    #[test]
    fn fetch_returns_redacted_text_and_artifact_file() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![RunLogsOutcome::Fetched(vec![job(
            "check",
            "failure",
            "step 1 ok\nAuthorization: token ghp_abcdefghij0123456789ABCDEFG\nstep 2 failed",
        )])]);
        let out = fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        let LogOutcome::Fetched(log) = out else {
            panic!("expected Fetched, got {out:?}");
        };
        // The artifact exists and matches the returned text.
        assert!(log.artifact.exists());
        let on_disk = std::fs::read_to_string(&log.artifact).unwrap();
        assert_eq!(on_disk, log.text);
        // The secret is gone from the summary text, the file, AND the per-job
        // log handed back to the caller.
        assert!(!log.text.contains("ghp_abcdefghij0123456789ABCDEFG"));
        assert!(!on_disk.contains("ghp_abcdefghij0123456789ABCDEFG"));
        assert!(!log.jobs[0].log.contains("ghp_abcdefghij0123456789ABCDEFG"));
        assert!(log.text.contains(REDACTED));
        assert!(log.text.contains("step 2 failed"));
        assert!(log.text.contains("===== check (failure) ====="));
        assert_eq!(client.calls(), 1);
    }

    #[test]
    fn fetch_passes_the_host_repo_run_and_token_to_fj() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![RunLogsOutcome::Fetched(vec![job(
            "c", "failure", "x",
        )])]);
        fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        let calls = client.calls.lock().unwrap();
        assert_eq!(
            calls[0],
            (
                "dev.a8n.run".to_string(),
                "contoso/claude-run".to_string(),
                7,
                Some("test-pat".to_string())
            )
        );
    }

    #[test]
    fn retries_then_succeeds_when_logs_flush_late() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![
            RunLogsOutcome::NotFound("no action run #7".to_string()),
            RunLogsOutcome::NotFound("no action run #7".to_string()),
            RunLogsOutcome::Fetched(vec![job("check", "failure", "the logs")]),
        ]);
        let mut sleeps = 0u32;
        let out =
            fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| sleeps += 1).unwrap();
        assert!(matches!(out, LogOutcome::Fetched(_)));
        assert_eq!(client.calls(), 3, "two misses then a hit");
        assert_eq!(sleeps, 2, "backed off before each retry");
    }

    #[test]
    fn persistent_miss_is_transient_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        // Empty queue -> the stub's drained default (not found) every time.
        let client = StubLogClient::new(vec![]);
        let out = fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        match out {
            LogOutcome::TransientMiss { reason } => assert!(reason.contains("no action run")),
            other => panic!("expected TransientMiss, got {other:?}"),
        }
        // 1 initial attempt + max_retries.
        assert_eq!(client.calls(), 1 + cfg().max_retries as usize);
    }

    #[test]
    fn auth_denied_is_terminal_after_one_call() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![RunLogsOutcome::AuthDenied(
            "HTTP 403: lacks read:repository".to_string(),
        )]);
        let mut sleeps = 0u32;
        let out =
            fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| sleeps += 1).unwrap();
        assert!(
            matches!(&out, LogOutcome::AuthDenied { reason } if reason.contains("403")),
            "{out:?}"
        );
        assert_eq!(client.calls(), 1, "no retry on a rejected token");
        assert_eq!(sleeps, 0);
    }

    #[test]
    fn a_fault_survives_the_retries_as_failed_not_a_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![
            RunLogsOutcome::Failed("fj not found on PATH".to_string()),
            RunLogsOutcome::Failed("fj not found on PATH".to_string()),
            RunLogsOutcome::Failed("fj not found on PATH".to_string()),
            RunLogsOutcome::Failed("fj not found on PATH".to_string()),
        ]);
        let out = fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        assert!(
            matches!(&out, LogOutcome::Failed { reason } if reason.contains("fj not found")),
            "{out:?}"
        );
    }

    #[test]
    fn a_job_with_an_empty_log_is_not_a_miss() {
        // `fj` maps a never-started job's 404 to an empty log; the run itself was
        // found, so this is a successful fetch, not a transient miss.
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![RunLogsOutcome::Fetched(vec![
            job("build", "failure", "boom\n"),
            job("deploy", "skipped", ""),
        ])]);
        let out = fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        let LogOutcome::Fetched(log) = out else {
            panic!("expected Fetched, got {out:?}");
        };
        assert_eq!(log.jobs.len(), 2);
        assert_eq!(client.calls(), 1, "no retry for an empty job log");
        assert!(log.text.contains("===== deploy (skipped) ====="));
    }

    #[test]
    fn artifact_is_named_per_repo_and_run() {
        let tmp = tempfile::tempdir().unwrap();
        let client = StubLogClient::new(vec![RunLogsOutcome::Fetched(vec![job(
            "c", "failure", "x",
        )])]);
        let out = fetch_run_log(&client, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        let LogOutcome::Fetched(log) = out else {
            panic!("expected Fetched");
        };
        assert_eq!(
            log.artifact.file_name().unwrap().to_string_lossy(),
            "contoso_claude-run-run7-all-jobs.log"
        );
    }

    #[test]
    fn parse_run_logs_reads_the_fj_json_document() {
        // The shape `fj actions logs --json` emits.
        let stdout = r#"{"run_number":889,"jobs":[{"name":"fmt + clippy","status":"failure","log":"boom\n"}]}"#;
        let jobs = parse_run_logs(stdout).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "fmt + clippy");
        assert_eq!(jobs[0].status, "failure");
        assert_eq!(jobs[0].log, "boom\n");
        assert!(parse_run_logs("not json").is_err());
    }

    /// An `fj` predating FJ-79 emits no `log_state`, and its
    /// output parses exactly as before, with nothing known about the state.
    #[test]
    fn parse_run_logs_still_reads_a_pre_fj79_document() {
        let stdout =
            r#"{"run_number":889,"jobs":[{"name":"check","status":"failure","log":"boom\n"}]}"#;
        let jobs = parse_run_logs(stdout).unwrap();
        assert_eq!(jobs[0].log, "boom\n");
        assert_eq!(jobs[0].log_state, "", "no state claimed");
        assert_eq!(jobs[0].log_reason, None);
        assert!(
            logs_permanently_missing(&jobs).is_none(),
            "an absent state is never read as a missing log"
        );
    }

    /// The per-job state FJ-79 added is parsed, so a log that is
    /// gone for good is distinguishable from a job that printed nothing. The
    /// document is the shape `fj actions logs --format json` emits: `log_state`
    /// is one of `available` / `missing` / `unavailable`, with `log_reason` null
    /// for an available log.
    #[test]
    fn parse_run_logs_reads_the_fj79_log_state() {
        let stdout = r#"{"run_number":4760,"jobs":[{"name":"build","status":"failure","log":"","log_state":"missing","log_reason":"open logs for task 43177: no such file or directory"},{"name":"deploy","status":"success","log":"ok\n","log_state":"available","log_reason":null}]}"#;
        let jobs = parse_run_logs(stdout).unwrap();
        assert_eq!(jobs[0].log_state, LOG_STATE_MISSING);
        assert!(jobs[0]
            .log_reason
            .as_deref()
            .unwrap()
            .contains("task 43177"));
        assert_eq!(jobs[1].log_state, "available");
        assert_eq!(jobs[1].log_reason, None);
        // One readable job means the run still has something to analyze.
        assert!(logs_permanently_missing(&jobs).is_none());
    }

    /// A run whose EVERY job log is gone is reported as
    /// permanently missing, carrying the forge's own reason.
    #[test]
    fn every_job_missing_is_permanently_missing_with_the_reason() {
        let jobs = vec![missing_job(
            "build",
            "open logs for task 43177: no such file",
        )];
        let reason = logs_permanently_missing(&jobs).expect("permanently missing");
        assert!(reason.contains("build"), "{reason}");
        assert!(reason.contains("task 43177"), "{reason}");
        // A run with no jobs claims nothing: there is no evidence either way.
        assert!(logs_permanently_missing(&[]).is_none());
        // Neither does a state `fj` reports as merely unreadable this time.
        let transient = vec![JobLog {
            log_state: "unavailable".to_string(),
            ..missing_job("build", "500 Internal Server Error")
        }];
        assert!(logs_permanently_missing(&transient).is_none());
    }

    // ---- URL / host derivation -------------------------------------------

    #[test]
    fn host_derived_from_html_url() {
        let t = LogTarget::for_run(
            "https://dev.a8n.run/contoso/claude-run/actions/runs/7",
            "contoso/claude-run",
            7,
            None,
        )
        .unwrap();
        assert_eq!(t.host, "dev.a8n.run");
        assert_eq!(t.run_number, 7);
        assert!(t.token.is_none());

        let t =
            LogTarget::for_run("http://localhost:3000/o/r/actions/runs/1", "o/r", 1, None).unwrap();
        assert_eq!(t.host, "localhost:3000", "port preserved");
    }

    #[test]
    fn host_none_when_url_has_no_host() {
        assert!(LogTarget::for_run("", "contoso/claude-run", 7, None).is_none());
    }

    /// This module builds no Forgejo HTTP request; every forge
    /// read shells out to `fj`. Fails if a hand-rolled REST path is
    /// reintroduced here. The needles are split so this test does not match
    /// itself.
    #[test]
    fn this_module_builds_no_forgejo_http_request() {
        let src = include_str!("forge_log.rs");
        for needle in [concat!("api/", "v1"), concat!("req", "west")] {
            assert!(
                !src.contains(needle),
                "`{needle}` is back in forge_log.rs: forge reads go through `fj`, not raw HTTP"
            );
        }
    }

    // ---- runs-list entries from `fj actions runs` --------------------------

    const TEST_REPO: &str = "initech/chat";

    /// A `fj actions runs --json` page carrying the reconciler's full field set.
    /// `head_branch` is fj's alias for Forgejo's `prettyref`.
    fn runs_page_full(runs: &[(i64, i64, &str, &str, &str)]) -> String {
        let items: Vec<String> = runs
            .iter()
            .map(|(id, index, workflow, branch, status)| {
                format!(
                    r#"{{"id":{id},"run_number":{index},"index_in_repo":{index},"workflow_id":"{workflow}","head_branch":{branch},"status":"{status}","conclusion":"{status}","html_url":"https://dev.a8n.run/initech/chat/actions/runs/{index}"}}"#,
                    branch = if branch.is_empty() { "null".to_string() } else { format!("\"{branch}\"") },
                )
            })
            .collect();
        format!(
            r#"{{"total_count":{},"runs":[{}]}}"#,
            items.len(),
            items.join(",")
        )
    }

    #[test]
    fn parse_entries_reads_all_fields_and_null_branch() {
        let body = runs_page_full(&[
            (21099, 4036, "ci.yml", "main", "success"),
            (21077, 4034, "ci.yml", "", "failure"), // head_branch null
        ]);
        let entries = parse_runs_list_entries(&body).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].global_id, 21099);
        assert_eq!(entries[0].workflow_id, "ci.yml");
        assert_eq!(entries[0].branch, "main");
        assert_eq!(entries[0].status, "success"); // conclusion wins over status
        assert!(entries[0].html_url.ends_with("/runs/4036"));
        // A null head_branch parses as an empty branch (log-resolved later).
        assert_eq!(entries[1].branch, "");
        assert_eq!(entries[1].status, "failure");
    }

    #[test]
    fn parse_entries_drops_a_non_branch_prettyref() {
        // Shapes observed on dev.a8n.run: a PR run reports `#<pr-number>` and a
        // ref-deleted push the bare sha. Neither is a branch, so both are left
        // empty and the reconciler resolves the branch from the run's log.
        let body = runs_page_full(&[
            (21099, 4036, "ci.yml", "#87", "failure"),
            (
                21098,
                4035,
                "ci.yml",
                "46ff1b787a53527831e2e36a3263dafd9666eb54",
                "success",
            ),
            (21097, 4034, "ci.yml", "pull/12/head", "failure"),
            (21096, 4033, "ci.yml", "refs/tags/v1.2.3", "success"),
        ]);
        let entries = parse_runs_list_entries(&body).unwrap();
        for e in &entries {
            assert_eq!(e.branch, "", "{e:?}");
        }
        // A real branch still survives, so a push run needs no log read.
        let body = runs_page_full(&[(21100, 4037, "ci.yml", "main", "success")]);
        assert_eq!(parse_runs_list_entries(&body).unwrap()[0].branch, "main");
    }

    /// One run exactly as `fj actions runs --json` emitted it on `dev.a8n.run`
    /// (`fabrikam/forgejo-cli` run 469), minus the bulky `event_payload` /
    /// `repository` / `trigger_user` objects. Pins the parse to real output.
    const REAL_RUNS_PAGE: &str = r#"{"total_count": 469, "runs": [{"run_number": 469, "head_branch": "main", "conclusion": "success", "ScheduleID": 0, "approved_by": 0, "commit_sha": "b08d8bcd98e2fff274c070849793da5a9d3c88a2", "created": "2026-08-01T04:02:34+02:00", "duration": 39000000000, "event": "push", "html_url": "https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/469", "id": 22958, "index_in_repo": 469, "is_fork_pull_request": false, "is_ref_deleted": false, "need_approval": false, "prettyref": "main", "started": "2026-08-01T04:02:35+02:00", "status": "success", "stopped": "2026-08-01T04:03:14+02:00", "title": "Merge pull request 'feat(actions): list workflow runs' (#87) from feat/actions-runs-command into main", "trigger_event": "push", "updated": "2026-08-01T04:03:14+02:00", "workflow_id": "check.yml"}]}"#;

    #[test]
    fn parse_entries_reads_a_real_fj_actions_runs_page() {
        let entries = parse_runs_list_entries(REAL_RUNS_PAGE).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.global_id, 22958);
        assert_eq!(e.index, 469);
        assert_eq!(e.workflow_id, "check.yml");
        assert_eq!(e.branch, "main");
        assert_eq!(e.status, "success");
        assert_eq!(
            e.html_url,
            "https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/469"
        );
    }

    /// A run entry carries the run's stop time, read from
    /// `stopped` with `updated` then `created` as fallbacks; all three absent is
    /// tolerated as an empty (unknown) value rather than a parse failure.
    #[test]
    fn parse_entries_reads_the_run_stop_time_with_fallbacks() {
        let e = &parse_runs_list_entries(REAL_RUNS_PAGE).unwrap()[0];
        assert_eq!(e.stopped, "2026-08-01T04:03:14+02:00");

        let updated = r#"[{"id":1,"index_in_repo":3,"updated":"2026-09-02T02:42:31+02:00","created":"2026-09-02T02:40:00+02:00"}]"#;
        assert_eq!(
            parse_runs_list_entries(updated).unwrap()[0].stopped,
            "2026-09-02T02:42:31+02:00",
            "`updated` is the first fallback"
        );
        let created = r#"[{"id":1,"index_in_repo":3,"created":"2026-09-02T02:40:00+02:00"}]"#;
        assert_eq!(
            parse_runs_list_entries(created).unwrap()[0].stopped,
            "2026-09-02T02:40:00+02:00",
            "`created` is the last fallback"
        );
        let none = r#"[{"id":1,"index_in_repo":3}]"#;
        assert_eq!(
            parse_runs_list_entries(none).unwrap()[0].stopped,
            "",
            "a page carrying no time at all still parses, with no time claimed"
        );
    }

    // ---- one run's start / stop times --------------------------------------

    /// AC: the parse reads `started` / `stopped` off a real
    /// `fj actions runs --json` page, selecting the run by its per-repo number.
    #[test]
    fn parse_run_timing_reads_started_and_stopped() {
        let t = parse_run_timing(REAL_RUNS_PAGE, 469).unwrap();
        assert_eq!(t.started, "2026-08-01T04:02:35+02:00");
        assert_eq!(t.stopped, "2026-08-01T04:03:14+02:00");
    }

    /// A page that carries no such run is an `Err` naming the run, so the caller
    /// logs it and analyzes without the timing.
    #[test]
    fn parse_run_timing_reports_a_missing_run() {
        let e = parse_run_timing(REAL_RUNS_PAGE, 470).unwrap_err();
        assert!(e.contains("no run #470"), "{e}");
        let e = parse_run_timing("not json", 469).unwrap_err();
        assert!(!e.is_empty(), "an unparseable page carries a reason");
    }

    /// A run still in flight reports no stop time: that is an `Err` rather than a
    /// half-empty timing the prompt would render as evidence.
    #[test]
    fn parse_run_timing_rejects_a_run_with_no_stop_time() {
        let body = r#"{"total_count":1,"runs":[{"id":1,"index_in_repo":42,"started":"2026-08-01T04:02:35+02:00","stopped":"","status":"running"}]}"#;
        let e = parse_run_timing(body, 42).unwrap_err();
        assert!(e.contains("no start / stop time"), "{e}");
    }

    #[test]
    fn parse_entries_still_reads_a_bare_array() {
        let entries = parse_runs_list_entries(
            r#"[{"id":7,"index_in_repo":3,"workflow_id":"ci.yml","head_branch":"main","status":"success"}]"#,
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 3);
    }

    // ---- workflow YAML through `fj repo file` -------------------------------

    /// A [`RepoFileClient`] serving canned results keyed by path, recording every
    /// `(path, git_ref)` it was asked for.
    struct StubRepoFile {
        files: HashMap<String, String>,
        asked: Mutex<Vec<(String, String)>>,
    }

    impl StubRepoFile {
        fn new(files: &[(&str, &str)]) -> Self {
            Self {
                files: files
                    .iter()
                    .map(|(p, c)| (p.to_string(), c.to_string()))
                    .collect(),
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl RepoFileClient for StubRepoFile {
        fn read_file(
            &self,
            _host: &str,
            _repo: &str,
            path: &str,
            git_ref: &str,
            _token: Option<&str>,
        ) -> std::result::Result<String, String> {
            self.asked
                .lock()
                .unwrap()
                .push((path.to_string(), git_ref.to_string()));
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| format!("cannot read `{path}`"))
        }
    }

    #[test]
    fn workflow_yaml_tries_dirs_and_returns_first_hit_redacted() {
        // The first dir (.forgejo) misses; the second (.github) resolves. The YAML
        // carries a token that must be redacted in the returned text.
        let yaml =
            "jobs:\n  check:\n    steps:\n      - run: echo ghp_abcdefghij0123456789ABCDEFG\n";
        let client = StubRepoFile::new(&[(".github/workflows/check.yml", yaml)]);
        let out = read_workflow_yaml(
            &client,
            "dev.a8n.run",
            TEST_REPO,
            "a456a957",
            "check.yml",
            Some("test-pat"),
            &[],
        )
        .expect("workflow yaml found");
        assert!(out.contains("jobs:"));
        assert!(
            !out.contains("ghp_abcdefghij0123456789ABCDEFG"),
            "token redacted"
        );
        assert!(out.contains(REDACTED));
        let asked = client.asked.lock().unwrap().clone();
        assert_eq!(asked[0].0, ".forgejo/workflows/check.yml");
        assert_eq!(asked[1].0, ".github/workflows/check.yml");
        assert!(asked.iter().all(|(_, r)| r == "a456a957"), "{asked:?}");
    }

    #[test]
    fn workflow_yaml_all_miss_returns_none() {
        let client = StubRepoFile::new(&[]);
        let out = read_workflow_yaml(
            &client,
            "dev.a8n.run",
            TEST_REPO,
            "a456a957",
            "check.yml",
            Some("test-pat"),
            &[],
        );
        assert!(out.is_none());
        assert_eq!(client.asked.lock().unwrap().len(), WORKFLOW_DIRS.len());
    }

    // ---- AC 3: secret redaction ------------------------------------------

    #[test]
    fn redact_masks_known_secret_value() {
        let secrets = vec!["super-secret-ci-token-value".to_string()];
        let out = redact("publishing with super-secret-ci-token-value now", &secrets);
        assert!(!out.contains("super-secret-ci-token-value"));
        assert!(out.contains(REDACTED));
    }

    #[test]
    fn redact_masks_prefixed_tokens() {
        let cases = [
            "ghp_abcdefghij0123456789ABCDEFGHIJ012345",
            "github_pat_11ABCDE0123456789_aBcDeFgHiJkLmNoP",
            "glpat-abcdef0123456789ABCD",
            "AGE-SECRET-KEY-1QWERTYUIOPASDFGHJKLZXCVBNM0123456789ABCD",
        ];
        for tok in cases {
            let out = redact(&format!("line before\nleaked={tok}\nline after"), &[]);
            assert!(!out.contains(tok), "token not redacted: {tok}");
            assert!(out.contains("line before") && out.contains("line after"));
        }
    }

    #[test]
    fn redact_masks_value_after_auth_keyword() {
        let out = redact("Authorization: Bearer aaaaaaaa1111bbbbbbbb2222", &[]);
        assert!(!out.contains("aaaaaaaa1111bbbbbbbb2222"));
        assert!(out.contains(REDACTED));
    }

    #[test]
    fn redact_leaves_ordinary_prose_alone() {
        // No prefix, no digit-bearing secret-ish value after a keyword.
        let input = "the token implementation was refactored in this commit\nall tests passed";
        assert_eq!(redact(input, &[]), input);
    }

    #[test]
    fn redact_short_known_secret_not_masked() {
        // Below MIN_KNOWN_SECRET_LEN: not masked (avoids nuking common substrings).
        let out = redact("value is abc", &["abc".to_string()]);
        assert_eq!(out, "value is abc");
    }

    // ---- branch parse + resolve ---------------------------------------------

    #[test]
    fn parse_branch_reads_the_checkout_ref() {
        let log = "Run actions/checkout\nGITHUB_REF=refs/heads/feat/CLAUDE-190\nGITHUB_SHA=abc\n";
        assert_eq!(
            parse_branch_from_log(log),
            Some("feat/CLAUDE-190".to_string())
        );
    }

    #[test]
    fn parse_branch_falls_back_to_ref_name() {
        let log = "some line\nGITHUB_REF_NAME=main\nmore\n";
        assert_eq!(parse_branch_from_log(log), Some("main".to_string()));
    }

    #[test]
    fn parse_branch_none_when_absent_or_tag() {
        assert_eq!(parse_branch_from_log("no ref here\ndone\n"), None);
        // A tag ref is not a branch (refs/heads/ only).
        assert_eq!(parse_branch_from_log("GITHUB_REF=refs/tags/v1.2.3\n"), None);
        assert_eq!(
            parse_branch_from_log("GITHUB_REF=refs/tags/v1.2.3\nGITHUB_REF_NAME=v1.2.3\n"),
            None,
            "a tag's short name is not a branch"
        );
    }

    // ---- both log forms crossed with both trigger types ---------------------
    //
    // The `*-map-*` fixtures are the `setupEnv => map[...]` lines captured
    // from stored runner logs (a PR run and a push run), with repo names and
    // addresses replaced by placeholders; the `env-lines-*` fixtures are the
    // line-leading `KEY=value` env dump.

    const MAP_PULL_REQUEST: &str =
        include_str!("../tests/fixtures/forge_log/setupenv-map-pull-request.txt");
    const MAP_PUSH: &str = include_str!("../tests/fixtures/forge_log/setupenv-map-push.txt");
    const ENV_PULL_REQUEST: &str =
        include_str!("../tests/fixtures/forge_log/env-lines-pull-request.txt");
    const ENV_PUSH: &str = include_str!("../tests/fixtures/forge_log/env-lines-push.txt");

    #[test]
    fn parse_branch_reads_the_setupenv_map_form() {
        assert_eq!(parse_branch_from_log(MAP_PUSH), Some("main".to_string()));
        assert_eq!(
            parse_branch_from_log(MAP_PULL_REQUEST),
            Some("release/v0.9.0".to_string())
        );
    }

    #[test]
    fn parse_branch_reads_the_env_line_form() {
        assert_eq!(parse_branch_from_log(ENV_PUSH), Some("main".to_string()));
        assert_eq!(
            parse_branch_from_log(ENV_PULL_REQUEST),
            Some("release/v0.9.0".to_string())
        );
    }

    #[test]
    fn parse_branch_never_returns_a_pr_number_or_pull_ref() {
        for (form, log) in [("map", MAP_PULL_REQUEST), ("env", ENV_PULL_REQUEST)] {
            let branch = parse_branch_from_log(log).unwrap();
            assert_eq!(branch, "release/v0.9.0", "{form} form");
            assert_ne!(
                branch, "468",
                "{form} form: the bare PR number is not a branch"
            );
            assert!(!branch.starts_with("refs/"), "{form} form: {branch}");
        }
        // Without GITHUB_HEAD_REF there is no source branch to recover, and the
        // PR number must not stand in for one.
        assert_eq!(
            parse_branch_from_log("GITHUB_REF=refs/pull/468/head\nGITHUB_REF_NAME=468\n"),
            None
        );
        // Some runners set GITHUB_HEAD_REF to the pull ref instead of the source
        // branch (seen on stored initech/billing-service logs); that is still
        // not a branch.
        assert_eq!(
            parse_branch_from_log(
                "setupEnv => map[GITHUB_HEAD_REF:refs/pull/165/head GITHUB_REF:refs/pull/165/head GITHUB_REF_NAME:165]"
            ),
            None
        );
    }

    #[test]
    fn parse_branch_ignores_non_setupenv_map_lines() {
        // The action-metadata dump on the line above `setupEnv` is also a Go
        // map; only the `setupEnv` one carries the run's environment.
        assert!(MAP_PUSH.contains("Read action &{Checkout"));
        assert_eq!(parse_branch_from_log(MAP_PUSH), Some("main".to_string()));
    }

    #[test]
    fn resolve_branch_prefers_run_detail_then_log() {
        // Run detail populated -> used verbatim.
        assert_eq!(
            resolve_branch(Some("release"), "GITHUB_REF=refs/heads/main\n"),
            Some("release".to_string())
        );
        // The deployed Forgejo case: head_branch null/None -> log-parse fallback.
        assert_eq!(
            resolve_branch(None, "GITHUB_REF=refs/heads/main\n"),
            Some("main".to_string())
        );
        // Empty run-detail branch also falls back to the log.
        assert_eq!(
            resolve_branch(Some("  "), "GITHUB_REF=refs/heads/main\n"),
            Some("main".to_string())
        );
    }

    // ---- failing-job selection over the fj job list -------------------------

    #[test]
    fn resolve_failing_job_picks_the_failing_one() {
        let jobs = vec![
            job("build", "success", ""),
            job("test", "failure", "boom"),
            job("lint", "waiting", ""),
        ];
        let failing = resolve_failing_job(&jobs).expect("a job");
        assert_eq!(failing.name, "test");
        assert_eq!(failing.log, "boom");
    }

    #[test]
    fn resolve_failing_job_defaults_to_first_when_none_failed() {
        let jobs = vec![job("only", "success", "")];
        assert_eq!(resolve_failing_job(&jobs).unwrap().name, "only");
        // Empty list -> no job.
        assert!(resolve_failing_job(&[]).is_none());
    }

    #[test]
    fn resolve_failing_job_matches_every_failure_spelling() {
        for status in ["failure", "FAILED", "error"] {
            let jobs = vec![job("build", "success", ""), job("test", status, "")];
            assert_eq!(resolve_failing_job(&jobs).unwrap().name, "test", "{status}");
        }
    }

    // ---- run-level verdict over the job list ---------------------------------

    #[test]
    fn jobs_verdict_all_cancelled_is_a_stopped_run() {
        let jobs = vec![job("build", "cancelled", ""), job("test", "cancelled", "")];
        let v = JobsVerdict::resolve(None, &jobs);
        assert_eq!(
            v,
            JobsVerdict::NoFailure {
                statuses: vec!["cancelled".to_string(), "cancelled".to_string()],
            }
        );
        assert!(v.stopped());
        assert_eq!(v.statuses_display(), "cancelled, cancelled");
    }

    #[test]
    fn jobs_verdict_all_skipped_is_a_stopped_run() {
        let jobs = vec![job("build", "SKIPPED", "")];
        let v = JobsVerdict::resolve(None, &jobs);
        assert!(v.stopped(), "status match is case-insensitive");
    }

    #[test]
    fn jobs_verdict_cancelled_plus_a_failure_is_a_failure() {
        let jobs = vec![
            job("build", "cancelled", ""),
            job("test", "failure", "boom"),
            job("lint", "cancelled", ""),
        ];
        let v = JobsVerdict::resolve(None, &jobs);
        assert_eq!(
            v,
            JobsVerdict::Failed {
                name: "test".to_string(),
                index: 1,
            }
        );
        assert!(!v.stopped());
    }

    #[test]
    fn jobs_verdict_all_success_is_no_failure_but_not_stopped() {
        let jobs = vec![job("build", "success", ""), job("test", "success", "")];
        let v = JobsVerdict::resolve(None, &jobs);
        assert!(matches!(v, JobsVerdict::NoFailure { .. }));
        // Not in the skip set, so this still mints and the statuses are logged.
        assert!(!v.stopped());
        assert_eq!(v.statuses_display(), "success, success");
    }

    #[test]
    fn jobs_verdict_empty_list_is_not_stopped() {
        let v = JobsVerdict::resolve(None, &[]);
        assert_eq!(
            v,
            JobsVerdict::NoFailure {
                statuses: Vec::new()
            }
        );
        // A run that resolved with no jobs at all says nothing about a cancel,
        // so it must not be swallowed: fail open and mint.
        assert!(!v.stopped());
    }

    #[test]
    fn jobs_verdict_unresolved_endpoint_is_unknown_and_never_stopped() {
        // The four ways `consume_events` ends up with no job list: a transient
        // miss, a rejected token, a fetch fault, and a panicked fetch task.
        for reason in [
            "log not available after retries: run not found",
            "log token rejected (401)",
            "log fetch failed: fj not on PATH",
            "log fetch task panicked: task panicked",
        ] {
            let v = JobsVerdict::resolve(Some(reason), &[]);
            assert_eq!(
                v,
                JobsVerdict::Unknown {
                    reason: reason.to_string(),
                }
            );
            assert!(!v.stopped(), "{reason}");
            assert_eq!(v.statuses_display(), "");
        }
        // A reason wins even if a job list somehow came with it.
        let v = JobsVerdict::resolve(
            Some("log token rejected (403)"),
            &[job("b", "cancelled", "")],
        );
        assert!(matches!(v, JobsVerdict::Unknown { .. }));
        assert!(!v.stopped());
    }
}
