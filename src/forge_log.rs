//! Forgejo Action log fetch with retry and secret redaction (CLAUDE-44).
//!
//! A run's logs are read through the `fj` CLI (CLAUDE-204):
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
//! ## Auth (CLAUDE-92 / CLAUDE-96 / CLAUDE-200)
//!
//! A private repo's read is authorized by the `log_token_ref` mapped to that
//! repo. The resolved PAT reaches the `fj` child through a temp single-host
//! `keys.json` ([`crate::provider::cli::FjActionLogs`]), never a process
//! argument and never a log line. A public repo's run reads without a token, so
//! a deployment that configures no token still analyzes public failures.
//! `read:repository` is the only scope needed: the three endpoints `fj` calls
//! all sit in Forgejo's repository group behind `reqRepoReader(unit.TypeActions)`.
//!
//! ## Endpoints still on HTTP
//!
//! `fj` has no command for the runs list or for reading a raw file at a ref, so
//! [`fetch_runs`] and [`fetch_workflow_yaml`] keep the [`LogTransport`] seam and
//! their direct REST calls, each with a `TODO` naming its tracking issue.

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

/// Transport seam for the two REST reads `fj` has no command for. Abstracted so
/// the parsing is unit-testable without a network (mirrors the `McpTransport`
/// seam in [`crate::provider::github`]).
pub trait LogTransport: Send + Sync {
    /// GET `url`, optionally bearing `token`. Returns the status + raw body on a
    /// completed request, or a human-readable error string on a transport
    /// failure (DNS, connect, TLS, ...).
    fn get_bytes(
        &self,
        url: &str,
        token: Option<&str>,
    ) -> std::result::Result<(u16, Vec<u8>), String>;
}

/// Real transport: a blocking `reqwest` GET run on a dedicated thread so it
/// never blocks the async runtime (same pattern as the GitHub MCP transport).
#[derive(Debug, Default, Clone)]
pub struct HttpLogTransport;

impl HttpLogTransport {
    pub fn new() -> Self {
        Self
    }
}

impl LogTransport for HttpLogTransport {
    fn get_bytes(
        &self,
        url: &str,
        token: Option<&str>,
    ) -> std::result::Result<(u16, Vec<u8>), String> {
        let url = url.to_string();
        let token = token.map(str::to_string);
        std::thread::spawn(move || -> std::result::Result<(u16, Vec<u8>), String> {
            let client = reqwest::blocking::Client::builder()
                .build()
                .map_err(|e| format!("log http client: {e}"))?;
            let mut req = client.get(&url);
            if let Some(t) = &token {
                req = req.header("Authorization", format!("token {t}"));
            }
            let resp = req
                .send()
                .map_err(|e| format!("log request to {url}: {e}"))?;
            let status = resp.status().as_u16();
            let body = resp
                .bytes()
                .map_err(|e| format!("log response body: {e}"))?
                .to_vec();
            Ok((status, body))
        })
        .join()
        .map_err(|_| "log transport thread panicked".to_string())?
    }
}

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
/// [`crate::provider::cli::FjActionLogs`]. Abstracted so the retry / redaction
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
    /// One call, no backoff. For the polling actions (CLAUDE-189 / CLAUDE-191),
    /// which read runs that finished long ago: a run `fj` cannot see there is
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
pub fn fetch_run_log<S: FnMut(Duration)>(
    client: &dyn ActionLogClient,
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
fn host_from_html_url(url: &str) -> Option<String> {
    let after = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = after.split('/').next().unwrap_or("");
    (!host.is_empty()).then(|| host.to_string())
}

/// Derive the forge origin (scheme + host[:port]) from a run's `html_url` by
/// stripping the `/{repo}/...` tail, for the two endpoints still on HTTP.
/// Returns `None` if `html_url` does not contain the repo segment.
pub fn base_from_html_url(html_url: &str, repo: &str) -> Option<String> {
    let needle = format!("/{repo}/");
    html_url
        .split_once(&needle)
        .map(|(base, _)| base.to_string())
        .filter(|b| !b.is_empty())
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

/// Number of runs requested per page and the paging ceiling. `50 * 40 = 2000`
/// most-recent runs are scanned, enough to reach a recently-minted "CI failure"
/// issue's run; a run older than that resolves `NotFound` and the caller notes
/// the log as unavailable rather than crashing.
const RUNS_PAGE_LIMIT: u32 = 50;

/// One page of the API runs-list endpoint
/// (`GET /api/v1/repos/{owner}/{repo}/actions/runs`), which lives on the API
/// router where PATs work.
fn runs_url(base_url: &str, repo: &str, page: u32, limit: u32) -> String {
    format!(
        "{}/api/v1/repos/{}/actions/runs?page={}&limit={}",
        base_url.trim_end_matches('/'),
        repo,
        page,
        limit,
    )
}

/// CLAUDE-191: one run pulled from the runs-list JSON with the full set of fields
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
}

/// CLAUDE-191: parse the runs-list body into [`RunEntry`]s, tolerant of Forgejo's
/// field naming (same shape as [`parse_runs_list`], plus the workflow / branch /
/// status / url fields). Runs missing an `id` are skipped. The branch is empty
/// when `head_branch` is null (the reconciler then resolves it from the log).
fn parse_runs_list_entries(bytes: &[u8]) -> std::result::Result<Vec<RunEntry>, String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let arr = value
        .get("workflow_runs")
        .and_then(|v| v.as_array())
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default();
    let str_field = |run: &serde_json::Value, keys: &[&str]| -> String {
        keys.iter()
            .find_map(|k| run.get(*k).and_then(serde_json::Value::as_str))
            .filter(|s| !s.is_empty())
            .unwrap_or("")
            .to_string()
    };
    let mut out = Vec::new();
    for run in arr {
        let Some(global_id) = run.get("id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        let index = ["index_in_repo", "index", "run_number", "number"]
            .iter()
            .find_map(|k| run.get(*k).and_then(serde_json::Value::as_i64))
            .unwrap_or(0);
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
            branch: str_field(&run, &["head_branch", "head_ref", "branch"]),
            status,
            html_url: str_field(&run, &["html_url", "url"]),
        });
    }
    Ok(out)
}

/// CLAUDE-191: outcome of fetching the runs list. Auth-class / not-found responses
/// on the FIRST page are terminal; a mid-paging error stops paging and returns the
/// entries already collected (partial coverage is better than none for a poll).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunsListOutcome {
    Resolved(Vec<RunEntry>),
    AuthDenied(u16),
    NotFound,
    Failed(String),
}

/// CLAUDE-191: page the repo's Action runs list into [`RunEntry`]s, newest-first,
/// up to `max_pages` pages of [`RUNS_PAGE_LIMIT`] each (or until a short/empty
/// page, the last one). Terminal on 401/403/404 on the first page; once some
/// entries are collected, a later-page error stops paging and returns them.
///
/// TODO(FJ-66): stays on the REST API because `fj` has no `actions runs` command
/// to list a repo's workflow runs. Move it onto the CLI once FJ-66 ships.
pub fn fetch_runs(
    transport: &dyn LogTransport,
    base_url: &str,
    repo: &str,
    token: &str,
    max_pages: u32,
) -> Result<RunsListOutcome> {
    let mut all: Vec<RunEntry> = Vec::new();
    for page in 1..=max_pages {
        let url = runs_url(base_url, repo, page, RUNS_PAGE_LIMIT);
        match transport.get_bytes(&url, Some(token)) {
            Ok((200, bytes)) => {
                let entries = match parse_runs_list_entries(&bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        if !all.is_empty() {
                            break;
                        }
                        return Ok(RunsListOutcome::Failed(format!("parsing runs list: {e}")));
                    }
                };
                let short = entries.len() < RUNS_PAGE_LIMIT as usize;
                all.extend(entries);
                if short {
                    break;
                }
            }
            Ok((status @ (401 | 403), _)) => {
                if !all.is_empty() {
                    break;
                }
                return Ok(RunsListOutcome::AuthDenied(status));
            }
            Ok((404, _)) => {
                if !all.is_empty() {
                    break;
                }
                return Ok(RunsListOutcome::NotFound);
            }
            Ok((status, body)) => {
                if !all.is_empty() {
                    break;
                }
                return Ok(RunsListOutcome::Failed(format!(
                    "status {status}, {} body bytes",
                    body.len()
                )));
            }
            Err(e) => {
                if !all.is_empty() {
                    break;
                }
                return Ok(RunsListOutcome::Failed(e));
            }
        }
    }
    Ok(RunsListOutcome::Resolved(all))
}

/// CLAUDE-189: candidate repo paths a failing workflow file may live under, tried
/// in order. `workflow_id` is the payload's workflow filename (e.g. `check.yml`).
const WORKFLOW_DIRS: &[&str] = &[
    ".forgejo/workflows",
    ".github/workflows",
    ".gitea/workflows",
];

/// CLAUDE-189: fetch the failing workflow's YAML from the repo at the failing
/// commit, read-only, so the analysis can map log position to workflow step
/// order. Tries each known workflow directory via the API raw-file endpoint
/// (`GET /api/v1/repos/{owner}/{repo}/raw/{path}?ref={commit}`) and returns the
/// first that resolves 200, redacted. `None` when none resolve (the analysis
/// proceeds over the log alone and says the YAML was unavailable). Never errors
/// on a miss.
///
/// TODO(FJ-67): stays on the REST API because `fj` has no command to read a raw
/// file at a ref. Move it onto the CLI once FJ-67 ships.
pub fn fetch_workflow_yaml(
    transport: &dyn LogTransport,
    base_url: &str,
    repo: &str,
    commit: &str,
    workflow_id: &str,
    token: &str,
    known_secrets: &[String],
) -> Option<String> {
    let base = base_url.trim_end_matches('/');
    // A workflow_id already carrying a path segment is tried verbatim first.
    let mut candidates: Vec<String> = Vec::new();
    if workflow_id.contains('/') {
        candidates.push(workflow_id.to_string());
    }
    for dir in WORKFLOW_DIRS {
        candidates.push(format!("{dir}/{workflow_id}"));
    }
    for path in candidates {
        let url = format!("{base}/api/v1/repos/{repo}/raw/{path}?ref={commit}");
        if let Ok((200, bytes)) = transport.get_bytes(&url, Some(token)) {
            let text = String::from_utf8_lossy(&bytes).to_string();
            if !text.trim().is_empty() {
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

/// CLAUDE-199: collect the ref fields from both forms a runner log uses: a
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

/// CLAUDE-190/199: parse the failing run's branch from a redacted job log's
/// environment dump. The run-detail API returns `head_branch: null` on the
/// deployed Forgejo, so the log is the reliable source.
///
/// Precedence: `GITHUB_HEAD_REF` (the only field carrying the source branch of
/// a PR run), else `GITHUB_REF`, else `GITHUB_REF_NAME`. A PR run's
/// `GITHUB_REF` is `refs/pull/N/head` and its `GITHUB_REF_NAME` the bare PR
/// number, so the last fallback is skipped when `GITHUB_REF` names a pull or
/// tag ref rather than returning a number or a tag name as a branch. `None`
/// when the log carries no readable ref (truncated / diagnostic-free /
/// logless).
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

/// CLAUDE-190: resolve the branch, preferring a non-empty run-detail `head_branch`
/// and falling back to the log environment dump. On the deployed Forgejo run
/// detail returns `head_branch: null`, so in practice this is the log parse; the
/// run-detail arm keeps the resolver correct where the field IS populated.
pub fn resolve_branch(run_detail_branch: Option<&str>, log: &str) -> Option<String> {
    match run_detail_branch {
        Some(b) if !b.trim().is_empty() => Some(b.trim().to_string()),
        _ => parse_branch_from_log(log),
    }
}

/// CLAUDE-190/204: the failing job of a run, selected from the jobs `fj actions
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
    use std::collections::VecDeque;
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

    // ---- CLAUDE-204: `fj actions logs` fetch ------------------------------

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

    #[test]
    fn base_url_derived_for_the_http_endpoints() {
        assert_eq!(
            base_from_html_url(
                "https://dev.a8n.run/contoso/claude-run/actions/runs/7",
                "contoso/claude-run"
            )
            .as_deref(),
            Some("https://dev.a8n.run")
        );
        assert!(
            base_from_html_url("https://dev.a8n.run/other/repo", "contoso/claude-run").is_none()
        );
    }

    // ---- CLAUDE-191: full runs-list entries + paging (still on HTTP) -------

    /// One queued bytes response: `(status, body)` or a transport error.
    type BytesResponse = std::result::Result<(u16, Vec<u8>), String>;

    /// A bytes transport that serves queued responses in request order and
    /// records the URLs, for the two endpoints `fj` has no command for.
    struct SeqBytesTransport {
        responses: Mutex<VecDeque<BytesResponse>>,
        urls: Mutex<Vec<String>>,
    }

    impl SeqBytesTransport {
        fn new(responses: Vec<BytesResponse>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                urls: Mutex::new(Vec::new()),
            }
        }
        fn urls(&self) -> Vec<String> {
            self.urls.lock().unwrap().clone()
        }
    }

    impl LogTransport for SeqBytesTransport {
        fn get_bytes(
            &self,
            url: &str,
            token: Option<&str>,
        ) -> std::result::Result<(u16, Vec<u8>), String> {
            assert_eq!(token, Some("test-pat"), "API fetch must send the token");
            self.urls.lock().unwrap().push(url.to_string());
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok((200, br#"{"workflow_runs":[]}"#.to_vec())))
        }
    }

    const TEST_BASE: &str = "https://dev.a8n.run";
    const TEST_REPO: &str = "initech/chat";
    const TEST_MAX_PAGES: u32 = 40;

    /// A runs-list page carrying the reconciler's full field set.
    fn runs_page_full(runs: &[(i64, i64, &str, &str, &str)]) -> Vec<u8> {
        let items: Vec<String> = runs
            .iter()
            .map(|(id, index, workflow, branch, status)| {
                format!(
                    r#"{{"id":{id},"index_in_repo":{index},"workflow_id":"{workflow}","head_branch":{branch},"status":"completed","conclusion":"{status}","html_url":"https://dev.a8n.run/initech/chat/actions/runs/{index}"}}"#,
                    branch = if branch.is_empty() { "null".to_string() } else { format!("\"{branch}\"") },
                )
            })
            .collect();
        format!(r#"{{"workflow_runs":[{}]}}"#, items.join(",")).into_bytes()
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
    fn fetch_runs_collects_across_pages_until_short() {
        // A full first page (50 entries) then a short second page ends paging.
        let full: Vec<(i64, i64, &str, &str, &str)> = (0..50)
            .map(|n| (90000 + n, 5000 + n, "ci.yml", "main", "success"))
            .collect();
        let page1 = runs_page_full(&full);
        let page2 = runs_page_full(&[(21077, 4034, "ci.yml", "feature", "failure")]);
        let transport = SeqBytesTransport::new(vec![Ok((200, page1)), Ok((200, page2))]);
        let out = fetch_runs(&transport, TEST_BASE, TEST_REPO, "test-pat", TEST_MAX_PAGES).unwrap();
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved");
        };
        assert_eq!(entries.len(), 51);
        assert_eq!(transport.urls().len(), 2, "stopped at the short page");
        assert!(
            transport.urls()[0].contains("/api/v1/repos/initech/chat/actions/runs?page=1")
        );
    }

    #[test]
    fn fetch_runs_auth_denied_on_first_page_is_terminal() {
        let transport = SeqBytesTransport::new(vec![Ok((403, Vec::new()))]);
        let out = fetch_runs(&transport, TEST_BASE, TEST_REPO, "test-pat", TEST_MAX_PAGES).unwrap();
        assert_eq!(out, RunsListOutcome::AuthDenied(403));
    }

    #[test]
    fn fetch_runs_error_after_first_page_returns_collected() {
        // Page 1 full (so paging continues), page 2 errors: keep page-1 entries.
        let full: Vec<(i64, i64, &str, &str, &str)> = (0..50)
            .map(|n| (90000 + n, 5000 + n, "ci.yml", "main", "success"))
            .collect();
        let page1 = runs_page_full(&full);
        let transport =
            SeqBytesTransport::new(vec![Ok((200, page1)), Err("connect refused".to_string())]);
        let out = fetch_runs(&transport, TEST_BASE, TEST_REPO, "test-pat", TEST_MAX_PAGES).unwrap();
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved with the collected page-1 entries");
        };
        assert_eq!(entries.len(), 50);
    }

    // ---- CLAUDE-189: workflow YAML fetch (still on HTTP) -----------------

    #[test]
    fn workflow_yaml_tries_dirs_and_returns_first_hit_redacted() {
        // First dir (.forgejo) 404s; second (.github) resolves. The YAML carries a
        // token that must be redacted in the returned text.
        let yaml =
            "jobs:\n  check:\n    steps:\n      - run: echo ghp_abcdefghij0123456789ABCDEFG\n";
        let transport = SeqBytesTransport::new(vec![
            Ok((404, Vec::new())),
            Ok((200, yaml.as_bytes().to_vec())),
        ]);
        let out = fetch_workflow_yaml(
            &transport,
            TEST_BASE,
            TEST_REPO,
            "a456a957",
            "check.yml",
            "test-pat",
            &[],
        )
        .expect("workflow yaml found");
        assert!(out.contains("jobs:"));
        assert!(
            !out.contains("ghp_abcdefghij0123456789ABCDEFG"),
            "token redacted"
        );
        assert!(out.contains(REDACTED));
        let urls = transport.urls();
        assert!(urls[0].contains(".forgejo/workflows/check.yml"));
        assert!(urls[1].contains(".github/workflows/check.yml"));
        assert!(urls[1].contains("ref=a456a957"));
    }

    #[test]
    fn workflow_yaml_all_miss_returns_none() {
        let transport = SeqBytesTransport::new(vec![
            Ok((404, Vec::new())),
            Ok((404, Vec::new())),
            Ok((404, Vec::new())),
        ]);
        let out = fetch_workflow_yaml(
            &transport,
            TEST_BASE,
            TEST_REPO,
            "a456a957",
            "check.yml",
            "test-pat",
            &[],
        );
        assert!(out.is_none());
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

    // ---- CLAUDE-190: branch parse + resolve -------------------------------

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

    // ---- CLAUDE-199: both log forms crossed with both trigger types -------
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

    // ---- CLAUDE-204: failing-job selection over the fj job list -----------

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
}
