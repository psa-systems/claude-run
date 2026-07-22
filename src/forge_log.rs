//! Forgejo Action log fetch with retry and secret redaction (CLAUDE-44).
//!
//! The sanctioned REST log API (FJ-22 / Forgejo PR 12666) is not in the deployed
//! Forgejo, so a failing job's log is pulled from the internal web route:
//!
//! ```text
//! GET /{owner}/{repo}/actions/runs/{index}/jobs/{job}/attempt/{n}/logs
//! ```
//!
//! which returns `200 text/plain`. The `/attempt/{n}/` segment is required
//! (omitting it 404s). Logs are pruned after a retention window, and a freshly
//! finished run can briefly 404 / return `resource does not exist` before its
//! logs flush, so the fetch retries with exponential backoff and a persistent
//! miss is surfaced as a *transient* outcome ([`LogOutcome::TransientMiss`]),
//! never an error or a panic (triage bucket 3 in the epic). Before any value is
//! returned or written to disk the text is run through [`redact`] so CI tokens
//! (a failed publish step can echo one) do not land in storage or a YouTrack
//! attachment.
//!
//! This internal route stays off-limits to the `fj` CLI (FJ-22) until PR 12666
//! ships a REST endpoint; it is sanctioned for this service only.
//!
//! ## Private-repo auth (CLAUDE-92 / CLAUDE-96)
//!
//! The internal route needs no auth for a *public* repo and accepts ONLY a
//! browser session for a private one: `services/auth/{basic,oauth2}.go`
//! `Verify` both bail on non-API web paths (which is why an
//! `Authorization: token` probe 404'd), so [`LogTarget::token`] stays `None`.
//! Private repos are served by [`fetch_run_logs_api`] instead: the v16 REST
//! endpoint (`/api/v1/.../actions/runs/{run_id}/logs`, Forgejo PR 12666, on
//! the API router where PATs work), addressed by the run's GLOBAL id and
//! authenticated with the webhook source's `log_token_ref`. On a pre-v16
//! instance that endpoint 404s and the consumer degrades to the CLAUDE-92
//! no-log human-action outcome after exactly one request.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::Result;

/// Job index probed for a run. The fast-path targets single-job CI workflows
/// (the pilot), whose failing job is index 0. Multi-job resolution is a future
/// refinement; documented as an explicit assumption per the issue.
pub const DEFAULT_JOB_INDEX: u64 = 0;

/// Attempt number probed for a run. A run's first execution is attempt 1; a
/// re-run increments it. The webhook payload carries no attempt, so the
/// first-run value is assumed.
pub const DEFAULT_ATTEMPT: u64 = 1;

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

/// A single HTTP response from the log route: status code plus the body text.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

/// Transport seam for the GET against the internal log route. Abstracted so the
/// retry/redaction logic is unit-testable without a network (mirrors the
/// `McpTransport` seam in [`crate::provider::github`]).
pub trait LogTransport: Send + Sync {
    /// GET `url`, optionally bearing `token`. Returns the status + body on a
    /// completed request, or a human-readable error string on a transport
    /// failure (DNS, connect, TLS, ...). The retry loop treats a transport
    /// error the same as a not-ready body: retry, then surface as transient.
    fn get(&self, url: &str, token: Option<&str>) -> std::result::Result<HttpResponse, String>;

    /// CLAUDE-96: GET returning raw bytes, for the v16 REST run-logs endpoint
    /// (`application/zip`). The default impl adapts [`LogTransport::get`] so
    /// text-only test stubs keep compiling; the real transport overrides it
    /// with a binary-safe request.
    fn get_bytes(
        &self,
        url: &str,
        token: Option<&str>,
    ) -> std::result::Result<(u16, Vec<u8>), String> {
        self.get(url, token)
            .map(|r| (r.status, r.body.into_bytes()))
    }
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
    fn get(&self, url: &str, token: Option<&str>) -> std::result::Result<HttpResponse, String> {
        let url = url.to_string();
        let token = token.map(str::to_string);
        std::thread::spawn(move || -> std::result::Result<HttpResponse, String> {
            let client = reqwest::blocking::Client::builder()
                .build()
                .map_err(|e| format!("log http client: {e}"))?;
            let mut req = client.get(&url).header("Accept", "text/plain");
            if let Some(t) = &token {
                // The day private-repo auth is resolved (see module docs) this is
                // the header to populate; today it is never set for the pilot.
                req = req.header("Authorization", format!("token {t}"));
            }
            let resp = req
                .send()
                .map_err(|e| format!("log request to {url}: {e}"))?;
            let status = resp.status().as_u16();
            let body = resp.text().map_err(|e| format!("log response body: {e}"))?;
            Ok(HttpResponse { status, body })
        })
        .join()
        .map_err(|_| "log transport thread panicked".to_string())?
    }

    /// Binary-safe variant for the v16 REST run-logs ZIP (CLAUDE-96).
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

/// Where to fetch a failing job's log from.
#[derive(Debug, Clone)]
pub struct LogTarget {
    /// Forge origin, e.g. `https://dev.a8n.run` (no trailing slash needed).
    pub base_url: String,
    /// `owner/repo`.
    pub repo: String,
    /// Run index within the repo (`index_in_repo` from the webhook).
    pub run_index: i64,
    /// Job index within the run.
    pub job_index: u64,
    /// Attempt number.
    pub attempt: u64,
    /// Token for a private repo. `None` for a public repo (the pilot). See the
    /// module docs: private-repo auth on this route is an unresolved follow-up.
    pub token: Option<String>,
}

impl LogTarget {
    /// Build a target for `(repo, run_index)` from the failure event's
    /// `html_url`, deriving the forge origin from it. Defaults to the first job
    /// and first attempt (see [`DEFAULT_JOB_INDEX`] / [`DEFAULT_ATTEMPT`]).
    /// Returns `None` when the origin cannot be derived from `html_url`.
    pub fn for_run(html_url: &str, repo: &str, run_index: i64) -> Option<Self> {
        let base_url = base_from_html_url(html_url, repo)?;
        Some(Self {
            base_url,
            repo: repo.to_string(),
            run_index,
            job_index: DEFAULT_JOB_INDEX,
            attempt: DEFAULT_ATTEMPT,
            token: None,
        })
    }

    /// The internal log-route URL for this target.
    pub fn logs_url(&self) -> String {
        format!(
            "{}/{}/actions/runs/{}/jobs/{}/attempt/{}/logs",
            self.base_url.trim_end_matches('/'),
            self.repo,
            self.run_index,
            self.job_index,
            self.attempt,
        )
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

/// A successfully fetched and redacted log.
#[derive(Debug, Clone)]
pub struct RedactedLog {
    /// Redacted log text, usable in a summary.
    pub text: String,
    /// Path to the redacted log written to disk, for a YouTrack attachment (one
    /// job can exceed an inline-comment size limit).
    pub artifact: PathBuf,
}

/// Outcome of a fetch. A persistent miss is `TransientMiss`, not an error, so
/// the caller treats it as "logs not available yet / pruned" rather than a fault.
#[derive(Debug, Clone)]
pub enum LogOutcome {
    Fetched(RedactedLog),
    TransientMiss { reason: String },
}

/// One classified attempt: the log is ready, or it is not (missing / pruned /
/// not-yet-flushed / a transport error), with a reason for the latter.
enum Probe {
    Ready(String),
    NotReady(String),
}

/// Fetch the failing job's log for `target`, retrying on not-ready logs, redact
/// it, and write the redacted text to an artifact file under `out_dir`. Returns
/// [`LogOutcome::Fetched`] on success or [`LogOutcome::TransientMiss`] after the
/// retries are exhausted. `known_secrets` are masked verbatim in addition to the
/// pattern-based redaction (pass the values the service itself knows leaked).
///
/// `sleep` is injected so tests drive the backoff without waiting; production
/// passes `std::thread::sleep`. The only hard error this returns is an I/O
/// failure writing the artifact; a missing log is a `TransientMiss`, never `Err`.
pub fn fetch_failing_job_log<S: FnMut(Duration)>(
    transport: &dyn LogTransport,
    cfg: &LogFetchConfig,
    target: &LogTarget,
    out_dir: &Path,
    known_secrets: &[String],
    mut sleep: S,
) -> Result<LogOutcome> {
    let url = target.logs_url();
    let mut backoff = cfg.initial_backoff;
    let mut last_reason = "no attempt made".to_string();

    for attempt_no in 0..=cfg.max_retries {
        let resp = transport.get(&url, target.token.as_deref());
        match classify(resp) {
            Probe::Ready(body) => {
                let text = redact(&body, known_secrets);
                let artifact = write_artifact(out_dir, target, &text)?;
                return Ok(LogOutcome::Fetched(RedactedLog { text, artifact }));
            }
            Probe::NotReady(reason) => {
                last_reason = reason;
                if attempt_no < cfg.max_retries {
                    sleep(backoff);
                    backoff = (backoff * 2).min(cfg.max_backoff);
                }
            }
        }
    }

    Ok(LogOutcome::TransientMiss {
        reason: last_reason,
    })
}

/// Classify a transport result. A `200` with a non-empty body that is not the
/// not-found sentinel is ready; everything else (non-200, empty body, the
/// `resource does not exist` sentinel, or a transport error) is not-ready.
fn classify(resp: std::result::Result<HttpResponse, String>) -> Probe {
    match resp {
        Ok(r) if r.status == 200 && is_ready_body(&r.body) => Probe::Ready(r.body),
        Ok(r) => Probe::NotReady(format!(
            "status {}, {} body bytes (logs not ready or pruned)",
            r.status,
            r.body.len()
        )),
        Err(e) => Probe::NotReady(format!("transport error: {e}")),
    }
}

/// True when a body is real log content (non-empty and not the route's
/// `resource does not exist` miss sentinel).
fn is_ready_body(body: &str) -> bool {
    let t = body.trim();
    !t.is_empty() && !t.to_ascii_lowercase().contains("resource does not exist")
}

/// Derive the forge origin (scheme + host[:port]) from a run's `html_url` by
/// stripping the `/{repo}/...` tail. Returns `None` if `html_url` does not
/// contain the repo segment.
fn base_from_html_url(html_url: &str, repo: &str) -> Option<String> {
    let needle = format!("/{repo}/");
    html_url
        .split_once(&needle)
        .map(|(base, _)| base.to_string())
        .filter(|b| !b.is_empty())
}

/// Write `text` to a per-(repo, run, job, attempt) artifact file under `out_dir`
/// and return its path. `out_dir` is created if absent.
fn write_artifact(out_dir: &Path, target: &LogTarget, text: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stem = target.repo.replace('/', "_");
    let name = format!(
        "{stem}-run{}-job{}-attempt{}.log",
        target.run_index, target.job_index, target.attempt
    );
    let path = out_dir.join(name);
    std::fs::write(&path, text)?;
    Ok(path)
}

/// CLAUDE-96: where to fetch a PRIVATE repo's action logs from - the v16 REST
/// endpoint (`GET /api/v1/repos/{owner}/{repo}/actions/runs/{run_id}/logs`,
/// Forgejo PR 12666), which honors `Authorization: token <PAT>` because it
/// lives on the API router (unlike the internal web route, which is
/// session-only). `{run_id}` is the run's GLOBAL id (`GetRunByID` in the
/// upstream handler), i.e. the payload's `run.id`, NOT `index_in_repo`.
#[derive(Debug, Clone)]
pub struct RunLogsApiTarget {
    /// Forge origin, e.g. `https://dev.a8n.run`.
    pub base_url: String,
    /// `owner/repo`.
    pub repo: String,
    /// Run index within the repo; used only for the artifact filename so it
    /// sorts next to the internal-route artifacts.
    pub run_index: i64,
    /// The run's global id from the payload (`run.id`).
    pub run_global_id: i64,
    /// PAT with repository read scope (resolved from the source's
    /// `log_token_ref`).
    pub token: String,
}

impl RunLogsApiTarget {
    /// Build a target from the failure event, deriving the forge origin from
    /// `html_url` like [`LogTarget::for_run`]. `None` when the origin cannot be
    /// derived.
    pub fn for_run(
        html_url: &str,
        repo: &str,
        run_index: i64,
        run_global_id: i64,
        token: String,
    ) -> Option<Self> {
        let base_url = base_from_html_url(html_url, repo)?;
        Some(Self {
            base_url,
            repo: repo.to_string(),
            run_index,
            run_global_id,
            token,
        })
    }

    /// The REST run-logs URL for this target.
    pub fn logs_url(&self) -> String {
        format!(
            "{}/api/v1/repos/{}/actions/runs/{}/logs",
            self.base_url.trim_end_matches('/'),
            self.repo,
            self.run_global_id,
        )
    }
}

/// CLAUDE-96: outcome of a REST run-logs fetch. Auth-class and not-found
/// responses are terminal after ONE request (the event arrives after the run
/// completed, so there is nothing to wait for); only transport errors and 5xx
/// retry within the configured budget.
#[derive(Debug, Clone)]
pub enum ApiLogOutcome {
    Fetched(RedactedLog),
    /// 401/403: the token was rejected.
    AuthDenied(u16),
    /// 404: the endpoint is absent (Forgejo < 16) or the run was pruned.
    NotFound,
    /// Transport/5xx after the retry budget, or a corrupt archive.
    Failed(String),
}

/// CLAUDE-96: fetch the run's logs through the v16 REST endpoint. The 200 body
/// is a ZIP with one entry per job (`{job-name}-{job-id}-attempt-{N}.log`); the
/// entries are concatenated (sorted by name, each under a `===== name =====`
/// header), redacted, and written to one artifact. Exactly one request decides
/// 200/401/403/404; only transport errors and other statuses retry.
pub fn fetch_run_logs_api<S: FnMut(Duration)>(
    transport: &dyn LogTransport,
    cfg: &LogFetchConfig,
    target: &RunLogsApiTarget,
    out_dir: &Path,
    known_secrets: &[String],
    mut sleep: S,
) -> Result<ApiLogOutcome> {
    let url = target.logs_url();
    let mut backoff = cfg.initial_backoff;
    let mut last_reason = "no attempt made".to_string();

    for attempt_no in 0..=cfg.max_retries {
        match transport.get_bytes(&url, Some(&target.token)) {
            Ok((200, bytes)) => {
                let text = match unzip_run_logs(&bytes) {
                    Ok(t) => t,
                    Err(e) => {
                        return Ok(ApiLogOutcome::Failed(format!(
                            "unpacking run-logs archive: {e}"
                        )))
                    }
                };
                let text = redact(&text, known_secrets);
                let artifact = write_api_artifact(out_dir, target, &text)?;
                return Ok(ApiLogOutcome::Fetched(RedactedLog { text, artifact }));
            }
            Ok((status @ (401 | 403), _)) => return Ok(ApiLogOutcome::AuthDenied(status)),
            Ok((404, _)) => return Ok(ApiLogOutcome::NotFound),
            Ok((status, body)) => {
                last_reason = format!("status {status}, {} body bytes", body.len());
            }
            Err(e) => last_reason = e,
        }
        if attempt_no < cfg.max_retries {
            sleep(backoff);
            backoff = (backoff * 2).min(cfg.max_backoff);
        }
    }

    Ok(ApiLogOutcome::Failed(last_reason))
}

/// Concatenate every entry of the run-logs ZIP, sorted by entry name, each
/// under a `===== name =====` header. Non-UTF-8 bytes are replaced rather than
/// erroring: these are CI logs and a stray escape must not lose the archive.
fn unzip_run_logs(bytes: &[u8]) -> std::result::Result<String, String> {
    use std::io::Read;
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|e| format!("zip open: {e}"))?;
    let mut names: Vec<String> = archive.file_names().map(str::to_string).collect();
    names.sort();
    let mut out = String::new();
    for name in names {
        let mut file = archive
            .by_name(&name)
            .map_err(|e| format!("zip entry {name}: {e}"))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)
            .map_err(|e| format!("zip entry {name}: {e}"))?;
        out.push_str(&format!("===== {name} =====\n"));
        out.push_str(&String::from_utf8_lossy(&raw));
        if !out.ends_with('\n') {
            out.push('\n');
        }
    }
    Ok(out)
}

/// Artifact path for an API-fetched run log: one file for ALL jobs of the run,
/// named to sort next to the internal-route per-job artifacts.
fn write_api_artifact(out_dir: &Path, target: &RunLogsApiTarget, text: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stem = target.repo.replace('/', "_");
    let name = format!("{stem}-run{}-all-jobs.log", target.run_index);
    let path = out_dir.join(name);
    std::fs::write(&path, text)?;
    Ok(path)
}

/// CLAUDE-189: where to list a repo's Action runs from, to resolve a run's
/// GLOBAL id from its per-repo `index_in_repo`. The run web URL
/// (`/actions/runs/{index}`) uses `index_in_repo`, but the v16 run-logs endpoint
/// keys by the global `run.id`; a pre-existing "CI failure" issue (minted before
/// the `Run-Global-Id:` marker shipped) carries only the index, so the analysis
/// action resolves it here. Reads the API runs-list endpoint
/// (`GET /api/v1/repos/{owner}/{repo}/actions/runs`), which lives on the API
/// router where PATs work.
#[derive(Debug, Clone)]
pub struct RunsListApiTarget {
    /// Forge origin, e.g. `https://dev.a8n.run`.
    pub base_url: String,
    /// `owner/repo`.
    pub repo: String,
    /// PAT with repository read scope.
    pub token: String,
}

impl RunsListApiTarget {
    /// Build a target from the run's `html_url`, deriving the forge origin like
    /// [`RunLogsApiTarget::for_run`]. `None` when the origin cannot be derived.
    pub fn for_run(html_url: &str, repo: &str, token: String) -> Option<Self> {
        let base_url = base_from_html_url(html_url, repo)?;
        Some(Self {
            base_url,
            repo: repo.to_string(),
            token,
        })
    }

    /// The runs-list URL for one page.
    pub fn runs_url(&self, page: u32, limit: u32) -> String {
        format!(
            "{}/api/v1/repos/{}/actions/runs?page={}&limit={}",
            self.base_url.trim_end_matches('/'),
            self.repo,
            page,
            limit,
        )
    }
}

/// CLAUDE-189: outcome of resolving a run's global id from its per-repo index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunIdOutcome {
    /// The global id for the requested `index_in_repo`.
    Resolved(i64),
    /// 401/403: the token was rejected.
    AuthDenied(u16),
    /// 404 (endpoint absent), or the index was not found in any page.
    NotFound,
    /// Transport / 5xx / unparseable body.
    Failed(String),
}

/// Number of runs requested per page and the paging ceiling. `50 * 40 = 2000`
/// most-recent runs are scanned, enough to reach a recently-minted "CI failure"
/// issue's run; a run older than that resolves `NotFound` and the caller notes
/// the log as unavailable rather than crashing.
const RUNS_PAGE_LIMIT: u32 = 50;
const RUNS_MAX_PAGES: u32 = 40;

/// CLAUDE-189: resolve the GLOBAL run id for `index_in_repo` by paging the
/// repo's Action runs list and matching each run's per-repo index. Terminal on
/// 401/403/404 after one request; a transport error / 5xx / unparseable body is
/// `Failed`. Pages until a match, an empty/short page (the last page), or the
/// paging ceiling.
pub fn resolve_run_global_id(
    transport: &dyn LogTransport,
    target: &RunsListApiTarget,
    index_in_repo: i64,
) -> Result<RunIdOutcome> {
    for page in 1..=RUNS_MAX_PAGES {
        let url = target.runs_url(page, RUNS_PAGE_LIMIT);
        match transport.get_bytes(&url, Some(&target.token)) {
            Ok((200, bytes)) => {
                let runs = match parse_runs_list(&bytes) {
                    Ok(r) => r,
                    Err(e) => return Ok(RunIdOutcome::Failed(format!("parsing runs list: {e}"))),
                };
                if let Some(id) = runs.iter().find(|r| r.index == index_in_repo).map(|r| r.id) {
                    return Ok(RunIdOutcome::Resolved(id));
                }
                // An empty or short page is the last page: the index is not here.
                if runs.len() < RUNS_PAGE_LIMIT as usize {
                    return Ok(RunIdOutcome::NotFound);
                }
            }
            Ok((status @ (401 | 403), _)) => return Ok(RunIdOutcome::AuthDenied(status)),
            Ok((404, _)) => return Ok(RunIdOutcome::NotFound),
            Ok((status, body)) => {
                return Ok(RunIdOutcome::Failed(format!(
                    "status {status}, {} body bytes",
                    body.len()
                )))
            }
            Err(e) => return Ok(RunIdOutcome::Failed(e)),
        }
    }
    Ok(RunIdOutcome::NotFound)
}

/// One run's (global id, per-repo index) pulled from the runs-list JSON.
struct RunRef {
    id: i64,
    index: i64,
}

/// Parse the runs-list body into `(id, index)` pairs, tolerant of Forgejo's
/// field naming: the per-repo index is `index_in_repo` (webhook shape) or one of
/// `index` / `run_number` / `number`. Runs missing an `id` are skipped.
fn parse_runs_list(bytes: &[u8]) -> std::result::Result<Vec<RunRef>, String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    // The endpoint returns `{ "workflow_runs": [...] }`; tolerate a bare array.
    let arr = value
        .get("workflow_runs")
        .and_then(|v| v.as_array())
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for run in arr {
        let Some(id) = run.get("id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        let index = ["index_in_repo", "index", "run_number", "number"]
            .iter()
            .find_map(|k| run.get(*k).and_then(serde_json::Value::as_i64));
        if let Some(index) = index {
            out.push(RunRef { id, index });
        }
    }
    Ok(out)
}

/// CLAUDE-191: one run pulled from the runs-list JSON with the full set of fields
/// the auto-close reconciler needs to group and evaluate runs. The reconciler
/// reduces these to the LATEST run per `(branch, workflow_id)` and reads each
/// latest run's per-job status from the jobs endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntry {
    /// The run's GLOBAL id (`run.id`); addresses the jobs / run-logs endpoints.
    pub global_id: i64,
    /// The run's per-repo index (`index_in_repo`); the `/actions/runs/<N>` URL.
    pub index: i64,
    /// Workflow filename, e.g. `ci.yml`.
    pub workflow_id: String,
    /// The run's branch from the runs list (`head_branch`), or empty when the API
    /// returns it null (the deployed Forgejo), in which case the reconciler
    /// resolves it from the run's log checkout section.
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
pub fn fetch_runs(
    transport: &dyn LogTransport,
    target: &RunsListApiTarget,
    max_pages: u32,
) -> Result<RunsListOutcome> {
    let mut all: Vec<RunEntry> = Vec::new();
    for page in 1..=max_pages {
        let url = target.runs_url(page, RUNS_PAGE_LIMIT);
        match transport.get_bytes(&url, Some(&target.token)) {
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

/// CLAUDE-190: parse the failing run's branch from a redacted job log's checkout
/// section. The `actions/checkout` step dumps `GITHUB_REF=refs/heads/<branch>`
/// (and `GITHUB_REF_NAME=<branch>` on a branch build); the run-detail API
/// returns `head_branch: null` on the deployed Forgejo, so the log is the
/// reliable source. Returns the first branch found, or `None` when the log
/// carries no readable ref line (truncated / diagnostic-free / logless). A tag
/// build (`refs/tags/...`) yields no branch here.
pub fn parse_branch_from_log(log: &str) -> Option<String> {
    // Prefer the fully-qualified branch ref.
    for line in log.lines() {
        if let Some(rest) = line.trim().strip_prefix("GITHUB_REF=") {
            if let Some(b) = rest.trim().strip_prefix("refs/heads/") {
                let b = b.trim();
                if !b.is_empty() {
                    return Some(b.to_string());
                }
            }
        }
    }
    // Fall back to the short ref name (a branch build sets it to the branch).
    for line in log.lines() {
        if let Some(rest) = line.trim().strip_prefix("GITHUB_REF_NAME=") {
            let b = rest.trim();
            if !b.is_empty() {
                return Some(b.to_string());
            }
        }
    }
    None
}

/// CLAUDE-190: resolve the branch, preferring a non-empty run-detail `head_branch`
/// and falling back to the log checkout section. On the deployed Forgejo run
/// detail returns `head_branch: null`, so in practice this is the log parse; the
/// run-detail arm keeps the resolver correct where the field IS populated.
pub fn resolve_branch(run_detail_branch: Option<&str>, log: &str) -> Option<String> {
    match run_detail_branch {
        Some(b) if !b.trim().is_empty() => Some(b.trim().to_string()),
        _ => parse_branch_from_log(log),
    }
}

/// CLAUDE-190: where to list a run's jobs, to resolve the failing job's name and
/// index for the root-cause dedupe key. Reads the API jobs endpoint
/// (`GET /api/v1/repos/{owner}/{repo}/actions/runs/{run_global_id}/jobs`), keyed
/// by the run's GLOBAL id (like the run-logs endpoint). A public repo needs no
/// token; a private repo passes the source's `log_token_ref`.
#[derive(Debug, Clone)]
pub struct RunJobsApiTarget {
    /// Forge origin, e.g. `https://dev.a8n.run`.
    pub base_url: String,
    /// `owner/repo`.
    pub repo: String,
    /// The run's global id from the payload (`run.id`).
    pub run_global_id: i64,
    /// PAT for a private repo; `None` for a public repo.
    pub token: Option<String>,
}

impl RunJobsApiTarget {
    /// Build a target from the failure event, deriving the forge origin from
    /// `html_url`. `None` when the origin cannot be derived.
    pub fn for_run(
        html_url: &str,
        repo: &str,
        run_global_id: i64,
        token: Option<String>,
    ) -> Option<Self> {
        let base_url = base_from_html_url(html_url, repo)?;
        Some(Self {
            base_url,
            repo: repo.to_string(),
            run_global_id,
            token,
        })
    }

    /// The jobs-list URL for this target.
    pub fn jobs_url(&self) -> String {
        format!(
            "{}/api/v1/repos/{}/actions/runs/{}/jobs",
            self.base_url.trim_end_matches('/'),
            self.repo,
            self.run_global_id,
        )
    }
}

/// One job pulled from the jobs-list JSON: its display `name` and outcome
/// `status` (the `conclusion` when present, else the `status`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    pub name: String,
    pub status: String,
}

/// CLAUDE-190: outcome of a jobs-list fetch. Auth-class / not-found responses are
/// terminal after one request; only transport errors and 5xx retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobsOutcome {
    Resolved(Vec<JobInfo>),
    AuthDenied(u16),
    NotFound,
    Failed(String),
}

/// Parse the jobs-list body into `JobInfo`s, tolerant of Forgejo's field naming:
/// the array is `workflow_jobs` (webhook shape) or `jobs`, or a bare array. The
/// outcome is `conclusion` when present, else `status`.
fn parse_jobs_list(bytes: &[u8]) -> std::result::Result<Vec<JobInfo>, String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let arr = value
        .get("workflow_jobs")
        .and_then(|v| v.as_array())
        .or_else(|| value.get("jobs").and_then(|v| v.as_array()))
        .or_else(|| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for job in arr {
        let name = job
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let status = job
            .get("conclusion")
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| job.get("status").and_then(serde_json::Value::as_str))
            .unwrap_or("")
            .to_string();
        out.push(JobInfo { name, status });
    }
    Ok(out)
}

/// CLAUDE-190: the failing job's `(name, index)` for the root-cause key: the
/// first job whose status is a failure, else the first job (index 0). `None`
/// for an empty list. The index is the job's position in the run's jobs list.
pub fn resolve_failing_job(jobs: &[JobInfo]) -> Option<(String, u64)> {
    if let Some((i, j)) = jobs
        .iter()
        .enumerate()
        .find(|(_, j)| is_failure_status(&j.status))
    {
        return Some((j.name.clone(), i as u64));
    }
    jobs.first().map(|j| (j.name.clone(), 0))
}

/// True for a job status/conclusion that denotes a failure.
fn is_failure_status(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "failure" | "failed" | "error"
    )
}

/// CLAUDE-190: fetch the run's jobs list. One request decides 200/401/403/404;
/// only transport errors and other statuses retry within the configured budget.
/// Best-effort: the caller degrades to a job-agnostic key on any non-`Resolved`.
pub fn fetch_run_jobs<S: FnMut(Duration)>(
    transport: &dyn LogTransport,
    cfg: &LogFetchConfig,
    target: &RunJobsApiTarget,
    mut sleep: S,
) -> Result<JobsOutcome> {
    let url = target.jobs_url();
    let mut backoff = cfg.initial_backoff;
    let mut last_reason = "no attempt made".to_string();

    for attempt_no in 0..=cfg.max_retries {
        match transport.get_bytes(&url, target.token.as_deref()) {
            Ok((200, bytes)) => {
                return match parse_jobs_list(&bytes) {
                    Ok(jobs) => Ok(JobsOutcome::Resolved(jobs)),
                    Err(e) => Ok(JobsOutcome::Failed(format!("parsing jobs list: {e}"))),
                };
            }
            Ok((status @ (401 | 403), _)) => return Ok(JobsOutcome::AuthDenied(status)),
            Ok((404, _)) => return Ok(JobsOutcome::NotFound),
            Ok((status, body)) => {
                last_reason = format!("status {status}, {} body bytes", body.len());
            }
            Err(e) => last_reason = e,
        }
        if attempt_no < cfg.max_retries {
            sleep(backoff);
            backoff = (backoff * 2).min(cfg.max_backoff);
        }
    }

    Ok(JobsOutcome::Failed(last_reason))
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

    /// Scripted transport: pops queued responses in order, defaulting to a
    /// not-ready 404 once drained so an "always not ready" run is easy to set up.
    struct StubTransport {
        responses: Mutex<VecDeque<std::result::Result<HttpResponse, String>>>,
        calls: Mutex<u32>,
    }

    impl StubTransport {
        fn new(responses: Vec<std::result::Result<HttpResponse, String>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                calls: Mutex::new(0),
            }
        }
        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    impl LogTransport for StubTransport {
        fn get(
            &self,
            _url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<HttpResponse, String> {
            *self.calls.lock().unwrap() += 1;
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(HttpResponse {
                    status: 404,
                    body: "resource does not exist".to_string(),
                }))
        }
    }

    fn ready(body: &str) -> std::result::Result<HttpResponse, String> {
        Ok(HttpResponse {
            status: 200,
            body: body.to_string(),
        })
    }

    fn not_ready() -> std::result::Result<HttpResponse, String> {
        Ok(HttpResponse {
            status: 404,
            body: "resource does not exist".to_string(),
        })
    }

    fn target() -> LogTarget {
        LogTarget {
            base_url: "https://dev.a8n.run".to_string(),
            repo: "contoso/claude-run".to_string(),
            run_index: 7,
            job_index: 0,
            attempt: 1,
            token: None,
        }
    }

    // ---- CLAUDE-96: v16 REST run-logs fetch -------------------------------

    /// Scripted bytes transport for the API endpoint: one fixed response,
    /// counting calls. `get` panics so a private fetch can never fall back to
    /// the text route.
    struct BytesTransport {
        status: u16,
        body: Vec<u8>,
        calls: Mutex<u32>,
    }

    impl BytesTransport {
        fn new(status: u16, body: Vec<u8>) -> Self {
            Self {
                status,
                body,
                calls: Mutex::new(0),
            }
        }
        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    impl LogTransport for BytesTransport {
        fn get(
            &self,
            url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<HttpResponse, String> {
            panic!("text route used for an API fetch: {url}");
        }
        fn get_bytes(
            &self,
            _url: &str,
            token: Option<&str>,
        ) -> std::result::Result<(u16, Vec<u8>), String> {
            assert_eq!(token, Some("test-pat"), "API fetch must send the token");
            *self.calls.lock().unwrap() += 1;
            Ok((self.status, self.body.clone()))
        }
    }

    fn api_target() -> RunLogsApiTarget {
        RunLogsApiTarget {
            base_url: "https://dev.a8n.run".to_string(),
            repo: "Contoso/website".to_string(),
            run_index: 39,
            run_global_id: 4242,
            token: "test-pat".to_string(),
        }
    }

    fn zip_of(entries: &[(&str, &str)]) -> Vec<u8> {
        use std::io::Write;
        let mut buf = std::io::Cursor::new(Vec::new());
        let mut w = zip::ZipWriter::new(&mut buf);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, body) in entries {
            w.start_file(*name, opts).unwrap();
            w.write_all(body.as_bytes()).unwrap();
        }
        w.finish().unwrap();
        buf.into_inner()
    }

    #[test]
    fn api_fetch_unpacks_sorted_entries_under_headers() {
        // Entries land sorted by name, each under a ===== header, redacted and
        // written to one artifact; exactly one request, zero sleeps.
        let zip = zip_of(&[
            ("b-build-9-attempt-1.log", "boom: assertion failed\n"),
            ("a-check-8-attempt-1.log", "checking...\n"),
        ]);
        let transport = BytesTransport::new(200, zip);
        let tmp = tempfile::tempdir().unwrap();
        let mut sleeps = 0;
        let out = fetch_run_logs_api(
            &transport,
            &LogFetchConfig::default(),
            &api_target(),
            tmp.path(),
            &[],
            |_| sleeps += 1,
        )
        .unwrap();
        let ApiLogOutcome::Fetched(log) = out else {
            panic!("expected Fetched, got {out:?}");
        };
        let a = log
            .text
            .find("===== a-check-8-attempt-1.log =====")
            .unwrap();
        let b = log
            .text
            .find("===== b-build-9-attempt-1.log =====")
            .unwrap();
        assert!(a < b, "entries sorted by name");
        assert!(log.text.contains("boom: assertion failed"));
        assert!(log.artifact.exists());
        assert_eq!(transport.calls(), 1);
        assert_eq!(sleeps, 0);
    }

    #[test]
    fn api_fetch_auth_denied_decides_after_one_request() {
        for status in [401_u16, 403] {
            let transport = BytesTransport::new(status, Vec::new());
            let tmp = tempfile::tempdir().unwrap();
            let mut sleeps = 0;
            let out = fetch_run_logs_api(
                &transport,
                &LogFetchConfig::default(),
                &api_target(),
                tmp.path(),
                &[],
                |_| sleeps += 1,
            )
            .unwrap();
            assert!(
                matches!(out, ApiLogOutcome::AuthDenied(s) if s == status),
                "{out:?}"
            );
            assert_eq!(transport.calls(), 1, "exactly one request for {status}");
            assert_eq!(sleeps, 0, "no backoff for auth-class responses");
        }
    }

    #[test]
    fn api_fetch_not_found_decides_after_one_request() {
        // 404 = endpoint absent (Forgejo < 16) or run pruned; nothing to wait for.
        let transport = BytesTransport::new(404, Vec::new());
        let tmp = tempfile::tempdir().unwrap();
        let mut sleeps = 0;
        let out = fetch_run_logs_api(
            &transport,
            &LogFetchConfig::default(),
            &api_target(),
            tmp.path(),
            &[],
            |_| sleeps += 1,
        )
        .unwrap();
        assert!(matches!(out, ApiLogOutcome::NotFound), "{out:?}");
        assert_eq!(transport.calls(), 1);
        assert_eq!(sleeps, 0);
    }

    #[test]
    fn api_fetch_corrupt_archive_fails_with_reason() {
        let transport = BytesTransport::new(200, b"this is not a zip".to_vec());
        let tmp = tempfile::tempdir().unwrap();
        let out = fetch_run_logs_api(
            &transport,
            &LogFetchConfig::default(),
            &api_target(),
            tmp.path(),
            &[],
            |_| {},
        )
        .unwrap();
        assert!(
            matches!(&out, ApiLogOutcome::Failed(r) if r.contains("archive")),
            "{out:?}"
        );
    }

    fn cfg() -> LogFetchConfig {
        LogFetchConfig {
            max_retries: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(8),
        }
    }

    // ---- CLAUDE-189: runs-list global-id resolution ----------------------

    /// One queued bytes response: `(status, body)` or a transport error.
    type BytesResponse = std::result::Result<(u16, Vec<u8>), String>;

    /// A bytes transport that maps each request URL to a queued response by the
    /// order requested. `get` panics so the resolver never uses the text route.
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
        fn get(&self, url: &str, _t: Option<&str>) -> std::result::Result<HttpResponse, String> {
            panic!("text route used for an API fetch: {url}");
        }
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

    fn runs_target() -> RunsListApiTarget {
        RunsListApiTarget {
            base_url: "https://dev.a8n.run".to_string(),
            repo: "initech/chat".to_string(),
            token: "test-pat".to_string(),
        }
    }

    fn runs_page(runs: &[(i64, i64)]) -> Vec<u8> {
        let items: Vec<String> = runs
            .iter()
            .map(|(id, index)| format!(r#"{{"id":{id},"index_in_repo":{index}}}"#))
            .collect();
        format!(r#"{{"workflow_runs":[{}]}}"#, items.join(",")).into_bytes()
    }

    #[test]
    fn resolve_matches_index_to_global_id_on_first_page() {
        // The CLAUDE-185 shape: index 4034 -> global id 21077.
        let body = runs_page(&[(21099, 4036), (21077, 4034), (21050, 4030)]);
        let transport = SeqBytesTransport::new(vec![Ok((200, body))]);
        let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
        assert_eq!(out, RunIdOutcome::Resolved(21077));
        assert_eq!(
            transport.urls().len(),
            1,
            "found on page 1, no further pages"
        );
    }

    #[test]
    fn resolve_pages_until_the_index_is_found() {
        // A full page (50 entries) that misses, then a second page with the hit.
        let mut full: Vec<(i64, i64)> = (0..50).map(|n| (90000 + n, 5000 + n)).collect();
        // Ensure none on page 1 matches index 4034.
        assert!(!full.iter().any(|(_, idx)| *idx == 4034));
        let page1 = runs_page(&full);
        full.clear();
        let page2 = runs_page(&[(21077, 4034)]);
        let transport = SeqBytesTransport::new(vec![Ok((200, page1)), Ok((200, page2))]);
        let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
        assert_eq!(out, RunIdOutcome::Resolved(21077));
        assert_eq!(
            transport.urls().len(),
            2,
            "paged once past the full first page"
        );
        assert!(transport.urls()[0].contains("page=1"));
        assert!(transport.urls()[1].contains("page=2"));
    }

    #[test]
    fn resolve_short_page_without_match_is_not_found() {
        let body = runs_page(&[(1, 10), (2, 11)]); // short page, no 4034
        let transport = SeqBytesTransport::new(vec![Ok((200, body))]);
        let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
        assert_eq!(out, RunIdOutcome::NotFound);
        assert_eq!(
            transport.urls().len(),
            1,
            "stopped at the last (short) page"
        );
    }

    #[test]
    fn resolve_auth_and_not_found_are_terminal() {
        for status in [401_u16, 403] {
            let transport = SeqBytesTransport::new(vec![Ok((status, Vec::new()))]);
            let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
            assert_eq!(out, RunIdOutcome::AuthDenied(status));
        }
        let transport = SeqBytesTransport::new(vec![Ok((404, Vec::new()))]);
        let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
        assert_eq!(out, RunIdOutcome::NotFound);
    }

    #[test]
    fn resolve_transport_error_is_failed() {
        let transport = SeqBytesTransport::new(vec![Err("connect refused".to_string())]);
        let out = resolve_run_global_id(&transport, &runs_target(), 4034).unwrap();
        assert!(matches!(out, RunIdOutcome::Failed(r) if r.contains("connect refused")));
    }

    // ---- CLAUDE-191: full runs-list entries + paging ----------------------

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
        let out = fetch_runs(&transport, &runs_target(), RUNS_MAX_PAGES).unwrap();
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved");
        };
        assert_eq!(entries.len(), 51);
        assert_eq!(transport.urls().len(), 2, "stopped at the short page");
    }

    #[test]
    fn fetch_runs_auth_denied_on_first_page_is_terminal() {
        let transport = SeqBytesTransport::new(vec![Ok((403, Vec::new()))]);
        let out = fetch_runs(&transport, &runs_target(), RUNS_MAX_PAGES).unwrap();
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
        let out = fetch_runs(&transport, &runs_target(), RUNS_MAX_PAGES).unwrap();
        let RunsListOutcome::Resolved(entries) = out else {
            panic!("expected Resolved with the collected page-1 entries");
        };
        assert_eq!(entries.len(), 50);
    }

    // ---- CLAUDE-189: workflow YAML fetch ---------------------------------

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
            "https://dev.a8n.run",
            "initech/chat",
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
            "https://dev.a8n.run",
            "initech/chat",
            "a456a957",
            "check.yml",
            "test-pat",
            &[],
        );
        assert!(out.is_none());
    }

    // ---- URL / base derivation -------------------------------------------

    #[test]
    fn logs_url_has_required_attempt_segment() {
        assert_eq!(
            target().logs_url(),
            "https://dev.a8n.run/contoso/claude-run/actions/runs/7/jobs/0/attempt/1/logs"
        );
    }

    #[test]
    fn base_url_derived_from_html_url() {
        let t = LogTarget::for_run(
            "https://dev.a8n.run/contoso/claude-run/actions/runs/7",
            "contoso/claude-run",
            7,
        )
        .unwrap();
        assert_eq!(t.base_url, "https://dev.a8n.run");
        assert_eq!(t.job_index, DEFAULT_JOB_INDEX);
        assert_eq!(t.attempt, DEFAULT_ATTEMPT);
        assert!(t.token.is_none());
    }

    #[test]
    fn base_url_none_when_repo_absent() {
        assert!(
            LogTarget::for_run("https://dev.a8n.run/other/repo", "contoso/claude-run", 7)
                .is_none()
        );
    }

    // ---- AC 1: fetch returns redacted text + a file artifact -------------

    #[test]
    fn fetch_returns_redacted_text_and_artifact_file() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = StubTransport::new(vec![ready(
            "step 1 ok\nAuthorization: token ghp_abcdefghij0123456789ABCDEFG\nstep 2 failed",
        )]);
        let secrets: Vec<String> = vec![];
        let out =
            fetch_failing_job_log(&transport, &cfg(), &target(), tmp.path(), &secrets, |_| {})
                .unwrap();
        let LogOutcome::Fetched(log) = out else {
            panic!("expected Fetched, got {out:?}");
        };
        // The artifact exists and matches the returned text.
        assert!(log.artifact.exists());
        let on_disk = std::fs::read_to_string(&log.artifact).unwrap();
        assert_eq!(on_disk, log.text);
        // The secret is gone from both the summary text and the file.
        assert!(!log.text.contains("ghp_abcdefghij0123456789ABCDEFG"));
        assert!(log.text.contains(REDACTED));
        assert!(log.text.contains("step 2 failed"));
        assert_eq!(transport.calls(), 1);
    }

    // ---- AC 2: retries on not-ready; persistent miss is transient --------

    #[test]
    fn retries_then_succeeds_when_logs_flush_late() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = StubTransport::new(vec![not_ready(), not_ready(), ready("the logs")]);
        let mut sleeps = 0u32;
        let out = fetch_failing_job_log(&transport, &cfg(), &target(), tmp.path(), &[], |_| {
            sleeps += 1
        })
        .unwrap();
        assert!(matches!(out, LogOutcome::Fetched(_)));
        assert_eq!(transport.calls(), 3, "two misses then a hit");
        assert_eq!(sleeps, 2, "backed off before each retry");
    }

    #[test]
    fn persistent_miss_is_transient_not_error() {
        let tmp = tempfile::tempdir().unwrap();
        // Empty queue -> the stub's drained default (not-ready) every time.
        let transport = StubTransport::new(vec![]);
        let out =
            fetch_failing_job_log(&transport, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        match out {
            LogOutcome::TransientMiss { reason } => assert!(reason.contains("not ready")),
            other => panic!("expected TransientMiss, got {other:?}"),
        }
        // 1 initial attempt + max_retries.
        assert_eq!(transport.calls(), 1 + cfg().max_retries);
    }

    #[test]
    fn transport_error_is_retried_and_surfaced_as_transient() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = StubTransport::new(vec![
            Err("connect refused".to_string()),
            Err("connect refused".to_string()),
            Err("connect refused".to_string()),
            Err("connect refused".to_string()),
        ]);
        let out =
            fetch_failing_job_log(&transport, &cfg(), &target(), tmp.path(), &[], |_| {}).unwrap();
        match out {
            LogOutcome::TransientMiss { reason } => assert!(reason.contains("transport error")),
            other => panic!("expected TransientMiss, got {other:?}"),
        }
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

    // ---- CLAUDE-190: jobs-list resolution ---------------------------------

    fn jobs_body(jobs: &[(&str, &str)]) -> Vec<u8> {
        let items: Vec<String> = jobs
            .iter()
            .map(|(name, status)| format!(r#"{{"name":"{name}","status":"{status}"}}"#))
            .collect();
        format!(r#"{{"workflow_jobs":[{}]}}"#, items.join(",")).into_bytes()
    }

    #[test]
    fn resolve_failing_job_picks_the_failing_one() {
        let jobs = parse_jobs_list(&jobs_body(&[
            ("build", "success"),
            ("test", "failure"),
            ("lint", "waiting"),
        ]))
        .unwrap();
        assert_eq!(
            resolve_failing_job(&jobs),
            Some(("test".to_string(), 1)),
            "the failing job's name and its index"
        );
    }

    #[test]
    fn resolve_failing_job_reads_conclusion_over_status() {
        // A finished job carries a `conclusion`; prefer it over `status`.
        let body =
            br#"{"workflow_jobs":[{"name":"check","status":"completed","conclusion":"failure"}]}"#;
        let jobs = parse_jobs_list(body).unwrap();
        assert_eq!(resolve_failing_job(&jobs), Some(("check".to_string(), 0)));
    }

    #[test]
    fn resolve_failing_job_defaults_to_first_when_none_failed() {
        let jobs = parse_jobs_list(&jobs_body(&[("only", "success")])).unwrap();
        assert_eq!(resolve_failing_job(&jobs), Some(("only".to_string(), 0)));
        // Empty list -> no job.
        assert_eq!(resolve_failing_job(&[]), None);
    }

    fn jobs_target() -> RunJobsApiTarget {
        RunJobsApiTarget {
            base_url: "https://dev.a8n.run".to_string(),
            repo: "contoso/claude-run".to_string(),
            run_global_id: 4242,
            token: Some("test-pat".to_string()),
        }
    }

    #[test]
    fn fetch_run_jobs_resolves_the_failing_job() {
        let body = jobs_body(&[("build", "success"), ("test", "failure")]);
        let transport = SeqBytesTransport::new(vec![Ok((200, body))]);
        let out = fetch_run_jobs(&transport, &cfg(), &jobs_target(), |_| {}).unwrap();
        let JobsOutcome::Resolved(jobs) = out else {
            panic!("expected Resolved, got {out:?}");
        };
        assert_eq!(resolve_failing_job(&jobs), Some(("test".to_string(), 1)));
        assert!(transport.urls()[0].contains("/actions/runs/4242/jobs"));
    }

    #[test]
    fn fetch_run_jobs_auth_and_not_found_are_terminal() {
        for status in [401_u16, 403] {
            let transport = SeqBytesTransport::new(vec![Ok((status, Vec::new()))]);
            let out = fetch_run_jobs(&transport, &cfg(), &jobs_target(), |_| {}).unwrap();
            assert_eq!(out, JobsOutcome::AuthDenied(status));
        }
        let transport = SeqBytesTransport::new(vec![Ok((404, Vec::new()))]);
        let out = fetch_run_jobs(&transport, &cfg(), &jobs_target(), |_| {}).unwrap();
        assert_eq!(out, JobsOutcome::NotFound);
    }
}
