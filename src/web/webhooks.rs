//! Forgejo webhook listener (CLAUDE-43, multi-source CLAUDE-87).
//!
//! `POST /webhooks/forgejo/{source}` routes on the daemon's TCP app receive
//! Forgejo Action-run webhook deliveries. Each `{source}` names a configured
//! [`crate::config::WebhookSource`] with its own HMAC secret, allow-list, and
//! rate-limit bucket; the replay store is shared across sources (delivery ids are
//! globally unique). The per-request pipeline, in order:
//!
//! 0. Resolve `{source}` from the config sources map; an unknown source -> 404.
//! 1. Per-source rate limit (fixed window, before any crypto) -> 429 when
//!    exceeded. One source's storm never starves another's bucket.
//! 2. Resolve that source's HMAC secret (CLAUDE-26 `secrets` table); a missing
//!    `secret_ref` or unresolvable secret fails closed -> 401.
//! 3. Verify the HMAC-SHA256 over the raw body against `X-Forgejo-Signature`
//!    with a constant-time compare -> 401 on mismatch / missing signature. The
//!    secret is the source's own, so a payload signed for one source's secret
//!    401s on a different source's path.
//! 4. Replay protection (shared store): reject a delivery whose
//!    `X-Forgejo-Delivery` id was already seen inside the configured window ->
//!    409. A missing id is a malformed request -> 400.
//! 5. Parse the `ActionPayload`. Keep only `action_run_failure` (payload
//!    `action == "failure"`) for a repo in that source's allow-list and emit a
//!    typed [`VerifiedFailureEvent`] to [`consume_events`] -> 202. Every other
//!    authenticated event is acked -> 200 and dropped so Forgejo does not retry.
//!
//! Every per-request log line carries the `source` field so a rejection or ack
//! is always attributable to its endpoint.
//!
//! CLAUDE-47 wires the fast-path end-to-end: [`consume_events`] composes the
//! sibling subtasks - log fetch + redaction (CLAUDE-44), the hybrid triage
//! engine (CLAUDE-45), and the triage-driven issue mint + dedupe + loop safety
//! (CLAUDE-46) - so a verified failure turns into a minted issue (bucket 1 ->
//! `AI Agent=Queued` for forgejo-ci-fix, bucket 2 -> a human-assigned issue) or
//! nothing (bucket 3, transient). The scheduled `youtrack` / `forgejo-ci-fix`
//! actions stay as the backstop and dedupe against webhook-minted issues through
//! the same [`crate::mint`] markers.
//!
//! TLS / reverse proxy and service authentication are out of scope here per the
//! epic decision (handled by the external proxy / the linked auth issue).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::config::{MintConfig, WebhookConfig, WebhookSource};
use crate::forge_log::LogOutcome;
use crate::job::RunnerDefaults;
use crate::mint::{self, MintCaps, MintOutcome, MintRequest, VcsRouteCache};
use crate::provider::{Backend, ForgePullRequest};
use crate::secrets::{self, SecretsKey};
use crate::state;
use crate::triage::{self, FailureClassifier, TriageInput};

/// Synthetic action name the webhook fast-path records its DEV-335 caps under
/// (per-key attempt counter + daily run ledger in [`crate::state`]), so the
/// event-driven mint throttles on the same accounting shape as the scheduled
/// actions instead of re-deriving its own.
pub const WEBHOOK_MINT_ACTION: &str = "webhook-fast-path";

/// `runs.triggered_by` value for the event-driven webhook fast-path, alongside
/// the existing `scheduler` / `trigger` / `cli` sources (CLAUDE-61). Each
/// consumed verified failure becomes one run recorded under this source so the
/// dashboard / action-detail surfaces show webhook activity like any action.
pub const WEBHOOK_TRIGGERED_BY: &str = "webhook";

/// Wall-clock cap for one triage classification (`claude --print`). The pass is
/// a single bounded-log JSON reply, so it needs far less than a full code run.
pub const TRIAGE_TIMEOUT_SECS: u64 = 120;

/// Hex-encoded HMAC-SHA256 of the raw body, keyed by the webhook secret.
pub const SIGNATURE_HEADER: &str = "X-Forgejo-Signature";
/// Per-delivery unique id, used for replay protection.
pub const DELIVERY_HEADER: &str = "X-Forgejo-Delivery";

/// Subset of the Forgejo Action-run webhook payload we consume. Extra fields in
/// the delivery are ignored (no `deny_unknown_fields`) so a Forgejo upgrade that
/// adds keys does not break ingestion.
#[derive(Debug, Clone, Deserialize)]
pub struct ActionPayload {
    /// One of `failure`, `success`, `recover`. Only `failure` is kept.
    pub action: String,
    pub run: ActionRun,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionRun {
    /// CLAUDE-96: the run's GLOBAL id, which addresses the v16 REST run-logs
    /// endpoint (`GetRunByID` in the upstream handler). Distinct from
    /// `index_in_repo`. Defensive default 0 = "payload carried no id"; the API
    /// fetch refuses to run with it.
    #[serde(default)]
    pub id: i64,
    pub index_in_repo: i64,
    pub commit_sha: String,
    pub workflow_id: String,
    pub status: String,
    pub trigger_user: ActionUser,
    pub html_url: String,
    pub repository: Repository,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "ActionUserRaw")]
pub struct ActionUser {
    /// Forgejo user objects carry both `login` and `username` (often with the
    /// same value). Both keys are accepted independently and reconciled in
    /// `TryFrom<ActionUserRaw>`, preferring `login`; at least one must be present.
    pub login: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ActionUserRaw {
    login: Option<String>,
    username: Option<String>,
}

impl TryFrom<ActionUserRaw> for ActionUser {
    type Error = String;
    fn try_from(raw: ActionUserRaw) -> Result<Self, Self::Error> {
        raw.login
            .or(raw.username)
            .map(|login| ActionUser { login })
            .ok_or_else(|| "user object has neither `login` nor `username`".to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Repository {
    /// `owner/repo`, matched against the configured allow-list.
    pub full_name: String,
    /// CLAUDE-92: visibility from the payload (`modules/structs/repo.go:57`).
    /// A private repo's logs cannot be fetched on this Forgejo version (the
    /// internal web logs route authenticates via session cookie only:
    /// `services/auth/{basic,oauth2}.go` `Verify` both bail on non-API paths,
    /// and the v14 repo Actions API has no logs endpoint, FJ-22 pending), so
    /// the consumer must not burn the retry budget discovering that. Serde
    /// default keeps older/foreign payloads on the public path.
    #[serde(default)]
    pub private: bool,
}

/// The typed event emitted downstream for a verified, allow-listed failure.
/// Carries exactly the fields the next stage (log fetch / triage) needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedFailureEvent {
    pub repo: String,
    pub index_in_repo: i64,
    pub commit_sha: String,
    pub workflow_id: String,
    pub trigger_user: String,
    pub html_url: String,
    /// CLAUDE-85: the run's own status from the payload. Forgejo's notifier
    /// (`services/webhook/notifier.go` `ActionRunNowDone`, v14.0.2) fires
    /// `action_run_failure` with `action: "failure"` for EVERY non-success done
    /// status - `failure`, `cancelled`, and `skipped` (`DoneStatuses()` in
    /// `models/actions/status.go`) - so this field, not the `action`, says
    /// whether anything actually failed. The consumer processes only
    /// [`RUN_STATUS_FAILURE`] and acks-and-skips the rest.
    pub status: String,
    /// CLAUDE-92: `repository.private` from the payload; gates the log fetch
    /// (see [`Repository::private`]).
    pub private: bool,
    /// CLAUDE-96: the run's global id (`run.id`), addressing the v16 REST
    /// run-logs endpoint. 0 when the payload carried none.
    pub run_global_id: i64,
    /// CLAUDE-96: the webhook source (config block name) this event arrived
    /// through, so the consumer can resolve the source's `log_token_ref`.
    pub source: String,
}

/// CLAUDE-85: the one `run.status` the pipeline acts on. Forgejo's status enum
/// (`models/actions/status.go`) is `unknown, success, failure, cancelled,
/// skipped, waiting, running, blocked`; of the done statuses that reach the
/// `action_run_failure` event (`failure`, `cancelled`, `skipped`), only a real
/// `failure` has something to fix. A `cancelled` run was stopped deliberately
/// and a `skipped` run never ran (and has no log, so fetching would burn the
/// full 60s retry budget); both finish in ~0s with no fetch, triage, or mint.
pub const RUN_STATUS_FAILURE: &str = "failure";

/// State injected into the webhook handler. Cheap to clone (`SqlitePool` and
/// `Sender` are `Arc`-shaped; the config and the rate-limiter map are
/// `Arc`-wrapped). CLAUDE-87: one [`RateLimiter`] per source (built once at
/// startup, keyed by source name) and a single shared [`SeenDeliveries`] replay
/// store across all sources.
#[derive(Clone)]
pub struct WebhookState {
    pool: SqlitePool,
    /// CLAUDE-101: live webhook config, shared with [`crate::settings::LiveSettings`]
    /// so a Settings save (new/edited source, allow-list, rate limit, secret refs)
    /// is routable on the next request without a restart.
    config: Arc<RwLock<WebhookConfig>>,
    events: mpsc::Sender<VerifiedFailureEvent>,
    seen: Arc<Mutex<SeenDeliveries>>,
    /// CLAUDE-101: live per-source limiter map, rebuilt by `LiveSettings` on save.
    rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
}

/// CLAUDE-87/101: build one fixed-window rate limiter per source, so exhausting
/// one source's budget never 429s another. Shared by [`WebhookState`] startup and
/// the live-apply rebuild in [`crate::settings::LiveSettings`], keeping the
/// limiter map and the routed source map in lock-step.
pub fn build_rate_map(config: &WebhookConfig) -> HashMap<String, Mutex<RateLimiter>> {
    config
        .sources
        .iter()
        .map(|(name, src)| {
            (
                name.clone(),
                Mutex::new(RateLimiter::new(
                    src.rate_limit_per_min,
                    Duration::from_secs(60),
                )),
            )
        })
        .collect()
}

impl WebhookState {
    /// Standalone constructor (tests, single-source callers): owns fresh shared
    /// handles built from `config`.
    pub fn new(
        pool: SqlitePool,
        config: WebhookConfig,
        events: mpsc::Sender<VerifiedFailureEvent>,
    ) -> Self {
        let seen = SeenDeliveries::new(Duration::from_secs(config.replay_window_secs));
        let rate = build_rate_map(&config);
        Self {
            pool,
            config: Arc::new(RwLock::new(config)),
            events,
            seen: Arc::new(Mutex::new(seen)),
            rate: Arc::new(RwLock::new(rate)),
        }
    }

    /// CLAUDE-101: build from handles shared with [`crate::settings::LiveSettings`]
    /// so a save swaps the routed config + limiters live. The replay store is
    /// built once here from `replay_window_secs` (restart-only by design).
    pub fn from_shared(
        pool: SqlitePool,
        config: Arc<RwLock<WebhookConfig>>,
        rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
        replay_window_secs: u64,
        events: mpsc::Sender<VerifiedFailureEvent>,
    ) -> Self {
        let seen = SeenDeliveries::new(Duration::from_secs(replay_window_secs));
        Self {
            pool,
            config,
            events,
            seen: Arc::new(Mutex::new(seen)),
            rate,
        }
    }
}

/// Router carrying the per-source webhook route, merged into the daemon's TCP
/// app. `{source}` is an axum path param resolved against the configured sources
/// map (CLAUDE-87).
pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/webhooks/forgejo/:source", post(forgejo_webhook))
        .with_state(state)
}

/// Everything [`consume_events`] needs to triage a verified failure and mint
/// (or decline to mint) a YouTrack issue from it. Built once at daemon start
/// from the resolved provider backend (CLAUDE-46 `IssueTracker` + `Forge`), the
/// on-disk mint config, and the shared `claude` invoker behind a
/// [`FailureClassifier`]. When the daemon cannot build one (no usable `yt`/`fj`
/// backend) it passes `None` and the consumer falls back to log-only behaviour.
pub struct MintContext {
    /// State DB, for the reused DEV-335 caps ledger.
    pub pool: SqlitePool,
    /// The LLM pass for the residue the deterministic rules cannot label.
    pub classifier: Arc<dyn FailureClassifier>,
    /// Provider backend: the `IssueTracker` the mint writes through and the
    /// `Forge` the `has_open_pr` signal is read from.
    pub backend: Arc<dyn Backend>,
    /// Repo-to-project routing, loop-safety bot logins, bucket-2 assignee, etc.
    /// CLAUDE-101: a live handle shared with [`crate::settings::LiveSettings`],
    /// read per event so a Settings save is honored by the next mint without a
    /// restart.
    pub mint_cfg: Arc<RwLock<MintConfig>>,
    /// Caps applied to the event-driven mint (per-key attempts + daily runs).
    pub defaults: RunnerDefaults,
    /// CLAUDE-86: TTL-cached inverse VCS routing map (repo -> project), built
    /// from YouTrack project VCS integrations and shared across mints.
    pub route_cache: Arc<VcsRouteCache>,
}

/// Drain emitted events. For each verified failure this fetches the failing
/// job's log via the internal route, redacts secrets, and writes a redacted
/// artifact (CLAUDE-44); then, when a [`MintContext`] is configured, triages the
/// failure (CLAUDE-45) and mints / declines a YouTrack issue (CLAUDE-46). The
/// fetch is best-effort: a not-ready / pruned log is a transient miss, not a
/// fault. A miss does not strand the consumer - it feeds triage as a bucket-3
/// signal (the rules pass routes an unavailable log to transient).
pub async fn consume_events(
    mut rx: mpsc::Receiver<VerifiedFailureEvent>,
    pool: SqlitePool,
    transport: Arc<dyn crate::forge_log::LogTransport>,
    out_dir: PathBuf,
    mint: Option<MintContext>,
    webhook_cfg: Arc<RwLock<WebhookConfig>>,
) {
    let cfg = crate::forge_log::LogFetchConfig::default();
    while let Some(ev) = rx.recv().await {
        // CLAUDE-101: snapshot the live webhook config for this event so a
        // Settings save (e.g. a source's `log_token_ref`) is picked up by the
        // next event without a restart.
        let webhook_cfg = webhook_cfg.read().await.clone();
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            commit = %ev.commit_sha,
            workflow = %ev.workflow_id,
            trigger_user = %ev.trigger_user,
            html_url = %ev.html_url,
            "verified action_run_failure received"
        );

        // CLAUDE-61: open a run row for every consumed event (user decision:
        // full observability including skips) so the web UI surfaces webhook
        // activity. `run_id` is `None` only if `run_begin` itself failed; the
        // pipeline still runs, it just goes unrecorded.
        let job_key = format!("{}#{}", ev.repo, ev.index_in_repo);
        let run_id = begin_webhook_run(&pool, &job_key, &ev.html_url).await;

        // CLAUDE-85: Forgejo fires `action_run_failure` for every non-success
        // done status (`failure`, `cancelled`, `skipped`; bare `else` in
        // `webhookNotifier.ActionRunNowDone`), so the run status, not the event
        // type, says whether anything failed. Only a real failure gets the
        // pipeline: a cancelled run was stopped deliberately, and a server-side
        // skipped run (workflow condition false, no runner task) has no log at
        // all - fetching would burn the full 60s retry budget on `resource
        // does not exist`. Everything else finishes in ~0s, no fetch, no
        // triage, no mint.
        if ev.status != RUN_STATUS_FAILURE {
            info!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                status = %ev.status,
                "run status is not 'failure'; log fetch and triage skipped"
            );
            let summary = format!(
                "run status is '{}', not 'failure'; log fetch and triage skipped; no issue",
                ev.status
            );
            finish_webhook_run(&pool, run_id, &job_key, &summary, true, None).await;
            continue;
        }

        // CLAUDE-92: the payload already declares visibility, and the event
        // always arrives AFTER the run completed, so a private repo is decided
        // here with ZERO fetch requests: the deployed Forgejo authenticates the
        // internal web logs route by session cookie only (Basic/OAuth2 `Verify`
        // bail on non-API paths; no API logs endpoint until FJ-22), so the
        // fetch cannot succeed and retrying would only burn the 60s budget.
        // The failure still surfaces: the no-log triage rule routes it to a
        // human (bucket 2) instead of silently dropping it as transient.
        let (log_text, log_available, log_artifact, log_unavailable_reason) = if ev.private {
            // CLAUDE-96: a private repo's logs are reachable ONLY through the
            // v16 REST endpoint with the source's PAT (`log_token_ref`); the
            // internal web route is session-only. With a token, exactly one
            // request decides success/401/403/404 (the run already finished, so
            // there is nothing to wait for); without one, the CLAUDE-92
            // unauthorized short-circuit applies with zero requests. Either
            // way a real failure still mints via the no-log bucket-2 rule.
            match fetch_private_log(
                &pool,
                &webhook_cfg,
                &cfg,
                transport.clone(),
                out_dir.clone(),
                &ev,
                run_id,
            )
            .await
            {
                PrivateLogOutcome::Fetched(log) => (log.text, true, Some(log.artifact), None),
                PrivateLogOutcome::Unavailable(reason) => {
                    if let Some(id) = run_id {
                        if let Err(e) =
                            state::run_stage_set(&pool, id, Some(STAGE_PRIVATE_UNAUTHORIZED)).await
                        {
                            warn!(error = %e, "setting webhook run stage failed");
                        }
                    }
                    (String::new(), false, None, Some(reason))
                }
            }
        } else {
            let Some(target) =
                crate::forge_log::LogTarget::for_run(&ev.html_url, &ev.repo, ev.index_in_repo)
            else {
                warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge origin from html_url; skipping log fetch");
                finish_webhook_run(
                    &pool,
                    run_id,
                    &job_key,
                    "could not derive forge origin from html_url; log fetch skipped",
                    false,
                    None,
                )
                .await;
                continue;
            };

            // The fetch is blocking (reqwest on a thread + backoff sleeps); run
            // it off the async runtime. No service-injected secrets reach the CI
            // run, so `known_secrets` is empty here and pattern redaction does
            // the work.
            let transport = transport.clone();
            let fetch_cfg = cfg.clone();
            let out_dir = out_dir.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                crate::forge_log::fetch_failing_job_log(
                    transport.as_ref(),
                    &fetch_cfg,
                    &target,
                    &out_dir,
                    &[],
                    std::thread::sleep,
                )
            })
            .await;

            // Normalise the fetch result into the triage inputs. A
            // `TransientMiss`, a fetch error, and a panicked task all collapse
            // to "no log" with the reason preserved; the no-log triage rule
            // (CLAUDE-92) routes those to a human rather than dropping them.
            match outcome {
                Ok(Ok(LogOutcome::Fetched(log))) => {
                    info!(
                        repo = %ev.repo,
                        run = ev.index_in_repo,
                        bytes = log.text.len(),
                        artifact = %log.artifact.display(),
                        "fetched and redacted failing job log"
                    );
                    (log.text, true, Some(log.artifact), None)
                }
                Ok(Ok(LogOutcome::TransientMiss { reason })) => {
                    warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "failing job log not available (transient miss)");
                    (
                        String::new(),
                        false,
                        None,
                        Some(format!("log not available after retries: {reason}")),
                    )
                }
                Ok(Err(e)) => {
                    warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch failed");
                    (
                        String::new(),
                        false,
                        None,
                        Some(format!("log fetch failed: {e}")),
                    )
                }
                Err(e) => {
                    warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch task panicked");
                    (
                        String::new(),
                        false,
                        None,
                        Some(format!("log fetch task panicked: {e}")),
                    )
                }
            }
        };

        let Some(ctx) = mint.as_ref() else {
            // No mint configured (no usable backend): log-only, as before. The
            // run is still recorded so the operator sees the event landed.
            let summary = if log_available {
                "log fetched and redacted; no provider backend configured (log-only)"
            } else {
                "log unavailable; no provider backend configured (log-only)"
            };
            finish_webhook_run(
                &pool,
                run_id,
                &job_key,
                summary,
                true,
                log_artifact.as_deref(),
            )
            .await;
            continue;
        };
        let artifact = log_artifact.clone();
        let result = triage_and_mint(
            ctx,
            run_id,
            &ev,
            log_text,
            log_available,
            log_artifact,
            log_unavailable_reason,
        )
        .await;
        finish_webhook_run(
            &pool,
            run_id,
            &job_key,
            &result.text,
            result.ok,
            artifact.as_deref(),
        )
        .await;
    }
}

/// CLAUDE-80: live stage labels for the webhook fast-path pipeline, surfaced on
/// the run-detail page (Stage row + the running job's LIVE column) via
/// `runs.stage`. `run_finish` clears the column, so no label outlives its run.
pub const STAGE_FETCHING_LOGS: &str = "fetching action logs";
/// CLAUDE-92: shown while a private-repo event is decided (no fetch possible);
/// the lasting record is the triage analysis and journal line, since the run
/// finishes within milliseconds and `run_finish` clears the stage.
pub const STAGE_PRIVATE_UNAUTHORIZED: &str = "private repo: unauthorized";
pub const STAGE_TRIAGING: &str = "triaging";
pub const STAGE_CLASSIFYING: &str = "classifying with claude";
pub const STAGE_MINTING: &str = "minting issue";

/// CLAUDE-96: outcome of the private-repo log path.
enum PrivateLogOutcome {
    Fetched(crate::forge_log::RedactedLog),
    /// Why there is no log; feeds the no-log triage rule's analysis (CLAUDE-92).
    Unavailable(String),
}

/// CLAUDE-96: try to fetch a private repo's logs through the v16 REST endpoint
/// using the event source's `log_token_ref`. "No token configured" decides with
/// ZERO requests; success, 401/403, and 404 decide with exactly ONE (the run
/// already finished, so there is nothing to wait for); only transport errors
/// and 5xx use the small retry budget. Every miss returns a reason string for
/// the bucket-2 analysis.
async fn fetch_private_log(
    pool: &SqlitePool,
    webhook_cfg: &WebhookConfig,
    fetch_cfg: &crate::forge_log::LogFetchConfig,
    transport: Arc<dyn crate::forge_log::LogTransport>,
    out_dir: PathBuf,
    ev: &VerifiedFailureEvent,
    run_id: Option<i64>,
) -> PrivateLogOutcome {
    let token_ref = webhook_cfg
        .sources
        .get(&ev.source)
        .and_then(|s| s.log_token_ref.clone());
    let Some(token_ref) = token_ref else {
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            source = %ev.source,
            "private repository; log fetch skipped (no log_token_ref configured for this source)"
        );
        return PrivateLogOutcome::Unavailable(
            "private repo: unauthorized (no log token configured for this source)".to_string(),
        );
    };
    let token = match resolve_log_token(pool, &token_ref).await {
        Ok(t) => t,
        Err(e) => {
            warn!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                source = %ev.source,
                token_ref = %token_ref,
                error = %e,
                "private repository; log token unresolvable"
            );
            return PrivateLogOutcome::Unavailable(format!(
                "private repo: unauthorized (log token '{token_ref}' unresolvable)"
            ));
        }
    };
    if ev.run_global_id <= 0 {
        warn!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            "private repository; payload carried no run id, cannot address the logs API"
        );
        return PrivateLogOutcome::Unavailable(
            "private repo: payload carried no run id; the v16 logs API cannot be addressed"
                .to_string(),
        );
    }
    let Some(target) = crate::forge_log::RunLogsApiTarget::for_run(
        &ev.html_url,
        &ev.repo,
        ev.index_in_repo,
        ev.run_global_id,
        token,
    ) else {
        warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge origin from html_url; skipping log fetch");
        return PrivateLogOutcome::Unavailable(
            "could not derive forge origin from html_url; log fetch skipped".to_string(),
        );
    };
    if let Some(id) = run_id {
        if let Err(e) = state::run_stage_set(pool, id, Some(STAGE_FETCHING_LOGS)).await {
            warn!(error = %e, "setting webhook run stage failed");
        }
    }

    let fetch_cfg = fetch_cfg.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        crate::forge_log::fetch_run_logs_api(
            transport.as_ref(),
            &fetch_cfg,
            &target,
            &out_dir,
            &[],
            std::thread::sleep,
        )
    })
    .await;

    use crate::forge_log::ApiLogOutcome;
    match outcome {
        Ok(Ok(ApiLogOutcome::Fetched(log))) => {
            info!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                bytes = log.text.len(),
                artifact = %log.artifact.display(),
                "fetched and redacted private-repo run logs via the REST API"
            );
            PrivateLogOutcome::Fetched(log)
        }
        Ok(Ok(ApiLogOutcome::AuthDenied(status))) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, status, "private repository; log token rejected");
            // CLAUDE-146: Forgejo distinguishes 401 from 403, so name the
            // actionable cause. 401 = the credential was not accepted (the log
            // token is not a valid Forgejo user PAT: wrong value, expired, or
            // not a `token`-scheme PAT). 403 = the token is valid but lacks
            // `read:repository` scope or Actions read access on this repo.
            let detail = if status == 401 {
                "HTTP 401: the log token is not a valid Forgejo user PAT (wrong value, expired, or not a token-scheme PAT)"
            } else {
                "HTTP 403: the log token is valid but lacks read:repository scope or Actions read access on this repo"
            };
            PrivateLogOutcome::Unavailable(format!("private repo: log token rejected ({detail})"))
        }
        Ok(Ok(ApiLogOutcome::NotFound)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "private repository; logs API returned 404 (Forgejo < 16, or run pruned)");
            PrivateLogOutcome::Unavailable(
                "private repo: log API unavailable (Forgejo < 16) or run pruned".to_string(),
            )
        }
        Ok(Ok(ApiLogOutcome::Failed(reason))) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "private repository; log fetch via API failed");
            PrivateLogOutcome::Unavailable(format!("log fetch via API failed: {reason}"))
        }
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "private repository; writing the log artifact failed");
            PrivateLogOutcome::Unavailable(format!("log fetch via API failed: {e}"))
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "private repository; log fetch task panicked");
            PrivateLogOutcome::Unavailable(format!("log fetch task panicked: {e}"))
        }
    }
}

/// CLAUDE-96: decrypt the named log token from the secrets store (same age-key
/// mechanism as the HMAC refs).
async fn resolve_log_token(pool: &SqlitePool, name: &str) -> crate::error::Result<String> {
    let key = SecretsKey::load()?;
    // CLAUDE-146: trim the decrypted token. A stored trailing space or CR
    // survives as a valid header character and yields a spurious HTTP 401.
    let token = secrets::resolve_secret(pool, &key, name).await?;
    Ok(token.trim().to_string())
}

/// CLAUDE-61: open the `runs` row for one webhook fast-path event and stamp its
/// single planned job key. CLAUDE-80: also registers the job in-flight (so the
/// run-detail badge reads `running`, not `queued`, for the whole pipeline) and
/// sets the initial stage. CLAUDE-93: stores the event's `html_url` as the
/// job's outbound link so the run-detail tables link the key straight to the
/// Forgejo Actions run. Returns the run id, or `None` when `run_begin` failed
/// (the pipeline still runs, just unrecorded). Failures are logged, not
/// propagated: a DB hiccup must never strand the event consumer.
async fn begin_webhook_run(pool: &SqlitePool, job_key: &str, html_url: &str) -> Option<i64> {
    match state::run_begin(pool, WEBHOOK_MINT_ACTION, WEBHOOK_TRIGGERED_BY).await {
        Ok(id) => {
            if let Err(e) =
                state::run_set_job_keys(pool, id, std::slice::from_ref(&job_key.to_string())).await
            {
                warn!(error = %e, "recording webhook run job key failed");
            }
            let links =
                std::collections::BTreeMap::from([(job_key.to_string(), html_url.to_string())]);
            if let Err(e) = state::run_set_job_links(pool, id, &links).await {
                warn!(error = %e, "recording webhook run job link failed");
            }
            if let Err(e) = state::run_inflight_add(pool, id, job_key).await {
                warn!(error = %e, "registering webhook job in flight failed");
            }
            if let Err(e) = state::run_stage_set(pool, id, Some(STAGE_FETCHING_LOGS)).await {
                warn!(error = %e, "setting webhook run stage failed");
            }
            Some(id)
        }
        Err(e) => {
            warn!(error = %e, "opening webhook run row failed; event runs unrecorded");
            None
        }
    }
}

/// CLAUDE-61: close a webhook fast-path run. Writes one `job_outcomes` row
/// carrying the human-readable `summary` (triage bucket + mint result, plus the
/// redacted-log artifact path when present) and finalizes the run. A webhook run
/// has no `claude --print` stream (`claude_ran = false`), so the run-detail page
/// shows the summary instead of a stream link. `attempt_counted = false`: the
/// DEV-335 caps ledger is advanced separately inside [`triage_and_mint`] and
/// must not be double counted here. A no-op when `run_id` is `None`.
async fn finish_webhook_run(
    pool: &SqlitePool,
    run_id: Option<i64>,
    job_key: &str,
    summary: &str,
    ok: bool,
    artifact: Option<&std::path::Path>,
) {
    let Some(run_id) = run_id else {
        return;
    };
    let full = match artifact {
        Some(p) => format!("{summary}; log={}", p.display()),
        None => summary.to_string(),
    };
    let outcome = state::JobOutcome {
        run_id,
        action_name: WEBHOOK_MINT_ACTION,
        job_key,
        attempt: 1,
        ok,
        claude_ran: false,
        claude_ok: false,
        claude_cost_usd: 0.0,
        attempt_counted: false,
        summary: Some(&full),
        stream_log_path: None,
        duration_ms: 0,
        timed_out: false,
    };
    if let Err(e) = state::job_outcome_insert(pool, &outcome).await {
        warn!(error = %e, "recording webhook run outcome failed");
    }
    let exit_code = if ok {
        crate::exit::OK
    } else {
        crate::exit::UNEXPECTED
    };
    if let Err(e) = state::run_finish(pool, run_id, exit_code, 1).await {
        warn!(error = %e, "finalizing webhook run failed");
    }
}

/// CLAUDE-61: the run-record summary for one webhook fast-path event. `text` is
/// the human-readable outcome (triage bucket + mint result, or an error), and
/// `ok` is whether the pipeline completed: a decided outcome (mint or any skip)
/// is `ok`, a triage/mint fault is not.
struct WebhookRunSummary {
    text: String,
    ok: bool,
}

impl WebhookRunSummary {
    fn decided(bucket: crate::triage::TriageBucket, outcome: &MintOutcome) -> Self {
        Self {
            text: format!("triage={bucket}; {}", mint_summary(outcome)),
            ok: true,
        }
    }

    fn error(message: String) -> Self {
        Self {
            text: format!("triage/mint error: {message}"),
            ok: false,
        }
    }
}

/// One-line description of a [`MintOutcome`] for the run summary: the minted
/// issue key on success, or the skip reason otherwise.
fn mint_summary(outcome: &MintOutcome) -> String {
    match outcome {
        MintOutcome::Created { key, project, .. } => format!("minted {key} in {project}"),
        MintOutcome::SkippedTransient => "no issue (transient)".to_string(),
        MintOutcome::SkippedBotLoop => "no issue (bot-loop guard)".to_string(),
        MintOutcome::SkippedOpenPr => "no issue (open PR; left to author)".to_string(),
        MintOutcome::SkippedAttemptCap => "no issue (per-key attempt cap)".to_string(),
        MintOutcome::SkippedDailyCap => "no issue (daily run cap)".to_string(),
        MintOutcome::SkippedDuplicateOpen { existing } => {
            format!("no issue (duplicate of open {existing})")
        }
        MintOutcome::SkippedResolved { existing } => {
            format!("no issue (resolved {existing} not reopened)")
        }
        MintOutcome::AssociatedWithOpen { existing } => {
            format!("associated with open {existing}")
        }
    }
}

/// Triage a verified failure and mint (or decline to mint) a YouTrack issue.
/// Reads the reused DEV-335 caps from the DB, resolves the `has_open_pr` signal
/// from the forge, runs the hybrid triage + mint on a blocking thread (every
/// step shells out), and advances the caps ledger only when an issue was
/// actually created. Logs the decision; never propagates an error to the loop.
async fn triage_and_mint(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<PathBuf>,
    log_unavailable_reason: Option<String>,
) -> WebhookRunSummary {
    // DEV-335 caps, reused (not re-derived): the per-key counter is keyed by the
    // run's (repo, index); the daily ledger is per synthetic action. The
    // read-then-increment below is race-free because `consume_events` drains the
    // channel serially - one event is fully processed (this `.await` completes)
    // before the next is pulled - so no two mints read the same stale counters.
    let job_key = format!("{}#{}", ev.repo, ev.index_in_repo);
    let attempts = state::attempts_get(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key)
        .await
        .unwrap_or(0);
    let daily = state::daily_count_get(&ctx.pool, WEBHOOK_MINT_ACTION)
        .await
        .unwrap_or(0);
    let caps = MintCaps {
        attempts,
        max_attempts: ctx.defaults.max_attempts_per_key,
        daily,
        max_runs_per_day: ctx.defaults.max_runs_per_day,
    };

    // CLAUDE-80: surface what the blocking pipeline is doing. The blocking
    // thread cannot await the pool, so stage labels travel over an unbounded
    // channel to a drainer task that performs the `runs.stage` UPDATEs. The
    // sender drops with the closure when the blocking task ends, closing the
    // channel; the drainer is awaited below so no stale stage write can land
    // after `finish_webhook_run` clears the column.
    let (stage_tx, mut stage_rx) = mpsc::unbounded_channel::<&'static str>();
    let stage_drainer = {
        let pool = ctx.pool.clone();
        tokio::spawn(async move {
            while let Some(stage) = stage_rx.recv().await {
                let Some(id) = run_id else { continue };
                if let Err(e) = state::run_stage_set(&pool, id, Some(stage)).await {
                    warn!(error = %e, stage, "updating webhook run stage failed");
                }
            }
        })
    };

    let backend = ctx.backend.clone();
    let classifier = ctx.classifier.clone();
    // CLAUDE-101: snapshot the live mint config for this event; the next event
    // re-reads, so a Settings save in between is honored without a restart.
    let mint_cfg = ctx.mint_cfg.read().await.clone();
    let route_cache = ctx.route_cache.clone();
    let event = ev.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let report_stage = move |stage: &'static str| {
            let _ = stage_tx.send(stage);
        };
        report_stage(STAGE_TRIAGING);
        let has_open_pr = resolve_has_open_pr(backend.as_ref(), &event);
        decide_mint(
            classifier.as_ref(),
            backend.as_ref(),
            &mint_cfg,
            route_cache.as_ref(),
            &caps,
            &event,
            log_text,
            log_available,
            log_artifact.as_deref(),
            log_unavailable_reason,
            has_open_pr,
            &report_stage,
        )
    })
    .await;
    let _ = stage_drainer.await;

    let (bucket, outcome) = match outcome {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint failed");
            return WebhookRunSummary::error(e.to_string());
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint task panicked");
            return WebhookRunSummary::error(format!("task panicked: {e}"));
        }
    };

    // Advance the caps ledger only on a real mint, so repeated failures for the
    // same key (and a busy day) throttle exactly like the scheduled path. Skips
    // (transient, dedupe, cap-hit, loop) create no issue and must not count.
    if matches!(outcome, MintOutcome::Created { .. }) {
        if let Err(e) = state::attempts_increment(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key).await {
            warn!(error = %e, "advancing webhook attempt counter failed");
        }
        if let Err(e) = state::daily_count_increment(&ctx.pool, WEBHOOK_MINT_ACTION).await {
            warn!(error = %e, "advancing webhook daily ledger failed");
        }
    }

    log_mint_outcome(ev, &outcome);
    WebhookRunSummary::decided(bucket, &outcome)
}

/// Pure triage + mint composition over the two traits, with `has_open_pr`
/// already resolved. Factored out so the bucket routing and dedupe are unit
/// tested against stub a classifier / tracker with no forge, DB, or process
/// spawn. Builds the [`TriageInput`], runs the hybrid [`triage::triage`], then
/// [`mint::mint`] on the result.
#[allow(clippy::too_many_arguments)]
fn decide_mint(
    classifier: &dyn FailureClassifier,
    tracker: &dyn crate::provider::IssueTracker,
    mint_cfg: &MintConfig,
    route_cache: &VcsRouteCache,
    caps: &MintCaps,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<&std::path::Path>,
    log_unavailable_reason: Option<String>,
    has_open_pr: bool,
    report_stage: &(dyn Fn(&'static str) + Send + Sync),
) -> crate::error::Result<(triage::TriageBucket, MintOutcome)> {
    let input = TriageInput {
        repo: ev.repo.clone(),
        workflow_id: ev.workflow_id.clone(),
        commit_sha: ev.commit_sha.clone(),
        html_url: ev.html_url.clone(),
        log: log_text,
        log_available,
        log_unavailable_reason,
        has_open_pr,
    };
    // CLAUDE-80: the classify stage only exists when the deterministic rules
    // abstain, so the label is reported from inside the classifier call rather
    // than unconditionally - a rules short-circuit never shows it.
    let reporting = ReportingClassifier {
        inner: classifier,
        report: report_stage,
    };
    let verdict = triage::triage(&input, &reporting, &mint_cfg.auto_fix);
    report_stage(STAGE_MINTING);
    let req = MintRequest {
        repo: &ev.repo,
        index_in_repo: ev.index_in_repo,
        commit_sha: &ev.commit_sha,
        workflow_id: &ev.workflow_id,
        html_url: &ev.html_url,
        trigger_user: &ev.trigger_user,
        triage: &verdict,
        log_path: log_artifact,
        private: ev.private,
        has_open_pr,
    };
    let outcome = mint::mint(tracker, mint_cfg, route_cache, &req, caps)?;
    Ok((verdict.bucket, outcome))
}

/// CLAUDE-80: classifier decorator that reports the "classifying with claude"
/// stage right before delegating, so the label appears exactly when (and only
/// when) the LLM pass actually runs - the deterministic rules short-circuit
/// never consults the classifier and therefore never shows it.
struct ReportingClassifier<'a> {
    inner: &'a dyn FailureClassifier,
    report: &'a (dyn Fn(&'static str) + Send + Sync),
}

impl FailureClassifier for ReportingClassifier<'_> {
    fn classify(&self, input: &TriageInput) -> crate::error::Result<crate::triage::Triage> {
        (self.report)(STAGE_CLASSIFYING);
        self.inner.classify(input)
    }
}

/// Best-effort `has_open_pr` signal for the triage classifier: list the repo's
/// open PRs and check whether the failing commit is the head of one. A forge
/// error or an underivable host degrades to `false` (treated as "no PR"), never
/// an error - the signal is advisory context for bucket 1 vs 2.
fn resolve_has_open_pr(backend: &dyn Backend, ev: &VerifiedFailureEvent) -> bool {
    let Some(host) = forge_host_from_url(&ev.html_url) else {
        warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge host for has_open_pr; assuming none");
        return false;
    };
    match backend.list_open_prs(&host, &ev.repo) {
        Ok(prs) => pr_matches_commit(&prs, &ev.commit_sha),
        Err(e) => {
            warn!(repo = %ev.repo, error = %e, "listing open PRs for has_open_pr failed; assuming none");
            false
        }
    }
}

/// True when some open PR's head commit matches `commit_sha`. Tolerates short /
/// long SHA mismatches by accepting a prefix relationship in either direction.
fn pr_matches_commit(prs: &[ForgePullRequest], commit_sha: &str) -> bool {
    prs.iter().any(|p| {
        !p.head_sha.is_empty()
            && (p.head_sha == commit_sha
                || commit_sha.starts_with(&p.head_sha)
                || p.head_sha.starts_with(commit_sha))
    })
}

/// Authority (`host[:port]`) of a forge `html_url`, the bare host the `fj`
/// forge backend addresses repos by (e.g. `dev.a8n.run`). `None` when the URL
/// has no host segment.
fn forge_host_from_url(url: &str) -> Option<String> {
    let after = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = after.split('/').next().unwrap_or("");
    (!host.is_empty()).then(|| host.to_string())
}

/// Structured log line for one mint decision, one branch per [`MintOutcome`].
fn log_mint_outcome(ev: &VerifiedFailureEvent, outcome: &MintOutcome) {
    match outcome {
        MintOutcome::Created {
            key,
            project,
            bucket,
            source,
        } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %key, %project, bucket = %bucket, routing = %source, "minted YouTrack issue from verified failure")
        }
        MintOutcome::SkippedTransient => {
            info!(repo = %ev.repo, run = ev.index_in_repo, "transient failure; minted nothing")
        }
        MintOutcome::SkippedBotLoop => {
            info!(repo = %ev.repo, run = ev.index_in_repo, trigger_user = %ev.trigger_user, "bot-loop guard; minted nothing")
        }
        MintOutcome::SkippedOpenPr => {
            info!(repo = %ev.repo, run = ev.index_in_repo, "open PR for this run; auto-fix suppressed (CLAUDE-149), minted nothing")
        }
        MintOutcome::SkippedAttemptCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "per-key attempt cap reached; minted nothing")
        }
        MintOutcome::SkippedDailyCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "daily run cap reached; minted nothing")
        }
        MintOutcome::SkippedDuplicateOpen { existing } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %existing, "open issue already exists; minted nothing")
        }
        MintOutcome::SkippedResolved { existing } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %existing, "resolved issue exists; not reopening")
        }
        MintOutcome::AssociatedWithOpen { existing } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %existing, "grouped onto existing open issue; minted nothing")
        }
    }
}

async fn forgejo_webhook(
    State(st): State<WebhookState>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let now = Instant::now();

    // Resolve the source from the live config (CLAUDE-101). An unknown source
    // 404s (it is not a configured endpoint), logged so a misdirected webhook is
    // diagnosable. Clone the source out so no read guard is held across the
    // secret-decrypt / channel-send awaits below.
    let src_cfg = {
        let cfg = st.config.read().await;
        match cfg.sources.get(&source) {
            Some(s) => s.clone(),
            None => {
                warn!(source = %source, "forgejo webhook rejected: unknown source");
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Per-source rate limit first: cheapest check, shields the HMAC +
    // secret-decrypt work from a request flood, and one source's storm never
    // starves another's bucket. The limiter map is rebuilt in lock-step with the
    // sources map (CLAUDE-101), so a resolved source always has a limiter; a
    // missing one is a bug.
    {
        let rate = st.rate.read().await;
        let Some(limiter) = rate.get(&source) else {
            warn!(source = %source, "forgejo webhook: no rate limiter for source (bug)");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if !limiter.lock().await.allow(now) {
            warn!(source = %source, "forgejo webhook rate limited");
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }

    // Resolve this source's HMAC secret. Fail closed: an unset ref or an
    // unresolvable secret means we cannot authenticate, so reject rather than
    // accept. The secret is hard-bound to the source, so a payload signed for a
    // different source's secret cannot authenticate here.
    let secret = match resolve_secret(&st, &src_cfg).await {
        Ok(s) => s,
        Err(e) => {
            warn!(source = %source, error = %e, "forgejo webhook secret unavailable; rejecting");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Read the delivery id up front so the rejection logs below can name it. The
    // value is untrusted until the signature verifies: logging it before auth is
    // fine, acting on it is not, so it is only recorded in the replay store after
    // authentication succeeds.
    let delivery = headers
        .get(DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let delivery_label = if delivery.is_empty() { "-" } else { delivery };

    // Verify the signature over the RAW body before any parsing. Both rejection
    // branches log the reason (never the signature value, the computed MAC, or
    // the body) so a silent 401 is always explained by the journal.
    let signature = headers.get(SIGNATURE_HEADER).and_then(|v| v.to_str().ok());
    let Some(signature) = signature else {
        warn!(
            source = %source,
            delivery = delivery_label,
            "forgejo webhook rejected: missing X-Forgejo-Signature header (is the webhook Secret set in Forgejo?)"
        );
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !verify_signature(secret.as_bytes(), &body, signature) {
        warn!(
            source = %source,
            delivery = delivery_label,
            body_bytes = body.len(),
            "forgejo webhook rejected: signature mismatch (stored secret and Forgejo webhook Secret differ?)"
        );
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Replay protection on authenticated requests only, against the shared store.
    // A missing delivery id is malformed; a previously-seen id inside the window
    // is a replay (including the same delivery fanned out to two sources).
    if delivery.is_empty() {
        warn!(source = %source, "forgejo webhook rejected: missing X-Forgejo-Delivery header");
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !st.seen.lock().await.check_and_record(delivery, now) {
        warn!(source = %source, delivery, "forgejo webhook replay rejected");
        return StatusCode::CONFLICT.into_response();
    }

    // Authenticated and de-duplicated: trace the accepted delivery (id + body
    // length only) before parsing so the happy path is followable at debug.
    debug!(
        source = %source,
        delivery,
        body_bytes = body.len(),
        "forgejo webhook authenticated"
    );

    // Parse only after the request is authenticated and de-duplicated.
    let payload: ActionPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            warn!(source = %source, error = %e, "forgejo webhook payload parse failed");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    match verified_failure_event(&payload, &src_cfg.allowed_repos, &source) {
        Some(event) => {
            if st.events.send(event).await.is_err() {
                warn!(source = %source, "forgejo webhook event channel closed; dropping event");
            }
            StatusCode::ACCEPTED.into_response()
        }
        // Authenticated but not an allow-listed failure: ack + drop so Forgejo
        // marks the delivery delivered and does not retry. Log which of the two
        // drop conditions fired so a 200-ack drop is never silent (the rest of
        // the handler logs every other branch).
        None => {
            let reason = if payload.action != "failure" {
                "non-failure action"
            } else {
                "repo not in allowed_repos"
            };
            info!(
                source = %source,
                delivery,
                action = %payload.action,
                repo = %payload.run.repository.full_name,
                reason,
                "forgejo webhook event dropped"
            );
            StatusCode::OK.into_response()
        }
    }
}

/// Resolve a source's configured webhook secret to plaintext, loading the age key
/// from `CLAUDE_RUN_SECRETS_KEY_FILE`. Errors when the source has no `secret_ref`
/// or the secret cannot be decrypted.
async fn resolve_secret(st: &WebhookState, src: &WebhookSource) -> crate::error::Result<String> {
    let name = src.secret_ref.as_deref().ok_or_else(|| {
        crate::error::Error::Secret("webhook source secret_ref is not configured".to_string())
    })?;
    let key = SecretsKey::load()?;
    secrets::resolve_secret(&st.pool, &key, name).await
}

/// Constant-time HMAC-SHA256 verification of `body` against the hex-encoded
/// `signature` under `secret`. Returns false on any decode/key/length error so
/// malformed input is indistinguishable from a bad signature.
pub fn verify_signature(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let expected = match hex::decode(signature.trim()) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

/// True when `repo` (an `owner/repo` full name) matches one of the allow-list
/// patterns (CLAUDE-79): exact `owner/repo`, per-owner `owner/*`, or the global
/// `*`. Only these three literal forms - no general globbing - so a typo cannot
/// silently widen what the issue-minting pipeline acts on. Matching is
/// case-sensitive, like the original exact match. An empty list denies all.
fn repo_allowed(allowed: &[String], repo: &str) -> bool {
    allowed.iter().any(|pat| match pat.as_str() {
        "*" => true,
        p => match p.strip_suffix("/*") {
            Some(owner) => repo.split_once('/').is_some_and(|(o, _)| o == owner),
            None => p == repo,
        },
    })
}

/// Build a [`VerifiedFailureEvent`] when the payload is an `action_run_failure`
/// for an allow-listed repo; `None` otherwise (caller acks and drops).
pub fn verified_failure_event(
    payload: &ActionPayload,
    allowed_repos: &[String],
    source: &str,
) -> Option<VerifiedFailureEvent> {
    if payload.action != "failure" {
        return None;
    }
    let repo = &payload.run.repository.full_name;
    if !repo_allowed(allowed_repos, repo) {
        return None;
    }
    Some(VerifiedFailureEvent {
        repo: repo.clone(),
        index_in_repo: payload.run.index_in_repo,
        commit_sha: payload.run.commit_sha.clone(),
        workflow_id: payload.run.workflow_id.clone(),
        private: payload.run.repository.private,
        run_global_id: payload.run.id,
        source: source.to_string(),
        trigger_user: payload.run.trigger_user.login.clone(),
        html_url: payload.run.html_url.clone(),
        status: payload.run.status.clone(),
    })
}

/// Time-bounded set of delivery ids for replay protection. Entries older than
/// the window are evicted lazily on each check, so the map stays bounded by the
/// delivery rate within one window.
pub struct SeenDeliveries {
    window: Duration,
    seen: HashMap<String, Instant>,
}

impl SeenDeliveries {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            seen: HashMap::new(),
        }
    }

    /// Record `id` as seen at `now`. Returns true if it is fresh (first sighting
    /// within the window), false if it is a replay.
    pub fn check_and_record(&mut self, id: &str, now: Instant) -> bool {
        self.seen
            .retain(|_, seen_at| now.duration_since(*seen_at) < self.window);
        if self.seen.contains_key(id) {
            return false;
        }
        self.seen.insert(id.to_string(), now);
        true
    }
}

/// Fixed-window rate limiter: at most `max` allowances per `window`.
pub struct RateLimiter {
    max: u32,
    window: Duration,
    window_start: Instant,
    count: u32,
}

impl RateLimiter {
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            max,
            window,
            // Start the first window in the past so `allow` resets it on the
            // first call regardless of when the limiter was constructed.
            window_start: Instant::now() - window,
            count: 0,
        }
    }

    /// True if a request at `now` is within the current window's budget.
    pub fn allow(&mut self, now: Instant) -> bool {
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.count = 0;
        }
        if self.count >= self.max {
            return false;
        }
        self.count += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"super-secret-webhook-key";

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn failure_payload(repo: &str) -> String {
        format!(
            r#"{{"action":"failure","run":{{"index_in_repo":7,"commit_sha":"abc123",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{{"login":"alice"}},
            "html_url":"https://dev.a8n.run/{repo}/actions/runs/7",
            "repository":{{"full_name":"{repo}"}}}}}}"#
        )
    }

    #[test]
    fn valid_signature_verifies() {
        let body = failure_payload("contoso/claude-run");
        let sig = sign(SECRET, body.as_bytes());
        assert!(verify_signature(SECRET, body.as_bytes(), &sig));
    }

    #[test]
    fn wrong_secret_or_tampered_body_fails() {
        let body = failure_payload("contoso/claude-run");
        let sig = sign(SECRET, body.as_bytes());
        assert!(!verify_signature(b"other-key", body.as_bytes(), &sig));
        assert!(!verify_signature(SECRET, b"tampered", &sig));
    }

    #[test]
    fn malformed_signature_fails_not_panics() {
        let body = failure_payload("contoso/claude-run");
        assert!(!verify_signature(SECRET, body.as_bytes(), "not-hex-zz"));
        assert!(!verify_signature(SECRET, body.as_bytes(), ""));
    }

    #[test]
    fn allow_listed_failure_emits_event() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("contoso/claude-run")).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert_eq!(
            event,
            VerifiedFailureEvent {
                repo: "contoso/claude-run".to_string(),
                index_in_repo: 7,
                commit_sha: "abc123".to_string(),
                workflow_id: "ci.yml".to_string(),
                private: false,
                run_global_id: 0,
                source: "test-source".to_string(),
                trigger_user: "alice".to_string(),
                html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
                status: "failure".to_string(),
            }
        );
    }

    #[test]
    fn failure_from_unlisted_repo_is_dropped() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("someone/else")).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed, "test-source").is_none());
    }

    // ---- CLAUDE-79: allow-list patterns (`*`, `owner/*`, exact) ------------

    #[test]
    fn repo_allowed_star_matches_every_repo() {
        let allowed = vec!["*".to_string()];
        assert!(repo_allowed(&allowed, "contoso/claude-run"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
    }

    #[test]
    fn repo_allowed_owner_wildcard_matches_only_that_owner() {
        let allowed = vec!["fabrikam/*".to_string()];
        assert!(repo_allowed(&allowed, "fabrikam/forgejo-cli"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
        assert!(!repo_allowed(&allowed, "contoso/claude-run"));
        // The bare owner (no slash) and a prefixed owner must not match.
        assert!(!repo_allowed(&allowed, "fabrikam"));
        assert!(!repo_allowed(&allowed, "xfabrikam/repo"));
    }

    #[test]
    fn repo_allowed_exact_and_empty_behave_as_before() {
        let exact = vec!["contoso/claude-run".to_string()];
        assert!(repo_allowed(&exact, "contoso/claude-run"));
        assert!(!repo_allowed(&exact, "contoso/governance"));
        // Case-sensitive, like the original exact match.
        assert!(!repo_allowed(&exact, "Contoso/claude-run"));
        // Empty list stays deny-all.
        assert!(!repo_allowed(&[], "contoso/claude-run"));
    }

    #[test]
    fn repo_allowed_mixed_list_takes_any_match() {
        let allowed = vec![
            "contoso/governance".to_string(),
            "fabrikam/*".to_string(),
        ];
        assert!(repo_allowed(&allowed, "contoso/governance"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
        assert!(!repo_allowed(&allowed, "contoso/claude-run"));
    }

    #[test]
    fn failure_event_emitted_for_owner_wildcard() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("fabrikam/gizmo")).unwrap();
        let allowed = vec!["fabrikam/*".to_string()];
        let event =
            verified_failure_event(&payload, &allowed, "test-source").expect("wildcard allows");
        assert_eq!(event.repo, "fabrikam/gizmo");
    }

    #[test]
    fn non_failure_action_is_dropped_even_when_listed() {
        let body = r#"{"action":"success","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"success","trigger_user":{"login":"bob"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed, "test-source").is_none());
    }

    #[test]
    fn trigger_user_accepts_username_alias() {
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{"username":"carol"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").unwrap();
        assert_eq!(event.trigger_user, "carol");
    }

    #[test]
    fn trigger_user_accepts_both_login_and_username() {
        // Live Forgejo 14.0.2 payloads carry both keys in one user object; the
        // old `#[serde(alias = "username")]` rejected this with `duplicate field
        // `login``. Both present must parse and yield the `login` value.
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure",
            "trigger_user":{"login":"dave","username":"dave"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").unwrap();
        assert_eq!(event.trigger_user, "dave");
    }

    #[test]
    fn trigger_user_with_neither_key_fails_parse() {
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let err = serde_json::from_str::<ActionPayload>(body).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("login"), "error names `login`: {msg}");
        assert!(msg.contains("username"), "error names `username`: {msg}");
    }

    #[test]
    fn replay_of_same_delivery_is_rejected() {
        let mut seen = SeenDeliveries::new(Duration::from_secs(300));
        let t0 = Instant::now();
        assert!(seen.check_and_record("delivery-1", t0), "first is fresh");
        assert!(
            !seen.check_and_record("delivery-1", t0),
            "second is a replay"
        );
        assert!(
            seen.check_and_record("delivery-2", t0),
            "a different id is fresh"
        );
    }

    #[test]
    fn delivery_id_is_fresh_again_after_window() {
        let window = Duration::from_secs(300);
        let mut seen = SeenDeliveries::new(window);
        let t0 = Instant::now();
        assert!(seen.check_and_record("delivery-1", t0));
        // Past the window the old entry is evicted, so the id is fresh again.
        let later = t0 + window + Duration::from_secs(1);
        assert!(seen.check_and_record("delivery-1", later));
    }

    #[test]
    fn rate_limiter_caps_per_window_and_resets() {
        let window = Duration::from_secs(60);
        let mut rl = RateLimiter::new(2, window);
        let t0 = Instant::now();
        assert!(rl.allow(t0));
        assert!(rl.allow(t0));
        assert!(!rl.allow(t0), "third in the same window is blocked");
        // Next window resets the budget.
        let next = t0 + window + Duration::from_secs(1);
        assert!(rl.allow(next));
    }

    // ---- CLAUDE-47: triage + mint composition -----------------------------

    use crate::triage::{ClassifiedBy, Triage, TriageBucket};
    use std::path::Path;
    use std::sync::Mutex as StdMutex;

    /// Canned [`FailureClassifier`]: returns a fixed bucket, recording that it
    /// ran so a test can assert the deterministic rules short-circuited it.
    struct StubClassifier {
        bucket: TriageBucket,
        calls: StdMutex<u32>,
    }

    impl StubClassifier {
        fn new(bucket: TriageBucket) -> Self {
            Self {
                bucket,
                calls: StdMutex::new(0),
            }
        }
    }

    impl FailureClassifier for StubClassifier {
        fn classify(&self, _input: &TriageInput) -> crate::error::Result<Triage> {
            *self.calls.lock().unwrap() += 1;
            Ok(Triage {
                bucket: self.bucket,
                analysis: "stub analysis".to_string(),
                classified_by: ClassifiedBy::Llm,
                auto_fix: None,
            })
        }
    }

    /// Minimal [`IssueTracker`] recording the mint's mutations; dedupe searches
    /// always miss (no existing issue).
    struct StubTracker {
        calls: StdMutex<Vec<String>>,
    }

    impl StubTracker {
        fn new() -> Self {
            Self {
                calls: StdMutex::new(Vec::new()),
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl crate::provider::IssueTracker for StubTracker {
        fn list_queued(&self, query: &str) -> crate::error::Result<Vec<serde_json::Value>> {
            self.calls.lock().unwrap().push(format!("list {query}"));
            Ok(Vec::new())
        }
        fn get_field(&self, _key: &str, _field: &str) -> crate::error::Result<String> {
            unreachable!()
        }
        fn set_field(&self, key: &str, field: &str, value: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_field {key} {field} {value}"));
            Ok(())
        }
        fn set_state(&self, key: &str, state: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_state {key} {state}"));
            Ok(())
        }
        fn set_assignee(&self, key: &str, login: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_assignee {key} {login}"));
            Ok(())
        }
        fn comment(&self, _key: &str, _body: &str) -> crate::error::Result<()> {
            unreachable!()
        }
        fn resolve_vcs(
            &self,
            _project_key: &str,
        ) -> crate::error::Result<crate::provider::VcsRepo> {
            unreachable!()
        }
        fn vcs_repo_map(&self) -> crate::error::Result<Vec<crate::provider::VcsMapEntry>> {
            // The webhook tests route via the explicit override, so the map is
            // never consulted; return empty to keep the stub total.
            Ok(Vec::new())
        }
        fn create_issue(
            &self,
            project: &str,
            _summary: &str,
            _description: &str,
            assignee: Option<&str>,
        ) -> crate::error::Result<String> {
            self.calls.lock().unwrap().push(format!(
                "create {project} assignee={}",
                assignee.unwrap_or("-")
            ));
            Ok("CLAUDE-99".to_string())
        }
        fn attach_file(&self, _key: &str, _file: &Path) -> crate::error::Result<()> {
            Ok(())
        }
    }

    fn event() -> VerifiedFailureEvent {
        VerifiedFailureEvent {
            repo: "fabrikam/forgejo-cli".to_string(),
            index_in_repo: 7,
            commit_sha: "abc123def456".to_string(),
            workflow_id: "ci.yml".to_string(),
            private: false,
            run_global_id: 4242,
            source: "test-source".to_string(),
            trigger_user: "alice".to_string(),
            html_url: "https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/7".to_string(),
            status: "failure".to_string(),
        }
    }

    fn mint_cfg() -> MintConfig {
        MintConfig {
            repo_project_map: [("fabrikam/forgejo-cli".to_string(), "FJ".to_string())]
                .into_iter()
                .collect(),
            human_assignee: Some("bob".to_string()),
            ..MintConfig::default()
        }
    }

    fn caps() -> MintCaps {
        MintCaps {
            attempts: 0,
            max_attempts: 3,
            daily: 0,
            max_runs_per_day: 20,
        }
    }

    fn route_cache() -> VcsRouteCache {
        VcsRouteCache::new(std::time::Duration::from_secs(900))
    }

    #[test]
    fn unavailable_log_mints_a_human_action_issue_without_the_classifier() {
        // CLAUDE-92: log_available = false -> the deterministic rules route to
        // bucket 2 (a real failure must surface even without a log), the LLM
        // classifier never runs, and an issue IS minted with the reason in the
        // analysis.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            String::new(),
            false,
            None,
            Some("private repo: unauthorized".to_string()),
            false,
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::HumanAction);
        assert!(
            matches!(
                out,
                MintOutcome::Created {
                    bucket: TriageBucket::HumanAction,
                    ..
                }
            ),
            "{out:?}"
        );
        assert_eq!(*classifier.calls.lock().unwrap(), 0, "rules short-circuit");
        assert!(
            tracker.calls().iter().any(|c| c.starts_with("create")),
            "{:?}",
            tracker.calls()
        );
    }

    #[test]
    fn transient_signature_in_log_mints_nothing() {
        // A clear infra signature is bucket 3 by the rules pass, even with a log.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "error: no runner available for this job".to_string(),
            true,
            None,
            None,
            true,
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::Transient);
        assert_eq!(out, MintOutcome::SkippedTransient);
        assert_eq!(*classifier.calls.lock().unwrap(), 0);
    }

    #[test]
    fn code_fix_classification_mints_and_queues() {
        // A non-transient log + a code_fix verdict with NO open PR -> issue in the
        // mapped project, queued for the fix bot, no human assignee. (With an open
        // PR the code_fix route is suppressed per CLAUDE-149; see
        // `open_pr_code_fix_is_left_to_the_author_and_mints_nothing`.)
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "assertion failed: expected 1 got 2".to_string(),
            true,
            None,
            None,
            false, // has_open_pr: no PR, so the code_fix route still queues
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::CodeFix);
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: TriageBucket::CodeFix,
                source: crate::mint::RouteSource::Override,
            }
        );
        assert_eq!(*classifier.calls.lock().unwrap(), 1, "classifier ran once");
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c == "create FJ assignee=-"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
    }

    #[test]
    fn human_action_classification_assigns_a_human() {
        let classifier = StubClassifier::new(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "the release tag push to main failed".to_string(),
            true,
            None,
            None,
            false,
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::HumanAction);
        assert!(matches!(
            out,
            MintOutcome::Created {
                bucket: TriageBucket::HumanAction,
                ..
            }
        ));
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c == "create FJ assignee=bob"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("AI Agent")), "{calls:?}");
    }

    #[test]
    fn cargo_fmt_push_to_main_routes_to_auto_fix_and_queues_without_the_llm() {
        // CLAUDE-104 end-to-end through decide_mint: a check.yml run with NO open
        // PR (push to main) failing on `cargo fmt --check` is routed
        // deterministically to the auto-fix bucket - queued for the runner, no
        // human assignee - and the LLM classifier (which would say human_action)
        // is never consulted.
        let classifier = StubClassifier::new(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let mut ev = event();
        ev.workflow_id = "check.yml".to_string();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &ev,
            "Run cargo fmt --check\nDiff in /build/src/main.rs at line 1:\n".to_string(),
            true,
            None,
            None,
            false, // has_open_pr: push to main, no PR
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::AutoFix);
        assert!(
            matches!(
                out,
                MintOutcome::Created {
                    bucket: TriageBucket::AutoFix,
                    ..
                }
            ),
            "{out:?}"
        );
        assert_eq!(
            *classifier.calls.lock().unwrap(),
            0,
            "auto-fix short-circuits the LLM"
        );
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c == "create FJ assignee=-"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
    }

    #[test]
    fn open_pr_code_fix_is_left_to_the_author_and_mints_nothing() {
        // CLAUDE-149 end-to-end through decide_mint: a failure WITH an open PR
        // that the classifier calls code_fix is the author's to fix. The
        // queue-for-fix route is suppressed, so no issue is minted and the fix
        // bot is never queued.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let ev = event();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &ev,
            "error[E0308]: mismatched types".to_string(),
            true,
            None,
            None,
            true, // has_open_pr: a failing open PR
            &|_| {},
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::CodeFix);
        assert_eq!(out, MintOutcome::SkippedOpenPr);
        let calls = tracker.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "no issue minted for an open-PR failure: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("AI Agent")),
            "fix bot never queued: {calls:?}"
        );
    }

    #[test]
    fn pr_matches_commit_handles_exact_and_prefix() {
        let pr = |sha: &str| ForgePullRequest {
            number: 1,
            title: String::new(),
            head_sha: sha.to_string(),
            head_ref: String::new(),
            author: String::new(),
            html_url: String::new(),
            updated_at: String::new(),
        };
        assert!(pr_matches_commit(&[pr("abc123")], "abc123"));
        // Full failing SHA, short PR head SHA -> prefix match still counts.
        assert!(pr_matches_commit(&[pr("abc123")], "abc123def456"));
        assert!(!pr_matches_commit(&[pr("def999")], "abc123def456"));
        // An empty head_sha never matches.
        assert!(!pr_matches_commit(&[pr("")], "abc123"));
        assert!(!pr_matches_commit(&[], "abc123"));
    }

    #[test]
    fn forge_host_from_url_extracts_authority() {
        assert_eq!(
            forge_host_from_url("https://dev.a8n.run/owner/repo/actions/runs/7").as_deref(),
            Some("dev.a8n.run")
        );
        assert_eq!(
            forge_host_from_url("http://localhost:3000/o/r").as_deref(),
            Some("localhost:3000")
        );
        assert_eq!(forge_host_from_url("").as_deref(), None);
    }

    // ---- CLAUDE-61: webhook runs are recorded ----------------------------

    /// Canned [`LogTransport`] returning a fixed 200 body, so the fetch is
    /// `Ready` on the first attempt and the backoff loop never sleeps.
    struct ReadyTransport;
    impl crate::forge_log::LogTransport for ReadyTransport {
        fn get(
            &self,
            _url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<crate::forge_log::HttpResponse, String> {
            Ok(crate::forge_log::HttpResponse {
                status: 200,
                body: "assertion failed: expected 1 got 2".to_string(),
            })
        }
    }

    async fn memory_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::db::MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    /// CLAUDE-85: a transport that fails the test on any GET, proving the
    /// skipped-run short-circuit never attempts a log fetch.
    struct PanicTransport;
    impl crate::forge_log::LogTransport for PanicTransport {
        fn get(
            &self,
            url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<crate::forge_log::HttpResponse, String> {
            panic!("log fetch attempted for a non-failure run: {url}");
        }
    }

    #[tokio::test]
    async fn non_failure_statuses_finish_without_a_log_fetch() {
        // CLAUDE-85: Forgejo fires action_run_failure for every non-success
        // done status (bare `else` in ActionRunNowDone), so `skipped` (server-
        // side skip, no log will ever exist) and `cancelled` (deliberately
        // stopped) must both bypass the fetch + triage instead of burning the
        // 60s retry budget or minting an issue.
        for status in ["skipped", "cancelled"] {
            let pool = memory_pool().await;
            let out_dir = tempfile::tempdir().unwrap();
            let (tx, rx) = mpsc::channel(4);
            let mut ev = event();
            ev.status = status.to_string();
            tx.send(ev).await.unwrap();
            drop(tx);

            consume_events(
                rx,
                pool.clone(),
                Arc::new(PanicTransport),
                out_dir.path().to_path_buf(),
                None,
                Arc::new(RwLock::new(WebhookConfig::default())),
            )
            .await;

            let (jobs, exit): (i64, Option<i64>) =
                sqlx::query_as("SELECT jobs_processed, exit_code FROM runs")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(jobs, 1, "{status}");
            assert_eq!(exit, Some(crate::exit::OK as i64), "{status}");

            let (ok, summary): (bool, Option<String>) =
                sqlx::query_as("SELECT ok, summary FROM job_outcomes")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(ok, "{status}");
            let summary = summary.expect("summary recorded");
            assert!(summary.contains(status), "{status}: {summary}");
            assert!(summary.contains("no issue"), "{status}: {summary}");
        }
    }

    #[test]
    fn verified_failure_event_carries_the_run_status() {
        // CLAUDE-85: a delivery with action=failure but a non-failure run
        // status (cancelled / skipped flavors) surfaces that status on the
        // event so the consumer can route on it.
        let body = failure_payload("contoso/claude-run")
            .replace(r#""status":"failure""#, r#""status":"skipped""#);
        let payload: ActionPayload = serde_json::from_str(&body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert_eq!(event.status, "skipped");
    }

    #[test]
    fn verified_failure_event_carries_repo_visibility() {
        // CLAUDE-92: `repository.private` rides the payload onto the event; a
        // payload without the field (older/foreign forge) defaults to public.
        let body = failure_payload("contoso/claude-run").replace(
            r#""repository":{"full_name":"contoso/claude-run"}"#,
            r#""repository":{"full_name":"contoso/claude-run","private":true}"#,
        );
        let payload: ActionPayload = serde_json::from_str(&body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert!(event.private, "private flag carried");

        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("contoso/claude-run")).unwrap();
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert!(!event.private, "absent field defaults to public");
    }

    #[tokio::test]
    async fn private_repo_event_never_touches_the_log_route() {
        // CLAUDE-92: the payload says the repo is private and this Forgejo has
        // no token auth for the internal logs route, so the consumer must make
        // ZERO fetch requests (no 60s retry burn) and still record the run.
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel(4);
        let mut ev = event();
        ev.private = true;
        tx.send(ev).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            Arc::new(PanicTransport),
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
        )
        .await;

        let (jobs, exit): (i64, Option<i64>) =
            sqlx::query_as("SELECT jobs_processed, exit_code FROM runs")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(jobs, 1);
        assert_eq!(exit, Some(crate::exit::OK as i64));

        let summary: Option<String> = sqlx::query_scalar("SELECT summary FROM job_outcomes")
            .fetch_one(&pool)
            .await
            .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log unavailable"), "{summary}");
    }

    /// CLAUDE-96: bytes-only transport for the REST run-logs endpoint. `get`
    /// panics so the private path can never fall back to the internal text
    /// route; `get_bytes` serves one fixed response and counts calls.
    struct ApiBytesTransport {
        status: u16,
        body: Vec<u8>,
        calls: std::sync::Mutex<u32>,
    }

    impl ApiBytesTransport {
        fn new(status: u16, body: Vec<u8>) -> Self {
            Self {
                status,
                body,
                calls: std::sync::Mutex::new(0),
            }
        }
    }

    impl crate::forge_log::LogTransport for ApiBytesTransport {
        fn get(
            &self,
            url: &str,
            _token: Option<&str>,
        ) -> std::result::Result<crate::forge_log::HttpResponse, String> {
            panic!("internal text route used for a private repo: {url}");
        }
        fn get_bytes(
            &self,
            url: &str,
            token: Option<&str>,
        ) -> std::result::Result<(u16, Vec<u8>), String> {
            assert!(
                url.contains("/api/v1/repos/fabrikam/forgejo-cli/actions/runs/4242/logs"),
                "API URL addressed by the global run id: {url}"
            );
            assert_eq!(token, Some("test-pat"), "token sent");
            *self.calls.lock().unwrap() += 1;
            Ok((self.status, self.body.clone()))
        }
    }

    /// Shared scaffolding for the private+token consume tests: age key file,
    /// stored `log-token` secret, and a webhook config whose `test-source`
    /// (the `event()` fixture's source) carries the `log_token_ref`.
    async fn log_token_setup(tmp: &std::path::Path) -> (SqlitePool, Arc<RwLock<WebhookConfig>>) {
        use age::secrecy::ExposeSecret;
        let key_path = tmp.join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

        let pool = memory_pool().await;
        let key = SecretsKey::load().unwrap();
        let ciphertext = key.encrypt("test-pat").unwrap();
        state::secret_create(&pool, "log-token", &ciphertext)
            .await
            .unwrap();

        let mut cfg = WebhookConfig::default();
        cfg.sources.insert(
            "test-source".to_string(),
            WebhookSource {
                log_token_ref: Some("log-token".to_string()),
                ..Default::default()
            },
        );
        (pool, Arc::new(RwLock::new(cfg)))
    }

    fn run_logs_zip() -> Vec<u8> {
        use std::io::Write;
        let mut buf = std::io::Cursor::new(Vec::new());
        let mut w = zip::ZipWriter::new(&mut buf);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        w.start_file("check-8-attempt-1.log", opts).unwrap();
        w.write_all(b"assertion failed: expected 1 got 2\n")
            .unwrap();
        w.finish().unwrap();
        buf.into_inner()
    }

    #[tokio::test]
    async fn private_repo_with_token_fetches_logs_via_the_api() {
        // CLAUDE-96: token configured + v16 endpoint available -> exactly one
        // authenticated request, log redacted to an artifact, pipeline behaves
        // like a public fetch.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let (pool, cfg) = log_token_setup(tmp.path()).await;
        let transport = Arc::new(ApiBytesTransport::new(200, run_logs_zip()));

        let (tx, rx) = mpsc::channel(4);
        let mut ev = event();
        ev.private = true;
        tx.send(ev).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            transport.clone(),
            tmp.path().to_path_buf(),
            None,
            cfg,
        )
        .await;

        assert_eq!(*transport.calls.lock().unwrap(), 1, "exactly one request");
        let summary: Option<String> = sqlx::query_scalar("SELECT summary FROM job_outcomes")
            .fetch_one(&pool)
            .await
            .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log fetched and redacted"), "{summary}");
        assert!(summary.contains("log="), "artifact referenced: {summary}");
    }

    #[tokio::test]
    async fn private_repo_with_token_degrades_on_a_pre_v16_instance() {
        // CLAUDE-96: the endpoint 404s on Forgejo < 16; exactly one request,
        // no retry burn, and the failure still surfaces as a no-log outcome.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let (pool, cfg) = log_token_setup(tmp.path()).await;
        let transport = Arc::new(ApiBytesTransport::new(404, Vec::new()));

        let (tx, rx) = mpsc::channel(4);
        let mut ev = event();
        ev.private = true;
        tx.send(ev).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            transport.clone(),
            tmp.path().to_path_buf(),
            None,
            cfg,
        )
        .await;

        assert_eq!(*transport.calls.lock().unwrap(), 1, "exactly one request");
        let summary: Option<String> = sqlx::query_scalar("SELECT summary FROM job_outcomes")
            .fetch_one(&pool)
            .await
            .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log unavailable"), "{summary}");
    }

    #[tokio::test]
    async fn consumed_event_records_a_webhook_run() {
        // Drive `consume_events` with one event and no mint context (log-only),
        // then assert the run + outcome rows landed under the synthetic action.
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel(4);
        tx.send(event()).await.unwrap();
        drop(tx); // close the channel so `consume_events` returns after draining.

        consume_events(
            rx,
            pool.clone(),
            Arc::new(ReadyTransport),
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
        )
        .await;

        // One finished run, recorded under the synthetic action + webhook source.
        // CLAUDE-80: the finished run carries no stage (cleared by run_finish)
        // and no stale in-flight marker.
        let (action, triggered, jobs, exit, stage): (
            String,
            String,
            i64,
            Option<i64>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT action_name, triggered_by, jobs_processed, exit_code, stage FROM runs",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(action, WEBHOOK_MINT_ACTION);
        assert_eq!(triggered, WEBHOOK_TRIGGERED_BY);
        assert_eq!(jobs, 1);
        assert_eq!(exit, Some(crate::exit::OK as i64));
        assert_eq!(stage, None, "stage cleared on finish");
        let inflight: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_inflight_jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(inflight, 0, "in-flight marker cleared on finish");

        // CLAUDE-93: the event's html_url is recorded as the job's outbound
        // link so the run-detail tables can link the key to the Forgejo run.
        let run_id: i64 = sqlx::query_scalar("SELECT id FROM runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        let links = state::run_job_links(&pool, run_id).await.unwrap();
        assert_eq!(
            links.get("fabrikam/forgejo-cli#7").map(String::as_str),
            Some("https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/7"),
            "{links:?}"
        );

        // The outcome carries a summary; with no backend it is the log-only note,
        // and it references the redacted artifact that the ready fetch wrote.
        let (job_key, summary): (String, Option<String>) =
            sqlx::query_as("SELECT job_key, summary FROM job_outcomes")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(job_key, "fabrikam/forgejo-cli#7");
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log-only"), "{summary}");
        assert!(summary.contains("log="), "artifact reference: {summary}");
    }

    #[test]
    fn decide_mint_reports_stages_in_order_for_an_llm_classification() {
        // CLAUDE-80: a non-transient log goes rules -> LLM -> mint, so the
        // reporter sees the classify stage between triage and mint.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let stages = std::sync::Mutex::new(Vec::new());
        let report = |s: &'static str| stages.lock().unwrap().push(s);
        decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "assertion failed: expected 1 got 2".to_string(),
            true,
            None,
            None,
            true,
            &report,
        )
        .unwrap();
        assert_eq!(
            *stages.lock().unwrap(),
            vec![STAGE_CLASSIFYING, STAGE_MINTING]
        );
    }

    #[test]
    fn decide_mint_skips_the_classify_stage_on_a_rules_short_circuit() {
        // CLAUDE-80: a rules-bucketed event (transient signature) never runs
        // the LLM, so "classifying with claude" must never be reported.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let stages = std::sync::Mutex::new(Vec::new());
        let report = |s: &'static str| stages.lock().unwrap().push(s);
        decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "error: no runner available for this job".to_string(),
            true,
            None,
            None,
            true,
            &report,
        )
        .unwrap();
        assert_eq!(*stages.lock().unwrap(), vec![STAGE_MINTING]);
    }

    #[test]
    fn mint_summary_describes_each_outcome() {
        assert_eq!(
            mint_summary(&MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: crate::triage::TriageBucket::CodeFix,
                source: crate::mint::RouteSource::Override,
            }),
            "minted CLAUDE-99 in FJ"
        );
        assert!(mint_summary(&MintOutcome::SkippedTransient).contains("transient"));
        assert!(mint_summary(&MintOutcome::SkippedDailyCap).contains("daily run cap"));
        // A decided skip is still an `ok` run (the pipeline completed).
        let s = WebhookRunSummary::decided(
            crate::triage::TriageBucket::Transient,
            &MintOutcome::SkippedTransient,
        );
        assert!(s.ok);
        assert!(s.text.contains("triage=transient"));
        assert!(!WebhookRunSummary::error("boom".to_string()).ok);
    }

    // ---- CLAUDE-64 / CLAUDE-87: every auth-rejection branch is reachable ----
    //
    // Drive `forgejo_webhook` directly so each rejection branch (and the
    // accepted path that emits the debug line) is exercised at the status-code
    // level. CLAUDE-87 adds per-source secret isolation, unknown-source 404,
    // and per-source rate buckets. These set the process-global key-file env, so
    // they take the shared lock with the secrets-UI test in `web::mod`.
    mod handler_auth {
        use super::*;
        use age::secrecy::ExposeSecret;
        use axum::http::HeaderValue;

        // Two sources, each with its own secret and allow-list, exercise the
        // CLAUDE-87 per-source isolation.
        const SOURCE_A: &str = "contoso";
        const SECRET_A: &str = "hmac-value-for-contoso";
        const SOURCE_B: &str = "fabrikam";
        const SECRET_B: &str = "hmac-value-for-fabrikam";

        /// Point the key-file env at a fresh key under `tmp` and store both
        /// sources' HMAC secrets, returning the backing pool. The caller holds
        /// [`crate::secrets::key_file_env_lock`] so the env stays stable across
        /// the `resolve_secret` call inside the handler.
        async fn setup_pool(tmp: &std::path::Path) -> SqlitePool {
            let key_path = tmp.join("key.txt");
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
            std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

            let pool = memory_pool().await;
            let key = SecretsKey::load().unwrap();
            for (name, value) in [("hmac-contoso", SECRET_A), ("hmac-fabrikam", SECRET_B)] {
                let ciphertext = key.encrypt(value).unwrap();
                state::secret_create(&pool, name, &ciphertext)
                    .await
                    .unwrap();
            }
            pool
        }

        /// A two-source config: `contoso` (exact repo) and `fabrikam`
        /// (owner wildcard), each rate-limited at `rate_per_min`.
        fn two_source_config(rate_per_min: u32) -> WebhookConfig {
            let sources = [
                (
                    SOURCE_A.to_string(),
                    WebhookSource {
                        secret_ref: Some("hmac-contoso".to_string()),
                        allowed_repos: vec!["contoso/claude-run".to_string()],
                        rate_limit_per_min: rate_per_min,
                        log_token_ref: None,
                    },
                ),
                (
                    SOURCE_B.to_string(),
                    WebhookSource {
                        secret_ref: Some("hmac-fabrikam".to_string()),
                        allowed_repos: vec!["fabrikam/*".to_string()],
                        rate_limit_per_min: rate_per_min,
                        log_token_ref: None,
                    },
                ),
            ]
            .into_iter()
            .collect();
            WebhookConfig {
                replay_window_secs: 300,
                sources,
            }
        }

        /// A [`WebhookState`] over [`two_source_config`] at the default rate.
        async fn state(
            tmp: &std::path::Path,
        ) -> (WebhookState, mpsc::Receiver<VerifiedFailureEvent>) {
            let pool = setup_pool(tmp).await;
            let (tx, rx) = mpsc::channel(8);
            (WebhookState::new(pool, two_source_config(60), tx), rx)
        }

        fn headers(signature: Option<&str>, delivery: Option<&str>) -> HeaderMap {
            let mut h = HeaderMap::new();
            if let Some(s) = signature {
                h.insert(SIGNATURE_HEADER, HeaderValue::from_str(s).unwrap());
            }
            if let Some(d) = delivery {
                h.insert(DELIVERY_HEADER, HeaderValue::from_str(d).unwrap());
            }
            h
        }

        #[tokio::test]
        async fn unknown_source_is_404() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            // A path segment that names no configured source short-circuits to 404
            // before any rate / crypto work.
            let resp = forgejo_webhook(
                State(st),
                Path("no-such-source".to_string()),
                headers(Some(&sig), Some("d-0")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn missing_signature_header_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let body = Bytes::from(failure_payload("contoso/claude-run"));
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(None, Some("d-1")),
                body,
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn bad_signature_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let body = Bytes::from(failure_payload("contoso/claude-run"));
            // Well-formed hex but the wrong MAC: the signature value never leaks
            // into a log; only the delivery id and body length are recorded.
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(Some("deadbeef"), Some("d-2")),
                body,
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn cross_source_signature_is_401() {
            // CLAUDE-87: a payload validly signed with source A's secret must NOT
            // authenticate on source B's path. Source B verifies against its own
            // secret only, so the otherwise-valid signature mismatches -> 401.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("fabrikam/gizmo");
            // Sign with A's secret, deliver to B.
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_B.to_string()),
                headers(Some(&sig), Some("x-1")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn unset_secret_ref_fails_closed_401() {
            // CLAUDE-87: a source whose secret_ref is unset cannot authenticate;
            // it must fail closed with 401, not accept.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let sources = [(
                "secretless".to_string(),
                WebhookSource {
                    secret_ref: None,
                    allowed_repos: vec!["*".to_string()],
                    rate_limit_per_min: 60,
                    log_token_ref: None,
                },
            )]
            .into_iter()
            .collect();
            let config = WebhookConfig {
                replay_window_secs: 300,
                sources,
            };
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, config, tx);
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path("secretless".to_string()),
                headers(Some(&sig), Some("s-1")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn missing_delivery_id_is_400() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig), None),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn authenticated_delivery_is_accepted() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig), Some("d-3")),
                Bytes::from(raw),
            )
            .await;
            // Accepted path: the debug "authenticated" line fires here and the
            // verified failure reaches the event channel.
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(rx.try_recv().is_ok());
        }

        #[tokio::test]
        async fn each_source_authenticates_on_its_own_path() {
            // CLAUDE-87: both sources accept a delivery signed with their own
            // secret and allow-listed for their own repos, proving four (here two)
            // endpoints work independently off one daemon.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;

            let raw_a = failure_payload("contoso/claude-run");
            let sig_a = sign(SECRET_A.as_bytes(), raw_a.as_bytes());
            let resp_a = forgejo_webhook(
                State(st.clone()),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig_a), Some("a-1")),
                Bytes::from(raw_a),
            )
            .await;
            assert_eq!(resp_a.status(), StatusCode::ACCEPTED);

            let raw_b = failure_payload("fabrikam/gizmo");
            let sig_b = sign(SECRET_B.as_bytes(), raw_b.as_bytes());
            let resp_b = forgejo_webhook(
                State(st),
                Path(SOURCE_B.to_string()),
                headers(Some(&sig_b), Some("b-1")),
                Bytes::from(raw_b),
            )
            .await;
            assert_eq!(resp_b.status(), StatusCode::ACCEPTED);

            assert_eq!(rx.recv().await.unwrap().repo, "contoso/claude-run");
            assert_eq!(rx.recv().await.unwrap().repo, "fabrikam/gizmo");
        }

        #[tokio::test]
        async fn rate_limit_is_per_source() {
            // CLAUDE-87: exhausting source A's budget (rate=1) must not 429
            // source B. A's second delivery is 429; B's first still 202.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, two_source_config(1), tx);

            let raw1 = failure_payload("contoso/claude-run");
            let sig1 = sign(SECRET_A.as_bytes(), raw1.as_bytes());
            let r1 = forgejo_webhook(
                State(st.clone()),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig1), Some("r-1")),
                Bytes::from(raw1),
            )
            .await;
            assert_eq!(r1.status(), StatusCode::ACCEPTED);

            // Source A's budget (1/min) is spent: the next A delivery is 429.
            let raw2 = failure_payload("contoso/claude-run");
            let sig2 = sign(SECRET_A.as_bytes(), raw2.as_bytes());
            let r2 = forgejo_webhook(
                State(st.clone()),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig2), Some("r-2")),
                Bytes::from(raw2),
            )
            .await;
            assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);

            // Source B's independent bucket is untouched: its first delivery 202s.
            let raw3 = failure_payload("fabrikam/gizmo");
            let sig3 = sign(SECRET_B.as_bytes(), raw3.as_bytes());
            let r3 = forgejo_webhook(
                State(st),
                Path(SOURCE_B.to_string()),
                headers(Some(&sig3), Some("r-3")),
                Bytes::from(raw3),
            )
            .await;
            assert_eq!(r3.status(), StatusCode::ACCEPTED);
        }

        #[tokio::test]
        async fn replay_store_is_shared_across_sources() {
            // CLAUDE-87: delivery ids are globally unique, so a replay of one
            // source's delivery id on another source is rejected (409) by the
            // shared store. The first delivery to A authenticates and records the
            // id; the same id delivered to B (validly signed for B) is a replay.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;

            let raw_a = failure_payload("contoso/claude-run");
            let sig_a = sign(SECRET_A.as_bytes(), raw_a.as_bytes());
            let r1 = forgejo_webhook(
                State(st.clone()),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig_a), Some("dup-1")),
                Bytes::from(raw_a),
            )
            .await;
            assert_eq!(r1.status(), StatusCode::ACCEPTED);

            let raw_b = failure_payload("fabrikam/gizmo");
            let sig_b = sign(SECRET_B.as_bytes(), raw_b.as_bytes());
            let r2 = forgejo_webhook(
                State(st),
                Path(SOURCE_B.to_string()),
                headers(Some(&sig_b), Some("dup-1")),
                Bytes::from(raw_b),
            )
            .await;
            assert_eq!(r2.status(), StatusCode::CONFLICT);
        }

        #[tokio::test]
        async fn non_failure_action_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run")
                .replace(r#""action":"failure""#, r#""action":"recover""#);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig), Some("d-4")),
                Bytes::from(raw),
            )
            .await;
            // Drop path ("non-failure action"): 200 so Forgejo does not retry,
            // the info "event dropped" line fires, nothing reaches the channel.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn unlisted_repo_is_acked_and_dropped() {
            // CLAUDE-87: source A's allow-list is `contoso/claude-run` only, so
            // a validly signed delivery for a different repo is acked + dropped.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("someone-else/other-repo");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(SOURCE_A.to_string()),
                headers(Some(&sig), Some("d-5")),
                Bytes::from(raw),
            )
            .await;
            // Drop path ("repo not in allowed_repos"): same 200 ack, no event.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }
    }
}
